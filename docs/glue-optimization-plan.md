# Glue Hudi Import Job 优化计划

## 背景

Glue ETL 作业 (`DataImportIntoLake`) 从 CSV 文件导入传感器数据到 Apache Hudi 数据湖。当前实现存在性能问题和设计缺陷。

**代码位置**: `src/glue/hudi_import/script.py`

## 前提条件

- **Glue job `max_concurrent_runs = 1`**：防止多个 job 同时处理同一批文件（当前已配置）
- **保留旧脚本备份**：部署前备份 S3 上的原脚本，用于快速回退
- **Terraform 配置同步**：代码改动需同步更新 `iac/glue.tf`

## 当前问题

### 严重性能问题

| 问题 | 位置 | 影响 |
|------|------|------|
| **DataFrame 重复计算** | 第 167 行: `filtered_df.count()` 在 `write()` 之后 | 数据处理两次，执行时间翻倍 |
| **串行归档操作** | 第 179-197 行: 逐个 S3 copy+delete | 100 文件 ≈ 30s，1000 文件 ≈ 5 分钟 |
| **无批次限制** | 读取目录下所有文件 | 2000+ 文件导致超时/内存溢出 |

### 设计问题

| 问题 | 位置 | 说明 |
|------|------|------|
| 全局可变状态 | 第 28 行: `processed_files: list[str] = []` | 难以测试，容易出错 |
| 硬编码 S3 路径 | 第 98、181、191 行 | 维护困难，与 Terraform 配置不同步 |
| 死代码 | 第 111-115 行: category_id 逻辑 | 两个分支执行相同操作 |
| 使用私有 API | 第 103 行: `_jdf.inputFiles()` | Spark 升级可能失效 |
| 日志不一致 | 混用 `print()` 和 `logger.info()` | 可观测性差 |
| 无空数据检查 | - | 无数据时仍尝试写入 Hudi |
| 无用的 pandas import | 第 17、25 行 | pandas 从未使用，Glue 用 Spark DataFrame |

## 优化方案

### 第一阶段：循环批处理（核心改动）

**目标**: 每批处理 500 个文件，循环直到全部处理完成。

```
触发一次 Glue Job
        │
        ▼
┌─────────────────────────────────────────┐
│  all_files = boto3 列出所有文件          │
│  batches = 按 BATCH_SIZE 分组            │
│                                         │
│  for batch in batches:                  │
│    spark.read.load([file1, file2, ...]) │
│    写入 Hudi                            │
│    归档这批文件                          │
└─────────────────────────────────────────┘
        │
        ▼
   全部处理完成，退出
```

**关键改动**:
1. 先用 boto3 一次性列出所有待处理文件
2. 按 `BATCH_SIZE` 分组成多个批次
3. 逐批调用 `spark.read.load([file1, file2, ...])` 读取指定文件
4. 每批处理后归档

**优势**：
- 文件列表在开始时确定，不受归档失败影响
- 不会因归档失败导致重复处理
- 处理过程中新增的文件等下次 job 运行

**配置项**:
```python
BATCH_SIZE = 500  # 每批文件数
MAX_BATCHES = 100  # 最大批次数（安全限制）
```

### 第二阶段：修复性能问题

#### 2.1 移除重复计算

```python
# 之前（问题代码）
filtered_df.write.format("hudi")...save(table_path)
print(f"Rows inserted: {filtered_df.count()}")  # 重新计算整个 DataFrame！

# 之后（优化代码）
filtered_df.cache()
row_count = filtered_df.count()  # 只计算一次
filtered_df.write.format("hudi")...save(table_path)
logger.info(f"Rows inserted: {row_count}")
filtered_df.unpersist()
```

#### 2.2 并行归档操作

```python
# 之前（问题代码）
for file_path in processed_files:
    s3.copy(...)
    s3.delete(...)

# 之后（优化代码）
ARCHIVE_WORKERS = 10

with ThreadPoolExecutor(max_workers=ARCHIVE_WORKERS) as executor:
    futures = [executor.submit(archive_file, f) for f in files]
    for future in as_completed(futures):
        ...
```

#### 2.3 归档失败处理

**问题**：如果归档失败，文件留在源目录，下次循环会重复处理，导致数据重复写入 Hudi。

**解决方案**：
```python
def archive_files(file_paths: list[str], max_retries: int = 3) -> tuple[int, int, list[str]]:
    """
    Returns: (success_count, failure_count, failed_files)
    """
    failed_files = []
    for file_path in file_paths:
        for attempt in range(max_retries):
            try:
                archive_single_file(file_path)
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Archive failed after {max_retries} attempts: {file_path}, {e}")
                    failed_files.append(file_path)

    return success_count, len(failed_files), failed_files
```

**失败文件处理策略**：
- 记录到 CloudWatch 日志
- 移动到 `sensorDataFilesError/` 目录（可选）
- 下次运行时跳过已在错误目录的文件

#### 2.4 运行时间保护

**问题**：大量文件可能导致 job 运行数小时，接近 timeout。

**解决方案**：
```python
MAX_RUNTIME_SECONDS = 3600 * 4  # 4 小时上限
job_start_time = datetime.now()

all_files = list_s3_files()
batches = chunk_list(all_files, BATCH_SIZE)[:MAX_BATCHES]  # 限制最大批次

for batch_num, batch in enumerate(batches, 1):
    # 检查运行时间
    elapsed = (datetime.now() - job_start_time).total_seconds()
    if elapsed > MAX_RUNTIME_SECONDS:
        logger.warning(f"Max runtime reached ({MAX_RUNTIME_SECONDS}s), "
                      f"processed {batch_num - 1}/{len(batches)} batches")
        break

    # ... 处理逻辑
```

### 第三阶段：代码质量改进

#### 3.1 消除全局状态

```python
# 之前
processed_files: list[str] = []

def perform_hudi_upsert(...):
    processed_files.extend(...)

def archive_processed_files():
    for f in processed_files:
        ...

# 之后
def perform_hudi_upsert(...) -> list[str]:
    ...
    return input_files

def archive_files(file_paths: list[str]) -> None:
    ...
```

#### 3.2 提取常量

```python
# 文件顶部
SOURCE_BUCKET = "hudibucketsrc"
SOURCE_PREFIX = "sensorDataFiles"
ARCHIVE_PREFIX = "sensorDataFilesArchived"
BATCH_SIZE = 500
ARCHIVE_WORKERS = 10
```

#### 3.3 移除死代码

```python
# 移除 category_id 参数及相关逻辑
# 两个分支当前执行相同操作，无意义
```

#### 3.4 使用公开 API

```python
# 之前
processed_files.extend(sensor_df._jdf.inputFiles())

# 之后
input_files = list(sensor_df.inputFiles())
```

#### 3.5 统一日志 + 批次级别指标

```python
# 将所有 print() 替换为 logger.info()
# 每批处理后记录详细指标
logger.info(f"Batch {batch_num} complete: "
            f"files={file_count}, rows={row_count}, "
            f"hudi_duration={hudi_duration}s, "
            f"archive={success}/{total}, "
            f"total_elapsed={elapsed}s")
```

**记录内容**：
- 批次编号
- 处理文件数 / 行数
- Hudi 写入耗时
- 归档成功/失败数
- 总运行时间

#### 3.6 添加空数据检查

```python
if not files:
    logger.info("No files to process")
    return

# 写入 Hudi 前也检查 DataFrame 是否为空
```

#### 3.7 移除无用的 pandas import

```python
# 删除以下两行（pandas 从未使用，Glue 用 Spark DataFrame）
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
```

**说明**: Glue job 运行在分布式 Spark 集群上，所有数据处理都使用 Spark DataFrame。pandas/Polars 是单机库，在此场景下无用且不应引入。

## 实施清单

- [x] **第一阶段：循环批处理**
  - [x] 添加 `list_s3_files()` 函数，一次性列出所有文件
  - [x] 添加 `chunk_list()` 函数，按 BATCH_SIZE 分组
  - [x] 修改 `perform_hudi_upsert()` 接受文件路径列表 → 改为 `process_single_batch()`
  - [x] 改为 `spark.read.load([file1, file2, ...])` 读取指定文件
  - [x] 在 `__main__` 中添加 for 循环逐批处理 → `process_all_files()` 编排器
  - [ ] 小批量测试（10 个文件）

- [x] **第二阶段：性能修复**
  - [x] count/write 前添加 DataFrame 缓存（使用 `.cache()` + `.unpersist()`）
  - [x] 用 ThreadPoolExecutor 实现并行归档（10 workers）
  - [x] 添加归档失败处理（重试 3 次 + 跳过并记录日志）
  - [x] 添加运行时间保护（MAX_RUNTIME_SECONDS = 14400）
  - [ ] 测试归档性能提升

- [x] **第三阶段：代码质量**
  - [x] 提取常量到文件顶部
  - [x] 移除全局 `processed_files` 变量 → 使用纯函数返回值
  - [x] 移除 `category_id` 参数和死代码 → 同时更新 glue.tf
  - [x] 将 `_jdf.inputFiles()` 替换为 `inputFiles()`
  - [x] 将所有 `print()` 替换为 `logger.info()`
  - [x] 添加空数据检查（row_count == 0 时跳过 Hudi 写入）
  - [x] 使用 `urllib.parse.unquote()` 进行 URL 解码
  - [x] 移除无用的 pandas import

- [ ] **测试**
  - [ ] 测试 0 个文件（空目录）
  - [ ] 测试 < 500 个文件（单批次）
  - [ ] 测试 > 500 个文件（多批次）
  - [ ] 测试 2000+ 个文件（压力测试）

- [ ] **部署**
  - [ ] 备份旧脚本: `aws s3 cp s3://.../hudiImportScript s3://.../hudiImportScript.bak`
  - [x] 决定 `--CATEGORY_ID` 参数处理方式 → 已移除
  - [x] 更新 `iac/glue.tf`（已移除 CATEGORY_ID 参数）
  - [ ] 上传新脚本到 S3: `s3://aws-glue-assets-.../scripts/hudiImportScript`
  - [ ] 验证 Glue job 使用新脚本
  - [ ] 监控 CloudWatch 前几次运行
  - [ ] 验证无异常后删除备份（或保留一周）

## 预期改进

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| 100 文件处理时间 | ~10 分钟 | ~5 分钟（无重复计算） |
| 100 文件归档时间 | ~30 秒 | ~5 秒（并行） |
| 2000 文件支持 | 超时/内存溢出 | 正常运行（4 批 × 500） |
| 代码可维护性 | 差 | 良好 |

## 风险与缓解措施

| 风险 | 缓解措施 |
|------|----------|
| 批处理循环永不退出 | 添加 `max_batches` 安全限制（如 100）+ `MAX_RUNTIME_SECONDS` 时间限制 |
| 归档失败导致重复处理 | 文件列表在开始时确定，不受归档失败影响；失败文件重试 3 次后跳过并记录日志 |
| Hudi 写入中途失败 | 文件留在源目录，下次运行重试（Hudi 有原子提交机制） |
| 运行时间过长 | 添加 4 小时运行时间上限，接近时主动退出 |
| 多个 job 同时运行 | 依赖 `max_concurrent_runs = 1` 配置，在代码注释中强调 |
| 新代码出问题需回退 | 部署前备份旧脚本到 S3，回退时直接覆盖 |
| cache() 内存不足 | 使用 `persist(StorageLevel.MEMORY_AND_DISK)` 作为后备 |

## 后续考虑

1. **动态批次大小**: 根据文件大小调整，而非仅按数量
2. **指标监控**: 添加 CloudWatch 指标（批次数、处理时间）
3. **告警**: 批次超过阈值时告警（表示积压）
