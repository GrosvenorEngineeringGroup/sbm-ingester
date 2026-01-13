# SBM Ingester 优化计划 - 简化版

## 📊 评估总结

通过深入分析代码和文档，以下是关键发现：

### ✅ 应该采纳的改进（简化代码，不破坏功能）

1. **Logger替换** - 用Powertools Logger替换自定义CloudWatchLogger
   - 简化58行代码 → 几行装饰器
   - 自动JSON结构化，CloudWatch Insights友好
   - **影响**: 仅日志格式，不影响功能

2. **Metrics简化** - 用Powertools Metrics替换手动metricsDict
   - 简化150行metrics管理代码
   - 自动聚合和发送
   - **影响**: 仅内部实现，不影响功能

3. **Tracer添加** - 添加X-Ray追踪
   - 性能可视化，识别瓶颈
   - **影响**: 纯观测性，零功能变化

4. **文件重命名** - `gemsDataParseAndWrite.py` → `app.py`
   - 符合标准约定
   - **影响**: 仅文件名，Terraform需同步更新

### ❌ 不应该采纳的（过度设计或性能损失）

1. **BatchProcessor** - ❌ 会破坏批量写入优化
   - 当前BATCH_SIZE=50机制每天节省数百美元S3成本
   - BatchProcessor会导致S3调用增加500%+
   - **结论**: 保持当前设计

2. **Lambda Layer** - ❌ 不必要
   - 3个Lambda依赖完全不重叠
   - redrive和nem12_mappings都<50KB
   - **结论**: 保持独立部署

3. **DynamoDB Idempotency** - ⚠️ 可选
   - 代码分析发现理论上的竞态条件（copy后、delete前崩溃）
   - 但生产环境目前稳定
   - **结论**: 可以保持现状，或作为Phase 2添加（额外保护层）

4. **过度细分目录结构** - ❌ 过度设计
   - 当前只有3个module文件
   - 不需要models/services/utils子目录
   - **结论**: 保持简洁结构

---

## 🎯 实施计划

### Phase 1: 核心简化（1天）

#### 1.1 添加Powertools依赖

```bash
cd /Users/zeyu/Desktop/GEG/sbm/sbm-ingester
uv add "aws-lambda-powertools>=3.5.0"
```

**pyproject.toml更新**:
```toml
dependencies = [
    "boto3>=1.42.10",
    "nemreader>=0.9.2",
    "pandas>=2.3.3",
    "aws-lambda-powertools>=3.5.0",  # 新增
    "requests>=2.32.5",
    "pytz>=2025.2",
]
```

#### 1.2 重命名主文件

```bash
# 重命名Lambda handler
mv ingester/src/gemsDataParseAndWrite.py ingester/src/app.py

# 更新GitHub Actions (.github/workflows/main.yml)
# 修改复制路径从gemsDataParseAndWrite.py → app.py
```

**更新Terraform (iac/sbm-ingester.tf)**:
```terraform
resource "aws_lambda_function" "sbm_files_ingester" {
  function_name = "sbm-files-ingester"
  handler       = "app.lambda_handler"  # 从 gemsDataParseAndWrite.lambda_handler 改为 app.lambda_handler
  runtime       = "python3.13"
  # ... 其他配置保持不变
}
```

#### 1.3 替换Logger

**修改 `ingester/src/modules/common.py`**:

```python
# 删除CloudWatchLogger类（第18-58行）
# 保留常量定义

# 仅保留以下内容：
PARSE_ERROR_LOG_GROUP = "sbm-ingester-parse-error-log"
RUNTIME_ERROR_LOG_GROUP = "sbm-ingester-runtime-error-log"
ERROR_LOG_GROUP = "sbm-ingester-error-log"
EXECUTION_LOG_GROUP = "sbm-ingester-execution-log"
METRICS_LOG_GROUP = "sbm-ingester-metrics-log"
BUCKET_NAME = "sbm-file-ingester"
PARSE_ERR_DIR = "newParseErr/"
IRREVFILES_DIR = "newIrrevFiles/"
PROCESSED_DIR = "newP/"
```

**修改 `ingester/src/app.py`** (原gemsDataParseAndWrite.py):

```python
from aws_lambda_powertools import Logger, Tracer, Metrics
from aws_lambda_powertools.metrics import MetricUnit
import modules.common as common

# 创建全局实例
logger = Logger(service="file-processor")
tracer = Tracer(service="file-processor")
metrics = Metrics(namespace="SBM/Ingester")

# 替换所有CloudWatchLogger调用
# 之前:
# execution_log = CloudWatchLogger(common.EXECUTION_LOG_GROUP)
# execution_log.log(f"Script Started Running at: {timestampNow}")

# 之后:
# logger.info("Script started", extra={"timestamp": timestampNow})

# 错误日志:
# error_log.log(f"Error: {e}")
# → logger.error("Error occurred", exc_info=True, extra={"error": str(e)})
```

#### 1.4 替换Metrics

**修改 `ingester/src/app.py` 中的metrics逻辑**:

```python
# 删除以下函数:
# - dailyInitializeMetricsDict() (第149-161行)
# - metricsDictPopulateValues() (第164-190行)

# 在parseAndWriteData()中替换metrics记录:

# 之前:
metricsDict: dict[str, dict[str, int]] = {}
metricsDictPopulateValues(
    metricsDict, metricsFileKey,
    ftpFilesCount, validProcessedFilesCount,
    parseErrFilesCount, irrevFilesCount,
    totalMonitorPointsCount, processedMonitorPointsCount, 0
)
metrics_log.log(json.dumps(metricsDict[metricsFileKey]))

# 之后:
metrics.add_metric(name="ValidProcessedFiles", unit=MetricUnit.Count, value=validProcessedFilesCount)
metrics.add_metric(name="ParseErrorFiles", unit=MetricUnit.Count, value=parseErrFilesCount)
metrics.add_metric(name="IrrelevantFiles", unit=MetricUnit.Count, value=irrevFilesCount)
metrics.add_metric(name="ProcessedMonitorPoints", unit=MetricUnit.Count, value=processedMonitorPointsCount)
metrics.add_metric(name="TotalMonitorPoints", unit=MetricUnit.Count, value=totalMonitorPointsCount)
```

#### 1.5 添加Tracer装饰器

```python
@tracer.capture_lambda_handler
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    # 现有逻辑保持不变
    tbp_files: list[dict[str, str]] = []
    for record in event["Records"]:
        # ...

    if tbp_files:
        parseAndWriteData(tbp_files)

    return {"statusCode": 200, "body": "Successfully processed files."}

@tracer.capture_method
def parseAndWriteData(tbp_files: list[dict[str, str]] | None = None) -> int | None:
    # 现有逻辑保持不变
    pass

@tracer.capture_method
def download_files_to_tmp(file_list: list[dict[str, str]], tmp_files_folder_path: str) -> list[str]:
    # 现有逻辑保持不变
    pass
```

---

### Phase 2: 幂等性和可靠性增强（1天）

#### 2.1 添加Powertools Idempotency

**为什么需要**：
- 虽然生产环境稳定，但代码分析发现理论竞态条件
- Powertools Idempotency比自己实现简单（10行vs 200+行）
- 作为额外保护层，不替代文件移动机制

**创建DynamoDB表 (iac/sbm-ingester.tf)**:

```terraform
# 添加DynamoDB表（如果不存在）
resource "aws_dynamodb_table" "idempotency" {
  name           = "sbm-ingester-idempotency"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "id"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "expiration"
    type = "N"
  }

  ttl {
    attribute_name = "expiration"
    enabled        = true
  }

  global_secondary_index {
    name            = "expiration-index"
    hash_key        = "expiration"
    projection_type = "ALL"
  }

  tags = {
    Name        = "sbm-ingester-idempotency"
    Environment = "production"
  }
}

# 添加Lambda IAM权限
resource "aws_iam_role_policy" "lambda_dynamodb" {
  name = "sbm-ingester-dynamodb-policy"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem"
        ]
        Resource = aws_dynamodb_table.idempotency.arn
      }
    ]
  })
}
```

**修改 `ingester/src/app.py`**:

```python
from aws_lambda_powertools.utilities.idempotency import (
    idempotent,
    DynamoDBPersistenceLayer,
    IdempotencyConfig
)

# 配置幂等性（在全局创建）
persistence_layer = DynamoDBPersistenceLayer(
    table_name="sbm-ingester-idempotency"
)

config = IdempotencyConfig(
    event_key_jmespath='[].{"bucket": bucket, "file_name": file_name}',  # 根据文件列表判断
    expires_after_seconds=86400  # 24小时过期
)

# 为parseAndWriteData添加幂等性保护
@idempotent(persistence_store=persistence_layer, config=config)
@tracer.capture_method
def parseAndWriteData(tbp_files: list[dict[str, str]] | None = None) -> int | None:
    """
    处理文件，带幂等性保护。
    相同的tbp_files列表只会处理一次。
    """
    # 现有逻辑保持不变
    # ...
```

**好处**：
- 如果Lambda重试相同的文件列表，自动返回缓存结果
- DynamoDB自动记录处理状态
- 24小时TTL自动清理旧记录

#### 2.2 配置SQS死信队列

**修改 `iac/sbm-ingester.tf`**:

```terraform
# 创建DLQ
resource "aws_sqs_queue" "sbm_files_ingester_dlq" {
  name                      = "sbm-files-ingester-dlq"
  message_retention_seconds = 1209600  # 14天

  tags = {
    Name        = "sbm-files-ingester-dlq"
    Environment = "production"
  }
}

# 更新主队列，添加redrive policy
resource "aws_sqs_queue" "sbm_files_ingester_queue" {
  name                       = "sbm-files-ingester-queue"
  visibility_timeout_seconds = 300

  # 新增：redrive policy
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.sbm_files_ingester_dlq.arn
    maxReceiveCount     = 3  # 失败3次后发送到DLQ
  })

  tags = {
    Name        = "sbm-files-ingester-queue"
    Environment = "production"
  }
}

# 添加DLQ告警
resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  alarm_name          = "sbm-ingester-dlq-messages"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Average"
  threshold           = 0
  alarm_description   = "Alert when messages appear in DLQ"
  alarm_actions       = [aws_sns_topic.sbm_alerts.arn]  # 假设已有SNS topic

  dimensions = {
    QueueName = aws_sqs_queue.sbm_files_ingester_dlq.name
  }
}
```

**好处**：
- 失败消息自动进入DLQ（不会无限重试）
- CloudWatch告警通知
- 14天保留期，方便调查问题

#### 2.3 简化目录结构（可选）

**只做必要的重组**，不过度细分：

```bash
# 创建新目录结构
mkdir -p src/functions/{file_processor,nem12_exporter,redrive_handler}
mkdir -p src/shared

# 移动文件
mv ingester/src/app.py src/functions/file_processor/app.py
mv ingester/src/modules/* src/shared/
mv nem12_mappings_to_s3/src/nem12_mappings_to_s3.py src/functions/nem12_exporter/app.py
mv redrive/src/redrive.py src/functions/redrive_handler/app.py

# 移动测试
mv ingester/tests tests/unit/
```

**新结构**：
```
sbm-ingester/
├── src/
│   ├── functions/
│   │   ├── file_processor/app.py
│   │   ├── nem12_exporter/app.py
│   │   └── redrive_handler/app.py
│   └── shared/
│       ├── parsers.py (合并nem_adapter + nonNemParserFuncs)
│       └── config.py (常量)
└── tests/
    └── unit/
```

**更新导入路径**：
```python
# 在 src/functions/file_processor/app.py 中
from shared.parsers import output_as_data_frames, nonNemParsersGetDf
from shared.config import BUCKET_NAME, PARSE_ERR_DIR, PROCESSED_DIR
```

**是否必须**：
- ❌ 不是强制的
- ✅ 如果觉得当前结构清晰，可以跳过这一步
- ⚠️ 如果做，需要更新GitHub Actions和Terraform中的路径

---

### Phase 3: 测试和验证（半天）

#### 3.1 更新单元测试

**修改测试文件中的CloudWatchLogger mock**:

```python
# ingester/tests/test_common.py - 删除CloudWatchLogger测试
# ingester/tests/test_integration.py - 更新Logger mock

from aws_lambda_powertools import Logger
from unittest.mock import patch

@patch.object(Logger, 'info')
def test_parseAndWriteData_with_powertools(mock_logger_info, ...):
    # 测试逻辑
    pass
```

#### 3.2 测试幂等性

**添加幂等性测试** (`tests/unit/test_idempotency.py`):

```python
import pytest
from moto import mock_aws
import boto3
from src.functions.file_processor.app import parseAndWriteData

@mock_aws
def test_idempotency_prevents_duplicate_processing():
    # 创建DynamoDB表
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
    dynamodb.create_table(
        TableName='sbm-ingester-idempotency',
        KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST'
    )

    # 创建S3 mock
    s3 = boto3.client('s3', region_name='ap-southeast-2')
    s3.create_bucket(
        Bucket='sbm-file-ingester',
        CreateBucketConfiguration={'LocationConstraint': 'ap-southeast-2'}
    )
    s3.put_object(Bucket='sbm-file-ingester', Key='newTBP/test.csv', Body=b'test data')

    # 第一次处理
    files = [{'bucket': 'sbm-file-ingester', 'file_name': 'newTBP/test.csv'}]
    result1 = parseAndWriteData(files)
    assert result1 == 1

    # 第二次处理（相同文件）
    result2 = parseAndWriteData(files)

    # 应该返回缓存结果，不重复处理
    assert result2 == result1

    # 验证文件只被处理一次
    # （检查hudibucketsrc中的输出文件数量）
```

#### 3.3 本地测试

```bash
# 运行所有测试
uv run pytest

# 运行覆盖率检查
uv run pytest --cov=ingester/src --cov-report=term-missing

# 确保100%覆盖率保持
```

#### 3.4 构建和部署测试

```bash
# 本地构建测试
cd ingester
zip -r ../test_build.zip src/

# 检查zip内容
unzip -l test_build.zip | grep -E "app.py|modules"
```

---

### Phase 4: 部署（GitHub Actions自动）

#### 4.1 先部署基础设施

```bash
# 部署DynamoDB表和SQS DLQ
cd iac
terraform init
terraform plan
terraform apply

# 确认资源创建成功
aws dynamodb describe-table --table-name sbm-ingester-idempotency
aws sqs get-queue-url --queue-name sbm-files-ingester-dlq
```

#### 4.2 更新GitHub Actions

**修改 `.github/workflows/main.yml`**:

```yaml
# 更新ingester构建步骤
- name: Build ingester
  run: |
    cd ingester
    # 复制源代码（新文件名）
    cp -r src/* ../build/ingester/
    # app.py已经在src/中了

# 确保依赖包含powertools
- name: Install dependencies
  run: |
    uv export --no-dev --no-hashes -o requirements.txt
    pip install -r requirements.txt -t build/ingester/
```

#### 4.3 推送到main分支触发部署

```bash
git add .
git commit -m "feat: migrate to AWS Lambda Powertools with idempotency

- Replace custom CloudWatchLogger with Powertools Logger
- Replace manual metricsDict with Powertools Metrics
- Add X-Ray Tracer for performance visibility
- Add Powertools Idempotency with DynamoDB
- Configure SQS DLQ with redrive policy
- Add CloudWatch alarm for DLQ messages
- Rename gemsDataParseAndWrite.py to app.py for standard convention
- Keep existing batch processing mechanism (BATCH_SIZE=50)
- Enhance file movement with idempotency protection"

git push origin main
```

---

## 📋 关键文件清单

### 需要修改的文件

| 文件 | 修改内容 | 代码行数变化 |
|-----|---------|------------|
| `pyproject.toml` | 添加powertools依赖 | +1行 |
| `ingester/src/gemsDataParseAndWrite.py` | 重命名为app.py | 文件重命名 |
| `ingester/src/app.py` | 替换Logger/Metrics, 添加Tracer | -200行, +30行 |
| `ingester/src/modules/common.py` | 删除CloudWatchLogger类 | -40行 |
| `iac/sbm-ingester.tf` | 添加DynamoDB表、SQS DLQ、IAM权限、CloudWatch告警 | +80行 |
| `.github/workflows/main.yml` | 更新文件路径 | ~2行 |
| `ingester/tests/test_common.py` | 删除CloudWatchLogger测试 | -30行 |
| `ingester/tests/test_integration.py` | 更新Logger mock | ~10行 |
| `tests/unit/test_idempotency.py` | 新增幂等性测试 | +50行 |

**总计**: 删除约270行代码，添加约160行，净减少110行 ✅

---

## 🔍 验证清单

### 功能验证

- [ ] Logger输出为JSON格式（CloudWatch Logs中查看）
- [ ] Metrics自动发送到CloudWatch（Metrics控制台查看`SBM/Ingester`命名空间）
- [ ] X-Ray Traces可见（X-Ray控制台查看service map）
- [ ] 幂等性工作正常（DynamoDB表中有记录）
- [ ] 重复文件不会被重新处理（检查DynamoDB缓存命中）
- [ ] SQS DLQ配置正确（maxReceiveCount=3）
- [ ] DLQ告警正常工作（手动发送消息到DLQ测试）
- [ ] 文件处理逻辑保持不变（newTBP → newP/newIrrevFiles/newParseErr）
- [ ] 批量写入机制保持不变（确认hudibucketsrc中有batch_*.csv文件）
- [ ] 所有测试通过（115 + 幂等性测试）
- [ ] 测试覆盖率保持100%

### 性能验证

- [ ] Lambda执行时间无显著增加（<5%差异）
- [ ] S3写入次数保持不变（仍然是批量，不是逐个）
- [ ] 内存使用无显著增加
- [ ] 冷启动时间<200ms（Powertools有轻微开销）

### 部署验证

- [ ] GitHub Actions构建成功
- [ ] Lambda函数更新成功（3个函数都部署）
- [ ] 没有import错误（powertools依赖正确安装）
- [ ] CloudWatch Logs显示新格式日志

---

## 🚫 明确不做的事

1. **不使用BatchProcessor**
   - 原因：会破坏BATCH_SIZE=50的S3写入优化
   - 成本影响：每天增加数百美元
   - 性能影响：Lambda执行时间增加

2. **不创建Lambda Layer**
   - 原因：3个Lambda依赖不重叠
   - redrive和nem12_mappings都<50KB，无需优化

3. **不过度重组目录结构**
   - 原因：当前结构已经清晰
   - 不需要models/services/utils子目录
   - 可选：轻量级重组到src/functions和src/shared

4. **不使用SSM Parameter Store**
   - 原因：增加冷启动延迟和成本
   - 环境变量足够

---

## 💰 预期收益

| 方面 | 改进 |
|-----|------|
| **代码量** | 净减少110行（删除270行，添加160行） |
| **可维护性** | CloudWatchLogger从58行→装饰器 |
| **可靠性** | 幂等性保护 + SQS DLQ |
| **可观测性** | 自动JSON日志 + X-Ray追踪 |
| **CloudWatch Insights** | 支持结构化查询 |
| **错误处理** | 自动DLQ + 告警 |
| **开发体验** | 标准化Logger API |
| **性能** | 零影响（保持批量优化） |
| **成本** | DynamoDB按请求计费（预计<$1/月） |
| **部署** | 需要先部署基础设施（Terraform） |

---

## ⏱️ 时间估算

| 阶段 | 时间 |
|-----|------|
| Phase 1: 核心简化 | 4小时 |
| Phase 2: 幂等性和可靠性 | 4小时 |
| Phase 3: 测试验证 | 2小时 |
| Phase 4: 部署和监控 | 1小时 |
| **总计** | **11小时（约1.5天）** |

---

## 🎯 成功标准

1. ✅ 所有测试通过（115 + 幂等性测试）
2. ✅ 测试覆盖率保持100%
3. ✅ CloudWatch Logs显示JSON格式
4. ✅ CloudWatch Metrics自动发送
5. ✅ X-Ray Traces可见
6. ✅ DynamoDB幂等性表正常工作
7. ✅ SQS DLQ配置正确，告警正常
8. ✅ 批量写入机制保持不变（S3调用次数无增加）
9. ✅ Lambda执行时间无显著变化
10. ✅ 重复文件被正确拦截（不重复处理）
11. ✅ 生产环境稳定运行1周无问题

---

## 📚 参考资源

- [Powertools Logger文档](https://docs.powertools.aws.dev/lambda/python/latest/core/logger/)
- [Powertools Metrics文档](https://docs.powertools.aws.dev/lambda/python/latest/core/metrics/)
- [Powertools Tracer文档](https://docs.powertools.aws.dev/lambda/python/latest/core/tracer/)
- 项目现有文档: `docs/powertools_migration.md`（参考但不完全采纳）
- 项目现有文档: `docs/structure_review.md`（参考但不完全采纳）

---

## 🎯 Phase 2的可选项决策

### 2.3 简化目录结构 - 可以跳过

这一步是**完全可选的**：

**跳过的理由**：
- 当前结构已经清晰（`ingester/`, `redrive/`, `nem12_mappings_to_s3/`）
- 只有3个Lambda，不需要过度抽象
- 避免不必要的文件移动和路径更新

**如果做的理由**：
- 统一风格，更符合"标准"Serverless项目结构
- 方便未来添加更多Lambda
- shared/目录更明确

**建议**：先完成Phase 1和Phase 2的其他部分，运行稳定后再考虑是否重组。如果当前结构工作良好，**不重组也完全可以**。
