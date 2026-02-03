# SBM 文件摄取管道 - 架构设计

## 目录

- [概述](#概述)
- [设计目标](#设计目标)
- [架构总览](#架构总览)
- [分层架构详解](#分层架构详解)
  - [Layer 1: 文件接收层](#layer-1-文件接收层-ingestion)
  - [Layer 2: 稳定性验证层](#layer-2-稳定性验证层-stabilization)
  - [Layer 3: 路由层](#layer-3-路由层-routing)
  - [Layer 4: 处理层](#layer-4-处理层-processing)
  - [Layer 5: 数据存储层](#layer-5-数据存储层-storage)
  - [Layer 6: 错误处理层](#layer-6-错误处理层-error-handling)
  - [Layer 7: 可观测性层](#layer-7-可观测性层-observability)
- [核心组件设计](#核心组件设计)
  - [FileStabilizer 状态机](#filestabilizer-状态机)
  - [文件类型检测器](#文件类型检测器)
  - [大文件切分器](#大文件切分器)
  - [处理器状态机](#处理器状态机)
- [S3 存储结构](#s3-存储结构)
- [EventBridge 规则配置](#eventbridge-规则配置)
- [DynamoDB 状态追踪表](#dynamodb-状态追踪表)
- [错误处理策略](#错误处理策略)
- [扩展新文件类型](#扩展新文件类型)
- [设计决策说明](#设计决策说明)

---

## 概述

SBM 文件摄取管道是一个事件驱动的无服务器数据处理系统，负责接收、验证、解析和转换来自多个外部数据源的能源数据文件。

### 当前支持的文件类型

| 文件类型 | 格式 | 数据内容 |
|---------|------|---------|
| NEM12 | CSV | 电表 30 分钟间隔数据 |
| NEM13 | CSV | 电表累积读数 |
| Optima Interval | CSV | BidEnergy 间隔用电数据 |
| Usage and Spend | CSV (UTF-16) | BidEnergy 月度账单汇总 |
| Envizi | CSV | 水/电数据 |
| ComX | CSV | Green Square 私网电表数据 |
| PDF | Binary | 账单、报告等文档 |

---

## 设计目标

1. **可靠性**：确保只处理完整上传的文件，零数据丢失
2. **可扩展性**：新增文件类型只需添加配置，无需修改核心逻辑
3. **大文件支持**：自动切分超大文件，递归处理
4. **可观测性**：端到端追踪，完整的审计日志
5. **容错性**：自动重试瞬态错误，优雅处理永久错误
6. **成本效益**：利用 Step Functions Wait 状态替代 Lambda 轮询

---

## 架构总览

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                            │
│                              SBM File Ingestion Pipeline                                   │
│                                                                                            │
│  ════════════════════════════════════════════════════════════════════════════════════════  │
│                                                                                            │
│                                   LAYER 1: INGESTION                                       │
│                                                                                            │
│     External Sources                                                                       │
│     ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                                       │
│     │ Optima  │ │Retailers│ │SkySpark │ │ Manual  │                                       │
│     └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘                                       │
│          │           │           │           │                                             │
│          └───────────┴───────────┴───────────┘                                             │
│                          │                                                                 │
│                          ▼                                                                 │
│                  ┌───────────────┐                                                         │
│                  │  S3: landing/ │  ◄── 所有文件首先落地于此                               │
│                  └───────┬───────┘                                                         │
│                          │                                                                 │
│                          │ S3 Event → EventBridge                                          │
│                          ▼                                                                 │
│  ════════════════════════════════════════════════════════════════════════════════════════  │
│                                                                                            │
│                              LAYER 2: STABILIZATION                                        │
│                                                                                            │
│                  ┌─────────────────────────────────────────────┐                           │
│                  │     Step Function: FileStabilizer           │                           │
│                  │                                             │                           │
│                  │  ┌─────────┐                                │                           │
│                  │  │ Record  │ 记录初始 size, etag, timestamp │                           │
│                  │  │ Initial │                                │                           │
│                  │  └────┬────┘                                │                           │
│                  │       │                                     │                           │
│                  │       ▼                                     │                           │
│                  │  ┌─────────┐                                │                           │
│                  │  │  Wait   │ 30 秒（Step Function 免费）    │                           │
│                  │  │ 30 sec  │                                │                           │
│                  │  └────┬────┘                                │                           │
│                  │       │                                     │                           │
│                  │       ▼                                     │                           │
│                  │  ┌─────────┐     ┌──────────────────────┐   │                           │
│                  │  │ Verify  │────►│ size/etag 变化?      │   │                           │
│                  │  │ Stable  │     │                      │   │                           │
│                  │  └─────────┘     │  Yes ──► 重试 (max 5)│   │                           │
│                  │                  │  No  ──► 继续        │   │                           │
│                  │                  └──────────────────────┘   │                           │
│                  │       │                                     │                           │
│                  │       ▼                                     │                           │
│                  │  ┌─────────┐                                │                           │
│                  │  │Validate │ 读取文件头，验证格式           │                           │
│                  │  │ Header  │ 检测文件类型                   │                           │
│                  │  └────┬────┘                                │                           │
│                  │       │                                     │                           │
│                  │       ▼                                     │                           │
│                  │  ┌─────────┐                                │                           │
│                  │  │ Publish │ 发布 FileReady 事件            │                           │
│                  │  │ Event   │ 包含: bucket, key, fileType,   │                           │
│                  │  └─────────┘       size, checksum           │                           │
│                  │                                             │                           │
│                  └─────────────────────────────────────────────┘                           │
│                          │                                                                 │
│                          │ FileReady Event                                                 │
│                          ▼                                                                 │
│  ════════════════════════════════════════════════════════════════════════════════════════  │
│                                                                                            │
│                                LAYER 3: ROUTING                                            │
│                                                                                            │
│                  ┌─────────────────────────────────────────────┐                           │
│                  │        EventBridge: file-processing         │                           │
│                  │                                             │                           │
│                  │  ┌─────────────────────────────────────────┐│                           │
│                  │  │              Routing Rules              ││                           │
│                  │  │                                         ││                           │
│                  │  │  fileType = "nem12" | "nem13"           ││                           │
│                  │  │      ──► StepFunction: NEMProcessor     ││                           │
│                  │  │                                         ││                           │
│                  │  │  fileType = "optima-interval"           ││                           │
│                  │  │      ──► StepFunction: OptimaProcessor  ││                           │
│                  │  │                                         ││                           │
│                  │  │  fileType = "bill" | "usage-spend"      ││                           │
│                  │  │      ──► StepFunction: BillProcessor    ││                           │
│                  │  │                                         ││                           │
│                  │  │  fileType = "pdf"                       ││                           │
│                  │  │      ──► StepFunction: PDFProcessor     ││                           │
│                  │  │                                         ││                           │
│                  │  │  fileType = "unknown"                   ││                           │
│                  │  │      ──► SQS: manual-review-queue       ││                           │
│                  │  │                                         ││                           │
│                  │  │  * (all events)                         ││                           │
│                  │  │      ──► CloudWatch Logs (audit)        ││                           │
│                  │  │                                         ││                           │
│                  │  └─────────────────────────────────────────┘│                           │
│                  │                                             │                           │
│                  └─────────────────────────────────────────────┘                           │
│                          │                                                                 │
│                          ▼                                                                 │
│  ════════════════════════════════════════════════════════════════════════════════════════  │
│                                                                                            │
│                              LAYER 4: PROCESSING                                           │
│                                                                                            │
│  ┌──────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                                                                      │  │
│  │  ┌────────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                    StepFunction: NEMProcessor                                  │  │  │
│  │  │                                                                                │  │  │
│  │  │   ┌──────────┐    ┌──────────┐    ┌──────────┐                                │  │  │
│  │  │   │  Parse   │───►│  Check   │───►│ >50MB?   │                                │  │  │
│  │  │   │  Header  │    │  Size    │    │          │                                │  │  │
│  │  │   └──────────┘    └──────────┘    └────┬─────┘                                │  │  │
│  │  │                                        │                                       │  │  │
│  │  │                         ┌──────────────┴──────────────┐                        │  │  │
│  │  │                         │                             │                        │  │  │
│  │  │                        Yes                           No                        │  │  │
│  │  │                         │                             │                        │  │  │
│  │  │                         ▼                             ▼                        │  │  │
│  │  │                   ┌──────────┐                  ┌──────────┐                   │  │  │
│  │  │                   │  Split   │                  │  Map     │                   │  │  │
│  │  │                   │  File    │                  │ Parallel │                   │  │  │
│  │  │                   └────┬─────┘                  │ by NMI   │                   │  │  │
│  │  │                        │                        └────┬─────┘                   │  │  │
│  │  │                        │                             │                         │  │  │
│  │  │                        ▼                             │                         │  │  │
│  │  │                   ┌──────────┐                       │                         │  │  │
│  │  │                   │  Write   │                       │                         │  │  │
│  │  │                   │ Chunks   │──► S3: landing/       │                         │  │  │
│  │  │                   │ back     │    (递归触发)         │                         │  │  │
│  │  │                   └──────────┘                       │                         │  │  │
│  │  │                                                      │                         │  │  │
│  │  │                         ┌────────────────────────────┘                         │  │  │
│  │  │                         │                                                      │  │  │
│  │  │                         ▼                                                      │  │  │
│  │  │   ┌──────────────────────────────────────────────────────────────────────┐    │  │  │
│  │  │   │                  Distributed Map (per NMI)                           │    │  │  │
│  │  │   │                                                                      │    │  │  │
│  │  │   │   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐         │    │  │  │
│  │  │   │   │  Parse   │──►│   Map    │──►│Transform │──►│  Write   │         │    │  │  │
│  │  │   │   │  NMI     │   │ Neptune  │   │  to CSV  │   │  Batch   │         │    │  │  │
│  │  │   │   │  Data    │   │   ID     │   │          │   │          │         │    │  │  │
│  │  │   │   └──────────┘   └──────────┘   └──────────┘   └──────────┘         │    │  │  │
│  │  │   │                                                                      │    │  │  │
│  │  │   └──────────────────────────────────────────────────────────────────────┘    │  │  │
│  │  │                         │                                                      │  │  │
│  │  │                         ▼                                                      │  │  │
│  │  │                   ┌──────────┐                                                 │  │  │
│  │  │                   │ Archive  │──► S3: processed/                               │  │  │
│  │  │                   │ Original │                                                 │  │  │
│  │  │                   └──────────┘                                                 │  │  │
│  │  │                                                                                │  │  │
│  │  └────────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                                      │  │
│  │  ┌────────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                    StepFunction: OptimaProcessor                               │  │  │
│  │  │                                                                                │  │  │
│  │  │   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐    │  │  │
│  │  │   │  Parse   │──►│ Validate │──►│   Map    │──►│Transform │──►│  Write   │    │  │  │
│  │  │   │  CSV     │   │  Schema  │   │ Neptune  │   │  to CSV  │   │  Batch   │    │  │  │
│  │  │   └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘    │  │  │
│  │  │                                                                                │  │  │
│  │  └────────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                                      │  │
│  │  ┌────────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                    StepFunction: BillProcessor                                 │  │  │
│  │  │                                                                                │  │  │
│  │  │   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐    │  │  │
│  │  │   │ Detect   │──►│ Convert  │──►│  Parse   │──►│ Validate │──►│  Upload  │    │  │  │
│  │  │   │ Encoding │   │ to UTF-8 │   │  Fields  │   │  Data    │   │ to S3    │    │  │  │
│  │  │   │(UTF-16?) │   │          │   │          │   │          │   │          │    │  │  │
│  │  │   └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘    │  │  │
│  │  │                                                                                │  │  │
│  │  └────────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                                      │  │
│  │  ┌────────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                    StepFunction: PDFProcessor                                  │  │  │
│  │  │                                                                                │  │  │
│  │  │   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐                   │  │  │
│  │  │   │ Extract  │──►│   OCR    │──►│ Classify │──►│  Route   │──► 其他处理器    │  │  │
│  │  │   │  Text    │   │(Textract)│   │  Type    │   │          │                   │  │  │
│  │  │   └──────────┘   └──────────┘   └──────────┘   └──────────┘                   │  │  │
│  │  │                                                                                │  │  │
│  │  └────────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                                      │  │
│  └──────────────────────────────────────────────────────────────────────────────────────┘  │
│                          │                                                                 │
│                          ▼                                                                 │
│  ════════════════════════════════════════════════════════════════════════════════════════  │
│                                                                                            │
│                               LAYER 5: DATA STORAGE                                        │
│                                                                                            │
│     ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                       │
│     │  S3: data-lake/ │   │     Neptune     │   │    DynamoDB     │                       │
│     │  (Hudi format)  │   │   (NMI → ID)    │   │   (State +      │                       │
│     │                 │   │                 │   │   Idempotency)  │                       │
│     └────────┬────────┘   └─────────────────┘   └─────────────────┘                       │
│              │                                                                             │
│              ▼                                                                             │
│     ┌─────────────────┐                                                                   │
│     │   Glue ETL      │  定时或文件数触发                                                 │
│     │  (Hudi Upsert)  │                                                                   │
│     └─────────────────┘                                                                   │
│                                                                                            │
│  ════════════════════════════════════════════════════════════════════════════════════════  │
│                                                                                            │
│                             LAYER 6: ERROR HANDLING                                        │
│                                                                                            │
│     ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│     │                                                                                 │   │
│     │   Step Function 失败                                                            │   │
│     │         │                                                                       │   │
│     │         ▼                                                                       │   │
│     │   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐        │   │
│     │   │ Move file   │   │  Send to    │   │  Publish    │   │  SNS Alert  │        │   │
│     │   │ to error/   │   │  SQS DLQ    │   │ ErrorEvent  │   │  (PagerDuty)│        │   │
│     │   │             │   │             │   │             │   │             │        │   │
│     │   └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘        │   │
│     │                                                                                 │   │
│     │   Error Categories:                                                             │   │
│     │   ├── ValidationError  ──► S3: error/invalid/    (永久失败，人工检查)           │   │
│     │   ├── MappingError     ──► S3: error/unmapped/   (无 Neptune 映射)              │   │
│     │   ├── ParseError       ──► S3: error/parse-err/  (格式错误)                     │   │
│     │   └── TransientError   ──► 自动重试 (Step Functions 内置)                       │   │
│     │                                                                                 │   │
│     └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                            │
│  ════════════════════════════════════════════════════════════════════════════════════════  │
│                                                                                            │
│                             LAYER 7: OBSERVABILITY                                         │
│                                                                                            │
│     ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐                       │
│     │   X-Ray         │   │  CloudWatch     │   │  CloudWatch     │                       │
│     │  (Distributed   │   │  Logs Insights  │   │  Dashboard      │                       │
│     │   Tracing)      │   │                 │   │                 │                       │
│     └─────────────────┘   └─────────────────┘   └─────────────────┘                       │
│                                                                                            │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 分层架构详解

### Layer 1: 文件接收层 (Ingestion)

**职责**：接收来自所有外部数据源的文件

**组件**：
- S3 Bucket: `sbm-file-ingester/landing/`
- S3 Event Notification → EventBridge

**设计要点**：
- 所有文件统一落地到 `landing/` 目录
- 使用 EventBridge 而非直接 S3 → SQS，获取更丰富的事件元数据
- 支持任意上传方式：PUT、Multipart Upload、Streaming

### Layer 2: 稳定性验证层 (Stabilization)

**职责**：确保文件完全上传后才进行处理

**组件**：
- Step Function: `FileStabilizer`
- Lambda: `file-stabilizer-record`
- Lambda: `file-stabilizer-verify`
- Lambda: `file-type-detector`

**设计要点**：
- 使用 Step Functions Wait 状态替代 Lambda `time.sleep()`，零成本等待
- 记录初始 size/etag，等待后比较确认稳定
- 最多重试 5 次，每次等待 30 秒
- 验证失败的文件移动到 `error/quarantine/`

### Layer 3: 路由层 (Routing)

**职责**：根据文件类型将文件路由到对应的处理器

**组件**：
- EventBridge Custom Bus: `file-processing`
- EventBridge Rules: 按 `fileType` 路由

**设计要点**：
- 使用 EventBridge 内容过滤实现路由
- 每种文件类型对应一条规则
- 所有事件同时发送到 CloudWatch Logs 进行审计
- 未知文件类型发送到人工审核队列

### Layer 4: 处理层 (Processing)

**职责**：解析、转换、写入数据

**组件**：
- Step Function: `NEMProcessor`
- Step Function: `OptimaProcessor`
- Step Function: `BillProcessor`
- Step Function: `PDFProcessor`
- 各类 Lambda 函数

**设计要点**：
- 每种文件类型一个独立的 Step Function
- 大文件自动切分，切分后的文件写回 `landing/` 递归处理
- 使用 Distributed Map 并行处理多个 NMI
- 处理完成后原文件归档到 `processed/`

### Layer 5: 数据存储层 (Storage)

**职责**：持久化处理结果

**组件**：
- S3: `hudibucketsrc/sensorDataFiles/` (CSV 输出)
- Neptune: NMI → Sensor ID 映射
- DynamoDB: 处理状态追踪
- Glue ETL: Hudi 数据湖 Upsert

### Layer 6: 错误处理层 (Error Handling)

**职责**：优雅处理各类错误

**错误分类**：

| 错误类型 | 处理方式 | 目标目录 |
|---------|---------|---------|
| TransientError | Step Functions 自动重试 | - |
| ParseError | 移动文件 + 告警 | `error/parse-err/` |
| ValidationError | 移动文件 + 告警 | `error/invalid/` |
| MappingError | 移动文件 | `error/unmapped/` |
| QuarantineError | 移动文件 + 告警 | `error/quarantine/` |

### Layer 7: 可观测性层 (Observability)

**组件**：
- X-Ray: 分布式追踪
- CloudWatch Logs: 结构化日志
- CloudWatch Metrics: 业务指标
- CloudWatch Dashboard: 可视化面板
- SNS: 告警通知

**关键指标**：
- FilesProcessed (按类型)
- ProcessingLatency (p50, p95, p99)
- ErrorRate (按类型、按错误分类)
- SplitOperations (切分次数、平均 chunk 数)
- NeptuneMappingHitRate

---

## 核心组件设计

### FileStabilizer 状态机

```yaml
Name: FileStabilizer
Type: STANDARD

States:
  RecordInitialState:
    Type: Task
    Resource: Lambda:file-stabilizer-record
    Comment: 记录初始 size, etag, lastModified
    Next: WaitForStability

  WaitForStability:
    Type: Wait
    Seconds: 30
    Comment: Step Functions Wait 状态免费
    Next: VerifyStable

  VerifyStable:
    Type: Task
    Resource: Lambda:file-stabilizer-verify
    Comment: 比较当前 size/etag 与初始值
    Next: IsStable

  IsStable:
    Type: Choice
    Choices:
      - Variable: $.isStable
        BooleanEquals: true
        Next: DetectFileType
      - Variable: $.retryCount
        NumericGreaterThanEquals: 5
        Next: Quarantine
    Default: IncrementRetry

  IncrementRetry:
    Type: Pass
    Parameters:
      retryCount.$: States.MathAdd($.retryCount, 1)
    Next: WaitForStability

  DetectFileType:
    Type: Task
    Resource: Lambda:file-type-detector
    Comment: 读取文件头，检测类型和编码
    Next: PublishFileReady

  PublishFileReady:
    Type: Task
    Resource: arn:aws:states:::events:putEvents
    Parameters:
      Entries:
        - Source: sbm.file-ingester
          DetailType: FileReadyForProcessing
          EventBusName: file-processing
          Detail:
            bucket.$: $.bucket
            key.$: $.key
            fileType.$: $.fileType
            size.$: $.size
            checksum.$: $.checksum
            encoding.$: $.encoding
    End: true

  Quarantine:
    Type: Task
    Resource: Lambda:file-stabilizer-quarantine
    Comment: 移动到 quarantine/，发送告警
    Next: Fail

  Fail:
    Type: Fail
    Error: FileStabilizationFailed
```

### 文件类型检测器

```python
# Lambda: file-type-detector

from enum import Enum
from dataclasses import dataclass
from typing import Protocol

class FileType(Enum):
    NEM12 = "nem12"
    NEM13 = "nem13"
    OPTIMA_INTERVAL = "optima-interval"
    OPTIMA_EXPORT = "optima-export"
    USAGE_SPEND = "usage-spend"
    BILL = "bill"
    PDF = "pdf"
    ENVIZI = "envizi"
    COMX = "comx"
    UNKNOWN = "unknown"


@dataclass
class FileDetectionResult:
    file_type: FileType
    encoding: str  # utf-8, utf-16-le, etc.
    confidence: float
    metadata: dict


class FileDetector(Protocol):
    """文件检测器协议"""
    @staticmethod
    def detect(key: str, head: bytes) -> FileDetectionResult | None:
        ...


class FileTypeDetector:
    """
    文件类型检测器。
    使用策略链模式，每个检测器独立可扩展。
    """

    detectors: list[FileDetector] = []

    @classmethod
    def register(cls, detector: type[FileDetector]) -> type[FileDetector]:
        """装饰器：注册检测器"""
        cls.detectors.append(detector)
        return detector

    @classmethod
    def detect(cls, bucket: str, key: str, s3_client) -> FileDetectionResult:
        """检测文件类型"""
        # 读取文件头（前 8KB）
        response = s3_client.get_object(
            Bucket=bucket, Key=key, Range="bytes=0-8191"
        )
        head_bytes = response["Body"].read()

        # 遍历检测器，返回第一个高置信度结果
        for detector in cls.detectors:
            result = detector.detect(key, head_bytes)
            if result and result.confidence > 0.8:
                return result

        return FileDetectionResult(
            file_type=FileType.UNKNOWN,
            encoding="utf-8",
            confidence=0.0,
            metadata={}
        )


# 检测器实现示例

@FileTypeDetector.register
class NEM12Detector:
    """NEM12/NEM13 文件检测器"""

    @staticmethod
    def detect(key: str, head: bytes) -> FileDetectionResult | None:
        for encoding in ["utf-8", "utf-16-le", "latin-1"]:
            try:
                text = head.decode(encoding)
                first_line = text.split("\n")[0]

                if first_line.startswith("100,NEM12"):
                    return FileDetectionResult(
                        file_type=FileType.NEM12,
                        encoding=encoding,
                        confidence=1.0,
                        metadata={"version": "NEM12"}
                    )
                if first_line.startswith("100,NEM13"):
                    return FileDetectionResult(
                        file_type=FileType.NEM13,
                        encoding=encoding,
                        confidence=1.0,
                        metadata={"version": "NEM13"}
                    )
            except UnicodeDecodeError:
                continue
        return None


@FileTypeDetector.register
class UsageSpendDetector:
    """Usage and Spend 报告检测器"""

    @staticmethod
    def detect(key: str, head: bytes) -> FileDetectionResult | None:
        key_lower = key.lower()

        if "usage and spend" in key_lower or "usageandspend" in key_lower:
            # 检测编码
            if head[:2] == b'\xff\xfe':  # UTF-16 LE BOM
                encoding = "utf-16-le"
            else:
                encoding = "utf-8"

            return FileDetectionResult(
                file_type=FileType.USAGE_SPEND,
                encoding=encoding,
                confidence=0.95,
                metadata={}
            )
        return None


@FileTypeDetector.register
class PDFDetector:
    """PDF 文件检测器"""

    @staticmethod
    def detect(key: str, head: bytes) -> FileDetectionResult | None:
        if head[:4] == b'%PDF':
            return FileDetectionResult(
                file_type=FileType.PDF,
                encoding="binary",
                confidence=1.0,
                metadata={"version": head[5:8].decode("ascii", errors="ignore")}
            )
        return None


@FileTypeDetector.register
class OptimaIntervalDetector:
    """Optima 间隔数据检测器"""

    @staticmethod
    def detect(key: str, head: bytes) -> FileDetectionResult | None:
        key_lower = key.lower()

        # 检查文件名模式
        if "interval" in key_lower or "export" in key_lower:
            try:
                text = head.decode("utf-8")
                # 检查 CSV 头部是否包含 Optima 特征列
                if "Identifier" in text and "Start Time" in text:
                    return FileDetectionResult(
                        file_type=FileType.OPTIMA_INTERVAL,
                        encoding="utf-8",
                        confidence=0.9,
                        metadata={}
                    )
            except UnicodeDecodeError:
                pass
        return None


@FileTypeDetector.register
class EnviziDetector:
    """Envizi 数据检测器"""

    @staticmethod
    def detect(key: str, head: bytes) -> FileDetectionResult | None:
        try:
            text = head.decode("utf-8")
            # 检查 Envizi 特征列
            if "Serial_No" in text and ("Interval_Start" in text or "Date_Time" in text):
                return FileDetectionResult(
                    file_type=FileType.ENVIZI,
                    encoding="utf-8",
                    confidence=0.9,
                    metadata={}
                )
        except UnicodeDecodeError:
            pass
        return None


@FileTypeDetector.register
class ComXDetector:
    """ComX 数据检测器"""

    @staticmethod
    def detect(key: str, head: bytes) -> FileDetectionResult | None:
        try:
            text = head.decode("utf-8")
            if "ComX510_Green_Square" in text:
                return FileDetectionResult(
                    file_type=FileType.COMX,
                    encoding="utf-8",
                    confidence=1.0,
                    metadata={}
                )
        except UnicodeDecodeError:
            pass
        return None
```

### 大文件切分器

```python
# Lambda: nem-splitter

from dataclasses import dataclass
import boto3

s3 = boto3.client("s3")


@dataclass
class ChunkInfo:
    """切分块信息"""
    chunk_number: int
    key: str
    nmis: list[str]
    size_bytes: int


class NEMFileSplitter:
    """
    NEM12/NEM13 大文件切分器。
    按 NMI block 切分，保证每个 chunk 是完整的 NEM 文件。
    """

    MAX_CHUNK_SIZE = 10 * 1024 * 1024  # 10MB per chunk
    MAX_NMIS_PER_CHUNK = 50

    def split(self, bucket: str, key: str) -> list[ChunkInfo]:
        """切分大文件，返回 chunk 信息列表"""
        # 解析文件结构
        header_lines, nmi_blocks = self._parse_structure(bucket, key)

        chunks = []
        current_chunk_nmis = []
        current_chunk_size = len("\n".join(header_lines).encode())

        for nmi, block_lines in nmi_blocks.items():
            block_size = len("\n".join(block_lines).encode())

            # 检查是否需要新 chunk
            should_split = (
                current_chunk_size + block_size > self.MAX_CHUNK_SIZE
                or len(current_chunk_nmis) >= self.MAX_NMIS_PER_CHUNK
            )

            if should_split and current_chunk_nmis:
                # 保存当前 chunk
                chunk = self._create_chunk(
                    bucket, key, len(chunks),
                    header_lines, current_chunk_nmis, nmi_blocks
                )
                chunks.append(chunk)
                current_chunk_nmis = []
                current_chunk_size = len("\n".join(header_lines).encode())

            current_chunk_nmis.append(nmi)
            current_chunk_size += block_size

        # 保存最后一个 chunk
        if current_chunk_nmis:
            chunk = self._create_chunk(
                bucket, key, len(chunks),
                header_lines, current_chunk_nmis, nmi_blocks
            )
            chunks.append(chunk)

        # 归档原文件
        self._archive_original(bucket, key)

        return chunks

    def _parse_structure(self, bucket: str, key: str) -> tuple[list, dict]:
        """解析 NEM 文件结构"""
        header_lines = []
        nmi_blocks = {}  # {nmi: [lines]}
        current_nmi = None

        response = s3.get_object(Bucket=bucket, Key=key)
        for line in response["Body"].iter_lines():
            line = line.decode("utf-8").strip()

            if line.startswith("100,"):  # Header record
                header_lines.append(line)
            elif line.startswith("200,"):  # NMI data details
                current_nmi = line.split(",")[1]
                nmi_blocks[current_nmi] = [line]
            elif line.startswith("900"):  # End record
                pass  # 跳过，后续手动添加
            elif current_nmi:
                nmi_blocks[current_nmi].append(line)

        return header_lines, nmi_blocks

    def _create_chunk(
        self, bucket: str, original_key: str, chunk_num: int,
        header_lines: list, nmis: list, nmi_blocks: dict
    ) -> ChunkInfo:
        """创建并上传一个 chunk"""
        # 组装 chunk 内容
        lines = header_lines.copy()
        for nmi in nmis:
            lines.extend(nmi_blocks[nmi])
        lines.append("900")  # End record

        content = "\n".join(lines)

        # 生成 chunk key
        base_name = original_key.rsplit("/", 1)[-1]
        name_parts = base_name.rsplit(".", 1)
        name = name_parts[0]
        ext = name_parts[1] if len(name_parts) > 1 else "csv"
        chunk_key = f"landing/{name}_chunk{chunk_num:04d}.{ext}"

        # 上传到 landing/（触发新的处理流程）
        s3.put_object(
            Bucket=bucket,
            Key=chunk_key,
            Body=content.encode("utf-8"),
            Metadata={
                "parent-file": original_key,
                "chunk-number": str(chunk_num),
                "nmis": ",".join(nmis)
            }
        )

        return ChunkInfo(
            chunk_number=chunk_num,
            key=chunk_key,
            nmis=nmis,
            size_bytes=len(content.encode())
        )

    def _archive_original(self, bucket: str, key: str) -> None:
        """归档原始大文件"""
        archive_key = key.replace("landing/", "processed/original/", 1)
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": key},
            Key=archive_key
        )
        s3.delete_object(Bucket=bucket, Key=key)
```

### 处理器状态机

#### NEMProcessor

```yaml
Name: NEMProcessor
Type: STANDARD

States:
  ParseHeader:
    Type: Task
    Resource: Lambda:nem-parse-header
    Comment: 解析 NEM12/NEM13 头部，提取 NMI 列表
    Next: CheckSize

  CheckSize:
    Type: Choice
    Choices:
      - Variable: $.size
        NumericGreaterThan: 52428800  # 50MB
        Next: SplitFile
    Default: ProcessDirectly

  SplitFile:
    Type: Task
    Resource: Lambda:nem-splitter
    Comment: 切分大文件，写回 landing/
    End: true  # 切分后的文件会触发新的处理流程

  ProcessDirectly:
    Type: Map
    ItemsPath: $.nmis
    MaxConcurrency: 10
    ItemProcessor:
      StartAt: ParseNMI
      States:
        ParseNMI:
          Type: Task
          Resource: Lambda:nem-parser
          Next: MapToNeptune
        MapToNeptune:
          Type: Task
          Resource: Lambda:neptune-mapper
          Next: TransformData
        TransformData:
          Type: Task
          Resource: Lambda:sensor-data-transformer
          End: true
    ResultPath: $.transformedData
    Next: BatchWrite

  BatchWrite:
    Type: Task
    Resource: Lambda:batch-csv-writer
    Comment: 批量写入 CSV 到 output/
    Next: ArchiveOriginal

  ArchiveOriginal:
    Type: Task
    Resource: Lambda:archive-file
    Comment: 移动原文件到 processed/
    Next: PublishComplete

  PublishComplete:
    Type: Task
    Resource: arn:aws:states:::events:putEvents
    Parameters:
      Entries:
        - Source: sbm.file-ingester
          DetailType: FileProcessingComplete
          Detail:
            fileType: nem12
            recordsProcessed.$: $.recordsProcessed
    End: true

# 错误处理
Catch:
  - ErrorEquals: [ValidationError]
    Next: MoveToInvalid
  - ErrorEquals: [MappingError]
    Next: MoveToUnmapped
  - ErrorEquals: [States.ALL]
    Next: HandleUnexpectedError
```

---

## S3 存储结构

```
sbm-file-ingester/
│
├── landing/                    # Layer 1: 所有文件首先落地
│   ├── 2026/01/29/            # 按日期分区（可选）
│   │   ├── file1.nem12
│   │   ├── file2.csv
│   │   └── ...
│   │
├── processing/                 # 被 Step Function 处理中的文件
│   └── {execution-id}/        # 按执行 ID 隔离
│       ├── original.nem12
│       └── chunks/
│           ├── chunk_0001.nem12
│           └── chunk_0002.nem12
│
├── processed/                  # 成功处理的原始文件归档
│   ├── 2026/W04/              # 按 ISO 周归档
│   │   └── file1.nem12
│   └── original/              # 被切分的原始大文件
│       └── large_file.nem12
│
├── output/                     # 转换后的 CSV 输出
│   └── sensorDataFiles/       # 供 Glue ETL 消费
│       ├── batch_20260129_001.csv
│       └── batch_20260129_002.csv
│
├── error/                      # 错误文件
│   ├── parse-err/             # 解析失败
│   ├── invalid/               # 验证失败
│   ├── unmapped/              # 无 Neptune 映射
│   └── quarantine/            # 稳定性验证失败
│
└── reports/                    # 账单报告等
    └── usage-spend/
        ├── bunnings/
        └── racv/
```

---

## EventBridge 规则配置

### 规则 1: NEM 文件

```json
{
  "Name": "nem-files-rule",
  "EventPattern": {
    "source": ["sbm.file-ingester"],
    "detail-type": ["FileReadyForProcessing"],
    "detail": {
      "fileType": ["nem12", "nem13"]
    }
  },
  "Targets": [{
    "Id": "nem-processor",
    "Arn": "arn:aws:states:ap-southeast-2:ACCOUNT:stateMachine:NEMProcessor",
    "RoleArn": "arn:aws:iam::ACCOUNT:role/EventBridgeToStepFunctions"
  }]
}
```

### 规则 2: Optima 间隔数据

```json
{
  "Name": "optima-interval-rule",
  "EventPattern": {
    "source": ["sbm.file-ingester"],
    "detail-type": ["FileReadyForProcessing"],
    "detail": {
      "fileType": ["optima-interval", "optima-export"]
    }
  },
  "Targets": [{
    "Id": "optima-processor",
    "Arn": "arn:aws:states:ap-southeast-2:ACCOUNT:stateMachine:OptimaProcessor"
  }]
}
```

### 规则 3: 账单和 Usage-Spend

```json
{
  "Name": "bills-rule",
  "EventPattern": {
    "source": ["sbm.file-ingester"],
    "detail-type": ["FileReadyForProcessing"],
    "detail": {
      "fileType": ["bill", "usage-spend", "invoice"]
    }
  },
  "Targets": [{
    "Id": "bill-processor",
    "Arn": "arn:aws:states:ap-southeast-2:ACCOUNT:stateMachine:BillProcessor"
  }]
}
```

### 规则 4: PDF 文件

```json
{
  "Name": "pdf-rule",
  "EventPattern": {
    "source": ["sbm.file-ingester"],
    "detail-type": ["FileReadyForProcessing"],
    "detail": {
      "fileType": ["pdf"]
    }
  },
  "Targets": [{
    "Id": "pdf-processor",
    "Arn": "arn:aws:states:ap-southeast-2:ACCOUNT:stateMachine:PDFProcessor"
  }]
}
```

### 规则 5: 未知文件类型（人工审核）

```json
{
  "Name": "unknown-files-rule",
  "EventPattern": {
    "source": ["sbm.file-ingester"],
    "detail-type": ["FileReadyForProcessing"],
    "detail": {
      "fileType": ["unknown"]
    }
  },
  "Targets": [{
    "Id": "manual-review-queue",
    "Arn": "arn:aws:sqs:ap-southeast-2:ACCOUNT:manual-review-queue"
  }]
}
```

### 规则 6: 审计所有事件

```json
{
  "Name": "audit-all-events",
  "EventPattern": {
    "source": ["sbm.file-ingester"]
  },
  "Targets": [{
    "Id": "cloudwatch-logs",
    "Arn": "arn:aws:logs:ap-southeast-2:ACCOUNT:log-group:/sbm/file-events"
  }]
}
```

---

## DynamoDB 状态追踪表

### 表结构

```
Table: sbm-file-processing-state

Primary Key: file_id (S3 key 的 hash)
Sort Key: timestamp

Attributes:
├── file_id (PK)           # 文件唯一标识
├── timestamp (SK)         # 事件时间戳
├── original_key           # S3 原始路径
├── file_type              # 检测到的文件类型
├── status                 # RECEIVED | STABILIZING | PROCESSING | COMPLETED | FAILED
├── execution_id           # Step Function execution ARN
├── parent_file_id         # 如果是 chunk，指向父文件
├── chunk_info:            # 切分信息（仅父文件）
│   ├── total_chunks       # 总 chunk 数
│   ├── completed_chunks   # 已完成 chunk 数
│   └── chunk_keys[]       # chunk S3 keys
├── processing_result:     # 处理结果
│   ├── records_processed  # 处理的记录数
│   ├── nmis_mapped        # 映射成功的 NMI 数
│   ├── output_files[]     # 输出文件列表
│   └── duration_ms        # 处理耗时
├── error_info:            # 错误信息（仅失败时）
│   ├── error_type         # 错误类型
│   ├── error_message      # 错误消息
│   └── error_location     # 错误发生位置
├── created_at             # 创建时间
├── updated_at             # 更新时间
└── ttl                    # 自动过期时间（90天后）
```

### 全局二级索引 (GSI)

| 索引名 | Partition Key | Sort Key | 用途 |
|-------|--------------|----------|------|
| status-index | status | timestamp | 按状态查询文件 |
| parent-index | parent_file_id | status | 查询父文件的所有 chunks |
| type-index | file_type | timestamp | 按文件类型统计 |

---

## 错误处理策略

### 错误分类

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           错误处理策略                                        │
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                     瞬态错误 (自动重试)                               │   │
│   │                                                                      │   │
│   │   - Lambda timeout                                                   │   │
│   │   - Neptune connection error                                         │   │
│   │   - S3 throttling                                                    │   │
│   │   - DynamoDB ProvisionedThroughputExceededException                 │   │
│   │                                                                      │   │
│   │   策略: Step Functions 内置 Retry                                    │   │
│   │   - IntervalSeconds: 2                                              │   │
│   │   - MaxAttempts: 3                                                  │   │
│   │   - BackoffRate: 2                                                  │   │
│   │                                                                      │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                    永久错误 (需人工处理)                              │   │
│   │                                                                      │   │
│   │   ParseError:                                                        │   │
│   │   └─► 移动到 error/parse-err/                                       │   │
│   │   └─► 文件格式损坏或不支持                                           │   │
│   │                                                                      │   │
│   │   ValidationError:                                                   │   │
│   │   └─► 移动到 error/invalid/                                         │   │
│   │   └─► 数据内容不符合预期                                             │   │
│   │                                                                      │   │
│   │   MappingError:                                                      │   │
│   │   └─► 移动到 error/unmapped/                                        │   │
│   │   └─► NMI 在 Neptune 中无映射                                        │   │
│   │                                                                      │   │
│   │   QuarantineError:                                                   │   │
│   │   └─► 移动到 error/quarantine/                                      │   │
│   │   └─► 文件稳定性验证失败                                             │   │
│   │                                                                      │   │
│   └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Step Functions Retry 配置

```json
{
  "Retry": [
    {
      "ErrorEquals": [
        "Lambda.ServiceException",
        "Lambda.TooManyRequestsException",
        "Lambda.AWSLambdaException"
      ],
      "IntervalSeconds": 2,
      "MaxAttempts": 6,
      "BackoffRate": 2,
      "JitterStrategy": "FULL"
    },
    {
      "ErrorEquals": ["States.Timeout"],
      "IntervalSeconds": 5,
      "MaxAttempts": 3,
      "BackoffRate": 1.5
    },
    {
      "ErrorEquals": ["TransientError"],
      "IntervalSeconds": 10,
      "MaxAttempts": 5,
      "BackoffRate": 2
    }
  ],
  "Catch": [
    {
      "ErrorEquals": ["ValidationError"],
      "ResultPath": "$.error",
      "Next": "MoveToInvalid"
    },
    {
      "ErrorEquals": ["MappingError"],
      "ResultPath": "$.error",
      "Next": "MoveToUnmapped"
    },
    {
      "ErrorEquals": ["States.ALL"],
      "ResultPath": "$.error",
      "Next": "HandleUnexpectedError"
    }
  ]
}
```

### 告警配置

| 告警名称 | 指标 | 阈值 | 通知渠道 |
|---------|------|------|---------|
| FileStabilizerHighFailureRate | ExecutionsFailed | > 5 / 5min | PagerDuty |
| DLQMessagesHigh | ApproximateNumberOfMessagesVisible | > 10 | PagerDuty |
| ProcessingLatencyHigh | ExecutionTime p95 | > 5min | Slack |
| LambdaErrorRateHigh | Errors | > 10 / 5min | Slack |
| UnknownFilesAccumulating | QueueDepth | > 50 / 1h | Email |

---

## 扩展新文件类型

当需要支持新的文件类型时，只需以下步骤：

### 步骤 1: 添加文件类型枚举

```python
class FileType(Enum):
    # ... 现有类型
    NEW_RETAILER = "new-retailer"  # 新增
```

### 步骤 2: 创建文件检测器

```python
@FileTypeDetector.register
class NewRetailerDetector:
    @staticmethod
    def detect(key: str, head: bytes) -> FileDetectionResult | None:
        if "new-retailer" in key.lower():
            return FileDetectionResult(
                file_type=FileType.NEW_RETAILER,
                encoding="utf-8",
                confidence=0.9,
                metadata={}
            )
        return None
```

### 步骤 3: 创建处理 Step Function

```yaml
Name: NewRetailerProcessor
Type: STANDARD
States:
  Parse:
    Type: Task
    Resource: Lambda:new-retailer-parser
    Next: Transform
  Transform:
    Type: Task
    Resource: Lambda:sensor-data-transformer
    Next: Write
  Write:
    Type: Task
    Resource: Lambda:batch-csv-writer
    End: true
```

### 步骤 4: 添加 EventBridge 规则

```json
{
  "Name": "new-retailer-rule",
  "EventPattern": {
    "source": ["sbm.file-ingester"],
    "detail-type": ["FileReadyForProcessing"],
    "detail": {
      "fileType": ["new-retailer"]
    }
  },
  "Targets": [{
    "Id": "new-retailer-processor",
    "Arn": "arn:aws:states:...:stateMachine:NewRetailerProcessor"
  }]
}
```

### 步骤 5: 部署

```bash
terraform apply
```

**无需修改任何现有代码或组件。**

---

## 设计决策说明

| 设计决策 | 理由 |
|---------|------|
| **Step Functions 而非 Lambda 编排** | 可视化工作流、内置重试、Wait 状态免费、支持长时运行 |
| **EventBridge 而非 SNS/SQS 直接路由** | 内容过滤能力强、规则灵活、审计追踪、易扩展 |
| **文件类型检测器策略链模式** | 新类型只需注册检测器，无需修改核心逻辑 |
| **landing/ → processing/ → processed/** | 清晰的生命周期，便于追踪和问题恢复 |
| **大文件切分回 landing/** | 利用现有流程递归处理，无需额外编排逻辑 |
| **DynamoDB 状态追踪** | 支持父子文件关联、进度追踪、幂等性保证 |
| **分层架构** | 每层职责单一，可独立扩展、测试和替换 |
| **Step Functions Wait 替代 time.sleep()** | 等待期间零成本，不浪费 Lambda 执行时间 |

---

## AWS 服务清单

| 服务 | 用途 | 数量 |
|------|------|------|
| S3 | 文件存储 | 1 bucket (多目录) |
| EventBridge | 事件路由 | 1 custom bus + 6 rules |
| Step Functions | 工作流编排 | 5 状态机 |
| Lambda | 计算 | ~15 函数 |
| DynamoDB | 状态存储 | 1 表 + 3 GSI |
| SQS | DLQ + 人工审核队列 | 2 队列 |
| CloudWatch | 日志、指标、告警 | 多个资源 |
| X-Ray | 分布式追踪 | 启用 |
| SNS | 告警通知 | 1 topic |

---

## 相关文档

- [CLAUDE.md](../CLAUDE.md) - 项目概述和开发指南
- [LEFTHOOK.md](LEFTHOOK.md) - Git Hooks 配置
