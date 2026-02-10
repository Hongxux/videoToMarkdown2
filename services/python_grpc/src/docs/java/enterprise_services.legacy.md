# V2 视频处理架构 - 完整技术文档

## 📋 项目概述

本项目实现了一个 **Java-Python 分层视频处理系统**，将教学视频自动转换为结构化的富文本文档。核心创新包括：

1. **异构语言协作** - Java 负责编排和并发，Python 负责 AI 推理
2. **两阶段 Module2 设计** - 解决 FFmpeg 依赖循环问题
3. **企业级容错机制** - 熔断器 + 指数退避重试
4. **动态资源调度** - 基于系统负载自适应并发

---

## 🏗️ 系统架构

### 整体架构图

```
┌─────────────────────────────────────────────────────────────────────┐
│                         用户界面层                                   │
│  ┌─────────────────┐    ┌─────────────────────────────────────┐    │
│  │   Web 前端       │    │   WebSocket 实时推送                 │    │
│  │   (index.html)   │◄───│   (TaskWebSocketHandler)            │    │
│  └────────┬────────┘    └─────────────────────────────────────┘    │
│           │                                                         │
│           ▼                                                         │
├─────────────────────────────────────────────────────────────────────┤
│                      Java 编排层 (Spring Boot)                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────────┐ │
│  │ REST API    │  │TaskQueue    │  │LoadBased    │  │Circuit     │ │
│  │ Controller  │──│Manager      │──│Scheduler    │  │Breaker     │ │
│  └─────────────┘  └─────────────┘  └─────────────┘  └────────────┘ │
│         │                                                │          │
│         ▼                                                ▼          │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              VideoProcessingOrchestrator                     │   │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐ │   │
│  │  │  Step 1  │  │  Step 2  │  │  Step 3  │  │   Step 4-6   │ │   │
│  │  │ Download │─►│ Whisper  │─►│ Stage1   │─►│   Module2    │ │   │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────┘ │   │
│  └─────────────────────────────────────────────────────────────┘   │
│         │                                                           │
│         │ gRPC                     ┌────────────────────┐          │
│         │                          │   FFmpegService    │          │
│         │                          │ (并行截图+切片)    │          │
│         │                          └────────────────────┘          │
├─────────┼───────────────────────────────────────────────────────────┤
│         ▼                    Python Worker 层                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │              Python gRPC Server (50051)                      │   │
│  │  ┌─────────────────────────────────────────────────────┐    │   │
│  │  │         GlobalResourceManager (Singleton)            │    │   │
│  │  │   • LLM Client (DeepSeek)                            │    │   │
│  │  │   • Vision AI Client                                  │    │   │
│  │  │   • KnowledgeClassifier                               │    │   │
│  │  └─────────────────────────────────────────────────────┘    │   │
│  │                                                              │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────────────┐    │   │
│  │  │ Stage1     │  │ Phase2A    │  │ Phase2B            │    │   │
│  │  │ Pipeline   │  │ analyze    │  │ assemble           │    │   │
│  │  │ (LangGraph)│  │ only       │  │ only               │    │   │
│  │  └────────────┘  └────────────┘  └────────────────────┘    │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## � 核心处理流程

### V2 六步流水线

```
┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐
│ 1.下载 │───►│2.转录 │───►│3.Stage1│───►│4.分析 │───►│5.FFmpeg│───►│6.组装 │
│ 视频  │    │Whisper│    │Pipeline│    │Phase2A│    │ 并行  │    │Phase2B│
└──────┘    └──────┘    └──────┘    └──────┘    └──────┘    └──────┘
  Python      Python      Python      Python       Java       Python
```

| 步骤 | 执行者 | 输入 | 输出 | 核心逻辑 |
|------|--------|------|------|----------|
| 1 | Python | URL | video.mp4 | yt-dlp 下载 |
| 2 | Python | video.mp4 | subtitles.json | Whisper 转录 |
| 3 | Python | subtitles | step2/step6.json | LangGraph 7步处理 |
| 4 | Python | step6.json | MaterialRequests | 语义分析，收集素材需求 |
| 5 | Java | MaterialRequests | screenshots/clips | FFmpeg 并行提取 |
| 6 | Python | Materials | markdown | Vision AI 验证 + 富文本组装 |

---

## 🧩 核心模块实现

### 1. Java 编排层

#### TaskQueueManager - 任务队列管理

```java
// 核心功能
- 优先级队列 (VIP > HIGH > NORMAL > LOW)
- 公平调度 (防止单用户独占)
- 并发控制 (Semaphore 限流)
- 任务生命周期跟踪

// 关键方法
submitTask(userId, videoUrl, priority)  // 提交任务
pollNextTask(timeout)                    // 获取待处理任务
completeTask(taskId, resultPath)         // 标记完成
```

#### CircuitBreaker - 熔断器

```java
// 状态机
CLOSED ──(失败>=5)──► OPEN ──(30s后)──► HALF_OPEN ──(成功x2)──► CLOSED
                              │                      │
                              └──────(失败)──────────┘

// 配置
failureThreshold = 5       // 触发熔断的失败次数
openDuration = 30s         // 熔断持续时间
halfOpenSuccessThreshold = 2  // 恢复所需成功次数
```

#### RetryPolicy - 重试策略

```java
// 指数退避公式
delay = initialDelay * (backoffMultiplier ^ attempt) ± 10%抖动

// 默认配置
maxRetries = 3
initialDelay = 1s
maxDelay = 30s
backoffMultiplier = 2.0
```

#### LoadBasedScheduler - 负载调度器

```java
// 监控指标
- CPU 使用率 (阈值: 50%/80%)
- 内存使用率 (阈值: 85%)
- 可用内存 (最低: 512MB)

// 状态
NORMAL    → 可增加并发
BUSY      → 保持并发
OVERLOADED → 减少并发，拒绝新任务
```

### 2. Python Worker 层

#### GlobalResourceManager - 单例资源管理

```python
# 目的：避免重复加载昂贵资源
class GlobalResourceManager:
    _instance = None
    
    def initialize(self):
        self.llm_client = LLMClient()          # DeepSeek
        self.vision_client = VisionAIClient()  # 截图验证
        self.knowledge_classifier = KnowledgeClassifier()
```

#### Stage1 Pipeline - 七步处理

```python
# 支持 max_step 提前终止
步骤1: 纠正字幕 (LLM)
步骤2: 精炼字幕 (LLM)
步骤3: 语义分割
步骤4: 段落合并
步骤5: 标题生成
步骤6: 跨段落合并
步骤7: 最终输出
```

#### Phase2A / Phase2B - Module2 两阶段

```python
# Phase2A: analyze_only()
输入: step2.json, step6.json
输出: ScreenshotRequest[], ClipRequest[], semantic_units.json
逻辑: 分析语义单元，决定需要哪些截图和视频片段

# Phase2B: assemble_only()
输入: semantic_units.json, screenshots/, clips/
输出: final_document.md, final_document.json
逻辑: Vision AI 验证截图质量，组装最终富文本
```

---

## 🔌 gRPC 接口定义

```protobuf
service VideoProcessingService {
  // 基础步骤
  rpc DownloadVideo(DownloadRequest) returns (DownloadResponse);
  rpc TranscribeVideo(TranscribeRequest) returns (TranscribeResponse);
  rpc ProcessStage1(Stage1Request) returns (Stage1Response);
  
  // Module2 两阶段 (V2新增)
  rpc AnalyzeSemanticUnits(AnalyzeRequest) returns (AnalyzeResponse);
  rpc AssembleRichText(AssembleRequest) returns (AssembleResponse);
  
  rpc HealthCheck(Empty) returns (HealthResponse);
}
```

---

## 🗂️ 项目结构

```
videoToMarkdownTest2/
├── proto/
│   └── video_processing.proto      # gRPC 接口定义
├── python_grpc_server.py           # Python gRPC 服务器
├── generate_grpc.bat               # gRPC 代码生成脚本
├── stage1_pipeline/
│   └── graph.py                    # Stage1 LangGraph (支持 max_step)
└── MVP_Module2_HEANCING/
    ├── module2_content_enhancement/
    │   ├── rich_text_pipeline.py   # Phase2A/2B 实现
    │   └── concrete_knowledge_validator.py
    └── enterprise_services/
        ├── frontend/
        │   └── index.html          # Web 界面
        └── java_orchestrator/
            └── src/main/java/com/mvp/module2/fusion/
                ├── controller/
                │   └── VideoProcessingController.java
                ├── grpc/
                │   └── PythonGrpcClient.java
                ├── queue/
                │   └── TaskQueueManager.java
                ├── resilience/
                │   ├── CircuitBreaker.java
                │   ├── RetryPolicy.java
                │   └── ResilientGrpcClient.java
                ├── scheduler/
                │   └── LoadBasedScheduler.java
                ├── service/
                │   ├── FFmpegService.java
                │   ├── DynamicTimeoutCalculator.java
                │   └── VideoProcessingOrchestrator.java
                └── websocket/
                    ├── TaskWebSocketHandler.java
                    └── WebSocketConfig.java
```

---

## 🚀 快速启动

### 环境要求

| 组件 | 版本 |
|------|------|
| Python | 3.10+ |
| Java | 17+ |
| Maven | 3.8+ |
| FFmpeg | 4.0+ (自动检测) |

### 启动步骤

```bash
# 1. 安装 Python 依赖
pip install grpcio grpcio-tools langgraph openai

# 2. 生成 gRPC 代码
generate_grpc.bat

# 3. 配置环境变量 (.env)
DEEPSEEK_API_KEY=your-key

# 4. 启动 Python Server
python python_grpc_server.py

# 5. 启动 Java Server
cd MVP_Module2_HEANCING/enterprise_services/java_orchestrator
mvn spring-boot:run

# 6. 打开前端
# 浏览器访问 frontend/index.html
```

---

## 📡 API 参考

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/tasks` | 提交任务 `{userId, videoUrl, priority}` |
| GET | `/api/tasks/{id}` | 查询任务状态 |
| DELETE | `/api/tasks/{id}` | 取消任务 |
| GET | `/api/health` | 健康检查 |
| WS | `/ws/tasks?userId=xxx` | 实时状态推送 |

---

## ⚙️ 配置项

### Java (`application.properties`)

```properties
grpc.python.host=localhost
grpc.python.port=50051
ffmpeg.threads=4
task.queue.max-concurrent=4
```

### 动态超时 (基于视频时长)

| 视频时长 | Stage1 | Phase2A | Phase2B |
|----------|--------|---------|---------|
| <10分钟 | 5分钟 | 3分钟 | 5分钟 |
| 10-30分钟 | 10分钟 | 5分钟 | 8分钟 |
| >30分钟 | 15分钟 | 8分钟 | 12分钟 |
