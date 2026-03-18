# 系统架构概览

更新日期：2026-03-17  
范围：`D:/videoToMarkdownTest2`

## 1. 系统定位
- 目标：把远端视频、本地视频、书籍文件和文章链接转换为可阅读、可编辑、可归档的 Markdown/JSON 知识产物，并沉淀截图、视频片段、分类、卡片与阅读遥测。
- 核心输入：
  - 远端链接或分享文本：视频 URL、BV、短链、文章链接。
  - 本地输入：浏览器上传文件、本地视频路径、本地书籍文件（`pdf`、`epub`、`txt`、`md`）。
  - 可选控制信息：任务优先级、输出目录、书籍章节/小节范围、合集信息、移动端上下文。
- 核心输出：
  - 任务状态与进度。
  - 结构化文档：`enhanced_output.md`、`result.json`、`video_meta.json`。
  - 素材与中间产物：`assets/`、`intermediates/`、语义单元、任务指标。
  - 辅助资产：分类汇总、概念卡片、阅读遥测、Android 更新清单。
- 职责边界：
  - Java 编排层负责控制平面：受理、持久化状态机、去重、探测、跨阶段编排、实时推送、移动端接口、卡片/分类/更新/遥测聚合。
  - Python gRPC 层负责计算平面：下载、转录、Stage1、Phase2A/2B、视觉链路、提示词加载、模型调用与进程级资源管理。
  - Web PWA 与 Android App 共享同一组 `/api/mobile` 与 `/ws/tasks` 能力。
  - 企业微信机器人是可选外部入口，不属于默认 Docker 启动必需组件。

## 2. 高层拓扑
```mermaid
flowchart LR
    Web["Web PWA\nstatic/index.html"]
    Android["Android App\napp/"]
    WeCom["WeCom Bot\napps/wecom-bot"]
    Java["Java Orchestrator\nservices/java-orchestrator"]
    Py["Python gRPC\nservices/python_grpc"]
    DB["SQLite\nvar/state/collections.db"]
    Storage["任务产物\nvar/storage/storage/{storage_key}"]
    Cards["概念卡片\nvar/cards"]
    Telemetry["阅读遥测\nvar/telemetry"]
    Updates["Android 更新\nvar/app-updates/android"]
    Providers["Whisper / yt-dlp / DeepSeek / DashScope / Vision / OCR / FFmpeg"]

    Web -->|REST /api/mobile\nWebSocket /ws/tasks| Java
    Android -->|REST /api/mobile\nWebSocket /ws/tasks| Java
    WeCom -->|REST /api/tasks| Java
    Java -->|gRPC| Py
    Java --> DB
    Java --> Storage
    Java --> Cards
    Java --> Telemetry
    Java --> Updates
    Py --> Storage
    Py --> Providers
```

## 3. 核心组件分层
### 3.1 Java 编排层
- 路径：`services/java-orchestrator/`
- 主要职责：
  - `controller/VideoProcessingController`：通用任务提交、上传、查询、取消、健康检查。
  - `controller/MobileMarkdownController`：移动端任务列表、增量变更、Markdown 读写、资源读取、锚点挂载/同步、导出、上传分片、任务遥测。
  - `controller/MobileCardController`：概念卡片、候选标题、AI 建议、Phase2B 结构化阅读辅助。
  - `controller/MobileAppUpdateController`：Android 版本检查、APK 下载、发布与回滚。
  - `controller/TelemetryIngestController`：移动端阅读遥测分流到冷数据与逻辑池。
  - `queue/TaskQueueManager`：优先级队列、任务状态机、受理持久化、重启恢复。
  - `worker/TaskProcessingWorker`：消费任务、归一化、去重、探测、并发信号量、watchdog 管理。
  - `service/VideoProcessingOrchestrator`：跨阶段编排与 Java 侧素材工程化执行。
  - `service/StorageTaskCacheService`：扫描并缓存 `var/storage/storage` 历史任务。
  - `service/StorageTaskCategoryService`：书籍和历史任务分类补录，写回统一分类事实源。
  - `service/TaskCleanupIndexService`：按任务状态机维护待清理索引，并在 `00:00-05:00` 窗口按索引执行中间产物清理。
  - `service/CategoryClassificationResultsRepository`：统一读写 `var/storage/category_classification_results.json`。
  - `websocket/TaskWebSocketHandler`：任务、合集、Phase2B 三类实时通道。
- 前端托管：
  - `src/main/resources/static/index.html` 是唯一静态主入口。
  - `WebConfig` 将历史入口 `/mobile-markdown.html` 永久重定向到 `/index.html`。

### 3.2 Python gRPC 计算层
- 路径：`services/python_grpc/src/`
- 主要职责：
  - `server/`：gRPC 启动、依赖预检、协议实现、运行时环境修补。
  - `server/grpc_service_impl.py`：`VideoProcessingService` 主实现，暴露下载、转录、Stage1、Phase2A、VL、组装等 RPC。
  - `media_engine/knowledge_engine/`：下载与转录底座。
  - `transcript_pipeline/`：Stage1 文本预处理与中间结果产出。
  - `content_pipeline/phase2a/`：语义分割、素材请求规划、VL 材料生成、截图路由、知识分类前置能力。
  - `content_pipeline/phase2b/`：富文本组装与视频分类收尾。
  - `content_pipeline/infra/`：提示词注册与加载、LLM 网关、运行时资源管理、缓存指标。
  - `vision_validation/` 与 `worker/`：CV 批处理 worker、共享内存与多进程运行时。

### 3.3 客户端与边缘入口
- `app/`：Android 客户端，包含语义块阅读器、合集 UI、可靠 WebSocket 客户端、前台提交服务、自动更新管理。
- `apps/grpc-server/main.py`：Python gRPC 标准启动入口。
- `apps/wecom-bot/main.py` + `services/python_grpc/src/apps/bot/wecom_bot.py`：企业微信回调机器人，负责消息入口、任务投递与状态回传。
- `contracts/proto/video_processing.proto`：Java/Python 之间的单一协议真源。

## 4. 运行态数据与事实源
### 4.1 控制平面
- SQLite：`var/state/collections.db`
- Redis：`docker-compose.yml` 中的可选 `redis` profile 服务，本地 `run_server.ps1` 默认关闭；如需启用，需要显式传 `-EnableRedis` 或手工启用 compose 的 `redis` profile。它只承担运行态热镜像，不是断点恢复的真源。
  - 运行态 Redis key 显式分成双 scope：
    - `rt:task:{task_id}:stage:{stage}`、`rt:task:{task_id}:events`、`rt:task:{task_id}:meta`
    - `rt:storage:{storage_key}:<stage>:llm:<chunk_id>:<llm_call_id>:aNNN`
  - `task scope` 只放阶段进度、事件流、阻塞态与人工重试语义；`storage scope` 只放可复用的已提交 LLM/chunk 热索引。
- 主要表：
  - `task_runtime_state`：任务状态机快照，包含 `task_id`、`status`、`progress`、`probe_payload_json`、`recovery_payload_json`、`book_options_json`、结果路径等。
  - `task_cleanup_queue`：只记录待清理任务，包含 `task_root`、`completed_at_ms`、`cleanup_after_ms`、`ttl_millis`、`policy_version` 等调度字段；清理成功后即删行。
  - `video_collections`、`collection_episodes`：合集与分集绑定。
  - `task_manual_collection_bindings`：人工归档合集绑定。
  - `file_metadata`、`file_probe_cache`：上传复用与探测缓存。

### 4.2 内容平面
- 任务主存储目录：`var/storage/storage/{storage_key}/`
- `storage_key` 生成方式：
  - 普通视频/本地文件：优先使用归一化输入的 MD5。
  - 书籍 leaf：可使用显式 `storageKey`，确保同一本书的叶子节点可稳定复用目录。
- 典型内容：
  - `enhanced_output.md`
  - `result.json`
  - `video_meta.json`
  - `assets/`
  - `intermediates/task_metrics_latest.json`
  - 规范化中间产物目录：
    - `intermediates/rt/`：运行态恢复真源，核心是任务内 `runtime_state.db`；新任务当前保留 `task_meta / stage_snapshots / scope_nodes / scope_edges / llm_records / llm_record_content / chunk_records / chunk_record_content / scope_hint_plan / scope_hint_latest`。文件侧默认只保留 `task_meta.json`、Java 续跑 hint `resume_index.json`、以及 `stage_state.json`/人工介入与 fallback 记录；`stage_journal.jsonl`、`outputs_manifest.json`、LLM/chunk attempt JSON 不再是新任务主写路径。
    - `intermediates/stages/stage1/outputs/`：`step1_validate.json`、`step2_correction.json`、`step3_merge.json`、`step3_5_translate.json`、`step4_clean_local.json`、`step5_6_dedup_merge.json`、`sentence_timestamps.json`
    - `intermediates/stages/phase2a/outputs/`：`semantic_units.json`、`semantic_units_vl_subset.json`、`vl_analysis.json`
    - `intermediates/stages/phase2a/audits/`：`vl_analysis_output_latest.json`、token cost audit、VL/Vision/DeepSeek 审计镜像

### 4.3 统一汇总资产
- 分类事实源：`var/storage/category_classification_results.json`
  - 视频主链由 Python `phase2b/video_category_service.py` 在 Phase2B 末尾写入。
  - 书籍任务与历史缺分类任务由 Java `StorageTaskCategoryService` 补录到同一文件。
- 概念卡片：`var/cards`
- 阅读遥测：`var/telemetry/*.ndjson`
- Android 更新：`var/app-updates/android/`
- 上传缓冲：`var/uploads/`

## 5. 任务控制平面
### 5.1 状态机
```mermaid
stateDiagram-v2
    [*] --> QUEUED
    QUEUED --> PROBING
    PROBING --> PROCESSING
    PROCESSING --> COMPLETED
    PROCESSING --> FAILED
    PROBING --> MANUAL_RETRY_REQUIRED
    PROCESSING --> MANUAL_RETRY_REQUIRED
    PROBING --> FATAL
    PROCESSING --> FATAL
    FAILED --> QUEUED: retry
    MANUAL_RETRY_REQUIRED --> QUEUED: retry
    FATAL --> QUEUED: retry
    QUEUED --> DEDUPED
    PROBING --> DEDUPED
    QUEUED --> CANCELLED
    PROBING --> CANCELLED
    PROCESSING --> CANCELLED
```

### 5.2 控制原则
- 提交成功等价于“已 durable accept”：
  - `TaskQueueManager.submitTask(...)` 在返回前先把最小任务快照写入 `task_runtime_state`，再进入内存队列。
- 归一化、去重、探测全部放在 worker 侧：
  - 提交线程不再执行同步探测，也不承担历史去重。
  - `TaskProcessingWorker` 在消费阶段统一执行输入归一化、活跃任务去重、历史任务复用判定与探测。
  - 对远端视频，probe 可以从下载关键路径摘出，先进入处理链路，再后台补写标题与探测 payload。
- 服务重启可恢复：
  - `TaskQueueManager.restorePersistedTasks()` 会把 SQLite 中的活跃任务恢复成运行时投影。
  - Java 控制面新增 `TaskRuntimeRecoveryService`，现在会优先读取任务目录 `intermediates/rt/runtime_state.db` 里的阶段快照；`stage_state.json` 与旧 `s/` 路径只保留兼容兜底，并把 `MANUAL_RETRY_REQUIRED/FATAL` 投影回 `TaskQueueManager` 的阻塞态。
  - `task_runtime_state.recovery_payload_json` 持久化保存 `stage / checkpoint / retry_mode / required_action / retry_entry_point / retry_strategy / operator_action / action_hint`，让控制面、移动端接口和人工排障共享同一份恢复语义。
  - 对没有阻塞指令的 `PROBING/PROCESSING` 中断任务，仍回退为 `QUEUED` 重新排队。
  - Python 侧 Phase2A/Phase2B 新增 `intermediates/rt/` 运行态提交真源：
    - `Phase2A` 以截图优化 chunk 为提交边界，已提交 chunk 重启后直接回填恢复，不再重复跑 CV worker。
    - `Phase2A screenshot chunk` 新增 `scope_graph + dirty plan`：依赖边真源已经收口到任务内 `runtime_state.db.scope_edges`，`scope_nodes` 只保留节点当前态与恢复必需的结构化标量。chunk 进入 restore 前会先校验本 scope 是否被标记 dirty，以及其依赖 scope 指纹是否漂移；命中 dirty 时不再错误复用旧 commit。
    - `Phase2B` 以单次 LLM 调用为提交边界，`llm_call/chunk/scope_hint` 先落任务内 `runtime_state.db`，可选 Redis 仅同步热状态；本地 attempt JSON 不再是新任务主写路径。
    - 统一恢复规划开始贯通主链：`transcribe subtitles -> stage1 outputs -> phase2a semantic_units -> phase2b outputs` 现在都能注册为 artifact/input scope；上游 `llm_call/chunk/artifact` 被标脏后，可经 `scope_graph` 继续向下游 artifact 传播 dirty 集。
    - `Stage1` 外层执行器现在开始消费统一 `resume_plan`：`grpc_service_impl.ProcessStage1` 会先基于 scope graph 和 step artifact 生成 `resume_state / resume_entry_step / dirty_scope_count / failed_scope_*`，`transcript_pipeline.graph` 只负责注入 plan 并把它翻译成 `_resume_mode / _last_completed_index`。这意味着 step 只保留为执行顺序，最小恢复单元继续下沉在内部 `llm_call` restore/commit 上。
    - `Stage1` 现在也不再只停留在 step 级恢复：`DeepSeekClient.complete_json` 在 `stage1` 运行态上下文里会把每次外部调用按 `llm_call` 提交到本地真源；重跑时若 `prompt/system/model/kwargs` 对应的提交已存在，会直接复用已提交响应，不再重打模型。
    - `Phase2A` 的 `deepseek_complete_text/json`、`vision_validate_image(s)`、`VLVideoAnalyzer._call_vl_api(...)` 在 `phase2a` 运行态上下文里同样按 `llm_call` 提交；`VL` 现在也是“调用前先 restore、调用后再 commit/fail”的闭环，`raw_llm_interactions` 继续保留为审计镜像而不再承担恢复真源职责。
    - 新任务默认不再写 `llm_call/chunk` attempt 目录；旧 `intermediates/rt/stage/<stage>/chunk/<chunk_id>/call/<llm_call_id>.aNNN/` 与更早的 `s/` 目录只保留历史任务兼容读取。
  - Python gRPC 阶段入口现在通过 `RuntimeStageSession` 同步 upsert 任务内 `runtime_state.db` 的阶段快照；`intermediates/rt/stage/<stage>/stage_state.json` 只在镜像开关打开时导出：
    - `DownloadVideo`、`TranscribeVideo`、`ProcessStage1`、`AnalyzeSemanticUnits`、`AssembleRichText`、`AnalyzeWithVL` 会把关键 checkpoint、完成度、错误分类和核心产物路径写入阶段状态。
    - `RuntimeStageSession` 统一封装：
      - 软/硬心跳发射；
      - 阶段 snapshot 更新；
      - runtime checkpoint 持久化；
      - 失败分类后的 `retry_mode / required_action / retry_entry_point` 语义；
      - 面向机器消费的 `retry_strategy / operator_action / action_hint` 语义。
    - 阶段级 state 的 ownership 收敛到 gRPC 入口；`markdown_enhancer`、`vl_material_generator` 等内层模块只保留 chunk/LLM-call 级恢复写入，不再重复写 `phase2a/phase2b` 阶段状态。
    - Redis 中的 `stage_state` 只保留热路径最小字段：`status/checkpoint/completed/pending/updated_at_ms/local_stage_state_path` 以及错误/重试指令，不再镜像本地完整 payload，避免下游重复记录可从本地真源继承的信息。
    - `task scope` 的 Redis 清理不再依赖后台扫描：
      - `COMPLETED / FAILED / CANCELLED / DEDUPED` 在 Java 控制面进入终态时立刻设置过期时间。
      - `QUEUED / PROBING / PROCESSING / MANUAL_RETRY_REQUIRED / FATAL` 进入或回到非终态时立刻取消过期。
    - `storage scope` 的 Redis / 本地 attempt 不再无限增长：
      - 每次新的 `llm_call` success / failure 提交后，运行态存储层立即按“最近 1 次成功 + 最近 1 次失败”裁剪旧 attempts。
      - 旧 success 至少保留 3 天；旧 failure 不保留历史堆积，只保留最近一次。
    - `COMPLETED` 任务的文件清理不再依赖扫目录判定：
      - Java 在持久化 `task_runtime_state` 时同步维护 `task_cleanup_queue`，把 `cleanup_after_ms = completed_at_ms + ttl` 一并落库。
      - TTL 变化时，`task_cleanup_queue` 会原子重算 `ttl_millis + cleanup_after_ms`，保证凌晨执行器只扫描待清理索引。
    - `TranscribeVideo` 在返回成功前会先 flush 异步字幕写盘，再把阶段标记为完成，避免“响应成功但字幕文件尚未真正提交”的假完成。
    - Java 侧不再只会“统一重排队”：
      - worker 失败后会先查询最新阶段状态；若 Python 已明确标记 `MANUAL_RETRY_REQUIRED/FATAL`，任务直接停在阻塞态，不再伪装成普通 `FAILED`。
      - `TaskProcessingWorker` 的失败广播跟随 `TaskQueueManager` 最终状态，不再硬编码把阻塞任务推成 `FAILED`。
      - `/api/mobile/tasks/{taskId}/retry` 与 `/api/tasks/{taskId}/retry` 显式暴露人工修复后的续跑入口，`retry` 会清空 `recoveryPayload` 并重新入队。
      - `/api/mobile/tasks`、`/api/mobile/tasks/{taskId}` 与 `/api/tasks/{taskId}` 会透出 `blocked / recoveryStage / recoveryCheckpoint / retryMode / requiredAction / retryEntryPoint / retryStrategy / operatorAction / actionHint`，让前端与运维看到的是可执行语义而不只是状态标签。
      - `TaskStatusPresentationService` 统一承接状态分类与 recovery payload 投影，HTTP controller 与 `TaskWebSocketHandler` 不再各自维护一套 `blocked/statusCategory/recovery*` 拼装逻辑。

### 5.3 实时通道
- WebSocket 入口：`/ws/tasks`
- 当前协议按语义拆分为两层：
  - 普通任务状态更新走“快照推送 + REST `/api/mobile/tasks/changes` 对账”模型，不再为每条状态消息维护离线补发队列。
  - 浏览器 `web-task-updates` 流额外启用传输层心跳：服务端定时发送 WebSocket `PingMessage`，浏览器回 `PongMessage`，用于更快识别半开连接。
  - 终态事件单独走 `TaskTerminalEventService`：`COMPLETED/FAILED` 事件按 `userId` 入队，浏览器建连时携带 `lastAckedTerminalEventId`，收到 `taskTerminalEvent` 后通过 `ack` 确认并支持断线补发。
- 支持的订阅维度：
  - 单任务进度。
  - 合集进度。
  - Phase2B 专用频道。

## 6. 主处理链路
### 6.1 视频任务主链
1. 客户端通过 `POST /api/tasks`、`POST /api/tasks/upload`、`POST /api/mobile/tasks/submit` 或移动端上传接口提交任务。
2. Java 持久化最小状态后入队，`TaskProcessingWorker` 取出任务并完成归一化、去重与探测。
3. `VideoProcessingOrchestrator` 进入跨阶段编排：
   - `DownloadVideo` 或将本地文件纳入 storage 目录。
   - `TranscribeVideo`
   - `ProcessStage1`
   - `AnalyzeSemanticUnits`
4. Phase2A 后进入两条分析路径之一：
   - 首选 VL 路径：`AnalyzeWithVL`
   - 回退传统路径：`ValidateCVBatch` + `ClassifyKnowledgeBatch` + `GenerateMaterialRequests`
5. Java 侧 `JavaCVFFmpegService` 负责截图、切片等工程化素材抽取。
6. Python `AssembleRichText` 组装最终 Markdown/JSON，并在 Phase2B 尾部完成视频分类写回。
7. Java 侧补充任务指标、缓存、清理、实时推送与终态持久化。

### 6.2 书籍与文章链路
1. `TaskProbeService` 对书籍文件和文章链接走专门探测分支，不再套用视频探测逻辑。
2. 文章链接先由 `Phase2bArticleLinkService` 抽取正文与图片，落为本地 Markdown 源，再进入书籍增强编排。
3. `BookMarkdownService` 负责基础抽取，产出书籍 Markdown 与元数据。
4. 若开启增强：
   - `BookEnhancedPipelineService` 先保护图片/表格/代码/公式占位。
   - 对英文段落做条件翻译。
   - 合成 Phase2A 输入并调用 `AnalyzeSemanticUnits` 与 `AssembleRichText`。
   - 最后回填占位符，生成增强版书籍 Markdown。
5. 若增强失败，自动回退到基础书籍结果，不阻断任务成功。
6. 书籍与历史任务分类由 Java 侧补齐，并继续写入统一分类事实源。

## 7. 客户端与阅读面能力
### 7.1 Web PWA
- 主页面由 `index.html` 直接托管，无单独前端构建链。
- 核心能力：
  - 任务列表与增量对账：`/api/mobile/tasks`、`/api/mobile/tasks/changes`
  - 提交与上传：普通提交、秒传检查、分片上传、上传探测
  - Markdown/资源：读取、整文保存、资源流式访问、导出
  - 锚点能力：挂载、同步、删除、挂载态查询
  - 实时能力：WebSocket 快照更新、浏览器传输层心跳、终态事件补发、REST 变更对账

### 7.2 概念卡片与阅读增强
- `/api/mobile/cards/**` 提供：
  - 标题索引与候选词筛选
  - 卡片读写
  - AI 建议
  - 思考块写回
  - Phase2B 结构化阅读辅助
- 卡片存储与 UI 解耦：
  - 数据层保存 Markdown/YAML frontmatter。
  - 反向链接与高亮候选在运行时计算，不污染底层文件。

### 7.3 Android 客户端
- 路径：`app/src/main/java/com/hongxu/videoToMarkdownTest2/`
- 关键能力：
  - `ReliableTaskWebSocketClient`、`TaskRealtimeClient`、`CollectionRealtimeClient`：统一封装心跳、重连、订阅恢复与任务实时状态消费。
  - `SemanticTopographyReader`：语义块阅读器，而不是整篇单块渲染。
  - `TaskSubmissionForegroundService`：前台任务提交与订阅。
  - `MobileAppAutoUpdateManager`：消费 `/api/mobile/app/update/**` 更新链路。

## 8. 部署与外部依赖
- 默认发布拓扑：`docker-compose.yml` 启动两个核心服务：
  - `python-grpc`：暴露 `50051`
  - `java-orchestrator`：暴露 `8080`
- 两个容器共享：
  - `./config`：配置真源
  - `./var`：运行态数据真源
- Python 侧外部依赖包括：
  - Whisper / faster-whisper
  - yt-dlp 与站点探测
  - DeepSeek / DashScope / Vision API
  - OCR / PP-Structure / OpenCV / FFmpeg
- `apps/grpc-server/main.py --check-deps` 是运行前依赖预检入口，用于提前发现环境缺失，而不是等运行期静默降级。

## 9. 当前架构收敛点
- 控制平面与内容平面已分离：
  - 控制状态看 SQLite。
  - 内容产物看 `var/storage/storage/{storage_key}`。
- 分类事实源已统一：
  - 任务列表只消费 `var/storage/category_classification_results.json`，不再拆分多份分类真相。
- 总览与日志分工已固定：
  - `overview.md` 只描述当前稳定架构。
  - 具体演进与性能数据进入 `upgrade-log.md`。
- 运行中断恢复已内建到状态机，而不是依赖人工补救。

## 10. 维护约束
- 协议改动先改 `contracts/proto/video_processing.proto`，再生成两端代码。
- 架构边界调整后必须同步更新：
  - `docs/architecture/overview.md`
  - `docs/architecture/repository-map.md`
  - `docs/architecture/upgrade-log.md`
- 重大故障修复沉淀到：
  - `docs/architecture/error-fixes.md`
- 运行期排障优先观察：
  - `task_runtime_state`
  - `var/storage/storage/{storage_key}/intermediates/task_metrics_latest.json`
  - `var/storage/category_classification_results.json`
  - `var/telemetry/*.ndjson`
