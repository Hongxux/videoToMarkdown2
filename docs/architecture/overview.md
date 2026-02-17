# 系统架构概览

更新日期：2026-02-17  
范围：`D:/videoToMarkdownTest2`

## 1. 系统目标与边界
- 目标：将视频内容转换为结构化知识文档（Markdown/JSON），并沉淀可复用素材（截图/视频片段）。
- 输入：`videoUrl`、本地视频路径或浏览器直传视频文件、任务优先级、输出目录、可选标题。
- 输出：知识文档、素材目录（`screenshots/`、`clips/`）以及阶段性中间产物（`semantic_units`、`step2/step6` 等）。
- 职责边界：
  - Java 编排层负责任务生命周期、跨阶段调度、进度回传、素材工程化执行。
  - Python 推理层负责转写、语义处理、视觉校验、富文本组装与模型调用。

## 2. 目录与数据落盘约束
- 运行产物统一落盘：`var/storage/{url_hash}/...`。
- 历史兼容读取：允许读取旧路径 `storage/{url_hash}`，但新任务不再写入旧路径。
- 关键约束：同一任务全链路必须复用同一个 `url_hash`，避免跨目录碎片化与状态漂移。

## 3. 高层架构
```mermaid
flowchart LR
    Client -->|REST /api| JavaOrchestrator
    Client -->|WebSocket /ws/tasks| JavaOrchestrator
    WeComUser -->|企业微信内部应用消息| WeComCallback
    WeComCallback -->|HTTP /api/tasks + /api/tasks/{id}| JavaOrchestrator
    WeComCallback -->|企业微信发送接口| WeComUser
    JavaOrchestrator -->|gRPC| PythonGrpcService
    JavaOrchestrator -->|JavaCV/FFmpeg| AssetExtractor
    PythonGrpcService -->|Whisper/LLM/Vision| ExternalProviders
    PythonGrpcService -->|Stage1/Phase2| Storage[(var/storage/{url_hash})]
```

## 4. 核心组件
- API/编排层（Java）
  - 路径：`services/java-orchestrator/`
  - 控制器：`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/controller/VideoProcessingController.java`
  - 核心编排：`service/VideoProcessingOrchestrator`
  - 调度与资源治理：`queue/TaskQueueManager`、`worker/TaskProcessingWorker`、`service/AdaptiveResourceOrchestrator`、`service/DynamicTimeoutCalculator`、`scheduler/LoadBasedScheduler`
  - 可靠性：`resilience/`（熔断、重试）
  - 通信：`grpc/PythonGrpcClient`、`websocket/TaskWebSocketHandler`
  - 素材工程化：`service/JavaCVFFmpegService`
- 移动端 Markdown 展示与任务提交：`controller/MobileMarkdownController` + `static/index.html`
  - 统一静态入口：`static/index.html` 直接承载主页面；`static/mobile-markdown.html` 与 `index.html` 保持同构内容，避免跳转中间页
    - 聚合运行中任务与 `var/storage/storage` 历史任务（历史任务以 `storage:{目录名}` 作为外部任务ID）
    - 提供任务列表与 Markdown 正文读取/写回（支持段落级编辑后的整文保存）
    - 提供任务目录内图片/视频等资源文件预览
    - 提供移动端任务提交（URL/BV 直提 + 本地视频文件上传，调用 `/api/mobile/tasks/submit` 与 `/api/mobile/tasks/upload`）
- 企业微信消息入口（Python）
  - 启动入口：`apps/wecom-bot/main.py`
  - 服务实现：`services/python_grpc/src/apps/bot/wecom_bot.py`
  - 职责：
    - 处理 `GET/POST /wechat/callback` 的签名校验与 AES 解密
    - 将 URL 指令映射为任务并串行执行（单工作线程）
    - 失败自动重试 2 次（总尝试 3 次）
    - 将任务状态回传到发起人的企业微信个人聊天
- 推理/处理层（Python）
  - gRPC 启动入口：`apps/grpc-server/main.py`
  - 服务与依赖预检：`services/python_grpc/src/server/`
  - 转写与知识引擎：`services/python_grpc/src/media_engine/knowledge_engine/`
  - Stage1 文本处理：`services/python_grpc/src/transcript_pipeline/`
  - Phase2A/2B 语义与富文本：`services/python_grpc/src/content_pipeline/`
  - CV 批处理执行：`services/python_grpc/src/vision_validation/worker.py`
- 合约与生成代码
  - Proto 真源：`contracts/proto/video_processing.proto`
  - Python 生成代码：`contracts/gen/python/`

## 5. 主链路调用（当前实现）
1. 客户端通过 `POST /api/tasks`（URL/BV/本地路径）或 `POST /api/tasks/upload`（浏览器直传视频）提交任务，Java 创建任务并入队；URL 提交入口支持“Bilibili BV号 -> 标准 URL”归一化。
2. Java 调用 Python gRPC：`DownloadVideo`、`TranscribeVideo`、`ProcessStage1`。
3. Java 调用 `AnalyzeSemanticUnits` 与 `GenerateMaterialRequests`，产出语义单元与素材请求。
4. 视觉链路按需调用 `ValidateCVBatch`、`ClassifyKnowledgeBatch`、`AnalyzeWithVL`。
5. Java 侧执行截图/切片等素材抽取，再调用 `AssembleRichText` 生成最终 Markdown/JSON。
6. 任务状态通过 REST 可查询，并通过 WebSocket 持续推送进度。
7. 企业微信消息链路中，`wecom_bot` 复用 Java REST 接口提交任务并轮询状态，按 `QUEUED/RUNNING/RETRYING/SUCCEEDED/FAILED_FINAL` 回传个人聊天。
8. 静态页面入口统一为 `/` -> `index.html`（主页面本体），并与 `/mobile-markdown.html` 保持同构内容；页面通过 `/api/mobile/tasks` 罗列任务（含内存任务与磁盘历史任务），按任务维度读取 markdown 与资源文件进行渲染。

## 6. 接口清单（2026-02-17）
- REST（Java）
  - `/api/tasks`
  - `/api/tasks/upload`
  - `/api/tasks/{taskId}`
  - `/api/tasks/user/{userId}`
  - `/api/stats`
  - `/api/health`
  - `/api/admin/reset-circuit-breaker`
  - `/api/mobile/tasks`
  - `/api/mobile/tasks/submit`
  - `/api/mobile/tasks/upload`
  - `/api/mobile/tasks/{taskId}/markdown`
  - `/api/mobile/tasks/{taskId}/markdown`（PUT：保存编辑后的 Markdown 内容）
  - `/api/mobile/tasks/{taskId}/markdown/by-path`
  - `/api/mobile/tasks/{taskId}/asset?path=...`
- WebSocket（Java）
  - `/ws/tasks`
- HTTP Callback（Python）
  - `GET /wechat/callback`（企业微信 URL 校验）
  - `POST /wechat/callback`（企业微信消息回调）
- gRPC（Java <-> Python）
  - `DownloadVideo`
  - `TranscribeVideo`
  - `ProcessStage1`
  - `AnalyzeSemanticUnits`
  - `GenerateMaterialRequests`
  - `ValidateCVBatch`
  - `ClassifyKnowledgeBatch`
  - `AnalyzeWithVL`
  - `AssembleRichText`
  - `ReleaseCVResources`
  - `HealthCheck`

## 7. 当前架构收敛点（截至 2026-02-14）
- Prompt 管理收敛：`MarkdownEnhancer` 等模块统一通过 `prompt_loader + prompt_registry` 获取模板，避免硬编码提示词与运行时脱节。
- Phase2B 素材过滤语义固定：使用 `should_include` 作为图片进入 Markdown 结构化流程的准入标记；被 Vision 拒绝的图片不再进入最终文档。
- 回填策略受限：`RichTextPipeline` 仅在“请求缺失”场景下从 `vl_analysis_cache.json` 回填素材请求，并过滤实际 VL 调用产物，避免缓存污染。
- 动作单元一致性：`GenerateMaterialRequests` 落盘时同步 `action_units` 与 `action_segments`，避免“字段分叉”导致的语义误判。
- 预处理能力前置校验：`apps/grpc-server/main.py --check-deps` 会检查 PP-Structure、PaddleX、人物预过滤后端与关键版本组合，降低运行期静默降级风险。
- Whisper 模型启动优化：已支持“首轮校验成功后重启复用”，减少重复完整性校验造成的冷启动耗时。

## 8. 迁移状态
- 历史兼容壳目录已下线，运行主链仅保留新分层目录。
- 历史文档归档：`services/python_grpc/src/docs/legacy/`、`docs/archive/`。
- 历史依赖清单归档：`requirements/legacy/`。

## 9. 维护约束
- 协议变更必须先更新 `contracts/proto/video_processing.proto` 并重新生成代码。
- 架构演进必须同步更新：
  - `docs/architecture/overview.md`
  - `docs/architecture/repository-map.md`
  - `docs/architecture/upgrade-log.md`
- 运行期排障优先使用：
  - `docs/architecture/error-fixes.md`
  - `docs/runbooks/README_preprocess_dependency_fix.md`

## 10. 2026-02-17 语义分割与 Phase2B 分组模型补充
- Unit/Group 双层模型：
  - Unit：只由知识类型 `k` 与时间连续性决定，要求 `k` 纯度，不允许跨类型混合。
  - Group：由核心论点（Core Argument）决定，作为 Phase2B 结构化输出与后续分析的基本聚合单元。
- Phase2A 输出约束：
  - 语义分割提示词与解析器统一采用 `knowledge_groups -> units[]` 输出模板。
  - Group 层字段：`group_name`、`reason`；Unit 层字段：`pids`、`k`、`m`、`title`。
  - 保留对旧格式 `semantic_units + group_name` 的兼容解析，用于历史缓存平滑迁移。
- VL 分析策略：
  - 在 `VLMaterialGenerator.generate()` 入口前置过滤 `k=0 (abstract)` 单元，仅保留 process/concrete 进入视觉链路。
  - 目标：减少无效视觉分析干扰与 token 消耗。
- Phase2B 输出模型：
  - 最终 JSON 统一为 `knowledge_groups[]`，每个 group 内包含 `group_name/reason/units[]`。
  - Markdown 固定两级：`## group_name`（一级）+ `### unit.title`（二级）。
  - 移除“按 LLM 再划分层级”的主路径依赖，避免重复分层与结构漂移。

## 11. 2026-02-17 Grouped 落盘与复用读取补充
- `semantic_units_phase2a.json` 统一采用 `knowledge_groups[]`，group 承载 `group_name/reason`，unit 不再重复组信息。
- 服务侧复用链路支持 grouped/legacy 双读；当回写 `material_requests/instructional_steps/_vl_route_override` 时，保持输入文件原始结构不变。
- 该策略确保 Phase2B、VL、RPC 内存透传链路在结构上保持一致，避免“写回后结构退化”。

## 12. 2026-02-17 Grouped 结构兜底一致性补充
- 语义分割 fallback 提示词与主提示词保持同一协议：`knowledge_groups -> units[]`，避免 prompt 文件不可用时协议回退。
- RPC 物化路径（`semantic_units_from_rpc_*.json`）统一落盘 grouped 结构，确保“展示形态”与主链路一致。
- 回写兜底分支在索引缺失时也重建 grouped，不再退化为扁平 `semantic_units`。

