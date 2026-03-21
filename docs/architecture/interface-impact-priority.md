# 高影响接口优先级清单

更新日期：2026-03-15  
范围：`services/java-orchestrator` 对外 HTTP 接口  
目标：识别当前对系统性能和业务影响最大的接口集合，并形成可维护的权责文档。

## 1. 判定口径

### 1.1 第一性原理
- 接口影响力 = `调用量` × `单次成本` × `业务关键性`。
- 只看“是否关键”会漏掉高频小接口带来的累计成本。
- 只看“是否高频”会漏掉低频但极昂贵、且未来可能放量的接口。

### 1.2 当前证据口径
- 已验证事实：
  - Java 侧 controller 边界已经稳定，见 `docs/architecture/overview.md`。
  - Web/PWA 的主要触发逻辑集中在 `services/java-orchestrator/src/main/resources/static/index.html` 与 `static/lib/*.js`，因此可以直接还原调用链和放大机制。
  - 阅读遥测已有真实落盘，见 `services/java-orchestrator/var/telemetry/mobile_reader_telemetry.ndjson`、`mobile_reader_logic_pool.ndjson`、`mobile_reader_cold.ndjson`。
- 口径限制：
  - 当前仓库快照中没有发现可直接用于接口级 Top N 统计的 HTTP access log、Micrometer counter 或 Tomcat accesslog。
  - 因此本文中的 Top 10 排名是“基于仓库内可验证调用机制的热度排序”，不是线上总请求量快照。

### 1.3 当前可直接复用的杠杆
- 控制器职责已按域拆分：
  - `MobileMarkdownController` 承担任务列表、提交、上传、阅读、锚点、导出主链路。
  - `VideoProcessingController` 承担通用任务与 `video-info` 探测。
  - `MobileCardController` 承担概念卡片、候选标题和 AI 建议。
  - `TelemetryIngestController` 承担阅读遥测统一入口。
  - `MobileAppUpdateController` 承担 Android 更新。
- 前端入口集中：
  - `index.html` 已经包含任务列表、上传、阅读、导出、任务提交的主调逻辑。
  - `mobile-anchor-panel.js` 已经暴露锚点读写和 5 秒增量同步机制。
  - `mobile-concept-cards.js` 已经暴露标题加载、候选筛选、AI 建议调用点。
- 调用放大器已在代码里显式存在：
  - 任务列表增量同步：健康 `180s`、恢复 `25s`。
  - 正文编辑后台同步：`140ms` debounce。
  - 阅读位置保存：`420ms` debounce。
  - 锚点本地笔记增量同步：`5000ms` 固定周期。
  - 文件分片上传：`2MB` 每片。

## 2. 调用热度 Top 10 接口

| 排名 | 接口 | 当前主调方/触发器 | 调用量放大公式 | 为什么进入 Top 10 | 当前权责 |
| --- | --- | --- | --- | --- | --- |
| 1 | `POST /api/mobile/tasks/upload/chunk` | `index.html` 上传流程 `uploadMaterialFileInChunks(...)` | `ceil(file_size / 2MB)` | 每个大文件都会被切成多次请求，直接放大带宽、磁盘写入、multipart 解析与上传时延；这是最典型的“单业务动作裂变为 N 次请求”的接口。 | `MobileMarkdownController` 负责接入；`FileTransferService` 负责分片落盘和恢复。 |
| 2 | `GET /api/mobile/tasks/changes` | 任务列表增量对账、WebSocket 恢复补偿、手动刷新 | `活跃会话时长 / 25s~180s + reconnect + manual refresh` | 这是首页/任务列表的持续热路径；即使单次成本不极高，长时间在线会稳定施压控制平面。 | `MobileMarkdownController` 负责增量拉取；前端 `taskListRefreshPolicy` 负责节流与恢复。 |
| 3 | `PUT /api/mobile/tasks/{taskId}/meta` | 段落收藏、评论、锚点、删除态等后台保存 | `编辑停顿次数`，最短 debounce `140ms` | 写入频次高于直觉，且直接影响阅读编辑一致性；如果没有治理，容易形成高 QPS 小写请求风暴。 | `MobileMarkdownController` 负责元数据写回；底层真源是任务目录内的 meta 文件。 |
| 4 | `GET /api/mobile/tasks` | 首页初次进入、强制 resync、列表重载 | `page_open + hard_resync` | 这是首页首屏主入口，承担完整任务快照加载，payload 比 `tasks/changes` 更重。 | `MobileMarkdownController` 负责任务列表快照输出。 |
| 5 | `GET /api/mobile/tasks/{taskId}/markdown` | 打开任务正文、切换主笔记、正文重载 | `task_open + note_switch` | 单次返回体大，是阅读体验的主时延来源；一旦正文变长，会直接影响首屏内容到达。 | `MobileMarkdownController` 负责正文读取；存储真源在任务目录。 |
| 6 | `GET /api/mobile/tasks/{taskId}/meta` | 打开任务、实时状态刷新、锚点面板加载 | `task_open + live_refresh + anchor_refresh` | 虽然单次小于正文，但与正文几乎成对出现，并且被阅读页、锚点面板、实时刷新共同复用。 | `MobileMarkdownController` 负责元数据读取与任务标题透出。 |
| 7 | `POST /api/mobile/tasks/submit` | 普通提交、书籍 leaf 拆分提交 | `submit_count + leaf_task_count` | 这是任务受理漏斗入口；书籍拆叶模式下会从“1 次用户动作”放大成“多个 submit 请求”。 | `MobileMarkdownController` 负责移动端提交；下游进入 `TaskQueueManager`。 |
| 8 | `GET/POST /api/mobile/video-info` | URL/文件/书籍预探测、提交前校验 | `probe_attempt_count` | 虽然不一定是最高 QPS，但探测逻辑成本高，且直接决定提交转化率，是高成本前置接口。 | `VideoProcessingController` 负责入口；下游走 probe/service 能力。 |
| 9 | `POST /api/mobile/tasks/{taskId}/anchors/{anchorId}/sync` | 锚点本地笔记增量同步 | `dirty_edit_duration / 5s` | 当前由 `mobile-anchor-panel.js` 固定 5 秒同步一次，只要存在持续编辑就会稳定产生写请求。 | `MobileMarkdownController` 负责锚点同步；锚点笔记属于阅读增强链路。 |
| 10 | `GET /api/mobile/tasks/{taskId}/asset` | 正文图片/媒体资源按需加载 | `rendered_asset_count` | 富文本、截图、书籍图像越多，请求数越多；这是阅读面最典型的“内容规模驱动流量”接口。 | `MobileMarkdownController` 负责资源流式读取。 |

## 3. 紧随其后的高热伴随接口

这些接口没有进入 Top 10，但与高热链路强绑定，应当跟随 Top 10 一起治理。

| 接口 | 原因 | 当前权责 |
| --- | --- | --- |
| `POST /api/mobile/tasks/upload/reuse-check` | 每次上传前必经，虽然单次调用数不高，但位于上传漏斗最前面。 | `MobileMarkdownController` |
| `GET /api/mobile/tasks/upload/chunk/status` | 每个可恢复上传至少检查一次；断点续传越常见，价值越高。 | `MobileMarkdownController` |
| `POST /api/mobile/tasks/upload/chunk/complete` | 每个分片上传链路的汇总提交点。 | `MobileMarkdownController` |
| `GET /api/mobile/tasks/{taskId}/markdown/by-path` | 侧边笔记、wikilink、挂载笔记切换时会频繁使用。 | `MobileMarkdownController` |
| `GET /api/mobile/cards/titles` | `index.html` 和 `mobile-concept-cards.js` 都会预取，是概念高亮和 wikilink 的基础索引。 | `MobileCardController` |

## 4. 业务重要性补充接口

以下接口当前不一定进入“调用热度 Top 10”，但从业务优先级、首页承载潜力、推送潜力和成本爆发风险看，必须提前纳入重点清单。

| 接口 | 为什么必须提前纳入 | 未来可能的起量场景 | 当前权责 |
| --- | --- | --- | --- |
| `POST /api/telemetry/ingest` | 已有真实遥测落盘，是推荐、分群、推送、阅读洞察的事实源入口。 | 首页个性化、阅读推送、用户分层、画像生成。 | `TelemetryIngestController` 负责接入；下游写入 `var/telemetry/*.ndjson`。 |
| `POST /api/mobile/cards/ai-advice` | 单次调用成本高，直接连接 LLM；一旦从“手动触发”升级为“默认展示”，成本曲线会立刻抬升。 | 首页摘要卡、阅读中实时建议、批量推送文案生成。 | `MobileCardController` |
| `POST /api/mobile/cards/titles/candidates` | 现在主要用于上下文候选高亮，未来如果首页/正文默认开启概念联想，会从低频变高频。 | 首屏概念推荐、阅读热词联想、搜索联动。 | `MobileCardController` |
| `GET /api/mobile/app/update/check` | 现在只在 Android 更新链路触发，但一旦 Android DAU 放大，它会成为每次冷启动的固定税。 | App 冷启动、灰度更新、强提醒升级。 | `MobileAppUpdateController` |
| `GET /api/mobile/tasks/{taskId}/export` / `GET /api/mobile/tasks/{taskId}/export/files` | 当前偏工具型，但如果“导出/分享”成为留存或传播动作，会从低频辅助接口升级为主链路。 | 首页分享、批量导出、社群转发、知识包分发。 | `MobileMarkdownController` |

## 5. 接口分组与权责归属

### 5.1 P0：现在线上最该盯住的接口组
- 上传受理组：
  - `POST /api/mobile/tasks/upload/reuse-check`
  - `GET /api/mobile/tasks/upload/chunk/status`
  - `POST /api/mobile/tasks/upload/chunk`
  - `POST /api/mobile/tasks/upload/chunk/complete`
  - `POST /api/mobile/tasks/submit`
- 阅读主链路组：
  - `GET /api/mobile/tasks`
  - `GET /api/mobile/tasks/changes`
  - `GET /api/mobile/tasks/{taskId}/markdown`
  - `GET /api/mobile/tasks/{taskId}/meta`
  - `PUT /api/mobile/tasks/{taskId}/meta`
  - `GET /api/mobile/tasks/{taskId}/asset`
- 探测入口组：
  - `GET/POST /api/mobile/video-info`

### 5.2 P1：当前量级未必最高，但未来极容易放大的接口组
- 阅读智能化组：
  - `POST /api/telemetry/ingest`
  - `GET /api/mobile/cards/titles`
  - `POST /api/mobile/cards/titles/candidates`
  - `POST /api/mobile/cards/ai-advice`
- 锚点深编辑组：
  - `POST /api/mobile/tasks/{taskId}/anchors/{anchorId}/sync`
  - `GET /api/mobile/tasks/{taskId}/anchors/{anchorId}/mounted`
  - `POST /api/mobile/tasks/{taskId}/anchors/{anchorId}/mount`
- Android 增长组：
  - `GET /api/mobile/app/update/check`

## 6. 立刻建议补齐的监控

- 监控维度必须统一到 `method + normalized_path + status + latency + response_bytes + caller_surface`。
- 先补这 10 个接口的真实 counter，再做下一轮 Top N 重排，避免长期依赖静态推断。
- 对以下接口必须单独打出请求放大器：
  - `upload/chunk`：`file_size`、`chunk_count`、`resume_hit`
  - `tasks/changes`：`ws_healthy`、`resync_required`
  - `tasks/{taskId}/meta`：`write_reason`、`path`
  - `video-info`：`input_kind`、`probe_result`
  - `asset`：`asset_type`、`asset_size`
  - `cards/ai-advice`：`model`、`token_estimate`

## 7. 结论

- 当前最值得优先治理的不是单个接口，而是三条接口簇：
  - 上传链路：它把单次业务动作放大成大量分片请求。
  - 任务列表链路：它决定首页实时性与控制平面常驻压力。
  - 阅读编辑链路：它决定正文加载成本和高频元数据写回成本。
- 如果要只选第一批治理目标，建议先盯住这 6 个接口：
  - `POST /api/mobile/tasks/upload/chunk`
  - `GET /api/mobile/tasks/changes`
  - `PUT /api/mobile/tasks/{taskId}/meta`
  - `GET /api/mobile/tasks`
  - `GET /api/mobile/tasks/{taskId}/markdown`
  - `GET/POST /api/mobile/video-info`
