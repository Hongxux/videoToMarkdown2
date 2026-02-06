# 架构升级记录

> 目的：记录系统架构升级的背景、关键决策与复用经验，便于复盘与迁移。

## 2026-02-06 VL 素材生成模块（Qwen3-VL-Plus）
- 日期：2026-02-06
- 版本/分支/提交：未记录
- 触发背景与问题：原有 GenerateMaterialRequests 依赖 CV 帧分析 + LLM 分类，对视频内容的理解有限；需要更直接的视频语义理解以生成更精准的截图/片段请求。
- 改动范围（模块/接口/数据）：
  - 新增 `MVP_Module2_HEANCING/module2_content_enhancement/vl_video_analyzer.py`：Qwen3-VL-Plus API 客户端
  - 新增 `MVP_Module2_HEANCING/module2_content_enhancement/vl_material_generator.py`：VL 素材生成编排器
  - 修改 `module2_config.yaml`：添加 `vl_material_generation` 配置节
  - 修改 `python_grpc_server.py`：在 `GenerateMaterialRequests` 中集成 VL 流程
- 关键决策与理由：
  - 可开关设计：通过 `vl_material_generation.enabled` 控制是否启用 VL 分析，便于 A/B 测试和回退
  - 讲解型仅截图：知识类型为"讲解型"时不截取视频片段（无视觉操作价值），但仍截取截图
  - 自动回退：VL 分析失败或配置未启用时，自动回退到原有 RichTextPipeline 流程
  - 时间戳转换：VL 返回的相对时间戳（片段内）自动转换为绝对时间戳（原视频）
  - 输入策略自适应：为兼容 DashScope 的 data-uri 单项 10MB 限制，按“data-uri(小片段) → DashScope File.upload 临时 URL（可选）→ 关键帧 image_url 降级”自动选择输入，降低 400 风险
  - 输出约束与容错解析：提示词尾追加 JSON 硬性约束；解析端支持括号配对提取/去尾随逗号/修复 `key_evidence` 多字符串模式/字段名漂移，并在解析失败时以更严格约束重试
- 兼容性影响：新增配置节 `vl_material_generation`；默认 `enabled: false`，不影响现有流程
- 风险与回滚方案：设置 `vl_material_generation.enabled: false` 即可完全禁用 VL 模块
- 验证方式与结果：单元测试通过（配置加载、时间戳转换、JSON 解析、VL API 调用）
- 可复用经验：对 VL 类能力优先做"可开关 + 自动回退 + 时间戳归一"设计，降低集成风险

## 记录字段
- 日期
- 版本/分支/提交
- 触发背景与问题
- 改动范围（模块/接口/数据）
- 关键决策与理由
- 兼容性影响
- 风险与回滚方案
- 验证方式与结果
- 可复用经验

## 2026-02-06 主链路 CV 内存优化与缓存命中率统计
- 日期：2026-02-06
- 版本/分支/提交：未记录
- 触发背景与问题：多进程 CV 占用偏高（OpenCV 运行时、float64 与数组拷贝导致单进程内存上涨）；缓存命中率缺少统一统计口径，难以判断“哪些缓存值得保留”。
- 改动范围（模块/接口/数据）：`cv_worker.py`（OpenCV OpenCL/优化开关、零拷贝读取、数组释放）；`python_grpc_server.py`（任务级缓存统计与最终落盘）；`MVP_Module2_HEANCING/module2_content_enhancement/cache_metrics.py`（统一收敛器）；`cv_runtime_config.py`（CV 精度配置）；`screenshot_selector.py`/`visual_element_detection_helpers.py`（float32 默认）；`visual_feature_extractor.py`/`cv_knowledge_validator.py`/`semantic_feature_extractor.py`/`vision_ai_client.py`/`llm_client.py`/`resource_manager.py`（缓存命中率打点）；文档更新。
- 关键决策与理由：
  - 默认 float32 + 可回退：兼顾内存与精度，可通过 `CV_FLOAT_DTYPE=64` 回退。
  - 禁用 OpenCL 运行时与可选优化路径：减少 OpenCV 运行时常驻内存。
  - SharedMemory 零拷贝：默认视图读取，注入缓存时显式 copy，降低峰值占用。
  - 统一缓存统计：模块内集中 hit/miss 口径，最终落盘便于分析。
- 兼容性影响：新增环境变量 `CV_FLOAT_DTYPE`、`CV_DISABLE_OPENCV_OPT`、`MODULE2_CACHE_METRICS_ENABLE`、`MODULE2_CACHE_METRICS_RESET_ON_TASK`；新增落盘文件 `outputDir/intermediates/cache_metrics.json`；默认精度调整可能引入轻微数值差异。
- 风险与回滚方案：如精度/性能异常，可设置 `CV_FLOAT_DTYPE=64` 或关闭 `CV_DISABLE_OPENCV_OPT`；如统计影响性能，可 `MODULE2_CACHE_METRICS_ENABLE=0`；必要时删除 `cache_metrics.json` 以回退。
- 验证方式与结果：跑一条含 `ValidateCVBatch + AssembleRichText` 的主链路；检查 `cache_metrics.json` 是否生成；切换 `CV_FLOAT_DTYPE=64`/`CV_DISABLE_OPENCV_OPT=1` 验证可控回退。
- 可复用经验：对多进程 CV 链路优先做“运行时裁剪 + 精度下调 + 零拷贝 + 统计闭环”，再做算法级优化。

## 2026-02-04 输出目录统一与文档补齐
- 日期：2026-02-04
- 版本/分支/提交：未记录
- 触发背景与问题：主链路输出目录存在分散风险，跨阶段产物难以统一管理。
- 改动范围（模块/接口/数据）：架构文档；Java/Python 输出目录归一逻辑；本地路径任务输出规则。
- 关键决策与理由：统一 `outputDir` 到 `storage/{url_hash}`，保证所有阶段产物同域聚合。
- 兼容性影响：外部传入 `outputDir` 的行为需要对齐新规则（URL 任务已强制统一，本地路径需补齐）。
- 风险与回滚方案：若统一规则引发路径依赖问题，短期可保留旧路径作为软链接/别名。
- 验证方式与结果：待根据实际任务跑通主链路验证。
- 可复用经验：输出目录的约束需在编排层统一治理，避免分散实现。

## 2026-02-04 Phase2 流式门闸与阶段缓存
- 日期：2026-02-04
- 版本/分支/提交：未记录
- 触发背景与问题：Phase2 延迟高，CV/LLM 阶段存在整段阻塞；重复任务缺少有效复用。
- 改动范围（模块/接口/数据）：`python_grpc_server.py`（ValidateCVBatch 流式门闸）；Java 编排（CV/LLM 缓存判断与复用）；`ConcreteKnowledgeValidator`（VisionAI 持久缓存与去重）。
- 关键决策与理由：CV 改为按批次流式回传；缓存签名基于 `url_hash + 关键配置 + 输入签名`；intermediates 命中则跳过 CV 与 LLM。
- 兼容性影响：旧缓存文件不含签名会被忽略，需重跑一次生成新缓存。
- 风险与回滚方案：如出现误命中可删除 `intermediates/*cache*` 或暂时禁用缓存逻辑恢复旧流程。
- 验证方式与结果：待通过单任务回归验证流式回传与缓存命中路径。
- 可复用经验：阶段级缓存必须配套签名校验，避免跨版本污染。

## 2026-02-04 初始化
- 本文件创建，等待首条升级记录。


## 2026-02-04 本地视频归档与素材请求合并
- 日期：2026-02-04
- 版本/分支/提交：未记录
- 触发背景与问题：本地路径任务未统一归档到 storage，Phase2A 初始素材请求在后续阶段可能被覆盖。
- 改动范围（模块/接口/数据）：Java 编排与 Python gRPC 服务；本地视频归档逻辑；FFmpeg 请求合并。
- 关键决策与理由：本地视频复制/硬链接到 storage/{hash} 以保证同域；合并 Phase2A 与生成请求以保留上游召回。
- 兼容性影响：增加一次 I/O；素材数量可能增加；请求去重依赖 id 或时间范围。
- 风险与回滚方案：复制/链接失败则回退原路径；必要时可暂时关闭请求合并逻辑。
- 验证方式与结果：待用本地视频与 URL 各跑一条主链路验证。
- 可复用经验：归档与合并应在编排层集中治理，避免分散实现。

## 2026-02-04 素材生成使用 action_units 知识类型
- 日期：2026-02-04
- 版本/分支/提交：未记录
- 触发背景与问题：Java 与 Python 侧重复分类导致结果不一致，clip 生成与语义标注冲突。
- 改动范围（模块/接口/数据）：python_grpc_server.py（GenerateMaterialRequests）；素材请求写回逻辑。
- 关键决策与理由：以 action_units 的 knowledge_type 为唯一来源，取消二次分类；合并逻辑仍保留。
- 兼容性影响：素材生成结果与 Java 分类一致；若 action_units 缺少知识类型，则使用 unit 级兜底。
- 风险与回滚方案：如需恢复可重新启用 LLM 分类；保留过滤逻辑与阈值。
- 验证方式与结果：待重新跑主链路核对 clip 与 knowledge_type 对齐。
- 可复用经验：跨语言分类结果应单点决策，避免重复推断。


## 2026-02-05 性能优化汇总（Phase2/CV/Vision/LLM）
- 背景：Phase2/CV 读帧耗时长、CPU 利用率低、Vision API 事件循环异常，单任务时延偏高。
- 优化目标：降低单任务时延、提高 IO/Compute 重叠、减少重复调用、提升可观测性。
- 关键改动：
  - CV 流式门闸：ValidateCVBatch 改为 chunk 化处理，IO/Compute 重叠，边计算边回传结果。
  - 动态 chunk：基于 unit 数量强制最少 chunk（目标 >= 5）以提高重叠与时延表现。
  - CV batch 上限下调：限制 batch 上限以避免单 chunk，提升流式效果。
  - 粗采样兜底：coarse 采样不足时回退到 3 点采样（start/mid/end），避免 Insufficient coarse frames。
  - 读帧剖析日志：在 _batch_read_frames_to_shm/_batch_read_coarse_frames_to_shm 输出 open/seek/read/shm/total 分阶段耗时。
  - 顺序读+采样：
    - 做法：先收集目标 frame_idx（去重/排序），只 seek 到最小帧；顺序 read 到最大帧，仅在命中目标帧时写入 shm，其余帧直接丢弃。
    - 机制收益：避免“每帧随机 seek”导致的 GOP 回溯解码，解码变为线性扫描，减少重复解码与磁盘随机访问。
    - 为什么提升明显：随机 seek 在 H.264/H.265 下需要从关键帧回溯解码到目标帧；顺序读只解码一次区间，CPU 利用率更高、总耗时显著下降。
  - 并行解码：
    - 做法：当范围较大/采样较多时，将帧区间切分为多段，使用多线程并行解码；每段独立 VideoCapture 顺序读。
    - 机制收益：把线性解码拆成多段并行，缩短总耗时，并提高 CPU 利用率。
    - 备注：并行度按解码跨度(range_span)自动升到 2~4 路，避免“目标帧很少但跨度很大”时仍退化为单路解码。
  - 持续喂任务流水线：
    - 做法：移除 chunk 屏障，改为“全局 pending in-flight 队列 + 背压节流”的流水线调度：
      - 统一任务流：将 CV 与 coarse-fine 合并成统一任务列表（按 start_sec 排序）后再按任务数分 chunk。
      - 持续喂入：每个 chunk 的 IO 完成后立刻提交任务到 ProcessPool，并将 wrapper task 放入 pending 集合；不再等待该 chunk 全部完成才进入下一批。
      - 只等首个完成：使用 asyncio.wait(..., FIRST_COMPLETED) 仅等待至少一个任务完成即立刻 yield 结果，持续释放 pending，避免 chunk 级 barrier。
      - 背压节流：设置 max_inflight（默认≈cv_worker_count*2），当 pending 达到上限时才 drain_completed，防止任务堆积导致内存爆。
      - 最终 drain：所有 chunk 喂完后继续 drain pending 直到清空，保证结果完整输出。
    - 机制收益：
      - 避免长尾任务把整个 chunk 卡死，降低 worker 空闲概率。
      - 让 CPU 更均匀被利用（worker 持续有活干），同时保留 IO/Compute 重叠与流式回传。
    - 可观测性日志：
      - Task unify / Tail merge / Streaming gate pipeline(inflight=...) / Feed chunk / Inflight throttle / completed 统计。
  - 任务统一与尾部合并：
    - 做法：将 cv/cf 任务合并为统一列表按任务数分 chunk；尾部 chunk 过小则与上一批合并（tail-merge）。
    - 机制收益：避免尾部小批导致大量 worker 空闲，提升并行度稳定性。
  - 禁用嵌套并行：
    - 做法：在 CV worker 内设置 OMP/MKL/OPENBLAS/NUMEXPR/VECLIB 线程为 1，并设置 cv2.setNumThreads(1)。
    - 机制收益：避免单进程内部多线程抢占核，确保多进程并行更均衡。
  - Vision API 等待剖析：记录 wait/http/avg_wait，明确 API 等待瓶颈。
  - Vision 事件循环修复：AsyncClient 绑定事件循环，检测 loop 关闭/切换后重建，避免 Event loop is closed。
  - CV/Vision 缓存：url_hash+配置签名复用 Vision 结果，pHash 去重与持久化缓存。
  - 阶段级复用：intermediates 存在且输入一致时跳过 CV/LLM。
  - Java 侧 batch 上调：CVValidationOrchestrator batchSize 提升以减少往返开销（总时长优先）。
- 影响范围：Phase2A/2B、CV 验证、Vision AI 过滤、Java 编排。
- 代价/权衡：顺序读会读到非目标帧，并行解码增加磁盘压力；batch 下调可能影响吞吐但改善时延。
- 验证方式：
  - 观察 ValidateCVBatch 日志是否出现多 chunk 与 IO/Compute 分阶段耗时。
  - 观察 Vision API timing/avg_wait 是否正常、Event loop is closed 是否消失。
- 相关文件：
  - python_grpc_server.py
  - MVP_Module2_HEANCING/module2_content_enhancement/vision_ai_client.py
  - MVP_Module2_HEANCING/module2_content_enhancement/concrete_knowledge_validator.py
  - MVP_Module2_HEANCING/enterprise_services/java_orchestrator/src/main/java/com/mvp/module2/fusion/service/CVValidationOrchestrator.java
  - MVP_Module2_HEANCING/enterprise_services/java_orchestrator/src/main/java/com/mvp/module2/fusion/service/KnowledgeClassificationOrchestrator.java
  - MVP_Module2_HEANCING/enterprise_services/java_orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java

## 2026-02-05 Module2 MarkdownEnhancer LLM 调用合并（降调用次数）
- 日期：2026-02-05
- 版本/分支/提交：未记录
- 触发背景与问题：Stage5（MarkdownEnhancer）对每个语义单元执行“正文增强 + 逻辑结构化”两次 LLM 调用（串行依赖），在 DeepSeek 调用成为吞吐瓶颈时会放大网络往返与限流排队成本。
- 改动范围（模块/接口/数据）：`MVP_Module2_HEANCING/module2_content_enhancement/markdown_enhancer.py`；新增合并提示词与合并调用逻辑；保持原两次调用路径作为回退。
- 关键决策与理由：将两个强依赖步骤合并为一次 `complete_json` 请求，单次返回 `{enhanced_body, structured_content}`，把“2 次请求调度开销”压缩为“1 次”，并在失败时自动回退以保证稳定性。
- 兼容性影响：默认开启合并调用；如需完全回退旧行为，可设置环境变量 `MODULE2_MARKDOWN_ENHANCER_COMBINE_CALLS=0`。
- 风险与回滚方案：合并提示词更长可能导致 JSON 输出不稳定或触发 token 上限；已内置异常捕获并回退到两次调用；紧急回滚直接关闭合并开关。
- 验证方式与结果：待使用一条 Phase2 任务回归观察日志中单 unit 的 LLM 请求次数是否从 2 降为 1，且产出的 Markdown 结构可被 Obsidian 正常渲染。
- 可复用经验：对“强依赖的多步 LLM 流程”，优先尝试“单次结构化输出 + Feature Flag + 回退路径”，以稳定性换取可观的吞吐收益。

## 2026-02-05 Adaptive Action Envelope：动作素材截取闭环化
- 日期：2026-02-05
- 版本/分支/提交：未记录
- 触发背景与问题：动作单元往往只捕捉到“变化发生瞬间”，易错过定位/准备过程；断续多动作（连续点多处）会被切碎，且当前最终只保留第一个 clip，导致语义不完整。
- 改动范围（模块/接口/数据）：`MVP_Module2_HEANCING/module2_content_enhancement/rich_text_pipeline.py`（素材生成 `_generate_materials`；素材请求 `_collect_material_requests`）。
- 改动范围补充（下游集成）：`python_grpc_server.py`（GenerateMaterialRequests：clip 生成使用动作包络，而非原始 action 边界）。
- 关键决策与理由：
  - 引入 Adaptive Action Envelope（自适应动作包络）：仅对知识类型为 `实操/推演/环境配置(配置)` 的动作生效，优先保证“定位→执行→结果确认”闭环。
  - 短语义单元（<=20s）：直接取 `unit.start_sec ~ unit.end_sec`，并对起止做视频边界裁剪（模仿 `visual_feature_extractor.py` 的安全边界策略），避免切分破坏完整性。
  - 长语义单元：基于 `Union(Action, SentenceOverlappingAction)` 扩边（start -0.4s，end +1.0s），并强制 `end <= unit.end_sec`，暂不跨越下一个语义单元。
  - 修复素材生成数据链：Java 侧构造 MaterialInputs 时若缺少 action_units，则用 unit 级 `knowledge_type` 覆盖 CV actionType，避免动作知识类型断链；同时 gRPC 传递 action_id 不再写死 0，保证下游回写一致。
  - 修复 LLM 分类缓存读取断链：`modality_classification_cache.json` 按对象序列化为 camelCase（unitId/actionId/knowledgeType），旧 loader 按 snake_case 读取会“误命中缓存但字段为空”，导致 action_units.knowledge_type 缺失；现兼容两种命名并在无有效字段时忽略缓存强制重算。
  - 多动作融合：同一语义单元内将动作段合并阈值由 `<1.0s` 放宽到 `<5.0s`，降低“只截到其中一段”的概率。
  - 下游统一使用包络：GenerateMaterialRequests 直接用动作包络生成 clip（并在无字幕时保留动作边界），避免 Phase2A 跳过素材请求导致包络逻辑不生效。
- 兼容性影响：clip 时间范围可能变长；素材数量不变但单 clip 更可能覆盖多动作；下游按 `unit.end_sec` 截断后不再跨到下一单元画面。
- 兼容性影响补充：GenerateMaterialRequests 的 clip 起止将跟随包络策略（短单元整段、长单元扩边）。
- 风险与回滚方案：若 clip 过长导致耗时/体积上升，可下调扩边/合并阈值或回退到句子对齐策略；必要时恢复 `<1.0s` 合并阈值。
- 验证方式与结果：待用包含“多次点击/配置生效确认”的视频单元回归，检查 clip 是否覆盖准备与结果静止期，且不跨越下一个语义单元。
- 验证方式补充：对比 GenerateMaterialRequests 输出的 clip 起止与 action 原始区间，确认包络对 `实操/推演/配置` 生效且无字幕时不被拉到 0。
- 可复用经验：当链路最终只消费单个 clip 时，应在 clip 生成前显式做“语义单元完备性 + 动作包络”聚合，避免上游召回多段但下游只取其一。

## 2026-02-05 Stage1 输出 sentence_timestamps.json 统一路径
- 日期：2026-02-05
- 版本/分支/提交：未记录
- 触发背景与问题：Phase2A 未传入 sentence_timestamps 导致句子时间回退到索引映射，语义单元文本与时间戳错位。
- 改动范围（模块/接口/数据）：`python_grpc_server.py`（ProcessStage1/AnalyzeSemanticUnits 输出与读取路径）。
- 关键决策与理由：
  - Stage1 产物中将 `local_storage/sentence_timestamps.json` 复制到 `intermediates/sentence_timestamps.json`，确保下游统一读取路径。
  - AnalyzeSemanticUnits 优先使用请求传入的 `sentence_timestamps_path`，缺失则回退到 `intermediates` 或 `local_storage`。
  - 若 sentence_timestamps 缺失则 ProcessStage1 自动补跑至至少 step4，确保生成。
- 兼容性影响：下游可稳定获取 sentence_timestamps；旧路径依旧可回退读取。
- 风险与回滚方案：若复制失败则继续使用 local_storage 路径；必要时关闭复制逻辑回退旧行为。
- 验证方式与结果：重新跑 Stage1，确认响应返回 sentence_timestamps_path 且 Phase2A 对齐结果不再错位。
- 可复用经验：跨阶段关键对齐文件应以“统一路径 + 回退路径”策略管理，避免隐性缺失。

## 2026-02-05 Vision 限流与 LLM 分类并发探测
- 日期：2026-02-05
- 版本/分支/提交：未记录
- 触发背景与问题：Vision API 有 60 req/min 硬限流且存在排队抖动；_vision_validate_v3 频繁 asyncio.run 导致事件循环与连接池反复创建；ClassifyKnowledgeBatch 固定并发难以逼近吞吐上限。
- 改动范围（模块/接口/数据）：`MVP_Module2_HEANCING/module2_content_enhancement/vision_ai_client.py`（严格限流 + 后台事件循环桥接）；`MVP_Module2_HEANCING/module2_content_enhancement/concrete_knowledge_validator.py`（同步调用复用单一 loop）；`python_grpc_server.py`（LLM 分类并发探测）；`MVP_Module2_HEANCING/module2_content_enhancement/llm_client.py`（LLM 调度器：token 加权 permits + 资源外部 cap）；`MVP_Module2_HEANCING/analyze_action_units.py`（统一走 LLM 调度器）；`MVP_Module2_HEANCING/module2_content_enhancement/markdown_enhancer.py`（批量并发 + as_completed 流式处理）。
- 关键决策与理由：
  - Vision API 增加严格 60 req/min 匀速器，优先解决硬限流与 429 抖动。
  - _vision_validate_v3 通过后台事件循环 + run_coroutine_threadsafe 复用连接池与并发 limiter，避免反复建/销毁 loop。
  - ClassifyKnowledgeBatch 使用 AIMD 探测并发上限，成功率高则逐步增压，失败时回退。
  - LLM 调度器：按 token 估算（字符/4，最大 4k）映射为加权 permits（每 800 token=1 permit），避免大请求拖慢小请求；CPU/内存占用作为外部 cap 收敛有效容量，支持批量并发调用与 as_completed 流式消费结果（单任务时延优先）。
- 兼容性影响：Vision 调用被严格节流，峰值吞吐下降但时延更稳定；LLM 分类并发由固定值变为动态调整。
- 风险与回滚方案：如节流导致时延过长，可调高 rate_limit_per_minute 或回退限流逻辑；如并发探测引起波动，可恢复固定 Semaphore 或下调 max_limit。
- 验证方式与结果：运行包含 Vision 校验 + LLM 分类的任务，确认 Vision API timing 日志包含 rate_wait 且 429 降低；观察分类并发随负载变化且无异常。
- 可复用经验：外部硬限流服务应使用速率限制优先于并发控制；同步环境调用异步 API 应使用统一后台 loop 复用连接池。

## 2026-02-05 ClassifyKnowledgeBatch 跨 Unit 动态分块批处理（降 LLM 调用次数）
- 日期：2026-02-05
- 版本/分支/提交：未记录
- 触发背景与问题：ClassifyKnowledgeBatch 以“每个 unit 调一次 classify_batch”的方式触发大量 DeepSeek 请求，在 unit 数较多时网络往返与调度开销显著放大，成为整体瓶颈。
- 改动范围（模块/接口/数据）：`MVP_Module2_HEANCING/module2_content_enhancement/knowledge_classifier.py`（新增 `classify_units_batch`：跨 unit 合并请求 + token_budget 动态分块）；`python_grpc_server.py`（ClassifyKnowledgeBatch 优先走 multi-unit 批处理，保留旧路径作为兼容回退）。
- 关键决策与理由：
  - 以“actions[*].id=unit_id:action_id”作为稳定映射键，批量输出 JSON 数组即可回填到 protobuf。
  - 通过 token_budget + max_units_per_chunk 做装箱分块，避免单次 prompt 过大导致输出不稳定或超限。
  - 保留环境变量 `MODULE2_KC_MULTI_UNIT_ENABLED` 作为 Feature Flag，必要时一键回退旧 per-unit 并发实现。
  - 增加结果自检：若 `Batch Miss` 占比过高（`MODULE2_KC_MULTI_UNIT_FALLBACK_MISS_RATIO`，默认 0.4），触发回退到旧 per-unit 路径，避免“合并 prompt/解析不稳”导致整批默认值。
- 兼容性影响：输出字段保持不变（仍返回 KnowledgeClassificationResult 列表）；当 multi-unit 批处理异常或结果自检失败时会回退旧路径。
- 风险与回滚方案：若合并 prompt 引起分类质量下降或 JSON 解析不稳定，可设置 `MODULE2_KC_MULTI_UNIT_ENABLED=0` 立即回退；或下调 `MODULE2_KC_MULTI_TOKEN_BUDGET`/`MODULE2_KC_MULTI_MAX_UNITS_PER_CHUNK`/`MODULE2_KC_MULTI_FULL_TEXT_CHARS` 降低单次请求规模；必要时调低 `MODULE2_KC_MULTI_UNIT_FALLBACK_MISS_RATIO` 更激进回退。
- 验证方式与结果：待用 unit 数较多的视频回归，观察日志中 LLM 请求次数是否显著下降，且返回结果与旧实现对齐。
- 可复用经验：对“多 unit 独立分类”任务，优先将“批量合并 + 动态分块 + 稳定映射键 + Feature Flag 回退”作为默认工程化模板。

## 2026-02-05 RichTextPipeline 跨 Unit 预分类（减少 DeepSeek 调用）
- 日期：2026-02-05
- 版本/分支/提交：未记录
- 触发背景与问题：RichTextPipeline 在 `_generate_materials/_collect_material_requests` 中按 unit 调 `classify_batch`，当 unit 数较多时会产生大量 DeepSeek 请求，成为 Phase2B 素材生成的主要瓶颈；同时对已存在 `knowledge_type` 的动作重复分类会造成不必要的额外调用。
- 改动范围（模块/接口/数据）：`MVP_Module2_HEANCING/module2_content_enhancement/rich_text_pipeline.py`（新增 `_preclassify_action_segments_multi_unit`，在 `_generate_materials_parallel` 前批量预分类；并优先复用 action_segments 已有 `knowledge_type/classification`）；`MVP_Module2_HEANCING/module2_content_enhancement/rich_text_pipeline.py`（KnowledgeClassifier 注入 step2_path）。
- 关键决策与理由：
  - 复用 `KnowledgeClassifier.classify_units_batch` 的动态分块能力，将跨 unit 的分类请求合并，减少网络往返与调度开销。
  - 对 action_segments 已带 `knowledge_type/classification` 的场景直接回填 `classification`，避免重复 LLM 调用。
  - 批处理失败时仅记录告警，不阻断主流程；后续仍可回退到原 per-unit `classify_batch` 路径。
- 兼容性影响：默认行为不变（Feature Flag 仍由 `MODULE2_KC_MULTI_UNIT_ENABLED` 控制）；预分类只回填 `action_segments[*].classification` 字段，不改变对外接口。
- 风险与回滚方案：若批处理导致分类质量波动，可设置 `MODULE2_KC_MULTI_UNIT_ENABLED=0` 回退 per-unit；若只想保留“复用已有 knowledge_type”可继续使用上游 action_units 分类结果。
- 验证方式与结果：运行 `python -m pytest -q` 通过；在 unit 数较多的任务中观察 DeepSeek 请求次数下降（chunk 数明显小于 unit 数）。
- 可复用经验：在素材生成类 pipeline 中，将“预计算（分类）”从 per-unit 内循环上移到批处理层，可同时降低调用次数与整体时延。
