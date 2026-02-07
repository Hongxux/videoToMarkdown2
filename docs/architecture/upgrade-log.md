# 架构升级记录

> 目的：记录系统架构升级的背景、关键决策与复用经验，便于复盘与迁移。

## 2026-02-07 Phase2A/Assets 性能优化（仅并发与复用，不改正确率策略）
- 日期：2026-02-07
- 版本/分支/提交：未记录
- 触发背景与问题：
  - 现网耗时统计显示 `Phase2A` 与 `assets` 阶段为主瓶颈。
  - 用户要求仅进行“并发架构 + 资源复用 + 读帧提速”优化，明确不调整影响正确率的策略参数。
- 改动范围（模块/接口/数据）：
  - `MVP_Module2_HEANCING/enterprise_services/java_orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java`
    - 增加提取流水线启动逻辑 `startExtractionPipeline(...)`，将素材请求生成后立即启动提取异步任务。
    - `ExtractionRequests` 增加 `extractionFuture`，主流程在 `Phase2B` 前等待同一 future，避免重复提取。
  - `MVP_Module2_HEANCING/enterprise_services/java_orchestrator/src/main/java/com/mvp/module2/fusion/service/JavaCVFFmpegService.java`
    - `extractAllAsync` 改为生产者-消费者：截图消费与 clip 队列并发消费。
    - clip 提取由串行改为多 worker 并发（`resolveClipWorkerCount`），并采用 worker-local grabber 复用。
    - 新增 `extractSingleClipWithGrabber` / `extractConcatClipWithGrabber`，避免每段 clip 重复 `grabber.start()`。
  - `python_grpc_server.py`
    - `_batch_read_frames_for_screenshots` 改为“顺序读 + 并行解码”模式，复用 `_batch_read_frames_to_shm` 的高吞吐思路，减少随机 seek。
    - `AnalyzeSemanticUnits` 不再每次 `new VisualFeatureExtractor`，改为按 `video_path` 从 `GlobalResourceManager` 缓存复用。
    - `RichTextPipeline` 注入全局单例 `SemanticUnitSegmenter`，减少 Phase2A 热路径重复初始化。
    - `ReleaseCVResources` 增加 `cleanup_visual_extractors`，统一资源回收。
  - `MVP_Module2_HEANCING/module2_content_enhancement/__init__.py`
    - 导出 `SemanticUnitSegmenter`，供 gRPC 服务统一单例注入。
- 关键决策与理由：
  - 决策1：先建异步提取流水线，再在主流程等待 future。
    - 为什么：满足“边生成边提取”并减少编排层空转等待。
    - 权衡：增加 future 生命周期管理复杂度，但不改变素材语义与输出格式。
  - 决策2：截图保持顺序复用，clip 走并发 worker。
    - 为什么：截图是 seek 密集型，顺序读更稳；clip 是编码密集型，适合并发。
    - 权衡：clip 并发过高会引发编码资源竞争，因此 worker 数限制为 `min(cpu/2, 6)`。
  - 决策3：资源复用限定在“同进程 + 同视频路径”缓存。
    - 为什么：能显著减少重复初始化成本且不改变算法判定。
    - 权衡：缓存需要显式清理，因此在 `ReleaseCVResources` 扩展回收。
- 兼容性影响：
  - gRPC/REST 接口与数据协议未变。
  - 素材请求生成逻辑、知识分类逻辑、VL 路由策略未改。
- 风险与回滚方案：
  - 若并发提取出现资源争用，可把 clip worker 数收敛到 1（退化为原串行）。
  - 若发现缓存引起异常复用，可在 `ReleaseCVResources` 前强制清理，或暂时回退为每次新建。
  - 回滚路径：恢复 `VideoProcessingOrchestrator` 提取调用为 `extractAllSync`，并恢复 `JavaCVFFmpegService` 原串行 clip 提取分支。
- 验证方式与结果：
  - 代码级验证：
    - Python 语法检查通过：`python -m py_compile python_grpc_server.py`
    - Java 编译校验通过：`mvn -DskipTests compile`（`java_orchestrator` 模块）
  - 运行期建议：
    - 观察日志中 `Start extraction producer-consumer pipeline` 与 `Reusing in-flight extraction future` 是否出现。
    - 对比 `JavaCV extraction completed ... in xxxms` 与改造前基线。
- 可复用经验：
  - 对“生成请求 -> 重 I/O 执行”链路，可优先引入 producer-consumer future 桥接，而非一次性全量阻塞。
  - 对视频类任务，先保证“顺序读+并行解码”的吞吐模型，再叠加算法优化，通常收益更稳定。

## 2026-02-07 VL 前置 stable 剔除预处理（process 单元降 token）
- 日期：2026-02-07
- 版本/分支/提交：未记录
- 触发背景与问题：`process` 语义单元中存在长时间静态画面（如等待、静态配置页停留、口述无操作），直接送入 VL 会增加输入冗余与 token/时延成本。
- 改动范围（模块/接口/数据）：
  - `MVP_Module2_HEANCING/module2_content_enhancement/cv_knowledge_validator.py`：`detect_visual_states` 新增 `stable_only` 参数；在 `_merge_state_intervals` 增加 stable-only 早返回，跳过动作单元分类/边界细化/合并。
  - `MVP_Module2_HEANCING/module2_content_enhancement/vl_material_generator.py`：新增 VL 前预处理链路（stable 区间检测、核心段剔除、片段拼接、时间轴映射、上下文提示注入）。
  - `MVP_Module2_HEANCING/config/module2_config.yaml`：新增 `vl_material_generation.pre_vl_static_pruning` 配置节。
  - `MVP_Module2_HEANCING/module2_content_enhancement/tests/test_vl_pre_prune.py`：新增单元测试（区间剔除、时间映射、上下文提示）。
- 关键决策与理由：
  - 复用既有 CV 检测主链路：继续使用“动态采样 + ROI 检测 + 帧级状态判定 + 边缘动画检测 + 连续状态合并”，仅裁剪为 stable-only 输出，避免重复造轮子。
  - 仅作用于 `process` 单元：abstract/concrete 保持原行为，降低误伤风险。
  - 剔除策略采用“边缘保留”：stable `[s,e]` 不整段删除，只剔除 `[s+1s, e-1s]` 核心段，兼顾成本与语义衔接。
  - 拼接策略对齐现有实现：前置阶段生成的 `kept_segments` 与 JavaCV 现有 `segments` 拼接语义一致，便于端到端一致性。
  - 时间轴可逆映射：VL 在裁剪片段上的相对时间戳映射回原始时间轴，保障截图/切片定位正确。
  - 上下文补偿：向 VL 追加“完整文本上下文 + knowledge_topic + 保留/剔除区间”，降低因片段跳跃导致的误判。
- 兼容性影响：默认开启但有阈值保护（最小时长、最小剔除比例等）；预处理失败自动回退原片段，不影响主链路可用性。
- 风险与回滚方案：可通过 `vl_material_generation.pre_vl_static_pruning.enabled: false` 一键关闭；若担心语义损失可增大 `keep_edge_sec` 或提高 `min_removed_ratio`。
- 验证方式与结果：新增单元测试覆盖“区间剔除边界、时间映射、上下文提示关键字段”；已有 VL/CV 主流程不改接口。
- 可复用经验：在高成本模型前增加“可逆的结构化压缩层（区间剔除 + 时间映射 + 上下文补偿）”，通常比单纯调 prompt 更稳定地降低成本。

### 2026-02-07 补充：stable 剔除后、合并前的片段边界纠偏
- 触发背景：`pre_vl_static_pruning` 在剔除 stable 核心段后，`kept_segments` 直接拼接会出现口语句被截断（句首/句尾被切掉）的问题，影响 VL 理解与后续素材定位质量。
- 改动点：在 `MVP_Module2_HEANCING/module2_content_enhancement/vl_material_generator.py` 新增“合并前边界修正”链路，并在 `_prepare_pruned_clip_for_vl` 中对每个 `kept_segment` 执行修正后再进入 ffmpeg 拼接。
- 复用策略（对齐既有 VideoClipExtractor 思路）：
  - 语义完整性基线：基于 `0.3s` 停顿阈值分割口语句；起点优先匹配引导词（如“下面/接下来/首先”），终点优先匹配确认词（如“好了/这就是/总结”）。
  - 物理锚点重标定：起点严格使用语义句头（不向后追物理点）；终点取 `max(语义结束, MSE 跳变点)`，覆盖“先讲完后翻页”的真实教学节奏。
  - 口语语流缓冲：起点向前 `0.2s`、终点向后 `0.3s`，并与相邻片段做重叠保护。
- 数据来源与调用链：
  - 字幕来源复用 `output_dir/intermediates/step2_correction_output.json`（优先 `corrected_text`），避免新增上游接口依赖。
  - 调用时机固定在“stable 剔除完成后、`_concat_segments_with_ffmpeg` 前”，保证对 VL 输入片段生效，同时不影响 Java 侧最终拼接协议。
- 配置与默认值：新增 `vl_material_generation.pre_vl_boundary_refine`（默认启用），关键参数包括 `pause_threshold_sec=0.3`、`start_buffer_sec=0.2`、`end_buffer_sec=0.3`、`mse_scan_after_end_sec=3.0`、`mse_sample_fps=2.0`。
- 验证结果：`MVP_Module2_HEANCING/module2_content_enhancement/tests/test_vl_pre_prune.py` 新增边界纠偏相关测试，`python -m pytest MVP_Module2_HEANCING/module2_content_enhancement/tests/test_vl_pre_prune.py -q` 通过（9 passed）。

### 2026-02-07 补充：AnalyzeWithVL 路由先做 process 预处理，再按有效时长分流
- 触发背景：原路由先按原始语义单元时长（10s）决定 `process_short/process_long`，导致 stable 剔除与边界修正对路由决策无影响。
- 改动点：
  - `python_grpc_server.py`：`AnalyzeWithVL` 中对所有 `knowledge_type=process` 单元，先执行与 `process_long` 一致的 VL 前预处理（stable 剔除 + 合并前边界修正），再按“预处理后有效时长”分流。
  - 分流阈值由 `10s` 调整为 `20s`。
  - 路由日志新增 `process_preprocessed` 和 `threshold` 字段，便于追踪路由行为变化。
  - `MVP_Module2_HEANCING/module2_content_enhancement/vl_material_generator.py`：`_prepare_pruned_clip_for_vl` 新增 `force_preprocess`；新增 `preprocess_process_units_for_routing` 供路由层批量预处理并返回有效时长。
- 决策链变化：`process` 单元由“原始时长分流”升级为“预处理后有效时长分流”，与 VL 输入片段定义保持一致。
- 兼容性与回滚：若需回滚，仅需恢复路由分流逻辑为原始 `duration` + `10s` 阈值，或在路由调用中关闭 `force_preprocess`。

### 2026-02-07 补充：按任务 token 节省率可观测性
- 新增能力：在 `VLVideoAnalyzer` 捕获每次调用 `usage.prompt_tokens/completion_tokens/total_tokens`，并在 `VLMaterialGenerator` 汇总到任务级 `token_stats`。
- 节省率定义（估算）：
  - `actual_total`：真实 VL 返回 `total_tokens` 累加。
  - `baseline_est`：对被前置裁剪的单元，按“token/保留秒数 * 原始时长”线性回推；未裁剪单元 baseline=actual。
  - `saved_est = baseline_est - actual_total`，`saved_ratio = saved_est / baseline_est`。
- 日志出口：`AnalyzeWithVL` 增加任务级日志 `VL Token节省估算`，输出 actual/baseline/saved/saved_ratio/pruned_units。
- 取舍说明：当前基线是“工程估算值”而非 A/B 双跑实测值；优点是线上零额外成本，缺点是对“token 与时长非线性关系”存在误差。

### 2026-02-07 补充：按任务落盘 token 报表 JSON
- 新增能力：`AnalyzeWithVL` 在每个任务结束（success/fallback/exception/no_units/vl_disabled）都会落盘 token 报表。
- 落盘路径：
  - 任务文件：`{output_dir}/intermediates/vl_token_report_{task_id}.json`
  - 最新快照：`{output_dir}/intermediates/vl_token_report_latest.json`
- 报表字段（核心）：
  - `status`、`vl_enabled`、`used_fallback`、`error_msg`
  - `routing_stats`（abstract/concrete/process_short/process_long）
  - `token_stats`（actual/baseline_est/saved_est/saved_ratio_est/pruned_units 等）
  - `result_counts`（screenshots/clips/vl_units 等）
- 设计取舍：报表写入不阻断主流程，落盘失败仅告警，避免因观测能力影响主链路可用性。

### 2026-02-07 补充：延迟切割，仅切 VL 目标语义单元
- 触发背景：此前进入 `VLMaterialGenerator.generate(...)` 后会按传入列表切割，但历史目录/清单可能来自更大集合，导致切片包含未使用片段，造成不必要 I/O 与编码开销。
- 改动点：`vl_material_generator._split_video_by_semantic_units` 改为使用 `intermediates/semantic_units_vl_subset.json` 作为输入，并输出到 `semantic_unit_clips_vl/`。
- 行为变化：
  - 仅在确定需要 VL 分析（`AnalyzeWithVL` 已路由到 `vl_units`）后，才切割这些目标单元。
  - 复用检查改为“当前 VL 目标单元是否齐全”，不再依赖全量 `semantic_units_phase2a.json`。
- 收益：减少无效切片、降低前置耗时与磁盘占用，且与“只对 process_long 走 VL”路由策略严格对齐。

### 2026-02-07 补充：stable 剔除新增时长门槛（>3s）
- 规则变更：VL 前置静态段剔除时，stable 区间必须“原始长度严格大于 3 秒”才允许进入剔除。
- 细节：
  - `stable_duration <= 3.0s`：不剔除。
  - `stable_duration > 3.0s`：保留两侧 `keep_edge_sec`，剔除核心段。
- 配置项：`vl_material_generation.pre_vl_static_pruning.min_stable_interval_sec`（默认 `3.0`）。
- 原因：避免对短稳定段过度裁剪，降低语义丢失风险，同时保持对长静态冗余段的成本优化收益。

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
  - 截图时间点 CV 优化的“预读阶段”优先做 Union 预读：当请求多且窗口重叠明显时，一次性预读覆盖区间并写入 SHM，避免短片段高频 seek/read 造成“看起来没开多进程”的假象。
  - 流式+背压+可回退的截图优化流水线：以 chunk 作为 SHM 生命周期边界（chunk 专属 registry，避免跨 chunk 淘汰 unlink）；每个 chunk 预读完成后立刻提交到 ProcessPool，使用 `FIRST_COMPLETED` drain 实现背压节流；通过 `streaming_overlap_buffers` 支持 double-buffer overlap，并复用 gRPC 侧全局 ProcessPool（避免重复 spawn）。
  - 输入策略自适应：为兼容 DashScope 的 data-uri 单项 10MB 限制，按“data-uri(小片段) → DashScope File.upload 临时 URL（可选）→ 关键帧 image_url 降级”自动选择输入，降低 400 风险
  - 输出约束与容错解析：提示词尾追加 JSON 硬性约束；解析端支持括号配对提取/去尾随逗号/修复 `key_evidence` 多字符串模式/字段名漂移，并在解析失败时以更严格约束重试
- 兼容性影响：新增配置节 `vl_material_generation`；默认 `enabled: false`，不影响现有流程
- 风险与回滚方案：设置 `vl_material_generation.enabled: false` 即可完全禁用 VL 模块
- 验证方式与结果：单元测试通过（配置加载、时间戳转换、JSON 解析、VL API 调用）
- 可复用经验：对 VL 类能力优先做"可开关 + 自动回退 + 时间戳归一"设计，降低集成风险

## 2026-02-06 VL 路由层（knowledge_type + 时长）
- 日期：2026-02-06
- 版本/分支/提交：未记录
- 触发背景与问题：VL 成本高，abstract/concrete/短过程单元收益低；同时 Java 侧会因 used_fallback 触发 legacy 流程，导致重复计算。
- 改动范围（模块/接口/数据）：
  - `python_grpc_server.py`：`AnalyzeWithVL` 增加路由分流、CV 截图、短过程 clip、合并去重
  - `resource_manager.get_io_executor`：复用 IO 线程池（调用）
- 关键决策与理由：
  - `abstract` 全跳过；`concrete` 与 `process<=10s` 走 CV 截图；`process>10s` 才进入 VL。
  - `process<=10s` 直接输出整段 clip，避免 VL 成本。
  - 路由截图使用 `selector.select_screenshots_for_range_sync`，范围限定在语义单元；保留全部截图结果。
  - 只要路由+VL 成功返回即 `used_fallback=false`，避免 Java 侧回退；VL 失败仍保留 fallback。
  - IO 线程池 + 并发信号量限制，减少资源争抢并与 VL 任务异步重叠。
  - 合并去重：按 `semantic_unit_id + 时间 + label`/`knowledge_type` 统一归一。
- 兼容性影响：gRPC 接口不变；不写回 `semantic_units_phase2a.json` 的 `material_requests`；关闭 `vl_material_generation.enabled` 即回退旧路由。
- 风险与回滚方案：若路由策略不稳定可关闭 VL 或回退旧实现；截图质量异常可降低并发或切回 legacy。
- 验证方式与结果：混合类型单任务验证 `used_fallback=false`，检查截图/片段数量与路由统计一致。
- 可复用经验：在高成本模型前增加规则路由，用低成本 CV 优先覆盖，配套合并去重与可回滚策略。

## 2026-02-06 语义单元提示词优化 + mult_steps 驱动 VL 提示
- 日期：2026-02-06
- 版本/分支/提交：未记录
- 触发背景与问题：语义单元切分存在目标不完整、拆分边界不稳定；多步骤实操需要更强的 VL 去冗余与截图规则。
- 改动范围（模块/接口/数据）：
  - `MVP_Module2_HEANCING/module2_content_enhancement/semantic_unit_segmenter.py`：SYSTEM/USER 提示词更新
  - `MVP_Module2_HEANCING/module2_content_enhancement/vl_video_analyzer.py`：支持 extra_prompt
  - `MVP_Module2_HEANCING/module2_content_enhancement/vl_material_generator.py`：对 mult_steps=true 追加 extra_prompt
  - `python_grpc_server.py`：短过程 mult_steps=false 仅截图
- 关键决策与理由：
  - 拆分规则分层：完整目标边界为一般拆分依据；操作对象本质改变为强制拆分条件。
  - mult_steps=true 的长过程片段追加去冗余提示，避免无效时段占用 clip。
  - 截图要求明确为“每一步终态 + 关键记忆帧”，保证复现性。
- 兼容性影响：输出结构不变，仅提示词与路由策略更新。
- 风险与回滚方案：如切分或 VL 质量下降，可回退提示词或禁用 extra_prompt。
- 验证方式与结果：检查切分边界稳定性与多步片段裁剪效果；确认短过程 mult_steps=false 不生成 clip。
- 可复用经验：将语义规则明确为“目标边界 + 强制拆分”，并对多步流程引入冗余剔除约束。

## 2026-02-07 VL 多段拼接合并（process>10s + mult_steps=true）
- 日期：2026-02-07
- 版本/分支/提交：未记录
- 触发背景与问题：多步骤长过程单元在 VL 分析后可能产出多个片段，导致下游切片数量膨胀且内容分散，影响学习连续性。
- 改动范围（模块/接口/数据）：
  - `proto/video_processing.proto`：`ClipRequest` 新增 `segments` 字段；新增 `ClipSegment` 消息
  - `MVP_Module2_HEANCING/module2_content_enhancement/vl_material_generator.py`：多段 clip 合并与 segments 生成
  - `python_grpc_server.py`：gRPC 输出携带 segments
  - `MVP_Module2_HEANCING/enterprise_services/java_orchestrator`：ClipRequest DTO 与 JavaCVFFmpegService 支持 segments 拼接
- 关键决策与理由：
  - 仅对 `process>10s` 且 `mult_steps=true` 的单元做拼接合并，避免影响短过程与非多步片段。
  - 合并后只输出一个 clip，segments 表达多段拼接顺序，保证“去空白”的连续学习体验。
  - `segments` 为空时保持旧 start/end 单段逻辑，确保向后兼容。
- 兼容性影响：gRPC/Java DTO 新增 `segments` 字段；旧客户端不传 segments 时仍按单段切片。
- 风险与回滚方案：如拼接导致切片异常，可暂时不发送 segments 回退到单段切片逻辑。
- 验证方式与结果：人工构造多段 clip_requests 验证只生成一个 clip，输出时长≈各段时长之和。
- 可复用经验：对“多段候选”的场景，优先用结构化 segments 表达，再由下游统一拼接。

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

### 2026-02-07 VL tutorial mode refactor (step-only output)
- Date: 2026-02-07
- Background: long `process` units are used for tutorial replay; VL should focus on segmentation and instructional keyframes, not knowledge-type classification.
- Scope:
  - `MVP_Module2_HEANCING/module2_content_enhancement/vl_video_analyzer.py`: tutorial schema fixed to `step_id`, `step_description`, `clip_start_sec`, `clip_end_sec`, `instructional_keyframe_timestamp`.
  - `MVP_Module2_HEANCING/module2_content_enhancement/vl_material_generator.py`: keep per-step clips in tutorial mode, export unit-level step JSON + step clips + keyframes.
  - `MVP_Module2_HEANCING/config/module2_config.yaml`: tutorial and duration routing thresholds configurable.
- Key decisions:
  - Remove VL internal knowledge-type classification in tutorial path.
  - Standardize asset naming: `{unit_id}_step_{index}_{action_brief}.mp4` and `{unit_id}_step_{index}_{action_brief}_key.png`.
  - Persist one step JSON per semantic unit for Phase2B rich-text assembly.
- Validation:
  - `python -m pytest -q MVP_Module2_HEANCING/module2_content_enhancement/tests/test_vl_tutorial_flow.py MVP_Module2_HEANCING/module2_content_enhancement/tests/test_vl_pre_prune.py` passed (11 passed).

## 2026-02-07 RichText assembly refactor (teaching-first)
- Date: 2026-02-07
- Background: the old markdown assembly was generic and did not fully consume tutorial step assets or screenshot metadata for abstract/concrete placement.
- Scope:
  - `MVP_Module2_HEANCING/module2_content_enhancement/markdown_enhancer.py`:
    - add knowledge type normalization + tutorial unit detection + step loading (inline + `{unit_id}_steps.json` merge),
    - add abstract/concrete structured path with `img_id + img_description` placeholder flow `[IMG:img_id]` -> Obsidian embed,
    - add process multistep ordered rendering with keyframes and step clips.
  - `MVP_Module2_HEANCING/module2_content_enhancement/tests/test_markdown_enhancer_rich_text.py`: add rich-text unit tests.
- Key decisions:
  - Keep hierarchy mapping unchanged: still use DeepSeek level/parent_id output to map Obsidian headings.
  - Abstract/concrete uses model placeholder positioning plus local deterministic replacement.
  - Process multistep uses tutorial JSON assets directly; no extra clip merging at assembly stage.
- Compatibility:
  - If DeepSeek is unavailable, abstract/concrete falls back to source text + appended image embeds.
  - If tutorial step JSON is missing, process falls back to normal section rendering.
- Validation:
  - `python -m pytest -q MVP_Module2_HEANCING/module2_content_enhancement/tests/test_markdown_enhancer_rich_text.py` passed (3 passed).
  - tutorial regression tests still pass (11 passed) with the command above.
- Reuse note:
  - The "LLM placeholders + local asset binding" pattern is reusable across markdown assembly modules.

## 2026-02-07 RichText ?????????????????
- ???2026-02-07
- ????????abstract/concrete ????????? `materials.screenshot_items`????????????????????? markdown ???/???????
- ???????/??/????
  - `MVP_Module2_HEANCING/module2_content_enhancement/rich_text_pipeline.py`
    - ???????`_slugify_text`?`_build_unit_asset_prefix`?`_build_action_brief`?`_build_request_base_name`?`_resolve_asset_output_path`?
    - ?????????????? `assets/{unit_id}/...` ?????????? unit ??? action ???
    - ?????? `_apply_external_materials` ?????????ID + ?????? + unit_id/title ???????????? `assets/{unit_id}/` ??????
    - `clip/screenshot` ?? ID ???unit?? + action???????????????????
  - `MVP_Module2_HEANCING/module2_content_enhancement/tests/test_rich_text_pipeline_asset_naming.py`
    - ?????????ID?????/???????????? unit ????
- ????????
  - ??????????????????????????????
  - markdown ????????????????????????
- ????????
  - `python -m pytest -q MVP_Module2_HEANCING/module2_content_enhancement/tests/test_rich_text_pipeline_asset_naming.py`
  - `python -m pytest -q MVP_Module2_HEANCING/module2_content_enhancement/tests/test_rich_text_pipeline_asset_naming.py MVP_Module2_HEANCING/module2_content_enhancement/tests/test_markdown_enhancer_rich_text.py MVP_Module2_HEANCING/module2_content_enhancement/tests/test_vl_tutorial_flow.py MVP_Module2_HEANCING/module2_content_enhancement/tests/test_vl_pre_prune.py`
  - ???17 passed?

## 2026-02-08 process（非VL分步）链路补齐：截图校验 + 占位替换 + 尾部视频
- 日期：2026-02-08
- 触发背景与问题：`process` 语义单元在非 `tutorial_stepwise` 场景下，未统一走“结构化图片占位 -> 本地替换”的链路；截图校验仅覆盖 `abstract/concrete`，导致 process 输出在图文一致性上有缺口。
- 改动范围（模块/接口/数据）：
  - `MVP_Module2_HEANCING/module2_content_enhancement/rich_text_pipeline.py`
  - `MVP_Module2_HEANCING/module2_content_enhancement/markdown_enhancer.py`
  - `MVP_Module2_HEANCING/module2_content_enhancement/tests/test_markdown_enhancer_rich_text.py`
  - `MVP_Module2_HEANCING/module2_content_enhancement/tests/test_rich_text_pipeline_asset_naming.py`
- 关键决策与理由：
  - 仅对 `process` 且“非 tutorial_stepwise”启用截图具象性校验，减少无效截图进入 markdown，同时不改变 tutorial 多步骤资产渲染行为。
  - 非 tutorial 的 `process` 统一走 DeepSeek 结构化 + `[IMG:img_id]` 占位替换链路，保证图片插入点与文字逻辑同步。
  - process 正文完成占位替换后，保留 `> Video` 尾部视频区块，但不再追加 `> Images Keyframes` 重复图片区块。
- 兼容性影响：
  - gRPC/proto 与 `result.json` 字段未变更。
  - 仅 markdown 呈现策略变化：`process`（非 tutorial）从“末尾图块”调整为“正文插图优先 + 尾部视频”。
- 风险与回滚方案：
  - 若发现 process 图片召回不足，可回滚 `should_validate_screenshot` 的 process 条件；
  - 若下游依赖末尾图块，可回滚 `_render_section` 中 process 图片块抑制逻辑。
- 验证方式与结果：
  - 新增与更新单元测试覆盖 process 非 tutorial 的占位替换、尾部视频、截图校验触发与 tutorial 豁免。
  - 详细结果见本次提交测试输出。
- 可复用经验：
  - “LLM 结构化占位 + 本地确定性绑定”可复用于其它知识类型，且能在不改协议情况下提升图文对齐。

