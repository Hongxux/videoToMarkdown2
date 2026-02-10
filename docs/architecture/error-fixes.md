# 错误修正记录

> 目的：记录错误修正的完整信息，避免同类问题重复发生。

## 记录字段
- 日期
- 现象与影响范围
- 触发条件
- 根因定位
- 修复措施
- 验证方式
- 预防方案（测试/监控/校验/回滚）
- 相关文件/接口
- 复盘要点

## 2026-02-04 初始化
- 本文件创建，等待首条错误修正记录。

## 2026-02-04 知识分类 actionId 回写丢失
- 日期：2026-02-04
- 现象与影响范围：知识分类结果无法回写到 action_units，导致语义单元缺少 knowledge_type/reasoning。
- 触发条件：action_units 的 id 未赋值或重复，分类返回 action_id=0/冲突。
- 根因定位：Java 侧未统一分配 ActionSegmentResult.id，Python 分类按 action_id 关联失败。
- 修复措施：Java 编排为 action_units 统一补齐/去重 id，并确保分类输入与 CV 结果共用同一 id。
- 验证方式：跑一条本地视频主链路，检查 semantic_units 中 action_units 的 id 非零且分类字段已回写。
- 预防方案（测试/监控/校验/回滚）：增加 action_id 非零与唯一性校验；缓存签名纳入 action_id；必要时记录告警并回退到仅保留 unit 级分类。
- 相关文件/接口：services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java、apps/grpc-server/main.py
- 复盘要点：action_id 是跨阶段关联键，必须在编排层统一治理。

## 2026-02-04 Phase2A 初始素材请求被忽略
- 日期：2026-02-04
- 现象与影响范围：Phase2A 召回的截图/切片在后续 FFmpeg 阶段被丢弃。
- 触发条件：GenerateMaterialRequests 返回结果覆盖 Phase2A 请求。
- 根因定位：Java 编排仅使用生成请求，未合并 Phase2A 初始请求。
- 修复措施：在 FFmpeg 提取前合并 Phase2A 与生成请求并去重。
- 验证方式：跑含 Phase2A 初始请求的视频，检查输出素材数量与请求一致。
- 预防方案（测试/监控/校验/回滚）：增加素材请求合并的回归用例；日志记录合并前后数量；必要时增加开关快速回退。
- 相关文件/接口：services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java
- 复盘要点：上游召回与下游策略要统一合并，避免覆盖。

## 2026-02-04 screenshot_selector 缩进错误
- 日期：2026-02-04
- 现象与影响范围：python_grpc_server 启动时报 IndentationError，模块导入失败。
- 触发条件：加载 screenshot_selector.py 时遇到异常缩进的三引号文本。
- 根因定位：遗留的占位文档块缩进不合法，破坏了函数缩进结构。
- 修复措施：移除异常三引号块，改为规范的中文注释与方法实现。
- 验证方式：重新导入 apps/grpc-server/main.py 通过；运行服务启动不再报错。
- 预防方案（测试/监控/校验/回滚）：引入 lint/格式化检查；合并前运行 py_compile 级别语法检查。
- 相关文件/接口：services/python_grpc/src/content_pipeline/screenshot_selector.py
- 复盘要点：文档占位不应影响语法结构，注释必须使用合法格式。

## 2026-02-04 Java 编译缺少 Comparator/LinkedHashMap 导入
- 日期：2026-02-04
- 现象与影响范围：maven-compiler-plugin 编译失败，KnowledgeClassificationOrchestrator 报找不到符号。
- 触发条件：在类中使用 Comparator/LinkedHashMap 但未导入。
- 根因定位：新增排序与签名构造逻辑后遗漏 java.util 导入。
- 修复措施：补充 java.util.Comparator 与 java.util.LinkedHashMap 导入。
- 验证方式：重新执行 mvn 编译通过。
- 预防方案（测试/监控/校验/回滚）：合并前运行编译检查；IDE 启用缺失导入提示。
- 相关文件/接口：services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/KnowledgeClassificationOrchestrator.java
- 复盘要点：签名与排序改动要同步检查 import。

## 2026-02-04 material_requests 未写回 semantic_units_phase2a.json
- 日期：2026-02-04
- 现象与影响范围：semantic_units_phase2a.json 中所有 material_requests 为空，导致后续对齐困难。
- 触发条件：文件结构为 dict 或更新逻辑未覆盖到 semantic_units 列表。
- 根因定位：写回逻辑仅按列表遍历，未兼容 {semantic_units: [...]} 结构，导致更新未生效。
- 修复措施：写回时兼容列表/字典结构，统一更新 semantic_units 列表。
- 验证方式：重新生成素材请求后检查 material_requests 中截图/切片条目。
- 预防方案（测试/监控/校验/回滚）：为语义单元文件增加结构校验；写回后记录条目数量。
- 相关文件/接口：apps/grpc-server/main.py
- 复盘要点：跨阶段文件格式必须显式兼容或统一规范。

## 2026-02-04 素材生成复用 action_units 知识类型
- 日期：2026-02-04
- 现象与影响范围：素材生成阶段与语义回写知识类型不一致，导致 clip 生成与讲解型标注冲突。
- 触发条件：MaterialGenerationInput 仅携带 CV actionSegments，缺少分类后的 knowledge_type。
- 根因定位：Java 侧未将 semantic_units.action_units 的知识类型带入素材生成请求。
- 修复措施：优先使用 semantic_units 的 action_units 构建素材生成输入，缺失时回退到 CV 动作段。
- 验证方式：对比同一单元 action_units 的 knowledge_type 与 clip 生成结果是否一致。
- 预防方案（测试/监控/校验/回滚）：在素材生成前校验 action_units 知识类型是否为空；必要时记录告警。
- 相关文件/接口：services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java
- 复盘要点：跨阶段知识类型应单点来源，避免 CV 动作类型被误当作知识类型。

## 2026-02-04 enhanced_output.md ??/??/????
- ???2026-02-04
- ????????enhanced_output.md ????????????????Obsidian ?????/????????????????
- ?????MarkdownEnhancer ??????? Markdown?????????????????????????
- ?????assemble_only ?? enhancer ??????? Markdown??????????????????????? Markdown ??? Obsidian ?????
- ?????assemble_only ?????????? Markdown ????? Markdown ?? Obsidian ????????????????????????????
- ??????? Phase2B assemble_only??? enhanced_output.md ????????????????????????????????????
- ???????/??/??/?????? Markdown ?????????????? material_requests ?????????????????????
- ????/???services/python_grpc/src/content_pipeline/rich_text_pipeline.py?services/python_grpc/src/content_pipeline/rich_text_document.py
- ??????????????????Obsidian ?????????????

## 2026-02-04 ??????
- ???2026-02-04
- ????????enhanced_output.md ????????????????????????????
- ?????MarkdownEnhancer ??????????????
- ?????_render_section ??? action_classifications/knowledge_type ?????
- ????????????? knowledge_type ?????????????????
- ???????? process/concrete/abstract ????????????????????/????/??????
- ???????/??/??/?????????????????????
- ????/???services/python_grpc/src/content_pipeline/markdown_enhancer.py
- ?????????????????????????

## 2026-02-05 Vision AI ?? Event loop is closed
- ???2026-02-05
- ????????Vision AI ??????? "Event loop is closed"????????????????Phase2B ???
- ????????????? asyncio.run ???? VisionAIClient?????????? AsyncClient?
- ?????AsyncClient ??????? event loop?????????????
- ?????VisionAIClient ?????? event loop?? loop ?????????? AsyncClient???????????
- ????????? Vision AI ? Phase2B ????????? Event loop is closed?Vision API timing ?????
- ???????/??/??/?????? loop ???????????????????? loop/??????????????
- ????/???services/python_grpc/src/content_pipeline/vision_ai_client.py?services/python_grpc/src/content_pipeline/concrete_knowledge_validator.py
- ???????????????????????? loop ???????

## 2026-02-04 讲解型仍生成 clip / 截图缺失
- 日期：2026-02-04
- 现象与影响范围：action_units 标注为讲解型仍生成 clip；部分任务未生成截图请求。

## 2026-02-06 mult_steps 未传递到后续链路
- 日期：2026-02-06
- 现象与影响范围：semantic_units_phase2a.json 缺少 mult_steps 字段，VL 路由与多步提示无法生效。
- 触发条件：语义单元切分生成了 mult_steps，但写盘与重载未包含该字段。
- 根因定位：RichTextPipeline._save_semantic_units 手工序列化未写入 mult_steps；_load_semantic_units 未读取 mult_steps。
- 修复措施：保存时写入 mult_steps；加载时回填到 SemanticUnit。
- 验证方式：重新生成 semantic_units_phase2a.json，确认每个单元包含 mult_steps；VL 路由短过程单元按 mult_steps 分流。
- 预防方案（测试/监控/校验/回滚）：新增语义单元字段完整性校验；阶段输出 JSON schema 校验；必要时回退到默认 mult_steps=false。
- 相关文件/接口：services/python_grpc/src/content_pipeline/rich_text_pipeline.py
- 复盘要点：手工序列化要同步新增字段，避免链路静默丢失。
- 触发条件：action_units knowledge_type 为空或被“knowledge”占位；截图任务异常时无兜底。
- 根因定位：Java 侧将 action_type 作为 knowledge_type 兜底，导致过滤失效；Python 未对占位类型归一。
- 修复措施：Java 侧仅使用 knowledge_type 或 unit 级兜底；Python 对占位类型回退到 unit 级；追加截图请求兜底。
- 验证方式：运行主链路，确认讲解型动作不生成 clip，且每个 unit 至少有 1 个截图请求。
- 预防方案（测试/监控/校验/回滚）：在素材生成前校验 knowledge_type；记录 clip/screenshot 计数告警；必要时恢复旧逻辑。
- 相关文件/接口：services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java、apps/grpc-server/main.py
- 复盘要点：知识类型占位值会破坏过滤逻辑，需在编排层统一规范。

## 2026-02-05 LLM 批量分类 JSON 解析失败
- 日期：2026-02-05
- 现象与影响范围：Batch JSON parse failed，批量分类结果回退默认值，影响知识类型准确性。
- 触发条件：LLM 输出“接近 JSON”但不严格（代码围栏/尾随文本、尾随逗号、中文标点（，：）、字符串内未转义换行/控制字符、数组截断缺少闭合 `]` 等）。
- 根因定位：解析与回填链路假设严格 `json.loads` 可用；当数组整体不合法时会导致整 chunk 结果全丢；批量 id 映射也较脆弱（如 `"ID:0"`）。
- 修复措施：
  - 引入 JSONish 容错解析：修复常见标点/尾随逗号/控制字符，兼容代码围栏与“括号配平”截取，并在数组失败时逐对象抽取以最大化保留有效结果。
  - 解析失败自动拆分 chunk 重试；对缺失项做一次缩小范围重试，尽量避免整批回退默认值。
  - 批量 id 归一：兼容 `"0"`/`0`/`"ID:0"` 等格式，稳定映射回原序号。
- 验证方式：运行 `python -m pytest -q`；在真实任务中观察 Batch JSON parse failed/Batch Miss 明显下降，分类结果不再大量回退默认值。
- 预防方案（测试/监控/校验/回滚）：保留解析回归用例；统计 Batch Miss 比例并告警；通过 `MODULE2_KC_BATCH_SPLIT_MAX_DEPTH` 限制拆分重试深度，必要时可回退旧策略。
- 相关文件/接口：`services/python_grpc/src/content_pipeline/knowledge_classifier.py`、`services/python_grpc/src/content_pipeline/tests/test_knowledge_classifier_parse.py`
- 复盘要点：LLM 输出必须做容错解析，避免批量结果全量回退。

## 2026-02-05 VisionAI 关闭时 Event loop is closed
- 日期：2026-02-05
- 现象与影响范围：日志告警 "VisionAI HTTP client close failed: Event loop is closed"，在 Phase2B 或校验结束阶段出现。
- 触发条件：AsyncClient 绑定的 event loop 已关闭或切换 loop 时触发 aclose。
- 根因定位：_get_client/close 在 loop 已关闭时仍尝试 aclose。
- 修复措施：增加 _safe_close_client；若 loop 已关闭则跳过 aclose 并清理引用，其他异常继续记录告警但不中断流程。
- 验证方式：运行包含 VisionAI 校验的流程，观察关闭阶段不再出现该告警。
- 预防方案（测试/监控/校验/回滚）：增加 loop 状态判断日志；必要时在调用侧确保在同一 loop 内关闭；保留告警用于回归监控。
- 相关文件/接口：services/python_grpc/src/content_pipeline/vision_ai_client.py
- 复盘要点：异步客户端生命周期必须绑定创建时的 loop，跨 loop 关闭需保护。

## 2026-02-05 gRPC Server 启动卡在启动行/编码异常
- 日期：2026-02-05
- 现象与影响范围：运行 `python apps/grpc-server/main.py` 只看到启动行后无后续日志；或在被管道捕获时抛 `UnicodeEncodeError: 'gbk' codec can't encode character`（emoji）。
- 触发条件：stdout/stderr 被重定向到非 UTF-8 编码的管道（常见于 Java 子进程、某些 IDE 终端）；启动/日志包含 emoji；或依赖缺失/导入耗时导致无可观测日志。
- 根因定位：启动阶段在 logging 配置之前输出包含 emoji 的日志；在 GBK 管道下严格编码触发 UnicodeEncodeError；同时缺乏 import/初始化阶段打点，导入慢/阻塞时容易被误判为“卡住”。
- 修复措施：启动时对 stdout/stderr 做 `errors=backslashreplace` 的 best-effort reconfigure；增加 `--check-deps` 依赖预检与 `--debug-imports` 启动 import 进度日志；在 `serve()` 中增加 Servicer 初始化耗时日志；补充统一依赖入口 `requirements.grpc_server.txt`。
- 验证方式：在依赖缺失环境运行 `python apps/grpc-server/main.py --check-deps` 能输出缺失清单；在 GBK/pipe 环境运行不再因 emoji 报 UnicodeEncodeError；启用 `--debug-imports` 能定位卡在哪个 import/初始化步骤。
- 预防方案（测试/监控/校验/回滚）：关键启动阶段分段打点；部署前跑 `--check-deps`；若需保证输出可读性，可在调用侧设置 `PYTHONIOENCODING=utf-8` 或关闭 emoji 输出。
- 相关文件/接口：apps/grpc-server/main.py、requirements.grpc_server.txt、docs/architecture/error-fixes.md
- 复盘要点：启动可观测性要覆盖 logging 配置前阶段；Windows 管道编码与 emoji 是常见坑，需在 bootstrap 阶段处理。
## 2026-02-05 enhanced_output.md 未嵌入视频
- 日期：2026-02-05
- 现象与影响范围：enhanced_output.md 没有生成 Obsidian 视频嵌入，导致 clip 实际存在但文档未引用。
- 触发条件：semantic_units_phase2a.json 中 material_requests 为空或丢失，assemble_only 未调用 _apply_external_materials。
- 根因定位：assemble_only 仅在 requests 存在时才应用外部素材，导致 clip fallback 逻辑未被触发。
- 修复措施：assemble_only 对每个 unit 都调用 _apply_external_materials；缺失 requests 时使用空 MaterialRequests 进入兜底匹配。
- 验证方式：重新运行 Phase2B，确认 result.json materials.clip 填充，enhanced_output.md 出现 ![[clips/xxx.mp4]]。
- 预防方案（测试/监控/校验/回滚）：增加 material_requests 为空的回归用例；记录每个 unit 的 clip/screenshot 应用数量；必要时回退到只用显式 requests。
- 相关文件/接口：services/python_grpc/src/content_pipeline/rich_text_pipeline.py
- 复盘要点：素材匹配必须有兜底路径，即使请求缺失也要尝试文件前缀匹配。
## 2026-02-05 Coarse batch read ThreadPoolExecutor 未定义
- 日期：2026-02-05
- 现象与影响范围：ValidateCVBatch 日志出现 "Coarse batch read failed: name 'ThreadPoolExecutor' is not defined"，粗采样批量读帧回退失败。
- 触发条件：进入粗采样并行读帧分支（worker_count > 1）。
- 根因定位：_batch_read_coarse_frames_to_shm 内直接使用 ThreadPoolExecutor，但未导入该符号。
- 修复措施：改为使用已导入的 futures.ThreadPoolExecutor，避免 NameError。
- 验证方式：跑含 coarse-fine 的 CVBatch，观察日志不再出现该告警，且有 Coarse batch read timing 输出。
- 预防方案（测试/监控/校验/回滚）：增加单元测试覆盖 worker_count>1 分支；启动时增加关键依赖符号自检。
- 相关文件/接口：apps/grpc-server/main.py
- 复盘要点：并行分支应避免未导入符号的隐式依赖。
## 2026-02-05 Batch read futures 变量遮蔽
- 日期：2026-02-05
- 现象与影响范围：日志出现 "Batch read failed: cannot access local variable 'futures' where it is not associated with a value"，批量读帧直接失败。
- 触发条件：进入 _batch_read_frames_to_shm 的并行解码分支（worker_count > 1）。
- 根因定位：函数内将列表命名为 futures，导致与模块级 futures 名称冲突；同时引用 futures.ThreadPoolExecutor 时触发局部变量未赋值错误。
- 修复措施：将列表变量改名为 future_list，避免遮蔽模块名称；统一使用 futures.ThreadPoolExecutor。
- 验证方式：再次运行批量读帧，确认不再出现上述告警，且 Batch read frames timing 正常输出。
- 预防方案（测试/监控/校验/回滚）：增加分支覆盖测试（worker_count>1）；避免使用与模块同名的局部变量。
- 相关文件/接口：apps/grpc-server/main.py
- 复盘要点：局部变量命名应避免与导入模块同名，尤其在异常分支不易暴露。
## 2026-02-05 更新 semantic_units_phase2a.json 时 confidence 字段缺失
- 日期：2026-02-05
- 现象与影响范围：日志出现 "Failed to update semantic_units_phase2a.json: confidence"，导致 action_units 回写中断。
- 触发条件：GenerateMaterialRequests 使用 ActionUnitForMaterialGeneration（不含 confidence/reasoning）时触发。
- 根因定位：回写逻辑直接访问 au.confidence/au.reasoning，字段在该消息类型中不存在。
- 修复措施：新增安全字段读取，兼容 protobuf 对象与 dict，缺失字段使用默认值。
- 验证方式：再次执行 GenerateMaterialRequests，确认 semantic_units_phase2a.json 可更新且无告警。
- 预防方案（测试/监控/校验/回滚）：为不同 action_unit 类型增加回写回归用例；在回写前记录字段缺失统计。
- 相关文件/接口：apps/grpc-server/main.py
- 复盘要点：跨消息类型回写需做字段兼容，避免直接访问可选字段。
## 2026-02-05 Phase2A 缓存 clipRequests 覆盖生成 clipRequests（包络不生效/片段错位）
- 日期：2026-02-05
- 现象与影响范围：同一个 `clip_id` 的切片范围未按“自适应动作包络”更新；极端情况下出现“clip_SU009_action0 实际内容像 SU006”的错位现象。
- 触发条件：`semantic_units_phase2a.json` 已存在且包含 `material_requests.clip_requests`（缓存命中）；随后又执行 GenerateMaterialRequests 产生同 `clip_id` 的新切片请求。
- 根因定位：Java 编排在 FFmpeg 前合并两路切片请求时，先插入 Phase2A（缓存）再插入生成结果，且使用 `computeIfAbsent` 去重，导致同 `clip_id` 时“旧请求优先生效”，新请求被静默丢弃。
- 修复措施：合并时调整优先级为“generatedRequests 优先，Phase2A 仅补缺”；并在 merge 阶段对同 ID 但时间段/语义单元不一致的情况打印 WARNING，直接暴露断链点。
- 验证方式：连续跑同一任务两次（第二次必然缓存命中），检查第二次仍能使用最新包络范围；观察日志存在 `[ClipMerge]` 冲突告警时，最终提取的切片仍以 generated 为准。
- 预防方案（测试/监控/校验/回滚）：增加“缓存命中 + 生成覆盖”的回归用例；对同 `clip_id` 范围冲突计数监控；必要时提供开关禁用 Phase2A 复用或禁用合并。
- 相关文件/接口：`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java`
- 复盘要点：新增功能不仅要“产出正确副作用”，还要保证下游合并/去重策略不会把新副作用吞掉。

## 2026-02-05 上游 action_units.knowledge_type 断链/疑似 CV actionType 污染
- 日期：2026-02-05
- 现象与影响范围：动作单元 `knowledge_type` 为空、为粗粒度 unit 类型（process/abstract）或疑似 CV actionType（click/drag/scroll/K4_operation），导致“讲解型过滤”“自适应动作包络”等策略误判。
- 触发条件：action_units 未成功回写知识分类结果；或 MaterialInputs 退化为使用 CV actionSegments（仅有 action_type）作为 knowledge_type。
- 根因定位：跨阶段数据承载字段混用（action_type vs knowledge_type）+ 缓存/回写缺失时的默认值掩盖了真实断链。
- 修复措施：在 Java->Python GenerateMaterialRequests 入参/出参链路增加“缺失/疑似 CV actionType/疑似默认值”的 WARNING 探针，打印 unit_id、action_id、范围与示例；并确保回写时始终写入 action_units.knowledge_type（至少为 unit 级兜底）。

## 2026-02-06 VL 分析 data-uri 超限与 JSON 解析失败
- 日期：2026-02-06
- 现象与影响范围：
  - DashScope 返回 `400 Bad Request: Exceeded limit on max bytes per data-uri item : 10485760`，部分语义单元 VL 分析直接失败。
  - VL 返回内容偶发非合法 JSON（Markdown 包裹、截断、`key_evidence` 写成多个独立字符串等），触发 `JSONDecodeError` 并导致该语义单元结果丢失。
- 触发条件：
  - 使用 `video_url` 的 data-uri 方式上传本地 mp4 时，单个片段文件在 base64 后超过 10MB 单项限制。
  - 模型输出包含自然语言/代码块包裹，或输出被截断；以及 `key_evidence` 字段格式漂移（字符串 vs 字符串数组）。
- 根因定位：
  - `VLVideoAnalyzer` 无条件将视频片段整体 base64 为 data-uri，未做大小门禁/降级策略。
  - 解析逻辑只做了简单代码块提取 + `json.loads`，对“包裹文本/字段漂移/常见格式错误”缺乏容错与重试策略。
- 修复措施：
  - 输入侧：新增“自动输入策略”以满足 data-uri 10MB 限制：小文件走 data-uri；大文件优先尝试 DashScope `File.upload` 获取临时 URL（可选依赖）；不可用/失败则降级为抽取少量关键帧（`image_url`），并对图片做尺寸/质量压缩确保单项不超限。
  - 输出侧：在提示词尾追加 JSON 硬性约束；解析侧增加括号配对提取、去尾随逗号、修复 `key_evidence` 多字符串模式、兼容字段名漂移（`suggested_screenshot_timestamps`），并在解析失败时用更严格约束重试。
- 验证方式：
  - 运行 `services/python_grpc/src/content_pipeline/tests/test_vl_analyzer.py` 的 `test_json_parsing()`，覆盖 `key_evidence` 典型坏格式与“自然语言包裹 JSON”提取。
  - 通过 `python -m py_compile` 校验模块语法；在真实任务中观察不再出现 data-uri 超限 400，且 JSON 解析失败显著减少。
- 预防方案（测试/监控/校验/回滚）：
  - 测试：持续补充 VL 响应解析的坏格式用例（截断/字段漂移/尾随逗号）。
  - 监控：对“输入降级路径（upload/keyframes）”计数与告警；对解析失败重试次数/失败率埋点。
  - 校验：在发送前统一做 data-uri 单项大小检查；必要时强制 keyframes 模式快速止血。
  - 回滚：将 `vl_material_generation.enabled` 置为 `false` 回退到原有生成链路。
- 相关文件/接口：`services/python_grpc/src/content_pipeline/vl_video_analyzer.py`、`services/python_grpc/src/content_pipeline/tests/test_vl_analyzer.py`
- 复盘要点：多模态输入必须显式考虑网关/供应商的 payload 限制；LLM 输出解析应按“非结构化输入”设计，配套约束、容错与重试闭环。
- 验证方式：跑包含多动作单元的视频，检查日志出现“上游 knowledge_type 缺失/疑似 CV actionType”时能定位具体 unit/action；同时 semantic_units_phase2a.json 的 action_units 中 knowledge_type 不再缺失。
- 预防方案（测试/监控/校验/回滚）：对 gRPC 入参做 schema 校验（action_units[*].knowledge_type 为空比例阈值告警）；为“无分类结果”提供显式标记而非静默默认；必要时回退到不依赖 knowledge_type 的保守裁剪策略。
- 相关文件/接口：`apps/grpc-server/main.py`、`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/grpc/PythonGrpcClient.java`、`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java`
- 复盘要点：数据链断裂要“可观测”，默认值只能兜底不能掩盖断链。

## 2026-02-05 LLM 分类缓存字段命名不兼容导致 action_units.knowledge_type 为空
- 日期：2026-02-05
- 现象与影响范围：分类缓存命中但解析后 `knowledgeType/knowledge_type` 为空，导致 action_units 回写缺失，策略退化为 unit 级兜底。
- 触发条件：历史缓存 `modality_classification_cache.json` 使用 camelCase（unitId/actionId/knowledgeType/keyEvidence）写入，但 loader 按 snake_case（unit_id/action_id/knowledge_type/key_evidence）读取。
- 根因定位：缓存写入/读取字段命名不一致，且缺少“解析后有效性校验”，导致静默使用空字段。
- 修复措施：loadFromCache 同时兼容 camelCase/snake_case，并在解析后对关键字段做有效性检查（无有效项则视为缓存失效并回退重算）。
- 验证方式：复用旧缓存运行一条任务，确认仍能正确解析并回写 knowledge_type；若缓存结构不兼容，日志提示忽略缓存并重新分类。
- 预防方案（测试/监控/校验/回滚）：缓存增加 schema_version；落盘时固定字段命名规范；增加缓存读写一致性的单测。
- 相关文件/接口：`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/KnowledgeClassificationOrchestrator.java`
- 复盘要点：缓存属于“上游返回值”，必须定义稳定 schema，并对解析结果做完整性校验。

## 2026-02-06 Java 编译缺失 CV/知识分类结果类型与 TimeoutConfig 类型
- 日期：2026-02-06
- 现象与影响范围：`fusion-orchestrator` 编译失败，`VideoProcessingOrchestrator` 报找不到 `CVValidationUnitResult`、`KnowledgeResultItem`、`DynamicTimeoutCalculator.Timeouts`。
- 触发条件：执行 `mvn compile` 或构建流程时进入 Java 编译阶段。
- 根因定位：错误导入 `CVValidationOrchestrator.CVValidationUnitResult` 与 `KnowledgeClassificationOrchestrator.KnowledgeResultItem`，实际类型定义在 `PythonGrpcClient` 内部；同时方法签名误用不存在的 `DynamicTimeoutCalculator.Timeouts`。
- 修复措施：移除错误导入，使用已存在的 `PythonGrpcClient.*` 内部类型；将方法签名统一为 `DynamicTimeoutCalculator.TimeoutConfig`。
- 验证方式：在 `java_orchestrator` 目录执行 `mvn -DskipTests compile`，确认编译通过。
- 预防方案（测试/监控/校验/回滚）：统一 DTO/结果类的归属与命名，避免跨类重复定义；CI 中保留编译检查；IDE 开启“错误导入提示”并在重构后跑一次编译验证。
- 相关文件/接口：`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java`
- 复盘要点：结果类型应集中定义，编排层只消费，不应误导向不存在的内部类。

## 2026-02-06 AnalyzeWithVL 截图优化“看起来没开多进程/Worker 空转”
- 日期：2026-02-06
- 现象与影响范围：VL 分析后进入截图时间点 CV 优化阶段，日志长时间停留在主进程预读（OpenCV Random Access），观察到仅 1 个新进程或 Worker 进程起来但几乎无有效任务；部分任务回退到中点时间戳。
- 触发条件：截图请求数量较多（>100）且时间窗口高度重叠（尤其短视频/短片段）；使用全局 SharedFrameRegistry 写入 SHM；同时按“逐请求预读→再提交”的链路导致长时间无提交。
- 根因定位：
  - 预读阶段串行：在提交到 ProcessPool 前先完成大量 `extract_frames_fast`，导致“看起来没开多进程”。
  - SHM 淘汰/解绑：全局 SharedFrameRegistry 有 `max_frames` 上限，批量预读会触发 LRU 淘汰并 `unlink`；Worker 侧 attach 时出现 `SharedMemory not found`，进而读不到帧，任务等价“空转”。
- 修复措施：
  - 以 chunk 作为 SHM 生命周期边界：每个 chunk 使用独立 SharedFrameRegistry，避免跨 chunk 淘汰 `unlink`。
  - 预读读帧策略改为“单次 seek + 顺序 read 扫描”：只在命中的候选帧上 resize + 写入 SHM，避免短窗口下 OpenCV Random Access（频繁 `cap.set`）导致的极端慢预读，从而让 worker 持续有活干。
  - Union 预读 + 流式喂入：每个 chunk 先 Union 预读覆盖区间，再立即提交任务；维护全局 pending 队列并用 `FIRST_COMPLETED` drain 实现背压节流。
  - IO/Compute 重叠：通过 `streaming_overlap_buffers` 支持 double-buffer overlap；复用 gRPC 侧全局 ProcessPool（避免重复 spawn）。
  - 可观测性增强：支持 `CV_POOL_WARMUP=1` 输出 Worker PID 集合；Worker 日志包含 PID，并在“读不到帧”时输出 shm_name 样本。
- 验证方式：跑 `AnalyzeWithVL`（截图请求 > 100），确认日志输出 workers/inflight/chunks/prefetch_ms/register_ms/submitted/completed；Worker 日志出现多个 PID 且有任务执行；`SharedMemory not found` 告警显著减少或消失。
- 预防方案（测试/监控/校验/回滚）：新增单元测试覆盖 chunk 切分；运行时日志记录 submitted/completed；可通过 `streaming_pipeline=false` 或 `streaming_overlap_buffers=1` 回退到更稳的顺序 chunk。
- 相关文件/接口：`services/python_grpc/src/content_pipeline/vl_material_generator.py`、`services/python_grpc/src/content_pipeline/visual_feature_extractor.py`、`services/python_grpc/src/vision_validation/worker.py`、`apps/grpc-server/main.py`、`config/module2_config.yaml`
- 复盘要点：SharedMemory 必须配套生命周期边界；“预读+全局缓存”在高并发下易触发淘汰与时序问题，需用 chunk/背压/可观测性闭环约束。

## 2026-02-06 JavaCV FFmpeg 素材提取超时（TimeoutException）
- 日期：2026-02-06
- 现象与影响范围：进入 FFmpeg/JavaCV 提取阶段后约 4-5 分钟失败，`Pipeline Failed ... java.util.concurrent.TimeoutException`，任务状态变为 FAILED。
- 触发条件：素材请求数量远高于按视频时长的估算（例如 700s 视频生成 225 screenshots + 92 clips），且 clip 提取为“逐段重新初始化 Grabber/Recorder + 编码写盘”，耗时显著高于简单计数估算。
- 根因定位：`DynamicTimeoutCalculator.calculateTimeouts(videoDuration)` 的 `ffmpegTimeoutSec` 仅基于视频时长做粗估（estimatedScreenshots/estimatedClips），与实际 material_requests 数量/切片总时长脱钩，导致 `JavaCVFFmpegService.extractAllSync(...).orTimeout()` 提前触发。
- 修复措施：
  - 在 `VideoProcessingOrchestrator` 基于真实 `screenshotRequests.size()`、`clipRequests.size()` 以及 `sum(end-start)` 计算提取超时，并记录到日志。
  - 在 `JavaCVFFmpegService` 输出提取开始日志时附带 timeout；超时时抛出更明确的错误信息（包含 timeout 秒数），便于排查。
- 验证方式：对同一视频/同一 material_requests 重新执行，确认不再在约 292s（旧估算）处超时；日志中能看到 “FFmpeg timeout computed: ...” 且提取阶段可以完成或在更合理的阈值上超时。
- 预防方案（测试/监控/校验/回滚）：将 “提取请求数量/总切片时长/计算出的 timeout” 纳入关键日志；当请求数量异常飙升时可增加告警与策略降采样（例如上限 clips/screenshots 或按单位合并去重）。
- 相关文件/接口：`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java`、`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/JavaCVFFmpegService.java`
- 复盘要点：timeout 必须依赖“真实工作量”（请求数、切片总时长），而不是仅按视频时长做静态估算；错误信息要包含关键上下文，便于线上快速定位。

## 2026-02-06 本地视频时长未探测导致超时偏低
- 日期：2026-02-06
- 现象与影响范围：本地视频任务偶发在素材提取或前置阶段出现 `TimeoutException`，超时阈值与实际时长明显不匹配。
- 触发条件：输入为本地路径且未下载；或下载返回时长为 0；同时素材请求数量较多/磁盘性能偏慢。
- 根因定位：本地路径分支未探测真实视频时长，`videoDuration` 使用默认值（60s），导致动态超时与实际耗时脱钩；FFmpeg 超时缺少可配置的环境缩放。
- 修复措施：
  - 使用 JavaCV Grabber 探测本地视频时长，并在下载时长缺失时回退探测。
  - 增加 `ffmpeg_extraction` 配置（timeout_multiplier/min/max），在计算超时后进行可配置缩放。
- 验证方式：用本地视频跑一条主链路，日志出现 “Probed video duration” 与 “FFmpeg timeout computed: X -> Y”；超时不再过早触发。
- 预防方案（测试/监控/校验/回滚）：保留时长探测日志；在配置中按硬件性能调整 `timeout_multiplier`；必要时将 `max_timeout_sec` 设为 0 以避免误裁剪。
- 相关文件/接口：`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/VideoProcessingOrchestrator.java`、`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/JavaCVFFmpegService.java`、`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/ModuleConfigService.java`、`config/module2_config.yaml`
- 复盘要点：动态超时必须依赖真实输入特征（时长/请求规模），并允许按环境做缩放以避免“同样逻辑不同机器超时”的漂移。

## 2026-02-07 VL 前置裁剪后时间轴偏移风险
- 日期：2026-02-07
- 现象与影响范围：对 `process` 单元做“stable 剔除 + 片段拼接”后，VL 输出时间戳若直接按原逻辑使用，会出现截图/切片定位偏移。
- 触发条件：VL 输入不是完整原片段，而是裁剪拼接后的新片段；输出的 `clip_start_sec/clip_end_sec/screenshot_timestamps` 仍按新片段时间轴。
- 根因定位：旧链路默认“VL 相对时间 == 原语义单元相对时间”，未考虑前置裁剪导致的时间轴非线性映射。
- 修复措施：
  - 在 `vl_material_generator.py` 增加 `kept_segments` 映射函数，将裁剪片段相对时间回映射到原语义单元时间轴。
  - 回写 clip 时同时补齐 `segments`，复用 Java 侧拼接语义，防止中间被剔除段重新纳入素材。
  - 增加单元测试覆盖“边缘保留剔除 + 时间映射”核心场景。
- 验证方式：运行 `test_vl_pre_prune.py`，确认 `stable=[1,5]` 且 `keep_edge=1` 时仅剔除 `[2,4]`；验证裁剪时间 `3.0s` 映射回原时间 `5.0s`。
- 预防方案（测试/监控/校验/回滚）：
  - 预处理默认保守阈值（`min_removed_ratio`、`min_keep_segment_sec`）防止过度裁剪。
  - 记录预处理日志（`removed_ratio/stable_count/kept_count`）用于线上回归比对。
  - 发生异常可通过 `pre_vl_static_pruning.enabled=false` 一键回退。
- 相关文件/接口：`services/python_grpc/src/content_pipeline/vl_material_generator.py`、`services/python_grpc/src/content_pipeline/tests/test_vl_pre_prune.py`、`config/module2_config.yaml`
- 复盘要点：任何“前置压缩输入”的策略都必须保证时间轴可逆映射，否则后续素材定位会系统性漂移。

## 2026-02-07 VL 多个视频片段截取结果相同（并触发 Java Concat DTS 非单调）
- 日期：2026-02-07
- 现象与影响范围：
  - 多个 `vl_clip_SUxxx_merged.mp4` 视觉上几乎是同一段内容，语义单元间区分度消失。
  - Java 侧出现大量 `No frame at timestamp ...`（超过原视频时长）与 `non monotonically increasing dts` / `av_interleaved_write_frame() error -22`。
- 触发条件：启用 VL 前置裁剪（stable 剔除）后，进入“时间回映射 + Java 侧多段拼接”链路时更容易触发。
- 根因定位（第一性拆解）：
  - **单元映射错误**：`_find_clip_for_unit` 使用 `if unit_id in filename` 子串匹配，`SU01` 可能误命中 `SU010`，导致多个单元复用同一源片段。
  - **时间基准污染**：预裁剪回映射阶段使用了循环外变量 `start_sec`，而不是当前任务元数据中的 `meta.start_sec`，导致多数请求被整体偏移到同一时间区间。
  - **分段回写过宽**：每个 clip 都回写整单元 `kept_segments`，而非 clip 自身命中的子区间，Java concat 收到重复/重叠片段后更易产出非单调 DTS。
  - **缺少边界收敛**：回映射后的 clip/screenshot 时间未统一 clamp 到 `[unit_start, unit_end]`，越界时间戳进入下游后直接触发抽帧失败。
- 修复措施：
  - **精确匹配 clip**：`manifest.json` 精确 `unit_id` 优先；文件名回退改为 token 边界匹配（正则），彻底规避子串误匹配。
  - **修正时间基准**：回映射统一使用 `unit_start_sec/unit_end_sec`（来自 `task_metadata`），消除跨单元变量串用。
  - **新增区间级映射**：增加 `_map_pruned_interval_to_original_segments(...)`，把 pruned 区间映射回原时间轴的“实际命中子段”，支持跨 gap 的多段返回。
  - **收紧 segments 回写**：`clip_item["segments"]` 改为当前 clip 的 `abs_segments`（命中子段），不再写整单元保留段。
  - **统一边界兜底**：对 clip/screenshot 时间戳执行区间约束（clamp 到单元边界），防止越界进入 Java 侧。
  - **Java 侧时间戳兜底**：`JavaCVFFmpegService.extractConcatClip` 对输出时间戳做单调递增校正，并用最后写入时间推进段间 offset，避免 muxer 因重复/回退 DTS 报错。
- 验证方式：
  - 单元测试：`test_vl_pre_prune.py` 新增/更新以下用例并通过。
    - `test_find_clip_for_unit_avoids_substring_collision`（防 SU01/SU010 冲突）
    - `test_map_pruned_interval_to_original_segments_cross_gap`（跨剔除 gap 的区间映射）
    - `test_removed_intervals_require_stable_longer_than_3s`（阈值改为 >3s）
  - 语法检查：`py_compile` 通过。
  - 复跑含 concat 的任务，确认不再出现 `non monotonically increasing dts` / `av_interleaved_write_frame() error -22`，且 clips 可正常落盘。
- 预防方案（监控/测试/校验/回滚）：
  - 监控：增加“素材请求时间戳越界计数”“单元到 clip 命中冲突计数”“concat 失败计数”作为告警维度。
  - 测试：保留并扩展“unit_id 冲突命名”“跨 gap 映射”“越界 clamp”回归测试。
  - 校验：在进入 Java 提取前增加轻量校验（`segments` 去重、排序、重叠合并后再下发）。
  - 兜底：Java 侧输出时间戳强制单调递增，降低异常数据对 concat 的致命影响。
  - 回滚：可临时关闭 `pre_vl_static_pruning.enabled` 回到未裁剪路径，保证主流程可用性。
- 相关文件/接口：`services/python_grpc/src/content_pipeline/vl_material_generator.py`、`services/python_grpc/src/content_pipeline/tests/test_vl_pre_prune.py`、`services/java-orchestrator/src/main/java/com/mvp/module2/fusion/service/JavaCVFFmpegService.java`、`docs/architecture/upgrade-log.md`
- 复盘要点：任何“先裁剪再理解”的链路，本质是“坐标系变换问题”；若 unit 绑定、时间基准、区间语义三者任一失真，下游必然表现为“片段重复 + 越界 + 拼接异常”。

## 2026-02-10 Python 侧 HTTP 请求日志刷屏
- 日期：2026-02-10
- 现象与影响范围：控制台持续出现 `HTTP Request: POST https://api.deepseek.com/chat/completions "HTTP/1.1 200 OK"`，关键信息被淹没，定位业务告警困难。
- 触发条件：根日志级别为 `INFO`，且 `httpx/httpcore/openai` 继承根 logger 配置时，会输出每次请求的 info 日志。
- 根因定位：日志初始化只配置了 root handler/formatter，未对第三方 HTTP 客户端 logger 做噪声分级控制。
- 修复措施：在 `configure_pipeline_logging(...)` 中默认下调 `httpx`、`httpcore`、`openai`、`openai._base_client` 到 `WARNING` 级别，保留 warning/error 可见性。
- 验证方式：执行最小复现脚本，确认 `httpx.info(...)` 不再输出、`httpx.warning(...)` 仍正常输出；同时验证 `py_compile` 通过。
- 预防方案（测试/监控/校验/回滚）：
  - 通过环境变量 `PIPELINE_ENABLE_HTTP_INFO_LOGS=1` 可临时恢复 HTTP info 日志，便于联调。
  - 通过环境变量 `PIPELINE_HTTP_LOG_LEVEL` 可按需覆盖（默认 `WARNING`）。
  - 保持默认静默策略，避免高并发场景日志 IO 反向影响吞吐。
- 相关文件/接口：`services/python_grpc/src/common/logging/pipeline_logging.py`
- 复盘要点：全局 `INFO` 在接入通用 SDK 后通常会引入“高频无业务价值日志”，应在日志入口统一做第三方噪声治理。

## 2026-02-10 Stage1 中间产物路径不稳定（step2/step6/sentence_timestamps）
- 日期：2026-02-10
- 现象与影响范围：
  - `transcript_pipeline` 执行后，`step2`、`step6` 与 `sentence_timestamps` 可能无法稳定在 `output_dir/intermediates/` 同路径获取。
  - 影响 Phase2A/Phase2B 对中间文件的统一消费，增加回退判断与补拷贝复杂度。
- 触发条件：
  - `StepOutputConfig` 被外部参数关闭关键步骤输出；或 `sentence_timestamps` 仅写入 `local_storage` 且服务层复制链路未命中。
- 根因定位：
  - `step2/step6` 落盘依赖可变开关，缺少“关键产物不可关闭”约束。
  - `sentence_timestamps` 的标准消费路径依赖服务层后置复制，不在 Stage1 pipeline 内直接保证。
- 修复措施：
  - 在 `StepOutputConfig` 中新增 `REQUIRED_ENABLED_STEPS`，强制包含 `step2_correction` 与 `step6_merge_cross`。
  - 在 `step4_clean_local` 保存 `local_storage` 后，直接同步落盘 `output_dir/intermediates/sentence_timestamps.json`。
  - 对 `intermediates` 写入异常采用 warning，不中断主流程。
- 验证方式：
  - `python -m py_compile services/python_grpc/src/transcript_pipeline/graph.py services/python_grpc/src/transcript_pipeline/nodes/phase2_preprocessing.py`
  - 运行最小脚本验证 `StepOutputConfig`：`disable_all/custom` 场景下仍包含 `step2/step6`。
- 预防方案（测试/监控/校验/回滚）：
  - 测试：新增/补充单测覆盖 `StepOutputConfig` 必选步骤与 `step4` intermediates 落盘行为。
  - 监控：在 Stage1 完成时增加文件存在性日志（`step2/step6/sentence_timestamps`）。
  - 校验：服务层返回 `Stage1Response` 前统一校验三文件并记录缺失原因。
  - 回滚：若需紧急回退，可仅保留服务层复制逻辑并恢复 `StepOutputConfig` 可关闭策略。
- 相关文件/接口：
  - `services/python_grpc/src/transcript_pipeline/graph.py`
  - `services/python_grpc/src/transcript_pipeline/nodes/phase2_preprocessing.py`
  - `services/python_grpc/src/server/grpc_service_impl.py`
- 复盘要点：
  - 下游强依赖的关键中间产物应在最靠近生产点处“原子保证”，而非依赖跨层补偿。

## 2026-02-10 AnalyzeSemanticUnits 读取 Stage1 产物失败（字段缺失 + get_video_duration 未定义）
- 日期：2026-02-10
- 现象与影响范围：
  - 日志出现：
    - `Missing required fields: ['subtitle_id', 'start_sec', 'end_sec']`
    - `Missing required fields: ['paragraph_id', 'text', 'source_sentence_ids']`
    - `AnalyzeSemanticUnits failed: name 'get_video_duration' is not defined`
  - 直接影响 `AnalyzeSemanticUnits`，并导致后续素材生成链路中断。
- 触发条件：
  - Stage1 输出文件较大时，`step2/step6` 被写成 `count/sample` 摘要结构；
  - Phase2A 初始化 `RichTextPipeline` 时命中未导入函数调用。
- 根因定位：
  - `StepOutputConfig` 的通用摘要化策略破坏了 `step2/step6` 的消费者契约（消费者要求完整数组）。
  - `rich_text_pipeline.py` 内直接使用 `get_video_duration(...)` 但缺少 import。
  - Stage1 复用校验缺少“坏结构识别”，会重复复用已损坏产物。
- 修复措施：
  - `transcript_pipeline/graph.py`：对 `step2_correction`、`step6_merge_cross` 强制全量落盘，不再摘要化。
  - `server/grpc_service_impl.py`：复用校验新增 `compacted_output` 检测，命中即拒绝复用并触发重算。
  - `content_pipeline/phase2b/assembly/rich_text_pipeline.py`：补充 `get_video_duration` 导入。
  - 增加测试：
    - `test_step_output_config.py`（验证 step2/step6 全量落盘）
    - `test_data_loader_compacted_output.py`（验证摘要结构会被识别为非法输入）
- 验证方式：
  - `python -m py_compile services/python_grpc/src/transcript_pipeline/graph.py services/python_grpc/src/server/grpc_service_impl.py services/python_grpc/src/content_pipeline/phase2b/assembly/rich_text_pipeline.py services/python_grpc/src/transcript_pipeline/tests/test_step_output_config.py services/python_grpc/src/content_pipeline/tests/test_data_loader_compacted_output.py`
  - `pytest -q services/python_grpc/src/transcript_pipeline/tests/test_step_output_config.py services/python_grpc/src/content_pipeline/tests/test_data_loader_compacted_output.py`（`4 passed`）
- 预防方案（测试/监控/校验/回滚）：
  - 测试：为所有“被下游直接消费”的中间文件新增结构契约测试，禁止摘要化。
  - 监控：Stage1 复用日志新增 `compacted_output` 原因统计，便于观察坏缓存发生率。
  - 校验：在 Stage1 结束时增加关键文件结构快检（数组/必填字段）。
  - 回滚：如需紧急回滚，仅保留“禁摘要 + 导入修复”，关闭冲突日志扫描逻辑。
- 相关文件/接口：
  - `services/python_grpc/src/transcript_pipeline/graph.py`
  - `services/python_grpc/src/server/grpc_service_impl.py`
  - `services/python_grpc/src/content_pipeline/phase2b/assembly/rich_text_pipeline.py`
  - `services/python_grpc/src/transcript_pipeline/tests/test_step_output_config.py`
  - `services/python_grpc/src/content_pipeline/tests/test_data_loader_compacted_output.py`
- 复盘要点：
  - “日志摘要友好”与“中间产物契约稳定”是两条不同目标，必须分层实现，不能共用同一输出格式。

## 2026-02-10 并行转录子任务异常导致字幕未落盘
- 日期：2026-02-10
- 现象与影响范围：
  - 开启并行转录后，部分任务出现“字幕没有被保存”或 `subtitle_path` 为空。
  - 影响 `TranscribeVideo -> ProcessStage1` 链路，Stage1 因缺少稳定字幕输入而中断或失败。
- 触发条件：
  - `ProcessPoolExecutor` 某个分段子任务异常（如子进程崩溃、模型加载异常、ffmpeg 子段提取失败抛异常）。
- 根因定位：
  - `parallel_transcription.transcribe_parallel(...)` 在聚合阶段直接调用 `future.result()`，任一 `future` 抛异常会中断整个并行转录流程。
  - 中断后 gRPC `TranscribeVideo` 无法进入“写 `subtitles.txt`”分支，最终表现为字幕未落盘。
- 修复措施：
  - 在并行聚合层对 `future.result()` 增加异常隔离，单段失败不再直接打断整批。
  - 记录失败分段并执行串行补偿重试（复用现有 `transcribe_segment`），最大化保留可恢复结果。
  - 并行+补偿后仍有失败时，显式抛出包含失败段数量的错误，避免“静默空结果”误导下游。
- 验证方式：
  - 新增 `test_parallel_transcription_fallback.py`：
    - 用例1：并行子任务失败后串行补偿成功，最终仍返回完整字幕。
    - 用例2：并行与串行补偿均失败时，抛出明确错误（包含失败段比例）。
  - 本地执行：`python -m pytest -q services/python_grpc/src/media_engine/knowledge_engine/core/tests/test_parallel_transcription_fallback.py`，结果 `2 passed`。
- 预防方案（测试/监控/校验/回滚）：
  - 测试：保留“并行失败 -> 串行补偿”回归测试，防止后续改动退化为全局失败。
  - 监控：关注日志关键字“进入串行补偿”“并行转录失败：仍有 x/y 个分段失败”。
  - 校验：在 gRPC 层保留失败快速返回，避免写入误导性空字幕文件。
  - 回滚：如需紧急回退，可临时关闭 `whisper.parallel.enabled` 退回单路转录。
- 相关文件/接口：
  - `services/python_grpc/src/media_engine/knowledge_engine/core/parallel_transcription.py`
  - `services/python_grpc/src/media_engine/knowledge_engine/core/tests/test_parallel_transcription_fallback.py`
  - `services/python_grpc/src/server/grpc_service_impl.py`
- 复盘要点：
  - 并行链路的稳定性关键在“失败隔离 + 可控补偿”，而不是假设每个 worker 必然成功。
