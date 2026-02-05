# 架构升级记录

> 目的：记录系统架构升级的背景、关键决策与复用经验，便于复盘与迁移。

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
  - 持续喂任务流水线：
    - 做法：，IO 读取完成即提交任务；使用 inflight 上限控制内存移除 chunk 屏障，边提交边回收完成任务。
    - 机制收益：避免长尾任务阻塞下一批，减少 worker 空闲，提高整体 CPU 利用率与吞吐。
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

