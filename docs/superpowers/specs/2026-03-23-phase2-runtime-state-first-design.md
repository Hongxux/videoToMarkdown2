# Phase2 运行时状态优先设计

## 背景

当前 Phase2 / Phase2B 链路里，这三类中间 JSON 仍然在部分路径中承担实际传输职责：

- `intermediates/step2_correction_output.json`
- `intermediates/step6_merge_cross_output.json`
- `intermediates/sentence_timestamps.json`

这会形成一种“双真相”结构：

- Stage1 结果已经存在于内存态 / runtime store 中。
- 下游部分链路已经支持直接注入内存对象。
- 但恢复链路仍会把 runtime state 重新落成 JSON，再让下游重新读回这些 JSON。

结果就是：

- 不必要的 I/O
- 状态重复存放
- 恢复链路复杂度升高
- 业务流与调试产物耦合

## 问题定义

系统不应该再把中间 JSON 文件作为 Phase2A / Phase2B 的主传输通道。

对于正常成功链路：

- Stage1 输出应以结构化内存态直接向后传递。

对于恢复链路：

- 应直接从 runtime store / 数据库重建 Stage1 结构化输出。
- 恢复的对象应是业务状态，而不是先恢复临时 JSON 文件，再从文件反向解析。

## 目标

1. 让 Stage1 runtime payload 成为以下数据的唯一主真相：
   - `step2_subtitles`
   - `step6_paragraphs`
   - `sentence_timestamps`
2. 正常链路不再生成上述三类中间 JSON。
3. 恢复链路不再通过回灌 JSON 来驱动下游。
4. 保留对历史任务的兼容能力：如果历史任务只有 JSON，也能继续恢复。
5. 如果运维或排障需要物理文件，提供显式导出能力，但该能力不能再反向影响业务链路。

## 非目标

1. 不改 Stage1 本身的 NLP / 字幕处理语义。
2. 不试图一次性去掉仓库里所有文件型产物。
3. 不以牺牲历史任务恢复能力为代价做激进收口。
4. 不顺手重构无关的 Phase2B 渲染逻辑。

## 当前杠杆

这次改造不需要重造基础设施，仓库里已经有关键能力。

### 1. 已有统一的 Stage1 runtime state

文件：

- `services/python_grpc/src/server/stage1_runtime_repository.py`

当前已经统一维护：

- `step2_subtitles`
- `step6_paragraphs`
- `sentence_timestamps`

### 2. 已有下游内存注入能力

文件：

- `services/python_grpc/src/content_pipeline/phase2b/assembly/rich_text_pipeline.py`

当前已经支持直接接收：

- `step2_subtitles`
- `step6_paragraphs`
- `sentence_timestamps`

### 3. 已有字幕/段落统一访问抽象

文件：

- `services/python_grpc/src/content_pipeline/shared/subtitle/subtitle_repository.py`

当前已经把字幕、段落、句子时间戳的读取与映射收敛到统一仓储接口。

### 4. 当前真正的回退点

文件：

- `services/python_grpc/src/server/runtime_recovery_context.py`

这里仍然会把 runtime views 回写成：

- `step2_correction_output.json`
- `step6_merge_cross_output.json`
- `sentence_timestamps.json`

这就是当前双轨制继续存在的核心原因。

## 架构根因

当前系统同时存在两套传输模型：

1. runtime-state-first
2. file-first compatibility

问题不在于“兼容层存在”，而在于兼容层仍然泄漏进主恢复链路，导致 JSON 不只是兼容输入，还在继续承担核心控制流职责。

这是错误的边界。

正确边界应该是：

- 兼容逻辑只存在于边缘
- 主链路与恢复链路都只面向结构化 runtime payload

## 目标架构

### 核心原则

运行时状态是唯一主传输层。

中间 JSON 只能扮演以下角色之一：

1. 历史任务导入输入
2. 显式调试导出物
3. 运维排障时的人工检查副产物

它们不能再是 runtime store 与 Phase2A / Phase2B 之间的必经跳板。

### Canonical Stage1 Payload

定义统一的 Stage1 下游载荷，至少包含：

- `step2_subtitles`
- `step6_paragraphs`
- `sentence_timestamps`
- `fingerprint`
- `checkpoint`
- 来源 / provenance 元信息

这个 payload 可以复用现有 runtime repository / store 的结构，但所有下游消费者都必须把它当成主输入，而不是把文件路径当主输入。

## 正常链路目标流程

正常成功流程应收敛为：

1. Stage1 完成并更新 runtime store / runtime cache。
2. Phase2A / Phase2B 直接获取结构化 Stage1 views。
3. `RichTextPipeline` 和相关下游组件直接消费内存对象。
4. 这三类 JSON 不再作为正常链路产物落盘。

## 恢复链路目标流程

恢复流程应收敛为：

1. 恢复上下文从 runtime store / 数据库中读取最近一次可用的 Stage1 payload。
2. 恢复上下文重建与正常链路完全一致的结构化输入对象。
3. Phase2A / Phase2B 使用同一套内存输入继续执行。
4. 不再为了满足下游读取而回灌 `step2/step6/sentence_timestamps.json`。

## 兼容边界

历史任务可能只具备：

- `step2_correction_output.json`
- `step6_merge_cross_output.json`
- `sentence_timestamps.json`

对这类任务，兼容层应该这样处理：

1. 只在入口处读取一次 legacy JSON。
2. 立即转换成 canonical runtime payload。
3. 后续全部走新的 runtime-state-first 路径。

这样既保留历史兼容，又避免新代码继续依赖这些旧文件。

## 调试 / 导出边界

如果运维或人工排障需要看物理文件，应提供显式导出能力，把 canonical payload 导出成 JSON。

但这个导出能力必须满足：

- 显式触发
- 不阻塞业务链路
- 语义上明确是“导出”，不是“传输”

## 需要改动的代码区域

### 1. Python gRPC 入口与编排

主要文件：

- `services/python_grpc/src/server/grpc_service_impl.py`
- `services/python_grpc/src/server/runtime_recovery_context.py`

需要做的事：

- 不再把 `step2_json_path`、`step6_json_path`、`sentence_timestamps_path` 当成主交接手段
- 只要 runtime state 可用，就优先传递结构化 payload
- 去掉恢复链路里“把 runtime state 再写回三类 JSON”的逻辑
- 对新任务，path 字段默认留空；仅在历史兼容导入或显式导出场景下才有值

### 2. SubtitleRepository 与 Phase2B 下游消费

主要文件：

- `services/python_grpc/src/content_pipeline/shared/subtitle/subtitle_repository.py`
- `services/python_grpc/src/content_pipeline/phase2b/assembly/rich_text_pipeline.py`
- `services/python_grpc/src/content_pipeline/shared/subtitle/data_loader.py`

需要做的事：

- 把内存 payload 消费路径明确为主路径
- 文件读取逻辑降级为兼容适配器
- `sentence_timestamps` 的构建优先使用 runtime payload，不能再因为兼容而悄悄回到写文件

### 3. 恢复契约

主要文件：

- `services/python_grpc/src/server/runtime_recovery_context.py`
- `services/python_grpc/src/server/stage1_runtime_repository.py`
- 相关 recovery tests

需要做的事：

- 恢复输出从“文件路径集合”转为“结构化 payload + 元数据”
- 校验逻辑从文件签名逐步迁移到 runtime payload fingerprint + store 元数据
- 保留对历史 JSON-only 任务的兼容导入

### 4. Java 编排侧兼容

主要文件：

- `services/java-orchestrator/src/main/java/com/mvp/module2/fusion/worker/TaskProcessingWorker.java`
- 以及所有把这三个 path 当成主要恢复契约的 DTO / decision 解析逻辑

需要做的事：

- 把 path 字段改成可空、默认空
- 让 Java 侧接受 runtime-state-first 的恢复元数据
- 旧 path 逻辑只保留给历史任务恢复分支

## 数据契约收口方向

### 下游真正需要的不是文件

下游实际需要的是：

- 字幕列表
- 合并后的段落列表
- 句子时间轴

文件只是过去的一种承载形式，不应再作为核心接口语义。

### 恢复元数据应该回答什么

恢复契约应该回答：

- 当前有哪些 runtime payload 可用
- 这些 payload 来自哪个 checkpoint
- 用什么 fingerprint 校验其有效性
- 来源是 live runtime、restored rows，还是 legacy file import

恢复契约不应该回答：

- “我帮你生成了三个临时 JSON 文件”

## 迁移策略

### 阶段 1：正常链路默认改为 runtime payload 传输

- 保留 legacy 文件读取能力
- 停止在正常成功链路中生成这三类 JSON
- 让 Phase2A / Phase2B 端到端优先消费内存 payload

### 阶段 2：去掉恢复链路中的 JSON 回灌

- `runtime_recovery_context` 直接重建结构化 payload
- 恢复后下游复用与正常链路相同的输入接口

### 阶段 3：将 legacy path 正式降级为兼容入口

- 历史任务仍可从 JSON 导入
- 新任务不再生成，也不再依赖这些 JSON

### 阶段 4：如果仍有需要，再增加显式导出能力

- 作为运维/排障工具
- 不进入业务主链路

## 风险

1. Java 侧目前仍读取这三个 path 字段。
   如果 Python 先停止写文件而 Java 还没同步，恢复会断。

2. 现有测试大量围绕物理 JSON 构造场景。
   如果测试不一起改，旧设计会被持续拉回。

3. 现有 fingerprint / dependency 逻辑里仍掺杂文件语义。
   半改造会留下新的双轨校验。

4. 如果过早删掉 JSON 兼容导入，历史任务恢复会受损。

## 验证策略

### 成功链路验证

- 验证 Phase2A / Phase2B 在仅有 runtime payload 的情况下可正常运行
- 验证正常链路不再生成这三类 JSON

### 恢复链路验证

- 验证没有这三类 JSON 时，仍能从 runtime store / restored rows 成功恢复
- 验证恢复后的下游组装仍可拿到完整结构化输入

### 历史兼容验证

- 验证只有 legacy JSON 的历史任务仍可导入
- 验证导入后会被转换成 canonical payload，再走新链路

### 负向验证

- 验证 runtime payload 缺失时，不会静默触发 JSON 回灌
- 验证新任务默认 path 字段为空，除非是历史兼容或显式导出场景

## 文档影响

实现落地后需要同步更新：

- `docs/architecture/overview.md`
- `docs/architecture/upgrade-log.md`
- 如果过程中伴随 bugfix，则更新 `docs/architecture/error-fixes.md`

架构文档里要明确写清：

- Stage1 runtime payload 是 canonical downstream transport
- legacy intermediate JSON 仅用于兼容导入 / 显式导出
- 恢复链路恢复的是结构化状态，不是临时文件

## 推荐实施顺序

1. 先收紧 Python 下游消费接口，让 runtime payload 成为主路径
2. 再去掉 Python 恢复链路里的 JSON 回灌
3. 同步改 Java 恢复契约
4. 重写受影响测试，让 runtime-state-first 成为断言标准
5. 最后再决定是否需要显式 debug export

## 待决策点

1. 显式 debug export 最终是做成单独命令、配置开关，还是 troubleshooting-only 管理入口
2. 文件签名是否完全退出校验体系，还是仅保留给历史兼容路径
3. Java 侧是否现在就接收更丰富的结构化恢复元数据，还是先由 Python 侧提前组装好再传入

## 结论

建议采用严格的 runtime-state-first 设计：

- 一份 canonical payload
- 一条成功链路
- 一条恢复链路
- 文件兼容只留在边缘

这是当前代价最可控、但又能真正去掉双轨制的方案。
