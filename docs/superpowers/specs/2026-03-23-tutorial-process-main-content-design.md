# Tutorial Process Main Content Design

**Goal:** 让教程型 `process` 语义单元与其他 Phase2B 结构化单元保持一致，改为以步骤级 `main_content` 作为优先输入源进入结构化链路，缺失时回退到 `main_operation`。

**Context**

- 当前教程型 `process` 在 Phase2B 里存在两处短路：
  - `MarkdownEnhancer._process_one(...)` 直接 passthrough，不进入 `skill_pipeline` / `media_preserved` 结构化链路。
  - `MarkdownEnhancer._render_section(...)` 对教程型 `process` 无条件调用 `_render_tutorial_steps(...)`，忽略 `structured_content`。
- 这导致教程步骤即使已经具备更好的步骤正文来源，也不会参与与 `abstract / concrete / proving` 一致的结构化整理。

**Design**

- 在教程步骤标准化入口 `_load_tutorial_steps(...)` 中补充 `main_content` 字段，并在 raw/manifest 合并时保留它。
- 为 `process` 增加统一 canonical 输入构造：
  - 教程型 `process`：按步骤输出骨架，步骤正文优先使用 `main_content`，缺失时回退 `main_operation`，再缺失时回退 `main_action`。
  - 非教程型 `process`：继续使用现有 `original_body`。
- 删除教程型 `process` 在 `_process_one(...)` 中的提前返回，让它与其他 `process` 一样：
  - `skill_pipeline` 开启时进入 `Phase2bStructuredUnitPipeline.process_unit(...)`
  - `skill_pipeline` 关闭时进入 `_build_structured_text_for_media_preserved_section(...)`
- 调整 `_render_section(...)`：
  - 教程型 `process` 优先渲染 `structured_content / enhanced_body`
  - 仅在结构化结果为空时，才回退 `_render_tutorial_steps(...)`

**Constraints**

- 不能破坏已有教程步骤的媒体嵌入、占位符替换、步骤类型展示与编号规则。
- 不能新增新的 tutorial 专用 Phase2B 分支，避免继续加深结构分叉。

**Testing**

- 新增回归测试，覆盖：
  - 教程型 `process` 确实进入 skill pipeline
  - `main_content` 优先于 `main_operation`
  - `main_content` 缺失时回退到 `main_operation`
  - 最终渲染使用结构化输出，而不是旧的教程 passthrough
