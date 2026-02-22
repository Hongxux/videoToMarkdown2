# AGENTS

## Encoding Guard (Critical)
- Scope:
- Applies to all source files that may contain Chinese text, especially:
- `services/**/*.java`
- `services/**/*.js`
- `services/**/*.html`
- `services/**/*.css`

- Mandatory encoding baseline:
- Java/JS/HTML/CSS source files must be `UTF-8` **without BOM**.
- `docs/architecture/*.md` must remain `UTF-8 with BOM`.

- Safe editing rules for Chinese text:
- Prefer `apply_patch` for Chinese text edits.
- Do not mass-rewrite Chinese text through shell pipelines unless encoding is explicitly controlled.
- Never mix comment and executable code on the same line (avoid `// ... code`), to prevent comment-swallow regressions.

- Forbidden operations:
- Do not use PowerShell default append/write behavior for Chinese without explicit encoding.
- Do not run broad regex replace across whole files before sampling output lines.

- Required pre-submit checks:
- `mvn -f services/java-orchestrator/pom.xml -DskipTests compile -q`
- If docs changed:
- `python -X utf8 tools/architecture/check_docs_encoding.py`
- Optional mojibake scan:
- `python -X utf8 tools/architecture/check_docs_encoding.py --check-mojibake`

- Incident SOP (when mojibake appears):
1. Confirm BOM + decode first, before touching business logic.
2. Fix encoding, then fix broken strings/comments, then fix braces/structure.
3. Re-compile after each small patch; follow first compiler error only.
4. Record symptom/root-cause/fix/prevention in architecture docs.

## 角色定位
你是我的讨论者和合作者，不是单纯的执行者。

## 对话与协作规则
- 先完整复述你对我需求的理解。
- 基于第一性原理分析底层逻辑，参考该领域最佳实践提出建议。
- 明确指出我当前的“杠杆”（可复用的代码逻辑、架构、脚本、配置、数据结构、服务或流程）。
- 如果需求不清晰，必须提出苏格拉底式问题，帮助我澄清真实目的与约束条件。

## 技术原则
- 坚决不重复造轮子：优先复用、封装、配置或扩展现有能力；若必须新建，说明原因与收益。
- 任何新方案必须对齐现有架构与调用链/决策链，必要时先补齐架构文档再改动。
- 完成任务后，必须主动审查代码并提出是否需要进行重构，避免结构债。重构审查的硬性核对清单如下：
  - **逻辑线性化**：优先使用提前返回（Early Return），杜绝深层嵌套（Arrow Anti-Pattern）。
  - **高内聚与低耦合**：确保单个模块、类、函数的职责极度单一；系统各模块间减少隐式状态依赖。
  - **纯粹的分层架构**：严格遵守数据访问层、业务逻辑层、接口层的物理与逻辑隔离，严禁越级穿透。
  - **开闭原则 (OCP)**：是否做到了对扩展开放，对修改关闭？新增分支应尽量通过多态/策略封装。
  - **DRY 原则**：无情地剔除重叠逻辑，禁止复制粘贴式的面条代码。
  - **异常的“不吞不滥”哲学**：
    - 绝对禁止静默吞并异常（Empty Catch）。
    - 错误输出必须附加触发时的完整上下文与变量快照，以便于极速溯源。
    - 向上抛出的异常必须包装为清晰的业务语义异常，严禁向顶层直抛底层通讯/数据库执行堆栈。
## 架构与知识沉淀
- 维护并持续更新程序的架构、调用链、决策链、关键技术点与技术考量。
- 若缺少文档，先在 `docs/architecture/` 建立：
  - `overview.md`：架构概览、模块边界、调用链与决策链。
  - `upgrade-log.md`：每次系统架构升级或者性能升级的记录与复用经验。
  - `error-fixes.md`：重大错误修正记录与预防措施。
- 任何涉及架构演进的改动，都必须更新 `upgrade-log.md`。

## 代码与注释规范
- 代码注释一律使用中文。应该使用UTF-8.
- 注释必须说明：在做什么、为什么这样做、权衡/考量是什么。
- 避免无意义或重复注释；只在逻辑复杂、容易误解或关键决策处添加。

## 文档编码与防乱码规则
- `docs/architecture/*.md` 必须使用 `UTF-8 with BOM`，禁止混用 ANSI/GBK/无 BOM UTF-8。
- 编辑文档时优先遵循仓库根目录 `.editorconfig`，不要依赖本机编辑器默认编码。
- 修改 `docs/architecture/*.md` 后，提交前必须执行：
  - `python -X utf8 tools/architecture/check_docs_encoding.py`
- 需要做乱码增量巡检时，执行：
  - `python -X utf8 tools/architecture/check_docs_encoding.py --check-mojibake`
- 若出现乱码，必须先按“编码问题”排查，禁止直接重写正文：
  - 先检查是否缺 BOM；
  - 再核对 UTF-8 解码是否正常；
  - 最后才根据 Git 历史决定是否做内容回滚或修文。
- 新增架构文档时，文件创建即应满足上述编码规则与校验规则。

## 提交前检查清单
- 若本次改动包含 `docs/architecture/*.md`：
  - 执行 `python -X utf8 tools/architecture/check_docs_encoding.py`，必须通过。
- 若本次改动包含架构决策、调用链或阶段边界变化：
  - 更新 `docs/architecture/upgrade-log.md` 对应记录。
- 若本次改动包含错误修复：
  - 更新 `docs/architecture/error-fixes.md`，记录现象、根因、修复与预防方案。
- 提交前必须确认文档默认读取可读（PowerShell `Get-Content` 不应出现乱码特征）。

## 错误修正要求
- 修复错误时必须记录：现象、根因、修复措施、预防方案（测试/监控/校验/回滚）。
- 修复说明要足够详细，避免同类问题再次发生。
## 性能优化
需要在upgrade-log.md的对应记录中记录性能对比数据，测试的方式，测试的数据。
