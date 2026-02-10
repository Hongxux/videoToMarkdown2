# Module2 使用说明

本文档聚焦 `services/python_grpc/src/content_pipeline/` 的 Prompt 管理层，说明如何集中维护 DeepSeek / VL / Vision AI 的提示词。

## 目标
- 将分散在各类中的 Prompt 文本统一收敛到模板目录。
- 通过 PromptKey 统一索引，避免“硬编码字符串”散落。
- 支持按环境配置覆盖（`root_dir` / `overrides`）与严格校验（`strict`）。
- 在迁移期保持兼容：模板缺失时可回退到调用点内置 fallback。

## 目录结构
- `services/python_grpc/src/content_pipeline/infra/llm/prompt_registry.py`：PromptKey 与默认模板路径注册表。
- `services/python_grpc/src/content_pipeline/infra/llm/prompt_loader.py`：统一加载、缓存、变量渲染与兜底策略。
- `services/python_grpc/src/content_pipeline/prompts/`：模板文件目录（按模型/场景分组）。

示例（节选）：
- `services/python_grpc/src/content_pipeline/prompts/deepseek/semantic_segment/system.txt`
- `services/python_grpc/src/content_pipeline/prompts/deepseek/knowledge_classifier/batch_user.txt`
- `services/python_grpc/src/content_pipeline/prompts/vision_ai/concrete_knowledge/user.txt`
- `services/python_grpc/src/content_pipeline/prompts/vl/video_analysis/default_user.txt`

## 配置项
配置文件：`config/module2_config.yaml`

```yaml
prompt_management:
  enabled: true
  root_dir: ""        # 可选：外部模板根目录
  strict: false        # 严格模式：缺模板时抛错
  overrides: {}        # 可选：按 PromptKey 指定模板绝对/相对路径
```

说明：
- `enabled=true`：启用集中管理路径解析与加载。
- `root_dir`：用于将默认模板路径重定位到外部目录（灰度/实验环境常用）。
- `overrides`：仅替换某几个 PromptKey 的模板，适合 A/B 实验。
- `strict=true`：模板缺失、不可读时直接报错；推荐用于 CI/预发。

## 调用方式
### 1) 获取纯文本模板
```python
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys

system_prompt = get_prompt(
    PromptKeys.DEEPSEEK_KC_BATCH_SYSTEM,
    fallback="你是一个知识分类助手..."
)
```

### 2) 渲染带变量模板
```python
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import render_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys

user_prompt = render_prompt(
    PromptKeys.DEEPSEEK_KC_BATCH_USER,
    context={
        "title": title,
        "semantic_units_json": semantic_units_json,
    },
    fallback="请分析以下语义单元：{semantic_units_json}"
)
```

## 加载优先级（重要）
`prompt_loader.get_prompt(...)` 的查找顺序：
1. `prompt_management.overrides[key]` 指定路径（若存在）。
2. `prompt_management.root_dir + registry默认相对路径`（若配置且存在）。
3. 包内默认模板（`services/python_grpc/src/content_pipeline/prompts/...`）。
4. 调用点传入的 `fallback`（若有）。
5. 若 `strict=true` 且前面都失败，抛出异常。

## 新增一个 Prompt 的标准流程
1. 在 `services/python_grpc/src/content_pipeline/prompts/` 下新增模板文件（按场景分层）。
2. 在 `services/python_grpc/src/content_pipeline/infra/llm/prompt_registry.py` 中新增 `PromptKeys` 常量与注册项。
3. 在业务调用点改为 `get_prompt` 或 `render_prompt`，并保留合理 fallback。
4. 补充/更新测试（至少覆盖：正常加载、覆盖路径、缺失回退、严格模式）。

## 常见问题
### Q1：报错“Prompt key not found”
- 先检查 `prompt_registry.py` 是否注册该 key。
- 再检查调用处 key 是否拼写一致。

### Q2：变量渲染失败（缺少占位符变量）
- 检查模板中的 `{var}` 是否都在 `context` 中提供。
- 在关键路径建议加默认 fallback，避免线上硬失败。

### Q3：外部覆盖没有生效
- 检查 `prompt_management.enabled` 是否为 `true`。
- 检查 `root_dir`/`overrides` 路径是否真实可读。
- 清理进程缓存后重试（如测试中可调用 `clear_prompt_loader_cache()`）。

## 回归测试
建议最小回归集合：
- `services/python_grpc/src/content_pipeline/tests/test_prompt_loader.py`
- `services/python_grpc/src/content_pipeline/tests/test_knowledge_classifier_config_path.py`
- `services/python_grpc/src/content_pipeline/tests/test_vl_tutorial_flow.py`

## Prompt 模板编写规范
### 1) 命名与目录
- 按“模型域 + 业务场景 + 角色”组织目录，例如：`deepseek/knowledge_classifier/batch_user.txt`。
- 文件名优先使用 `system.txt`、`user.txt`、`constraints.txt` 等稳定命名，减少歧义。
- 一个 PromptKey 只绑定一个模板文件，避免同义重复模板。

### 2) 变量占位符
- 统一使用 Python `str.format` 占位符：`{var_name}`。
- 占位符命名使用 `snake_case`，语义清晰，禁止单字母变量（如 `{x}`）。
- 新增占位符时，必须同步更新调用点 `context` 与测试用例。
- 对可选变量，建议在调用点准备默认值，避免渲染失败。

### 3) 内容结构
- 推荐模板结构：角色定义 -> 输入说明 -> 输出格式 -> 硬约束 -> 反例/禁止项。
- 输出格式尽量“机器可解析”，优先 JSON 字段清单，避免自然语言歧义。
- 约束条目编号化（1/2/3...），便于排查模型偏差。
- Few-shot 示例尽量短小且贴近当前任务，避免大段历史样例。

### 4) 兼容与安全
- 默认保持 fallback 可用：即使模板缺失也不应阻塞主链路。
- 在 `strict=true` 环境中，模板必须完整可读；新增模板前先在 CI 校验。
- 模板中不得包含真实密钥、账号、URL token 等敏感信息。
- 涉及路径或命令时，使用中性示例，不写本机私有路径。

### 5) 评审清单（提交前）
- 是否已在 `prompt_registry.py` 注册新 key 与默认路径。
- 是否已在调用点改为 `get_prompt`/`render_prompt` 并提供 fallback。
- 是否补充了至少 1 条加载测试与 1 条渲染测试。
- 是否验证 `overrides` 与 `strict` 行为不破坏现有流程。
- 是否更新架构文档（`docs/architecture/upgrade-log.md` 必填）。

### 6) 推荐模板骨架
```text
[Role]
你是...

[Input]
- 字段A: {field_a}
- 字段B: {field_b}

[Output]
仅输出 JSON 数组，每个对象字段：id, type, start_sec, end_sec。

[Hard Constraints]
1) 禁止输出 Markdown 代码块。
2) 禁止输出解释性文本。
3) 时间字段必须为非负浮点数。
```

