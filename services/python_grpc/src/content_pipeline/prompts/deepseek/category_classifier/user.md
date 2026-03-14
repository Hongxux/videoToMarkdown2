## 输入参数信息

- **视频标题**:
{{video_title}}

- **第一段核心正文内容**:
{{first_unit_text}}

- **视频的所有大纲模块分组名列表**:
{{group_names}}

- **现有分类路径库**（一行一个，格式：级1/级2/...）:
{{categories}}

- **目标分类层级深度**（target_level）:
{{target_level}}

- **允许的最大分类层级深度**（max_target_level）:
{{max_target_level}}

- **父目录约束**（required_prefix；为空表示无父目录约束）:
{{required_prefix}}

---

请根据上述信息，理解视频的核心知识领域，并从现有分类路径库中挑选最精准的分类。
如果现有分类无法精准覆盖视频内容的核心主题，或者现有分类不符合 MECE 规则，你必须基于既有的分类原则和命名规范，自行创建一个新的分类路径。

输出的 `category_path` 必须严格满足以下要求：
- 深度必须 `>= target_level` 且 `<= max_target_level`
- 如果给出了父目录约束 `required_prefix`，则新路径必须以该父目录为前缀，并在其下继续细分
- 路径只表达知识领域，不混入教程形式、难度、年份或场景

请只输出包含 `category_path`、`is_new` 和 `reasoning` 的合法 JSON。
