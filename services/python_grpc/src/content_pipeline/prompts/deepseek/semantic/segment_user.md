请基于“语义闭环 + 知识类型纯度 + 时间连续性”进行语义分割，并按 Group 输出。

输出要求（严格）：
1. 只输出 JSON，不输出解释文本。
2. 顶层字段只能是 `knowledge_groups`。
3. 每个 Group 只能有字段：`group_name`, `reason`, `units`。
   - `group_name` 必须是**高度概括的主题短语**（限制15字内），**绝对禁止摘抄原文和使用省略号**。
   - 尽可能将连续的、讨论同一宏观话题的段落聚合成一个巨大的 Group，**切忌频繁新建 Group**。
4. 每个 Unit 只能有字段：`pids`, `k`, `m`, `title`。
5. `k` 只能是 `0/1/2`；`m` 只能是 `0/1`（整数）。
6. 同一知识类型且时间连续的段落必须合并为同一个 Unit。同一 Group 内不同类型的知识必须拆分为不同 Unit。
7. 严禁输出 `semantic_units`、`group_id`、`reasoning`、`confidence`、`text`、`full_text` 等字段。

输入段落：
{paragraphs_json}

输出模板：
{{
  "knowledge_groups": [
    {{
      "group_name": "CloudBot 环境配置",
      "reason": "围绕 CloudBot 从准备、配置到验证的同一核心论点",
      "units": [
        {{"pids": ["P001", "P002"], "k": 0, "m": 0, "title": "CloudBot 配置前置说明"}},
        {{"pids": ["P003", "P004"], "k": 2, "m": 1, "title": "配置 CloudBot 运行环境"}}
      ]
    }}
  ]
}}
