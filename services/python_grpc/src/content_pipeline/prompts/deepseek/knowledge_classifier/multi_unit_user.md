请批量分析以下【多个语义单元】的动作单元。

## 重要约束
- 你必须返回 JSON 数组
- 每个结果对象必须包含字段：id / knowledge_type / confidence / reasoning / key_evidence
- 其中 id 必须严格等于输入 actions[*].id（格式形如 "SU001:action_1"），用于回填映射

## 输入数据（JSON）
{units_json}
