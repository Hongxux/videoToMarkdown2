该提示词已废弃。

背景：
- Phase2B 已切换为 `knowledge_groups -> units` 输出。
- Markdown 组装固定为两级结构：`group_name`（一级）+ `unit.title`（二级）。
- 主流程不再调用 LLM 做层级分类。

兼容输出（仅供旧路径调用）：
```json
{
  "hierarchy": []
}
```
