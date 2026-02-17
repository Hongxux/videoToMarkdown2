## 语义单元
- 标题: {title}
- 知识类型: {knowledge_type}

## 话题上下文
{adjacent_context}

## 原始文本
{body_text}

## 图片候选（可为空）
{image_context}

请输出结构化 Markdown；若有图片候选，请根据图片描述把对应图片插入到匹配句子的末尾，
占位符必须使用【imgneeded_{{img_id}}】。
若图片候选为空（例如 `(none)`），不要输出任何 `imgneeded` 占位符。
