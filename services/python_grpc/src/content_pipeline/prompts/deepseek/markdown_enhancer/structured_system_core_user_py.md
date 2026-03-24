## 语义单元类型
{unit_type}

## 相邻上下文
{adjacent_context}

## 原始文本
{body_text}

## 图片候选
{image_candidates}

---

## 输出要求
1. 按内容主题拆成多个 section。
2. 为每个 section 标注：
   - `logic_tags`
   - `scene_tags`
   - `title`
3. `logic_tags` 仅可使用：
   - `parallel`
   - `hierarchical`
   - `causal`
   - `progressive`
   - `contrast`
   - `conditional`
4. `scene_tags` 仅可使用：
   - `technical`
   - `procedure`
   - `reading`
   - `narrative`
5. section 初稿必须使用缩进 Markdown，便于后续 Phase 2 skill 精修。
6. 如果图片候选为空（如 `(none)`），不要生成任何 `imgneeded` 占位符。

---

## 输出格式
先输出 `json` 代码块，再输出 `---`，再输出 section Markdown 初稿。
