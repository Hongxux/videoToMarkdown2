[硬性约束 - 默认模式]
1) 只输出一个标准 JSON，不要 Markdown 代码块，不要解释，不要前后缀文字。
2) 顶层必须是扁平 JSON 数组：[{...}, {...}]。
3) 每个对象必须包含字段：
   - id
   - knowledge_type
   - no_needed_video
   - confidence
   - clip_start_sec
   - clip_end_sec
   - suggested_screenshoot_timestamps
   可选字段：
   - should_type（仅允许 abstract / concrete）
4) 严禁输出 reasoning / key_evidence 字段，避免无关文本增加 token。
5) 时间边界规则：
   - 对于非“讲解型”内容，禁止随意输出 -1；请根据视觉变化尽力估算起止时间。
   - 若该知识类型贯穿整个片段，可设 [0.0, clip_duration]。
   - 仅在视觉信息完全无法支持判断时，才允许输出 -1。
6) no_needed_video 判定规则：
   - 若该片段不存在有价值的动态展示，且仅靠文字即可完整传达信息，必须返回 no_needed_video=true。
   - 若视频中的动态演示对理解或复现有价值，返回 no_needed_video=false。
7) should_type 路由覆盖规则：
   - should_type=abstract: 按 abstract 路由处理。
   - should_type=concrete: 按 concrete 路由处理。
