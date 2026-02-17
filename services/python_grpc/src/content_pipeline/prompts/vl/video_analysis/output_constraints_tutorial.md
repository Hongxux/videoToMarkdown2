[硬性约束 - 教程分步模式]
1) 格式要求: 必须输出且仅输出一个标准的 JSON 数组。严禁使用 Markdown 代码块 (```json)，严禁包含任何前缀、后缀或解释性文字。
2) 语言要求: JSON 中的所有文本字段（如 description, action 等）必须使用中文。
3) 结构完整性: 数组中的每一项必须代表一个完整的教学步骤。
4) 字段定义:
   - 必填:
     - step_id (Integer)
     - step_description (String): 详细描述该步骤的操作内容。
     - clip_start_sec (Float): 步骤开始时间。
     - clip_end_sec (Float): 步骤结束时间。
     - instructional_keyframe_timestamp (List[Float]): 关键帧时间点。
   - 可选 (若无内容，请直接在 JSON 中省略该字段):
     - main_action (String): 核心动作摘要。
     - main_operation (List[String]): 具体操作点列表。
     - precautions (List[String]): 易错点或注意事项。
     - step_summary (String): 步骤一句话总结。
     - operation_guidance (List[String]): 操作指引。
     - no_needed_video (Boolean): 是否不需要视频表达。
     - should_type (String): 路由覆盖类型，仅允许 abstract / concrete。
5) 禁令: 严禁输出 reasoning, key_evidence 或 knowledge_type 字段。
6) 分段逻辑:
   - 完整性: 同一步骤的“原理解释 + 操作执行 + 结果反馈”必须合并在一段。
   - 紧凑性: 剔除无效的思考时间、鼠标游走或无信息的静默片段。
   - 时长限制: 单个步骤不得少于 5 秒。短步骤必须与相邻步骤合并。
7) 关键帧质量: 必须选择清晰展示操作结果或关键输入状态的帧（如点击提交前的填写完成状态）。
8) 时间戳规范: 必须使用从 0.0 开始的相对时间。严禁使用 -1。如果动作贯穿全片，使用 [0.0, 视频总时长]。
9) no_needed_video 判定规则:
   - 若该步骤不存在有价值的动态展示，且仅靠文字即可完整传达信息，必须返回 no_needed_video=true。
   - 若该步骤中的动态演示对理解或复现有价值，返回 no_needed_video=false。
10) should_type 路由覆盖规则（可选）:
   - should_type=abstract: 按 abstract 路由处理。
   - should_type=concrete: 按 concrete 路由处理。
   - 若 no_needed_video=true，则应等价按 abstract 路由处理（覆盖优先级最高）。
