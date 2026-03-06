[硬性约束 - 教程分步模式]
1) 格式要求: 必须输出且仅输出一个标准的 JSON 数组。严禁使用 Markdown 代码块 (```json)，严禁包含任何前缀、后缀或解释性文字。
2) 语言要求: JSON 中的所有文本字段（如 description, action 等）必须使用中文。
3) 结构完整性: 数组中的每一项必须代表一个完整的教学步骤。
4) 字段定义:
   - 必填:
     - step_id (Integer)
      - step_type (String): 【必填】严格从以下枚举中选取其一：`MAIN_FLOW` (主流程，必须执行的关键线性步骤), `CONDITIONAL` (条件分支，仅在特定情况或特定版本下才需要的步骤), `OPTIONAL` (可选操作，锦上添花的设置，跳过也不影响最终目标), `TROUBLESHOOTING` (排错处理，视频中展示了报错/失败情况并演示了如何修复)。
      - step_description (String): 该步骤的简短说明（仅作为本步骤的短标题）。
      - main_operation (String): 【必填】完全替代视频演示的核心教学文本（Markdown 格式）。结构上，你必须在整段文本最开头写明目标（如 `** 本步目标**：...`），在最后结尾写明预期结果（如 `** 预期反馈**：操作后的界面变化...`），在两者之间使用编号列表写出具体的实操点击路径和参数，并请极度自然地将**“讲解者的解释和原因”**杂揉在这几步操作文本中。**如果 `step_type` 不是主干流程，必须在最上方显眼处写明触发条件（例如：“**【遇到报错时执行：如果终端提示...】**”）。** 必须在需要的步骤后精准嵌入图片占位符 `[KEYFRAME_{N}]`（N从1开始）。注意：`instructional_keyframes` 数组里的所有图片，都必须有对应的 `[KEYFRAME_{N}]` 占位符去承载！这绝对不能遗漏！
      - clip_start_sec (Float): 步骤开始时间。
      - clip_end_sec (Float): 步骤结束时间。
      - instructional_keyframes (List[Object]): 该步骤中最重要的截图凭证。必须选取最能代表该步骤结果的瞬间。列表中的第 N 个元素必须严格对应 `main_operation` 里的 `[KEYFRAME_{N}]` 占位符。
       - timestamp_sec (Float): 关键帧精确相对时间（秒）。**核心红线：截图在精不在多。** 只有那些在脱离了视觉辅助后难以通过纯文本讲清的内容，才配被截图！**你必须着重截取旨在向读者展示元素之间「空间相对关系」或「逻辑相对关系」的画面。**必须用最少、最清晰的图覆盖最多信息。绝对禁止截取空白界面、被遮挡、被极度放大失去上下文、模糊过渡或被选定反色污染的劣质画面。
        - frame_reason (String): 描述这张图**为什么不可或缺**（即它展示了怎样难以用纯文字描述的空间相对关系或逻辑交互，从而必须配图）。用于 Markdown 图注。
        - target_ui_type (String): 目标 UI 元素的具体类型（例如：提交按钮、输入框、下拉菜单栏、侧边栏列表项等）。
        - target_text (String): 目标区域囊括的具体文本内容（必须如实记录画面上的字眼，以便进行后续的 OCR 断言或网格匹配，如："Confirm"、"Proxy"、"File"）。
        - target_relative_position (String): 目标在整个画面中的大致方位（如“屏幕右上角”）以及与其他显著 UI 模块的空间关系（如“位于左侧导航树的最下方”、“在 Cancel 按钮的右侧”）。该语义特征将作为 Stage 2 视觉网格锚定的关键线索。
   - 可选 (若无内容，请直接在 JSON 中省略该字段):
     - main_action (String): 核心动作摘要。
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
    
11) 严格输出示例 (请深刻理解其结构与占位符映射关系):
```json
[
  {
    "step_id": 1,
    "step_type": "MAIN_FLOW",
    "step_description": "修改 IDE 代理设置",
    "main_operation": "**本步目标**：为 Android Studio 配置本地代理，应对国内网络环境。\n\n1. 点击菜单栏 **File -> Settings**。这里建议先配好本地代理映射，以免后续下载依赖时卡死。\n   [KEYFRAME_1]\n2. 在搜索框输入 `Proxy`，将 IP 改为 `127.0.0.1`，点击 Apply。\n   [KEYFRAME_2]\n\n**预期反馈**：能够看到 Proxy 界面下方的连通性测试显示为绿色 Success。",
    "clip_start_sec": 10.0,
    "clip_end_sec": 20.0,
    "instructional_keyframes": [
      {
        "timestamp_sec": 12.5,
        "frame_reason": "展示顶部工具栏中 File 与下拉菜单中 Settings 的空间层级关系，辅助快速定位入口",
        "target_ui_type": "菜单项",
        "target_text": "Settings",
        "target_relative_position": "位于屏幕顶端工具栏偏左，File 下拉菜单中"
      },
      {
        "timestamp_sec": 18.0,
        "frame_reason": "展示 Proxy 配置面板中 IP 输入框、底部的 Apply 按钮与右侧 Success 反馈的同屏逻辑关联，提供填写后的视觉核对标准",
        "target_ui_type": "配置表单及按钮",
        "target_text": "127.0.0.1, Apply, Success",
        "target_relative_position": "画面中部的代理配置面板，要求包含输入框、底部的 Apply 按钮和右侧结果反馈文字"
      }
    ]
  }
]
```
