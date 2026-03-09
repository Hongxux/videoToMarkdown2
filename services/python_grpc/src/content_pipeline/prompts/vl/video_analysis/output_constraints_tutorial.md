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
      - instructional_keyframes (List[Object]): 该步骤中最重要的截图凭证。必须选取最能代表该步骤结果的瞬间。
        - keyframe_id (Integer): 截图编号（N从1开始递增），必须与 `main_operation` 里的 `[KEYFRAME_{N}]` 占位符的 N 保持一致。
       - timestamp_sec (Float): 关键帧精确相对时间（秒）。**核心红线：截图在精不在多。** 只有那些在脱离了视觉辅助后难以通过纯文本讲清的内容，才配被截图！**极度重要：对于能够用文字完全表达清楚逻辑关系的内容，坚决不要使用图片，必须极力避免图片冗余！你必须着重截取旨在向读者展示元素之间「空间相对关系」或「逻辑相对关系」的画面。**必须用最少、最清晰的图覆盖最多信息。绝对禁止截取空白界面、被遮挡、被极度放大失去上下文、模糊过渡或被选定反色污染的劣质画面。
        - frame_reason (String): 描述如何引导用户看这张图，**此处的引导性词语必须与之对应步骤的 `main_operation` 讲解内容紧密配合，以辅助 `main_operation` 讲解为核心目的。**语气必须模仿讲师口吻（例如：“大家请看画面左侧...”）。先指出需要看的信息的相对位置（借助元素、文字和在图片中的大体位置），说明需要看的信息的内容是什么，以及它如何作证/呼应正文的操作指引。用于 Markdown 图注。
   - 可选 (若无内容，请直接在 JSON 中省略该字段):
      - instructional_clips (List[Object]): 动态视频剪辑片段，当静态关键帧无法表达过程时使用。
        - clip_id (Integer): 片段编号（N从1开始递增），与 `main_operation` 里的 `[CLIP_{N}]` 占位符保持高度一致。
        - start_sec (Float): 开始相对秒数。
        - end_sec (Float): 结束相对秒数。**绝对最高红线：end_sec 减去 start_sec 必须 <= 5.0**！
        - clip_reason (String): 描述如何引导用户看这段动画/视频，**此处的引导性词语必须与之对应步骤的 `main_operation` 讲解内容紧密配合，以辅助 `main_operation` 讲解为核心目的。**语气必须模仿讲师口吻（例如：“大家请看右上角指示灯的变化...”）。先指出需要关注的动态元素和过渡过程发生的大体位置，说明这段动画展现了什么连贯性原理、UI 过渡或逻辑流转关系，以及它如何作证/呼应正文的操作指引。用于辅助 Markdown 视频注。
      - main_action (String): 核心动作摘要。
     - precautions (List[String]): 易错点或注意事项。
     - step_summary (String): 步骤一句话总结。
     - operation_guidance (List[String]): 操作指引。
5) 禁令: 严禁输出 reasoning, key_evidence 或 knowledge_type 字段。
6) 分段逻辑:
   - 完整性: 同一步骤的“原理解释 + 操作执行 + 结果反馈”必须合并在一段。
   - 紧凑性: 剔除无效的思考时间、鼠标游走或无信息的静默片段。
   - 时长限制: 单个步骤不得少于 5 秒。短步骤必须与相邻步骤合并。
7) 关键帧质量: 必须选择清晰展示操作结果或关键输入状态的帧（如点击提交前的填写完成状态）。
8) 时间戳规范: 必须使用从 0.0 开始的相对时间。严禁使用 -1。如果动作贯穿全片，使用 [0.0, 视频总时长]。
9) 严格输出示例 (请深刻理解其结构与占位符映射关系):
```json
[
  {
    "step_id": 1,
    "step_type": "MAIN_FLOW",
    "step_description": "修改 IDE 代理设置",
    "main_operation": "**本步目标**：为 Android Studio 配置本地代理，应对国内网络环境。\n\n1. 点击菜单栏 **File -> Settings**。这里建议先配好本地代理映射，以免后续下载依赖时卡死。\n   [KEYFRAME_1]\n2. 在搜索框输入 `Proxy`，将 IP 改为 `127.0.0.1`，点击 Apply。\n   [KEYFRAME_2]\n3. 观察右上角的网络请求指示灯，确保其由红变绿，意味着代理链路握手彻底完成。\n   [CLIP_1]\n\n**预期反馈**：能够看到 Proxy 界面下方的连通性测试显示为绿色 Success，且指示灯变为绿色。",
    "clip_start_sec": 10.0,
    "clip_end_sec": 20.0,
    "instructional_keyframes": [
      {
        "keyframe_id": 1,
        "timestamp_sec": 12.5,
        "frame_reason": "大家请看画面顶部偏左的工具栏区域，点击 File 后展开的下拉菜单中包含 Settings 选项。看这张图是为了明确 File 与 Settings 的空间层级关系，辅助大家快速定位入口"
      },
      {
        "keyframe_id": 2,
        "timestamp_sec": 18.0,
        "frame_reason": "大家请看画面中部的代理配置面板，这里有中部的 IP 输入框、底部的 Apply 按钮和右侧结果反馈的 Success 文字。看这张图是为了确认填写后的最终状态，为您提供完整的视觉核对标准"
      }
    ],
    "instructional_clips": [
      {
        "clip_id": 1,
        "start_sec": 18.5,
        "end_sec": 19.5,
        "clip_reason": "大家请看右上角的网络请求指示灯，这里必须要用动态视频，才能向大家清晰展示指示灯由红变绿的流转过渡态过程，请注意结合刚刚输入的操作感受状态的切换脉络"
      }
    ]
  }
]
```
