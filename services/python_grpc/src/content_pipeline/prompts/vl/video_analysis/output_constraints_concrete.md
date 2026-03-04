[硬性约束 - 具象化分析模式 (concrete)]
1) 格式: 必须且仅输出标准 JSON 数组。禁 Markdown 代码块 (```json)及前后缀释义。
2) 语言: JSON 文本字段必用中文。
3) 结构: 每一项代表一个核心视觉片段。
4) 字段定义:
   - 必填:
     - segment_id (Int): 序号，从 1 开始。
     - segment_description (Str): 短标题。
     - main_content (Str): 讲解与画面结合的富排版文本。用讲师第一人称。首行 `> **核心论点**：...`。用加粗标签+4空格缩进建逻辑树。涉视觉元素处句末精准插 `[KEYFRAME_{N}]`（N从1始），与截图数组一一对应，绝勿遗漏。
     - clip_start_sec (Float): 开始时间。
     - clip_end_sec (Float): 结束时间。
     - instructional_keyframes (List): 画面时间戳对象。第 N 个元素对应 `[KEYFRAME_{N}]`。
       - timestamp_sec (Float): 精确秒数。**在精不在多**，仅留全貌最终态，拒碎片。绝对禁止：1.初始空白 2.大部遮挡/极度放大防迷失 3.翻页模糊变动 4.反选光标污染或草稿。
         时机要求(优先定稿)：1.定稿(代码/推演出最终全貌结果) 2.巨变(首次切换新画面稳定时) 3.聚焦(被指示/高亮/放大) 4.定格(图表播完展现全貌)。
       - frame_reason (Str): 截图依据图注。
   - 可选 (若无则全省该键):
     - precautions (List[Str]): 易错点。
     - segment_summary (Str): 一句话总结。

5) 禁令: 严禁输出 reasoning/key_evidence/step_type 等无关内容。
6) 分段: 按逻辑/关键画面切换拆不重叠片。单片段可含多图。
7) 时间戳: 相对秒数，禁填-1。贯穿全片可填 [0.0, 视频总时长]。
    
8) 严格输出示例:
[
  {
    "segment_id": 1,
    "segment_description": "分布式缓存架构介绍",
    "main_content": "> **核心论点**：引入 Redis 缓存层是我们解决高并发瓶颈的关键战略。\n\n- **架构解析**：接下来我们一起来拆解如何通过 Redis 降低数据库压力。\n    - **原有直连痛点**：大家看左图，这是原有直连路线。高并发下受 I/O 限制，极易引发性能灾难。[KEYFRAME_1]\n    - **Redis 层流转**：来看新架构版图。请求处理变两段式：系统优先查此缓存，未命中再向底层库发起查询并回写，从而保护 DB。[KEYFRAME_2]",
    "clip_start_sec": 5.0,
    "clip_end_sec": 35.0,
    "instructional_keyframes": [
      {
        "timestamp_sec": 12.5,
        "frame_reason": "原有直连数据库逻辑图"
      },
      {
        "timestamp_sec": 28.0,
        "frame_reason": "引入 Redis 全新架构图"
      }
    ]
  }
]
