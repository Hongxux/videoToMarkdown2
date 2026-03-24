CV 模块检测到以下语义单元存在“跨模态冲突”，请结合视觉锚点信息重新审视。

## 冲突单元信息
- **Unit ID**: {unit_id}
- **当前文本**: "{text}"
- **当前时序**: {start_sec:.1f}s - {end_sec:.1f}s
- **预判类型**: {llm_type}
- **视觉统计**: 稳定={s_stable:.0%}, 动作={s_action:.0%}, 冗余={s_redundant:.0%}
- **视觉锚点**: {anchors}（这些时间点发生了显著视觉状态切换）
- **冲突原因**: {reason}

## 决策选项
### 1. Split（强制拆分）
- 场景：文字明显对应不同视觉状态（如前半概念讲解、后半操作演示）
- 视觉锚点确实是知识点自然分界线
- 返回拆分点时间戳（必须接近某个视觉锚点）

### 2. Adjust（边界微调）
- 场景：单元核心语义完整，但首尾包含无关转场/冗余画面
- 收缩或扩展 start_sec/end_sec 以避开冗余

### 3. Keep（保持原判）
- 场景：视觉变化仅是 PPT 动画或无关干扰，文本语义不可分割
- 维持原时序，标记为跨模态可接受差异

## 输出格式（JSON）
```json
{{
  "decision": "split" | "adjust" | "keep",
  "rationale": "决策理由（20字以内）",
  "split_point": 12.5,
  "new_timeline": [10.0, 25.0]
}}
```

注意：
- split 时必须提供 split_point（秒）
- adjust 时必须提供 new_timeline（[start, end]）
- keep 时两者都不需要

请输出 JSON 决策：
