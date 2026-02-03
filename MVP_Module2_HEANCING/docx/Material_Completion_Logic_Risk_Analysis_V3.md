# 素材补全逻辑全链路解析与风险评估 (Current Implementation V3)

本文档旨在梳理当前系统（Optimization V3）中判定素材补全形式（Refinement Type）的完整逻辑链路，并识别潜在的准确性风险点。

## 一、 完整判定逻辑链路 (The Decision Pipeline)

判定过程是一个**从“认知意图”到“实证匹配”的漏斗型筛选流程**：

### 第一阶段：认知需求预判 (Cognitive Demand Classification)
**核心组件**: `CognitiveDemandClassifier.classify(fault_text)`
**决策优先级**:
1.  **认知分层 (Cognitive Layering)**: 基于布鲁姆教育目标分类（最高优先级）。
    *   **应用层 (Apply)**: 关键词(操作/遍历/演示) $\rightarrow$ **VIDEO** (主)
    *   **理解层 (Understand)**: 关键词(结构/原理/组成) $\rightarrow$ **SCREENSHOT** (主)
    *   **记忆层 (Memory)**: 关键词(定义/概念) $\rightarrow$ **TEXT** (主)
2.  **SRL 结构模拟 (SRL Heuristics)**: 若分层未命中，分析谓词-论元结构。
    *   谓词(遍历/点击) $\rightarrow$ **VIDEO**
    *   谓词(包含/位于) + 论元(结构/图) $\rightarrow$ **SCREENSHOT**
3.  **传统词典兜底 (Legacy Keywords)**: 关键词计数统计。
4.  **LLM 兜底**: 若上述规则均失效，调用 LLM 进行最终判定。

### 第二阶段：第一性原理视觉修正 (Visual First-Principles Override)
**核心组件**: `MaterialValidator.validate_for_demand`
在此阶段，我们引入**视觉特征作为绝对真理**，修正文本判定的偏差：
*   **数学公式修正**: 即使文本判定为 Text/Video，若 `VisualElementDetector` 检测到 **分数线 (Fraction)** 或 **上下标 (Superscript)** 特征，强制修正需求为 **SCREENSHOT**。
    *   *原理*: 数学公式的本质是空间结构，文字无法有效传达。

### 第三阶段：多模态实证验证 (Multimodal Empirical Validation)
**核心组件**: `MaterialValidator` (V3 Fusion Scoring)
根据预判的需求类型，计算素材的匹配得分 (MatchScore)。
**公式**: $$Score = 0.3 \times S_{text} + 0.5 \times S_{visual} + 0.2 \times S_{audio}$$

| 维度 | 计算逻辑 (Proxy) | 逻辑意图 |
| :--- | :--- | :--- |
| **$S_{text}$ (30%)** | 语义相似度 (BERT) | 确保补全内容与断层文本语义相关。 |
| **$S_{visual}$ (50%)** | **Video**: 6类动作特征 (PageTurn, Cursor, etc.)<br>**Screenshot**: 5类结构特征 (Tree, Grid, Hierarchy) | 验证画面是否具备承载知识的物理能力。 |
| **$S_{audio}$ (20%)** | **语速 (Speech Rate)** | **Video**: 高语速 (>5字/秒) 加分 (暗示操作演示)<br>**Screenshot**: 中语速 (2-5字/秒) 加分 (暗示解释说明) |

### 第四阶段：最终决策 (Final Decision)
**核心组件**: `MultimodalFusionDecider`
*   比较 Text, Screenshot, Video 三者的最终 Score。
*   **Argmax**: 选择得分最高的类型。
*   **阈值门控**: 若最高分 < 0.6，强制回退为 **TEXT** (保底策略)。

---

## 二、 潜在风险点与识别不准确的根源 (Risk Analysis)

尽管 V3 版本引入了多模态融合，但受限于工程实现的“代理指标 (Proxies)”，仍存在以下风险：

### 1. 认知分类层的风险
*   **多义词误判**: 
    *   *风险点*: 关键词如“结构”通常指静态图，但在代码讲解中可能指“动态构建结构的过程”。
    *   *后果*: **误判为 Screenshot**，导致丢失动态构建过程。
*   **SRL 规则简陋**: 
    *   *风险点*: 目前仅通过简单的列表匹配模拟 SRL，无法处理复杂句式（如倒装句、被动语态）。如“这个结构被用于演示循环”，主语是结构（静），但核心意图是演示（动）。
    *   *后果*: **分类摇摆不定**，可能回退到 Text。

### 2. 视觉修正层的风险
*   **公式检测的假阳性 (False Positive)**:
    *   *风险点*: `detect_math_formula` 依赖于“短横线”和“小框垂直偏移”。UI 界面中的 分隔线、下划线按钮 可能被误判为分数线。
    *   *后果*: **强制锁定 Screenshot**，即便画面是在播放视频 Demo。
*   **手绘/板书识别率低**:
    *   *风险点*: 目前的视觉检测器针对的是“机打/标准”的图形（矩形/圆/直线）。手绘的波浪线、不规则圆圈可能被忽略。
    *   *后果*: **$S_{visual}$ 得分过低**，导致本该选 Screenshot 的板书被降级为 Text。

### 3. 多模态验证层的风险 (最大风险源)
*   **音频分数的“语速代理”失效**:
    *   *风险点*: 使用“语速”代表“音频特征”是非常粗糙的代理。
        *   **反例 A**: 讲师在做复杂的代码演示（Video需求），但语速很慢，边想边敲。 -> **Audio Score 低，导致误判**。
        *   **反例 B**: 讲师在快速朗读一页静态 PPT（Screenshot需求）。 -> **Audio Score 误判为 Video**。
    *   *后果*: 音频特征可能成为干扰项，而非辅助项。
*   **视觉特征与语义的割裂**:
    *   *风险点*: $S_{visual}$ 只看“有没有框/有没有动”，不看“框里写了什么”。
    *   *后果*: 画面可能有一个无关的广告弹窗（检测到矩形），但与断层文本（讲二叉树）毫无关系。系统会因为“检测到矩形”而给 Visual 高分。

### 4. 决策阈值的风险
*   **一刀切的阈值 (0.6)**:
    *   *风险点*: 不同视频的质量差异巨大（清晰度、比特率）。低清视频的 MSE 和边缘检测效果差，得分普遍偏低。
    *   *后果*: **过度回退**。在低清视频中，即使真的是视频需求，也因为得分没过线而被强制转为 Text。

## 三、 改进建议 (Next Steps)
1.  **Audio**: 引入真正的音频事件分类 (Audio Event Classification)，识别键盘敲击声、鼠标点击声，而非仅靠语速。
2.  **Visual**: 引入 OCR 辅助验证，只有当视觉元素中的文字与断层文本有重叠时，才给予 Visual 高分（解决“无关弹窗”问题）。
3.  **Decision**: 将阈值改为动态阈值，基于视频整体质量基线进行浮动。
