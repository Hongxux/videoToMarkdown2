# 函数级重构规范：从脚本到服务

**日期**: 2026-01-27
**核心原则**: 
1.  **计算引力 (Computation Gravity)**: 繁重的数学运算/像素处理 -> 留在 Python。
2.  **逻辑引力 (Logic Gravity)**: 业务规则/状态流转/编排 -> 迁移至 Java。

---

## 1. 第一阶段：特征提取层 ("士兵"层)

**策略**: 将现有的 Python 类方法封装为无状态的 FastAPI 接口。

### 1.1 `VisualFeatureExtractor.extract_visual_features`
*   **现状**: Python 类方法
*   **目标**: `POST /api/v1/features/extract` (Python 服务)
*   **重构方案**:
    *   **保留**: 所有 OpenCV/Canny/SSIM 的计算逻辑。
    *   **变更**: 不再返回内存对象，而是返回一个兼容 Java `VisualFeatures` DTO 结构的 JSON 字典。
    *   **理由**: CPU 密集型任务。Java 做图像处理效率不如 OpenCV (C++) 封装好的 Python 库，且开发成本高。

### 1.2 `DynamicDecisionEngine.judge_is_dynamic`
*   **现状**: Python 类方法
*   **目标**: Python 内部私有函数
*   **重构方案**:
    *   **变更**: 该函数不再由"控制器"直接调用。它变为 `extract_visual_features` 的辅助函数。
    *   **输出**: 布尔值结果 (`is_dynamic`) 成为 1.1 接口 JSON 响应中的一个字段。

### 1.3 `ScreenshotSelector.select_screenshot`
*   **现状**: Python 类方法
*   **目标**: `POST /api/v1/material/select-frame` (Python 服务)
*   **重构方案**:
    *   **保留**: "岛屿聚类"、"熵计算"、"拉普拉斯方差"等逻辑。
    *   **变更**: 接收 `video_path` 和 `time_window`。将选中的最佳帧保存到共享存储 (NAS/本地)，接口仅返回 `{ "path": "/data/frames/img_001.jpg", "dhash": "10110..." }`。
    *   **理由**: 涉及像素级的选取和裁剪操作。

---

## 2. 第二阶段：决策核心层 ("指挥官"层)

**策略**: 将 Python 中面条式的 "If-Else" 逻辑重写为 Java 的 "责任链模式"。

### 2.1 `MultimodalFusionDecider.decide_enhancement_type_refined`
*   **现状**: 复杂的 Python 逻辑
*   **目标**: `Java Service: FusionDecisionService.decideAsync()`
*   **重构方案**:
    *   **动作**: **Java 完全重写**。
    *   **实现**: 这是编排入口，负责使用 `CompletableFuture` 并行调用 Python 接口，然后触发决策链。

### 2.2 `MultimodalFusionDecider._check_noise_filter`
*   **现状**: Python 辅助函数
*   **目标**: `Java Class: NoiseFilterProcessor`
*   **重构方案**:
    *   **代码迁移**:
        ```java
        // Python: if visual.is_dynamic and semantic.is_abstract: return Noise
        // Java:
        if (ctx.getVisual().isDynamic() && "abstract".equals(ctx.getSemantic().getType())) {
            ctx.setDecision(EnhancementType.TEXT); // 标记为噪音
        }
        ```

### 2.3 `MultimodalFusionDecider._make_final_decision` (核心规则)
*   **现状**: Python 辅助函数
*   **目标**: `Java Class: TypeAnchoringProcessor`
*   **重构方案**:
    *   **动作**: 将硬编码规则（如 "process" -> VIDEO）转化为 Java 逻辑步骤。
    *   **扩展**: 此处应用 **策略模式 (Strategy Pattern)**，以便未来区分数学课和代码课的不同判断标准。

### 2.4 `confidence_calculator.py`
*   **现状**: Python 逻辑
*   **目标**: `Java Utility: ConfidenceUtils`
*   **重构方案**:
    *   **动作**: Java 重写。
    *   **理由**: 纯数学公式（加权平均），Java 运行速度很快，没必要为了简单的算术去调用 Python。

---

## 3. 第三阶段：优化与去重层 ("分析师"层)

**策略**: 混合模式。Java 管理列表，Python 提供指纹。

### 3.1 `MaterialOptimizer.optimize_enhancements` (主循环)
*   **现状**: Python 逻辑
*   **目标**: `Java Service: MaterialOptimizationService.optimize()`
*   **重构方案**:
    *   **动作**: 循环逻辑在 Java 重写。Java 持有 `Enhancement` 对象列表，决定删除哪一个。

### 3.2 `MaterialOptimizer._cluster_by_visual_invariants` (重活)
*   **现状**: Python 逻辑 (dHash, 直方图)
*   **目标**: **逻辑拆分 (Split)**
    *   **部分 A (计算哈希)**: **Python**。在阶段 1.3 (截图选择) 时，顺便计算出图片的 `dhash` 和 `histogram`，放入元数据返回。
    *   **部分 B (比对哈希)**: **Java**。
        *   Java 读取元数据中的 `dhash` 字符串。
        *   Java 计算两个字符串的汉明距离 (位运算 XOR)。Java 做这个极快。
    *   **收益**: Java 实现了 "O(1)" 级别的快速比对，完全不需要加载图片文件！实现了"特征提取"与"相似度比对"的完美解耦。

---

## 4. 总结对照表

| 函数名 | 原归属 (Python) | 新归属 | 类型 |
| :--- | :--- | :--- | :--- |
| `extract_visual_features` | `VisualFeatureExtractor` | **Python API** | 纯计算 |
| `select_screenshot` | `ScreenshotSelector` | **Python API** | 纯计算 |
| `decide_enhancement_type` | `MultimodalFusionDecider` | **Java Service** | 业务逻辑 |
| `_check_noise_filter` | `MultimodalFusionDecider` | **Java Processor** | 业务逻辑 |
| `calculate_ocr_confidence` | `ConfidenceCalculator` | **Java Util** | 简单数学 |
| `optimize_enhancements` | `MaterialOptimizer` | **Java Service** | 业务逻辑 |
| `calc_image_hash` | `MaterialOptimizer` | **Python API** | 纯计算 |
| `compare_image_hash` | `MaterialOptimizer` | **Java Service** | 业务逻辑 (位运算) |

**结论**: 像素留在 Python，规则和列表移到 Java。在 Python 端预计算特征（如 Hash），让 Java 成为高效的比较引擎。
