# 全量重构方案：基于第一性原理的完整覆盖 (Comprehensive Refactoring Spec)

**核心第一性原理 (The Core First Principle)**:
*   **计算 (Compute)**: 依赖 GPU、向量运算、像素处理、特定 AI 库 (OpenCV, Torch) 的逻辑 -> **Python Worker (Stateless API)**。
*   **编排 (Orchestration)**: 依赖 业务状态、数据库、规则判断、流程控制 的逻辑 -> **Java Orchestrator (Spring Boot Service)**。
*   **无国界 (Border-less)**: 纯数学公式、字符串解析、配置读取 -> **优先 Java (性能与类型安全)**，除非逻辑极度依赖 Numpy。

---

## 1. 核心决策与编排 (Core Logic & Decisions)

这些模块是系统的"大脑"，包含复杂的 If-Else 业务规则。
**最佳实践**: 业务规则应放在静态类型语言中，利用设计模式 (Strategy/Chain) 确保可维护性。

| 文件名 | 原职责 | **新归属 (Target)** | **重构说明 (Refactoring Strategy)** |
| :--- | :--- | :--- | :--- |
| `multimodal_fusion.py` | 决策核心 | **Java Service** | 100% 重写。`FusionDecisionService`。这里是纯业务逻辑，不含像素计算。 |
| `dynamic_decision_engine.py` | 动静判定逻辑 | **Split (拆分)** | 计算部分 (MSE/SSIM) 留 Python；判定阈值逻辑 (Config Rules) 移至 Java。 |
| `fault_detector.py` | 文本断层检测 | **Split (拆分)** | 简单的规则检测移至 Java；涉及 NLP 语义的检测通过 Spring AI 调用 LLM。 |
| `material_optimizer.py` | 素材去重 | **Split (拆分)** | Python 算 Hash (特征)；Java 算 Hamming Distance 并管理去重列表。 |
| `confidence_calculator.py` | 加权打分 | **Java Util** | 纯数学加权公式。Java double 运算比 Python 更快且类型安全。 |
| `cognitive_demand_classifier.py` | 认知负荷分类 | **Python API / Java** | 如果基于规则，移至 Java；如果涉及 NLP 模型推理，留在 Python 供 Java 调用。 |

## 2. 特征提取与视觉计算 (Compute & Vision)

这些模块是"眼睛"，严重依赖 OpenCV 和 PyTorch。
**最佳实践**: 不要试图用 JavaCV 重写 OpenCV 逻辑，不仅慢而且难以维护。保持 Python 服务的形态。

| 文件名 | 原职责 | **新归属 (Target)** | **重构说明 (Refactoring Strategy)** |
| :--- | :--- | :--- | :--- |
| `visual_feature_extractor.py` | 视觉特征计算 | **Python Worker** | 核心计算单元。OpenCV 代码不动，包装为 FastAPI 接口。 |
| `screenshot_selector.py` | 选帧算法 | **Python Worker** | 涉及像素级操作 (Laplacian 算子)。保留在 Python。 |
| `video_clip_extractor.py` | 视频剪切 | **Python Worker** | FFmpeg 包装器。虽然 Java 也能调 FFmpeg，但 Python 处理多媒体路径更方便，建议保留。 |
| `visual_element_detection_helpers.py` | 形状检测 | **Python Worker** | `cv2.findContours` 等逻辑，必须留在 Python。 |
| `math_formula_visual_detector.py` | 公式检测 | **Python Worker** | 涉及特定的视觉模式识别，留在 Python。 |
| `semantic_feature_extractor.py` | 文本语义向量 | **Java (Spring AI)** | **关键变更**: 如果是调 OpenAI 接口，直接用 **Spring AI**。如果是本地 BERT，留 Python。鉴于目前趋势，**推荐切回 Java 调 LLM**。 |
| `ocr_utils.py` | 本地 OCR | **Python Worker** | 如果用 PaddleOCR/RapidOCR，必须 Python。 |
| `structure_dynamic_helper.py` | 结构化辅助 | **Python Worker** | 辅助 CV 算法，跟随 Vision 模块。 |

## 3. 数据与基础工具 (Data & Infrastructure)

这些模块负责搬运数据或加载配置。
**最佳实践**: Java 的 ORM (Hibernate/JPA) 和 Configuration (Spring Config) 远强于 Python 的字典一把梭。

| 文件名 | 原职责 | **新归属 (Target)** | **重构说明 (Refactoring Strategy)** |
| :--- | :--- | :--- | :--- |
| `data_loader.py` | 加载 JSON/Subtitles | **Java Repository** | 改用 `Jackson` + `Spring Data`。强类型解析更安全。 |
| `config_loader.py` | 加载 YAML | **Spring Config** | 改用 `@ConfigurationProperties`。Spring 自动处理 yaml注入，无需手写 loader。 |
| `llm_client.py` | 调用 LLM | **Spring AI** | **废弃**。使用 Spring AI (`ChatClient`) 统一管理 Prompt 和 Context。 |
| `resource_utils.py` | 路径管理 | **Java Service** | Spring Boot 的 ResourceLoader 更强大。 |
| `timestamp_mapper.py` | 时间戳对齐 | **Java Tuple/Util** | 简单的二分查找或线性插值，Java 实现。 |
| `data_structures.py` | 数据类定义 | **Java DTOs** | 转化为 Java POJOs / Records (`Lombok` @Data)。 |
| `fusion_helpers.py` | 杂项辅助 | **Distribute (打散)** |根据功能拆分到对应的 Java Util 类中。 |

## 4. 边缘模块 (Edge Cases)

| 文件名 | 原职责 | **新归属 (Target)** | **重构说明 (Refactoring Strategy)** |
| :--- | :--- | :--- | :--- |
| `text_generator.py` | 生成补全文案 | **Java Service (Spring AI)** | Prompt 工程。Java 处理 String Template 更规范。 |
| `material_validator.py` | 最终校验 | **Java Processor** | 此为"业务最后一道防线"，属于编排流程的一部分，移至 Java 责任链末端。 |
| `asr_utils.py` | 语音转文字 | **Python Worker / Service** | 如果是 Whisper 本地模型，留 Python；如果是云 API，移至 Java。 |
| `subtitle_utils.py` | 字幕解析 | **Java Parser** | 字符串处理，Java 正则/解析库性能更好。 |
| `onnx_exporter.py` | 模型导出 | **Deprecated (废弃)** | 服务化后无需导出 ONNX，Python 直接加载 PyTorch 模型即可。 |

---

## 5. 重构后的架构全景图

```mermaid
graph TD
    subgraph "Java Control Plane (Spring Boot)"
        Config[Spring Config]
        Repo[Data Repository]
        Service[FusionDecisionService]
        
        Validator[MaterialValidator]
        Optimizer[MaterialOptimizer (List Logic)]
        
        Service --> Validator
        Service --> Optimizer
    end
    
    subgraph "Spring AI"
        LLM[LLM Client (Semantic/TextGen)]
    end
    
    subgraph "Python Data Plane (FastAPI)"
        Visual[VisualFeatureExtractor]
        OCR[OCR Engine]
        Selector[ScreenshotSelector]
        Clipper[VideoClipper]
    end
    
    Config & Repo --> Service
    Service -->|Async HTTP| Visual
    Service -->|Async HTTP| OCR
    Service -->|Prompt| LLM
    Service -.->|Calc Hash| Optimizer
```
