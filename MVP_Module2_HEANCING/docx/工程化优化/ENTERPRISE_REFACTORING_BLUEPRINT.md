# Enterprise Refactoring Blueprint: "Symphony" Architecture

**Date**: 2026-01-27
**Target**: MVP Module 2 (Content Enhancement)
**Objective**: Transform from Script-based Pipeline to Enterprise Microservices

## 1. 核心结论

**可以彻底重构。**

不仅可以，而且为了达到企业级交付标准（高并发、可观测、易维护），**必须**引入 Spring Boot + RabbitMQ 的架构。

但重构的定义不是 "把 Python 代码翻译成 Java"，而是 **"控制面 (Control Plane) 与数据面 (Data Plane) 的彻底分离"**。

---

## 2. 技术选型必要性分析 (Technology Stack Analysis)

### 2.1 Spring Boot (The Conductor / 指挥家)
*   **必要性: ★★★★★**
*   **角色**: 整个系统的 "大脑"。
*   **理由**:
    *   Python 脚本适合 "跑一次"，但在 "Web 服务化" 方面（连接池管理、请求上下文、安全认证）远不如 Java 生态成熟。
    *   这是你面试时展示 "后端工程能力" 的核心载体。

### 2.2 RabbitMQ (The Nervous System / 神经系统)
*   **必要性: ★★★★★**
*   **角色**: 异步通信与削峰填谷。
*   **理由**:
    *   **异构语言通信的最佳桥梁**: Java 发送消息，Python 监听消费，完美解耦。
    *   **缓冲**: 视频处理是慢任务（分钟级），高并发请求进来时，MQ 保证系统不崩，Python Worker 慢慢处理。
    *   **资源隔离**: 可以精确控制 CV 任务的并发数（Consume Rate），防止 OOM。

### 2.3 Redis (The Short-term Memory / 海马体)
*   **必要性: ★★★★☆**
*   **角色**: 任务状态与特征缓存。
*   **理由**:
    *   **特征缓存**: 同一个视频的不同片段可能复用相同的 OCR/Visual Features。Redis 避免 Python 重复计算。
    *   **进度条**: 前端轮询 "任务进度 80%"，这个状态应该存在 Redis 里，而不是查数据库。

### 2.4 MySQL (The Cortex / 大脑皮层)
*   **必要性: ★★★★☆**
*   **角色**: 决策归档与长久记忆。
*   **理由**:
    *   **复盘与优化**: 存储所有的 `MultimodalDecision` 日志。面试官会问："你怎么知道你的决策模型准不准？" 答："我把所有决策理由存入 MySQL，定期人工抽检回溯。"
    *   **结构化业务数据**: 增强后的文档结构、素材引用关系。

### 2.5 Spring AI (The Language Center / 语言中枢)
*   **必要性: ★★★☆☆ (取决于 LLM 调用路径)**
*   **角色**: 统一的 LLM 接口层。
*   **理由**:
    *   如果 LLM 是通过 HTTP API (OpenAI/Gemini) 调用，**强烈建议使用 Spring AI**。Java 的并发 IO 优势比 Python request 强，且 Spring AI 提供了很好的 Prompt 模板管理。
    *   **架构变更**: `text_generator.py` 和 `semantic_feature_extractor.py` 中的 LLM 调用部分可以直接迁移到 Java，只有 CV 部分保留在 Python。

---

## 3. "Sichuan Opera" (川剧变脸) 重构方案

我们将系统重构为 **Java Orchestrator (编排者)** + **Python Workers (执行者)** 的模式。

### 3.1 架构图

```mermaid
graph TD
    Client[Web/Mobile Client] -->|Upload Video| Gateway[Spring Boot Controller]
    
    subgraph "Java Core (Spring Boot)"
        Gateway --> Service[Orchestration Service]
        Service -->|Task Created| MySQL
        Service -->|Update Status| Redis
        
        Service -->|Message: EXTRACT_FEATURES| MQ_Extract[RabbitMQ: cv.extract]
        Service -->|Direct Call| SpringAI[Spring AI Service]
    end
    
    subgraph "Python Workers (Executors)"
        MQ_Extract -->|Consume| Py_Worker1[Python CV Worker]
        Py_Worker1 -->|Process Video| OpenCV[OpenCV / Torch]
        Py_Worker1 -->|Features JSON| Redis
        Py_Worker1 -->|Callback: DONE| MQ_Callback[RabbitMQ: task.callback]
    end
    
    subgraph "Decoupled Logic"
        MQ_Callback -->|Consume| Java_Decision[Java Decision Listener]
        Java_Decision -->|Read Features| Redis
        Java_Decision -->|Run Logic| FusionEngine[FusionDecisionService (Java)]
        FusionEngine -->|Result| MySQL
    end
```

### 3.2 详细流程

1.  **提交任务**: 用户上传视频，Java 端生成 `TaskId`，写入 MySQL (Status: PENDING)，并返回 ID 给前端。
2.  **异步分发**: Java 发送消息 `{taskId, videoPath, timeRange}` 到 RabbitMQ 队列 `queue.cv.extract`。
3.  **Python 执行**:
    *   Python 守护进程 (`worker.py`) 监听队列。
    *   收到消息，调用 `VisualFeatureExtractor` 和 `OCR`。
    *   **关键改变**: Python **不** 做决策，只产出数据。
    *   Python 将提取的特征 (`VisualFeatures`, `OCRText`) 序列化存入 Redis (`key: task:features:{id}`)。
    *   Python 发送 "完成信号" 到 `queue.task.callback`。
4.  **Java 决策**:
    *   Java 监听 `queue.task.callback`。
    *   从 Redis 读取特征数据。
    *   调用 **Spring AI** 并发获取语义特征 (Semantic Feature)。
    *   执行我们刚才写的 `FusionDecisionService` (责任链 + 策略模式) 算出结果。
5.  **结果落地**: Java 将最终决策写入 MySQL，并生成 Markdown 文件。

---

## 4. 关键代码迁移路线

1.  **保留 Python**:
    *   `visual_feature_extractor.py` (OpenCV, heavy math)
    *   `screenshot_selector.py` (Pixel-level manipulation)
    *   `video_clip_extractor.py` (FFmpeg wrapper)
    
2.  **迁移至 Java (Spring Boot/AI)**:
    *   `multimodal_fusion.py` -> `FusionDecisionService.java`
    *   `dynamic_decision_engine.py` -> `DecisionProcessors` (Logic)
    *   `llm_client.py` -> **Spring AI System** (ChatClient)
    *   `data_loader.py` -> **Spring Data JPA / RedisTemplate**

## 5. 总结

这套方案将使你的项目从一个 "脚本工具" 跃升为 "分布式智能处理系统"。
它完美契合面试中对 **分布式架构、异步解耦、微服务拆分、AI 工程化** 的所有考察点。
