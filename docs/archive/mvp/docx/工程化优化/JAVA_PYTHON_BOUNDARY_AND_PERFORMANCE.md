# Java/Python 协同架构：职责界定与性能损耗分析

**日期**: 2026-01-27
**背景**: 已有 Java 后端与 前端，需完成 "Java 编排决策 - Python 执行算子" 的最后拼图。

---

## 1. 职责边界界定 (Responsibility Boundary)

为了避免代码逻辑混乱，我们采用 **"指挥官 - 士兵" (Commander - Soldier)** 模型来划分职责。

### 1.1 总体原则
*   **Java (指挥官)**: 负责 **"State (状态)"** 和 **"Why (业务逻辑)"**。它决定 *什么时候* 做 *什么事*，以及根据结果 *决定下一步*。
*   **Python (士兵)**: 负责 **"Stateless (无状态)"** 和 **"How (算法实现)"**。它只管接收指令，执行复杂的数学/图像计算，返回结果。

### 1.2 详细职责矩阵

| 维度 | Java Backend (Orchestrator) | Python Worker (Executor) |
| :--- | :--- | :--- |
| **核心关注点** | 业务流程、用户交互、数据一致性 | 图像处理、深度学习推理、向量计算 |
| **状态管理** | **Stateful**: 维护 `Order`, `Task`, `User` 状态 | **Stateless**: 纯函数式输入输出 (Input -> Output) |
| **决策权** | **Have Final Say**: 决定用视频还是截图，决定是否重试 | **No Decision**: 仅返回客观指标 (例: MSE=50, 包含箭头=True) |
| **依赖库** | JPA, Spring Security, Redis, WebClient | OpenCV, NumPy, PyTorch, FFmpeg |
| **并发模型** | IO 密集型 (Thread Pool / Reactive) | 计算密集型 (Process Pool / GPU) |
| **错误处理** | 决定是重试、报错还是降级 (Fallback) | 抛出具体的异常类型 (OOM, File Not Found) |

### 1.3 典型场景举例：Module 2 决策流

1.  **发起**: Java 收到请求，创建 Task，状态 `PENDING`。
2.  **指令**: Java 发送指令 `{action: "extract_visual_features", video_path: "/data/v1.mp4"}` 给 Python。
3.  **执行 (Python)**:
    *   读取视频，计算每一帧的 MSE、SSIM。
    *   **注意**: Python **不判断** "MSE > 50 就是动态"，Python 只返回 `{ "avg_mse": 55.2, "is_dynamic_candidate": true }`。
4.  **决策 (Java)**:
    *   Java 拿到 `{avg_mse: 55.2}`。
    *   Java 读取配置策略（可能是 "数学课 MSE>40 算动态"）。
    *   Java 结合语义特征（Semantic），最终拍板：**"这是一个动态视频"**。
5.  **落地**: Java 更新数据库。

---

## 2. 跨语言通信的性能损耗分析 (Performance Overhead Analysis)

跨语言调用 ("进程间通信", IPC) 确实比同语言调用慢，但在视频处理场景下，**这个损耗通常可以忽略不计**，前提是你设计得当。

### 2.1 损耗在哪里？

1.  **序列化/反序列化 (Serialization)**:
    *   Java 对象 -> JSON 字符串 -> Python 字典。
    *   **损耗**: 取决于数据量。如果只传配置参数，微秒级；如果传 Base64 图片，**灾难级**。
2.  **网络传输 (Network Latency)**:
    *   HTTP (Localhost): 约 1-2ms RTT。
    *   RabbitMQ: 约 1-3ms RTT。
    *   **结论**: 对于耗时 5秒~5分钟 的视频算法来说，2ms 的通信延迟是 **0.1%** 的开销，完全可接受。

### 2.2 性能陷阱与优化策略 (Performance Traps)

#### 🔴 陷阱 1: "Data Value" Passing (传值)
*   **错误做法**: Python 截取一张 1080P 图片，转成 Base64 字符串，塞在 JSON 里通过 HTTP 发给 Java。Java 再解码存文件。
*   **损耗**: 极高。1080P 图片约 5MB，Base64 后变 7MB。JSON 解析 RAM 消耗巨大，网络阻塞。
*   **后果**: Java GC 频繁，系统吞吐量暴跌。

#### 🟢 策略 1: "Data Reference" Passing (传引用)
*   **正确做法**: Python 将图片存入 **共享存储** (NAS / S3 / 本地磁盘 / Redis)，只把 **路径 (Path)** 或 **Redis Key** 发给 Java。
*   **Java**: 收到 `{"image_path": "/data/frames/img_01.jpg"}`。
*   **损耗**: 几乎为零。

#### 🔴 陷阱 2: 细粒度频繁调用 ("Chatty" Interface)
*   **错误做法**: Java 循环调用 Python: `for frame in video: check_frame(frame)`
*   **损耗**: 网络握手开销累积。如有 1000 帧，就是 1000 次 HTTP 请求，额外增加 1-2秒 延迟。

#### 🟢 策略 2: 粗粒度批处理 ("Chunky" Interface)
*   **正确做法**: Java 发一次指令: `analyze_video(video_path, start=0, end=100)`。
*   **损耗**: 1次 HTTP 请求。Python 内部循环效率极高 (C底层)。

---

## 3. 结论

1.  **职责界定**: Java 是 **"大脑"** (负责状态流转、最终决策)，Python 是 **"小脑"** (负责运动控制、感知输入)。Python 不应该碰数据库，Java 不应该碰像素。
2.  **性能损耗**: 只要遵守 **"传引用不传大对象"** 和 **"批处理"** 原则，HTTP/MQ 带来的通信损耗相对于 CV 算法本身的耗时来说，**完全可以忽略不计**。
3.  **数据流**:
    *   小数据 (Config, TaskID, Status): 走 **HTTP/MQ JSON**。
    *   大数据 (Images, Feature Vectors): 走 **Redis / File System**，仅传递 Key/Path。
