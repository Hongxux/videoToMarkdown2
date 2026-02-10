# Phase 2 架构设计与并发优化深度解析

针对您提出的“Phase 2 CPU 计算密集型任务如何利用多进程/多线程加速，并保持进程持续饱和”的问题，经过对 `python_grpc_server.py`, `CVValidationOrchestrator.java` 及 `AdaptiveResourceOrchestrator.java` 的深度分析，目前的架构设计如下：

## 1. 总体设计理念：生产者-消费者模型

系统采用了经典的 **Java (生产者/调度者) + Python (消费者/执行者)** 双层架构：

*   **Java 端 (Orchestrator)**：负责“推”。它不进行重计算，而是作为指挥官，将所有语义单元拆解为多个批次 (Batches)，并根据系统负载源源不断地推送到 Python 端。
*   **Python 端 (gRPC Server)**：负责“算”。它作为一个长期驻留的计算服务，维护着一个高性能的进程池，专门处理 CPU 密集型任务，绕过 Python 的 GIL 限制。

---

## 2. 核心加速机制：多进程与多线程的协同

### A. Python 端：ProcessPool + SharedMemory (算力核心)
Phase 2 (CV 验证) 是计算最密集的环节，Python 端采用了以下策略来榨干 CPU：

1.  **多进程并行 (Multi-processing)**：
    *   **实现**：使用了 `concurrent.futures.ProcessPoolExecutor` (`cv_process_pool`)。
    *   **目的**：彻底绕过 Python 的 GIL (全局解释器锁)。每个 Worker 是一个独立的进程，拥有独立的 Python 解释器，能够利用多核 CPU 并行执行 CV 模型推理。
    *   **动态伸缩**：Worker 数量不是固定的，而是根据当前 CPU 核心数和可用内存动态计算：`min(CPU核心数-1, 可用内存/1.5GB, 8)`。这既保证了算力最大化，又防止了内存溢出 (OOM)。

2.  **共享内存 (Shared Memory)**：
    *   **瓶颈**：多进程通信通常面临巨大的序列化/反序列化开销 (Pickling overhead)，尤其是传输大量视频帧时。
    *   **优化**：使用了 `SharedFrameRegistry`。
    *   **流程**：主进程 (gRPC 线程) 负责读取视频帧并写入**共享内存块**；Worker 进程直接从共享内存读取数据引用。实现了**零拷贝 (Zero-copy)** 数据分发，让 CPU 专注于计算而非数据搬运。

### B. Java 端：Async IO + 虚拟信号量 (高并发调度)
1.  **多线程分发**：使用 `FixedThreadPool` (大小等于物理核数) 并发发送 gRPC 请求。
2.  **流式响应 (Streaming)**：通过 `validateCVBatchStreaming` 接口，Python 算完一个单元，Java 就能立刻收到一个结果。这允许后续的链路 (如 LLM 分类) 立即启动，形成了跨语言的流水线。

---

## 3. “持续喂饱”机制：如何让进程不停息？

为了杜绝 CPU 闲置，架构中在 Java 和 Python 两端都实现了精妙的流控与缓冲机制：

### A. Java 端的“宏观油门”：AdaptiveResourceOrchestrator
Java 端实现了一个基于 **AIMD (加法增大，乘法减小)** 算法的自适应流控器：
*   **监测**：实时监控系统的 CPU Load 和空闲内存。
*   **策略**：
    *   如果 CPU < 60% 且内存充足，**增加**并发许可 (Permits)，多推一些任务给 Python。
    *   如果 CPU > 85% 或内存告急，**快速减少**并发，防止系统卡死。
*   **效果**：保证发给 Python 的任务队列永远略大于 Python 的处理能力，确保 Python Worker 永远有活干。

### B. Python 端的“微观流水线”：IO/Compute Overlap (重叠)
在 `ValidateCVBatch` 内部，实现了一个微型流水线来掩盖磁盘 IO 延迟：
*   **Chunking**：将一个大的 Batch 拆分为多个小 Chunk。
*   **Prefetch (预取)**：
    *   当 CPU ProcessPool 正在全力计算 **Chunk N** 时；
    *   主线程已经通过 `asyncio` 异步读取并解码 **Chunk N+1** 的视频帧到共享内存中。
*   **效果**：当 Worker 进程完成当前计算时，下一批数据已经在内存中就绪。CPU 几乎不需要等待磁盘 IO，实现了 **100% 的占空比**。

---

### 总结
您的程序通过 **Java 端的自适应 AIMD 压力输送** + **Python 端的多进程无锁计算** + **IO/计算流水线级预取**，成功实现了一个能够榨干多核 CPU 性能且具备高吞吐量的计算架构。
