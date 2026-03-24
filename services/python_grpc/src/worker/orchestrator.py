"""Worker 进程编排层。

职责边界：
- 负责 worker 数量决策、生命周期管理与信号处理。
- 不承担单个 worker 进程内的业务执行。

主要功能：
- `resolve_worker_count`：根据输入参数和机器资源计算 worker 数量。
- `WorkerOrchestrator.start`：启动、等待并优雅回收 worker 进程。
"""

from __future__ import annotations

import multiprocessing as mp
import signal
import sys
from dataclasses import dataclass, field

from .runtime import run_worker_process


def resolve_worker_count(requested_workers: int | None) -> int:
    """根据输入与机器资源决定 worker 数量。"""
    if isinstance(requested_workers, int) and requested_workers > 0:
        return requested_workers
    return min(max(mp.cpu_count() - 1, 1), 4)


@dataclass
class WorkerOrchestrator:
    """Worker 进程编排器。"""

    num_workers: int | None = None
    processes: list[mp.Process] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.num_workers = resolve_worker_count(self.num_workers)
        print(
            "\n".join(
                [
                    "=" * 56,
                    "VideoToMarkdown Worker Orchestrator",
                    f"CPU Cores: {mp.cpu_count()}",
                    f"Workers: {self.num_workers}",
                    "=" * 56,
                ]
            )
        )

    def start(self) -> None:
        """启动并等待所有 worker 进程。"""
        print(f"启动 {self.num_workers} 个 worker 进程...")
        self._register_signals()
        self._spawn_workers()
        self._join_workers()

    def _spawn_workers(self) -> None:
        """方法说明：WorkerOrchestrator._spawn_workers 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        for index in range(self.num_workers):
            process = mp.Process(target=run_worker_process, args=(index + 1,))
            process.start()
            self.processes.append(process)
            print(f"Worker #{index + 1} 已启动 (PID: {process.pid})")

    def _join_workers(self) -> None:
        """方法说明：WorkerOrchestrator._join_workers 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        print("\nWorker Orchestrator 运行中，按 Ctrl+C 停止\n")
        for process in self.processes:
            process.join()

    def _register_signals(self) -> None:
        """方法说明：WorkerOrchestrator._register_signals 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        try:
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
        except Exception as exc:
            print(f"信号注册失败（可能非主线程）: {exc}")

    def _handle_signal(self, signum, frame) -> None:  # noqa: ARG002
        """方法说明：WorkerOrchestrator._handle_signal 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        print("\n收到停止信号，正在关闭 worker...")
        self._terminate_workers()
        print("所有 worker 已停止")
        sys.exit(0)

    def _terminate_workers(self) -> None:
        """方法说明：WorkerOrchestrator._terminate_workers 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        for process in self.processes:
            if process.is_alive():
                process.terminate()
        for process in self.processes:
            process.join(timeout=5)
            if process.is_alive():
                process.kill()


__all__ = ["WorkerOrchestrator", "resolve_worker_count"]

