"""Worker 启动入口（薄入口）。

职责边界：
- 仅负责启动入口转发，不承载 worker 编排与执行实现。

主要功能：
- 调用 `services.python_grpc.src.worker.entrypoint.main`。
"""

from __future__ import annotations

from services.python_grpc.src.worker.entrypoint import main


if __name__ == "__main__":
    main()
