"""企业微信机器人启动入口（薄入口）。"""

from __future__ import annotations

from pathlib import Path
import sys


def _bootstrap_repo_root() -> None:
    repo_root = str(Path(__file__).resolve().parents[2])
    if repo_root in sys.path:
        sys.path.remove(repo_root)
    sys.path.insert(0, repo_root)


_bootstrap_repo_root()

from services.python_grpc.src.apps.bot.wecom_bot import main


if __name__ == "__main__":
    main()
