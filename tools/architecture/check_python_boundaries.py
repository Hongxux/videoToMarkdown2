"""Architecture boundary checks for Python service layers.

This script enforces high-level constraints for the refactor initiative:
- `server/service.py` remains a delegation layer
- no direct dependency inversion from startup runner into legacy entry
"""

from __future__ import annotations

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]


def _line_count(path: Path) -> int:
    return sum(1 for _ in path.open("r", encoding="utf-8"))


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> int:
    failures: list[str] = []

    server_service = REPO_ROOT / "services" / "python_grpc" / "src" / "server" / "service.py"
    server_impl = REPO_ROOT / "services" / "python_grpc" / "src" / "server" / "grpc_service_impl.py"
    startup_runner = REPO_ROOT / "services" / "python_grpc" / "src" / "server" / "startup_runner.py"

    if not server_service.exists():
        failures.append("Missing services/python_grpc/src/server/service.py")
    else:
        service_lines = _line_count(server_service)
        if service_lines > 220:
            failures.append(f"server/service.py too large: {service_lines} > 220")
        service_text = _read(server_service)
        if "from . import grpc_service_impl as impl" not in service_text:
            failures.append("server/service.py must delegate to services/python_grpc/src/server/grpc_service_impl.py")

    if not server_impl.exists():
        failures.append("Missing services/python_grpc/src/server/grpc_service_impl.py")

    if startup_runner.exists():
        runner_text = _read(startup_runner)
        if "from python_grpc_server" in runner_text:
            failures.append("startup_runner.py must not import from python_grpc_server")

    if failures:
        print("[boundary-check] FAILED")
        for item in failures:
            print(f"- {item}")
        return 1

    print("[boundary-check] PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
