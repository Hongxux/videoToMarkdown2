"""
配置单一真源校验脚本。

目标：
1) 强制要求仅 `config/` 下保留真实配置文件；
2) 阻止历史副本路径回流；
3) 作为 CI/本地 pre-check 使用。

使用方式：
    python scripts/maintenance/check_config_single_source.py
"""

from __future__ import annotations

from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]


REQUIRED = [
    REPO_ROOT / "config" / "video_config.yaml",
    REPO_ROOT / "config" / "module2_config.yaml",
    REPO_ROOT / "config" / "fault_detection_config.yaml",
    REPO_ROOT / "config" / "dictionaries.yaml",
]

LEGACY_FORBIDDEN = [
    REPO_ROOT / "services" / "python_grpc" / "config" / "video_config.yaml",
    REPO_ROOT / "services" / "python_grpc" / "src" / "config" / "module2_config.yaml",
    REPO_ROOT / "services" / "python_grpc" / "src" / "config" / "fault_detection_config.yaml",
    REPO_ROOT / "services" / "python_grpc" / "src" / "config" / "dictionaries.yaml",
]


def main() -> int:
    missing = [path for path in REQUIRED if not path.exists()]
    legacy_exists = [path for path in LEGACY_FORBIDDEN if path.exists()]

    if missing:
        print("[FAIL] missing required config files:")
        for path in missing:
            print(f"  - {path}")

    if legacy_exists:
        print("[FAIL] legacy config duplicates still exist:")
        for path in legacy_exists:
            print(f"  - {path}")

    if missing or legacy_exists:
        print("\nPlease keep config single source under `config/` only.")
        return 1

    print("[OK] config single source check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
