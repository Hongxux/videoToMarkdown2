"""工作区清理工具。

用途：
1) 清理 Python 缓存与测试缓存；
2) 清理已迁移后的重复目录；
3) 将根目录历史文档归档到 docs/archive；
4) 可选清理运行态数据目录，降低磁盘占用。

用法：
    python scripts/maintenance/cleanup_workspace.py
    python scripts/maintenance/cleanup_workspace.py --dry-run
    python scripts/maintenance/cleanup_workspace.py --include-runtime-data
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ARCHIVE_DOCX_DIR = REPO_ROOT / "docs" / "archive" / "root-docx"
ARCHIVE_NOTES_DIR = REPO_ROOT / "docs" / "archive" / "notes"


@dataclass
class CleanupStats:
    moved_files: int = 0
    removed_dirs: int = 0
    removed_files: int = 0
    touched_paths: list[str] = field(default_factory=list)


def record_path(stats: CleanupStats, path: Path) -> None:
    stats.touched_paths.append(str(path.relative_to(REPO_ROOT)))


def safe_remove_file(path: Path, stats: CleanupStats, dry_run: bool) -> None:
    if not path.exists():
        return
    record_path(stats, path)
    if not dry_run:
        path.unlink()
    stats.removed_files += 1


def safe_remove_dir(path: Path, stats: CleanupStats, dry_run: bool) -> None:
    if not path.exists():
        return
    record_path(stats, path)
    if not dry_run:
        shutil.rmtree(path)
    stats.removed_dirs += 1


def move_root_docx(stats: CleanupStats, dry_run: bool) -> None:
    source_dir = REPO_ROOT / "docx"
    if not source_dir.exists() or not source_dir.is_dir():
        return
    files = [entry for entry in source_dir.iterdir() if entry.is_file()]
    if not files:
        safe_remove_dir(source_dir, stats, dry_run)
        return

    if not dry_run:
        ARCHIVE_DOCX_DIR.mkdir(parents=True, exist_ok=True)

    for source_file in files:
        destination = ARCHIVE_DOCX_DIR / source_file.name
        record_path(stats, source_file)
        if not dry_run:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source_file), str(destination))
        stats.moved_files += 1

    if not dry_run and not any(source_dir.iterdir()):
        source_dir.rmdir()
        stats.removed_dirs += 1


def move_stage1_note(stats: CleanupStats, dry_run: bool) -> None:
    for note_path in REPO_ROOT.glob("Stage1*.md"):
        destination = ARCHIVE_NOTES_DIR / note_path.name
        record_path(stats, note_path)
        if not dry_run:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(note_path), str(destination))
        stats.moved_files += 1


def remove_windows_reserved_con(stats: CleanupStats, dry_run: bool) -> None:
    target = REPO_ROOT / "CON"
    target_literal = rf"\\?\{target}"

    probe = subprocess.run(
        ["cmd.exe", "/c", "if", "exist", target_literal, "(echo", "1)", "else", "(echo", "0)"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    exists = probe.returncode == 0 and "1" in probe.stdout
    if not exists:
        return

    if dry_run:
        stats.touched_paths.append("CON")
        return

    delete_result = subprocess.run(
        ["cmd.exe", "/c", "del", target_literal],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    verify = subprocess.run(
        ["cmd.exe", "/c", "if", "exist", target_literal, "(echo", "1)", "else", "(echo", "0)"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    removed = verify.returncode == 0 and "0" in verify.stdout
    if removed:
        stats.removed_files += 1
        stats.touched_paths.append("CON")
        return

    error_text = delete_result.stderr.strip() or delete_result.stdout.strip() or "unknown error"
    raise RuntimeError(f"删除 CON 失败: {error_text}")


def cleanup_python_cache(stats: CleanupStats, dry_run: bool) -> None:
    for cache_dir in REPO_ROOT.rglob("__pycache__"):
        safe_remove_dir(cache_dir, stats, dry_run)
    for pyc_file in REPO_ROOT.rglob("*.pyc"):
        safe_remove_file(pyc_file, stats, dry_run)
    for pyo_file in REPO_ROOT.rglob("*.pyo"):
        safe_remove_file(pyo_file, stats, dry_run)


def cleanup_known_legacy_dirs(stats: CleanupStats, dry_run: bool) -> None:
    safe_remove_dir(REPO_ROOT / ".pytest_cache", stats, dry_run)
    safe_remove_dir(REPO_ROOT / "generated_grpc", stats, dry_run)
    safe_remove_dir(REPO_ROOT / "MiniCMP", stats, dry_run)


def cleanup_runtime_data(stats: CleanupStats, dry_run: bool) -> None:
    runtime_dirs = [REPO_ROOT / "storage", REPO_ROOT / "var" / "storage"]
    for runtime_dir in runtime_dirs:
        if not runtime_dir.exists() or not runtime_dir.is_dir():
            continue
        for child in runtime_dir.iterdir():
            if child.name.lower() == "readme.md":
                continue
            if child.is_dir():
                safe_remove_dir(child, stats, dry_run)
            else:
                safe_remove_file(child, stats, dry_run)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="清理仓库历史遗留文件与缓存")
    parser.add_argument("--dry-run", action="store_true", help="仅打印将要处理的路径，不实际修改")
    parser.add_argument(
        "--include-runtime-data",
        action="store_true",
        help="额外清理 storage/ 与 var/storage/ 下运行产物",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    stats = CleanupStats()

    move_root_docx(stats, args.dry_run)
    move_stage1_note(stats, args.dry_run)
    cleanup_python_cache(stats, args.dry_run)
    cleanup_known_legacy_dirs(stats, args.dry_run)
    remove_windows_reserved_con(stats, args.dry_run)

    if args.include_runtime_data:
        cleanup_runtime_data(stats, args.dry_run)

    mode_text = "dry-run" if args.dry_run else "apply"
    print(f"[cleanup:{mode_text}] moved_files={stats.moved_files} removed_dirs={stats.removed_dirs} removed_files={stats.removed_files}")
    if stats.touched_paths:
        print("[cleanup] touched paths:")
        for item in sorted(set(stats.touched_paths)):
            print(f"  - {item}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
