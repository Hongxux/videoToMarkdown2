import shutil
from pathlib import Path


def make_repo_tmp_dir(test_name: str) -> Path:
    """
    Create a writable temp dir under repo `var/` to avoid Windows TEMP ACL issues.
    """
    repo_root = Path(__file__).resolve().parents[5]
    base = repo_root / "var" / "tmp_tests_transcript"
    base.mkdir(parents=True, exist_ok=True)

    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(test_name))
    path = base / safe
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path

