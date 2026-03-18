from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.phase2b_runtime_repository import (
    build_phase2b_runtime_repository,
    get_phase2b_repository_views,
    update_phase2b_repository_views,
)


def test_phase2b_runtime_repository_builds_and_updates_views(tmp_path):
    payload = build_phase2b_runtime_repository(
        output_dir=str(tmp_path),
        task_id="task-phase2b",
        title="phase2b-title",
    )

    assert payload["status"] == "PLANNED"
    assert payload["ready"] is False

    update_phase2b_repository_views(
        payload,
        markdown_path=str(tmp_path / "result.md"),
        json_path=str(tmp_path / "result.json"),
        title="phase2b-title",
    )

    views = get_phase2b_repository_views(payload)
    assert payload["status"] == "READY"
    assert payload["ready"] is True
    assert views["markdown_path"].endswith("result.md")
    assert views["json_path"].endswith("result.json")
    assert views["title"] == "phase2b-title"
    assert views["fingerprint"]
