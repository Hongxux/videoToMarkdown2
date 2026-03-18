from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.phase2a_runtime_repository import (
    build_phase2a_runtime_repository,
    get_phase2a_repository_views,
    update_phase2a_repository_views,
)


def test_phase2a_runtime_repository_exposes_views_only(tmp_path):
    output_dir = str(tmp_path / "task")
    semantic_path = str(tmp_path / "task" / "semantic_units_phase2a.json")
    payload = build_phase2a_runtime_repository(
        output_dir=output_dir,
        semantic_units_path=semantic_path,
        semantic_units=[{"unit_id": "U001", "start_sec": 1.0, "end_sec": 2.0}],
        task_id="task-phase2a",
        ref_id="phase2a_ref_001",
    )

    assert payload["ready"] is True
    assert payload["ref_id"] == "phase2a_ref_001"
    assert isinstance(payload.get("views"), dict)
    assert payload["views"]["semantic_units"][0]["unit_id"] == "U001"
    assert payload["views"]["fingerprint"]
    assert get_phase2a_repository_views(payload)["semantic_units_path"].endswith("semantic_units_phase2a.json")


def test_phase2a_runtime_repository_updates_views_in_place(tmp_path):
    output_dir = str(tmp_path / "task")
    payload = build_phase2a_runtime_repository(
        output_dir=output_dir,
        task_id="task-phase2a",
    )

    update_phase2a_repository_views(
        payload,
        semantic_units_path=str(tmp_path / "task" / "semantic_units_phase2a.json"),
        semantic_units=[{"unit_id": "U101", "start_sec": 3.0, "end_sec": 4.0}],
        reused=True,
    )

    views = get_phase2a_repository_views(payload)
    assert payload["status"] == "READY"
    assert payload["ready"] is True
    assert payload["reused"] is True
    assert views["semantic_units"][0]["unit_id"] == "U101"
    assert views["fingerprint"]
