from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.runtime_stage_repository import RuntimeStageRepositoryRegistry


def test_runtime_stage_repository_registry_roundtrip_returns_deepcopy(tmp_path):
    registry = RuntimeStageRepositoryRegistry()
    output_dir = str(tmp_path / "task")

    stored = registry.put(
        stage="stage1",
        output_dir=output_dir,
        repository_id="stage1::task",
        schema_version="stage1.runtime_repository.v1",
        payload={
            "step2_subtitles": [{"subtitle_id": "S001"}],
            "step6_paragraphs": [{"paragraph_id": "P001"}],
        },
    )

    assert stored.repository_id == "stage1::task"

    loaded = registry.get(stage="stage1", output_dir=output_dir)
    assert loaded is not None
    payload = loaded.clone_payload()
    payload["step2_subtitles"][0]["subtitle_id"] = "MUTATED"

    loaded_again = registry.get(stage="stage1", output_dir=output_dir)
    assert loaded_again is not None
    assert loaded_again.payload["step2_subtitles"][0]["subtitle_id"] == "S001"


def test_runtime_stage_repository_registry_supports_lookup_by_repository_id_and_clear(tmp_path):
    registry = RuntimeStageRepositoryRegistry()
    output_dir = str(tmp_path / "task")

    registry.put(
        stage="phase2a",
        output_dir=output_dir,
        repository_id="phase2a_ref_001",
        schema_version="phase2a.runtime_repository.v1",
        payload={
            "ref_id": "phase2a_ref_001",
            "semantic_units": [{"unit_id": "U101"}],
        },
    )

    by_ref = registry.get_by_repository_id("phase2a_ref_001")
    assert by_ref is not None
    assert by_ref.payload["semantic_units"][0]["unit_id"] == "U101"

    removed = registry.clear(stage="phase2a", output_dir=output_dir)
    assert removed is not None
    assert registry.get(stage="phase2a", output_dir=output_dir) is None
    assert registry.get_by_repository_id("phase2a_ref_001") is None


def test_runtime_stage_repository_registry_mutate_updates_payload_without_replacing_entry(tmp_path):
    registry = RuntimeStageRepositoryRegistry()
    output_dir = str(tmp_path / "task")

    registry.put(
        stage="transcribe",
        output_dir=output_dir,
        repository_id="transcribe::task",
        schema_version="transcribe.runtime_repository.v1",
        payload={
            "segments": {},
            "status": "PLANNED",
        },
    )

    def _mutator(payload):
        payload.setdefault("segments", {})
        payload["segments"]["ts000001"] = {"status": "RUNNING"}
        payload["status"] = "RUNNING"
        return payload

    updated = registry.mutate(
        stage="transcribe",
        output_dir=output_dir,
        repository_id="transcribe::task",
        schema_version="transcribe.runtime_repository.v1",
        mutator=_mutator,
    )

    assert updated.repository_id == "transcribe::task"
    assert updated.payload["segments"]["ts000001"]["status"] == "RUNNING"

    loaded = registry.get(stage="transcribe", output_dir=output_dir)
    assert loaded is not None
    assert loaded.payload["status"] == "RUNNING"
