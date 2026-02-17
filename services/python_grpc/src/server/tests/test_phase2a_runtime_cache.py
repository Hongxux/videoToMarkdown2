import threading
import sys
import json
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

try:
    from services.python_grpc.src.server import grpc_service_impl as impl
except Exception as import_error:
    pytest.skip(f"grpc_service_impl import unavailable: {import_error}", allow_module_level=True)


def _build_servicer_for_cache_tests():
    servicer = impl._VideoProcessingServicerCore.__new__(impl._VideoProcessingServicerCore)
    servicer._phase2a_runtime_cache_lock = threading.Lock()
    servicer._phase2a_runtime_cache = {}
    servicer._phase2a_ref_cache = {}
    return servicer


def test_phase2a_runtime_cache_roundtrip_returns_deepcopy(tmp_path):
    servicer = _build_servicer_for_cache_tests()
    output_dir = str(tmp_path / "task")
    semantic_path = str(tmp_path / "task" / "semantic_units_phase2a.json")
    payload = [{"unit_id": "U001", "start_sec": 1.0, "end_sec": 2.0}]

    servicer._cache_phase2a_runtime_semantic_units(
        output_dir=output_dir,
        semantic_units_path=semantic_path,
        semantic_units=payload,
    )

    loaded_once = servicer._get_phase2a_runtime_semantic_units(
        output_dir=output_dir,
        semantic_units_path=semantic_path,
    )
    assert loaded_once == payload

    loaded_once[0]["unit_id"] = "MUTATED"
    loaded_twice = servicer._get_phase2a_runtime_semantic_units(
        output_dir=output_dir,
        semantic_units_path=semantic_path,
    )
    assert loaded_twice[0]["unit_id"] == "U001"


def test_phase2a_runtime_cache_clear_removes_entry(tmp_path):
    servicer = _build_servicer_for_cache_tests()
    output_dir = str(tmp_path / "task")
    semantic_path = str(tmp_path / "task" / "semantic_units_phase2a.json")

    servicer._cache_phase2a_runtime_semantic_units(
        output_dir=output_dir,
        semantic_units_path=semantic_path,
        semantic_units=[],
    )
    assert servicer._get_phase2a_runtime_semantic_units(output_dir=output_dir, semantic_units_path=semantic_path) == []

    servicer._clear_phase2a_runtime_cache(output_dir)
    assert servicer._get_phase2a_runtime_semantic_units(output_dir=output_dir, semantic_units_path=semantic_path) is None


def test_phase2a_runtime_cache_ref_lookup_and_clear(tmp_path):
    servicer = _build_servicer_for_cache_tests()
    output_dir = str(tmp_path / "task")
    semantic_path = str(tmp_path / "task" / "semantic_units_phase2a.json")
    payload = [{"unit_id": "U002", "start_sec": 3.0, "end_sec": 4.0}]

    entry = servicer._cache_phase2a_runtime_semantic_units(
        output_dir=output_dir,
        semantic_units_path=semantic_path,
        semantic_units=payload,
        task_id="task-demo",
    )
    assert isinstance(entry, dict)
    ref_id = entry.get("ref_id")
    assert isinstance(ref_id, str) and ref_id

    by_ref = servicer._get_phase2a_runtime_cache_entry_by_ref(ref_id)
    assert isinstance(by_ref, dict)
    assert by_ref.get("semantic_units")[0]["unit_id"] == "U002"

    servicer._clear_phase2a_runtime_cache(output_dir)
    assert servicer._get_phase2a_runtime_cache_entry_by_ref(ref_id) is None


def test_build_grouped_semantic_units_payload_strips_unit_level_group_metadata():
    servicer = _build_servicer_for_cache_tests()
    payload = [
        {
            "unit_id": "U001",
            "start_sec": 0.0,
            "end_sec": 10.0,
            "group_id": 1,
            "group_name": "核心话题A",
            "group_reason": "同一核心论点聚合",
            "knowledge_type": "abstract",
        },
        {
            "unit_id": "U002",
            "start_sec": 10.0,
            "end_sec": 20.0,
            "group_id": 1,
            "group_name": "核心话题A",
            "group_reason": "同一核心论点聚合",
            "knowledge_type": "process",
        },
    ]

    grouped = servicer._build_grouped_semantic_units_payload(payload)
    assert isinstance(grouped, dict)
    assert isinstance(grouped.get("knowledge_groups"), list)
    assert len(grouped["knowledge_groups"]) == 1

    group = grouped["knowledge_groups"][0]
    assert group["group_name"] == "核心话题A"
    assert group["reason"] == "同一核心论点聚合"
    assert len(group["units"]) == 2
    assert "group_name" not in group["units"][0]
    assert "group_reason" not in group["units"][0]
    assert "group_id" not in group["units"][0]


def test_materialize_semantic_units_payload_writes_grouped_file(tmp_path):
    servicer = _build_servicer_for_cache_tests()
    output_dir = str(tmp_path / "task")
    payload = [
        {
            "unit_id": "U100",
            "start_sec": 1.0,
            "end_sec": 2.0,
            "group_name": "核心话题B",
            "group_reason": "同一核心论点聚合",
            "knowledge_type": "concrete",
        }
    ]

    path = servicer._materialize_semantic_units_payload(
        output_dir=output_dir,
        task_id="task-demo",
        semantic_units=payload,
    )
    persisted = json.loads(Path(path).read_text(encoding="utf-8"))
    assert isinstance(persisted.get("knowledge_groups"), list)
    assert persisted["knowledge_groups"][0]["group_name"] == "核心话题B"
    assert persisted["knowledge_groups"][0]["units"][0]["unit_id"] == "U100"
    assert "group_name" not in persisted["knowledge_groups"][0]["units"][0]
