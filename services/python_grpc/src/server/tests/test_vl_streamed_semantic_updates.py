import copy
import sys
import threading
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

try:
    from services.python_grpc.src.server import grpc_service_impl as impl
except Exception as import_error:
    pytest.skip(f"grpc_service_impl import unavailable: {import_error}", allow_module_level=True)


def _build_servicer():
    servicer = impl._VideoProcessingServicerCore.__new__(impl._VideoProcessingServicerCore)
    servicer._phase2a_runtime_cache_lock = threading.Lock()
    servicer._phase2a_runtime_cache = {}
    servicer._phase2a_ref_cache = {}
    return servicer


def test_apply_streamed_vl_unit_output_updates_concrete_main_content():
    servicer = _build_servicer()
    semantic_units = [
        {
            "unit_id": "SU001",
            "knowledge_type": "process",
            "start_sec": 0.0,
            "end_sec": 12.0,
        }
    ]
    persist_payload = servicer._build_grouped_semantic_units_payload(copy.deepcopy(semantic_units))
    units_map = {item["unit_id"]: item for item in semantic_units}
    persist_units_map = servicer._build_semantic_unit_index(persist_payload)

    unit_output = {
        "unit_id": "SU001",
        "success": True,
        "analysis_mode": "concrete",
        "raw_response_json": [
            {
                "segment_id": 2,
                "segment_description": "segment-2",
                "main_content": "segment-two [KEYFRAME_2]",
                "clip_start_sec": 6.0,
                "clip_end_sec": 10.0,
            },
            {
                "segment_id": 1,
                "segment_description": "segment-1",
                "main_content": "segment-one [KEYFRAME_1]",
                "clip_start_sec": 0.0,
                "clip_end_sec": 5.0,
            },
        ],
        "clip_requests": [
            {
                "clip_id": "SU001/SU001_clip_001",
                "start_sec": 0.0,
                "end_sec": 5.0,
                "semantic_unit_id": "SU001",
            }
        ],
        "screenshot_requests": [
            {
                "screenshot_id": "SU001/SU001_ss_001",
                "timestamp_sec": 2.5,
                "semantic_unit_id": "SU001",
            }
        ],
        "metadata": {
            "semantic_unit": {
                "unit_id": "SU001",
                "knowledge_type": "concrete",
                "_vl_analysis_mode_override": "concrete",
                "_vl_route_override": "concrete",
            }
        },
    }

    summary = servicer._apply_streamed_vl_unit_output_to_semantic_nodes(
        unit_output=unit_output,
        units_map=units_map,
        persist_units_map=persist_units_map,
    )

    assert summary["unit_id"] == "SU001"
    assert summary["route_patch_updated"] == 1
    assert summary["material_requests_updated"] == 1
    assert summary["concrete_main_content_updated"] == 1
    assert [item["segment_id"] for item in summary["normalized_segments"]] == [1, 2]
    assert summary["final_main_content"] == "segment-one [KEYFRAME_1]\n\nsegment-two [KEYFRAME_2]"

    for target in (units_map["SU001"], persist_units_map["SU001"]):
        assert target["knowledge_type"] == "concrete"
        assert target["_vl_route_override"] == "concrete"
        assert target["_vl_analysis_mode_override"] == "concrete"
        assert target["full_text"] == "segment-one [KEYFRAME_1]\n\nsegment-two [KEYFRAME_2]"
        assert target["text"] == "segment-one [KEYFRAME_1]\n\nsegment-two [KEYFRAME_2]"
        assert [item["segment_id"] for item in target["_vl_concrete_segments"]] == [1, 2]
        assert target["material_requests"]["clip_requests"][0]["clip_id"] == "SU001/SU001_clip_001"
        assert target["material_requests"]["screenshot_requests"][0]["screenshot_id"] == "SU001/SU001_ss_001"


def test_apply_streamed_vl_unit_output_updates_route_patch_without_concrete_body():
    servicer = _build_servicer()
    semantic_units = [
        {
            "unit_id": "SU002",
            "knowledge_type": "process",
            "start_sec": 12.0,
            "end_sec": 18.0,
        }
    ]
    persist_payload = servicer._build_grouped_semantic_units_payload(copy.deepcopy(semantic_units))
    units_map = {item["unit_id"]: item for item in semantic_units}
    persist_units_map = servicer._build_semantic_unit_index(persist_payload)

    unit_output = {
        "unit_id": "SU002",
        "success": True,
        "analysis_mode": "default",
        "raw_response_json": [],
        "clip_requests": [],
        "screenshot_requests": [],
        "metadata": {
            "semantic_unit": {
                "unit_id": "SU002",
                "knowledge_type": "abstract",
                "_vl_route_override": "abstract",
                "_vl_no_needed_video": True,
            }
        },
    }

    summary = servicer._apply_streamed_vl_unit_output_to_semantic_nodes(
        unit_output=unit_output,
        units_map=units_map,
        persist_units_map=persist_units_map,
    )

    assert summary["unit_id"] == "SU002"
    assert summary["route_patch_updated"] == 1
    assert summary["material_requests_updated"] == 1
    assert summary["concrete_main_content_updated"] == 0
    assert summary["final_main_content"] == ""

    for target in (units_map["SU002"], persist_units_map["SU002"]):
        assert target["knowledge_type"] == "abstract"
        assert target["_vl_route_override"] == "abstract"
        assert target["_vl_no_needed_video"] is True
        assert target["material_requests"]["clip_requests"] == []
        assert target["material_requests"]["screenshot_requests"] == []
        assert "full_text" not in target
        assert "text" not in target
