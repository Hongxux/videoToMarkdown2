import threading
import sys
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


def test_runtime_semantic_units_preferred_when_keyframes_are_present():
    servicer = _build_servicer()
    current_payload = [
        {
            "unit_id": "SU004",
            "knowledge_type": "concrete",
            "full_text": "plain speech text without keyframe placeholders",
            "instructional_steps": [],
        }
    ]
    runtime_payload = [
        {
            "unit_id": "SU004",
            "knowledge_type": "concrete",
            "full_text": "step one [KEYFRAME_1]\nstep two [KEYFRAME_2]",
            "_vl_concrete_segments": [{"segment_id": "seg_01"}],
            "instructional_steps": [{"step_id": 1}, {"step_id": 2}],
            "materials": {
                "screenshot_items": [
                    {"img_id": "SU004_img_01"},
                    {"img_id": "SU004_img_02"},
                ]
            },
        }
    ]

    assert servicer._should_prefer_runtime_semantic_units(
        current_payload=current_payload,
        runtime_payload=runtime_payload,
    )


def test_runtime_semantic_units_not_preferred_when_less_complete():
    servicer = _build_servicer()
    current_payload = [
        {
            "unit_id": "SU005",
            "knowledge_type": "concrete",
            "full_text": "main flow [KEYFRAME_1] [KEYFRAME_2]",
            "_vl_concrete_segments": [{"segment_id": "seg_01"}, {"segment_id": "seg_02"}],
            "instructional_steps": [{"step_id": 1}, {"step_id": 2}, {"step_id": 3}],
            "materials": {"screenshot_items": [{"img_id": "SU005_img_01"}]},
        }
    ]
    runtime_payload = [
        {
            "unit_id": "SU005",
            "knowledge_type": "concrete",
            "full_text": "brief text only",
            "_vl_concrete_segments": [],
            "instructional_steps": [],
            "materials": {"screenshot_items": []},
        }
    ]

    assert not servicer._should_prefer_runtime_semantic_units(
        current_payload=current_payload,
        runtime_payload=runtime_payload,
    )
