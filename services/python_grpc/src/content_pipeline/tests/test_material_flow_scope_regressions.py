from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnit
from services.python_grpc.src.content_pipeline.phase2b.assembly.material_flow import apply_external_materials
from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import MaterialRequests


class _PipelineStub:
    assets_dir = "."
    _concrete_validator = None
    _phase2b_concrete_ai_vision_enabled = False

    def __init__(self):
        self._prestructure_seen_raw_signatures = {}
        self._prevalidated_concrete_results = {}

    def _build_sentence_timestamps(self):
        return {}

    def _map_timestamp_to_sentence_id(self, *_args, **_kwargs):
        return ""

    def _get_sentence_text_by_id(self, *_args, **_kwargs):
        return ""

    def _record_image_match_audit(self, *_args, **_kwargs):
        return None


def test_apply_external_materials_process_missing_request_does_not_raise_unbound_issue():
    unit = SemanticUnit(
        unit_id="SU_SCOPE_01",
        knowledge_type="process",
        knowledge_topic="Process Material Scope",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=4.0,
    )

    apply_external_materials(
        _PipelineStub(),
        unit,
        screenshots_dir="",
        clips_dir="",
        material_requests=MaterialRequests(
            screenshot_requests=[],
            clip_requests=[],
            action_classifications=[],
        ),
    )

    assert unit.materials is not None
    issues = unit.materials.metadata.get("material_resolution_issues", [])
    assert len(issues) == 1
    assert issues[0]["kind"] == "screenshot_request_not_propagated"
    assert issues[0]["unit_id"] == "SU_SCOPE_01"
