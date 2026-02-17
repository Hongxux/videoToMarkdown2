import asyncio
import json
import types
from pathlib import Path

from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnit
from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline import RichTextPipeline


def test_analyze_only_persists_semantic_units_when_unit_processing_fails(tmp_path):
    output_dir = tmp_path / "out"
    inter_dir = output_dir / "intermediates"
    inter_dir.mkdir(parents=True, exist_ok=True)

    (inter_dir / "step2_correction_output.json").write_text(
        '{"corrected_subtitles":[{"subtitle_id":"S001","corrected_text":"hello","start_sec":0.0,"end_sec":1.0}]}',
        encoding="utf-8",
    )
    (inter_dir / "step6_merge_cross_output.json").write_text(
        '{"pure_text_script":[{"paragraph_id":"P001","text":"body","source_sentence_ids":["S001"]}]}',
        encoding="utf-8",
    )

    pipeline = RichTextPipeline(
        video_path="",
        step2_path="",
        step6_path="",
        output_dir=str(output_dir),
    )

    unit = SemanticUnit(
        unit_id="SUX01",
        knowledge_type="process",
        knowledge_topic="Topic",
        full_text="unit text",
        source_paragraph_ids=["P001"],
        source_sentence_ids=["S001"],
        start_sec=0.0,
        end_sec=5.0,
    )
    unit.action_segments = []
    unit.stable_islands = []

    async def _fake_segment(**_kwargs):
        return types.SimpleNamespace(semantic_units=[unit])

    async def _fake_apply_modality(units):
        return units

    async def _raise_collect_requests(_current_unit):
        raise RuntimeError("mock collect error")

    pipeline.segmenter = types.SimpleNamespace(segment=_fake_segment)
    pipeline._apply_modality_classification = _fake_apply_modality
    pipeline._collect_material_requests = _raise_collect_requests

    screenshot_requests, clip_requests, semantic_units_path = asyncio.run(pipeline.analyze_only())

    assert screenshot_requests == []
    assert clip_requests == []
    assert Path(semantic_units_path).exists()

    mirror_path = output_dir / "intermediates" / "semantic_units_phase2a.json"
    assert mirror_path.exists()

    payload = json.loads(Path(semantic_units_path).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    assert isinstance(payload.get("knowledge_groups"), list)
    assert len(payload["knowledge_groups"]) == 1
    assert payload["knowledge_groups"][0]["units"][0]["unit_id"] == "SUX01"

    mirrored_payload = json.loads(mirror_path.read_text(encoding="utf-8"))
    assert mirrored_payload == payload
