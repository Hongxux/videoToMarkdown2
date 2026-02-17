import json
from pathlib import Path

from services.python_grpc.src.content_pipeline.markdown_enhancer import (
    EnhancedSection,
    MarkdownEnhancer,
)
from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import (
    ConcreteKnowledgeResult,
    ConcreteKnowledgeValidator,
)
from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnit
from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import (
    ClipRequest,
    MaterialRequests,
    ScreenshotRequest,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline import RichTextPipeline


def _build_pipeline(tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    pipeline = RichTextPipeline(
        video_path="",
        step2_path="",
        step6_path="",
        output_dir=str(output_dir),
    )
    return pipeline, output_dir


def test_apply_external_materials_matches_legacy_request_id_without_unit_folder(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    assets_dir = output_dir / "assets"
    unit_dir = assets_dir / "SUX01"
    unit_dir.mkdir(parents=True, exist_ok=True)
    legacy_name = "SUX01_install_head"
    legacy_path = unit_dir / f"{legacy_name}.png"
    legacy_path.write_bytes(b"img")

    unit = SemanticUnit(
        unit_id="SUX01",
        knowledge_type="process",
        knowledge_topic="Legacy Id Match",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id=legacy_name,
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SUX01",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(assets_dir),
        clips_dir=str(assets_dir),
        material_requests=requests,
    )

    assert unit.materials is not None
    assert unit.materials.screenshot_paths == [str(legacy_path.resolve())]


def test_load_semantic_units_backfills_material_requests_from_vl_cache(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    semantic_units_path = output_dir / "semantic_units_phase2a.json"
    semantic_units_path.write_text(
        json.dumps(
            {
                "schema_version": "phase2a.grouped.v1",
                "knowledge_groups": [
                    {
                        "group_id": 1,
                        "group_name": "unit-group",
                        "reason": "同一核心论点聚合",
                        "units": [
                            {
                                "unit_id": "SUX90",
                                "knowledge_type": "process",
                                "knowledge_topic": "unit90",
                                "full_text": "demo",
                                "source_paragraph_ids": [],
                                "source_sentence_ids": [],
                                "start_sec": 0.0,
                                "end_sec": 10.0,
                                "material_requests": {
                                    "screenshot_requests": [],
                                    "clip_requests": [],
                                },
                            },
                            {
                                "unit_id": "SUX91",
                                "knowledge_type": "process",
                                "knowledge_topic": "unit91",
                                "full_text": "demo",
                                "source_paragraph_ids": [],
                                "source_sentence_ids": [],
                                "start_sec": 11.0,
                                "end_sec": 20.0,
                                "material_requests": {
                                    "screenshot_requests": [
                                        {
                                            "screenshot_id": "SUX91/SUX91_existing",
                                            "timestamp_sec": 11.5,
                                            "label": "existing",
                                            "semantic_unit_id": "SUX91",
                                        }
                                    ],
                                    "clip_requests": [],
                                },
                            },
                        ],
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    vl_cache_path = output_dir / "vl_analysis_cache.json"
    vl_cache_path.write_text(
        json.dumps(
            {
                "aggregated_screenshots": [
                    {
                        "screenshot_id": "SUX90/SUX90_ss_route_001",
                        "timestamp_sec": 1.25,
                        "label": "route_001",
                        "semantic_unit_id": "SUX90",
                        "analysis_mode": "route_sampling",
                    },
                    {
                        "screenshot_id": "SUX90/SUX90_ss_vl_action_001_head",
                        "timestamp_sec": 1.35,
                        "label": "action_001_head",
                        "semantic_unit_id": "SUX90",
                        "analysis_mode": "legacy_action_units",
                    },
                    {
                        "screenshot_id": "SUX91/SUX91_ss_vl_action_001_head",
                        "timestamp_sec": 12.25,
                        "label": "action_001_head",
                        "semantic_unit_id": "SUX91",
                        "analysis_mode": "legacy_action_units",
                    },
                ],
                "aggregated_clips": [
                    {
                        "clip_id": "SUX90/SUX90_clip_route_001",
                        "start_sec": 0.2,
                        "end_sec": 2.8,
                        "knowledge_type": "process",
                        "semantic_unit_id": "SUX90",
                        "segments": [{"start_sec": 0.2, "end_sec": 2.8}],
                        "analysis_mode": "route_sampling",
                    },
                    {
                        "clip_id": "SUX90/SUX90_clip_vl_action_001",
                        "start_sec": 0.3,
                        "end_sec": 3.0,
                        "knowledge_type": "process",
                        "semantic_unit_id": "SUX90",
                        "analysis_mode": "legacy_action_units",
                    },
                    {
                        "clip_id": "SUX91/SUX91_clip_vl_action_001",
                        "start_sec": 11.3,
                        "end_sec": 12.9,
                        "knowledge_type": "process",
                        "semantic_unit_id": "SUX91",
                        "analysis_mode": "legacy_action_units",
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    _, material_requests_map = pipeline._load_semantic_units(str(semantic_units_path))

    sux90_requests = material_requests_map["SUX90"]
    assert len(sux90_requests.screenshot_requests) == 1
    assert sux90_requests.screenshot_requests[0].screenshot_id == "SUX90/SUX90_ss_route_001"
    assert len(sux90_requests.clip_requests) == 1
    assert isinstance(sux90_requests.clip_requests[0], ClipRequest)
    assert sux90_requests.clip_requests[0].clip_id == "SUX90/SUX90_clip_route_001"
    assert sux90_requests.clip_requests[0].segments == [{"start_sec": 0.2, "end_sec": 2.8}]

    sux91_requests = material_requests_map["SUX91"]
    assert len(sux91_requests.screenshot_requests) == 1
    assert sux91_requests.screenshot_requests[0].screenshot_id == "SUX91/SUX91_existing"
    assert len(sux91_requests.clip_requests) == 0


def test_apply_external_materials_records_rejected_item_when_validator_rejects_all(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)
    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    shot_id = "SUX02/SUX02_head"
    shot_path = assets_dir / f"{shot_id}.png"
    shot_path.parent.mkdir(parents=True, exist_ok=True)
    shot_path.write_bytes(b"img")

    class _RejectResult:
        should_include = False
        reason = "reject"
        img_description = "reject"

    class _AlwaysRejectValidator:
        def validate(self, _image_path: str):
            return _RejectResult()

    pipeline._concrete_validator = _AlwaysRejectValidator()

    unit = SemanticUnit(
        unit_id="SUX02",
        knowledge_type="abstract",
        knowledge_topic="Reject All",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id=shot_id,
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SUX02",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(assets_dir),
        clips_dir=str(assets_dir),
        material_requests=requests,
    )

    assert unit.materials is not None
    assert unit.materials.screenshot_paths == []
    assert unit.materials.screenshot_items
    assert unit.materials.screenshot_items[0].get("should_include") is False
    assert unit.materials.screenshot_items[0].get("img_description") == "reject"


def test_apply_external_materials_process_degraded_branch_runs_validator(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)
    assets_dir = output_dir / "assets"
    unit_dir = assets_dir / "SUX04"
    unit_dir.mkdir(parents=True, exist_ok=True)
    screenshot_path = unit_dir / "SUX04_fallback_001.png"
    screenshot_path.write_bytes(b"img")

    class _AcceptResult:
        should_include = True
        reason = "accept"
        img_description = "accept"

    calls = {"count": 0}

    class _CountingValidator:
        def validate(self, _image_path: str):
            calls["count"] += 1
            return _AcceptResult()

    pipeline._concrete_validator = _CountingValidator()

    unit = SemanticUnit(
        unit_id="SUX04",
        knowledge_type="process",
        knowledge_topic="Process Degraded Branch",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(assets_dir),
        clips_dir=str(assets_dir),
        material_requests=requests,
    )

    assert calls["count"] == 1
    assert unit.materials is not None
    assert unit.materials.screenshot_paths == [str(screenshot_path.resolve())]


def test_apply_external_materials_short_process_with_explicit_screenshot_runs_validator(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)
    assets_dir = output_dir / "assets"
    unit_dir = assets_dir / "SUX05"
    unit_dir.mkdir(parents=True, exist_ok=True)
    screenshot_path = unit_dir / "SUX05_short_tail.png"
    screenshot_path.write_bytes(b"img")

    class _AcceptResult:
        should_include = True
        reason = "accept"
        img_description = "accept"

    calls = {"count": 0}

    class _CountingValidator:
        def validate(self, _image_path: str):
            calls["count"] += 1
            return _AcceptResult()

    pipeline._concrete_validator = _CountingValidator()

    unit = SemanticUnit(
        unit_id="SUX05",
        knowledge_type="process",
        knowledge_topic="Short Process Explicit Screenshot",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=3.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SUX05/SUX05_short_tail",
                timestamp_sec=2.4,
                label="tail",
                semantic_unit_id="SUX05",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(assets_dir),
        clips_dir=str(assets_dir),
        material_requests=requests,
    )

    assert calls["count"] == 1
    assert unit.materials is not None
    assert unit.materials.screenshot_paths == [str(screenshot_path.resolve())]


def test_apply_external_materials_restores_image_if_validator_deletes_it(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)
    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    shot_id = "SUX03/SUX03_head"
    shot_path = assets_dir / f"{shot_id}.png"
    shot_path.parent.mkdir(parents=True, exist_ok=True)
    shot_path.write_bytes(b"img")

    class _DeleteRejectResult:
        should_include = False
        reason = "reject"
        img_description = "reject"

    class _DeletingValidator:
        def validate(self, image_path: str):
            path_obj = Path(image_path)
            if path_obj.exists():
                path_obj.unlink()
            return _DeleteRejectResult()

    pipeline._concrete_validator = _DeletingValidator()

    unit = SemanticUnit(
        unit_id="SUX03",
        knowledge_type="abstract",
        knowledge_topic="Delete Then Reject",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id=shot_id,
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SUX03",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(assets_dir),
        clips_dir=str(assets_dir),
        material_requests=requests,
    )

    assert shot_path.exists()
    assert unit.materials is not None
    assert unit.materials.screenshot_paths == []
    assert unit.materials.screenshot_items
    assert unit.materials.screenshot_items[0].get("should_include") is False


def test_refresh_subtitle_context_from_semantic_units_dir(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)
    assert pipeline.step2_path == ""
    assert pipeline.sentence_timestamps_path == ""

    semantic_root = tmp_path / "phase2a_out"
    semantic_inter_dir = semantic_root / "intermediates"
    semantic_inter_dir.mkdir(parents=True, exist_ok=True)
    step2_path = semantic_inter_dir / "step2_correction_output.json"
    step2_path.write_text(
        '{"corrected_subtitles":[{"subtitle_id":"S001","corrected_text":"hello","start_sec":0.0,"end_sec":2.0}]}',
        encoding="utf-8",
    )
    step6_path = semantic_inter_dir / "step6_merge_cross_output.json"
    step6_path.write_text(
        '{"pure_text_script":[{"paragraph_id":"P001","text":"body","source_sentence_ids":["S001"]}]}',
        encoding="utf-8",
    )
    sentence_ts_path = semantic_inter_dir / "sentence_timestamps.json"
    sentence_ts_path.write_text('{"S001":{"start_sec":0.0,"end_sec":2.0}}', encoding="utf-8")
    semantic_units_path = semantic_root / "semantic_units_phase2a.json"
    semantic_units_path.write_text("[]", encoding="utf-8")

    pipeline._refresh_subtitle_context_from_semantic_units(str(semantic_units_path))

    assert pipeline.step2_path == str(step2_path)
    assert pipeline.step6_path == str(step6_path)
    assert pipeline.sentence_timestamps_path == str(sentence_ts_path)


def test_validator_false_result_does_not_delete_image_file(tmp_path):
    shot_path = tmp_path / "img.png"
    shot_path.write_bytes(b"img")

    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)
    result = ConcreteKnowledgeResult(
        has_concrete=False,
        has_formula=False,
        confidence=0.1,
        concrete_type="none",
        reason="vision_false",
        is_mixed=False,
        non_text_ratio=0.0,
        should_include=False,
    )

    validator._finalize_validation_result(str(shot_path), result, cache_result=False)

    assert shot_path.exists()


def test_markdown_enhancer_skips_vision_false_screenshot_items():
    enhancer = MarkdownEnhancer()
    filtered = enhancer._filter_screenshot_items(
        [
            {"img_id": "img1", "img_path": "a.png", "should_include": False},
            {"img_id": "img2", "img_path": "b.png", "should_include": True},
        ]
    )

    assert len(filtered) == 1
    assert filtered[0]["img_id"] == "img2"


def test_markdown_enhancer_drops_paths_when_all_items_are_vision_false():
    enhancer = MarkdownEnhancer()
    filtered_items = enhancer._filter_screenshot_items(
        [{"img_id": "img1", "img_path": "a.png", "should_include": False}]
    )
    filtered_paths = enhancer._filter_screenshot_paths(
        ["a.png"],
        filtered_items,
        drop_when_items_empty=True,
    )

    assert filtered_items == []
    assert filtered_paths == []


def test_apply_external_materials_passes_upstream_ocr_hint_to_validator(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)
    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    shot_id = "SUX06/SUX06_text_only"
    shot_path = assets_dir / f"{shot_id}.png"
    shot_path.parent.mkdir(parents=True, exist_ok=True)
    shot_path.write_bytes(b"img")

    captured = {"ocr_text": ""}

    class _TextOnlyResult:
        should_include = False
        reason = "text-only"
        img_description = "ocr reused"

    class _CaptureValidator:
        def validate(
            self,
            _image_path: str,
            ocr_text: str = "",
            skip_duplicate_check: bool = False,
        ):
            captured["ocr_text"] = ocr_text
            return _TextOnlyResult()

    pipeline._concrete_validator = _CaptureValidator()

    unit = SemanticUnit(
        unit_id="SUX06",
        knowledge_type="concrete",
        knowledge_topic="OCR Hint Reuse",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )

    req = ScreenshotRequest(
        screenshot_id=shot_id,
        timestamp_sec=1.0,
        label="head",
        semantic_unit_id="SUX06",
    )
    setattr(req, "ocr_text", "cached OCR from upstream")
    requests = MaterialRequests(
        screenshot_requests=[req],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(assets_dir),
        clips_dir=str(assets_dir),
        material_requests=requests,
    )

    assert captured["ocr_text"] == "cached OCR from upstream"
    assert unit.materials is not None
    assert unit.materials.screenshot_paths == []
    assert unit.materials.screenshot_items
    assert unit.materials.screenshot_items[0].get("img_description") == "ocr reused"


def test_markdown_enhancer_augment_items_keep_excluded_screenshot_descriptions():
    enhancer = MarkdownEnhancer()
    section = EnhancedSection(
        unit_id="SUX07",
        title="Concrete Unit",
        knowledge_type="concrete",
        screenshot_items=[
            {
                "img_id": "SUX07_img_01",
                "img_path": "assets/SUX07_img_01.png",
                "img_description": "include desc",
                "should_include": True,
            }
        ],
        augment_screenshot_items=[
            {
                "img_id": "SUX07_img_99",
                "img_path": "assets/SUX07_img_99.png",
                "img_description": "excluded desc for augment",
                "should_include": False,
                "timestamp_sec": 3.2,
            }
        ],
    )

    augment_items = enhancer._build_augment_image_items(section)

    assert len(augment_items) == 1
    assert augment_items[0]["img_id"] == "SUX07_img_99"
    assert augment_items[0]["img_description"] == "excluded desc for augment"
