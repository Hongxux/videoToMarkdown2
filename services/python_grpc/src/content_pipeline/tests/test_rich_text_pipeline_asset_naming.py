import asyncio
import types
from pathlib import Path

from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import (
    ClipRequest,
    MaterialRequests,
    ScreenshotRequest,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline import RichTextPipeline
from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnit


def _build_pipeline(tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    pipeline = RichTextPipeline(
        video_path="",
        step2_path="",
        step6_path="",
        output_dir=str(output_dir),
    )
    pipeline._phase2b_concrete_ai_vision_enabled = True
    return pipeline, output_dir


def test_pipeline_auto_discovers_intermediate_step2_step6_and_sentence_timestamps(tmp_path):
    output_dir = tmp_path / "out"
    inter_dir = output_dir / "intermediates"
    inter_dir.mkdir(parents=True, exist_ok=True)

    step2_path = inter_dir / "step2_correction_output.json"
    step2_path.write_text(
        '{"corrected_subtitles":[{"subtitle_id":"S001","corrected_text":"hello","start_sec":0.0,"end_sec":1.0}]}',
        encoding="utf-8",
    )

    step6_path = inter_dir / "step6_merge_cross_output.json"
    step6_path.write_text(
        '{"pure_text_script":[{"paragraph_id":"P001","text":"body","source_sentence_ids":["S001"]}]}',
        encoding="utf-8",
    )

    st_path = inter_dir / "sentence_timestamps.json"
    st_path.write_text('{"S001":{"start_sec":0.0,"end_sec":1.0}}', encoding="utf-8")

    pipeline = RichTextPipeline(
        video_path="",
        step2_path="",
        step6_path="",
        output_dir=str(output_dir),
    )

    assert pipeline.step2_path == str(step2_path)
    assert pipeline.step6_path == str(step6_path)
    assert pipeline.sentence_timestamps_path == str(st_path)
    assert len(pipeline.subtitles) == 1
    assert len(pipeline.paragraphs) == 1


def test_collect_material_requests_uses_title_and_action_description(tmp_path):
    pipeline, _ = _build_pipeline(tmp_path)

    unit = SemanticUnit(
        unit_id="SU900",
        knowledge_type="process",
        knowledge_topic="Open Settings",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=10.0,
        end_sec=30.0,
    )
    unit.action_segments = [
        {
            "start_sec": 12.0,
            "end_sec": 20.0,
            "internal_stable_islands": [{"start": 14.0, "end": 15.0}],
            "classification": {
                "knowledge_type": "process",
                "confidence": 0.9,
                "description": "open settings",
            },
        }
    ]

    requests = asyncio.run(pipeline._collect_material_requests(unit))

    assert requests.clip_requests, "expected at least one clip request"
    assert requests.screenshot_requests, "expected at least one screenshot request"

    clip_id = requests.clip_requests[0].clip_id
    screenshot_id = requests.screenshot_requests[0].screenshot_id

    assert clip_id.startswith("SU900/")
    assert screenshot_id.startswith("SU900/")
    assert "su900_open" in clip_id.lower()
    assert "open_settings" in clip_id.lower()
    assert "su900_open" in screenshot_id.lower()


def test_apply_external_materials_uses_existing_assets_without_copy(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    class _AlwaysInclude:
        def validate(self, image_path: str):
            class _Result:
                should_include = True
                reason = "ok"
                img_description = "ok"

            return _Result()

    pipeline._concrete_validator = _AlwaysInclude()

    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    (screenshots_dir / "SU901").mkdir(parents=True, exist_ok=True)

    source_img = screenshots_dir / "SU901" / "random_name.png"
    source_img.write_bytes(b"img")
    source_clip = clips_dir / "SU901" / "random_video_name.mp4"
    source_clip.write_bytes(b"video")

    unit = SemanticUnit(
        unit_id="SU901",
        knowledge_type="process",
        knowledge_topic="Install NodeJS",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=20.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SU901/random_name",
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SU901",
            )
        ],
        clip_requests=[
            ClipRequest(
                clip_id="SU901/random_video_name",
                start_sec=0.0,
                end_sec=5.0,
                knowledge_type="process",
                semantic_unit_id="SU901",
            )
        ],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests,
    )

    assert unit.materials is not None
    assert unit.materials.screenshot_paths, "screenshot should be found"
    assert unit.materials.clip_path, "clip should be found"
    assert len(unit.materials.clip_paths) == 1

    screenshot_path = Path(unit.materials.screenshot_paths[0])
    clip_path = Path(unit.materials.clip_path)

    assert screenshot_path.exists()
    assert clip_path.exists()
    assert screenshot_path.parent.name == "SU901"
    assert clip_path.parent.name == "SU901"
    assert screenshot_path == source_img.resolve()
    assert clip_path == source_clip.resolve()
    assert Path(unit.materials.clip_paths[0]) == source_clip.resolve()


def test_apply_external_materials_prefers_request_id_path_without_copy(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    class _AlwaysInclude:
        def validate(self, image_path: str):
            class _Result:
                should_include = True
                reason = "ok"
                img_description = "ok"

            return _Result()

    pipeline._concrete_validator = _AlwaysInclude()

    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    unit = SemanticUnit(
        unit_id="SU902",
        knowledge_type="process",
        knowledge_topic="Install NodeJS",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=20.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SU902/SU902_install_nodejs_action_01_open_settings_head",
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SU902",
            )
        ],
        clip_requests=[
            ClipRequest(
                clip_id="SU902/SU902_install_nodejs_action_01_open_settings",
                start_sec=0.0,
                end_sec=5.0,
                knowledge_type="process",
                semantic_unit_id="SU902",
            )
        ],
        action_classifications=[],
    )

    screenshot_incoming_path = screenshots_dir / f"{requests.screenshot_requests[0].screenshot_id}.png"
    clip_incoming_path = clips_dir / f"{requests.clip_requests[0].clip_id}.mp4"
    screenshot_incoming_path.parent.mkdir(parents=True, exist_ok=True)
    clip_incoming_path.parent.mkdir(parents=True, exist_ok=True)
    screenshot_incoming_path.write_bytes(b"img2")
    clip_incoming_path.write_bytes(b"video2")

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests,
    )

    screenshot_path = Path(unit.materials.screenshot_paths[0])
    clip_path = Path(unit.materials.clip_path)

    assert screenshot_path == screenshot_incoming_path.resolve()
    assert clip_path == clip_incoming_path.resolve()
    assert unit.materials.clip_paths == [str(clip_incoming_path.resolve())]


def test_apply_external_materials_validates_concept_screenshots(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    keep_name = "SU903/SU903_step_head"
    drop_name = "SU903/SU903_step_tail"
    (screenshots_dir / f"{keep_name}.png").parent.mkdir(parents=True, exist_ok=True)
    (screenshots_dir / f"{keep_name}.png").write_bytes(b"keep")
    (screenshots_dir / f"{drop_name}.png").write_bytes(b"drop")

    unit = SemanticUnit(
        unit_id="SU903",
        knowledge_type="concrete",
        knowledge_topic="Apply Config",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=20.0,
    )

    calls = []

    class _ValidateResult:
        def __init__(self, should_include: bool, reason: str):
            self.should_include = should_include
            self.reason = reason
            self.img_description = reason

    class _StubValidator:
        def validate(self, image_path: str):
            calls.append(Path(image_path).name)
            if "SU903_step_head" in image_path:
                return _ValidateResult(True, "kept")
            return _ValidateResult(False, "filtered")

    pipeline._concrete_validator = _StubValidator()

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id=keep_name,
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SU903",
            ),
            ScreenshotRequest(
                screenshot_id=drop_name,
                timestamp_sec=2.0,
                label="tail",
                semantic_unit_id="SU903",
            ),
        ],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests,
    )

    assert len(calls) == 2
    assert len(unit.materials.screenshot_paths) == 1
    assert Path(keep_name).name.lower() in Path(unit.materials.screenshot_paths[0]).name.lower()


def test_apply_external_materials_skips_validation_for_process(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    shot_name = "SU904/SU904_step_head"
    (screenshots_dir / f"{shot_name}.png").parent.mkdir(parents=True, exist_ok=True)
    (screenshots_dir / f"{shot_name}.png").write_bytes(b"img")

    unit = SemanticUnit(
        unit_id="SU904",
        knowledge_type="process",
        knowledge_topic="Tutorial Config",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=20.0,
        mult_steps=False,
        instructional_steps=[],
    )

    class _FailValidator:
        def validate(self, image_path: str):
            raise AssertionError("process unit should not run screenshot validator")

    pipeline._concrete_validator = _FailValidator()

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id=shot_name,
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SU904",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests,
    )

    assert len(unit.materials.screenshot_paths) == 1


def test_apply_external_materials_skips_non_assets_candidates_in_no_copy_mode(tmp_path):
    pipeline, _ = _build_pipeline(tmp_path)

    class _AlwaysInclude:
        def validate(self, image_path: str):
            class _Result:
                should_include = True
                reason = "ok"
                img_description = "ok"

            return _Result()

    pipeline._concrete_validator = _AlwaysInclude()

    screenshots_dir = tmp_path / "incoming_screens"
    clips_dir = tmp_path / "incoming_clips"
    (screenshots_dir / "SU905").mkdir(parents=True, exist_ok=True)
    (clips_dir / "SU905").mkdir(parents=True, exist_ok=True)
    (screenshots_dir / "SU905" / "shot.png").write_bytes(b"img")
    (clips_dir / "SU905" / "clip.mp4").write_bytes(b"clip")

    unit = SemanticUnit(
        unit_id="SU905",
        knowledge_type="process",
        knowledge_topic="Install NodeJS",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=20.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="missing_id",
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SU905",
            )
        ],
        clip_requests=[
            ClipRequest(
                clip_id="missing_clip",
                start_sec=0.0,
                end_sec=5.0,
                knowledge_type="process",
                semantic_unit_id="SU905",
            )
        ],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests,
    )

    assert unit.materials is not None
    assert unit.materials.screenshot_paths == []
    assert unit.materials.clip_path == ""
    assert unit.materials.clip_paths == []


def test_apply_external_materials_fallback_scans_unit_assets_when_requests_empty(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    assets_dir = output_dir / "assets"
    unit_dir = assets_dir / "SU906"
    unit_dir.mkdir(parents=True, exist_ok=True)

    fallback_screenshot = unit_dir / "SU906_ss_route_001.jpg"
    fallback_clip = unit_dir / "SU906_clip_route_001.mp4"
    fallback_screenshot.write_bytes(b"img")
    fallback_clip.write_bytes(b"clip")

    unit = SemanticUnit(
        unit_id="SU906",
        knowledge_type="process",
        knowledge_topic="Fallback Unit",
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

    assert unit.materials is not None
    assert unit.materials.screenshot_paths == [str(fallback_screenshot.resolve())]
    assert unit.materials.clip_path == str(fallback_clip.resolve())
    assert unit.materials.clip_paths == [str(fallback_clip.resolve())]


def test_apply_external_materials_keeps_multiple_clip_paths(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    class _AlwaysInclude:
        def validate(self, image_path: str):
            class _Result:
                should_include = True
                reason = "ok"
                img_description = "ok"

            return _Result()

    pipeline._concrete_validator = _AlwaysInclude()

    assets_dir = output_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    unit = SemanticUnit(
        unit_id="SU907",
        knowledge_type="process",
        knowledge_topic="Multi Clip",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=20.0,
    )

    clip_id_1 = "SU907/SU907_action_01"
    clip_id_2 = "SU907/SU907_action_02"
    clip_1 = assets_dir / f"{clip_id_1}.mp4"
    clip_2 = assets_dir / f"{clip_id_2}.mp4"
    clip_1.parent.mkdir(parents=True, exist_ok=True)
    clip_2.parent.mkdir(parents=True, exist_ok=True)
    clip_1.write_bytes(b"clip1")
    clip_2.write_bytes(b"clip2")

    requests = MaterialRequests(
        screenshot_requests=[],
        clip_requests=[
            ClipRequest(
                clip_id=clip_id_1,
                start_sec=0.0,
                end_sec=5.0,
                knowledge_type="process",
                semantic_unit_id="SU907",
            ),
            ClipRequest(
                clip_id=clip_id_2,
                start_sec=6.0,
                end_sec=12.0,
                knowledge_type="process",
                semantic_unit_id="SU907",
            ),
        ],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(assets_dir),
        clips_dir=str(assets_dir),
        material_requests=requests,
    )

    assert unit.materials is not None
    assert unit.materials.clip_path == str(clip_1.resolve())
    assert unit.materials.clip_paths == [
        str(clip_1.resolve()),
        str(clip_2.resolve()),
    ]


def test_apply_external_materials_reuses_prevalidated_concrete_result(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    shot_name = "SU906/SU906_head"
    shot_path = screenshots_dir / f"{shot_name}.png"
    shot_path.parent.mkdir(parents=True, exist_ok=True)
    shot_path.write_bytes(b"img")

    unit = SemanticUnit(
        unit_id="SU906",
        knowledge_type="abstract",
        knowledge_topic="Coref Reuse",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=20.0,
    )

    class _Result:
        should_include = True
        reason = "prevalidated"
        img_description = "prevalidated_desc"

    class _FailValidator:
        def validate(self, image_path: str):
            raise AssertionError("validate should not be called when prevalidated cache exists")

    pipeline._concrete_validator = _FailValidator()
    pipeline._prevalidated_concrete_results[str(shot_path.resolve())] = _Result()

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id=shot_name,
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SU906",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests,
    )

    assert len(unit.materials.screenshot_paths) == 1
    assert unit.materials.screenshot_items[0]["img_description"] == "prevalidated_desc"


def test_apply_external_materials_records_sentence_mapping_for_screenshot_item(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

    class _AlwaysInclude:
        def validate(self, image_path: str):
            class _Result:
                should_include = True
                reason = "ok"
                img_description = "vision_desc"

            return _Result()

    pipeline._concrete_validator = _AlwaysInclude()

    class _Subtitle:
        def __init__(self, text: str, start_sec: float, end_sec: float, subtitle_id: str):
            self.text = text
            self.start_sec = start_sec
            self.end_sec = end_sec
            self.subtitle_id = subtitle_id

    pipeline.subtitles = [
        _Subtitle("step one", 0.0, 3.0, "S001"),
        _Subtitle("step two", 3.0, 8.0, "S002"),
    ]

    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    shot_name = "SU950/SU950_head"
    shot_path = screenshots_dir / f"{shot_name}.png"
    shot_path.parent.mkdir(parents=True, exist_ok=True)
    shot_path.write_bytes(b"img")

    unit = SemanticUnit(
        unit_id="SU950",
        knowledge_type="abstract",
        knowledge_topic="Sentence Mapping",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=20.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id=shot_name,
                timestamp_sec=5.0,
                label="head",
                semantic_unit_id="SU950",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )

    pipeline._apply_external_materials(
        unit=unit,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests,
    )

    assert len(unit.materials.screenshot_items) == 1
    item = unit.materials.screenshot_items[0]
    assert item["img_description"] == "vision_desc"
    assert item["sentence_id"] == "S002"
    assert item["sentence_text"] == "step two"
    assert float(item["timestamp_sec"]) == 5.0


def test_apply_external_materials_writes_image_match_audit(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "content_pipeline:",
                "  observability:",
                "    image_match_audit:",
                "      enabled: true",
            ]
        ),
        encoding="utf-8",
    )

    import os
    os.environ["MODULE2_CONFIG_PATH"] = str(config_path)

    try:
        pipeline, output_dir = _build_pipeline(tmp_path)

        class _AlwaysInclude:
            def validate(self, image_path: str):
                class _Result:
                    should_include = True
                    reason = "ok"
                    img_description = "audit_desc"

                return _Result()

        pipeline._concrete_validator = _AlwaysInclude()

        class _Subtitle:
            def __init__(self, text: str, start_sec: float, end_sec: float, subtitle_id: str):
                self.text = text
                self.start_sec = start_sec
                self.end_sec = end_sec
                self.subtitle_id = subtitle_id

        pipeline.subtitles = [
            _Subtitle("step one", 0.0, 3.0, "S001"),
            _Subtitle("step two", 3.0, 8.0, "S002"),
        ]

        screenshots_dir = output_dir / "assets"
        clips_dir = output_dir / "assets"
        screenshots_dir.mkdir(parents=True, exist_ok=True)
        clips_dir.mkdir(parents=True, exist_ok=True)

        shot_name = "SU960/SU960_head"
        shot_path = screenshots_dir / f"{shot_name}.png"
        shot_path.parent.mkdir(parents=True, exist_ok=True)
        shot_path.write_bytes(b"img")

        unit = SemanticUnit(
            unit_id="SU960",
            knowledge_type="abstract",
            knowledge_topic="Audit Mapping",
            full_text="demo",
            source_paragraph_ids=[],
            source_sentence_ids=[],
            start_sec=0.0,
            end_sec=20.0,
        )

        requests = MaterialRequests(
            screenshot_requests=[
                ScreenshotRequest(
                    screenshot_id=shot_name,
                    timestamp_sec=5.0,
                    label="head",
                    semantic_unit_id="SU960",
                )
            ],
            clip_requests=[],
            action_classifications=[],
        )

        pipeline._apply_external_materials(
            unit=unit,
            screenshots_dir=str(screenshots_dir),
            clips_dir=str(clips_dir),
            material_requests=requests,
        )
        audit_path = pipeline._flush_image_match_audit()

        assert audit_path
        from pathlib import Path as _Path
        path_obj = _Path(audit_path)
        assert path_obj.exists()

        import json
        payload = json.loads(path_obj.read_text(encoding="utf-8"))
        records = payload.get("records", [])
        assert records
        first = records[0]
        assert first["unit_id"] == "SU960"
        assert first["sentence_id"] == "S002"
        assert first["mapping_status"] == "mapped"
    finally:
        os.environ.pop("MODULE2_CONFIG_PATH", None)


def test_analyze_only_exposes_phase2a_contract(tmp_path):
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
        unit_id="SUA01",
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

    async def _fake_collect_requests(current_unit):
        return MaterialRequests(
            screenshot_requests=[
                ScreenshotRequest(
                    screenshot_id=f"{current_unit.unit_id}/{current_unit.unit_id}_head",
                    timestamp_sec=1.0,
                    label="head",
                    semantic_unit_id=current_unit.unit_id,
                )
            ],
            clip_requests=[
                ClipRequest(
                    clip_id=f"{current_unit.unit_id}/{current_unit.unit_id}_clip",
                    start_sec=0.5,
                    end_sec=2.5,
                    knowledge_type="process",
                    semantic_unit_id=current_unit.unit_id,
                )
            ],
            action_classifications=[],
        )

    pipeline.segmenter = types.SimpleNamespace(segment=_fake_segment)
    pipeline._apply_modality_classification = _fake_apply_modality
    pipeline._collect_material_requests = _fake_collect_requests

    screenshot_requests, clip_requests, semantic_units_path = asyncio.run(pipeline.analyze_only())

    assert len(screenshot_requests) == 1
    assert len(clip_requests) == 1
    assert Path(semantic_units_path).exists()

    import json

    payload = json.loads(Path(semantic_units_path).read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    assert payload.get("knowledge_groups")
    first_group = payload["knowledge_groups"][0]
    assert first_group.get("units")
    first_unit = first_group["units"][0]
    assert first_unit["unit_id"] == "SUA01"
    assert first_unit["material_requests"]["screenshot_requests"]
    assert first_unit["material_requests"]["clip_requests"]


def test_assemble_only_exposes_phase2b_contract(tmp_path):
    output_dir = tmp_path / "out"
    assets_dir = output_dir / "assets" / "SUB01"
    assets_dir.mkdir(parents=True, exist_ok=True)

    screenshot_path = assets_dir / "SUB01_head.png"
    clip_path = assets_dir / "SUB01_clip.mp4"
    screenshot_path.write_bytes(b"img")
    clip_path.write_bytes(b"video")

    semantic_units_path = output_dir / "semantic_units_phase2a.json"
    semantic_units_path.write_text(
        """
{
  "schema_version": "phase2a.grouped.v1",
  "knowledge_groups": [
    {
      "group_id": 1,
      "group_name": "Assemble Topic",
      "reason": "同一核心论点聚合",
      "units": [
        {
          "unit_id": "SUB01",
          "knowledge_type": "process",
          "knowledge_topic": "Assemble Topic",
          "full_text": "Assemble body",
          "source_paragraph_ids": [],
          "source_sentence_ids": [],
          "start_sec": 0.0,
          "end_sec": 8.0,
          "stable_islands": [],
          "action_segments": [],
          "material_requests": {
            "screenshot_requests": [
              {
                "screenshot_id": "SUB01/SUB01_head",
                "timestamp_sec": 1.0,
                "label": "head",
                "semantic_unit_id": "SUB01"
              }
            ],
            "clip_requests": [
              {
                "clip_id": "SUB01/SUB01_clip",
                "start_sec": 0.2,
                "end_sec": 2.4,
                "knowledge_type": "process",
                "semantic_unit_id": "SUB01"
              }
            ]
          }
        }
      ]
    }
  ]
}
""".strip(),
        encoding="utf-8",
    )

    pipeline = RichTextPipeline(
        video_path="",
        step2_path="",
        step6_path="",
        output_dir=str(output_dir),
    )

    markdown_path, json_path = asyncio.run(
        pipeline.assemble_only(
            semantic_units_json_path=str(semantic_units_path),
            screenshots_dir=str(output_dir / "assets"),
            clips_dir=str(output_dir / "assets"),
            title="Assemble Title",
        )
    )

    assert Path(markdown_path).exists()
    assert Path(json_path).exists()
    assert Path(markdown_path).name == "Assemble Title.md"

    import json

    doc_payload = json.loads(Path(json_path).read_text(encoding="utf-8"))
    assert doc_payload.get("title") == "Assemble Title"
    assert doc_payload.get("knowledge_groups")
    first_group = doc_payload["knowledge_groups"][0]
    assert first_group.get("units")
    assert first_group["units"][0]["unit_id"] == "SUB01"


