import asyncio
from pathlib import Path

from MVP_Module2_HEANCING.module2_content_enhancement.rich_text_pipeline import (
    ClipRequest,
    MaterialRequests,
    RichTextPipeline,
    ScreenshotRequest,
)
from MVP_Module2_HEANCING.module2_content_enhancement.semantic_unit_segmenter import SemanticUnit


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


def test_apply_external_materials_reorganizes_into_unit_assets_dir(tmp_path):
    pipeline, output_dir = _build_pipeline(tmp_path)

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
    (screenshots_dir / "SU901").mkdir(parents=True, exist_ok=True)
    (clips_dir / "SU901").mkdir(parents=True, exist_ok=True)

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
                screenshot_id="missing_id",
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SU901",
            )
        ],
        clip_requests=[
            ClipRequest(
                clip_id="missing_clip",
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
    assert unit.materials.screenshot_paths, "screenshot should be found and normalized"
    assert unit.materials.clip_path, "clip should be found and normalized"

    screenshot_path = Path(unit.materials.screenshot_paths[0])
    clip_path = Path(unit.materials.clip_path)

    assert screenshot_path.exists()
    assert clip_path.exists()
    assert screenshot_path.parent.name == "SU901"
    assert clip_path.parent.name == "SU901"
    assert screenshot_path.name.lower().startswith("su901_")
    assert clip_path.name.lower().startswith("su901_")

    # ??????????? assets ???
    assets_dir = output_dir / "assets" / "SU901"
    assert str(screenshot_path).startswith(str(assets_dir))
    assert str(clip_path).startswith(str(assets_dir))


def test_apply_external_materials_prefers_request_id_for_normalized_names(tmp_path):
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
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    # ?????????????????? request_id ???????
    (screenshots_dir / "raw_abc.png").write_bytes(b"img")
    (clips_dir / "raw_clip.mp4").write_bytes(b"video")

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

    # 请求 ID 含语义单元子路径，提取阶段应能直接命中该层级
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

    assert "open_settings" in screenshot_path.name.lower()
    assert "open_settings" in clip_path.name.lower()


def test_apply_external_materials_validates_concept_screenshots(tmp_path):
    pipeline, _ = _build_pipeline(tmp_path)

    screenshots_dir = tmp_path / "incoming_screens"
    clips_dir = tmp_path / "incoming_clips"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    keep_name = "SU903_step_head"
    drop_name = "SU903_step_tail"
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
            if keep_name in image_path:
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
    assert keep_name.lower() in Path(unit.materials.screenshot_paths[0]).name.lower()


def test_apply_external_materials_skips_validation_for_process(tmp_path):
    pipeline, _ = _build_pipeline(tmp_path)

    screenshots_dir = tmp_path / "incoming_screens"
    clips_dir = tmp_path / "incoming_clips"
    screenshots_dir.mkdir(parents=True, exist_ok=True)
    clips_dir.mkdir(parents=True, exist_ok=True)

    shot_name = "SU904_step_head"
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
