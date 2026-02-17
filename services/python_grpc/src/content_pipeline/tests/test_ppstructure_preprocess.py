import cv2
import numpy as np
from pathlib import Path
from types import SimpleNamespace

from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import (
    ConcreteKnowledgeValidator,
)
from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import (
    SemanticUnit,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import (
    MaterialRequests,
    ScreenshotRequest,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline import RichTextPipeline


def _write_dummy_image(path: Path, width: int = 320, height: int = 200) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = np.full((height, width, 3), 255, dtype=np.uint8)
    cv2.rectangle(image, (20, 20), (120, 80), (0, 0, 255), 2)
    cv2.rectangle(image, (140, 30), (260, 120), (255, 0, 0), 2)
    cv2.imwrite(str(path), image)


def _build_pipeline(tmp_path: Path) -> RichTextPipeline:
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True, exist_ok=True)
    return RichTextPipeline(
        video_path="",
        step2_path="",
        step6_path="",
        output_dir=str(output_dir),
    )


def test_extract_structured_screenshots_groups_expected_types(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU900" / "raw.png"
    _write_dummy_image(image_path)

    validator._structure_preprocess_enabled = True
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "figure", "bbox": (10, 10, 80, 80)},
            {"type": "figure caption", "bbox": (12, 82, 120, 100)},
            {"type": "figure title", "bbox": (10, 0, 140, 12)},
            {"type": "table", "bbox": (150, 20, 280, 120)},
            {"type": "table caption", "bbox": (150, 122, 300, 145)},
            {"type": "algorithm", "bbox": (20, 120, 100, 180)},
            {"type": "formula", "bbox": (120, 130, 190, 180)},
            {"type": "image", "bbox": (220, 130, 300, 190)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU900/raw",
        timestamp_sec=12.3,
    )

    assert outputs is not None
    assert len(outputs) == 5
    group_types = sorted(item.get("group_type") for item in outputs)
    assert group_types == ["algorithm", "figure_bundle", "formula", "image", "table_bundle"]
    assert all(Path(item["image_path"]).exists() for item in outputs)


def test_extract_structured_screenshots_includes_bbox_metadata(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU911" / "raw.png"
    _write_dummy_image(image_path, width=320, height=200)

    validator._structure_preprocess_enabled = True
    validator._structure_crop_margin_px = 0
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "algorithm", "bbox": (20, 30, 120, 130)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU911/raw",
        timestamp_sec=5.0,
    )

    assert outputs is not None
    assert len(outputs) == 1
    item = outputs[0]
    assert item["bbox_xyxy"] == [20, 30, 120, 130]
    assert item["parent_image_size"] == [320, 200]
    assert item["bbox_normalized_xyxy"][0] == 20.0 / 320.0
    assert item["bbox_normalized_xyxy"][1] == 30.0 / 200.0


def test_extract_structured_screenshots_merges_high_overlap_bboxes(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU905" / "raw.png"
    _write_dummy_image(image_path)

    validator._structure_preprocess_enabled = True
    validator._structure_bbox_overlap_merge_threshold = 0.9
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "algorithm", "bbox": (20, 20, 140, 140)},
            {"type": "algorithm", "bbox": (24, 24, 138, 138)},
            {"type": "formula", "bbox": (180, 40, 280, 140)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU905/raw",
        timestamp_sec=6.0,
    )

    assert outputs is not None
    group_types = sorted(item.get("group_type") for item in outputs)
    assert group_types == ["algorithm", "formula"]


def test_extract_structured_screenshots_absorbs_image_when_overlapped_by_bundle(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU918" / "raw.png"
    _write_dummy_image(image_path)

    validator._structure_preprocess_enabled = True
    validator._structure_bbox_overlap_merge_threshold = 0.9
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "figure", "bbox": (10, 10, 170, 140)},
            {"type": "figure caption", "bbox": (15, 142, 170, 170)},
            {"type": "image", "bbox": (20, 20, 160, 130)},
            {"type": "image", "bbox": (220, 20, 300, 90)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU918/raw",
        timestamp_sec=8.2,
    )

    assert outputs is not None
    group_types = sorted(item.get("group_type") for item in outputs)
    assert group_types == ["figure_bundle", "image"]


def test_extract_structured_screenshots_merges_all_high_overlap_children_into_bundle(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU919" / "raw.png"
    _write_dummy_image(image_path)

    validator._structure_preprocess_enabled = True
    validator._structure_bbox_overlap_merge_threshold = 0.9
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "figure", "bbox": (20, 20, 180, 150)},
            {"type": "image", "bbox": (24, 24, 176, 146)},
            {"type": "algorithm", "bbox": (26, 26, 174, 144)},
            {"type": "formula", "bbox": (28, 28, 172, 142)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU919/raw",
        timestamp_sec=8.8,
    )

    assert outputs is not None
    group_types = sorted(item.get("group_type") for item in outputs)
    assert group_types == ["figure_bundle"]


def test_extract_structured_screenshots_expands_bundle_with_nearby_text_and_code(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU921" / "raw.png"
    _write_dummy_image(image_path, width=260, height=220)

    validator._structure_preprocess_enabled = True
    validator._structure_crop_margin_px = 0
    validator._structure_context_nearby_px = 3
    validator._structure_target_types = {
        "figure",
        "figure caption",
        "figure title",
        "table",
        "table caption",
        "algorithm",
        "formula",
        "image",
    }
    validator._structure_context_types = {"text", "code"}
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "figure", "bbox": (50, 50, 120, 120)},
            {"type": "text", "bbox": (122, 50, 170, 120)},
            {"type": "code", "bbox": (50, 122, 120, 180)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU921/raw",
        timestamp_sec=9.1,
    )

    assert outputs is not None
    assert len(outputs) == 1
    assert outputs[0]["group_type"] == "figure_bundle"
    expanded = cv2.imread(outputs[0]["image_path"])
    assert expanded is not None
    assert expanded.shape[1] == 120
    assert expanded.shape[0] == 130


def test_extract_structured_screenshots_fallback_to_raw_when_bbox_coverage_too_large(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU906" / "raw.png"
    _write_dummy_image(image_path, width=320, height=200)

    validator._structure_preprocess_enabled = True
    validator._structure_skip_split_bbox_coverage_threshold = 0.6
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "figure", "bbox": (10, 8, 310, 190)},
            {"type": "figure caption", "bbox": (12, 192, 300, 199)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU906/raw",
        timestamp_sec=7.5,
    )

    assert outputs is None


def test_extract_structured_screenshots_still_splits_when_bbox_coverage_below_threshold(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU907" / "raw.png"
    _write_dummy_image(image_path, width=320, height=200)

    validator._structure_preprocess_enabled = True
    validator._structure_skip_split_bbox_coverage_threshold = 0.95
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "algorithm", "bbox": (20, 20, 140, 120)},
            {"type": "formula", "bbox": (180, 30, 280, 120)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU907/raw",
        timestamp_sec=8.0,
    )

    assert outputs is not None
    group_types = sorted(item.get("group_type") for item in outputs)
    assert group_types == ["algorithm", "formula"]


def test_extract_structured_screenshots_fallback_uses_recognized_enclosing_bbox(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU930" / "raw.png"
    _write_dummy_image(image_path, width=1000, height=1000)

    validator._structure_preprocess_enabled = True
    validator._structure_skip_split_bbox_coverage_threshold = 0.9
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "algorithm", "bbox": (100, 100, 200, 200)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU930/raw",
        timestamp_sec=10.0,
    )

    # Coverage should be computed against recognized enclosing bbox (100x100), not full image.
    assert outputs is None


def test_extract_structured_screenshots_fallback_uses_sum_area_for_required_types(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU931" / "raw.png"
    _write_dummy_image(image_path, width=320, height=200)

    validator._structure_preprocess_enabled = True
    validator._structure_skip_split_bbox_coverage_threshold = 0.8
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks",
        lambda _: [
            {"type": "algorithm", "bbox": (20, 20, 140, 120)},   # area 12000
            {"type": "image", "bbox": (80, 40, 200, 140)},       # area 12000
            {"type": "header", "bbox": (10, 10, 210, 150)},      # recognized enclosing area 28000
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU931/raw",
        timestamp_sec=11.0,
    )

    # 24000 / 28000 = 0.8571; this should fallback only when numerator uses area sum.
    assert outputs is None


def test_dedupe_structured_candidates_skips_same_parent_comparison(tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    validator._structure_dedup_similarity_threshold = 0.95

    img_a = tmp_path / "assets" / "SU901" / "a.png"
    img_b = tmp_path / "assets" / "SU901" / "b.png"
    _write_dummy_image(img_a, width=120, height=80)
    _write_dummy_image(img_b, width=120, height=80)

    candidates = [
        {
            "image_path": str(img_a),
            "parent_key": "SU901/raw",
            "timestamp_sec": 1.0,
            "is_structured_crop": True,
        },
        {
            "image_path": str(img_b),
            "parent_key": "SU901/raw",
            "timestamp_sec": 1.1,
            "is_structured_crop": True,
        },
    ]

    kept = validator.dedupe_structured_candidates_keep_latest(candidates)
    assert len(kept) == 2
    assert img_a.exists()
    assert img_b.exists()


def test_dedupe_structured_candidates_keeps_earlier_across_parents(tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    validator._structure_dedup_similarity_threshold = 0.95

    img_old = tmp_path / "assets" / "SU910" / "old.png"
    img_new = tmp_path / "assets" / "SU920" / "new.png"
    _write_dummy_image(img_old, width=120, height=80)
    _write_dummy_image(img_new, width=120, height=80)

    candidates = [
        {
            "image_path": str(img_old),
            "parent_key": "SU910/raw",
            "timestamp_sec": 10.0,
            "is_structured_crop": True,
        },
        {
            "image_path": str(img_new),
            "parent_key": "SU920/raw",
            "timestamp_sec": 20.0,
            "is_structured_crop": True,
        },
    ]

    kept = validator.dedupe_structured_candidates_keep_latest(candidates)
    assert len(kept) == 1
    assert kept[0]["parent_key"] == "SU910/raw"
    assert img_old.exists()
    assert not img_new.exists()


def test_dedupe_structured_candidates_only_removes_exact_duplicates(tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    validator._structure_dedup_similarity_threshold = 0.95

    img_old = tmp_path / "assets" / "SU930" / "old.png"
    img_new = tmp_path / "assets" / "SU940" / "new.png"
    _write_dummy_image(img_old, width=120, height=80)
    _write_dummy_image(img_new, width=120, height=80)

    modified = cv2.imread(str(img_new))
    assert modified is not None
    cv2.circle(modified, (8, 8), 4, (0, 255, 0), -1)
    cv2.imwrite(str(img_new), modified)

    candidates = [
        {
            "image_path": str(img_old),
            "parent_key": "SU930/raw",
            "timestamp_sec": 10.0,
            "is_structured_crop": True,
        },
        {
            "image_path": str(img_new),
            "parent_key": "SU940/raw",
            "timestamp_sec": 20.0,
            "is_structured_crop": True,
        },
    ]

    kept = validator.dedupe_structured_candidates_keep_latest(candidates)
    assert len(kept) == 2
    assert img_old.exists()
    assert img_new.exists()


def test_extract_structured_screenshots_runtime_error_returns_none(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU915" / "raw.png"
    _write_dummy_image(image_path)

    validator._structure_preprocess_enabled = True
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: object())
    monkeypatch.setattr(validator, "_collect_structure_blocks", lambda _: None)

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU915/raw",
        timestamp_sec=1.0,
    )
    assert outputs is None


def test_extract_structured_screenshots_with_paddlex_fallback(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU916" / "raw.png"
    _write_dummy_image(image_path, width=320, height=220)

    validator._structure_preprocess_enabled = True
    monkeypatch.setattr(validator, "_get_structure_engine", lambda: None)
    monkeypatch.setattr(validator, "_get_paddlex_layout_model", lambda: object())
    monkeypatch.setattr(
        validator,
        "_collect_structure_blocks_via_paddlex",
        lambda _: [
            {"type": "figure", "bbox": (20, 20, 140, 120)},
            {"type": "figure_title", "bbox": (20, 0, 180, 18)},
            {"type": "table", "bbox": (160, 30, 300, 150)},
            {"type": "algorithm", "bbox": (30, 150, 130, 210)},
        ],
    )

    outputs = validator.extract_structured_screenshots(
        image_path=str(image_path),
        source_id="SU916/raw",
        timestamp_sec=2.0,
    )

    assert outputs is not None
    assert len(outputs) == 3
    group_types = sorted(item.get("group_type") for item in outputs)
    assert group_types == ["algorithm", "figure_bundle", "table_bundle"]


def test_collect_structure_blocks_disables_ppstructure_after_backend_error(monkeypatch, tmp_path):
    validator = ConcreteKnowledgeValidator(output_dir=str(tmp_path))
    image_path = tmp_path / "assets" / "SU917" / "raw.png"
    _write_dummy_image(image_path, width=260, height=180)

    class _BrokenEngine:
        def __call__(self, _):
            raise RuntimeError(
                "OneDnnContext does not have the input Filter. [operator < fused_conv2d > error]"
            )

    validator._structure_preprocess_enabled = True
    validator._structure_disable_after_backend_error = True
    validator._structure_engine = _BrokenEngine()
    validator._structure_engine_init_error = None

    fallback_calls = {"count": 0}

    def _fallback(_):
        fallback_calls["count"] += 1
        return []

    monkeypatch.setattr(validator, "_collect_structure_blocks_via_paddlex", _fallback)

    outputs = validator._collect_structure_blocks(str(image_path))
    assert outputs == []
    assert fallback_calls["count"] == 1
    assert validator._structure_engine is None
    assert validator._structure_engine_init_error == "runtime_backend_error:RuntimeError"
    assert validator._get_structure_engine() is None


def test_apply_external_materials_uses_structured_crops_and_skips_unit_scan_fallback(tmp_path):
    pipeline = _build_pipeline(tmp_path)
    output_dir = Path(pipeline.output_dir)
    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    (screenshots_dir / "SU930").mkdir(parents=True, exist_ok=True)
    raw_img = screenshots_dir / "SU930" / "raw.png"
    _write_dummy_image(raw_img, width=200, height=120)

    class _StubValidator:
        def __init__(self):
            self.validate_called = 0

        def extract_structured_screenshots(self, image_path: str, source_id: str = "", timestamp_sec=None):
            return []

        def dedupe_structured_candidates_keep_latest(self, candidates):
            return candidates

        def validate(self, image_path: str, skip_duplicate_check: bool = False):
            self.validate_called += 1
            return SimpleNamespace(should_include=False, img_description="ocr text", reason="text-only")

    stub_validator = _StubValidator()
    pipeline._concrete_validator = stub_validator

    unit = SemanticUnit(
        unit_id="SU930",
        knowledge_type="concrete",
        knowledge_topic="topic",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SU930/raw",
                timestamp_sec=3.0,
                label="head",
                semantic_unit_id="SU930",
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

    assert stub_validator.validate_called == 1
    assert unit.materials is not None
    assert unit.materials.screenshot_paths == []
    assert len(unit.materials.screenshot_items) == 1
    assert unit.materials.screenshot_items[0].get("should_include") is False
    assert unit.materials.screenshot_items[0].get("img_description") == "ocr text"
    assert unit.materials.screenshot_items[0].get("img_path") == str(raw_img.resolve())
    assert not raw_img.exists()


def test_apply_external_materials_pre_dedup_skips_repeated_ppstructure_and_validate(tmp_path):
    pipeline = _build_pipeline(tmp_path)
    output_dir = Path(pipeline.output_dir)
    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    (screenshots_dir / "SU941").mkdir(parents=True, exist_ok=True)
    img_a = screenshots_dir / "SU941" / "a.png"
    img_b = screenshots_dir / "SU941" / "b.png"
    _write_dummy_image(img_a, width=200, height=120)
    _write_dummy_image(img_b, width=200, height=120)

    class _StubValidator:
        def __init__(self):
            self.extract_called = 0
            self.validate_called = 0

        def extract_structured_screenshots(self, image_path: str, source_id: str = "", timestamp_sec=None):
            self.extract_called += 1
            return None

        def dedupe_structured_candidates_keep_latest(self, candidates):
            return candidates

        def validate(self, image_path: str, skip_duplicate_check: bool = False):
            self.validate_called += 1
            return SimpleNamespace(should_include=True, img_description="ok", reason="")

    stub_validator = _StubValidator()
    pipeline._concrete_validator = stub_validator

    unit = SemanticUnit(
        unit_id="SU941",
        knowledge_type="concrete",
        knowledge_topic="topic",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SU941/a",
                timestamp_sec=1.0,
                label="first",
                semantic_unit_id="SU941",
            ),
            ScreenshotRequest(
                screenshot_id="SU941/b",
                timestamp_sec=2.0,
                label="second",
                semantic_unit_id="SU941",
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

    assert stub_validator.extract_called == 1
    assert stub_validator.validate_called == 1
    assert img_a.exists()
    assert not img_b.exists()


def test_apply_external_materials_grouped_structured_prevalidation(tmp_path):
    pipeline = _build_pipeline(tmp_path)
    output_dir = Path(pipeline.output_dir)
    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    (screenshots_dir / "SU961").mkdir(parents=True, exist_ok=True)
    raw_img = screenshots_dir / "SU961" / "raw.png"
    _write_dummy_image(raw_img, width=240, height=160)

    class _StubValidator:
        def __init__(self):
            self.group_called = 0
            self.validate_called = 0

        def extract_structured_screenshots(self, image_path: str, source_id: str = "", timestamp_sec=None):
            crop_1 = Path(image_path).parent / "raw__ppstructure_figure_bundle_01.png"
            crop_2 = Path(image_path).parent / "raw__ppstructure_algorithm_02.png"
            _write_dummy_image(crop_1, width=100, height=80)
            _write_dummy_image(crop_2, width=90, height=70)
            parent_key = str(Path(image_path).resolve())
            return [
                {
                    "image_path": str(crop_1),
                    "group_type": "figure_bundle",
                    "source_id": source_id,
                    "timestamp_sec": timestamp_sec,
                    "parent_image_path": image_path,
                    "parent_key": parent_key,
                    "is_structured_crop": True,
                    "crop_index": 1,
                    "bbox_xyxy": [10, 10, 120, 100],
                    "bbox_normalized_xyxy": [0.0417, 0.0625, 0.5, 0.625],
                    "parent_image_size": [240, 160],
                },
                {
                    "image_path": str(crop_2),
                    "group_type": "algorithm",
                    "source_id": source_id,
                    "timestamp_sec": timestamp_sec,
                    "parent_image_path": image_path,
                    "parent_key": parent_key,
                    "is_structured_crop": True,
                    "crop_index": 2,
                    "bbox_xyxy": [130, 20, 220, 120],
                    "bbox_normalized_xyxy": [0.5417, 0.1250, 0.9167, 0.7500],
                    "parent_image_size": [240, 160],
                },
            ]

        def dedupe_structured_candidates_keep_latest(self, candidates):
            return candidates

        def validate_structured_group(self, *, parent_image_path: str, items, ocr_text: str = ""):
            self.group_called += 1
            assert len(items) == 2
            assert Path(parent_image_path).name == "raw.png"
            assert items[0].get("bbox_xyxy") == [10, 10, 120, 100]
            assert items[1].get("bbox_xyxy") == [130, 20, 220, 120]
            return [
                SimpleNamespace(should_include=True, img_description="group_desc_1", reason=""),
                SimpleNamespace(should_include=True, img_description="group_desc_2", reason=""),
            ]

        def validate(self, image_path: str, ocr_text: str = "", skip_duplicate_check: bool = False):
            self.validate_called += 1
            raise AssertionError("single-image validate should not be called for grouped structured crops")

    stub_validator = _StubValidator()
    pipeline._concrete_validator = stub_validator

    unit = SemanticUnit(
        unit_id="SU961",
        knowledge_type="concrete",
        knowledge_topic="topic",
        full_text="demo",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )

    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SU961/raw",
                timestamp_sec=3.0,
                label="head",
                semantic_unit_id="SU961",
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

    assert stub_validator.group_called == 1
    assert stub_validator.validate_called == 0
    assert unit.materials is not None
    assert len(unit.materials.screenshot_paths) == 2
    descriptions = [item.get("img_description") for item in unit.materials.screenshot_items]
    assert descriptions == ["group_desc_1", "group_desc_2"]


def test_apply_external_materials_pre_dedup_skips_cross_unit_ppstructure_reextract(tmp_path):
    pipeline = _build_pipeline(tmp_path)
    output_dir = Path(pipeline.output_dir)
    screenshots_dir = output_dir / "assets"
    clips_dir = output_dir / "assets"
    (screenshots_dir / "SU951").mkdir(parents=True, exist_ok=True)
    (screenshots_dir / "SU952").mkdir(parents=True, exist_ok=True)
    img_a = screenshots_dir / "SU951" / "a.png"
    img_b = screenshots_dir / "SU952" / "b.png"
    _write_dummy_image(img_a, width=220, height=140)
    _write_dummy_image(img_b, width=220, height=140)

    class _StubValidator:
        def __init__(self):
            self.extract_called = 0
            self.validate_called = 0

        def extract_structured_screenshots(self, image_path: str, source_id: str = "", timestamp_sec=None):
            self.extract_called += 1
            return None

        def dedupe_structured_candidates_keep_latest(self, candidates):
            return candidates

        def validate(self, image_path: str, skip_duplicate_check: bool = False):
            self.validate_called += 1
            return SimpleNamespace(should_include=True, img_description="ok", reason="")

    stub_validator = _StubValidator()
    pipeline._concrete_validator = stub_validator

    unit_a = SemanticUnit(
        unit_id="SU951",
        knowledge_type="concrete",
        knowledge_topic="topic-a",
        full_text="demo-a",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=0.0,
        end_sec=8.0,
    )
    requests_a = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SU951/a",
                timestamp_sec=1.0,
                label="first",
                semantic_unit_id="SU951",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )
    pipeline._apply_external_materials(
        unit=unit_a,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests_a,
    )

    assert stub_validator.extract_called == 1
    assert stub_validator.validate_called == 1
    assert img_a.exists()

    unit_b = SemanticUnit(
        unit_id="SU952",
        knowledge_type="concrete",
        knowledge_topic="topic-b",
        full_text="demo-b",
        source_paragraph_ids=[],
        source_sentence_ids=[],
        start_sec=8.0,
        end_sec=16.0,
    )
    requests_b = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SU952/b",
                timestamp_sec=9.0,
                label="second",
                semantic_unit_id="SU952",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )
    pipeline._apply_external_materials(
        unit=unit_b,
        screenshots_dir=str(screenshots_dir),
        clips_dir=str(clips_dir),
        material_requests=requests_b,
    )

    assert stub_validator.extract_called == 1
    assert stub_validator.validate_called == 1
    assert not img_b.exists()
    assert unit_b.materials is not None
    assert unit_b.materials.screenshot_paths == []
    assert unit_b.materials.screenshot_items == []


def test_extract_text_page_description_prefers_upstream_ocr_text():
    validator = ConcreteKnowledgeValidator.__new__(ConcreteKnowledgeValidator)
    validator._ocr_extractor = None
    validator._ocr_extractor_init_error = None

    calls = {"count": 0}

    def _unexpected_getter():
        calls["count"] += 1
        raise AssertionError("upstream OCR exists, should not request local OCR extractor")

    validator._get_ocr_extractor = _unexpected_getter

    desc = validator._extract_text_page_description(
        image_path="unused.png",
        ocr_text="  line1\nline2  ",
    )

    assert desc == "line1 line2"
    assert calls["count"] == 0
