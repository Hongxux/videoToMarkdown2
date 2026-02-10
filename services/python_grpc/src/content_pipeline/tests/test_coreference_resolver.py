import asyncio
from pathlib import Path

from services.python_grpc.src.content_pipeline.coreference_resolver import CoreferenceResolver
from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import MaterialRequests, ScreenshotRequest
from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnit


class _FakeLLMNoNeedCV:
    async def complete_json(self, prompt: str, system_message: str = None):
        # 新规则：可由上下文解决时不输出断层项（等价于不替换）
        return {"gaps": []}, 0, {}


class _FakeLLMNeedCVThenRewrite:
    async def complete_json(self, prompt: str, system_message: str = None):
        # Encoding fixed: need_CV note.
        if system_message and "need_CV" in system_message:
            return {
                "gaps": [
                    {
                        "gap_id": "G1",
                        "sentence_text": "杩欎釜鎸夐挳浼氭墦寮€璁剧疆銆?,
                        "need_CV": True,
                    }
                ]
            }, 0, {}

        # Encoding fixed: corrupted comment cleaned.
            return {
                "replaced_text": "璁剧疆鎸夐挳浼氭墦寮€璁剧疆銆?,
                "confidence": 0.92,
                "reason": "鏍规嵁鍥剧墖鎻忚堪鑷劧淇",
            }, 0, {}

        return {"gaps": []}, 0, {}


def _build_unit() -> SemanticUnit:
    return SemanticUnit(
        unit_id="SU100",
        knowledge_type="abstract",
        knowledge_topic="璁剧疆鍏ュ彛",
        full_text="杩欎釜鎸夐挳浼氭墦寮€璁剧疆銆傜劧鍚庡畠浼氬姞杞介厤缃€?,
        source_paragraph_ids=[],
        source_sentence_ids=["S001", "S002"],
        start_sec=0.0,
        end_sec=8.0,
    )


def _build_subtitles():
    class _Sub:
        def __init__(self, subtitle_id: str, text: str, start_sec: float, end_sec: float):
            self.subtitle_id = subtitle_id
            self.text = text
            self.start_sec = start_sec
            self.end_sec = end_sec

    return [
        _Sub("S001", "杩欎釜鎸夐挳浼氭墦寮€璁剧疆銆?, 0.0, 3.0),
        _Sub("S002", "鐒跺悗瀹冧細鍔犺浇閰嶇疆銆?, 3.0, 8.0),
    ]


def test_context_resolvable_keeps_original_expression(tmp_path):
    class _NoVisionConcrete:
        def validate_for_coreference(self, image_path: str, sentence_text: str, context_text: str = ""):
            raise AssertionError("no need_CV gaps should not trigger vision")

    resolver = CoreferenceResolver(
        llm_client=_FakeLLMNoNeedCV(),
        concrete_validator=_NoVisionConcrete(),
        screenshot_selector=None,
        confidence_threshold=0.8,
    )

    unit = _build_unit()
    before = unit.full_text
    result = asyncio.run(
        resolver.resolve_unit_coreference(
            unit=unit,
            material_requests=MaterialRequests([], [], []),
            screenshots_dir=str(tmp_path),
            sentence_timestamps={
                "S001": {"start_sec": 0.0, "end_sec": 3.0},
                "S002": {"start_sec": 3.0, "end_sec": 8.0},
            },
            subtitles=_build_subtitles(),
            video_path="",
        )
    )

    assert result.updated_text == before
    assert result.gaps == []
    assert result.prevalidated_results == {}


def test_need_cv_true_triggers_vision_and_rewrite(tmp_path):
    screenshot_path = tmp_path / "SU100_head.png"
    screenshot_path.write_bytes(b"img")

    class _ConcreteResult:
        should_include = True
        confidence = 0.9
        reason = "valid"
        img_description = "screen"

    class _VisionConcrete:
        def validate_for_coreference(self, image_path: str, sentence_text: str, context_text: str = ""):
            return {
                "img_description": "鍥句腑鏄?settings 鎸夐挳",
                "confidence": 0.92,
                "reason": "鍥剧墖鏄剧ず杩欐槸璁剧疆鎸夐挳",
                "concrete_result": _ConcreteResult(),
            }

    resolver = CoreferenceResolver(
        llm_client=_FakeLLMNeedCVThenRewrite(),
        concrete_validator=_VisionConcrete(),
        screenshot_selector=None,
        confidence_threshold=0.8,
    )

    unit = _build_unit()
    requests = MaterialRequests(
        screenshot_requests=[
            ScreenshotRequest(
                screenshot_id="SU100_head",
                timestamp_sec=1.0,
                label="head",
                semantic_unit_id="SU100",
            )
        ],
        clip_requests=[],
        action_classifications=[],
    )

    result = asyncio.run(
        resolver.resolve_unit_coreference(
            unit=unit,
            material_requests=requests,
            screenshots_dir=str(tmp_path),
            sentence_timestamps={
                "S001": {"start_sec": 0.0, "end_sec": 3.0},
                "S002": {"start_sec": 3.0, "end_sec": 8.0},
            },
            subtitles=_build_subtitles(),
            video_path="",
        )
    )

    assert "璁剧疆鎸夐挳浼氭墦寮€璁剧疆銆? in result.updated_text
    assert result.gaps and result.gaps[0].source == "vision_existing"
    assert str(Path(screenshot_path).resolve()) in result.prevalidated_results



