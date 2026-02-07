import asyncio
from pathlib import Path

from MVP_Module2_HEANCING.module2_content_enhancement.coreference_resolver import CoreferenceResolver
from MVP_Module2_HEANCING.module2_content_enhancement.rich_text_pipeline import MaterialRequests, ScreenshotRequest
from MVP_Module2_HEANCING.module2_content_enhancement.semantic_unit_segmenter import SemanticUnit


class _FakeLLMHighConfidence:
    async def complete_json(self, prompt: str, system_message: str = None):
        return {
            "gaps": [
                {
                    "gap_id": "G1",
                    "sentence_text": "这个按钮会打开设置。",
                    "replaced_text": "设置按钮会打开设置。",
                    "confidence": 0.95,
                    "reason": "上下文明确",
                }
            ]
        }, 0, {}


class _FakeLLMLowConfidence:
    async def complete_json(self, prompt: str, system_message: str = None):
        return {
            "gaps": [
                {
                    "gap_id": "G1",
                    "sentence_text": "这个按钮会打开设置。",
                    "replaced_text": "该按钮会打开设置。",
                    "confidence": 0.5,
                    "reason": "低置信度，需视觉确认",
                }
            ]
        }, 0, {}


def _build_unit() -> SemanticUnit:
    return SemanticUnit(
        unit_id="SU100",
        knowledge_type="abstract",
        knowledge_topic="设置入口",
        full_text="这个按钮会打开设置。然后它会加载配置。",
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
        _Sub("S001", "这个按钮会打开设置。", 0.0, 3.0),
        _Sub("S002", "然后它会加载配置。", 3.0, 8.0),
    ]


def test_resolve_unit_coreference_high_confidence_uses_deepseek_only(tmp_path):
    class _NoVisionConcrete:
        def validate_for_coreference(self, image_path: str, sentence_text: str, context_text: str = ""):
            raise AssertionError("high-confidence gap should not trigger vision refinement")

    resolver = CoreferenceResolver(
        llm_client=_FakeLLMHighConfidence(),
        concrete_validator=_NoVisionConcrete(),
        screenshot_selector=None,
        confidence_threshold=0.8,
    )

    unit = _build_unit()
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

    assert "设置按钮会打开设置。" in result.updated_text
    assert result.prevalidated_results == {}
    assert result.gaps and result.gaps[0].source == "deepseek"


def test_resolve_unit_coreference_low_confidence_uses_existing_screenshot_and_cache(tmp_path):
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
                "replaced_text": "设置按钮会打开设置。",
                "replace_confidence": 0.92,
                "replace_reason": "图片显示这是设置按钮",
                "concrete_result": _ConcreteResult(),
            }

    resolver = CoreferenceResolver(
        llm_client=_FakeLLMLowConfidence(),
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

    assert "设置按钮会打开设置。" in result.updated_text
    assert result.gaps and result.gaps[0].source == "vision_existing"
    assert str(Path(screenshot_path).resolve()) in result.prevalidated_results

