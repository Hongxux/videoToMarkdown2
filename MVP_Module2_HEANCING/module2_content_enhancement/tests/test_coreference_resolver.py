import asyncio
from pathlib import Path

from MVP_Module2_HEANCING.module2_content_enhancement.coreference_resolver import CoreferenceResolver
from MVP_Module2_HEANCING.module2_content_enhancement.rich_text_pipeline import MaterialRequests, ScreenshotRequest
from MVP_Module2_HEANCING.module2_content_enhancement.semantic_unit_segmenter import SemanticUnit


class _FakeLLMNoNeedCV:
    async def complete_json(self, prompt: str, system_message: str = None):
        # 新规则：可由上下文解决时不输出断层项（等价于不替换）
        return {"gaps": []}, 0, {}


class _FakeLLMNeedCVThenRewrite:
    async def complete_json(self, prompt: str, system_message: str = None):
        # 第一阶段：断层检测，仅输出 need_CV=true
        if system_message and "need_CV" in system_message:
            return {
                "gaps": [
                    {
                        "gap_id": "G1",
                        "sentence_text": "这个按钮会打开设置。",
                        "need_CV": True,
                    }
                ]
            }, 0, {}

        # 第二阶段：基于视觉描述自然改写
        if system_message and "润色专家" in system_message:
            return {
                "replaced_text": "设置按钮会打开设置。",
                "confidence": 0.92,
                "reason": "根据图片描述自然修复",
            }, 0, {}

        return {"gaps": []}, 0, {}


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
                "img_description": "图中是 settings 按钮",
                "confidence": 0.92,
                "reason": "图片显示这是设置按钮",
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

    assert "设置按钮会打开设置。" in result.updated_text
    assert result.gaps and result.gaps[0].source == "vision_existing"
    assert str(Path(screenshot_path).resolve()) in result.prevalidated_results

