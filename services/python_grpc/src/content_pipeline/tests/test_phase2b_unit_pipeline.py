import asyncio
import json

from services.python_grpc.src.content_pipeline.markdown_enhancer import EnhancedSection, MarkdownEnhancer
from services.python_grpc.src.content_pipeline.phase2b.pipeline_service import (
    Phase2bStructuredUnitPipeline,
    Phase2bUnitPipelineResult,
)


class _FakeEnhancer:
    def __init__(self, responses):
        self._responses = list(responses)
        self._structured_text_model = "fake-model"
        self.step_names = []

    def _normalize_knowledge_type(self, knowledge_type):
        return str(knowledge_type or "").strip().lower() or "abstract"

    def _resolve_concrete_base_text(self, section):
        return str(section.original_body or "")

    def _is_tutorial_process_section(self, section):
        return False

    def _build_augment_image_items(self, section):
        return []

    async def _augment_body_with_image_descriptions(self, section, base_text, augment_items):
        return base_text

    def _build_concept_image_items(self, section):
        return []

    def _build_concept_clip_items(self, section):
        return []

    def _build_concrete_keyframe_embeds_for_section(self, section, image_items):
        return [], {}

    def _build_concrete_clip_embeds_by_segment_order(self, section, clip_items):
        return []

    def _replace_tutorial_keyframe_placeholders(self, text, keyframe_embeds, keyframe_embed_map=None):
        return text

    def _replace_clip_placeholders(self, text, clip_embeds):
        return text

    def _replace_image_placeholders(self, text, image_items):
        return text

    def _replace_tutorial_legacy_placeholders(self, text, keyframe_embeds):
        return text

    def _strip_imgneeded_placeholders(self, text):
        return text

    def _append_missing_clip_embeds(self, text, clip_items):
        return text

    def _append_missing_image_embeds(self, text, image_items):
        return text

    def _restore_media_preserved_base_text(self, section, base_text, structured_text):
        return structured_text

    async def _write_llm_trace_record(self, **kwargs):
        return None

    async def _complete_text_with_model_fallback(self, **kwargs):
        return self._responses.pop(0), None, None

    async def _execute_recoverable_llm_call(self, *, step_name, call_factory, **kwargs):
        self.step_names.append(step_name)
        return await call_factory()

    async def _build_structured_text_for_concept(self, section, prev_title="", next_title=""):
        return "fallback"

    async def _build_structured_text_for_media_preserved_section(self, section, prev_title="", next_title=""):
        return "fallback"

    async def _enhance_and_extract(self, section):
        return "", "fallback"

    async def _enhance_text(self, section):
        return "fallback"

    async def _extract_logic(self, section):
        return "fallback"


def test_phase2b_structured_unit_pipeline_runs_phase1_phase2_phase3_in_order():
    enhancer = _FakeEnhancer(
        [
            """```json
{
  "sections": [
    {"id": "s1", "logic_tags": ["hierarchical"], "scene_tags": ["reading"], "title": "Part One"},
    {"id": "s2", "logic_tags": ["causal"], "scene_tags": ["reading"], "title": "Part Two"}
  ]
}
```
---
## s1: Part One
- alpha

## s2: Part Two
- beta
""",
            "- refined alpha",
            "- refined beta",
            "> **核心论点**：summary\n\n- **Part One**\n    - refined alpha\n\n- **Part Two**\n    - refined beta",
        ]
    )
    pipeline = Phase2bStructuredUnitPipeline(enhancer)
    section = EnhancedSection(
        unit_id="SU001",
        title="Unit Title",
        knowledge_type="abstract",
        original_body="original body",
    )

    result = asyncio.run(pipeline.process_unit(section, prev_title="Prev", next_title="Next"))

    assert result.fallback_used is False
    assert result.markdown.startswith("> **核心论点**")
    assert "refined alpha" in result.markdown
    assert "refined beta" in result.markdown
    assert enhancer.step_names == [
        "structured_pipeline_phase1",
        "structured_pipeline_phase2_s1",
        "structured_pipeline_phase2_s2",
        "structured_pipeline_phase3",
    ]


def test_markdown_enhancer_skill_pipeline_ignores_section_inflight_cap(monkeypatch, tmp_path):
    monkeypatch.setenv("MODULE2_MARKDOWN_ENHANCER_SKILL_PIPELINE", "1")
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)

    enhancer = MarkdownEnhancer()
    enhancer._enabled = True
    enhancer._section_max_inflight = 1

    class _ConcurrentPipeline:
        def __init__(self):
            self.started = 0
            self.max_started = 0
            self._gate = asyncio.Event()

        async def process_unit(self, section, prev_title="", next_title=""):
            self.started += 1
            self.max_started = max(self.max_started, self.started)
            if self.started >= 2:
                self._gate.set()
            await self._gate.wait()
            self.started -= 1
            return Phase2bUnitPipelineResult(markdown=f"- body {section.unit_id}")

    pipeline = _ConcurrentPipeline()
    enhancer._structured_unit_pipeline = pipeline

    async def _fake_hierarchy(sections, subject):
        _ = (sections, subject)
        return {
            "SU001": {"level": 2, "parent_id": None},
            "SU002": {"level": 2, "parent_id": None},
        }

    monkeypatch.setattr(enhancer, "_classify_hierarchy", _fake_hierarchy)

    payload = {
        "title": "Demo",
        "knowledge_groups": [
            {
                "group_id": 1,
                "group_name": "G1",
                "units": [
                    {"unit_id": "SU001", "title": "U1", "knowledge_type": "abstract", "body_text": "A"},
                    {"unit_id": "SU002", "title": "U2", "knowledge_type": "abstract", "body_text": "B"},
                ],
            }
        ],
    }
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    markdown = asyncio.run(asyncio.wait_for(enhancer.enhance(str(result_path), markdown_dir=str(tmp_path)), timeout=2.0))

    assert pipeline.max_started >= 2
    assert "body SU001" in markdown
    assert "body SU002" in markdown
