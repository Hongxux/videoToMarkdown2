from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys


if TYPE_CHECKING:
    from services.python_grpc.src.content_pipeline.markdown_enhancer import EnhancedSection, MarkdownEnhancer


JSON_CODE_BLOCK_PATTERN = re.compile(r"```json\s*(\{.*?\})\s*```", flags=re.DOTALL)
SECTION_HEADER_PATTERN = re.compile(r"^##\s+(s\d+)\s*:\s*(.+?)\s*$", flags=re.MULTILINE)


DEFAULT_PHASE1_SYSTEM_PROMPT = """你是语义单元结构化工程师。
请把单个语义单元拆成多个 section，并为后续 skill 精修输出结构化初稿。
仅基于原文重构，不补充外部事实。
logic_tags 允许：parallel / hierarchical / causal / progressive / contrast / conditional。
scene_tags 允许：technical / procedure / reading / narrative。
"""

DEFAULT_PHASE1_USER_PROMPT = """## 语义单元类型
{unit_type}

## 相邻上下文
{adjacent_context}

## 原始文本
{body_text}

## 图片候选
{image_candidates}

请先输出 json 代码块，再输出 `---`，最后按 `## sN: 标题` 输出 section 初稿。"""

DEFAULT_PHASE3_SYSTEM_PROMPT = """你是语义单元最终审核员。
请检查单元 Markdown 的结构、事实、错别字与图片占位。
第一行必须输出 `> **{lead_label}**：...`。
若图片候选为空，严禁输出任何 imgneeded 占位符。
若图片候选存在，只能使用 `【imgneeded_{{img_id}}】`，且同一个 img_id 只能出现一次。
"""

DEFAULT_PYTHON_IMAGE_EMBED_SKILL = """# Skill: image_embed
1. 图片占位符只能使用 `【imgneeded_{{img_id}}】`。
2. 同一个 `img_id` 全文只能出现一次。
3. 占位符必须放在最匹配句子的句末。
4. 没有图片候选时，不要输出任何 `imgneeded`。
"""

DEFAULT_PYTHON_CROSS_UNIT_BRIDGE_SKILL = """# Skill: cross_unit_bridge
1. 如果前后单元存在连续上下文，可用极短的桥接句承接。
2. 过渡句只做衔接，不新增事实。
3. 如果上下文不相关，不要强行桥接。
"""

DEFAULT_PYTHON_PROVING_UNIT_SKILL = """# Skill: proving_unit
1. 论证型单元要写成摘要式结构，而不是口号式结论。
2. 优先展开：论证目标、论证方式、关键证据链。
3. 结论必须来自原文证据。
"""

DEFAULT_REFINE_SYSTEM_PROMPT = """你是知识结构精修师。
请根据下方 section 初稿与 skill 规则进行精修。
保持原始层级，不得捏造事实，不得输出解释。

## 待精修的初稿片段
{section_markdown}

## 附带的 Skill 规则
{skill_rules}

直接输出精修后的 Markdown 正文。"""

DEFAULT_REFINE_USER_PROMPT = "请直接输出精修后的 Markdown 正文，不要重复 section 标题。"


@dataclass
class PipelineSection:
    section_id: str
    title: str
    logic_tags: List[str]
    scene_tags: List[str]
    header: str
    body: str
    full_markdown: str


@dataclass
class Phase2bUnitPipelineResult:
    markdown: str
    phase1_output: str = ""
    fallback_used: bool = False
    fallback_reason: str = ""
    debug_sections: List[Dict[str, Any]] = None


class Phase2bStructuredUnitPipeline:
    def __init__(self, enhancer: "MarkdownEnhancer"):
        self._enhancer = enhancer
        self._shared_prompt_cache: Dict[str, str] = {}
        self._shared_skill_root = self._resolve_shared_skill_root()

    async def process_unit(
        self,
        section: "EnhancedSection",
        *,
        prev_title: str = "",
        next_title: str = "",
    ) -> Phase2bUnitPipelineResult:
        try:
            base_text = await self._build_phase1_input_text(section)
            image_context = self._render_image_context(section)
            adjacent_context = self._render_adjacent_context(prev_title, next_title)
            normalized_kt = self._enhancer._normalize_knowledge_type(section.knowledge_type)

            phase1_system = self._load_python_prompt(
                PromptKeys.DEEPSEEK_MD_SKILL_PIPELINE_PHASE1_SYSTEM,
                DEFAULT_PHASE1_SYSTEM_PROMPT,
            )
            phase1_user_template = self._load_python_prompt(
                PromptKeys.DEEPSEEK_MD_SKILL_PIPELINE_PHASE1_USER,
                DEFAULT_PHASE1_USER_PROMPT,
            )
            phase1_user = phase1_user_template.format(
                unit_type=normalized_kt,
                adjacent_context=adjacent_context,
                body_text=base_text,
                image_candidates=image_context,
            )
            phase1_output = await self._call_text(
                section=section,
                step_name="structured_pipeline_phase1",
                system_prompt=phase1_system,
                user_prompt=phase1_user,
            )
            parsed_sections = self._parse_phase1_output(phase1_output, normalized_kt, section)
            if not parsed_sections:
                raise ValueError("phase1 parsed sections empty")

            refined_sections = await asyncio.gather(
                *[
                    self._refine_one_section(
                        source_section=section,
                        normalized_unit_type=normalized_kt,
                        phase1_section=phase1_section,
                        prev_title=prev_title,
                        next_title=next_title,
                    )
                    for phase1_section in parsed_sections
                ]
            )
            assembled_markdown = self._assemble_refined_sections(refined_sections)

            lead_label = "摘要" if normalized_kt == "proving" else "核心论点"
            lead_instruction = (
                "请把第一行写成论证摘要，概括论证目标、方式和关键证据链。"
                if normalized_kt == "proving"
                else "请把第一行写成单元的核心论点概括。"
            )
            phase3_system_template = self._load_python_prompt(
                PromptKeys.DEEPSEEK_MD_SKILL_PIPELINE_PHASE3_SYSTEM,
                DEFAULT_PHASE3_SYSTEM_PROMPT,
            )
            phase3_system = phase3_system_template.format(
                lead_label=lead_label,
                lead_instruction=lead_instruction,
                unit_type=normalized_kt,
                image_candidates=image_context,
                markdown_text=assembled_markdown,
            )
            phase3_user = (
                f"## 单元类型\n{normalized_kt}\n\n"
                f"## 额外要求\n{lead_instruction}\n\n"
                f"## 图片候选\n{image_context}\n\n"
                f"## 待检查 Markdown 全文\n{assembled_markdown}"
            )
            phase3_output = await self._call_text(
                section=section,
                step_name="structured_pipeline_phase3",
                system_prompt=phase3_system,
                user_prompt=phase3_user,
            )
            final_markdown = self._finalize_markdown(
                section=section,
                normalized_unit_type=normalized_kt,
                base_text=base_text,
                structured_text=phase3_output or assembled_markdown,
            )
            return Phase2bUnitPipelineResult(
                markdown=final_markdown,
                phase1_output=phase1_output,
                fallback_used=False,
                fallback_reason="",
                debug_sections=refined_sections,
            )
        except Exception as exc:
            fallback_markdown = await self._fallback_existing_pipeline(
                section=section,
                prev_title=prev_title,
                next_title=next_title,
            )
            return Phase2bUnitPipelineResult(
                markdown=fallback_markdown,
                phase1_output="",
                fallback_used=True,
                fallback_reason=str(exc),
                debug_sections=[],
            )

    async def _refine_one_section(
        self,
        *,
        source_section: "EnhancedSection",
        normalized_unit_type: str,
        phase1_section: PipelineSection,
        prev_title: str,
        next_title: str,
    ) -> Dict[str, Any]:
        skill_texts, skill_ids = self._resolve_skill_rules(
            source_section=source_section,
            normalized_unit_type=normalized_unit_type,
            phase1_section=phase1_section,
            prev_title=prev_title,
            next_title=next_title,
        )
        refine_system_template = self._load_shared_refine_system_prompt()
        refine_system = refine_system_template.format(
            section_markdown=phase1_section.full_markdown,
            skill_rules="\n\n---\n\n".join(skill_texts) if skill_texts else "（无额外 skill）",
        )
        refined_body = await self._call_text(
            section=source_section,
            step_name=f"structured_pipeline_phase2_{phase1_section.section_id}",
            system_prompt=refine_system,
            user_prompt=DEFAULT_REFINE_USER_PROMPT,
        )
        refined_body = self._strip_section_header(refined_body)
        return {
            "id": phase1_section.section_id,
            "title": phase1_section.title,
            "logic_tags": list(phase1_section.logic_tags),
            "scene_tags": list(phase1_section.scene_tags),
            "skill_ids": skill_ids,
            "markdown": refined_body or phase1_section.body,
        }

    async def _call_text(
        self,
        *,
        section: "EnhancedSection",
        step_name: str,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        start_ts = time.perf_counter()
        try:
            content, meta, _ = await self._enhancer._execute_recoverable_llm_call(
                step_name=step_name,
                unit_id=str(section.unit_id),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model_name=self._enhancer._structured_text_model,
                call_factory=lambda: self._enhancer._complete_text_with_model_fallback(
                    prompt=user_prompt,
                    system_message=system_prompt,
                    model=self._enhancer._structured_text_model,
                ),
                fallback_context=section.fallback_context,
            )
            duration_ms = (time.perf_counter() - start_ts) * 1000.0
            await self._enhancer._write_llm_trace_record(
                step_name=step_name,
                unit_id=str(section.unit_id),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_text=content,
                duration_ms=duration_ms,
                success=True,
                metadata=meta,
                fallback_context=section.fallback_context,
            )
            return str(content or "").strip()
        except Exception as exc:
            duration_ms = (time.perf_counter() - start_ts) * 1000.0
            await self._enhancer._write_llm_trace_record(
                step_name=step_name,
                unit_id=str(section.unit_id),
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_text="",
                duration_ms=duration_ms,
                success=False,
                error_msg=str(exc),
                fallback_context=section.fallback_context,
            )
            raise

    async def _build_phase1_input_text(self, section: "EnhancedSection") -> str:
        normalized_kt = self._enhancer._normalize_knowledge_type(section.knowledge_type)
        base_text = self._enhancer._resolve_concrete_base_text(section)
        if normalized_kt == "process":
            base_text = self._enhancer._resolve_process_base_text(section)
        if normalized_kt not in {"concrete", "process"}:
            augment_items = self._enhancer._build_augment_image_items(section)
            base_text = await self._enhancer._augment_body_with_image_descriptions(section, base_text, augment_items)
        return str(base_text or section.original_body or "").strip()

    def _parse_phase1_output(
        self,
        raw_output: str,
        normalized_unit_type: str,
        source_section: "EnhancedSection",
    ) -> List[PipelineSection]:
        text = self._trim_blank_lines(raw_output)
        matched = JSON_CODE_BLOCK_PATTERN.search(text)
        if not matched:
            raise ValueError("phase1 metadata json missing")
        metadata = json.loads(matched.group(1))
        markdown_part = text[matched.end():].strip()
        if markdown_part.startswith("---"):
            markdown_part = markdown_part[3:].strip()
        header_matches = list(SECTION_HEADER_PATTERN.finditer(markdown_part))
        blocks: Dict[str, Dict[str, str]] = {}
        for index, header in enumerate(header_matches):
            block_end = header_matches[index + 1].start() if index + 1 < len(header_matches) else len(markdown_part)
            full_markdown = self._trim_blank_lines(markdown_part[header.start():block_end])
            body = self._trim_blank_lines(full_markdown.split("\n", 1)[1] if "\n" in full_markdown else "")
            blocks[header.group(1)] = {
                "title": header.group(2).strip(),
                "header": header.group(0).strip(),
                "body": body,
                "full_markdown": full_markdown,
            }
        output_sections: List[PipelineSection] = []
        for item in metadata.get("sections", []):
            section_id = str(item.get("id", "") or "").strip()
            if not section_id or section_id not in blocks:
                continue
            block = blocks[section_id]
            logic_tags = self._dedupe_tags(item.get("logic_tags", []))
            scene_tags = self._merge_scene_tags(
                normalized_unit_type=normalized_unit_type,
                source_section=source_section,
                scene_tags=item.get("scene_tags", []),
            )
            output_sections.append(
                PipelineSection(
                    section_id=section_id,
                    title=str(item.get("title", "") or block["title"]).strip() or block["title"],
                    logic_tags=logic_tags,
                    scene_tags=scene_tags,
                    header=block["header"],
                    body=block["body"],
                    full_markdown=block["full_markdown"],
                )
            )
        if not output_sections:
            raise ValueError("phase1 parsed no usable sections")
        return output_sections

    def _resolve_skill_rules(
        self,
        *,
        source_section: "EnhancedSection",
        normalized_unit_type: str,
        phase1_section: PipelineSection,
        prev_title: str,
        next_title: str,
    ) -> tuple[List[str], List[str]]:
        skill_texts: List[str] = []
        skill_ids: List[str] = []

        for logic_tag in phase1_section.logic_tags:
            skill_id = f"logic_{logic_tag}"
            skill_text = self._load_shared_skill_text(skill_id)
            if skill_text:
                skill_ids.append(skill_id)
                skill_texts.append(skill_text)

        for scene_tag in phase1_section.scene_tags:
            skill_id = f"scene_{scene_tag}"
            skill_text = self._load_shared_skill_text(skill_id)
            if skill_text:
                skill_ids.append(skill_id)
                skill_texts.append(skill_text)

        obsidian_skill = self._load_shared_skill_text("obsidian_enhancements")
        if obsidian_skill:
            skill_ids.append("obsidian_enhancements")
            skill_texts.append(obsidian_skill)

        image_embed_skill = self._load_python_prompt(
            PromptKeys.DEEPSEEK_MD_SKILL_PIPELINE_IMAGE_EMBED,
            DEFAULT_PYTHON_IMAGE_EMBED_SKILL,
        )
        if image_embed_skill:
            skill_ids.append("image_embed")
            skill_texts.append(image_embed_skill)

        if normalized_unit_type == "abstract" and (prev_title or next_title):
            cross_unit_skill = self._load_python_prompt(
                PromptKeys.DEEPSEEK_MD_SKILL_PIPELINE_CROSS_UNIT_BRIDGE,
                DEFAULT_PYTHON_CROSS_UNIT_BRIDGE_SKILL,
            )
            if cross_unit_skill:
                skill_ids.append("cross_unit_bridge")
                skill_texts.append(cross_unit_skill)

        if normalized_unit_type == "proving":
            proving_skill = self._load_python_prompt(
                PromptKeys.DEEPSEEK_MD_SKILL_PIPELINE_PROVING_UNIT,
                DEFAULT_PYTHON_PROVING_UNIT_SKILL,
            )
            if proving_skill:
                skill_ids.append("proving_unit")
                skill_texts.append(proving_skill)

        return skill_texts, skill_ids

    def _merge_scene_tags(
        self,
        *,
        normalized_unit_type: str,
        source_section: "EnhancedSection",
        scene_tags: Any,
    ) -> List[str]:
        merged = self._dedupe_tags(scene_tags)
        inferred: List[str] = []
        if normalized_unit_type == "process":
            inferred.append("procedure")
        elif normalized_unit_type == "concrete":
            inferred.append("technical")
        elif normalized_unit_type == "proving":
            inferred.append("reading")
        else:
            inferred.append("reading")
        if source_section.tutorial_steps and "procedure" not in inferred:
            inferred.append("procedure")
        for tag in inferred:
            if tag not in merged:
                merged.append(tag)
        return merged

    def _assemble_refined_sections(self, sections: List[Dict[str, Any]]) -> str:
        rendered: List[str] = []
        for section in sections:
            title = str(section.get("title", "") or "").strip()
            body = self._trim_blank_lines(str(section.get("markdown", "") or ""))
            if not title and not body:
                continue
            if not body:
                rendered.append(f"- **{title}**")
                continue
            rendered.append(f"- **{title}**\n{self._indent_block(body)}")
        return "\n\n".join(rendered).strip()

    def _finalize_markdown(
        self,
        *,
        section: "EnhancedSection",
        normalized_unit_type: str,
        base_text: str,
        structured_text: str,
    ) -> str:
        image_items = self._enhancer._build_concept_image_items(section)
        clip_items = self._enhancer._build_concept_clip_items(section)
        keyframe_embeds, keyframe_embed_map = self._enhancer._build_concrete_keyframe_embeds_for_section(
            section,
            image_items,
        )
        clip_embeds = self._enhancer._build_concrete_clip_embeds_by_segment_order(section, clip_items)
        final_text = structured_text
        final_text = self._enhancer._replace_tutorial_keyframe_placeholders(
            final_text,
            keyframe_embeds,
            keyframe_embed_map=keyframe_embed_map,
        )
        final_text = self._enhancer._replace_clip_placeholders(final_text, clip_embeds)
        final_text = self._enhancer._replace_image_placeholders(final_text, image_items)
        final_text = self._enhancer._replace_tutorial_legacy_placeholders(final_text, keyframe_embeds)
        final_text = self._enhancer._strip_imgneeded_placeholders(final_text).strip()
        final_text = self._enhancer._append_missing_clip_embeds(final_text or base_text, clip_items)
        if image_items:
            final_text = self._enhancer._append_missing_image_embeds(final_text or base_text, image_items)
        if normalized_unit_type in {"concrete", "process"}:
            return self._enhancer._restore_media_preserved_base_text(
                section,
                base_text=base_text,
                structured_text=final_text or base_text,
            )
        return str(final_text or base_text).strip()

    async def _fallback_existing_pipeline(
        self,
        *,
        section: "EnhancedSection",
        prev_title: str,
        next_title: str,
    ) -> str:
        normalized_kt = self._enhancer._normalize_knowledge_type(section.knowledge_type)
        if normalized_kt == "abstract":
            return await self._enhancer._build_structured_text_for_concept(
                section,
                prev_title=prev_title,
                next_title=next_title,
            )
        if normalized_kt in {"concrete", "process"}:
            return await self._enhancer._build_structured_text_for_media_preserved_section(
                section,
                prev_title=prev_title,
                next_title=next_title,
            )
        if self._enhancer._combine_llm_calls:
            try:
                _, structured_content = await self._enhancer._enhance_and_extract(section)
                return structured_content
            except Exception:
                pass
        section.enhanced_body = await self._enhancer._enhance_text(section)
        return await self._enhancer._extract_logic(section)

    def _load_python_prompt(self, prompt_key: str, fallback: str) -> str:
        return get_prompt(prompt_key, fallback=fallback)

    def _load_shared_refine_system_prompt(self) -> str:
        cache_key = "__shared_phase2_refine_system__"
        if cache_key in self._shared_prompt_cache:
            return self._shared_prompt_cache[cache_key]
        prompt_path = self._shared_skill_root.parent / "phase2_refine_system.md"
        if prompt_path.exists():
            text = prompt_path.read_text(encoding="utf-8")
        else:
            text = DEFAULT_REFINE_SYSTEM_PROMPT
        self._shared_prompt_cache[cache_key] = text
        return text

    def _load_shared_skill_text(self, skill_id: str) -> str:
        if not skill_id:
            return ""
        cache_key = f"skill::{skill_id}"
        if cache_key in self._shared_prompt_cache:
            return self._shared_prompt_cache[cache_key]
        prompt_path = self._shared_skill_root / f"{skill_id}.md"
        text = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else ""
        self._shared_prompt_cache[cache_key] = text
        return text

    def _resolve_shared_skill_root(self) -> Path:
        current = Path(__file__).resolve()
        for candidate in current.parents:
            repo_root = candidate
            java_skill_root = repo_root / "services" / "java-orchestrator" / "src" / "main" / "resources" / "prompts" / "ai-structrued" / "skills"
            if java_skill_root.exists():
                return java_skill_root
        raise FileNotFoundError("java shared skill root not found")

    def _render_image_context(self, section: "EnhancedSection") -> str:
        image_items = self._enhancer._build_concept_image_items(section)
        if not image_items:
            return "(none)"
        return "\n".join(
            f"- img_id={item['img_id']} | img_description={item['img_description']}"
            for item in image_items
        )

    @staticmethod
    def _render_adjacent_context(prev_title: str, next_title: str) -> str:
        parts: List[str] = []
        if prev_title:
            parts.append(f"- previous: {prev_title}")
        if next_title:
            parts.append(f"- next: {next_title}")
        return "\n".join(parts) if parts else "(none)"

    @staticmethod
    def _strip_section_header(text: str) -> str:
        normalized = Phase2bStructuredUnitPipeline._trim_blank_lines(text)
        if normalized.startswith("## "):
            return Phase2bStructuredUnitPipeline._trim_blank_lines(
                normalized.split("\n", 1)[1] if "\n" in normalized else ""
            )
        return normalized

    @staticmethod
    def _trim_blank_lines(text: str) -> str:
        normalized = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
        while normalized.startswith("\n"):
            normalized = normalized[1:]
        while normalized.endswith("\n"):
            normalized = normalized[:-1]
        return normalized

    @staticmethod
    def _dedupe_tags(tags: Any) -> List[str]:
        output: List[str] = []
        for tag in tags if isinstance(tags, list) else []:
            normalized = str(tag or "").strip().lower().replace("-", "_")
            if not normalized or normalized in output:
                continue
            output.append(normalized)
        return output

    @staticmethod
    def _indent_block(text: str, indent: str = "    ") -> str:
        lines = str(text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
        return "\n".join(f"{indent}{line}" if line else "" for line in lines)
