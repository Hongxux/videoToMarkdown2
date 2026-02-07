"""
模块说明：指代断层预补全器，用于 Phase2B 截图校验前的句级消歧。
执行逻辑：
1) 调用 DeepSeek 识别并补全指代断层，输出 replaced_text + confidence。
2) 对低置信度断层（默认 <0.8）触发视觉补全：优先复用现有截图，否则按句子时间范围抽图。
3) 返回替换后的文本与可复用的 concrete 校验结果，供后续截图校验阶段复用。
实现方式：LLMClient + ConcreteKnowledgeValidator + ScreenshotSelector（可选）。
核心价值：在不重复调用截图校验的前提下，先修复文本指代歧义。
"""

from __future__ import annotations

import os
import re
import cv2
import json
import tempfile
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


logger = logging.getLogger(__name__)


COREFERENCE_SYSTEM_PROMPT = """你是教学视频文本编辑专家。
请识别文本中指代不清的句子（如“这个/它/那一步/这里/该方法”），并基于上下文补全为可直接替换原句的完整句子。

要求：
1. 只改写存在指代断层的句子。
2. 不得添加原文没有的新事实。
3. 输出严格 JSON。
"""


COREFERENCE_USER_PROMPT = """## 语义单元文本
{full_text}

## 句子列表（含时间范围）
{sentence_json}

## 输出格式（严格 JSON）
{{
  "gaps": [
    {{
      "gap_id": "G1",
      "sentence_text": "原句",
      "replaced_text": "补全后句子",
      "confidence": 0.0,
      "reason": "补全依据"
    }}
  ]
}}

如果没有可补全的指代断层，返回 {{"gaps": []}}。
"""


@dataclass
class CorefGapCandidate:
    gap_id: str
    sentence_text: str
    deepseek_replaced_text: str
    deepseek_confidence: float
    final_replaced_text: str
    final_confidence: float
    source: str
    reason: str = ""


@dataclass
class CorefResolutionResult:
    updated_text: str
    gaps: List[CorefGapCandidate] = field(default_factory=list)
    prevalidated_results: Dict[str, Any] = field(default_factory=dict)


class CoreferenceResolver:
    """Phase2B 指代断层预补全执行器。"""

    def __init__(
        self,
        llm_client: Optional[Any],
        concrete_validator: Optional[Any],
        screenshot_selector: Optional[Any] = None,
        confidence_threshold: float = 0.8,
    ):
        self._llm_client = llm_client
        self._concrete_validator = concrete_validator
        self._screenshot_selector = screenshot_selector
        self._confidence_threshold = float(confidence_threshold)

    async def resolve_unit_coreference(
        self,
        unit: Any,
        material_requests: Any,
        screenshots_dir: str,
        sentence_timestamps: Dict[str, Dict[str, float]],
        subtitles: List[Any],
        video_path: str,
    ) -> CorefResolutionResult:
        """
        对单个语义单元执行“先文本、后视觉”的指代断层补全。
        """
        original_text = str(getattr(unit, "full_text", "") or "").strip()
        if not original_text:
            return CorefResolutionResult(updated_text=original_text)

        sentence_entries = self._build_sentence_entries(
            unit=unit,
            sentence_timestamps=sentence_timestamps,
            subtitles=subtitles,
        )
        if not sentence_entries:
            return CorefResolutionResult(updated_text=original_text)

        deepseek_gaps = await self._detect_and_fill_with_deepseek(
            full_text=original_text,
            sentence_entries=sentence_entries,
        )
        if not deepseek_gaps:
            return CorefResolutionResult(updated_text=original_text)

        updated_text = original_text
        resolved_gaps: List[CorefGapCandidate] = []
        prevalidated_results: Dict[str, Any] = {}

        for idx, gap in enumerate(deepseek_gaps, start=1):
            sentence_text = str(gap.get("sentence_text") or "").strip()
            if not sentence_text:
                continue
            deepseek_replaced = str(gap.get("replaced_text") or sentence_text).strip()
            deepseek_conf = self._safe_float(gap.get("confidence", 0.0), 0.0)
            reason = str(gap.get("reason") or "").strip()

            sentence_meta = self._match_sentence_entry(sentence_entries, sentence_text)
            if not sentence_meta:
                sentence_meta = {
                    "sentence_text": sentence_text,
                    "start_sec": float(getattr(unit, "start_sec", 0.0) or 0.0),
                    "end_sec": float(getattr(unit, "end_sec", 0.0) or 0.0),
                    "context_before": "",
                    "context_after": "",
                }

            final_text = deepseek_replaced or sentence_text
            final_conf = deepseek_conf
            source = "deepseek"

            if deepseek_conf < self._confidence_threshold:
                vision_result, vision_cache = self._vision_refine_low_confidence_gap(
                    unit=unit,
                    material_requests=material_requests,
                    screenshots_dir=screenshots_dir,
                    sentence_meta=sentence_meta,
                    sentence_text=sentence_text,
                    video_path=video_path,
                )
                if vision_cache:
                    prevalidated_results.update(vision_cache)
                if vision_result:
                    vision_text = str(vision_result.get("replaced_text") or "").strip()
                    if vision_text:
                        final_text = vision_text
                    final_conf = self._safe_float(vision_result.get("confidence", final_conf), final_conf)
                    source = str(vision_result.get("source") or "vision")
                    vision_reason = str(vision_result.get("reason") or "").strip()
                    if vision_reason:
                        reason = vision_reason

            updated_text = self._replace_sentence_once(updated_text, sentence_text, final_text)
            resolved_gaps.append(
                CorefGapCandidate(
                    gap_id=str(gap.get("gap_id") or f"G{idx}"),
                    sentence_text=sentence_text,
                    deepseek_replaced_text=deepseek_replaced,
                    deepseek_confidence=deepseek_conf,
                    final_replaced_text=final_text,
                    final_confidence=final_conf,
                    source=source,
                    reason=reason,
                )
            )

        return CorefResolutionResult(
            updated_text=updated_text,
            gaps=resolved_gaps,
            prevalidated_results=prevalidated_results,
        )

    async def _detect_and_fill_with_deepseek(
        self,
        full_text: str,
        sentence_entries: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """调用 DeepSeek 识别并补全指代断层。"""
        if not self._llm_client:
            return []

        compact_entries = [
            {
                "sentence_id": item.get("sentence_id", ""),
                "sentence_text": item.get("sentence_text", ""),
                "start_sec": self._safe_float(item.get("start_sec", 0.0), 0.0),
                "end_sec": self._safe_float(item.get("end_sec", 0.0), 0.0),
            }
            for item in sentence_entries
        ]

        user_prompt = COREFERENCE_USER_PROMPT.format(
            full_text=full_text,
            sentence_json=json.dumps(compact_entries, ensure_ascii=False, indent=2),
        )

        try:
            response = await self._llm_client.complete_json(
                prompt=user_prompt,
                system_message=COREFERENCE_SYSTEM_PROMPT,
            )
            if isinstance(response, tuple):
                result = response[0]
            else:
                result = response
            if not isinstance(result, dict):
                return []
            gaps = result.get("gaps", [])
            if not isinstance(gaps, list):
                return []
            normalized = []
            for idx, item in enumerate(gaps, start=1):
                if not isinstance(item, dict):
                    continue
                sentence_text = str(item.get("sentence_text") or "").strip()
                replaced_text = str(item.get("replaced_text") or "").strip()
                if not sentence_text or not replaced_text:
                    continue
                normalized.append(
                    {
                        "gap_id": str(item.get("gap_id") or f"G{idx}"),
                        "sentence_text": sentence_text,
                        "replaced_text": replaced_text,
                        "confidence": self._safe_float(item.get("confidence", 0.0), 0.0),
                        "reason": str(item.get("reason") or "").strip(),
                    }
                )
            return normalized
        except Exception as e:
            logger.warning(f"DeepSeek coreference resolve failed: {e}")
            return []

    def _vision_refine_low_confidence_gap(
        self,
        unit: Any,
        material_requests: Any,
        screenshots_dir: str,
        sentence_meta: Dict[str, Any],
        sentence_text: str,
        video_path: str,
    ) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
        """低置信度断层的视觉补全：优先现有截图，不足时抽图。"""
        if not self._concrete_validator:
            return None, {}

        start_sec = self._safe_float(sentence_meta.get("start_sec", 0.0), 0.0)
        end_sec = self._safe_float(sentence_meta.get("end_sec", start_sec), start_sec)
        if end_sec < start_sec:
            start_sec, end_sec = end_sec, start_sec

        context_text = self._build_context(sentence_meta)
        cache_results: Dict[str, Any] = {}

        existing_images = self._find_existing_sentence_screenshots(
            material_requests=material_requests,
            screenshots_dir=screenshots_dir,
            unit_id=str(getattr(unit, "unit_id", "") or ""),
            start_sec=start_sec,
            end_sec=end_sec,
        )

        best_result: Optional[Dict[str, Any]] = None

        for image_path in existing_images:
            try:
                result = self._concrete_validator.validate_for_coreference(
                    image_path=image_path,
                    sentence_text=sentence_text,
                    context_text=context_text,
                )
            except Exception as e:
                logger.warning(f"vision refine with existing screenshot failed: {e}")
                continue

            result_conf = self._safe_float(result.get("replace_confidence", 0.0), 0.0)
            if best_result is None or result_conf > self._safe_float(best_result.get("confidence", 0.0), 0.0):
                best_result = {
                    "replaced_text": str(result.get("replaced_text") or "").strip(),
                    "confidence": result_conf,
                    "reason": str(result.get("replace_reason") or result.get("reason") or "").strip(),
                    "source": "vision_existing",
                }

            concrete_result = result.get("concrete_result")
            if concrete_result is not None:
                cache_results[self._path_key(image_path)] = concrete_result

        if best_result:
            return best_result, cache_results

        generated_images = self._generate_screenshots_for_sentence(
            video_path=video_path,
            start_sec=start_sec,
            end_sec=end_sec,
            max_images=3,
        )

        try:
            for image_path in generated_images:
                try:
                    result = self._concrete_validator.validate_for_coreference(
                        image_path=image_path,
                        sentence_text=sentence_text,
                        context_text=context_text,
                    )
                except Exception as e:
                    logger.warning(f"vision refine with generated screenshot failed: {e}")
                    continue

                result_conf = self._safe_float(result.get("replace_confidence", 0.0), 0.0)
                if best_result is None or result_conf > self._safe_float(best_result.get("confidence", 0.0), 0.0):
                    best_result = {
                        "replaced_text": str(result.get("replaced_text") or "").strip(),
                        "confidence": result_conf,
                        "reason": str(result.get("replace_reason") or result.get("reason") or "").strip(),
                        "source": "vision_generated",
                    }
        finally:
            for image_path in generated_images:
                try:
                    os.unlink(image_path)
                except Exception:
                    pass

        return best_result, cache_results

    def _build_sentence_entries(
        self,
        unit: Any,
        sentence_timestamps: Dict[str, Dict[str, float]],
        subtitles: List[Any],
    ) -> List[Dict[str, Any]]:
        subtitle_by_sid: Dict[str, Any] = {}
        subtitle_by_id: Dict[str, Any] = {}
        for idx, sub in enumerate(subtitles or [], start=1):
            sid = f"S{idx:03d}"
            subtitle_by_sid[sid] = sub
            subtitle_id = str(getattr(sub, "subtitle_id", "") or "").strip()
            if subtitle_id:
                subtitle_by_id[subtitle_id] = sub

        sentence_ids = list(getattr(unit, "source_sentence_ids", []) or [])
        entries: List[Dict[str, Any]] = []
        for sid in sentence_ids:
            sid_str = str(sid or "").strip()
            if not sid_str:
                continue
            ts = sentence_timestamps.get(sid_str, {}) if isinstance(sentence_timestamps, dict) else {}
            sub = subtitle_by_sid.get(sid_str) or subtitle_by_id.get(sid_str)
            sentence_text = str(getattr(sub, "text", "") or "").strip()
            if not sentence_text:
                continue

            start_sec = self._safe_float(
                ts.get("start_sec", getattr(sub, "start_sec", getattr(unit, "start_sec", 0.0))),
                self._safe_float(getattr(unit, "start_sec", 0.0), 0.0),
            )
            end_sec = self._safe_float(
                ts.get("end_sec", getattr(sub, "end_sec", getattr(unit, "end_sec", start_sec))),
                start_sec,
            )
            entries.append(
                {
                    "sentence_id": sid_str,
                    "sentence_text": sentence_text,
                    "start_sec": start_sec,
                    "end_sec": max(start_sec, end_sec),
                }
            )

        if entries:
            for idx, item in enumerate(entries):
                item["context_before"] = entries[idx - 1]["sentence_text"] if idx > 0 else ""
                item["context_after"] = entries[idx + 1]["sentence_text"] if idx + 1 < len(entries) else ""
            return entries

        # 回退：基于 full_text 分句并均分时间范围
        full_text = str(getattr(unit, "full_text", "") or "").strip()
        split_sentences = [s for s in re.split(r"(?<=[。！？!?；;])\s*", full_text) if s.strip()]
        if not split_sentences:
            return []

        unit_start = self._safe_float(getattr(unit, "start_sec", 0.0), 0.0)
        unit_end = self._safe_float(getattr(unit, "end_sec", unit_start), unit_start)
        duration = max(0.1, unit_end - unit_start)
        seg = duration / max(1, len(split_sentences))

        for idx, sentence_text in enumerate(split_sentences):
            start_sec = unit_start + idx * seg
            end_sec = min(unit_end, start_sec + seg)
            entries.append(
                {
                    "sentence_id": f"F{idx + 1:03d}",
                    "sentence_text": sentence_text.strip(),
                    "start_sec": start_sec,
                    "end_sec": max(start_sec, end_sec),
                    "context_before": split_sentences[idx - 1].strip() if idx > 0 else "",
                    "context_after": split_sentences[idx + 1].strip() if idx + 1 < len(split_sentences) else "",
                }
            )
        return entries

    def _match_sentence_entry(self, entries: List[Dict[str, Any]], sentence_text: str) -> Optional[Dict[str, Any]]:
        target = self._normalize_sentence(sentence_text)
        if not target:
            return None
        for item in entries:
            if self._normalize_sentence(item.get("sentence_text", "")) == target:
                return item
        return None

    def _find_existing_sentence_screenshots(
        self,
        material_requests: Any,
        screenshots_dir: str,
        unit_id: str,
        start_sec: float,
        end_sec: float,
    ) -> List[str]:
        requests = list(getattr(material_requests, "screenshot_requests", []) or [])
        if not requests:
            return []

        paths: List[str] = []
        for req in requests:
            req_ts = self._safe_float(getattr(req, "timestamp_sec", -1.0), -1.0)
            req_unit = str(getattr(req, "semantic_unit_id", "") or "").strip()
            if req_unit and req_unit != unit_id:
                continue
            if req_ts < start_sec or req_ts > end_sec:
                continue
            req_id = str(getattr(req, "screenshot_id", "") or "").strip()
            for path_item in self._find_request_image_paths(
                screenshots_dir=screenshots_dir,
                unit_id=unit_id,
                request_id=req_id,
            ):
                if path_item not in paths:
                    paths.append(path_item)
        return paths

    def _find_request_image_paths(self, screenshots_dir: str, unit_id: str, request_id: str) -> List[str]:
        exts = [".png", ".jpg", ".jpeg"]
        req = str(request_id or "").strip().replace("\\", "/").strip("/")
        if not req:
            return []

        req_path = Path(req)
        base_name = req_path.name
        stem_name = req_path.stem if req_path.suffix else base_name

        candidates: List[Path] = []
        if req_path.suffix:
            candidates.extend(
                [
                    Path(screenshots_dir) / req,
                    Path(screenshots_dir) / unit_id / base_name,
                    Path(screenshots_dir) / base_name,
                ]
            )

        for ext in exts:
            candidates.extend(
                [
                    Path(screenshots_dir) / f"{req}{ext}",
                    Path(screenshots_dir) / unit_id / f"{base_name}{ext}",
                    Path(screenshots_dir) / unit_id / f"{stem_name}{ext}",
                    Path(screenshots_dir) / f"{base_name}{ext}",
                    Path(screenshots_dir) / f"{stem_name}{ext}",
                ]
            )

        found: List[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if candidate.exists():
                key = self._path_key(str(candidate))
                if key not in seen:
                    seen.add(key)
                    found.append(str(candidate))
        return found

    def _generate_screenshots_for_sentence(
        self,
        video_path: str,
        start_sec: float,
        end_sec: float,
        max_images: int = 3,
    ) -> List[str]:
        if not self._screenshot_selector or not video_path or not os.path.exists(video_path):
            return []

        if end_sec <= start_sec:
            end_sec = start_sec + 0.8

        try:
            selections = self._screenshot_selector.select_screenshots_for_range_sync(
                video_path=video_path,
                start_sec=start_sec,
                end_sec=end_sec,
                coarse_fps=2.0,
                fine_fps=8.0,
            )
        except Exception as e:
            logger.warning(f"select_screenshots_for_range_sync failed: {e}")
            return []

        if not selections:
            return []

        ranked = sorted(
            [item for item in selections if isinstance(item, dict)],
            key=lambda item: self._safe_float(item.get("score", 0.0), 0.0),
            reverse=True,
        )[: max(1, int(max_images))]

        image_paths: List[str] = []
        for item in ranked:
            ts = self._safe_float(item.get("timestamp_sec", start_sec), start_sec)
            frame_path = self._extract_frame_at_timestamp(video_path, ts)
            if frame_path:
                image_paths.append(frame_path)
        return image_paths

    def _extract_frame_at_timestamp(self, video_path: str, timestamp_sec: float) -> str:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return ""
        try:
            safe_ts = max(0.0, self._safe_float(timestamp_sec, 0.0))
            cap.set(cv2.CAP_PROP_POS_MSEC, safe_ts * 1000.0)
            ok, frame = cap.read()
            if not ok or frame is None:
                return ""
            fd, temp_path = tempfile.mkstemp(prefix="coref_frame_", suffix=".jpg")
            os.close(fd)
            cv2.imwrite(temp_path, frame)
            return temp_path
        finally:
            cap.release()

    def _replace_sentence_once(self, content: str, old_sentence: str, new_sentence: str) -> str:
        old = str(old_sentence or "").strip()
        new = str(new_sentence or "").strip()
        if not content or not old or not new or old == new:
            return content
        if old in content:
            return content.replace(old, new, 1)
        return content

    @staticmethod
    def _normalize_sentence(text: str) -> str:
        return re.sub(r"\s+", "", str(text or "")).strip()

    @staticmethod
    def _safe_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    @staticmethod
    def _path_key(path: str) -> str:
        try:
            return str(Path(path).resolve())
        except Exception:
            return os.path.abspath(path)

    @staticmethod
    def _build_context(sentence_meta: Dict[str, Any]) -> str:
        before = str(sentence_meta.get("context_before") or "").strip()
        after = str(sentence_meta.get("context_after") or "").strip()
        if before and after:
            return f"前文：{before}\n后文：{after}"
        if before:
            return f"前文：{before}"
        if after:
            return f"后文：{after}"
        return ""

