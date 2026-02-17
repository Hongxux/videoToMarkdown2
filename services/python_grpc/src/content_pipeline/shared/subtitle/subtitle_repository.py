"""
统一字幕仓储：集中管理 Step2/Step6 字幕解析、时间轴映射与区间检索。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.python_grpc.src.common.utils.numbers import safe_float
from services.python_grpc.src.content_pipeline.shared.subtitle.data_loader import load_corrected_subtitles, load_merged_segments

logger = logging.getLogger(__name__)


class SubtitleRepository:
    """统一封装 Step2/Step6 字幕相关读取、定位与映射能力。"""

    DEFAULT_STEP2_CANDIDATES = [
        "step2_correction_output.json",
        "step2_output.json",
    ]
    DEFAULT_STEP6_CANDIDATES = [
        "step6_merge_cross_output.json",
        "step6_output.json",
    ]
    DEFAULT_SENTENCE_TS_CANDIDATES = ["sentence_timestamps.json"]

    def __init__(
        self,
        *,
        step2_path: str = "",
        step6_path: str = "",
        sentence_timestamps_path: str = "",
        output_dir: str = "",
    ) -> None:
        self.output_dir = str(output_dir or "").strip()
        self.step2_path = str(step2_path or "").strip()
        self.step6_path = str(step6_path or "").strip()
        self.sentence_timestamps_path = str(sentence_timestamps_path or "").strip()

        self._subtitles: Optional[List[Any]] = None
        self._normalized_subtitles: Optional[List[Dict[str, Any]]] = None
        self._paragraphs: Optional[List[Dict[str, Any]]] = None
        self._sentence_timestamps: Optional[Dict[str, Dict[str, float]]] = None

    @classmethod
    def from_output_dir(
        cls,
        *,
        output_dir: str,
        step2_path: str = "",
        step6_path: str = "",
        sentence_timestamps_path: str = "",
    ) -> "SubtitleRepository":
        """方法说明：SubtitleRepository.from_output_dir 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        resolved_step2 = cls.resolve_intermediate_path(
            provided_path=step2_path,
            output_dir=output_dir,
            candidate_names=cls.DEFAULT_STEP2_CANDIDATES,
        )
        resolved_step6 = cls.resolve_intermediate_path(
            provided_path=step6_path,
            output_dir=output_dir,
            candidate_names=cls.DEFAULT_STEP6_CANDIDATES,
        )
        resolved_sentence_ts = cls.resolve_intermediate_path(
            provided_path=sentence_timestamps_path,
            output_dir=output_dir,
            candidate_names=cls.DEFAULT_SENTENCE_TS_CANDIDATES,
        )
        return cls(
            step2_path=resolved_step2,
            step6_path=resolved_step6,
            sentence_timestamps_path=resolved_sentence_ts,
            output_dir=output_dir,
        )

    @staticmethod
    def resolve_intermediate_path(
        *,
        provided_path: Optional[str],
        output_dir: str,
        candidate_names: List[str],
    ) -> str:
        """方法说明：SubtitleRepository.resolve_intermediate_path 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        raw = str(provided_path or "").strip()
        if raw:
            if Path(raw).exists():
                return raw
            logger.warning(f"Provided intermediate path not found: {raw}")

        base_dir = Path(str(output_dir or "").strip())
        if not str(base_dir):
            return ""

        candidates: List[Path] = []
        intermediates_dir = base_dir / "intermediates"
        for name in candidate_names:
            candidate_name = str(name or "").strip()
            if not candidate_name:
                continue
            candidates.append(intermediates_dir / candidate_name)
            candidates.append(base_dir / candidate_name)

        for candidate in candidates:
            if candidate.exists():
                logger.info(f"Auto-discovered intermediate file: {candidate}")
                return str(candidate)
        return ""

    def set_paths(
        self,
        *,
        step2_path: Optional[str] = None,
        step6_path: Optional[str] = None,
        sentence_timestamps_path: Optional[str] = None,
        output_dir: Optional[str] = None,
        clear_cache: bool = True,
    ) -> None:
        """方法说明：SubtitleRepository.set_paths 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if output_dir is not None:
            self.output_dir = str(output_dir or "").strip()
        if step2_path is not None:
            self.step2_path = str(step2_path or "").strip()
        if step6_path is not None:
            self.step6_path = str(step6_path or "").strip()
        if sentence_timestamps_path is not None:
            self.sentence_timestamps_path = str(sentence_timestamps_path or "").strip()
        if clear_cache:
            self.clear_cache()

    def clear_cache(self) -> None:
        """方法说明：SubtitleRepository.clear_cache 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        self._subtitles = None
        self._normalized_subtitles = None
        self._paragraphs = None
        self._sentence_timestamps = None

    def set_raw_subtitles(
        self,
        subtitles: Optional[List[Any]],
        *,
        clear_sentence_timestamps: bool = True,
    ) -> None:
        """直接注入内存字幕列表，供非文件场景复用统一检索逻辑。"""
        self._subtitles = list(subtitles or [])
        self._normalized_subtitles = None
        if clear_sentence_timestamps:
            self._sentence_timestamps = None

    def set_raw_paragraphs(self, paragraphs: Optional[List[Dict[str, Any]]]) -> None:
        """直接注入内存段落列表，供非文件场景复用统一段落检索逻辑。"""
        normalized: List[Dict[str, Any]] = []
        for item in list(paragraphs or []):
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "paragraph_id": str(item.get("paragraph_id", "") or ""),
                    "text": str(item.get("text", "") or ""),
                    "source_sentence_ids": list(item.get("source_sentence_ids", []) or []),
                    "merge_type": str(item.get("merge_type", "") or ""),
                }
            )
        self._paragraphs = normalized

    def load_step2_subtitles(self, *, strict: bool = False) -> List[Any]:
        """方法说明：SubtitleRepository.load_step2_subtitles 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if self._subtitles is not None:
            return self._subtitles

        if not self.step2_path:
            self._subtitles = []
            return self._subtitles

        try:
            self._subtitles = load_corrected_subtitles(self.step2_path)
        except Exception as error:
            if strict:
                raise
            logger.warning(f"Load step2 subtitles failed: {self.step2_path}, error={error}")
            self._subtitles = []
        return self._subtitles

    def list_subtitles(self, *, strict: bool = False) -> List[Dict[str, Any]]:
        """方法说明：SubtitleRepository.list_subtitles 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if self._normalized_subtitles is not None:
            return self._normalized_subtitles

        normalized: List[Dict[str, Any]] = []
        raw_subtitles = self.load_step2_subtitles(strict=strict)
        for index, subtitle in enumerate(raw_subtitles, start=1):
            if isinstance(subtitle, dict):
                start_sec = safe_float(subtitle.get("start_sec", 0.0), 0.0)
                end_sec = safe_float(subtitle.get("end_sec", start_sec), start_sec)
                subtitle_id = str(subtitle.get("subtitle_id", "") or f"S{index:03d}")
                text = str(subtitle.get("corrected_text", "") or subtitle.get("text", "") or "")
            else:
                start_sec = safe_float(getattr(subtitle, "start_sec", 0.0), 0.0)
                end_sec = safe_float(getattr(subtitle, "end_sec", start_sec), start_sec)
                subtitle_id = str(getattr(subtitle, "subtitle_id", "") or f"S{index:03d}")
                text = str(
                    getattr(subtitle, "corrected_text", "")
                    or getattr(subtitle, "text", "")
                    or ""
                )

            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec

            normalized.append(
                {
                    "subtitle_id": subtitle_id,
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                    "text": text.strip(),
                }
            )

        normalized.sort(key=lambda item: (item["start_sec"], item["end_sec"]))
        self._normalized_subtitles = normalized
        return self._normalized_subtitles

    def load_step6_paragraphs(self, *, strict: bool = False) -> List[Dict[str, Any]]:
        """方法说明：SubtitleRepository.load_step6_paragraphs 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if self._paragraphs is not None:
            return self._paragraphs

        if not self.step6_path:
            self._paragraphs = []
            return self._paragraphs

        try:
            merged_segments = load_merged_segments(self.step6_path)
            self._paragraphs = [
                {
                    "paragraph_id": seg.segment_id,
                    "text": seg.full_text,
                    "source_sentence_ids": list(seg.source_sentence_ids or []),
                    "merge_type": seg.merge_type,
                }
                for seg in merged_segments
            ]
            return self._paragraphs
        except Exception as primary_error:
            try:
                self._paragraphs = self._load_step6_paragraphs_fallback()
                return self._paragraphs
            except Exception as fallback_error:
                if strict:
                    raise
                logger.warning(
                    f"Load step6 paragraphs failed: {self.step6_path}, "
                    f"primary_error={primary_error}, fallback_error={fallback_error}"
                )
                self._paragraphs = []
                return self._paragraphs

    def _load_step6_paragraphs_fallback(self) -> List[Dict[str, Any]]:
        """方法说明：SubtitleRepository._load_step6_paragraphs_fallback 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        with open(self.step6_path, "r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)

        payload = data.get("output", data) if isinstance(data, dict) else {}
        raw_paragraphs = payload.get("pure_text_script", []) if isinstance(payload, dict) else []
        if not isinstance(raw_paragraphs, list):
            return []

        paragraphs: List[Dict[str, Any]] = []
        for index, item in enumerate(raw_paragraphs, start=1):
            if not isinstance(item, dict):
                continue
            paragraph_id = str(item.get("paragraph_id", "") or f"P{index:03d}")
            text = str(item.get("text", "") or "")
            source_sentence_ids = item.get("source_sentence_ids", [])
            if not isinstance(source_sentence_ids, list):
                source_sentence_ids = []
            paragraphs.append(
                {
                    "paragraph_id": paragraph_id,
                    "text": text,
                    "source_sentence_ids": [str(sid) for sid in source_sentence_ids],
                    "merge_type": str(item.get("merge_type", "") or ""),
                }
            )
        return paragraphs

    def build_sentence_timestamps(
        self,
        *,
        prefer_external: bool = True,
        strict: bool = False,
    ) -> Dict[str, Dict[str, float]]:
        """方法说明：SubtitleRepository.build_sentence_timestamps 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if self._sentence_timestamps is not None:
            return self._sentence_timestamps

        if prefer_external and self.sentence_timestamps_path and Path(self.sentence_timestamps_path).exists():
            try:
                with open(self.sentence_timestamps_path, "r", encoding="utf-8") as file_obj:
                    data = json.load(file_obj)
                self._sentence_timestamps = self._normalize_sentence_timestamps(data)
                return self._sentence_timestamps
            except Exception as error:
                if strict:
                    raise
                logger.warning(
                    f"Failed to load external sentence_timestamps: {self.sentence_timestamps_path}, error={error}"
                )

        fallback: Dict[str, Dict[str, float]] = {}
        for index, subtitle in enumerate(self.list_subtitles(strict=strict), start=1):
            fallback[f"S{index:03d}"] = {
                "start_sec": float(subtitle.get("start_sec", 0.0)),
                "end_sec": float(subtitle.get("end_sec", 0.0)),
            }
        self._sentence_timestamps = fallback
        return self._sentence_timestamps

    def _normalize_sentence_timestamps(self, data: Any) -> Dict[str, Dict[str, float]]:
        """方法说明：SubtitleRepository._normalize_sentence_timestamps 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not isinstance(data, dict):
            return {}

        normalized: Dict[str, Dict[str, float]] = {}
        for sentence_id, meta in data.items():
            if not isinstance(meta, dict):
                continue
            start_sec = safe_float(meta.get("start_sec", 0.0), 0.0)
            end_sec = safe_float(meta.get("end_sec", start_sec), start_sec)
            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec
            normalized[str(sentence_id)] = {
                "start_sec": start_sec,
                "end_sec": end_sec,
            }
        return normalized

    def align_to_sentence_start(self, timestamp_sec: float) -> float:
        """方法说明：SubtitleRepository.align_to_sentence_start 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        target = safe_float(timestamp_sec, 0.0)
        best_start = 0.0
        for subtitle in self.list_subtitles():
            sub_start = float(subtitle.get("start_sec", 0.0))
            if sub_start <= target:
                best_start = sub_start
            else:
                break
        return best_start

    def align_to_sentence_end(self, timestamp_sec: float) -> float:
        """方法说明：SubtitleRepository.align_to_sentence_end 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        target = safe_float(timestamp_sec, 0.0)
        subtitles = self.list_subtitles()
        for subtitle in subtitles:
            sub_end = float(subtitle.get("end_sec", 0.0))
            if sub_end >= target:
                return sub_end
        return float(subtitles[-1].get("end_sec", target)) if subtitles else target

    def clamp_time_range(
        self,
        start_sec: float,
        end_sec: float,
        *,
        video_duration: float = 0.0,
    ) -> Tuple[float, float]:
        """方法说明：SubtitleRepository.clamp_time_range 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        start = max(0.0, safe_float(start_sec, 0.0))
        end = max(start, safe_float(end_sec, start))

        duration = safe_float(video_duration, 0.0)
        if duration > 0:
            max_end = float(duration)
            start = max(0.0, min(start, max_end))
            end = max(start, min(end, max_end))
        return start, end

    def extract_subtitles_in_range(
        self,
        start_sec: float,
        end_sec: float,
        *,
        expand_to_sentence_boundary: bool = False,
    ) -> List[Dict[str, Any]]:
        """方法说明：SubtitleRepository.extract_subtitles_in_range 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        start, end = self._normalize_range(start_sec, end_sec)
        if expand_to_sentence_boundary:
            start, end = self._expand_to_sentence_boundary(start, end)

        hits: List[Dict[str, Any]] = []
        for subtitle in self.list_subtitles():
            sub_start = float(subtitle.get("start_sec", 0.0))
            sub_end = float(subtitle.get("end_sec", 0.0))
            if sub_start < end and sub_end > start:
                hits.append(subtitle)
        return hits

    def get_subtitles_in_range(
        self,
        start_sec: float,
        end_sec: float,
        *,
        expand_to_sentence_boundary: bool = True,
        include_ts_prefix: bool = False,
        empty_fallback: str = "(无字幕)",
    ) -> str:
        """方法说明：SubtitleRepository.get_subtitles_in_range 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        hits = self.extract_subtitles_in_range(
            start_sec,
            end_sec,
            expand_to_sentence_boundary=expand_to_sentence_boundary,
        )
        if not hits:
            return empty_fallback

        lines: List[str] = []
        for subtitle in hits:
            text = str(subtitle.get("text", "") or "").strip()
            if not text:
                continue
            if include_ts_prefix:
                lines.append(f"[{float(subtitle.get('start_sec', 0.0)):.1f}s] {text}")
            else:
                lines.append(text)

        if not lines:
            return empty_fallback
        return "\n".join(lines) if include_ts_prefix else " ".join(lines)

    def map_timestamp_to_sentence_id(
        self,
        timestamp_sec: float,
        *,
        mode: str = "in_range_then_nearest",
    ) -> str:
        """方法说明：SubtitleRepository.map_timestamp_to_sentence_id 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        try:
            target = float(timestamp_sec)
        except Exception:
            return ""

        sentence_timestamps = self.build_sentence_timestamps()
        if not sentence_timestamps:
            return ""

        in_range_hits: List[Tuple[float, str]] = []
        nearest_hit: Optional[Tuple[float, str]] = None

        for sid, meta in sentence_timestamps.items():
            if not isinstance(meta, dict):
                continue
            start_sec = safe_float(meta.get("start_sec", 0.0), 0.0)
            end_sec = safe_float(meta.get("end_sec", start_sec), start_sec)
            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec

            if start_sec <= target <= end_sec:
                in_range_hits.append((end_sec - start_sec, str(sid)))
                continue

            center = (start_sec + end_sec) / 2.0
            distance = abs(center - target)
            if nearest_hit is None or distance < nearest_hit[0]:
                nearest_hit = (distance, str(sid))

        if mode == "nearest_only":
            return nearest_hit[1] if nearest_hit else ""

        if in_range_hits:
            in_range_hits.sort(key=lambda item: item[0])
            return in_range_hits[0][1]
        return nearest_hit[1] if nearest_hit else ""

    def get_sentence_text(self, sentence_id: str) -> str:
        """方法说明：SubtitleRepository.get_sentence_text 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        sid = str(sentence_id or "").strip()
        if not sid:
            return ""

        subtitles = self.list_subtitles()
        for idx, subtitle in enumerate(subtitles, start=1):
            if sid == f"S{idx:03d}":
                return str(subtitle.get("text", "") or "").strip()

        by_subtitle_id = {str(sub.get("subtitle_id", "") or ""): sub for sub in subtitles}
        matched = by_subtitle_id.get(sid)
        if not matched:
            return ""
        return str(matched.get("text", "") or "").strip()

    def build_relative_subtitles(
        self,
        *,
        unit_start_sec: float,
        unit_end_sec: float,
    ) -> List[Dict[str, Any]]:
        """方法说明：SubtitleRepository.build_relative_subtitles 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        unit_start = safe_float(unit_start_sec, 0.0)
        unit_end = safe_float(unit_end_sec, 0.0)
        if unit_end <= unit_start:
            return []

        unit_duration = unit_end - unit_start
        relative_subtitles: List[Dict[str, Any]] = []
        for subtitle in self.list_subtitles():
            sub_start = float(subtitle.get("start_sec", 0.0))
            sub_end = float(subtitle.get("end_sec", 0.0))
            if sub_end <= unit_start or sub_start >= unit_end:
                continue

            rel_start = max(0.0, sub_start - unit_start)
            rel_end = min(unit_duration, sub_end - unit_start)
            if rel_end <= rel_start:
                continue

            relative_subtitles.append(
                {
                    "start_sec": rel_start,
                    "end_sec": rel_end,
                    "text": str(subtitle.get("text", "") or ""),
                    "subtitle_id": str(subtitle.get("subtitle_id", "") or ""),
                }
            )

        return relative_subtitles

    def _normalize_range(self, start_sec: float, end_sec: float) -> Tuple[float, float]:
        """方法说明：SubtitleRepository._normalize_range 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        start = safe_float(start_sec, 0.0)
        end = safe_float(end_sec, start)
        if end < start:
            start, end = end, start
        return start, end

    def _expand_to_sentence_boundary(self, start_sec: float, end_sec: float) -> Tuple[float, float]:
        """方法说明：SubtitleRepository._expand_to_sentence_boundary 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        effective_start = start_sec
        effective_end = end_sec
        for subtitle in self.list_subtitles():
            sub_start = float(subtitle.get("start_sec", 0.0))
            sub_end = float(subtitle.get("end_sec", 0.0))
            if sub_start <= start_sec < sub_end:
                effective_start = min(effective_start, sub_start)
            if sub_start < end_sec <= sub_end:
                effective_end = max(effective_end, sub_end)
        return effective_start, effective_end

    
