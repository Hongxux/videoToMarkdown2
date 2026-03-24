"""
Stage1 恢复投影器：仅在恢复/重开任务时，从任务目录 runtime_state.db 的 committed llm_call
重建 Stage1 关键产物；正常热路径仍由内存态对象直传。
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from services.python_grpc.src.common.utils.runtime_recovery_store import RuntimeRecoveryStore
from services.python_grpc.src.transcript_pipeline.nodes.phase2_preprocessing import (
    _contains_cjk,
    _drops_cjk_en_glossary_pair,
)
from services.python_grpc.src.transcript_pipeline.nodes.step_contracts import (
    assemble_step3_merged_sentences,
    assemble_step4_cleaned_sentences,
    build_fallback_paragraphs,
    build_step3_window_candidates,
    deduplicate_paragraphs,
    merge_step4_cleaned_maps,
    parse_step1_topic_payload,
    parse_step2_llm_payload,
    parse_step3_merged_sentences,
    parse_step35_translated_sentences,
    parse_step4_cleaned_sentences,
    parse_step56_dedup_merge_payload,
    reconcile_step2_item,
    reconcile_step4_item,
    sentence_id_and_text_pairs,
)
from services.python_grpc.src.transcript_pipeline.tools.file_validator import read_subtitle_sample

logger = logging.getLogger(__name__)

_NUMERIC_SUFFIX_PATTERN = re.compile(r"(\d+)(?!.*\d)")


def _safe_json_loads(payload: str) -> Optional[Dict[str, Any]]:
    try:
        decoded = json.loads(str(payload or "{}"))
    except Exception:
        return None
    return decoded if isinstance(decoded, dict) else None


def _resolve_row_scope_ids(row: Optional[Dict[str, Any]]) -> List[str]:
    normalized: List[str] = []
    seen: set[str] = set()
    raw_scope_ids = (row or {}).get("request_scope_ids", [])
    if isinstance(raw_scope_ids, list):
        for item in raw_scope_ids:
            scope_id = str(item or "").strip()
            if not scope_id or scope_id in seen:
                continue
            seen.add(scope_id)
            normalized.append(scope_id)
    return normalized


def _unit_sort_key(unit_id: str, llm_call_id: str) -> Tuple[int, str, str]:
    normalized_unit = str(unit_id or "").strip()
    match = _NUMERIC_SUFFIX_PATTERN.search(normalized_unit)
    if match is not None:
        return (int(match.group(1)), normalized_unit, str(llm_call_id or "").strip())
    return (10**9, normalized_unit, str(llm_call_id or "").strip())


def _normalize_subtitles(raw_subtitles: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(list(raw_subtitles or []), start=1):
        if not isinstance(item, dict):
            continue
        subtitle_id = str(item.get("subtitle_id", "") or f"SUB{index:03d}").strip() or f"SUB{index:03d}"
        if subtitle_id in seen_ids:
            continue
        seen_ids.add(subtitle_id)
        try:
            start_sec = float(item.get("start_sec", 0.0) or 0.0)
        except Exception:
            start_sec = 0.0
        try:
            end_sec = float(item.get("end_sec", start_sec) or start_sec)
        except Exception:
            end_sec = start_sec
        normalized.append(
            {
                "subtitle_id": subtitle_id,
                "text": str(item.get("text", item.get("corrected_text", "")) or ""),
                "start_sec": start_sec,
                "end_sec": max(start_sec, end_sec),
            }
        )
    normalized.sort(key=lambda item: (float(item.get("start_sec", 0.0)), float(item.get("end_sec", 0.0))))
    return normalized


def _build_sentence_timestamps(sentences: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    timestamps: Dict[str, Dict[str, float]] = {}
    for item in list(sentences or []):
        if not isinstance(item, dict):
            continue
        sentence_id = str(item.get("sentence_id", "") or "").strip()
        if not sentence_id:
            continue
        try:
            start_sec = float(item.get("start_sec", 0.0) or 0.0)
        except Exception:
            start_sec = 0.0
        try:
            end_sec = float(item.get("end_sec", start_sec) or start_sec)
        except Exception:
            end_sec = start_sec
        timestamps[sentence_id] = {
            "start_sec": start_sec,
            "end_sec": max(start_sec, end_sec),
        }
    return timestamps


def _finalize_step56_projection(
    *,
    cleaned_sentences: Sequence[Dict[str, Any]],
    aggregated_keep_ids: Sequence[str],
    aggregated_paragraphs: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    ordered_pairs = sentence_id_and_text_pairs(list(cleaned_sentences or []))
    ordered_ids = [sentence_id for sentence_id, _ in ordered_pairs]
    sentence_text_map = {sentence_id: text for sentence_id, text in ordered_pairs}
    keep_id_set = {str(item or "").strip() for item in list(aggregated_keep_ids or []) if str(item or "").strip()}
    if keep_id_set:
        ordered_keep_ids = [sentence_id for sentence_id in ordered_ids if sentence_id in keep_id_set]
    else:
        ordered_keep_ids = list(ordered_ids)
        keep_id_set = set(ordered_keep_ids)

    non_redundant_sentences = [
        dict(sentence)
        for sentence in list(cleaned_sentences or [])
        if str(sentence.get("sentence_id", "") or "").strip() in keep_id_set
    ]
    if not non_redundant_sentences and cleaned_sentences:
        non_redundant_sentences = [dict(sentence) for sentence in list(cleaned_sentences or []) if isinstance(sentence, dict)]
        ordered_keep_ids = [
            str(sentence.get("sentence_id", "") or "").strip()
            for sentence in non_redundant_sentences
            if str(sentence.get("sentence_id", "") or "").strip()
        ]
        keep_id_set = set(ordered_keep_ids)

    normalized_paragraphs: List[Dict[str, Any]] = []
    for paragraph in list(aggregated_paragraphs or []):
        if not isinstance(paragraph, dict):
            continue
        source_sentence_ids_raw = paragraph.get("source_sentence_ids", [])
        if not isinstance(source_sentence_ids_raw, list):
            continue
        valid_source_ids: List[str] = []
        seen_ids: set[str] = set()
        for sentence_id in source_sentence_ids_raw:
            normalized_id = str(sentence_id or "").strip()
            if not normalized_id or normalized_id not in keep_id_set or normalized_id in seen_ids:
                continue
            seen_ids.add(normalized_id)
            valid_source_ids.append(normalized_id)
        if not valid_source_ids:
            continue
        text = str(paragraph.get("text", "") or "").strip()
        if not text:
            text = " ".join(
                str(sentence_text_map.get(sentence_id, "") or "").strip()
                for sentence_id in valid_source_ids
            ).strip()
        if not text:
            continue
        normalized_paragraphs.append(
            {
                "text": text,
                "source_sentence_ids": valid_source_ids,
                "merge_type": str(paragraph.get("merge_type", "未合并") or "未合并"),
            }
        )

    if not normalized_paragraphs:
        normalized_paragraphs = build_fallback_paragraphs(ordered_keep_ids, sentence_text_map)

    deduplicated_paragraphs = deduplicate_paragraphs(normalized_paragraphs)
    pure_text_script = [
        {
            "paragraph_id": f"P{index + 1:03d}",
            "text": paragraph["text"],
            "source_sentence_ids": paragraph["source_sentence_ids"],
            "merge_type": paragraph.get("merge_type", "未合并"),
        }
        for index, paragraph in enumerate(deduplicated_paragraphs)
    ]
    return non_redundant_sentences, pure_text_script


class Stage1ProjectionRepository:
    """从任务内 runtime_state.db 投影 Stage1 关键产物。"""

    def __init__(self, *, output_dir: str, task_id: str = "") -> None:
        self.output_dir = str(output_dir or "").strip()
        self.task_id = str(task_id or "").strip()
        self.store = RuntimeRecoveryStore(output_dir=self.output_dir, task_id=self.task_id)

    def load_projected_state(self) -> Optional[Dict[str, Any]]:
        if not self.output_dir:
            return None
        snapshot = self.store.load_stage_snapshot(stage="stage1")
        if not isinstance(snapshot, dict):
            return None
        subtitle_path = str(snapshot.get("subtitle_path", "") or "").strip()
        if not subtitle_path:
            return None

        original_subtitles = _normalize_subtitles(read_subtitle_sample(subtitle_path, count=None))
        if not original_subtitles:
            return None

        committed_rows = self._load_committed_stage1_llm_rows()
        if not committed_rows:
            return None

        rows_by_step: Dict[str, List[Dict[str, Any]]] = {}
        for row in committed_rows:
            stage_step = str(row.get("stage_step", "") or "").strip()
            if not stage_step:
                continue
            rows_by_step.setdefault(stage_step, []).append(row)
        for stage_step in list(rows_by_step.keys()):
            rows_by_step[stage_step].sort(
                key=lambda row: _unit_sort_key(
                    str(row.get("unit_id", "") or ""),
                    str(row.get("llm_call_id", "") or ""),
                )
            )

        domain, main_topic = self._project_topic(snapshot, rows_by_step.get("step1_validate", []))
        corrected_payload = self._project_step2(original_subtitles, rows_by_step.get("step2_correction", []))
        corrected_subtitles = list(corrected_payload.get("corrected_subtitles", []) or [])
        if not corrected_subtitles:
            return None

        merged_sentences = self._project_step3(corrected_subtitles, rows_by_step.get("step3_merge", []))
        translated_sentences = self._project_step35(merged_sentences, rows_by_step.get("step3_5_translate", []))
        cleaned_sentences = self._project_step4(
            translated_sentences=translated_sentences,
            merged_sentences=merged_sentences,
            step4_rows=rows_by_step.get("step4_clean_local", []),
        )
        non_redundant_sentences, pure_text_script = self._project_step56(
            cleaned_sentences=cleaned_sentences,
            step56_rows=rows_by_step.get("step5_6_dedup_merge", []),
        )
        sentence_timestamps = _build_sentence_timestamps(translated_sentences or merged_sentences)

        return {
            "projected_from_runtime": True,
            "subtitle_path": subtitle_path,
            "domain": domain,
            "main_topic": main_topic,
            "corrected_subtitles": corrected_subtitles,
            "correction_summary": list(corrected_payload.get("correction_summary", []) or []),
            "cleanup_summary": list(corrected_payload.get("cleanup_summary", []) or []),
            "merged_sentences": merged_sentences,
            "translated_sentences": translated_sentences,
            "cleaned_sentences": cleaned_sentences,
            "non_redundant_sentences": non_redundant_sentences,
            "pure_text_script": pure_text_script,
            "sentence_timestamps": sentence_timestamps,
        }

    def _load_committed_stage1_llm_rows(self) -> List[Dict[str, Any]]:
        sqlite_rows = self.store.list_sqlite_llm_records(
            stage="stage1",
            status="SUCCESS",
            limit=4000,
        )
        requests: List[Dict[str, Any]] = []
        record_by_key: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for row in list(sqlite_rows or []):
            if not isinstance(row, dict):
                continue
            chunk_id = str(row.get("chunk_id", "") or "").strip()
            llm_call_id = str(row.get("llm_call_id", "") or "").strip()
            input_fingerprint = str(row.get("input_fingerprint", "") or "").strip()
            if not chunk_id or not llm_call_id or not input_fingerprint:
                continue
            request = {
                "stage": "stage1",
                "chunk_id": chunk_id,
                "llm_call_id": llm_call_id,
                "input_fingerprint": input_fingerprint,
            }
            requests.append(request)
            record_by_key[(chunk_id, llm_call_id, input_fingerprint)] = dict(row)

        rows: List[Dict[str, Any]] = []
        for item in self.store.batch_load_committed_llm_responses(requests):
            request = dict(item.get("request", {}) or {})
            restored = dict(item.get("restored", {}) or {})
            if not restored:
                continue
            response_payload = _safe_json_loads(str(restored.get("response_text", "") or ""))
            if not isinstance(response_payload, dict):
                continue
            key = (
                str(request.get("chunk_id", "") or "").strip(),
                str(request.get("llm_call_id", "") or "").strip(),
                str(request.get("input_fingerprint", "") or "").strip(),
            )
            record = dict(record_by_key.get(key, {}) or {})
            rows.append(
                {
                    **record,
                    **request,
                    "request_scope_ids": list(restored.get("request_scope_ids", []) or []),
                    "response_payload": response_payload,
                    "response_text": str(restored.get("response_text", "") or ""),
                }
            )
        return rows

    def _project_topic(
        self,
        snapshot: Dict[str, Any],
        topic_rows: Sequence[Dict[str, Any]],
    ) -> Tuple[str, str]:
        for row in list(topic_rows or []):
            payload = dict(row.get("response_payload", {}) or {})
            domain, main_topic, _ = parse_step1_topic_payload(payload)
            if domain or main_topic:
                return domain, main_topic
        return (
            str(snapshot.get("domain", "") or "").strip(),
            str(snapshot.get("main_topic", "") or "").strip(),
        )

    def _project_step2(
        self,
        original_subtitles: Sequence[Dict[str, Any]],
        step2_rows: Sequence[Dict[str, Any]],
    ) -> Dict[str, Any]:
        subtitle_by_id = {
            str(item.get("subtitle_id", "") or "").strip(): dict(item)
            for item in list(original_subtitles or [])
            if isinstance(item, dict) and str(item.get("subtitle_id", "") or "").strip()
        }
        ordered_subtitle_ids = [str(item.get("subtitle_id", "") or "").strip() for item in list(original_subtitles or [])]
        corrected_by_id: Dict[str, Dict[str, Any]] = {}
        correction_summary: List[Dict[str, Any]] = []
        cleanup_summary: List[Dict[str, Any]] = []

        for row in list(step2_rows or []):
            response_payload = dict(row.get("response_payload", {}) or {})
            batch_ids = [
                subtitle_id
                for subtitle_id in _resolve_row_scope_ids(row)
                if subtitle_id in subtitle_by_id
            ]
            if not batch_ids:
                continue
            parsed_payload, _ = parse_step2_llm_payload(response_payload)
            cleanup_payload = {
                key: response_payload.get(key)
                for key in ("d", "r", "removals", "cleaned_sentences")
                if key in response_payload
            }
            cleanup_by_id, _ = parse_step4_cleaned_sentences(
                cleanup_payload,
                valid_sentence_ids=set(batch_ids),
            )
            for subtitle_id in batch_ids:
                source_subtitle = dict(subtitle_by_id.get(subtitle_id, {}) or {})
                parsed_item = dict(parsed_payload.get(subtitle_id, {}) or {})
                parsed_cleanup = dict(cleanup_by_id.get(subtitle_id, {}) or {})
                reconciled_text, reconciled_corrections, _ = reconcile_step2_item(
                    str(source_subtitle.get("text", "") or ""),
                    str(parsed_item.get("corrected_text", "") or ""),
                    list(parsed_item.get("corrections", []) or []),
                    subtitle_id=subtitle_id,
                )
                cleaned_text, applied_removals, _ = reconcile_step4_item(
                    reconciled_text,
                    str(parsed_cleanup.get("cleaned_text", "") or ""),
                    list(parsed_cleanup.get("removals", []) or []),
                    sentence_id=subtitle_id,
                )
                if cleaned_text and _drops_cjk_en_glossary_pair(reconciled_text, cleaned_text):
                    cleaned_text = reconciled_text
                if not cleaned_text:
                    cleaned_text = reconciled_text
                corrected_by_id[subtitle_id] = {
                    "subtitle_id": subtitle_id,
                    "corrected_text": cleaned_text,
                    "start_sec": source_subtitle.get("start_sec", 0.0),
                    "end_sec": source_subtitle.get("end_sec", 0.0),
                }
                correction_summary.extend(reconciled_corrections)
                cleanup_summary.extend(applied_removals)

        corrected_subtitles: List[Dict[str, Any]] = []
        for subtitle_id in ordered_subtitle_ids:
            original = subtitle_by_id.get(subtitle_id)
            if not isinstance(original, dict):
                continue
            corrected_subtitles.append(
                dict(
                    corrected_by_id.get(
                        subtitle_id,
                        {
                            "subtitle_id": subtitle_id,
                            "corrected_text": str(original.get("text", "") or ""),
                            "start_sec": original.get("start_sec", 0.0),
                            "end_sec": original.get("end_sec", 0.0),
                        },
                    )
                )
            )
        return {
            "corrected_subtitles": corrected_subtitles,
            "correction_summary": correction_summary,
            "cleanup_summary": cleanup_summary,
        }

    def _project_step3(
        self,
        corrected_subtitles: Sequence[Dict[str, Any]],
        step3_rows: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        subtitle_by_id = {
            str(item.get("subtitle_id", "") or "").strip(): dict(item)
            for item in list(corrected_subtitles or [])
            if isinstance(item, dict) and str(item.get("subtitle_id", "") or "").strip()
        }
        ordered_subtitle_ids = [str(item.get("subtitle_id", "") or "").strip() for item in list(corrected_subtitles or [])]
        subtitle_index_by_id = {subtitle_id: index for index, subtitle_id in enumerate(ordered_subtitle_ids)}
        all_candidates: List[Dict[str, Any]] = []

        for window_index, row in enumerate(list(step3_rows or [])):
            response_payload = dict(row.get("response_payload", {}) or {})
            valid_subtitle_ids = {
                subtitle_id
                for subtitle_id in _resolve_row_scope_ids(row)
                if subtitle_id in subtitle_by_id
            }
            parsed_items, _ = parse_step3_merged_sentences(
                response_payload,
                valid_subtitle_ids=valid_subtitle_ids,
            )
            window_candidates, _ = build_step3_window_candidates(
                parsed_items,
                subtitle_index_by_id=subtitle_index_by_id,
                ordered_subtitle_ids=ordered_subtitle_ids,
                subtitle_by_id=subtitle_by_id,
            )
            for candidate in window_candidates:
                candidate["window_index"] = window_index
            all_candidates.extend(window_candidates)

        merged_sentences, _, _ = assemble_step3_merged_sentences(
            all_candidates,
            ordered_subtitle_ids=ordered_subtitle_ids,
            subtitle_by_id=subtitle_by_id,
        )
        for index, item in enumerate(merged_sentences, start=1):
            item["sentence_id"] = f"S{index:03d}"
        return merged_sentences

    def _project_step35(
        self,
        merged_sentences: Sequence[Dict[str, Any]],
        step35_rows: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        passthrough_by_id: Dict[str, Dict[str, Any]] = {}
        translate_candidate_by_id: Dict[str, Dict[str, Any]] = {}
        for item in list(merged_sentences or []):
            if not isinstance(item, dict):
                continue
            sentence_id = str(item.get("sentence_id", "") or "").strip()
            if not sentence_id:
                continue
            normalized_item = {
                "sentence_id": sentence_id,
                "text": str(item.get("text", "") or ""),
                "start_sec": item.get("start_sec", 0.0),
                "end_sec": item.get("end_sec", 0.0),
                "source_subtitle_ids": list(item.get("source_subtitle_ids", []) or []),
            }
            if _contains_cjk(normalized_item["text"]):
                passthrough_by_id[sentence_id] = dict(normalized_item)
            else:
                translate_candidate_by_id[sentence_id] = dict(normalized_item)

        translated_by_id: Dict[str, Dict[str, Any]] = {}
        for row in list(step35_rows or []):
            response_payload = dict(row.get("response_payload", {}) or {})
            ordered_batch_ids = [
                sentence_id
                for sentence_id in _resolve_row_scope_ids(row)
                if sentence_id in translate_candidate_by_id
            ]
            if not ordered_batch_ids:
                continue
            translated_text_by_id, _ = parse_step35_translated_sentences(
                response_payload,
                valid_sentence_ids=set(ordered_batch_ids),
            )
            for sentence_id in ordered_batch_ids:
                source_item = dict(translate_candidate_by_id.get(sentence_id, {}) or {})
                translated_by_id[sentence_id] = {
                    "sentence_id": sentence_id,
                    "text": str(translated_text_by_id.get(sentence_id, source_item.get("text", "")) or source_item.get("text", "")),
                    "start_sec": source_item.get("start_sec", 0.0),
                    "end_sec": source_item.get("end_sec", 0.0),
                    "source_subtitle_ids": list(source_item.get("source_subtitle_ids", []) or []),
                }

        final_sentences: List[Dict[str, Any]] = []
        for source in list(merged_sentences or []):
            if not isinstance(source, dict):
                continue
            sentence_id = str(source.get("sentence_id", "") or "").strip()
            if not sentence_id:
                continue
            if sentence_id in passthrough_by_id:
                final_sentences.append(dict(passthrough_by_id[sentence_id]))
                continue
            if sentence_id in translated_by_id:
                final_sentences.append(dict(translated_by_id[sentence_id]))
                continue
            final_sentences.append(
                {
                    "sentence_id": sentence_id,
                    "text": str(source.get("text", "") or ""),
                    "start_sec": source.get("start_sec", 0.0),
                    "end_sec": source.get("end_sec", 0.0),
                    "source_subtitle_ids": list(source.get("source_subtitle_ids", []) or []),
                }
            )
        return final_sentences

    def _project_step4(
        self,
        *,
        translated_sentences: Sequence[Dict[str, Any]],
        merged_sentences: Sequence[Dict[str, Any]],
        step4_rows: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        source_sentences = list(translated_sentences or merged_sentences or [])
        if not step4_rows:
            ordered_sources = sorted(
                enumerate(source_sentences),
                key=lambda pair: (
                    float(pair[1].get("start_sec", 0.0)) if isinstance(pair[1], dict) else 0.0,
                    float(pair[1].get("end_sec", 0.0)) if isinstance(pair[1], dict) else 0.0,
                    pair[0],
                ),
            )
            cleaned_sentences: List[Dict[str, Any]] = []
            for _, source in ordered_sources:
                if not isinstance(source, dict):
                    continue
                sentence_id = str(source.get("sentence_id", "") or "").strip()
                if not sentence_id:
                    continue
                cleaned_sentences.append(
                    {
                        "sentence_id": sentence_id,
                        "cleaned_text": str(source.get("text", source.get("cleaned_text", "")) or ""),
                    }
                )
            return cleaned_sentences

        source_by_id = {
            str(item.get("sentence_id", "") or "").strip(): dict(item)
            for item in source_sentences
            if isinstance(item, dict) and str(item.get("sentence_id", "") or "").strip()
        }
        cleaned_maps: List[Dict[str, Dict[str, Any]]] = []
        for row in list(step4_rows or []):
            response_payload = dict(row.get("response_payload", {}) or {})
            valid_sentence_ids = {
                sentence_id
                for sentence_id in _resolve_row_scope_ids(row)
                if sentence_id in source_by_id
            }
            cleaned_by_id, _ = parse_step4_cleaned_sentences(
                response_payload,
                valid_sentence_ids=valid_sentence_ids,
            )
            cleaned_maps.append(cleaned_by_id)
        merged_cleaned_by_id, _ = merge_step4_cleaned_maps(cleaned_maps)
        cleaned_sentences, _ = assemble_step4_cleaned_sentences(
            list(source_sentences or []),
            llm_cleaned_by_id=merged_cleaned_by_id,
            glossary_guard=_drops_cjk_en_glossary_pair,
        )
        return cleaned_sentences

    def _project_step56(
        self,
        *,
        cleaned_sentences: Sequence[Dict[str, Any]],
        step56_rows: Sequence[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        cleaned_by_id = {
            str(item.get("sentence_id", "") or "").strip(): dict(item)
            for item in list(cleaned_sentences or [])
            if isinstance(item, dict) and str(item.get("sentence_id", "") or "").strip()
        }
        aggregated_keep_ids: List[str] = []
        aggregated_paragraphs: List[Dict[str, Any]] = []
        for row in list(step56_rows or []):
            response_payload = dict(row.get("response_payload", {}) or {})
            ordered_batch_ids = [
                sentence_id
                for sentence_id in _resolve_row_scope_ids(row)
                if sentence_id in cleaned_by_id
            ]
            if not ordered_batch_ids:
                continue
            id_text_pairs = sentence_id_and_text_pairs([cleaned_by_id[sentence_id] for sentence_id in ordered_batch_ids])
            sentence_text_map = {sentence_id: text for sentence_id, text in id_text_pairs}
            keep_ids, paragraphs, _ = parse_step56_dedup_merge_payload(
                response_payload,
                ordered_batch_ids=ordered_batch_ids,
                sentence_text_map=sentence_text_map,
            )
            aggregated_keep_ids.extend(keep_ids)
            aggregated_paragraphs.extend(paragraphs)

        return _finalize_step56_projection(
            cleaned_sentences=cleaned_sentences,
            aggregated_keep_ids=aggregated_keep_ids,
            aggregated_paragraphs=aggregated_paragraphs,
        )
