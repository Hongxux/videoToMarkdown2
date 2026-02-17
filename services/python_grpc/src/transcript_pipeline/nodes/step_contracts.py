"""
Step 合约与本地重建工具集。

职责：
1) 统一 LLM payload 解析，隔离字段演进影响。
2) 提供本地重建与顺序对齐能力，降低节点文件复杂度。
3) 输出可观测计数，便于后续兼容层收敛与质量治理。
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from services.python_grpc.src.common.utils.text_patch import (
    find_all_occurrences as _find_all_occurrences,
    find_contextual_match_positions as _find_contextual_match_positions,
    replace_by_index as _replace_by_index,
)
from services.python_grpc.src.common.utils.patch_protocol import (
    normalize_removal_patch_item as _normalize_removal_patch_item,
)


def order_records_by_reference_ids(
    records: List[Dict[str, Any]],
    reference_ids: Iterable[str],
    *,
    id_key: str,
) -> List[Dict[str, Any]]:
    """按参考 ID 顺序重排记录，未知 ID 记录保持原相对顺序追加到尾部。"""
    if not records:
        return []

    normalized_reference_ids = [str(item or "").strip() for item in reference_ids]
    first_index_by_id: Dict[str, int] = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        record_id = str(record.get(id_key, "") or "").strip()
        if not record_id or record_id in first_index_by_id:
            continue
        first_index_by_id[record_id] = index

    ordered: List[Dict[str, Any]] = []
    used_indexes: Set[int] = set()
    for record_id in normalized_reference_ids:
        index = first_index_by_id.get(record_id)
        if index is None:
            continue
        ordered.append(records[index])
        used_indexes.add(index)

    for index, record in enumerate(records):
        if index in used_indexes:
            continue
        ordered.append(record)

    return ordered


def parse_step1_topic_payload(result: Dict[str, Any]) -> Tuple[str, str, Dict[str, int]]:
    """解析 Step1 domain/main_topic，兼容轻量短键。"""
    metrics = Counter()
    if not isinstance(result, dict):
        metrics["invalid_payload_type"] += 1
        return "", "", dict(metrics)

    # 兼容短键：d/main_topic(mt)
    domain = str(result.get("domain", result.get("d", "")) or "").strip()
    main_topic = str(result.get("main_topic", result.get("mt", "")) or "").strip()
    if "d" in result:
        metrics["compact_key_hits"] += 1
    if "mt" in result:
        metrics["compact_key_hits"] += 1
    if "domain" in result:
        metrics["verbose_key_hits"] += 1
    if "main_topic" in result:
        metrics["verbose_key_hits"] += 1
    if not domain:
        metrics["empty_domain"] += 1
    if not main_topic:
        metrics["empty_main_topic"] += 1
    return domain, main_topic, dict(metrics)


def normalize_step2_corrections(
    corrections: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, str]], Dict[str, int]]:
    """标准化 Step2 纠错项，兼容短键与历史别名。"""
    metrics = Counter()
    normalized: List[Dict[str, str]] = []
    for item in corrections or []:
        if not isinstance(item, dict):
            metrics["invalid_item_type"] += 1
            continue
        original = str(item.get("original", item.get("o", ""))).strip()
        corrected = str(item.get("corrected", item.get("c", ""))).strip()
        if not original or not corrected or original == corrected:
            metrics["dropped_invalid_correction"] += 1
            continue

        left_raw = item.get("left_context", item.get("context_before", item.get("l", "")))
        right_raw = item.get("right_context", item.get("context_after", item.get("r", "")))
        left_context = str(left_raw if left_raw is not None else "")
        right_context = str(right_raw if right_raw is not None else "")
        subtitle_id = str(item.get("subtitle_id", item.get("sid", ""))).strip()
        if "o" in item or "c" in item or "l" in item or "r" in item or "sid" in item:
            metrics["compact_key_hits"] += 1
        if "original" in item or "corrected" in item:
            metrics["verbose_key_hits"] += 1
        if "context_before" in item or "context_after" in item:
            metrics["legacy_alias_hits"] += 1
        normalized.append(
            {
                "original": original,
                "corrected": corrected,
                "left_context": left_context,
                "right_context": right_context,
                "subtitle_id": subtitle_id,
            }
        )
    metrics["normalized_count"] += len(normalized)
    return normalized, dict(metrics)


def parse_step2_llm_payload(
    result: Dict[str, Any],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    """
    统一解析 Step2 LLM 输出，兼容：
    1) 新格式：{"c": [...]} 或 {"corrections": [...]}
    2) 旧格式：{"corrected_subtitles": [{"subtitle_id","corrected_text","corrections": [...]}]}
    """
    metrics = Counter()
    per_subtitle: Dict[str, Dict[str, Any]] = {}

    def _slot(subtitle_id: str) -> Dict[str, Any]:
        sid = str(subtitle_id or "").strip()
        if not sid:
            return {}
        if sid not in per_subtitle:
            per_subtitle[sid] = {"corrected_text": "", "corrections": []}
        return per_subtitle[sid]

    if not isinstance(result, dict):
        metrics["invalid_payload_type"] += 1
        return per_subtitle, dict(metrics)

    top_level_corrections: List[Dict[str, Any]] = []
    for key in ("c", "corrections"):
        payload_items = result.get(key, [])
        if not isinstance(payload_items, list):
            continue
        top_level_corrections.extend(payload_items)
        if key == "c":
            metrics["top_level_compact_shape_hits"] += 1
        else:
            metrics["top_level_verbose_shape_hits"] += 1

    normalized_top, normalize_metrics = normalize_step2_corrections(top_level_corrections)
    metrics.update(normalize_metrics)
    for item in normalized_top:
        sid = item.get("subtitle_id", "")
        slot = _slot(sid)
        if not slot:
            metrics["dropped_missing_subtitle_id"] += 1
            continue
        slot["corrections"].append(item)

    legacy_items = result.get("corrected_subtitles", [])
    if isinstance(legacy_items, list):
        metrics["legacy_corrected_subtitles_shape_hits"] += 1
        for item in legacy_items:
            if not isinstance(item, dict):
                metrics["legacy_invalid_item_type"] += 1
                continue
            sid = str(item.get("subtitle_id", "")).strip()
            slot = _slot(sid)
            if not slot:
                metrics["dropped_legacy_missing_subtitle_id"] += 1
                continue
            if item.get("corrected_text") is not None:
                slot["corrected_text"] = str(item.get("corrected_text", "")).strip()
            nested = item.get("corrections", [])
            if isinstance(nested, list):
                normalized_nested, nested_metrics = normalize_step2_corrections(nested)
                metrics.update(nested_metrics)
                for correction in normalized_nested:
                    if not correction.get("subtitle_id"):
                        correction["subtitle_id"] = sid
                slot["corrections"].extend(normalized_nested)
    metrics["subtitle_slot_count"] += len(per_subtitle)
    return per_subtitle, dict(metrics)


def apply_step2_corrections_to_text(
    text: str,
    corrections: List[Dict[str, str]],
) -> Tuple[str, Set[int], Dict[str, int]]:
    """按上下文保守回放纠错，返回文本、已应用索引与观测计数。"""
    metrics = Counter()
    updated = str(text or "")
    applied_indexes: Set[int] = set()

    for idx, correction in enumerate(corrections):
        metrics["correction_total"] += 1
        original = correction["original"]
        corrected = correction["corrected"]
        left_context = correction.get("left_context", "")
        right_context = correction.get("right_context", "")

        original_positions = _find_contextual_match_positions(
            updated,
            original,
            left_context=left_context,
            right_context=right_context,
        )
        if len(original_positions) == 1:
            updated = _replace_by_index(updated, original_positions[0], len(original), corrected)
            applied_indexes.add(idx)
            metrics["applied_contextual_replace"] += 1
            continue

        corrected_positions = _find_contextual_match_positions(
            updated,
            corrected,
            left_context=left_context,
            right_context=right_context,
        )
        if len(corrected_positions) == 1:
            applied_indexes.add(idx)
            metrics["applied_contextual_existing"] += 1
            continue

        if not left_context and not right_context:
            plain_positions = _find_all_occurrences(updated, original)
            if len(plain_positions) == 1:
                updated = _replace_by_index(updated, plain_positions[0], len(original), corrected)
                applied_indexes.add(idx)
                metrics["applied_fallback_replace"] += 1
                continue
            corrected_plain_positions = _find_all_occurrences(updated, corrected)
            if len(corrected_plain_positions) == 1:
                applied_indexes.add(idx)
                metrics["applied_fallback_existing"] += 1
                continue
            if len(plain_positions) > 1 or len(corrected_plain_positions) > 1:
                metrics["skipped_ambiguous"] += 1
            else:
                metrics["skipped_no_match"] += 1
            continue

        if len(original_positions) > 1 or len(corrected_positions) > 1:
            metrics["skipped_ambiguous"] += 1
        else:
            metrics["skipped_no_match"] += 1

    return updated, applied_indexes, dict(metrics)


def reconcile_step2_item(
    original_text: str,
    llm_corrected_text: str,
    llm_corrections: List[Dict[str, Any]],
    *,
    subtitle_id: str = "",
) -> Tuple[str, List[Dict[str, str]], Dict[str, int]]:
    """统一 corrected_text 与 corrections，并返回观测计数。"""
    normalized_corrections, normalize_metrics = normalize_step2_corrections(llm_corrections)
    if subtitle_id:
        for correction in normalized_corrections:
            if not correction.get("subtitle_id"):
                correction["subtitle_id"] = subtitle_id
    llm_text = str(llm_corrected_text or original_text or "")
    source_text = str(original_text or "")

    corrected_from_llm, llm_applied, llm_metrics = apply_step2_corrections_to_text(
        llm_text,
        normalized_corrections,
    )
    corrected_from_source, source_applied, source_metrics = apply_step2_corrections_to_text(
        source_text,
        normalized_corrections,
    )

    final_metrics = Counter()
    final_metrics.update(normalize_metrics)
    if len(source_applied) > len(llm_applied):
        final_text = corrected_from_source
        final_applied = source_applied
        final_metrics.update({f"source_{k}": v for k, v in source_metrics.items()})
        final_metrics["selected_source_path"] += 1
    else:
        final_text = corrected_from_llm
        final_applied = llm_applied
        final_metrics.update({f"llm_{k}": v for k, v in llm_metrics.items()})
        final_metrics["selected_llm_path"] += 1

    final_corrections = [normalized_corrections[idx] for idx in sorted(final_applied)]
    final_metrics["final_applied_corrections"] += len(final_corrections)
    return final_text, final_corrections, dict(final_metrics)


def parse_step3_merged_sentences(
    result: Dict[str, Any],
    *,
    valid_subtitle_ids: Set[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """解析 Step3 merged_sentences，做最小合法化与观测。"""
    metrics = Counter()
    raw_items = []
    if isinstance(result, dict):
        if isinstance(result.get("merged_sentences"), list):
            raw_items = result.get("merged_sentences", [])
            metrics["verbose_shape_hits"] += 1
        elif isinstance(result.get("merged_groups"), list):
            raw_items = result.get("merged_groups", [])
            metrics["merged_groups_shape_hits"] += 1
        elif isinstance(result.get("m"), list):
            raw_items = result.get("m", [])
            metrics["compact_shape_hits"] += 1
        elif isinstance(result.get("mg"), list):
            raw_items = result.get("mg", [])
            metrics["compact_merge_groups_shape_hits"] += 1

    parsed: List[Dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            metrics["dropped_invalid_item_type"] += 1
            continue
        text = str(item.get("text", item.get("t", ""))).strip()
        if not text:
            metrics["dropped_empty_text"] += 1
            continue
        source_ids_raw = item.get("source_subtitle_ids", item.get("sids", []))
        if not isinstance(source_ids_raw, list):
            metrics["dropped_invalid_source_ids_type"] += 1
            continue
        source_ids: List[str] = []
        seen: Set[str] = set()
        for sid in source_ids_raw:
            normalized = str(sid or "").strip()
            if not normalized or normalized in seen or normalized not in valid_subtitle_ids:
                continue
            seen.add(normalized)
            source_ids.append(normalized)
        if not source_ids:
            metrics["dropped_empty_source_ids"] += 1
            continue
        parsed.append({"text": text, "source_subtitle_ids": source_ids})
        metrics["accepted_items"] += 1
    metrics["raw_items"] += len(raw_items)
    return parsed, dict(metrics)


def build_step3_window_candidates(
    parsed_items: List[Dict[str, Any]],
    *,
    subtitle_index_by_id: Dict[str, int],
    ordered_subtitle_ids: List[str],
    subtitle_by_id: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """将 Step3 解析结果标准化为窗口级合并候选，过滤掉无效分组。"""
    metrics = Counter()
    window_candidates: List[Dict[str, Any]] = []
    for item in parsed_items:
        source_ids = list(item.get("source_subtitle_ids", []))
        if len(source_ids) < 2:
            metrics["dropped_non_merge_group"] += 1
            continue

        ordered_source_ids = sorted(source_ids, key=lambda sid: subtitle_index_by_id.get(sid, 10**9))
        source_indices = [subtitle_index_by_id[sid] for sid in ordered_source_ids if sid in subtitle_index_by_id]
        if len(source_indices) < 2:
            metrics["dropped_invalid_group_indices"] += 1
            continue

        start_index = min(source_indices)
        end_index = max(source_indices)
        expected_ids = ordered_subtitle_ids[start_index : end_index + 1]
        if ordered_source_ids != expected_ids:
            # 只接受连续字幕组，避免跨越拼接破坏时序稳定性。
            metrics["dropped_non_contiguous_group"] += 1
            continue

        relevant_subs = [subtitle_by_id[sid] for sid in ordered_source_ids if sid in subtitle_by_id]
        start_sec = min((s.get("start_sec", 0) for s in relevant_subs), default=0)
        end_sec = max((s.get("end_sec", 0) for s in relevant_subs), default=0)

        window_candidates.append(
            {
                "text": item.get("text", ""),
                "start_sec": start_sec,
                "end_sec": end_sec,
                "source_subtitle_ids": ordered_source_ids,
                "start_index": start_index,
                "span_len": len(ordered_source_ids),
            }
        )
        metrics["accepted_merge_candidates"] += 1
    return window_candidates, dict(metrics)


def assemble_step3_merged_sentences(
    all_candidates: List[Dict[str, Any]],
    *,
    ordered_subtitle_ids: List[str],
    subtitle_by_id: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Set[str], Dict[str, int]]:
    """将窗口候选组全局装配为最终 merged_sentences，并补齐直通句。"""
    metrics = Counter()
    deduped_candidates: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    for candidate in all_candidates:
        key = tuple(candidate.get("source_subtitle_ids", []))
        if not key:
            continue
        existing = deduped_candidates.get(key)
        if existing is None:
            deduped_candidates[key] = candidate
            continue
        metrics["deduped_duplicate_groups"] += 1
        if int(candidate.get("window_index", 10**9)) < int(existing.get("window_index", 10**9)):
            deduped_candidates[key] = candidate

    candidates_by_start: Dict[int, List[Dict[str, Any]]] = {}
    for candidate in deduped_candidates.values():
        start_index = int(candidate.get("start_index", -1))
        if start_index < 0:
            continue
        candidates_by_start.setdefault(start_index, []).append(candidate)
    for start_index in list(candidates_by_start.keys()):
        candidates_by_start[start_index].sort(
            key=lambda item: (-int(item.get("span_len", 0)), int(item.get("window_index", 10**9)))
        )

    merged_sentences: List[Dict[str, Any]] = []
    merged_subtitle_ids: Set[str] = set()
    cursor = 0
    while cursor < len(ordered_subtitle_ids):
        selected_group = None
        for option in candidates_by_start.get(cursor, []):
            source_ids = list(option.get("source_subtitle_ids", []))
            if not source_ids:
                continue
            expected_ids = ordered_subtitle_ids[cursor : cursor + len(source_ids)]
            if source_ids != expected_ids:
                metrics["dropped_conflict_group"] += 1
                continue
            selected_group = option
            break

        if selected_group is not None:
            source_ids = list(selected_group.get("source_subtitle_ids", []))
            merged_sentences.append(
                {
                    "text": selected_group.get("text", ""),
                    "start_sec": selected_group.get("start_sec", 0),
                    "end_sec": selected_group.get("end_sec", 0),
                    "source_subtitle_ids": source_ids,
                }
            )
            merged_subtitle_ids.update(source_ids)
            metrics["selected_merge_groups"] += 1
            cursor += len(source_ids)
            continue

        passthrough_id = ordered_subtitle_ids[cursor]
        passthrough_sub = subtitle_by_id.get(passthrough_id, {})
        merged_sentences.append(
            {
                "text": str(passthrough_sub.get("corrected_text", passthrough_sub.get("text", ""))),
                "start_sec": passthrough_sub.get("start_sec", 0),
                "end_sec": passthrough_sub.get("end_sec", 0),
                "source_subtitle_ids": [passthrough_id],
            }
        )
        metrics["passthrough_sentences"] += 1
        cursor += 1

    metrics["raw_merge_candidates"] += len(all_candidates)
    metrics["deduped_merge_candidates"] += len(deduped_candidates)
    metrics["merged_subtitle_count"] += len(merged_subtitle_ids)
    metrics["input_subtitle_count"] += len(ordered_subtitle_ids)
    metrics["output_sentence_count"] += len(merged_sentences)
    return merged_sentences, merged_subtitle_ids, dict(metrics)


def parse_step35_translated_sentences(
    result: Dict[str, Any],
    *,
    valid_sentence_ids: Set[str],
) -> Tuple[Dict[str, str], Dict[str, int]]:
    """解析 Step3.5 translated_sentences。"""
    metrics = Counter()
    raw_items = []
    if isinstance(result, dict):
        if isinstance(result.get("translated_sentences"), list):
            raw_items = result.get("translated_sentences", [])
            metrics["verbose_shape_hits"] += 1
        elif isinstance(result.get("t"), list):
            raw_items = result.get("t", [])
            metrics["compact_shape_hits"] += 1

    translated_by_id: Dict[str, str] = {}
    for item in raw_items:
        if not isinstance(item, dict):
            metrics["dropped_invalid_item_type"] += 1
            continue
        sentence_id = str(item.get("sentence_id", item.get("sid", ""))).strip()
        translated_text = str(item.get("translated_text", item.get("tt", item.get("t", "")))).strip()
        if not sentence_id or sentence_id not in valid_sentence_ids:
            metrics["dropped_invalid_sentence_id"] += 1
            continue
        if not translated_text:
            metrics["dropped_empty_text"] += 1
            continue
        if sentence_id in translated_by_id:
            metrics["dropped_duplicate_sentence_id"] += 1
            continue
        translated_by_id[sentence_id] = translated_text
        metrics["accepted_items"] += 1
    metrics["raw_items"] += len(raw_items)
    return translated_by_id, dict(metrics)


def normalize_step4_removals(
    removals: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, str]], Dict[str, int]]:
    """标准化 Step4 删除项，兼容短键与历史别名。"""
    metrics = Counter()
    normalized: List[Dict[str, str]] = []
    for item in removals or []:
        if not isinstance(item, dict):
            metrics["invalid_removal_item_type"] += 1
            continue
        normalized_item = _normalize_removal_patch_item(item)
        if normalized_item is None:
            metrics["dropped_invalid_removal"] += 1
            continue

        if "o" in item or "l" in item or "r" in item or "sid" in item:
            metrics["compact_key_hits"] += 1
        if "original" in item or "sentence_id" in item:
            metrics["verbose_key_hits"] += 1
        if "context_before" in item or "context_after" in item:
            metrics["legacy_alias_hits"] += 1
        normalized.append(normalized_item)
    metrics["normalized_removal_count"] += len(normalized)
    return normalized, dict(metrics)


def apply_step4_removals_to_text(
    text: str,
    removals: List[Dict[str, str]],
) -> Tuple[str, Set[int], Dict[str, int]]:
    """按上下文保守执行 Step4 删除补丁，返回文本、已应用索引与观测计数。"""
    metrics = Counter()
    updated = str(text or "")
    applied_indexes: Set[int] = set()

    for idx, removal in enumerate(removals):
        metrics["removal_total"] += 1
        original = removal["original"]
        left_context = removal.get("left_context", "")
        right_context = removal.get("right_context", "")

        original_positions = _find_contextual_match_positions(
            updated,
            original,
            left_context=left_context,
            right_context=right_context,
        )
        if len(original_positions) == 1:
            updated = _replace_by_index(updated, original_positions[0], len(original), "")
            applied_indexes.add(idx)
            metrics["applied_contextual_remove"] += 1
            continue

        if not left_context and not right_context:
            plain_positions = _find_all_occurrences(updated, original)
            if len(plain_positions) == 1:
                updated = _replace_by_index(updated, plain_positions[0], len(original), "")
                applied_indexes.add(idx)
                metrics["applied_fallback_remove"] += 1
                continue
            if len(plain_positions) > 1:
                metrics["skipped_ambiguous"] += 1
            else:
                metrics["skipped_no_match"] += 1
            continue

        if len(original_positions) > 1:
            metrics["skipped_ambiguous"] += 1
        else:
            metrics["skipped_no_match"] += 1

    return updated, applied_indexes, dict(metrics)


def reconcile_step4_item(
    original_text: str,
    llm_cleaned_text: str,
    llm_removals: List[Dict[str, Any]],
    *,
    sentence_id: str = "",
) -> Tuple[str, List[Dict[str, str]], Dict[str, int]]:
    """统一 Step4 文本清理结果：优先应用 removals，兼容 legacy cleaned_text。"""
    normalized_removals, normalize_metrics = normalize_step4_removals(llm_removals)
    if sentence_id:
        for removal in normalized_removals:
            if not removal.get("sentence_id"):
                removal["sentence_id"] = sentence_id
    source_text = str(original_text or "")
    removal_text, applied_indexes, removal_metrics = apply_step4_removals_to_text(
        source_text,
        normalized_removals,
    )
    legacy_text = str(llm_cleaned_text or "").strip()

    final_metrics = Counter()
    final_metrics.update(normalize_metrics)
    final_metrics.update(removal_metrics)

    if applied_indexes:
        final_text = removal_text
        final_metrics["selected_removal_patch_path"] += 1
    elif legacy_text:
        final_text = legacy_text
        final_metrics["selected_legacy_cleaned_text_path"] += 1
    else:
        final_text = source_text
        final_metrics["selected_passthrough_path"] += 1

    final_removals = [normalized_removals[idx] for idx in sorted(applied_indexes)]
    final_metrics["final_applied_removals"] += len(final_removals)
    return final_text, final_removals, dict(final_metrics)


def parse_step4_cleaned_sentences(
    result: Dict[str, Any],
    *,
    valid_sentence_ids: Set[str],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    """
    解析 Step4 payload，兼容：
    1) 新格式：{"removals":[...]} 或 {"d":[...]} 或 {"r":[...]}
    2) 旧格式：{"cleaned_sentences":[{"sentence_id","text"}]} 或 {"c":[...]}
    """
    metrics = Counter()
    raw_cleaned_items: List[Dict[str, Any]] = []
    top_level_removals: List[Dict[str, Any]] = []
    if isinstance(result, dict):
        if isinstance(result.get("cleaned_sentences"), list):
            raw_cleaned_items = result.get("cleaned_sentences", [])
            metrics["verbose_shape_hits"] += 1
        elif isinstance(result.get("c"), list):
            raw_cleaned_items = result.get("c", [])
            metrics["compact_shape_hits"] += 1

        if isinstance(result.get("removals"), list):
            top_level_removals.extend(result.get("removals", []))
            metrics["verbose_removal_shape_hits"] += 1
        if isinstance(result.get("d"), list):
            top_level_removals.extend(result.get("d", []))
            metrics["compact_removal_shape_hits"] += 1
        if isinstance(result.get("r"), list):
            top_level_removals.extend(result.get("r", []))
            metrics["compact_removal_shape_hits"] += 1

    cleaned_by_id: Dict[str, Dict[str, Any]] = {}

    def _slot(sentence_id: str) -> Dict[str, Any]:
        sid = str(sentence_id or "").strip()
        if not sid:
            return {}
        if sid not in cleaned_by_id:
            cleaned_by_id[sid] = {
                "sentence_id": sid,
                "cleaned_text": "",
                "removals": [],
            }
        return cleaned_by_id[sid]

    normalized_top_removals, top_metrics = normalize_step4_removals(top_level_removals)
    metrics.update(top_metrics)
    for removal in normalized_top_removals:
        sentence_id = str(removal.get("sentence_id", "")).strip()
        if not sentence_id:
            metrics["dropped_missing_sentence_id"] += 1
            continue
        if sentence_id not in valid_sentence_ids:
            metrics["dropped_invalid_sentence_id"] += 1
            continue
        slot = _slot(sentence_id)
        if not slot:
            metrics["dropped_missing_sentence_id"] += 1
            continue
        slot["removals"].append(removal)

    for item in raw_cleaned_items:
        if not isinstance(item, dict):
            metrics["dropped_invalid_item_type"] += 1
            continue
        sentence_id = str(item.get("sentence_id", item.get("sid", ""))).strip()
        if not sentence_id or sentence_id not in valid_sentence_ids:
            metrics["dropped_invalid_sentence_id"] += 1
            continue
        slot = _slot(sentence_id)
        if not slot:
            metrics["dropped_missing_sentence_id"] += 1
            continue
        if "removed_items" in item or "ri" in item:
            metrics["legacy_removed_items_ignored"] += 1

        nested_removals_raw = item.get("removals", item.get("d", item.get("r", [])))
        if isinstance(nested_removals_raw, list):
            normalized_nested_removals, nested_metrics = normalize_step4_removals(nested_removals_raw)
            metrics.update(nested_metrics)
            for removal in normalized_nested_removals:
                removal_sentence_id = str(removal.get("sentence_id", "")).strip()
                if not removal_sentence_id:
                    removal["sentence_id"] = sentence_id
                    removal_sentence_id = sentence_id
                if removal_sentence_id != sentence_id:
                    metrics["dropped_cross_sentence_removal"] += 1
                    continue
                slot["removals"].append(removal)

        cleaned_text = str(item.get("text", item.get("t", item.get("cleaned_text", item.get("ct", ""))))).strip()
        if cleaned_text:
            if str(slot.get("cleaned_text", "")).strip():
                metrics["dropped_duplicate_sentence_id"] += 1
            else:
                slot["cleaned_text"] = cleaned_text
                metrics["accepted_cleaned_text_items"] += 1
        elif not slot.get("removals"):
            metrics["dropped_empty_text"] += 1

    deduped_by_id: Dict[str, Dict[str, Any]] = {}
    for sentence_id, item in cleaned_by_id.items():
        removals = list(item.get("removals", []))
        deduped_removals: List[Dict[str, str]] = []
        seen_removal_keys: Set[Tuple[str, str, str]] = set()
        for removal in removals:
            key = (
                str(removal.get("original", "")),
                str(removal.get("left_context", "")),
                str(removal.get("right_context", "")),
            )
            if key in seen_removal_keys:
                metrics["dropped_duplicate_removal_item"] += 1
                continue
            seen_removal_keys.add(key)
            deduped_removals.append(removal)
        cleaned_text = str(item.get("cleaned_text", "")).strip()
        if not cleaned_text and not deduped_removals:
            metrics["dropped_empty_sentence_payload"] += 1
            continue
        deduped_by_id[sentence_id] = {
            "sentence_id": sentence_id,
            "cleaned_text": cleaned_text,
            "removals": deduped_removals,
        }
        metrics["accepted_items"] += 1

    metrics["raw_items"] += len(raw_cleaned_items)
    metrics["raw_removal_items"] += len(top_level_removals)
    metrics["sentence_slot_count"] += len(deduped_by_id)
    return deduped_by_id, dict(metrics)


def merge_step4_cleaned_maps(
    cleaned_maps: List[Dict[str, Dict[str, Any]]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    """合并各批次 Step4 清理结果，按首次出现保留，避免跨批次覆盖。"""
    metrics = Counter()
    merged_by_id: Dict[str, Dict[str, Any]] = {}
    for cleaned_map in cleaned_maps:
        if not isinstance(cleaned_map, dict):
            metrics["dropped_invalid_cleaned_map_type"] += 1
            continue
        for sentence_id, cleaned_item in cleaned_map.items():
            normalized_id = str(sentence_id or "").strip()
            if not normalized_id:
                metrics["dropped_empty_sentence_id"] += 1
                continue
            if normalized_id in merged_by_id:
                metrics["dropped_duplicate_sentence_id_across_batches"] += 1
                continue
            if not isinstance(cleaned_item, dict):
                metrics["dropped_invalid_cleaned_item_type"] += 1
                continue
            merged_by_id[normalized_id] = cleaned_item
            metrics["accepted_cleaned_items"] += 1
    return merged_by_id, dict(metrics)


def assemble_step4_cleaned_sentences(
    source_sentences: List[Dict[str, Any]],
    *,
    llm_cleaned_by_id: Dict[str, Dict[str, Any]],
    glossary_guard: Optional[Callable[[str, str], bool]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """基于 LLM 子集输出装配 Step4 最终结果：优先本地删除补丁，未覆盖句自动直通。"""
    metrics = Counter()
    assembled: List[Dict[str, Any]] = []

    ordered_sources = sorted(
        enumerate(source_sentences),
        key=lambda pair: (
            float(pair[1].get("start_sec", 0.0)),
            float(pair[1].get("end_sec", 0.0)),
            pair[0],
        ),
    )
    for _, source in ordered_sources:
        if not isinstance(source, dict):
            metrics["dropped_invalid_source_item_type"] += 1
            continue
        sentence_id = str(source.get("sentence_id", "")).strip()
        if not sentence_id:
            metrics["dropped_missing_sentence_id_in_source"] += 1
            continue
        source_text = str(source.get("text", ""))
        cleaned_item = llm_cleaned_by_id.get(sentence_id)
        if cleaned_item is None:
            metrics["sentence_passthrough_used"] += 1
            cleaned_text = source_text
        else:
            metrics["sentence_llm_covered"] += 1
            cleaned_text, applied_removals, reconcile_metrics = reconcile_step4_item(
                original_text=source_text,
                llm_cleaned_text=str(cleaned_item.get("cleaned_text", cleaned_item.get("text", ""))),
                llm_removals=cleaned_item.get("removals", []),
                sentence_id=sentence_id,
            )
            metrics.update(reconcile_metrics)
            metrics["applied_removal_count"] += len(applied_removals)
            if not cleaned_text:
                metrics["empty_cleaned_text_fallback_used"] += 1
                cleaned_text = source_text
            elif glossary_guard is not None and glossary_guard(source_text, cleaned_text):
                metrics["bilingual_pair_guard_fallback_used"] += 1
                cleaned_text = source_text

        assembled.append(
            {
                "sentence_id": sentence_id,
                "cleaned_text": cleaned_text,
            }
        )

    metrics["source_sentence_count"] += len(source_sentences)
    metrics["output_sentence_count"] += len(assembled)
    return assembled, dict(metrics)


def sentence_id_and_text_pairs(sentences: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """提取句子 ID 与文本，兼容 cleaned_text/text 字段。"""
    pairs: List[Tuple[str, str]] = []
    for sentence in sentences:
        if not isinstance(sentence, dict):
            continue
        sentence_id = str(sentence.get("sentence_id", "")).strip()
        if not sentence_id:
            continue
        text = str(sentence.get("cleaned_text", sentence.get("text", ""))).strip()
        pairs.append((sentence_id, text))
    return pairs


def build_fallback_paragraphs(
    sentence_ids: List[str],
    sentence_text_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    """当 LLM 未给出可用段落时，按句子降级生成段落。"""
    paragraphs: List[Dict[str, Any]] = []
    for sentence_id in sentence_ids:
        text = str(sentence_text_map.get(sentence_id, "")).strip()
        if not text:
            continue
        paragraphs.append(
            {
                "text": text,
                "source_sentence_ids": [sentence_id],
                "merge_type": "未合并",
            }
        )
    return paragraphs


def _normalize_keep_ids(
    raw_keep_ids: Any,
    ordered_batch_ids: List[str],
) -> Tuple[List[str], Dict[str, int]]:
    metrics = Counter()
    batch_id_set = set(ordered_batch_ids)
    keep_ids: List[str] = []
    seen: Set[str] = set()
    if isinstance(raw_keep_ids, list):
        for item in raw_keep_ids:
            sentence_id = str(item or "").strip()
            if not sentence_id:
                metrics["dropped_empty_keep_id"] += 1
                continue
            if sentence_id not in batch_id_set:
                metrics["dropped_outside_batch_keep_id"] += 1
                continue
            if sentence_id in seen:
                metrics["dropped_duplicate_keep_id"] += 1
                continue
            seen.add(sentence_id)
            keep_ids.append(sentence_id)
    if keep_ids:
        metrics["keep_ids_fallback_used"] += 0
        return keep_ids, dict(metrics)
    metrics["keep_ids_fallback_used"] += 1
    return list(ordered_batch_ids), dict(metrics)


def _normalize_paragraphs(
    raw_paragraphs: Any,
    ordered_keep_ids: List[str],
    sentence_text_map: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    metrics = Counter()
    keep_id_set = set(ordered_keep_ids)
    paragraphs: List[Dict[str, Any]] = []
    if isinstance(raw_paragraphs, list):
        for item in raw_paragraphs:
            if not isinstance(item, dict):
                metrics["dropped_invalid_paragraph_item_type"] += 1
                continue
            text = str(item.get("text", item.get("t", ""))).strip()
            source_sentence_ids_raw = item.get("source_sentence_ids", item.get("sids", []))
            source_sentence_ids: List[str] = []
            seen_ids: Set[str] = set()
            if isinstance(source_sentence_ids_raw, list):
                for sentence_id in source_sentence_ids_raw:
                    normalized = str(sentence_id or "").strip()
                    if not normalized or normalized not in keep_id_set or normalized in seen_ids:
                        continue
                    seen_ids.add(normalized)
                    source_sentence_ids.append(normalized)
            if not source_sentence_ids:
                metrics["dropped_paragraph_without_source_ids"] += 1
                continue
            if not text:
                text = " ".join(
                    str(sentence_text_map.get(sentence_id, "")).strip()
                    for sentence_id in source_sentence_ids
                ).strip()
            if not text:
                metrics["dropped_paragraph_without_text"] += 1
                continue
            merge_type = str(item.get("merge_type", item.get("mt", "未合并"))).strip() or "未合并"
            paragraphs.append(
                {
                    "text": text,
                    "source_sentence_ids": source_sentence_ids,
                    "merge_type": merge_type,
                }
            )
            metrics["accepted_paragraphs"] += 1
    if paragraphs:
        return paragraphs, dict(metrics)
    metrics["paragraph_fallback_used"] += 1
    return build_fallback_paragraphs(ordered_keep_ids, sentence_text_map), dict(metrics)


def parse_step56_dedup_merge_payload(
    result: Dict[str, Any],
    *,
    ordered_batch_ids: List[str],
    sentence_text_map: Dict[str, str],
) -> Tuple[List[str], List[Dict[str, Any]], Dict[str, int]]:
    """解析 Step56 keep_sentence_ids + paragraphs，支持轻量短键。"""
    metrics = Counter()
    if not isinstance(result, dict):
        keep_ids = list(ordered_batch_ids)
        paragraphs = build_fallback_paragraphs(keep_ids, sentence_text_map)
        metrics["invalid_payload_type"] += 1
        return keep_ids, paragraphs, dict(metrics)

    if "k" in result or "p" in result:
        metrics["compact_shape_hits"] += 1
    if "keep_sentence_ids" in result or "paragraphs" in result:
        metrics["verbose_shape_hits"] += 1

    keep_ids, keep_metrics = _normalize_keep_ids(
        result.get("keep_sentence_ids", result.get("k")),
        ordered_batch_ids,
    )
    paragraphs, paragraph_metrics = _normalize_paragraphs(
        result.get("paragraphs", result.get("p")),
        keep_ids,
        sentence_text_map,
    )
    metrics.update(keep_metrics)
    metrics.update(paragraph_metrics)
    return keep_ids, paragraphs, dict(metrics)


def deduplicate_paragraphs(paragraphs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """去重：若段落 source_sentence_ids 为已有段落子集则删除。"""
    if not paragraphs:
        return []

    candidates = []
    for index, paragraph in enumerate(paragraphs):
        source_ids = set(paragraph.get("source_sentence_ids", []))
        candidates.append(
            {
                "index": index,
                "ids": source_ids,
                "count": len(source_ids),
                "item": paragraph,
            }
        )
    candidates.sort(key=lambda item: item["count"], reverse=True)

    kept = []
    for candidate in candidates:
        if any(candidate["ids"].issubset(item["ids"]) for item in kept):
            continue
        kept.append(candidate)

    kept.sort(key=lambda item: item["index"])
    return [item["item"] for item in kept]
