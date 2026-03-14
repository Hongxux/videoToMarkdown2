import asyncio
import json
import logging
import os
import time
from collections import Counter, deque
from pathlib import Path
from typing import Any, Awaitable, Callable, Deque, Dict, List, Optional, Tuple

from .checkpoint import STEP_INDEX_MAP, SQLiteCheckpointer
from .nodes import phase2_preprocessing as pp
from .nodes import step1_node
from .state import PipelineState
from .tools import file_validator


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def should_use_streaming_stage1_executor(
    *,
    max_step: int,
    resume: bool,
    resume_state: Optional[Dict[str, Any]],
    resume_from_step: Optional[str],
    enable_checkpoints: bool,
) -> Tuple[bool, str]:
    if not _read_bool_env("TRANSCRIPT_STAGE1_STREAMING_ENABLED", True):
        return False, "disabled by TRANSCRIPT_STAGE1_STREAMING_ENABLED"
    if int(max_step) < 6:
        return False, "requires full Stage1 run"
    if enable_checkpoints:
        return False, "langgraph memory checkpoint mode is enabled"
    if resume:
        return False, "resume mode is enabled"
    if resume_state:
        return False, "resume_state is provided"
    if str(resume_from_step or "").strip():
        return False, "resume_from_step is provided"
    step3_overlap = max(0, min(9, int(pp._read_int_env("TRANSCRIPT_STEP3_WINDOW_OVERLAP", 0))))
    if step3_overlap != 0:
        return False, "TRANSCRIPT_STEP3_WINDOW_OVERLAP must be 0"
    step56_window_size, _ = pp._resolve_step56_window_size()
    step56_overlap, _ = pp._resolve_step56_window_overlap(step56_window_size)
    if step56_overlap != 0:
        return False, "TRANSCRIPT_STEP56_WINDOW_OVERLAP must be 0"
    return True, "enabled"


def _merge_step_result(state: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(state)
    if result.get("token_usage"):
        merged["token_usage"] = {
            **dict(merged.get("token_usage", {})),
            **dict(result.get("token_usage", {})),
        }
    if result.get("step_timings"):
        merged["step_timings"] = {
            **dict(merged.get("step_timings", {})),
            **dict(result.get("step_timings", {})),
        }
    if result.get("step_observability"):
        merged["step_observability"] = {
            **dict(merged.get("step_observability", {})),
            **dict(result.get("step_observability", {})),
        }
    if result.get("llm_calls"):
        merged["llm_calls"] = list(merged.get("llm_calls", [])) + list(result.get("llm_calls", []))
    if result.get("errors"):
        merged["errors"] = list(merged.get("errors", [])) + list(result.get("errors", []))
    for key, value in result.items():
        if key in {"token_usage", "step_timings", "step_observability", "llm_calls", "errors"}:
            continue
        merged[key] = value
    return merged


class _OrderedTaskRunner:
    def __init__(self, max_inflight: int):
        self._max_inflight = 0 if max_inflight <= 0 else int(max_inflight)
        self._queued: Deque[Tuple[int, Callable[[], Awaitable[Any]]]] = deque()
        self._running: Dict[asyncio.Task[Any], int] = {}
        self._ready: Dict[int, Any] = {}
        self._next_index = 0
        self._next_flush_index = 0

    async def submit(self, factory: Callable[[], Awaitable[Any]]) -> int:
        index = self._next_index
        self._next_index += 1
        self._queued.append((index, factory))
        self._start_pending()
        return index

    def _capacity(self) -> int:
        if self._max_inflight <= 0:
            return max(1, len(self._queued) + len(self._running))
        return self._max_inflight

    def _start_pending(self) -> None:
        while self._queued and len(self._running) < self._capacity():
            index, factory = self._queued.popleft()
            task = asyncio.create_task(factory())
            self._running[task] = index

    async def _collect_done(self, *, block: bool) -> bool:
        if not self._running:
            return False
        done, _ = await asyncio.wait(
            list(self._running.keys()),
            timeout=None if block else 0,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if not done:
            return False
        for task in done:
            index = self._running.pop(task)
            self._ready[index] = task.result()
        self._start_pending()
        return True

    async def drain_available(self, on_result: Callable[[int, Any], Awaitable[None]]) -> None:
        while True:
            collected = await self._collect_done(block=False)
            flushed = False
            while self._next_flush_index in self._ready:
                result = self._ready.pop(self._next_flush_index)
                await on_result(self._next_flush_index, result)
                self._next_flush_index += 1
                flushed = True
            if not collected and not flushed:
                return

    async def finish(self, on_result: Callable[[int, Any], Awaitable[None]]) -> None:
        while self._running or self._queued or self._next_flush_index < self._next_index:
            await self.drain_available(on_result)
            if self._next_flush_index >= self._next_index and not self._running and not self._queued:
                return
            if self._running:
                await self._collect_done(block=True)


class StreamingStage1Graph:
    def __init__(
        self,
        *,
        sqlite_checkpointer: Optional[SQLiteCheckpointer] = None,
        output_config: Optional[Any] = None,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
        max_step: int = 6,
    ) -> None:
        self._sqlite_checkpointer = sqlite_checkpointer
        self._output_config = output_config
        self._progress_callback = progress_callback
        self._max_step = int(max_step or 6)

    def _persist_step(self, step_name: str, state: Dict[str, Any]) -> None:
        step_index = STEP_INDEX_MAP.get(step_name, 0)
        if self._sqlite_checkpointer:
            thread_id = str(state.get("_thread_id", "default"))
            self._sqlite_checkpointer.save_checkpoint(thread_id, step_name, step_index, state)
        if self._output_config:
            self._output_config.save_step_output(step_name, state)
        if self._progress_callback:
            try:
                completed = max(0, min(int(step_index), self._max_step))
                pending = max(0, self._max_step - completed)
                self._progress_callback(
                    {
                        "event": "step_completed",
                        "stage": "stage1",
                        "step_name": step_name,
                        "checkpoint": step_name,
                        "completed": completed,
                        "pending": pending,
                        "status": "running",
                        "timestamp_ms": int(time.time() * 1000),
                    }
                )
            except Exception as error:
                logging.getLogger("stage1_pipeline").warning(
                    "Stage1 streaming progress callback failed at %s: %s",
                    step_name,
                    error,
                )

    async def ainvoke(self, initial_state: Dict[str, Any], _config: Dict[str, Any]) -> Dict[str, Any]:
        return await run_stage1_streaming_executor(
            dict(initial_state),
            on_step_completed=self._persist_step,
        )


async def _save_step4_sentence_timestamps(output_dir: str, translated_sentences: List[Dict[str, Any]]) -> None:
    storage = pp.LocalStorage(str(Path(output_dir) / "local_storage"))
    timestamps = {
        str(item.get("sentence_id", "")): {
            "start_sec": item.get("start_sec", 0),
            "end_sec": item.get("end_sec", 0),
        }
        for item in translated_sentences
        if isinstance(item, dict) and str(item.get("sentence_id", "")).strip()
    }
    storage.save_sentence_timestamps(timestamps)
    intermediates_dir = Path(output_dir) / "intermediates"
    intermediates_dir.mkdir(parents=True, exist_ok=True)
    with open(intermediates_dir / "sentence_timestamps.json", "w", encoding="utf-8") as output_stream:
        json.dump(timestamps, output_stream, ensure_ascii=False, indent=2)


def _persist_streaming_step(
    state: Dict[str, Any],
    result: Dict[str, Any],
    *,
    step_name: str,
    on_step_completed: Optional[Callable[[str, Dict[str, Any]], None]],
) -> Dict[str, Any]:
    merged = _merge_step_result(state, result)
    if on_step_completed is not None:
        on_step_completed(step_name, merged)
    return merged


def _finalize_step56_output(
    *,
    cleaned_sentences: List[Dict[str, Any]],
    aggregated_keep_ids: List[str],
    aggregated_paragraphs: List[Dict[str, Any]],
    total_tokens: int,
    duration_ms: float,
    observability: Counter,
) -> Dict[str, Any]:
    ordered_all_pairs = pp._sentence_id_and_text_pairs_impl(cleaned_sentences)
    ordered_all_ids = [sentence_id for sentence_id, _ in ordered_all_pairs]
    sentence_text_map = {sentence_id: text for sentence_id, text in ordered_all_pairs}

    keep_id_set = set(filter(None, aggregated_keep_ids))
    if keep_id_set:
        ordered_keep_ids = [sentence_id for sentence_id in ordered_all_ids if sentence_id in keep_id_set]
    else:
        ordered_keep_ids = list(ordered_all_ids)
        keep_id_set = set(ordered_keep_ids)

    non_redundant_sentences = [
        sentence
        for sentence in cleaned_sentences
        if str(sentence.get("sentence_id", "")).strip() in keep_id_set
    ]
    if not non_redundant_sentences and cleaned_sentences:
        non_redundant_sentences = list(cleaned_sentences)
        ordered_keep_ids = [str(item.get("sentence_id", "")).strip() for item in cleaned_sentences]
        keep_id_set = set(filter(None, ordered_keep_ids))

    normalized_paragraphs: List[Dict[str, Any]] = []
    for paragraph in aggregated_paragraphs:
        if not isinstance(paragraph, dict):
            continue
        source_sentence_ids = paragraph.get("source_sentence_ids", [])
        if not isinstance(source_sentence_ids, list):
            continue
        valid_source_ids: List[str] = []
        seen: set[str] = set()
        for sentence_id in source_sentence_ids:
            normalized = str(sentence_id or "").strip()
            if not normalized or normalized not in keep_id_set or normalized in seen:
                continue
            seen.add(normalized)
            valid_source_ids.append(normalized)
        if not valid_source_ids:
            continue
        text = str(paragraph.get("text", "")).strip()
        if not text:
            text = " ".join(
                str(sentence_text_map.get(sentence_id, "")).strip()
                for sentence_id in valid_source_ids
            ).strip()
        if not text:
            continue
        merge_type = str(paragraph.get("merge_type", "未合并")).strip() or "未合并"
        normalized_paragraphs.append(
            {
                "text": text,
                "source_sentence_ids": valid_source_ids,
                "merge_type": merge_type,
            }
        )

    if not normalized_paragraphs:
        normalized_paragraphs = pp._build_fallback_paragraphs_impl(ordered_keep_ids, sentence_text_map)

    deduplicated_paragraphs = pp._deduplicate_paragraphs_impl(normalized_paragraphs)
    final_paragraphs = [
        {
            "paragraph_id": f"P{index + 1:03d}",
            "text": paragraph["text"],
            "source_sentence_ids": paragraph["source_sentence_ids"],
            "merge_type": paragraph.get("merge_type", "未合并"),
        }
        for index, paragraph in enumerate(deduplicated_paragraphs)
    ]
    return {
        "non_redundant_sentences": non_redundant_sentences,
        "pure_text_script": final_paragraphs,
        "current_step": pp.STEP5_6_NODE_NAME,
        "current_step_status": "completed",
        "token_usage": {pp.STEP5_6_NODE_NAME: int(total_tokens)},
        "step_timings": {pp.STEP5_6_NODE_NAME: float(duration_ms)},
        "step_observability": {pp.STEP5_6_NODE_NAME: dict(observability)},
    }


async def run_stage1_streaming_executor(
    initial_state: Dict[str, Any],
    *,
    on_step_completed: Optional[Callable[[str, Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    state = dict(initial_state)

    step1_result = await step1_node(state)
    state = _merge_step_result(state, step1_result)
    if on_step_completed:
        on_step_completed("step1_validate", state)
    if not state.get("is_valid", False):
        return state

    output_dir = str(state.get("output_dir", "output"))
    subtitles = file_validator.read_subtitle_sample(state["subtitle_path"], count=None)
    schema_strict_mode = pp._read_bool_env("TRANSCRIPT_SCHEMA_STRICT_MODE", False)

    corrected_subtitles: List[Dict[str, Any]] = []
    correction_summary: List[Dict[str, Any]] = []
    cleanup_summary: List[Dict[str, Any]] = []
    merged_sentences: List[Dict[str, Any]] = []
    merged_sentence_order: List[str] = []
    translate_candidates: List[Dict[str, Any]] = []
    translated_sentences: List[Dict[str, Any]] = []
    translated_ready_by_id: Dict[str, Dict[str, Any]] = {}
    cleaned_passthrough_sentences: List[Dict[str, Any]] = []
    step56_keep_ids: set[str] = set()
    step56_paragraphs: List[Dict[str, Any]] = []

    step2_tokens = 0
    step3_tokens = 0
    step35_tokens = 0
    step56_tokens = 0
    next_sentence_counter = 1
    translated_flush_index = 0
    next_step3_submit_start = 0
    next_step35_submit_index = 0
    next_step56_submit_index = 0
    step2_step4_merged_done = True
    step56_enabled = False

    step2_observability = Counter()
    step3_observability = Counter()
    step35_observability = Counter()
    step4_observability = Counter({"compat_passthrough_mode_used": 1})
    step56_observability = Counter()

    step2_started_at = time.perf_counter()
    step3_started_at = time.perf_counter()
    step35_started_at = time.perf_counter()
    step4_started_at = time.perf_counter()
    step56_started_at = time.perf_counter()

    step2_llm = pp.create_llm_client(purpose="refinement")
    step3_llm = pp.create_llm_client(purpose="refinement")
    step35_llm = None
    step56_llm = None

    step3_window_size = 10
    step35_window_size = max(1, pp._read_int_env("TRANSCRIPT_STEP35_WINDOW_SIZE", 50))
    step56_window_size, _ = pp._resolve_step56_window_size()
    step56_max_inflight, _ = pp._resolve_step56_max_inflight(default=24)

    async def _flush_translated_ready() -> None:
        nonlocal translated_flush_index
        nonlocal next_step56_submit_index
        while translated_flush_index < len(merged_sentence_order):
            sentence_id = merged_sentence_order[translated_flush_index]
            translated_sentence = translated_ready_by_id.get(sentence_id)
            if translated_sentence is None:
                break
            translated_sentences.append(translated_sentence)
            cleaned_passthrough_sentences.append(
                {
                    "sentence_id": sentence_id,
                    "cleaned_text": str(translated_sentence.get("text", "")),
                }
            )
            translated_flush_index += 1
        step4_observability["passthrough_sentence_count"] = len(cleaned_passthrough_sentences)
        if not step56_enabled:
            return
        while len(cleaned_passthrough_sentences) - next_step56_submit_index >= step56_window_size:
            batch = list(
                cleaned_passthrough_sentences[
                    next_step56_submit_index : next_step56_submit_index + step56_window_size
                ]
            )
            next_step56_submit_index += len(batch)
            await step56_runner.submit(lambda batch=batch: _process_step56_window(batch))
        await step56_runner.drain_available(_on_step56_result)

    async def _process_step2_batch(
        batch: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], int, Optional[Exception], Dict[str, int]]:
        subtitles_text = "\n".join(f"[{item['subtitle_id']}] {item['text']}" for item in batch)
        prompt = pp.CORRECTION_PROMPT.format(
            domain=state.get("domain", "general"),
            subtitles=subtitles_text,
            context_default=pp.STEP2_CONTEXT_WINDOW_DEFAULT,
            context_max=pp.STEP2_CONTEXT_WINDOW_MAX,
        )
        prompt = f"{prompt}\n\n{pp.STEP2_MERGED_CLEANUP_APPEND_PROMPT}"
        try:
            result, response = await step2_llm.complete_json(
                prompt,
                system_prompt=pp.CORRECTION_SYSTEM_PROMPT,
            )
            parsed_payload, payload_metrics = pp._parse_step2_llm_payload_impl(result)
            batch_metrics = Counter(payload_metrics)
            valid_subtitle_ids = {
                str(item.get("subtitle_id", "")).strip()
                for item in batch
                if isinstance(item, dict)
            }
            cleanup_payload = {
                key: result.get(key)
                for key in ("d", "r", "removals", "cleaned_sentences")
                if isinstance(result, dict) and key in result
            }
            cleanup_by_id, cleanup_metrics = pp._parse_step4_cleaned_sentences_impl(
                cleanup_payload,
                valid_sentence_ids=valid_subtitle_ids,
            )
            for metric_name, metric_value in cleanup_metrics.items():
                batch_metrics[f"cleanup_{metric_name}"] += int(metric_value)
            if schema_strict_mode and payload_metrics.get("legacy_corrected_subtitles_shape_hits", 0) > 0:
                raise ValueError("Step2 strict schema mode rejects legacy corrected_subtitles payload")

            batch_corrected: List[Dict[str, Any]] = []
            batch_corrections: List[Dict[str, Any]] = []
            batch_cleanup_removals: List[Dict[str, Any]] = []
            for subtitle in batch:
                subtitle_id = str(subtitle["subtitle_id"])
                parsed = parsed_payload.get(subtitle_id, {})
                parsed_cleanup = cleanup_by_id.get(subtitle_id, {})
                reconciled_text, reconciled_corrections, reconcile_metrics = pp._reconcile_step2_item_with_metrics(
                    original_text=subtitle["text"],
                    llm_corrected_text=str(parsed.get("corrected_text", "")),
                    llm_corrections=parsed.get("corrections", []),
                    subtitle_id=subtitle_id,
                )
                batch_metrics.update(reconcile_metrics)
                cleaned_text, applied_removals, cleanup_reconcile_metrics = pp._reconcile_step4_item_impl(
                    original_text=reconciled_text,
                    llm_cleaned_text=str(parsed_cleanup.get("cleaned_text", "")),
                    llm_removals=parsed_cleanup.get("removals", []),
                    sentence_id=subtitle_id,
                )
                for metric_name, metric_value in cleanup_reconcile_metrics.items():
                    batch_metrics[f"cleanup_{metric_name}"] += int(metric_value)
                if cleaned_text and pp._drops_cjk_en_glossary_pair(reconciled_text, cleaned_text):
                    batch_metrics["cleanup_bilingual_pair_guard_fallback_used"] += 1
                    cleaned_text = reconciled_text
                if not cleaned_text:
                    batch_metrics["cleanup_empty_text_fallback_used"] += 1
                    cleaned_text = reconciled_text
                batch_corrected.append(
                    {
                        "subtitle_id": subtitle_id,
                        "corrected_text": cleaned_text,
                        "start_sec": subtitle["start_sec"],
                        "end_sec": subtitle["end_sec"],
                    }
                )
                batch_corrections.extend(reconciled_corrections)
                batch_cleanup_removals.extend(applied_removals)
            return (
                batch_corrected,
                batch_corrections,
                batch_cleanup_removals,
                int(response.total_tokens),
                None,
                dict(batch_metrics),
            )
        except Exception as error:
            fallback = [
                {
                    "subtitle_id": item["subtitle_id"],
                    "corrected_text": item["text"],
                    "start_sec": item["start_sec"],
                    "end_sec": item["end_sec"],
                }
                for item in batch
            ]
            return fallback, [], [], 0, error, {"batch_fallback_used": 1}

    async def _process_step3_window(
        batch: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], int, Optional[Exception], Dict[str, int]]:
        ordered_subtitle_ids = [
            str(item.get("subtitle_id", "")).strip()
            for item in batch
            if isinstance(item, dict) and str(item.get("subtitle_id", "")).strip()
        ]
        subtitle_by_id = {subtitle_id: item for subtitle_id, item in zip(ordered_subtitle_ids, batch)}
        subtitle_index_by_id = {subtitle_id: idx for idx, subtitle_id in enumerate(ordered_subtitle_ids)}
        subtitles_text = "\n".join(
            f"[{str(item.get('subtitle_id', '')).strip()}] {str(item.get('corrected_text', item.get('text', '')))}"
            for item in batch
        )
        prompt = pp.MERGE_PROMPT.format(subtitles=subtitles_text)
        try:
            result, response = await step3_llm.complete_json(
                prompt,
                system_prompt=pp.MERGE_SYSTEM_PROMPT,
            )
            parsed_items, parse_metrics = pp._parse_step3_merged_sentences_impl(
                result,
                valid_subtitle_ids=set(ordered_subtitle_ids),
            )
            if schema_strict_mode and parse_metrics.get("compact_shape_hits", 0) > 0:
                raise ValueError("Step3 strict schema mode rejects compact payload shape")
            window_candidates, candidate_metrics = pp._build_step3_window_candidates_impl(
                parsed_items,
                subtitle_index_by_id=subtitle_index_by_id,
                ordered_subtitle_ids=ordered_subtitle_ids,
                subtitle_by_id=subtitle_by_id,
            )
            merged_batch, merged_subtitle_ids, assemble_metrics = pp._assemble_step3_merged_sentences_impl(
                window_candidates,
                ordered_subtitle_ids=ordered_subtitle_ids,
                subtitle_by_id=subtitle_by_id,
            )
            metrics = Counter(parse_metrics)
            metrics.update(candidate_metrics)
            metrics.update(assemble_metrics)
            metrics["merged_subtitle_count"] += len(merged_subtitle_ids)
            return merged_batch, int(response.total_tokens), None, dict(metrics)
        except Exception as error:
            merged_batch, _, assemble_metrics = pp._assemble_step3_merged_sentences_impl(
                [],
                ordered_subtitle_ids=ordered_subtitle_ids,
                subtitle_by_id=subtitle_by_id,
            )
            metrics = Counter(assemble_metrics)
            metrics["window_fallback_used"] += 1
            return merged_batch, 0, error, dict(metrics)

    async def _process_step35_window(
        batch: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], int, Optional[Exception], Dict[str, int]]:
        nonlocal step35_llm
        if step35_llm is None:
            step35_llm = pp.create_llm_client(purpose="refinement")
        sentences_text = "\n".join(f"[{item['sentence_id']}] {item['text']}" for item in batch)
        if sentences_text:
            sentences_text = "\n" + sentences_text
        prompt = pp.TRANSLATION_PROMPT.format(sentences=sentences_text)
        try:
            result, response = await step35_llm.complete_json(
                prompt,
                system_prompt=pp.TRANSLATION_SYSTEM_PROMPT,
            )
            translated_by_id, parse_metrics = pp._parse_step35_translated_sentences_impl(
                result,
                valid_sentence_ids={str(item.get("sentence_id", "")) for item in batch},
            )
            if schema_strict_mode and parse_metrics.get("compact_shape_hits", 0) > 0:
                raise ValueError("Step3.5 strict schema mode rejects compact payload shape")
            translated_batch = [
                {
                    "sentence_id": str(item.get("sentence_id", "")),
                    "text": translated_by_id.get(str(item.get("sentence_id", "")), item.get("text", "")),
                    "start_sec": item.get("start_sec", 0),
                    "end_sec": item.get("end_sec", 0),
                    "source_subtitle_ids": item.get("source_subtitle_ids", []),
                }
                for item in batch
            ]
            return translated_batch, int(response.total_tokens), None, parse_metrics
        except Exception as error:
            fallback_batch = [
                {
                    "sentence_id": str(item.get("sentence_id", "")),
                    "text": item.get("text", ""),
                    "start_sec": item.get("start_sec", 0),
                    "end_sec": item.get("end_sec", 0),
                    "source_subtitle_ids": item.get("source_subtitle_ids", []),
                }
                for item in batch
            ]
            return fallback_batch, 0, error, {"window_fallback_used": 1}

    async def _process_step56_window(
        batch: List[Dict[str, Any]],
    ) -> Tuple[List[str], List[Dict[str, Any]], int, Optional[Exception], Dict[str, int]]:
        nonlocal step56_llm
        if step56_llm is None:
            step56_llm = pp.create_llm_client(purpose="analysis")
        id_text_pairs = pp._sentence_id_and_text_pairs_impl(batch)
        ordered_batch_ids = [sentence_id for sentence_id, _ in id_text_pairs]
        sentence_text_map = {sentence_id: text for sentence_id, text in id_text_pairs}
        prompt = pp.CLEAN_CROSS_PROMPT.format(
            main_topic=str(state.get("main_topic", "")).strip(),
            sentences="\n".join(
                f"[{sentence_id}] {sentence_text_map.get(sentence_id, '')}"
                for sentence_id in ordered_batch_ids
            ),
        )
        try:
            result, response = await step56_llm.complete_json(
                prompt,
                system_prompt=pp.STEP56_DEDUP_MERGE_SYSTEM_PROMPT,
            )
            keep_ids, paragraphs, parse_metrics = pp._parse_step56_dedup_merge_payload_impl(
                result,
                ordered_batch_ids=ordered_batch_ids,
                sentence_text_map=sentence_text_map,
            )
            if schema_strict_mode and parse_metrics.get("compact_shape_hits", 0) > 0:
                raise ValueError("Step56 strict schema mode rejects compact payload shape")
            return keep_ids, paragraphs, int(response.total_tokens), None, parse_metrics
        except Exception as error:
            fallback_ids = list(ordered_batch_ids)
            fallback_paragraphs = pp._build_fallback_paragraphs_impl(fallback_ids, sentence_text_map)
            return fallback_ids, fallback_paragraphs, 0, error, {"window_fallback_used": 1}

    async def _on_step56_result(_index: int, result: Any) -> None:
        nonlocal step56_tokens
        keep_ids, paragraphs, tokens, _error, parse_metrics = result
        step56_tokens += int(tokens)
        step56_keep_ids.update(keep_ids)
        step56_paragraphs.extend(paragraphs)
        step56_observability.update(parse_metrics or {})

    async def _on_step35_result(_index: int, result: Any) -> None:
        nonlocal step35_tokens
        translated_batch, tokens, _error, parse_metrics = result
        step35_tokens += int(tokens)
        step35_observability.update(parse_metrics or {})
        for item in translated_batch:
            translated_ready_by_id[str(item.get("sentence_id", ""))] = item
        await _flush_translated_ready()

    async def _on_step3_result(_index: int, result: Any) -> None:
        nonlocal step3_tokens
        nonlocal next_sentence_counter
        nonlocal next_step35_submit_index
        merged_batch, tokens, _error, parse_metrics = result
        step3_tokens += int(tokens)
        step3_observability.update(parse_metrics or {})
        for item in merged_batch:
            normalized_item = dict(item)
            normalized_item["sentence_id"] = f"S{next_sentence_counter:03d}"
            next_sentence_counter += 1
            sentence_id = str(normalized_item["sentence_id"])
            merged_sentences.append(normalized_item)
            merged_sentence_order.append(sentence_id)
            if pp._contains_cjk(str(normalized_item.get("text", ""))):
                translated_ready_by_id[sentence_id] = normalized_item
                step35_observability["passthrough_chinese_count"] += 1
            else:
                translate_candidates.append(normalized_item)
                step35_observability["translate_candidate_count"] += 1
        while len(translate_candidates) - next_step35_submit_index >= step35_window_size:
            batch = list(
                translate_candidates[
                    next_step35_submit_index : next_step35_submit_index + step35_window_size
                ]
            )
            next_step35_submit_index += len(batch)
            await step35_runner.submit(lambda batch=batch: _process_step35_window(batch))
        await _flush_translated_ready()
        await step35_runner.drain_available(_on_step35_result)

    async def _on_step2_result(_index: int, result: Any) -> None:
        nonlocal step2_tokens
        nonlocal next_step3_submit_start
        nonlocal step2_step4_merged_done
        batch_corrected, batch_corrections, batch_cleanup_removals, tokens, error, batch_metrics = result
        corrected_subtitles.extend(batch_corrected)
        correction_summary.extend(batch_corrections)
        cleanup_summary.extend(batch_cleanup_removals)
        step2_tokens += int(tokens)
        step2_observability.update(batch_metrics or {})
        if error is not None:
            step2_step4_merged_done = False
        while len(corrected_subtitles) - next_step3_submit_start >= step3_window_size:
            batch = list(
                corrected_subtitles[
                    next_step3_submit_start : next_step3_submit_start + step3_window_size
                ]
            )
            next_step3_submit_start += len(batch)
            await step3_runner.submit(lambda batch=batch: _process_step3_window(batch))
        await step3_runner.drain_available(_on_step3_result)

    step2_runner = _OrderedTaskRunner(pp._resolve_step_max_inflight("STEP2"))
    step3_runner = _OrderedTaskRunner(pp._resolve_step_max_inflight("STEP3", default=48))
    step35_runner = _OrderedTaskRunner(pp._resolve_step_max_inflight("STEP35"))
    step56_runner = _OrderedTaskRunner(step56_max_inflight)

    batch_size = max(1, pp._read_int_env("TRANSCRIPT_STEP2_BATCH_SIZE", 20))
    for start in range(0, len(subtitles), batch_size):
        batch = list(subtitles[start : start + batch_size])
        await step2_runner.submit(lambda batch=batch: _process_step2_batch(batch))
    await step2_runner.finish(_on_step2_result)

    subtitle_timestamps = {
        item["subtitle_id"]: {
            "start_sec": item["start_sec"],
            "end_sec": item["end_sec"],
            "text": str(item["corrected_text"])[:50],
        }
        for item in corrected_subtitles
    }
    pp.LocalStorage(str(Path(output_dir) / "local_storage")).save_subtitle_timestamps(subtitle_timestamps)
    step2_output = {
        "corrected_subtitles": corrected_subtitles,
        "correction_summary": correction_summary,
        "cleanup_summary": cleanup_summary,
        "current_step": "step2_correction",
        "current_step_status": "completed",
        "token_usage": {"step2_correction": step2_tokens},
        "step_observability": {"step2_correction": dict(step2_observability)},
        "step_timings": {"step2_correction": (time.perf_counter() - step2_started_at) * 1000},
        pp.STEP2_STEP4_MERGED_STATE_FLAG: step2_step4_merged_done,
    }
    state = _merge_step_result(state, step2_output)
    if on_step_completed:
        on_step_completed("step2_correction", state)

    if step2_step4_merged_done:
        step56_enabled = True
        await _flush_translated_ready()

    if len(corrected_subtitles) > next_step3_submit_start:
        batch = list(corrected_subtitles[next_step3_submit_start:])
        next_step3_submit_start += len(batch)
        await step3_runner.submit(lambda batch=batch: _process_step3_window(batch))
    await step3_runner.finish(_on_step3_result)

    step3_output = {
        "merged_sentences": merged_sentences,
        "current_step": "step3_merge",
        "current_step_status": "completed",
        "token_usage": {"step3_merge": step3_tokens},
        "step_observability": {"step3_merge": dict(step3_observability)},
        "step_timings": {"step3_merge": (time.perf_counter() - step3_started_at) * 1000},
    }
    state = _merge_step_result(state, step3_output)
    if on_step_completed:
        on_step_completed("step3_merge", state)

    if len(translate_candidates) > next_step35_submit_index:
        batch = list(translate_candidates[next_step35_submit_index:])
        next_step35_submit_index += len(batch)
        await step35_runner.submit(lambda batch=batch: _process_step35_window(batch))
    await step35_runner.finish(_on_step35_result)

    step35_output = {
        "translated_sentences": translated_sentences,
        "current_step": "step3_5_translate",
        "current_step_status": "completed",
        "token_usage": {"step3_5_translate": step35_tokens},
        "step_observability": {"step3_5_translate": dict(step35_observability)},
        "step_timings": {"step3_5_translate": (time.perf_counter() - step35_started_at) * 1000},
    }
    state = _merge_step_result(state, step35_output)
    if on_step_completed:
        on_step_completed("step3_5_translate", state)

    if not step2_step4_merged_done:
        state[pp.STEP2_STEP4_MERGED_STATE_FLAG] = False
        step4_result = await pp.step4_node(state)
        state = _merge_step_result(state, step4_result)
        if on_step_completed:
            on_step_completed("step4_clean_local", state)
        step56_result = await pp.step5_6_node(state)
        state = _merge_step_result(state, step56_result)
        if on_step_completed:
            on_step_completed(pp.STEP5_6_NODE_NAME, state)
        return state

    await _save_step4_sentence_timestamps(output_dir, translated_sentences)
    step4_output = {
        "cleaned_sentences": cleaned_passthrough_sentences,
        "current_step": "step4_clean_local",
        "current_step_status": "completed",
        "token_usage": {"step4_clean_local": 0},
        "step_observability": {"step4_clean_local": dict(step4_observability)},
        "step_timings": {"step4_clean_local": (time.perf_counter() - step4_started_at) * 1000},
    }
    state = _merge_step_result(state, step4_output)
    if on_step_completed:
        on_step_completed("step4_clean_local", state)

    if len(cleaned_passthrough_sentences) > next_step56_submit_index:
        batch = list(cleaned_passthrough_sentences[next_step56_submit_index:])
        next_step56_submit_index += len(batch)
        await step56_runner.submit(lambda batch=batch: _process_step56_window(batch))
    await step56_runner.finish(_on_step56_result)

    ordered_all_pairs = pp._sentence_id_and_text_pairs_impl(cleaned_passthrough_sentences)
    ordered_all_ids = [sentence_id for sentence_id, _ in ordered_all_pairs]
    sentence_text_map = {sentence_id: text for sentence_id, text in ordered_all_pairs}
    ordered_keep_ids = (
        [sentence_id for sentence_id in ordered_all_ids if sentence_id in step56_keep_ids]
        if step56_keep_ids
        else list(ordered_all_ids)
    )
    keep_id_set = set(ordered_keep_ids)
    non_redundant_sentences = [
        sentence
        for sentence in cleaned_passthrough_sentences
        if str(sentence.get("sentence_id", "")).strip() in keep_id_set
    ]
    if not non_redundant_sentences and cleaned_passthrough_sentences:
        non_redundant_sentences = list(cleaned_passthrough_sentences)
        ordered_keep_ids = [
            str(item.get("sentence_id", "")).strip()
            for item in cleaned_passthrough_sentences
        ]
        keep_id_set = set(filter(None, ordered_keep_ids))

    normalized_paragraphs: List[Dict[str, Any]] = []
    for paragraph in step56_paragraphs:
        if not isinstance(paragraph, dict):
            continue
        source_sentence_ids = paragraph.get("source_sentence_ids", [])
        if not isinstance(source_sentence_ids, list):
            continue
        valid_source_ids: List[str] = []
        seen: set[str] = set()
        for sentence_id in source_sentence_ids:
            normalized = str(sentence_id or "").strip()
            if not normalized or normalized not in keep_id_set or normalized in seen:
                continue
            seen.add(normalized)
            valid_source_ids.append(normalized)
        if not valid_source_ids:
            continue
        text = str(paragraph.get("text", "")).strip()
        if not text:
            text = " ".join(
                str(sentence_text_map.get(sentence_id, "")).strip()
                for sentence_id in valid_source_ids
            ).strip()
        if not text:
            continue
        normalized_paragraphs.append(
            {
                "text": text,
                "source_sentence_ids": valid_source_ids,
                "merge_type": str(paragraph.get("merge_type", "single")).strip() or "single",
            }
        )
    if not normalized_paragraphs:
        normalized_paragraphs = pp._build_fallback_paragraphs_impl(ordered_keep_ids, sentence_text_map)
    deduplicated_paragraphs = pp._deduplicate_paragraphs_impl(normalized_paragraphs)
    pure_text_script = [
        {
            "paragraph_id": f"P{index + 1:03d}",
            "text": paragraph["text"],
            "source_sentence_ids": paragraph["source_sentence_ids"],
            "merge_type": paragraph.get("merge_type", "single"),
        }
        for index, paragraph in enumerate(deduplicated_paragraphs)
    ]

    step56_output = {
        "non_redundant_sentences": non_redundant_sentences,
        "pure_text_script": pure_text_script,
        "current_step": pp.STEP5_6_NODE_NAME,
        "current_step_status": "completed",
        "token_usage": {pp.STEP5_6_NODE_NAME: step56_tokens},
        "step_observability": {pp.STEP5_6_NODE_NAME: dict(step56_observability)},
        "step_timings": {pp.STEP5_6_NODE_NAME: (time.perf_counter() - step56_started_at) * 1000},
    }
    state = _merge_step_result(state, step56_output)
    if on_step_completed:
        on_step_completed(pp.STEP5_6_NODE_NAME, state)
    return state
