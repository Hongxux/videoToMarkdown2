"""PDF 章节抽取器（MinerU 优先，失败回退到 PyMuPDF）。"""

from __future__ import annotations

import atexit
import logging
import json
import os
import threading
import re
import shutil
import subprocess
import sys
import time
import uuid
from collections import deque
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, List, Optional, Set, Tuple

import requests

from services.python_grpc.src.common.utils.process_pool import create_spawn_process_pool
from services.python_grpc.src.server.watchdog_signal_writer import TaskWatchdogSignalWriter

logger = logging.getLogger(__name__)

_IMAGE_MD_PATTERN = re.compile(r"!\[[^\]]*]\(([^)]+)\)")
_INLINE_FORMULA_PATTERN = re.compile(r"(?<!\$)\$[^$\n]+\$(?!\$)")
_MD_FENCE_PATTERN = re.compile(r"^```(?:markdown|md)?\s*([\s\S]*?)\s*```$", flags=re.IGNORECASE)
_TABLE_LINE_PATTERN = re.compile(r"^\|.+\|$")
_MINERU_MODEL_BOOTSTRAP_LAST_TS = 0.0
_MINERU_PIPELINE_MODEL_SENTINEL = Path("Layout") / "YOLO" / "doclayout_yolo_docstructbench_imgsz1280_2501.pt"

_BOOK_MARKDOWN_FILTER_SYSTEM_PROMPT = "你是一个专业的学术文档和代码校对专家。"
_BOOK_PDF_WATCHDOG_STAGE = "book_pdf_extract"
_BOOK_MARKDOWN_FILTER_USER_PROMPT_TEMPLATE = """以下是通过 OCR 从 PDF 提取的 Markdown 文本，其中可能包含损坏的数学公式（LaTeX 语法错误）或丢失了缩进的代码块。
请在【完全不改变原文语义、不删减内容】的前提下，修复以下问题：
1. 修复不闭合或语法错误的 LaTeX 公式（如 $\\frac{{a}}{{b}} 错写成 $\\frac{{a b}}）。
2. 根据上下文代码逻辑（特别是如果是 Python/Java/C++），重新格式化并补全代码块的正确缩进。
3. 修复明显的 OCR 错别字（如将 "1" 错认为 "l"，"0" 错认为 "O"）。
原文本：
{mineru_markdown_output}
请直接输出修复后的 Markdown 文本："""


@dataclass
class ExtractBookPdfResult:
    success: bool
    markdown: str = ""
    markdown_path: str = ""
    extractor: str = "unknown"
    image_count: int = 0
    table_count: int = 0
    code_block_count: int = 0
    formula_block_count: int = 0
    error_msg: str = ""
    image_paths: List[str] = field(default_factory=list)


@dataclass
class _CodeBBoxRegion:
    page_idx: int
    x0: float
    y0: float
    x1: float
    y1: float


@dataclass
class _MineruSliceExtractResult:
    success: bool
    start_page: int
    end_page: int
    markdown: str = ""
    image_paths: List[str] = field(default_factory=list)
    error_msg: str = ""


@dataclass
class _BookPdfExtractProgressReporter:
    task_id: str
    section_id: str
    start_page: int
    end_page: int
    total_pages: int
    writer: Optional[TaskWatchdogSignalWriter] = None
    _completed_pages: Set[int] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def emit_queue_ready(self, extractor: str) -> None:
        self._emit_running(
            checkpoint="book_pdf_pages_queued",
            extractor=extractor,
            extra={
                "section_id": self.section_id,
                "page_start": self.start_page,
                "page_end": self.end_page,
            },
        )

    def emit_fallback(self, reason: str) -> None:
        self._emit_running(
            checkpoint="fallback_to_pymupdf",
            extractor="pymupdf",
            extra={
                "section_id": self.section_id,
                "reason": str(reason or "").strip()[:200],
            },
        )

    def record_completed_pages(self, start_page: int, end_page: int, extractor: str) -> None:
        normalized_start = max(1, int(start_page or 1))
        normalized_end = max(normalized_start, int(end_page or normalized_start))
        with self._lock:
            changed = False
            for page_no in range(normalized_start, normalized_end + 1):
                if page_no in self._completed_pages:
                    continue
                self._completed_pages.add(page_no)
                changed = True
            if not changed:
                return
            completed = len(self._completed_pages)
        checkpoint = (
            f"page_{normalized_start:04d}_done"
            if normalized_start == normalized_end
            else f"pages_{normalized_start:04d}_{normalized_end:04d}_done"
        )
        self._emit_running(
            checkpoint=checkpoint,
            extractor=extractor,
            completed=completed,
            extra={
                "section_id": self.section_id,
                "page_start": normalized_start,
                "page_end": normalized_end,
            },
        )

    def emit_completed(self, extractor: str) -> None:
        completed = self._completed_count()
        self._emit(
            status="completed",
            checkpoint="book_pdf_extract_completed",
            completed=max(completed, self.total_pages),
            extractor=extractor,
            extra={
                "section_id": self.section_id,
                "page_start": self.start_page,
                "page_end": self.end_page,
            },
        )

    def emit_failed(self, checkpoint: str, error_msg: str, extractor: str) -> None:
        self._emit(
            status="failed",
            checkpoint=checkpoint,
            completed=self._completed_count(),
            extractor=extractor,
            extra={
                "section_id": self.section_id,
                "error": str(error_msg or "").strip()[:300],
            },
        )

    def _completed_count(self) -> int:
        with self._lock:
            return len(self._completed_pages)

    def _emit_running(
        self,
        *,
        checkpoint: str,
        extractor: str,
        completed: Optional[int] = None,
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        self._emit(
            status="running",
            checkpoint=checkpoint,
            completed=self._completed_count() if completed is None else completed,
            extractor=extractor,
            extra=extra,
        )

    def _emit(
        self,
        *,
        status: str,
        checkpoint: str,
        completed: int,
        extractor: str,
        extra: Optional[Dict[str, object]] = None,
    ) -> None:
        if self.writer is None:
            return
        safe_completed = max(0, min(int(completed), self.total_pages))
        safe_pending = max(0, self.total_pages - safe_completed)
        payload: Dict[str, object] = {
            "extractor": str(extractor or "").strip() or "unknown",
        }
        if isinstance(extra, dict):
            payload.update(extra)
        self.writer.emit(
            status=status,
            checkpoint=checkpoint,
            completed=safe_completed,
            pending=safe_pending,
            signal_type="hard",
            extra=payload,
        )


@dataclass
class _MineruPageTaskPayload:
    task_id: str
    mineru_bin: str
    use_mineru_cli: bool
    mineru_env: dict
    sliced_pdf_path: str
    output_dir: str
    output_root: str
    image_dir: str
    section_id: str
    start_page: int
    end_page: int
    timeout_seconds: int


@dataclass
class _MineruSharedJob:
    job_id: str
    task_id: str
    section_id: str
    total_tasks: int
    pending_payloads: Deque[_MineruPageTaskPayload]
    total_pages: int = 0
    progress_reporter: Optional[_BookPdfExtractProgressReporter] = None
    done_event: threading.Event = field(default_factory=threading.Event)
    results: List[_MineruSliceExtractResult] = field(default_factory=list)
    error_msg: str = ""
    inflight_count: int = 0
    queued: bool = False
    submitted_count: int = 0
    completed_count: int = 0


_BOOK_PDF_MINERU_POOL_LOCK = threading.Lock()
_BOOK_PDF_MINERU_POOL: Optional[ProcessPoolExecutor] = None
_BOOK_PDF_MINERU_POOL_WORKERS = 0
_BOOK_PDF_MINERU_SCHEDULER_COND = threading.Condition()
_BOOK_PDF_MINERU_PENDING_JOBS: Deque[_MineruSharedJob] = deque()
_BOOK_PDF_MINERU_ACTIVE_FUTURES: Dict[Future, Tuple[_MineruSharedJob, _MineruPageTaskPayload]] = {}
_BOOK_PDF_MINERU_COMPLETED_FUTURES: Deque[Future] = deque()
_BOOK_PDF_MINERU_DISPATCHER: Optional[threading.Thread] = None
_BOOK_PDF_MINERU_STOP = False


def extract_book_pdf_markdown(
    task_id: str,
    pdf_path: str,
    output_dir: str,
    start_page: int,
    end_page: int,
    image_dir: str,
    output_root: str,
    section_id: str = "",
    prefer_mineru: bool = True,
    timeout_seconds: int = 300,
) -> ExtractBookPdfResult:
    """按页码范围抽取 PDF，并输出可直接拼接进 Markdown 的正文块。"""
    normalized_pdf_path = str(pdf_path or "").strip()
    if not normalized_pdf_path:
        return ExtractBookPdfResult(success=False, error_msg="pdf_path is empty")
    pdf_file = Path(normalized_pdf_path)
    if not pdf_file.is_file():
        return ExtractBookPdfResult(success=False, error_msg=f"pdf not found: {pdf_file}")

    output_dir_path = Path(output_dir or "").expanduser().resolve()
    output_root_path = Path(output_root or output_dir or "").expanduser().resolve()
    image_dir_path = Path(image_dir or output_dir_path / "assets" / "book_images").expanduser().resolve()
    output_dir_path.mkdir(parents=True, exist_ok=True)
    output_root_path.mkdir(parents=True, exist_ok=True)
    image_dir_path.mkdir(parents=True, exist_ok=True)

    page_start, page_end, page_error = _normalize_page_range(pdf_file, start_page, end_page)
    if page_error:
        return ExtractBookPdfResult(success=False, error_msg=page_error)
    total_pages = max(1, page_end - page_start + 1)
    safe_section_id = str(section_id or "").strip() or "section"
    progress_reporter = _BookPdfExtractProgressReporter(
        task_id=str(task_id or "").strip() or "book_pdf_extract",
        section_id=safe_section_id,
        start_page=page_start,
        end_page=page_end,
        total_pages=total_pages,
        writer=TaskWatchdogSignalWriter(
            task_id=str(task_id or "").strip() or "book_pdf_extract",
            output_dir=str(output_dir_path),
            stage=_BOOK_PDF_WATCHDOG_STAGE,
            total_steps=total_pages,
        ),
    )
    preferred_extractor = "mineru" if prefer_mineru else "pymupdf"
    progress_reporter.emit_queue_ready(preferred_extractor)

    slice_root = output_dir_path / "intermediates" / "book_pdf_slices"
    slice_root.mkdir(parents=True, exist_ok=True)
    slice_dir = _ensure_unique_dir(
        slice_root / f"{_safe_token(section_id or 'section')}-p{page_start:04d}-{page_end:04d}"
    )
    sliced_pdf_path = slice_dir / "sliced.pdf"
    try:
        _slice_pdf(pdf_file, sliced_pdf_path, page_start, page_end)
    except Exception as error:
        logger.warning(
            "[%s] slice pdf failed, pdf=%s, pages=%s-%s, err=%s",
            task_id,
            pdf_file,
            page_start,
            page_end,
            error,
        )
        progress_reporter.emit_failed("slice_pdf_failed", str(error), preferred_extractor)
        return ExtractBookPdfResult(success=False, error_msg=f"slice pdf failed: {error}")

    if prefer_mineru:
        mineru_result = _extract_with_mineru(
            task_id=task_id,
            sliced_pdf_path=sliced_pdf_path,
            output_dir=output_dir_path,
            output_root=output_root_path,
            image_dir=image_dir_path,
            section_id=section_id,
            start_page=page_start,
            end_page=page_end,
            timeout_seconds=timeout_seconds,
            progress_reporter=progress_reporter,
        )
        if mineru_result.success:
            progress_reporter.emit_completed("mineru")
            return mineru_result
        logger.info(
            "[%s] mineru extraction unavailable, fallback to pymupdf, reason=%s",
            task_id,
            mineru_result.error_msg,
        )
        progress_reporter.emit_fallback(mineru_result.error_msg)

    try:
        pymupdf_result = _extract_with_pymupdf(
            task_id=task_id,
            sliced_pdf_path=sliced_pdf_path,
            output_dir=output_dir_path,
            output_root=output_root_path,
            image_dir=image_dir_path,
            section_id=section_id,
            start_page=page_start,
            end_page=page_end,
            progress_reporter=progress_reporter,
        )
    except Exception as error:
        progress_reporter.emit_failed("pymupdf_extract_failed", str(error), "pymupdf")
        raise
    if pymupdf_result.success:
        progress_reporter.emit_completed("pymupdf")
    else:
        progress_reporter.emit_failed(
            "pymupdf_extract_failed",
            pymupdf_result.error_msg,
            "pymupdf",
        )
    return pymupdf_result


def _normalize_page_range(pdf_file: Path, start_page: int, end_page: int) -> Tuple[int, int, str]:
    import fitz

    with fitz.open(pdf_file) as document:
        total_pages = int(document.page_count)
    if total_pages <= 0:
        return 0, 0, "pdf has no pages"

    normalized_start = max(1, int(start_page or 1))
    normalized_end = max(1, int(end_page or normalized_start))
    if normalized_start > total_pages:
        return 0, 0, f"start_page out of range: {normalized_start} > {total_pages}"
    normalized_end = min(total_pages, max(normalized_start, normalized_end))
    return normalized_start, normalized_end, ""


def _slice_pdf(pdf_file: Path, sliced_pdf_path: Path, start_page: int, end_page: int) -> None:
    import fitz

    src_doc = fitz.open(pdf_file)
    try:
        sliced_doc = fitz.open()
        try:
            sliced_doc.insert_pdf(src_doc, from_page=start_page - 1, to_page=end_page - 1)
            sliced_doc.save(sliced_pdf_path)
        finally:
            sliced_doc.close()
    finally:
        src_doc.close()


def _extract_with_mineru(
    task_id: str,
    sliced_pdf_path: Path,
    output_dir: Path,
    output_root: Path,
    image_dir: Path,
    section_id: str,
    start_page: int,
    end_page: int,
    timeout_seconds: int,
    progress_reporter: Optional[_BookPdfExtractProgressReporter] = None,
) -> ExtractBookPdfResult:
    mineru_bin = _discover_mineru_cli()
    if not mineru_bin:
        return ExtractBookPdfResult(success=False, error_msg="mineru cli not found")

    use_mineru_cli = _is_mineru_cli_binary(mineru_bin)
    if use_mineru_cli:
        _ensure_mineru_pipeline_models(task_id=task_id)

    parallel_enabled = _read_bool_env("BOOK_PDF_MINERU_PAGE_PARALLEL_ENABLED", True)
    mineru_env = os.environ.copy() if use_mineru_cli else _build_mineru_runtime_env(output_dir)
    try:
        page_tasks = _build_mineru_page_tasks(
            sliced_pdf_path=sliced_pdf_path,
            output_dir=output_dir,
            section_id=section_id,
            start_page=start_page,
            end_page=end_page,
            parallel_enabled=parallel_enabled,
        )
    except Exception as error:
        return ExtractBookPdfResult(success=False, error_msg=f"build mineru page tasks failed: {error}")
    if not page_tasks:
        return ExtractBookPdfResult(success=False, error_msg="mineru page task list is empty")

    worker_count = _resolve_mineru_shared_pool_workers() if parallel_enabled else 1

    logger.info(
        "[%s] mineru extraction dispatch, pages=%s-%s, tasks=%s, workers=%s, parallel=%s, mode=%s",
        task_id,
        start_page,
        end_page,
        len(page_tasks),
        worker_count,
        parallel_enabled,
        "shared-page-pool" if parallel_enabled else "serial",
    )

    slice_results: List[_MineruSliceExtractResult] = []
    if not parallel_enabled:
        for page_start, page_end, task_pdf_path in page_tasks:
            result = _extract_mineru_page_task(
                task_id=task_id,
                mineru_bin=mineru_bin,
                use_mineru_cli=use_mineru_cli,
                mineru_env=mineru_env,
                sliced_pdf_path=str(task_pdf_path),
                output_dir=str(output_dir),
                output_root=str(output_root),
                image_dir=str(image_dir),
                section_id=section_id,
                start_page=page_start,
                end_page=page_end,
                timeout_seconds=timeout_seconds,
            )
            if not result.success:
                return ExtractBookPdfResult(
                    success=False,
                    error_msg=f"mineru task failed pages={page_start}-{page_end}: {result.error_msg}",
                )
            slice_results.append(result)
            if progress_reporter is not None:
                progress_reporter.record_completed_pages(
                    start_page=result.start_page,
                    end_page=result.end_page,
                    extractor="mineru",
                )
    else:
        try:
            slice_results, shared_error = _run_mineru_page_tasks_with_shared_pool(
                task_id=task_id,
                mineru_bin=mineru_bin,
                use_mineru_cli=use_mineru_cli,
                mineru_env=mineru_env,
                output_dir=output_dir,
                output_root=output_root,
                image_dir=image_dir,
                section_id=section_id,
                timeout_seconds=timeout_seconds,
                page_tasks=page_tasks,
                progress_reporter=progress_reporter,
            )
        except Exception as error:
            return ExtractBookPdfResult(success=False, error_msg=f"mineru process pool failed: {error}")
        if shared_error:
            return ExtractBookPdfResult(success=False, error_msg=shared_error)

    slice_results = sorted(slice_results, key=lambda item: (item.start_page, item.end_page))
    merged_markdown_parts = [str(item.markdown or "") for item in slice_results if str(item.markdown or "").strip()]
    merged_markdown = "\n\n".join(part.rstrip("\n") for part in merged_markdown_parts).strip()
    if not merged_markdown:
        return ExtractBookPdfResult(success=False, error_msg="mineru extraction produced empty markdown")

    merged_images: List[str] = []
    for item in slice_results:
        for path in item.image_paths:
            if path not in merged_images:
                merged_images.append(path)

    refined_markdown = _maybe_refine_markdown_with_llm(task_id=task_id, markdown=merged_markdown)
    markdown_target = _write_markdown_output(
        output_dir=output_dir,
        section_id=section_id,
        start_page=start_page,
        end_page=end_page,
        markdown=refined_markdown,
        extractor="mineru",
    )
    stats = _collect_markdown_stats(refined_markdown)
    logger.info(
        "[%s] mineru extraction success, pages=%s-%s, tasks=%s, workers=%s, images=%s, tables=%s, code=%s, formula=%s",
        task_id,
        start_page,
        end_page,
        len(page_tasks),
        worker_count,
        stats[0],
        stats[1],
        stats[2],
        stats[3],
    )
    return ExtractBookPdfResult(
        success=True,
        markdown=refined_markdown,
        markdown_path=str(markdown_target),
        extractor="mineru",
        image_count=stats[0],
        table_count=stats[1],
        code_block_count=stats[2],
        formula_block_count=stats[3],
        image_paths=merged_images,
    )


def _resolve_mineru_shared_pool_workers() -> int:
    hard_cap = max(1, _read_int_env("BOOK_PDF_MINERU_WORKER_MAX", 8))
    configured_workers = _read_int_env("BOOK_PDF_MINERU_WORKERS", 0)
    if configured_workers > 0:
        return max(1, min(hard_cap, configured_workers))
    default_workers = _read_int_env("BOOK_PDF_MINERU_DEFAULT_WORKERS", 4)
    return max(1, min(hard_cap, default_workers))


def _shutdown_book_pdf_mineru_pool() -> None:
    global _BOOK_PDF_MINERU_POOL
    global _BOOK_PDF_MINERU_POOL_WORKERS
    global _BOOK_PDF_MINERU_DISPATCHER
    global _BOOK_PDF_MINERU_STOP

    with _BOOK_PDF_MINERU_SCHEDULER_COND:
        _BOOK_PDF_MINERU_STOP = True
        _BOOK_PDF_MINERU_SCHEDULER_COND.notify_all()
    dispatcher = _BOOK_PDF_MINERU_DISPATCHER
    if dispatcher is not None and dispatcher.is_alive():
        dispatcher.join(timeout=2.0)
    _BOOK_PDF_MINERU_DISPATCHER = None
    with _BOOK_PDF_MINERU_SCHEDULER_COND:
        _BOOK_PDF_MINERU_PENDING_JOBS.clear()
        _BOOK_PDF_MINERU_ACTIVE_FUTURES.clear()
        _BOOK_PDF_MINERU_COMPLETED_FUTURES.clear()
    with _BOOK_PDF_MINERU_POOL_LOCK:
        if _BOOK_PDF_MINERU_POOL is not None:
            try:
                _BOOK_PDF_MINERU_POOL.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            _BOOK_PDF_MINERU_POOL = None
            _BOOK_PDF_MINERU_POOL_WORKERS = 0


atexit.register(_shutdown_book_pdf_mineru_pool)


def _get_book_pdf_mineru_pool(worker_count: int) -> ProcessPoolExecutor:
    global _BOOK_PDF_MINERU_POOL
    global _BOOK_PDF_MINERU_POOL_WORKERS

    safe_workers = max(1, int(worker_count))
    with _BOOK_PDF_MINERU_POOL_LOCK:
        if (
            _BOOK_PDF_MINERU_POOL is None
            or _BOOK_PDF_MINERU_POOL_WORKERS != safe_workers
        ):
            if _BOOK_PDF_MINERU_POOL is not None:
                try:
                    _BOOK_PDF_MINERU_POOL.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
            _BOOK_PDF_MINERU_POOL = create_spawn_process_pool(max_workers=safe_workers)
            _BOOK_PDF_MINERU_POOL_WORKERS = safe_workers
    return _BOOK_PDF_MINERU_POOL


def _ensure_book_pdf_mineru_dispatcher() -> None:
    global _BOOK_PDF_MINERU_DISPATCHER
    global _BOOK_PDF_MINERU_STOP

    with _BOOK_PDF_MINERU_SCHEDULER_COND:
        if _BOOK_PDF_MINERU_DISPATCHER is not None and _BOOK_PDF_MINERU_DISPATCHER.is_alive():
            return
        _BOOK_PDF_MINERU_STOP = False
        _BOOK_PDF_MINERU_DISPATCHER = threading.Thread(
            target=_book_pdf_mineru_dispatch_loop,
            name="book-pdf-mineru-dispatcher",
            daemon=True,
        )
        _BOOK_PDF_MINERU_DISPATCHER.start()


def _book_pdf_mineru_future_done(future: Future) -> None:
    with _BOOK_PDF_MINERU_SCHEDULER_COND:
        _BOOK_PDF_MINERU_COMPLETED_FUTURES.append(future)
        _BOOK_PDF_MINERU_SCHEDULER_COND.notify_all()


def _fail_book_pdf_mineru_jobs_locked(error_msg: str) -> None:
    normalized_error = str(error_msg or "").strip() or "mineru shared dispatcher failed"
    seen_job_ids = set()
    jobs: List[_MineruSharedJob] = []
    for job in list(_BOOK_PDF_MINERU_PENDING_JOBS):
        if job.job_id in seen_job_ids:
            continue
        seen_job_ids.add(job.job_id)
        jobs.append(job)
    for job, _payload in list(_BOOK_PDF_MINERU_ACTIVE_FUTURES.values()):
        if job.job_id in seen_job_ids:
            continue
        seen_job_ids.add(job.job_id)
        jobs.append(job)
    for job in jobs:
        if not job.error_msg:
            job.error_msg = normalized_error
        job.pending_payloads.clear()
        job.queued = False
        if job.inflight_count <= 0 and not job.done_event.is_set():
            job.done_event.set()
    _BOOK_PDF_MINERU_PENDING_JOBS.clear()


def _dispatch_book_pdf_mineru_jobs_locked() -> None:
    if not _BOOK_PDF_MINERU_PENDING_JOBS:
        return
    worker_count = _resolve_mineru_shared_pool_workers()
    try:
        pool = _get_book_pdf_mineru_pool(worker_count)
    except Exception as error:
        _fail_book_pdf_mineru_jobs_locked(f"mineru shared pool init failed: {error}")
        return
    available_slots = max(0, worker_count - len(_BOOK_PDF_MINERU_ACTIVE_FUTURES))
    while available_slots > 0 and _BOOK_PDF_MINERU_PENDING_JOBS:
        job = _BOOK_PDF_MINERU_PENDING_JOBS.popleft()
        job.queued = False
        if job.done_event.is_set() or job.error_msg or not job.pending_payloads:
            if not job.pending_payloads and job.inflight_count <= 0 and not job.done_event.is_set():
                job.done_event.set()
            continue
        payload = job.pending_payloads.popleft()
        try:
            future = pool.submit(
                _extract_mineru_page_task,
                payload.task_id,
                payload.mineru_bin,
                payload.use_mineru_cli,
                payload.mineru_env,
                payload.sliced_pdf_path,
                payload.output_dir,
                payload.output_root,
                payload.image_dir,
                payload.section_id,
                payload.start_page,
                payload.end_page,
                payload.timeout_seconds,
            )
        except Exception as error:
            job.error_msg = f"mineru process submit failed pages={payload.start_page}-{payload.end_page}: {error}"
            job.pending_payloads.clear()
            if job.inflight_count <= 0:
                job.done_event.set()
            continue
        job.inflight_count += 1
        job.submitted_count += 1
        _BOOK_PDF_MINERU_ACTIVE_FUTURES[future] = (job, payload)
        future.add_done_callback(_book_pdf_mineru_future_done)
        if job.pending_payloads and not job.queued:
            _BOOK_PDF_MINERU_PENDING_JOBS.append(job)
            job.queued = True
        available_slots -= 1


def _drain_book_pdf_mineru_completed_locked() -> None:
    while _BOOK_PDF_MINERU_COMPLETED_FUTURES:
        future = _BOOK_PDF_MINERU_COMPLETED_FUTURES.popleft()
        meta = _BOOK_PDF_MINERU_ACTIVE_FUTURES.pop(future, None)
        if meta is None:
            continue
        job, payload = meta
        job.inflight_count = max(0, job.inflight_count - 1)
        try:
            result = future.result()
        except Exception as error:
            if not job.error_msg:
                job.error_msg = f"mineru process failed pages={payload.start_page}-{payload.end_page}: {error}"
        else:
            if not result.success:
                if not job.error_msg:
                    job.error_msg = (
                        f"mineru task failed pages={payload.start_page}-{payload.end_page}: "
                        f"{result.error_msg}"
                    )
            else:
                job.results.append(result)
                job.completed_count += 1
                if job.progress_reporter is not None:
                    job.progress_reporter.record_completed_pages(
                        start_page=result.start_page,
                        end_page=result.end_page,
                        extractor="mineru",
                    )
        if job.error_msg:
            job.pending_payloads.clear()
        if not job.pending_payloads and job.inflight_count <= 0 and not job.done_event.is_set():
            job.done_event.set()


def _book_pdf_mineru_dispatch_loop() -> None:
    while True:
        try:
            with _BOOK_PDF_MINERU_SCHEDULER_COND:
                _drain_book_pdf_mineru_completed_locked()
                _dispatch_book_pdf_mineru_jobs_locked()
                should_stop = (
                    _BOOK_PDF_MINERU_STOP
                    and not _BOOK_PDF_MINERU_PENDING_JOBS
                    and not _BOOK_PDF_MINERU_ACTIVE_FUTURES
                    and not _BOOK_PDF_MINERU_COMPLETED_FUTURES
                )
                if should_stop:
                    return
                _BOOK_PDF_MINERU_SCHEDULER_COND.wait(timeout=0.5)
        except Exception as error:
            logger.exception("book pdf mineru dispatcher loop failed: %s", error)
            with _BOOK_PDF_MINERU_SCHEDULER_COND:
                _fail_book_pdf_mineru_jobs_locked(f"mineru shared dispatcher failed: {error}")
                _BOOK_PDF_MINERU_SCHEDULER_COND.wait(timeout=0.5)


def _run_mineru_page_tasks_with_shared_pool(
    task_id: str,
    mineru_bin: str,
    use_mineru_cli: bool,
    mineru_env: dict,
    output_dir: Path,
    output_root: Path,
    image_dir: Path,
    section_id: str,
    timeout_seconds: int,
    page_tasks: List[Tuple[int, int, Path]],
    progress_reporter: Optional[_BookPdfExtractProgressReporter] = None,
) -> Tuple[List[_MineruSliceExtractResult], str]:
    if not page_tasks:
        return [], "mineru page task list is empty"

    _ensure_book_pdf_mineru_dispatcher()
    safe_section_id = str(section_id or "").strip() or "section"
    # 使用无 maxlen 的 deque 作为进程内无界消息队列，按页保存待提取 PDF 任务。
    payloads = deque(
        _MineruPageTaskPayload(
            task_id=task_id,
            mineru_bin=mineru_bin,
            use_mineru_cli=use_mineru_cli,
            mineru_env=mineru_env,
            sliced_pdf_path=str(task_pdf_path),
            output_dir=str(output_dir),
            output_root=str(output_root),
            image_dir=str(image_dir),
            section_id=safe_section_id,
            start_page=page_start,
            end_page=page_end,
            timeout_seconds=timeout_seconds,
        )
        for page_start, page_end, task_pdf_path in page_tasks
    )
    job = _MineruSharedJob(
        job_id=uuid.uuid4().hex,
        task_id=task_id,
        section_id=safe_section_id,
        total_tasks=len(page_tasks),
        pending_payloads=payloads,
        total_pages=sum(max(1, page_end - page_start + 1) for page_start, page_end, _task_pdf_path in page_tasks),
        progress_reporter=progress_reporter,
    )
    logger.info(
        "[%s] mineru shared pool queued, section=%s, pages=%s-%s, pageTasks=%s",
        task_id,
        safe_section_id,
        page_tasks[0][0],
        page_tasks[-1][1],
        len(page_tasks),
    )
    with _BOOK_PDF_MINERU_SCHEDULER_COND:
        _BOOK_PDF_MINERU_PENDING_JOBS.append(job)
        job.queued = True
        _BOOK_PDF_MINERU_SCHEDULER_COND.notify_all()
    job.done_event.wait()
    if job.error_msg:
        logger.warning(
            "[%s] mineru shared pool failed, section=%s, completed=%s/%s, err=%s",
            task_id,
            safe_section_id,
            job.completed_count,
            job.total_tasks,
            job.error_msg,
        )
        return [], job.error_msg
    logger.info(
        "[%s] mineru shared pool finished, section=%s, completed=%s/%s",
        task_id,
        safe_section_id,
        job.completed_count,
        job.total_tasks,
    )
    return sorted(job.results, key=lambda item: (item.start_page, item.end_page)), ""


def _build_mineru_page_tasks(
    sliced_pdf_path: Path,
    output_dir: Path,
    section_id: str,
    start_page: int,
    end_page: int,
    parallel_enabled: bool = True,
) -> List[Tuple[int, int, Path]]:
    if start_page > end_page:
        return []

    total_pages = end_page - start_page + 1
    if (not parallel_enabled) or total_pages <= 1:
        return [(start_page, end_page, sliced_pdf_path)]

    configured_batch_size = str(os.environ.get("BOOK_PDF_MINERU_PAGE_BATCH_SIZE", "") or "").strip()
    if configured_batch_size:
        page_batch_size = max(1, _read_int_env("BOOK_PDF_MINERU_PAGE_BATCH_SIZE", 1))
        if page_batch_size >= total_pages:
            return [(start_page, end_page, sliced_pdf_path)]
    else:
        page_batch_size = 1

    page_ranges = []
    cursor = start_page
    while cursor <= end_page:
        segment_end = min(end_page, cursor + page_batch_size - 1)
        page_ranges.append((cursor, segment_end))
        cursor = segment_end + 1
    if len(page_ranges) <= 1:
        return [(start_page, end_page, sliced_pdf_path)]

    # 按页段切片，确保每个 MinerU 子进程只处理独立 PDF，避免共享 IO 状态导致互相干扰。
    # 默认按单页任务切片，让进程池从页任务队列中领取工作，避免“等分后某一段拖慢全局”。
    task_root = output_dir / "intermediates" / "book_mineru_page_slices"
    task_root.mkdir(parents=True, exist_ok=True)
    task_dir = _ensure_unique_dir(
        task_root / f"{_safe_token(section_id or 'section')}-p{start_page:04d}-{end_page:04d}"
    )

    tasks: List[Tuple[int, int, Path]] = []
    for segment_start, segment_end in page_ranges:
        local_start = segment_start - start_page + 1
        local_end = segment_end - start_page + 1
        segment_path = task_dir / f"slice-p{segment_start:04d}-{segment_end:04d}.pdf"
        _slice_pdf(
            pdf_file=sliced_pdf_path,
            sliced_pdf_path=segment_path,
            start_page=local_start,
            end_page=local_end,
        )
        tasks.append((segment_start, segment_end, segment_path))
    return tasks


def _decide_mineru_parallel_workers(task_count: int) -> int:
    if task_count <= 1:
        return 1

    hard_cap = max(1, _read_int_env("BOOK_PDF_MINERU_WORKER_MAX", 8))
    worker_min = max(1, min(hard_cap, _read_int_env("BOOK_PDF_MINERU_WORKER_MIN", 1)))
    configured_workers = _read_int_env("BOOK_PDF_MINERU_WORKERS", 0)
    if configured_workers > 0:
        return max(worker_min, min(task_count, hard_cap, configured_workers))

    default_workers = max(worker_min, min(hard_cap, _read_int_env("BOOK_PDF_MINERU_DEFAULT_WORKERS", 4)))
    if task_count <= default_workers:
        return min(task_count, default_workers)

    cpu_cores = max(1, int(os.cpu_count() or 1))
    cpu_divisor = max(1, _read_int_env("BOOK_PDF_MINERU_WORKER_CPU_DIVISOR", 2))
    cpu_budget = max(1, cpu_cores // cpu_divisor)
    if cpu_cores >= 4:
        cpu_budget = max(2, cpu_budget)

    ram_budget = hard_cap
    available_ram_gb = _read_available_memory_gb()
    if available_ram_gb is not None:
        reserved_ram_gb = max(0.0, _read_float_env("BOOK_PDF_MINERU_WORKER_RESERVED_RAM_GB", 2.0))
        ram_per_worker_gb = max(0.2, _read_float_env("BOOK_PDF_MINERU_WORKER_RAM_PER_GB", 2.0))
        estimated = int((available_ram_gb - reserved_ram_gb) / ram_per_worker_gb)
        ram_budget = max(worker_min, estimated)

    return max(worker_min, min(task_count, hard_cap, cpu_budget, ram_budget))

def _extract_mineru_page_task(
    task_id: str,
    mineru_bin: str,
    use_mineru_cli: bool,
    mineru_env: dict,
    sliced_pdf_path: str,
    output_dir: str,
    output_root: str,
    image_dir: str,
    section_id: str,
    start_page: int,
    end_page: int,
    timeout_seconds: int,
) -> _MineruSliceExtractResult:
    sliced_pdf = Path(str(sliced_pdf_path or "")).expanduser().resolve()
    resolved_output_dir = Path(str(output_dir or "")).expanduser().resolve()
    resolved_output_root = Path(str(output_root or "")).expanduser().resolve()
    resolved_image_dir = Path(str(image_dir or "")).expanduser().resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    resolved_output_root.mkdir(parents=True, exist_ok=True)
    resolved_image_dir.mkdir(parents=True, exist_ok=True)

    mineru_output_root = resolved_output_dir / "intermediates" / "book_mineru_raw"
    mineru_output_root.mkdir(parents=True, exist_ok=True)
    mineru_output_dir = _ensure_unique_dir(
        mineru_output_root / f"{_safe_token(section_id or 'section')}-p{start_page:04d}-{end_page:04d}"
    )
    if use_mineru_cli:
        commands = _build_mineru_cli_commands(
            mineru_bin=mineru_bin,
            sliced_pdf_path=sliced_pdf,
            mineru_output_dir=mineru_output_dir,
        )
    else:
        commands = [
            [mineru_bin, "-p", str(sliced_pdf), "-o", str(mineru_output_dir), "-m", "txt"],
            [mineru_bin, "-p", str(sliced_pdf), "-o", str(mineru_output_dir), "-m", "auto"],
            [mineru_bin, "-p", str(sliced_pdf), "-o", str(mineru_output_dir)],
        ]

    last_error = "mineru execution failed"
    for command in commands:
        try:
            completed = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=max(30, int(timeout_seconds)),
                check=False,
                env=mineru_env,
            )
        except Exception as error:
            last_error = f"run {command[0]} failed: {error}"
            continue

        if completed.returncode != 0:
            stderr_preview = (completed.stderr or "").strip()
            stdout_preview = (completed.stdout or "").strip()
            last_error = stderr_preview or stdout_preview or f"exit code {completed.returncode}"
            continue

        md_path = _find_markdown_file(mineru_output_dir)
        if md_path is None:
            stderr_preview = (completed.stderr or "").strip()
            stdout_preview = (completed.stdout or "").strip()
            detail = stderr_preview or stdout_preview
            if detail:
                detail = detail[:800]
                last_error = f"mineru no markdown output: {detail}"
            else:
                last_error = "mineru succeeded but no markdown output found"
            continue

        markdown = md_path.read_text(encoding="utf-8", errors="replace")
        middle_json_path = _find_mineru_middle_json(mineru_output_dir)
        markdown = _refill_mineru_code_blocks_with_vector_text(
            task_id=task_id,
            markdown=markdown,
            middle_json_path=middle_json_path,
            sliced_pdf_path=sliced_pdf,
        )
        rewritten_markdown, copied_paths = _rewrite_markdown_image_paths(
            markdown=markdown,
            markdown_file=md_path,
            image_dir=resolved_image_dir,
            output_root=resolved_output_root,
            section_id=section_id,
            start_page=start_page,
            end_page=end_page,
        )
        return _MineruSliceExtractResult(
            success=True,
            start_page=start_page,
            end_page=end_page,
            markdown=rewritten_markdown,
            image_paths=copied_paths,
        )

    return _MineruSliceExtractResult(
        success=False,
        start_page=start_page,
        end_page=end_page,
        error_msg=last_error,
    )


def _discover_mineru_cli() -> str:
    python_executable = Path(sys.executable).resolve()
    python_root = python_executable.parent
    script_dir = python_root / "Scripts"
    binary_names = [
        "mineru",
        "mineru.exe",
        "magic-pdf",
        "magic_pdf",
        "magic-pdf.exe",
        "magic_pdf.exe",
    ]

    candidates = [
        os.getenv("MINERU_BIN", "").strip(),
        os.getenv("MAGIC_PDF_BIN", "").strip(),
        shutil.which("mineru"),
        shutil.which("magic-pdf"),
        shutil.which("magic_pdf"),
        *[str(script_dir / name) for name in binary_names],
        *[str(python_root / name) for name in binary_names],
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return ""


def _is_mineru_cli_binary(binary_path: str) -> bool:
    name = Path(str(binary_path or "")).name.lower()
    return name.startswith("mineru")


def _build_mineru_cli_commands(mineru_bin: str, sliced_pdf_path: Path, mineru_output_dir: Path) -> List[List[str]]:
    source = str(os.getenv("BOOK_PDF_MINERU_MODEL_SOURCE", "local") or "local").strip().lower()
    if source not in {"local", "huggingface", "modelscope"}:
        source = "local"
    include_hybrid = _read_bool_env("BOOK_PDF_MINERU_PREFER_HYBRID", True)
    source_candidates: List[str] = [source]
    for fallback_source in _read_mineru_source_candidates(
            env_key="BOOK_PDF_MINERU_FALLBACK_SOURCES",
            default_value="huggingface,modelscope"):
        if fallback_source not in source_candidates:
            source_candidates.append(fallback_source)

    commands: List[List[str]] = []
    for source_candidate in source_candidates:
        if include_hybrid:
            commands.append(
                [
                    mineru_bin,
                    "-p",
                    str(sliced_pdf_path),
                    "-o",
                    str(mineru_output_dir),
                    "-b",
                    "hybrid-auto-engine",
                    "-m",
                    "txt",
                    "--source",
                    source_candidate,
                ]
            )
        commands.append(
            [
                mineru_bin,
                "-p",
                str(sliced_pdf_path),
                "-o",
                str(mineru_output_dir),
                "-b",
                "pipeline",
                "-m",
                "txt",
                "--source",
                source_candidate,
            ]
        )
        commands.append(
            [
                mineru_bin,
                "-p",
                str(sliced_pdf_path),
                "-o",
                str(mineru_output_dir),
                "-b",
                "pipeline",
                "-m",
                "auto",
                "--source",
                source_candidate,
            ]
        )

    deduped_commands: List[List[str]] = []
    seen = set()
    for command in commands:
        key = tuple(command)
        if key in seen:
            continue
        seen.add(key)
        deduped_commands.append(command)
    return deduped_commands


def _read_mineru_source_candidates(env_key: str, default_value: str) -> List[str]:
    raw_value = str(os.getenv(env_key, default_value) or default_value).strip().lower()
    if not raw_value:
        return []
    candidates: List[str] = []
    for token in re.split(r"[,;|\s]+", raw_value):
        source = token.strip().lower()
        if source not in {"local", "huggingface", "modelscope"}:
            continue
        if source in candidates:
            continue
        candidates.append(source)
    return candidates


def _discover_mineru_models_download_cli() -> str:
    python_executable = Path(sys.executable).resolve()
    python_root = python_executable.parent
    script_dir = python_root / "Scripts"
    binary_names = [
        "mineru-models-download",
        "mineru-models-download.exe",
    ]

    candidates = [
        os.getenv("MINERU_MODELS_DOWNLOAD_BIN", "").strip(),
        shutil.which("mineru-models-download"),
        *[str(script_dir / name) for name in binary_names],
        *[str(python_root / name) for name in binary_names],
    ]
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return ""


def _ensure_mineru_pipeline_models(task_id: str) -> None:
    global _MINERU_MODEL_BOOTSTRAP_LAST_TS
    if _mineru_local_models_ready():
        return

    cooldown_seconds = max(0, _read_int_env("BOOK_PDF_MINERU_MODEL_BOOTSTRAP_COOLDOWN_SEC", 900))
    now = time.time()
    if cooldown_seconds > 0 and _MINERU_MODEL_BOOTSTRAP_LAST_TS > 0:
        if now - _MINERU_MODEL_BOOTSTRAP_LAST_TS < cooldown_seconds:
            logger.info(
                "[%s] skip mineru model bootstrap due cooldown, waitSec=%s",
                task_id,
                int(cooldown_seconds - (now - _MINERU_MODEL_BOOTSTRAP_LAST_TS)),
            )
            return

    _MINERU_MODEL_BOOTSTRAP_LAST_TS = now
    downloader = _discover_mineru_models_download_cli()
    if not downloader:
        logger.warning("[%s] mineru-models-download cli not found, skip model bootstrap", task_id)
        return

    timeout_seconds = max(60, _read_int_env("BOOK_PDF_MINERU_MODEL_DOWNLOAD_TIMEOUT_SEC", 1800))
    source_candidates = _read_mineru_source_candidates(
        env_key="BOOK_PDF_MINERU_MODEL_BOOTSTRAP_SOURCES",
        default_value="huggingface,modelscope",
    )
    if not source_candidates:
        source_candidates = ["huggingface", "modelscope"]

    last_detail = "unknown"
    for source in source_candidates:
        try:
            completed = subprocess.run(
                [downloader, "-s", source, "-m", "pipeline"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout_seconds,
                check=False,
                env=os.environ.copy(),
            )
        except Exception as error:
            last_detail = f"source={source}, err={error}"
            continue

        if completed.returncode == 0:
            logger.info("[%s] mineru pipeline model bootstrap done, source=%s", task_id, source)
            return

        detail = (completed.stderr or "").strip() or (completed.stdout or "").strip() or f"exit={completed.returncode}"
        last_detail = f"source={source}, detail={detail[:600]}"
        logger.warning("[%s] mineru model bootstrap failed, source=%s: %s", task_id, source, detail[:600])

    logger.warning("[%s] mineru model bootstrap all sources failed: %s", task_id, last_detail)


def _mineru_local_models_ready() -> bool:
    config_path = _resolve_mineru_tools_config_path()
    if not config_path:
        return False
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    for models_root in _iter_mineru_pipeline_model_roots(config):
        model_file = models_root / _MINERU_PIPELINE_MODEL_SENTINEL
        if model_file.is_file():
            return True
    return False


def _resolve_mineru_tools_config_path() -> Optional[Path]:
    configured_path = str(os.getenv("MINERU_TOOLS_CONFIG_JSON", "") or "").strip()
    candidates: List[Path] = []
    if configured_path:
        candidates.append(Path(configured_path))
    candidates.append(Path.home() / "mineru.json")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _iter_mineru_pipeline_model_roots(config: dict) -> List[Path]:
    models_dir = (config or {}).get("models-dir")
    raw_paths: List[str] = []
    if isinstance(models_dir, str):
        normalized = models_dir.strip()
        if normalized:
            raw_paths.append(normalized)
    elif isinstance(models_dir, dict):
        pipeline_dir = str(models_dir.get("pipeline") or "").strip()
        if pipeline_dir:
            raw_paths.append(pipeline_dir)

    candidates: List[Path] = []
    seen = set()
    for raw_path in raw_paths:
        root = Path(raw_path)
        for candidate in (root, root / "models"):
            key = str(candidate)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate)
    return candidates


def _maybe_refine_markdown_with_llm(task_id: str, markdown: str) -> str:
    if not str(markdown or "").strip():
        return markdown

    enabled = _read_bool_env("BOOK_PDF_MARKDOWN_LLM_FILTER_ENABLED", True)
    if not enabled:
        return markdown

    api_key = str(os.getenv("DEEPSEEK_API_KEY", "") or "").strip()
    if not api_key:
        logger.info("[%s] skip markdown llm filter: DEEPSEEK_API_KEY not set", task_id)
        return markdown

    max_chars = max(2000, _read_int_env("BOOK_PDF_MARKDOWN_LLM_FILTER_MAX_CHARS", 120000))
    if len(markdown) > max_chars:
        logger.info(
            "[%s] skip markdown llm filter: text too long (%s > %s)",
            task_id,
            len(markdown),
            max_chars,
        )
        return markdown

    model_name = str(os.getenv("BOOK_PDF_MARKDOWN_LLM_FILTER_MODEL", "deepseek-chat") or "deepseek-chat").strip()
    timeout_seconds = max(10, _read_int_env("BOOK_PDF_MARKDOWN_LLM_FILTER_TIMEOUT_SEC", 45))
    temperature = _read_float_env("BOOK_PDF_MARKDOWN_LLM_FILTER_TEMPERATURE", 0.0)
    chunk_max_chars = max(50, _read_int_env("BOOK_PDF_MARKDOWN_LLM_FILTER_CHUNK_MAX_CHARS", 6000))
    max_workers = max(1, min(12, _read_int_env("BOOK_PDF_MARKDOWN_LLM_FILTER_MAX_WORKERS", 4)))
    url = _build_chat_completions_url(
        str(os.getenv("BOOK_PDF_MARKDOWN_LLM_FILTER_BASE_URL", "https://api.deepseek.com/v1") or "")
    )
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    chunks = _split_markdown_into_llm_chunks(markdown=markdown, chunk_max_chars=chunk_max_chars)
    if not chunks:
        return markdown
    if len(chunks) == 1:
        return _refine_markdown_chunk_with_llm(
            task_id=task_id,
            chunk_index=0,
            chunk_text=chunks[0],
            model_name=model_name,
            url=url,
            headers=headers,
            timeout_seconds=timeout_seconds,
            temperature=temperature,
        )

    worker_count = min(len(chunks), max_workers)
    logger.info(
        "[%s] markdown llm filter start, chunks=%s, workers=%s, chunk_max_chars=%s",
        task_id,
        len(chunks),
        worker_count,
        chunk_max_chars,
    )
    refined_by_index: Dict[int, str] = {idx: chunk for idx, chunk in enumerate(chunks)}

    if worker_count <= 1:
        for idx, chunk in enumerate(chunks):
            refined_by_index[idx] = _refine_markdown_chunk_with_llm(
                task_id=task_id,
                chunk_index=idx,
                chunk_text=chunk,
                model_name=model_name,
                url=url,
                headers=headers,
                timeout_seconds=timeout_seconds,
                temperature=temperature,
            )
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                executor.submit(
                    _refine_markdown_chunk_with_llm,
                    task_id,
                    idx,
                    chunk,
                    model_name,
                    url,
                    headers,
                    timeout_seconds,
                    temperature,
                ): idx
                for idx, chunk in enumerate(chunks)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    refined_by_index[idx] = future.result()
                except Exception as error:
                    logger.warning(
                        "[%s] markdown llm filter chunk failed, chunk=%s, fallback original: %s",
                        task_id,
                        idx,
                        error,
                    )
                    refined_by_index[idx] = chunks[idx]

    rebuilt = "".join(refined_by_index[idx] for idx in range(len(chunks)))
    return rebuilt or markdown


def _refine_markdown_chunk_with_llm(
    task_id: str,
    chunk_index: int,
    chunk_text: str,
    model_name: str,
    url: str,
    headers: dict,
    timeout_seconds: int,
    temperature: float,
) -> str:
    if not str(chunk_text or "").strip():
        return chunk_text

    masked_chunk, marker_map = _mask_image_markers(chunk_text)
    prompt = _BOOK_MARKDOWN_FILTER_USER_PROMPT_TEMPLATE.format(mineru_markdown_output=masked_chunk)
    content = _call_markdown_filter_llm(
        task_id=task_id,
        chunk_index=chunk_index,
        prompt=prompt,
        model_name=model_name,
        url=url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        temperature=temperature,
    )
    if not content:
        return chunk_text

    refined = _strip_wrapping_markdown_fence(content)
    if not refined:
        return chunk_text

    restored = _restore_image_markers(refined, marker_map)
    if restored is None:
        logger.warning(
            "[%s] markdown llm filter chunk changed image marker topology, chunk=%s, fallback original",
            task_id,
            chunk_index,
        )
        return chunk_text
    return restored


def _call_markdown_filter_llm(
    task_id: str,
    chunk_index: int,
    prompt: str,
    model_name: str,
    url: str,
    headers: dict,
    timeout_seconds: int,
    temperature: float,
) -> str:
    payload = {
        "model": model_name,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": _BOOK_MARKDOWN_FILTER_SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    }
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        body = response.json()
        choices = body.get("choices") if isinstance(body, dict) else None
        if not isinstance(choices, list) or not choices:
            logger.warning("[%s] markdown llm filter returned empty choices, chunk=%s", task_id, chunk_index)
            return ""
        content = str((((choices[0] or {}).get("message") or {}).get("content") or "")).strip()
        if not content:
            logger.warning("[%s] markdown llm filter returned empty content, chunk=%s", task_id, chunk_index)
            return ""
        return content
    except Exception as error:
        logger.warning(
            "[%s] markdown llm filter failed, chunk=%s, fallback original chunk: %s",
            task_id,
            chunk_index,
            error,
        )
        return ""


def _split_markdown_into_llm_chunks(markdown: str, chunk_max_chars: int) -> List[str]:
    units = _split_markdown_into_atomic_units(markdown)
    if not units:
        return []

    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
    for unit in units:
        unit_len = len(unit)
        if current and (current_len + unit_len > chunk_max_chars):
            chunks.append("".join(current))
            current = [unit]
            current_len = unit_len
            continue
        if (not current) and unit_len > chunk_max_chars:
            chunks.append(unit)
            continue
        current.append(unit)
        current_len += unit_len
    if current:
        chunks.append("".join(current))
    return chunks


def _split_markdown_into_atomic_units(markdown: str) -> List[str]:
    lines = str(markdown or "").splitlines(keepends=True)
    if not lines:
        return []

    units: List[str] = []
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()

        if _is_markdown_fence_line(stripped):
            end = index + 1
            while end < len(lines):
                if _is_markdown_fence_line(lines[end].strip()):
                    end += 1
                    break
                end += 1
            units.append("".join(lines[index:end]))
            index = end
            continue

        if _is_formula_block_start(stripped):
            end = _consume_formula_block(lines, index)
            units.append("".join(lines[index:end]))
            index = end
            continue

        if _is_markdown_table_line(stripped):
            end = index + 1
            while end < len(lines) and _is_markdown_table_line(lines[end].strip()):
                end += 1
            units.append("".join(lines[index:end]))
            index = end
            continue

        if not stripped:
            end = index + 1
            while end < len(lines) and (not lines[end].strip()):
                end += 1
            units.append("".join(lines[index:end]))
            index = end
            continue

        end = index + 1
        while end < len(lines):
            probe = lines[end].strip()
            if (not probe) or _is_markdown_fence_line(probe) or _is_formula_block_start(probe) or _is_markdown_table_line(probe):
                break
            end += 1
        units.append("".join(lines[index:end]))
        index = end
    return units


def _is_markdown_fence_line(stripped_line: str) -> bool:
    return str(stripped_line or "").startswith("```")


def _is_markdown_table_line(stripped_line: str) -> bool:
    line = str(stripped_line or "").strip()
    if not line:
        return False
    return bool(_TABLE_LINE_PATTERN.match(line))


def _is_formula_block_start(stripped_line: str) -> bool:
    line = str(stripped_line or "").strip()
    if not line:
        return False
    if line.startswith("$$"):
        return True
    if line.startswith("\\["):
        return True
    if line.startswith("\\begin{"):
        return True
    return False


def _consume_formula_block(lines: List[str], start_index: int) -> int:
    opening = str(lines[start_index] or "").strip()
    if opening.startswith("$$"):
        if opening.count("$$") >= 2 and len(opening) > 2:
            return start_index + 1
        cursor = start_index + 1
        while cursor < len(lines):
            if "$$" in lines[cursor]:
                return cursor + 1
            cursor += 1
        return len(lines)

    if opening.startswith("\\["):
        cursor = start_index + 1
        while cursor < len(lines):
            if "\\]" in lines[cursor]:
                return cursor + 1
            cursor += 1
        return len(lines)

    begin_match = re.match(r"\\begin\{([^}]+)\}", opening)
    if begin_match:
        end_tag = f"\\end{{{begin_match.group(1)}}}"
        cursor = start_index + 1
        while cursor < len(lines):
            if end_tag in lines[cursor]:
                return cursor + 1
            cursor += 1
        return len(lines)

    return start_index + 1


def _mask_image_markers(markdown: str) -> Tuple[str, dict]:
    marker_map = {}
    source_text = str(markdown or "")

    def replace(match: re.Match) -> str:
        while True:
            token = f"[[SYS_MEDIA_{uuid.uuid4().hex[:12]}]]"
            if token not in source_text and token not in marker_map:
                break
        marker_map[token] = match.group(0)
        return token

    masked = _IMAGE_MD_PATTERN.sub(replace, source_text)
    return masked, marker_map


def _restore_image_markers(markdown: str, marker_map: dict) -> Optional[str]:
    restored = str(markdown or "")
    for token in marker_map.keys():
        if restored.count(token) != 1:
            return None
    for token, original_marker in marker_map.items():
        restored = restored.replace(token, original_marker)
    return restored


def _strip_wrapping_markdown_fence(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    matched = _MD_FENCE_PATTERN.match(raw)
    if matched:
        return str(matched.group(1) or "").strip()
    return raw


def _build_chat_completions_url(base_url: str) -> str:
    normalized = str(base_url or "").strip() or "https://api.deepseek.com/v1"
    normalized = normalized.rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    return f"{normalized}/chat/completions"


def _read_bool_env(key: str, default: bool) -> bool:
    raw = str(os.getenv(key, "") or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _read_int_env(key: str, default: int) -> int:
    raw = str(os.getenv(key, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _read_float_env(key: str, default: float) -> float:
    raw = str(os.getenv(key, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _read_available_memory_gb() -> Optional[float]:
    try:
        import psutil

        memory = psutil.virtual_memory()
        return float(memory.available) / float(1024 ** 3)
    except Exception:
        return None


def _build_mineru_runtime_env(output_dir: Path) -> dict:
    runtime_root = output_dir / "intermediates" / "book_mineru_runtime"
    models_dir = runtime_root / "models"
    runtime_root.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    config_path = runtime_root / "magic-pdf.json"
    if not config_path.exists():
        config = {
            "bucket_info": {
                "[default]": ["", "", ""],
            },
            "models-dir": str(models_dir),
            "device-mode": "cpu",
            "layout-config": {
                "model": "doclayout_yolo",
            },
            "table-config": {
                "model": "rapid_table",
                "enable": False,
                "max_time": 400,
            },
            "formula-config": {
                "mfd_model": "yolo_v8_mfd",
                "mfr_model": "unimernet_small",
                "enable": False,
            },
        }
        config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    runtime_env = os.environ.copy()
    runtime_env["MINERU_TOOLS_CONFIG_JSON"] = str(config_path)
    runtime_env.setdefault("MINERU_MODEL_SOURCE", "huggingface")
    return runtime_env


def _find_markdown_file(root: Path) -> Optional[Path]:
    candidates = sorted(root.rglob("*.md"), key=lambda p: len(str(p)))
    if not candidates:
        return None
    # 优先包含 "md"/"markdown" 输出路径，其次选最大文件
    preferred = [p for p in candidates if "markdown" in p.as_posix().lower() or "/md/" in p.as_posix().lower()]
    pool = preferred or candidates
    return max(pool, key=lambda p: p.stat().st_size if p.exists() else 0)


def _find_mineru_middle_json(root: Path) -> Optional[Path]:
    candidates = sorted(root.rglob("*middle.json"), key=lambda p: len(str(p)))
    if not candidates:
        return None
    preferred = [path for path in candidates if path.name.lower().endswith("_middle.json")]
    pool = preferred or candidates
    return max(pool, key=lambda p: p.stat().st_size if p.exists() else 0)


def _refill_mineru_code_blocks_with_vector_text(
    task_id: str,
    markdown: str,
    middle_json_path: Optional[Path],
    sliced_pdf_path: Path,
) -> str:
    if not _read_bool_env("BOOK_PDF_MINERU_VECTOR_CODE_REFILL_ENABLED", True):
        return markdown
    if not str(markdown or "").strip():
        return markdown
    if middle_json_path is None or (not middle_json_path.is_file()):
        return markdown

    code_fence_spans = _locate_markdown_code_fence_content_spans(markdown)
    if not code_fence_spans:
        return markdown

    code_regions = _extract_code_bboxes_from_middle_json(middle_json_path)
    if not code_regions:
        logger.info("[%s] skip mineru vector code refill: no code bbox found in %s", task_id, middle_json_path)
        return markdown

    vector_texts = _extract_vector_text_for_code_regions(sliced_pdf_path, code_regions)
    if not vector_texts:
        return markdown

    replacements: Dict[int, str] = {}
    for index in range(min(len(code_fence_spans), len(vector_texts))):
        candidate = str(vector_texts[index] or "").replace("\r\n", "\n").replace("\r", "\n").strip("\n")
        if not candidate.strip():
            continue
        replacements[index] = candidate

    if not replacements:
        return markdown

    rewritten = _replace_markdown_code_fence_contents(
        markdown=markdown,
        code_fence_spans=code_fence_spans,
        replacements=replacements,
    )
    logger.info(
        "[%s] mineru vector code refill applied, markdown_code_blocks=%s, bbox_regions=%s, replaced=%s",
        task_id,
        len(code_fence_spans),
        len(code_regions),
        len(replacements),
    )
    return rewritten


def _locate_markdown_code_fence_content_spans(markdown: str) -> List[Tuple[int, int]]:
    spans: List[Tuple[int, int]] = []
    lines = str(markdown or "").splitlines(keepends=True)
    offset = 0
    inside_fence = False
    content_start = 0

    for line in lines:
        stripped = line.lstrip()
        is_fence_line = stripped.startswith("```")
        if (not inside_fence) and is_fence_line:
            inside_fence = True
            content_start = offset + len(line)
        elif inside_fence and is_fence_line:
            spans.append((content_start, offset))
            inside_fence = False
        offset += len(line)
    return spans


def _replace_markdown_code_fence_contents(
    markdown: str,
    code_fence_spans: List[Tuple[int, int]],
    replacements: Dict[int, str],
) -> str:
    if not replacements:
        return markdown

    chunks: List[str] = []
    cursor = 0
    for index, (content_start, content_end) in enumerate(code_fence_spans):
        chunks.append(markdown[cursor:content_start])
        original = markdown[content_start:content_end]
        replacement = replacements.get(index)
        if replacement is None:
            chunks.append(original)
        else:
            normalized = replacement
            if original.endswith("\n") and (not normalized.endswith("\n")):
                normalized = normalized + "\n"
            if (not original.endswith("\n")) and normalized.endswith("\n"):
                normalized = normalized.rstrip("\n")
            chunks.append(normalized)
        cursor = content_end
    chunks.append(markdown[cursor:])
    return "".join(chunks)


def _extract_code_bboxes_from_middle_json(middle_json_path: Path) -> List[_CodeBBoxRegion]:
    try:
        payload = json.loads(middle_json_path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return []

    page_infos = []
    if isinstance(payload, dict):
        raw_pages = payload.get("pdf_info")
        if isinstance(raw_pages, list):
            page_infos = raw_pages
    elif isinstance(payload, list):
        page_infos = payload

    if not page_infos:
        return []

    code_type_tokens = _read_code_bbox_type_tokens()
    collected: List[_CodeBBoxRegion] = []
    seen = set()
    for index, page in enumerate(page_infos):
        if not isinstance(page, dict):
            continue
        page_idx = int(page.get("page_idx", index))
        _collect_code_bboxes_from_node(
            node=page,
            page_idx=page_idx,
            code_type_tokens=code_type_tokens,
            collected=collected,
            seen=seen,
        )

    collected.sort(key=lambda item: (item.page_idx, item.y0, item.x0))
    return collected


def _collect_code_bboxes_from_node(
    node,
    page_idx: int,
    code_type_tokens: List[str],
    collected: List[_CodeBBoxRegion],
    seen: set,
) -> None:
    if isinstance(node, list):
        for child in node:
            _collect_code_bboxes_from_node(
                node=child,
                page_idx=page_idx,
                code_type_tokens=code_type_tokens,
                collected=collected,
                seen=seen,
            )
        return

    if not isinstance(node, dict):
        return

    block_type = str(node.get("type") or node.get("block_type") or node.get("category") or "").strip().lower()
    bbox = _coerce_bbox(node.get("bbox") or node.get("bbox_fs") or node.get("box"))
    if bbox and _looks_like_code_block_type(block_type, code_type_tokens):
        key = (
            int(page_idx),
            round(float(bbox[0]), 2),
            round(float(bbox[1]), 2),
            round(float(bbox[2]), 2),
            round(float(bbox[3]), 2),
        )
        if key not in seen:
            seen.add(key)
            collected.append(
                _CodeBBoxRegion(
                    page_idx=int(page_idx),
                    x0=float(bbox[0]),
                    y0=float(bbox[1]),
                    x1=float(bbox[2]),
                    y1=float(bbox[3]),
                )
            )

    for value in node.values():
        if isinstance(value, (list, dict)):
            _collect_code_bboxes_from_node(
                node=value,
                page_idx=page_idx,
                code_type_tokens=code_type_tokens,
                collected=collected,
                seen=seen,
            )


def _read_code_bbox_type_tokens() -> List[str]:
    raw = str(
        os.getenv(
            "BOOK_PDF_MINERU_CODE_BBOX_TYPES",
            "code,program,programming,listing,pseudocode,algorithm,source_code",
        )
        or ""
    )
    tokens = []
    for part in re.split(r"[,;|\s]+", raw):
        normalized = str(part or "").strip().lower().replace("-", "_")
        if normalized:
            tokens.append(normalized)
    return tokens or ["code"]


def _looks_like_code_block_type(block_type: str, code_type_tokens: List[str]) -> bool:
    normalized = str(block_type or "").strip().lower().replace("-", "_")
    if not normalized:
        return False
    if normalized in code_type_tokens:
        return True
    return any(token in normalized for token in code_type_tokens)


def _coerce_bbox(value) -> Optional[Tuple[float, float, float, float]]:
    if not isinstance(value, (list, tuple)) or len(value) < 4:
        return None
    try:
        x0 = float(value[0])
        y0 = float(value[1])
        x1 = float(value[2])
        y1 = float(value[3])
    except Exception:
        return None
    left = min(x0, x1)
    top = min(y0, y1)
    right = max(x0, x1)
    bottom = max(y0, y1)
    if (right - left) <= 1 or (bottom - top) <= 1:
        return None
    return left, top, right, bottom


def _extract_vector_text_for_code_regions(sliced_pdf_path: Path, regions: List[_CodeBBoxRegion]) -> List[str]:
    if not regions:
        return []

    import fitz

    preserve_whitespace_flag = int(getattr(fitz, "TEXT_PRESERVE_WHITESPACE", 0))
    preserve_ligature_flag = int(getattr(fitz, "TEXT_PRESERVE_LIGATURES", 0))
    flags = preserve_whitespace_flag | preserve_ligature_flag
    padding = max(0.0, _read_float_env("BOOK_PDF_MINERU_CODE_BBOX_PADDING", 1.5))

    extracted: List[str] = []
    with fitz.open(sliced_pdf_path) as document:
        for region in regions:
            if region.page_idx < 0 or region.page_idx >= document.page_count:
                extracted.append("")
                continue
            page = document[region.page_idx]
            clip = fitz.Rect(
                region.x0 - padding,
                region.y0 - padding,
                region.x1 + padding,
                region.y1 + padding,
            ).intersect(page.rect)
            if clip.is_empty:
                extracted.append("")
                continue
            text = ""
            try:
                text = page.get_text("text", clip=clip, sort=True, flags=flags)
            except TypeError:
                try:
                    text = page.get_text("text", clip=clip, flags=flags)
                except Exception:
                    text = ""
            except Exception:
                text = ""
            if not str(text or "").strip():
                try:
                    text = page.get_textbox(clip) or ""
                except Exception:
                    text = ""
            extracted.append(str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip("\n"))
    return extracted


def _rewrite_markdown_image_paths(
    markdown: str,
    markdown_file: Path,
    image_dir: Path,
    output_root: Path,
    section_id: str,
    start_page: int,
    end_page: int,
) -> Tuple[str, List[str]]:
    image_dir.mkdir(parents=True, exist_ok=True)
    rewritten = markdown
    copied_paths: List[str] = []
    image_index = 0
    replacements = {}

    for match in _IMAGE_MD_PATTERN.finditer(markdown):
        raw_path = str(match.group(1) or "").strip()
        if not raw_path or raw_path in replacements:
            continue
        if raw_path.startswith("http://") or raw_path.startswith("https://") or raw_path.startswith("data:"):
            continue

        source_path = _resolve_image_source_path(raw_path, markdown_file)
        if source_path is None or (not source_path.exists()):
            continue

        image_index += 1
        extension = source_path.suffix or ".png"
        safe_section = _safe_token(section_id or "section")
        file_name = f"mineru-{safe_section}-p{start_page:04d}-{end_page:04d}-img{image_index:03d}{extension}"
        target_path = _ensure_unique_path(image_dir / file_name)
        shutil.copyfile(source_path, target_path)
        relative_path = _relative_path(output_root, target_path)
        replacements[raw_path] = relative_path
        copied_paths.append(relative_path)

    for source, target in replacements.items():
        rewritten = rewritten.replace(f"]({source})", f"]({target})")

    return rewritten, copied_paths


def _resolve_image_source_path(raw_path: str, markdown_file: Path) -> Optional[Path]:
    normalized = raw_path.replace("\\", "/").strip()
    candidate = Path(normalized)
    if candidate.is_absolute():
        return candidate
    direct = (markdown_file.parent / normalized).resolve()
    if direct.exists():
        return direct
    # 某些工具会把图片输出到 markdown 同级目录之外，尝试基于文件名回溯一次
    basename = Path(normalized).name
    if not basename:
        return None
    for matched in markdown_file.parent.rglob(basename):
        if matched.is_file():
            return matched
    return direct


def _extract_with_pymupdf(
    task_id: str,
    sliced_pdf_path: Path,
    output_dir: Path,
    output_root: Path,
    image_dir: Path,
    section_id: str,
    start_page: int,
    end_page: int,
    progress_reporter: Optional[_BookPdfExtractProgressReporter] = None,
) -> ExtractBookPdfResult:
    import fitz

    markdown_lines: List[str] = []
    image_paths: List[str] = []
    image_counter = 0

    with fitz.open(sliced_pdf_path) as document:
        for local_page_idx in range(document.page_count):
            absolute_page_no = start_page + local_page_idx
            page = document[local_page_idx]
            blocks = page.get_text("dict").get("blocks", [])
            blocks = sorted(
                blocks,
                key=lambda item: (
                    float((item.get("bbox") or [0.0, 0.0, 0.0, 0.0])[1]),
                    float((item.get("bbox") or [0.0, 0.0, 0.0, 0.0])[0]),
                ),
            )
            for block in blocks:
                block_type = int(block.get("type", -1))
                if block_type == 0:
                    text = _compose_text_block(block)
                    if text:
                        markdown_lines.append(text)
                        markdown_lines.append("")
                    continue
                if block_type == 1:
                    image_bytes = block.get("image")
                    if not isinstance(image_bytes, (bytes, bytearray)) or len(image_bytes) == 0:
                        continue
                    extension = str(block.get("ext") or "png").strip().lower()
                    if not extension.startswith("."):
                        extension = "." + extension
                    image_counter += 1
                    safe_section = _safe_token(section_id or "section")
                    file_name = f"pymupdf-{safe_section}-p{start_page:04d}-{end_page:04d}-img{image_counter:03d}{extension}"
                    target = _ensure_unique_path(image_dir / file_name)
                    target.write_bytes(bytes(image_bytes))
                    rel_path = _relative_path(output_root, target)
                    image_paths.append(rel_path)
                    markdown_lines.append(f"![image-{image_counter}]({rel_path})")
                    markdown_lines.append("")
            if progress_reporter is not None:
                progress_reporter.record_completed_pages(
                    start_page=absolute_page_no,
                    end_page=absolute_page_no,
                    extractor="pymupdf",
                )

    markdown = "\n".join(markdown_lines).strip()
    markdown = _maybe_refine_markdown_with_llm(task_id=task_id, markdown=markdown)
    markdown_target = _write_markdown_output(
        output_dir=output_dir,
        section_id=section_id,
        start_page=start_page,
        end_page=end_page,
        markdown=markdown,
        extractor="pymupdf",
    )
    stats = _collect_markdown_stats(markdown)
    logger.info(
        "[%s] pymupdf extraction success, pages=%s-%s, images=%s, tables=%s, code=%s, formula=%s",
        task_id,
        start_page,
        end_page,
        stats[0],
        stats[1],
        stats[2],
        stats[3],
    )
    return ExtractBookPdfResult(
        success=True,
        markdown=markdown,
        markdown_path=str(markdown_target),
        extractor="pymupdf",
        image_count=stats[0],
        table_count=stats[1],
        code_block_count=stats[2],
        formula_block_count=stats[3],
        image_paths=image_paths,
    )


def _compose_text_block(block: dict) -> str:
    lines = block.get("lines") or []
    chunks: List[str] = []
    for line in lines:
        spans = line.get("spans") or []
        text = "".join(str(span.get("text") or "") for span in spans).strip()
        if text:
            chunks.append(text)
    return "\n".join(chunks).strip()


def _write_markdown_output(
    output_dir: Path,
    section_id: str,
    start_page: int,
    end_page: int,
    markdown: str,
    extractor: str,
) -> Path:
    target_dir = output_dir / "intermediates" / "book_pdf_extract"
    target_dir.mkdir(parents=True, exist_ok=True)
    token = _safe_token(section_id or "section")
    file_name = f"{token}-p{start_page:04d}-{end_page:04d}-{extractor}.md"
    target_path = _ensure_unique_path(target_dir / file_name)
    target_path.write_text(markdown, encoding="utf-8")
    return target_path


def _collect_markdown_stats(markdown: str) -> Tuple[int, int, int, int]:
    image_count = len(_IMAGE_MD_PATTERN.findall(markdown))

    table_count = 0
    in_table = False
    for raw_line in markdown.splitlines():
        line = raw_line.strip()
        looks_like_table = line.startswith("|") and line.endswith("|")
        if looks_like_table and not in_table:
            table_count += 1
            in_table = True
        elif not looks_like_table:
            in_table = False

    fence_count = markdown.count("```")
    code_block_count = fence_count // 2

    formula_block_count = markdown.count("$$") // 2
    formula_block_count += len(_INLINE_FORMULA_PATTERN.findall(markdown))
    return image_count, table_count, code_block_count, formula_block_count


def _ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    index = 2
    while True:
        candidate = path.with_name(f"{stem}-{index}{suffix}")
        if not candidate.exists():
            return candidate
        index += 1


def _ensure_unique_dir(path: Path) -> Path:
    if not path.exists():
        path.mkdir(parents=True, exist_ok=True)
        return path
    index = 2
    while True:
        candidate = path.parent / f"{path.name}-{index}"
        if not candidate.exists():
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        index += 1


def _relative_path(output_root: Path, target: Path) -> str:
    try:
        relative = target.resolve().relative_to(output_root.resolve())
        return relative.as_posix()
    except Exception:
        try:
            return os.path.relpath(target.resolve(), output_root.resolve()).replace("\\", "/")
        except Exception:
            return target.name


def _safe_token(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(value or "").strip())
    cleaned = cleaned.strip("-")
    return cleaned or "section"
