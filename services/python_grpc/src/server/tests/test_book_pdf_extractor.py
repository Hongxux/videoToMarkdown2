from pathlib import Path
from collections import deque
from concurrent.futures import Future
import json
import sys

import fitz

import services.python_grpc.src.server.book_pdf_extractor as extractor_mod
from services.python_grpc.src.server.book_pdf_extractor import (
    _BookPdfExtractProgressReporter,
    _build_mineru_page_tasks,
    _build_mineru_runtime_env,
    _decide_mineru_parallel_workers,
    _dispatch_book_pdf_mineru_jobs_locked,
    _discover_mineru_cli,
    _mineru_local_models_ready,
    _MineruPageTaskPayload,
    _MineruSharedJob,
    _maybe_refine_markdown_with_llm,
    _refill_mineru_code_blocks_with_vector_text,
    _resolve_mineru_shared_pool_workers,
    _split_markdown_into_llm_chunks,
    extract_book_pdf_markdown,
)


def _build_sample_pdf(pdf_path: Path) -> None:
    doc = fitz.open()
    try:
        page1 = doc.new_page()
        page1.insert_text((72, 72), "Chapter 1")
        page1.insert_text((72, 100), "This is page one.")
        page1.insert_text((72, 130), "| ColA | ColB |")
        page1.insert_text((72, 155), "| --- | --- |")
        page1.insert_text((72, 180), "| 1 | 2 |")

        page2 = doc.new_page()
        page2.insert_text((72, 72), "Chapter 2")
        page2.insert_text((72, 100), "This is page two only.")

        doc.save(pdf_path)
    finally:
        doc.close()


def _build_code_pdf(pdf_path: Path) -> None:
    doc = fitz.open()
    try:
        page = doc.new_page()
        page.insert_text((72, 72), "def hello(name):", fontname="courier", fontsize=11)
        page.insert_text((72, 88), "    if name is None:", fontname="courier", fontsize=11)
        page.insert_text((72, 104), "        return ''", fontname="courier", fontsize=11)
        page.insert_text((72, 120), "    return f\"hi {name}\"", fontname="courier", fontsize=11)
        doc.save(pdf_path)
    finally:
        doc.close()


def _build_multi_page_pdf(pdf_path: Path, page_count: int) -> None:
    doc = fitz.open()
    try:
        for page_idx in range(page_count):
            page = doc.new_page()
            page.insert_text((72, 72), f"Page {page_idx + 1}")
            page.insert_text((72, 92), "MinerU task split probe")
        doc.save(pdf_path)
    finally:
        doc.close()


def _reset_shared_pool_state() -> None:
    extractor_mod._BOOK_PDF_MINERU_PENDING_JOBS.clear()
    extractor_mod._BOOK_PDF_MINERU_ACTIVE_FUTURES.clear()
    extractor_mod._BOOK_PDF_MINERU_COMPLETED_FUTURES.clear()
    extractor_mod._BOOK_PDF_MINERU_DISPATCHER = None
    extractor_mod._BOOK_PDF_MINERU_STOP = False


class _FakeMineruExecutor:
    def __init__(self) -> None:
        self.submissions = []

    def submit(self, fn, *args):
        future = Future()
        self.submissions.append(args)
        future.set_result(
            extractor_mod._MineruSliceExtractResult(
                success=True,
                start_page=int(args[9]),
                end_page=int(args[10]),
                markdown=f"page-{int(args[9])}",
            )
        )
        return future


class _RecordingWatchdogWriter:
    def __init__(self) -> None:
        self.events = []

    def emit(self, **kwargs):
        self.events.append(dict(kwargs))


def test_extract_book_pdf_markdown_uses_page_slice_and_fallback(tmp_path: Path) -> None:
    pdf_path = tmp_path / "sample.pdf"
    _build_sample_pdf(pdf_path)

    output_dir = tmp_path / "out"
    result = extract_book_pdf_markdown(
        task_id="test_book_pdf_slice",
        pdf_path=str(pdf_path),
        output_dir=str(output_dir),
        start_page=1,
        end_page=1,
        image_dir=str(output_dir / "assets" / "book_images"),
        output_root=str(output_dir),
        section_id="c1s1",
        prefer_mineru=False,
        timeout_seconds=120,
    )

    assert result.success
    assert result.extractor == "pymupdf"
    assert "This is page one." in result.markdown
    assert "This is page two only." not in result.markdown
    assert result.markdown_path
    assert Path(result.markdown_path).is_file()


def test_extract_book_pdf_markdown_rejects_invalid_page_range(tmp_path: Path) -> None:
    pdf_path = tmp_path / "sample.pdf"
    _build_sample_pdf(pdf_path)

    output_dir = tmp_path / "out2"
    result = extract_book_pdf_markdown(
        task_id="test_invalid_range",
        pdf_path=str(pdf_path),
        output_dir=str(output_dir),
        start_page=9,
        end_page=10,
        image_dir=str(output_dir / "assets" / "book_images"),
        output_root=str(output_dir),
        section_id="c9s9",
        prefer_mineru=False,
        timeout_seconds=120,
    )

    assert not result.success
    assert "out of range" in (result.error_msg or "").lower()


def test_build_mineru_page_tasks_defaults_to_single_page_queue(tmp_path: Path, monkeypatch) -> None:
    pdf_path = tmp_path / "ten-pages.pdf"
    _build_multi_page_pdf(pdf_path, page_count=10)

    output_dir = tmp_path / "out-range"
    monkeypatch.delenv("BOOK_PDF_MINERU_PAGE_BATCH_SIZE", raising=False)

    tasks = _build_mineru_page_tasks(
        sliced_pdf_path=pdf_path,
        output_dir=output_dir,
        section_id="range-c1s1t1-c2s2t3",
        start_page=1,
        end_page=10,
        parallel_enabled=True,
    )

    assert [(start, end) for start, end, _path in tasks] == [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 4),
        (5, 5),
        (6, 6),
        (7, 7),
        (8, 8),
        (9, 9),
        (10, 10),
    ]
    assert all(path.is_file() for _start, _end, path in tasks)


def test_decide_mineru_parallel_workers_prefers_four_default_workers(monkeypatch) -> None:
    monkeypatch.delenv("BOOK_PDF_MINERU_WORKERS", raising=False)
    monkeypatch.setenv("BOOK_PDF_MINERU_DEFAULT_WORKERS", "4")
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_MAX", "8")
    monkeypatch.setattr(extractor_mod.os, "cpu_count", lambda: 2)
    monkeypatch.setattr(extractor_mod, "_read_available_memory_gb", lambda: 1.0)

    assert _decide_mineru_parallel_workers(4) == 4


def test_resolve_mineru_shared_pool_workers_defaults_to_four(monkeypatch) -> None:
    monkeypatch.delenv("BOOK_PDF_MINERU_WORKERS", raising=False)
    monkeypatch.setenv("BOOK_PDF_MINERU_DEFAULT_WORKERS", "4")
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_MAX", "8")

    assert _resolve_mineru_shared_pool_workers() == 4


def test_dispatch_book_pdf_mineru_jobs_round_robin_across_jobs(monkeypatch) -> None:
    _reset_shared_pool_state()
    fake_executor = _FakeMineruExecutor()
    monkeypatch.setattr(extractor_mod, "_get_book_pdf_mineru_pool", lambda worker_count: fake_executor)
    monkeypatch.setattr(extractor_mod, "_resolve_mineru_shared_pool_workers", lambda: 4)

    def build_job(task_id: str, pages: list[int]) -> _MineruSharedJob:
        payloads = deque(
            _MineruPageTaskPayload(
                task_id=task_id,
                mineru_bin="mineru",
                use_mineru_cli=True,
                mineru_env={},
                sliced_pdf_path=f"/tmp/{task_id}-{page_no}.pdf",
                output_dir="/tmp/out",
                output_root="/tmp/out",
                image_dir="/tmp/out/assets",
                section_id=task_id,
                start_page=page_no,
                end_page=page_no,
                timeout_seconds=120,
            )
            for page_no in pages
        )
        return _MineruSharedJob(
            job_id=task_id,
            task_id=task_id,
            section_id=task_id,
            total_tasks=len(pages),
            pending_payloads=payloads,
        )

    jobs = [
        build_job("job-a", [1, 2, 3]),
        build_job("job-b", [10, 11]),
        build_job("job-c", [20]),
    ]
    for job in jobs:
        extractor_mod._BOOK_PDF_MINERU_PENDING_JOBS.append(job)
        job.queued = True

    _dispatch_book_pdf_mineru_jobs_locked()

    submitted_pages = [int(args[9]) for args in fake_executor.submissions]
    assert submitted_pages == [1, 10, 20, 2]
    assert jobs[0].queued is True
    assert jobs[1].queued is True
    assert jobs[2].queued is False
    _reset_shared_pool_state()


def test_book_pdf_progress_reporter_emits_hard_progress_once_per_page() -> None:
    writer = _RecordingWatchdogWriter()
    reporter = _BookPdfExtractProgressReporter(
        task_id="task-1",
        section_id="c1s1t1",
        start_page=3,
        end_page=4,
        total_pages=2,
        writer=writer,
    )

    reporter.emit_queue_ready("mineru")
    reporter.record_completed_pages(3, 3, "mineru")
    reporter.record_completed_pages(3, 3, "pymupdf")
    reporter.record_completed_pages(4, 4, "pymupdf")
    reporter.emit_completed("pymupdf")

    checkpoints = [event["checkpoint"] for event in writer.events]
    assert checkpoints == [
        "book_pdf_pages_queued",
        "page_0003_done",
        "page_0004_done",
        "book_pdf_extract_completed",
    ]
    assert writer.events[1]["completed"] == 1
    assert writer.events[1]["pending"] == 1
    assert writer.events[2]["completed"] == 2
    assert writer.events[2]["pending"] == 0
    assert all(event["signal_type"] == "hard" for event in writer.events)


def test_discover_mineru_cli_finds_script_near_python(monkeypatch, tmp_path: Path) -> None:
    fake_python = tmp_path / "python" / "python.exe"
    fake_python.parent.mkdir(parents=True, exist_ok=True)
    fake_python.write_text("", encoding="utf-8")

    fake_magic = fake_python.parent / "Scripts" / "magic-pdf.exe"
    fake_magic.parent.mkdir(parents=True, exist_ok=True)
    fake_magic.write_text("", encoding="utf-8")

    monkeypatch.setenv("MAGIC_PDF_BIN", "")
    monkeypatch.setattr("services.python_grpc.src.server.book_pdf_extractor.shutil.which", lambda _: None)
    monkeypatch.setattr(sys, "executable", str(fake_python))

    resolved = _discover_mineru_cli()
    assert resolved == str(fake_magic)


def test_build_mineru_runtime_env_writes_config(tmp_path: Path) -> None:
    env = _build_mineru_runtime_env(tmp_path)
    config_path = Path(env["MINERU_TOOLS_CONFIG_JSON"])
    assert config_path.is_file()
    assert (tmp_path / "intermediates" / "book_mineru_runtime" / "models").is_dir()


def test_mineru_local_models_ready_accepts_legacy_string_models_dir(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "mineru.json"
    models_dir = tmp_path / "models"
    sentinel = models_dir / "Layout" / "YOLO" / "doclayout_yolo_docstructbench_imgsz1280_2501.pt"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("", encoding="utf-8")
    config_path.write_text(json.dumps({"models-dir": str(models_dir)}), encoding="utf-8")

    monkeypatch.setenv("MINERU_TOOLS_CONFIG_JSON", str(config_path))

    assert _mineru_local_models_ready() is True


def test_mineru_local_models_ready_accepts_pipeline_snapshot_root(tmp_path: Path, monkeypatch) -> None:
    config_path = tmp_path / "mineru.json"
    snapshot_root = tmp_path / "snapshot"
    sentinel = (
        snapshot_root
        / "models"
        / "Layout"
        / "YOLO"
        / "doclayout_yolo_docstructbench_imgsz1280_2501.pt"
    )
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    sentinel.write_text("", encoding="utf-8")
    config_path.write_text(
        json.dumps({"models-dir": {"pipeline": str(snapshot_root), "vlm": ""}}),
        encoding="utf-8",
    )

    monkeypatch.setenv("MINERU_TOOLS_CONFIG_JSON", str(config_path))

    assert _mineru_local_models_ready() is True


def test_maybe_refine_markdown_with_llm_keeps_image_marker(monkeypatch) -> None:
    source_markdown = "line-1\n![image-1](assets/book_images/a.png)\nline-2"

    monkeypatch.setenv("BOOK_PDF_MARKDOWN_LLM_FILTER_ENABLED", "1")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.setenv("BOOK_PDF_MARKDOWN_LLM_FILTER_BASE_URL", "https://api.deepseek.com/v1")
    monkeypatch.setattr(
        extractor_mod,
        "_mask_image_markers",
        lambda _: ("line-1\n[[SYS_MEDIA_TOKEN_001]]\nline-2", {"[[SYS_MEDIA_TOKEN_001]]": "![image-1](assets/book_images/a.png)"}),
    )

    class _DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": "line-1\n[[SYS_MEDIA_TOKEN_001]]\nline-2-fixed"
                        }
                    }
                ]
            }

    monkeypatch.setattr(
        extractor_mod.requests,
        "post",
        lambda *args, **kwargs: _DummyResponse(),
    )

    refined = _maybe_refine_markdown_with_llm(task_id="t1", markdown=source_markdown)
    assert "![image-1](assets/book_images/a.png)" in refined
    assert "line-2-fixed" in refined


def test_maybe_refine_markdown_with_llm_fallback_when_token_lost(monkeypatch) -> None:
    source_markdown = "line-1\n![image-1](assets/book_images/a.png)\nline-2"

    monkeypatch.setenv("BOOK_PDF_MARKDOWN_LLM_FILTER_ENABLED", "1")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.setattr(
        extractor_mod,
        "_mask_image_markers",
        lambda _: ("line-1\n[[SYS_MEDIA_TOKEN_001]]\nline-2", {"[[SYS_MEDIA_TOKEN_001]]": "![image-1](assets/book_images/a.png)"}),
    )

    class _DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": "line-1\nline-2-fixed"}}]}

    monkeypatch.setattr(
        extractor_mod.requests,
        "post",
        lambda *args, **kwargs: _DummyResponse(),
    )

    refined = _maybe_refine_markdown_with_llm(task_id="t2", markdown=source_markdown)
    assert refined == source_markdown


def test_split_markdown_into_llm_chunks_keeps_code_table_formula_atomic() -> None:
    markdown = (
        "paragraph-a line-1\nparagraph-a line-2\n\n"
        "```python\n"
        "def add(a, b):\n"
        "    return a + b\n"
        "```\n\n"
        "| colA | colB |\n"
        "| --- | --- |\n"
        "| 1 | 2 |\n\n"
        "$$\n"
        "E = mc^2\n"
        "$$\n\n"
        "paragraph-b line-1\n"
    )
    code_block = "```python\ndef add(a, b):\n    return a + b\n```"
    table_block = "| colA | colB |\n| --- | --- |\n| 1 | 2 |"
    formula_block = "$$\nE = mc^2\n$$"

    chunks = _split_markdown_into_llm_chunks(markdown=markdown, chunk_max_chars=60)

    assert len(chunks) >= 2
    assert any(code_block in chunk for chunk in chunks)
    assert any(table_block in chunk for chunk in chunks)
    assert any(formula_block in chunk for chunk in chunks)
    assert "".join(chunks) == markdown


def test_maybe_refine_markdown_with_llm_chunk_parallel_keeps_order(monkeypatch) -> None:
    source_markdown = (
        "paragraph-a line-1\nparagraph-a line-2\n\n"
        "paragraph-b line-1\nparagraph-b line-2\n\n"
        "paragraph-c line-1\nparagraph-c line-2\n\n"
        "paragraph-d line-1\nparagraph-d line-2\n"
    )
    monkeypatch.setenv("BOOK_PDF_MARKDOWN_LLM_FILTER_ENABLED", "1")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "dummy-key")
    monkeypatch.setenv("BOOK_PDF_MARKDOWN_LLM_FILTER_CHUNK_MAX_CHARS", "50")
    monkeypatch.setenv("BOOK_PDF_MARKDOWN_LLM_FILTER_MAX_WORKERS", "4")

    def _fake_refine(*args, **kwargs):
        chunk_index = kwargs.get("chunk_index")
        if chunk_index is None:
            chunk_index = args[1]
        chunk_text = kwargs.get("chunk_text")
        if chunk_text is None:
            chunk_text = args[2]
        return f"[chunk-{chunk_index}]{chunk_text}"

    monkeypatch.setattr(extractor_mod, "_refine_markdown_chunk_with_llm", _fake_refine)
    chunks = _split_markdown_into_llm_chunks(source_markdown, chunk_max_chars=50)

    refined = _maybe_refine_markdown_with_llm(task_id="t3", markdown=source_markdown)
    expected = "".join(f"[chunk-{idx}]{chunk}" for idx, chunk in enumerate(chunks))
    assert refined == expected


def test_refill_mineru_code_blocks_with_vector_text(tmp_path: Path) -> None:
    sliced_pdf = tmp_path / "sliced.pdf"
    _build_code_pdf(sliced_pdf)

    middle_json = tmp_path / "sliced_middle.json"
    payload = {
        "pdf_info": [
            {
                "page_idx": 0,
                "para_blocks": [
                    {
                        "type": "code",
                        "bbox": [60, 60, 360, 132],
                    }
                ],
            }
        ]
    }
    middle_json.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    source_markdown = "before\n\n```python\nbad ocr text\n```\n\nafter\n"
    refined = _refill_mineru_code_blocks_with_vector_text(
        task_id="code-refill",
        markdown=source_markdown,
        middle_json_path=middle_json,
        sliced_pdf_path=sliced_pdf,
    )

    assert "bad ocr text" not in refined
    assert "def hello(name):" in refined
    assert "return f\"hi {name}\"" in refined


def test_build_mineru_page_tasks_split_pdf_by_batch_size(tmp_path: Path, monkeypatch) -> None:
    source_pdf = tmp_path / "full.pdf"
    _build_multi_page_pdf(source_pdf, page_count=4)

    sliced_pdf = tmp_path / "slice.pdf"
    extractor_mod._slice_pdf(source_pdf, sliced_pdf, 1, 4)
    monkeypatch.setenv("BOOK_PDF_MINERU_PAGE_BATCH_SIZE", "2")

    tasks = _build_mineru_page_tasks(
        sliced_pdf_path=sliced_pdf,
        output_dir=tmp_path / "out",
        section_id="sec-1",
        start_page=1,
        end_page=4,
    )

    assert [(start, end) for start, end, _ in tasks] == [(1, 2), (3, 4)]
    assert all(path.is_file() for _, _, path in tasks)
    with fitz.open(tasks[0][2]) as first_slice:
        assert first_slice.page_count == 2
    with fitz.open(tasks[1][2]) as second_slice:
        assert second_slice.page_count == 2


def test_decide_mineru_parallel_workers_honors_manual_override(monkeypatch) -> None:
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKERS", "6")
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_MAX", "8")
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_MIN", "1")

    workers = _decide_mineru_parallel_workers(task_count=4)
    assert workers == 4


def test_decide_mineru_parallel_workers_limited_by_memory(monkeypatch) -> None:
    monkeypatch.delenv("BOOK_PDF_MINERU_WORKERS", raising=False)
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_MAX", "12")
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_MIN", "1")
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_CPU_DIVISOR", "2")
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_RESERVED_RAM_GB", "1.0")
    monkeypatch.setenv("BOOK_PDF_MINERU_WORKER_RAM_PER_GB", "1.0")
    monkeypatch.setattr(extractor_mod.os, "cpu_count", lambda: 16)
    monkeypatch.setattr(extractor_mod, "_read_available_memory_gb", lambda: 3.6)

    workers = _decide_mineru_parallel_workers(task_count=10)
    assert workers == 2
