import asyncio
import shutil
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.download_service import _compute_backoff_delay_sec, run_download_flow


def _build_logger_stub():
    return types.SimpleNamespace(
        info=lambda *_args, **_kwargs: None,
        warning=lambda *_args, **_kwargs: None,
        error=lambda *_args, **_kwargs: None,
    )


def _prepare_output_root(name: str) -> Path:
    output_root = Path("var") / name
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    return output_root


def test_compute_backoff_delay_sec_exponential_and_cap():
    assert _compute_backoff_delay_sec(retry_index=1, base_delay_sec=1.0, max_delay_sec=16.0) == 1.0
    assert _compute_backoff_delay_sec(retry_index=2, base_delay_sec=1.0, max_delay_sec=16.0) == 2.0
    assert _compute_backoff_delay_sec(retry_index=3, base_delay_sec=1.0, max_delay_sec=16.0) == 4.0
    assert _compute_backoff_delay_sec(retry_index=5, base_delay_sec=1.0, max_delay_sec=6.0) == 6.0


def test_run_download_flow_retries_then_succeeds():
    output_root = _prepare_output_root("tmp_download_service_retry_success")
    captured = {"calls": 0}

    async def _resolve_share_link_stub(raw_text: str):
        return types.SimpleNamespace(
            extracted_url=raw_text,
            resolved_url=raw_text,
            platform="",
            canonical_id="",
            resolver="",
            title="",
            content_type="video",
        )

    def _build_task_dir_encoding_source(video_url: str) -> str:
        return str(video_url or "")

    def _get_primary_storage_root() -> str:
        return str(output_root)

    def _is_douyin_url(_video_url: str) -> bool:
        return False

    async def _douyin_downloader(**_kwargs):
        raise AssertionError("douyin downloader should not be called")

    def _load_download_video_options(_config):
        return {}

    class _VideoProcessorStub:
        def __init__(self, **_kwargs):
            self.last_video_title = "retry success title"

        def download(self, url: str, output_dir: str, filename: str) -> str:
            _ = url
            captured["calls"] += 1
            if captured["calls"] < 3:
                raise RuntimeError("yt-dlp execution failed: ERROR: aria2c exited with code 1")
            output_path = Path(output_dir) / f"{filename}.mp4"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"video")
            return str(output_path)

    def _get_video_duration(_video_path: str) -> float:
        return 1.0

    def _write_video_meta_file(**_kwargs):
        return None

    result = asyncio.run(
        run_download_flow(
            task_id="task-download-retry-success",
            raw_video_input="https://example.com/video",
            config={
                "video": {
                    "download_retry_attempts": 3,
                    "download_retry_base_delay_sec": 0,
                    "download_retry_max_delay_sec": 0,
                }
            },
            resolve_share_link=_resolve_share_link_stub,
            build_task_dir_encoding_source=_build_task_dir_encoding_source,
            get_primary_storage_root=_get_primary_storage_root,
            is_douyin_url=_is_douyin_url,
            douyin_downloader=_douyin_downloader,
            load_download_video_options=_load_download_video_options,
            video_processor_cls=_VideoProcessorStub,
            get_video_duration=_get_video_duration,
            write_video_meta_file=_write_video_meta_file,
            logger=_build_logger_stub(),
        )
    )

    assert result.success is True
    assert result.video_title == "retry success title"
    assert captured["calls"] == 3


def test_run_download_flow_retry_exhausted_returns_failure():
    output_root = _prepare_output_root("tmp_download_service_retry_fail")
    captured = {"calls": 0}

    async def _resolve_share_link_stub(raw_text: str):
        return types.SimpleNamespace(
            extracted_url=raw_text,
            resolved_url=raw_text,
            platform="",
            canonical_id="",
            resolver="",
            title="",
            content_type="video",
        )

    def _build_task_dir_encoding_source(video_url: str) -> str:
        return str(video_url or "")

    def _get_primary_storage_root() -> str:
        return str(output_root)

    def _is_douyin_url(_video_url: str) -> bool:
        return False

    async def _douyin_downloader(**_kwargs):
        raise AssertionError("douyin downloader should not be called")

    def _load_download_video_options(_config):
        return {}

    class _VideoProcessorStub:
        def __init__(self, **_kwargs):
            self.last_video_title = ""

        def download(self, url: str, output_dir: str, filename: str) -> str:
            _ = url
            _ = output_dir
            _ = filename
            captured["calls"] += 1
            raise RuntimeError("fatal download error")

    def _get_video_duration(_video_path: str) -> float:
        return 1.0

    def _write_video_meta_file(**_kwargs):
        return None

    result = asyncio.run(
        run_download_flow(
            task_id="task-download-retry-fail",
            raw_video_input="https://example.com/video",
            config={
                "video": {
                    "download_retry_attempts": 3,
                    "download_retry_base_delay_sec": 0,
                    "download_retry_max_delay_sec": 0,
                }
            },
            resolve_share_link=_resolve_share_link_stub,
            build_task_dir_encoding_source=_build_task_dir_encoding_source,
            get_primary_storage_root=_get_primary_storage_root,
            is_douyin_url=_is_douyin_url,
            douyin_downloader=_douyin_downloader,
            load_download_video_options=_load_download_video_options,
            video_processor_cls=_VideoProcessorStub,
            get_video_duration=_get_video_duration,
            write_video_meta_file=_write_video_meta_file,
            logger=_build_logger_stub(),
        )
    )

    assert result.success is False
    assert "fatal download error" in result.error_msg
    assert captured["calls"] == 3
