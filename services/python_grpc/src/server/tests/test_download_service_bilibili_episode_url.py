import asyncio
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.download_service import run_download_flow


def test_run_download_flow_preserves_bilibili_episode_query_for_download():
    captured = {}
    output_root = Path("var/tmp_download_service_bilibili_p")
    if output_root.exists():
        for path in sorted(output_root.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        output_root.rmdir()
    output_root.mkdir(parents=True, exist_ok=True)

    async def _resolve_share_link_stub(_raw_text: str):
        return types.SimpleNamespace(
            extracted_url="https://www.bilibili.com/video/BV1n9CwYoEro?spm_id_from=333.788.videopod.episodes&p=2",
            resolved_url="https://www.bilibili.com/video/BV1n9CwYoEro",
            platform="bilibili",
            canonical_id="BV1n9CwYoEro",
            resolver="canonical-no-redirect",
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
        raise AssertionError("douyin downloader should not be called in bilibili test")

    def _load_download_video_options(_config):
        return {}

    class _VideoProcessorStub:
        def __init__(self, **_kwargs):
            self.last_video_title = ""

        def download(self, url: str, output_dir: str, filename: str) -> str:
            captured["download_url"] = url
            output_path = Path(output_dir) / f"{filename}.mp4"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"video")
            return str(output_path)

    def _get_video_duration(_video_path: str) -> float:
        return 2.0

    def _write_video_meta_file(**_kwargs):
        return None

    logger = types.SimpleNamespace(
        info=lambda *_args, **_kwargs: None,
        warning=lambda *_args, **_kwargs: None,
        error=lambda *_args, **_kwargs: None,
    )

    result = asyncio.run(
        run_download_flow(
            task_id="task-test-bilibili-p",
            raw_video_input="https://www.bilibili.com/video/BV1n9CwYoEro?p=2",
            config={"video": {}},
            resolve_share_link=_resolve_share_link_stub,
            build_task_dir_encoding_source=_build_task_dir_encoding_source,
            get_primary_storage_root=_get_primary_storage_root,
            is_douyin_url=_is_douyin_url,
            douyin_downloader=_douyin_downloader,
            load_download_video_options=_load_download_video_options,
            video_processor_cls=_VideoProcessorStub,
            get_video_duration=_get_video_duration,
            write_video_meta_file=_write_video_meta_file,
            logger=logger,
        )
    )

    assert result.success is True
    assert "p=2" in captured["download_url"]
    assert "p=2" in result.resolved_url
    assert "spm_id_from=333.788.videopod.episodes" in captured["download_url"]
    assert "spm_id_from=333.788.videopod.episodes" in result.resolved_url
