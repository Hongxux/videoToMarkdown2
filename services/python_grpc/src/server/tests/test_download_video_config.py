import asyncio
import json
import sys
import types
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


try:
    from services.python_grpc.src.server import grpc_service_impl as impl
except Exception as exc:  # pragma: no cover - 受测试环境依赖版本影响
    pytest.skip(f"grpc_service_impl import unavailable: {exc}", allow_module_level=True)


def test_load_download_video_options_prefers_environment(monkeypatch):
    config = {
        "video": {
            "download_proxy": "http://cfg-proxy:7890",
            "disable_ssl_verify": False,
            "download_cookies_file": "cfg_cookies.txt",
            "download_cookies_from_browser": "edge:Default",
            "prefer_h264": True,
            "short_video_max_duration_sec": 3500,
        }
    }
    monkeypatch.setenv("YTDLP_PROXY", "http://env-proxy:7890")
    monkeypatch.setenv("YTDLP_DISABLE_SSL_VERIFY", "true")
    monkeypatch.setenv("YTDLP_COOKIES_FILE", "env_cookies.txt")
    monkeypatch.setenv("YTDLP_COOKIES_FROM_BROWSER", "chrome")
    monkeypatch.setenv("YTDLP_PREFER_H264", "false")
    monkeypatch.setenv("YTDLP_SHORT_VIDEO_MAX_DURATION_SEC", "1800")

    options = impl._load_download_video_options(config)

    assert options["proxy"] == "http://env-proxy:7890"
    assert options["disable_ssl_verify"] is True
    assert options["cookies_file"] == "env_cookies.txt"
    assert options["cookies_from_browser"] == "chrome"
    assert options["prefer_h264"] is False
    assert options["short_video_max_duration_sec"] == 1800.0


def test_load_download_video_options_uses_selected_profile_when_top_level_empty(monkeypatch):
    config = {
        "video": {
            "download_profile": "public_no_cookie",
            "download_proxy": "",
            "download_cookies_file": "",
            "download_cookies_from_browser": "",
            "download_profiles": {
                "public_no_cookie": {
                    "download_proxy": "",
                    "download_cookies_file": "",
                    "download_cookies_from_browser": "",
                },
                "login_cookie": {
                    "download_proxy": "http://127.0.0.1:7897",
                    "download_cookies_file": "",
                    "download_cookies_from_browser": "edge:Default",
                },
            },
        }
    }
    monkeypatch.delenv("YTDLP_PROXY", raising=False)
    monkeypatch.delenv("YTDLP_COOKIES_FILE", raising=False)
    monkeypatch.delenv("YTDLP_COOKIES_FROM_BROWSER", raising=False)

    options_public = impl._load_download_video_options(config)
    assert options_public["proxy"] is None
    assert options_public["cookies_file"] is None
    assert options_public["cookies_from_browser"] is None

    config["video"]["download_profile"] = "login_cookie"
    options_login = impl._load_download_video_options(config)
    assert options_login["proxy"] == "http://127.0.0.1:7897"
    assert options_login["cookies_file"] is None
    assert options_login["cookies_from_browser"] == "edge:Default"


def test_load_download_video_options_non_empty_top_level_overrides_profile(monkeypatch):
    config = {
        "video": {
            "download_profile": "login_cookie",
            "download_proxy": "http://cfg-proxy:7890",
            "download_cookies_file": "manual_cookie.txt",
            "download_cookies_from_browser": "chrome",
            "download_profiles": {
                "login_cookie": {
                    "download_proxy": "http://127.0.0.1:7897",
                    "download_cookies_file": "",
                    "download_cookies_from_browser": "edge:Default",
                },
            },
        }
    }
    monkeypatch.delenv("YTDLP_PROXY", raising=False)
    monkeypatch.delenv("YTDLP_COOKIES_FILE", raising=False)
    monkeypatch.delenv("YTDLP_COOKIES_FROM_BROWSER", raising=False)

    options = impl._load_download_video_options(config)
    assert options["proxy"] == "http://cfg-proxy:7890"
    assert options["cookies_file"] == "manual_cookie.txt"
    assert options["cookies_from_browser"] == "chrome"


def test_load_download_video_options_external_downloader(monkeypatch):
    config = {
        "video": {
            "external_downloader": "aria2c",
            "external_downloader_args": [
                "--split=8",
                "--max-connection-per-server=8",
            ],
        }
    }
    monkeypatch.delenv("YTDLP_EXTERNAL_DOWNLOADER", raising=False)
    monkeypatch.delenv("YTDLP_EXTERNAL_DOWNLOADER_ARGS", raising=False)

    options = impl._load_download_video_options(config)
    assert options["external_downloader"] == "aria2c"
    assert options["external_downloader_args"] == [
        "--split=8",
        "--max-connection-per-server=8",
    ]

    monkeypatch.setenv("YTDLP_EXTERNAL_DOWNLOADER", "aria2c.exe")
    monkeypatch.setenv("YTDLP_EXTERNAL_DOWNLOADER_ARGS", "--split=12 --max-connection-per-server=12")

    env_options = impl._load_download_video_options(config)
    assert env_options["external_downloader"] == "aria2c.exe"
    assert env_options["external_downloader_args"] == [
        "--split=12",
        "--max-connection-per-server=12",
    ]


def test_download_video_passes_cookie_options_to_video_processor(monkeypatch, tmp_path):
    captured = {}

    class _VideoProcessorStub:
        def __init__(self, **kwargs):
            captured["init_kwargs"] = kwargs
            self.last_video_title = ""

        def download(self, url: str, output_dir: str, filename: str) -> str:
            captured["download_call"] = {
                "url": url,
                "output_dir": output_dir,
                "filename": filename,
            }
            self.last_video_title = "测试下载器标题"
            output_path = Path(output_dir) / f"{filename}.mp4"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"video")
            return str(output_path)

    class _ServicerStub:
        def __init__(self):
            self.config = {
                "video": {
                    "download_proxy": "http://cfg-proxy:7890",
                    "disable_ssl_verify": True,
                    "download_cookies_from_browser": "chrome",
                    "download_cookies_file": "",
                    "prefer_h264": False,
                    "short_video_max_duration_sec": 2400,
                    "external_downloader": "aria2c",
                    "external_downloader_args": ["--split=4"],
                }
            }

        def _cache_metrics_begin(self, *_args, **_kwargs):
            return None

        def _increment_tasks(self):
            return None

        def _decrement_tasks(self):
            return None

        def _get_video_duration(self, _video_path: str) -> float:
            return 1.25

    monkeypatch.setattr(impl, "VideoProcessor", _VideoProcessorStub)
    monkeypatch.setattr(impl, "_get_primary_storage_root", lambda: str(tmp_path))

    request = types.SimpleNamespace(
        task_id="task-download-cookie",
        video_url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
        output_dir="",
    )
    response = asyncio.run(impl._VideoProcessingServicerCore.DownloadVideo(_ServicerStub(), request, None))

    assert response.success is True
    assert captured["init_kwargs"]["proxy"] == "http://cfg-proxy:7890"
    assert captured["init_kwargs"]["disable_ssl_verify"] is True
    assert captured["init_kwargs"]["cookies_from_browser"] == "chrome"
    assert captured["init_kwargs"]["prefer_h264"] is False
    assert captured["init_kwargs"]["short_video_max_duration_sec"] == 2400.0
    assert captured["init_kwargs"]["external_downloader"] == "aria2c"
    assert captured["init_kwargs"]["external_downloader_args"] == ["--split=4"]
    assert captured["download_call"]["filename"] == "video"
    assert response.video_title == "测试下载器标题"


def test_download_video_routes_douyin_url_to_douyin_downloader(monkeypatch, tmp_path):
    call_log = {"video_processor_init_count": 0, "douyin_call": {}}

    class _VideoProcessorShouldNotRun:
        def __init__(self, **_kwargs):
            call_log["video_processor_init_count"] += 1

        def download(self, **_kwargs):
            raise AssertionError("VideoProcessor.download should not be called for douyin url")

    async def _douyin_downloader_stub(*, task_id: str, video_url: str, task_dir: str, video_filename: str = "video") -> str:
        call_log["douyin_call"] = {
            "task_id": task_id,
            "video_url": video_url,
            "task_dir": task_dir,
            "video_filename": video_filename,
        }
        output_path = Path(task_dir) / f"{video_filename}.mp4"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"douyin-video")
        return str(output_path)

    class _ServicerStub:
        def __init__(self):
            self.config = {"video": {}}

        def _cache_metrics_begin(self, *_args, **_kwargs):
            return None

        def _increment_tasks(self):
            return None

        def _decrement_tasks(self):
            return None

        def _get_video_duration(self, _video_path: str) -> float:
            return 2.5

    monkeypatch.setattr(impl, "VideoProcessor", _VideoProcessorShouldNotRun)
    monkeypatch.setattr(impl, "_download_video_with_douyin_downloader", _douyin_downloader_stub)
    monkeypatch.setattr(impl, "_get_primary_storage_root", lambda: str(tmp_path))

    request = types.SimpleNamespace(
        task_id="task-download-douyin",
        video_url="https://www.douyin.com/video/7466666666666666666",
        output_dir="",
    )
    response = asyncio.run(impl._VideoProcessingServicerCore.DownloadVideo(_ServicerStub(), request, None))

    assert response.success is True
    assert call_log["video_processor_init_count"] == 0
    assert call_log["douyin_call"]["video_url"] == request.video_url
    assert call_log["douyin_call"]["video_filename"] == "video"


def test_download_video_recovers_title_from_douyin_runtime_meta(monkeypatch, tmp_path):
    class _VideoProcessorShouldNotRun:
        def __init__(self, **_kwargs):
            raise AssertionError("VideoProcessor should not be initialized for douyin url")

    async def _resolve_share_link_stub(raw_text: str, timeout_ms: int = 45000):
        return types.SimpleNamespace(
            extracted_url=raw_text,
            resolved_url=raw_text,
            platform="douyin",
            canonical_id="7466666666666666666",
            resolver="canonical-no-redirect",
            title="",
            title_source="",
            title_confidence=0.0,
            content_type="video",
        )

    async def _douyin_downloader_stub(*, task_id: str, video_url: str, task_dir: str, video_filename: str = "video") -> str:
        output_path = Path(task_dir) / f"{video_filename}.mp4"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"douyin-video")
        (Path(task_dir) / "douyin_runtime_meta.json").write_text(
            json.dumps(
                {
                    "title": "抖音真实标题",
                    "author": "测试作者",
                    "video_url": "https://aweme.snssdk.com/fake.mp4",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return str(output_path)

    class _ServicerStub:
        def __init__(self):
            self.config = {"video": {}}

        def _cache_metrics_begin(self, *_args, **_kwargs):
            return None

        def _increment_tasks(self):
            return None

        def _decrement_tasks(self):
            return None

        def _get_video_duration(self, _video_path: str) -> float:
            return 1.8

    monkeypatch.setattr(impl, "VideoProcessor", _VideoProcessorShouldNotRun)
    monkeypatch.setattr(impl, "resolve_share_link", _resolve_share_link_stub)
    monkeypatch.setattr(impl, "_download_video_with_douyin_downloader", _douyin_downloader_stub)
    monkeypatch.setattr(impl, "_get_primary_storage_root", lambda: str(tmp_path))

    request = types.SimpleNamespace(
        task_id="task-download-douyin-title-fallback",
        video_url="https://www.douyin.com/video/7466666666666666666",
        output_dir="",
    )
    response = asyncio.run(impl._VideoProcessingServicerCore.DownloadVideo(_ServicerStub(), request, None))

    assert response.success is True
    assert response.video_title == "抖音真实标题"
    assert "downloader-runtime-meta" in response.link_resolver


def test_download_video_preserves_bilibili_episode_query_for_batch_collection(monkeypatch, tmp_path):
    captured = {}

    class _VideoProcessorStub:
        def __init__(self, **_kwargs):
            self.last_video_title = ""

        def download(self, url: str, output_dir: str, filename: str) -> str:
            captured["download_url"] = url
            output_path = Path(output_dir) / f"{filename}.mp4"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"video")
            return str(output_path)

    async def _resolve_share_link_stub(raw_text: str, timeout_ms: int = 45000):
        _ = timeout_ms
        _ = raw_text
        return types.SimpleNamespace(
            extracted_url="https://www.bilibili.com/video/BV1n9CwYoEro?spm_id_from=333.788.videopod.episodes&p=2",
            resolved_url="https://www.bilibili.com/video/BV1n9CwYoEro",
            platform="bilibili",
            canonical_id="BV1n9CwYoEro",
            resolver="canonical-no-redirect",
            title="",
            title_source="",
            title_confidence=0.0,
            content_type="video",
        )

    class _ServicerStub:
        def __init__(self):
            self.config = {"video": {}}

        def _cache_metrics_begin(self, *_args, **_kwargs):
            return None

        def _increment_tasks(self):
            return None

        def _decrement_tasks(self):
            return None

        def _get_video_duration(self, _video_path: str) -> float:
            return 2.0

    monkeypatch.setattr(impl, "VideoProcessor", _VideoProcessorStub)
    monkeypatch.setattr(impl, "resolve_share_link", _resolve_share_link_stub)
    monkeypatch.setattr(impl, "_get_primary_storage_root", lambda: str(tmp_path))

    request = types.SimpleNamespace(
        task_id="task-download-bilibili-episode-query",
        video_url="https://www.bilibili.com/video/BV1n9CwYoEro?p=2",
        output_dir="",
    )
    response = asyncio.run(impl._VideoProcessingServicerCore.DownloadVideo(_ServicerStub(), request, None))

    assert response.success is True
    assert "p=2" in captured["download_url"]
    assert "p=2" in response.resolved_url
