import asyncio
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
        }
    }
    monkeypatch.setenv("YTDLP_PROXY", "http://env-proxy:7890")
    monkeypatch.setenv("YTDLP_DISABLE_SSL_VERIFY", "true")
    monkeypatch.setenv("YTDLP_COOKIES_FILE", "env_cookies.txt")
    monkeypatch.setenv("YTDLP_COOKIES_FROM_BROWSER", "chrome")
    monkeypatch.setenv("YTDLP_PREFER_H264", "false")

    options = impl._load_download_video_options(config)

    assert options["proxy"] == "http://env-proxy:7890"
    assert options["disable_ssl_verify"] is True
    assert options["cookies_file"] == "env_cookies.txt"
    assert options["cookies_from_browser"] == "chrome"
    assert options["prefer_h264"] is False


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


def test_download_video_passes_cookie_options_to_video_processor(monkeypatch, tmp_path):
    captured = {}

    class _VideoProcessorStub:
        def __init__(self, **kwargs):
            captured["init_kwargs"] = kwargs

        def download(self, url: str, output_dir: str, filename: str) -> str:
            captured["download_call"] = {
                "url": url,
                "output_dir": output_dir,
                "filename": filename,
            }
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
    assert captured["download_call"]["filename"] == "video"
