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


def test_get_video_info_detects_bilibili_collection_and_current_episode(monkeypatch):
    captured = {}

    class _VideoProcessorStub:
        def __init__(self, **_kwargs):
            self.last_video_title = ""

        def probe_video_info(self, url: str):
            captured["probe_url"] = url
            return {
                "title": "合集标题",
                "duration": 999.0,
                "thumbnail": "https://img.example.com/collection.jpg",
                "entries": [
                    {
                        "playlist_index": 1,
                        "title": "第一集",
                        "duration": 101.0,
                        "webpage_url": "https://www.bilibili.com/video/BV1n9CwYoEro?p=1",
                        "thumbnail": "https://img.example.com/ep1.jpg",
                    },
                    {
                        "playlist_index": 2,
                        "title": "第二集",
                        "duration": 202.0,
                        "webpage_url": "https://www.bilibili.com/video/BV1n9CwYoEro?p=2",
                        "thumbnails": [
                            {"url": "https://img.example.com/ep2-small.jpg"},
                            {"url": "https://img.example.com/ep2-large.jpg"},
                        ],
                    },
                ],
            }

    async def _resolve_share_link_stub(raw_text: str, timeout_ms: int = 45000):
        _ = timeout_ms
        return types.SimpleNamespace(
            extracted_url=raw_text,
            resolved_url="https://www.bilibili.com/video/BV1n9CwYoEro",
            platform="bilibili",
            canonical_id="BV1n9CwYoEro",
            resolver="canonical-no-redirect",
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

    monkeypatch.setattr(impl, "VideoProcessor", _VideoProcessorStub)
    monkeypatch.setattr(impl, "resolve_share_link", _resolve_share_link_stub)

    request = types.SimpleNamespace(
        task_id="task-video-info-bili",
        video_input="https://www.bilibili.com/video/BV1n9CwYoEro?spm_id_from=333.788.videopod.episodes&p=2",
    )
    response = asyncio.run(impl._VideoProcessingServicerCore.GetVideoInfo(_ServicerStub(), request, None))

    assert response.success is True
    assert response.is_collection is True
    assert response.total_episodes == 2
    assert response.current_episode_index == 2
    assert response.current_episode_title == "第二集"
    assert response.duration_sec == 202.0
    assert response.video_title == "合集标题"
    assert response.cover_url == "https://img.example.com/ep2-large.jpg"
    assert [item.title for item in response.episodes] == ["第一集", "第二集"]
    assert [item.episode_cover_url for item in response.episodes] == [
        "https://img.example.com/ep1.jpg",
        "https://img.example.com/ep2-large.jpg",
    ]
    assert captured["probe_url"].endswith("p=2")
    assert "p=2" in response.resolved_url
    assert "spm_id_from=333.788.videopod.episodes" in response.resolved_url


def test_get_video_info_supports_bare_bv_input(monkeypatch):
    captured = {}

    class _VideoProcessorStub:
        def __init__(self, **_kwargs):
            self.last_video_title = ""

        def probe_video_info(self, url: str):
            captured["probe_url"] = url
            return {
                "title": "单集标题",
                "duration": 321.0,
                "thumbnails": [
                    {"url": "//img.example.com/single.jpg"},
                ],
            }

    async def _resolve_share_link_fail(_raw_text: str, timeout_ms: int = 45000):
        _ = timeout_ms
        raise RuntimeError("resolve unavailable")

    class _ServicerStub:
        def __init__(self):
            self.config = {"video": {}}

        def _cache_metrics_begin(self, *_args, **_kwargs):
            return None

        def _increment_tasks(self):
            return None

        def _decrement_tasks(self):
            return None

    monkeypatch.setattr(impl, "VideoProcessor", _VideoProcessorStub)
    monkeypatch.setattr(impl, "resolve_share_link", _resolve_share_link_fail)

    request = types.SimpleNamespace(
        task_id="task-video-info-bv",
        video_input="BV1n9CwYoEro",
    )
    response = asyncio.run(impl._VideoProcessingServicerCore.GetVideoInfo(_ServicerStub(), request, None))

    assert response.success is True
    assert response.is_collection is False
    assert response.total_episodes == 1
    assert response.current_episode_index == 1
    assert response.current_episode_title == "单集标题"
    assert response.video_title == "单集标题"
    assert response.source_platform == "bilibili"
    assert response.canonical_id == "BV1n9CwYoEro"
    assert response.cover_url == "https://img.example.com/single.jpg"
    assert captured["probe_url"] == "https://www.bilibili.com/video/BV1n9CwYoEro"


def test_get_video_info_parses_douyin_share_link(monkeypatch):
    """验证抖音分享文本（含短链）能正确识别平台、canonical_id 和 content_type。"""
    captured = {}

    class _VideoProcessorStub:
        def __init__(self, **_kwargs):
            self.last_video_title = ""

        def probe_video_info(self, url: str):
            captured["probe_url"] = url
            return {
                "title": "抖音测试视频标题",
                "duration": 60.0,
                "thumbnail": "https://p3.douyinpic.com/cover.jpg",
            }

    async def _resolve_share_link_stub(raw_text: str, timeout_ms: int = 45000):
        _ = timeout_ms
        return types.SimpleNamespace(
            extracted_url="https://v.douyin.com/iRNBho5/",
            resolved_url="https://www.douyin.com/video/7123456789012345678",
            platform="douyin",
            canonical_id="7123456789012345678",
            resolver="playwright",
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

    async def _probe_douyin_stub(*, video_url, timeout_ms=30000):
        captured["probe_url"] = video_url
        return {
            "title": "抖音测试视频标题",
            "duration": 60.0,
            "thumbnail": "https://p3.douyinpic.com/cover.jpg",
            "webpage_url": video_url,
        }

    monkeypatch.setattr(impl, "VideoProcessor", _VideoProcessorStub)
    monkeypatch.setattr(impl, "resolve_share_link", _resolve_share_link_stub)
    monkeypatch.setattr(impl, "_probe_douyin_video_info", _probe_douyin_stub)

    request = types.SimpleNamespace(
        task_id="task-video-info-douyin-share",
        video_input="7.47 复制打开抖音，看看【某某的作品】 https://v.douyin.com/iRNBho5/",
    )
    response = asyncio.run(impl._VideoProcessingServicerCore.GetVideoInfo(_ServicerStub(), request, None))

    assert response.success is True
    assert response.source_platform == "douyin"
    assert response.canonical_id == "7123456789012345678"
    assert response.content_type == "video"
    assert response.video_title == "抖音测试视频标题"
    assert response.duration_sec == 60.0
    assert "douyin.com/video/" in response.resolved_url


def test_get_video_info_parses_douyin_direct_url(monkeypatch):
    """验证直接抖音链接（非短链）能正确解析。"""
    captured = {}

    class _VideoProcessorStub:
        def __init__(self, **_kwargs):
            self.last_video_title = ""

        def probe_video_info(self, url: str):
            captured["probe_url"] = url
            return {
                "title": "直接链接视频",
                "duration": 120.0,
                "thumbnail": "https://p3.douyinpic.com/direct.jpg",
            }

    async def _resolve_share_link_stub(raw_text: str, timeout_ms: int = 45000):
        _ = timeout_ms
        return types.SimpleNamespace(
            extracted_url="https://www.douyin.com/video/7999888777666555444",
            resolved_url="https://www.douyin.com/video/7999888777666555444",
            platform="douyin",
            canonical_id="7999888777666555444",
            resolver="canonical-no-redirect",
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

    async def _probe_douyin_stub(*, video_url, timeout_ms=30000):
        captured["probe_url"] = video_url
        return {
            "title": "直接链接视频",
            "duration": 120.0,
            "thumbnail": "https://p3.douyinpic.com/direct.jpg",
            "webpage_url": video_url,
        }

    monkeypatch.setattr(impl, "VideoProcessor", _VideoProcessorStub)
    monkeypatch.setattr(impl, "resolve_share_link", _resolve_share_link_stub)
    monkeypatch.setattr(impl, "_probe_douyin_video_info", _probe_douyin_stub)

    request = types.SimpleNamespace(
        task_id="task-video-info-douyin-direct",
        video_input="https://www.douyin.com/video/7999888777666555444",
    )
    response = asyncio.run(impl._VideoProcessingServicerCore.GetVideoInfo(_ServicerStub(), request, None))

    assert response.success is True
    assert response.source_platform == "douyin"
    assert response.canonical_id == "7999888777666555444"
    assert response.content_type == "video"
    assert response.video_title == "直接链接视频"
    assert response.is_collection is False
    assert response.total_episodes == 1
    assert captured["probe_url"] == "https://www.douyin.com/video/7999888777666555444"

