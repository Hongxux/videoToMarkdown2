import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


try:
    from services.python_grpc.src.server import grpc_service_impl as impl
except Exception as exc:  # pragma: no cover - 受测试环境依赖版本影响
    pytest.skip(f"grpc_service_impl import unavailable: {exc}", allow_module_level=True)


def test_extract_bilibili_video_id_from_bv_path():
    url = "https://www.bilibili.com/video/BV1xx411c7mD/?spm_id_from=333.1007"
    assert impl._extract_bilibili_video_id(url) == "BV1xx411c7mD"


def test_extract_bilibili_video_id_from_av_path():
    url = "https://www.bilibili.com/video/av170001?p=2"
    assert impl._extract_bilibili_video_id(url) == "AV170001"


def test_extract_bilibili_video_id_from_query_params():
    url = "https://www.bilibili.com/video/?bvid=bv1ab411c7de&aid=999"
    assert impl._extract_bilibili_video_id(url) == "bv1ab411c7de"


def test_build_task_dir_encoding_source_prefers_bilibili_video_id():
    url = "https://m.bilibili.com/video/BV1ab411c7de?share_source=copy_web"
    assert impl._build_task_dir_encoding_source(url) == "BV1ab411c7de"


def test_build_task_dir_encoding_source_non_bilibili_keeps_original():
    url = "https://example.com/video/BV1ab411c7de"
    assert impl._build_task_dir_encoding_source(url) == url


def test_is_douyin_url_supports_main_domain():
    assert impl._is_douyin_url("https://www.douyin.com/video/7466666666666666666")


def test_is_douyin_url_supports_short_link_domain():
    assert impl._is_douyin_url("https://v.douyin.com/AbCdEfG/")


def test_is_douyin_url_rejects_non_douyin_domain():
    assert not impl._is_douyin_url("https://www.youtube.com/watch?v=dQw4w9WgXcQ")
