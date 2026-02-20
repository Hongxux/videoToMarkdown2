import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server import douyin_download as mod


def test_extract_video_id_from_modal_id_url():
    url = "https://www.douyin.com/jingxuan?modal_id=7604776435760319796"
    assert mod._extract_video_id(url) == "7604776435760319796"


def test_extract_video_id_from_video_path_url():
    url = "https://www.douyin.com/video/7466666666666666666"
    assert mod._extract_video_id(url) == "7466666666666666666"


def test_build_candidate_page_url_prefers_video_path():
    url = "https://www.douyin.com/jingxuan?modal_id=7604776435760319796"
    assert mod._build_candidate_page_url(url) == "https://www.douyin.com/video/7604776435760319796"


def test_build_candidate_page_url_fallback_to_original():
    url = "https://www.douyin.com/"
    assert mod._build_candidate_page_url(url) == url

