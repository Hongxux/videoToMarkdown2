import asyncio
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server import share_link_resolver as mod


def test_resolve_douyin_share_text_with_short_link(monkeypatch):
    share_text = (
        "0.23 复制打开抖音，看看【撞到了鬼的图文作品】我叫请勿# 上热门话题🔥 "
        "@抖音小助手 @抖音创作... https://v.douyin.com/Q1ymsCrYEa0/ aNW:/ F@u.fb 10/29"
    )

    async def _fake_playwright(url: str, timeout_ms: int = 45000):
        assert "v.douyin.com" in url
        return ("https://www.douyin.com/jingxuan?modal_id=7604776435760319796", "撞到了鬼的图文作品")

    monkeypatch.setattr(mod, "_resolve_url_with_playwright", _fake_playwright)

    resolved = asyncio.run(mod.resolve_share_link(share_text))
    assert resolved.platform == "douyin"
    assert resolved.canonical_id == "7604776435760319796"
    assert resolved.resolved_url == "https://www.douyin.com/video/7604776435760319796"
    assert resolved.resolver == "playwright"
    assert resolved.title == "撞到了鬼的图文作品"


def test_resolve_bilibili_short_link(monkeypatch):
    share_text = "【从夯到拉，锐评 39 个前端技术！-哔哩哔哩】 https://b23.tv/sAEO5dN"

    async def _fake_playwright(url: str, timeout_ms: int = 45000):
        assert "b23.tv" in url
        return ("https://www.bilibili.com/video/BV1xx411c7mD/?share_source=copy_web", "从夯到拉，锐评 39 个前端技术！-哔哩哔哩")

    monkeypatch.setattr(mod, "_resolve_url_with_playwright", _fake_playwright)

    resolved = asyncio.run(mod.resolve_share_link(share_text))
    assert resolved.platform == "bilibili"
    assert resolved.canonical_id == "BV1xx411c7mD"
    assert resolved.resolved_url == "https://www.bilibili.com/video/BV1xx411c7mD"
    assert resolved.resolver == "playwright"
    assert resolved.title == "从夯到拉，锐评 39 个前端技术！"


def test_resolve_prefers_embedded_title_when_page_title_empty(monkeypatch):
    share_text = "看看【撞到了鬼的图文作品】 https://v.douyin.com/AbCdEf/"

    async def _fake_playwright(url: str, timeout_ms: int = 45000):
        return ("https://www.douyin.com/note/7598573188708049137", "")

    monkeypatch.setattr(mod, "_resolve_url_with_playwright", _fake_playwright)

    resolved = asyncio.run(mod.resolve_share_link(share_text))
    assert resolved.platform == "douyin"
    assert resolved.title == "撞到了鬼的图文作品"


def test_resolve_plain_bilibili_link_without_redirect(monkeypatch):
    url = "https://www.bilibili.com/video/BV17YCEZ5EAQ/?spm_id_from=333.1007"

    async def _fake_playwright(url: str, timeout_ms: int = 45000):
        return (url, "BV17YCEZ5EAQ")

    monkeypatch.setattr(mod, "_resolve_url_with_playwright", _fake_playwright)

    resolved = asyncio.run(mod.resolve_share_link(url))
    assert resolved.platform == "bilibili"
    assert resolved.canonical_id == "BV17YCEZ5EAQ"
    assert resolved.resolved_url == "https://www.bilibili.com/video/BV17YCEZ5EAQ"
    assert resolved.resolver == "canonical-no-redirect"
