from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[7]))

from services.python_grpc.src.media_engine.knowledge_engine.core import video as video_mod

_H264_FMT = video_mod.VideoProcessor._H264_PREFERRED_SELECTOR


class _YoutubeDLSuccessStub:
    last_opts = None

    def __init__(self, opts):
        type(self).last_opts = opts

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def download(self, _urls):
        outtmpl = self.last_opts["outtmpl"]
        output_path = Path(outtmpl.replace("%(ext)s", "mp4"))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"video")


class _YoutubeDLNotBotStub:
    def __init__(self, _opts):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def download(self, _urls):
        raise Exception(
            "ERROR: [youtube] YFjfBk8HI5o: Sign in to confirm youre not a bot. "
            "Use --cookies-from-browser or --cookies for the authentication."
        )


class _CookieJarStub:
    def save(self, filename=None, ignore_discard=True, ignore_expires=True):
        _ = (ignore_discard, ignore_expires)
        Path(filename).write_text("# Netscape HTTP Cookie File", encoding="utf-8")


class _YoutubeDLFormatFallbackStub:
    calls = []

    def __init__(self, opts):
        self._opts = opts

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def extract_info(self, _url, download=False):
        _ = download
        return {
            "formats": [
                {"format_id": "sb0", "vcodec": "none", "acodec": "none", "height": 90, "tbr": 10},
                {"format_id": "96", "vcodec": "avc1.640028", "acodec": "mp4a.40.2", "height": 1080, "tbr": 4505},
            ]
        }

    def download(self, _urls):
        fmt = self._opts.get("format")
        type(self).calls.append(fmt)
        if fmt in (_H264_FMT, "best", "bestvideo+bestaudio/best"):
            raise Exception(
                "ERROR: [youtube] YFjfBk8HI5o: Requested format is not available. "
                "Use --list-formats for a list of available formats"
            )
        if fmt != "96":
            raise AssertionError(f"unexpected format fallback: {fmt}")
        outtmpl = self._opts["outtmpl"]
        output_path = Path(outtmpl.replace("%(ext)s", "mp4"))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"video")


class _YoutubeDLM3u8FallbackStub:
    calls = []

    def __init__(self, opts):
        self._opts = opts

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def extract_info(self, _url, download=False):
        _ = download
        return {
            "formats": [
                {
                    "format_id": "93",
                    "vcodec": "avc1.4D401E",
                    "acodec": "mp4a.40.2",
                    "height": 360,
                    "tbr": 804,
                    "fps": 30,
                    "protocol": "m3u8_native",
                    "url": "https://example.com/video_93.m3u8",
                }
            ]
        }

    def download(self, _urls):
        fmt = self._opts.get("format")
        type(self).calls.append(fmt)
        raise Exception(
            "ERROR: [youtube] YFjfBk8HI5o: Requested format is not available. "
            "Use --list-formats for a list of available formats"
        )


class _YoutubeDLFormatIdChainStub:
    calls = []

    def __init__(self, opts):
        self._opts = opts

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def extract_info(self, _url, download=False):
        _ = download
        return {
            "formats": [
                {"format_id": "96", "vcodec": "avc1.640028", "acodec": "mp4a.40.2", "height": 1080, "tbr": 4505},
                {"format_id": "95", "vcodec": "avc1.64001F", "acodec": "mp4a.40.2", "height": 720, "tbr": 2348},
            ]
        }

    def download(self, _urls):
        fmt = self._opts.get("format")
        type(self).calls.append(fmt)
        if fmt in (_H264_FMT, "best", "bestvideo+bestaudio/best", "96"):
            raise Exception(
                "ERROR: [youtube] YFjfBk8HI5o: Requested format is not available. "
                "Use --list-formats for a list of available formats"
            )
        if fmt != "95":
            raise AssertionError(f"unexpected format fallback: {fmt}")
        outtmpl = self._opts["outtmpl"]
        output_path = Path(outtmpl.replace("%(ext)s", "mp4"))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"video")


class _YoutubeDLProbeFailFallbackIdStub:
    calls = []

    def __init__(self, opts):
        self._opts = opts

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def extract_info(self, _url, download=False):
        _ = download
        raise Exception("probe failed")

    def download(self, _urls):
        fmt = self._opts.get("format")
        type(self).calls.append(fmt)
        if fmt in (_H264_FMT, "best", "bestvideo+bestaudio/best", "96"):
            raise Exception(
                "ERROR: [youtube] YFjfBk8HI5o: Requested format is not available. "
                "Use --list-formats for a list of available formats"
            )
        if fmt != "95":
            raise AssertionError(f"unexpected format fallback: {fmt}")
        outtmpl = self._opts["outtmpl"]
        output_path = Path(outtmpl.replace("%(ext)s", "mp4"))
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"video")


def test_download_applies_cookie_auth_options(monkeypatch, tmp_path):
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text("# Netscape HTTP Cookie File", encoding="utf-8")

    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLSuccessStub)

    processor = video_mod.VideoProcessor(
        proxy="http://127.0.0.1:7890",
        cookies_file=str(cookie_file),
        cookies_from_browser="edge:Default",
    )
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert video_path.endswith("video.mp4")
    assert _YoutubeDLSuccessStub.last_opts["format"] == _H264_FMT
    assert _YoutubeDLSuccessStub.last_opts["proxy"] == "http://127.0.0.1:7890"
    assert _YoutubeDLSuccessStub.last_opts["cookiefile"] == str(cookie_file.resolve())
    assert "cookiesfrombrowser" not in _YoutubeDLSuccessStub.last_opts


def test_download_youtube_enables_player_client_chain(monkeypatch, tmp_path):
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLSuccessStub)

    processor = video_mod.VideoProcessor()
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    extractor_args = _YoutubeDLSuccessStub.last_opts.get("extractor_args")
    assert isinstance(extractor_args, dict)
    youtube_args = extractor_args.get("youtube")
    assert isinstance(youtube_args, dict)
    player_clients = youtube_args.get("player_client")
    assert isinstance(player_clients, list)
    assert player_clients[:3] == ["web_safari", "tv_downgraded", "web"]


def test_download_fallback_to_explicit_format_id(monkeypatch, tmp_path):
    _YoutubeDLFormatFallbackStub.calls = []
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLFormatFallbackStub)

    processor = video_mod.VideoProcessor()
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert _YoutubeDLFormatFallbackStub.calls[:4] == [_H264_FMT, "best", "bestvideo+bestaudio/best", "96"]


def test_download_fallback_to_m3u8_ffmpeg(monkeypatch, tmp_path):
    _YoutubeDLM3u8FallbackStub.calls = []
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLM3u8FallbackStub)

    def _run_ffmpeg_stub(cmd, check=True):
        _ = (check,)
        output_path = Path(cmd[-1])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"video")
        return 0

    monkeypatch.setattr(video_mod.subprocess, "run", _run_ffmpeg_stub)

    processor = video_mod.VideoProcessor(proxy="http://127.0.0.1:7897")
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert _YoutubeDLM3u8FallbackStub.calls[:4] == [_H264_FMT, "best", "bestvideo+bestaudio/best", "93"]


def test_download_fallback_to_next_explicit_format_id(monkeypatch, tmp_path):
    _YoutubeDLFormatIdChainStub.calls = []
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLFormatIdChainStub)

    processor = video_mod.VideoProcessor()
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert _YoutubeDLFormatIdChainStub.calls[:5] == [_H264_FMT, "best", "bestvideo+bestaudio/best", "96", "95"]


def test_download_fallback_to_fixed_youtube_id_chain_when_probe_fails(monkeypatch, tmp_path):
    _YoutubeDLProbeFailFallbackIdStub.calls = []
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLProbeFailFallbackIdStub)

    processor = video_mod.VideoProcessor()
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert _YoutubeDLProbeFailFallbackIdStub.calls[:5] == [_H264_FMT, "best", "bestvideo+bestaudio/best", "96", "95"]


def test_download_can_disable_h264_preference(monkeypatch, tmp_path):
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLSuccessStub)

    processor = video_mod.VideoProcessor(prefer_h264=False)
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert _YoutubeDLSuccessStub.last_opts["format"] == "best"


def test_download_auto_exports_cookie_file_from_browser(monkeypatch, tmp_path):
    cookie_file = tmp_path / "cookies_auto.txt"
    called = {}

    def _extract_stub(browser_name, profile=None, logger=None, *, keyring=None, container=None):
        _ = logger
        called["browser_name"] = browser_name
        called["profile"] = profile
        called["keyring"] = keyring
        called["container"] = container
        return _CookieJarStub()

    monkeypatch.setattr(video_mod, "extract_cookies_from_browser", _extract_stub)
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLSuccessStub)

    processor = video_mod.VideoProcessor(
        cookies_file=str(cookie_file),
        cookies_from_browser="edge:Default",
    )
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert cookie_file.exists()
    assert called["browser_name"] == "edge"
    assert called["profile"] == "Default"
    assert _YoutubeDLSuccessStub.last_opts["cookiefile"] == str(cookie_file.resolve())


def test_download_auto_downgrades_to_browser_when_cookie_file_missing(monkeypatch, tmp_path):
    cookie_file = tmp_path / "cookies_missing.txt"

    def _extract_fail_stub(browser_name, profile=None, logger=None, *, keyring=None, container=None):
        _ = (browser_name, profile, logger, keyring, container)
        raise Exception(
            "Could not copy Chrome cookie database. "
            "See https://github.com/yt-dlp/yt-dlp/issues/7271 for more info"
        )

    monkeypatch.setattr(video_mod, "extract_cookies_from_browser", _extract_fail_stub)
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLSuccessStub)

    processor = video_mod.VideoProcessor(
        cookies_file=str(cookie_file),
        cookies_from_browser="edge:Default",
    )
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert not cookie_file.exists()
    assert _YoutubeDLSuccessStub.last_opts["cookiesfrombrowser"] == ("edge", "Default")
    assert "cookiefile" not in _YoutubeDLSuccessStub.last_opts


def test_download_not_bot_error_contains_cookie_guidance(monkeypatch, tmp_path):
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLNotBotStub)

    processor = video_mod.VideoProcessor()
    with pytest.raises(RuntimeError) as exc_info:
        processor.download(
            url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
            output_dir=str(tmp_path),
            filename="video",
        )

    message = str(exc_info.value)
    assert "download_cookies_from_browser" in message
    assert "not a bot" in message.lower()


def test_download_chrome_cookie_copy_error_has_specific_hint(monkeypatch, tmp_path):
    class _YoutubeDLChromeCopyFailStub:
        def __init__(self, _opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def download(self, _urls):
            raise Exception(
                "ERROR: Could not copy Chrome cookie database. "
                "See https://github.com/yt-dlp/yt-dlp/issues/7271 for more info"
            )

    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLChromeCopyFailStub)

    processor = video_mod.VideoProcessor(cookies_from_browser="chrome")
    with pytest.raises(RuntimeError) as exc_info:
        processor.download(
            url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
            output_dir=str(tmp_path),
            filename="video",
        )

    message = str(exc_info.value)
    assert "Chrome Cookie 数据库" in message
    assert "YTDLP_COOKIES_FROM_BROWSER" in message


def test_download_retries_without_cookie_when_browser_cookie_copy_fails(monkeypatch, tmp_path):
    class _YoutubeDLCookieRetryStub:
        calls = []

        def __init__(self, opts):
            self._opts = opts

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def download(self, _urls):
            type(self).calls.append(dict(self._opts))
            if "cookiefile" in self._opts or "cookiesfrombrowser" in self._opts:
                raise Exception(
                    "ERROR: Could not copy Chrome cookie database. "
                    "See https://github.com/yt-dlp/yt-dlp/issues/7271 for more info"
                )
            outtmpl = self._opts["outtmpl"]
            output_path = Path(outtmpl.replace("%(ext)s", "mp4"))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"video")

    _YoutubeDLCookieRetryStub.calls = []
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLCookieRetryStub)

    processor = video_mod.VideoProcessor(cookies_from_browser="edge:Default")
    video_path = processor.download(
        url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert len(_YoutubeDLCookieRetryStub.calls) >= 2
    first_call = _YoutubeDLCookieRetryStub.calls[0]
    last_call = _YoutubeDLCookieRetryStub.calls[-1]
    assert "cookiesfrombrowser" in first_call
    assert "cookiesfrombrowser" not in last_call
    assert "cookiefile" not in last_call


def test_download_retries_without_cookie_when_dpapi_decrypt_fails(monkeypatch, tmp_path):
    class _YoutubeDLDpapiRetryStub:
        calls = []

        def __init__(self, opts):
            self._opts = opts

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def download(self, _urls):
            type(self).calls.append(dict(self._opts))
            if "cookiefile" in self._opts or "cookiesfrombrowser" in self._opts:
                raise Exception(
                    "ERROR: Failed to decrypt with DPAPI. "
                    "See https://github.com/yt-dlp/yt-dlp/issues/10927 for more info"
                )
            outtmpl = self._opts["outtmpl"]
            output_path = Path(outtmpl.replace("%(ext)s", "mp4"))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"video")

    _YoutubeDLDpapiRetryStub.calls = []
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLDpapiRetryStub)

    processor = video_mod.VideoProcessor(cookies_from_browser="edge:Default")
    video_path = processor.download(
        url="https://www.bilibili.com/video/BV17YCEZ5EAQ",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert len(_YoutubeDLDpapiRetryStub.calls) >= 2
    first_call = _YoutubeDLDpapiRetryStub.calls[0]
    last_call = _YoutubeDLDpapiRetryStub.calls[-1]
    assert "cookiesfrombrowser" in first_call
    assert "cookiesfrombrowser" not in last_call
    assert "cookiefile" not in last_call


def test_download_retries_without_proxy_when_gateway_502(monkeypatch, tmp_path):
    class _YoutubeDLProxy502RetryStub:
        calls = []

        def __init__(self, opts):
            self._opts = opts

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def download(self, _urls):
            type(self).calls.append(dict(self._opts))
            if self._opts.get("proxy"):
                raise Exception(
                    "ERROR: [generic] Unable to download webpage: HTTP Error 502: Bad Gateway "
                    "(caused by <HTTPError 502: Bad Gateway>)"
                )
            outtmpl = self._opts["outtmpl"]
            output_path = Path(outtmpl.replace("%(ext)s", "mp4"))
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"video")

    _YoutubeDLProxy502RetryStub.calls = []
    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLProxy502RetryStub)

    processor = video_mod.VideoProcessor(proxy="http://127.0.0.1:7897")
    video_path = processor.download(
        url="https://www.bilibili.com/video/BV17YCEZ5EAQ",
        output_dir=str(tmp_path),
        filename="video",
    )

    assert Path(video_path).exists()
    assert len(_YoutubeDLProxy502RetryStub.calls) >= 2
    first_call = _YoutubeDLProxy502RetryStub.calls[0]
    last_call = _YoutubeDLProxy502RetryStub.calls[-1]
    assert first_call.get("proxy") == "http://127.0.0.1:7897"
    assert "proxy" not in last_call


def test_download_proxy_connection_error_has_specific_hint(monkeypatch, tmp_path):
    class _YoutubeDLProxyFailStub:
        def __init__(self, _opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def download(self, _urls):
            raise Exception(
                "ERROR: [youtube] YFjfBk8HI5o: Unable to download API page: "
                "('Unable to connect to proxy', NewConnectionError(\"HTTPSConnection(host='127.0.0.1', "
                "port=7890): Failed to establish a new connection: [WinError 10061] target actively refused\")) "
                "(caused by ProxyError(...))"
            )

    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLProxyFailStub)

    processor = video_mod.VideoProcessor(proxy="http://127.0.0.1:7890")
    with pytest.raises(RuntimeError) as exc_info:
        processor.download(
            url="https://www.youtube.com/watch?v=YFjfBk8HI5o",
            output_dir=str(tmp_path),
            filename="video",
        )

    message = str(exc_info.value)
    assert "连接代理失败" in message
    assert "7890" in message


def test_download_geo_restricted_error_has_specific_hint(monkeypatch, tmp_path):
    class _YoutubeDLGeoRestrictedStub:
        def __init__(self, _opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def download(self, _urls):
            raise Exception(
                "ERROR: [BiliBili] 1VPMXBJEBS: This video may be deleted or geo-restricted. "
                "You might want to try a VPN or a proxy server (with --proxy)"
            )

    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLGeoRestrictedStub)

    processor = video_mod.VideoProcessor(proxy="http://127.0.0.1:7897")
    with pytest.raises(RuntimeError) as exc_info:
        processor.download(
            url="https://www.bilibili.com/video/BV1VPMXBJEBS",
            output_dir=str(tmp_path),
            filename="video",
        )

    message = str(exc_info.value)
    assert ("地区限制" in message) or ("地域限制" in message)
    assert "YTDLP_PROXY" in message
    assert "download_cookies_file" in message


def test_download_bilibili_bvid_extractor_error_has_specific_hint(monkeypatch, tmp_path):
    class _YoutubeDLBilibiliBvidExtractorFailStub:
        def __init__(self, _opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def download(self, _urls):
            raise Exception(
                "ERROR: [BiliBili] 1XKIJBSEBJ: An extractor error has occurred. "
                "(caused by KeyError('bvid'))"
            )

    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLBilibiliBvidExtractorFailStub)

    processor = video_mod.VideoProcessor()
    with pytest.raises(RuntimeError) as exc_info:
        processor.download(
            url="https://www.bilibili.com/video/BV1XKIJBSEBJ",
            output_dir=str(tmp_path),
            filename="video",
        )

    message = str(exc_info.value)
    assert "bvid" in message.lower()
    assert ("大小写" in message) or ("原始链接" in message)


def test_download_bilibili_bvid_extractor_error_without_site_prefix_has_specific_hint(monkeypatch, tmp_path):
    class _YoutubeDLBvidExtractorNoSitePrefixFailStub:
        def __init__(self, _opts):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def download(self, _urls):
            raise Exception(
                "ERROR: 1XKIJBSEBJ: An extractor error has occurred. "
                "(caused by KeyError('bvid'))"
            )

    monkeypatch.setattr(video_mod.yt_dlp, "YoutubeDL", _YoutubeDLBvidExtractorNoSitePrefixFailStub)

    processor = video_mod.VideoProcessor()
    with pytest.raises(RuntimeError) as exc_info:
        processor.download(
            url="https://www.bilibili.com/video/BV1XKIJBSEBJ",
            output_dir=str(tmp_path),
            filename="video",
        )

    message = str(exc_info.value)
    assert "bvid" in message.lower()
    assert ("大小写" in message) or ("原始链接" in message)
