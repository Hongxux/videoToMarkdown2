"""
模块说明：视频转Markdown流程中的 video 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import os
import subprocess
import yt_dlp
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from yt_dlp.cookies import extract_cookies_from_browser
from .processing import BaseProcessor

class VideoProcessor(BaseProcessor):
    """类说明：VideoProcessor 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""

    _H264_PREFERRED_SELECTOR = (
        "bestvideo[vcodec~='^(avc1|h264)']+bestaudio[acodec~='^(mp4a|aac)']/"
        "best[vcodec~='^(avc1|h264)'][acodec~='^(mp4a|aac)']/"
        "best[ext=mp4]/best"
    )
    _DEFAULT_FORMAT_CANDIDATES = (
        "best",
        "bestvideo+bestaudio/best",
    )
    _H264_FIRST_FORMAT_CANDIDATES = (
        _H264_PREFERRED_SELECTOR,
        "best",
        "bestvideo+bestaudio/best",
    )
    _SHORT_VIDEO_MAX_DURATION_SEC = 3600.0
    _YOUTUBE_HLS_FALLBACK_FORMAT_IDS = ("96", "95", "94", "93", "92", "91")
    _YOUTUBE_PLAYER_CLIENT_CHAIN = ("web_safari", "tv_downgraded", "web")
    
    def __init__(
        self,
        on_progress=None,
        proxy=None,
        disable_ssl_verify=False,
        cookies_file: Optional[str] = None,
        cookies_from_browser: Optional[str] = None,
        prefer_h264: bool = True,
        short_video_max_duration_sec: Optional[float] = None,
        external_downloader: Optional[str] = None,
        external_downloader_args: Optional[list[str]] = None,
    ):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - on_progress: 函数入参（类型：未标注）。
        - proxy: 函数入参（类型：未标注）。
        - disable_ssl_verify: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        super().__init__(on_progress=on_progress)
        self.proxy = proxy
        self.disable_ssl_verify = disable_ssl_verify
        self.cookies_file = cookies_file
        self.cookies_from_browser = cookies_from_browser
        self.prefer_h264 = bool(prefer_h264)
        self.short_video_max_duration_sec = self._normalize_short_video_max_duration_sec(
            short_video_max_duration_sec
        )
        raw_external_downloader = str(external_downloader or "").strip()
        self.external_downloader = raw_external_downloader or None
        self.external_downloader_args = [
            str(arg).strip() for arg in (external_downloader_args or []) if str(arg).strip()
        ]
        self._cookie_export_attempted = False
        self._cookie_export_error: Optional[str] = None
        self._last_explicit_probe_error: Optional[str] = None
        self._last_m3u8_probe_error: Optional[str] = None
        self._last_video_title: str = ""

    @property
    def last_video_title(self) -> str:
        return str(self._last_video_title or "").strip()

    def _capture_title_from_info_dict(self, info: Any) -> None:
        if not isinstance(info, dict):
            return
        candidates = []
        candidates.append(info.get("title"))
        first_entry = None
        entries = info.get("entries")
        if isinstance(entries, list) and entries:
            first_entry = entries[0]
        if isinstance(first_entry, dict):
            candidates.append(first_entry.get("title"))
        for raw in candidates:
            title = " ".join(str(raw or "").split()).strip()
            if title:
                self._last_video_title = title
                return

    def _get_format_candidates(self) -> Tuple[str, ...]:
        """
        做什么：返回本次下载的 format selector 回退链。
        为什么：优先尝试 H.264，可减少后续 OpenCV 解码兼容问题。
        权衡：优先 H.264 可能牺牲部分极限码率；可通过配置关闭。
        """
        if self.prefer_h264:
            return self._H264_FIRST_FORMAT_CANDIDATES
        return self._DEFAULT_FORMAT_CANDIDATES

    @classmethod
    def _normalize_short_video_max_duration_sec(cls, raw_value: Optional[float]) -> float:
        """
        做什么：规范化短视频阈值配置。
        为什么：统一兜底行为，避免非法值导致格式策略异常。
        权衡：非法/非正值会回退默认阈值，不支持“0=关闭”语义。
        """
        try:
            value = float(raw_value)
        except Exception:
            return cls._SHORT_VIDEO_MAX_DURATION_SEC
        if value <= 0:
            return cls._SHORT_VIDEO_MAX_DURATION_SEC
        return value

    @staticmethod
    def _extract_duration_from_info(info: Dict[str, Any]) -> Optional[float]:
        """从 yt-dlp 探测结果提取时长（秒）。"""
        raw_duration = info.get("duration")
        try:
            duration = float(raw_duration)
            if duration > 0:
                return duration
        except Exception:
            return None
        return None

    @classmethod
    def _prioritize_short_video_highest_resolution_candidates(
        cls,
        base_candidates: Tuple[str, ...],
    ) -> Tuple[str, ...]:
        """
        做什么：将短视频格式链重排为“最高分辨率优先”。
        为什么：短视频体量相对可控，优先画质收益更大。
        权衡：可能增加编解码压力，因此仅对 1 小时以内视频启用。
        """
        ordered = ["bestvideo+bestaudio/best", *base_candidates]
        deduplicated: list[str] = []
        for item in ordered:
            selector = str(item or "").strip()
            if selector and selector not in deduplicated:
                deduplicated.append(selector)
        return tuple(deduplicated)

    def _probe_video_duration_for_format_selection(
        self,
        *,
        url: str,
        base_opts: Dict[str, Any],
    ) -> Optional[float]:
        """
        做什么：在正式下载前探测视频时长，用于决定格式优先级。
        为什么：满足“短视频优先最高分辨率”的下载策略。
        权衡：会增加一次轻量 metadata 请求，但可换来更稳定的画质策略。
        """
        probe_opts = dict(base_opts)
        probe_opts.pop("format", None)
        try:
            with yt_dlp.YoutubeDL(probe_opts) as ydl:
                info = ydl.extract_info(url, download=False)
            if isinstance(info, dict):
                return self._extract_duration_from_info(info)
        except Exception:
            return None
        return None

    @staticmethod
    def _is_youtube_url(url: str) -> bool:
        lower_url = (url or "").lower()
        return "youtube.com/" in lower_url or "youtu.be/" in lower_url

    def _with_youtube_player_client_chain(self, opts: Dict[str, Any]) -> Dict[str, Any]:
        """为 YouTube 场景补充更稳的 player_client 提取顺序。"""
        merged = dict(opts)
        extractor_args = merged.get("extractor_args")
        if not isinstance(extractor_args, dict):
            extractor_args = {}
        else:
            extractor_args = dict(extractor_args)

        youtube_args_raw = extractor_args.get("youtube")
        if isinstance(youtube_args_raw, dict):
            youtube_args = dict(youtube_args_raw)
        else:
            youtube_args = {}

        existing_clients = youtube_args.get("player_client") or []
        if isinstance(existing_clients, str):
            existing_clients = [existing_clients]
        elif not isinstance(existing_clients, list):
            existing_clients = []

        merged_clients = []
        for c in [*self._YOUTUBE_PLAYER_CLIENT_CHAIN, *existing_clients]:
            value = str(c).strip()
            if value and value not in merged_clients:
                merged_clients.append(value)

        youtube_args["player_client"] = merged_clients
        extractor_args["youtube"] = youtube_args
        merged["extractor_args"] = extractor_args
        return merged

    @staticmethod
    def _parse_cookies_from_browser(raw_value: Optional[str]) -> Optional[Tuple[Any, ...]]:
        """解析 cookies-from-browser 字符串为 yt-dlp 需要的 tuple。"""
        if not raw_value:
            return None
        value = str(raw_value).strip()
        if not value:
            return None
        parts = [part.strip() for part in value.split(":")]
        normalized = [part if part else None for part in parts[:4]]
        while normalized and normalized[-1] is None:
            normalized.pop()
        return tuple(normalized) if normalized else None

    def _build_auth_options(self) -> Dict[str, Any]:
        """构建 yt-dlp 认证参数（代理 + Cookie 文件优先 + 浏览器 Cookie 兜底）。"""
        opts: Dict[str, Any] = {}
        if self.proxy:
            opts["proxy"] = self.proxy

        self._maybe_export_cookie_file_from_browser()
        browser_opt = self._parse_cookies_from_browser(self.cookies_from_browser)

        if self.cookies_file:
            cookie_path = os.path.abspath(os.path.expanduser(self.cookies_file))
            if os.path.isfile(cookie_path):
                opts["cookiefile"] = cookie_path
            elif browser_opt:
                # 做什么：当 cookie 文件缺失时自动降级为浏览器直读。
                # 为什么：避免“文件缺失/导出失败”成为硬阻断，让下载链路继续推进。
                # 权衡：若浏览器库仍不可读，后续会在 yt-dlp 阶段报更准确的浏览器权限错误。
                opts["cookiesfrombrowser"] = browser_opt
                if self._cookie_export_error:
                    self.emit_progress(
                        "download",
                        0.11,
                        f"自动导出 Cookie 失败，已降级为浏览器直读: {self.cookies_from_browser}",
                    )
                else:
                    self.emit_progress(
                        "download",
                        0.11,
                        f"Cookie 文件缺失，已降级为浏览器直读: {self.cookies_from_browser}",
                    )
            else:
                if self._cookie_export_error:
                    raise FileNotFoundError(
                        f"Cookie 文件不存在: {cookie_path}。自动导出失败原因: {self._cookie_export_error}"
                    )
                raise FileNotFoundError(f"Cookie 文件不存在: {cookie_path}")
        else:
            if browser_opt:
                opts["cookiesfrombrowser"] = browser_opt
        return opts

    def _maybe_export_cookie_file_from_browser(self) -> None:
        """当同时配置 browser 与 cookiefile 时，自动导出浏览器 Cookie 到文件。"""
        if self._cookie_export_attempted:
            return
        if not (self.cookies_file and self.cookies_from_browser):
            return

        self._cookie_export_attempted = True
        browser_spec = self._parse_cookies_from_browser(self.cookies_from_browser)
        if not browser_spec:
            return

        browser_name = browser_spec[0]
        profile = browser_spec[1] if len(browser_spec) > 1 else None
        keyring = browser_spec[2] if len(browser_spec) > 2 else None
        container = browser_spec[3] if len(browser_spec) > 3 else None
        cookie_path = os.path.abspath(os.path.expanduser(self.cookies_file))
        cookie_parent = os.path.dirname(cookie_path)
        if cookie_parent:
            os.makedirs(cookie_parent, exist_ok=True)

        try:
            jar = extract_cookies_from_browser(
                browser_name,
                profile=profile,
                keyring=keyring,
                container=container,
            )
            jar.save(cookie_path, ignore_discard=True, ignore_expires=True)
            self.emit_progress("download", 0.11, f"已自动导出浏览器 Cookie 到: {cookie_path}")
            self._cookie_export_error = None
        except Exception as exc:
            self._cookie_export_error = str(exc)
            if os.path.isfile(cookie_path):
                self.emit_progress(
                    "download",
                    0.11,
                    f"自动导出 Cookie 失败，回退使用现有文件: {cookie_path}",
                )

    def _build_download_error_message(self, err: Exception) -> str:
        """将 yt-dlp 原始错误包装为可执行的修复提示。"""
        raw = str(err)
        lower_raw = raw.lower()
        browser_cookie_access_failed = self._is_browser_cookie_access_error(err)
        if browser_cookie_access_failed:
            configured_browser = self.cookies_from_browser or "chrome"
            return (
                "yt-dlp 读取浏览器 Cookie 失败（可能是 Chrome Cookie 数据库复制失败或 DPAPI 解密失败）。"
                " 常见原因是浏览器进程占用、服务权限上下文不一致，或系统密钥不可用。"
                f" 当前 browser 配置: `{configured_browser}`。"
                " 请尝试：1) 完全退出 Chrome（含后台进程）；"
                "2) 让服务与浏览器在同一系统用户和同一权限级别下运行；"
                "3) 切换为 `download_cookies_from_browser: edge:Default`；"
                "4) 改用 `download_cookies_file`。"
                " 若已改配置仍提示 Chrome，请检查环境变量 `YTDLP_COOKIES_FROM_BROWSER` 是否仍为 `chrome`。"
                f" 原始错误: {raw}"
            )

        proxy_connection_failed = (
            "unable to connect to proxy" in lower_raw
            or "proxyerror" in lower_raw
            or "winerror 10061" in lower_raw
        )
        if proxy_connection_failed:
            configured_proxy = self.proxy or "(empty)"
            return (
                "yt-dlp 连接代理失败，当前下载未进入视频格式选择阶段。"
                f" 当前 proxy 配置: `{configured_proxy}`。"
                " 请检查代理进程是否已启动，以及端口是否正确（例如你命令行可用的是 7897，但服务当前是 7890）。"
                " 若暂不使用代理，请清空 `video.download_proxy` 与环境变量 `YTDLP_PROXY` 后重试。"
                f" 原始错误: {raw}"
            )

        gateway_failed = self._is_gateway_bad_response_error(err)
        if gateway_failed:
            configured_proxy = self.proxy or "(empty)"
            if self.proxy:
                return (
                    "yt-dlp 请求目标站点失败（网关错误 502/503/504），当前代理出口疑似异常。"
                    f" 当前 proxy 配置: `{configured_proxy}`。"
                    " 请优先更换代理节点或检查代理上游连通性；"
                    " 若需快速验证，可临时清空 `video.download_proxy` 与 `YTDLP_PROXY` 后重试。"
                    f" 原始错误: {raw}"
                )
            return (
                "yt-dlp 请求目标站点失败（网关错误 502/503/504）。"
                " 这通常是站点上游临时故障或当前网络出口链路异常。"
                " 请稍后重试，或切换可用代理出口后再试。"
                f" 原始错误: {raw}"
            )

        bilibili_bvid_extractor_failed = self._is_bilibili_bvid_extractor_error(err)
        if bilibili_bvid_extractor_failed:
            return (
                "yt-dlp 解析 Bilibili 页面失败（未提取到 bvid 字段）。"
                " 这通常是链接本身不可见/无效，或 BV 号大小写与原始链接不一致导致。"
                " 请先在浏览器确认该链接可直接播放，并尽量使用页面地址栏原始链接（不要手动改 BV 大小写）。"
                f" 原始错误: {raw}"
            )

        geo_or_deleted = (
            "geo-restricted" in lower_raw
            or "region restricted" in lower_raw
            or "region-restricted" in lower_raw
            or "video may be deleted" in lower_raw
            or "has been deleted" in lower_raw
            or "this video is unavailable" in lower_raw
        )
        if geo_or_deleted:
            return (
                "yt-dlp 下载失败：视频可能已删除、不可见，或受地区限制。"
                " 请先确认链接在浏览器可直接播放；"
                " 若当前网络存在地域限制，请配置 `video.download_proxy` 或环境变量 `YTDLP_PROXY` 到可访问出口。"
                " 若视频需要登录态，请更新 `download_cookies_from_browser` / `download_cookies_file` 后重试。"
                f" 原始错误: {raw}"
            )

        if "requested format is not available" in lower_raw:
            return (
                "yt-dlp 下载失败：目标站点当前可用流与本地格式筛选条件不匹配。"
                " 已自动尝试回退格式（H.264 优先 -> best -> bestvideo+bestaudio/best -> 显式 format_id -> m3u8+ffmpeg）但仍失败。"
                " 这通常意味着该视频当前无可下载流、受地区/版权限制，或需更新 Cookie 后重试。"
                f" 原始错误: {raw}"
            )

        needs_cookie = (
            "not a bot" in lower_raw
            or "cookies-from-browser" in lower_raw
            or "use --cookies" in lower_raw
            or "sign in to confirm" in lower_raw
        )
        if not needs_cookie:
            return f"yt-dlp 执行失败: {raw}"

        if self.cookies_file or self.cookies_from_browser:
            hint = "当前已配置 Cookie 但仍被风控，建议重新导出 Cookie 或切换 browser/profile。"
        else:
            hint = (
                "当前未配置 Cookie。请在 config/video_config.yaml 的 video 段设置 "
                "`download_cookies_from_browser`（例如 `chrome` 或 `edge:Default`）"
                " 或 `download_cookies_file`。"
            )
        return f"yt-dlp 被 YouTube 风控拦截（需要登录态 Cookie）。{hint} 原始错误: {raw}"

    @staticmethod
    def _is_browser_cookie_access_error(err: Exception) -> bool:
        """判断是否为浏览器 Cookie 访问失败错误（复制失败或 DPAPI 解密失败）。"""
        lower_raw = str(err).lower()
        return (
            "could not copy chrome cookie database" in lower_raw
            or ("could not copy" in lower_raw and "cookie database" in lower_raw and "chrome" in lower_raw)
            or "failed to decrypt with dpapi" in lower_raw
        )

    @staticmethod
    def _is_gateway_bad_response_error(err: Exception) -> bool:
        """判断是否为网关层错误（典型 502/503/504）。"""
        lower_raw = str(err).lower()
        return (
            "http error 502" in lower_raw
            or "http error 503" in lower_raw
            or "http error 504" in lower_raw
            or "bad gateway" in lower_raw
            or "gateway timeout" in lower_raw
        )

    @staticmethod
    def _is_bilibili_bvid_extractor_error(err: Exception) -> bool:
        """判断是否为 Bilibili 提取阶段 bvid 缺失错误。"""
        lower_raw = str(err).lower()
        # 兼容两种上游格式：
        # 1) ERROR: [BiliBili] ... (caused by KeyError('bvid'))
        # 2) ERROR: <id>: An extractor error has occurred. (caused by KeyError('bvid'))
        # 某些版本/场景会缺失 [BiliBili] 前缀，因此不能把站点标签作为硬条件。
        has_bvid_key_error = "keyerror('bvid')" in lower_raw
        has_extractor_marker = "extractor error has occurred" in lower_raw
        return has_bvid_key_error and has_extractor_marker

    @staticmethod
    def _is_format_unavailable_error(err: Exception) -> bool:
        return "requested format is not available" in str(err).lower()

    @staticmethod
    def _is_h264_codec(codec_name: Any) -> bool:
        normalized = str(codec_name or "").strip().lower()
        return normalized.startswith("avc1") or normalized.startswith("h264")

    @classmethod
    def _pick_ranked_muxed_format_ids(cls, info: Dict[str, Any], *, prefer_h264: bool) -> list[str]:
        """从 formats 中挑选音视频同轨格式，并按质量高到低返回 format_id 列表。"""
        formats = info.get("formats") or []
        candidates = []
        for fmt in formats:
            if not isinstance(fmt, dict):
                continue
            format_id = fmt.get("format_id")
            vcodec = fmt.get("vcodec")
            acodec = fmt.get("acodec")
            if not format_id:
                continue
            if vcodec in (None, "none") or acodec in (None, "none"):
                continue
            candidates.append(fmt)

        if not candidates:
            return []

        def _score(item: Dict[str, Any]) -> Tuple[float, float, float, float]:
            codec_score = 1.0 if (prefer_h264 and cls._is_h264_codec(item.get("vcodec"))) else 0.0
            height = float(item.get("height") or 0.0)
            tbr = float(item.get("tbr") or 0.0)
            fps = float(item.get("fps") or 0.0)
            return (codec_score, height, tbr, fps)

        ranked = sorted(candidates, key=_score, reverse=True)
        format_ids = []
        for fmt in ranked:
            fmt_id = str(fmt.get("format_id"))
            if fmt_id not in format_ids:
                format_ids.append(fmt_id)
        return format_ids

    def _resolve_explicit_muxed_format_ids(
        self,
        url: str,
        base_opts: Dict[str, Any],
        *,
        prefer_h264: Optional[bool] = None,
    ) -> list[str]:
        """当 selector 匹配失败时，探测 formats 并返回显式 format_id 候选列表。"""
        rank_with_h264 = self.prefer_h264 if prefer_h264 is None else bool(prefer_h264)
        probe_opts = dict(base_opts)
        probe_opts.pop("format", None)
        errors = []
        try:
            with yt_dlp.YoutubeDL(probe_opts) as ydl:
                info = ydl.extract_info(url, download=False)
            self._last_explicit_probe_error = None
            format_ids = self._pick_ranked_muxed_format_ids(
                info if isinstance(info, dict) else {},
                prefer_h264=rank_with_h264,
            )
            if format_ids:
                return format_ids
            errors.append("no_formats_from_default_probe")
        except Exception as exc:
            errors.append(str(exc))

        if self._is_youtube_url(url):
            try:
                yt_probe_opts = self._with_youtube_player_client_chain(probe_opts)
                with yt_dlp.YoutubeDL(yt_probe_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                format_ids = self._pick_ranked_muxed_format_ids(
                    info if isinstance(info, dict) else {},
                    prefer_h264=rank_with_h264,
                )
                if format_ids:
                    self._last_explicit_probe_error = None
                    return format_ids
                errors.append("no_formats_from_youtube_client_chain_probe")
            except Exception as exc:
                errors.append(f"yt_client_chain_probe_failed: {str(exc)}")

        self._last_explicit_probe_error = " | ".join(errors) if errors else None
        return []

    @staticmethod
    def _pick_best_m3u8_url(info: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        """从 formats 中挑选最佳 m3u8 音视频同轨流，返回 (url, format_id)。"""
        formats = info.get("formats") or []
        candidates = []
        for fmt in formats:
            if not isinstance(fmt, dict):
                continue
            format_id = fmt.get("format_id")
            protocol = str(fmt.get("protocol") or "").lower()
            stream_url = fmt.get("url")
            vcodec = fmt.get("vcodec")
            acodec = fmt.get("acodec")
            if not format_id or not stream_url:
                continue
            if "m3u8" not in protocol:
                continue
            if vcodec in (None, "none") or acodec in (None, "none"):
                continue
            candidates.append(fmt)

        if not candidates:
            return None

        def _score(item: Dict[str, Any]) -> Tuple[float, float, float]:
            height = float(item.get("height") or 0.0)
            tbr = float(item.get("tbr") or 0.0)
            fps = float(item.get("fps") or 0.0)
            return (height, tbr, fps)

        best = max(candidates, key=_score)
        return (str(best.get("url")), str(best.get("format_id")))

    def _resolve_best_m3u8_url(self, url: str, base_opts: Dict[str, Any]) -> Optional[Tuple[str, str]]:
        """仅探测信息并提取可用于 ffmpeg 的 m3u8 直链。"""
        probe_opts = dict(base_opts)
        probe_opts.pop("format", None)
        errors = []
        try:
            with yt_dlp.YoutubeDL(probe_opts) as ydl:
                info = ydl.extract_info(url, download=False)
            m3u8_info = self._pick_best_m3u8_url(info if isinstance(info, dict) else {})
            if m3u8_info:
                self._last_m3u8_probe_error = None
                return m3u8_info
            errors.append("no_m3u8_from_default_probe")
        except Exception as exc:
            errors.append(str(exc))

        if self._is_youtube_url(url):
            try:
                yt_probe_opts = self._with_youtube_player_client_chain(probe_opts)
                with yt_dlp.YoutubeDL(yt_probe_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                m3u8_info = self._pick_best_m3u8_url(info if isinstance(info, dict) else {})
                if m3u8_info:
                    self._last_m3u8_probe_error = None
                    return m3u8_info
                errors.append("no_m3u8_from_youtube_client_chain_probe")
            except Exception as exc:
                errors.append(f"yt_client_chain_probe_failed: {str(exc)}")

        self._last_m3u8_probe_error = " | ".join(errors) if errors else None
        return None

    def _download_m3u8_with_ffmpeg(self, ffmpeg_path: str, m3u8_url: str, output_file: str) -> None:
        """用 ffmpeg 直接拉取 m3u8，减少 yt-dlp 在下载阶段触发风控的概率。"""
        ffmpeg_cmd = [
            ffmpeg_path,
            "-y",
            "-loglevel",
            "error",
            "-stats",
        ]
        if self.proxy:
            ffmpeg_cmd.extend(["-http_proxy", self.proxy])
        ffmpeg_cmd.extend(
            [
                "-i",
                m3u8_url,
                "-c",
                "copy",
                output_file,
            ]
        )
        subprocess.run(ffmpeg_cmd, check=True)

    def download(self, url: str, output_dir: str, filename: str = "video") -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.proxy
        - 条件：ffmpeg_path
        - 条件：p == 'ffmpeg' or (os.path.exists(p) and os.path.isfile(p))
        依据来源（证据链）：
        - 输入参数：filename。
        - 对象内部状态：self.proxy。
        输入参数：
        - url: 函数入参（类型：str）。
        - output_dir: 目录路径（类型：str）。
        - filename: 函数入参（类型：str）。
        输出参数：
        - 字符串结果。"""
        os.makedirs(output_dir, exist_ok=True)
        self._last_video_title = ""
        # yt-dlp 的 template 不包含扩展名，它会自动添加
        output_template = os.path.join(output_dir, f"{filename}.%(ext)s")
        
        self.emit_progress("download", 0.1, f"准备下载: {url}")
        
        import sys
        
        # 尝试定位 ffmpeg
        ffmpeg_path = None
        possible_paths = [
            os.path.join(sys.prefix, 'Library', 'bin', 'ffmpeg.exe'), # Windows Conda
            os.path.join(sys.prefix, 'bin', 'ffmpeg'), # Linux/Mac Conda
            'ffmpeg' # System PATH
        ]
        
        for p in possible_paths:
            if p == 'ffmpeg' or (os.path.exists(p) and os.path.isfile(p)):
                ffmpeg_path = p
                break

        base_format_candidates = self._get_format_candidates()

        # 配置选项
        ydl_opts = {
            # 自动选择最佳可用格式：优先 H.264（可配置关闭），失败后回退。
            'format': base_format_candidates[0],
            'outtmpl': output_template,
            'merge_output_format': 'mp4',
            'noplaylist': True,
            'progress_hooks': [self._progress_hook],
            'quiet': True,
            'no_warnings': True,
            # 性能优化与稳定性配置
            'concurrent_fragment_downloads': 8,    # 并发下载分片 (对B站Dash流有效)
            'http_chunk_size': 10 * 1024 * 1024,   # 10MB chunk
            'socket_timeout': 30,                  # 增加超时时间 (单位: 秒)
            'retries': 10,                         # 增加重试次数
            'fragment_retries': 10,                # 分片下载重试次数
            'nocheckcertificate': self.disable_ssl_verify,  # 可配置的 SSL 验证
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        }
        
        auth_opts = self._build_auth_options()
        ydl_opts.update(auth_opts)
        if self.external_downloader:
            ydl_opts["external_downloader"] = self.external_downloader
            self.emit_progress("download", 0.145, f"使用外部下载器: {self.external_downloader}")
            if self.external_downloader_args:
                downloader_key = Path(self.external_downloader).stem.lower()
                args_copy = list(self.external_downloader_args)
                ydl_opts["external_downloader_args"] = {
                    downloader_key: args_copy,
                    "default": args_copy,
                }
                self.emit_progress("download", 0.146, f"外部下载器参数: {' '.join(args_copy)}")
        if self._is_youtube_url(url):
            ydl_opts = self._with_youtube_player_client_chain(ydl_opts)
            self.emit_progress("download", 0.14, "YouTube 下载启用 player_client 回退链: web_safari/tv_downgraded/web")

        format_candidates = base_format_candidates
        explicit_probe_prefer_h264 = self.prefer_h264
        probed_duration = self._probe_video_duration_for_format_selection(url=url, base_opts=ydl_opts)
        if probed_duration is not None and probed_duration < self.short_video_max_duration_sec:
            format_candidates = self._prioritize_short_video_highest_resolution_candidates(base_format_candidates)
            explicit_probe_prefer_h264 = False
            self.emit_progress(
                "download",
                0.205,
                f"检测到短视频({probed_duration:.1f}s < {self.short_video_max_duration_sec:.1f}s)，优先最高分辨率下载",
            )
        ydl_opts["format"] = format_candidates[0]

        if 'cookiefile' in auth_opts:
            self.emit_progress("download", 0.12, f"使用 Cookie 文件: {auth_opts['cookiefile']}")
        elif 'cookiesfrombrowser' in auth_opts:
            browser_name = auth_opts['cookiesfrombrowser'][0]
            self.emit_progress("download", 0.12, f"使用浏览器 Cookie: {browser_name}")
        if self.proxy:
            self.emit_progress("download", 0.13, f"使用下载代理: {self.proxy}")
        else:
            self.emit_progress("download", 0.13, "未配置下载代理（与手工命令上下文可能不一致）")
        
        if ffmpeg_path:
            ydl_opts['ffmpeg_location'] = ffmpeg_path
            self.emit_progress("download", 0.15, f"使用FFmpeg: {ffmpeg_path}")
        else:
             self.emit_progress("download", 0.15, "警告: 未找到FFmpeg，可能无法合并高清视频")
        
        self.emit_progress("download", 0.2, "初始化下载引擎...")
        
        try:
            last_error = None
            attempt_trace: list[str] = []
            self._last_explicit_probe_error = None
            self._last_m3u8_probe_error = None
            for idx, format_selector in enumerate(format_candidates):
                attempt_opts = dict(ydl_opts)
                attempt_opts["format"] = format_selector
                attempt_trace.append(format_selector)
                try:
                    with yt_dlp.YoutubeDL(attempt_opts) as ydl:
                        self.emit_progress("download", 0.3, f"开始下载流（format={format_selector}）...")
                        ydl.download([url])
                    last_error = None
                    break
                except Exception as attempt_err:
                    last_error = attempt_err
                    if self._is_format_unavailable_error(attempt_err):
                        if idx < len(format_candidates) - 1:
                            next_format = format_candidates[idx + 1]
                            self.emit_progress(
                                "download",
                                0.26,
                                f"当前格式不可用，自动回退重试: {next_format}",
                            )
                            continue
                        # 最后一个 selector 也不可用时，进入显式 format_id 回退分支
                        self.emit_progress("download", 0.27, "格式 selector 全部不可用，尝试显式 format_id 回退")
                        break
                    raise

            if last_error is not None and self._is_format_unavailable_error(last_error):
                explicit_format_ids = self._resolve_explicit_muxed_format_ids(
                    url,
                    ydl_opts,
                    prefer_h264=explicit_probe_prefer_h264,
                )
                if not explicit_format_ids and self._is_youtube_url(url):
                    explicit_format_ids = list(self._YOUTUBE_HLS_FALLBACK_FORMAT_IDS)
                    self.emit_progress(
                        "download",
                        0.285,
                        "未探测到显式 format_id，尝试 YouTube HLS 兜底ID链: 96/95/94/93/92/91",
                    )
                    if self._last_explicit_probe_error:
                        self.emit_progress(
                            "download",
                            0.286,
                            f"显式 format 探测失败，已改用固定ID链: {self._last_explicit_probe_error}",
                        )
                if explicit_format_ids:
                    for explicit_format_id in explicit_format_ids:
                        explicit_opts = dict(ydl_opts)
                        explicit_opts["format"] = explicit_format_id
                        attempt_trace.append(explicit_format_id)
                        self.emit_progress("download", 0.28, f"回退到显式 format_id: {explicit_format_id}")
                        try:
                            with yt_dlp.YoutubeDL(explicit_opts) as ydl:
                                ydl.download([url])
                            last_error = None
                            break
                        except Exception as explicit_err:
                            last_error = explicit_err
                            if self._is_format_unavailable_error(explicit_err):
                                continue
                            raise
                    if last_error is not None and self._is_format_unavailable_error(last_error):
                        self.emit_progress("download", 0.285, "显式 format_id 候选均不可用，继续尝试 m3u8+ffmpeg")
                else:
                    self.emit_progress("download", 0.285, "未解析到可用显式 format_id，继续尝试 m3u8+ffmpeg")

            if (
                last_error is not None
                and self._is_format_unavailable_error(last_error)
                and ffmpeg_path
            ):
                self.emit_progress("download", 0.29, "尝试 m3u8 提取并回退到 ffmpeg 下载")
                m3u8_info = self._resolve_best_m3u8_url(url, ydl_opts)
                if m3u8_info:
                    m3u8_url, m3u8_format_id = m3u8_info
                    attempt_trace.append(f"m3u8:{m3u8_format_id}")
                    ffmpeg_output = os.path.join(output_dir, f"{filename}.mp4")
                    try:
                        self.emit_progress(
                            "download",
                            0.3,
                            f"已提取 m3u8(format_id={m3u8_format_id})，开始 ffmpeg 下载",
                        )
                        self._download_m3u8_with_ffmpeg(
                            ffmpeg_path=ffmpeg_path,
                            m3u8_url=m3u8_url,
                            output_file=ffmpeg_output,
                        )
                        last_error = None
                    except Exception as ffmpeg_err:
                        last_error = Exception(
                            f"{str(last_error)}；m3u8+ffmpeg 回退失败: {str(ffmpeg_err)}"
                        )
                else:
                    if self._last_m3u8_probe_error:
                        attempt_trace.append("m3u8:probe_error")
                        self.emit_progress(
                            "download",
                            0.295,
                            f"m3u8 探测失败，结束回退: {self._last_m3u8_probe_error}",
                        )
                    else:
                        attempt_trace.append("m3u8:none")
                        self.emit_progress("download", 0.295, "未提取到可用 m3u8 直链，结束回退")

            if last_error is not None:
                attempts = " -> ".join(attempt_trace) if attempt_trace else "none"
                probe_notes = []
                if self._last_explicit_probe_error:
                    probe_notes.append(f"explicit_probe={self._last_explicit_probe_error}")
                if self._last_m3u8_probe_error:
                    probe_notes.append(f"m3u8_probe={self._last_m3u8_probe_error}")
                probe_tail = f" [probes={' | '.join(probe_notes)}]" if probe_notes else ""
                raise Exception(f"{str(last_error)} [attempts={attempts}]{probe_tail}")
            
            self.emit_progress("download", 0.9, "下载完成，确认文件...")
            
            # 查找下载的文件 (不固定扩展名，支持 mp4, mkv, webm 等)
            valid_exts = {'.mp4', '.mkv', '.webm', '.mov', '.avi'}
            for file in os.listdir(output_dir):
                f_path = Path(output_dir) / file
                if file.startswith(filename) and f_path.suffix.lower() in valid_exts:
                    abs_path = str(f_path.absolute())
                    self.emit_progress("download", 1.0, f"视频就绪: {file}", data={"path": abs_path})
                    return abs_path
            
            raise FileNotFoundError(f"未在 {output_dir} 找到以 {filename} 开头的有效视频文件")
            
        except Exception as e:
            if (
                self._is_browser_cookie_access_error(e)
                and ("cookiefile" in auth_opts or "cookiesfrombrowser" in auth_opts)
            ):
                self.emit_progress(
                    "download",
                    0.21,
                    "浏览器 Cookie 读取失败，自动降级为无 Cookie 重试一次",
                )
                origin_cookie_file = self.cookies_file
                origin_cookie_browser = self.cookies_from_browser
                origin_export_attempted = self._cookie_export_attempted
                origin_export_error = self._cookie_export_error
                try:
                    self.cookies_file = None
                    self.cookies_from_browser = None
                    self._cookie_export_attempted = False
                    self._cookie_export_error = None
                    return self.download(url, output_dir, filename)
                finally:
                    self.cookies_file = origin_cookie_file
                    self.cookies_from_browser = origin_cookie_browser
                    self._cookie_export_attempted = origin_export_attempted
                    self._cookie_export_error = origin_export_error
            if self._is_gateway_bad_response_error(e) and self.proxy:
                self.emit_progress(
                    "download",
                    0.22,
                    "代理出口返回网关错误，自动降级为无代理重试一次",
                )
                origin_proxy = self.proxy
                try:
                    self.proxy = None
                    return self.download(url, output_dir, filename)
                finally:
                    self.proxy = origin_proxy
            self.emit_progress("download", 0.0, f"下载失败: {str(e)}")
            raise RuntimeError(self._build_download_error_message(e))

    def _progress_hook(self, d):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：d['status'] == 'downloading'
        - 条件：d['status'] == 'finished'
        依据来源（证据链）：
        - 输入参数：d。
        - 配置字段：status。
        输入参数：
        - d: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if d['status'] == 'downloading':
            self._capture_title_from_info_dict(d.get("info_dict"))
            # 计算百分比
            try:
                p = d.get('_percent_str', '0%').replace('%','')
                progress = float(p) / 100.0
                # 映射到 0.3 - 0.9 范围
                final_progress = 0.3 + (progress * 0.6)
                self.emit_progress("download", final_progress, f"下载中: {d.get('_percent_str')}")
            except:
                pass
        elif d['status'] == 'finished':
            self._capture_title_from_info_dict(d.get("info_dict"))
            self.emit_progress("download", 0.9, "下载完成，正在合并...")

    def probe_video_info(self, url: str) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 用 yt-dlp 仅探测元信息，不执行下载。
        2) 返回原始 info_dict 供上层组装平台/分集结构。
        实现方式：yt_dlp.YoutubeDL.extract_info(download=False)。
        核心价值：复用现有鉴权与代理配置，避免重复实现站点解析逻辑。
        输入参数：
        - url: 视频链接（类型：str）。
        输出参数：
        - dict：yt-dlp 的元信息字典，失败时抛出异常。"""
        probe_opts: Dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 30,
            "retries": 5,
            "skip_download": True,
            "extract_flat": False,
        }
        probe_opts.update(self._build_auth_options())

        with yt_dlp.YoutubeDL(probe_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        if not isinstance(info, dict):
            return {}
        self._capture_title_from_info_dict(info)
        return info
    
    def detect_playlist(self, url: str) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - url: 函数入参（类型：str）。
        输出参数：
        - 布尔判断结果。"""
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,  # 只提取信息，不下载
                'socket_timeout': 30,
                'retries': 5,
            }
            ydl_opts.update(self._build_auth_options())
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                # 检查是否有 entries（播放列表标志）
                return 'entries' in info and info['entries'] is not None
        except Exception as e:
            self.emit_progress("download", 0.0, f"检测播放列表失败: {str(e)}")
            return False
    
    def get_playlist_info(self, url: str) -> dict:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：'entries' not in info
        - 条件：entry
        依据来源（证据链）：
        输入参数：
        - url: 函数入参（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
                'socket_timeout': 30,
                'retries': 5,
            }
            ydl_opts.update(self._build_auth_options())
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                
                if 'entries' not in info:
                    return None
                
                episodes = []
                for i, entry in enumerate(info['entries'], 1):
                    if entry:  # 有时 entries 可能包含 None
                        episodes.append({
                            "index": i,
                            "title": entry.get('title', f'Episode {i}'),
                            "url": entry.get('url', entry.get('webpage_url', ''))
                        })
                
                return {
                    "title": info.get('title', 'Unknown Playlist'),
                    "total_episodes": len(episodes),
                    "episodes": episodes
                }
        except Exception as e:
            self.emit_progress("download", 0.0, f"获取播放列表信息失败: {str(e)}")
            return None
    
    @staticmethod
    def parse_episode_range(range_str: str, total_episodes: int) -> list:
        """
        执行逻辑：
        1) 接收原始输入。
        2) 按规则解析为内部结构。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：规范化输入结构，提升下游稳定性。
        决策逻辑：
        - 条件：range_str.lower() == 'all'
        - 条件：'-' in part
        依据来源（证据链）：
        - 输入参数：range_str。
        输入参数：
        - range_str: 函数入参（类型：str）。
        - total_episodes: 函数入参（类型：int）。
        输出参数：
        - 列表结果（与输入或处理结果一一对应）。"""
        if range_str.lower() == 'all':
            return list(range(1, total_episodes + 1))
        
        episodes = set()
        parts = range_str.split(',')
        
        for part in parts:
            part = part.strip()
            if '-' in part:
                # 范围，例如 "1-5"
                try:
                    start, end = part.split('-')
                    start, end = int(start.strip()), int(end.strip())
                    episodes.update(range(start, end + 1))
                except:
                    pass
            else:
                # 单个数字
                try:
                    episodes.add(int(part))
                except:
                    pass
        
        # 过滤超出范围的集数
        valid_episodes = [e for e in sorted(episodes) if 1 <= e <= total_episodes]
        return valid_episodes
    
    def download_playlist(self, url: str, output_base_dir: str, episode_range: list = None) -> list:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not playlist_info
        - 条件：episode_range
        依据来源（证据链）：
        - 输入参数：episode_range。
        输入参数：
        - url: 函数入参（类型：str）。
        - output_base_dir: 目录路径（类型：str）。
        - episode_range: 函数入参（类型：list）。
        输出参数：
        - 列表结果（与输入或处理结果一一对应）。"""
        playlist_info = self.get_playlist_info(url)
        if not playlist_info:
            raise RuntimeError("无法获取播放列表信息")
        
        total = playlist_info['total_episodes']
        episodes_to_download = episode_range if episode_range else list(range(1, total + 1))
        
        self.emit_progress("download", 0.0, f"准备下载 {len(episodes_to_download)} 集（共 {total} 集）")
        
        downloaded_videos = []
        
        for i, episode_index in enumerate(episodes_to_download, 1):
            episode = playlist_info['episodes'][episode_index - 1]
            
            # 为每一集创建独立目录
            episode_dir = os.path.join(output_base_dir, f"episode_{episode_index:02d}", "downloads")
            
            self.emit_progress("download", i / len(episodes_to_download), 
                             f"下载第 {episode_index} 集: {episode['title'][:30]}...")
            
            try:
                video_path = self.download(episode['url'], episode_dir, filename="video")
                downloaded_videos.append({
                    "index": episode_index,
                    "title": episode['title'],
                    "path": video_path,
                    "output_dir": os.path.dirname(episode_dir)  # episode_XX 目录
                })
            except Exception as e:
                self.emit_progress("download", -1, f"第 {episode_index} 集下载失败: {str(e)}")
        
        self.emit_progress("download", 1.0, f"完成！成功下载 {len(downloaded_videos)} 集")
        return downloaded_videos

