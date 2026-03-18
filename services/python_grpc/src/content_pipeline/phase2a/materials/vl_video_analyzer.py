"""
VL-Based Video Analyzer - 基于视觉语言模型的视频分析器

功能：
1. 使用可配置的 VL 模型分析语义单元视频片段
2. 解析 AI 返回的知识类型、视频截取区间、截图时间戳
3. 将相对时间戳（片段内）转换为绝对时间戳（原视频）

使用方法：
    from vl_video_analyzer import VLVideoAnalyzer
    
    analyzer = VLVideoAnalyzer(config)
    result = await analyzer.analyze_clip(
        clip_path="path/to/clip.mp4",
        semantic_unit_start_sec=100.0,
        semantic_unit_id="SU001"
    )
"""

import os
import re
import json
import base64
import asyncio
import logging
import io
import math
import random
import traceback
import httpx
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, Awaitable
from dataclasses import dataclass, field
from openai import AsyncOpenAI
from services.python_grpc.src.common.utils.hash_policy import (
    fast_digest_text,
    md5_short_text_compat,
    sha256_text,
)
from services.python_grpc.src.common.utils.numbers import safe_int, safe_float
from services.python_grpc.src.common.utils.runtime_llm_context import current_runtime_llm_context
from services.python_grpc.src.common.utils.opencv_decode import (
    get_video_basic_metadata,
    open_video_capture_with_fallback,
    resolve_ffmpeg_bin,
)
# 统一 LLM 调用入口
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.fallback_audit import append_llm_fallback_event
from services.python_grpc.src.content_pipeline.infra.llm.token_costing import normalize_usage_payload
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys
from services.python_grpc.src.content_pipeline.common.utils.id_utils import build_unit_relative_asset_id
from services.python_grpc.src.content_pipeline.common.utils import json_payload_repair

logger = logging.getLogger(__name__)


_DASHSCOPE_DATA_URI_ITEM_MAX_BYTES = 10 * 1024 * 1024  # 来自 DashScope 400 错误信息
_DATA_URI_SAFETY_RATIO = 0.90  # 留出协议/编码冗余，避免卡边界导致 400
_DASHSCOPE_BATCH_BODY_MAX_BYTES = 6 * 1024 * 1024
# 按用户要求放宽 base64 视频回退阈值到 1GB。
_MAX_RAW_BYTES_FOR_BASE64_DATA_URI = 1024 * 1024 * 1024

_VL_HTTP_CLIENT_POOL: Dict[float, List[Dict[str, Any]]] = {}
_VL_HTTP_CLIENT_POOL_LOCK = threading.Lock()


@dataclass(frozen=True)
class VLHttpClientLease:
    """VL HTTP 连接池租约。"""

    timeout_sec: float
    client_id: str


@dataclass
class _VLClientState:
    """VL 客户端状态，用于双客户端切换与在途跟踪。"""

    lease: VLHttpClientLease
    http_client: httpx.AsyncClient
    openai_client: AsyncOpenAI
    inflight: int = 0
    retiring: bool = False


def _normalize_vl_timeout_sec(timeout_sec: Any) -> float:
    """归一化 VL HTTP 超时配置。"""
    try:
        value = float(timeout_sec)
    except Exception:
        value = 120.0
    if value <= 0:
        value = 120.0
    return value


def _acquire_vl_http_client(
    timeout_sec: Any,
    *,
    force_new: bool = False,
) -> tuple[httpx.AsyncClient, VLHttpClientLease]:
    """获取可复用的 VL HTTP 客户端。"""
    normalized_timeout = _normalize_vl_timeout_sec(timeout_sec)
    with _VL_HTTP_CLIENT_POOL_LOCK:
        entries = _VL_HTTP_CLIENT_POOL.get(normalized_timeout) or []
        if not force_new:
            for entry in entries:
                client = entry.get("client")
                if isinstance(client, httpx.AsyncClient) and not client.is_closed:
                    entry["refs"] = int(entry.get("refs", 0)) + 1
                    return client, VLHttpClientLease(normalized_timeout, str(entry.get("id") or ""))
        client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            headers={"Accept-Encoding": "gzip, deflate"},
            timeout=httpx.Timeout(normalized_timeout, connect=10.0)
        )
        entry_id = uuid.uuid4().hex
        entries.append({"id": entry_id, "client": client, "refs": 1})
        _VL_HTTP_CLIENT_POOL[normalized_timeout] = entries
        return client, VLHttpClientLease(normalized_timeout, entry_id)


async def _release_vl_http_client(
    lease: Optional[VLHttpClientLease],
    client: Optional[httpx.AsyncClient],
) -> None:
    """释放 VL HTTP 客户端引用计数，必要时关闭连接池。"""
    if lease is None or client is None:
        return
    should_close = False
    timeout_key = float(getattr(lease, "timeout_sec", 0.0) or 0.0)
    target_id = str(getattr(lease, "client_id", "") or "")
    with _VL_HTTP_CLIENT_POOL_LOCK:
        entries = _VL_HTTP_CLIENT_POOL.get(timeout_key)
        if not entries:
            return
        entry = None
        for candidate in entries:
            if str(candidate.get("id") or "") == target_id and candidate.get("client") is client:
                entry = candidate
                break
        if entry is None:
            return
        refs = max(0, int(entry.get("refs", 0)) - 1)
        if refs == 0:
            entries.remove(entry)
            if not entries:
                _VL_HTTP_CLIENT_POOL.pop(timeout_key, None)
            should_close = True
        else:
            entry["refs"] = refs
    if should_close:
        try:
            await client.aclose()
            logger.info("VL 共享 HTTP 客户端已关闭: timeout=%.2fs", float(timeout_key))
        except Exception as exc:
            logger.warning("VL 共享 HTTP 客户端关闭失败: %s", exc)


def _iter_exception_chain(exc: BaseException, max_depth: int = 6) -> list[BaseException]:
    """遍历异常链，避免遗漏连接池相关异常。"""
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: Optional[BaseException] = exc
    depth = 0
    while current is not None and depth < max(1, int(max_depth)):
        current_id = id(current)
        if current_id in seen:
            break
        seen.add(current_id)
        chain.append(current)
        current = current.__cause__ or current.__context__
        depth += 1
    return chain


def _is_connection_pool_exhausted_error(exc: BaseException) -> bool:
    """识别连接池资源不足导致的异常。"""
    for current in _iter_exception_chain(exc):
        if isinstance(current, httpx.PoolTimeout):
            return True
        text = str(current).lower()
        if "pool timeout" in text or "connection pool" in text or "no available connections" in text:
            return True
    return False


def _should_release_vl_http_client(exc: BaseException) -> bool:
    """识别需要重置 VL HTTP 客户端的异常类型。"""
    for current in _iter_exception_chain(exc):
        if isinstance(current, (httpx.TimeoutException, httpx.NetworkError)):
            return True
    return False


async def shutdown_vl_http_client_pool() -> None:
    """统一关闭 VL 共用 HTTP 连接池。"""
    clients: list[httpx.AsyncClient] = []
    with _VL_HTTP_CLIENT_POOL_LOCK:
        for entries in _VL_HTTP_CLIENT_POOL.values():
            for entry in entries:
                client = entry.get("client")
                if isinstance(client, httpx.AsyncClient) and not client.is_closed:
                    clients.append(client)
        _VL_HTTP_CLIENT_POOL.clear()
    for client in clients:
        try:
            await client.aclose()
        except Exception as exc:
            logger.warning("VL 共用 HTTP 连接池关闭失败: %s", exc)


@dataclass
class VLAnalysisResult:
    """单个视频片段的分析结果"""
    id: int = 0
    knowledge_type: str = ""
    no_needed_video: bool = False
    should_type: str = ""
    confidence: float = 0.0
    reasoning: str = ""
    key_evidence: str = ""
    clip_start_sec: float = 0.0  # 相对时间（片段内）
    clip_end_sec: float = 0.0    # 相对时间（片段内）
    suggested_screenshoot_timestamps: List[float] = field(default_factory=list)  # 相对时间

    # 教程模式新增字段
    step_id: int = 0
    step_description: str = ""
    step_type: str = "MAIN_FLOW"
    analysis_mode: str = "default"
    main_action: str = ""
    main_operation: List[str] = field(default_factory=list)
    instructional_keyframes: List[Dict[str, Any]] = field(default_factory=list)
    instructional_clips: List[Dict[str, Any]] = field(default_factory=list)
    precautions: List[str] = field(default_factory=list)
    step_summary: str = ""
    operation_guidance: List[str] = field(default_factory=list)

    # 绝对时间（由 VLVideoAnalyzer.convert_timestamps 计算）
    absolute_clip_start_sec: float = 0.0
    absolute_clip_end_sec: float = 0.0
    absolute_screenshot_timestamps: List[float] = field(default_factory=list)


@dataclass
class VLClipAnalysisResponse:
    """视频片段分析的完整响应"""
    success: bool = False
    error_msg: str = ""
    analysis_results: List[VLAnalysisResult] = field(default_factory=list)
    clip_requests: List[Dict[str, Any]] = field(default_factory=list)
    screenshot_requests: List[Dict[str, Any]] = field(default_factory=list)
    token_usage: Dict[str, int] = field(default_factory=dict)
    analysis_mode: str = "default"
    raw_response_json: List[Dict[str, Any]] = field(default_factory=list)
    raw_llm_interactions: List[Dict[str, Any]] = field(default_factory=list)


class VLVideoAnalyzer:
    """基于视觉语言模型（VL）的通用视频分析器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化分析器
        
        Args:
            config: VL 配置，包含 api 配置、截图优化配置等
        """
        api_config = config.get("api", {})
        
        self.base_url = str(
            api_config.get("base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1") or ""
        ).strip()
        default_model = "ernie-4.5-turbo-vl-32k" if self._is_qianfan_endpoint(self.base_url) else "qwen-vl-max-latest"
        self.model = str(api_config.get("model", default_model) or "").strip()
        self.provider = str(api_config.get("provider", "") or "").strip().lower()

        # 统一鉴权解析：兼容 api_key（DashScope）与 bearer_token（千帆）。
        # 解析优先级：显式 api_key > 显式 bearer_token > 环境变量。
        api_key = str(api_config.get("api_key", "") or "").strip()
        if not api_key:
            api_key = str(api_config.get("bearer_token", "") or "").strip()

        api_key_env = str(api_config.get("api_key_env", "") or "").strip()
        bearer_env = str(api_config.get("bearer_token_env", "") or "").strip()
        if not api_key_env:
            api_key_env = "QIANFAN_BEARER_TOKEN" if self._is_qianfan_endpoint(self.base_url) else "DASHSCOPE_API_KEY"
        if not bearer_env:
            bearer_env = "VISION_AI_BEARER_TOKEN"

        if not api_key:
            for env_name in (api_key_env, bearer_env):
                if not env_name:
                    continue
                candidate = str(os.environ.get(env_name, "") or "").strip()
                if candidate:
                    api_key = candidate
                    break
        self._api_key = api_key
        self._api_key_env = api_key_env

        # 千帆可选 appid 透传（官方推荐用于计费/路由）。
        self.appid = str(api_config.get("appid", "") or "").strip()
        if not self.appid:
            appid_env = str(api_config.get("appid_env", "VISION_AI_APP_ID") or "").strip()
            if appid_env:
                self.appid = str(os.environ.get(appid_env, "") or "").strip()

        self.max_retries = max(0, safe_int(api_config.get("max_retries", 6), 6))
        self.vl_retry_initial_backoff_sec = max(
            0.0,
            safe_float(api_config.get("retry_initial_backoff_sec", 2.0), 2.0),
        )
        self.vl_retry_multiplier = max(
            1.0,
            safe_float(api_config.get("retry_multiplier", 2.0), 2.0),
        )
        self.vl_retry_max_backoff_sec = max(
            self.vl_retry_initial_backoff_sec,
            safe_float(api_config.get("retry_max_backoff_sec", 16.0), 16.0),
        )
        
        # 视频压缩配置 (API 限制 10MB)
        self.max_video_size_mb = api_config.get("max_video_size_mb", 8)  # 留 2MB buffer
        self.compression_crf = api_config.get("compression_crf", 28)  # 0-51, 越大压缩越多
        self.max_tokens = api_config.get("max_tokens", 4096)
        self.temperature = api_config.get("temperature", 0.2)
        # 最小请求超时统一下限，避免短视频被过早超时
        min_request_timeout_sec = 30.0
        self.vl_request_timeout_sec = max(
            min_request_timeout_sec,
            safe_float(api_config.get("request_timeout_sec", 120.0), 120.0),
        )
        self.vl_request_timeout_ratio_by_video_duration = max(
            0.0,
            safe_float(api_config.get("request_timeout_ratio_by_video_duration", 0.5), 0.5),
        )
        self.vl_request_timeout_min_sec = max(
            min_request_timeout_sec,
            safe_float(api_config.get("request_timeout_min_sec", 30.0), 30.0),
        )
        self.long_video_upload_compress_enabled = bool(api_config.get("long_video_upload_compress_enabled", False))
        self.long_video_upload_target_height = max(
            180,
            safe_int(api_config.get("long_video_upload_target_height", 720), 720),
        )
        self.long_video_upload_target_bitrate = str(
            api_config.get("long_video_upload_target_bitrate", "3M") or "3M"
        ).strip()
        self.long_video_upload_min_bitrate = str(
            api_config.get("long_video_upload_min_bitrate", "2M") or "2M"
        ).strip()
        self.long_video_upload_max_bitrate = str(
            api_config.get("long_video_upload_max_bitrate", "4M") or "4M"
        ).strip()
        self.long_video_upload_timeout_sec = max(
            60,
            safe_int(api_config.get("long_video_upload_timeout_sec", 1800), 1800),
        )
        self.long_video_upload_crf = min(
            51,
            max(0, safe_int(api_config.get("long_video_upload_crf", 28), 28)),
        )
        self.long_video_upload_preset = str(
            api_config.get("long_video_upload_preset", "fast") or "fast"
        ).strip() or "fast"
        self.long_video_upload_target_fps = max(
            1.0,
            safe_float(api_config.get("long_video_upload_target_fps", 15.0), 15.0),
        )
        self.long_video_upload_drop_audio = bool(api_config.get("long_video_upload_drop_audio", False))
        self.dashscope_upload_chunk_size_bytes = max(
            64 * 1024,
            safe_int(api_config.get("upload_chunk_size_bytes", 2 * 1024 * 1024), 2 * 1024 * 1024),
        )
        self.dashscope_upload_timeout_by_video_duration = bool(
            api_config.get("upload_timeout_by_video_duration", True)
        )
        self.dashscope_upload_timeout_min_sec = max(
            1.0,
            safe_float(api_config.get("upload_timeout_min_sec", 1.0), 1.0),
        )
        self.dashscope_upload_retry_max_attempts = max(
            1,
            safe_int(api_config.get("upload_retry_max_attempts", 5), 5),
        )
        self.dashscope_upload_retry_initial_backoff_sec = max(
            0.0,
            safe_float(api_config.get("upload_retry_initial_backoff_sec", 2.0), 2.0),
        )
        self.dashscope_upload_retry_multiplier = max(
            1.0,
            safe_float(api_config.get("upload_retry_multiplier", 2.0), 2.0),
        )
        self.dashscope_upload_retry_max_backoff_sec = max(
            self.dashscope_upload_retry_initial_backoff_sec,
            safe_float(api_config.get("upload_retry_max_backoff_sec", 16.0), 16.0),
        )
        self.dashscope_upload_retry_jitter_sec = max(
            0.0,
            safe_float(api_config.get("upload_retry_jitter_sec", 0.0), 0.0),
        )
        self.temp_url_reuse_ttl_sec = max(
            60,
            safe_int(api_config.get("temp_url_reuse_ttl_sec", 900), 900),
        )
        self.vl_offline_task_enabled = bool(api_config.get("offline_task_enabled", False))
        self.vl_offline_poll_interval_sec = max(
            1.0,
            safe_float(api_config.get("offline_poll_interval_sec", 5.0), 5.0),
        )
        self.vl_offline_result_format = str(api_config.get("offline_result_format", "message") or "message").strip()
        self.vl_offline_max_wait_sec = max(
            self.vl_offline_poll_interval_sec,
            safe_float(api_config.get("offline_max_wait_sec", 86400.0), 86400.0),
        )
        self._video_duration_cache: Dict[str, float] = {}

        # 输入策略：
        # - DashScope 默认 auto（data-uri -> upload -> keyframes）
        # - 千帆默认 keyframes，避免误走 DashScope 专有上传链路
        default_video_input_mode = "keyframes" if self._is_qianfan_endpoint(self.base_url) else "auto"
        self.video_input_mode = str(
            api_config.get("video_input_mode", default_video_input_mode) or default_video_input_mode
        ).strip().lower()
        self.max_input_frames = int(api_config.get("max_input_frames", 6))
        self.max_image_dim = int(api_config.get("max_image_dim", 1024))
        
        self.http_client = None
        self.client = None
        self._http_client_lease = None
        self._http_client_acquired = False
        # 双客户端切换与安全释放状态
        self._client_lock = asyncio.Lock()
        self._active_client_state = None
        self._retired_client_states = []
        
        # 截图优化配置
        self.screenshot_optimization = config.get("screenshot_optimization", {})
        self.optimize_screenshots = self.screenshot_optimization.get("enabled", True)
        self.search_window_sec = self.screenshot_optimization.get("search_window_sec", 1.0)
        
        # 回退配置
        self.fallback_config = config.get("fallback", {})
        
        # 加载提示词模板
        self.prompt_template = self._load_prompt_template()
        self._tutorial_system_prompt = get_prompt(
            PromptKeys.VL_VIDEO_ANALYSIS_TUTORIAL_SYSTEM,
            fallback=self._get_tutorial_system_prompt(),
        )
        self._concrete_system_prompt = get_prompt(
            PromptKeys.VL_VIDEO_ANALYSIS_CONCRETE_SYSTEM,
            fallback=self._get_concrete_system_prompt(),
        )
        self._constraints_default = get_prompt(
            PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_DEFAULT,
            fallback=self._get_builtin_output_constraints_default(),
        )
        self._constraints_tutorial = get_prompt(
            PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_TUTORIAL,
            fallback=self._get_builtin_output_constraints_tutorial(),
        )
        self._constraints_concrete = get_prompt(
            PromptKeys.VL_VIDEO_ANALYSIS_CONSTRAINTS_CONCRETE,
            fallback=self._get_builtin_output_constraints_concrete(),
        )

        # Tutorial mode settings for long multi-step process units
        tutorial_cfg = config.get("tutorial_mode", {}) if isinstance(config.get("tutorial_mode", {}), dict) else {}
        self.tutorial_min_step_duration_sec = float(tutorial_cfg.get("min_step_duration_sec", 5.0))
        
        logger.info(
            f"VLVideoAnalyzer 初始化完成: provider={self.provider or 'auto'}, "
            f"model={self.model}, mode={self.video_input_mode}, base_url={self.base_url}"
        )

    @staticmethod
    def _is_dashscope_endpoint(base_url: str) -> bool:
        normalized = str(base_url or "").strip().lower()
        return "dashscope.aliyuncs.com" in normalized

    @staticmethod
    def _is_qianfan_endpoint(base_url: str) -> bool:
        normalized = str(base_url or "").strip().lower()
        return "qianfan.baidubce.com" in normalized or "aistudio.baidu.com" in normalized

    @staticmethod
    def _safe_json_preview(value: Any, max_len: int = 1200) -> str:
        try:
            text = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            text = str(value)
        if len(text) > max_len:
            return text[:max_len] + "...(truncated)"
        return text

    def _format_exception_detail(self, error: BaseException, *, max_depth: int = 4) -> str:
        if error is None:
            return "unknown_error"

        details: List[str] = []
        seen: set[int] = set()
        current: Optional[BaseException] = error
        depth = 0
        while current is not None and depth < max_depth and id(current) not in seen:
            seen.add(id(current))
            line = f"{type(current).__name__}: {current}"

            status_code = getattr(current, "status_code", None)
            if status_code is not None:
                line += f" [status_code={status_code}]"

            request_id = getattr(current, "request_id", None)
            if request_id:
                line += f" [request_id={request_id}]"

            body = getattr(current, "body", None)
            if body not in (None, ""):
                line += f" [body={self._safe_json_preview(body)}]"

            response = getattr(current, "response", None)
            response_text: Any = None
            if response is not None:
                if isinstance(response, dict):
                    response_text = response.get("text") or response.get("body") or response.get("content")
                else:
                    response_text = getattr(response, "text", None)
                    if response_text is None:
                        response_text = getattr(response, "content", None)
            if response_text not in (None, "", b""):
                if isinstance(response_text, (bytes, bytearray)):
                    try:
                        response_text = response_text.decode("utf-8", errors="replace")
                    except Exception:
                        response_text = str(response_text)
                line += f" [response={self._safe_json_preview(str(response_text))}]"

            details.append(line.strip())
            current = current.__cause__ or current.__context__
            depth += 1

        return " | caused_by=".join(details) if details else str(error)

    def __del__(self):
        """析构时尽力释放共享连接池引用。"""
        if not getattr(self, "_http_client_acquired", False):
            return
        client_state = getattr(self, "_active_client_state", None)
        if client_state is None:
            return
        client = getattr(client_state, "http_client", None)
        lease = getattr(client_state, "lease", None)
        if client is None or lease is None:
            return
        try:
            loop = asyncio.get_event_loop()
        except Exception:
            return
        if loop.is_closed():
            return
        if loop.is_running():
            try:
                loop.create_task(_release_vl_http_client(lease, client))
            except Exception:
                return

    def _build_openai_client(self, http_client: httpx.AsyncClient) -> AsyncOpenAI:
        client_kwargs: Dict[str, Any] = {
            "api_key": self._api_key,
            "base_url": self.base_url,
            "http_client": http_client,
        }
        default_headers: Dict[str, str] = {}
        if self._api_key:
            default_headers["Authorization"] = f"Bearer {self._api_key}"
        if self.appid:
            default_headers["appid"] = self.appid
        if default_headers:
            client_kwargs["default_headers"] = default_headers
        return AsyncOpenAI(**client_kwargs)

    async def close(self):
        """显式释放共享连接池引用。"""
        states: list[_VLClientState] = []
        async with self._client_lock:
            if self._active_client_state is not None:
                states.append(self._active_client_state)
            if self._retired_client_states:
                states.extend(self._retired_client_states)
            self._active_client_state = None
            self._retired_client_states = []
            self.http_client = None
            self.client = None
            self._http_client_lease = None
            self._http_client_acquired = False
        for state in states:
            await _release_vl_http_client(state.lease, state.http_client)

    async def _ensure_client(self) -> None:
        release_targets: list[tuple[VLHttpClientLease, httpx.AsyncClient]] = []
        async with self._client_lock:
            if (
                self._active_client_state is not None
                and self._active_client_state.http_client is not None
                and not self._active_client_state.http_client.is_closed
                and self._active_client_state.openai_client is not None
            ):
                return
            stale_state = self._active_client_state
            if stale_state is not None:
                stale_state.retiring = True
                self._retired_client_states.append(stale_state)
                self._active_client_state = None
                if stale_state.inflight <= 0:
                    release_targets.append((stale_state.lease, stale_state.http_client))
                    try:
                        self._retired_client_states.remove(stale_state)
                    except ValueError:
                        pass
            http_client, lease = _acquire_vl_http_client(self.vl_request_timeout_sec)
            openai_client = self._build_openai_client(http_client)
            self._active_client_state = _VLClientState(
                lease=lease,
                http_client=http_client,
                openai_client=openai_client,
            )
            self.http_client = http_client
            self.client = openai_client
            self._http_client_lease = lease
            self._http_client_acquired = True
        for lease, client in release_targets:
            await _release_vl_http_client(lease, client)

    async def _acquire_client_state(self) -> _VLClientState:
        await self._ensure_client()
        async with self._client_lock:
            if self._active_client_state is None:
                raise RuntimeError("VL active client not initialized")
            self._active_client_state.inflight += 1
            return self._active_client_state

    async def _release_client_state(self, client_state: _VLClientState) -> None:
        release_target: Optional[tuple[VLHttpClientLease, httpx.AsyncClient]] = None
        async with self._client_lock:
            client_state.inflight = max(0, int(client_state.inflight) - 1)
            if client_state.retiring and client_state.inflight == 0:
                release_target = (client_state.lease, client_state.http_client)
                if client_state in self._retired_client_states:
                    self._retired_client_states.remove(client_state)
        if release_target is not None:
            await _release_vl_http_client(release_target[0], release_target[1])

    async def _rotate_active_client(
        self,
        *,
        failed_state: Optional[_VLClientState],
        reason: str,
    ) -> None:
        old_id = ""
        new_id = ""
        async with self._client_lock:
            active_state = self._active_client_state
            if active_state is None:
                return
            if failed_state is not None and active_state is not failed_state:
                return
            if active_state.retiring:
                return
            old_id = str(active_state.lease.client_id or "")
            active_state.retiring = True
            self._retired_client_states.append(active_state)
            http_client, lease = _acquire_vl_http_client(
                self.vl_request_timeout_sec,
                force_new=True,
            )
            openai_client = self._build_openai_client(http_client)
            self._active_client_state = _VLClientState(
                lease=lease,
                http_client=http_client,
                openai_client=openai_client,
            )
            self.http_client = http_client
            self.client = openai_client
            self._http_client_lease = lease
            self._http_client_acquired = True
            new_id = str(lease.client_id or "")
        if reason:
            logger.warning(
                "VL 客户端已切换: reason=%s, old_client_id=%s, new_client_id=%s",
                reason,
                old_id or "-",
                new_id or "-",
            )

    def _normalize_analysis_mode(self, analysis_mode: Optional[str]) -> str:
        """方法说明：VLVideoAnalyzer._normalize_analysis_mode 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        mode = str(analysis_mode or "default").strip().lower()
        if mode in {"tutorial", "tutorial_stepwise", "teaching"}:
            return "tutorial_stepwise"
        if mode in {"concrete", "concrete_focus"}:
            return "concrete"
        return "default"

    def _build_video_duration_cache_key(self, video_path: str) -> str:
        source = Path(str(video_path or ""))
        try:
            stat = source.stat()
            return f"{str(source.resolve())}::{int(stat.st_size)}::{int(stat.st_mtime_ns)}"
        except Exception:
            return str(source)

    def _extract_duration_from_filename(self, video_path: str) -> float:
        stem = Path(str(video_path or "")).stem
        if not stem:
            return 0.0
        match = re.search(r"(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)$", stem)
        if not match:
            return 0.0
        start_sec = safe_float(match.group(1), 0.0)
        end_sec = safe_float(match.group(2), 0.0)
        if end_sec <= start_sec:
            return 0.0
        return max(0.0, end_sec - start_sec)

    def _resolve_video_duration_sec(self, video_path: str) -> float:
        cache_key = self._build_video_duration_cache_key(video_path)
        cached_duration = self._video_duration_cache.get(cache_key)
        if cached_duration is not None:
            return max(0.0, float(cached_duration))

        duration_sec = self._extract_duration_from_filename(video_path)
        if duration_sec <= 0.0:
            try:
                _fps, metadata_duration, _width, _height = get_video_basic_metadata(video_path)
                duration_sec = max(0.0, float(metadata_duration))
            except Exception:
                duration_sec = 0.0

        self._video_duration_cache[cache_key] = max(0.0, float(duration_sec))
        return max(0.0, float(duration_sec))

    def _resolve_vl_request_timeout_sec(self, video_path: str) -> float:
        fallback_timeout = max(1.0, float(self.vl_request_timeout_sec))
        duration_ratio = max(0.0, float(self.vl_request_timeout_ratio_by_video_duration))
        if duration_ratio <= 0.0:
            return fallback_timeout

        duration_sec = self._resolve_video_duration_sec(video_path)
        if duration_sec <= 0.0:
            return fallback_timeout

        calculated_timeout = duration_sec * duration_ratio
        if calculated_timeout <= 0.0:
            return fallback_timeout

        return max(float(self.vl_request_timeout_min_sec), float(calculated_timeout))

    def _resolve_vl_hedge_delay_ms(self, video_path: str) -> int:
        timeout_sec = self._resolve_vl_request_timeout_sec(video_path)
        return max(1, int(round(timeout_sec * 1000.0)))

    def _build_long_video_upload_output_path(self, video_path: str) -> Path:
        source = Path(video_path)
        try:
            stat = source.stat()
            fingerprint = (
                f"{str(source.resolve())}::{int(stat.st_size)}::{int(stat.st_mtime_ns)}::"
                f"{self.long_video_upload_target_height}::{self.long_video_upload_crf}::"
                f"{self.long_video_upload_preset}::{self.long_video_upload_target_fps}::"
                f"{self.long_video_upload_drop_audio}"
            )
        except Exception:
            fingerprint = (
                f"{video_path}::{self.long_video_upload_target_height}::{self.long_video_upload_crf}::"
                f"{self.long_video_upload_preset}::{self.long_video_upload_target_fps}::"
                f"{self.long_video_upload_drop_audio}"
            )
        digest = md5_short_text_compat(fingerprint)
        cache_dir = source.parent / "_vl_upload_cache"
        return cache_dir / f"{source.stem}_{digest}_{int(self.long_video_upload_target_height)}p.mp4"

    async def _compress_video_for_dashscope_upload(self, video_path: str) -> str:
        ffmpeg_bin = resolve_ffmpeg_bin()
        if not ffmpeg_bin:
            logger.warning("VL long-video upload compression skipped because ffmpeg is unavailable: %s", video_path)
            return video_path

        source_path = Path(video_path)
        if not source_path.exists():
            return video_path

        output_path = self._build_long_video_upload_output_path(video_path)
        try:
            if output_path.exists() and output_path.stat().st_size > 0:
                source_size = source_path.stat().st_size
                output_size = output_path.stat().st_size
                if output_size < source_size:
                    return str(output_path)
                logger.warning(
                    "VL upload compression cache ignored because compressed file is not smaller: source=%s, compressed=%s, source_bytes=%s, compressed_bytes=%s",
                    video_path,
                    str(output_path),
                    source_size,
                    output_size,
                )
                try:
                    output_path.unlink(missing_ok=True)
                except Exception:
                    pass
        except Exception:
            pass

        output_path.parent.mkdir(parents=True, exist_ok=True)
        target_height = int(self.long_video_upload_target_height)
        fps_value = float(self.long_video_upload_target_fps)
        if abs(fps_value - int(round(fps_value))) < 1e-6:
            fps_arg = str(int(round(fps_value)))
        else:
            fps_arg = f"{fps_value:.3f}".rstrip("0").rstrip(".")
        scale_filter = (
            f"scale=if(gt(ih\\,{target_height})\\,-2\\,iw):"
            f"if(gt(ih\\,{target_height})\\,{target_height}\\,ih)"
        )
        command = [
            ffmpeg_bin,
            "-y",
            "-i",
            str(source_path),
            "-map",
            "0:v:0",
            "-vf",
            scale_filter,
            "-c:v",
            "libx264",
            "-crf",
            str(int(self.long_video_upload_crf)),
            "-preset",
            str(self.long_video_upload_preset),
            "-r",
            fps_arg,
        ]
        if self.long_video_upload_drop_audio:
            command.extend(["-an"])
        else:
            command.extend(
                [
                    "-map",
                    "0:a?",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "128k",
                ]
            )
        command.extend(
            [
                "-movflags",
                "+faststart",
                str(output_path),
            ]
        )

        def _run_ffmpeg() -> None:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=int(self.long_video_upload_timeout_sec),
            )
            if result.returncode != 0:
                stderr_text = str(result.stderr or "").strip()
                raise RuntimeError(
                    f"ffmpeg rc={result.returncode}, stderr={stderr_text[:500]}"
                )

        try:
            await asyncio.to_thread(_run_ffmpeg)
        except Exception as exc:
            logger.warning(
                "VL long-video upload compression failed, fallback to source video: path=%s, error=%s",
                video_path,
                self._format_exception_detail(exc),
            )
            return video_path

        try:
            if output_path.exists() and output_path.stat().st_size > 0:
                source_size = source_path.stat().st_size
                output_size = output_path.stat().st_size
                if output_size >= source_size:
                    logger.warning(
                        "VL upload compression fallback to source because compressed file is not smaller: source=%s, compressed=%s, source_bytes=%s, compressed_bytes=%s",
                        video_path,
                        str(output_path),
                        source_size,
                        output_size,
                    )
                    try:
                        output_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return video_path
                logger.info(
                    "VL long-video upload compression applied: source=%s, compressed=%s, source_bytes=%s, compressed_bytes=%s",
                    video_path,
                    str(output_path),
                    source_size,
                    output_size,
                )
                return str(output_path)
        except Exception:
            pass
        return video_path

    async def _prepare_video_for_dashscope_upload(self, video_path: str) -> str:
        normalized_path = str(video_path or "").replace("\\", "/").strip().lower()
        if normalized_path:
            parts = [part for part in normalized_path.split("/") if part]
            if "semantic_unit_clips_vl" in parts and "_stream_units" in parts:
                return video_path
        if not bool(self.long_video_upload_compress_enabled):
            return video_path

        return await self._compress_video_for_dashscope_upload(video_path)

    def _should_use_dashscope_offline_task(self) -> bool:
        return bool(self.vl_offline_task_enabled) and self._is_dashscope_endpoint(self.base_url)

    def _to_plain_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, list):
            return [self._to_plain_value(item) for item in value]
        if isinstance(value, tuple):
            return [self._to_plain_value(item) for item in value]
        if isinstance(value, dict):
            return {str(k): self._to_plain_value(v) for k, v in value.items()}
        if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
            try:
                return self._to_plain_value(value.to_dict())
            except Exception:
                pass
        if hasattr(value, "__dict__"):
            try:
                return self._to_plain_value(vars(value))
            except Exception:
                pass
        try:
            return self._to_plain_value(dict(value))
        except Exception:
            return str(value)

    def _convert_openai_messages_to_dashscope_messages(self, messages: Any) -> List[Dict[str, Any]]:
        converted: List[Dict[str, Any]] = []
        if not isinstance(messages, list):
            return [{"role": "user", "content": [{"text": str(messages or "")}]}]

        for message in messages:
            if not isinstance(message, dict):
                continue
            role = str(message.get("role", "user") or "user").strip() or "user"
            content = message.get("content")
            converted_content: List[Dict[str, Any]] = []

            if isinstance(content, str):
                if content:
                    converted_content.append({"text": content})
            elif isinstance(content, list):
                for item in content:
                    if not isinstance(item, dict):
                        text_value = str(item or "")
                        if text_value:
                            converted_content.append({"text": text_value})
                        continue
                    item_type = str(item.get("type", "") or "").strip().lower()
                    if item_type == "text":
                        text_value = str(item.get("text", "") or "")
                        if text_value:
                            converted_content.append({"text": text_value})
                        continue
                    if item_type == "image_url":
                        image_payload = item.get("image_url") if isinstance(item.get("image_url"), dict) else {}
                        image_url = str(image_payload.get("url", "") or "")
                        if image_url:
                            converted_content.append({"image": image_url})
                        continue
                    if item_type == "video_url":
                        video_payload = item.get("video_url") if isinstance(item.get("video_url"), dict) else {}
                        video_url = str(video_payload.get("url", "") or "")
                        if video_url:
                            converted_content.append({"video": video_url})
                        continue
                    unknown_text = self._safe_json_preview(item, max_len=4000)
                    if unknown_text:
                        converted_content.append({"text": unknown_text})
            else:
                fallback_text = self._safe_json_preview(content, max_len=4000)
                if fallback_text:
                    converted_content.append({"text": fallback_text})

            if not converted_content:
                converted_content = [{"text": ""}]
            converted.append({"role": role, "content": converted_content})

        if not converted:
            return [{"role": "user", "content": [{"text": ""}]}]
        return converted

    def _extract_text_from_dashscope_payload(self, payload: Any) -> str:
        plain_payload = self._to_plain_value(payload)

        def _extract(node: Any) -> str:
            if isinstance(node, str):
                return node
            if isinstance(node, list):
                for item in node:
                    result = _extract(item)
                    if result:
                        return result
                return ""
            if not isinstance(node, dict):
                return ""

            direct_text = node.get("text")
            if isinstance(direct_text, str) and direct_text.strip():
                return direct_text

            content = node.get("content")
            if isinstance(content, str) and content.strip():
                return content
            if isinstance(content, list):
                merged: List[str] = []
                for content_item in content:
                    if isinstance(content_item, dict):
                        text_value = content_item.get("text")
                        if isinstance(text_value, str) and text_value.strip():
                            merged.append(text_value)
                    elif isinstance(content_item, str) and content_item.strip():
                        merged.append(content_item)
                if merged:
                    return "".join(merged)

            for nested_key in ("message", "output", "result"):
                nested_value = node.get(nested_key)
                result = _extract(nested_value)
                if result:
                    return result

            for list_key in ("choices", "results", "items", "data"):
                list_value = node.get(list_key)
                if isinstance(list_value, list):
                    result = _extract(list_value)
                    if result:
                        return result
            return ""

        return _extract(plain_payload)

    def _extract_finish_reason_from_dashscope_payload(self, payload: Any) -> Optional[str]:
        plain_payload = self._to_plain_value(payload)
        if not isinstance(plain_payload, dict):
            return None

        finish_reason = plain_payload.get("finish_reason")
        if isinstance(finish_reason, str) and finish_reason.strip():
            return finish_reason

        choices = plain_payload.get("choices")
        if isinstance(choices, list):
            for choice in choices:
                choice_payload = self._to_plain_value(choice)
                if not isinstance(choice_payload, dict):
                    continue
                reason = choice_payload.get("finish_reason")
                if isinstance(reason, str) and reason.strip():
                    return reason
        return None

    def _normalize_dashscope_usage(self, usage_payload: Any) -> Dict[str, Any]:
        return normalize_usage_payload(self._to_plain_value(usage_payload))

    def _extract_batch_id(self, batch_payload: Any) -> str:
        payload = self._to_plain_value(batch_payload)
        if not isinstance(payload, dict):
            return ""
        value = payload.get("id") or payload.get("batch_id")
        return str(value).strip() if value else ""

    def _extract_batch_status(self, batch_payload: Any) -> str:
        payload = self._to_plain_value(batch_payload)
        if not isinstance(payload, dict):
            return ""
        status = payload.get("status")
        if isinstance(status, str) and status.strip():
            return status.strip().lower()
        return ""

    def _extract_batch_output_file_id(self, batch_payload: Any) -> str:
        payload = self._to_plain_value(batch_payload)
        if not isinstance(payload, dict):
            return ""
        value = payload.get("output_file_id")
        return str(value).strip() if value else ""

    def _extract_batch_error_file_id(self, batch_payload: Any) -> str:
        payload = self._to_plain_value(batch_payload)
        if not isinstance(payload, dict):
            return ""
        value = payload.get("error_file_id")
        return str(value).strip() if value else ""

    def _extract_batch_error_preview(self, batch_payload: Any) -> str:
        payload = self._to_plain_value(batch_payload)
        if not isinstance(payload, dict):
            return ""
        errors = payload.get("errors")
        if errors not in (None, "", [], {}):
            return self._safe_json_preview(errors)
        return ""

    @staticmethod
    def _is_batch_done_status(task_status: str) -> bool:
        return str(task_status or "").strip().lower() in {
            "completed",
            "succeeded",
            "success",
            "done",
            "finished",
        }

    @staticmethod
    def _is_batch_failed_status(task_status: str) -> bool:
        return str(task_status or "").strip().lower() in {
            "failed",
            "expired",
            "cancelled",
            "canceled",
            "error",
        }

    def _build_dashscope_batch_input_record(
        self,
        *,
        messages: Any,
        custom_id: str,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "max_tokens": int(self.max_tokens),
            "temperature": float(self.temperature),
        }
        return {
            "custom_id": str(custom_id or ""),
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": body,
        }

    async def _read_batch_output_text(
        self,
        output_file_id: str,
        client: Optional[AsyncOpenAI] = None,
    ) -> str:
        if not output_file_id:
            return ""
        if client is None:
            await self._ensure_client()
            client = self.client
        if client is None:
            return ""
        content_response = await client.files.content(output_file_id)
        text_attr = getattr(content_response, "text", "")
        if callable(text_attr):
            text_value = text_attr()
        else:
            text_value = text_attr
        if isinstance(text_value, str) and text_value.strip():
            return text_value

        read_method = getattr(content_response, "read", None)
        if callable(read_method):
            binary_value = read_method()
            if isinstance(binary_value, (bytes, bytearray)):
                return bytes(binary_value).decode("utf-8", errors="replace")
            if isinstance(binary_value, str):
                return binary_value
        return ""

    def _extract_batch_result_body(self, *, jsonl_text: str, custom_id: str) -> Any:
        last_line_preview = ""
        for raw_line in str(jsonl_text or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            last_line_preview = line
            try:
                row = self._to_plain_value(json.loads(line))
            except Exception:
                continue
            if not isinstance(row, dict):
                continue
            row_custom_id = str(row.get("custom_id", "") or "").strip()
            if row_custom_id != str(custom_id or "").strip():
                continue

            row_error = row.get("error")
            if row_error not in (None, "", {}, []):
                raise RuntimeError(
                    f"DashScope batch response has error row: custom_id={custom_id}, error={self._safe_json_preview(row_error)}"
                )

            response_payload = self._to_plain_value(row.get("response"))
            if isinstance(response_payload, dict):
                response_status = safe_int(response_payload.get("status_code", 0), 0)
                if response_status >= 400:
                    raise RuntimeError(
                        "DashScope batch response status failed: "
                        f"custom_id={custom_id}, status_code={response_status}, response={self._safe_json_preview(response_payload)}"
                    )
                if "body" in response_payload:
                    return self._to_plain_value(response_payload.get("body"))
                return response_payload

            return row

        raise RuntimeError(
            "DashScope batch output missing target custom_id: "
            f"custom_id={custom_id}, output_preview={self._safe_json_preview(last_line_preview)}"
        )

    async def _call_vl_api_with_dashscope_offline_task(
        self,
        *,
        messages: Any,
        client: Optional[AsyncOpenAI] = None,
    ) -> tuple[str, Optional[str], Dict[str, int], Dict[str, Any]]:
        if not self._api_key:
            raise RuntimeError(
                f"DashScope offline task requires api_key, env={self._api_key_env or 'DASHSCOPE_API_KEY'}"
            )

        if client is None:
            await self._ensure_client()
            client = self.client
        if client is None:
            raise RuntimeError("VL client not initialized")

        plain_messages = self._to_plain_value(messages)
        request_transport_meta = self._summarize_message_transport(plain_messages)
        request_custom_id = f"vl-offline-{uuid.uuid4().hex}"
        batch_record = self._build_dashscope_batch_input_record(
            messages=plain_messages,
            custom_id=request_custom_id,
        )
        batch_jsonl = json.dumps(batch_record, ensure_ascii=False) + "\n"
        body_bytes = self._json_utf8_size_bytes(batch_record.get("body", {}))
        jsonl_bytes = len(batch_jsonl.encode("utf-8"))
        logger.info(
            "DashScope VL offline batch input prepared: transport=%s, body_bytes=%s, body_limit_bytes=%s, jsonl_bytes=%s, video_url_count=%s, temp_url_count=%s, data_uri_video_count=%s, image_url_count=%s",
            str(request_transport_meta.get("transport", "unknown") or "unknown"),
            body_bytes,
            int(_DASHSCOPE_BATCH_BODY_MAX_BYTES),
            jsonl_bytes,
            int(request_transport_meta.get("video_url_count", 0) or 0),
            int(request_transport_meta.get("temp_url_count", 0) or 0),
            int(request_transport_meta.get("data_uri_video_count", 0) or 0),
            int(request_transport_meta.get("image_url_count", 0) or 0),
        )
        if body_bytes > int(_DASHSCOPE_BATCH_BODY_MAX_BYTES):
            logger.warning(
                "DashScope VL offline batch body exceeds known server limit before submit: transport=%s, body_bytes=%s, body_limit_bytes=%s",
                str(request_transport_meta.get("transport", "unknown") or "unknown"),
                body_bytes,
                int(_DASHSCOPE_BATCH_BODY_MAX_BYTES),
            )
        batch_file_payload = (
            "vl_offline_input.jsonl",
            batch_jsonl.encode("utf-8"),
            "application/jsonl",
        )

        try:
            input_file = await client.files.create(
                file=batch_file_payload,
                purpose="batch",
            )
        except Exception as exc:
            raise RuntimeError(
                f"DashScope batch input upload failed: {self._format_exception_detail(exc)}"
            ) from exc

        input_file_id = str(getattr(input_file, "id", "") or "").strip()
        if not input_file_id:
            raise RuntimeError(
                f"DashScope batch input upload succeeded but file id is missing: {self._safe_json_preview(input_file)}"
            )
        logger.info(
            "DashScope batch JSONL uploaded: input_file_id=%s, transport=%s, body_bytes=%s, jsonl_bytes=%s, note=batch JSONL only stores the offline request envelope and does not prove video media temp_url upload succeeded",
            input_file_id,
            str(request_transport_meta.get("transport", "unknown") or "unknown"),
            body_bytes,
            jsonl_bytes,
        )

        try:
            submit_response = await client.batches.create(
                input_file_id=input_file_id,
                endpoint="/v1/chat/completions",
                completion_window="24h",
            )
        except Exception as exc:
            raise RuntimeError(
                f"DashScope batch submit failed: {self._format_exception_detail(exc)}"
            ) from exc

        task_id = self._extract_batch_id(submit_response)
        if not task_id:
            raise RuntimeError(
                f"DashScope batch submit succeeded but task_id is missing: {self._safe_json_preview(submit_response)}"
            )

        logger.info(
            "DashScope VL offline batch submitted: task_id=%s, input_file_id=%s, poll_interval_sec=%.1f, transport=%s, body_bytes=%s",
            task_id,
            input_file_id,
            float(self.vl_offline_poll_interval_sec),
            str(request_transport_meta.get("transport", "unknown") or "unknown"),
            body_bytes,
        )

        deadline = time.monotonic() + float(self.vl_offline_max_wait_sec)
        poll_count = 0
        last_status = ""

        while True:
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"DashScope offline task polling timeout: task_id={task_id}, last_status={last_status or 'UNKNOWN'}"
                )

            poll_count += 1
            try:
                poll_response = await client.batches.retrieve(task_id)
            except Exception as exc:
                raise RuntimeError(
                    f"DashScope batch polling failed: task_id={task_id}, detail={self._format_exception_detail(exc)}"
                ) from exc

            task_status = self._extract_batch_status(poll_response)
            if task_status:
                last_status = task_status
            error_preview = self._extract_batch_error_preview(poll_response)

            if self._is_batch_failed_status(task_status):
                raise RuntimeError(
                    "DashScope batch task failed: "
                    f"task_id={task_id}, task_status={task_status}, errors={error_preview or 'none'}"
                )

            if self._is_batch_done_status(task_status):
                output_file_id = self._extract_batch_output_file_id(poll_response)
                if not output_file_id:
                    error_file_id = self._extract_batch_error_file_id(poll_response)
                    error_preview = ""
                    if error_file_id:
                        try:
                            error_preview = await self._read_batch_output_text(error_file_id, client=client)
                        except Exception as exc:
                            error_preview = f"read_error={self._format_exception_detail(exc)}"
                    raise RuntimeError(
                        "DashScope batch completed but output_file_id is empty: "
                        f"task_id={task_id}, error_file_id={error_file_id or 'none'}, "
                        f"error_preview={self._safe_json_preview(error_preview, max_len=1500)}, "
                        f"response={self._safe_json_preview(poll_response)}"
                    )
                output_text = await self._read_batch_output_text(output_file_id, client=client)
                final_payload = self._extract_batch_result_body(
                    jsonl_text=output_text,
                    custom_id=request_custom_id,
                )
                content = self._extract_text_from_dashscope_payload(final_payload)
                finish_reason = self._extract_finish_reason_from_dashscope_payload(final_payload)
                usage_payload = {}
                if isinstance(final_payload, dict):
                    usage_payload = final_payload.get("usage", {}) or {}
                usage = self._normalize_dashscope_usage(usage_payload)
                if not content:
                    raise RuntimeError(
                        "DashScope batch completed but parsed content is empty: "
                        f"task_id={task_id}, output_file_id={output_file_id}, payload={self._safe_json_preview(final_payload)}"
                    )
                return (
                    content,
                    finish_reason,
                    usage,
                    {
                        "task_id": task_id,
                        "batch_id": task_id,
                        "input_file_id": input_file_id,
                        "output_file_id": output_file_id,
                        "task_status": task_status.upper(),
                        "poll_count": poll_count,
                        "body_bytes": int(body_bytes),
                        "jsonl_bytes": int(jsonl_bytes),
                        "message_transport": str(request_transport_meta.get("transport", "unknown") or "unknown"),
                        "message_transport_meta": dict(request_transport_meta or {}),
                    },
                )

            await asyncio.sleep(float(self.vl_offline_poll_interval_sec))

    def _normalize_step_type(self, value: Any) -> str:
        """归一教程步骤类型，未知值回落为 MAIN_FLOW。"""
        text = str(value or "").strip().upper()
        if not text:
            return "MAIN_FLOW"
        if text in {"MAIN_FLOW", "MAIN", "PRIMARY", "PRIMARY_FLOW"}:
            return "MAIN_FLOW"
        if text in {"CONDITIONAL", "CONDITION", "BRANCH"}:
            return "CONDITIONAL"
        if text in {"OPTIONAL", "OPTION"}:
            return "OPTIONAL"
        if text in {"TROUBLESHOOTING", "TROUBLESHOOT", "DEBUG", "ERROR_FIX", "RECOVERY"}:
            return "TROUBLESHOOTING"
        return "MAIN_FLOW"

    @staticmethod
    def _build_route_rules_zh(*, subject: str, no_needed_index: int, should_type_index: int) -> str:
        """统一生成中文路由规则文本，避免 default/tutorial 规则漂移。"""
        return (
            f"{no_needed_index}) no_needed_video 判定规则：\n"
            f"   - 若{subject}不存在有价值的动态展示，且仅靠文字即可完整传达信息，必须返回 no_needed_video=true。\n"
            f"   - 若视频中的动态演示对理解或复现有价值，返回 no_needed_video=false。\n"
            f"{should_type_index}) should_type 路由覆盖规则（可选）：\n"
            "   - should_type=abstract: 按 abstract 路由处理。\n"
            "   - should_type=concrete: 按 concrete 路由处理。\n"
            "   - 若 no_needed_video=true，则应等价按 abstract 路由处理（覆盖优先级最高）。\n"
        )

    @staticmethod
    def _build_route_rules_en(
        *,
        subject: str,
        no_needed_prefix: str = "",
        should_type_prefix: str = "",
    ) -> str:
        """统一生成英文路由规则文本，供 fallback prompt 复用。"""
        no_needed_lead = f"{no_needed_prefix} " if str(no_needed_prefix or "").strip() else ""
        should_type_lead = f"{should_type_prefix} " if str(should_type_prefix or "").strip() else ""
        return (
            f"{no_needed_lead}no_needed_video rule: true only when {subject} has no valuable dynamic visual signal and text alone is sufficient.\n"
            f"{should_type_lead}should_type rule (optional): abstract/concrete only. "
            "If no_needed_video=true, treat it as abstract routing with highest priority.\n"
        )

    @staticmethod
    def _get_builtin_output_constraints_tutorial() -> str:
        """方法说明：VLVideoAnalyzer._get_builtin_output_constraints_tutorial 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return (
            "\n\n"
            "[Hard Constraints - Tutorial Stepwise Mode]\n"
            "1) Output exactly one valid JSON array. No markdown, no prefix/suffix text, no explanations.\n"
            "2) Each array item must be one complete step.\n"
            "3) Required fields per item: step_id (Integer), step_description (String), "
            "clip_start_sec (Float), clip_end_sec (Float), main_operation (String), instructional_keyframes (List[Object]).\n"
            "   instructional_keyframes item fields: keyframe_id (String, e.g. KEYFRAME_1), timestamp_sec (Float), optional frame_reason (String), "
            "optional bbox ([xmin,ymin,xmax,ymax], 0-1000).\n"
            "   Optional fields: instructional_clips (List[Object]), main_action (String), precautions (List[String]), "
            "step_summary (String), operation_guidance (List[String]), step_type (MAIN_FLOW/CONDITIONAL/OPTIONAL/TROUBLESHOOTING).\n"
            "   If an optional field is unnecessary for a step, omit it or return an empty value.\n"
            "4) Do not output reasoning, key_evidence, or knowledge_type fields.\n"
            "5) Segmentation rules:\n"
            "   - Keep explanation + execution + result of the same step together.\n"
            "   - Remove thinking/hesitation time (mouse wandering, idle pause, no new information).\n"
            "   - No step shorter than 5 seconds. Merge short steps with adjacent steps.\n"
            "6) instructional_keyframes must be true instructional keyframes, "
            "prefer final state or just-before-submit moments.\n"
            "7) instructional_clips are optional short motion clips aligned with [CLIP_N] placeholders in main_operation.\n"
            "8) Avoid -1 for timestamps; if action spans whole clip use [0.0, clip_duration].\n"
        )

    @staticmethod
    def _get_builtin_output_constraints_default() -> str:
        """方法说明：VLVideoAnalyzer._get_builtin_output_constraints_default 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return (
            "\n\n"
            "【输出硬性约束】\n"
            "1) 只输出一个标准 JSON，不要 Markdown 代码块、不要解释、不要前后缀文字。\n"
            "2) 顶层必须是一个扁平 JSON 数组：[{...}, {...}]。\n"
            "3) 每个对象必须包含字段：id, knowledge_type, no_needed_video, confidence, clip_start_sec, clip_end_sec, suggested_screenshoot_timestamps。\n"
            "   可选字段：should_type（仅允许 abstract / concrete）。\n"
            "4) 严禁输出 reasoning / key_evidence 字段，避免无关文本增加 token。\n"
            "5) 时间边界判断规则：\n"
            "   - 对于非【讲解型】内容，禁止随意输出 -1；请根据视觉变化尽力估算起止时间。\n"
            "   - 若该知识类型贯穿整个片段，可设 [0.0, clip_duration]。\n"
            "   - 仅在视觉信息完全无法支持判断时，才允许输出 -1。\n"
            + VLVideoAnalyzer._build_route_rules_zh(
                subject="该片段",
                no_needed_index=6,
                should_type_index=7,
            )
        )

    @staticmethod
    def _get_builtin_output_constraints_concrete() -> str:
        return (
            "\n\n"
            "[Hard Constraints - Concrete Mode]\n"
            "1) Output exactly one valid JSON array. No markdown fences and no extra text.\n"
            "2) Each array item must include: segment_id (Integer), segment_description (String), main_content (String), "
            "clip_start_sec (Float), clip_end_sec (Float), instructional_keyframes (List[Object]).\n"
            "3) instructional_keyframes item fields: keyframe_id (String, e.g. KEYFRAME_1), timestamp_sec (Float), frame_reason (String), "
            "optional bbox([xmin,ymin,xmax,ymax],0-1000).\n"
            "4) Optional instructional_clips item fields: clip_id (String, e.g. CLIP_1), start_sec (Float), end_sec (Float), clip_reason (String).\n"
            "5) Keep all textual fields in Chinese. Do not output reasoning/key_evidence/step_type.\n"
            "6) main_content must be markdown and use [KEYFRAME_N]/[CLIP_N] placeholders aligned with instructional_keyframes/instructional_clips.\n"
            "7) Use relative clip timestamps (from 0.0). Do not output -1.\n"
        )

    def _get_output_constraints(self, analysis_mode: str = "default") -> str:
        """获取当前分析模式的输出约束提示词。"""
        mode = self._normalize_analysis_mode(analysis_mode)
        if mode == "tutorial_stepwise":
            return getattr(self, "_constraints_tutorial", "") or self._get_builtin_output_constraints_tutorial()
        if mode == "concrete":
            return getattr(self, "_constraints_concrete", "") or self._get_builtin_output_constraints_concrete()
        return getattr(self, "_constraints_default", "") or self._get_builtin_output_constraints_default()

    def _load_prompt_template(self) -> str:
        """方法说明：VLVideoAnalyzer._load_prompt_template 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return get_prompt(
            PromptKeys.VL_VIDEO_ANALYSIS_DEFAULT_USER,
            fallback=self._get_default_prompt(),
        )

    def _get_default_prompt(self) -> str:
        """获取默认提示词"""
        return '''请分析这段视频，识别其中的知识片段。

对于每个识别出的知识片段，请输出以下信息：
1. id: 片段序号（从0开始）
2. knowledge_type: 知识类型，必须是以下之一：
   - "实操" - 实际操作演示
   - "推演" - 推理演示过程
   - "环境配置" - 环境或配置设置
   - "过程性知识" - 过程性知识展示
   - "讲解型" - 纯讲解无视觉操作
3. confidence: 置信度（0-1）
4. clip_start_sec: 片段起始时间（秒，相对于视频开头）
5. clip_end_sec: 片段结束时间（秒）
6. suggested_screenshoot_timestamps: 建议的截图时间点数组（秒）
7. no_needed_video: 是否不需要视频表达（布尔值）
   - true: 该片段无有价值的动态展示，仅靠文字即可完整承载信息
   - false: 该片段存在有价值的动态展示
8. should_type: 路由覆盖类型（可选）
   - 仅允许 "abstract" 或 "concrete"
   - 若不需要覆盖，可省略

请以 JSON 数组格式输出，格式如下：
```json
[
  {
    "id": 0,
    "knowledge_type": "实操",
    "no_needed_video": false,
    "should_type": "concrete",
    "confidence": 0.9,
    "clip_start_sec": 0.0,
    "clip_end_sec": 10.0,
    "suggested_screenshoot_timestamps": [2.0, 5.0, 8.0]
  }
]
```'''
    
    def convert_timestamps(
        self, 
        relative_timestamps: List[float], 
        semantic_unit_start_sec: float
    ) -> List[float]:
        """
        将相对时间戳转换为绝对时间戳
        
        Args:
            relative_timestamps: 相对于片段开头的时间戳列表
            semantic_unit_start_sec: 语义单元在原视频中的起始时间
            
        Returns:
            绝对时间戳列表（相对于原视频开头）
        """
        return [semantic_unit_start_sec + ts for ts in relative_timestamps]
    
    async def analyze_clip(
        self,
        clip_path: str,
        semantic_unit_start_sec: float,
        semantic_unit_id: str,
        extra_prompt: Optional[str] = None,
        analysis_mode: str = "default",
        wave_id: str = "",
    ) -> VLClipAnalysisResponse:
        """
        分析单个视频片段。

        Args:
            clip_path: 视频片段文件路径
            semantic_unit_start_sec: 语义单元在原视频中的起始时间（秒）
            semantic_unit_id: 语义单元 ID
            extra_prompt: 附加提示词（可选）
            analysis_mode: default / tutorial_stepwise

        Returns:
            VLClipAnalysisResponse: 包含切片与关键帧请求的完整结果
        """
        result = VLClipAnalysisResponse()
        normalized_mode = self._normalize_analysis_mode(analysis_mode)
        result.analysis_mode = normalized_mode

        try:
            # 调用 VL API
            api_call_result = await self._call_vl_api(
                clip_path,
                semantic_unit_id=semantic_unit_id,
                extra_prompt=extra_prompt,
                analysis_mode=normalized_mode,
                wave_id=wave_id,
            )
            if isinstance(api_call_result, tuple) and len(api_call_result) == 4:
                analysis_results, token_usage, raw_json, raw_interactions = api_call_result
            elif isinstance(api_call_result, tuple) and len(api_call_result) == 3:
                analysis_results, token_usage, raw_json = api_call_result
                raw_interactions = []
            else:
                raise ValueError(f"unexpected _call_vl_api return payload: {type(api_call_result)}")
            result.token_usage = token_usage
            result.raw_response_json = raw_json or []
            result.raw_llm_interactions = raw_interactions or []

            if not analysis_results:
                result.success = False
                result.error_msg = "VL API returned empty result"
                return result

            # 计算并补齐绝对时间字段
            no_needed_video_count = 0
            should_abstract_count = 0
            should_concrete_count = 0
            for i, ar in enumerate(analysis_results):
                ar.analysis_mode = normalized_mode

                # 将相对时间转换为原视频绝对时间
                ar.absolute_clip_start_sec = semantic_unit_start_sec + ar.clip_start_sec
                ar.absolute_clip_end_sec = semantic_unit_start_sec + ar.clip_end_sec
                ar.absolute_screenshot_timestamps = [
                    semantic_unit_start_sec + ts
                    for ts in ar.suggested_screenshoot_timestamps
                ]

                route_override = str(getattr(ar, "should_type", "") or "").strip().lower()
                if route_override == "abstract":
                    should_abstract_count += 1
                elif route_override == "concrete":
                    should_concrete_count += 1

                if ar.no_needed_video:
                    ar.knowledge_type = "abstract"
                    ar.should_type = "abstract"
                    route_override = "abstract"
                    no_needed_video_count += 1

                result.analysis_results.append(ar)

                step_id = int(ar.step_id) if int(ar.step_id) > 0 else (i + 1)
                step_type = self._normalize_step_type(getattr(ar, "step_type", ""))
                action_brief = self._sanitize_action_brief(ar.step_description)

                instructional_clip_requests: List[Dict[str, Any]] = []
                for clip_index, clip_meta in enumerate(list(getattr(ar, "instructional_clips", []) or []), start=1):
                    normalized_clip_id = self._normalize_clip_id(
                        clip_meta.get("clip_id", clip_meta.get("clipId", clip_index)),
                        fallback_index=clip_index,
                    )
                    clip_number_match = re.search(r"(\d+)$", normalized_clip_id)
                    clip_number = int(clip_number_match.group(1)) if clip_number_match else clip_index
                    clip_reason = str(clip_meta.get("clip_reason", "") or "").strip()
                    clip_rel_start = safe_float(clip_meta.get("start_sec", ar.clip_start_sec), ar.clip_start_sec)
                    clip_rel_end = safe_float(clip_meta.get("end_sec", ar.clip_end_sec), clip_rel_start)
                    if clip_rel_end < clip_rel_start:
                        clip_rel_start, clip_rel_end = clip_rel_end, clip_rel_start
                    clip_abs_start = semantic_unit_start_sec + clip_rel_start
                    clip_abs_end = semantic_unit_start_sec + clip_rel_end
                    if normalized_mode == "tutorial_stepwise":
                        clip_stem = f"{semantic_unit_id}_clip_step_{step_id:02d}_clip_{clip_number:02d}_{action_brief}"
                    else:
                        clip_stem = f"{semantic_unit_id}_clip_concrete_seg_{step_id:02d}_clip_{clip_number:02d}_{action_brief}"
                    instructional_clip_requests.append({
                        "clip_id": self._build_unit_relative_asset_id(semantic_unit_id, clip_stem),
                        "start_sec": clip_abs_start,
                        "end_sec": clip_abs_end,
                        "_analysis_relative_start_sec": clip_rel_start,
                        "_analysis_relative_end_sec": clip_rel_end,
                        "knowledge_type": "process" if normalized_mode == "tutorial_stepwise" else ar.knowledge_type,
                        "semantic_unit_id": semantic_unit_id,
                        "step_id": step_id,
                        "step_description": ar.step_description,
                        "step_type": step_type,
                        "action_brief": action_brief,
                        "main_action": str(ar.main_action or "").strip(),
                        "main_operation": list(ar.main_operation or []),
                        "precautions": list(ar.precautions or []),
                        "step_summary": str(ar.step_summary or "").strip(),
                        "operation_guidance": list(ar.operation_guidance or []),
                        "analysis_mode": normalized_mode,
                        "instructional_clip_id": normalized_clip_id,
                        "clip_reason": clip_reason,
                    })

                if normalized_mode == "tutorial_stepwise":
                    if instructional_clip_requests:
                        result.clip_requests.extend(instructional_clip_requests)
                    tutorial_clip_stem = f"{semantic_unit_id}_clip_step_{step_id:02d}_{action_brief}"
                    result.clip_requests.append({
                        "clip_id": self._build_unit_relative_asset_id(semantic_unit_id, tutorial_clip_stem),
                        "start_sec": ar.absolute_clip_start_sec,
                        "end_sec": ar.absolute_clip_end_sec,
                        "_analysis_relative_start_sec": ar.clip_start_sec,
                        "_analysis_relative_end_sec": ar.clip_end_sec,
                        "knowledge_type": "process",
                        "semantic_unit_id": semantic_unit_id,
                        "step_id": step_id,
                        "step_description": ar.step_description,
                        "step_type": step_type,
                        "action_brief": action_brief,
                        "main_action": str(ar.main_action or "").strip(),
                        "main_operation": list(ar.main_operation or []),
                        "precautions": list(ar.precautions or []),
                        "step_summary": str(ar.step_summary or "").strip(),
                        "operation_guidance": list(ar.operation_guidance or []),
                        "analysis_mode": normalized_mode,
                    })
                elif normalized_mode == "concrete":
                    if instructional_clip_requests:
                        result.clip_requests.extend(instructional_clip_requests)
                else:
                    if ar.no_needed_video:
                        continue
                    if route_override == "abstract":
                        continue
                    k_type = str(ar.knowledge_type or "").strip("[]() \"'").lower()
                    should_build_clip = normalized_mode != "concrete" and route_override != "concrete"
                    if should_build_clip and k_type not in {"\u8bb2\u89e3\u578b", "explanation", "abstract_explanation"}:
                        default_clip_stem = f"{semantic_unit_id}_clip_vl_{i + 1:03d}"
                        result.clip_requests.append({
                            "clip_id": self._build_unit_relative_asset_id(semantic_unit_id, default_clip_stem),
                            "start_sec": ar.absolute_clip_start_sec,
                            "end_sec": ar.absolute_clip_end_sec,
                            "_analysis_relative_start_sec": ar.clip_start_sec,
                            "_analysis_relative_end_sec": ar.clip_end_sec,
                            "knowledge_type": ar.knowledge_type,
                            "semantic_unit_id": semantic_unit_id,
                            "step_id": step_id,
                            "step_description": ar.step_description,
                            "step_type": step_type,
                            "action_brief": action_brief,
                            "main_action": str(ar.main_action or "").strip(),
                            "main_operation": list(ar.main_operation or []),
                            "precautions": list(ar.precautions or []),
                            "step_summary": str(ar.step_summary or "").strip(),
                            "operation_guidance": list(ar.operation_guidance or []),
                            "analysis_mode": normalized_mode,
                        })

                if ar.no_needed_video:
                    continue
                if route_override == "abstract":
                    continue
                for j, ts in enumerate(ar.absolute_screenshot_timestamps):
                    screenshot_id = self._build_unit_relative_asset_id(
                        semantic_unit_id,
                        f"{semantic_unit_id}_ss_vl_{i + 1:02d}_{j + 1:02d}",
                    )
                    label = f"{ar.knowledge_type}_screenshot_{j+1}"
                    keyframe_meta: Dict[str, Any] = {}
                    if normalized_mode == "tutorial_stepwise":
                        screenshot_id = self._build_unit_relative_asset_id(
                            semantic_unit_id,
                            f"{semantic_unit_id}_ss_step_{step_id:02d}_key_{j + 1:02d}_{action_brief}",
                        )
                        label = f"step_{step_id:02d}:{ar.step_description or action_brief}_keyframe_{j+1}"
                    elif normalized_mode == "concrete":
                        screenshot_id = self._build_unit_relative_asset_id(
                            semantic_unit_id,
                            f"{semantic_unit_id}_ss_concrete_seg_{step_id:02d}_key_{j + 1:02d}",
                        )
                        label = f"concrete_segment_{step_id:02d}_keyframe_{j+1}"
                    if j < len(ar.instructional_keyframes or []):
                        keyframe_meta = dict(ar.instructional_keyframes[j] or {})
                    keyframe_id = self._normalize_keyframe_id(
                        keyframe_meta.get("keyframe_id", keyframe_meta.get("keyframeId", j + 1)),
                        fallback_index=j + 1,
                    )

                    result.screenshot_requests.append({
                        "screenshot_id": screenshot_id,
                        "timestamp_sec": ts,
                        "label": label,
                        "semantic_unit_id": semantic_unit_id,
                        "_relative_timestamp": ar.suggested_screenshoot_timestamps[j],
                        "_analysis_relative_timestamp": ar.suggested_screenshoot_timestamps[j],
                        "_semantic_unit_start": semantic_unit_start_sec,
                        "step_id": step_id,
                        "step_description": ar.step_description,
                        "step_type": step_type,
                        "action_brief": action_brief,
                        "analysis_mode": normalized_mode,
                        "is_instructional_keyframe": normalized_mode == "tutorial_stepwise",
                        "keyframe_index": j + 1,
                        "keyframe_id": keyframe_id,
                        "frame_reason": str(keyframe_meta.get("frame_reason", "") or ""),
                        "bbox": self._normalize_bbox_1000(keyframe_meta.get("bbox")),
                    })

            result.success = True
            logger.info(
                f"VL analysis completed: {semantic_unit_id}, mode={normalized_mode}, "
                f"clips={len(result.clip_requests)}, screenshots={len(result.screenshot_requests)}, "
                f"no_needed_video={no_needed_video_count}, "
                f"should_abstract={should_abstract_count}, should_concrete={should_concrete_count}, "
                f"prompt_tokens={result.token_usage.get('prompt_tokens', 0)}, "
                f"total_tokens={result.token_usage.get('total_tokens', 0)}"
            )

        except Exception as e:
            error_detail = self._format_exception_detail(e)
            logger.error(f"VL analysis failed ({semantic_unit_id}): {error_detail}", exc_info=True)
            result.success = False
            result.error_msg = error_detail
            result.raw_llm_interactions = list(getattr(e, "_raw_llm_interactions", []) or [])

        return result

    async def analyze_clips_batch(
        self,
        *,
        tasks: List[Dict[str, Any]],
        max_inflight: Optional[int] = None,
        return_exceptions: bool = True,
        result_callback: Optional[Callable[[int, Any], Awaitable[None] | None]] = None,
    ) -> List[Any]:
        """批量分析多个视频片段，按输入顺序返回结果。"""
        if not tasks:
            return []

        default_inflight = max(1, min(len(tasks), (os.cpu_count() or 4)))
        try:
            resolved_inflight = int(max_inflight) if max_inflight is not None else default_inflight
        except Exception:
            resolved_inflight = default_inflight
        resolved_inflight = max(1, min(resolved_inflight, len(tasks)))

        semaphore = asyncio.Semaphore(resolved_inflight)
        ordered_results: List[Any] = [None] * len(tasks)
        pending_callbacks: set[asyncio.Task] = set()

        async def _run_single(index: int, task: Dict[str, Any]) -> tuple[int, Any]:
            async with semaphore:
                try:
                    result_item = await self.analyze_clip(
                        clip_path=str(task.get("clip_path", "") or ""),
                        semantic_unit_start_sec=float(task.get("semantic_unit_start_sec", 0.0) or 0.0),
                        semantic_unit_id=str(task.get("semantic_unit_id", "") or ""),
                        extra_prompt=task.get("extra_prompt"),
                        analysis_mode=str(task.get("analysis_mode", "default") or "default"),
                        wave_id=str(task.get("wave_id", "") or ""),
                    )
                except Exception as exc:
                    if return_exceptions:
                        result_item = exc
                    else:
                        raise
            return index, result_item

        inflight_tasks = [
            asyncio.create_task(_run_single(index, task))
            for index, task in enumerate(tasks)
        ]
        try:
            for done_task in asyncio.as_completed(inflight_tasks):
                index, result_item = await done_task
                ordered_results[index] = result_item
                if result_callback is not None:
                    async def _invoke_callback(i: int, item: Any) -> None:
                        try:
                            callback_result = result_callback(i, item)
                            if asyncio.isfuture(callback_result) or asyncio.iscoroutine(callback_result):
                                await callback_result
                        except Exception as callback_error:
                            logger.warning("[VL-Batch] result_callback failed: index=%s, err=%s", i, callback_error)

                    callback_task = asyncio.create_task(_invoke_callback(index, result_item))
                    pending_callbacks.add(callback_task)
                    callback_task.add_done_callback(lambda finished: pending_callbacks.discard(finished))
        finally:
            for task in inflight_tasks:
                if not task.done():
                    task.cancel()
            if pending_callbacks:
                await asyncio.gather(*list(pending_callbacks), return_exceptions=True)

        if return_exceptions:
            return [item if item is not None else RuntimeError("empty_batch_result") for item in ordered_results]

        finalized_results: List[VLClipAnalysisResponse] = []
        for index, item in enumerate(ordered_results):
            if not isinstance(item, VLClipAnalysisResponse):
                raise RuntimeError(f"batch result invalid at index={index}")
            finalized_results.append(item)
        return finalized_results

    def _extract_token_usage(self, response: Any) -> Dict[str, Any]:
        """
        从 OpenAI 兼容响应中提取 token 使用量。

        兼容对象/字典两种网关返回形态，缺失字段时兜底为 0。
        """
        usage = getattr(response, "usage", None)
        if usage is None and isinstance(response, dict):
            usage = response.get("usage")
        return normalize_usage_payload(usage)

    def _build_vl_cache_key(
        self,
        *,
        video_path: str,
        analysis_mode: str,
        extra_prompt: Optional[str],
        response_format: Optional[Dict[str, Any]],
    ) -> str:
        """
        作用：构建 VL 调用缓存键。
        为什么：同一视频与同一参数组合应复用结果，避免重复成本。
        权衡：缓存键过粗可能误命中，过细会降低命中率，因此覆盖关键输入与配置。
        """
        abs_path = str(Path(video_path).resolve())
        file_size = 0
        file_mtime = 0
        try:
            stat = Path(abs_path).stat()
            file_size = int(stat.st_size)
            file_mtime = int(stat.st_mtime)
        except Exception:
            file_size = 0
            file_mtime = 0

        prompt_hash = fast_digest_text(self.prompt_template or "")
        response_format_text = ""
        if response_format:
            try:
                response_format_text = json.dumps(response_format, sort_keys=True, ensure_ascii=False)
            except Exception:
                response_format_text = str(response_format)

        payload = {
            "video_path": abs_path,
            "file_size": file_size,
            "file_mtime": file_mtime,
            "model": self.model,
            "base_url": self.base_url,
            "temperature": float(self.temperature),
            "max_tokens": int(self.max_tokens),
            "analysis_mode": str(analysis_mode or ""),
            "prompt_hash": prompt_hash,
            "extra_prompt": str(extra_prompt or ""),
            "video_input_mode": str(self.video_input_mode or ""),
            "max_input_frames": int(self.max_input_frames),
            "max_image_dim": int(self.max_image_dim),
            "compression_crf": float(self.compression_crf),
            "max_video_size_mb": float(self.max_video_size_mb),
            "vl_request_timeout_sec": float(self.vl_request_timeout_sec),
            "vl_request_timeout_ratio_by_video_duration": float(self.vl_request_timeout_ratio_by_video_duration),
            "vl_request_timeout_min_sec": float(self.vl_request_timeout_min_sec),
            "vl_offline_task_enabled": bool(self.vl_offline_task_enabled),
            "vl_offline_poll_interval_sec": float(self.vl_offline_poll_interval_sec),
            "vl_offline_max_wait_sec": float(self.vl_offline_max_wait_sec),
            "long_video_upload_compress_enabled": bool(self.long_video_upload_compress_enabled),
            "long_video_upload_target_height": int(self.long_video_upload_target_height),
            "long_video_upload_target_bitrate": str(self.long_video_upload_target_bitrate or ""),
            "long_video_upload_min_bitrate": str(self.long_video_upload_min_bitrate or ""),
            "long_video_upload_max_bitrate": str(self.long_video_upload_max_bitrate or ""),
            "long_video_upload_timeout_sec": int(self.long_video_upload_timeout_sec),
            "long_video_upload_crf": int(self.long_video_upload_crf),
            "long_video_upload_preset": str(self.long_video_upload_preset or ""),
            "long_video_upload_target_fps": float(self.long_video_upload_target_fps),
            "long_video_upload_drop_audio": bool(self.long_video_upload_drop_audio),
            "dashscope_upload_chunk_size_bytes": int(self.dashscope_upload_chunk_size_bytes),
            "dashscope_upload_timeout_by_video_duration": bool(self.dashscope_upload_timeout_by_video_duration),
            "dashscope_upload_timeout_min_sec": float(self.dashscope_upload_timeout_min_sec),
            "dashscope_upload_retry_max_attempts": int(self.dashscope_upload_retry_max_attempts),
            "dashscope_upload_retry_initial_backoff_sec": float(self.dashscope_upload_retry_initial_backoff_sec),
            "dashscope_upload_retry_multiplier": float(self.dashscope_upload_retry_multiplier),
            "dashscope_upload_retry_max_backoff_sec": float(self.dashscope_upload_retry_max_backoff_sec),
            "dashscope_upload_retry_jitter_sec": float(self.dashscope_upload_retry_jitter_sec),
            "optimize_screenshots": bool(self.optimize_screenshots),
            "search_window_sec": float(self.search_window_sec),
            "tutorial_min_step_duration_sec": float(self.tutorial_min_step_duration_sec),
            "response_format": response_format_text,
        }

        raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return "vl:" + fast_digest_text(raw)

    @staticmethod
    def _sanitize_media_url_for_audit(url: str) -> Dict[str, Any]:
        """审计落盘时对媒体 URL 做轻量脱敏，避免 data-uri 体积膨胀。"""
        raw = str(url or "")
        if not raw:
            return {"kind": "empty", "value": ""}
        if raw.startswith("data:"):
            digest = sha256_text(raw)
            prefix = raw[:64]
            return {
                "kind": "data_uri",
                "prefix": prefix,
                "length": len(raw),
                "sha256": digest,
            }
        return {"kind": "url", "value": raw}

    def _sanitize_messages_for_audit(self, messages: Any) -> List[Dict[str, Any]]:
        """保留文本与结构化字段，压缩媒体体积，便于逐步追溯 LLM 交互。"""
        sanitized: List[Dict[str, Any]] = []
        if not isinstance(messages, list):
            return sanitized

        for message in messages:
            if not isinstance(message, dict):
                continue
            role = str(message.get("role", "") or "").strip()
            content = message.get("content")
            normalized_content: Any
            if isinstance(content, list):
                normalized_items: List[Dict[str, Any]] = []
                for item in content:
                    if not isinstance(item, dict):
                        normalized_items.append({"type": "unknown", "value": str(item)})
                        continue
                    item_type = str(item.get("type", "") or "").strip().lower()
                    if item_type == "text":
                        normalized_items.append(
                            {
                                "type": "text",
                                "text": str(item.get("text", "") or ""),
                            }
                        )
                    elif item_type == "image_url":
                        image_obj = item.get("image_url") if isinstance(item.get("image_url"), dict) else {}
                        normalized_items.append(
                            {
                                "type": "image_url",
                                "image_url": self._sanitize_media_url_for_audit(str(image_obj.get("url", "") or "")),
                            }
                        )
                    elif item_type == "video_url":
                        video_obj = item.get("video_url") if isinstance(item.get("video_url"), dict) else {}
                        normalized_items.append(
                            {
                                "type": "video_url",
                                "video_url": self._sanitize_media_url_for_audit(str(video_obj.get("url", "") or "")),
                            }
                        )
                    else:
                        normalized_items.append(
                            {
                                "type": item_type or "unknown",
                                "value": json.loads(json.dumps(item, ensure_ascii=False)),
                            }
                        )
                normalized_content = normalized_items
            elif isinstance(content, str):
                normalized_content = content
            elif content is None:
                normalized_content = ""
            else:
                try:
                    normalized_content = json.loads(json.dumps(content, ensure_ascii=False))
                except Exception:
                    normalized_content = str(content)
            sanitized.append({"role": role, "content": normalized_content})
        return sanitized

    @staticmethod
    def _build_runtime_video_signature(video_path: str) -> Dict[str, Any]:
        safe_path = str(video_path or "").strip()
        if not safe_path:
            return {"path": "", "exists": False}
        path = Path(safe_path)
        try:
            stat_result = path.stat()
            return {
                "path": str(path.resolve()),
                "exists": True,
                "size": int(stat_result.st_size),
                "mtime_ns": int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000))),
                "suffix": str(path.suffix or "").lower(),
            }
        except Exception:
            return {
                "path": safe_path,
                "exists": False,
                "suffix": str(path.suffix or "").lower(),
            }

    @staticmethod
    def _sanitize_media_url_for_runtime_restore(url: str) -> Dict[str, Any]:
        """恢复判重不能依赖临时 URL，这里只保留稳定特征。"""
        raw = str(url or "")
        if not raw:
            return {"kind": "empty", "value": ""}
        if raw.startswith("data:"):
            header, _, _ = raw.partition(",")
            return {
                "kind": "data_uri",
                "header": header[:96],
                "length": len(raw),
                "sha256": sha256_text(raw),
            }
        suffix = ""
        try:
            suffix = str(Path(raw.split("?", 1)[0]).suffix or "").lower()
        except Exception:
            suffix = ""
        scheme = raw.split("://", 1)[0].lower() if "://" in raw else ""
        return {
            "kind": "remote_url",
            "scheme": scheme,
            "suffix": suffix,
            "value": "omitted",
        }

    def _sanitize_messages_for_runtime_restore(self, messages: Any) -> List[Dict[str, Any]]:
        """恢复真源只保留稳定输入，避免 temp_url 等瞬时字段污染 fingerprint。"""
        sanitized: List[Dict[str, Any]] = []
        if not isinstance(messages, list):
            return sanitized

        for message in messages:
            if not isinstance(message, dict):
                continue
            role = str(message.get("role", "") or "").strip()
            content = message.get("content")
            normalized_content: Any
            if isinstance(content, list):
                normalized_items: List[Dict[str, Any]] = []
                for item in content:
                    if not isinstance(item, dict):
                        normalized_items.append({"type": "unknown", "value": str(item)})
                        continue
                    item_type = str(item.get("type", "") or "").strip().lower()
                    if item_type == "text":
                        normalized_items.append({"type": "text", "text": str(item.get("text", "") or "")})
                        continue
                    if item_type == "image_url":
                        image_obj = item.get("image_url") if isinstance(item.get("image_url"), dict) else {}
                        normalized_items.append(
                            {
                                "type": "image_url",
                                "image_url": self._sanitize_media_url_for_runtime_restore(
                                    str(image_obj.get("url", "") or "")
                                ),
                            }
                        )
                        continue
                    if item_type == "video_url":
                        video_obj = item.get("video_url") if isinstance(item.get("video_url"), dict) else {}
                        normalized_items.append(
                            {
                                "type": "video_url",
                                "video_url": self._sanitize_media_url_for_runtime_restore(
                                    str(video_obj.get("url", "") or "")
                                ),
                            }
                        )
                        continue
                    normalized_items.append({"type": item_type or "unknown", "value": str(item)})
                normalized_content = normalized_items
            elif isinstance(content, str):
                normalized_content = content
            elif content is None:
                normalized_content = ""
            else:
                normalized_content = str(content)
            sanitized.append({"role": role, "content": normalized_content})
        return sanitized

    def _build_vl_runtime_identity(
        self,
        *,
        semantic_unit_id: str,
        analysis_mode: str,
        video_path: str,
        wave_id: str = "",
    ) -> Dict[str, Any]:
        unit_token = re.sub(r"[^0-9A-Za-z._-]+", "_", str(semantic_unit_id or "").strip()).strip("._-") or "UNKNOWN_UNIT"
        clip_token = re.sub(r"[^0-9A-Za-z._-]+", "_", Path(str(video_path or "")).stem).strip("._-") or "clip"
        clip_token = clip_token[:24]
        normalized_mode = str(analysis_mode or "default").strip().lower() or "default"
        identity = {
            "step_name": f"phase2a_vl_{normalized_mode}",
            "unit_id": unit_token,
            "chunk_id": f"unit_{unit_token}_vl_{clip_token}",
            "request_scope_ids": [unit_token],
        }
        if str(wave_id or "").strip():
            identity["wave_id"] = str(wave_id).strip()
        return identity

    def _build_vl_media_prepare_runtime_identity(
        self,
        *,
        semantic_unit_id: str,
        video_path: str,
        wave_id: str = "",
    ) -> Dict[str, Any]:
        unit_token = re.sub(r"[^0-9A-Za-z._-]+", "_", str(semantic_unit_id or "").strip()).strip("._-") or "UNKNOWN_UNIT"
        clip_token = re.sub(r"[^0-9A-Za-z._-]+", "_", Path(str(video_path or "")).stem).strip("._-") or "clip"
        clip_token = clip_token[:24]
        identity = {
            "step_name": "phase2a_vl_media_prepare",
            "unit_id": unit_token,
            "chunk_id": f"unit_{unit_token}_media_{clip_token}",
            "request_scope_ids": [unit_token],
        }
        if str(wave_id or "").strip():
            identity["wave_id"] = str(wave_id).strip()
        return identity

    def _build_vl_media_prepare_request_payload(
        self,
        *,
        video_path: str,
        video_duration_sec: float,
    ) -> Dict[str, Any]:
        return {
            "provider": "dashscope",
            "base_url": str(self.base_url or "").strip(),
            "video": self._build_runtime_video_signature(video_path),
            "video_duration_sec": float(video_duration_sec or 0.0),
            "video_input_mode": str(self.video_input_mode or "").strip(),
            "long_video_upload_compress_enabled": bool(self.long_video_upload_compress_enabled),
            "long_video_upload_target_height": int(self.long_video_upload_target_height),
            "long_video_upload_target_bitrate": str(self.long_video_upload_target_bitrate or ""),
            "long_video_upload_min_bitrate": str(self.long_video_upload_min_bitrate or ""),
            "long_video_upload_max_bitrate": str(self.long_video_upload_max_bitrate or ""),
            "long_video_upload_crf": int(self.long_video_upload_crf),
            "long_video_upload_preset": str(self.long_video_upload_preset or ""),
            "long_video_upload_target_fps": float(self.long_video_upload_target_fps),
            "long_video_upload_drop_audio": bool(self.long_video_upload_drop_audio),
            "dashscope_upload_chunk_size_bytes": int(self.dashscope_upload_chunk_size_bytes),
            "dashscope_upload_timeout_by_video_duration": bool(self.dashscope_upload_timeout_by_video_duration),
            "dashscope_upload_timeout_min_sec": float(self.dashscope_upload_timeout_min_sec),
            "dashscope_upload_retry_max_attempts": int(self.dashscope_upload_retry_max_attempts),
            "dashscope_upload_retry_initial_backoff_sec": float(self.dashscope_upload_retry_initial_backoff_sec),
            "dashscope_upload_retry_multiplier": float(self.dashscope_upload_retry_multiplier),
            "dashscope_upload_retry_max_backoff_sec": float(self.dashscope_upload_retry_max_backoff_sec),
            "dashscope_upload_retry_jitter_sec": float(self.dashscope_upload_retry_jitter_sec),
        }

    def _build_vl_runtime_request_payload(
        self,
        *,
        video_path: str,
        messages: Any,
        analysis_mode: str,
        extra_prompt: Optional[str],
        request_transport_meta: Dict[str, Any],
        video_duration_sec: float,
        use_dashscope_offline_task: bool,
        vl_request_timeout_sec: float,
        vl_hedge_delay_ms: int,
    ) -> Dict[str, Any]:
        prompt_hash = sha256_text(self.prompt_template or "")
        return {
            "provider": str(self.provider or "").strip().lower() or "vl",
            "model": str(self.model or "").strip(),
            "base_url": str(self.base_url or "").strip(),
            "analysis_mode": str(analysis_mode or "").strip(),
            "extra_prompt": str(extra_prompt or ""),
            "video": self._build_runtime_video_signature(video_path),
            "video_duration_sec": float(video_duration_sec or 0.0),
            "prompt_template_hash": prompt_hash,
            "temperature": float(self.temperature),
            "max_tokens": int(self.max_tokens),
            "video_input_mode": str(self.video_input_mode or ""),
            "max_input_frames": int(self.max_input_frames),
            "max_image_dim": int(self.max_image_dim),
            "max_video_size_mb": float(self.max_video_size_mb),
            "compression_crf": float(self.compression_crf),
            "vl_request_timeout_sec": float(vl_request_timeout_sec),
            "vl_hedge_delay_ms": int(vl_hedge_delay_ms or 0),
            "vl_offline_task_enabled": bool(use_dashscope_offline_task),
            "vl_offline_poll_interval_sec": float(self.vl_offline_poll_interval_sec),
            "vl_offline_max_wait_sec": float(self.vl_offline_max_wait_sec),
            "long_video_upload_compress_enabled": bool(self.long_video_upload_compress_enabled),
            "long_video_upload_target_height": int(self.long_video_upload_target_height),
            "long_video_upload_target_bitrate": str(self.long_video_upload_target_bitrate or ""),
            "long_video_upload_min_bitrate": str(self.long_video_upload_min_bitrate or ""),
            "long_video_upload_max_bitrate": str(self.long_video_upload_max_bitrate or ""),
            "long_video_upload_timeout_sec": int(self.long_video_upload_timeout_sec),
            "long_video_upload_crf": int(self.long_video_upload_crf),
            "long_video_upload_preset": str(self.long_video_upload_preset or ""),
            "long_video_upload_target_fps": float(self.long_video_upload_target_fps),
            "long_video_upload_drop_audio": bool(self.long_video_upload_drop_audio),
            "dashscope_upload_chunk_size_bytes": int(self.dashscope_upload_chunk_size_bytes),
            "dashscope_upload_timeout_by_video_duration": bool(self.dashscope_upload_timeout_by_video_duration),
            "dashscope_upload_timeout_min_sec": float(self.dashscope_upload_timeout_min_sec),
            "dashscope_upload_retry_max_attempts": int(self.dashscope_upload_retry_max_attempts),
            "dashscope_upload_retry_initial_backoff_sec": float(self.dashscope_upload_retry_initial_backoff_sec),
            "dashscope_upload_retry_multiplier": float(self.dashscope_upload_retry_multiplier),
            "dashscope_upload_retry_max_backoff_sec": float(self.dashscope_upload_retry_max_backoff_sec),
            "dashscope_upload_retry_jitter_sec": float(self.dashscope_upload_retry_jitter_sec),
            "message_transport_meta": dict(request_transport_meta or {}),
            "messages": self._sanitize_messages_for_runtime_restore(messages),
        }

    @staticmethod
    def _compute_retry_backoff_sec(
        *,
        attempt_index: int,
        initial_backoff_sec: float,
        multiplier: float,
        max_backoff_sec: float,
        jitter_sec: float = 0.0,
    ) -> float:
        wait_time = float(initial_backoff_sec) * (float(multiplier) ** float(max(0, int(attempt_index))))
        wait_time = min(float(max_backoff_sec), wait_time)
        if jitter_sec > 0:
            wait_time += random.uniform(0.0, float(jitter_sec))
        return max(0.0, wait_time)

    def _summarize_message_transport(self, messages: Any) -> Dict[str, Any]:
        summary: Dict[str, Any] = {
            "transport": "unknown",
            "video_url_count": 0,
            "temp_url_count": 0,
            "data_uri_video_count": 0,
            "image_url_count": 0,
            "data_uri_video_bytes": 0,
            "image_data_uri_bytes": 0,
        }
        if not isinstance(messages, list):
            return summary

        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if not isinstance(content, list):
                continue
            for item in content:
                if not isinstance(item, dict):
                    continue
                item_type = str(item.get("type", "") or "").strip().lower()
                if item_type == "video_url":
                    summary["video_url_count"] += 1
                    video_obj = item.get("video_url") if isinstance(item.get("video_url"), dict) else {}
                    raw_url = str(video_obj.get("url", "") or "")
                    if raw_url.startswith("data:"):
                        summary["data_uri_video_count"] += 1
                        summary["data_uri_video_bytes"] += len(raw_url.encode("utf-8"))
                    elif raw_url:
                        summary["temp_url_count"] += 1
                elif item_type == "image_url":
                    summary["image_url_count"] += 1
                    image_obj = item.get("image_url") if isinstance(item.get("image_url"), dict) else {}
                    raw_url = str(image_obj.get("url", "") or "")
                    if raw_url.startswith("data:"):
                        summary["image_data_uri_bytes"] += len(raw_url.encode("utf-8"))

        if summary["temp_url_count"] > 0:
            summary["transport"] = "temp_url"
        elif summary["data_uri_video_count"] > 0:
            summary["transport"] = "data_uri_video"
        elif summary["image_url_count"] > 0:
            summary["transport"] = "keyframes"
        elif summary["video_url_count"] > 0:
            summary["transport"] = "video_url"
        else:
            summary["transport"] = "text_only"
        return summary

    @staticmethod
    def _extract_temp_urls_from_messages(messages: Any) -> List[str]:
        temp_urls: List[str] = []
        seen: set[str] = set()
        if not isinstance(messages, list):
            return temp_urls
        for message in messages:
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if not isinstance(content, list):
                continue
            for item in content:
                if not isinstance(item, dict):
                    continue
                if str(item.get("type", "") or "").strip().lower() != "video_url":
                    continue
                video_obj = item.get("video_url") if isinstance(item.get("video_url"), dict) else {}
                raw_url = str(video_obj.get("url", "") or "").strip()
                if not raw_url or raw_url.startswith("data:"):
                    continue
                if raw_url in seen:
                    continue
                seen.add(raw_url)
                temp_urls.append(raw_url)
        return temp_urls

    @staticmethod
    def _json_utf8_size_bytes(value: Any) -> int:
        try:
            return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))
        except Exception:
            return 0

    async def _call_vl_api(
        self,
        video_path: str,
        semantic_unit_id: str,
        extra_prompt: Optional[str] = None,
        analysis_mode: str = "default",
        wave_id: str = "",
    ) -> tuple[List[VLAnalysisResult], Dict[str, int], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        调用 VL API 并解析结果。

        Returns:
            tuple: (分析结果列表, token 使用量, 归一化 JSON 结果)
        """
        normalized_mode = self._normalize_analysis_mode(analysis_mode)
        video_duration_sec = self._resolve_video_duration_sec(video_path)
        use_dashscope_offline_task = self._should_use_dashscope_offline_task()
        vl_request_timeout_sec = self._resolve_vl_request_timeout_sec(video_path)
        vl_hedge_delay_ms = self._resolve_vl_hedge_delay_ms(video_path)
        if use_dashscope_offline_task:
            logger.info(
                "VL offline task mode enabled: video_path=%s, duration_sec=%.2f, poll_interval_sec=%.1f, max_wait_sec=%.1f",
                video_path,
                video_duration_sec,
                float(self.vl_offline_poll_interval_sec),
                float(self.vl_offline_max_wait_sec),
            )
        else:
            logger.info(
                "VL request timing resolved: video_path=%s, duration_sec=%.2f, timeout_sec=%.2f, hedge_delay_ms=%s",
                video_path,
                video_duration_sec,
                vl_request_timeout_sec,
                vl_hedge_delay_ms,
            )
        runtime_context = current_runtime_llm_context()
        runtime_identity = self._build_vl_runtime_identity(
            semantic_unit_id=semantic_unit_id,
            analysis_mode=normalized_mode,
            video_path=video_path,
            wave_id=wave_id,
        )
        messages_result = await self._build_messages(
            video_path,
            extra_prompt=extra_prompt,
            analysis_mode=normalized_mode,
            runtime_identity=runtime_identity,
            return_transport_detail=True,
        )
        media_prepare_interaction: Optional[Dict[str, Any]] = None
        if isinstance(messages_result, tuple) and len(messages_result) == 2:
            messages, media_prepare_interaction = messages_result
        else:
            messages = messages_result
        request_messages_audit = self._sanitize_messages_for_audit(messages)
        request_transport_meta = self._summarize_message_transport(messages)
        raw_interactions: List[Dict[str, Any]] = []
        if isinstance(media_prepare_interaction, dict):
            raw_interactions.append(media_prepare_interaction)
        else:
            temp_urls = self._extract_temp_urls_from_messages(messages)
            if temp_urls:
                raw_interactions.append(
                    {
                        "stage": "vl_media_prepare",
                        "attempt": 1,
                        "success": True,
                        "request": {
                            "video_path": str(video_path or ""),
                            "analysis_mode": normalized_mode,
                            "video_duration_sec": float(video_duration_sec),
                            "message_transport": str(request_transport_meta.get("transport", "unknown") or "unknown"),
                            "message_transport_meta": dict(request_transport_meta or {}),
                        },
                        "response": {
                            "temp_urls": list(temp_urls),
                            "temp_url_count": len(temp_urls),
                        },
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    }
                )
        runtime_request_payload = self._build_vl_runtime_request_payload(
            video_path=video_path,
            messages=messages,
            analysis_mode=normalized_mode,
            extra_prompt=extra_prompt,
            request_transport_meta=request_transport_meta,
            video_duration_sec=video_duration_sec,
            use_dashscope_offline_task=use_dashscope_offline_task,
            vl_request_timeout_sec=vl_request_timeout_sec,
            vl_hedge_delay_ms=vl_hedge_delay_ms,
        )
        runtime_scope = (
            runtime_context.build_scope_descriptor(
                provider="vl",
                request_name="vl_video_analysis",
                request_payload=runtime_request_payload,
                runtime_identity=runtime_identity,
            )
            if runtime_context is not None
            else {
                "chunk_id": "",
                "llm_call_id": "",
                "scope_ref": "",
            }
        )
        if runtime_context is not None:
            restored = runtime_context.load_committed_call(
                provider="vl",
                request_name="vl_video_analysis",
                request_payload=runtime_request_payload,
                runtime_identity=runtime_identity,
            )
            if restored is not None:
                restored_metadata = dict(restored.get("response_metadata", {}) or {})
                restored_text = str(restored.get("response_text", "") or "")
                try:
                    parsed_results, raw_json = self._parse_response_with_payload(
                        restored_text,
                        finish_reason=str(restored_metadata.get("finish_reason", "") or "") or None,
                        analysis_mode=normalized_mode,
                    )
                except Exception as restore_error:
                    logger.warning(
                        "VL runtime restore parse failed, fallback to live request: unit=%s err=%s",
                        semantic_unit_id,
                        restore_error,
                    )
                else:
                    restored_usage = dict(restored_metadata.get("usage", {}) or {})
                    raw_interactions.append(
                        {
                            "stage": "vl_video_analysis",
                            "attempt": int(restored.get("attempt", 0) or 0),
                            "success": True,
                            "request": {
                                "model": self.model,
                                "temperature": float(self.temperature),
                                "max_tokens": int(self.max_tokens),
                                "analysis_mode": normalized_mode,
                                "video_path": str(video_path or ""),
                                "video_duration_sec": float(video_duration_sec),
                                "offline_task_enabled": bool(use_dashscope_offline_task),
                                "timeout_sec": float(vl_request_timeout_sec),
                                "hedge_delay_ms": int(vl_hedge_delay_ms),
                                "message_transport": str(request_transport_meta.get("transport", "unknown") or "unknown"),
                                "message_transport_meta": dict(request_transport_meta or {}),
                                "messages": request_messages_audit,
                            },
                            "response": {
                                "model": str(restored_metadata.get("model", self.model) or self.model),
                                "cache_hit": True,
                                "runtime_restored": True,
                                "restored_attempt": int(restored.get("attempt", 0) or 0),
                                "attempt_dir": str(restored.get("attempt_dir", "") or ""),
                                "finish_reason": str(restored_metadata.get("finish_reason", "") or ""),
                                "usage": restored_usage,
                                "offline_task_meta": dict(restored_metadata.get("offline_task_meta", {}) or {}),
                                "content": restored_text,
                                "parsed_payload": raw_json,
                            },
                            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        }
                    )
                    return parsed_results, restored_usage, raw_json, raw_interactions

        # 构建稳定 cache_key（仅用于首轮尝试，避免重试阶段误用缓存）
        base_cache_key = None
        try:
            base_cache_key = self._build_vl_cache_key(
                video_path=video_path,
                analysis_mode=normalized_mode,
                extra_prompt=extra_prompt,
                response_format=None,
            )
        except Exception as exc:
            logger.debug(f"VL cache key build skipped: {exc}")

        # 调用 API（含重试）
        last_error = None
        pool_retry_index = 0
        pool_retry_max_attempts = 5
        pool_retry_jitter_sec = max(0.0, float(self.vl_retry_initial_backoff_sec) * 0.3)
        effective_max_retries = max(0, int(self.max_retries))
        attempt = 0
        while attempt <= effective_max_retries:
            client_state = await self._acquire_client_state()
            try:
                response_kwargs = {
                    "model": self.model,
                    "messages": messages,
                    "max_tokens": self.max_tokens,
                    "temperature": self.temperature,
                }

                if attempt > 0:
                    response_kwargs["temperature"] = 0.0
                    # DashScope 网关对 response_format 的兼容性不一致，需要容错。
                    # 使用 json_object 可提升重试场景的 JSON 稳定性。
                    response_kwargs["response_format"] = {"type": "json_object"}

                offline_task_meta: Dict[str, Any] = {}
                vl_response: Optional[llm_gateway.VLChatResult] = None
                if use_dashscope_offline_task:
                    content, finish_reason, token_usage, offline_task_meta = (
                        await self._call_vl_api_with_dashscope_offline_task(
                            messages=messages,
                            client=client_state.openai_client,
                        )
                    )
                    vl_response = llm_gateway.VLChatResult(
                        content=content,
                        finish_reason=finish_reason,
                        usage=dict(token_usage or {}),
                        model=str(offline_task_meta.get("model", self.model) or self.model),
                        cache_hit=False,
                    )
                else:
                    try:
                        cache_key = base_cache_key if attempt == 0 else None
                        result = await llm_gateway.vl_chat_completion(
                            client=client_state.openai_client,
                            model=self.model,
                            messages=messages,
                            max_tokens=self.max_tokens,
                            temperature=response_kwargs["temperature"],
                            response_format=response_kwargs.get("response_format"),
                            cache_key=cache_key,
                            timeout=vl_request_timeout_sec,
                            hedge_delay_ms=vl_hedge_delay_ms,
                        )
                    except Exception as e:
                        # 兼容非 OpenAI 官方网关：不支持 response_format 时回退。
                        err_str = str(e).lower()
                        if "response_format" in response_kwargs and ("response_format" in err_str or "unknown" in err_str):
                            response_kwargs.pop("response_format", None)
                            cache_key = base_cache_key if attempt == 0 else None
                            result = await llm_gateway.vl_chat_completion(
                                client=client_state.openai_client,
                                model=self.model,
                                messages=messages,
                                max_tokens=self.max_tokens,
                                temperature=response_kwargs["temperature"],
                                response_format=None,
                                cache_key=cache_key,
                                timeout=vl_request_timeout_sec,
                                hedge_delay_ms=vl_hedge_delay_ms,
                            )
                        else:
                            raise

                    vl_response = result
                    content = vl_response.content
                    finish_reason = vl_response.finish_reason
                    token_usage = vl_response.usage
                parsed_results, raw_json = self._parse_response_with_payload(
                    content,
                    finish_reason=finish_reason,
                    analysis_mode=normalized_mode,
                )
                if runtime_context is not None:
                    runtime_context.persist_success(
                        provider="vl",
                        request_name="vl_video_analysis",
                        request_payload=runtime_request_payload,
                        response_text=str(content or ""),
                        response_metadata={
                            "model": str(getattr(vl_response, "model", self.model) or self.model),
                            "cache_hit": bool(getattr(vl_response, "cache_hit", False)),
                            "finish_reason": str(finish_reason or ""),
                            "usage": dict(token_usage or {}),
                            "offline_task_meta": dict(offline_task_meta or {}),
                            "raw_json": list(raw_json or []),
                        },
                        runtime_identity=runtime_identity,
                    )
                raw_interactions.append(
                    {
                        "stage": "vl_video_analysis",
                        "attempt": attempt + 1,
                        "success": True,
                        "request": {
                            "model": self.model,
                            "temperature": float(response_kwargs.get("temperature", self.temperature)),
                            "max_tokens": int(self.max_tokens),
                            "response_format": response_kwargs.get("response_format"),
                            "analysis_mode": normalized_mode,
                            "video_path": str(video_path or ""),
                            "video_duration_sec": float(video_duration_sec),
                            "offline_task_enabled": bool(use_dashscope_offline_task),
                            "offline_task_meta": dict(offline_task_meta or {}),
                            "timeout_sec": float(vl_request_timeout_sec),
                            "hedge_delay_ms": int(vl_hedge_delay_ms),
                            "message_transport": str(request_transport_meta.get("transport", "unknown") or "unknown"),
                            "message_transport_meta": dict(request_transport_meta or {}),
                            "messages": request_messages_audit,
                        },
                        "response": {
                            "model": str(getattr(vl_response, "model", self.model) or self.model),
                            "cache_hit": bool(getattr(vl_response, "cache_hit", False)),
                            "finish_reason": finish_reason,
                            "usage": dict(token_usage or {}),
                            "offline_task_meta": dict(offline_task_meta or {}),
                            "content": str(content or ""),
                            "parsed_payload": raw_json,
                        },
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    }
                )
                return parsed_results, token_usage, raw_json, raw_interactions

            except Exception as e:
                err_detail = self._format_exception_detail(e)
                try:
                    setattr(e, "_display_error_detail", err_detail)
                except Exception:
                    pass
                last_error = e
                should_release_client = _should_release_vl_http_client(e)
                if should_release_client and not _is_connection_pool_exhausted_error(e):
                    await self._rotate_active_client(
                        failed_state=client_state,
                        reason=err_detail,
                    )
                raw_interactions.append(
                    {
                        "stage": "vl_video_analysis",
                        "attempt": attempt + 1,
                        "success": False,
                        "request": {
                            "model": self.model,
                            "temperature": float(response_kwargs.get("temperature", self.temperature)),
                            "max_tokens": int(self.max_tokens),
                            "response_format": response_kwargs.get("response_format"),
                            "analysis_mode": normalized_mode,
                            "video_path": str(video_path or ""),
                            "video_duration_sec": float(video_duration_sec),
                            "offline_task_enabled": bool(use_dashscope_offline_task),
                            "offline_task_meta": dict(locals().get("offline_task_meta", {}) or {}),
                            "timeout_sec": float(vl_request_timeout_sec),
                            "hedge_delay_ms": int(vl_hedge_delay_ms),
                            "message_transport": str(request_transport_meta.get("transport", "unknown") or "unknown"),
                            "message_transport_meta": dict(request_transport_meta or {}),
                            "messages": request_messages_audit,
                        },
                        "error": err_detail,
                        "error_raw": str(e),
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    }
                )
                if _is_connection_pool_exhausted_error(e) and pool_retry_index < pool_retry_max_attempts:
                    wait_time = self._compute_retry_backoff_sec(
                        attempt_index=pool_retry_index,
                        initial_backoff_sec=float(self.vl_retry_initial_backoff_sec),
                        multiplier=float(self.vl_retry_multiplier),
                        max_backoff_sec=float(self.vl_retry_max_backoff_sec),
                        jitter_sec=pool_retry_jitter_sec,
                    )
                    pool_retry_index += 1
                    effective_max_retries += 1
                    logger.warning(
                        "VL 连接池资源不足，触发退避等待: attempt=%s pool_retry=%s/%s wait=%.2fs error=%s",
                        attempt + 1,
                        pool_retry_index,
                        pool_retry_max_attempts,
                        wait_time,
                        err_detail,
                    )
                    await asyncio.sleep(wait_time)
                    attempt += 1
                    continue
                if attempt < effective_max_retries:
                    wait_time = self._compute_retry_backoff_sec(
                        attempt_index=attempt,
                        initial_backoff_sec=float(self.vl_retry_initial_backoff_sec),
                        multiplier=float(self.vl_retry_multiplier),
                        max_backoff_sec=float(self.vl_retry_max_backoff_sec),
                    )
                    logger.warning(
                        f"VL API call failed (attempt {attempt+1}/{effective_max_retries+1}): "
                        f"{err_detail}, wait {wait_time}s"
                    )

                    # 若解析失败，附加更严格约束以提升下次 JSON 成功率。
                    err_str = str(e).lower()
                    if "json" in err_str or "parse" in err_str or "decode" in err_str or "unterminated" in err_str:
                        messages = await self._build_messages(
                            video_path,
                            extra_prompt=extra_prompt,
                            override_prompt=(
                                "You must output only a JSON array. No explanation text."
                                "Do not output reasoning or key_evidence fields."
                            ),
                            analysis_mode=normalized_mode,
                            runtime_identity=runtime_identity,
                        )
                    await asyncio.sleep(wait_time)
                attempt += 1
            finally:
                await self._release_client_state(client_state)
        if last_error is not None:
            setattr(last_error, "_raw_llm_interactions", raw_interactions)
            if not getattr(last_error, "_display_error_detail", None):
                setattr(last_error, "_display_error_detail", self._format_exception_detail(last_error))
            if runtime_context is not None:
                runtime_context.persist_failure(
                    provider="vl",
                    request_name="vl_video_analysis",
                    request_payload=runtime_request_payload,
                    error=last_error,
                    runtime_identity=runtime_identity,
                )
                failure_records: List[Dict[str, Any]] = []
                for interaction in list(raw_interactions or []):
                    if not isinstance(interaction, dict) or bool(interaction.get("success", False)):
                        continue
                    failure_records.append(
                        {
                            "provider": "vl",
                            "request_name": "vl_video_analysis",
                            "attempt": int(interaction.get("attempt", len(failure_records) + 1) or (len(failure_records) + 1)),
                            "analysis_mode": normalized_mode,
                            "error_message": str(
                                interaction.get("error_raw")
                                or interaction.get("error")
                                or getattr(last_error, "_display_error_detail", None)
                                or str(last_error or "")
                            ),
                            "request_context": dict(interaction.get("request", {}) or {}),
                            "recorded_at": str(interaction.get("timestamp_utc", "") or ""),
                        }
                    )
                append_llm_fallback_event(
                    step_name=str(runtime_identity.get("step_name", "phase2a_vl") or "phase2a_vl"),
                    unit_id=str(runtime_identity.get("unit_id", semantic_unit_id) or semantic_unit_id),
                    llm_call_id=str(runtime_scope.get("llm_call_id", "") or ""),
                    chunk_id=str(runtime_scope.get("chunk_id", "") or ""),
                    scope_ref=str(runtime_scope.get("scope_ref", "") or ""),
                    request_payload=runtime_request_payload,
                    fallback_payload={
                        "is_fallback": True,
                        "fallback_kind": "vl_analysis_failed",
                        "fallback_label": f"phase2a_vl_{normalized_mode}_failed",
                        "fallback_reason": str(
                            getattr(last_error, "_display_error_detail", None)
                            or str(last_error or "")
                        ),
                        "repair_stage": "phase2a",
                        "previous_failures": failure_records,
                    },
                    extra={
                        "analysis_mode": normalized_mode,
                        "video_path": str(video_path or ""),
                        "provider": str(self.provider or "").strip().lower() or "vl",
                        "stack_trace": "".join(
                            traceback.format_exception(type(last_error), last_error, last_error.__traceback__)
                        ),
                    },
                )
        raise last_error

    def _get_tutorial_system_prompt(self) -> str:
        """方法说明：VLVideoAnalyzer._get_tutorial_system_prompt 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if hasattr(self, "_tutorial_system_prompt") and self._tutorial_system_prompt:
            return self._tutorial_system_prompt

        return (
            "You are an instructional video editor for 1-on-1 teaching replication.\n"
            "Your only task is to split the clip into complete procedural steps and choose instructional keyframes / instructional clips.\n"
            "Do NOT classify knowledge types.\n"
            "For each step, output only: step_id, step_description, clip_start_sec, clip_end_sec, "
            "main_operation, instructional_keyframes, and optional fields "
            "(instructional_clips, main_action, precautions, step_summary, operation_guidance).\n"
            "instructional_keyframes item schema: keyframe_id (e.g. KEYFRAME_1), timestamp_sec, optional frame_reason, "
            "optional bbox([xmin,ymin,xmax,ymax],0-1000).\n"
            "instructional_clips item schema: clip_id (e.g. CLIP_1), start_sec, end_sec, optional clip_reason.\n"
            "Optional fields can be omitted or left empty when unnecessary.\n"
            "Keep explanation + execution + result in the same step.\n"
            "Remove hesitation/thinking-only intervals with no new information.\n"
            "Each step should be at least 5 seconds; merge overly short steps with neighbors.\n"
        )

    def _get_concrete_system_prompt(self) -> str:
        if hasattr(self, "_concrete_system_prompt") and self._concrete_system_prompt:
            return self._concrete_system_prompt
        return (
            "You are a concrete-knowledge video analyst.\n"
            "Focus on extracting visually grounded content and precise keyframe timestamps.\n"
            "For each segment, provide: segment_id, segment_description, main_content, clip_start_sec, clip_end_sec, instructional_keyframes.\n"
            "Optional instructional_clips item schema: clip_id (e.g. CLIP_1), start_sec, end_sec, clip_reason.\n"
            "main_content must be markdown in Chinese and include [KEYFRAME_N]/[CLIP_N] placeholders aligned with instructional_keyframes/instructional_clips.\n"
            "Do not output reasoning or extra narration outside JSON.\n"
        )

    async def _build_messages(
        self,
        video_path: str,
        extra_prompt: Optional[str] = None,
        override_prompt: Optional[str] = None,
        analysis_mode: str = "default",
        *,
        runtime_identity: Optional[Dict[str, Any]] = None,
        return_transport_detail: bool = False,
    ) -> Any:
        """
        构建多模态消息。

        处理 DashScope 的 data-uri 单项 10MB 限制：
        - 小视频：直接 data-uri video_url
        - 大视频：优先尝试 DashScope File.upload 获取临时 URL（若安装了 dashscope）
        - 仍不可用：降级为抽取关键帧（image_url），并把每帧的时间戳作为文本标注提供给模型
        """
        # 统一构建系统提示词：教程模式不复用知识分类提示词。
        normalized_mode = self._normalize_analysis_mode(analysis_mode)
        if normalized_mode == "tutorial_stepwise":
            system_content = (
                self._get_tutorial_system_prompt()
                + self._get_output_constraints(normalized_mode)
                + "\n\n[Task] Split the procedural clip into steps and output stepwise JSON only."
            )
        elif normalized_mode == "concrete":
            system_content = (
                self._get_concrete_system_prompt()
                + self._get_output_constraints(normalized_mode)
                + "\n\n[Task] Analyze concrete visual segments and output JSON only."
            )
        else:
            system_content = (
                self.prompt_template
                + self._get_output_constraints(normalized_mode)
                + "\n\n[Task] Analyze the input video (or keyframes) and return JSON in the required schema."
            )
        
        user_text = ""
        if extra_prompt:
            user_text += extra_prompt.strip() + "\n"
        if override_prompt:
            user_text = "【重试补充要求】\n" + override_prompt + "\n" + user_text

        mode = (self.video_input_mode or "auto").lower()
        if mode not in ("auto", "data_uri", "dashscope_upload", "keyframes"):
            mode = "keyframes" if self._is_qianfan_endpoint(self.base_url) else "auto"

        video_file_size = 0
        try:
            video_file_size = Path(video_path).stat().st_size
        except Exception:
            video_file_size = 0

        # 1) data-uri（仅 DashScope 小文件安全）
        can_use_dashscope_inline = self._is_dashscope_endpoint(self.base_url)
        if can_use_dashscope_inline and mode in ("auto", "data_uri") and video_file_size and video_file_size <= _MAX_RAW_BYTES_FOR_BASE64_DATA_URI:
            video_base64 = self._encode_video_base64(video_path)
            if video_base64:
                logger.info(
                    "VL media transport resolved: transport=data_uri_video, video_path=%s, size_bytes=%s, note=video will be embedded inline in request body",
                    video_path,
                    video_file_size,
                )
                messages = [
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": [
                        {"type": "video_url", "video_url": {"url": f"data:video/mp4;base64,{video_base64}"}},
                        {"type": "text", "text": user_text},
                    ]}
                ]
                if return_transport_detail:
                    return messages, None
                return messages

        # 2) DashScope File.upload 获取临时 URL（需要 dashscope SDK）
        dashscope_upload_error_detail = ""
        media_prepare_interaction: Optional[Dict[str, Any]] = None
        if can_use_dashscope_inline and mode in ("auto", "dashscope_upload"):
            try:
                media_prepare_detail = await self._resolve_dashscope_temp_url_with_runtime_reuse(
                    video_path,
                    semantic_unit_id=str((runtime_identity or {}).get("unit_id", "") or ""),
                    analysis_mode=normalized_mode,
                    video_duration_sec=self._resolve_video_duration_sec(video_path),
                    runtime_identity=runtime_identity,
                    raise_on_failure=(mode == "dashscope_upload"),
                )
                temp_url = str(media_prepare_detail.get("temp_url", "") or "").strip()
                media_prepare_interaction = (
                    dict(media_prepare_detail.get("interaction", {}) or {})
                    if isinstance(media_prepare_detail, dict)
                    else None
                )
            except Exception as upload_error:
                if mode == "dashscope_upload":
                    raise
                temp_url = None
                dashscope_upload_error_detail = self._format_exception_detail(upload_error)
                logger.warning(
                    "DashScope SDK upload failed, fallback strategy will continue: %s",
                    dashscope_upload_error_detail,
                )
            if temp_url:
                logger.info(
                    "VL media transport resolved: transport=temp_url, video_path=%s, tmp_url=%s, note=video media upload succeeded and VL request will reference temp_url instead of inline media",
                    video_path,
                    temp_url,
                )
                messages = [
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": [
                        {"type": "video_url", "video_url": {"url": temp_url}},
                        {"type": "text", "text": user_text},
                    ]}
                ]
                if return_transport_detail:
                    return messages, media_prepare_interaction
                return messages
        if mode == "dashscope_upload":
            if not can_use_dashscope_inline:
                raise RuntimeError(
                    f"dashscope_upload mode requires DashScope endpoint, current base_url={self.base_url}"
                )
            raise RuntimeError(
                "dashscope_upload mode requires successful DashScope Files.upload temp_url before VL analysis: "
                f"video_path={video_path}, size_bytes={video_file_size}, "
                f"upload_error={dashscope_upload_error_detail or 'temporary URL missing'}"
            )
        # 3) 降级为关键帧（千帆链路默认路径）
        frames = await self._extract_keyframes(video_path, max_frames=self.max_input_frames)
        if not frames:
            raise ValueError(f"无法读取视频文件或抽帧失败: {video_path}")

        system_content += (
            "\n\n【关键帧输入说明】\n"
            "当输入为关键帧与时间戳时，请根据帧变化估算 clip_start_sec 与 clip_end_sec。\n"
            "If adjacent frames belong to the same step, estimate boundaries using their time span.\n"
        )
        content_items: List[Dict[str, Any]] = [{
            "type": "text",
            "text": "关键帧如下（含时间戳）："
        }]
        for idx, frame in enumerate(frames):
            content_items.append({"type": "text", "text": f"Frame {idx+1} @ {frame['timestamp_sec']:.2f}s"})
            content_items.append({"type": "image_url", "image_url": {"url": frame["data_uri"]}})

        if user_text:
            content_items.append({"type": "text", "text": user_text})
        logger.info(
            "VL media transport resolved: transport=keyframes, video_path=%s, frame_count=%s, max_image_dim=%s, note=keyframes will be embedded inline in request body",
            video_path,
            len(frames),
            int(self.max_image_dim),
        )
        
        messages = [
            {"role": "system", "content": system_content},
            {"role": "user", "content": content_items}
        ]
        if return_transport_detail:
            return messages, media_prepare_interaction
        return messages

    async def _try_get_dashscope_temp_url(
        self,
        video_path: str,
        *,
        raise_on_failure: bool = False,
    ) -> Optional[str]:
        detail = await self._resolve_dashscope_temp_url_with_runtime_reuse(
            video_path,
            semantic_unit_id="",
            analysis_mode="default",
            video_duration_sec=self._resolve_video_duration_sec(video_path),
            runtime_identity=None,
            raise_on_failure=raise_on_failure,
        )
        temp_url = str(detail.get("temp_url", "") or "").strip()
        return temp_url or None

    async def _resolve_dashscope_temp_url_with_runtime_reuse(
        self,
        video_path: str,
        *,
        semantic_unit_id: str,
        analysis_mode: str,
        video_duration_sec: float,
        runtime_identity: Optional[Dict[str, Any]],
        raise_on_failure: bool = False,
    ) -> Dict[str, Any]:
        """
        使用 DashScope SDK 上传本地文件，获取临时 URL。

        如果 dashscope SDK 不存在或上传失败，返回空结果（由上层降级到关键帧）。
        """
        if not self._is_dashscope_endpoint(self.base_url):
            if raise_on_failure:
                raise RuntimeError(
                    f"DashScope Files.upload requires DashScope endpoint, current base_url={self.base_url}"
                )
            return {"temp_url": "", "interaction": None}
        try:
            import dashscope  # type: ignore
        except Exception as e:
            error_detail = self._format_exception_detail(e)
            if raise_on_failure:
                raise RuntimeError(f"dashscope SDK unavailable: {error_detail}") from e
            logger.debug(f"dashscope SDK unavailable, skip temp URL upload: {error_detail}")
            return {"temp_url": "", "interaction": None}

        if not self._api_key:
            if raise_on_failure:
                raise RuntimeError(
                    f"DashScope Files.upload requires api_key, env={self._api_key_env or 'DASHSCOPE_API_KEY'}"
                )
            return {"temp_url": "", "interaction": None}
        resolved_video_duration_sec = max(0.0, float(video_duration_sec or 0.0))
        if self.dashscope_upload_timeout_by_video_duration:
            resolved_upload_timeout_sec = max(self.dashscope_upload_timeout_min_sec, resolved_video_duration_sec)
        else:
            resolved_upload_timeout_sec = max(
                self.dashscope_upload_timeout_min_sec,
                float(self.long_video_upload_timeout_sec),
            )
        runtime_context = current_runtime_llm_context()
        media_prepare_identity = self._build_vl_media_prepare_runtime_identity(
            semantic_unit_id=semantic_unit_id,
            video_path=video_path,
            wave_id=str((runtime_identity or {}).get("wave_id", "") or ""),
        )
        media_prepare_request_payload = self._build_vl_media_prepare_request_payload(
            video_path=video_path,
            video_duration_sec=resolved_video_duration_sec,
        )
        scope_descriptor = (
            runtime_context.build_scope_descriptor(
                provider="dashscope",
                request_name="vl_media_prepare",
                request_payload=media_prepare_request_payload,
                runtime_identity=media_prepare_identity,
            )
            if runtime_context is not None
            else {"scope_ref": ""}
        )
        if runtime_context is not None:
            restored = runtime_context.load_committed_call(
                provider="dashscope",
                request_name="vl_media_prepare",
                request_payload=media_prepare_request_payload,
                runtime_identity=media_prepare_identity,
            )
            if isinstance(restored, dict):
                restored_metadata = dict(restored.get("response_metadata", {}) or {})
                restored_temp_url = str(restored_metadata.get("temp_url") or restored.get("response_text", "") or "").strip()
                soft_expires_at_ms = safe_int(restored_metadata.get("soft_expires_at_ms", 0), 0)
                if restored_temp_url and (soft_expires_at_ms <= 0 or int(time.time() * 1000) < soft_expires_at_ms):
                    return {
                        "temp_url": restored_temp_url,
                        "interaction": {
                            "stage": "vl_media_prepare",
                            "attempt": int(restored.get("attempt", 0) or 0),
                            "success": True,
                            "request": {
                                "video_path": str(video_path or ""),
                                "analysis_mode": str(analysis_mode or "default"),
                                "video_duration_sec": float(resolved_video_duration_sec),
                                "message_transport": "temp_url",
                            },
                            "response": {
                                "temp_urls": [restored_temp_url],
                                "temp_url_count": 1,
                                "cache_hit": True,
                                "runtime_restored": True,
                                "restored_attempt": int(restored.get("attempt", 0) or 0),
                                "soft_expires_at_ms": soft_expires_at_ms,
                            },
                            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        },
                    }
                scope_ref = str(scope_descriptor.get("scope_ref", "") or "").strip()
                if scope_ref:
                    # temp_url 失效只影响上传阶段本身，已提交的 VL 结果仍可独立恢复。
                    runtime_context.store.mark_scope_dirty(
                        scope_ref,
                        reason="temp_url_soft_ttl_expired",
                        include_descendants=False,
                    )
        upload_video_path = await self._prepare_video_for_dashscope_upload(video_path)

        def _normalize_dashscope_media_url(raw_url: Any) -> str:
            url = str(raw_url or "").strip()
            if url.startswith("http://dashscope-file-mgr.oss-"):
                return "https://" + url[len("http://") :]
            return url

        def _extract_temp_url_from_output(output: Any) -> tuple[Optional[str], str]:
            if not isinstance(output, dict):
                return None, ""

            direct_url = _normalize_dashscope_media_url(output.get("url"))
            if direct_url:
                return direct_url, "output.url"

            uploaded_files = output.get("uploaded_files")
            if not isinstance(uploaded_files, list):
                return None, ""

            # 新版 SDK 返回 uploaded_files[].file_id，需要二次 Files.get 拿到可访问 URL。
            for item in uploaded_files:
                if not isinstance(item, dict):
                    continue
                item_url = _normalize_dashscope_media_url(item.get("url"))
                if item_url:
                    return item_url, "uploaded_files.url"
                file_id = str(item.get("file_id") or "").strip()
                if not file_id:
                    continue
                meta_resp = dashscope.Files.get(file_id=file_id)
                meta_status = getattr(meta_resp, "status_code", None)
                meta_output = getattr(meta_resp, "output", None)
                if meta_status == 200 and isinstance(meta_output, dict):
                    meta_url = _normalize_dashscope_media_url(meta_output.get("url"))
                    if meta_url:
                        return meta_url, f"files.get({file_id})"
                if isinstance(meta_resp, dict) and meta_resp.get("status_code") == 200:
                    meta_url = _normalize_dashscope_media_url((meta_resp.get("output") or {}).get("url"))
                    if meta_url:
                        return meta_url, f"files.get({file_id})"
            return None, ""

        def _upload() -> Dict[str, Any]:
            dashscope.api_key = self._api_key
            # Files.upload 需要 file_path 字符串参数
            resp = dashscope.Files.upload(
                file_path=upload_video_path,
                purpose="file-extract",
                chunk_size=int(self.dashscope_upload_chunk_size_bytes),
                timeout=float(resolved_upload_timeout_sec),
                headers={"Authorization": f"Bearer {self._api_key}"},
            )
            status_code = getattr(resp, "status_code", None)
            request_id = getattr(resp, "request_id", None)
            output = getattr(resp, "output", None)
            if status_code == 200:
                temp_url, url_source = _extract_temp_url_from_output(output)
                if temp_url:
                    try:
                        file_size = Path(upload_video_path).stat().st_size
                    except Exception:
                        file_size = 0
                    logger.info(
                        "DashScope Files.upload succeeded: video_path=%s, source_video_path=%s, tmp_url=%s, size_bytes=%s, url_source=%s, request_id=%s, timeout_sec=%.2f, chunk_size=%s",
                        upload_video_path,
                        video_path,
                        temp_url,
                        file_size,
                        url_source or "unknown",
                        request_id or "unknown",
                        float(resolved_upload_timeout_sec),
                        int(self.dashscope_upload_chunk_size_bytes),
                    )
                    return {
                        "temp_url": temp_url,
                        "request_id": str(request_id or ""),
                        "url_source": str(url_source or ""),
                        "upload_video_path": str(upload_video_path or ""),
                        "source_video_path": str(video_path or ""),
                        "file_size": int(file_size or 0),
                    }
                raise RuntimeError(
                    "DashScope Files.upload succeeded but no temporary URL was found in output: "
                    f"{self._safe_json_preview(output)}"
                )
            # 兼容 dict 形式返回
            if isinstance(resp, dict) and resp.get("status_code") == 200:
                dict_output = resp.get("output") or {}
                temp_url, url_source = _extract_temp_url_from_output(dict_output)
                if temp_url:
                    try:
                        file_size = Path(upload_video_path).stat().st_size
                    except Exception:
                        file_size = 0
                    logger.info(
                        "DashScope video media upload succeeded: transport=temp_url, video_path=%s, source_video_path=%s, tmp_url=%s, size_bytes=%s, url_source=%s, request_id=%s, timeout_sec=%.2f, chunk_size=%s",
                        upload_video_path,
                        video_path,
                        temp_url,
                        file_size,
                        url_source or "unknown",
                        resp.get("request_id", "unknown"),
                        float(resolved_upload_timeout_sec),
                        int(self.dashscope_upload_chunk_size_bytes),
                    )
                    return {
                        "temp_url": temp_url,
                        "request_id": str(resp.get("request_id", "") or ""),
                        "url_source": str(url_source or ""),
                        "upload_video_path": str(upload_video_path or ""),
                        "source_video_path": str(video_path or ""),
                        "file_size": int(file_size or 0),
                    }
                raise RuntimeError(
                    "DashScope Files.upload(dict) succeeded but no temporary URL was found in output: "
                    f"{self._safe_json_preview(dict_output)}"
                )
            message = getattr(resp, "message", None) or str(resp)
            raise RuntimeError(f"DashScope Files.upload 失败: {message}")

        max_attempts = max(1, int(self.dashscope_upload_retry_max_attempts))
        for attempt in range(1, max_attempts + 1):
            try:
                upload_result = await asyncio.to_thread(_upload)
                temp_url = str(upload_result.get("temp_url", "") or "").strip()
                committed_at_ms = int(time.time() * 1000)
                soft_expires_at_ms = committed_at_ms + int(self.temp_url_reuse_ttl_sec) * 1000
                if temp_url and runtime_context is not None:
                    runtime_context.persist_success(
                        provider="dashscope",
                        request_name="vl_media_prepare",
                        request_payload=media_prepare_request_payload,
                        response_text=temp_url,
                        response_metadata={
                            "temp_url": temp_url,
                            "request_id": str(upload_result.get("request_id", "") or ""),
                            "url_source": str(upload_result.get("url_source", "") or ""),
                            "upload_video_path": str(upload_result.get("upload_video_path", "") or ""),
                            "upload_video_signature": self._build_runtime_video_signature(
                                str(upload_result.get("upload_video_path", "") or "")
                            ),
                            "source_video_path": str(video_path or ""),
                            "source_video_signature": self._build_runtime_video_signature(video_path),
                            "resolved_upload_timeout_sec": float(resolved_upload_timeout_sec),
                            "temp_url_reuse_ttl_sec": int(self.temp_url_reuse_ttl_sec),
                            "committed_at_ms": committed_at_ms,
                            "soft_expires_at_ms": soft_expires_at_ms,
                            "file_size": int(upload_result.get("file_size", 0) or 0),
                        },
                        runtime_identity=media_prepare_identity,
                        metadata={
                            "stage_step": "vl_media_prepare",
                            "analysis_mode": str(analysis_mode or "default"),
                        },
                    )
                return {
                    "temp_url": temp_url,
                    "interaction": {
                        "stage": "vl_media_prepare",
                        "attempt": attempt,
                        "success": True,
                        "request": {
                            "video_path": str(video_path or ""),
                            "analysis_mode": str(analysis_mode or "default"),
                            "video_duration_sec": float(resolved_video_duration_sec),
                            "message_transport": "temp_url",
                        },
                        "response": {
                            "temp_urls": [temp_url],
                            "temp_url_count": 1,
                            "cache_hit": False,
                            "runtime_restored": False,
                            "request_id": str(upload_result.get("request_id", "") or ""),
                            "url_source": str(upload_result.get("url_source", "") or ""),
                            "soft_expires_at_ms": soft_expires_at_ms,
                        },
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    },
                }
            except Exception as e:
                error_detail = self._format_exception_detail(e)
                if attempt >= max_attempts:
                    if runtime_context is not None and raise_on_failure:
                        runtime_context.persist_failure(
                            provider="dashscope",
                            request_name="vl_media_prepare",
                            request_payload=media_prepare_request_payload,
                            error=e,
                            runtime_identity=media_prepare_identity,
                            metadata={
                                "stage_step": "vl_media_prepare",
                                "analysis_mode": str(analysis_mode or "default"),
                            },
                        )
                    if raise_on_failure:
                        raise RuntimeError(
                            f"DashScope Files.upload failed after {max_attempts} attempts: {error_detail}"
                        ) from e
                    logger.warning(
                        "DashScope temp URL upload failed after %s attempts, fallback to next strategy: %s",
                        max_attempts,
                        error_detail,
                    )
                    return {"temp_url": "", "interaction": None}

                backoff_sec = self._compute_retry_backoff_sec(
                    attempt_index=attempt - 1,
                    initial_backoff_sec=float(self.dashscope_upload_retry_initial_backoff_sec),
                    multiplier=float(self.dashscope_upload_retry_multiplier),
                    max_backoff_sec=float(self.dashscope_upload_retry_max_backoff_sec),
                    jitter_sec=float(self.dashscope_upload_retry_jitter_sec),
                )

                logger.warning(
                    "DashScope Files.upload failed (attempt %s/%s): %s, retry in %.2fs",
                    attempt,
                    max_attempts,
                    error_detail,
                    backoff_sec,
                )
                if backoff_sec > 0:
                    await asyncio.sleep(backoff_sec)
        return {"temp_url": "", "interaction": None}
    
    def _encode_video_base64(self, video_path: str) -> Optional[str]:
        """将视频文件编码为 base64（仅适用于小文件）"""
        try:
            with open(video_path, "rb") as f:
                return base64.b64encode(f.read()).decode("utf-8")
        except Exception as e:
            logger.error(f"视频编码失败: {e}")
            return None

    def _should_force_inline_transcode_for_keyframe_extract(self, video_path: str) -> bool:
        """
        判断关键帧抽取是否应强制内联转码。

        做什么：仅当输入片段位于 semantic_unit_clips_vl 子目录时返回 True。
        为什么：该目录是 VL 待分析片段子集，允许局部 AV1->H.264 转码，避免全量视频被同步转码。
        权衡：通过目录边界限制影响面；其它路径仍沿用默认解码策略。
        """
        normalized_path = str(video_path or "").replace("\\", "/").strip().lower()
        if not normalized_path:
            return False
        parts = [part for part in normalized_path.split("/") if part]
        return "semantic_unit_clips_vl" in parts
    
    async def _extract_keyframes(self, video_path: str, max_frames: int = 6) -> List[Dict[str, Any]]:
        """
        从视频中抽取少量关键帧并编码为 data-uri（image/jpeg）。

        目标：绕过 DashScope 对单个 data-uri item 10MB 的限制。
        """
        try:
            import cv2  # type: ignore
            from PIL import Image  # type: ignore
        except Exception as e:
            logger.warning(f"关键帧抽取依赖不可用（opencv/pillow）：{e}")
            return []

        force_inline_transcode = self._should_force_inline_transcode_for_keyframe_extract(video_path)
        cap, effective_video_path, used_fallback = open_video_capture_with_fallback(
            video_path,
            logger=logger,
            allow_inline_transcode=True if force_inline_transcode else None,
        )
        if cap is None or not cap.isOpened():
            logger.warning(
                "关键帧抽取无法打开视频: source=%s, effective=%s, force_inline_transcode=%s",
                video_path,
                effective_video_path,
                force_inline_transcode,
            )
            return []
        if used_fallback:
            logger.info(
                "关键帧抽取使用 OpenCV 解码兜底路径: source=%s, effective=%s",
                video_path,
                effective_video_path,
            )

        try:
            fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
            frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0
            duration = (frame_count / fps) if fps > 0 and frame_count > 0 else 0.0

            # 均匀采样，避免过多帧
            if max_frames <= 0:
                max_frames = 1
            if duration <= 0:
                timestamps = [0.0]
            else:
                # 避免取到末尾导致 seek 失败
                end = max(0.0, duration - 0.05)
                if max_frames == 1:
                    timestamps = [max(0.0, end * 0.5)]
                else:
                    step = end / (max_frames - 1)
                    timestamps = [i * step for i in range(max_frames)]

            frames: List[Dict[str, Any]] = []
            for ts in timestamps:
                try:
                    cap.set(cv2.CAP_PROP_POS_MSEC, ts * 1000.0)
                    ok, frame_bgr = cap.read()
                    if not ok or frame_bgr is None:
                        continue
                    frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
                    image = Image.fromarray(frame_rgb)
                    data_uri = self._encode_image_as_jpeg_data_uri(image)
                    if not data_uri:
                        continue
                    frames.append({"timestamp_sec": float(ts), "data_uri": data_uri})
                except Exception:
                    continue

            return frames
        finally:
            cap.release()

    def _encode_image_as_jpeg_data_uri(self, image) -> Optional[str]:
        """将 PIL.Image 编码为满足 10MB 限制的 JPEG data-uri。"""
        try:
            from PIL import Image  # type: ignore
        except Exception:
            return None

        if not isinstance(image, Image.Image):
            return None

        # 先按最大边缩放
        w, h = image.size
        max_dim = max(1, int(self.max_image_dim))
        if max(w, h) > max_dim:
            if w >= h:
                new_w = max_dim
                new_h = max(1, int(h * (max_dim / w)))
            else:
                new_h = max_dim
                new_w = max(1, int(w * (max_dim / h)))
            image = image.resize((new_w, new_h))

        quality = 82
        scale_rounds = 0
        while True:
            buf = io.BytesIO()
            try:
                image.save(buf, format="JPEG", quality=quality, optimize=True)
            except Exception:
                image.save(buf, format="JPEG", quality=quality)

            raw = buf.getvalue()
            if len(raw) <= int(_DASHSCOPE_DATA_URI_ITEM_MAX_BYTES * _DATA_URI_SAFETY_RATIO):
                b64 = base64.b64encode(raw).decode("utf-8")
                return f"data:image/jpeg;base64,{b64}"

            # 先降质量，再缩放
            if quality > 45:
                quality = max(45, quality - 10)
                continue

            if scale_rounds >= 4:
                return None

            w, h = image.size
            image = image.resize((max(1, int(w * 0.85)), max(1, int(h * 0.85))))
            scale_rounds += 1
            quality = 82

    def _sanitize_action_brief(self, text_value: str, max_len: int = 48) -> str:
        """Shorten a step description into a filename-safe action brief."""
        raw = str(text_value or "").strip().lower()
        raw = re.sub(r"[^a-z0-9]+", "_", raw)
        raw = re.sub(r"_+", "_", raw).strip("_")
        if not raw:
            return "action"
        if len(raw) > max_len:
            return raw[:max_len].rstrip("_") or "action"
        return raw

    def _build_unit_relative_asset_id(self, semantic_unit_id: str, file_stem: str) -> str:
        """方法说明：VLVideoAnalyzer._build_unit_relative_asset_id 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        default_stem = f"{str(semantic_unit_id or '').strip() or 'SU000'}_asset_001"
        return build_unit_relative_asset_id(semantic_unit_id, file_stem, default_stem=default_stem)

    def _normalize_timestamp_list(self, value: Any) -> List[float]:
        """Normalize scalar/list/string timestamp input into a non-negative float list."""
        if value is None:
            return []

        raw_items: List[Any]
        if isinstance(value, (list, tuple, set)):
            raw_items = list(value)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            raw_items = [value]
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return []

            parsed_value: Any = None
            try:
                parsed_value = json.loads(text)
            except Exception:
                parsed_value = None

            if isinstance(parsed_value, list):
                raw_items = parsed_value
            elif isinstance(parsed_value, (int, float)) and not isinstance(parsed_value, bool):
                raw_items = [parsed_value]
            else:
                tokens = [token for token in re.split(r"[\s,;|]+", text) if token]
                raw_items = tokens
        else:
            return []

        normalized: List[float] = []
        seen: set[float] = set()
        for item in raw_items:
            if isinstance(item, bool):
                continue
            try:
                ts = float(item)
            except Exception:
                continue

            if not math.isfinite(ts) or ts < 0:
                continue

            ts = round(ts, 6)
            if ts in seen:
                continue
            seen.add(ts)
            normalized.append(ts)

        return normalized

    def _normalize_text_list(self, value: Any) -> List[str]:
        """Normalize scalar/list/string text input into a de-duplicated non-empty string list."""
        if value is None:
            return []

        raw_items: List[Any]
        if isinstance(value, (list, tuple, set)):
            raw_items = list(value)
        elif isinstance(value, str):
            text = value.strip()
            if not text:
                return []

            parsed_value: Any = None
            try:
                parsed_value = json.loads(text)
            except Exception:
                parsed_value = None

            if isinstance(parsed_value, list):
                raw_items = parsed_value
            else:
                raw_items = [segment for segment in re.split(r"[\n;；]+", text) if segment and segment.strip()]
        else:
            raw_items = [value]

        normalized: List[str] = []
        seen: set[str] = set()
        for item in raw_items:
            text_item = str(item or "").strip()
            if not text_item:
                continue
            dedup_key = text_item.lower()
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            normalized.append(text_item)

        return normalized

    def _normalize_main_operation(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, (list, tuple, set)):
            return self._normalize_text_list(list(value))
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return []
            try:
                parsed_value = json.loads(text)
            except Exception:
                parsed_value = None
            if isinstance(parsed_value, list):
                return self._normalize_text_list(parsed_value)
            return [text]
        text_item = str(value or "").strip()
        return [text_item] if text_item else []

    def _normalize_bbox_1000(self, value: Any) -> Optional[List[int]]:
        if not isinstance(value, (list, tuple)) or len(value) != 4:
            return None
        try:
            xmin = int(round(float(value[0])))
            ymin = int(round(float(value[1])))
            xmax = int(round(float(value[2])))
            ymax = int(round(float(value[3])))
        except Exception:
            return None

        xmin = max(0, min(1000, xmin))
        ymin = max(0, min(1000, ymin))
        xmax = max(0, min(1000, xmax))
        ymax = max(0, min(1000, ymax))

        if xmax < xmin:
            xmin, xmax = xmax, xmin
        if ymax < ymin:
            ymin, ymax = ymax, ymin
        return [xmin, ymin, xmax, ymax]

    def _normalize_keyframe_id(self, value: Any, *, fallback_index: int) -> str:
        """统一 keyframe_id 格式为 KEYFRAME_N，缺失时回退到顺序编号。"""
        fallback = max(1, int(fallback_index))
        text = str(value or "").strip()
        if not text:
            return f"KEYFRAME_{fallback}"

        matched = re.search(r"KEYFRAME[_\-\s]*(\d+)", text, flags=re.IGNORECASE)
        if matched:
            return f"KEYFRAME_{int(matched.group(1))}"

        if re.fullmatch(r"\d+", text):
            return f"KEYFRAME_{int(text)}"

        return f"KEYFRAME_{fallback}"

    def _normalize_clip_id(self, value: Any, *, fallback_index: int) -> str:
        fallback = max(1, int(fallback_index))
        text = str(value or "").strip()
        if not text:
            return f"CLIP_{fallback}"

        matched = re.search(r"CLIP[_\-\s]*(\d+)", text, flags=re.IGNORECASE)
        if matched:
            return f"CLIP_{int(matched.group(1))}"

        if re.fullmatch(r"\d+", text):
            return f"CLIP_{int(text)}"

        return f"CLIP_{fallback}"

    def _normalize_instructional_keyframes(
        self,
        value: Any,
        *,
        fallback_timestamps: Optional[List[float]] = None,
    ) -> List[Dict[str, Any]]:
        fallback_timestamps = list(fallback_timestamps or [])
        normalized: List[Dict[str, Any]] = []
        if isinstance(value, list):
            for index, item in enumerate(value, start=1):
                if not isinstance(item, dict):
                    continue
                raw_ts = item.get("timestamp_sec", item.get("timestamp", item.get("ts", None)))
                if raw_ts is None and (index - 1) < len(fallback_timestamps):
                    raw_ts = fallback_timestamps[index - 1]
                ts_list = self._normalize_timestamp_list(raw_ts)
                if not ts_list:
                    continue
                entry: Dict[str, Any] = {
                    "timestamp_sec": ts_list[0],
                    "frame_reason": str(item.get("frame_reason", "") or "").strip(),
                    "keyframe_id": self._normalize_keyframe_id(
                        item.get("keyframe_id", item.get("keyframeId", item.get("id"))),
                        fallback_index=index,
                    ),
                }
                bbox = self._normalize_bbox_1000(item.get("bbox"))
                if bbox is not None:
                    entry["bbox"] = bbox
                normalized.append(entry)
            if normalized:
                return normalized

        for index, ts in enumerate(fallback_timestamps, start=1):
            normalized.append(
                {
                    "timestamp_sec": float(ts),
                    "frame_reason": "",
                    "keyframe_id": self._normalize_keyframe_id(None, fallback_index=index),
                }
            )
        return normalized

    def _normalize_instructional_clips(
        self,
        value: Any,
        *,
        clip_start_sec: float,
        clip_end_sec: float,
    ) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []

        normalized: List[Dict[str, Any]] = []
        max_duration_sec = 5.0
        for index, item in enumerate(value, start=1):
            if not isinstance(item, dict):
                continue

            start_sec = safe_float(
                item.get("start_sec", item.get("clip_start_sec", item.get("start", clip_start_sec))),
                clip_start_sec,
            )
            end_sec = safe_float(
                item.get("end_sec", item.get("clip_end_sec", item.get("end", start_sec))),
                start_sec,
            )
            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec
            start_sec = max(clip_start_sec, min(start_sec, clip_end_sec))
            end_sec = max(clip_start_sec, min(end_sec, clip_end_sec))
            if end_sec < start_sec:
                end_sec = start_sec
            if end_sec - start_sec > max_duration_sec:
                end_sec = min(clip_end_sec, start_sec + max_duration_sec)
            if end_sec <= start_sec:
                continue

            normalized.append(
                {
                    "clip_id": self._normalize_clip_id(
                        item.get("clip_id", item.get("clipId", item.get("id"))),
                        fallback_index=index,
                    ),
                    "start_sec": round(float(start_sec), 6),
                    "end_sec": round(float(end_sec), 6),
                    "clip_reason": str(item.get("clip_reason", item.get("reason", "")) or "").strip(),
                }
            )

        return normalized

    def _normalize_bool_flag(self, value: Any) -> bool:
        """将模型返回的多种布尔表达归一为 bool。"""
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value != 0
        text = str(value or "").strip().lower()
        if not text:
            return False
        return text in {"1", "true", "yes", "y", "on", "是", "是的"}

    def _normalize_should_type(self, value: Any) -> str:
        """将 should_type 归一为 abstract/concrete/空字符串。"""
        text = str(value or "").strip().lower()
        if text in {"abstract", "抽象", "讲解", "explanation"}:
            return "abstract"
        if text in {"concrete", "具象", "具体", "实例"}:
            return "concrete"
        return ""

    def _normalize_route_controls(self, no_needed_video: Any, should_type: Any) -> tuple[bool, str]:
        """统一归一路由控制字段，并应用 no_needed_video 的最高优先级。"""
        normalized_no_needed_video = self._normalize_bool_flag(no_needed_video)
        normalized_should_type = self._normalize_should_type(should_type)
        if normalized_no_needed_video:
            return True, "abstract"
        return normalized_no_needed_video, normalized_should_type

    def _enforce_tutorial_step_constraints(
        self,
        results: List[VLAnalysisResult],
    ) -> List[VLAnalysisResult]:
        """Apply stepwise normalization for tutorial outputs."""
        if not results:
            return []

        min_duration = max(0.0, float(getattr(self, "tutorial_min_step_duration_sec", 5.0)))

        ordered = sorted(
            results,
            key=lambda item: int(item.step_id) if int(item.step_id) > 0 else 10**9,
        )

        normalized_results: List[VLAnalysisResult] = []
        for index, item in enumerate(ordered, start=1):
            clip_start = safe_float(item.clip_start_sec, 0.0)
            clip_end = safe_float(item.clip_end_sec, clip_start)
            if clip_end < clip_start:
                clip_start, clip_end = clip_end, clip_start

            if clip_end - clip_start < min_duration:
                clip_end = clip_start + min_duration

            step_description = str(item.step_description or "").strip() or f"step_{index:02d}"
            step_type = self._normalize_step_type(getattr(item, "step_type", ""))
            main_action = str(item.main_action or "").strip()
            main_operation = self._normalize_main_operation(item.main_operation)
            precautions = self._normalize_text_list(item.precautions)
            step_summary = str(item.step_summary or "").strip()
            operation_guidance = self._normalize_text_list(item.operation_guidance)

            timestamps = self._normalize_timestamp_list(item.suggested_screenshoot_timestamps)
            clamped_timestamps: List[float] = []
            for ts in timestamps:
                if ts < clip_start:
                    ts = clip_start
                elif ts > clip_end:
                    ts = clip_end
                clamped_timestamps.append(ts)

            if not clamped_timestamps:
                fallback_ts = clip_start if clip_end <= clip_start else (clip_start + clip_end) / 2.0
                clamped_timestamps = [round(fallback_ts, 6)]

            keyframes = self._normalize_instructional_keyframes(
                item.instructional_keyframes,
                fallback_timestamps=clamped_timestamps,
            )
            instructional_clips = self._normalize_instructional_clips(
                getattr(item, "instructional_clips", []),
                clip_start_sec=clip_start,
                clip_end_sec=clip_end,
            )
            clamped_keyframes: List[Dict[str, Any]] = []
            for key_index, keyframe in enumerate(keyframes, start=1):
                raw_ts = keyframe.get("timestamp_sec", None)
                fallback_ts = clamped_timestamps[min(key_index - 1, len(clamped_timestamps) - 1)]
                key_ts = safe_float(raw_ts, fallback_ts)
                if key_ts < clip_start:
                    key_ts = clip_start
                elif key_ts > clip_end:
                    key_ts = clip_end
                key_entry: Dict[str, Any] = {
                    "timestamp_sec": round(key_ts, 6),
                    "frame_reason": str(keyframe.get("frame_reason", "") or "").strip(),
                    "keyframe_id": self._normalize_keyframe_id(
                        keyframe.get("keyframe_id", keyframe.get("keyframeId", key_index)),
                        fallback_index=key_index,
                    ),
                }
                bbox = self._normalize_bbox_1000(keyframe.get("bbox"))
                if bbox is not None:
                    key_entry["bbox"] = bbox
                clamped_keyframes.append(key_entry)

            if not clamped_keyframes:
                clamped_keyframes = [
                    {
                        "timestamp_sec": ts,
                        "frame_reason": "",
                        "keyframe_id": self._normalize_keyframe_id(None, fallback_index=i),
                    }
                    for i, ts in enumerate(clamped_timestamps, start=1)
                ]
            clamped_timestamps = [float(item.get("timestamp_sec", 0.0)) for item in clamped_keyframes]

            item.step_id = index
            item.step_description = step_description
            item.step_type = step_type
            item.knowledge_type = "process"
            item.analysis_mode = "tutorial_stepwise"
            item.main_action = main_action
            item.main_operation = main_operation
            item.instructional_keyframes = clamped_keyframes
            item.instructional_clips = instructional_clips
            item.precautions = precautions
            item.step_summary = step_summary
            item.operation_guidance = operation_guidance
            item.clip_start_sec = clip_start
            item.clip_end_sec = clip_end
            item.suggested_screenshoot_timestamps = clamped_timestamps
            normalized_results.append(item)

        return normalized_results

    
    def _parse_response_with_payload(
        self,
        content: str,
        finish_reason: Optional[str] = None,
        analysis_mode: str = "default",
    ) -> tuple[List[VLAnalysisResult], List[Dict[str, Any]]]:
        """
        解析 VL API 返回内容并提取可用 JSON。
        """
        normalized_mode = self._normalize_analysis_mode(analysis_mode)
        json_str = self._extract_json_candidate(content)

        data: Optional[Any] = None
        last_err: Optional[Exception] = None

        data, last_err = self._parse_json_payload(json_str)
        if data is None:
            salvaged_items, salvaged_err = self._extract_salvaged_json_objects(json_str)
            if salvaged_items:
                data = salvaged_items
            elif salvaged_err is not None:
                last_err = salvaged_err

        if data is None:
            finish_hint = f", finish_reason={finish_reason}" if finish_reason else ""
            logger.error(f"JSON parse failed: {last_err}{finish_hint}, raw: {content[:500]}")
            raise ValueError(f"JSON parse failed: {last_err}{finish_hint}")

        if isinstance(data, dict):
            if isinstance(data.get("results"), list):
                items = data.get("results", [])
            elif isinstance(data.get("steps"), list):
                items = data.get("steps", [])
            else:
                items = [data]
        elif isinstance(data, list):
            items = data
        else:
            items = [data]

        results: List[VLAnalysisResult] = []
        normalized_payload: List[Dict[str, Any]] = []
        has_step_schema = False

        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue

            # 兼容教程 schema、默认 schema 与 concrete schema
            step_id = safe_int(item.get("step_id", item.get("id", index + 1)), index + 1)
            segment_id = safe_int(item.get("segment_id", item.get("id", step_id)), step_id)
            step_description = str(
                item.get("step_description", item.get("description", item.get("title", ""))) or ""
            ).strip()
            segment_description = str(
                item.get(
                    "segment_description",
                    item.get("segment_title", step_description),
                )
                or ""
            ).strip()
            step_type = self._normalize_step_type(
                item.get("step_type", item.get("stepType", item.get("step_category", item.get("type", ""))))
            )
            main_action = str(
                item.get(
                    "main_action",
                    item.get("主要动作", ""),
                )
                or ""
            ).strip()
            raw_main_operation = item.get("main_operation", None)
            if raw_main_operation is None:
                raw_main_operation = item.get("main_operations", None)
            if raw_main_operation is None:
                raw_main_operation = item.get("primary_operations", None)
            if raw_main_operation is None:
                raw_main_operation = item.get("主要操作", None)
            main_operation = self._normalize_main_operation(raw_main_operation)
            raw_precautions = item.get("precautions", None)
            if raw_precautions is None:
                raw_precautions = item.get("notes", None)
            if raw_precautions is None:
                raw_precautions = item.get("注意事项", None)
            if raw_precautions is None:
                raw_precautions = item.get("cautions", None)
            precautions = self._normalize_text_list(raw_precautions)
            step_summary = str(
                item.get("step_summary", item.get("步骤小结", item.get("summary", ""))) or ""
            ).strip()
            raw_operation_guidance = item.get("operation_guidance", None)
            if raw_operation_guidance is None:
                raw_operation_guidance = item.get("操作指导", None)
            if raw_operation_guidance is None:
                raw_operation_guidance = item.get("guidance", None)
            operation_guidance = self._normalize_text_list(raw_operation_guidance)
            main_content = str(
                item.get(
                    "main_content",
                    item.get("content", item.get("markdown_content", "")),
                )
                or ""
            ).strip()

            raw_timestamps = item.get("instructional_keyframe_timestamp", None)
            if raw_timestamps is None:
                raw_timestamps = item.get("instructional_keyframe_timestamps", None)
            if raw_timestamps is None:
                raw_timestamps = item.get("suggested_screenshoot_timestamps", None)
            if raw_timestamps is None:
                raw_timestamps = item.get("suggested_screenshot_timestamps", [])
            timestamps = self._normalize_timestamp_list(raw_timestamps)

            raw_instructional_keyframes = item.get("instructional_keyframes", None)
            instructional_keyframes = self._normalize_instructional_keyframes(
                raw_instructional_keyframes,
                fallback_timestamps=timestamps,
            )
            if instructional_keyframes:
                timestamps = [float(frame.get("timestamp_sec", 0.0)) for frame in instructional_keyframes]

            clip_start_sec = safe_float(item.get("clip_start_sec", 0.0), 0.0)
            clip_end_sec = safe_float(item.get("clip_end_sec", 0.0), 0.0)
            if clip_end_sec < clip_start_sec:
                clip_start_sec, clip_end_sec = clip_end_sec, clip_start_sec

            raw_instructional_clips = item.get("instructional_clips", None)
            instructional_clips = self._normalize_instructional_clips(
                raw_instructional_clips,
                clip_start_sec=clip_start_sec,
                clip_end_sec=clip_end_sec,
            )

            key_evidence = item.get("key_evidence", "")
            if isinstance(key_evidence, list):
                key_evidence = "; ".join([str(x) for x in key_evidence if x is not None])
            else:
                key_evidence = str(key_evidence) if key_evidence is not None else ""

            concrete_like = normalized_mode == "concrete"
            tutorial_like = (not concrete_like) and bool(
                ("step_id" in item)
                or ("step_description" in item)
                or ("instructional_keyframe_timestamp" in item)
                or normalized_mode == "tutorial_stepwise"
            )
            has_step_schema = has_step_schema or tutorial_like

            if tutorial_like:
                # 教程分步模式已下线 no_needed_video/should_type，不再解析这两个字段。
                no_needed_video, should_type = False, ""
            else:
                raw_no_needed_video = item.get("no_needed_video", item.get("no_need_video", item.get("video_not_needed")))
                raw_should_type = item.get("should_type", item.get("target_type", item.get("suggested_type")))
                no_needed_video, should_type = self._normalize_route_controls(raw_no_needed_video, raw_should_type)

            knowledge_type = str(item.get("knowledge_type", "") or "").strip()
            if tutorial_like:
                # 教程模式忽略模型返回的 knowledge_type，仅保留步骤结构
                knowledge_type = "process"
            elif concrete_like:
                knowledge_type = "concrete"
                if should_type in {"abstract", "concrete"}:
                    knowledge_type = should_type
                if no_needed_video:
                    knowledge_type = "abstract"
            else:
                if should_type in {"abstract", "concrete"}:
                    knowledge_type = should_type
                if no_needed_video:
                    knowledge_type = "abstract"

            result = VLAnalysisResult(
                id=safe_int(item.get("id", step_id), step_id),
                knowledge_type=knowledge_type,
                no_needed_video=no_needed_video,
                should_type=should_type,
                confidence=safe_float(item.get("confidence", 0.0), 0.0),
                reasoning=str(item.get("reasoning", "") or ""),
                key_evidence=key_evidence,
                clip_start_sec=clip_start_sec,
                clip_end_sec=clip_end_sec,
                suggested_screenshoot_timestamps=timestamps,
                step_id=step_id if tutorial_like else (segment_id if concrete_like else 0),
                step_description=step_description if tutorial_like else segment_description,
                step_type=step_type if tutorial_like else "MAIN_FLOW",
                analysis_mode="tutorial_stepwise" if tutorial_like else ("concrete" if concrete_like else "default"),
                main_action=main_action if tutorial_like else "",
                main_operation=main_operation if tutorial_like else ([main_content] if concrete_like and main_content else []),
                instructional_keyframes=instructional_keyframes if (tutorial_like or concrete_like) else [],
                instructional_clips=instructional_clips if (tutorial_like or concrete_like) else [],
                precautions=precautions if tutorial_like else [],
                step_summary=step_summary if tutorial_like else "",
                operation_guidance=operation_guidance if tutorial_like else [],
            )
            results.append(result)

            if tutorial_like:
                normalized_payload.append({
                    "step_id": step_id,
                    "step_description": step_description,
                    "step_type": step_type,
                    "main_action": main_action,
                    "main_operation": main_operation,
                    "precautions": precautions,
                    "step_summary": step_summary,
                    "operation_guidance": operation_guidance,
                    "clip_start_sec": clip_start_sec,
                    "clip_end_sec": clip_end_sec,
                    "instructional_keyframes": instructional_keyframes,
                    "instructional_clips": instructional_clips,
                    "instructional_keyframe_timestamp": timestamps,
                })
            elif concrete_like:
                concrete_payload: Dict[str, Any] = {
                    "segment_id": segment_id,
                    "segment_description": segment_description,
                    "main_content": main_content,
                    "no_needed_video": bool(no_needed_video),
                    "should_type": should_type,
                    "clip_start_sec": clip_start_sec,
                    "clip_end_sec": clip_end_sec,
                    "instructional_keyframes": instructional_keyframes,
                    "instructional_clips": instructional_clips,
                    "instructional_keyframe_timestamp": timestamps,
                }
                if precautions:
                    concrete_payload["precautions"] = precautions
                if step_summary:
                    concrete_payload["segment_summary"] = step_summary
                normalized_payload.append(concrete_payload)
            else:
                normalized_payload.append({
                    "id": safe_int(item.get("id", index), index),
                    "knowledge_type": knowledge_type,
                    "no_needed_video": bool(no_needed_video),
                    "should_type": should_type,
                    "confidence": safe_float(item.get("confidence", 0.0), 0.0),
                    "clip_start_sec": clip_start_sec,
                    "clip_end_sec": clip_end_sec,
                    "suggested_screenshoot_timestamps": timestamps,
                })

        if normalized_mode == "tutorial_stepwise" or has_step_schema:
            results = self._enforce_tutorial_step_constraints(results)
            normalized_payload = [
                {
                    "step_id": int(r.step_id),
                    "step_description": str(r.step_description or "").strip(),
                    "step_type": self._normalize_step_type(getattr(r, "step_type", "")),
                    "main_action": str(r.main_action or "").strip(),
                    "main_operation": list(r.main_operation or []),
                    "instructional_keyframes": list(r.instructional_keyframes or []),
                    "instructional_clips": list(r.instructional_clips or []),
                    "precautions": list(r.precautions or []),
                    "step_summary": str(r.step_summary or "").strip(),
                    "operation_guidance": list(r.operation_guidance or []),
                    "clip_start_sec": float(r.clip_start_sec),
                    "clip_end_sec": float(r.clip_end_sec),
                    "instructional_keyframe_timestamp": list(r.suggested_screenshoot_timestamps or []),
                }
                for r in results
            ]

        return results, normalized_payload

    def _parse_response(
        self,
        content: str,
        finish_reason: Optional[str] = None,
        analysis_mode: str = "default",
    ) -> List[VLAnalysisResult]:
        """方法说明：VLVideoAnalyzer._parse_response 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        results, _ = self._parse_response_with_payload(
            content,
            finish_reason=finish_reason,
            analysis_mode=analysis_mode,
        )
        return results
    def _extract_json_candidate(self, content: str) -> str:
        """
        从模型回复中提取最可能的 JSON 片段。

        处理场景：
        - ```json ... ``` 代码块包裹
        - 回复前后带自然语言
        - 顶层为数组或对象
        """
        if not content:
            return ""

        # 1) 优先提取第一个代码块
        json_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", content, flags=re.IGNORECASE)
        if json_match:
            return json_match.group(1).strip()

        # 2) 尝试从第一个 { 或 [ 开始做括号配对提取
        start_idx = None
        for i, ch in enumerate(content):
            if ch in "[{":
                start_idx = i
                break
        if start_idx is None:
            return content.strip()

        extracted = self._extract_balanced_json(content[start_idx:])
        return (extracted or content[start_idx:]).strip()

    def _extract_balanced_json(self, s: str) -> Optional[str]:
        """从字符串开头提取括号配对的 JSON（忽略字符串内部的括号）。"""
        if not s or s[0] not in "[{":
            return None

        stack = []
        in_str = False
        escape = False
        for i, ch in enumerate(s):
            if in_str:
                if escape:
                    escape = False
                    continue
                if ch == "\\\\":
                    escape = True
                    continue
                if ch == "\"":
                    in_str = False
                continue

            if ch == "\"":
                in_str = True
                continue

            if ch in "[{":
                stack.append(ch)
            elif ch in "]}":
                if not stack:
                    return None
                left = stack.pop()
                if (left == "[" and ch != "]") or (left == "{" and ch != "}"):
                    return None
                if not stack:
                    return s[: i + 1]

        return None

    def _parse_json_payload(self, text: str) -> tuple[Optional[Any], Optional[Exception]]:
        return json_payload_repair.parse_json_payload(
            text,
            extra_repairers=[self._repair_key_evidence_field],
        )

    def _build_json_parse_candidates(self, text: str) -> List[str]:
        return json_payload_repair.build_json_parse_candidates(
            text,
            extra_repairers=[self._repair_key_evidence_field],
        )

    def _extract_salvaged_json_objects(self, text: str) -> tuple[List[Dict[str, Any]], Optional[Exception]]:
        return json_payload_repair.extract_salvaged_json_objects(
            text,
            extra_repairers=[self._repair_key_evidence_field],
        )

    def _extract_top_level_objects(self, text: str) -> List[str]:
        return json_payload_repair.extract_top_level_objects(text)

    def _normalize_jsonish_text(self, text: str) -> str:
        return json_payload_repair.normalize_jsonish_text(text)

    def _escape_control_chars_in_strings(self, text: str) -> str:
        return json_payload_repair.escape_control_chars_in_strings(text)

    def _remove_trailing_commas(self, text: str) -> str:
        return json_payload_repair.remove_trailing_commas(text)

    def _repair_unclosed_json(self, text: str) -> str:
        return json_payload_repair.repair_unclosed_json(text)

    def _repair_key_evidence_field(self, json_str: str) -> str:
        """
        修复模型常见输出：
        "key_evidence": "a", "b", "c", "clip_start_sec": ...
        将其改写为：
        "key_evidence": ["a", "b", "c"], "clip_start_sec": ...
        """
        pattern = re.compile(
            r"\"key_evidence\"\s*:\s*(?P<vals>\"(?:\\.|[^\"\\])*\"(?:\s*,\s*\"(?:\\.|[^\"\\])*\")+)(?=\s*,\s*\"[A-Za-z_][^\"]*\"\s*:)",
            flags=re.MULTILINE,
        )

        def _repl(m: re.Match) -> str:
            vals = m.group("vals")
            parts = re.findall(r"\"(?:\\\\.|[^\"\\\\])*\"", vals)
            return "\"key_evidence\": [" + ", ".join(parts) + "]"

        return pattern.sub(_repl, json_str)

