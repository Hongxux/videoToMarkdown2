"""
模块说明：Python gRPC 服务端，承接 Java 编排请求并驱动视频处理流水线。
执行逻辑：
1) 暴露下载、转写、Stage1、Phase2A/2B、CV 验证等 gRPC 接口。
2) 统一输出目录到 storage/{hash}，集中管理中间产物。
3) 管理全局资源与并行执行（ProcessPool + SharedMemory）。
实现方式：grpc.aio + RichTextPipeline + CVKnowledgeValidator + SharedFrameRegistry。
核心价值：打通 Java-Python 调用链，集中资源管理与缓存复用。
输入：
- gRPC 请求参数（视频路径/URL、配置路径、任务数据）。
输出：
- gRPC 响应/流式结果与落盘文件路径。"""

import os
import sys


def _configure_opencv_env() -> None:
    """
    执行逻辑：
    1) 禁用 OpenCV OpenCL 运行时加载。
    2) 避免无用组件占用内存。
    实现方式：设置环境变量。
    核心价值：降低单进程 OpenCV 运行时内存占用。
    """
    if not os.getenv("OPENCV_OPENCL_RUNTIME"):
        os.environ["OPENCV_OPENCL_RUNTIME"] = "disabled"


_configure_opencv_env()

from services.python_grpc.src.server.runtime_env import sanitize_user_site_packages

sanitize_user_site_packages()


def _reconfigure_stdio_errors() -> None:
    """
    执行逻辑：
    1) 尝试将 stdout/stderr 的 errors 策略设为 backslashreplace。
    2) 避免在 Windows/Java 子进程管道等非 UTF-8 环境输出 emoji 时抛 UnicodeEncodeError。
    实现方式：sys.stdout.reconfigure / sys.stderr.reconfigure。
    核心价值：提升启动阶段可观测性与稳定性，避免“打印一行后卡住/退出”的误判。
    决策逻辑：
    - 条件：stream 支持 reconfigure。
    - 条件：仅设置 errors，不强制修改 encoding（避免影响终端显示）。
    依据来源（证据链）：
    - 报错样例：UnicodeEncodeError: 'gbk' codec can't encode character '\\U0001f680'。
    输入参数：无。
    输出参数：无（仅修改 stdout/stderr 行为）。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="backslashreplace")
        except Exception:
            pass


def _safe_print(message: str) -> None:
    """
    执行逻辑：
    1) 优先按原字符串输出并 flush。
    2) 若遇到编码异常，则将不可编码字符转义后再输出。
    实现方式：print + UnicodeEncodeError fallback（ascii/backslashreplace）。
    核心价值：让启动日志在被管道捕获/重定向时仍可输出，不因 emoji 导致直接退出。
    输入参数：
    - message: 待输出字符串（类型：str）。
    输出参数：无（仅产生副作用：stdout 输出）。"""
    try:
        print(message, flush=True)
    except UnicodeEncodeError:
        safe = message.encode("ascii", "backslashreplace").decode("ascii")
        print(safe, flush=True)


def _is_truthy_env(name: str) -> bool:
    """
    执行逻辑：读取 env 并判断是否为真值开关。
    实现方式：字符串归一化 + 集合判断。
    核心价值：统一环境变量开关解析，减少分支重复。
    输入参数：
    - name: 环境变量名（类型：str）。
    输出参数：
    - bool：是否为真值。"""
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


_reconfigure_stdio_errors()

_DEBUG_IMPORTS = _is_truthy_env("GRPC_SERVER_DEBUG_IMPORTS")
_CHECK_DEPS = False

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument(
        "--check-deps",
        action="store_true",
        help="仅做依赖预检并退出（用于排查启动卡住/导入失败）。",
    )
    parser.add_argument(
        "--debug-imports",
        action="store_true",
        help="启动阶段打印关键 import 的进度（也可用 env: GRPC_SERVER_DEBUG_IMPORTS=1）。",
    )
    _args, _unknown = parser.parse_known_args()
    _CHECK_DEPS = _args.check_deps
    _DEBUG_IMPORTS = _DEBUG_IMPORTS or _args.debug_imports


def _boot(msg: str) -> None:
    """
    执行逻辑：在启动调试模式下输出一条 bootstrap 日志。
    实现方式：读取全局开关 + _safe_print。
    核心价值：定位“卡在 import/初始化哪一步”。
    输入参数：
    - msg: 日志内容（类型：str）。
    输出参数：无（仅产生副作用：stdout 输出）。"""
    if _DEBUG_IMPORTS:
        _safe_print(msg)


def _prepend_sys_path(path_value: str) -> None:
    """
    执行逻辑：将路径插入 sys.path 头部（去重）。
    实现方式：包含性判断 + sys.path.insert(0, ...)。
    核心价值：保证内部包可被导入，同时避免重复插入造成 sys.path 膨胀。
    输入参数：
    - path_value: 待插入路径（类型：str）。
    输出参数：无（仅修改 sys.path）。"""
    if not path_value:
        return
    # 强制置顶：若已存在则先移除再插入，避免“存在但不在首位”导致同名包抢占
    if path_value in sys.path:
        try:
            sys.path.remove(path_value)
        except ValueError:
            pass
    sys.path.insert(0, path_value)


def _run_dependency_preflight() -> int:
    """
    执行逻辑：
    1) 逐项尝试 import 关键依赖与关键模块。
    2) 汇总缺失模块与导入异常并输出可执行的修复建议。
    实现方式：importlib.import_module + 异常收集。
    核心价值：把“启动卡住/导入失败”转化为可定位、可执行的安装清单。
    输入参数：无。
    输出参数：
    - int：0=通过；2=失败（缺失/异常）。"""
    import importlib
    import traceback as _tb

    # 覆盖当前 gRPC 入口顶部 import 以及其主要依赖链路
    modules_to_check = [
        ("psutil", "psutil"),
        ("grpc", "grpcio"),
        ("grpc.aio", "grpcio"),
        ("numpy", "numpy"),
        # gRPC 生成代码与内部模块（能进一步暴露缺失的三方依赖）
        ("video_processing_pb2", None),
        ("video_processing_pb2_grpc", None),
        ("services.python_grpc.src.transcript_pipeline.graph", None),
        ("services.python_grpc.src.media_engine.knowledge_engine.core.video", None),
        ("services.python_grpc.src.media_engine.knowledge_engine.core.transcription", None),
        ("services.python_grpc.src.content_pipeline", None),
    ]

    missing_modules = set()
    import_errors = []

    for module_name, _pip_hint in modules_to_check:
        _boot(f"[CHECK] import {module_name}")
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError as e:
            missing_modules.add(e.name or module_name)
        except Exception:
            import_errors.append((module_name, _tb.format_exc()))

    if missing_modules or import_errors:
        _safe_print("依赖预检失败：以下问题可能导致服务启动停在启动行后无后续日志。")

        if missing_modules:
            _safe_print("缺失模块（ModuleNotFoundError）：")
            for name in sorted(missing_modules):
                _safe_print(f"- {name}")

        if import_errors:
            _safe_print("导入异常（非缺失模块）：")
            for module_name, detail in import_errors:
                _safe_print(f"- {module_name}")
                _safe_print(detail.strip())

        _safe_print("建议修复：")
        _safe_print("1) 先安装统一依赖：pip install -r requirements.grpc_server.txt")
        _safe_print("2) 若仍失败：python apps/grpc-server/main.py --check-deps --debug-imports")
        return 2

    _safe_print("依赖预检通过。")
    return 0


_safe_print("🚀 PYTHON GRPC SERVER IS STARTING - VERSION V3.1 (PARALLEL CV) 🚀")

# 添加项目路径（尽量前置，便于 --check-deps 也能检查内部模块）
current_dir = os.path.dirname(os.path.abspath(__file__))
from .import_path_setup import setup_import_paths

repo_root = setup_import_paths(__file__, _prepend_sys_path)
# 关键修复：再次把当前仓库根路径置顶，避免命名空间包被上级目录同名仓库抢占
_prepend_sys_path(repo_root)

if _CHECK_DEPS:
    raise SystemExit(_run_dependency_preflight())
_boot("[BOOT] import logging")
import logging
import asyncio
import threading
from services.python_grpc.src.common.logging import configure_pipeline_logging
_boot("[BOOT] import psutil")
import psutil
import traceback
import time
import hashlib
import shutil
import json
import gzip
import re
import copy
import uuid
import shlex
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from collections import Counter
from concurrent import futures
from typing import Callable, Optional, List, Dict, Any, Tuple
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from urllib.request import url2pathname
import yaml

from services.python_grpc.src.config_paths import (
    resolve_module2_config_file,
    resolve_video_config_path,
)

_boot("[BOOT] import grpc")
import grpc
import gc
_boot("[BOOT] import numpy")
import numpy as np
from grpc import aio
import functools

# gRPC 生成的代码 (需要先运行 protoc 生成)
_boot("[BOOT] import gRPC pb2/pb2_grpc")
import video_processing_pb2
import video_processing_pb2_grpc

# 模块导入
_boot("[BOOT] import services.python_grpc.src.transcript_pipeline.graph")
from services.python_grpc.src.transcript_pipeline.graph import run_pipeline
from services.python_grpc.src.common.utils.async_disk_writer import (
    enqueue_json_write,
    enqueue_text_write,
    flush_async_json_writes,
)
_boot("[BOOT] import services.python_grpc.src.media_engine.knowledge_engine.core.video")
from services.python_grpc.src.media_engine.knowledge_engine.core.video import VideoProcessor
_boot("[BOOT] import services.python_grpc.src.media_engine.knowledge_engine.core.transcription")
from services.python_grpc.src.media_engine.knowledge_engine.core.transcription import Transcriber
try:
    import services.python_grpc.src.media_engine.knowledge_engine as _knowledge_engine_pkg
    _safe_print(f"[BOOT] knowledge_engine package search path: {list(getattr(_knowledge_engine_pkg, '__path__', []))}")
except Exception as _vtm_e:
    _safe_print(f"[BOOT] Failed to inspect knowledge_engine package path: {_vtm_e}")
_boot("[BOOT] import services.python_grpc.src.content_pipeline")
from services.python_grpc.src.content_pipeline import (
    RichTextPipeline,
    PipelineConfig,
    ScreenshotRequest,
    ClipRequest,
    MaterialRequests,
    SemanticUnitSegmenter
)
from services.python_grpc.src.content_pipeline.shared.semantic_payload import (
    iter_semantic_unit_nodes as shared_iter_semantic_unit_nodes,
    build_semantic_unit_index as shared_build_semantic_unit_index,
    normalize_semantic_units_payload as shared_normalize_semantic_units_payload,
    build_grouped_semantic_units_payload as shared_build_grouped_semantic_units_payload,
)
from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import (
    VisualFeatureExtractor,
    get_visual_process_pool,
    get_shared_frame_registry,
    SharedFrameRegistry,
)
# 🔑 Import tools for GenerateMaterialRequests
from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector
from services.python_grpc.src.content_pipeline.infra.llm.llm_client import AdaptiveConcurrencyLimiter
from .douyin_download import (
    download_video_with_douyin_downloader as _download_video_with_douyin_downloader,
    probe_douyin_video_info as _probe_douyin_video_info,
)
from .download_service import run_download_flow
from .platform_rules import (
    build_task_dir_encoding_source as _build_task_dir_encoding_source_from_rules,
    extract_bilibili_video_id as _extract_bilibili_video_id_from_rules,
    extract_douyin_aweme_ref as _extract_douyin_aweme_ref_from_rules,
    is_bilibili_host as _is_bilibili_host_from_rules,
    is_douyin_host as _is_douyin_host_from_rules,
    is_douyin_url as _is_douyin_url_from_rules,
)
from .share_link_resolver import resolve_share_link
from .vl_report_writer import VLReportWriter
from .watchdog_signal_writer import (
    TaskWatchdogSignalWriter,
    publish_watchdog_signal,
    read_watchdog_signals,
)
from .book_pdf_extractor import extract_book_pdf_markdown

logger = logging.getLogger(__name__)


RESUME_META_SCHEMA_VERSION = "resume_meta_v1"
RESUME_GROUPS = (
    "transcribe",
    "stage1_text",
    "stage1_semantic",
    "stage1_visual",
    "stage1_document",
    "phase2a",
    "assets",
    "phase2b",
)


@dataclass(frozen=True)
class ResumeControl:
    """断点重续控制配置。"""

    enabled: bool
    mode: str
    validation: str
    on_invalid_reuse: str
    non_priority_retention_days: int
    priority_keep_only_phase2a: bool
    groups: Dict[str, bool]


def _deep_merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """递归合并字典，override 优先级更高。"""
    result = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = _deep_merge_dict(result[key], value)
        else:
            result[key] = value
    return result


def _to_bool(value: Any, default: bool = False) -> bool:
    """将配置值转为布尔值。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _to_int(value: Any, default: int) -> int:
    """将配置值转为整数。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float) -> float:
    """Convert config value to float."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_cli_args(value: Any) -> Optional[List[str]]:
    """将配置值归一化为命令行参数列表。"""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = shlex.split(text, posix=False)
        except ValueError:
            parsed = text.split()
        normalized = [str(item).strip() for item in parsed if str(item).strip()]
        return normalized or None
    if isinstance(value, (list, tuple)):
        normalized = [str(item).strip() for item in value if str(item).strip()]
        return normalized or None
    return None


def _load_yaml_file(path: Path) -> Dict[str, Any]:
    """加载 YAML 文件，失败时返回空字典。"""
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
            return data if isinstance(data, dict) else {}
    except Exception as exc:
        logger.warning(f"Failed to load yaml {path}: {exc}")
        return {}


def _load_download_video_options(config: Dict[str, Any]) -> Dict[str, Any]:
    """读取下载配置并映射为 VideoProcessor 初始化参数。"""
    video_cfg = config.get("video", {}) if isinstance(config, dict) else {}
    if not isinstance(video_cfg, dict):
        video_cfg = {}

    profile_name_raw = str(video_cfg.get("download_profile", "") or "").strip()
    profile_name = profile_name_raw or "public_no_cookie"
    profile_cfg: Dict[str, Any] = {}
    profiles_raw = video_cfg.get("download_profiles", {})
    if isinstance(profiles_raw, dict):
        candidate = profiles_raw.get(profile_name)
        if isinstance(candidate, dict):
            profile_cfg = candidate
        elif profile_name_raw:
            logger.warning(f"Unknown video.download_profile: {profile_name}")
    elif profile_name_raw:
        logger.warning("video.download_profiles is not a dict, ignored")

    def _pick_profile_str(config_key: str) -> Optional[str]:
        value = profile_cfg.get(config_key)
        if value is None:
            return None
        normalized = str(value).strip()
        return normalized or None

    def _pick_str(config_key: str, env_key: str) -> Optional[str]:
        env_value = os.getenv(env_key, "").strip()
        if env_value:
            return env_value
        config_value = video_cfg.get(config_key)
        if config_value is None:
            return _pick_profile_str(config_key)
        value = str(config_value).strip()
        if value:
            return value
        return _pick_profile_str(config_key)

    def _pick_args(config_key: str, env_key: str) -> Optional[List[str]]:
        env_value = os.getenv(env_key, "").strip()
        if env_value:
            return _normalize_cli_args(env_value)

        config_value = video_cfg.get(config_key)
        if config_value is None:
            return _normalize_cli_args(profile_cfg.get(config_key))

        normalized = _normalize_cli_args(config_value)
        if normalized:
            return normalized
        return _normalize_cli_args(profile_cfg.get(config_key))

    disable_ssl_env = os.getenv("YTDLP_DISABLE_SSL_VERIFY", "").strip()
    if disable_ssl_env:
        disable_ssl_verify = _to_bool(disable_ssl_env, False)
    else:
        disable_ssl_verify = _to_bool(video_cfg.get("disable_ssl_verify", False), False)

    prefer_h264_env = os.getenv("YTDLP_PREFER_H264", "").strip()
    if prefer_h264_env:
        prefer_h264 = _to_bool(prefer_h264_env, True)
    else:
        prefer_h264 = _to_bool(video_cfg.get("prefer_h264", True), True)

    short_video_max_duration_sec_env = os.getenv("YTDLP_SHORT_VIDEO_MAX_DURATION_SEC", "").strip()
    if short_video_max_duration_sec_env:
        short_video_max_duration_sec = _to_float(short_video_max_duration_sec_env, 3600.0)
    else:
        short_video_max_duration_sec = _to_float(video_cfg.get("short_video_max_duration_sec", 3600.0), 3600.0)
    if short_video_max_duration_sec <= 0:
        short_video_max_duration_sec = 3600.0

    return {
        "proxy": _pick_str("download_proxy", "YTDLP_PROXY"),
        "disable_ssl_verify": disable_ssl_verify,
        "cookies_file": _pick_str("download_cookies_file", "YTDLP_COOKIES_FILE"),
        "cookies_from_browser": _pick_str("download_cookies_from_browser", "YTDLP_COOKIES_FROM_BROWSER"),
        "prefer_h264": prefer_h264,
        "short_video_max_duration_sec": short_video_max_duration_sec,
        "external_downloader": _pick_str("external_downloader", "YTDLP_EXTERNAL_DOWNLOADER"),
        "external_downloader_args": _pick_args("external_downloader_args", "YTDLP_EXTERNAL_DOWNLOADER_ARGS"),
        "youtube_download_proxy": _pick_str("youtube_download_proxy", "YTDLP_YOUTUBE_PROXY"),
        "youtube_simple_downloader_script": _pick_str(
            "youtube_simple_downloader_script",
            "YTDLP_YOUTUBE_SIMPLE_DOWNLOADER_SCRIPT",
        ),
        "youtube_pot_script_home": _pick_str("youtube_pot_script_home", "YTDLP_YOUTUBE_POT_SCRIPT_HOME"),
        "youtube_pot_http_base_url": _pick_str("youtube_pot_http_base_url", "YTDLP_YOUTUBE_POT_HTTP_BASE_URL"),
        "youtube_js_runtimes": _pick_args("youtube_js_runtimes", "YTDLP_YOUTUBE_JS_RUNTIMES"),
        "youtube_remote_components": _pick_args("youtube_remote_components", "YTDLP_YOUTUBE_REMOTE_COMPONENTS"),
    }


def _load_resume_control_from_configs() -> ResumeControl:
    """从双配置源加载断点重续配置并归一化。"""
    video_config_path = resolve_video_config_path(anchor_file=__file__)
    module2_config_path = resolve_module2_config_file(anchor_file=__file__)
    video_config = _load_yaml_file(video_config_path) if video_config_path else {}
    module2_config = _load_yaml_file(module2_config_path) if module2_config_path else {}

    module2_resume = module2_config.get("resume_control", {}) if isinstance(module2_config, dict) else {}
    video_resume = video_config.get("resume_control", {}) if isinstance(video_config, dict) else {}
    merged = _deep_merge_dict(module2_resume, video_resume)

    merged_groups = merged.get("groups", {}) if isinstance(merged.get("groups", {}), dict) else {}
    groups = {group: _to_bool(merged_groups.get(group, False), False) for group in RESUME_GROUPS}

    return ResumeControl(
        enabled=_to_bool(merged.get("enabled", False), False),
        mode=str(merged.get("mode", "file_reuse") or "file_reuse"),
        validation=str(merged.get("validation", "moderate") or "moderate"),
        on_invalid_reuse=str(merged.get("on_invalid_reuse", "recompute") or "recompute"),
        non_priority_retention_days=max(0, _to_int(merged.get("non_priority_retention_days", 7), 7)),
        priority_keep_only_phase2a=_to_bool(merged.get("priority_keep_only_phase2a", True), True),
        groups=groups,
    )


def _utc_now_iso() -> str:
    """获取 UTC ISO 时间字符串。"""
    return datetime.now(timezone.utc).isoformat()


def _safe_parse_iso_datetime(value: str) -> Optional[datetime]:
    """安全解析 ISO 时间。"""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _file_signature(path: str) -> Dict[str, Any]:
    """生成文件签名（存在性、大小、修改时间）。"""
    try:
        abs_path = os.path.abspath(path)
        if not os.path.exists(abs_path):
            return {"exists": False, "path": abs_path}
        stat = os.stat(abs_path)
        return {
            "exists": True,
            "path": abs_path,
            "size": stat.st_size,
            "mtime": int(stat.st_mtime),
        }
    except Exception:
        return {"exists": False, "path": os.path.abspath(path)}


def _build_input_fingerprint(video_path: str, subtitle_path: str = "", extra: Optional[Dict[str, Any]] = None) -> str:
    """构建输入指纹。"""
    payload = {
        "video": _file_signature(video_path),
        "subtitle": _file_signature(subtitle_path) if subtitle_path else None,
        "extra": extra or {},
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _resource_meta_path(resource_path: str) -> str:
    """返回资源元数据文件路径。"""
    return f"{resource_path}.meta.json"


def _read_resource_meta(resource_path: str) -> Dict[str, Any]:
    """读取资源元数据。"""
    meta_path = _resource_meta_path(resource_path)
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as file:
            data = json.load(file)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _is_compacted_stage1_output(resource_path: str) -> bool:
    """检测 Stage1 关键产物是否为压缩预览结构（count/sample）。"""
    basename = os.path.basename(resource_path)
    if basename not in {"step2_correction_output.json", "step6_merge_cross_output.json"}:
        return False

    try:
        with open(resource_path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        return False

    payload = data.get("output", data) if isinstance(data, dict) else {}
    if not isinstance(payload, dict):
        return False

    if basename == "step2_correction_output.json":
        target = payload.get("corrected_subtitles")
    else:
        target = payload.get("pure_text_script")

    return (
        isinstance(target, dict)
        and isinstance(target.get("count"), int)
        and isinstance(target.get("sample"), list)
    )


def _load_stage1_output_list(resource_path: str, output_field: str) -> Tuple[Optional[List[Dict[str, Any]]], str]:
    """读取 Stage1 步骤输出中的指定列表字段。"""
    if not os.path.exists(resource_path):
        return None, "missing_resource"

    try:
        with open(resource_path, "r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        return None, "invalid_json"

    payload = data.get("output", data) if isinstance(data, dict) else {}
    if not isinstance(payload, dict):
        return None, "invalid_payload"

    value = payload.get(output_field)
    if isinstance(value, list):
        return value, "ok"
    if (
        isinstance(value, dict)
        and isinstance(value.get("count"), int)
        and isinstance(value.get("sample"), list)
    ):
        return None, "compacted_output"
    return None, "missing_output_field"


def _write_resource_meta(
    resource_path: str,
    *,
    group: str,
    input_fingerprint: str,
    dependencies: Optional[Dict[str, Any]] = None,
    priority: bool = False,
) -> None:
    """写入资源元数据。"""
    meta_path = _resource_meta_path(resource_path)
    payload = {
        "schema_version": RESUME_META_SCHEMA_VERSION,
        "created_at": _utc_now_iso(),
        "resource_path": os.path.abspath(resource_path),
        "group": group,
        "input_fingerprint": input_fingerprint,
        "dependencies": dependencies or {},
        "priority": priority,
    }
    with open(meta_path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def _validate_resource_reuse(
    resource_path: str,
    *,
    group: str,
    expected_input_fingerprint: str,
) -> (bool, str):
    """执行中等强度资源复用校验。"""
    if not os.path.exists(resource_path):
        return False, "missing_resource"
    if os.path.getsize(resource_path) <= 0:
        return False, "empty_resource"
    if group == "stage1_text" and _is_compacted_stage1_output(resource_path):
        return False, "compacted_output"

    meta = _read_resource_meta(resource_path)
    if not meta:
        return False, "missing_meta"
    if meta.get("schema_version") != RESUME_META_SCHEMA_VERSION:
        return False, "schema_mismatch"
    if meta.get("group") != group:
        return False, "group_mismatch"
    if meta.get("input_fingerprint") != expected_input_fingerprint:
        return False, "fingerprint_mismatch"

    dependencies = meta.get("dependencies", {})
    if isinstance(dependencies, dict):
        for dep_name, dep_sig in dependencies.items():
            if not isinstance(dep_sig, dict):
                return False, f"dependency_invalid_{dep_name}"
            dep_path = dep_sig.get("path", "")
            if not dep_path:
                return False, f"dependency_missing_path_{dep_name}"
            current = _file_signature(dep_path)
            if bool(dep_sig.get("exists", False)) != bool(current.get("exists", False)):
                return False, f"dependency_exists_mismatch_{dep_name}"
            if dep_sig.get("exists", False):
                if dep_sig.get("size") != current.get("size"):
                    return False, f"dependency_size_mismatch_{dep_name}"
                if dep_sig.get("mtime") != current.get("mtime"):
                    return False, f"dependency_mtime_mismatch_{dep_name}"
    return True, "ok"


def _phase2a_semantic_units_candidates(output_dir: str) -> List[str]:
    """返回 Phase2A 语义单元产物候选路径（主路径优先，兼容 intermediates）。"""
    abs_output_dir = os.path.abspath(output_dir)
    candidates = [
        os.path.join(abs_output_dir, "semantic_units_phase2a.json"),
        os.path.join(abs_output_dir, "intermediates", "semantic_units_phase2a.json"),
    ]
    deduplicated: List[str] = []
    seen = set()
    for candidate in candidates:
        normalized = os.path.normcase(os.path.normpath(candidate))
        if normalized in seen:
            continue
        seen.add(normalized)
        deduplicated.append(candidate)
    return deduplicated


def _resolve_reuse_candidate(
    candidate_paths: List[str],
    *,
    group: str,
    expected_input_fingerprint: str,
    reuse_enabled: bool,
) -> Tuple[Optional[str], str]:
    """按复用策略选择可复用产物路径。"""
    normalized_candidates: List[str] = []
    seen = set()
    for path in candidate_paths:
        normalized = os.path.normcase(os.path.normpath(path))
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_candidates.append(path)

    if not normalized_candidates:
        return None, "missing_resource"

    if reuse_enabled:
        candidate_reasons: List[str] = []
        for candidate in normalized_candidates:
            valid, reason = _validate_resource_reuse(
                candidate,
                group=group,
                expected_input_fingerprint=expected_input_fingerprint,
            )
            if valid:
                return candidate, "ok"
            candidate_reasons.append(f"{candidate}:{reason}")
        return None, " | ".join(candidate_reasons) if candidate_reasons else "missing_resource"

    for candidate in normalized_candidates:
        if not os.path.exists(candidate):
            continue
        if os.path.getsize(candidate) <= 0:
            continue
        return candidate, "legacy_exists"
    return None, "missing_resource"


def _find_stage1_output_conflicts(output_dir: str) -> List[Dict[str, str]]:
    """扫描同一任务哈希下的 stage1 关键文件冲突（size/mtime/path）。"""
    conflicts: List[Dict[str, str]] = []
    try:
        abs_output_dir = os.path.abspath(output_dir)
        task_hash = os.path.basename(abs_output_dir)
        storage_roots = [root for root in _get_storage_roots_for_scan() if os.path.isdir(root)]
        if not task_hash or not storage_roots:
            return conflicts

        candidates = []
        for storage_root in storage_roots:
            candidates.extend(
                [
                    os.path.join(storage_root, task_hash, "intermediates", "step2_correction_output.json"),
                    os.path.join(storage_root, task_hash, "intermediates", "step6_merge_cross_output.json"),
                ]
            )

        canonical: Dict[str, Dict[str, Any]] = {}
        for candidate in candidates:
            normalized = os.path.normpath(candidate)
            if normalized in canonical:
                continue
            try:
                if not os.path.exists(candidate):
                    continue
                signature = _file_signature(candidate)
                basename = os.path.basename(candidate)
                if basename not in canonical:
                    canonical[basename] = signature
                else:
                    previous = canonical[basename]
                    if (
                        previous.get("size") != signature.get("size")
                        or previous.get("mtime") != signature.get("mtime")
                        or previous.get("path") != signature.get("path")
                    ):
                        conflicts.append(
                            {
                                "resource": basename,
                                "kept": str(previous.get("path", "")),
                                "conflict": str(signature.get("path", "")),
                            }
                        )
            except Exception:
                continue
    except Exception:
        return conflicts
    return conflicts

# 配置日志级别和格式
configure_pipeline_logging(
    level=logging.INFO,
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True,
)



# =============================================================================
# 输出目录统一规则
# =============================================================================

def _is_http_url(value: str) -> bool:
    """
    执行逻辑：
    1) 判空并统一小写。
    2) 判断是否以 http:// 或 https:// 开头。
    实现方式：字符串前缀匹配。
    核心价值：区分远程 URL 与本地路径，避免错误处理。
    决策逻辑：
    - 条件：not value
    - 条件：value.startswith("http://") or value.startswith("https://")
    依据来源（证据链）：
    - 输入参数：value。
    输入参数：
    - value: URL 或路径字符串（类型：str）。
    输出参数：
    - bool：是否为 http/https URL。
    """
    if not value:
        return False
    lower = value.lower()
    return lower.startswith("http://") or lower.startswith("https://")


def _is_bilibili_host(host: str) -> bool:
    return _is_bilibili_host_from_rules(host)


def _is_douyin_host(host: str) -> bool:
    return _is_douyin_host_from_rules(host)


def _is_douyin_url(video_url: str) -> bool:
    return _is_douyin_url_from_rules(video_url)


def _is_youtube_url(video_url: str) -> bool:
    lower_url = str(video_url or "").lower()
    return "youtube.com/" in lower_url or "youtu.be/" in lower_url


def _extract_bilibili_video_id(video_url: str) -> Optional[str]:
    extracted = _extract_bilibili_video_id_from_rules(video_url)
    return extracted or None


def _extract_douyin_canonical_id(video_url: str) -> str:
    """从抖音 URL 中提取 aweme ID 作为 canonical_id。"""
    kind, aweme_id = _extract_douyin_aweme_ref_from_rules(video_url)
    return aweme_id or ""


def _build_task_dir_encoding_source(video_url: str) -> str:
    return _build_task_dir_encoding_source_from_rules(video_url)


def _normalize_video_title(raw_title: str) -> str:
    """归一化视频标题，去除首尾空白和重复空格。"""
    title = re.sub(r"\s+", " ", str(raw_title or "")).strip()
    return title


def _first_non_blank(*values: Optional[str]) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _normalize_cover_url(raw_url: Any) -> str:
    candidate = str(raw_url or "").strip()
    if not candidate:
        return ""
    if candidate.startswith("//"):
        candidate = f"https:{candidate}"
    if _is_http_url(candidate):
        return candidate
    return ""


def _extract_cover_url(info: Any) -> str:
    if not isinstance(info, dict):
        return ""

    direct_url = _first_non_blank(
        info.get("thumbnail"),
        info.get("cover"),
        info.get("cover_url"),
        info.get("poster"),
        info.get("pic"),
    )
    normalized_direct_url = _normalize_cover_url(direct_url)
    if normalized_direct_url:
        return normalized_direct_url

    thumbnails = info.get("thumbnails")
    if isinstance(thumbnails, list):
        for item in reversed(thumbnails):
            if isinstance(item, dict):
                url = _first_non_blank(item.get("url"), item.get("src"), item.get("thumbnail"))
            else:
                url = str(item or "").strip()
            normalized_url = _normalize_cover_url(url)
            if normalized_url:
                return normalized_url
    return ""


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _extract_first_http_url(raw_text: str) -> str:
    text = str(raw_text or "").strip()
    if not text:
        return ""
    match = re.search(r"(https?://[^\s]+)", text, flags=re.IGNORECASE)
    if not match:
        return text if _is_http_url(text) else ""
    candidate = str(match.group(1) or "").strip()
    trailing_punctuation = "\"'`()[]{}<>，。！？；:,.!?;"
    while candidate and candidate[-1] in trailing_punctuation:
        candidate = candidate[:-1]
    return candidate.strip()


def _normalize_video_probe_input(raw_video_input: str) -> str:
    value = str(raw_video_input or "").strip()
    if not value:
        return ""
    if _is_http_url(value):
        return value
    bv_match = re.search(r"(?i)\b(BV[0-9A-Za-z]{10})\b", value)
    if bv_match:
        return f"https://www.bilibili.com/video/{bv_match.group(1)}"
    return value


def _extract_episode_index_from_url(candidate_url: str) -> int:
    url = str(candidate_url or "").strip()
    if not _is_http_url(url):
        return 0
    try:
        parsed = urlparse(url)
    except Exception:
        return 0
    query_values = parse_qs(parsed.query or "")
    raw_episode_values = query_values.get("p") or query_values.get("P") or []
    for raw_value in raw_episode_values:
        value = _safe_int(raw_value, 0)
        if value > 0:
            return value
    return 0


def _detect_source_platform(url: str) -> str:
    if not _is_http_url(url):
        return ""
    try:
        host = urlparse(url).netloc
    except Exception:
        return ""
    if _is_bilibili_host(host):
        return "bilibili"
    if _is_douyin_host(host):
        return "douyin"
    return "unknown"


def _infer_video_info_content_type(platform: str, resolved_url: str) -> str:
    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform == "bilibili":
        return "video"
    if normalized_platform == "douyin":
        lower_url = str(resolved_url or "").lower()
        if "/note/" in lower_url:
            return "note"
        if "/video/" in lower_url:
            return "video"
    return "unknown"


def _build_episode_candidates(info: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries = info.get("entries")
    if not isinstance(entries, list):
        return []

    episodes: List[Dict[str, Any]] = []
    used_indexes = set()
    for index, entry in enumerate(entries, 1):
        if not isinstance(entry, dict):
            continue
        episode_index = _safe_int(entry.get("playlist_index"), 0)
        if episode_index <= 0:
            episode_index = index
        if episode_index <= 0 or episode_index in used_indexes:
            continue

        title = _normalize_video_title(
            entry.get("title")
            or entry.get("episode")
            or entry.get("part")
            or f"第{episode_index}集"
        )
        episode_url = _first_non_blank(entry.get("webpage_url"), entry.get("url"))
        episodes.append(
            {
                "index": episode_index,
                "title": title,
                "duration_sec": _safe_float(entry.get("duration"), 0.0),
                "episode_url": episode_url,
                "episode_cover_url": _extract_cover_url(entry),
            }
        )
        used_indexes.add(episode_index)

    episodes.sort(key=lambda item: item["index"])
    return episodes


def _resolve_current_episode_index(
    *,
    requested_episode_index: int,
    info: Dict[str, Any],
    episodes: List[Dict[str, Any]],
    total_episodes: int,
) -> int:
    if requested_episode_index > 0:
        if total_episodes <= 0 or requested_episode_index <= total_episodes:
            return requested_episode_index
        return 0

    info_index = _safe_int(info.get("playlist_index"), 0)
    if info_index <= 0:
        requested_entries = info.get("requested_entries")
        if isinstance(requested_entries, list) and requested_entries:
            info_index = _safe_int(requested_entries[0], 0)
    if info_index > 0 and (total_episodes <= 0 or info_index <= total_episodes):
        return info_index

    if total_episodes == 1:
        return 1
    if not episodes:
        return 0
    return 1


def _write_video_meta_file(
    *,
    task_dir: str,
    video_path: str,
    source_url: str,
    resolved_url: str,
    platform: str,
    canonical_id: str,
    title: str,
    resolver: str,
) -> None:
    """
    写入下载元数据，供 Java 编排侧在组装 Markdown 时复用标题。
    """
    if not task_dir:
        return
    payload = {
        "source_url": str(source_url or ""),
        "resolved_url": str(resolved_url or ""),
        "video_path": str(video_path or ""),
        "platform": str(platform or ""),
        "canonical_id": str(canonical_id or ""),
        "title": _normalize_video_title(title),
        "resolver": str(resolver or ""),
        "generated_at": datetime.now().isoformat(),
    }
    meta_path = os.path.join(task_dir, "video_meta.json")
    with open(meta_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def _upsert_video_meta_topic_fields(
    *,
    task_dir: str,
    domain: str,
    main_topic: str,
) -> None:
    normalized_task_dir = str(task_dir or "").strip()
    if not normalized_task_dir:
        return

    normalized_domain = str(domain or "").strip()
    normalized_main_topic = str(main_topic or "").strip()
    if not normalized_domain and not normalized_main_topic:
        return

    meta_path = os.path.join(normalized_task_dir, "video_meta.json")
    payload: Dict[str, Any] = {}
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as file_obj:
                loaded = json.load(file_obj)
            if isinstance(loaded, dict):
                payload = dict(loaded)
        except Exception as exc:
            logger.warning(f"Failed to read existing video_meta.json from {meta_path}: {exc}")

    if normalized_domain:
        payload["domain"] = normalized_domain
    if normalized_main_topic:
        payload["main_topic"] = normalized_main_topic

    with open(meta_path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def _read_video_meta_title(task_dir: str) -> str:
    if not task_dir:
        return ""
    meta_path = os.path.join(task_dir, "video_meta.json")
    if not os.path.exists(meta_path):
        return ""
    try:
        with open(meta_path, "r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    except Exception as exc:
        logger.warning(f"Failed to read video_meta title from {meta_path}: {exc}")
        return ""
    if not isinstance(payload, dict):
        return ""
    return _normalize_video_title(str(payload.get("title", "") or ""))


def _is_placeholder_assemble_title(raw_title: str) -> bool:
    normalized = _normalize_video_title(raw_title)
    if not normalized:
        return True
    placeholder_set = {
        "视频内容",
        "知识文档",
        "video content",
        "knowledge document",
    }
    return normalized.lower() in placeholder_set


def _build_title_from_video_path(video_path: str) -> str:
    normalized_path = _normalize_local_video_path(video_path)
    if not normalized_path:
        return ""
    candidate_title = ""
    try:
        from services.python_grpc.src.transcript_pipeline.tools.file_validator import (
            extract_video_title as _extract_video_title_from_path,
        )
        candidate_title = _extract_video_title_from_path(normalized_path)
    except Exception:
        candidate_title = Path(normalized_path).stem
    stem = _normalize_video_title(candidate_title)
    if not stem:
        return ""
    generic_stems = {"video", "clip", "output", "input"}
    if stem.lower() in generic_stems:
        return ""
    return stem


def _resolve_assemble_document_title(*, request_title: str, output_dir: str, video_path: str) -> str:
    normalized_request_title = _normalize_video_title(request_title)
    if normalized_request_title and not _is_placeholder_assemble_title(normalized_request_title):
        return normalized_request_title

    meta_title = _read_video_meta_title(output_dir)
    if meta_title:
        return meta_title

    path_title = _build_title_from_video_path(video_path)
    if path_title:
        return path_title

    return "视频内容"


def _normalize_local_video_path(video_path: str) -> str:
    """
    执行逻辑：
    1) 为空则直接返回。
    2) 处理 file:// URL 并转换为本地绝对路径。
    3) 其他路径统一转为绝对路径。
    实现方式：urlparse + url2pathname + os.path.abspath。
    核心价值：保证路径规范化，便于 hash 与缓存复用。
    决策逻辑：
    - 条件：not video_path
    - 条件：video_path 以 file:// 开头
    - 条件：parsed.netloc 非空时转为 UNC 路径
    依据来源（证据链）：
    - 输入参数：video_path。
    - URL 字段：parsed.netloc、parsed.path。
    输入参数：
    - video_path: 本地路径或 file:// URL（类型：str）。
    输出参数：
    - 规范化后的绝对路径字符串。
    """
    if not video_path:
        return video_path
    lower = video_path.lower()
    if lower.startswith("file://"):
        parsed = urlparse(video_path)
        path = url2pathname(parsed.path)
        if parsed.netloc and parsed.netloc not in ("", "localhost"):
            path = f"//{parsed.netloc}{path}"
        return os.path.abspath(path)
    return os.path.abspath(video_path)


def _get_primary_storage_root() -> str:
    """
    执行逻辑：
    1) 优先读取环境变量 `V2M_STORAGE_ROOT`。
    2) 未配置时默认使用仓库内 `var/storage/storage`。
    实现方式：Path 解析 + 环境变量覆盖。
    核心价值：统一字幕与中间产物落盘根目录，避免“写到了旧目录但外部看不到”。
    决策逻辑：
    - 条件：os.getenv("V2M_STORAGE_ROOT") 是否存在。
    依据来源（证据链）：
    - 环境变量：V2M_STORAGE_ROOT。
    - 工程目录结构：repo_root/var/storage/storage。
    输入参数：
    - 无。
    输出参数：
    - 存储根目录绝对路径（类型：str）。
    """
    env_root = os.getenv("V2M_STORAGE_ROOT", "").strip()
    if env_root:
        return os.path.abspath(env_root)
    repo_root = Path(__file__).resolve().parents[4]
    return os.path.abspath(str(repo_root / "var" / "storage" / "storage"))


def _get_storage_roots_for_scan() -> List[str]:
    """
    执行逻辑：
    1) 返回主存储根目录。
    2) 追加历史兼容目录（server/storage、repo/storage）用于复用扫描。
    实现方式：列表去重。
    核心价值：兼容历史产物路径，降低迁移期间复用失效概率。
    输入参数：
    - 无。
    输出参数：
    - 存储根目录列表（类型：List[str]）。
    """
    roots = [
        _get_primary_storage_root(),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "storage")),
        os.path.abspath(str(Path(__file__).resolve().parents[4] / "storage")),
    ]
    deduplicated = []
    seen = set()
    for root in roots:
        normalized = os.path.normcase(os.path.normpath(root))
        if normalized in seen:
            continue
        seen.add(normalized)
        deduplicated.append(root)
    return deduplicated


def _normalize_output_dir(video_path: str) -> str:
    """
    执行逻辑：
    1) 计算 storage_root 与视频绝对路径。
    2) 若视频已在 storage 下，返回其父目录。
    3) 否则对路径规范化并计算 md5 作为输出目录。
    实现方式：os.path + hashlib.md5。
    核心价值：统一输出目录规则，便于复用与清理。
    决策逻辑：
    - 条件：os.path.commonpath([abs_video_path, storage_root]) == storage_root
    - 条件：跨盘符导致 commonpath 抛错时走 hash 分支
    依据来源（证据链）：
    - 文件系统路径：abs_video_path、storage_root。
    输入参数：
    - video_path: 文件路径（类型：str）。
    输出参数：
    - 输出目录路径（storage/{hash} 或已存在的父目录）。
    补充说明：
    统一输出目录到 storage/{hash}：
    - 若视频已在 storage 下，直接使用其父目录
    - 若为本地路径，按规范化绝对路径计算 hash"""
    storage_root = _get_primary_storage_root()
    storage_roots_for_scan = _get_storage_roots_for_scan()
    abs_video_path = _normalize_local_video_path(video_path)
    
    for candidate_root in storage_roots_for_scan:
        try:
            if os.path.commonpath([abs_video_path, candidate_root]) == candidate_root:
                return os.path.dirname(abs_video_path)
        except ValueError:
            # 不同盘符时 commonpath 会抛错，继续检查其他根目录
            continue
    
    normalized = os.path.normcase(abs_video_path)
    path_hash = hashlib.md5(normalized.encode("utf-8")).hexdigest()
    return os.path.join(storage_root, path_hash)


def _ensure_local_video_in_storage(video_path: str) -> str:
    """
    执行逻辑：
    1) 若为空或为 HTTP URL，直接返回原路径。
    2) 若已在 storage 下则直接返回。
    3) 创建 storage 目录并优先硬链接，失败则复制。
    实现方式：os.link + shutil.copy2。
    核心价值：保证视频与中间产物同域，便于缓存与清理。
    决策逻辑：
    - 条件：not video_path or _is_http_url(video_path)
    - 条件：commonpath 判断已在 storage 下
    - 条件：target_path 已存在则直接复用
    - 条件：硬链接失败则回退到复制
    依据来源（证据链）：
    - 输入参数：video_path。
    - 文件系统状态：目标文件是否存在、硬链接异常。
    输入参数：
    - video_path: 本地路径或 URL（类型：str）。
    输出参数：
    - 归档后的本地视频路径（storage 目录内）。
    """
    if not video_path or _is_http_url(video_path):
        return video_path

    abs_video_path = _normalize_local_video_path(video_path)
    output_dir = _normalize_output_dir(abs_video_path)
    for candidate_root in _get_storage_roots_for_scan():
        try:
            if os.path.commonpath([abs_video_path, candidate_root]) == candidate_root:
                return abs_video_path
        except ValueError:
            # 不同盘符时 commonpath 会抛错，继续检查其他根目录
            continue

    try:
        os.makedirs(output_dir, exist_ok=True)
        target_path = os.path.join(output_dir, os.path.basename(abs_video_path))

        if os.path.exists(target_path):
            return target_path

        try:
            os.link(abs_video_path, target_path)
            logger.info(f"Linked local video into storage: {target_path}")
            return target_path
        except Exception:
            # 硬链接失败则复制：做什么是降级保证；为什么是跨盘/权限限制常见；权衡是多一次磁盘写入
            pass

        shutil.copy2(abs_video_path, target_path)
        logger.info(f"Copied local video into storage: {target_path}")
        return target_path
    except Exception as e:
        logger.warning(f"Failed to place local video in storage, fallback to original path: {e}")
        return abs_video_path


# =============================================================================
# 🚀 CV 验证模块级函数 (ThreadPool 兼容)
# =============================================================================

# 进程/线程内 Validator 缓存 (避免重复创建)
_cv_validator_cache = {}
_cv_validator_lock = threading.Lock()


def run_cv_validation_unit(video_path: str, unit_data: dict) -> dict:
    """
    执行逻辑：
    1) 从缓存获取或创建 CVKnowledgeValidator。
    2) 调用 detect_visual_states 获取稳定岛与动作段。
    3) 序列化结果并推断 modality/knowledge_subtype。
    4) 异常时返回包含 error 的兜底结果。
    实现方式：线程内缓存 + CVKnowledgeValidator API 调用。
    核心价值：为 CV 批处理提供稳定的单元级验证能力。
    决策逻辑：
    - 条件：video_path not in _cv_validator_cache
    - 条件：not action_units and stable_islands（判定为截图型）
    - 条件：action_units 存在时按首个动作判定 modality
    - 条件：hasattr(au, 'internal_stable_islands') and au.internal_stable_islands
    依据来源（证据链）：
    - 输入参数：video_path、unit_data。
    - CV 输出：stable_islands、action_units。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - unit_data: 单元数据（含 start_sec/end_sec/unit_id）。
    输出参数：
    - dict：unit_id、modality、knowledge_subtype、stable_islands、action_segments（失败时含 error）。"""
    global _cv_validator_cache, _cv_validator_lock
    
    try:
        # 线程内 Validator 复用 (避免重复打开视频)
        with _cv_validator_lock:
            if video_path not in _cv_validator_cache:
                from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator
                logger.info(f"🔄 Creating CVKnowledgeValidator for ThreadPool: {video_path}")
                _cv_validator_cache[video_path] = CVKnowledgeValidator(video_path, use_resource_manager=True)
            validator = _cv_validator_cache[video_path]
        
        # 执行验证 - 使用正确的 API: detect_visual_states
        stable_islands, action_units, redundancy_segments = validator.detect_visual_states(
            start_sec=unit_data["start_sec"],
            end_sec=unit_data["end_sec"]
        )
        
        # 构建结果字典
        # 将 StableIsland 和 ActionUnit 对象序列化为 dict
        stable_islands_data = []
        for si in stable_islands:
            stable_islands_data.append({
                "start_sec": si.start_sec,
                "end_sec": si.end_sec,
                "mid_sec": (si.start_sec + si.end_sec) / 2,
                "duration_sec": si.end_sec - si.start_sec
            })
        
        action_segments_data = []
        for au in action_units:
            # ActionUnit 可能有 internal_stable_islands 属性
            internal_islands = []
            if hasattr(au, 'internal_stable_islands') and au.internal_stable_islands:
                for isi in au.internal_stable_islands:
                    internal_islands.append({
                        "start_sec": isi.start_sec,
                        "end_sec": isi.end_sec,
                        "mid_sec": (isi.start_sec + isi.end_sec) / 2,
                        "duration_sec": isi.end_sec - isi.start_sec
                    })
            
            action_segments_data.append({
                "start_sec": au.start_sec,
                "end_sec": au.end_sec,
                "action_type": getattr(au, 'action_type', au.classify() if hasattr(au, 'classify') else 'knowledge'),
                "internal_stable_islands": internal_islands
            })
        
        # 确定主要模态 (基于 action_units 的分析)
        if not action_units and stable_islands:
            modality = "screenshot"  # 纯稳定 → 截图
            knowledge_subtype = "static"
        elif action_units:
            # 根据第一个 action_unit 的分类决定
            first_action = action_units[0]
            if hasattr(first_action, 'classify_modality'):
                modality_result = first_action.classify_modality()
                modality = modality_result.value if hasattr(modality_result, 'value') else str(modality_result)
            else:
                modality = "video_screenshot"
            knowledge_subtype = getattr(first_action, 'action_type', 'mixed')
        else:
            modality = "unknown"
            knowledge_subtype = "unknown"
        
        return {
            "unit_id": unit_data["unit_id"],
            "modality": modality,
            "knowledge_subtype": knowledge_subtype,
            "stable_islands": stable_islands_data,
            "action_segments": action_segments_data
        }
        
    except Exception as e:
        logger.error(f"CV validation failed for {unit_data['unit_id']}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "unit_id": unit_data["unit_id"],
            "modality": "unknown",
            "knowledge_subtype": "unknown",
            "stable_islands": [],
            "action_segments": [],
            "error": str(e)
        }



class GlobalResourceManager:
    """
    类说明：全局资源单例，集中管理转写器、分类器、Vision AI 与 CV 缓存。
    执行逻辑：
    1) 通过单例模式确保进程内共享资源。
    2) 采用 lazy load 按需初始化，减少冷启动开销。
    3) 为视频级工具与验证器提供缓存复用。
    实现方式：双重检查锁 + 属性惰性初始化。
    核心价值：避免重复初始化昂贵组件，提高并发稳定性。
    输入：
    - initialize 的 config 配置（可选）。
    输出：
    - 各资源实例或内部缓存更新。"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """
        执行逻辑：
        1) 若实例不存在则创建。
        2) 通过双重检查锁避免竞态。
        实现方式：线程锁 + 类变量缓存。
        核心价值：保证全局资源单例。
        决策逻辑：
        - 条件：cls._instance is None
        依据来源（证据链）：
        - 类变量：GlobalResourceManager._instance。
        输入参数：
        - 无。
        输出参数：
        - GlobalResourceManager 单例实例。"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def initialize(self, config: dict = None):
        """
        执行逻辑：
        1) 若已初始化则直接返回。
        2) 设置配置并初始化资源缓存容器。
        实现方式：内部状态赋值 + 锁保护。
        核心价值：确保资源初始化只执行一次。
        决策逻辑：
        - 条件：self._initialized
        依据来源（证据链）：
        - 对象内部状态：self._initialized。
        输入参数：
        - config: 配置对象/字典（类型：dict）。
        输出参数：
        - 无（仅更新内部状态与缓存）。"""
        if self._initialized:
            return
        
        with self._lock:
            if self._initialized:
                return
            
            self.config = config or {}
            self._llm_client = None
            self._vision_client = None
            self._transcriber = None
            self._knowledge_classifier = None
            self._semantic_unit_segmenter = None
            
            # 🚀 CV Validators Cache
            self._cv_validators = {}
            self._cv_validator_lock = threading.Lock()

            # 🚀 Visual Extractor 缓存（按 video_path 复用）
            self._visual_extractors = {}
            self._visual_extractors_lock = threading.Lock()
            
            self._initialized = True
            logger.info("✅ Global resources config saved (lazy loading enabled)")

    @property
    def semantic_unit_segmenter(self):
        """
        执行逻辑：
        1) 懒加载语义切分器（含其 LLM 客户端）。
        2) 返回进程内单例，避免每次 Phase2A 重复初始化。
        实现方式：双重检查锁 + 单例缓存。
        核心价值：减少 Phase2A 热路径上的对象构建与模型客户端初始化成本。
        """
        if self._semantic_unit_segmenter is None:
            with self._lock:
                if self._semantic_unit_segmenter is None:
                    try:
                        self._semantic_unit_segmenter = SemanticUnitSegmenter()
                        logger.info("  → SemanticUnitSegmenter loaded lazily")
                    except Exception as e:
                        logger.error(f"SemanticUnitSegmenter init failed: {e}")
        return self._semantic_unit_segmenter

    def get_visual_extractor(self, video_path: str):
        """
        执行逻辑：
        1) 检查 video_path 对应的视觉提取器缓存。
        2) 缺失时创建并缓存 VisualFeatureExtractor。
        实现方式：缓存字典 + 惰性初始化。
        核心价值：避免 AnalyzeSemanticUnits 重复 new VisualFeatureExtractor。
        """
        with self._visual_extractors_lock:
            if video_path not in self._visual_extractors:
                logger.info(f"🔄 Creating VisualFeatureExtractor for: {video_path}")
                self._visual_extractors[video_path] = VisualFeatureExtractor(video_path)
            return self._visual_extractors[video_path]

    def release_visual_extractor(self, video_path: str):
        """
        执行逻辑：
        1) 从缓存中移除指定视频的视觉提取器。
        2) 若对象提供 cleanup，则主动释放资源。
        """
        with self._visual_extractors_lock:
            extractor = self._visual_extractors.pop(video_path, None)
        if extractor is not None and hasattr(extractor, "cleanup"):
            try:
                extractor.cleanup()
            except Exception:
                pass

    def cleanup_visual_extractors(self):
        """
        执行逻辑：
        1) 清理全部缓存的视觉提取器。
        2) 释放跨任务残留资源，避免内存持续增长。
        """
        with self._visual_extractors_lock:
            extractors = list(self._visual_extractors.values())
            self._visual_extractors.clear()
        for extractor in extractors:
            if hasattr(extractor, "cleanup"):
                try:
                    extractor.cleanup()
                except Exception:
                    pass

    @property
    def transcriber(self):
        """
        执行逻辑:
        1) 懒加载 Transcriber 实例。
        2) 返回已缓存的转写器。
        实现方式:双重检查锁 + 延迟导入。
        核心价值:减少冷启动时间并复用昂贵资源。
        决策逻辑:
        - 条件:self._transcriber is None
        依据来源(证据链):
        - 对象内部状态:self._transcriber。
        输入参数:
        - 无。
        输出参数:
        - Transcriber 实例或 None。"""
        if self._transcriber is None:
            with self._lock:
                if self._transcriber is None:
                    try:
                        from services.python_grpc.src.media_engine.knowledge_engine.core.transcription import Transcriber
                        
                        # Get whisper config
                        w_config = self.config.get("whisper", {})
                        
                        # ── Strategy 解析（fastest / dynamic / custom）──
                        strategy = w_config.get("strategy", "custom")
                        raw_model_size = w_config.get("model_size", "medium")
                        raw_device = w_config.get("device", "cpu")
                        raw_compute = w_config.get("compute_type", "int8")

                        if strategy == "fastest":
                            resolved_model = "base"
                            resolved_device = "cpu"
                            resolved_compute = "int8"
                            logger.info(f"  → Strategy=fastest → model=base, device=cpu, compute=int8")
                        elif strategy == "dynamic":
                            resolved_model = "medium"
                            resolved_device = raw_device
                            resolved_compute = raw_compute
                            logger.info(f"  → Strategy=dynamic → model=medium (auto-select at runtime)")
                        else:
                            resolved_model = raw_model_size
                            resolved_device = raw_device
                            resolved_compute = raw_compute
                            logger.info(f"  → Strategy=custom → model={resolved_model}")

                        # Check parallel config
                        parallel_config = w_config.get("parallel", {})
                        is_parallel = parallel_config.get("enabled", False)
                        num_workers = parallel_config.get("num_workers", 3)
                        segment_duration = parallel_config.get("segment_duration", 600)
                        
                        logger.info(f"  → Initializing Transcriber with config: parallel={is_parallel}, workers={num_workers}")
                        
                        self._transcriber = Transcriber(
                            model_size=resolved_model,
                            device=resolved_device,
                            compute_type=resolved_compute,
                            parallel=is_parallel,
                            num_workers=num_workers,
                            segment_duration=segment_duration,
                            config=self.config
                        )
                        logger.info(f"  → Transcriber loaded lazily (Parallel={is_parallel}, Workers={num_workers})")
                    except Exception as e:
                        logger.error(f"Transcriber init failed: {e}")
        return self._transcriber

    @property
    def knowledge_classifier(self):
        """
        执行逻辑:
        1) 懒加载 KnowledgeClassifier。
        2) 返回已缓存的分类器实例。
        实现方式:双重检查锁 + 延迟导入。
        核心价值:避免重复初始化 LLM 相关资源。
        决策逻辑:
        - 条件:self._knowledge_classifier is None
        依据来源(证据链):
        - 对象内部状态:self._knowledge_classifier。
        输入参数:
        - 无。
        输出参数:
        - KnowledgeClassifier 实例或 None。"""
        if self._knowledge_classifier is None:
            with self._lock:
                if self._knowledge_classifier is None:
                    try:
                        from services.python_grpc.src.content_pipeline.phase2a.segmentation.knowledge_classifier import KnowledgeClassifier
                        # LLMClient 内部现在也是延迟加载 httpx 客户端的,所以这里初始化是安全的
                        self._knowledge_classifier = KnowledgeClassifier()
                        logger.info("  → KnowledgeClassifier loaded lazily")
                    except Exception as e:
                        logger.error(f"KnowledgeClassifier init failed: {e}")
        return self._knowledge_classifier

    @property
    def vision_client(self):
        """
        执行逻辑：
        1) 懒加载 Vision AI 客户端。
        2) 返回已缓存的客户端实例。
        实现方式：双重检查锁 + get_vision_ai_client。
        核心价值：复用连接池，避免重复创建。
        决策逻辑：
        - 条件：self._vision_client is None
        依据来源（证据链）：
        - 对象内部状态：self._vision_client。
        输入参数：
        - 无。
        输出参数：
        - Vision AI 客户端实例或 None。"""
        if self._vision_client is None:
            with self._lock:
                if self._vision_client is None:
                    try:
                        from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import get_vision_ai_client
                        self._vision_client = get_vision_ai_client(self.config)
                        logger.info("  → Vision AI client loaded lazily")
                    except Exception as e:
                        logger.error(f"Vision client init failed: {e}")
        return self._vision_client
    
    def _init_llm_client(self, config):
        """
        执行逻辑：
        1) 从配置或环境变量读取 DeepSeek API Key。
        2) 有 Key 则创建 OpenAI 客户端，否则置空。
        实现方式：OpenAI SDK + 配置字段解析。
        核心价值：为知识分类等模块提供统一 LLM 入口。
        决策逻辑：
        - 条件：api_key 存在才初始化客户端
        依据来源（证据链）：
        - 配置字段：deepseek_api_key、deepseek_base_url。
        - 环境变量：DEEPSEEK_API_KEY。
        输入参数：
        - config: 配置字典（包含 deepseek_* 字段）。
        输出参数：
        - 无（仅更新 self.llm_client）。"""
        try:
            from openai import OpenAI
            api_key = config.get("deepseek_api_key") or os.getenv("DEEPSEEK_API_KEY")
            if api_key:
                self.llm_client = OpenAI(
                    api_key=api_key,
                    base_url=config.get("deepseek_base_url", "https://api.deepseek.com")
                )
                logger.info("  → LLM client initialized (DeepSeek)")
            else:
                self.llm_client = None
                logger.warning("  → LLM client not available (no API key)")
        except Exception as e:
            logger.error(f"  → LLM client init failed: {e}")
            self.llm_client = None
    
    def _init_media_tools(self, config):
        """
        执行逻辑：
        1) 初始化 Vision AI 客户端（全局连接池）。
        2) 清理视频级视觉工具引用，等待按视频惰性创建。
        实现方式：get_vision_ai_client + 成员变量置空。
        核心价值：保持资源集中管理，降低跨视频污染风险。
        输入参数：
        - config: 配置字典。
        输出参数：
        - 无（仅更新内部状态）。"""
        try:
            # Vision AI Client is global as it handles connections/pool
            from services.python_grpc.src.content_pipeline.infra.llm.vision_ai_client import get_vision_ai_client
            self.vision_client = get_vision_ai_client(config)
            logger.info("  → Vision AI client initialized (Global Pool)")
            
            # visual_extractor and screenshot_selector are VIDEO-SPECIFIC.
            # They will be initialized per-video in the handlers or via a cache.
            self.visual_extractor = None 
            self.screenshot_selector = None
            
        except Exception as e:
            logger.error(f"  → Media tools init failed: {e}")
            self.vision_client = None

    def _init_transcriber(self, config):
        """
        执行逻辑：
        1) 读取 whisper_model/whisper_device 配置。
        2) 根据 CPU 核心数计算并行 worker 数（最少 2，最多 8）。
        3) 初始化 Transcriber 并尝试预加载模型。
        实现方式：Transcriber + int8 量化 + 并行转录。
        核心价值：在保证稳定性的前提下提升转写吞吐。
        输入参数：
        - config: 配置字典（whisper_model/whisper_device）。
        输出参数：
        - 无（仅更新 self.transcriber）。"""
        try:
            logger.info("  → Initializing Whisper Transcriber (this may take a while)...")
            # 默认配置
            model_size = config.get("whisper_model", "medium")
            device = config.get("whisper_device", "cpu")
            
            # 🔑 性能优化配置
            # 自动检测 CPU 核心数，设置合理的并行数
            import multiprocessing
            cpu_count = multiprocessing.cpu_count()
            
            # 使用 CPU 核心数的一半作为 workers（最少2，最多8）
            num_workers = max(2, min(cpu_count // 2, 8))
            
            logger.info(f"  → CPU cores: {cpu_count}, using {num_workers} workers for parallel transcription")
            
            self.transcriber = Transcriber(
                model_size=model_size,
                device=device,
                compute_type="int8",     # 🔑 启用 int8 量化加速
                parallel=True,           # 🔑 启用并行转录
                num_workers=num_workers, # 🔑 根据 CPU 核心数动态设置
                config=config
            )
            # 🔑 预加载模型 (异步加载以便不阻塞启动，但此处保持同步以确保就绪)
            # 添加更强健的初始化逻辑
            try:
                self.transcriber._load_model()
                logger.info(f"  → Whisper Transcriber initialized (parallel={True}, int8={True}, workers={num_workers})")
            except Exception as e:
                logger.warning(f"  → Whisper Transcriber model pre-load failed (will retry on first task): {e}")
        except Exception as e:
            logger.error(f"  → Whisper Transcriber component init failed: {e}")
            self.transcriber = None

    # 🚀 CV Validators Cache (global singleton per video)
    def _init_video_tools_cache(self):
        """
        执行逻辑：
        1) 初始化视频级工具缓存容器。
        2) 创建缓存锁以支持并发访问。
        实现方式：字典 + threading.Lock。
        核心价值：复用视觉工具，避免重复初始化。
        决策逻辑：
        - 条件：not hasattr(self, '_video_tools')
        依据来源（证据链）：
        - 对象内部状态：是否已存在 _video_tools。
        输入参数：
        - 无。
        输出参数：
        - 无（仅更新内部缓存）。"""
        if not hasattr(self, "_video_tools"):
            self._video_tools = {} # video_path -> {extractor, selector}
            self._video_tools_lock = threading.Lock()

    def get_screenshot_selector(self, video_path: str):
        """
        执行逻辑：
        1) 检查视频级缓存是否已有 selector。
        2) 缺失时创建 VisualFeatureExtractor 与 ScreenshotSelector。
        实现方式：缓存字典 + 惰性初始化。
        核心价值：按视频复用视觉提取器，减少重复开销。
        决策逻辑：
        - 条件：video_path not in self._video_tools
        依据来源（证据链）：
        - 输入参数：video_path。
        - 对象内部状态：self._video_tools。
        输入参数：
        - video_path: 文件路径（类型：str）。
        输出参数：
        - ScreenshotSelector 实例。"""
        self._init_video_tools_cache()
        with self._video_tools_lock:
            if video_path not in self._video_tools:
                logger.info(f"🔄 Initializing ScreenshotSelector for: {video_path}")
                extractor = VisualFeatureExtractor(video_path)
                selector = ScreenshotSelector(visual_extractor=extractor, config=self.config)
                self._video_tools[video_path] = {
                    "extractor": extractor,
                    "selector": selector
                }
            return self._video_tools[video_path]["selector"]

    def cleanup_video_tools(self):
        """
        执行逻辑：
        1) 清理全部缓存的 screenshot selector / extractor。
        2) 主动释放其中 extractor 持有的 SHM 与视频句柄，避免长期驻留。
        """
        self._init_video_tools_cache()
        with self._video_tools_lock:
            entries = list(self._video_tools.values())
            self._video_tools.clear()
        for entry in entries:
            extractor = entry.get("extractor") if isinstance(entry, dict) else None
            if extractor is not None and hasattr(extractor, "cleanup"):
                try:
                    extractor.cleanup()
                except Exception as e:
                    logger.warning(f"Failed to cleanup screenshot extractor: {e}")

    def get_cv_validator(self, video_path: str):
        """
        执行逻辑：
        1) 检查视频级 CV 验证器缓存。
        2) 缺失时创建 CVKnowledgeValidator 并写入缓存。
        实现方式：缓存字典 + 惰性初始化。
        核心价值：避免重复打开视频与重复初始化 CV 模型。
        决策逻辑：
        - 条件：video_path not in self._cv_validators
        依据来源（证据链）：
        - 输入参数：video_path。
        - 对象内部状态：self._cv_validators。
        输入参数：
        - video_path: 文件路径（类型：str）。
        输出参数：
        - CVKnowledgeValidator 实例。"""
        with self._cv_validator_lock:
            if video_path not in self._cv_validators:
                logger.info(f"🔄 Creating CVKnowledgeValidator for: {video_path}")
                try:
                    from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator
                    self._cv_validators[video_path] = CVKnowledgeValidator(video_path)
                    logger.info(f"✅ CVKnowledgeValidator created for: {video_path}")
                except Exception as e:
                    logger.error(f"❌ Failed to create CVKnowledgeValidator: {e}")
                    raise
            return self._cv_validators[video_path]
    
    def cleanup_cv_validators(self):
        """
        执行逻辑：
        1) 遍历并清理所有缓存的 CV 验证器。
        2) 清空缓存字典释放引用。
        实现方式：循环调用 cleanup + clear。
        核心价值：释放视频级资源，避免内存泄漏。
        决策逻辑：
        - 条件：hasattr(validator, 'cleanup')
        依据来源（证据链）：
        - 验证器对象：是否存在 cleanup 方法。
        输入参数：
        - 无。
        输出参数：
        - 无（仅更新内部缓存与资源状态）。"""
        with self._cv_validator_lock:
            for video_path, validator in self._cv_validators.items():
                try:
                    if hasattr(validator, 'cleanup'):
                        validator.cleanup()
                except Exception as e:
                    logger.warning(f"Failed to cleanup validator for {video_path}: {e}")
            self._cv_validators.clear()
            logger.info("🧹 All CV validators cleaned up")


class Stage1HeartbeatWriter:
    """Stage1 结构化心跳写盘器。"""

    STEP_INDEX = {
        "step1_validate": 1,
        "step2_correction": 2,
        "step3_merge": 3,
        "step3_5_translate": 4,
        "step4_clean_local": 5,
        "step5_6_dedup_merge": 6,
        "step6_merge_cross": 6,
    }

    def __init__(self, task_id: str, output_dir: str, max_step: int) -> None:
        self._task_id = str(task_id or "")
        self._output_dir = str(output_dir or "")
        self._max_step = max(1, int(max_step or 1))
        self._seq = 0
        self._lock = threading.Lock()
        self._path = os.path.join(self._output_dir, "intermediates", "stage1_watchdog_heartbeat.json")
        os.makedirs(os.path.dirname(self._path), exist_ok=True)

    @property
    def path(self) -> str:
        return self._path

    def _step_to_completed(self, step_name: str) -> int:
        raw = str(step_name or "").strip()
        if raw.isdigit():
            return max(0, min(int(raw), self._max_step))
        value = self.STEP_INDEX.get(raw, 0)
        return max(0, min(int(value), self._max_step))

    def emit(
        self,
        *,
        status: str,
        checkpoint: str,
        completed: int,
        pending: Optional[int] = None,
        signal_type: str = "hard",
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        safe_completed = max(0, min(int(completed), self._max_step))
        safe_pending = (
            max(0, int(pending))
            if pending is not None
            else max(0, self._max_step - safe_completed)
        )
        safe_pending = max(0, min(safe_pending, self._max_step))
        safe_status = str(status or "running").strip().lower() or "running"
        safe_checkpoint = str(checkpoint or "unknown").strip() or "unknown"
        safe_signal_type = str(signal_type or "hard").strip().lower() or "hard"
        if safe_signal_type not in {"hard", "soft"}:
            safe_signal_type = "hard"
        payload: Dict[str, Any] = {
            "schema": "stage_watchdog.v1",
            "source": "python_stage1_heartbeat",
            "stage": "stage1",
            "task_id": self._task_id,
            "status": safe_status,
            "completed": safe_completed,
            "pending": safe_pending,
            "checkpoint": safe_checkpoint,
            "signal_type": safe_signal_type,
            "updated_at_ms": int(time.time() * 1000),
        }
        if isinstance(extra, dict):
            for key, value in extra.items():
                if key not in payload and isinstance(value, (str, int, float, bool)):
                    payload[key] = value
        with self._lock:
            self._seq += 1
            payload["seq"] = self._seq
            published_payload = publish_watchdog_signal(payload)
            if isinstance(published_payload, dict):
                try:
                    payload["stream_seq"] = max(0, int(published_payload.get("stream_seq", 0)))
                except Exception:
                    payload["stream_seq"] = 0
            tmp_path = f"{self._path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as file:
                json.dump(payload, file, ensure_ascii=False)
            os.replace(tmp_path, self._path)

    def emit_from_event(self, event: Dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        checkpoint = str(
            event.get("checkpoint")
            or event.get("step_name")
            or event.get("event")
            or "unknown"
        ).strip()
        status = str(event.get("status") or "running").strip().lower() or "running"
        completed_raw = event.get("completed", 0)
        pending_raw = event.get("pending")
        try:
            completed = int(completed_raw)
        except Exception:
            completed = self._step_to_completed(str(completed_raw))
        pending: Optional[int]
        try:
            pending = int(pending_raw) if pending_raw is not None else None
        except Exception:
            pending = None
        signal_type = str(event.get("signal_type") or "hard").strip().lower() or "hard"
        self.emit(
            status=status,
            checkpoint=checkpoint,
            completed=completed,
            pending=pending,
            signal_type=signal_type,
        )


class _VideoProcessingServicerCore(video_processing_pb2_grpc.VideoProcessingServiceServicer):
    """
    类说明：gRPC 服务实现，承载视频处理各阶段的编排与调度。
    执行逻辑：
    1) 初始化全局资源与并行执行环境（ProcessPool + SharedMemory）。
    2) 按 RPC 请求触发下载、转写、Stage1/Phase2A/2B、CV 验证等流程。
    3) 统一输出路径与缓存策略，保证跨阶段复用。
    实现方式：grpc.aio + RichTextPipeline + GlobalResourceManager。
    核心价值：把 Java 编排请求稳定映射到 Python 处理链路。"""
    
    def __init__(self, config: dict = None):
        """
        执行逻辑：
        1) 保存配置并初始化 GlobalResourceManager。
        2) 初始化任务计数与并发控制。
        3) 根据 CPU/内存评估并设置 CV ProcessPool 大小。
        4) 创建 ProcessPool 与 SharedFrameRegistry。
        实现方式：psutil 评估资源 + ProcessPoolExecutor。
        核心价值：保证 CV 并发在可用资源范围内稳定运行。
        决策逻辑：
        - 条件：hasattr(mem, 'transferable')（Windows 下优先使用 transferable）
        - 条件：cv_worker_count 由 CPU 核数与内存阈值共同限制
        依据来源（证据链）：
        - 运行指标：psutil.virtual_memory().available/transferable。
        - 运行指标：multiprocessing.cpu_count()。
        输入参数：
        - config: 配置对象/字典（类型：dict）。
        输出参数：
        - 无（仅更新内部状态与进程池）。"""
        self.config = config or {}
        self.resume_control = _load_resume_control_from_configs()
        
        # 🔑 使用全局资源管理器
        self.resources = GlobalResourceManager()
        self.resources.initialize(config)
        
        # 活跃任务计数
        self._active_tasks = 0
        self._task_lock = threading.Lock()
        self._cache_metrics_task_id = None
        self._resume_report_lock = threading.Lock()
        self._stage1_runtime_cache_lock = threading.Lock()
        # Stage1 运行态缓存：按 output_dir 保留到任务完成，不做 TTL/容量淘汰。
        self._stage1_runtime_cache: Dict[str, Dict[str, Any]] = {}
        self._phase2a_runtime_cache_lock = threading.Lock()
        # Phase2A 运行态缓存：保存语义单元序列化结果，供 AnalyzeWithVL 同进程内存直传。
        self._phase2a_runtime_cache: Dict[str, Dict[str, Any]] = {}
        # Phase2A 引用缓存：ref_id -> cache_entry，用于 Java/Python 跨 RPC 无路径传递。
        self._phase2a_ref_cache: Dict[str, Dict[str, Any]] = {}

        # LLM 分类并发探测器：AIMD 逐步加压，遇到失败回退
        self._classify_concurrency_limiter = AdaptiveConcurrencyLimiter(
            initial_limit=10,
            min_limit=2,
            max_limit=300
        )
        
        # 🚀 V6: Java 控制并发 + Python ProcessPool + SharedMemory
        # - Java 控制发送多少并行请求 (熔断/重试)
        # - Python 使用 ProcessPool 绕过 GIL
        # - SharedFrameRegistry 实现帧共享
        import multiprocessing
        
        # 🚀 释放物理算力: 设为 CPU 核心数
        # 实际负载由 Java 端 Semaphore 精确控制，Python 端只提供最大能力底座
        # [FIX] 动态计算 Worker 数量防止 OOM
        # 策略: 至少保留 4GB 给系统，剩余内存每 3GB 允许一个 Worker (Windows Spawn 模式开销大)
        mem = psutil.virtual_memory()
        available_ram_gb = mem.transferable if hasattr(mem, 'transferable') else mem.available / (1024**3)
        # 保底/硬上限改为可配置，避免不同机器用同一组硬编码参数。
        reserved_ram_gb = max(0.0, _to_float(os.getenv("MODULE2_CV_WORKER_RESERVED_RAM_GB", "4"), 4.0))
        ram_per_worker_gb = max(0.2, _to_float(os.getenv("MODULE2_CV_WORKER_RAM_PER_GB", "1.5"), 1.5))
        worker_min = max(1, _to_int(os.getenv("MODULE2_CV_WORKER_MIN", "1"), 1))
        hard_cap = max(1, _to_int(os.getenv("MODULE2_CV_WORKER_HARD_CAP", "6"), 6))
        # 保底 1 个，默认上限 6 个（可通过环境变量提升）
        cpu_cores = multiprocessing.cpu_count()
        estimated_by_ram = int((available_ram_gb - reserved_ram_gb) / ram_per_worker_gb)
        max_workers_by_ram = max(worker_min, estimated_by_ram)
        self.cv_worker_count = min(max(worker_min, cpu_cores - 1), max_workers_by_ram, hard_cap)
        logger.info(
            f"🚀 CV ProcessPool Config: {self.cv_worker_count} workers "
            f"(Limit by RAM: {max_workers_by_ram}, CPU: {cpu_cores}, HardCap: {hard_cap}, "
            f"reserve_ram_gb={reserved_ram_gb}, ram_per_worker_gb={ram_per_worker_gb})"
        )

        
        # 创建 ProcessPool (使用 spawn 方式确保 Windows 兼容)
        from services.python_grpc.src.common.utils.process_pool import create_spawn_process_pool
        from services.python_grpc.src.vision_validation.worker import init_cv_worker
        self.cv_process_pool = create_spawn_process_pool(
            max_workers=self.cv_worker_count,
            initializer=init_cv_worker
        )
        
        # SharedFrameRegistry 用于主进程预读帧
        self.frame_registry = get_shared_frame_registry()
        
        logger.info(f"🚀 CV ProcessPool created: {self.cv_worker_count} workers + SharedMemory")
        logger.info("VideoProcessingServicer initialized (Java controls concurrency)")
        logger.info(
            "Resume control: enabled=%s mode=%s validation=%s retention_days=%s groups=%s",
            self.resume_control.enabled,
            self.resume_control.mode,
            self.resume_control.validation,
            self.resume_control.non_priority_retention_days,
            self.resume_control.groups,
        )

    def _clear_stage1_runtime_cache(self, output_dir: str) -> None:
        """任务完成后清理指定 output_dir 的 Stage1 运行态缓存。"""
        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        if not normalized_output_dir:
            return
        with self._stage1_runtime_cache_lock:
            removed = self._stage1_runtime_cache.pop(normalized_output_dir, None)
        if removed is not None:
            logger.info("Stage1 runtime cache cleared: output_dir=%s", normalized_output_dir)

    def _cache_stage1_runtime_outputs(self, output_dir: str, final_state: Optional[Dict[str, Any]]) -> None:
        """缓存 Stage1 关键产物到内存，供非复用链路直接透传。"""
        if not isinstance(final_state, dict):
            return

        corrected_subtitles = final_state.get("corrected_subtitles", [])
        pure_text_script = final_state.get("pure_text_script", [])
        if not isinstance(corrected_subtitles, list):
            corrected_subtitles = []
        if not isinstance(pure_text_script, list):
            pure_text_script = []
        if not corrected_subtitles and not pure_text_script:
            return

        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        if not normalized_output_dir:
            return

        cache_entry = {
            "step2_subtitles": copy.deepcopy(corrected_subtitles),
            "step6_paragraphs": copy.deepcopy(pure_text_script),
        }
        with self._stage1_runtime_cache_lock:
            self._stage1_runtime_cache[normalized_output_dir] = cache_entry

        logger.info(
            "Stage1 runtime cache updated (retain-until-task-complete): output_dir=%s, step2_items=%s, step6_paragraphs=%s",
            normalized_output_dir,
            len(corrected_subtitles),
            len(pure_text_script),
        )

    def _get_stage1_runtime_outputs(self, output_dir: str) -> Optional[Dict[str, Any]]:
        """读取 Stage1 进程内缓存命中，未命中返回 None。"""
        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        if not normalized_output_dir:
            return None

        with self._stage1_runtime_cache_lock:
            entry = self._stage1_runtime_cache.get(normalized_output_dir)
            if entry is None:
                return None
            return {
                "step2_subtitles": copy.deepcopy(entry.get("step2_subtitles", [])),
                "step6_paragraphs": copy.deepcopy(entry.get("step6_paragraphs", [])),
            }

    def _clear_phase2a_runtime_cache(self, output_dir: str) -> None:
        """任务完成后清理指定 output_dir 的 Phase2A 运行态缓存。"""
        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        if not normalized_output_dir:
            return
        with self._phase2a_runtime_cache_lock:
            removed = self._phase2a_runtime_cache.pop(normalized_output_dir, None)
            if isinstance(removed, dict):
                removed_ref_id = str(removed.get("ref_id", "")).strip()
                if removed_ref_id:
                    self._phase2a_ref_cache.pop(removed_ref_id, None)
        if removed is not None:
            logger.info("Phase2A runtime cache cleared: output_dir=%s", normalized_output_dir)

    def _cache_phase2a_runtime_semantic_units(
        self,
        output_dir: str,
        semantic_units_path: str,
        semantic_units: Optional[List[Dict[str, Any]]],
        task_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        """缓存 Phase2A 语义单元序列化结果，避免 AnalyzeWithVL 强依赖落盘时序。"""
        if not isinstance(semantic_units, list):
            return None

        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        if not normalized_output_dir:
            return None

        normalized_path = os.path.abspath(str(semantic_units_path or "").strip()) if semantic_units_path else ""
        canonical_bytes = json.dumps(
            semantic_units,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        fingerprint = hashlib.sha256(canonical_bytes).hexdigest()
        compressed_bytes = gzip.compress(canonical_bytes)
        if len(compressed_bytes) < len(canonical_bytes):
            inline_payload = compressed_bytes
            inline_codec = "json-utf8-gzip"
        else:
            inline_payload = canonical_bytes
            inline_codec = "json-utf8"
        inline_sha256 = hashlib.sha256(inline_payload).hexdigest()

        ref_id = f"{(task_id or 'phase2a')}_{uuid.uuid4().hex}"
        cache_entry = {
            "semantic_units_path": normalized_path,
            "semantic_units": copy.deepcopy(semantic_units),
            "output_dir": normalized_output_dir,
            "task_id": str(task_id or "").strip(),
            "ref_id": ref_id,
            "unit_count": len(semantic_units),
            "schema_version": "phase2a.v1",
            "fingerprint": fingerprint,
            "inline_payload": inline_payload,
            "inline_codec": inline_codec,
            "inline_sha256": inline_sha256,
        }
        with self._phase2a_runtime_cache_lock:
            previous = self._phase2a_runtime_cache.get(normalized_output_dir)
            if isinstance(previous, dict):
                previous_ref_id = str(previous.get("ref_id", "")).strip()
                if previous_ref_id:
                    self._phase2a_ref_cache.pop(previous_ref_id, None)
            self._phase2a_runtime_cache[normalized_output_dir] = cache_entry
            self._phase2a_ref_cache[ref_id] = cache_entry

        logger.info(
            "Phase2A runtime cache updated: output_dir=%s, semantic_units=%s, path=%s, ref_id=%s",
            normalized_output_dir,
            len(semantic_units),
            normalized_path,
            ref_id,
        )
        return {
            "semantic_units_path": normalized_path,
            "output_dir": normalized_output_dir,
            "task_id": str(task_id or "").strip(),
            "ref_id": ref_id,
            "unit_count": len(semantic_units),
            "schema_version": "phase2a.v1",
            "fingerprint": fingerprint,
            "inline_payload": inline_payload,
            "inline_codec": inline_codec,
            "inline_sha256": inline_sha256,
        }

    def _get_phase2a_runtime_semantic_units(
        self,
        output_dir: str,
        semantic_units_path: str = "",
        deep_copy: bool = True,
    ) -> Optional[List[Dict[str, Any]]]:
        """读取 Phase2A 语义单元运行态缓存；未命中返回 None。"""
        normalized_output_dir = os.path.abspath(str(output_dir or "").strip())
        if not normalized_output_dir:
            return None

        with self._phase2a_runtime_cache_lock:
            entry = self._phase2a_runtime_cache.get(normalized_output_dir)
            if entry is None:
                return None
            cached_units = entry.get("semantic_units")
            cached_path = str(entry.get("semantic_units_path", "")).strip()

        if not isinstance(cached_units, list):
            return None

        normalized_requested_path = os.path.abspath(str(semantic_units_path or "").strip()) if semantic_units_path else ""
        if normalized_requested_path and cached_path and os.path.normcase(normalized_requested_path) != os.path.normcase(cached_path):
            logger.info(
                "Phase2A runtime cache path mismatch tolerated: output_dir=%s, request=%s, cached=%s",
                normalized_output_dir,
                    normalized_requested_path,
                    cached_path,
            )
        if deep_copy:
            return copy.deepcopy(cached_units)
        return cached_units

    def _get_phase2a_runtime_cache_entry_by_ref(
        self,
        ref_id: str,
        deep_copy: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """按 ref_id 读取 Phase2A 缓存条目；未命中返回 None。"""
        normalized_ref_id = str(ref_id or "").strip()
        if not normalized_ref_id:
            return None
        with self._phase2a_runtime_cache_lock:
            entry = self._phase2a_ref_cache.get(normalized_ref_id)
            if entry is None:
                return None
            if deep_copy:
                return copy.deepcopy(entry)
            return entry

    def _iter_semantic_unit_nodes(self, data: Any) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
        """遍历语义单元节点并附带分组元信息。"""
        return shared_iter_semantic_unit_nodes(data)

    def _build_semantic_unit_index(self, data: Any) -> Dict[str, Dict[str, Any]]:
        """为语义单元 payload 建立 `unit_id -> unit_node` 索引。"""
        return shared_build_semantic_unit_index(data)

    def _normalize_semantic_units_payload(self, data: Any) -> List[Dict[str, Any]]:
        """规范化语义单元载荷，统一返回扁平 List[Dict]。"""
        return shared_normalize_semantic_units_payload(data)

    def _build_grouped_semantic_units_payload(
        self,
        semantic_units: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """将扁平语义单元重建为 `knowledge_groups` 结构。"""
        return shared_build_grouped_semantic_units_payload(
            semantic_units,
            schema_version="phase2a.grouped.v1",
            default_group_reason="同一核心论点聚合",
            strip_unit_group_fields=True,
        )

    def _load_semantic_units_from_json_path(self, json_path: str) -> List[Dict[str, Any]]:
        """从 JSON 文件加载语义单元并做结构规范化。"""
        normalized_path = os.path.abspath(str(json_path or "").strip())
        if not normalized_path:
            return []
        with open(normalized_path, "r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)
        return self._normalize_semantic_units_payload(data)

    def _build_semantic_units_inline_message(
        self,
        semantic_units: Optional[List[Dict[str, Any]]],
        cache_entry: Optional[Dict[str, Any]] = None,
    ) -> video_processing_pb2.SemanticUnitsInline:
        """将语义单元编码为 inline protobuf（优先 gzip 以降低传输体积）。"""
        inline_msg = video_processing_pb2.SemanticUnitsInline()
        if isinstance(cache_entry, dict):
            cached_payload = bytes(cache_entry.get("inline_payload") or b"")
            if cached_payload:
                inline_msg.payload = cached_payload
                inline_msg.codec = str(cache_entry.get("inline_codec", "") or "json-utf8")
                inline_msg.unit_count = int(cache_entry.get("unit_count", 0) or 0)
                inline_msg.sha256 = str(cache_entry.get("inline_sha256", "") or hashlib.sha256(cached_payload).hexdigest())
                return inline_msg

        if not isinstance(semantic_units, list):
            return inline_msg

        raw_bytes = json.dumps(
            semantic_units,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        compressed_bytes = gzip.compress(raw_bytes)
        if len(compressed_bytes) < len(raw_bytes):
            payload = compressed_bytes
            codec = "json-utf8-gzip"
        else:
            payload = raw_bytes
            codec = "json-utf8"
        inline_msg.payload = payload
        inline_msg.codec = codec
        inline_msg.unit_count = len(semantic_units)
        inline_msg.sha256 = hashlib.sha256(payload).hexdigest()
        return inline_msg

    def _decode_semantic_units_inline_message(
        self,
        inline_msg: video_processing_pb2.SemanticUnitsInline,
    ) -> List[Dict[str, Any]]:
        """解析 inline protobuf，返回规范化语义单元列表。"""
        payload = bytes(getattr(inline_msg, "payload", b"") or b"")
        if not payload:
            return []

        codec = str(getattr(inline_msg, "codec", "") or "").strip().lower()
        if codec in {"json-utf8-gzip", "gzip"}:
            decoded_bytes = gzip.decompress(payload)
        elif codec in {"json-utf8", "json"} or not codec:
            decoded_bytes = payload
        else:
            raise ValueError(f"unsupported semantic_units_inline codec: {codec}")

        payload_text = decoded_bytes.decode("utf-8")
        parsed = json.loads(payload_text)
        return self._normalize_semantic_units_payload(parsed)

    def _collect_semantic_unit_quality_metrics(
        self,
        semantic_units: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, int]:
        metrics = {
            "unit_count": 0,
            "keyframe_unit_count": 0,
            "keyframe_placeholder_count": 0,
            "vl_segment_count": 0,
            "instructional_step_count": 0,
            "screenshot_item_count": 0,
            "text_chars": 0,
        }
        if not isinstance(semantic_units, list):
            return metrics

        keyframe_pattern = re.compile(r"\[\s*KEYFRAME_\d+\s*\]", flags=re.IGNORECASE)
        for unit in semantic_units:
            if not isinstance(unit, dict):
                continue
            metrics["unit_count"] += 1

            text_candidates = [
                unit.get("full_text"),
                unit.get("main_content"),
                unit.get("body_text"),
                unit.get("enhanced_body"),
                unit.get("original_body"),
            ]
            unit_text = "\n".join(
                [str(candidate).strip() for candidate in text_candidates if str(candidate or "").strip()]
            )
            if unit_text:
                metrics["text_chars"] += len(unit_text)

            keyframe_hits = len(keyframe_pattern.findall(unit_text))
            metrics["keyframe_placeholder_count"] += keyframe_hits
            if keyframe_hits > 0:
                metrics["keyframe_unit_count"] += 1

            vl_segments = unit.get("_vl_concrete_segments")
            if isinstance(vl_segments, list):
                metrics["vl_segment_count"] += len(vl_segments)

            instructional_steps = unit.get("instructional_steps")
            if isinstance(instructional_steps, list):
                metrics["instructional_step_count"] += len(instructional_steps)

            materials = unit.get("materials")
            if isinstance(materials, dict):
                screenshot_items = materials.get("screenshot_items")
                if isinstance(screenshot_items, list):
                    metrics["screenshot_item_count"] += len(screenshot_items)
        return metrics

    def _should_prefer_runtime_semantic_units(
        self,
        current_payload: Optional[List[Dict[str, Any]]],
        runtime_payload: Optional[List[Dict[str, Any]]],
    ) -> bool:
        if not isinstance(runtime_payload, list) or not runtime_payload:
            return False
        if not isinstance(current_payload, list) or not current_payload:
            return True

        current_metrics = self._collect_semantic_unit_quality_metrics(current_payload)
        runtime_metrics = self._collect_semantic_unit_quality_metrics(runtime_payload)

        current_score = (
            current_metrics["keyframe_placeholder_count"],
            current_metrics["keyframe_unit_count"],
            current_metrics["vl_segment_count"],
            current_metrics["instructional_step_count"],
            current_metrics["screenshot_item_count"],
            current_metrics["text_chars"],
            current_metrics["unit_count"],
        )
        runtime_score = (
            runtime_metrics["keyframe_placeholder_count"],
            runtime_metrics["keyframe_unit_count"],
            runtime_metrics["vl_segment_count"],
            runtime_metrics["instructional_step_count"],
            runtime_metrics["screenshot_item_count"],
            runtime_metrics["text_chars"],
            runtime_metrics["unit_count"],
        )
        return runtime_score > current_score

    def _materialize_semantic_units_payload(
        self,
        output_dir: str,
        task_id: str,
        semantic_units: List[Dict[str, Any]],
    ) -> str:
        """
        将内存语义单元落盘到 intermediates，供 Phase2B assemble_only 复用既有文件装配链路。
        为什么：在不改动 RichTextPipeline 输入契约的前提下，消除 Java->Python 路径传递依赖。
        """
        intermediates_dir = os.path.join(output_dir, "intermediates")
        os.makedirs(intermediates_dir, exist_ok=True)
        grouped_payload = self._build_grouped_semantic_units_payload(semantic_units)
        suffix = hashlib.sha256(json.dumps(grouped_payload, ensure_ascii=False, default=str).encode("utf-8")).hexdigest()[:12]
        task_tag = str(task_id or "unknown")
        materialized_path = os.path.join(intermediates_dir, f"semantic_units_from_rpc_{task_tag}_{suffix}.json")
        with open(materialized_path, "w", encoding="utf-8") as file_obj:
            json.dump(grouped_payload, file_obj, ensure_ascii=False, indent=2)
        return materialized_path



    
    async def HealthCheck(self, request, context):
        """
        执行逻辑：
        1) 读取当前 CPU/内存使用情况。
        2) 组装 ServerStatus 并返回健康响应。
        实现方式：psutil 系统指标采集。
        核心价值：为 Java 编排侧提供可观测性信号。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - HealthCheckResponse（含健康状态、版本、活跃任务数与资源占用）。"""
        # 获取系统状态
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        
        status = video_processing_pb2.ServerStatus(
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            available_memory_mb=memory.available // (1024 * 1024),
            gpu_available=False  # TODO: 检测GPU
        )

        return video_processing_pb2.HealthCheckResponse(
            healthy=True,
            version="2.0.0",
            active_tasks=self._active_tasks,
            status=status
        )

    async def StreamTaskWatchdogSignals(self, request, context):
        task_id = str(getattr(request, "task_id", "") or "").strip()
        if not task_id:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "task_id is required")

        stage_filter = str(getattr(request, "stage", "") or "").strip().lower()
        from_stream_seq_raw = getattr(request, "from_stream_seq", 0)
        idle_timeout_raw = getattr(request, "idle_timeout_sec", 0)
        try:
            cursor = max(0, int(from_stream_seq_raw))
        except Exception:
            cursor = 0
        try:
            idle_timeout_sec = max(5, int(idle_timeout_raw))
        except Exception:
            idle_timeout_sec = 30

        def _safe_int(value: Any, fallback: int = 0) -> int:
            try:
                return max(0, int(value))
            except Exception:
                return max(0, int(fallback))

        last_event_at = time.monotonic()
        while True:
            events = read_watchdog_signals(
                task_id=task_id,
                from_stream_seq=cursor,
                stage=stage_filter or None,
                limit=128,
            )
            if events:
                for event in events:
                    stream_seq = _safe_int(event.get("stream_seq", 0), 0)
                    cursor = max(cursor, stream_seq)
                    status = str(event.get("status") or "running").strip().lower() or "running"
                    signal_type = str(event.get("signal_type") or "hard").strip().lower() or "hard"
                    if signal_type not in {"hard", "soft"}:
                        signal_type = "hard"
                    payload = video_processing_pb2.WatchdogSignalEvent(
                        schema=str(event.get("schema") or "task_watchdog.v1"),
                        source=str(event.get("source") or "python_watchdog"),
                        task_id=str(event.get("task_id") or task_id),
                        stage=str(event.get("stage") or "unknown"),
                        status=status,
                        checkpoint=str(event.get("checkpoint") or "unknown"),
                        completed=_safe_int(event.get("completed", 0), 0),
                        pending=_safe_int(event.get("pending", 0), 0),
                        seq=_safe_int(event.get("seq", 0), 0),
                        stream_seq=stream_seq,
                        updated_at_ms=_safe_int(event.get("updated_at_ms", 0), 0),
                        signal_type=signal_type,
                    )
                    yield payload
                    last_event_at = time.monotonic()
                    if stage_filter and status in {"completed", "failed"}:
                        return
                continue

            if time.monotonic() - last_event_at >= idle_timeout_sec:
                return

            await asyncio.sleep(0.5)

    async def DownloadVideo(self, request, context):
        """
        执行逻辑：
        1) 解析输入链接并按平台规则生成统一 task 目录。
        2) 委托 download_service 执行下载与元数据落盘，保持 grpc 层精简。
        3) 返回增强后的 DownloadResponse，供 Java 编排与 sidecar 复用。
        """
        task_id = request.task_id
        self._cache_metrics_begin(task_id, "DownloadVideo")
        raw_video_input = str(request.video_url or "")
        predicted_output_dir = ""
        try:
            task_source = _build_task_dir_encoding_source(raw_video_input)
            task_hash = hashlib.md5(task_source.encode("utf-8")).hexdigest()
            predicted_output_dir = os.path.join(_get_primary_storage_root(), task_hash)
        except Exception:
            predicted_output_dir = str(request.output_dir or "").strip()
        download_watchdog = None
        soft_heartbeat_stop = threading.Event()
        soft_heartbeat_thread: Optional[threading.Thread] = None
        if predicted_output_dir:
            try:
                download_watchdog = TaskWatchdogSignalWriter(
                    task_id=task_id,
                    output_dir=predicted_output_dir,
                    stage="download",
                    total_steps=3,
                )
                download_watchdog.emit(
                    status="running",
                    checkpoint="download_prepare",
                    completed=0,
                    pending=3,
                    signal_type="hard",
                )
            except Exception as watchdog_error:
                logger.warning(f"[{task_id}] Download watchdog init failed: {watchdog_error}")

        logger.info(f"[{task_id}] DownloadVideo: {raw_video_input}")

        def _emit_download_hard_heartbeat_loop() -> None:
            """在 aria2c/yt-dlp 下载期间周期性发送 hard 心跳，防止 Java Watchdog idle 判断触发下载重启。
            说明：Java watchdog 的 idle 窗口（默认 180s）只计 hard 信号间隔；soft 信号不重置 idle 计数器。
            因此必须发 hard 信号，间隔需小于 idle-window-min-seconds（默认 180s）。
            """
            interval_sec = max(
                15.0,
                float(_to_int(os.getenv("DOWNLOAD_HARD_HEARTBEAT_SEC", 30), 30)),
            )
            watch_root = predicted_output_dir or ""
            last_bytes = 0
            seq_counter = 2  # flow_start 已占 seq=2
            while not soft_heartbeat_stop.wait(interval_sec):
                if download_watchdog is None:
                    continue
                seq_counter += 1
                # 扫描目录获取已下载字节数（作为进展证明附在 checkpoint 里）
                total_bytes = last_bytes
                if watch_root:
                    try:
                        total = 0
                        for _root, _dirs, _files in os.walk(watch_root):
                            for _f in _files:
                                try:
                                    total += os.path.getsize(os.path.join(_root, _f))
                                except OSError:
                                    pass
                        total_bytes = total
                        last_bytes = total
                    except Exception:
                        pass
                checkpoint_label = f"download_in_progress|bytes={total_bytes}"
                try:
                    download_watchdog.emit(
                        status="running",
                        checkpoint=checkpoint_label,
                        completed=1,
                        pending=2,
                        signal_type="hard",
                    )
                except Exception as heartbeat_error:
                    logger.warning(f"[{task_id}] Download hard heartbeat emit failed: {heartbeat_error}")

        try:
            self._increment_tasks()
            if download_watchdog is not None:
                download_watchdog.emit(
                    status="running",
                    checkpoint="download_flow_start",
                    completed=1,
                    pending=2,
                    signal_type="hard",
                )
                soft_heartbeat_thread = threading.Thread(
                    target=_emit_download_hard_heartbeat_loop,
                    name=f"download-heartbeat-{task_id}",
                    daemon=True,
                )
                soft_heartbeat_thread.start()
            flow_result = await run_download_flow(
                task_id=task_id,
                raw_video_input=raw_video_input,
                config=self.config,
                resolve_share_link=resolve_share_link,
                build_task_dir_encoding_source=_build_task_dir_encoding_source,
                get_primary_storage_root=_get_primary_storage_root,
                is_douyin_url=_is_douyin_url,
                douyin_downloader=_download_video_with_douyin_downloader,
                load_download_video_options=_load_download_video_options,
                video_processor_cls=VideoProcessor,
                get_video_duration=self._get_video_duration,
                write_video_meta_file=_write_video_meta_file,
                logger=logger,
            )
            soft_heartbeat_stop.set()

            if download_watchdog is not None:
                if flow_result.success:
                    download_watchdog.emit(
                        status="completed",
                        checkpoint="download_response_ready",
                        completed=3,
                        pending=0,
                        signal_type="hard",
                        extra={"content_type": str(flow_result.content_type or "unknown")},
                    )
                else:
                    download_watchdog.emit(
                        status="failed",
                        checkpoint="download_failed",
                        completed=1,
                        pending=2,
                        signal_type="hard",
                        extra={"error": str(flow_result.error_msg or "")[:200]},
                    )

            return video_processing_pb2.DownloadResponse(
                success=flow_result.success,
                video_path=flow_result.video_path,
                file_size_bytes=flow_result.file_size_bytes,
                duration_sec=flow_result.duration_sec,
                error_msg=flow_result.error_msg,
                resolved_url=flow_result.resolved_url,
                video_title=flow_result.video_title,
                source_platform=flow_result.source_platform,
                canonical_id=flow_result.canonical_id,
                link_resolver=flow_result.link_resolver,
                content_type=flow_result.content_type,
            )
        except Exception as e:
            soft_heartbeat_stop.set()
            logger.error(f"[{task_id}] DownloadVideo failed: {e}")
            if download_watchdog is not None:
                try:
                    download_watchdog.emit(
                        status="failed",
                        checkpoint="download_exception",
                        completed=1,
                        pending=2,
                        signal_type="hard",
                        extra={"error": str(e)[:200]},
                    )
                except Exception as watchdog_error:
                    logger.warning(f"[{task_id}] Download watchdog emit failed: {watchdog_error}")
            return video_processing_pb2.DownloadResponse(
                success=False,
                video_path="",
                file_size_bytes=0,
                duration_sec=0.0,
                error_msg=str(e),
                resolved_url="",
                video_title="",
                source_platform="",
                canonical_id="",
                link_resolver="",
                content_type="unknown",
            )
        finally:
            soft_heartbeat_stop.set()
            self._decrement_tasks()


    async def GetVideoInfo(self, request, context):
        """
        执行逻辑：
        1) 解析分享文本/BV/URL，统一得到待探测链接与平台信息。
        2) 复用 VideoProcessor 仅探测元信息，不下载视频文件。
        3) 组装合集与分集结构，返回给 Java 侧 REST API。
        """
        task_id = str(request.task_id or f"video-info-{int(time.time() * 1000)}")
        self._cache_metrics_begin(task_id, "GetVideoInfo")
        raw_video_input = str(request.video_input or "").strip()

        if not raw_video_input:
            return video_processing_pb2.VideoInfoResponse(
                success=False,
                error_msg="video_input cannot be empty",
                raw_input="",
                resolved_url="",
                source_platform="unknown",
                canonical_id="",
                video_title="",
                duration_sec=0.0,
                is_collection=False,
                total_episodes=0,
                current_episode_index=0,
                current_episode_title="",
                episodes=[],
                link_resolver="",
                content_type="unknown",
                cover_url="",
            )

        logger.info(f"[{task_id}] GetVideoInfo: {raw_video_input}")

        normalized_input = _normalize_video_probe_input(raw_video_input)
        requested_episode_index = _extract_episode_index_from_url(_extract_first_http_url(raw_video_input))

        resolved_url = _extract_first_http_url(normalized_input)
        if not resolved_url and _is_http_url(normalized_input):
            resolved_url = normalized_input

        extracted_url = _extract_first_http_url(normalized_input)
        source_platform = _detect_source_platform(resolved_url)
        canonical_id = ""
        if source_platform == "bilibili":
            canonical_id = _extract_bilibili_video_id(resolved_url) or ""
        elif source_platform == "douyin":
            canonical_id = _extract_douyin_canonical_id(resolved_url)
        link_resolver = "fallback-input"
        content_type = _infer_video_info_content_type(source_platform, resolved_url)

        probe_url = _first_non_blank(extracted_url, resolved_url)

        try:
            self._increment_tasks()

            try:
                resolved_share = await resolve_share_link(normalized_input)
                extracted_url = _first_non_blank(getattr(resolved_share, "extracted_url", ""), extracted_url)
                resolved_url = _first_non_blank(getattr(resolved_share, "resolved_url", ""), resolved_url)
                source_platform = _first_non_blank(getattr(resolved_share, "platform", ""), source_platform)
                canonical_id = _first_non_blank(getattr(resolved_share, "canonical_id", ""), canonical_id)
                link_resolver = _first_non_blank(getattr(resolved_share, "resolver", ""), link_resolver)
                content_type = _first_non_blank(
                    getattr(resolved_share, "content_type", ""),
                    _infer_video_info_content_type(source_platform, resolved_url),
                )
            except Exception as resolve_error:
                logger.warning(f"[{task_id}] GetVideoInfo resolve_share_link fallback: {resolve_error}")
                if not content_type:
                    content_type = _infer_video_info_content_type(source_platform, resolved_url)

            if source_platform == "bilibili":
                probe_url = _first_non_blank(extracted_url, resolved_url, probe_url)
            else:
                probe_url = _first_non_blank(resolved_url, extracted_url, probe_url)

            if requested_episode_index <= 0:
                requested_episode_index = _extract_episode_index_from_url(extracted_url)
            if requested_episode_index <= 0:
                requested_episode_index = _extract_episode_index_from_url(resolved_url)

            if not _is_http_url(probe_url):
                raise ValueError("invalid video input: cannot resolve HTTP URL")

            download_options = _load_download_video_options(self.config)
            if not _is_youtube_url(probe_url):
                download_options = dict(download_options)
                download_options["cookies_file"] = None
                download_options["cookies_from_browser"] = None
            info = None
            video_processor = None

            # 抖音优先走浏览器探测（yt-dlp 需要 cookies 易失败）
            if source_platform == "douyin":
                try:
                    logger.info(f"[{task_id}] GetVideoInfo: Douyin detected, using browser probe for {probe_url}")
                    info = await _probe_douyin_video_info(video_url=probe_url, timeout_ms=30000)
                except Exception as douyin_probe_err:
                    logger.warning(f"[{task_id}] GetVideoInfo: Douyin browser probe failed, falling back to yt-dlp: {douyin_probe_err}")

            # 非抖音或抖音浏览器探测失败时走 yt-dlp
            if not isinstance(info, dict) or not info:
                video_processor = VideoProcessor(**download_options)
                info = await asyncio.to_thread(video_processor.probe_video_info, probe_url)

            if not isinstance(info, dict) or not info:
                raise RuntimeError("video info probe returned empty payload")

            episodes = _build_episode_candidates(info)
            total_episodes = len(episodes)
            if total_episodes <= 0:
                total_episodes = max(
                    _safe_int(info.get("playlist_count"), 0),
                    _safe_int(info.get("n_entries"), 0),
                )
            if total_episodes <= 0:
                total_episodes = 1

            current_episode_index = _resolve_current_episode_index(
                requested_episode_index=requested_episode_index,
                info=info,
                episodes=episodes,
                total_episodes=total_episodes,
            )

            video_title = _normalize_video_title(
                info.get("title")
                or info.get("fulltitle")
                or getattr(video_processor, "last_video_title", "")
            )
            duration_sec = _safe_float(info.get("duration"), 0.0)
            current_episode_title = ""
            cover_url = _extract_cover_url(info)

            selected_episode: Optional[Dict[str, Any]] = None
            if episodes and current_episode_index > 0:
                for item in episodes:
                    if item.get("index") == current_episode_index:
                        selected_episode = item
                        break
                if selected_episode is None and 1 <= current_episode_index <= len(episodes):
                    selected_episode = episodes[current_episode_index - 1]

            if selected_episode is not None:
                current_episode_title = _normalize_video_title(selected_episode.get("title"))
                selected_duration = _safe_float(selected_episode.get("duration_sec"), 0.0)
                if selected_duration > 0:
                    duration_sec = selected_duration
                selected_cover_url = _first_non_blank(selected_episode.get("episode_cover_url"))
                if selected_cover_url:
                    cover_url = selected_cover_url

            if not video_title and episodes:
                if total_episodes == 1:
                    video_title = _normalize_video_title(episodes[0].get("title"))
                else:
                    video_title = _normalize_video_title(info.get("playlist_title") or "")

            if not current_episode_title:
                if total_episodes == 1 and episodes:
                    current_episode_title = _normalize_video_title(episodes[0].get("title"))
                else:
                    current_episode_title = video_title

            if current_episode_index <= 0 and total_episodes == 1:
                current_episode_index = 1

            if not canonical_id:
                if source_platform == "bilibili":
                    canonical_id = _first_non_blank(
                        _extract_bilibili_video_id(resolved_url) or "",
                        _extract_bilibili_video_id(probe_url) or "",
                    )
                elif source_platform == "douyin":
                    canonical_id = _first_non_blank(
                        _extract_douyin_canonical_id(resolved_url),
                        _extract_douyin_canonical_id(probe_url),
                    )

            episode_items = [
                video_processing_pb2.EpisodeInfo(
                    index=_safe_int(item.get("index"), 0),
                    title=_normalize_video_title(item.get("title")),
                    duration_sec=_safe_float(item.get("duration_sec"), 0.0),
                    episode_url=_first_non_blank(item.get("episode_url")),
                    episode_cover_url=_first_non_blank(item.get("episode_cover_url")),
                )
                for item in episodes
            ]

            response_resolved_url = (
                _first_non_blank(probe_url, extracted_url, resolved_url)
                if source_platform == "bilibili"
                else _first_non_blank(resolved_url, probe_url, extracted_url)
            )

            return video_processing_pb2.VideoInfoResponse(
                success=True,
                error_msg="",
                raw_input=raw_video_input,
                resolved_url=response_resolved_url,
                source_platform=_first_non_blank(source_platform, "unknown"),
                canonical_id=canonical_id,
                video_title=video_title,
                duration_sec=duration_sec,
                is_collection=total_episodes > 1,
                total_episodes=total_episodes,
                current_episode_index=current_episode_index,
                current_episode_title=current_episode_title,
                episodes=episode_items,
                link_resolver=link_resolver,
                content_type=_first_non_blank(content_type, "unknown"),
                cover_url=cover_url,
            )
        except Exception as exc:
            logger.error(f"[{task_id}] GetVideoInfo failed: {exc}")
            return video_processing_pb2.VideoInfoResponse(
                success=False,
                error_msg=str(exc),
                raw_input=raw_video_input,
                resolved_url=(
                    _first_non_blank(probe_url, extracted_url, resolved_url)
                    if source_platform == "bilibili"
                    else _first_non_blank(resolved_url, probe_url, extracted_url)
                ),
                source_platform=_first_non_blank(source_platform, "unknown"),
                canonical_id=canonical_id,
                video_title="",
                duration_sec=0.0,
                is_collection=False,
                total_episodes=0,
                current_episode_index=0,
                current_episode_title="",
                episodes=[],
                link_resolver=link_resolver,
                content_type=_first_non_blank(content_type, "unknown"),
                cover_url="",
            )
        finally:
            self._decrement_tasks()

    async def TranscribeVideo(self, request, context):
        """
        执行逻辑：
        1) 将视频归档到 storage/{hash} 并确定输出目录。
        2) 若已有 subtitles.txt 则直接复用，否则调用 Transcriber 生成。
        3) 保存字幕并返回路径与摘要预览。
        实现方式：全局 Transcriber + 文件系统读写。
        核心价值：复用缓存字幕，减少重复转写成本。
        决策逻辑：
        - 条件：os.path.exists(subtitle_path)
        - 条件：not transcriber（无法初始化时直接失败）
        - 条件：len(subtitle_text) > 100（仅返回摘要）
        依据来源（证据链）：
        - 文件系统状态：subtitle_path 是否存在。
        - 内部状态：self.resources.transcriber。
        - 字符长度：subtitle_text 长度。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - TranscribeResponse（含 subtitle_path 与 subtitle_text 摘要）。"""
        task_id = request.task_id
        self._cache_metrics_begin(task_id, "TranscribeVideo")
        # 统一本地视频归档到 storage/{hash}：做什么是同域化；为什么是便于复用与清理；权衡是新增一次 I/O
        video_path = _ensure_local_video_in_storage(request.video_path)
        from services.python_grpc.src.media_engine.knowledge_engine.core.language_normalizer import (
            normalize_whisper_language,
            language_for_fingerprint,
        )

        resource_config = getattr(self.resources, "config", {}) or {}
        default_language = (resource_config.get("whisper", {}) or {}).get("language", "auto")
        requested_language = request.language or default_language
        whisper_language = normalize_whisper_language(requested_language)
        fingerprint_language = language_for_fingerprint(requested_language)
        
        logger.info(f"[{task_id}] TranscribeVideo: {video_path} (language={fingerprint_language})")
        transcribe_watchdog: Optional[TaskWatchdogSignalWriter] = None
        soft_heartbeat_stop = threading.Event()
        soft_heartbeat_thread: Optional[threading.Thread] = None
        soft_heartbeat_lock = threading.Lock()
        soft_heartbeat_state: Dict[str, Any] = {
            "status": "running",
            "checkpoint": "transcribe_prepare",
            "completed": 0,
            "pending": 1,
        }

        def _emit_transcribe_soft_heartbeat_loop() -> None:
            interval_sec = max(
                5.0,
                float(_to_int(os.getenv("TRANSCRIBE_SOFT_HEARTBEAT_SEC", 20), 20)),
            )
            while not soft_heartbeat_stop.wait(interval_sec):
                if transcribe_watchdog is None:
                    continue
                try:
                    with soft_heartbeat_lock:
                        snapshot = dict(soft_heartbeat_state)
                    transcribe_watchdog.emit(
                        status=str(snapshot.get("status") or "running"),
                        checkpoint=str(snapshot.get("checkpoint") or "transcribe_pending"),
                        completed=int(snapshot.get("completed", 0)),
                        pending=int(snapshot.get("pending", 1)),
                        signal_type="soft",
                    )
                except Exception as heartbeat_error:
                    logger.warning(f"[{task_id}] Transcribe soft heartbeat emit failed: {heartbeat_error}")

        def _update_transcribe_soft_state(
            *,
            status: Optional[str] = None,
            checkpoint: Optional[str] = None,
            completed: Optional[int] = None,
            pending: Optional[int] = None,
        ) -> None:
            with soft_heartbeat_lock:
                if status is not None:
                    soft_heartbeat_state["status"] = str(status).strip().lower() or "running"
                if checkpoint is not None:
                    soft_heartbeat_state["checkpoint"] = str(checkpoint).strip() or "unknown"
                if completed is not None:
                    soft_heartbeat_state["completed"] = max(0, int(completed))
                if pending is not None:
                    soft_heartbeat_state["pending"] = max(0, int(pending))
        
        try:
            self._increment_tasks()
            
            # 统一输出目录到 storage/{hash}：做什么是集中字幕产物；为什么是避免源目录污染；权衡是多一次路径映射
            output_dir = _normalize_output_dir(video_path)
            
            # 确保目录存在
            os.makedirs(output_dir, exist_ok=True)
            transcribe_watchdog = TaskWatchdogSignalWriter(
                task_id=task_id,
                output_dir=output_dir,
                stage="transcribe",
                total_steps=1,
            )
            transcribe_watchdog.emit(
                status="running",
                checkpoint="transcribe_prepare",
                completed=0,
                pending=1,
                signal_type="hard",
            )
            soft_heartbeat_thread = threading.Thread(
                target=_emit_transcribe_soft_heartbeat_loop,
                name=f"transcribe-soft-heartbeat-{task_id}",
                daemon=True,
            )
            soft_heartbeat_thread.start()
            
            # 🔑 检查是否已存在字幕文件（缓存复用）
            subtitle_path = os.path.join(output_dir, "subtitles.txt")
            
            reuse_fingerprint = _build_input_fingerprint(
                video_path,
                extra={"language": fingerprint_language, "stage": "transcribe"},
            )
            should_try_reuse = self._is_group_reuse_enabled("transcribe")
            reused = False

            if should_try_reuse:
                is_valid, reason = _validate_resource_reuse(
                    subtitle_path,
                    group="transcribe",
                    expected_input_fingerprint=reuse_fingerprint,
                )
                if is_valid:
                    with open(subtitle_path, "r", encoding="utf-8") as f:
                        subtitle_text = f.read()
                    reused = True
                    _update_transcribe_soft_state(
                        status="completed",
                        checkpoint="transcribe_reused",
                        completed=1,
                        pending=0,
                    )
                    transcribe_watchdog.emit(
                        status="completed",
                        checkpoint="transcribe_reused",
                        completed=1,
                        pending=0,
                        signal_type="hard",
                    )
                    logger.info(f"[{task_id}] ✅ Reusing existing subtitles: {subtitle_path}")
                    self._append_resume_report(
                        output_dir=output_dir,
                        task_id=task_id,
                        stage="TranscribeVideo",
                        group="transcribe",
                        resource_path=subtitle_path,
                        action="reuse",
                        reason=reason,
                        priority=False,
                    )
                else:
                    self._append_resume_report(
                        output_dir=output_dir,
                        task_id=task_id,
                        stage="TranscribeVideo",
                        group="transcribe",
                        resource_path=subtitle_path,
                        action="recompute",
                        reason=reason,
                        priority=False,
                    )

            if not reused and os.path.exists(subtitle_path) and not should_try_reuse:
                # 兼容旧行为：未开启复用控制时沿用文件存在即复用
                with open(subtitle_path, "r", encoding="utf-8") as f:
                    subtitle_text = f.read()
                _update_transcribe_soft_state(
                    status="completed",
                    checkpoint="transcribe_reused_legacy",
                    completed=1,
                    pending=0,
                )
                transcribe_watchdog.emit(
                    status="completed",
                    checkpoint="transcribe_reused_legacy",
                    completed=1,
                    pending=0,
                    signal_type="hard",
                )
                logger.info(f"[{task_id}] ✅ Reusing existing subtitles: {subtitle_path}")
            elif not reused:
                # 🔑 使用全局单例 Transcriber
                transcriber = self.resources.transcriber
                if not transcriber:
                    raise RuntimeError("Global Transcriber not initialized")
                _update_transcribe_soft_state(
                    status="running",
                    checkpoint="transcribe_engine_running",
                    completed=0,
                    pending=1,
                )
                transcribe_watchdog.emit(
                    status="running",
                    checkpoint="transcribe_engine_running",
                    completed=0,
                    pending=1,
                    signal_type="hard",
                )
                
                # transcribe 是异步方法
                def _on_transcribe_segment_completed(event: Optional[Dict[str, Any]]) -> None:
                    if transcribe_watchdog is None or not isinstance(event, dict):
                        return
                    try:
                        status = str(event.get("status") or "running").strip().lower() or "running"
                        checkpoint = str(
                            event.get("checkpoint") or "transcribe_segment_completed"
                        ).strip() or "transcribe_segment_completed"
                        completed = max(0, _to_int(event.get("completed", 0), 0))
                        pending = max(0, _to_int(event.get("pending", 0), 0))
                        extra: Dict[str, Any] = {}
                        for key in ("segment_id", "segment_index", "total_segments"):
                            if key in event:
                                extra[key] = _to_int(event.get(key), 0)
                        _update_transcribe_soft_state(
                            status=status,
                            checkpoint=checkpoint,
                            completed=completed,
                            pending=pending,
                        )
                        transcribe_watchdog.emit(
                            status=status,
                            checkpoint=checkpoint,
                            completed=completed,
                            pending=pending,
                            signal_type="hard",
                            extra=extra or None,
                        )
                    except Exception as progress_error:
                        logger.warning(f"[{task_id}] Transcribe segment progress bridge failed: {progress_error}")

                subtitle_text = await transcriber.transcribe(
                    video_path,
                    language=whisper_language,
                    progress_callback=_on_transcribe_segment_completed,
                )
                
                # 🔑 保存字幕文件为 subtitles.txt（异步写盘进程，不阻塞主流程）
                enqueue_text_write(subtitle_path, subtitle_text, scope_key=output_dir)

                _write_resource_meta(
                    subtitle_path,
                    group="transcribe",
                    input_fingerprint=reuse_fingerprint,
                    dependencies={},
                    priority=False,
                )
                _update_transcribe_soft_state(
                    status="completed",
                    checkpoint="transcribe_persist_queued",
                    completed=1,
                    pending=0,
                )
                transcribe_watchdog.emit(
                    status="completed",
                    checkpoint="transcribe_persist_queued",
                    completed=1,
                    pending=0,
                    signal_type="hard",
                )

                logger.info(f"[{task_id}] Subtitles queued to async writer: {subtitle_path}")
            
            return video_processing_pb2.TranscribeResponse(
                success=True,
                subtitle_path=subtitle_path,
                subtitle_text=subtitle_text[:100] + "..." if len(subtitle_text) > 100 else subtitle_text, 
                segments=[],
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] TranscribeVideo failed: {e}")
            if transcribe_watchdog is not None:
                try:
                    _update_transcribe_soft_state(
                        status="failed",
                        checkpoint="transcribe_failed",
                        completed=0,
                        pending=1,
                    )
                    transcribe_watchdog.emit(
                        status="failed",
                        checkpoint="transcribe_failed",
                        completed=0,
                        pending=1,
                        signal_type="hard",
                        extra={"error": str(e)[:200]},
                    )
                except Exception as heartbeat_error:
                    logger.warning(f"[{task_id}] Transcribe watchdog emit failed: {heartbeat_error}")
            return video_processing_pb2.TranscribeResponse(
                success=False,
                subtitle_path="",
                subtitle_text="",
                segments=[],
                error_msg=str(e)
            )
        finally:
            soft_heartbeat_stop.set()
            if soft_heartbeat_thread is not None and soft_heartbeat_thread.is_alive():
                soft_heartbeat_thread.join(timeout=2.0)
            self._decrement_tasks()
    
    async def ProcessStage1(self, request, context):
        """
        执行逻辑：
        1) 归档视频并确定 intermediates 目录。
        2) 若 step2/step6 已存在则直接复用。
        3) 否则调用 Stage1 pipeline 生成中间产物。
        实现方式：run_pipeline + 文件系统读写。
        核心价值：避免重复计算，保障 Stage1 可复用。
        决策逻辑：
        - 条件：os.path.exists(step2_path) and os.path.exists(step6_path)
        依据来源（证据链）：
        - 文件系统状态：step2_path、step6_path 是否存在。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - Stage1Response（含 step2/step6 路径与 sentence_timestamps）。"""
        task_id = request.task_id
        self._cache_metrics_begin(task_id, "ProcessStage1")
        # 统一本地视频归档到 storage/{hash}：做什么是保证中间产物同域；为什么是避免路径分散；权衡是可能增加一次复制/链接
        video_path = _ensure_local_video_in_storage(request.video_path)
        subtitle_path = os.path.abspath(request.subtitle_path) # Convert to absolute path immediately
        max_step = request.max_step or 24
        
        # 统一输出目录到 storage/{hash}：做什么是聚合中间产物；为什么是保证后续阶段可复用；权衡是需要额外路径计算
        output_dir = _normalize_output_dir(video_path)
        intermediates_dir = os.path.join(output_dir, "intermediates")
        
        # 确保目录存在
        os.makedirs(intermediates_dir, exist_ok=True)
        stage1_heartbeat = Stage1HeartbeatWriter(task_id=task_id, output_dir=output_dir, max_step=max_step)
        stage1_heartbeat.emit(
            status="running",
            checkpoint="pipeline_prepare",
            completed=0,
            pending=max_step,
        )
        soft_heartbeat_interval_sec = max(
            5.0,
            float(_to_int(os.getenv("STAGE1_SOFT_HEARTBEAT_SEC", 20), 20)),
        )
        soft_heartbeat_lock = threading.Lock()
        soft_heartbeat_state: Dict[str, Any] = {
            "status": "running",
            "checkpoint": "pipeline_prepare",
            "completed": 0,
            "pending": max_step,
        }
        soft_heartbeat_stop = threading.Event()

        def _update_soft_heartbeat_state(event: Optional[Dict[str, Any]] = None) -> None:
            if not isinstance(event, dict):
                return
            status = str(event.get("status") or "running").strip().lower() or "running"
            checkpoint = str(
                event.get("checkpoint")
                or event.get("step_name")
                or event.get("event")
                or "unknown"
            ).strip()
            completed_raw = event.get("completed", soft_heartbeat_state.get("completed", 0))
            pending_raw = event.get("pending", soft_heartbeat_state.get("pending", max_step))
            try:
                completed = int(completed_raw)
            except Exception:
                completed = int(soft_heartbeat_state.get("completed", 0))
            try:
                pending = int(pending_raw)
            except Exception:
                pending = int(soft_heartbeat_state.get("pending", max_step))
            with soft_heartbeat_lock:
                soft_heartbeat_state["status"] = status
                soft_heartbeat_state["checkpoint"] = checkpoint or "unknown"
                soft_heartbeat_state["completed"] = max(0, completed)
                soft_heartbeat_state["pending"] = max(0, pending)

        def _stage1_soft_heartbeat_loop() -> None:
            while not soft_heartbeat_stop.wait(soft_heartbeat_interval_sec):
                try:
                    with soft_heartbeat_lock:
                        snapshot = dict(soft_heartbeat_state)
                    stage1_heartbeat.emit(
                        status=str(snapshot.get("status") or "running"),
                        checkpoint=str(snapshot.get("checkpoint") or "pipeline_pending"),
                        completed=int(snapshot.get("completed", 0)),
                        pending=int(snapshot.get("pending", max_step)),
                        signal_type="soft",
                    )
                except Exception as soft_heartbeat_error:
                    logger.warning(f"[{task_id}] Stage1 soft heartbeat emit failed: {soft_heartbeat_error}")

        soft_heartbeat_thread = threading.Thread(
            target=_stage1_soft_heartbeat_loop,
            name=f"stage1-soft-heartbeat-{task_id}",
            daemon=True,
        )
        soft_heartbeat_thread.start()
        
        # 输出文件路径
        step2_path = os.path.join(intermediates_dir, "step2_correction_output.json")
        step3_path = os.path.join(intermediates_dir, "step3_merge_output.json")
        step35_path = os.path.join(intermediates_dir, "step3_5_translate_output.json")
        step4_path = os.path.join(intermediates_dir, "step4_clean_local_output.json")
        step6_path = os.path.join(intermediates_dir, "step6_merge_cross_output.json")
        
        logger.info(
            f"[{task_id}] ProcessStage1: max_step={max_step}, output_dir={output_dir}, "
            "flow=step1_validate->step2_correction->step3_merge->step3_5_translate->step4_clean_local->step5_6_dedup_merge"
        )
        
        try:
            self._increment_tasks()
            
            # 🔑 检查是否已存在输出文件（缓存复用）
            # 字幕写盘已异步化；仅当文件未就绪时做一次有界等待，避免阻塞常态路径。
            subtitle_ready = os.path.exists(subtitle_path) and os.path.getsize(subtitle_path) > 0
            if not subtitle_ready:
                subtitle_wait_sec = max(
                    1.0,
                    float(_to_int(os.getenv("TRANSCRIPT_ASYNC_SUBTITLE_WAIT_SEC", 15), 15)),
                )
                flushed = flush_async_json_writes(timeout_sec=subtitle_wait_sec, scope_key=output_dir)
                subtitle_ready = os.path.exists(subtitle_path) and os.path.getsize(subtitle_path) > 0
                if subtitle_ready:
                    logger.info(
                        f"[{task_id}] Subtitle became ready after async wait "
                        f"(flush={flushed}, wait_sec={subtitle_wait_sec})"
                    )
                else:
                    raise FileNotFoundError(
                        f"subtitle_path not ready after async wait: {subtitle_path}"
                    )

            local_sentence_ts = os.path.join(output_dir, "local_storage", "sentence_timestamps.json")
            need_sentence_ts = not os.path.exists(local_sentence_ts)

            stage1_group_enabled = (
                self._is_group_reuse_enabled("stage1_text")
                or self._is_group_reuse_enabled("stage1_semantic")
                or self._is_group_reuse_enabled("stage1_visual")
                or self._is_group_reuse_enabled("stage1_document")
            )

            stage1_fp = _build_input_fingerprint(
                video_path,
                subtitle_path,
                extra={"max_step": max_step, "stage": "stage1"},
            )

            path_conflicts = _find_stage1_output_conflicts(output_dir)
            for conflict in path_conflicts:
                logger.warning(
                    f"[{task_id}] Stage1 output conflict detected: "
                    f"resource={conflict.get('resource')}, kept={conflict.get('kept')}, "
                    f"conflict={conflict.get('conflict')}"
                )

            reused_stage1 = False
            resume_state: Dict[str, Any] = {}
            resume_from_step = ""
            if stage1_group_enabled:
                checks: Dict[str, Tuple[str, bool, str]] = {}
                for key, resource in (
                    ("step2", step2_path),
                    ("step6", step6_path),
                ):
                    valid, reason = _validate_resource_reuse(
                        resource,
                        group="stage1_text",
                        expected_input_fingerprint=stage1_fp,
                    )
                    checks[key] = (resource, valid, reason)

                valid_ts = False
                reason_ts = "missing_sentence_timestamps"
                if not need_sentence_ts:
                    valid_ts, reason_ts = _validate_resource_reuse(
                        local_sentence_ts,
                        group="stage1_text",
                        expected_input_fingerprint=stage1_fp,
                    )

                reused_stage1 = (
                    (not need_sentence_ts)
                    and checks.get("step2", ("", False, ""))[1]
                    and checks.get("step6", ("", False, ""))[1]
                    and valid_ts
                )
                for resource, valid, reason in checks.values():
                    self._append_resume_report(
                        output_dir=output_dir,
                        task_id=task_id,
                        stage="ProcessStage1",
                        group="stage1_text",
                        resource_path=resource,
                        action="reuse" if valid and reused_stage1 else "recompute",
                        reason=reason,
                        priority=False,
                    )

                if need_sentence_ts:
                    self._append_resume_report(
                        output_dir=output_dir,
                        task_id=task_id,
                        stage="ProcessStage1",
                        group="stage1_text",
                        resource_path=local_sentence_ts,
                        action="recompute",
                        reason="missing_sentence_timestamps",
                        priority=False,
                    )

                if not reused_stage1:
                    step2_valid = checks.get("step2", ("", False, ""))[1]
                    if step2_valid:
                        corrected_subtitles, step2_reason = _load_stage1_output_list(
                            step2_path,
                            "corrected_subtitles",
                        )
                        if corrected_subtitles is not None:
                            resume_state["corrected_subtitles"] = corrected_subtitles
                            resume_from_step = "step2_correction"
                        else:
                            logger.warning(
                                f"[{task_id}] step2 payload invalid for partial reuse: {step2_reason}"
                            )
                            step2_valid = False

                    if step2_valid:
                        for resource, output_field, state_key, step_name in (
                            (step3_path, "merged_sentences", "merged_sentences", "step3_merge"),
                            (step35_path, "translated_sentences", "translated_sentences", "step3_5_translate"),
                            (step4_path, "cleaned_sentences", "cleaned_sentences", "step4_clean_local"),
                        ):
                            valid, reason = _validate_resource_reuse(
                                resource,
                                group="stage1_text",
                                expected_input_fingerprint=stage1_fp,
                            )
                            if valid:
                                payload, payload_reason = _load_stage1_output_list(resource, output_field)
                                if payload is not None:
                                    resume_state[state_key] = payload
                                    resume_from_step = step_name
                                else:
                                    valid = False
                                    reason = payload_reason

                            self._append_resume_report(
                                output_dir=output_dir,
                                task_id=task_id,
                                stage="ProcessStage1",
                                group="stage1_text",
                                resource_path=resource,
                                action="reuse" if valid else "recompute",
                                reason=reason,
                                priority=False,
                            )

            stage1_final_state: Dict[str, Any] = {}
            if reused_stage1:
                _update_soft_heartbeat_state(
                    {
                        "status": "completed",
                        "checkpoint": "reused_stage1_outputs",
                        "completed": max_step,
                        "pending": 0,
                    }
                )
                stage1_heartbeat.emit(
                    status="completed",
                    checkpoint="reused_stage1_outputs",
                    completed=max_step,
                    pending=0,
                )
                logger.info(f"[{task_id}] ✅ Reusing existing Stage1 outputs")
            else:
                if os.path.exists(step2_path) and os.path.exists(step6_path) and need_sentence_ts:
                    logger.warning(f"[{task_id}] sentence_timestamps.json missing, regenerating Step4 (and upstream) outputs")
                # 🔑 调用 Stage1 Pipeline（支持 max_step）
                # 当前链路在 step3_merge 与 step4_clean_local 之间包含 step3_5_translate。
                # 仍强制 effective_max_step >= 4，确保 sentence_timestamps 至少经过 step4_clean_local 生成。
                effective_max_step = max_step if max_step >= 4 else 4
                logger.info(
                    f"[{task_id}] Stage1 effective_max_step={effective_max_step} "
                    "(ensure step4_clean_local and sentence_timestamps)"
                )
                if resume_from_step:
                    logger.info(
                        f"[{task_id}] Stage1 partial reuse hit: resume_from={resume_from_step}, "
                        f"resume_fields={sorted(resume_state.keys())}"
                    )
                def _stage1_progress_callback(event: Dict[str, Any]) -> None:
                    _update_soft_heartbeat_state(event)
                    stage1_heartbeat.emit_from_event(event)

                stage1_final_state = await run_pipeline(
                   video_path=video_path,
                   subtitle_path=subtitle_path,
                   output_dir=output_dir,
                   max_step=effective_max_step,
                   output_steps=[
                       "step2_correction",
                       "step3_merge",
                       "step3_5_translate",
                       "step4_clean_local",
                       "step5_6_dedup_merge",
                    ],
                    resume_state=resume_state or None,
                    resume_from_step=resume_from_step or None,
                    progress_callback=_stage1_progress_callback,
                 )

                # Stage1 step2~step6 产物改为异步落盘后，这里只在关键文件未就绪时等待。
                required_outputs = [step2_path, step6_path]
                pending_required = [
                    path
                    for path in required_outputs
                    if (not os.path.exists(path)) or os.path.getsize(path) <= 0
                ]
                if pending_required:
                    persist_wait_sec = max(
                        1.0,
                        float(_to_int(os.getenv("TRANSCRIPT_ASYNC_STAGE1_PERSIST_WAIT_SEC", 30), 30)),
                    )
                    flushed = flush_async_json_writes(timeout_sec=persist_wait_sec, scope_key=output_dir)
                    pending_required = [
                        path
                        for path in required_outputs
                        if (not os.path.exists(path)) or os.path.getsize(path) <= 0
                    ]
                    if pending_required:
                        raise RuntimeError(
                            "Stage1 outputs not ready after async wait: "
                            f"{pending_required} (flush={flushed}, wait_sec={persist_wait_sec})"
                        )
                self._cache_stage1_runtime_outputs(output_dir=output_dir, final_state=stage1_final_state)

                resource_meta_specs = [
                    (step2_path, {}),
                    (step3_path, {"step2": _file_signature(step2_path)}),
                    (step35_path, {"step3": _file_signature(step3_path)}),
                    (
                        step4_path,
                        {
                            "step3": _file_signature(step3_path),
                            "step3_5": _file_signature(step35_path),
                        },
                    ),
                    (step6_path, {"step4": _file_signature(step4_path)}),
                ]
                for resource, dependencies in resource_meta_specs:
                    _write_resource_meta(
                        resource,
                        group="stage1_text",
                        input_fingerprint=stage1_fp,
                        dependencies=dependencies,
                        priority=False,
                    )
            
            # 补齐 sentence_timestamps.json（来自 Stage1 local_storage）
            intermediates_dir = os.path.join(output_dir, "intermediates")
            os.makedirs(intermediates_dir, exist_ok=True)
            inter_sentence_ts = os.path.join(intermediates_dir, "sentence_timestamps.json")
            sentence_timestamps_path = ""
            if os.path.exists(local_sentence_ts):
                try:
                    # 复制到 intermediates，供 Phase2A/Phase2B 统一读取
                    import shutil
                    shutil.copy2(local_sentence_ts, inter_sentence_ts)
                    sentence_timestamps_path = inter_sentence_ts

                    _write_resource_meta(
                        local_sentence_ts,
                        group="stage1_text",
                        input_fingerprint=stage1_fp,
                        dependencies={
                            "step2": _file_signature(step2_path),
                            "step6": _file_signature(step6_path),
                        },
                        priority=False,
                    )
                except Exception as e:
                    logger.warning(f"[{task_id}] Copy sentence_timestamps.json failed: {e}")
                    sentence_timestamps_path = local_sentence_ts
            else:
                logger.warning(f"[{task_id}] sentence_timestamps.json not found at {local_sentence_ts}")
                sentence_timestamps_path = inter_sentence_ts if os.path.exists(inter_sentence_ts) else ""

            stage1_domain = str((stage1_final_state or {}).get("domain", "") or "").strip()
            stage1_main_topic = str((stage1_final_state or {}).get("main_topic", "") or "").strip()
            if not stage1_domain:
                stage1_domain = str((resume_state or {}).get("domain", "") or "").strip()
            if not stage1_main_topic:
                stage1_main_topic = str((resume_state or {}).get("main_topic", "") or "").strip()
            if stage1_domain or stage1_main_topic:
                try:
                    _upsert_video_meta_topic_fields(
                        task_dir=output_dir,
                        domain=stage1_domain,
                        main_topic=stage1_main_topic,
                    )
                except Exception as topic_meta_error:
                    logger.warning(f"[{task_id}] Failed to update video_meta domain/main_topic: {topic_meta_error}")

            stage1_heartbeat.emit(
                status="completed",
                checkpoint="stage1_response_ready",
                completed=max_step,
                pending=0,
            )
            _update_soft_heartbeat_state(
                {
                    "status": "completed",
                    "checkpoint": "stage1_response_ready",
                    "completed": max_step,
                    "pending": 0,
                }
            )
              
            return video_processing_pb2.Stage1Response(
                success=True,
                step2_json_path=step2_path,
                step6_json_path=step6_path,
                sentence_timestamps_path=sentence_timestamps_path,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] ProcessStage1 failed: {e}")
            try:
                _update_soft_heartbeat_state(
                    {
                        "status": "failed",
                        "checkpoint": "stage1_failed",
                        "completed": 0,
                        "pending": max_step,
                    }
                )
                stage1_heartbeat.emit(
                    status="failed",
                    checkpoint="stage1_failed",
                    completed=0,
                    pending=max_step,
                    extra={"error": str(e)[:200]},
                )
            except Exception as heartbeat_error:
                logger.warning(f"[{task_id}] Stage1 heartbeat write failed: {heartbeat_error}")
            return video_processing_pb2.Stage1Response(
                success=False,
                step2_json_path="",
                step6_json_path="",
                sentence_timestamps_path="",
                error_msg=str(e)
            )
        finally:
            soft_heartbeat_stop.set()
            if soft_heartbeat_thread.is_alive():
                soft_heartbeat_thread.join(timeout=2.0)
            self._decrement_tasks()
    
    async def AnalyzeSemanticUnits(self, request, context):
        """
        执行逻辑：
        1) 归档视频并确定 Phase2A 输出目录。
        2) 若 semantic_units_phase2a.json 已存在则直接复用并解析。
        3) 否则构建 RichTextPipeline 执行仅语义切分并落盘。
        4) AnalyzeResponse 返回 semantic_units_ref/semantic_units_inline；
           素材请求由后续 Hybrid Analysis 生成。
        实现方式：RichTextPipeline + JSON 读写。
        核心价值：固化阶段边界（Phase2A 仅分割），避免在此阶段执行 CV/LLM 素材策略。
        决策逻辑：
        - 条件：os.path.exists(semantic_units_path)
        依据来源（证据链）：
        - 文件系统状态：semantic_units_path 是否存在。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - AnalyzeResponse（含 screenshot_requests/clip_requests/semantic_units_ref/semantic_units_inline）。"""
        import os  # Explicit local import
        task_id = request.task_id
        # 统一本地视频归档到 storage/{hash}：做什么是统一 Phase2A 路径；为什么是避免素材找不到；权衡是多一次 I/O
        video_path = _ensure_local_video_in_storage(request.video_path)
        step2_json_path = os.path.abspath(request.step2_json_path) if request.step2_json_path else "" # Convert to absolute path immediately
        step6_json_path = os.path.abspath(request.step6_json_path) if request.step6_json_path else "" # Convert to absolute path immediately
        sentence_timestamps_path = os.path.abspath(request.sentence_timestamps_path) if request.sentence_timestamps_path else ""
        
        # 统一输出目录到 storage/{hash}：做什么是让 Phase2A 产物与后续一致；为什么是减少跨目录查找；权衡是忽略外部路径差异
        output_dir = _normalize_output_dir(video_path)
        phase2a_candidates = _phase2a_semantic_units_candidates(output_dir)
        semantic_units_path = phase2a_candidates[0]
        if not sentence_timestamps_path:
            # 默认使用 intermediates 路径（Stage1 已复制到此处）
            sentence_timestamps_path = os.path.join(output_dir, "intermediates", "sentence_timestamps.json")
            if not os.path.exists(sentence_timestamps_path):
                # 回退到 local_storage
                local_sentence_ts = os.path.join(output_dir, "local_storage", "sentence_timestamps.json")
                if os.path.exists(local_sentence_ts):
                    sentence_timestamps_path = local_sentence_ts
                else:
                    sentence_timestamps_path = ""
        
        logger.info(f"[{task_id}] AnalyzeSemanticUnits (Phase2A), output_dir={output_dir}")
        analyze_watchdog = TaskWatchdogSignalWriter(
            task_id=task_id,
            output_dir=output_dir,
            stage="phase2a",
            total_steps=3,
        )
        analyze_soft_stop = threading.Event()
        analyze_soft_thread: Optional[threading.Thread] = None
        analyze_soft_lock = threading.Lock()
        analyze_soft_state: Dict[str, Any] = {
            "status": "running",
            "checkpoint": "phase2a_prepare",
            "completed": 0,
            "pending": 3,
        }

        def _emit_analyze_soft_loop() -> None:
            interval_sec = max(
                5.0,
                float(_to_int(os.getenv("PHASE2A_SOFT_HEARTBEAT_SEC", 20), 20)),
            )
            while not analyze_soft_stop.wait(interval_sec):
                try:
                    with analyze_soft_lock:
                        snapshot = dict(analyze_soft_state)
                    analyze_watchdog.emit(
                        status=str(snapshot.get("status") or "running"),
                        checkpoint=str(snapshot.get("checkpoint") or "phase2a_pending"),
                        completed=int(snapshot.get("completed", 0)),
                        pending=int(snapshot.get("pending", 3)),
                        signal_type="soft",
                    )
                except Exception as soft_error:
                    logger.warning(f"[{task_id}] Phase2A soft heartbeat emit failed: {soft_error}")

        def _update_analyze_soft_state(
            *,
            status: Optional[str] = None,
            checkpoint: Optional[str] = None,
            completed: Optional[int] = None,
            pending: Optional[int] = None,
        ) -> None:
            with analyze_soft_lock:
                if status is not None:
                    analyze_soft_state["status"] = str(status).strip().lower() or "running"
                if checkpoint is not None:
                    analyze_soft_state["checkpoint"] = str(checkpoint).strip() or "unknown"
                if completed is not None:
                    analyze_soft_state["completed"] = max(0, int(completed))
                if pending is not None:
                    analyze_soft_state["pending"] = max(0, int(pending))
        
        try:
            self._increment_tasks()
            analyze_watchdog.emit(
                status="running",
                checkpoint="phase2a_prepare",
                completed=0,
                pending=3,
                signal_type="hard",
            )
            analyze_soft_thread = threading.Thread(
                target=_emit_analyze_soft_loop,
                name=f"phase2a-soft-heartbeat-{task_id}",
                daemon=True,
            )
            analyze_soft_thread.start()

            phase2a_fp = _build_input_fingerprint(
                video_path,
                extra={
                    "step2": _file_signature(step2_json_path),
                    "step6": _file_signature(step6_json_path),
                    "sentence_timestamps": _file_signature(sentence_timestamps_path) if sentence_timestamps_path else None,
                    "stage": "phase2a",
                },
            )
            phase2a_reuse_enabled = self._is_group_reuse_enabled("phase2a")
            reuse_candidate_path, phase2a_reason = _resolve_reuse_candidate(
                phase2a_candidates,
                group="phase2a",
                expected_input_fingerprint=phase2a_fp,
                reuse_enabled=phase2a_reuse_enabled,
            )
            
            # 🔑 检查是否已存在 Phase2A 输出（缓存复用）
            if reuse_candidate_path:
                semantic_units_path = reuse_candidate_path
                logger.warning(
                    f"[{task_id}] ✅ Reusing existing Phase2A output: {semantic_units_path} "
                    f"(cache hit -> Phase2A 仅语义切分产物复用，不在本阶段生成素材请求)"
                )

                self._append_resume_report(
                    output_dir=output_dir,
                    task_id=task_id,
                    stage="AnalyzeSemanticUnits",
                    group="phase2a",
                    resource_path=semantic_units_path,
                    action="reuse",
                    reason=phase2a_reason,
                    priority=True,
                )

                logger.info(f"[{task_id}] Loaded from cache: Phase2A semantic units ready (no material requests in this stage)")

                semantic_units_payload = self._get_phase2a_runtime_semantic_units(
                    output_dir=output_dir,
                    semantic_units_path=semantic_units_path,
                    deep_copy=False,
                )
                if semantic_units_payload is None:
                    try:
                        semantic_units_payload = self._load_semantic_units_from_json_path(semantic_units_path)
                    except Exception as load_error:
                        logger.warning(
                            f"[{task_id}] Failed to load semantic units for ref/inline response from {semantic_units_path}: {load_error}"
                        )
                        semantic_units_payload = []

                response = video_processing_pb2.AnalyzeResponse(
                    success=True,
                    screenshot_requests=[],
                    clip_requests=[],
                    error_msg=""
                )
                cache_entry = self._cache_phase2a_runtime_semantic_units(
                    output_dir=output_dir,
                    semantic_units_path=semantic_units_path,
                    semantic_units=semantic_units_payload,
                    task_id=task_id,
                )
                if isinstance(cache_entry, dict):
                    response.semantic_units_ref.CopyFrom(
                        video_processing_pb2.SemanticUnitsRef(
                            ref_id=str(cache_entry.get("ref_id", "")),
                            task_id=task_id,
                            output_dir=output_dir,
                            unit_count=int(cache_entry.get("unit_count", 0) or 0),
                            schema_version=str(cache_entry.get("schema_version", "phase2a.v1")),
                            fingerprint=str(cache_entry.get("fingerprint", "")),
                        )
                    )
                    response.semantic_units_inline.CopyFrom(
                        self._build_semantic_units_inline_message(
                            semantic_units_payload,
                            cache_entry=cache_entry,
                        )
                    )
                _update_analyze_soft_state(
                    status="completed",
                    checkpoint="phase2a_reused_ready",
                    completed=3,
                    pending=0,
                )
                analyze_watchdog.emit(
                    status="completed",
                    checkpoint="phase2a_reused_ready",
                    completed=3,
                    pending=0,
                    signal_type="hard",
                )
                return response

            if phase2a_reuse_enabled and not reuse_candidate_path:
                self._append_resume_report(
                    output_dir=output_dir,
                    task_id=task_id,
                    stage="AnalyzeSemanticUnits",
                    group="phase2a",
                    resource_path=phase2a_candidates[0],
                    action="recompute",
                    reason=phase2a_reason,
                    priority=True,
                )
            
            
            # 确保目录存在
            os.makedirs(output_dir, exist_ok=True)

            runtime_stage1_outputs = self._get_stage1_runtime_outputs(output_dir)
            runtime_step2_subtitles: Optional[List[Dict[str, Any]]] = None
            runtime_step6_paragraphs: Optional[List[Dict[str, Any]]] = None
            if runtime_stage1_outputs:
                candidate_step2 = runtime_stage1_outputs.get("step2_subtitles", [])
                candidate_step6 = runtime_stage1_outputs.get("step6_paragraphs", [])
                if isinstance(candidate_step2, list) and candidate_step2:
                    runtime_step2_subtitles = candidate_step2
                if isinstance(candidate_step6, list) and candidate_step6:
                    runtime_step6_paragraphs = candidate_step6
                logger.info(
                    f"[{task_id}] Stage1 runtime cache hit: "
                    f"step2_items={len(runtime_step2_subtitles or [])}, "
                    f"step6_paragraphs={len(runtime_step6_paragraphs or [])}"
                )
            else:
                logger.info(f"[{task_id}] Stage1 runtime cache miss, fallback to JSON loading")
             
            # 🔑 创建 RichTextPipeline (使用正确的构造函数签名)
            pipeline = RichTextPipeline(
                video_path=video_path,
                step2_path=step2_json_path,
                step6_path=step6_json_path,
                output_dir=output_dir,
                sentence_timestamps_path=sentence_timestamps_path,
                segmenter=self.resources.semantic_unit_segmenter,
                step2_subtitles=runtime_step2_subtitles,
                step6_paragraphs=runtime_step6_paragraphs,
            )

            # 复用全局单例切分器：做什么是避免每次 new Segmenter/LLMClient；为什么是降低 Phase2A 热路径开销；
            # 权衡是该实例跨任务共享，需要保证其内部无任务级可变状态（当前满足）。
            shared_segmenter = self.resources.semantic_unit_segmenter
            if shared_segmenter is not None:
                pipeline.segmenter = shared_segmenter

            logger.info(f"[{task_id}] Phase2A segmentation-only mode enabled: skip material request generation")
            _update_analyze_soft_state(
                status="running",
                checkpoint="phase2a_segmentation_running",
                completed=1,
                pending=2,
            )
            analyze_watchdog.emit(
                status="running",
                checkpoint="phase2a_segmentation_running",
                completed=1,
                pending=2,
                signal_type="hard",
            )
            semantic_units_path = await pipeline.analyze_segmentation_only()
            runtime_semantic_units = getattr(pipeline, "latest_phase2a_semantic_units_payload", None)
            cache_entry = None
            if not isinstance(runtime_semantic_units, list):
                # 兜底：如果 pipeline 未暴露内存 payload，则尝试在本次请求内同步回读一次最新落盘结果。
                # 目的：尽量保证 AnalyzeResponse 携带 inline/ref，进一步降低 Java->Python 路径依赖。
                try:
                    flush_async_json_writes(timeout_sec=10.0, scope_key=output_dir)
                    runtime_semantic_units = self._load_semantic_units_from_json_path(semantic_units_path)
                    logger.info(
                        f"[{task_id}] Phase2A runtime payload recovered from json path: "
                        f"units={len(runtime_semantic_units)}"
                    )
                except Exception as load_error:
                    logger.warning(
                        f"[{task_id}] Phase2A runtime payload unavailable after segmentation: "
                        f"path={semantic_units_path}, error={load_error}"
                    )
            if isinstance(runtime_semantic_units, list):
                cache_entry = self._cache_phase2a_runtime_semantic_units(
                    output_dir=output_dir,
                    semantic_units_path=semantic_units_path,
                    semantic_units=runtime_semantic_units,
                    task_id=task_id,
                )
            else:
                logger.info(
                    f"[{task_id}] Phase2A runtime cache skipped: payload unavailable, path={semantic_units_path}"
                )

            phase2a_dependencies = {
                "step2": _file_signature(step2_json_path),
                "step6": _file_signature(step6_json_path),
                "sentence_timestamps": _file_signature(sentence_timestamps_path) if sentence_timestamps_path else {},
            }
            for candidate_path in _phase2a_semantic_units_candidates(output_dir):
                _write_resource_meta(
                    candidate_path,
                    group="phase2a",
                    input_fingerprint=phase2a_fp,
                    dependencies=phase2a_dependencies,
                    priority=True,
                )
            
            response = video_processing_pb2.AnalyzeResponse(
                success=True,
                screenshot_requests=[],
                clip_requests=[],
                error_msg=""
            )
            if isinstance(runtime_semantic_units, list) and isinstance(cache_entry, dict):
                response.semantic_units_ref.CopyFrom(
                    video_processing_pb2.SemanticUnitsRef(
                        ref_id=str(cache_entry.get("ref_id", "")),
                        task_id=task_id,
                        output_dir=output_dir,
                        unit_count=int(cache_entry.get("unit_count", 0) or 0),
                        schema_version=str(cache_entry.get("schema_version", "phase2a.v1")),
                        fingerprint=str(cache_entry.get("fingerprint", "")),
                    )
                )
                response.semantic_units_inline.CopyFrom(
                    self._build_semantic_units_inline_message(
                        runtime_semantic_units,
                        cache_entry=cache_entry,
                    )
                )
            _update_analyze_soft_state(
                status="completed",
                checkpoint="phase2a_response_ready",
                completed=3,
                pending=0,
            )
            analyze_watchdog.emit(
                status="completed",
                checkpoint="phase2a_response_ready",
                completed=3,
                pending=0,
                signal_type="hard",
            )
            return response
            
        except Exception as e:
            logger.error(f"[{task_id}] AnalyzeSemanticUnits failed: {e}")
            logger.exception(e)  # Log full traceback
            try:
                _update_analyze_soft_state(
                    status="failed",
                    checkpoint="phase2a_failed",
                    completed=1,
                    pending=2,
                )
                analyze_watchdog.emit(
                    status="failed",
                    checkpoint="phase2a_failed",
                    completed=1,
                    pending=2,
                    signal_type="hard",
                    extra={"error": str(e)[:200]},
                )
            except Exception as heartbeat_error:
                logger.warning(f"[{task_id}] Phase2A watchdog emit failed: {heartbeat_error}")
            return video_processing_pb2.AnalyzeResponse(
                success=False,
                screenshot_requests=[],
                clip_requests=[],
                error_msg=str(e)
            )
        finally:
            analyze_soft_stop.set()
            if analyze_soft_thread is not None and analyze_soft_thread.is_alive():
                analyze_soft_thread.join(timeout=2.0)
            self._decrement_tasks()

    async def ClassifyKnowledgeBatch(self, request, context):
        """
        执行逻辑：
        1) 获取 KnowledgeClassifier 并设置 Step2 路径（可选）。
        2) 使用 Semaphore 限制并发，批量分类 action_segments。
        3) 汇总结果并转换为 protobuf 返回。
        实现方式：asyncio.gather + KnowledgeClassifier.classify_batch。
        核心价值：批量调用 LLM 降低延迟与成本。
        决策逻辑：
        - 条件：not classifier
        - 条件：hasattr(request, 'step2_path') and request.step2_path
        - 条件：i >= len(action_segments)
        依据来源（证据链）：
        - 输入参数：request.step2_path。
        - 分类结果长度与 action_segments 数量。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - KnowledgeClassificationResponse（包含结果列表与错误信息）。"""
        task_id = request.task_id
        try:
            self._increment_tasks()
            classifier = self.resources.knowledge_classifier
            
            if not classifier:
                return video_processing_pb2.KnowledgeClassificationResponse(
                    success=False, error_msg="KnowledgeClassifier not initialized"
                )
            
            # 🔑 设置当前任务的 Step 2 路径（动态更新）
            if hasattr(request, 'step2_path') and request.step2_path:
                incoming_step2_path = request.step2_path
                previous_step2_path = getattr(classifier, 'step2_path', "")
                path_changed = str(previous_step2_path or "") != str(incoming_step2_path or "")
                classifier.step2_path = incoming_step2_path
                if path_changed:
                    classifier._all_subtitles_cache = None  # 仅路径变更时清缓存，避免批量分类重复冷启动
                    subtitle_repo = getattr(classifier, 'subtitle_repo', None)
                    if subtitle_repo is not None and hasattr(subtitle_repo, 'set_paths'):
                        subtitle_repo.set_paths(step2_path=incoming_step2_path, clear_cache=True)
            
            # 🚀 DeepSeek 并发探测：AIMD 动态逼近吞吐上限
            limiter = self._classify_concurrency_limiter

            async def _classify_per_unit() -> list:
                """
                做什么：按 unit 并发调用 classify_batch（旧路径）。
                为什么：当 multi-unit 合并请求失败/解析不稳定时，作为兼容回退。
                权衡：LLM 调用次数更多，但稳定性更高。
                """

                async def process_unit(u):
                    acquired_permits = await limiter.acquire()
                    try:
                        action_segments = [
                            {"start_sec": au.start_sec, "end_sec": au.end_sec, "id": au.id}
                            for au in u.action_units
                        ]

                        batch_results = await classifier.classify_batch(
                            semantic_unit_title=u.title,
                            semantic_unit_text=u.text,
                            action_segments=action_segments,
                        )

                        await limiter.record_success()
                        unit_results_proto = []
                        for i, res in enumerate(batch_results):
                            if i >= len(action_segments):
                                break
                            action_id = action_segments[i]["id"]
                            unit_results_proto.append(
                                video_processing_pb2.KnowledgeClassificationResult(
                                    unit_id=u.unit_id,
                                    action_id=action_id,
                                    knowledge_type=res.get("knowledge_type", "过程性知识"),
                                    confidence=res.get("confidence", 0.5),
                                    key_evidence=res.get("key_evidence", ""),
                                    reasoning=res.get("reasoning", ""),
                                )
                            )
                        return unit_results_proto
                    except Exception as e:
                        await limiter.record_failure(is_rate_limit=False)
                        logger.error(f"Unit {u.unit_id} classification failed: {e}")
                        return []
                    finally:
                        if acquired_permits:
                            await limiter.release(acquired_permits)

                tasks = [process_unit(u) for u in request.units]
                all_unit_results = await asyncio.gather(*tasks)
                return [r for sublist in all_unit_results for r in sublist]

            # 🚀 优化：跨 unit 合并请求（减少 LLM 调用次数），仍保留外部 limiter 做 AIMD 探测
            flat_results = []
            raw = (os.getenv("MODULE2_KC_MULTI_UNIT_ENABLED", "1") or "").strip().lower()
            multi_unit_enabled = raw in ("1", "true", "yes", "y", "on")

            if hasattr(classifier, "classify_units_batch") and multi_unit_enabled:
                try:
                    units_payload = []
                    unit_actions_map = {}
                    for u in request.units:
                        action_segments = [
                            {"start_sec": au.start_sec, "end_sec": au.end_sec, "id": au.id}
                            for au in u.action_units
                        ]
                        unit_actions_map[u.unit_id] = action_segments
                        units_payload.append(
                            {
                                "unit_id": u.unit_id,
                                "title": u.title,
                                "full_text": u.text,
                                "action_segments": action_segments,
                            }
                        )

                    results_map = await classifier.classify_units_batch(
                        units_payload, external_limiter=limiter
                    )

                    # 结果自检：大量 Batch Miss 说明 JSON 解析或输出结构不稳定，触发回退
                    total_actions = 0
                    miss_actions = 0
                    for u in request.units:
                        action_segments = unit_actions_map.get(u.unit_id, [])
                        total_actions += len(action_segments)
                        batch_results = (
                            results_map.get(u.unit_id, []) if isinstance(results_map, dict) else []
                        )
                        for i in range(len(action_segments)):
                            res = batch_results[i] if i < len(batch_results) else {}
                            if isinstance(res, dict) and res.get("key_evidence") == "Batch Miss":
                                miss_actions += 1

                    fallback_ratio = float(os.getenv("MODULE2_KC_MULTI_UNIT_FALLBACK_MISS_RATIO", "0.4") or "0.4")
                    if total_actions > 0 and (
                        (not isinstance(results_map, dict))
                        or (not results_map)
                        or ((miss_actions / total_actions) > fallback_ratio)
                    ):
                        raise RuntimeError(
                            f"multi-unit results look invalid: miss={miss_actions}/{total_actions}, ratio>{fallback_ratio}"
                        )

                    for u in request.units:
                        action_segments = unit_actions_map.get(u.unit_id, [])
                        batch_results = (
                            results_map.get(u.unit_id, []) if isinstance(results_map, dict) else []
                        )
                        for i, action in enumerate(action_segments):
                            res = batch_results[i] if i < len(batch_results) else {}
                            flat_results.append(
                                video_processing_pb2.KnowledgeClassificationResult(
                                    unit_id=u.unit_id,
                                    action_id=action.get("id", ""),
                                    knowledge_type=res.get("knowledge_type", "过程性知识"),
                                    confidence=float(res.get("confidence", 0.5)),
                                    key_evidence=res.get("key_evidence", ""),
                                    reasoning=res.get("reasoning", ""),
                                )
                            )
                except Exception as e:
                    logger.warning(f"[{task_id}] multi-unit classify failed: {e} -> fallback per-unit")
                    flat_results = await _classify_per_unit()
            else:
                # 兼容：旧实现按 unit 并发（保持行为不变）
                flat_results = await _classify_per_unit()
            
            return video_processing_pb2.KnowledgeClassificationResponse(
                success=True, results=flat_results, error_msg=""
            )
        except Exception as e:
            logger.error(f"[{task_id}] ClassifyKnowledgeBatch failed: {e}")
            return video_processing_pb2.KnowledgeClassificationResponse(success=False, error_msg=str(e))
        finally:
            self._decrement_tasks()

    async def _phase2a_generate_material_requests_impl(self, request, context):
        """
        执行逻辑：
        1) 构建 RichTextPipeline/截图范围计算器。
        2) 将 gRPC 单元转为 SemanticUnit 并执行两阶段合并 + 复用 action_units 知识类型过滤。
        3) 为过滤后的动作生成 clip 请求。
        4) 汇总稳定岛，计算截图范围并选择最佳截图。
        5) 更新 semantic_units_phase2a.json 并返回素材请求。
        实现方式：RichTextPipeline._classify_and_filter_actions + ScreenshotSelector。
        核心价值：统一素材请求生成逻辑，保证截图/剪辑与语义一致。
        决策逻辑：
        - 条件：screenshot_tasks
        - 条件：hasattr(request, 'video_duration') and request.video_duration（否则默认 0.0）
        - 条件：all_stable_islands（无稳定岛时回退到中点策略）
        依据来源（证据链）：
        - 输入参数：request.video_duration、request.units。
        - 过滤结果：clip_actions、all_stable_islands。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - GenerateMaterialRequestsResponse（含 screenshot_requests/clip_requests）。
        补充说明：
        新架构流程：
        1. 第一阶段合并：动作单元按时间合并（间隔 < 1s）
        2. 基于 action_units 知识类型过滤：剔除讲解型/噪声类型
        3. 第二阶段合并：同类型动作按间隔 < 5s 合并
        4. 全局截图：基于稳定岛范围选择最佳截图"""
        task_id = request.task_id
        # 统一本地视频归档到 storage/{hash}：做什么是保证素材生成可追溯；为什么是与前序一致；权衡是增加一次拷贝/链接
        video_path = _ensure_local_video_in_storage(request.video_path)
        
        
        try:
            self._increment_tasks()
            
            # 短日志：定位 Java -> Python 是否带上 action_units.knowledge_type
            for u in request.units:
                if u.action_units:
                    first = u.action_units[0]
                    logger.info(
                        f"[{task_id}] MaterialRequests input: unit={u.unit_id}, actions={len(u.action_units)}, first_kt={first.knowledge_type}"
                    )
                    break

            # 💥 断链探针：上游 knowledge_type 缺失/疑似 CV actionType（用于定位默认值/字段错用）
            import re
            coarse_unit_types = {"abstract", "process", "concrete", "configuration", "deduction", "practical", "scan", "scanning"}
            for u in request.units:
                if not u.action_units:
                    continue
                unit_kt = (getattr(u, "knowledge_type", "") or "").strip()
                missing_cnt = 0
                cv_like_cnt = 0
                default_like_cnt = 0
                example = ""

                for au in u.action_units:
                    kt_raw = getattr(au, "knowledge_type", "")
                    kt = (kt_raw or "").strip()
                    kt_lower = kt.lower()

                    is_missing = (not kt) or kt_lower in {"unknown", "knowledge", "none", "null"}
                    is_cv_like = bool(re.match(r"(?i)^k\\d+_", kt)) or any(
                        m in kt_lower for m in ("operation", "click", "drag", "scroll", "mouse", "keyboard")
                    )
                    is_default_like = (kt == unit_kt) and (unit_kt.lower() in coarse_unit_types)

                    if is_missing:
                        missing_cnt += 1
                    elif is_cv_like:
                        cv_like_cnt += 1
                    elif is_default_like:
                        default_like_cnt += 1

                    if (is_missing or is_cv_like or is_default_like) and not example:
                        example = (
                            f"action_id={getattr(au, 'id', 0)}, kt={kt!r}, "
                            f"start={getattr(au, 'start_sec', 0.0):.2f}, end={getattr(au, 'end_sec', 0.0):.2f}"
                        )

                if missing_cnt or cv_like_cnt or default_like_cnt:
                    logger.warning(
                        f"[{task_id}] 上游 knowledge_type 缺失/疑似 CV actionType: "
                        f"unit={u.unit_id}, actions={len(u.action_units)}, "
                        f"missing={missing_cnt}, cv_like={cv_like_cnt}, default_like={default_like_cnt}, "
                        f"unit_kt={unit_kt!r}, example=({example})"
                    )
            
            # =====================================================================
            # GenerateMaterialRequests 核心流程（CV/LLM 分析后的素材请求生成）
            # =====================================================================

            
            # 🚀 V9.0: 使用 RichTextPipeline 的两阶段合并逻辑
            from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_pipeline import RichTextPipeline
            from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_range_calculator import ScreenshotRangeCalculator
            
            # 初始化组件
            output_dir = _normalize_output_dir(video_path)
            intermediates_dir = os.path.join(output_dir, "intermediates")
            
            # 🔑 构造中间文件路径 (用于加载字幕和上下文)
            step2_path = os.path.join(intermediates_dir, "step2_correction_output.json")
            step6_path = os.path.join(intermediates_dir, "step6_merge_cross_output.json")
            sentence_timestamps_path = os.path.join(intermediates_dir, "sentence_timestamps.json")
            
            pipeline = RichTextPipeline(
                video_path=video_path, 
                output_dir=output_dir,
                step2_path=step2_path,
                step6_path=step6_path,
                sentence_timestamps_path=sentence_timestamps_path,
                segmenter=self.resources.semantic_unit_segmenter,
            )
            # 🚀 Fix: Ensure video_duration is a float
            video_duration = float(request.video_duration) if hasattr(request, 'video_duration') and request.video_duration else 0.0
            calculator = ScreenshotRangeCalculator(video_duration)
            selector = self.resources.get_screenshot_selector(video_path)
            
            # 转换 gRPC units 为 SemanticUnit 对象
            from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnit
            units = []
            
            for u in request.units:
                # 转换 action_units
                action_segments = []
                for au in u.action_units:
                    action_segments.append({
                        "start_sec": au.start_sec,
                        "end_sec": au.end_sec,
                        "knowledge_type": au.knowledge_type,
                        "stable_islands": []  # 稍后填充
                    })
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(
                            "GenerateMaterialRequests action_unit: start_sec=%.3f end_sec=%.3f knowledge_type=%s",
                            au.start_sec,
                            au.end_sec,
                            au.knowledge_type,
                        )
                
                # 🚀 关键修复: 从 proto 提取 stable_islands
                stable_islands = []
                for si in u.stable_islands:
                    stable_islands.append({
                        "start_sec": si.start_sec,
                        "end_sec": si.end_sec,
                        "mid_sec": si.mid_sec,
                        "duration_sec": si.duration_sec
                    })
                
                unit = SemanticUnit(
                    unit_id=u.unit_id,
                    knowledge_type=u.knowledge_type,
                    knowledge_topic=getattr(u, 'knowledge_topic', None) or getattr(u, 'title', None) or "未知主题",
                    full_text=u.full_text,
                    source_paragraph_ids=[],
                    source_sentence_ids=[],
                    start_sec=u.start_sec,
                    end_sec=u.end_sec,
                    action_segments=action_segments,
                    stable_islands=stable_islands  # 🚀 传入稳定岛
                )
                units.append(unit)
            
            # 🚀 核心：两阶段合并 + 复用 action_units 的知识类型（不再二次分类）
            logger.info(f"[{task_id}] Running two-stage merge with existing action_units knowledge_type...")
            filter_results = {}
            STAGE1_GAP_THRESHOLD = 1.0
            STAGE2_GAP_THRESHOLD = 5.0
            EXPLAINABLE_TYPES = ['讲解', '概念', '原理', '定义', '背景', '解释', 'Concept', 'Principle', 'explanation']
            NOISE_TYPES = ['noise', 'transition', '噪点', '转场']

            for unit in units:
                unit_id = unit.unit_id
                action_segments = unit.action_segments or []
                stable_islands = unit.stable_islands or []

                if not action_segments:
                    # 无动作单元，只用稳定岛生成截图
                    filter_results[unit_id] = {
                        'clip_actions': [],
                        'all_stable_islands': stable_islands,
                        'crossed_islands_stage1': [],
                        'crossed_islands_stage2': []
                    }
                    continue

                # 第一阶段合并（按时间邻近合并，不改变知识类型）
                sorted_actions = sorted(action_segments, key=lambda x: x.get('start_sec', 0))
                merged_stage1, crossed_stage1 = pipeline._merge_actions_local(
                    sorted_actions, stable_islands, STAGE1_GAP_THRESHOLD
                )

                # 补齐缺失知识类型：做什么是兜底；为什么是避免误过滤；权衡是可能偏向 unit 级判断
                for a in merged_stage1:
                    k_type_raw = str(a.get('knowledge_type', '')).strip()
                    if (not k_type_raw) or k_type_raw.lower() in ('knowledge', 'unknown'):
                        a['knowledge_type'] = unit.knowledge_type or '过程性知识'

                # 过滤讲解型/噪声类型，仅保留需要视频的动作
                video_worthy_actions = []
                for a in merged_stage1:
                    k_type = str(a.get('knowledge_type', ''))
                    is_explainable = any(t in k_type for t in EXPLAINABLE_TYPES)
                    is_noise = any(t in k_type.lower() for t in NOISE_TYPES)
                    if not is_explainable and not is_noise:
                        video_worthy_actions.append(a)
                    else:
                        logger.debug(f"[{unit_id}] Filtered action [{a.get('start_sec', 0):.1f}s-{a.get('end_sec', 0):.1f}s]: type={k_type}")

                # 第二阶段合并（只对保留的动作合并）
                merged_stage2, crossed_stage2 = pipeline._merge_actions_local_stage2(
                    video_worthy_actions, stable_islands, STAGE2_GAP_THRESHOLD
                )

                # 收集稳定岛用于截图
                all_stable = pipeline._collect_all_stable_islands_local(
                    merged_stage2, stable_islands, crossed_stage1, crossed_stage2
                )

                filter_results[unit_id] = {
                    'clip_actions': merged_stage2,
                    'all_stable_islands': all_stable,
                    'crossed_islands_stage1': crossed_stage1,
                    'crossed_islands_stage2': crossed_stage2
                }
            
            # 生成素材请求
            final_ss = []
            final_clips = []
            
            for unit in units:
                unit_id = unit.unit_id
                result = filter_results.get(unit_id, {})
                
                clip_actions = result.get('clip_actions', [])
                all_stable_islands = result.get('all_stable_islands', [])
                
                # 1. 为过滤后的动作单元生成视频切片
                for i, action in enumerate(clip_actions):
                    action_start = float(action.get('start_sec', 0))
                    action_end = float(action.get('end_sec', 0))
                    knowledge_type = action.get('knowledge_type') or unit.knowledge_type or '过程性知识'
                    logger.info(f"[{task_id}] 为过滤后的动作单元生成视频切片 for unit={unit_id}, action{i}, kt={knowledge_type}")
                    # Sentence 对齐：无字幕时保持动作边界，避免起点被拉到 0
                    if getattr(pipeline, "subtitles", None):
                        sentence_start = pipeline._align_to_sentence_start(action_start)
                        sentence_end = pipeline._align_to_sentence_end(action_end)
                    else:
                        sentence_start = action_start
                        sentence_end = action_end
                    
                    # 自适应动作包络（与 rich_text_pipeline 保持一致）
                    envelope_start, envelope_end = pipeline._compute_action_envelope(
                        unit=unit,
                        action_start=action_start,
                        action_end=action_end,
                        sentence_start=sentence_start,
                        sentence_end=sentence_end,
                        knowledge_type=knowledge_type
                    )
                    unit_start = float(getattr(unit, "start_sec", 0.0))
                    unit_end = float(getattr(unit, "end_sec", 0.0))
                    unit_duration = unit_end - unit_start
                    logger.warning(
                        f"[{task_id}] {unit_id} action{i}: "
                        f"unit[{unit_start:.2f}-{unit_end:.2f}={unit_duration:.2f}s] "
                        f"action[{action_start:.2f}-{action_end:.2f}] "
                        f"envelope[{envelope_start:.2f}-{envelope_end:.2f}] "
                        f"kt={knowledge_type}"
                    )
                    
                    final_clips.append(self._build_clip_request_pb({
                        "clip_id": f"{unit_id}/{unit_id}_clip_action_{i + 1:03d}",
                        "start_sec": envelope_start,
                        "end_sec": envelope_end,
                        "knowledge_type": knowledge_type,
                        "semantic_unit_id": unit_id
                    }, unit_id))
                
            
            # 🚀 V9.0 优化：批量并行截图选择（ProcessPool）
            # 现在所有类型都从 ValidateCVBatch 获得稳定岛
            # (process 通过 CV 检测，concrete/abstract 通过先粗后细检测)
            logger.info(f"[{task_id}] Starting parallel screenshot selection...")
            
            # 1. 收集所有稳定岛 (批量处理以解决重叠)
            all_islands_to_process = []  # [(start, end, "unit_id|index"), ...]
            
            for unit in units:
                unit_id = unit.unit_id
                result = filter_results.get(unit_id, {})
                all_stable_islands = result.get('all_stable_islands', [])
                
                if all_stable_islands:
                    for i, island in enumerate(all_stable_islands):
                         # 使用 composite ID 追踪来源: "SU001|0"
                        composite_id = f"{unit_id}|{i}"
                        all_islands_to_process.append((
                            island.get('start_sec', 0),
                            island.get('end_sec', 0),
                            composite_id
                        ))
            
            # 2. 批量计算截图范围 (解决重叠)
            calculated_ranges = calculator.calculate_ranges(all_islands_to_process)
            
            # 3. 转换为截图任务
            screenshot_tasks = []
            
            # 3.1 处理有稳定岛的单元
            for r in calculated_ranges:
                # 解码 composite ID
                parts = r.semantic_unit_id.split("|")
                if len(parts) == 2:
                    u_id, idx_str = parts
                    screenshot_tasks.append({
                        "unit_id": u_id,
                        "island_index": int(idx_str),
                        "expanded_start": r.start_sec,
                        "expanded_end": r.end_sec
                    })
            
            # 3.2 处理完全没有稳定岛的单元 (回退到中点)
            # 检查哪些 unit_id 没有生成截图任务
            units_with_tasks = set(t["unit_id"] for t in screenshot_tasks)
            
            for unit in units:
                if unit.unit_id not in units_with_tasks:
                    # Double check if it really had no islands or if islands were filtered/merged out excessively?
                    # If it had no islands initially, we fallback here.
                    screenshot_tasks.append({
                        "unit_id": unit.unit_id,
                        "island_index": 0,
                        "expanded_start": float(unit.start_sec),
                        "expanded_end": min(float(unit.end_sec), float(unit.start_sec) + 2.0)
                    })
            
            if screenshot_tasks:
                estimated_ss_frames = 0
                for task in screenshot_tasks:
                    start_sec = float(task.get("expanded_start", 0.0))
                    end_sec = float(task.get("expanded_end", start_sec))
                    estimated_ss_frames += max(1, int(max(0.0, end_sec - start_sec) / 0.5) + 1)

                screenshot_registry = self._create_ephemeral_frame_registry(estimated_ss_frames)
                try:
                    # Step 1: 批量读取所有需要的帧到 SharedMemory
                    shm_map = await self._batch_read_frames_for_screenshots(
                        video_path,
                        screenshot_tasks,
                        frame_registry=screenshot_registry,
                    )

                    # Step 2: 提交到 ProcessPool 并行计算
                    from services.python_grpc.src.vision_validation.worker import run_screenshot_selection_task
                    loop = asyncio.get_event_loop()
                    futures = []

                    for task in screenshot_tasks:
                        key = f"{task['unit_id']}_island{task['island_index']}"
                        task_shm_frames = shm_map.get(key, {})

                        if not task_shm_frames:
                            # 回退：如果没有读取到帧，使用中点时间戳
                            final_ss.append(video_processing_pb2.ScreenshotRequest(
                                screenshot_id=f"{task['unit_id']}/{task['unit_id']}_ss_island_{task['island_index'] + 1:03d}",
                                timestamp_sec=(task['expanded_start'] + task['expanded_end']) / 2,
                                label=f"稳定岛{task['island_index']}",
                                semantic_unit_id=task['unit_id'],
                                frame_reason="",
                            ))
                            continue

                        future = loop.run_in_executor(
                            self.cv_process_pool,  # 复用现有的 ProcessPool
                            functools.partial(
                                run_screenshot_selection_task,
                                video_path=video_path,
                                unit_id=task['unit_id'],
                                island_index=task['island_index'],
                                expanded_start=task['expanded_start'],
                                expanded_end=task['expanded_end'],
                                shm_frames=task_shm_frames,
                                fps=30.0  # 默认帧率，可从视频元信息获取
                            )
                        )
                        futures.append(future)

                    # Step 3: 等待所有任务完成
                    if futures:
                        results = await asyncio.gather(*futures)

                        # Step 4: 构建 ScreenshotRequest
                        for result in results:
                            final_ss.append(video_processing_pb2.ScreenshotRequest(
                                screenshot_id=f"{result['unit_id']}/{result['unit_id']}_ss_island_{result['island_index'] + 1:03d}",
                                timestamp_sec=result['selected_timestamp'],
                                label=f"稳定岛{result['island_index']}",
                                semantic_unit_id=result['unit_id'],
                                frame_reason="",
                            ))
                finally:
                    self._cleanup_ephemeral_frame_registry(screenshot_registry)

                logger.info(f"[{task_id}] Parallel screenshot selection completed: {len(final_ss)} screenshots")
            
            logger.info(f"[{task_id}] Generated {len(final_clips)} clips, {len(final_ss)} screenshots")

            # 兜底：若未生成任何截图请求，按单元中点补齐
            if not final_ss and units:
                for unit in units:
                    mid_ts = (float(unit.start_sec) + float(unit.end_sec)) / 2
                    final_ss.append(video_processing_pb2.ScreenshotRequest(
                        screenshot_id=f"{unit.unit_id}/{unit.unit_id}_ss_fallback_001",
                        timestamp_sec=mid_ts,
                        label="fallback",
                        semantic_unit_id=unit.unit_id,
                        frame_reason="",
                    ))
                logger.info(f"[{task_id}] Fallback screenshots generated: {len(final_ss)}")
                
            # 🚀 V9.0: 更新 semantic_units_phase2a.json 包含完整的素材信息
            try:
                output_dir = _normalize_output_dir(video_path)
                semantic_units_path = os.path.join(output_dir, "semantic_units_phase2a.json")
                
                if os.path.exists(semantic_units_path):
                    import json
                    with open(semantic_units_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    # 兼容 grouped/legacy 两种结构，统一建立 unit_id -> unit 节点引用索引。
                    units_index = self._build_semantic_unit_index(data)
                    
                    # 构建 unit_id -> 素材请求映射
                    unit_ss_map = {}
                    unit_clip_map = {}
                    unit_action_map = {}
                    
                    for ss in final_ss:
                        unit_id = ss.semantic_unit_id
                        if unit_id not in unit_ss_map:
                            unit_ss_map[unit_id] = []
                        unit_ss_map[unit_id].append({
                            "screenshot_id": ss.screenshot_id,
                            "timestamp_sec": ss.timestamp_sec,
                            "label": ss.label,
                            "semantic_unit_id": ss.semantic_unit_id
                        })
                    
                    for clip in final_clips:
                        unit_id = clip.semantic_unit_id
                        if unit_id not in unit_clip_map:
                            unit_clip_map[unit_id] = []
                        unit_clip_map[unit_id].append({
                            "clip_id": clip.clip_id,
                            "start_sec": clip.start_sec,
                            "end_sec": clip.end_sec,
                            "knowledge_type": clip.knowledge_type,
                            "semantic_unit_id": clip.semantic_unit_id
                        })
                    
                    # 从请求中提取 action_units 信息
                    # 统一兼容 protobuf 对象与 dict，避免缺失字段导致回写失败
                    def _safe_get_action_field(action_unit, field_name: str, default_value):
                        if isinstance(action_unit, dict):
                            return action_unit.get(field_name, default_value)
                        return getattr(action_unit, field_name, default_value)

                    for u in request.units:
                        if u.action_units:
                            unit_action_map[u.unit_id] = [
                                {
                                    "id": au.id if hasattr(au, 'id') else i,
                                    "start_sec": au.start_sec,
                                    "end_sec": au.end_sec,
                                    # action_type 可能不存在：做什么是兜底写入；为什么是避免写回失败；权衡是字段语义可能退化为知识类型
                                    "action_type": getattr(au, "action_type", "") or getattr(au, "knowledge_type", ""),
                                    "knowledge_type": _safe_get_action_field(au, "knowledge_type", ""),
                                    "confidence": _safe_get_action_field(au, "confidence", 0.0),
                                    "reasoning": _safe_get_action_field(au, "reasoning", "")
                                }
                                for i, au in enumerate(u.action_units)
                            ]
                    
                    # 更新 JSON 数据（按 unit_id 原地回写，保持原始结构不被打平）。
                    for unit_id, item in units_index.items():
                        
                        # 更新素材请求
                        if "material_requests" not in item:
                            item["material_requests"] = {}
                        item["material_requests"]["screenshot_requests"] = unit_ss_map.get(unit_id, [])
                        item["material_requests"]["clip_requests"] = unit_clip_map.get(unit_id, [])
                        
                        # 同步 action_units / action_segments，避免两字段口径不一致
                        # 约定：落盘后 action_segments 由 action_units 一致映射得到
                        action_units_for_unit = unit_action_map.get(unit_id, [])
                        item["action_units"] = action_units_for_unit
                        item["action_segments"] = [
                            {
                                "start_sec": float(action.get("start_sec", 0.0)),
                                "end_sec": float(action.get("end_sec", 0.0)),
                                "knowledge_type": str(action.get("knowledge_type", "") or ""),
                                "action_type": str(action.get("action_type", "") or ""),
                                "confidence": float(action.get("confidence", 0.0) or 0.0),
                                "reasoning": str(action.get("reasoning", "") or ""),
                                "stable_islands": [],
                            }
                            for action in action_units_for_unit
                        ]
                        
                        # 标记 CV 验证完成
                        item["cv_validated"] = True

                    # 保存更新后的 JSON
                    with open(semantic_units_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    
                    phase2a_fp = _build_input_fingerprint(
                        video_path,
                        extra={
                            "step2": _file_signature(step2_path),
                            "step6": _file_signature(step6_path),
                            "sentence_timestamps": _file_signature(sentence_timestamps_path) if sentence_timestamps_path else None,
                            "stage": "phase2a",
                        },
                    )
                    _write_resource_meta(
                        semantic_units_path,
                        group="phase2a",
                        input_fingerprint=phase2a_fp,
                        dependencies={
                            "step2": _file_signature(step2_path),
                            "step6": _file_signature(step6_path),
                            "sentence_timestamps": _file_signature(sentence_timestamps_path) if sentence_timestamps_path else {},
                        },
                        priority=True,
                    )

                    logger.info(f"[{task_id}] Updated semantic_units_phase2a.json with {len(final_ss)} screenshots, {len(final_clips)} clips")
            except Exception as e:
                logger.warning(f"[{task_id}] Failed to update semantic_units_phase2a.json: {e}")
            
            return video_processing_pb2.GenerateMaterialRequestsResponse(
                success=True,
                screenshot_requests=final_ss,
                clip_requests=final_clips,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] GenerateMaterialRequests failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return video_processing_pb2.GenerateMaterialRequestsResponse(success=False, error_msg=str(e))
        finally:
            self._decrement_tasks()
    
    async def AssembleRichText(self, request, context):
        """
        执行逻辑：
        1) 归档视频并规范化输入路径。
        2) 创建 RichTextPipeline 执行 assemble_only。
        3) 统计截图/剪辑数量并返回结果路径。
        实现方式：RichTextPipeline + 文件系统统计。
        核心价值：统一 Phase2B 富文本组装输出。
        决策逻辑：
        - 条件：os.path.exists(clips_dir)
        - 条件：os.path.exists(screenshots_dir)
        依据来源（证据链）：
        - 文件系统状态：clips_dir、screenshots_dir 是否存在。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - AssembleResponse（含 markdown/json 路径与统计信息）。"""
        task_id = request.task_id
        semantic_source_case = request.WhichOneof("semantic_units_source") if hasattr(request, "WhichOneof") else None
        screenshots_dir = os.path.abspath(request.screenshots_dir) # Convert to absolute path immediately
        clips_dir = os.path.abspath(request.clips_dir) # Convert to absolute path immediately
        # 统一本地视频归档到 storage/{hash}：做什么是确保最终装配可追溯；为什么是与前序同域；权衡是可能增加一次 I/O
        video_path = _ensure_local_video_in_storage(request.video_path)
        title = request.title or "视频内容"
        
        # 统一输出目录到 storage/{hash}：做什么是让最终产物同域聚合；为什么是便于回放定位；权衡是覆盖调用方传入的 output_dir
        output_dir = _normalize_output_dir(video_path)
        title = _resolve_assemble_document_title(
            request_title=str(getattr(request, "title", "") or ""),
            output_dir=output_dir,
            video_path=video_path,
        )
        self._cache_metrics_begin(task_id, "AssembleRichText")
        
        # 确保目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"[{task_id}] AssembleRichText (Phase2B)")
        logger.info(f"  → video_path: {video_path}")
        logger.info(f"  → semantic_source: {semantic_source_case or 'runtime_or_empty'}")
        logger.info(f"  → screenshots_dir: {screenshots_dir}")
        logger.info(f"  → clips_dir: {clips_dir}")
        logger.info(f"  → output_dir: {output_dir}")

        assemble_watchdog = TaskWatchdogSignalWriter(
            task_id=task_id,
            output_dir=output_dir,
            stage="phase2b",
            total_steps=4,
        )
        assemble_soft_stop = threading.Event()
        assemble_soft_thread: Optional[threading.Thread] = None
        assemble_soft_lock = threading.Lock()
        assemble_soft_state: Dict[str, Any] = {
            "status": "running",
            "checkpoint": "phase2b_prepare",
            "completed": 0,
            "pending": 4,
        }

        def _emit_assemble_soft_loop() -> None:
            interval_sec = max(
                5.0,
                float(_to_int(os.getenv("PHASE2B_SOFT_HEARTBEAT_SEC", 20), 20)),
            )
            while not assemble_soft_stop.wait(interval_sec):
                try:
                    with assemble_soft_lock:
                        snapshot = dict(assemble_soft_state)
                    assemble_watchdog.emit(
                        status=str(snapshot.get("status") or "running"),
                        checkpoint=str(snapshot.get("checkpoint") or "phase2b_pending"),
                        completed=int(snapshot.get("completed", 0)),
                        pending=int(snapshot.get("pending", 4)),
                        signal_type="soft",
                    )
                except Exception as soft_error:
                    logger.warning(f"[{task_id}] Phase2B soft heartbeat emit failed: {soft_error}")

        def _update_assemble_soft_state(
            *,
            status: Optional[str] = None,
            checkpoint: Optional[str] = None,
            completed: Optional[int] = None,
            pending: Optional[int] = None,
        ) -> None:
            with assemble_soft_lock:
                if status is not None:
                    assemble_soft_state["status"] = str(status).strip().lower() or "running"
                if checkpoint is not None:
                    assemble_soft_state["checkpoint"] = str(checkpoint).strip() or "unknown"
                if completed is not None:
                    assemble_soft_state["completed"] = max(0, int(completed))
                if pending is not None:
                    assemble_soft_state["pending"] = max(0, int(pending))

        from services.python_grpc.src.content_pipeline.infra.llm.deepseek_audit import (
            build_phase2b_audit_context,
            push_deepseek_audit_context,
            pop_deepseek_audit_context,
        )
        audit_token = None

        try:
            self._increment_tasks()
            assemble_watchdog.emit(
                status="running",
                checkpoint="phase2b_prepare",
                completed=0,
                pending=4,
                signal_type="hard",
            )
            assemble_soft_thread = threading.Thread(
                target=_emit_assemble_soft_loop,
                name=f"phase2b-soft-heartbeat-{task_id}",
                daemon=True,
            )
            assemble_soft_thread.start()

            audit_context = build_phase2b_audit_context(
                output_dir=output_dir,
                task_id=task_id,
                video_path=video_path,
            )
            audit_token = push_deepseek_audit_context(audit_context)

            semantic_units_payload: List[Dict[str, Any]] = []
            if semantic_source_case == "semantic_units_inline":
                semantic_units_payload = self._decode_semantic_units_inline_message(request.semantic_units_inline)
                _update_assemble_soft_state(
                    status="running",
                    checkpoint="phase2b_semantic_inline_ready",
                    completed=1,
                    pending=3,
                )
                assemble_watchdog.emit(
                    status="running",
                    checkpoint="phase2b_semantic_inline_ready",
                    completed=1,
                    pending=3,
                    signal_type="hard",
                )
                logger.info(
                    f"[{task_id}] AssembleRichText loaded semantic units from inline payload: units={len(semantic_units_payload)}"
                )
            elif semantic_source_case == "semantic_units_ref":
                ref_id = str(request.semantic_units_ref.ref_id or "").strip()
                ref_entry = self._get_phase2a_runtime_cache_entry_by_ref(ref_id)
                if isinstance(ref_entry, dict):
                    semantic_units_payload = ref_entry.get("semantic_units", []) or []
                    _update_assemble_soft_state(
                        status="running",
                        checkpoint="phase2b_semantic_ref_ready",
                        completed=1,
                        pending=3,
                    )
                    assemble_watchdog.emit(
                        status="running",
                        checkpoint="phase2b_semantic_ref_ready",
                        completed=1,
                        pending=3,
                        signal_type="hard",
                    )
                    logger.info(
                        f"[{task_id}] AssembleRichText loaded semantic units from ref cache: "
                        f"ref_id={ref_id}, units={len(semantic_units_payload)}"
                    )
                else:
                    logger.warning(
                        f"[{task_id}] AssembleRichText semantic_units_ref not found, fallback to runtime/path: ref_id={ref_id}"
                    )

            runtime_semantic_units = self._get_phase2a_runtime_semantic_units(
                output_dir=output_dir,
                semantic_units_path="",
            )
            if runtime_semantic_units is not None and self._should_prefer_runtime_semantic_units(
                current_payload=semantic_units_payload,
                runtime_payload=runtime_semantic_units,
            ):
                previous_metrics = self._collect_semantic_unit_quality_metrics(semantic_units_payload)
                runtime_metrics = self._collect_semantic_unit_quality_metrics(runtime_semantic_units)
                semantic_units_payload = runtime_semantic_units
                _update_assemble_soft_state(
                    status="running",
                    checkpoint="phase2b_semantic_runtime_ready",
                    completed=1,
                    pending=3,
                )
                assemble_watchdog.emit(
                    status="running",
                    checkpoint="phase2b_semantic_runtime_ready",
                    completed=1,
                    pending=3,
                    signal_type="hard",
                )

                logger.info(
                    f"[{task_id}] AssembleRichText selected runtime semantic units: "
                    f"units={len(semantic_units_payload)}, "
                    f"current_metrics={previous_metrics}, runtime_metrics={runtime_metrics}"
                )

            materialized_semantic_units_path = ""
            if semantic_units_payload:
                materialized_semantic_units_path = self._materialize_semantic_units_payload(
                    output_dir=output_dir,
                    task_id=task_id,
                    semantic_units=semantic_units_payload,
                )
                self._cache_phase2a_runtime_semantic_units(
                    output_dir=output_dir,
                    semantic_units_path=materialized_semantic_units_path,
                    semantic_units=semantic_units_payload,
                    task_id=task_id,
                )
                logger.info(f"[{task_id}] AssembleRichText materialized semantic units for Phase2B: {materialized_semantic_units_path}")

            if not materialized_semantic_units_path:
                raise FileNotFoundError("semantic_units source missing: neither inline/ref/runtime available")
            _update_assemble_soft_state(
                status="running",
                checkpoint="phase2b_materialized_ready",
                completed=2,
                pending=2,
            )
            assemble_watchdog.emit(
                status="running",
                checkpoint="phase2b_materialized_ready",
                completed=2,
                pending=2,
                signal_type="hard",
            )

            # 🔑 创建 RichTextPipeline
            # 注意: Phase2B 主要使用 semantic_units_json，step2/step6 在 Phase2A 已处理
            # 此处使用占位值，实际逻辑在 assemble_only 中加载 semantic_units_json
            pipeline = RichTextPipeline(
                video_path=video_path,
                step2_path="",  # Phase2B 不需要
                step6_path="",  # Phase2B 不需要
                output_dir=output_dir,
                segmenter=self.resources.semantic_unit_segmenter,
            )
            
            # 🔑 调用 Phase2B: assemble_only
            markdown_path, json_path = await pipeline.assemble_only(
                semantic_units_json_path=materialized_semantic_units_path,
                screenshots_dir=screenshots_dir,
                clips_dir=clips_dir,
                title=title
            )
            _update_assemble_soft_state(
                status="running",
                checkpoint="phase2b_assembled_ready",
                completed=3,
                pending=1,
            )
            assemble_watchdog.emit(
                status="running",
                checkpoint="phase2b_assembled_ready",
                completed=3,
                pending=1,
                signal_type="hard",
            )

            try:
                from services.python_grpc.src.content_pipeline.phase2b.video_category_service import (
                    classify_phase2b_output,
                )

                await classify_phase2b_output(
                    output_dir=output_dir,
                    title=title,
                    result_json_path=json_path,
                )
            except Exception as category_error:
                logger.warning(f"[{task_id}] Phase2B category classification failed: {category_error}")
             
            # 统计信息
            stats = video_processing_pb2.AssembleStats(
                total_sections=0,
                video_clips_count=len(os.listdir(clips_dir)) if os.path.exists(clips_dir) else 0,
                screenshots_count=len(os.listdir(screenshots_dir)) if os.path.exists(screenshots_dir) else 0,
                text_only_count=0,
                vision_validated_count=0
            )
            _update_assemble_soft_state(
                status="completed",
                checkpoint="phase2b_response_ready",
                completed=4,
                pending=0,
            )
            assemble_watchdog.emit(
                status="completed",
                checkpoint="phase2b_response_ready",
                completed=4,
                pending=0,
                signal_type="hard",
            )
            
            return video_processing_pb2.AssembleResponse(
                success=True,
                markdown_path=markdown_path,
                json_path=json_path,
                stats=stats,
                error_msg=""
            )
            
        except Exception as e:
            import traceback # Import traceback for detailed error logging
            logger.error(f"[{task_id}] AssembleRichText failed: {e}")
            logger.error(traceback.format_exc()) # Log full traceback
            try:
                with assemble_soft_lock:
                    failed_completed = int(assemble_soft_state.get("completed", 0))
                    failed_pending = max(1, int(assemble_soft_state.get("pending", 1)))
                _update_assemble_soft_state(
                    status="failed",
                    checkpoint="phase2b_failed",
                    completed=failed_completed,
                    pending=failed_pending,
                )
                assemble_watchdog.emit(
                    status="failed",
                    checkpoint="phase2b_failed",
                    completed=failed_completed,
                    pending=failed_pending,
                    signal_type="hard",
                    extra={"error": str(e)[:200]},
                )
            except Exception as heartbeat_error:
                logger.warning(f"[{task_id}] Phase2B watchdog emit failed: {heartbeat_error}")
            return video_processing_pb2.AssembleResponse(
                success=False,
                markdown_path="",
                json_path="",
                stats=video_processing_pb2.AssembleStats(), # Keep stats field, but initialize empty
                error_msg=str(e)
            )
        finally:
            assemble_soft_stop.set()
            if assemble_soft_thread is not None and assemble_soft_thread.is_alive():
                assemble_soft_thread.join(timeout=2.0)
            if audit_token is not None:
                try:
                    pop_deepseek_audit_context(audit_token)
                except Exception as audit_cleanup_exc:
                    logger.warning(f"[{task_id}] clean deepseek audit context failed: {audit_cleanup_exc}")
            # 任务进入最终装配收尾后，释放本任务对应的 Stage1 运行态缓存。
            self._clear_stage1_runtime_cache(output_dir)
            self._clear_phase2a_runtime_cache(output_dir)
            self._write_cache_metrics(output_dir, task_id, "AssembleRichText")
            self._cleanup_non_priority_resources(output_dir, task_id)
            self._decrement_tasks()

    async def ExtractBookPdf(self, request, context):
        """按指定页码范围抽取 PDF，优先 MinerU，失败回退 PyMuPDF。"""
        task_id = str(getattr(request, "task_id", "") or "book_pdf_extract").strip() or "book_pdf_extract"
        try:
            self._increment_tasks()
            timeout_seconds = max(60, _to_int(os.getenv("BOOK_PDF_EXTRACT_TIMEOUT_SEC", "300"), 300))
            result = await asyncio.to_thread(
                extract_book_pdf_markdown,
                task_id=task_id,
                pdf_path=str(getattr(request, "pdf_path", "") or ""),
                output_dir=str(getattr(request, "output_dir", "") or ""),
                start_page=int(getattr(request, "start_page", 0) or 0),
                end_page=int(getattr(request, "end_page", 0) or 0),
                image_dir=str(getattr(request, "image_dir", "") or ""),
                output_root=str(getattr(request, "output_root", "") or ""),
                section_id=str(getattr(request, "section_id", "") or ""),
                prefer_mineru=bool(getattr(request, "prefer_mineru", True)),
                timeout_seconds=timeout_seconds,
            )
            return video_processing_pb2.ExtractBookPdfResponse(
                success=bool(result.success),
                markdown=result.markdown or "",
                markdown_path=result.markdown_path or "",
                extractor=result.extractor or "",
                image_count=int(result.image_count),
                table_count=int(result.table_count),
                code_block_count=int(result.code_block_count),
                formula_block_count=int(result.formula_block_count),
                error_msg=result.error_msg or "",
                image_paths=list(result.image_paths or []),
            )
        except Exception as error:
            logger.error(f"[{task_id}] ExtractBookPdf failed: {error}")
            logger.error(traceback.format_exc())
            return video_processing_pb2.ExtractBookPdfResponse(
                success=False,
                markdown="",
                markdown_path="",
                extractor="unknown",
                image_count=0,
                table_count=0,
                code_block_count=0,
                formula_block_count=0,
                error_msg=str(error),
            )
        finally:
            self._decrement_tasks()
    
    def _get_video_duration(self, video_path: str) -> float:
        """
        执行逻辑：
        1) 若文件不存在则返回默认时长。
        2) 调用 ffprobe 解析视频时长。
        3) 解析失败时返回默认值。
        实现方式：subprocess.run + ffprobe。
        核心价值：为下载/剪辑等流程提供时长信息。
        决策逻辑：
        - 条件：not os.path.exists(video_path)
        - 条件：result.returncode == 0 and result.stdout.strip()
        依据来源（证据链）：
        - 输入参数：video_path。
        - 子进程输出：ffprobe 返回码与 stdout。
        输入参数：
        - video_path: 文件路径（类型：str）。
        输出参数：
        - float：视频时长（秒）。"""
        try:
            if not os.path.exists(video_path):
                return 300.0
            import subprocess
            result = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", video_path],
                capture_output=True, text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                 return float(result.stdout.strip())
            return 300.0
        except:
            return 300.0  # 默认5分钟

    def _build_clip_segments_pb(self, clip: Any) -> List[video_processing_pb2.ClipSegment]:
        """
        说明：将 clip 请求中的 segments 转为 protobuf 结构。
        取舍：只保留 end_sec > start_sec 的片段，避免空段影响拼接。
        """
        segments = []
        raw_segments = None
        if isinstance(clip, dict):
            raw_segments = clip.get("segments", None)
        else:
            raw_segments = getattr(clip, "segments", None)
        if not raw_segments:
            return segments

        for seg in raw_segments:
            if isinstance(seg, dict):
                start_sec = seg.get("start_sec", seg.get("start", 0.0))
                end_sec = seg.get("end_sec", seg.get("end", 0.0))
            else:
                start_sec = getattr(seg, "start_sec", 0.0)
                end_sec = getattr(seg, "end_sec", 0.0)
            try:
                start_sec = float(start_sec)
                end_sec = float(end_sec)
            except Exception:
                continue
            if end_sec <= start_sec:
                continue
            segments.append(video_processing_pb2.ClipSegment(
                start_sec=start_sec,
                end_sec=end_sec
            ))
        return segments

    def _build_clip_request_pb(self, clip: Any, default_unit_id: str = "") -> video_processing_pb2.ClipRequest:
        """
        说明：统一构建 ClipRequest（兼容 dict 与 dataclass）。
        取舍：segments 为空则保持 start/end 单段逻辑，兼容旧版链路。
        """
        if isinstance(clip, dict):
            clip_id = clip.get(
                "clip_id",
                f"{default_unit_id}/{default_unit_id}_clip_fallback_001" if default_unit_id else "clip_fallback_001",
            )
            start_sec = clip.get("start_sec", 0.0)
            end_sec = clip.get("end_sec", 0.0)
            knowledge_type = clip.get("knowledge_type", "")
            semantic_unit_id = clip.get("semantic_unit_id", default_unit_id)
        else:
            clip_id = getattr(
                clip,
                "clip_id",
                f"{default_unit_id}/{default_unit_id}_clip_fallback_001" if default_unit_id else "clip_fallback_001",
            )
            start_sec = getattr(clip, "start_sec", 0.0)
            end_sec = getattr(clip, "end_sec", 0.0)
            knowledge_type = getattr(clip, "knowledge_type", "")
            semantic_unit_id = getattr(clip, "semantic_unit_id", default_unit_id)

        segments = self._build_clip_segments_pb(clip)
        return video_processing_pb2.ClipRequest(
            clip_id=clip_id,
            start_sec=float(start_sec),
            end_sec=float(end_sec),
            knowledge_type=knowledge_type,
            semantic_unit_id=semantic_unit_id,
            segments=segments
        )
    
    def _increment_tasks(self):
        """
        执行逻辑：
        1) 加锁更新活跃任务计数。
        2) 为健康检查与负载控制提供依据。
        实现方式：线程锁保护的计数自增。
        核心价值：可观测性与并发控制。
        输入参数：
        - 无。
        输出参数：
        - 无（仅更新内部计数）。"""
        with self._task_lock:
            self._active_tasks += 1
    
    def _decrement_tasks(self):
        """
        执行逻辑：
        1) 加锁减少活跃任务计数。
        2) 保持任务计数与实际运行一致。
        实现方式：线程锁保护的计数自减。
        核心价值：准确反映负载状态。
        输入参数：
        - 无。
        输出参数：
        - 无（仅更新内部计数）。"""
        with self._task_lock:
            self._active_tasks -= 1

    def _is_group_reuse_enabled(self, group: str) -> bool:
        """判断指定分组是否启用文件复用。"""
        if not self.resume_control.enabled:
            return False
        if self.resume_control.mode != "file_reuse":
            return False
        return bool(self.resume_control.groups.get(group, False))

    def _cache_metrics_begin(self, task_id: str, stage: str) -> None:
        """
        执行逻辑：
        1) 按任务维度重置缓存统计（可配置）。
        2) 设置当前任务与阶段上下文。
        实现方式：调用 Module2 缓存统计器。
        核心价值：统一命中率统计口径并支持落盘。
        """
        try:
            from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics
        except Exception:
            return

        if not task_id:
            return

        if cache_metrics.reset_on_task_enabled():
            last_task = getattr(self, "_cache_metrics_task_id", None)
            if last_task != task_id:
                cache_metrics.reset()
                self._cache_metrics_task_id = task_id

        cache_metrics.set_context(task_id=task_id, stage=stage)

    def _write_cache_metrics(self, output_dir: str, task_id: str, stage: str) -> None:
        """
        执行逻辑：
        1) 生成缓存命中率快照。
        2) 追加落盘到 intermediates/cache_metrics.json。
        实现方式：JSON 追加写入。
        核心价值：形成可追溯的缓存命中率报告。
        """
        try:
            from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics
        except Exception:
            return

        try:
            import json
            snapshot = cache_metrics.snapshot(task_id=task_id, stage=stage)
            intermediates_dir = os.path.join(output_dir, "intermediates")
            os.makedirs(intermediates_dir, exist_ok=True)
            out_path = os.path.join(intermediates_dir, "cache_metrics.json")

            records = []
            if os.path.exists(out_path):
                try:
                    with open(out_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            records = data
                        elif isinstance(data, dict):
                            records = [data]
                except Exception:
                    records = []

            records.append(snapshot)
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Cache metrics write failed: {e}")

    def _append_resume_report(
        self,
        output_dir: str,
        task_id: str,
        stage: str,
        group: str,
        resource_path: str,
        action: str,
        reason: str,
        priority: bool,
    ) -> None:
        """追加写入断点重续报告。"""
        try:
            intermediates_dir = os.path.join(output_dir, "intermediates")
            os.makedirs(intermediates_dir, exist_ok=True)
            report_path = os.path.join(intermediates_dir, "resume_report.json")

            record = {
                "timestamp": _utc_now_iso(),
                "task_id": task_id,
                "stage": stage,
                "group": group,
                "resource_path": os.path.abspath(resource_path),
                "action": action,
                "reason": reason,
                "priority": priority,
            }

            with self._resume_report_lock:
                records: List[Dict[str, Any]] = []
                if os.path.exists(report_path):
                    try:
                        with open(report_path, "r", encoding="utf-8") as file:
                            raw = json.load(file)
                            if isinstance(raw, list):
                                records = raw
                            elif isinstance(raw, dict):
                                records = [raw]
                    except Exception:
                        records = []
                records.append(record)
                with open(report_path, "w", encoding="utf-8") as file:
                    json.dump(records, file, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.warning(f"Append resume report failed: {exc}")

    def _cleanup_non_priority_resources(self, output_dir: str, task_id: str) -> None:
        """按 TTL 清理非优先资源（优先资源仅限 Phase2A）。"""
        retention_days = self.resume_control.non_priority_retention_days
        if retention_days < 0:
            return

        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        storage_root = Path(output_dir)
        if not storage_root.exists():
            return

        removed_count = 0
        removed_meta_count = 0

        for meta_path in storage_root.rglob("*.meta.json"):
            try:
                with open(meta_path, "r", encoding="utf-8") as file:
                    meta = json.load(file)
                if not isinstance(meta, dict):
                    continue

                created_at = _safe_parse_iso_datetime(str(meta.get("created_at", "")))
                if not created_at:
                    continue
                if created_at > cutoff:
                    continue

                is_priority = bool(meta.get("priority", False))
                if self.resume_control.priority_keep_only_phase2a and is_priority:
                    continue

                resource_path = str(meta.get("resource_path", "")).strip()
                if resource_path and os.path.exists(resource_path):
                    try:
                        os.remove(resource_path)
                        removed_count += 1
                    except Exception:
                        pass

                try:
                    os.remove(meta_path)
                    removed_meta_count += 1
                except Exception:
                    pass
            except Exception:
                continue

        if removed_count or removed_meta_count:
            logger.info(
                "[%s] cleanup non-priority resources done: removed_files=%s removed_meta=%s ttl_days=%s",
                task_id,
                removed_count,
                removed_meta_count,
                retention_days,
            )

    def _create_ephemeral_frame_registry(self, expected_frames: int) -> SharedFrameRegistry:
        """按批次创建独立 SHM 注册表，避免跨批次 LRU 淘汰相互影响。"""
        safe_expected = max(1, int(expected_frames))
        headroom = max(4, safe_expected // 10)
        return SharedFrameRegistry(max_frames=safe_expected + headroom)

    def _cleanup_ephemeral_frame_registry(self, registry: Optional[SharedFrameRegistry]) -> None:
        """释放批次级 SHM 资源，避免命名空间和内存残留。"""
        if registry is None:
            return
        try:
            registry.cleanup()
        except Exception as exc:
            logger.debug(f"Cleanup ephemeral SHM registry failed: {exc}")
             
    def _batch_read_frames_to_shm(
        self,
        video_path: str,
        units_data: list,
        frame_registry: Optional[SharedFrameRegistry] = None,
    ) -> dict:
        """
        执行逻辑：
        1) 为每个单元采样 start/mid/end 三个时间点。
        2) 批量读取视频帧并写入 SharedMemory。
        3) 返回 unit_id 到帧引用的映射。
        实现方式：OpenCV VideoCapture + SharedFrameRegistry。
        核心价值：减少重复 IO，提升 CV 并行吞吐。
        决策逻辑：
        - 条件：not cap.isOpened()
        - 条件：frame_idx in valid_shm_refs
        - 条件：int(curr) != frame_idx
        依据来源（证据链）：
        - 视频元数据：FPS、总帧数。
        输入参数：
        - video_path: 文件路径（类型：str）。
        - units_data: 函数入参（类型：list）。
        输出参数：
        - dict：{unit_id: {frame_idx: shm_ref}}。"""
        shm_map = {} # unit_id -> {frame_idx: shm_ref}
        registry = frame_registry or self.frame_registry
        import cv2
        import time
        import threading
        
        # 1. Collect Requests
        frame_requests = [] # (time, unit_id)
        for u in units_data:
            uid = u['unit_id']
            # Start, Mid, End frames strategy
            start, end = u['start_sec'], u['end_sec']
            frame_requests.append((start, uid))
            frame_requests.append(((start+end)/2, uid))
            frame_requests.append((end, uid))
            
        # Sort by time to optimize seeking
        frame_requests.sort(key=lambda x: x[0])
        if not frame_requests:
            return {}
        
        # 为顺序读 + 采样 + 并行解码做准备
        open_start = time.perf_counter()
        cap = cv2.VideoCapture(video_path)
        open_ms = (time.perf_counter() - open_start) * 1000.0
        try:
            if not cap.isOpened():
                return {}
            fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            frame_idx_set = set()
            for req_time, _uid in frame_requests:
                frame_idx = int(req_time * fps)
                frame_idx = max(0, min(frame_idx, total_frames - 1))
                frame_idx_set.add(frame_idx)
            if not frame_idx_set:
                return {}
            frame_idx_list = sorted(frame_idx_set)
            start_idx = frame_idx_list[0]
            end_idx = frame_idx_list[-1]
            
            # 动态并行度：按“解码跨度(range_span)”自动升到 2~4 路并行解码
            # 说明：
            # - 目标帧很少但跨度很大时，顺序读仍需要解码整段区间；此时用 range_span 决策更靠谱。
            # - 过小区间并行收益不明显，反而增加额外 open/seek 开销。
            range_span = end_idx - start_idx + 1
            worker_count = 1
            if range_span >= 150:
                worker_count = 2
            if range_span >= 600:
                worker_count = 3
            if range_span >= 1500:
                worker_count = 4
            # 若目标帧很多，也允许提升并行度（coarse 场景）
            if len(frame_idx_list) >= 60:
                worker_count = max(worker_count, 2)
            if len(frame_idx_list) >= 150:
                worker_count = max(worker_count, 3)
            if len(frame_idx_list) >= 300:
                worker_count = max(worker_count, 4)
            worker_count = min(worker_count, 4)
            logger.info(
                f"Decode workers decision: target_frames={len(frame_idx_list)}, "
                f"range_span={range_span} -> workers={worker_count}"
            )
            
            def _decode_range(seg_start: int, seg_end: int):
                t_open = time.perf_counter()
                local_cap = cv2.VideoCapture(video_path)
                local_open_ms = (time.perf_counter() - t_open) * 1000.0
                if not local_cap.isOpened():
                    return {}, local_open_ms, 0.0, 0.0, 0.0
                
                t_seek = time.perf_counter()
                local_cap.set(cv2.CAP_PROP_POS_FRAMES, seg_start)
                local_seek_ms = (time.perf_counter() - t_seek) * 1000.0
                
                local_read_ms = 0.0
                local_shm_ms = 0.0
                local_refs = {}
                curr_idx = seg_start
                
                while curr_idx <= seg_end:
                    t0 = time.perf_counter()
                    ret, frame = local_cap.read()
                    local_read_ms += (time.perf_counter() - t0) * 1000.0
                    if not ret or frame is None:
                        break
                    if curr_idx in frame_idx_set:
                        t1 = time.perf_counter()
                        registry.register_frame(curr_idx, frame)
                        ref = registry.get_shm_ref(curr_idx)
                        if ref:
                            local_refs[curr_idx] = ref
                        local_shm_ms += (time.perf_counter() - t1) * 1000.0
                    del frame
                    curr_idx += 1
                
                local_cap.release()
                return local_refs, local_open_ms, local_seek_ms, local_read_ms, local_shm_ms
            
            # 单线程或并行解码
            valid_shm_refs = {}
            seek_ms = 0.0
            read_ms = 0.0
            shm_ms = 0.0
            if worker_count <= 1:
                seek_start = time.perf_counter()
                cap.set(cv2.CAP_PROP_POS_FRAMES, start_idx)
                seek_ms = (time.perf_counter() - seek_start) * 1000.0
                curr_idx = start_idx
                while curr_idx <= end_idx:
                    t0 = time.perf_counter()
                    ret, frame = cap.read()
                    read_ms += (time.perf_counter() - t0) * 1000.0
                    if not ret or frame is None:
                        break
                    if curr_idx in frame_idx_set:
                        t1 = time.perf_counter()
                        registry.register_frame(curr_idx, frame)
                        ref = registry.get_shm_ref(curr_idx)
                        if ref:
                            valid_shm_refs[curr_idx] = ref
                        shm_ms += (time.perf_counter() - t1) * 1000.0
                    del frame
                    curr_idx += 1
            else:
                # 分段并行顺序读
                seg_size = (range_span + worker_count - 1) // worker_count
                ranges = []
                for i in range(worker_count):
                    seg_start = start_idx + i * seg_size
                    seg_end = min(end_idx, seg_start + seg_size - 1)
                    if seg_start <= seg_end:
                        ranges.append((seg_start, seg_end))
                
                with futures.ThreadPoolExecutor(max_workers=len(ranges), thread_name_prefix="cv_decode") as executor:
                    future_list = [executor.submit(_decode_range, s, e) for s, e in ranges]
                    for fut in future_list:
                        local_refs, o_ms, s_ms, r_ms, m_ms = fut.result()
                        valid_shm_refs.update(local_refs)
                        open_ms += o_ms
                        seek_ms += s_ms
                        read_ms += r_ms
                        shm_ms += m_ms
            
            # Re-map results to Unit ID
            for u in units_data:
                uid = u['unit_id']
                u_map = {}
                start, end = u['start_sec'], u['end_sec']
                for t in [start, (start+end)/2, end]:
                    idx = int(t * fps)
                    idx = max(0, min(idx, total_frames - 1))
                    if idx in valid_shm_refs:
                        u_map[str(idx)] = valid_shm_refs[idx]
                if u_map:
                    shm_map[uid] = u_map
                    
            total_ms = open_ms + seek_ms + read_ms + shm_ms
            logger.info(
                f"Batch read frames: open={open_ms:.1f}ms, seek={seek_ms:.1f}ms, "
                f"read={read_ms:.1f}ms, shm={shm_ms:.1f}ms, total={total_ms:.1f}ms, "
                f"frames={len(frame_idx_list)}, range=[{start_idx},{end_idx}], workers={worker_count}"
            )
            return shm_map
        except Exception as e:
            logger.warning(f"Batch read failed: {e}")
            return {}
        finally:
            cap.release()
    
    def _batch_read_coarse_frames_to_shm(
        self, 
        video_path: str, 
        units_data: list,
        coarse_fps: float = 2.0,
        frame_registry: Optional[SharedFrameRegistry] = None,
    ) -> dict:
        """
        执行逻辑：
        1) 按 coarse_fps 在区间内均匀采样时间点。
        2) 批量读取帧并写入 SharedMemory。
        3) 返回 unit_id 到采样帧引用的映射。
        实现方式：OpenCV VideoCapture + SharedFrameRegistry。
        核心价值：为先粗后细路径提供高效帧采样。
        决策逻辑：
        - 条件：not cap.isOpened()
        - 条件：frame_idx in valid_refs
        - 条件：curr != frame_idx
        依据来源（证据链）：
        - 视频元数据：FPS、总帧数。
        输入参数：
        - video_path: 文件路径（类型：str）。
        - units_data: 函数入参（类型：list）。
        - coarse_fps: 函数入参（类型：float）。
        输出参数：
        - dict：{unit_id: {timestamp: shm_ref}}。"""
        import cv2
        import time
        registry = frame_registry or self.frame_registry
        
        coarse_interval = 1.0 / coarse_fps
        coarse_shm_map = {}
        open_start = time.perf_counter()
        cap = cv2.VideoCapture(video_path)
        open_ms = (time.perf_counter() - open_start) * 1000.0
        try:
            if not cap.isOpened():
                return {}
            
            fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            # 收集所有需要读取的时间戳
            all_requests = []  # (timestamp, unit_id)
            for u in units_data:
                uid = u["unit_id"]
                start_sec = u["start_sec"]
                end_sec = u["end_sec"]
                
                t = start_sec
                while t < end_sec:
                    all_requests.append((t, uid))
                    t += coarse_interval
                # 兜底：区间过短时至少采样 start/mid/end
                if start_sec >= end_sec or not any(req_uid == uid for _ts, req_uid in all_requests):
                    mid = (start_sec + end_sec) / 2
                    all_requests.extend([(start_sec, uid), (mid, uid), (end_sec, uid)])
            
            if not all_requests:
                return {}
            all_requests.sort(key=lambda x: x[0])
            
            # 批量读取（顺序读 + 采样）
            frame_to_requests = {}  # frame_idx -> list[(uid, req_time)]
            for req_time, uid in all_requests:
                frame_idx = int(req_time * fps)
                frame_idx = max(0, min(frame_idx, total_frames - 1))
                frame_to_requests.setdefault(frame_idx, []).append((uid, req_time))
            
            frame_idx_list = sorted(frame_to_requests.keys())
            start_idx = frame_idx_list[0]
            end_idx = frame_idx_list[-1]
            
            range_span = end_idx - start_idx + 1
            worker_count = 1
            if range_span >= 150:
                worker_count = 2
            if range_span >= 600:
                worker_count = 3
            if range_span >= 1500:
                worker_count = 4
            if len(frame_idx_list) >= 60:
                worker_count = max(worker_count, 2)
            if len(frame_idx_list) >= 150:
                worker_count = max(worker_count, 3)
            if len(frame_idx_list) >= 300:
                worker_count = max(worker_count, 4)
            worker_count = min(worker_count, 4)
            logger.info(
                f"Decode workers decision (coarse): target_frames={len(frame_idx_list)}, "
                f"range_span={range_span} -> workers={worker_count}"
            )
            
            def _decode_range(seg_start: int, seg_end: int):
                t_open = time.perf_counter()
                local_cap = cv2.VideoCapture(video_path)
                local_open_ms = (time.perf_counter() - t_open) * 1000.0
                if not local_cap.isOpened():
                    return {}, local_open_ms, 0.0, 0.0, 0.0
                
                t_seek = time.perf_counter()
                local_cap.set(cv2.CAP_PROP_POS_FRAMES, seg_start)
                local_seek_ms = (time.perf_counter() - t_seek) * 1000.0
                
                local_read_ms = 0.0
                local_shm_ms = 0.0
                local_refs = {}
                curr_idx = seg_start
                
                while curr_idx <= seg_end:
                    t0 = time.perf_counter()
                    ret, frame = local_cap.read()
                    local_read_ms += (time.perf_counter() - t0) * 1000.0
                    if not ret or frame is None:
                        break
                    if curr_idx in frame_to_requests:
                        t1 = time.perf_counter()
                        registry.register_frame(curr_idx, frame)
                        ref = registry.get_shm_ref(curr_idx)
                        if ref:
                            local_refs[curr_idx] = ref
                        local_shm_ms += (time.perf_counter() - t1) * 1000.0
                    del frame
                    curr_idx += 1
                
                local_cap.release()
                return local_refs, local_open_ms, local_seek_ms, local_read_ms, local_shm_ms
            
            seek_ms = 0.0
            read_ms = 0.0
            shm_ms = 0.0
            valid_refs = {}
            if worker_count <= 1:
                seek_start = time.perf_counter()
                cap.set(cv2.CAP_PROP_POS_FRAMES, start_idx)
                seek_ms = (time.perf_counter() - seek_start) * 1000.0
                
                curr_idx = start_idx
                while curr_idx <= end_idx:
                    t0 = time.perf_counter()
                    ret, frame = cap.read()
                    read_ms += (time.perf_counter() - t0) * 1000.0
                    if not ret or frame is None:
                        break
                    if curr_idx in frame_to_requests:
                        t1 = time.perf_counter()
                        registry.register_frame(curr_idx, frame)
                        ref = registry.get_shm_ref(curr_idx)
                        if ref:
                            valid_refs[curr_idx] = ref
                    shm_ms += (time.perf_counter() - t1) * 1000.0
                    del frame
                    curr_idx += 1
            else:
                seg_size = (range_span + worker_count - 1) // worker_count
                ranges = []
                for i in range(worker_count):
                    seg_start = start_idx + i * seg_size
                    seg_end = min(end_idx, seg_start + seg_size - 1)
                    if seg_start <= seg_end:
                        ranges.append((seg_start, seg_end))
                
                with futures.ThreadPoolExecutor(max_workers=len(ranges), thread_name_prefix="cv_decode") as executor:
                    future_list = [executor.submit(_decode_range, s, e) for s, e in ranges]
                    for fut in future_list:
                        local_refs, o_ms, s_ms, r_ms, m_ms = fut.result()
                        valid_refs.update(local_refs)
                        open_ms += o_ms
                        seek_ms += s_ms
                        read_ms += r_ms
                        shm_ms += m_ms
            
            for frame_idx, ref in valid_refs.items():
                for uid, req_time in frame_to_requests.get(frame_idx, []):
                    if uid not in coarse_shm_map:
                        coarse_shm_map[uid] = {}
                    coarse_shm_map[uid][req_time] = ref

            # 兜底：若粗采样仍不足，回退到 3 点采样（start/mid/end）
            missing_units = []
            for u in units_data:
                uid = u["unit_id"]
                if uid not in coarse_shm_map or len(coarse_shm_map[uid]) < 2:
                    missing_units.append(u)
            if missing_units:
                logger.warning(
                    f"Coarse frames insufficient for {len(missing_units)} units, fallback to 3-point sampling"
                )
                fallback_map = self._batch_read_frames_to_shm(
                    video_path,
                    missing_units,
                    frame_registry=registry,
                )
                for u in missing_units:
                    uid = u["unit_id"]
                    if uid not in coarse_shm_map:
                        coarse_shm_map[uid] = {}
                    start_sec = u["start_sec"]
                    end_sec = u["end_sec"]
                    mid = (start_sec + end_sec) / 2
                    for t in (start_sec, mid, end_sec):
                        frame_idx = int(t * fps)
                        frame_idx = max(0, min(frame_idx, total_frames - 1))
                        ref = None
                        if uid in fallback_map:
                            ref = fallback_map[uid].get(str(frame_idx)) or fallback_map[uid].get(frame_idx)
                        if ref:
                            coarse_shm_map[uid][t] = ref
            
            total_ms = open_ms + seek_ms + read_ms + shm_ms
            logger.info(
                f"Coarse batch read: open={open_ms:.1f}ms, seek={seek_ms:.1f}ms, "
                f"read={read_ms:.1f}ms, shm={shm_ms:.1f}ms, total={total_ms:.1f}ms, "
                f"frames={len(frame_idx_list)}, range=[{start_idx},{end_idx}], workers={worker_count}"
            )
            return coarse_shm_map
            
        except Exception as e:
            logger.warning(f"Coarse batch read failed: {e}")
            return {}
        finally:
            cap.release()
    
    async def _batch_read_frames_for_screenshots(
        self, 
        video_path: str, 
        screenshot_tasks: List[dict],
        frame_registry: Optional[SharedFrameRegistry] = None,
    ) -> Dict[str, Dict[float, dict]]:
        """
        执行逻辑：
        1) 依据截图范围按 0.5s 采样时间点。
        2) 读取对应帧并写入 SharedMemory。
        3) 返回每个截图任务对应的帧引用映射。
        实现方式：OpenCV VideoCapture + SharedFrameRegistry。
        核心价值：批量读帧减少 IO，提升截图选择速度。
        决策逻辑：
        - 条件：frame_map
        - 条件：not ret or frame is None
        - 条件：shm_ref
        依据来源（证据链）：
        - 视频元数据：FPS、总帧数。
        输入参数：
        - video_path: 文件路径（类型：str）。
        - screenshot_tasks: 函数入参（类型：List[dict]）。
        输出参数：
        - dict：{unit_id_island: {timestamp: shm_ref}}。"""
        import cv2
        registry = frame_registry or self.frame_registry
        
        shm_map = {}
        cap = cv2.VideoCapture(video_path)

        try:
            fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

            # 1) 收集任务采样点并建立 task -> frame_idx 映射
            task_to_frame_idxs: Dict[str, List[int]] = {}
            all_frame_idx_set = set()
            for task in screenshot_tasks:
                key = f"{task['unit_id']}_island{task['island_index']}"
                start_sec = float(task['expanded_start'])
                end_sec = float(task['expanded_end'])

                timestamps = np.arange(start_sec, end_sec + 0.1, 0.5)
                frame_idxs = []
                for ts in timestamps:
                    idx = int(float(ts) * fps)
                    idx = max(0, min(idx, max(0, total_frames - 1)))
                    frame_idxs.append(idx)
                    all_frame_idx_set.add(idx)
                task_to_frame_idxs[key] = frame_idxs

            if not all_frame_idx_set:
                return {}

            # 2) 顺序读 + 并行解码（复用 _batch_read_frames_to_shm 的高吞吐路径）
            frame_idx_list = sorted(all_frame_idx_set)
            start_idx = frame_idx_list[0]
            end_idx = frame_idx_list[-1]
            range_span = end_idx - start_idx + 1

            worker_count = 1
            if range_span >= 150:
                worker_count = 2
            if range_span >= 600:
                worker_count = 3
            if range_span >= 1500:
                worker_count = 4
            if len(frame_idx_list) >= 60:
                worker_count = max(worker_count, 2)
            if len(frame_idx_list) >= 150:
                worker_count = max(worker_count, 3)
            if len(frame_idx_list) >= 300:
                worker_count = max(worker_count, 4)
            worker_count = min(worker_count, 4)

            open_ms = 0.0
            seek_ms = 0.0
            read_ms = 0.0
            shm_ms = 0.0
            valid_shm_refs = {}

            def _decode_range(seg_start: int, seg_end: int):
                t_open = time.perf_counter()
                local_cap = cv2.VideoCapture(video_path)
                local_open_ms = (time.perf_counter() - t_open) * 1000.0
                if not local_cap.isOpened():
                    return {}, local_open_ms, 0.0, 0.0, 0.0

                t_seek = time.perf_counter()
                local_cap.set(cv2.CAP_PROP_POS_FRAMES, seg_start)
                local_seek_ms = (time.perf_counter() - t_seek) * 1000.0

                local_refs = {}
                local_read_ms = 0.0
                local_shm_ms = 0.0
                curr_idx = seg_start
                while curr_idx <= seg_end:
                    t0 = time.perf_counter()
                    ret, frame = local_cap.read()
                    local_read_ms += (time.perf_counter() - t0) * 1000.0
                    if not ret or frame is None:
                        break
                    if curr_idx in all_frame_idx_set:
                        t1 = time.perf_counter()
                        registry.register_frame(curr_idx, frame)
                        ref = registry.get_shm_ref(curr_idx)
                        if ref:
                            local_refs[curr_idx] = ref
                        local_shm_ms += (time.perf_counter() - t1) * 1000.0
                    del frame
                    curr_idx += 1

                local_cap.release()
                return local_refs, local_open_ms, local_seek_ms, local_read_ms, local_shm_ms

            if worker_count <= 1:
                seek_start = time.perf_counter()
                cap.set(cv2.CAP_PROP_POS_FRAMES, start_idx)
                seek_ms = (time.perf_counter() - seek_start) * 1000.0
                curr_idx = start_idx
                while curr_idx <= end_idx:
                    t0 = time.perf_counter()
                    ret, frame = cap.read()
                    read_ms += (time.perf_counter() - t0) * 1000.0
                    if not ret or frame is None:
                        break
                    if curr_idx in all_frame_idx_set:
                        t1 = time.perf_counter()
                        registry.register_frame(curr_idx, frame)
                        ref = registry.get_shm_ref(curr_idx)
                        if ref:
                            valid_shm_refs[curr_idx] = ref
                        shm_ms += (time.perf_counter() - t1) * 1000.0
                    del frame
                    curr_idx += 1
            else:
                seg_size = (range_span + worker_count - 1) // worker_count
                ranges = []
                for i in range(worker_count):
                    seg_start = start_idx + i * seg_size
                    seg_end = min(end_idx, seg_start + seg_size - 1)
                    if seg_start <= seg_end:
                        ranges.append((seg_start, seg_end))

                with futures.ThreadPoolExecutor(max_workers=len(ranges), thread_name_prefix="ss_decode") as executor:
                    future_list = [executor.submit(_decode_range, s, e) for s, e in ranges]
                    for fut in future_list:
                        local_refs, o_ms, s_ms, r_ms, m_ms = fut.result()
                        valid_shm_refs.update(local_refs)
                        open_ms += o_ms
                        seek_ms += s_ms
                        read_ms += r_ms
                        shm_ms += m_ms

            # 3) 回填任务映射
            for key, idx_list in task_to_frame_idxs.items():
                frame_map = {}
                if not idx_list:
                    continue
                for idx in idx_list:
                    ref = valid_shm_refs.get(idx)
                    if ref:
                        # 保持原接口：timestamp key 为 float，值为 shm_ref
                        ts = float(idx / fps)
                        frame_map[ts] = ref
                if frame_map:
                    shm_map[key] = frame_map

            total_ms = open_ms + seek_ms + read_ms + shm_ms
            logger.info(
                f"✅ Batch read screenshot frames: tasks={len(shm_map)}, "
                f"open={open_ms:.1f}ms, seek={seek_ms:.1f}ms, read={read_ms:.1f}ms, "
                f"shm={shm_ms:.1f}ms, total={total_ms:.1f}ms, frames={len(frame_idx_list)}, workers={worker_count}"
            )
            return shm_map

        except Exception as e:
            logger.error(f"❌ Batch read for screenshots failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}
        finally:
            cap.release()
    

    async def _validation_validate_cv_batch_impl(self, request, context):
        """
        执行逻辑：
        1) 按知识类型分流：process 走完整 CV，abstract/concrete 走先粗后细。
        2) 动态计算批大小并分块读帧到 SharedMemory。
        3) 提交 CV Worker 计算稳定岛与动作段，流式返回。
        实现方式：ProcessPool + SharedFrameRegistry + asyncio 流式输出。
        核心价值：在资源可控前提下提升 CV 批处理吞吐。
        决策逻辑：
        - 条件：skipped_units
        - 条件：kt in ('abstract', '讲解型', '抽象', 'concrete', '具象')
        - 条件：all_units_data
        依据来源（证据链）：
        - 输入参数：request.semantic_units、knowledge_type。
        - 系统指标：psutil.virtual_memory().available。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - CVValidationResponse 流（包含稳定岛与动作段结果）。"""
        from services.python_grpc.src.vision_validation.worker import run_cv_validation_task
        import functools
        
        task_id = request.task_id
        video_path = os.path.abspath(request.video_path)
        semantic_units = request.semantic_units
        
        logger.info(f"[{task_id}] ValidateCVBatch Start: {len(semantic_units)} units")
        
        try:
            self._increment_tasks()
            
            # 序列化所有数据
            all_units_data = []
            skipped_units = []  # 🚀 剪枝: 跳过 abstract/讲解型
            
            for u in semantic_units:
                unit_data = {
                    "unit_id": u.unit_id,
                    "start_sec": u.start_sec,
                    "end_sec": u.end_sec,
                    "knowledge_type": u.knowledge_type
                }
                
                # 🚀 Knowledge Type 剪枝: 仅对 process 执行完整 CV 检测
                # concrete/abstract/讲解型 使用先粗后细截图选择
                kt = u.knowledge_type.lower() if u.knowledge_type else ""
                if kt in ("abstract", "讲解型", "抽象", "concrete", "具象"):
                    # 跳过抽象/具象类型，不执行 CV 动作单元检测
                    skipped_units.append(unit_data)
                else:
                    # 仅 process/过程性 类型执行完整 CV 检测
                    all_units_data.append(unit_data)
            
            if skipped_units:
                logger.info(f"[{task_id}] Pruned {len(skipped_units)} concrete/abstract units → coarse-fine screenshot path")
            
            # 🚀 Chunked Processing
            # [FIX] 动态计算 Batch Size
            # 基础: 50. 依据内存向上调整.
            mem = psutil.virtual_memory()
            avail_gb = mem.available / (1024**3)
            # 策略: 每 4GB 空闲内存增加 30 任务量.
            # 16GB 空闲 -> +120 -> 170. 4GB 空闲 -> 50.
            dynamic_batch = 50 + int((max(0, avail_gb - 4) / 4) * 30)
            # 为了提高单任务时延与 IO/Compute 重叠，降低 batch 上限
            MIN_BATCH_SIZE = 8
            MAX_BATCH_SIZE = 24
            BATCH_SIZE = min(max(MIN_BATCH_SIZE, dynamic_batch), MAX_BATCH_SIZE)  # 初始限制范围 [8, 24]
            # 单任务时延优先：根据单元数量强制生成多个 chunk
            # 目标：尽量保证 >= 3 个 chunk，形成 IO/Compute 重叠
            units_total = len(all_units_data) + len(skipped_units)
            units_max = units_total
            TARGET_MIN_CHUNKS = 5
            if units_max > 0:
                latency_batch = max(1, (units_max + TARGET_MIN_CHUNKS - 1) // TARGET_MIN_CHUNKS)
                BATCH_SIZE = min(BATCH_SIZE, latency_batch)
                logger.info(
                    f"[{task_id}] Latency batch adjust: units_max={units_max}, "
                    f"target_chunks={TARGET_MIN_CHUNKS}, batch={BATCH_SIZE}"
                )
            
            logger.info(f"[{task_id}] Dynamic Batch Config: Size={BATCH_SIZE} (Available RAM={avail_gb:.1f}GB)")
            results_data = []
            cf_results_data = []  # 先粗后细结果
            total_io_ms = 0.0
            total_tasks = 0
            io_chunks = 0
            completed_tasks = 0
            
            loop = asyncio.get_running_loop()
            
            # 流式门闸：按批次读取 + 计算 + 回传，IO/Compute 重叠
            COARSE_FPS = 2.0
            coarse_interval = 1.0 / COARSE_FPS

            def _estimate_task_frames(task_type: str, unit: dict) -> int:
                if task_type == "cv":
                    return 3
                start_sec = float(unit.get("start_sec", 0.0))
                end_sec = float(unit.get("end_sec", start_sec))
                return max(3, int(max(0.0, end_sec - start_sec) * COARSE_FPS) + 1)

            def chunk_list(items, size):
                if not items:
                    return []
                frame_budget = max(8, int(getattr(self.frame_registry, "max_frames", 80) * 0.9))
                chunks = []
                current = []
                current_frames = 0
                for item in items:
                    estimated = _estimate_task_frames(item.get("type", "cv"), item.get("unit", {}))
                    if current and (len(current) >= size or (current_frames + estimated) > frame_budget):
                        chunks.append(current)
                        current = []
                        current_frames = 0
                    current.append(item)
                    current_frames += estimated
                if current:
                    chunks.append(current)
                return chunks

            tasks = []
            for u in all_units_data:
                tasks.append({"type": "cv", "unit": u})
            for u in skipped_units:
                tasks.append({"type": "cf", "unit": u})
            if tasks:
                tasks.sort(key=lambda x: x["unit"].get("start_sec", 0.0))
            logger.info(
                f"[{task_id}] Task unify: cv={len(all_units_data)}, cf={len(skipped_units)}, total={len(tasks)}"
            )

            task_chunks = chunk_list(tasks, BATCH_SIZE) if tasks else []
            total_chunks = len(task_chunks)
            max_inflight = max(1, self.cv_worker_count * 2)

            logger.info(
                f"[{task_id}] Streaming gate pipeline: chunks={total_chunks}, batch={BATCH_SIZE}, "
                f"inflight={max_inflight}, mode=chunk_isolated_shm"
            )

            async def wrap_task(fut, t_type, uid):
                """
                执行逻辑：
                1) 等待任务完成并补齐 unit_id。
                2) 捕获异常并返回统一结构。
                实现方式：await 任务 + 结果类型判断。
                核心价值：保证流式结果包含上下文，避免单任务阻塞。
                决策逻辑：
                - 条件：res 为 dict 时补回 unit_id
                - 条件：任务异常时返回异常对象
                依据来源（证据链）：
                - 任务结果类型与异常捕获。
                输入参数：
                - fut: awaitable 任务。
                - t_type: 任务类型标记。
                - uid: 语义单元 ID。
                输出参数：
                - (t_type, uid, result|exception) 元组。
                """
                try:
                    res = await fut
                    if isinstance(res, dict):
                        res['unit_id'] = uid  # 补回 unit_id，确保结果可追溯
                    return t_type, uid, res
                except Exception as e:
                    logger.error(f"Task failed for {uid} ({t_type}): {e}")
                    return t_type, uid, e

            async def read_chunk(task_chunk):
                """
                执行逻辑：
                1) 并行读取 CV 与 coarse-fine 的帧到共享内存。
                2) 汇总 IO 结果与耗时并返回。
                实现方式：loop.run_in_executor + batch_read 方法。
                核心价值：IO 与计算解耦，降低等待时间。
                决策逻辑：
                - 条件：task_chunk 为空则直接返回空结果
                依据来源（证据链）：
                - 输入参数：task_chunk。
                输入参数：
                - task_chunk: 统一任务列表（cv/cf 混合）。
                输出参数：
                - (shm_map, coarse_shm_map, io_ms, cv_count, cf_count, cv_registry, cf_registry)。
                """
                io_start = time.perf_counter()
                if not task_chunk:
                    return {}, {}, 0.0, 0, 0, None, None
                cv_chunk = [t["unit"] for t in task_chunk if t["type"] == "cv"]
                cf_chunk = [t["unit"] for t in task_chunk if t["type"] == "cf"]

                cv_registry = self._create_ephemeral_frame_registry(
                    sum(_estimate_task_frames("cv", unit) for unit in cv_chunk)
                ) if cv_chunk else None
                cf_registry = self._create_ephemeral_frame_registry(
                    sum(_estimate_task_frames("cf", unit) for unit in cf_chunk)
                ) if cf_chunk else None

                try:
                    io_futures = []
                    if cv_chunk:
                        cv_io_future = loop.run_in_executor(
                            None,
                            functools.partial(
                                self._batch_read_frames_to_shm,
                                video_path,
                                cv_chunk,
                                cv_registry,
                            ),
                        )
                        io_futures.append(("cv", cv_io_future))

                    if cf_chunk:
                        cf_io_future = loop.run_in_executor(
                            None,
                            functools.partial(
                                self._batch_read_coarse_frames_to_shm,
                                video_path,
                                cf_chunk,
                                COARSE_FPS,
                                cf_registry,
                            ),
                        )
                        io_futures.append(("cf", cf_io_future))

                    io_results = {}
                    for io_type, io_future in io_futures:
                        io_results[io_type] = await io_future

                    shm_map = io_results.get("cv", {})
                    coarse_shm_map = io_results.get("cf", {})
                    io_ms = (time.perf_counter() - io_start) * 1000.0
                    return shm_map, coarse_shm_map, io_ms, len(cv_chunk), len(cf_chunk), cv_registry, cf_registry
                except Exception:
                    self._cleanup_ephemeral_frame_registry(cv_registry)
                    self._cleanup_ephemeral_frame_registry(cf_registry)
                    raise

            async def drain_completed(pending_tasks):
                if not pending_tasks:
                    return pending_tasks, 0, []
                done, pending = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
                completed_count = 0
                responses = []
                for done_task in done:
                    try:
                        task_type, unit_id, res = done_task.result()
                    except Exception as e:
                        logger.error(f"Task wrapper failed: {e}")
                        continue

                    if isinstance(res, Exception):
                        continue
                    if not isinstance(res, dict):
                        continue

                    # 1. 组装 StableIslands
                    pb_islands = []
                    for si in res.get("stable_islands", []):
                        if isinstance(si, dict):
                            pb_islands.append(video_processing_pb2.StableIsland(
                                start_sec=float(si.get("start_sec", 0.0)),
                                end_sec=float(si.get("end_sec", 0.0)),
                                mid_sec=float(si.get("mid_sec", 0.0)),
                                duration_sec=float(si.get("duration_sec", 0.0))
                            ))

                    # 2. 组装 ActionSegments
                    pb_actions = []
                    for act in res.get("action_segments", []):
                        if isinstance(act, dict):
                            internal_islands = []
                            for Isi in act.get("internal_stable_islands", []):
                                internal_islands.append(video_processing_pb2.StableIsland(
                                    start_sec=float(Isi.get("start_sec", 0.0)),
                                    end_sec=float(Isi.get("end_sec", 0.0)),
                                    mid_sec=float(Isi.get("mid_sec", 0.0)),
                                    duration_sec=float(Isi.get("duration_sec", 0.0))
                                ))

                            pb_actions.append(video_processing_pb2.ActionSegment(
                                start_sec=float(act.get("start_sec", 0.0)),
                                end_sec=float(act.get("end_sec", 0.0)),
                                action_type=str(act.get("action_type", "")),
                                internal_stable_islands=internal_islands
                            ))

                    # 3. 生成 CVValidationResult 并回传
                    pb_result = video_processing_pb2.CVValidationResult(
                        unit_id=unit_id,
                        stable_islands=pb_islands,
                        action_segments=pb_actions
                    )

                    responses.append(video_processing_pb2.CVValidationResponse(
                        success=True,
                        results=[pb_result]
                    ))
                    completed_count += 1
                return pending, completed_count, responses

            if total_chunks > 0:
                for idx, task_chunk in enumerate(task_chunks):
                    pending = set()
                    shm_map = {}
                    coarse_shm_map = {}
                    cv_registry = None
                    cf_registry = None

                    try:
                        shm_map, coarse_shm_map, io_ms, io_cv_cnt, io_cf_cnt, cv_registry, cf_registry = await read_chunk(task_chunk)
                        logger.info(
                            f"[{task_id}] Chunk {idx + 1}/{total_chunks} IO done: {io_ms:.1f}ms "
                            f"(cv_units={io_cv_cnt}, cf_units={io_cf_cnt})"
                        )
                        total_io_ms += io_ms
                        io_chunks += 1

                        if not task_chunk:
                            continue

                        # 提交任务（chunk 内受控并发）
                        from services.python_grpc.src.vision_validation.worker import run_coarse_fine_screenshot_task
                        submitted = 0
                        for t in task_chunk:
                            unit_data = t["unit"]
                            unit_id = unit_data["unit_id"]
                            if t["type"] == "cv":
                                shm_frames = shm_map.get(unit_id, None)
                                task_func = functools.partial(
                                    run_cv_validation_task,
                                    video_path,
                                    unit_data,
                                    shm_frames
                                )
                                future = loop.run_in_executor(self.cv_process_pool, task_func)
                                pending.add(asyncio.create_task(wrap_task(future, "cv", unit_id)))
                            else:
                                future = loop.run_in_executor(
                                    self.cv_process_pool,
                                    functools.partial(
                                        run_coarse_fine_screenshot_task,
                                        unit_id=unit_id,
                                        start_sec=unit_data["start_sec"],
                                        end_sec=unit_data["end_sec"],
                                        coarse_shm_frames=coarse_shm_map.get(unit_id, {}),
                                        coarse_interval=coarse_interval,
                                        fine_shm_frames_by_island=None,
                                        video_path=video_path,
                                        analysis_max_width=route_screenshot_analysis_max_width,
                                    )
                                )
                                pending.add(asyncio.create_task(wrap_task(future, "cf", unit_id)))
                            submitted += 1

                            while len(pending) >= max_inflight:
                                logger.info(
                                    f"[{task_id}] Inflight throttle: pending={len(pending)}, limit={max_inflight}"
                                )
                                pending, completed, responses = await drain_completed(pending)
                                for resp in responses:
                                    yield resp
                                completed_tasks += completed

                        total_tasks += submitted
                        logger.info(
                            f"[{task_id}] Feed chunk {idx + 1}/{total_chunks}: submitted={submitted}, inflight={len(pending)}"
                        )

                        while pending:
                            pending, completed, responses = await drain_completed(pending)
                            for resp in responses:
                                yield resp
                            completed_tasks += completed
                    finally:
                        # 先确保本 chunk 消费完成，再释放该 chunk 的 SHM 生命周期。
                        while pending:
                            pending, completed, responses = await drain_completed(pending)
                            for resp in responses:
                                yield resp
                            completed_tasks += completed

                        self._cleanup_ephemeral_frame_registry(cv_registry)
                        self._cleanup_ephemeral_frame_registry(cf_registry)

                        if 'shm_map' in locals():
                            del shm_map
                        if 'coarse_shm_map' in locals():
                            del coarse_shm_map
                        gc.collect()
            if total_chunks > 0:
                avg_io = total_io_ms / max(1, io_chunks)
                logger.info(
                    f"[{task_id}] CVBatch totals: io={total_io_ms:.1f}ms (avg/chunk={avg_io:.1f}ms), "
                    f"tasks={total_tasks}, completed={completed_tasks}, chunks={total_chunks}"
                )
            logger.info(f"[{task_id}] ValidateCVBatch Streaming Complete")
            
        except Exception as e:
            logger.error(f"[{task_id}] ValidateCVBatch Failed: {e}")
            logger.error(traceback.format_exc())
            yield video_processing_pb2.CVValidationResponse(
                success=False,
                results=[],
                error_msg=str(e)
            )

    # ========== 🚀 V6: 资源释放 ==========
    

    async def _validation_analyze_with_vl_impl(self, request, context):
        """
        🔥 V7: VL-Based Analysis - 使用 Qwen3-VL-Plus 直接分析视频
        
        完全跳过 CV/LLM 流程，直接使用视觉语言模型分析视频片段。
        
        流程：
        1. 检查 VL 配置是否启用
        2. 加载 semantic_units JSON
        3. 对每个语义单元调用 VL 分析
        4. 生成截图/片段请求（讲解型仅截图）
        5. 返回结果供 Java FFmpeg 提取
        
        参数:
            request: VLAnalysisRequest
            context: gRPC context
            
        返回:
            VLAnalysisResponse
        """
        logger.info("===== AnalyzeWithVL Request Received =====")
        task_id = request.task_id
        # 统一将本地视频归档到 storage/{hash}，确保 VL 阶段与前序阶段落盘同域
        video_path = _ensure_local_video_in_storage(request.video_path)
        output_dir = _normalize_output_dir(video_path) if video_path else (request.output_dir or "")
        # 统一语义单元持久化路径：不再接收 Java 传入路径，仅在服务内用于报表/兜底持久化。
        semantic_units_path = os.path.join(output_dir, "semantic_units_phase2a.json")
        semantic_source_case = request.WhichOneof("semantic_units_source") if hasattr(request, "WhichOneof") else None
        vl_model_name = "qwen-vl-max-2025-08-13"

        vl_report_writer = VLReportWriter(
            task_id=task_id,
            video_path=video_path,
            semantic_units_path=semantic_units_path,
            output_dir=output_dir,
            logger=logger,
        )

        def _sync_vl_report_context() -> None:
            vl_report_writer.task_id = task_id
            vl_report_writer.video_path = video_path
            vl_report_writer.semantic_units_path = semantic_units_path
            vl_report_writer.output_dir = output_dir

        def _persist_task_token_report(payload: dict) -> str:
            _sync_vl_report_context()
            return vl_report_writer.persist_token_report(payload=payload or {}, vl_model=vl_model_name)

        def _persist_vl_analysis_output(payload: dict) -> str:
            _sync_vl_report_context()
            return vl_report_writer.persist_analysis_output(payload=payload or {}, vl_model=vl_model_name)

        def _summarize_vl_unit_failures(
            unit_outputs: Optional[List[Dict[str, Any]]],
            *,
            limit: int = 10,
        ) -> List[Dict[str, Any]]:
            summarized: List[Dict[str, Any]] = []
            for item in list(unit_outputs or []):
                if not isinstance(item, dict):
                    continue
                if bool(item.get("success", False)):
                    continue
                interactions = list(item.get("raw_llm_interactions", []) or [])
                last_interaction = interactions[-1] if interactions else {}
                summarized.append(
                    {
                        "unit_id": str(item.get("unit_id", "") or ""),
                        "analysis_mode": str(item.get("analysis_mode", "") or "").strip().lower(),
                        "error_msg": str(item.get("error_msg", item.get("error", "")) or ""),
                        "llm_attempts": len(interactions),
                        "last_llm_error": str(last_interaction.get("error", "") or ""),
                    }
                )
                if len(summarized) >= max(1, int(limit)):
                    break
            return summarized

        def _persist_vl_last_error(payload: dict) -> str:
            _sync_vl_report_context()
            try:
                intermediates_dir = os.path.join(output_dir, "intermediates")
                os.makedirs(intermediates_dir, exist_ok=True)
                error_path = os.path.join(intermediates_dir, "vl_last_error.json")
                error_payload = {
                    "task_id": task_id,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "vl_model": vl_model_name,
                    "video_path": video_path,
                    "semantic_units_path": semantic_units_path,
                }
                if isinstance(payload, dict):
                    error_payload.update(payload)
                enqueue_json_write(
                    error_path,
                    error_payload,
                    ensure_ascii=False,
                    indent=2,
                    scope_key=output_dir,
                )
                logger.info(f"[{task_id}] queued VL last error snapshot: path={error_path}")
                return error_path
            except Exception as persist_error:
                logger.warning(f"[{task_id}] queue VL last error snapshot failed: {persist_error}")
                return ""

        logger.info(
            f"[{task_id}] AnalyzeWithVL 开始: video={video_path}, units_json={semantic_units_path}, "
            f"semantic_source={semantic_source_case or 'runtime_or_empty'}"
        )

        vl_watchdog: Optional[TaskWatchdogSignalWriter] = None
        try:
            vl_watchdog = TaskWatchdogSignalWriter(
                task_id=task_id,
                output_dir=output_dir,
                stage="analysis_extraction",
                total_steps=1,
            )
        except Exception as watchdog_init_error:
            logger.warning(f"[{task_id}] AnalyzeWithVL watchdog init failed: {watchdog_init_error}")

        vl_heartbeat_stop = threading.Event()
        vl_heartbeat_thread: Optional[threading.Thread] = None
        vl_heartbeat_lock = threading.Lock()
        vl_heartbeat_state: Dict[str, Any] = {
            "status": "running",
            "checkpoint": "analyze_with_vl_start",
            "completed": 0,
            "pending": 1,
        }
        vl_budget_seconds = 1
        vl_started_at_sec = 0.0

        def _update_vl_heartbeat_state(
            *,
            status: Optional[str] = None,
            checkpoint: Optional[str] = None,
            completed: Optional[int] = None,
            pending: Optional[int] = None,
        ) -> None:
            with vl_heartbeat_lock:
                if status is not None:
                    vl_heartbeat_state["status"] = str(status).strip().lower() or "running"
                if checkpoint is not None:
                    vl_heartbeat_state["checkpoint"] = str(checkpoint).strip() or "unknown"
                if completed is not None:
                    vl_heartbeat_state["completed"] = max(0, int(completed))
                if pending is not None:
                    vl_heartbeat_state["pending"] = max(0, int(pending))

        def _emit_vl_heartbeat(signal_type: str = "hard") -> None:
            if vl_watchdog is None:
                return
            try:
                with vl_heartbeat_lock:
                    snapshot = dict(vl_heartbeat_state)
                vl_watchdog.emit(
                    status=str(snapshot.get("status", "running")),
                    checkpoint=str(snapshot.get("checkpoint", "unknown")),
                    completed=max(0, int(snapshot.get("completed", 0))),
                    pending=max(0, int(snapshot.get("pending", 0))),
                    signal_type=signal_type,
                    extra={
                        "source": "python_vl_heartbeat",
                        "vl_budget_seconds": int(max(1, vl_budget_seconds)),
                    },
                )
            except Exception as heartbeat_error:
                logger.warning(f"[{task_id}] AnalyzeWithVL heartbeat emit failed: {heartbeat_error}")

        def _emit_vl_hard_heartbeat_loop() -> None:
            nonlocal vl_budget_seconds, vl_started_at_sec
            interval_sec = max(
                5.0,
                float(_to_int(os.getenv("VL_ANALYSIS_HARD_HEARTBEAT_SEC", "15"), 15)),
            )
            while not vl_heartbeat_stop.wait(interval_sec):
                try:
                    if vl_started_at_sec > 0.0 and vl_budget_seconds > 0:
                        elapsed_sec = max(0, int(time.time() - vl_started_at_sec))
                        elapsed_sec = min(vl_budget_seconds, elapsed_sec)
                        _update_vl_heartbeat_state(
                            status="running",
                            checkpoint="vl_running",
                            completed=max(1, elapsed_sec),
                            pending=max(0, vl_budget_seconds - elapsed_sec),
                        )
                    _emit_vl_heartbeat(signal_type="hard")
                except Exception as heartbeat_error:
                    logger.warning(f"[{task_id}] AnalyzeWithVL heartbeat loop failed: {heartbeat_error}")

        routing_generator = None
        vl_generator = None
        vl_task = None

        try:
            self._increment_tasks()
            _update_vl_heartbeat_state(
                status="running",
                checkpoint="analyze_with_vl_started",
                completed=0,
                pending=1,
            )
            _emit_vl_heartbeat(signal_type="hard")
            vl_heartbeat_thread = threading.Thread(
                target=_emit_vl_hard_heartbeat_loop,
                name=f"vl-hard-heartbeat-{task_id}",
                daemon=True,
            )
            vl_heartbeat_thread.start()
            
            # 加载 VL 配置
            from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
            vl_config = load_module2_config().get("vl_material_generation", {})
            if isinstance(vl_config.get("api", {}), dict):
                vl_model_name = str(vl_config.get("api", {}).get("model", vl_model_name) or vl_model_name).strip()
            vl_enabled = vl_config.get("enabled", False)
            
            if not vl_enabled:
                logger.info(f"[{task_id}] VL 模块未启用，返回 vl_enabled=False")
                _persist_task_token_report({
                    "status": "vl_disabled",
                    "vl_enabled": False,
                    "used_fallback": False,
                    "vl_model": vl_model_name,
                    "routing_stats": {},
                    "token_stats": {},
                })
                _persist_vl_analysis_output({
                    "status": "vl_disabled",
                    "vl_enabled": False,
                    "used_fallback": False,
                    "vl_model": vl_model_name,
                    "routing_stats": {},
                    "token_stats": {},
                    "result_counts": {
                        "semantic_units_total": 0,
                        "vl_units": 0,
                        "screenshots": 0,
                        "clips": 0,
                    },
                    "merged_screenshots": [],
                    "merged_clips": [],
                })
                _update_vl_heartbeat_state(
                    status="completed",
                    checkpoint="vl_disabled",
                    completed=1,
                    pending=0,
                )
                _emit_vl_heartbeat(signal_type="hard")
                return video_processing_pb2.VLAnalysisResponse(
                    success=True,
                    vl_enabled=False,
                    used_fallback=False,
                    error_msg=""
                )
            
            # 加载语义单元：inline/ref 优先，其次运行态缓存，最后回退磁盘读取。
            data = None
            semantic_units: List[Dict[str, Any]] = []
            source_case = semantic_source_case

            if source_case == "semantic_units_inline":
                semantic_units = self._decode_semantic_units_inline_message(request.semantic_units_inline)
                data = semantic_units
                logger.info(f"[{task_id}] AnalyzeWithVL loaded semantic units from inline payload: units={len(semantic_units)}")
            elif source_case == "semantic_units_ref":
                ref_id = str(request.semantic_units_ref.ref_id or "").strip()
                ref_entry = self._get_phase2a_runtime_cache_entry_by_ref(ref_id)
                if isinstance(ref_entry, dict):
                    semantic_units = ref_entry.get("semantic_units", []) or []
                    data = semantic_units
                    ref_semantic_units_path = str(ref_entry.get("semantic_units_path", "") or "").strip()
                    if ref_semantic_units_path:
                        semantic_units_path = ref_semantic_units_path
                    logger.info(
                        f"[{task_id}] AnalyzeWithVL loaded semantic units from ref cache: "
                        f"ref_id={ref_id}, units={len(semantic_units)}"
                    )
                else:
                    logger.warning(f"[{task_id}] AnalyzeWithVL semantic_units_ref not found, fallback to runtime/disk: ref_id={ref_id}")

            if not semantic_units:
                runtime_semantic_units = self._get_phase2a_runtime_semantic_units(
                    output_dir=output_dir,
                    semantic_units_path=semantic_units_path,
                )
                if runtime_semantic_units is not None:
                    semantic_units = runtime_semantic_units
                    data = semantic_units
                    logger.info(
                        f"[{task_id}] AnalyzeWithVL loaded semantic units from runtime cache: "
                        f"units={len(semantic_units)}, output_dir={output_dir}"
                    )
                else:
                    candidate_paths: List[str] = []
                    seen_paths = set()
                    for candidate in [semantic_units_path, *_phase2a_semantic_units_candidates(output_dir)]:
                        normalized_candidate = os.path.abspath(str(candidate or "").strip())
                        if not normalized_candidate or normalized_candidate in seen_paths:
                            continue
                        seen_paths.add(normalized_candidate)
                        candidate_paths.append(normalized_candidate)

                    selected_path = next((path for path in candidate_paths if os.path.exists(path)), "")
                    if not selected_path:
                        # 异步写盘场景下，先做一次有限等待 flush，再尝试候选路径。
                        flush_async_json_writes(timeout_sec=10.0, scope_key=output_dir)
                        selected_path = next((path for path in candidate_paths if os.path.exists(path)), "")

                    if not selected_path:
                        raise FileNotFoundError(
                            f"semantic_units_json not found (source={source_case or 'legacy_path'}, candidates={candidate_paths})"
                        )

                    with open(selected_path, "r", encoding="utf-8") as selected_file:
                        data = json.load(selected_file)
                    semantic_units = self._normalize_semantic_units_payload(data)
                    semantic_units_path = selected_path

            if semantic_units:
                cache_path = semantic_units_path
                if str(cache_path).startswith("<"):
                    cache_path = ""
                self._cache_phase2a_runtime_semantic_units(
                    output_dir=output_dir,
                    semantic_units_path=cache_path,
                    semantic_units=semantic_units,
                    task_id=task_id,
                )

            _update_vl_heartbeat_state(
                status="running",
                checkpoint="semantic_units_ready",
                completed=0,
                pending=max(1, len(semantic_units)),
            )
            _emit_vl_heartbeat(signal_type="hard")
            
            if not semantic_units:
                logger.warning(f"[{task_id}] 无语义单元，跳过 VL 分析")
                _persist_task_token_report({
                    "status": "no_semantic_units",
                    "vl_enabled": True,
                    "used_fallback": False,
                    "vl_model": vl_model_name,
                    "routing_stats": {"total": 0},
                    "token_stats": {},
                })
                _persist_vl_analysis_output({
                    "status": "no_semantic_units",
                    "vl_enabled": True,
                    "used_fallback": False,
                    "vl_model": vl_model_name,
                    "routing_stats": {"total": 0},
                    "token_stats": {},
                    "result_counts": {
                        "semantic_units_total": 0,
                        "vl_units": 0,
                        "screenshots": 0,
                        "clips": 0,
                    },
                    "merged_screenshots": [],
                    "merged_clips": [],
                })
                _update_vl_heartbeat_state(
                    status="completed",
                    checkpoint="no_semantic_units",
                    completed=1,
                    pending=0,
                )
                _emit_vl_heartbeat(signal_type="hard")
                return video_processing_pb2.VLAnalysisResponse(
                    success=True,
                    vl_enabled=True,
                    used_fallback=False,
                    error_msg="No semantic units found"
                )
            
            # 调用 VL 分析
            # ==================================================================
            # 路由层：按 knowledge_type + 时长分流（避免不必要的 VL 负载）
            # ==================================================================
            from services.python_grpc.src.content_pipeline.infra.runtime.resource_manager import get_io_executor
            from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator

            def _safe_float(value, default=0.0):
                try:
                    return float(value)
                except Exception:
                    return default

            def _normalize_knowledge_type(raw_value):
                kt = (str(raw_value).strip() if raw_value is not None else "").lower()
                abstract_aliases = {"abstract", "抽象", "讲解", "explanation"}
                concrete_aliases = {"concrete", "具象", "具体", "实例", "示例"}
                process_aliases = {"process", "过程", "过程性", "流程", "操作"}
                if kt in abstract_aliases:
                    return "abstract"
                if kt in concrete_aliases:
                    return "concrete"
                if kt in process_aliases:
                    return "process"
                return "process"

            def _map_routing_intervals_to_absolute(raw_intervals, unit_start_sec, unit_end_sec):
                """
                将路由预处理里的区间统一映射到“原视频绝对时间轴”。
                兼容相对区间（相对单元起点）和已是绝对区间两种输入。
                """
                mapped = []
                if not isinstance(raw_intervals, list):
                    return mapped

                duration = max(0.0, float(unit_end_sec) - float(unit_start_sec))
                for item in raw_intervals:
                    s = e = None
                    if isinstance(item, (list, tuple)) and len(item) >= 2:
                        s, e = item[0], item[1]
                    elif isinstance(item, dict):
                        s = item.get("start_sec", item.get("start"))
                        e = item.get("end_sec", item.get("end"))
                    try:
                        s = float(s)
                        e = float(e)
                    except Exception:
                        continue
                    if e <= s:
                        continue

                    # 相对区间：通常落在 [0, duration]。
                    if s >= -1e-6 and e <= duration + 1e-6:
                        abs_s = unit_start_sec + s
                        abs_e = unit_start_sec + e
                    else:
                        abs_s = s
                        abs_e = e

                    abs_s = max(unit_start_sec, min(abs_s, unit_end_sec))
                    abs_e = max(unit_start_sec, min(abs_e, unit_end_sec))
                    if abs_e > abs_s:
                        mapped.append((abs_s, abs_e))

                if not mapped:
                    return []
                mapped.sort(key=lambda x: x[0])
                merged = [mapped[0]]
                for s, e in mapped[1:]:
                    last_s, last_e = merged[-1]
                    if s <= last_e + 1e-6:
                        merged[-1] = (last_s, max(last_e, e))
                    else:
                        merged.append((s, e))
                return merged

            def _select_screenshots_sync(
                unit_id,
                start_sec,
                end_sec,
                stable_islands_override=None,
                action_segments_override=None,
            ):
                """
                说明：在 IO 线程池中执行的同步截图选择。
                取舍：每次调用创建轻量级 selector，避免多线程共享状态引发不稳定。
                """
                try:
                    selector = ScreenshotSelector.create_lightweight()
                    results = selector.select_screenshots_for_range_sync(
                        video_path=video_path,
                        start_sec=start_sec,
                        end_sec=end_sec,
                        coarse_fps=_safe_float(routing_cfg.get("screenshot_coarse_fps", 2.0), 2.0),
                        fine_fps=_safe_float(routing_cfg.get("screenshot_fine_fps", 10.0), 10.0),
                        stable_islands_override=stable_islands_override,
                        action_segments_override=action_segments_override,
                        analysis_max_width=max(
                            0,
                            int(_safe_float(routing_cfg.get("screenshot_analysis_max_width", 640), 640)),
                        ),
                        long_window_fine_chunk_sec=max(
                            0.0,
                            _safe_float(routing_cfg.get("screenshot_long_window_fine_chunk_sec", 20.0), 20.0),
                        ),
                        decode_open_timeout_sec=route_decode_open_timeout_sec,
                        decode_allow_inline_transcode=route_decode_allow_inline_transcode,
                        decode_enable_async_transcode=route_decode_enable_async_transcode,
                    )
                    if results:
                        return results
                except Exception as e:
                    logger.warning(f"[{task_id}] 路由截图选择失败: unit={unit_id}, err={e}")
                mid = (start_sec + end_sec) / 2 if end_sec >= start_sec else start_sec
                return [{"timestamp_sec": mid, "score": 0.0}]

            routing_stats = {
                "total": len(semantic_units),
                "abstract": 0,
                "concrete": 0,
                "process": 0,
                "process_preprocessed": 0,
                "process_static_legacy": 0,
                "unknown": 0
            }
            vl_units = []
            cv_screenshot_units = []
            cv_clip_units = []
            routing_cfg = vl_config.get("routing", {}) if isinstance(vl_config.get("routing", {}), dict) else {}
            # 路由截图属于在线热路径：默认禁用同步整段转码，避免 AV1 导致分钟级阻塞。
            route_decode_open_timeout_sec = max(
                5,
                int(_safe_float(routing_cfg.get("screenshot_decode_open_timeout_sec", 30.0), 30.0)),
            )
            route_decode_allow_inline_transcode = bool(
                routing_cfg.get("screenshot_decode_allow_inline_transcode", False)
            )
            route_decode_enable_async_transcode = bool(
                routing_cfg.get("screenshot_decode_enable_async_transcode", True)
            )
            force_process_preprocess = bool(routing_cfg.get("process_force_preprocess_before_routing", True))
            route_screenshot_mode = str(routing_cfg.get("screenshot_pipeline_mode", "process_streaming")).strip().lower()
            route_screenshot_analysis_max_width = max(
                0,
                int(_safe_float(routing_cfg.get("screenshot_analysis_max_width", 640), 640)),
            )
            route_screenshot_queue_maxsize = max(
                1,
                int(_safe_float(routing_cfg.get("screenshot_queue_maxsize", max(8, self.cv_worker_count * 2)), max(8, self.cv_worker_count * 2))),
            )
            route_screenshot_worker_cfg = routing_cfg.get("screenshot_worker_count", "auto")
            if str(route_screenshot_worker_cfg).strip().lower() == "auto":
                route_screenshot_workers = max(1, min(self.cv_worker_count, 8))
            else:
                route_screenshot_workers = max(
                    1,
                    min(
                        self.cv_worker_count,
                        int(_safe_float(route_screenshot_worker_cfg, max(1, min(self.cv_worker_count, 8)))),
                    ),
                )

            process_units = []
            for unit in semantic_units:
                raw_kt = unit.get("knowledge_type", "")
                if _normalize_knowledge_type(raw_kt) == "process":
                    process_units.append(unit)

            process_route_map = {}
            routing_generator = None
            if process_units:
                routing_generator = VLMaterialGenerator(vl_config, cv_executor=self.cv_process_pool)
                process_route_map = await routing_generator.preprocess_process_units_for_routing(
                    video_path=video_path,
                    process_units=process_units,
                    output_dir=output_dir,
                    force_preprocess=force_process_preprocess,
                )

            for unit in semantic_units:
                raw_kt = unit.get("knowledge_type", "")
                kt = _normalize_knowledge_type(raw_kt)
                unit["knowledge_type"] = kt
                if kt == "process" and not (str(raw_kt).strip().lower() in {"process", "过程", "过程性", "流程", "操作"}):
                    routing_stats["unknown"] += 1
                start_sec = _safe_float(unit.get("start_sec", 0.0))
                end_sec = _safe_float(unit.get("end_sec", 0.0))
                duration = max(0.0, end_sec - start_sec)

                if kt == "abstract":
                    routing_stats["abstract"] += 1
                    continue
                if kt == "concrete":
                    routing_stats["concrete"] += 1
                    unit["_vl_analysis_mode_override"] = "concrete"
                    vl_units.append(unit)
                    continue

                # process: 按时长分流
                route_info = process_route_map.get(str(unit.get("unit_id", "") or ""), {})
                effective_duration = _safe_float(route_info.get("effective_duration_sec", duration), duration)
                if bool(route_info.get("preprocess_applied", False)):
                    routing_stats["process_preprocessed"] += 1
                pre_prune_info = route_info.get("pre_prune_info", {})
                if not isinstance(pre_prune_info, dict):
                    pre_prune_info = {}
                unit["_routing_pre_prune"] = pre_prune_info
                unit["_routing_effective_duration_sec"] = effective_duration
                unit["_routing_stable_intervals_abs"] = _map_routing_intervals_to_absolute(
                    pre_prune_info.get("stable_intervals_raw"),
                    start_sec,
                    end_sec,
                )
                unit["_routing_action_segments_abs"] = _map_routing_intervals_to_absolute(
                    pre_prune_info.get("kept_segments"),
                    start_sec,
                    end_sec,
                )

                # process 统一先做“静态主导降级”判定（与短/长分流解耦）；
                # 命中后直接送入 VL 侧降级链路，复用 stable-island -> action-units 逻辑。
                force_stable_action_legacy = False
                if routing_generator is not None:
                    try:
                        force_stable_action_legacy = routing_generator._should_use_stable_action_legacy_branch(
                            semantic_unit=unit,
                            pre_prune_info=pre_prune_info,
                            raw_duration_sec=duration,
                        )
                    except Exception as _routing_branch_error:
                        logger.warning(
                            f"[{task_id}] 静态降级路由判定异常: unit={unit.get('unit_id', '')}, "
                            f"err={_routing_branch_error}"
                        )
                        force_stable_action_legacy = False

                routing_stats["process"] += 1
                if force_stable_action_legacy:
                    routing_stats["process_static_legacy"] += 1
                    unit["_routing_force_legacy_action"] = True
                vl_units.append(unit)

            logger.info(
                f"[{task_id}] VL 路由统计: total={routing_stats['total']}, "
                f"abstract={routing_stats['abstract']}, concrete={routing_stats['concrete']}, process={routing_stats['process']}, "
                f"process_preprocessed={routing_stats['process_preprocessed']}, "
                f"process_static_legacy={routing_stats['process_static_legacy']}, "
                f"unknown={routing_stats['unknown']}"
            )

            vl_budget_multiplier = max(
                1.0,
                _safe_float(routing_cfg.get("vl_watchdog_budget_multiplier", 3.0), 3.0),
            )
            vl_total_duration_sec = 0.0
            for vl_unit in vl_units:
                start_sec = _safe_float(vl_unit.get("start_sec", 0.0))
                end_sec = _safe_float(vl_unit.get("end_sec", start_sec))
                effective_duration_sec = _safe_float(vl_unit.get("_routing_effective_duration_sec", -1.0), -1.0)
                if effective_duration_sec > 0.0:
                    vl_total_duration_sec += effective_duration_sec
                elif end_sec > start_sec:
                    vl_total_duration_sec += (end_sec - start_sec)
            vl_budget_seconds = max(1, int(vl_total_duration_sec * vl_budget_multiplier + 0.999))
            _update_vl_heartbeat_state(
                status="running",
                checkpoint="routing_ready" if vl_units else "routing_no_vl_units",
                completed=0 if vl_units else 1,
                pending=vl_budget_seconds if vl_units else 0,
            )
            _emit_vl_heartbeat(signal_type="hard")
            logger.info(
                f"[{task_id}] VL heartbeat budget: vl_units={len(vl_units)}, "
                f"total_duration={vl_total_duration_sec:.2f}s, multiplier={vl_budget_multiplier:.2f}x, "
                f"budget={vl_budget_seconds}s"
            )

            # ==================================================================
            # 预启动 VL 任务（与路由侧截图并行，形成 IO/Compute 重叠）
            # ==================================================================
            vl_task = None
            vl_t0 = None
            vl_token_stats = {}
            vl_unit_analysis_outputs: List[Dict[str, Any]] = []
            vl_failure_snapshot: Dict[str, Any] = {}
            if vl_units:
                vl_t0 = time.perf_counter()
                generator = VLMaterialGenerator(vl_config, cv_executor=self.cv_process_pool)
                vl_generator = generator
                vl_task = asyncio.create_task(generator.generate(video_path, vl_units, output_dir))
                vl_started_at_sec = time.time()
                _update_vl_heartbeat_state(
                    status="running",
                    checkpoint="vl_running",
                    completed=min(1, vl_budget_seconds),
                    pending=max(0, vl_budget_seconds - min(1, vl_budget_seconds)),
                )
                _emit_vl_heartbeat(signal_type="hard")

            # ==================================================================
            # 路由侧：截图选择（process<=20s）
            # ==================================================================
            vl_screenshot_requests = []
            vl_clip_requests = []
            cv_screenshot_requests = []
            if cv_screenshot_units:
                route_t0 = time.perf_counter()
                loop = asyncio.get_event_loop()
                total_units = len(cv_screenshot_units)

                def _consume_vl_result(vl_result):
                    nonlocal vl_token_stats, vl_unit_analysis_outputs, vl_failure_snapshot
                    vl_token_stats = getattr(vl_result, "token_stats", {}) or {}
                    vl_unit_analysis_outputs = list(getattr(vl_result, "unit_analysis_outputs", []) or [])
                    if not vl_result.success:
                        raw_error_msg = str(getattr(vl_result, "error_msg", "") or "")
                        vl_failure_snapshot = {
                            "status": "fallback",
                            "used_fallback": True,
                            "error_msg": raw_error_msg,
                            "token_stats": vl_token_stats,
                            "unit_failures": _summarize_vl_unit_failures(vl_unit_analysis_outputs),
                            "unit_analysis_outputs_count": len(vl_unit_analysis_outputs),
                        }
                        return False, raw_error_msg
                    vl_unit_ids = {u.get("unit_id", "") for u in vl_units}
                    for ss in vl_result.screenshot_requests:
                        if ss.get("semantic_unit_id", "") in vl_unit_ids:
                            vl_screenshot_requests.append(ss)
                    for clip in vl_result.clip_requests:
                        if clip.get("semantic_unit_id", "") in vl_unit_ids:
                            vl_clip_requests.append(clip)
                    return True, ""

                vl_consumed = False
                if route_screenshot_mode == "process_streaming":
                    from services.python_grpc.src.vision_validation.worker import run_select_screenshots_for_range_task

                    coarse_fps = max(0.5, _safe_float(routing_cfg.get("screenshot_coarse_fps", 2.0), 2.0))
                    fine_fps = max(1.0, _safe_float(routing_cfg.get("screenshot_fine_fps", 10.0), 10.0))
                    analysis_max_width = max(
                        0,
                        int(_safe_float(routing_cfg.get("screenshot_analysis_max_width", 640), 640)),
                    )
                    long_window_fine_chunk_sec = max(
                        0.0,
                        _safe_float(routing_cfg.get("screenshot_long_window_fine_chunk_sec", 20.0), 20.0),
                    )
                    ordered_units = sorted(
                        list(enumerate(cv_screenshot_units)),
                        key=lambda item: (_safe_float(item[1].get("start_sec", 0.0)), item[0]),
                    )

                    unit_queue: asyncio.Queue = asyncio.Queue(maxsize=route_screenshot_queue_maxsize)
                    result_queue: asyncio.Queue = asyncio.Queue(maxsize=route_screenshot_queue_maxsize)
                    pid_counter: Counter = Counter()

                    async def _producer() -> None:
                        for order_idx, unit in ordered_units:
                            await unit_queue.put((order_idx, unit))
                        for _ in range(route_screenshot_workers):
                            await unit_queue.put(None)

                    async def _consumer(worker_no: int) -> None:
                        while True:
                            item = await unit_queue.get()
                            if item is None:
                                unit_queue.task_done()
                                break

                            order_idx, unit = item
                            unit_id = unit.get("unit_id", "")
                            start_sec = _safe_float(unit.get("start_sec", 0.0))
                            end_sec = _safe_float(unit.get("end_sec", 0.0))
                            stable_override = []
                            action_override = []
                            if isinstance(unit.get("_routing_stable_intervals_abs"), list):
                                for interval_item in unit.get("_routing_stable_intervals_abs") or []:
                                    if not isinstance(interval_item, (list, tuple)) or len(interval_item) < 2:
                                        continue
                                    try:
                                        s = float(interval_item[0])
                                        e = float(interval_item[1])
                                    except (TypeError, ValueError):
                                        continue
                                    if e > s:
                                        stable_override.append((s, e))
                            if isinstance(unit.get("_routing_action_segments_abs"), list):
                                for interval_item in unit.get("_routing_action_segments_abs") or []:
                                    if not isinstance(interval_item, (list, tuple)) or len(interval_item) < 2:
                                        continue
                                    try:
                                        s = float(interval_item[0])
                                        e = float(interval_item[1])
                                    except (TypeError, ValueError):
                                        continue
                                    if e > s:
                                        action_override.append((s, e))

                            try:
                                result = await loop.run_in_executor(
                                    self.cv_process_pool,
                                    functools.partial(
                                        run_select_screenshots_for_range_task,
                                        video_path=video_path,
                                        unit_id=unit_id,
                                        start_sec=start_sec,
                                        end_sec=end_sec,
                                        coarse_fps=coarse_fps,
                                        fine_fps=fine_fps,
                                        stable_islands_override=stable_override,
                                        action_segments_override=action_override,
                                        analysis_max_width=analysis_max_width,
                                        long_window_fine_chunk_sec=long_window_fine_chunk_sec,
                                        decode_open_timeout_sec=route_decode_open_timeout_sec,
                                        decode_allow_inline_transcode=route_decode_allow_inline_transcode,
                                        decode_enable_async_transcode=route_decode_enable_async_transcode,
                                    ),
                                )
                            except Exception as e:
                                logger.warning(
                                    f"[{task_id}] 路由截图 worker={worker_no} 异常: unit={unit_id}, err={e}"
                                )
                                mid = (start_sec + end_sec) / 2 if end_sec >= start_sec else start_sec
                                result = {
                                    "unit_id": unit_id,
                                    "start_sec": start_sec,
                                    "end_sec": end_sec,
                                    "screenshots": [{"timestamp_sec": mid, "score": 0.0}],
                                    "worker_pid": -1,
                                    "elapsed_ms": 0.0,
                                    "error": str(e),
                                }

                            await result_queue.put((order_idx, result))
                            unit_queue.task_done()

                    async def _collector(total_count: int) -> Dict[int, Dict[str, Any]]:
                        collected: Dict[int, Dict[str, Any]] = {}
                        done = 0
                        log_every = max(1, total_count // 4)

                        while done < total_count:
                            order_idx, result = await result_queue.get()
                            collected[order_idx] = result
                            done += 1
                            pid = result.get("worker_pid")
                            if isinstance(pid, int):
                                pid_counter[pid] += 1

                            if done % log_every == 0 or done == total_count:
                                logger.info(
                                    f"[{task_id}] 路由截图流式进度: {done}/{total_count}, "
                                    f"queue={unit_queue.qsize()}, active_pids={len(pid_counter)}"
                                )
                        return collected

                    producer_task = asyncio.create_task(_producer())
                    collector_task = asyncio.create_task(_collector(total_units))
                    consumer_tasks = [
                        asyncio.create_task(_consumer(i))
                        for i in range(route_screenshot_workers)
                    ]

                    await producer_task
                    await unit_queue.join()
                    await asyncio.gather(*consumer_tasks)
                    collected_results = await collector_task

                    for order_idx, _ in ordered_units:
                        result = collected_results.get(order_idx, {})
                        unit_id = result.get("unit_id", "")
                        start_sec = _safe_float(result.get("start_sec", 0.0))
                        end_sec = _safe_float(result.get("end_sec", start_sec))
                        ss_list = result.get("screenshots", []) or []
                        for idx, ss in enumerate(ss_list):
                            ts = _safe_float(ss.get("timestamp_sec", (start_sec + end_sec) / 2))
                            cv_screenshot_requests.append({
                                "screenshot_id": f"{unit_id}/{unit_id}_ss_route_{idx + 1:03d}",
                                "timestamp_sec": ts,
                                "label": f"routed_range_{idx}",
                                "semantic_unit_id": unit_id,
                            })

                    route_ms = (time.perf_counter() - route_t0) * 1000.0
                    logger.info(
                        f"[{task_id}] 路由截图完成: units={len(cv_screenshot_units)}, "
                        f"screenshots={len(cv_screenshot_requests)}, ms={route_ms:.1f}, "
                        f"mode=process_streaming, workers={route_screenshot_workers}, "
                        f"queue_maxsize={route_screenshot_queue_maxsize}, pids={sorted(pid_counter.keys())}"
                    )
                else:
                    executor = get_io_executor()
                    cpu_count = os.cpu_count() or 4
                    max_concurrency = max(1, min(4, cpu_count // 2))
                    semaphore = asyncio.Semaphore(max_concurrency)
                    batch_size = max(1, max_concurrency * 4)
                    total_batches = (total_units + batch_size - 1) // batch_size

                    async def _run_cv_screenshot(unit):
                        unit_id = unit.get("unit_id", "")
                        start_sec = _safe_float(unit.get("start_sec", 0.0))
                        end_sec = _safe_float(unit.get("end_sec", 0.0))
                        stable_override = []
                        action_override = []
                        if isinstance(unit.get("_routing_stable_intervals_abs"), list):
                            for interval_item in unit.get("_routing_stable_intervals_abs") or []:
                                if not isinstance(interval_item, (list, tuple)) or len(interval_item) < 2:
                                    continue
                                try:
                                    s = float(interval_item[0])
                                    e = float(interval_item[1])
                                except (TypeError, ValueError):
                                    continue
                                if e > s:
                                    stable_override.append((s, e))
                        if isinstance(unit.get("_routing_action_segments_abs"), list):
                            for interval_item in unit.get("_routing_action_segments_abs") or []:
                                if not isinstance(interval_item, (list, tuple)) or len(interval_item) < 2:
                                    continue
                                try:
                                    s = float(interval_item[0])
                                    e = float(interval_item[1])
                                except (TypeError, ValueError):
                                    continue
                                if e > s:
                                    action_override.append((s, e))
                        async with semaphore:
                            results = await loop.run_in_executor(
                                executor,
                                functools.partial(
                                    _select_screenshots_sync,
                                    unit_id,
                                    start_sec,
                                    end_sec,
                                    stable_override,
                                    action_override,
                                )
                            )
                        return unit_id, start_sec, end_sec, results

                    for batch_idx in range(total_batches):
                        start = batch_idx * batch_size
                        end = min(start + batch_size, total_units)
                        batch_units = cv_screenshot_units[start:end]

                        if vl_task and vl_task.done() and not vl_consumed:
                            vl_result = vl_task.result()
                            ok, err = _consume_vl_result(vl_result)
                            if not ok:
                                logger.warning(f"[{task_id}] VL 分析失败，提前回退: {err}")
                                _update_vl_heartbeat_state(
                                    status="fallback",
                                    checkpoint="vl_failed_early",
                                    completed=max(1, vl_budget_seconds),
                                    pending=0,
                                )
                                _emit_vl_heartbeat(signal_type="hard")
                                _persist_task_token_report({
                                    "status": "fallback",
                                    "vl_enabled": True,
                                    "used_fallback": True,
                                    "vl_model": vl_model_name,
                                    "error_msg": err,
                                    "routing_stats": routing_stats,
                                    "token_stats": vl_token_stats,
                                })
                                _persist_vl_analysis_output({
                                    "status": "fallback",
                                    "vl_enabled": True,
                                    "used_fallback": True,
                                    "vl_model": vl_model_name,
                                    "error_msg": err,
                                    "routing_stats": routing_stats,
                                    "token_stats": vl_token_stats,
                                    "result_counts": {
                                        "semantic_units_total": len(semantic_units),
                                        "vl_units": len(vl_units),
                                        "screenshots": len(cv_screenshot_requests) + len(vl_screenshot_requests),
                                        "clips": len(cv_clip_requests) + len(vl_clip_requests),
                                    },
                                    "merged_screenshots": list(cv_screenshot_requests) + list(vl_screenshot_requests),
                                    "merged_clips": list(cv_clip_requests) + list(vl_clip_requests),
                                })
                                _persist_vl_last_error(
                                    {
                                        "status": "fallback",
                                        "checkpoint": "vl_failed_early",
                                        "error_msg": err,
                                        "routing_stats": routing_stats,
                                        "token_stats": vl_token_stats,
                                        "result_counts": {
                                            "semantic_units_total": len(semantic_units),
                                            "vl_units": len(vl_units),
                                            "screenshots": len(cv_screenshot_requests) + len(vl_screenshot_requests),
                                            "clips": len(cv_clip_requests) + len(vl_clip_requests),
                                        },
                                        "failure_snapshot": dict(vl_failure_snapshot or {}),
                                    }
                                )
                                return video_processing_pb2.VLAnalysisResponse(
                                    success=False,
                                    vl_enabled=True,
                                    used_fallback=True,
                                    error_msg=err
                                )
                            vl_consumed = True
                            if vl_t0 is None:
                                vl_t0 = time.perf_counter()
                            vl_ms = (time.perf_counter() - vl_t0) * 1000.0
                            logger.info(
                                f"[{task_id}] VL 结果提前合并: screenshots={len(vl_screenshot_requests)}, "
                                f"clips={len(vl_clip_requests)}, ms={vl_ms:.1f}"
                            )

                        tasks = [_run_cv_screenshot(u) for u in batch_units]
                        results = await asyncio.gather(*tasks)
                        batch_screenshots = 0
                        for unit_id, start_sec, end_sec, ss_list in results:
                            for idx, ss in enumerate(ss_list or []):
                                ts = _safe_float(ss.get("timestamp_sec", (start_sec + end_sec) / 2))
                                cv_screenshot_requests.append({
                                    "screenshot_id": f"{unit_id}/{unit_id}_ss_route_{idx + 1:03d}",
                                    "timestamp_sec": ts,
                                    "label": f"routed_range_{idx}",
                                    "semantic_unit_id": unit_id
                                })
                                batch_screenshots += 1

                        logger.info(
                            f"[{task_id}] 路由截图批次 {batch_idx + 1}/{total_batches}: "
                            f"units={len(batch_units)}, batch_ss={batch_screenshots}, "
                            f"total_ss={len(cv_screenshot_requests)}"
                        )

                    route_ms = (time.perf_counter() - route_t0) * 1000.0
                    logger.info(
                        f"[{task_id}] 路由截图完成: units={len(cv_screenshot_units)}, "
                        f"screenshots={len(cv_screenshot_requests)}, ms={route_ms:.1f}, "
                        f"mode=legacy_batch, concurrency={max_concurrency}, batch_size={batch_size}"
                    )

            # ==================================================================
            # 路由侧：短过程 clip（process<=10s）
            # ==================================================================
            cv_clip_requests = []
            for unit in cv_clip_units:
                unit_id = unit.get("unit_id", "")
                start_sec = _safe_float(unit.get("start_sec", 0.0))
                end_sec = _safe_float(unit.get("end_sec", 0.0))
                action_segments = []
                if isinstance(unit.get("_routing_action_segments_abs"), list):
                    for interval_item in unit.get("_routing_action_segments_abs") or []:
                        if not isinstance(interval_item, (list, tuple)) or len(interval_item) < 2:
                            continue
                        try:
                            s = float(interval_item[0])
                            e = float(interval_item[1])
                        except (TypeError, ValueError):
                            continue
                        s = max(start_sec, min(s, end_sec))
                        e = max(start_sec, min(e, end_sec))
                        if e > s:
                            action_segments.append((s, e))

                if action_segments:
                    for idx, (seg_start, seg_end) in enumerate(action_segments):
                        cv_clip_requests.append({
                            "clip_id": f"{unit_id}/{unit_id}_clip_route_action_{idx + 1:03d}",
                            "start_sec": seg_start,
                            "end_sec": seg_end,
                            "knowledge_type": unit.get("knowledge_type", ""),
                            "semantic_unit_id": unit_id,
                            "segments": [{"start_sec": seg_start, "end_sec": seg_end}],
                        })
                else:
                    cv_clip_requests.append({
                        "clip_id": f"{unit_id}/{unit_id}_clip_route_001",
                        "start_sec": start_sec,
                        "end_sec": end_sec,
                        "knowledge_type": unit.get("knowledge_type", ""),
                        "semantic_unit_id": unit_id
                    })

            # ==================================================================
            # VL 分析：仅处理 process>10s 的单元
            # ==================================================================
            if vl_task:
                if "vl_consumed" in locals() and vl_consumed:
                    logger.info(
                        f"[{task_id}] VL 已在批次处理中合并: "
                        f"screenshots={len(vl_screenshot_requests)}, clips={len(vl_clip_requests)}"
                    )
                else:
                    vl_result = await vl_task
                    if not vl_result.success:
                        logger.warning(f"[{task_id}] VL 分析失败，需要回退: {vl_result.error_msg}")
                        _update_vl_heartbeat_state(
                            status="fallback",
                            checkpoint="vl_failed",
                            completed=max(1, vl_budget_seconds),
                            pending=0,
                        )
                        _emit_vl_heartbeat(signal_type="hard")
                        _persist_task_token_report({
                            "status": "fallback",
                            "vl_enabled": True,
                            "used_fallback": True,
                            "vl_model": vl_model_name,
                            "error_msg": vl_result.error_msg,
                            "routing_stats": routing_stats,
                            "token_stats": vl_token_stats,
                        })
                        _persist_vl_analysis_output({
                            "status": "fallback",
                            "vl_enabled": True,
                            "used_fallback": True,
                            "vl_model": vl_model_name,
                            "error_msg": vl_result.error_msg,
                            "routing_stats": routing_stats,
                            "token_stats": vl_token_stats,
                            "result_counts": {
                                "semantic_units_total": len(semantic_units),
                                "vl_units": len(vl_units),
                                "screenshots": len(cv_screenshot_requests) + len(vl_screenshot_requests),
                                "clips": len(cv_clip_requests) + len(vl_clip_requests),
                            },
                            "merged_screenshots": list(cv_screenshot_requests) + list(vl_screenshot_requests),
                            "merged_clips": list(cv_clip_requests) + list(vl_clip_requests),
                        })
                        _persist_vl_last_error(
                            {
                                "status": "fallback",
                                "checkpoint": "vl_failed",
                                "error_msg": str(vl_result.error_msg or ""),
                                "routing_stats": routing_stats,
                                "token_stats": vl_token_stats,
                                "result_counts": {
                                    "semantic_units_total": len(semantic_units),
                                    "vl_units": len(vl_units),
                                    "screenshots": len(cv_screenshot_requests) + len(vl_screenshot_requests),
                                    "clips": len(cv_clip_requests) + len(vl_clip_requests),
                                },
                                "failure_snapshot": dict(vl_failure_snapshot or {}),
                                "unit_failures": _summarize_vl_unit_failures(vl_unit_analysis_outputs),
                            }
                        )
                        return video_processing_pb2.VLAnalysisResponse(
                            success=False,
                            vl_enabled=True,
                            used_fallback=True,
                            error_msg=vl_result.error_msg
                        )
                    vl_unit_ids = {u.get("unit_id", "") for u in vl_units}
                    vl_unit_analysis_outputs = list(getattr(vl_result, "unit_analysis_outputs", []) or [])
                    for ss in vl_result.screenshot_requests:
                        if ss.get("semantic_unit_id", "") in vl_unit_ids:
                            vl_screenshot_requests.append(ss)
                    for clip in vl_result.clip_requests:
                        if clip.get("semantic_unit_id", "") in vl_unit_ids:
                            vl_clip_requests.append(clip)

                    if vl_t0 is None:
                        vl_t0 = time.perf_counter()
                    vl_ms = (time.perf_counter() - vl_t0) * 1000.0
                    logger.info(
                        f"[{task_id}] VL 处理完成: units={len(vl_units)}, "
                        f"screenshots={len(vl_screenshot_requests)}, clips={len(vl_clip_requests)}, ms={vl_ms:.1f}"
                    )

                    if vl_token_stats:
                        logger.info(
                            f"[{task_id}] VL Token节省估算: "
                            f"actual={vl_token_stats.get('total_tokens_actual', 0)}, "
                            f"baseline_est={vl_token_stats.get('total_tokens_baseline_est', 0)}, "
                            f"saved_est={vl_token_stats.get('saved_tokens_est', 0)}, "
                            f"saved_ratio={float(vl_token_stats.get('saved_ratio_est', 0.0)) * 100:.2f}%, "
                            f"pruned_units={vl_token_stats.get('pruned_units', 0)}/{vl_token_stats.get('vl_units', 0)}"
                        )
            else:
                logger.info(f"[{task_id}] VL 路由为空: process>10s=0, 跳过 VL API")

            # ==================================================================
            # 合并 + 去重 + 稳定排序
            # ==================================================================
            def _dedup_screenshots(items):
                seen_index = {}
                deduped = []

                def _score_screenshot(candidate):
                    """优先保留包含 frame_reason/bbox/ocr_text 的截图请求，避免透传信息被无信息副本覆盖。"""
                    score = 0
                    if str(candidate.get("frame_reason", "") or "").strip():
                        score += 4
                    if candidate.get("bbox") not in (None, "", []):
                        score += 2
                    if str(candidate.get("ocr_text", "") or "").strip():
                        score += 1
                    return score

                for item in items:
                    key = (
                        item.get("semantic_unit_id", ""),
                        float(item.get("timestamp_sec", 0.0)),
                        item.get("label", "")
                    )
                    if key in seen_index:
                        existing_idx = seen_index[key]
                        existing_item = deduped[existing_idx]
                        if _score_screenshot(item) > _score_screenshot(existing_item):
                            merged = dict(existing_item or {})
                            merged.update(dict(item or {}))
                            deduped[existing_idx] = merged
                        continue
                    seen_index[key] = len(deduped)
                    deduped.append(dict(item or {}))
                deduped.sort(key=lambda x: (x.get("semantic_unit_id", ""), float(x.get("timestamp_sec", 0.0))))
                return deduped

            def _dedup_clips(items):
                seen = set()
                deduped = []
                for item in items:
                    key = (
                        item.get("semantic_unit_id", ""),
                        float(item.get("start_sec", 0.0)),
                        float(item.get("end_sec", 0.0)),
                        item.get("knowledge_type", "")
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped.append(item)
                deduped.sort(key=lambda x: (x.get("semantic_unit_id", ""), float(x.get("start_sec", 0.0))))
                return deduped

            merged_screenshots = _dedup_screenshots(cv_screenshot_requests + vl_screenshot_requests)
            merged_clips = _dedup_clips(cv_clip_requests + vl_clip_requests)

            screenshot_requests = [
                video_processing_pb2.ScreenshotRequest(
                    screenshot_id=ss.get("screenshot_id", ""),
                    timestamp_sec=ss.get("timestamp_sec", 0.0),
                    label=ss.get("label", ""),
                    semantic_unit_id=ss.get("semantic_unit_id", ""),
                    frame_reason=str(ss.get("frame_reason", "") or ""),
                )
                for ss in merged_screenshots
            ]
            clip_requests = [
                self._build_clip_request_pb(clip, clip.get("semantic_unit_id", ""))
                for clip in merged_clips
            ]

            logger.info(
                f"[{task_id}] AnalyzeWithVL 混合结果: total_units={len(semantic_units)}, "
                f"vl_units={len(vl_units)}, screenshots={len(screenshot_requests)}, clips={len(clip_requests)}"
            )

            # ==================================================================
            # 🚀 Persistence: Save instructional_steps back to JSON
            # ==================================================================
            concrete_analysis_records: List[Dict[str, Any]] = []
            try:
                has_updates = False
                units_map = {u.get("unit_id"): u for u in semantic_units}
                persist_payload: Any = data
                if semantic_units_path and os.path.exists(semantic_units_path):
                    try:
                        with open(semantic_units_path, "r", encoding="utf-8") as persisted_file:
                            persist_payload = json.load(persisted_file)
                    except Exception as load_error:
                        logger.warning(f"[{task_id}] Failed to load persisted semantic_units payload: {load_error}")
                persist_units_map = self._build_semantic_unit_index(persist_payload)
                if not persist_units_map:
                    # 兜底重建为 grouped 结构，避免回写时退化为旧的扁平展示格式。
                    persist_payload = self._build_grouped_semantic_units_payload(list(semantic_units or []))
                    persist_units_map = self._build_semantic_unit_index(persist_payload)

                # 将 AnalyzeWithVL 聚合后的素材请求回写到 semantic_units，确保 process/concrete 的
                # frame_reason 在 Phase2B 可直接从 semantic_units_phase2a.json 透传使用。
                unit_screenshot_map: Dict[str, List[Dict[str, Any]]] = {}
                for screenshot in merged_screenshots:
                    if not isinstance(screenshot, dict):
                        continue
                    unit_id = str(screenshot.get("semantic_unit_id", "") or "").strip()
                    screenshot_id = str(screenshot.get("screenshot_id", "") or "").strip()
                    if not unit_id or not screenshot_id:
                        continue
                    screenshot_payload: Dict[str, Any] = {
                        "screenshot_id": screenshot_id,
                        "timestamp_sec": _safe_float(screenshot.get("timestamp_sec", 0.0), 0.0),
                        "label": str(screenshot.get("label", "") or "").strip(),
                        "semantic_unit_id": unit_id,
                        "frame_reason": str(screenshot.get("frame_reason", "") or "").strip(),
                        "ocr_text": str(screenshot.get("ocr_text", "") or "").strip(),
                    }
                    bbox_value = screenshot.get("bbox")
                    if isinstance(bbox_value, list) and bbox_value:
                        screenshot_payload["bbox"] = list(bbox_value)
                    unit_screenshot_map.setdefault(unit_id, []).append(screenshot_payload)

                unit_clip_map: Dict[str, List[Dict[str, Any]]] = {}
                for clip in merged_clips:
                    if not isinstance(clip, dict):
                        continue
                    unit_id = str(clip.get("semantic_unit_id", "") or "").strip()
                    clip_id = str(clip.get("clip_id", "") or "").strip()
                    if not unit_id or not clip_id:
                        continue
                    clip_payload: Dict[str, Any] = {
                        "clip_id": clip_id,
                        "start_sec": _safe_float(clip.get("start_sec", 0.0), 0.0),
                        "end_sec": _safe_float(clip.get("end_sec", 0.0), 0.0),
                        "knowledge_type": str(clip.get("knowledge_type", "") or "").strip(),
                        "semantic_unit_id": unit_id,
                    }
                    raw_segments = clip.get("segments")
                    if isinstance(raw_segments, list) and raw_segments:
                        clip_payload["segments"] = raw_segments
                    unit_clip_map.setdefault(unit_id, []).append(clip_payload)

                updated_material_request_units = 0
                for unit_id, target_node in persist_units_map.items():
                    if unit_id not in unit_screenshot_map and unit_id not in unit_clip_map:
                        continue
                    if not isinstance(target_node, dict):
                        continue
                    material_requests = target_node.get("material_requests", {})
                    if not isinstance(material_requests, dict):
                        material_requests = {}
                    material_requests["screenshot_requests"] = list(unit_screenshot_map.get(unit_id, []))
                    material_requests["clip_requests"] = list(unit_clip_map.get(unit_id, []))
                    target_node["material_requests"] = material_requests
                    if unit_id in units_map and isinstance(units_map[unit_id], dict):
                        units_map[unit_id]["material_requests"] = material_requests
                    updated_material_request_units += 1
                    has_updates = True

                updated_instructional_units = 0
                if vl_clip_requests:
                    # Group steps by unit
                    unit_steps = {} # uid -> list of step dicts
                    screenshot_step_map: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
                    for ss in vl_screenshot_requests:
                        if not isinstance(ss, dict):
                            continue
                        uid = str(ss.get("semantic_unit_id", "") or "").strip()
                        step_id_value = int(_safe_float(ss.get("step_id", 0), 0.0))
                        if not uid or step_id_value <= 0:
                            continue
                        screenshot_step_map.setdefault((uid, step_id_value), []).append(ss)
                    for key in list(screenshot_step_map.keys()):
                        screenshot_step_map[key].sort(
                            key=lambda item: (
                                int(_safe_float(item.get("keyframe_index", 0), 0.0)),
                                _safe_float(item.get("timestamp_sec", 0.0), 0.0),
                            )
                        )
                    for clip in vl_clip_requests:
                        if clip.get("analysis_mode") == "tutorial_stepwise":
                            uid = str(clip.get("semantic_unit_id", "") or "").strip()
                            if not uid:
                                continue

                            if uid not in unit_steps:
                                unit_steps[uid] = []

                            # Find matching screenshots and透传 frame_reason 到 step 级结构。
                            step_id = int(_safe_float(clip.get("step_id", 0), 0.0))
                            step_id = int(_safe_float(clip.get("step_id", 0), 0.0))
                            matched_screenshots = screenshot_step_map.get((uid, step_id), [])
                            step_ss_ids: List[str] = []
                            step_keyframe_details: List[Dict[str, Any]] = []
                            seen_step_screenshot_ids: set[str] = set()
                            for ss in matched_screenshots:
                                screenshot_id = str(ss.get("screenshot_id", "") or "").strip()
                                if screenshot_id and screenshot_id not in seen_step_screenshot_ids:
                                    seen_step_screenshot_ids.add(screenshot_id)
                                    step_ss_ids.append(screenshot_id)
                                detail: Dict[str, Any] = {}
                                if screenshot_id:
                                    detail["screenshot_id"] = screenshot_id
                                timestamp_sec = _safe_float(ss.get("timestamp_sec", None), None)
                                if timestamp_sec is not None:
                                    detail["timestamp_sec"] = timestamp_sec
                                frame_reason = str(ss.get("frame_reason", "") or "").strip()
                                if frame_reason:
                                    detail["frame_reason"] = frame_reason
                                bbox = ss.get("bbox")
                                if isinstance(bbox, list) and bbox:
                                    detail["bbox"] = list(bbox)
                                if detail:
                                    step_keyframe_details.append(detail)

                            step_payload: Dict[str, Any] = {
                                "step_id": step_id if step_id > 0 else clip.get("step_id"),
                                "step_description": clip.get("step_description", ""),
                                "description": clip.get("step_description", ""),
                                "step_type": str(clip.get("step_type") or "MAIN_FLOW").strip().upper() or "MAIN_FLOW",
                                "main_action": str(clip.get("main_action") or "").strip(),
                                "main_operation": list(clip.get("main_operation", []) or []),
                                "precautions": list(clip.get("precautions", []) or []),
                                "step_summary": str(clip.get("step_summary") or "").strip(),
                                "operation_guidance": list(clip.get("operation_guidance", []) or []),
                                "timestamp_range": [clip.get("start_sec"), clip.get("end_sec")],
                                "materials": {
                                    "clip_id": clip.get("clip_id"),
                                    "screenshot_ids": step_ss_ids
                                }
                            }
                            if step_keyframe_details:
                                step_payload["instructional_keyframe_details"] = step_keyframe_details
                                instructional_keyframes = []
                                for detail in step_keyframe_details:
                                    keyframe_item: Dict[str, Any] = {
                                        "timestamp_sec": _safe_float(detail.get("timestamp_sec", 0.0), 0.0),
                                        "frame_reason": str(detail.get("frame_reason", "") or "").strip(),
                                    }
                                    if isinstance(detail.get("bbox"), list):
                                        keyframe_item["bbox"] = list(detail.get("bbox") or [])
                                    instructional_keyframes.append(keyframe_item)
                                if instructional_keyframes:
                                    step_payload["instructional_keyframes"] = instructional_keyframes
                            unit_steps[uid].append(step_payload)
                    
                    for uid, steps in unit_steps.items():
                        if uid in units_map:
                            # Sort by step_id
                            try:
                                steps.sort(key=lambda x: int(x["step_id"]))
                            except:
                                pass
                            units_map[uid]["instructional_steps"] = steps
                            target_node = persist_units_map.get(uid)
                            if isinstance(target_node, dict):
                                target_node["instructional_steps"] = steps
                            updated_instructional_units += 1
                            has_updates = True

                route_override_units = [
                    unit
                    for unit in semantic_units
                    if str(unit.get("_vl_route_override", "") or "").strip().lower() in {"abstract", "concrete"}
                ]
                no_needed_video_units = [
                    str(unit.get("unit_id", "") or "")
                    for unit in route_override_units
                    if bool(unit.get("_vl_no_needed_video", False))
                ]
                should_type_abstract_units = [
                    str(unit.get("unit_id", "") or "")
                    for unit in route_override_units
                    if str(unit.get("_vl_route_override", "") or "").strip().lower() == "abstract"
                    and not bool(unit.get("_vl_no_needed_video", False))
                ]
                should_type_concrete_units = [
                    str(unit.get("unit_id", "") or "")
                    for unit in route_override_units
                    if str(unit.get("_vl_route_override", "") or "").strip().lower() == "concrete"
                ]
                synced_route_override_units = 0
                for unit in route_override_units:
                    unit_id = str(unit.get("unit_id", "") or "").strip()
                    if not unit_id:
                        continue
                    target_node = persist_units_map.get(unit_id)
                    if not isinstance(target_node, dict):
                        continue
                    target_node["_vl_route_override"] = str(unit.get("_vl_route_override", "") or "").strip()
                    target_node["_vl_no_needed_video"] = bool(unit.get("_vl_no_needed_video", False))
                    synced_route_override_units += 1
                if synced_route_override_units > 0:
                    has_updates = True

                updated_concrete_main_content_units = 0
                if vl_unit_analysis_outputs:
                    for unit_output in vl_unit_analysis_outputs:
                        if not isinstance(unit_output, dict):
                            continue
                        analysis_mode = str(unit_output.get("analysis_mode", "") or "").strip().lower()
                        if analysis_mode != "concrete":
                            continue
                        unit_id = str(unit_output.get("unit_id", "") or "").strip()
                        if not unit_id:
                            continue
                        raw_segments = unit_output.get("raw_response_json", []) or []
                        if not isinstance(raw_segments, list):
                            raw_segments = []
                        normalized_segments: List[Dict[str, Any]] = []
                        for index, segment in enumerate(raw_segments, start=1):
                            if not isinstance(segment, dict):
                                continue
                            segment_id = int(
                                _safe_float(
                                    segment.get("segment_id", segment.get("id", index)),
                                    float(index),
                                )
                            )
                            if segment_id <= 0:
                                segment_id = index
                            main_content = str(segment.get("main_content", "") or "").strip()
                            if not main_content:
                                continue
                            normalized_segments.append(
                                {
                                    "segment_id": segment_id,
                                    "segment_description": str(
                                        segment.get(
                                            "segment_description",
                                            segment.get("step_description", ""),
                                        )
                                        or ""
                                    ).strip(),
                                    "main_content": main_content,
                                    "clip_start_sec": _safe_float(segment.get("clip_start_sec", 0.0), 0.0),
                                    "clip_end_sec": _safe_float(segment.get("clip_end_sec", 0.0), 0.0),
                                    "instructional_keyframes": list(segment.get("instructional_keyframes", []) or []),
                                }
                            )
                        normalized_segments.sort(
                            key=lambda item: int(_safe_float(item.get("segment_id", 0), 0.0))
                        )
                        final_main_content = "\n\n".join(
                            [
                                str(item.get("main_content", "") or "").strip()
                                for item in normalized_segments
                                if str(item.get("main_content", "") or "").strip()
                            ]
                        ).strip()
                        if final_main_content:
                            if unit_id in units_map and isinstance(units_map[unit_id], dict):
                                units_map[unit_id]["full_text"] = final_main_content
                                units_map[unit_id]["text"] = final_main_content
                                units_map[unit_id]["_vl_concrete_segments"] = normalized_segments
                            target_node = persist_units_map.get(unit_id)
                            if isinstance(target_node, dict):
                                target_node["full_text"] = final_main_content
                                target_node["text"] = final_main_content
                                target_node["_vl_concrete_segments"] = normalized_segments
                            updated_concrete_main_content_units += 1
                            has_updates = True
                        concrete_analysis_records.append(
                            {
                                "unit_id": unit_id,
                                "analysis_mode": "concrete",
                                "raw_response_json": raw_segments,
                                "normalized_segments": normalized_segments,
                                "final_main_content": final_main_content,
                                "clip_requests": list(unit_output.get("clip_requests", []) or []),
                                "screenshot_requests": list(unit_output.get("screenshot_requests", []) or []),
                                "raw_llm_interactions": list(unit_output.get("raw_llm_interactions", []) or []),
                            }
                        )

                if has_updates:
                    with open(semantic_units_path, "w", encoding="utf-8") as f:
                        json.dump(persist_payload, f, ensure_ascii=False, indent=2)
                    logger.info(
                        f"[{task_id}] Persisted semantic_units updates to {semantic_units_path} "
                        f"(material_request_units={updated_material_request_units}, "
                        f"instructional_units={updated_instructional_units}, "
                        f"concrete_main_content_units={updated_concrete_main_content_units}, "
                        f"route_override_units={synced_route_override_units}, "
                        f"no_needed_video_units={len(no_needed_video_units)}, "
                        f"should_type_abstract_units={len(should_type_abstract_units)}, "
                        f"should_type_concrete_units={len(should_type_concrete_units)})"
                    )

            except Exception as e:
                logger.error(f"[{task_id}] Failed to persist instructional_steps: {e}")

            if concrete_analysis_records:
                try:
                    intermediates_dir = os.path.join(output_dir, "intermediates")
                    os.makedirs(intermediates_dir, exist_ok=True)
                    concrete_analysis_path = os.path.join(intermediates_dir, "vl_concrete_analysis.json")
                    concrete_analysis_payload = {
                        "task_id": task_id,
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "vl_model": vl_model_name,
                        "unit_count": len(concrete_analysis_records),
                        "units": concrete_analysis_records,
                    }
                    enqueue_json_write(
                        concrete_analysis_path,
                        concrete_analysis_payload,
                        ensure_ascii=False,
                        indent=2,
                        scope_key=output_dir,
                    )
                    logger.info(
                        f"[{task_id}] queued concrete VL analysis output: path={concrete_analysis_path}, "
                        f"units={len(concrete_analysis_records)}"
                    )
                except Exception as concrete_persist_error:
                    logger.warning(
                        f"[{task_id}] queue concrete VL analysis output failed: {concrete_persist_error}"
                    )

            _persist_task_token_report({
                "status": "success",
                "vl_enabled": True,
                "used_fallback": False,
                "vl_model": vl_model_name,
                "routing_stats": routing_stats,
                "token_stats": vl_token_stats,
                "result_counts": {
                    "semantic_units_total": len(semantic_units),
                    "vl_units": len(vl_units),
                    "screenshots": len(screenshot_requests),
                    "clips": len(clip_requests),
                    "vl_clips_generated": len(vl_clip_requests),
                    "vl_screenshots_generated": len(vl_screenshot_requests),
                },
            })
            _persist_vl_analysis_output({
                "status": "success",
                "vl_enabled": True,
                "used_fallback": False,
                "vl_model": vl_model_name,
                "routing_stats": routing_stats,
                "token_stats": vl_token_stats,
                "result_counts": {
                    "semantic_units_total": len(semantic_units),
                    "vl_units": len(vl_units),
                    "screenshots": len(screenshot_requests),
                    "clips": len(clip_requests),
                    "vl_clips_generated": len(vl_clip_requests),
                    "vl_screenshots_generated": len(vl_screenshot_requests),
                },
                "merged_screenshots": merged_screenshots,
                "merged_clips": merged_clips,
            })
            _update_vl_heartbeat_state(
                status="completed",
                checkpoint="vl_response_ready",
                completed=max(1, vl_budget_seconds),
                pending=0,
            )
            _emit_vl_heartbeat(signal_type="hard")

            return video_processing_pb2.VLAnalysisResponse(
                success=True,
                vl_enabled=True,
                used_fallback=False,
                screenshot_requests=screenshot_requests,
                clip_requests=clip_requests,
                units_analyzed=len(vl_units),
                vl_clips_generated=len(vl_clip_requests),
                vl_screenshots_generated=len(vl_screenshot_requests),
                error_msg=""
            )

        except Exception as e:
            error_detail = str(getattr(e, "_display_error_detail", "") or str(e))
            logger.error(f"[{task_id}] AnalyzeWithVL 异常: {error_detail}", exc_info=True)
            _update_vl_heartbeat_state(
                status="failed",
                checkpoint="vl_exception",
                completed=max(1, vl_budget_seconds),
                pending=0,
            )
            _emit_vl_heartbeat(signal_type="hard")
            _persist_task_token_report({
                "status": "exception",
                "vl_enabled": True,
                "used_fallback": True,
                "vl_model": vl_model_name,
                "error_msg": error_detail,
                "routing_stats": {},
                "token_stats": {},
            })
            _persist_vl_analysis_output({
                "status": "exception",
                "vl_enabled": True,
                "used_fallback": True,
                "vl_model": vl_model_name,
                "error_msg": error_detail,
                "routing_stats": {},
                "token_stats": {},
                "result_counts": {
                    "semantic_units_total": 0,
                    "vl_units": 0,
                    "screenshots": 0,
                    "clips": 0,
                },
                "merged_screenshots": [],
                "merged_clips": [],
            })
            _persist_vl_last_error(
                {
                    "status": "exception",
                    "checkpoint": "vl_exception",
                    "error_msg": error_detail,
                    "error_type": type(e).__name__,
                    "traceback": traceback.format_exc(),
                    "routing_stats": {},
                    "token_stats": vl_token_stats,
                    "failure_snapshot": dict(vl_failure_snapshot or {}),
                    "unit_failures": _summarize_vl_unit_failures(vl_unit_analysis_outputs),
                }
            )
            return video_processing_pb2.VLAnalysisResponse(
                success=False,
                vl_enabled=True,
                used_fallback=True,
                error_msg=error_detail
            )
        finally:
            if vl_task is not None and not vl_task.done():
                vl_task.cancel()
                try:
                    await vl_task
                except Exception as cleanup_error:
                    logger.warning(f"[{task_id}] VL 任务清理失败: {cleanup_error}")
            elif vl_task is not None:
                try:
                    _ = vl_task.exception()
                except Exception:
                    pass
            if vl_generator is not None:
                try:
                    await vl_generator.close()
                except Exception as cleanup_error:
                    logger.warning(f"[{task_id}] VL 生成器关闭失败: {cleanup_error}")
            if routing_generator is not None and routing_generator is not vl_generator:
                try:
                    await routing_generator.close()
                except Exception as cleanup_error:
                    logger.warning(f"[{task_id}] VL 路由生成器关闭失败: {cleanup_error}")
            vl_heartbeat_stop.set()
            if vl_heartbeat_thread is not None and vl_heartbeat_thread.is_alive():
                vl_heartbeat_thread.join(timeout=2.0)
            self._decrement_tasks()

    async def _validation_release_cv_resources_impl(self, request, context):
        """
        执行逻辑：
        1) 清理 CV 验证器缓存。
        2) 触发 GC 释放内存。
        实现方式：cleanup_cv_validators + gc.collect。
        核心价值：释放长期占用资源，保持进程稳定。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - ReleaseResourcesResponse（包含释放结果与提示信息）。"""
        task_id = request.task_id
        logger.info(f"[{task_id}] Request to ReleaseCVResources")
        
        try:
            # 清理所有缓存的验证器
            self.resources.cleanup_cv_validators()
            self.resources.cleanup_visual_extractors()
            self.resources.cleanup_video_tools()
            
            # 强制 GC
            import gc
            gc.collect()
            
            return video_processing_pb2.ReleaseResourcesResponse(
                success=True,
                message="Successfully released all CV resources and cleared caches.",
                freed_workers_count=0, # Currently we just clear dictionaries
                freed_memory_mb=0.0    # Detailed memory tracking not implemented
            )
        except Exception as e:
            logger.error(f"[{task_id}] ReleaseCVResources Failed: {e}")
            return video_processing_pb2.ReleaseResourcesResponse(
                success=False,
                message=f"Failed to release resources: {str(e)}",
                freed_workers_count=0,
                freed_memory_mb=0.0
            )

from .stages.phase2a_stage import Phase2AMaterialStageMixin
from .stages.validation_vl_stage import ValidationAndVLStageMixin


class VideoProcessingServicer(
    ValidationAndVLStageMixin,
    Phase2AMaterialStageMixin,
    _VideoProcessingServicerCore,
):
    """按阶段职责组合后的 gRPC 服务实现。"""

    pass


async def serve(host: str = "0.0.0.0", port: int = 50051):
    """
    执行逻辑：
    1) 创建 gRPC aio 服务器并注册服务。
    2) 监听指定地址并启动服务。
    3) 等待终止信号并执行优雅关闭。
    实现方式：grpc.aio.server + 线程池执行器。
    核心价值：提供稳定的 RPC 接入点。
    决策逻辑：
    - 条件：hasattr(servicer, 'process_pool')
    依据来源（证据链）：
    - Servicer 属性：process_pool 是否存在。
    输入参数：
    - host: 函数入参（类型：str）。
    - port: 函数入参（类型：int）。
    输出参数：
    - 无（仅产生副作用，如启动/停止服务）。"""
    server, servicer, listen_addr = _bootstrap_grpc_server(host=host, port=port)
    logger.info(f"Starting gRPC server on {listen_addr}")
    await server.start()

    try:
        await server.wait_for_termination()
    finally:
        await _shutdown_grpc_server(server=server, servicer=servicer)


def _load_server_config() -> Dict[str, Any]:
    """加载服务配置。

    职责边界：
    - 仅负责通过统一解析器读取配置。
    - 不执行服务实例化与网络监听。
    """
    config_path = resolve_video_config_path(anchor_file=__file__)
    config = _load_yaml_file(config_path) if config_path else {}
    logger.info(f"Loaded config from {config_path}: {list(config.keys())}")
    return config


def _bootstrap_grpc_server(host: str, port: int):
    """构建 gRPC 服务器与服务实例。

    职责边界：
    - 负责创建 server、servicer 并注册 gRPC 服务。
    - 不负责阻塞等待与优雅关闭编排。
    """
    server = aio.server()
    logger.info("初始化 VideoProcessingServicer（首次 warmup 可能较慢）...")
    init_t0 = time.perf_counter()

    config = _load_server_config()
    servicer = VideoProcessingServicer(config)
    logger.info(f"VideoProcessingServicer initialized in {time.perf_counter() - init_t0:.2f}s")
    video_processing_pb2_grpc.add_VideoProcessingServiceServicer_to_server(servicer, server)

    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    return server, servicer, listen_addr


async def _shutdown_grpc_server(server, servicer) -> None:
    """执行 gRPC 服务优雅关闭。"""
    logger.info("Stopping server...")
    await server.stop(5)
    if hasattr(servicer, "process_pool"):
        servicer.process_pool.shutdown()
        logger.info("Process pool shut down")
    llm_client = getattr(servicer, "llm_client", None)
    if llm_client is not None:
        close_func = getattr(llm_client, "close", None)
        if callable(close_func):
            try:
                result = close_func()
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.warning("Servicer LLM client close failed: %s", exc)
    try:
        from services.python_grpc.src.content_pipeline.infra.llm import llm_client as module2_llm_client

        await module2_llm_client.shutdown_pool_manager()
    except Exception as exc:
        logger.warning("Module2 LLM pool shutdown failed: %s", exc)
    try:
        from services.python_grpc.src.content_pipeline.infra.llm import vision_ai_client

        await vision_ai_client.shutdown_vision_ai_client()
    except Exception as exc:
        logger.warning("VisionAI client shutdown failed: %s", exc)
    try:
        from services.python_grpc.src.content_pipeline.phase2a.materials import vl_video_analyzer

        await vl_video_analyzer.shutdown_vl_http_client_pool()
    except Exception as exc:
        logger.warning("VL HTTP pool shutdown failed: %s", exc)
    try:
        from services.python_grpc.src.transcript_pipeline.llm import client as transcript_llm_client

        await transcript_llm_client.shutdown_deepseek_client_cache()
    except Exception as exc:
        logger.warning("Transcript LLM client shutdown failed: %s", exc)


if __name__ == "__main__":
    # Windows下多进程必须在 if __name__ == "__main__" 保护下
    # 并通过 freeze_support 支持打包 (虽然这里不需要打包)
    from multiprocessing import freeze_support
    freeze_support()
    
    configure_pipeline_logging(
        level=logging.INFO,
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        force=True,
    )
    
    asyncio.run(serve())
