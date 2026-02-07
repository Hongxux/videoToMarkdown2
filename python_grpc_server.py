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
    if path_value in sys.path:
        return
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

    # 覆盖 python_grpc_server.py 顶部 import 以及其主要依赖链路
    modules_to_check = [
        ("psutil", "psutil"),
        ("grpc", "grpcio"),
        ("grpc.aio", "grpcio"),
        ("numpy", "numpy"),
        # gRPC 生成代码与内部模块（能进一步暴露缺失的三方依赖）
        ("proto.video_processing_pb2", None),
        ("proto.video_processing_pb2_grpc", None),
        ("stage1_pipeline.graph", None),
        ("videoToMarkdown.knowledge_engine.core.video", None),
        ("videoToMarkdown.knowledge_engine.core.transcription", None),
        ("MVP_Module2_HEANCING.module2_content_enhancement", None),
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
        _safe_print("2) 若仍失败：python python_grpc_server.py --check-deps --debug-imports")
        return 2

    _safe_print("依赖预检通过。")
    return 0


_safe_print("🚀 PYTHON GRPC SERVER IS STARTING - VERSION V3.1 (PARALLEL CV) 🚀")

# 添加项目路径（尽量前置，便于 --check-deps 也能检查内部模块）
current_dir = os.path.dirname(os.path.abspath(__file__))
_prepend_sys_path(current_dir)
_prepend_sys_path(os.path.dirname(current_dir))
_prepend_sys_path(os.path.join(current_dir, "MVP_Module2_HEANCING"))
_prepend_sys_path(os.path.join(current_dir, "proto"))

if _CHECK_DEPS:
    raise SystemExit(_run_dependency_preflight())
_boot("[BOOT] import logging")
import logging
import asyncio
import threading
_boot("[BOOT] import psutil")
import psutil
import traceback
import time
import hashlib
import shutil
from concurrent import futures
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
from urllib.request import url2pathname

_boot("[BOOT] import grpc")
import grpc
import gc
_boot("[BOOT] import numpy")
import numpy as np
from grpc import aio
import functools

# gRPC 生成的代码 (需要先运行 protoc 生成)
_boot("[BOOT] import gRPC pb2/pb2_grpc")
from proto import video_processing_pb2
from proto import video_processing_pb2_grpc

# 模块导入
_boot("[BOOT] import stage1_pipeline.graph")
from stage1_pipeline.graph import run_pipeline
_boot("[BOOT] import videoToMarkdown (VideoProcessor)")
from videoToMarkdown.knowledge_engine.core.video import VideoProcessor
_boot("[BOOT] import videoToMarkdown (Transcriber)")
from videoToMarkdown.knowledge_engine.core.transcription import Transcriber
_boot("[BOOT] import MVP_Module2_HEANCING.module2_content_enhancement")
from MVP_Module2_HEANCING.module2_content_enhancement import (
    RichTextPipeline,
    PipelineConfig,
    ScreenshotRequest,
    ClipRequest,
    MaterialRequests
)
from MVP_Module2_HEANCING.module2_content_enhancement.visual_feature_extractor import (
    VisualFeatureExtractor,
    get_visual_process_pool,
    get_shared_frame_registry
)
# 🔑 Import tools for GenerateMaterialRequests
from MVP_Module2_HEANCING.module2_content_enhancement.screenshot_selector import ScreenshotSelector
from MVP_Module2_HEANCING.module2_content_enhancement.video_clip_extractor import VideoClipExtractor
from MVP_Module2_HEANCING.module2_content_enhancement.llm_client import AdaptiveConcurrencyLimiter

logger = logging.getLogger(__name__)

# 配置日志级别和格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True  # 强制重新配置,覆盖之前的配置
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
    storage_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "storage"))
    abs_video_path = _normalize_local_video_path(video_path)
    
    try:
        if os.path.commonpath([abs_video_path, storage_root]) == storage_root:
            return os.path.dirname(abs_video_path)
    except ValueError:
        # 不同盘符时 commonpath 会抛错，直接走 hash 分支
        pass
    
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
    storage_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "storage"))

    try:
        if os.path.commonpath([abs_video_path, storage_root]) == storage_root:
            return abs_video_path
    except ValueError:
        # 不同盘符时 commonpath 会抛错，直接走复制/硬链接分支
        pass

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
                from MVP_Module2_HEANCING.module2_content_enhancement.cv_knowledge_validator import CVKnowledgeValidator
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
            
            # 🚀 CV Validators Cache
            self._cv_validators = {}
            self._cv_validator_lock = threading.Lock()
            
            self._initialized = True
            logger.info("✅ Global resources config saved (lazy loading enabled)")

    @property
    def transcriber(self):
        """
        执行逻辑：
        1) 懒加载 Transcriber 实例。
        2) 返回已缓存的转写器。
        实现方式：双重检查锁 + 延迟导入。
        核心价值：减少冷启动时间并复用昂贵资源。
        决策逻辑：
        - 条件：self._transcriber is None
        依据来源（证据链）：
        - 对象内部状态：self._transcriber。
        输入参数：
        - 无。
        输出参数：
        - Transcriber 实例或 None。"""
        if self._transcriber is None:
            with self._lock:
                if self._transcriber is None:
                    try:
                        from videoToMarkdown.knowledge_engine.core.transcription import Transcriber
                        self._transcriber = Transcriber()
                        logger.info("  → Transcriber loaded lazily")
                    except Exception as e:
                        logger.error(f"Transcriber init failed: {e}")
        return self._transcriber

    @property
    def knowledge_classifier(self):
        """
        执行逻辑：
        1) 懒加载 KnowledgeClassifier。
        2) 返回已缓存的分类器实例。
        实现方式：双重检查锁 + 延迟导入。
        核心价值：避免重复初始化 LLM 相关资源。
        决策逻辑：
        - 条件：self._knowledge_classifier is None
        依据来源（证据链）：
        - 对象内部状态：self._knowledge_classifier。
        输入参数：
        - 无。
        输出参数：
        - KnowledgeClassifier 实例或 None。"""
        if self._knowledge_classifier is None:
            with self._lock:
                if self._knowledge_classifier is None:
                    try:
                        from MVP_Module2_HEANCING.module2_content_enhancement.knowledge_classifier import KnowledgeClassifier
                        # LLMClient 内部现在也是延迟加载 httpx 客户端的，所以这里初始化是安全的
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
                        from MVP_Module2_HEANCING.module2_content_enhancement.vision_ai_client import get_vision_ai_client
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
            from MVP_Module2_HEANCING.module2_content_enhancement.vision_ai_client import get_vision_ai_client
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
                    from MVP_Module2_HEANCING.module2_content_enhancement.cv_knowledge_validator import CVKnowledgeValidator
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


class VideoProcessingServicer(video_processing_pb2_grpc.VideoProcessingServiceServicer):
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
        
        # 🔑 使用全局资源管理器
        self.resources = GlobalResourceManager()
        self.resources.initialize(config)
        
        # 活跃任务计数
        self._active_tasks = 0
        self._task_lock = threading.Lock()
        self._cache_metrics_task_id = None

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
        from concurrent.futures import ProcessPoolExecutor
        import multiprocessing
        
        # 🚀 释放物理算力: 设为 CPU 核心数
        # 实际负载由 Java 端 Semaphore 精确控制，Python 端只提供最大能力底座
        # [FIX] 动态计算 Worker 数量防止 OOM
        # 策略: 至少保留 4GB 给系统，剩余内存每 3GB 允许一个 Worker (Windows Spawn 模式开销大)
        mem = psutil.virtual_memory()
        available_ram_gb = mem.transferable if hasattr(mem, 'transferable') else mem.available / (1024**3)
        # 保底 1 个, 上限 8 个 (或 CPU 核心数-1)
        cpu_cores = multiprocessing.cpu_count()
        # [User Request] 1.5GB per worker
        max_workers_by_ram = max(1, int((available_ram_gb - 4) / 1.5))
        # 🚀 OOM Fix: Force max workers to 4 on Windows to prevent PageFile overflow
        HARD_CAP = 6
        self.cv_worker_count = min(max(1, cpu_cores-1), max_workers_by_ram, HARD_CAP)
        logger.info(
            f"🚀 CV ProcessPool Config: {self.cv_worker_count} workers "
            f"(Limit by RAM: {max_workers_by_ram}, CPU: {cpu_cores}, HardCap: {HARD_CAP})"
        )

        
        # 创建 ProcessPool (使用 spawn 方式确保 Windows 兼容)
        from cv_worker import init_cv_worker
        self.cv_process_pool = ProcessPoolExecutor(
            max_workers=self.cv_worker_count,
            initializer=init_cv_worker
        )
        
        # SharedFrameRegistry 用于主进程预读帧
        self.frame_registry = get_shared_frame_registry()
        
        logger.info(f"🚀 CV ProcessPool created: {self.cv_worker_count} workers + SharedMemory")
        logger.info("VideoProcessingServicer initialized (Java controls concurrency)")



    
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

    async def DownloadVideo(self, request, context):
        """
        执行逻辑：
        1) 以 URL 哈希创建 storage/{hash} 目录。
        2) 调用 VideoProcessor 下载视频到固定文件名。
        3) 计算文件大小与时长并返回结果。
        实现方式：VideoProcessor.download + ffprobe。
        核心价值：统一下载路径，便于后续复用与清理。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - DownloadResponse（含 video_path、file_size_bytes、duration_sec）。"""
        task_id = request.task_id
        self._cache_metrics_begin(task_id, "DownloadVideo")
        video_url = request.video_url
        # output_dir 从 request 中获取，但我们会覆盖为统一的 storage 目录
        
        logger.info(f"[{task_id}] DownloadVideo: {video_url}")
        
        try:
            self._increment_tasks()

            # 🔑 生成基于 URL 的哈希值作为目录名
            import hashlib
            url_hash = hashlib.md5(video_url.encode('utf-8')).hexdigest()
            
            # 🔑 统一存储目录: storage/{url_hash}/
            storage_root = os.path.join(os.path.dirname(__file__), "storage")
            task_dir = os.path.join(storage_root, url_hash)
            os.makedirs(task_dir, exist_ok=True)
            
            # 视频固定命名为 video (VideoProcessor会自动添加扩展名)
            video_filename = "video"
            
            # 使用 VideoProcessor 下载
            downloader = VideoProcessor()
            video_path = await asyncio.to_thread(
                downloader.download,
                url=video_url,
                output_dir=task_dir,
                filename=video_filename
            )
            
            # 获取视频时长
            duration_sec = self._get_video_duration(video_path)
            file_size = os.path.getsize(video_path)
            
            logger.info(f"[{task_id}] Video saved to: {video_path}")
            
            return video_processing_pb2.DownloadResponse(
                success=True,
                video_path=video_path,
                file_size_bytes=file_size,
                duration_sec=duration_sec,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] DownloadVideo failed: {e}")
            return video_processing_pb2.DownloadResponse(
                success=False,
                video_path="",
                file_size_bytes=0,
                duration_sec=0.0,
                error_msg=str(e)
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
        language = request.language or "zh"
        
        logger.info(f"[{task_id}] TranscribeVideo: {video_path}")
        
        try:
            self._increment_tasks()
            
            # 统一输出目录到 storage/{hash}：做什么是集中字幕产物；为什么是避免源目录污染；权衡是多一次路径映射
            output_dir = _normalize_output_dir(video_path)
            
            # 确保目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 🔑 检查是否已存在字幕文件（缓存复用）
            subtitle_path = os.path.join(output_dir, "subtitles.txt")
            
            if os.path.exists(subtitle_path):
                # 复用已有字幕
                with open(subtitle_path, "r", encoding="utf-8") as f:
                    subtitle_text = f.read()
                logger.info(f"[{task_id}] ✅ Reusing existing subtitles: {subtitle_path}")
            else:
                # 🔑 使用全局单例 Transcriber
                transcriber = self.resources.transcriber
                if not transcriber:
                    raise RuntimeError("Global Transcriber not initialized")
                
                # transcribe 是异步方法
                subtitle_text = await transcriber.transcribe(video_path)
                
                # 🔑 保存字幕文件为 subtitles.txt
                with open(subtitle_path, "w", encoding="utf-8") as f:
                    f.write(subtitle_text)
                
                logger.info(f"[{task_id}] Subtitles saved to: {subtitle_path}")
            
            return video_processing_pb2.TranscribeResponse(
                success=True,
                subtitle_path=subtitle_path,
                subtitle_text=subtitle_text[:100] + "..." if len(subtitle_text) > 100 else subtitle_text, 
                segments=[],
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] TranscribeVideo failed: {e}")
            return video_processing_pb2.TranscribeResponse(
                success=False,
                subtitle_path="",
                subtitle_text="",
                segments=[],
                error_msg=str(e)
            )
        finally:
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
        
        # 输出文件路径
        step2_path = os.path.join(intermediates_dir, "step2_correction_output.json")
        step6_path = os.path.join(intermediates_dir, "step6_merge_cross_output.json")
        
        logger.info(f"[{task_id}] ProcessStage1: max_step={max_step}, output_dir={output_dir}")
        
        try:
            self._increment_tasks()
            
            # 🔑 检查是否已存在输出文件（缓存复用）
            local_sentence_ts = os.path.join(output_dir, "local_storage", "sentence_timestamps.json")
            need_sentence_ts = not os.path.exists(local_sentence_ts)
            if os.path.exists(step2_path) and os.path.exists(step6_path) and not need_sentence_ts:
                logger.info(f"[{task_id}] ✅ Reusing existing Stage1 outputs")
            else:
                if os.path.exists(step2_path) and os.path.exists(step6_path) and need_sentence_ts:
                    logger.warning(f"[{task_id}] sentence_timestamps.json missing, regenerating Step4 (and upstream) outputs")
                # 🔑 调用 Stage1 Pipeline (支持 max_step)
                # 确保 sentence_timestamps 至少经过 step4_clean_local 生成
                effective_max_step = max_step if max_step >= 4 else 4
                await run_pipeline(
                   video_path=video_path,
                   subtitle_path=subtitle_path,
                   output_dir=output_dir,
                   max_step=effective_max_step
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
                except Exception as e:
                    logger.warning(f"[{task_id}] Copy sentence_timestamps.json failed: {e}")
                    sentence_timestamps_path = local_sentence_ts
            else:
                logger.warning(f"[{task_id}] sentence_timestamps.json not found at {local_sentence_ts}")
                sentence_timestamps_path = inter_sentence_ts if os.path.exists(inter_sentence_ts) else ""
            
            return video_processing_pb2.Stage1Response(
                success=True,
                step2_json_path=step2_path,
                step6_json_path=step6_path,
                sentence_timestamps_path=sentence_timestamps_path,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] ProcessStage1 failed: {e}")
            return video_processing_pb2.Stage1Response(
                success=False,
                step2_json_path="",
                step6_json_path="",
                sentence_timestamps_path="",
                error_msg=str(e)
            )
        finally:
            self._decrement_tasks()
    
    async def AnalyzeSemanticUnits(self, request, context):
        """
        执行逻辑：
        1) 归档视频并确定 Phase2A 输出目录。
        2) 若 semantic_units_phase2a.json 已存在则直接复用并解析。
        3) 否则构建 RichTextPipeline + VisualFeatureExtractor 执行 analyze_only。
        4) 将 screenshot/clip 结果转换为 protobuf 返回。
        实现方式：RichTextPipeline + JSON 读写。
        核心价值：复用 Phase2A 结果，避免重复分析。
        决策逻辑：
        - 条件：os.path.exists(semantic_units_path)
        依据来源（证据链）：
        - 文件系统状态：semantic_units_path 是否存在。
        输入参数：
        - request: 函数入参（类型：未标注）。
        - context: 函数入参（类型：未标注）。
        输出参数：
        - AnalyzeResponse（含 screenshot_requests/clip_requests/semantic_units_json_path）。"""
        import os  # Explicit local import
        task_id = request.task_id
        # 统一本地视频归档到 storage/{hash}：做什么是统一 Phase2A 路径；为什么是避免素材找不到；权衡是多一次 I/O
        video_path = _ensure_local_video_in_storage(request.video_path)
        step2_json_path = os.path.abspath(request.step2_json_path) if request.step2_json_path else "" # Convert to absolute path immediately
        step6_json_path = os.path.abspath(request.step6_json_path) if request.step6_json_path else "" # Convert to absolute path immediately
        sentence_timestamps_path = os.path.abspath(request.sentence_timestamps_path) if request.sentence_timestamps_path else ""
        
        # 统一输出目录到 storage/{hash}：做什么是让 Phase2A 产物与后续一致；为什么是减少跨目录查找；权衡是忽略外部路径差异
        output_dir = _normalize_output_dir(video_path)
        semantic_units_path = os.path.join(output_dir, "semantic_units_phase2a.json")
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
        
        try:
            self._increment_tasks()
            
            # 🔑 检查是否已存在 Phase2A 输出（缓存复用）
            if os.path.exists(semantic_units_path):
                logger.warning(
                    f"[{task_id}] ✅ Reusing existing Phase2A output: {semantic_units_path} "
                    f"(cache hit -> 不会进入 _collect_material_requests；如需验证新策略请删除该文件后重跑)"
                )
                
                # 从已有文件中提取 screenshot 和 clip 请求
                import json
                with open(semantic_units_path, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                
                pb_screenshots = []
                pb_clips = []
                
                # 解析 semantic_units 提取素材需求
                # JSON 格式是一个列表，每个单元有 material_requests.screenshot_requests 和 material_requests.clip_requests
                semantic_units = cached_data if isinstance(cached_data, list) else cached_data.get("semantic_units", [])
                
                for su in semantic_units:
                    material_reqs = su.get("material_requests", {})
                    unit_id = su.get("unit_id", "")
                    
                    # 获取 screenshots
                    for ss in material_reqs.get("screenshot_requests", []):
                        pb_screenshots.append(video_processing_pb2.ScreenshotRequest(
                            screenshot_id=ss.get("screenshot_id", f"ss_{unit_id}"),
                            timestamp_sec=ss.get("timestamp_sec", 0.0),
                            label=ss.get("label", ""),
                            semantic_unit_id=ss.get("semantic_unit_id", unit_id)
                        ))
                    
                    # 获取 clips
                    for clip in material_reqs.get("clip_requests", []):
                        pb_clips.append(self._build_clip_request_pb(clip, unit_id))
                
                logger.info(f"[{task_id}] Loaded from cache: {len(pb_screenshots)} screenshots, {len(pb_clips)} clips")
                
                return video_processing_pb2.AnalyzeResponse(
                    success=True,
                    screenshot_requests=pb_screenshots,
                    clip_requests=pb_clips,
                    semantic_units_json_path=semantic_units_path,
                    error_msg=""
                )
            
            
            # 确保目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 🔑 创建 RichTextPipeline (使用正确的构造函数签名)
            pipeline = RichTextPipeline(
                video_path=video_path,
                step2_path=step2_json_path,
                step6_path=step6_json_path,
                output_dir=output_dir,
                sentence_timestamps_path=sentence_timestamps_path
            )
            
            # 🚀 注入视觉提取器，使 Phase2A 能够执行视觉打分推荐最佳时间戳
            visual_extractor = VisualFeatureExtractor(video_path)
            pipeline.set_visual_extractor(visual_extractor)
            
            logger.warning(f"[{task_id}] Entering _collect_material_requests via analyze_only()")
            # 🔑 调用 Phase2A: analyze_only
            screenshot_requests, clip_requests, semantic_units_path = await pipeline.analyze_only()
            
            # 转换为 protobuf 格式
            pb_screenshots = [
                video_processing_pb2.ScreenshotRequest(
                    screenshot_id=r.screenshot_id,
                    timestamp_sec=r.timestamp_sec,
                    label=r.label,
                    semantic_unit_id=r.semantic_unit_id
                )
                for r in screenshot_requests
            ]
            
            pb_clips = [
                self._build_clip_request_pb(r, getattr(r, "semantic_unit_id", ""))
                for r in clip_requests
            ]
            
            return video_processing_pb2.AnalyzeResponse(
                success=True,
                screenshot_requests=pb_screenshots,
                clip_requests=pb_clips,
                semantic_units_json_path=semantic_units_path,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] AnalyzeSemanticUnits failed: {e}")
            logger.exception(e)  # Log full traceback
            return video_processing_pb2.AnalyzeResponse(
                success=False,
                screenshot_requests=[],
                clip_requests=[],
                semantic_units_json_path="",
                error_msg=str(e)
            )
        finally:
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
                classifier.step2_path = request.step2_path
                classifier._all_subtitles_cache = None  # 清除缓存
            
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

    async def GenerateMaterialRequests(self, request, context):
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
            from MVP_Module2_HEANCING.module2_content_enhancement.rich_text_pipeline import RichTextPipeline
            from MVP_Module2_HEANCING.module2_content_enhancement.screenshot_range_calculator import ScreenshotRangeCalculator
            
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
                sentence_timestamps_path=sentence_timestamps_path
            )
            # 🚀 Fix: Ensure video_duration is a float
            video_duration = float(request.video_duration) if hasattr(request, 'video_duration') and request.video_duration else 0.0
            calculator = ScreenshotRangeCalculator(video_duration)
            selector = self.resources.get_screenshot_selector(video_path)
            
            # 转换 gRPC units 为 SemanticUnit 对象
            from MVP_Module2_HEANCING.module2_content_enhancement.semantic_unit_segmenter import SemanticUnit
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
                    logger.info("======GenerateMaterialRequests开始进行转换======")
                    logger.info(f"Action Unit: start_sec={au.start_sec}, end_sec={au.end_sec}, knowledge_type={au.knowledge_type}")
                
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
                        "clip_id": f"clip_{unit_id}_action{i}",
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
                # Step 1: 批量读取所有需要的帧到 SharedMemory
                shm_map = await self._batch_read_frames_for_screenshots(
                    video_path, 
                    screenshot_tasks
                )
                
                # Step 2: 提交到 ProcessPool 并行计算
                from cv_worker import run_screenshot_selection_task
                loop = asyncio.get_event_loop()
                futures = []
                
                for task in screenshot_tasks:
                    key = f"{task['unit_id']}_island{task['island_index']}"
                    task_shm_frames = shm_map.get(key, {})
                    
                    if not task_shm_frames:
                        # 回退：如果没有读取到帧，使用中点时间戳
                        final_ss.append(video_processing_pb2.ScreenshotRequest(
                            screenshot_id=f"{task['unit_id']}_island{task['island_index']}",
                            timestamp_sec=(task['expanded_start'] + task['expanded_end']) / 2,
                            label=f"稳定岛{task['island_index']}",
                            semantic_unit_id=task['unit_id']
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
                            screenshot_id=f"{result['unit_id']}_island{result['island_index']}",
                            timestamp_sec=result['selected_timestamp'],
                            label=f"稳定岛{result['island_index']}",
                            semantic_unit_id=result['unit_id']
                        ))
                
                # Step 5: 清理 SharedMemory
                # FrameRegistry 会自动管理，无需手动清理
                logger.info(f"[{task_id}] Parallel screenshot selection completed: {len(final_ss)} screenshots")
            
            logger.info(f"[{task_id}] Generated {len(final_clips)} clips, {len(final_ss)} screenshots")

            # 兜底：若未生成任何截图请求，按单元中点补齐
            if not final_ss and units:
                for unit in units:
                    mid_ts = (float(unit.start_sec) + float(unit.end_sec)) / 2
                    final_ss.append(video_processing_pb2.ScreenshotRequest(
                        screenshot_id=f"{unit.unit_id}_fallback",
                        timestamp_sec=mid_ts,
                        label="fallback",
                        semantic_unit_id=unit.unit_id
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

                    # 兼容两种结构：列表 or {"semantic_units": [...]}
                    if isinstance(data, dict):
                        units_data = data.get("semantic_units", [])
                    else:
                        units_data = data
                    
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
                    
                    # 更新 JSON 数据
                    for item in units_data:
                        unit_id = item.get("unit_id", "")
                        
                        # 更新素材请求
                        if "material_requests" not in item:
                            item["material_requests"] = {}
                        item["material_requests"]["screenshot_requests"] = unit_ss_map.get(unit_id, [])
                        item["material_requests"]["clip_requests"] = unit_clip_map.get(unit_id, [])
                        
                        # 更新 action_units
                        if unit_id in unit_action_map:
                            item["action_units"] = unit_action_map[unit_id]
                        
                        # 标记 CV 验证完成
                        item["cv_validated"] = True

                    if isinstance(data, dict):
                        data["semantic_units"] = units_data
                    
                    # 保存更新后的 JSON
                    with open(semantic_units_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    
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
        semantic_units_json_path = os.path.abspath(request.semantic_units_json_path) # Convert to absolute path immediately
        screenshots_dir = os.path.abspath(request.screenshots_dir) # Convert to absolute path immediately
        clips_dir = os.path.abspath(request.clips_dir) # Convert to absolute path immediately
        # 统一本地视频归档到 storage/{hash}：做什么是确保最终装配可追溯；为什么是与前序同域；权衡是可能增加一次 I/O
        video_path = _ensure_local_video_in_storage(request.video_path)
        title = request.title or "视频内容"
        
        # 统一输出目录到 storage/{hash}：做什么是让最终产物同域聚合；为什么是便于回放定位；权衡是覆盖调用方传入的 output_dir
        output_dir = _normalize_output_dir(video_path)
        self._cache_metrics_begin(task_id, "AssembleRichText")
        
        # 确保目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"[{task_id}] AssembleRichText (Phase2B)")
        logger.info(f"  → video_path: {video_path}")
        logger.info(f"  → semantic_units_json_path: {semantic_units_json_path}")
        logger.info(f"  → screenshots_dir: {screenshots_dir}")
        logger.info(f"  → clips_dir: {clips_dir}")
        logger.info(f"  → output_dir: {output_dir}")
        
        try:
            self._increment_tasks()
            
            # 🔑 创建 RichTextPipeline
            # 注意: Phase2B 主要使用 semantic_units_json，step2/step6 在 Phase2A 已处理
            # 此处使用占位值，实际逻辑在 assemble_only 中加载 semantic_units_json
            pipeline = RichTextPipeline(
                video_path=video_path,
                step2_path="",  # Phase2B 不需要
                step6_path="",  # Phase2B 不需要
                output_dir=output_dir
            )
            
            # 🔑 调用 Phase2B: assemble_only
            markdown_path, json_path = await pipeline.assemble_only(
                semantic_units_json_path=semantic_units_json_path,
                screenshots_dir=screenshots_dir,
                clips_dir=clips_dir,
                title=title
            )
            
            # 统计信息
            stats = video_processing_pb2.AssembleStats(
                total_sections=0,
                video_clips_count=len(os.listdir(clips_dir)) if os.path.exists(clips_dir) else 0,
                screenshots_count=len(os.listdir(screenshots_dir)) if os.path.exists(screenshots_dir) else 0,
                text_only_count=0,
                vision_validated_count=0
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
            return video_processing_pb2.AssembleResponse(
                success=False,
                markdown_path="",
                json_path="",
                stats=video_processing_pb2.AssembleStats(), # Keep stats field, but initialize empty
                error_msg=str(e)
            )
        finally:
            self._write_cache_metrics(output_dir, task_id, "AssembleRichText")
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
            clip_id = clip.get("clip_id", f"clip_{default_unit_id}")
            start_sec = clip.get("start_sec", 0.0)
            end_sec = clip.get("end_sec", 0.0)
            knowledge_type = clip.get("knowledge_type", "")
            semantic_unit_id = clip.get("semantic_unit_id", default_unit_id)
        else:
            clip_id = getattr(clip, "clip_id", f"clip_{default_unit_id}")
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

    def _cache_metrics_begin(self, task_id: str, stage: str) -> None:
        """
        执行逻辑：
        1) 按任务维度重置缓存统计（可配置）。
        2) 设置当前任务与阶段上下文。
        实现方式：调用 Module2 缓存统计器。
        核心价值：统一命中率统计口径并支持落盘。
        """
        try:
            from MVP_Module2_HEANCING.module2_content_enhancement import cache_metrics
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
            from MVP_Module2_HEANCING.module2_content_enhancement import cache_metrics
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
            
    def _batch_read_frames_to_shm(self, video_path: str, units_data: list) -> dict:
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
                        self.frame_registry.register_frame(curr_idx, frame)
                        ref = self.frame_registry.get_shm_ref(curr_idx)
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
                        self.frame_registry.register_frame(curr_idx, frame)
                        ref = self.frame_registry.get_shm_ref(curr_idx)
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
        coarse_fps: float = 2.0
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
                        self.frame_registry.register_frame(curr_idx, frame)
                        ref = self.frame_registry.get_shm_ref(curr_idx)
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
                        self.frame_registry.register_frame(curr_idx, frame)
                        ref = self.frame_registry.get_shm_ref(curr_idx)
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
                fallback_map = self._batch_read_frames_to_shm(video_path, missing_units)
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
        screenshot_tasks: List[dict]
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
        
        shm_map = {}
        cap = cv2.VideoCapture(video_path)
        
        try:
            fps = cap.get(cv2.CAP_PROP_FPS)
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            for task in screenshot_tasks:
                key = f"{task['unit_id']}_island{task['island_index']}"
                start_sec = task['expanded_start']
                end_sec = task['expanded_end']
                
                # 采样帧（每 0.5s 一帧，确保覆盖扩展范围）
                timestamps = np.arange(start_sec, end_sec + 0.1, 0.5)
                frame_map = {}
                
                for ts in timestamps:
                    frame_idx = int(ts * fps)
                    frame_idx = max(0, min(frame_idx, total_frames - 1))
                    
                    # Seek and read
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                    ret, frame = cap.read()
                    
                    if not ret or frame is None:
                        continue
                    
                    # 写入 SharedMemory（使用 FrameRegistry）
                    self.frame_registry.register_frame(frame_idx, frame)
                    shm_ref = self.frame_registry.get_shm_ref(frame_idx)
                    
                    if shm_ref:
                        frame_map[ts] = shm_ref
                    
                    # 立即释放本地内存
                    del frame
                
                if frame_map:
                    shm_map[key] = frame_map
            
            logger.info(f"✅ Batch read {len(shm_map)} screenshot tasks, total frames in SharedMemory")
            return shm_map
            
        except Exception as e:
            logger.error(f"❌ Batch read for screenshots failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}
        finally:
            cap.release()
    

    async def ValidateCVBatch(self, request, context):
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
        from cv_worker import run_cv_validation_task
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

            def chunk_list(items, size):
                return [items[i:i + size] for i in range(0, len(items), size)]

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
            # tail-merge：尾部太小则合并到前一批
            if len(task_chunks) >= 2:
                tail_size = len(task_chunks[-1])
                merge_threshold = max(1, BATCH_SIZE // 2)
                if tail_size < merge_threshold:
                    task_chunks[-2].extend(task_chunks[-1])
                    task_chunks.pop()
                    logger.info(
                        f"[{task_id}] Tail merge: last_size={tail_size} -> merged_size={len(task_chunks[-1])}"
                    )
            total_chunks = len(task_chunks)
            max_inflight = max(1, self.cv_worker_count * 2)

            logger.info(
                f"[{task_id}] Streaming gate pipeline: chunks={total_chunks}, batch={BATCH_SIZE}, "
                f"inflight={max_inflight}"
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
                - (shm_map, coarse_shm_map, io_ms, cv_count, cf_count)。
                """
                io_start = time.perf_counter()
                if not task_chunk:
                    return {}, {}, 0.0, 0, 0
                cv_chunk = [t["unit"] for t in task_chunk if t["type"] == "cv"]
                cf_chunk = [t["unit"] for t in task_chunk if t["type"] == "cf"]

                io_futures = []
                if cv_chunk:
                    cv_io_future = loop.run_in_executor(
                        None,
                        self._batch_read_frames_to_shm,
                        video_path,
                        cv_chunk
                    )
                    io_futures.append(("cv", cv_io_future))

                if cf_chunk:
                    cf_io_future = loop.run_in_executor(
                        None,
                        self._batch_read_coarse_frames_to_shm,
                        video_path,
                        cf_chunk,
                        COARSE_FPS
                    )
                    io_futures.append(("cf", cf_io_future))

                io_results = {}
                for io_type, io_future in io_futures:
                    io_results[io_type] = await io_future

                shm_map = io_results.get("cv", {})
                coarse_shm_map = io_results.get("cf", {})

                io_ms = (time.perf_counter() - io_start) * 1000.0
                return shm_map, coarse_shm_map, io_ms, len(cv_chunk), len(cf_chunk)

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
                prefetch_task = asyncio.create_task(read_chunk(task_chunks[0]))
                pending = set()

                for idx, task_chunk in enumerate(task_chunks):
                    if idx + 1 < total_chunks:
                        next_task = asyncio.create_task(read_chunk(task_chunks[idx + 1]))
                    else:
                        next_task = None

                    shm_map, coarse_shm_map, io_ms, io_cv_cnt, io_cf_cnt = await prefetch_task
                    prefetch_task = next_task
                    logger.info(
                        f"[{task_id}] Chunk {idx + 1}/{total_chunks} IO done: {io_ms:.1f}ms "
                        f"(cv_units={io_cv_cnt}, cf_units={io_cf_cnt})"
                    )
                    total_io_ms += io_ms
                    io_chunks += 1

                    if not task_chunk:
                        continue

                    # 提交任务（持续喂入）
                    from cv_worker import run_coarse_fine_screenshot_task
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
                                    fine_shm_frames_by_island=None
                                )
                            )
                            pending.add(asyncio.create_task(wrap_task(future, "cf", unit_id)))
                        submitted += 1
                    total_tasks += submitted
                    logger.info(
                        f"[{task_id}] Feed chunk {idx + 1}/{total_chunks}: submitted={submitted}, inflight={len(pending)}"
                    )

                    # inflight 控制：避免任务堆积导致内存爆
                    while len(pending) >= max_inflight:
                        logger.info(
                            f"[{task_id}] Inflight throttle: pending={len(pending)}, limit={max_inflight}"
                        )
                        pending, completed, responses = await drain_completed(pending)
                        for resp in responses:
                            yield resp
                        completed_tasks += completed

                    # Memory Guard (chunk-level)
                    if 'shm_map' in locals():
                        del shm_map
                    if 'coarse_shm_map' in locals():
                        del coarse_shm_map
                    gc.collect()

                # drain remaining tasks
                while pending:
                    pending, completed, responses = await drain_completed(pending)
                    for resp in responses:
                        yield resp
                    completed_tasks += completed
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
    

    async def AnalyzeWithVL(self, request, context):
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
        task_id = request.task_id
        video_path = request.video_path
        semantic_units_path = request.semantic_units_json_path
        output_dir = request.output_dir

        def _persist_task_token_report(payload: dict) -> str:
            """
            按任务落盘 VL token 报表。

            为什么：
            1) 任务级可观测性需要可追溯文件，而不仅是日志；
            2) 便于后续做离线聚合分析（成本、节省率、裁剪效果）。
            """
            try:
                import json as _json

                base_dir = output_dir or (os.path.dirname(video_path) if video_path else os.getcwd())
                report_dir = os.path.join(base_dir, "intermediates")
                os.makedirs(report_dir, exist_ok=True)

                report_name = f"vl_token_report_{task_id}.json" if task_id else "vl_token_report_unknown.json"
                report_path = os.path.join(report_dir, report_name)
                latest_path = os.path.join(report_dir, "vl_token_report_latest.json")

                report_payload = {
                    "version": "1.0",
                    "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "task_id": task_id,
                    "video_path": video_path,
                    "semantic_units_path": semantic_units_path,
                    "output_dir": output_dir,
                }
                report_payload.update(payload or {})

                with open(report_path, "w", encoding="utf-8") as report_file:
                    _json.dump(report_payload, report_file, ensure_ascii=False, indent=2)

                with open(latest_path, "w", encoding="utf-8") as latest_file:
                    _json.dump(report_payload, latest_file, ensure_ascii=False, indent=2)

                logger.info(f"[{task_id}] VL token报表已落盘: {report_path}")
                return report_path
            except Exception as report_error:
                logger.warning(f"[{task_id}] VL token报表落盘失败: {report_error}")
                return ""
        
        logger.info(f"[{task_id}] AnalyzeWithVL 开始: video={video_path}, units_json={semantic_units_path}")
        
        try:
            self._increment_tasks()
            
            # 加载 VL 配置
            from MVP_Module2_HEANCING.module2_content_enhancement.config_loader import load_module2_config
            vl_config = load_module2_config().get("vl_material_generation", {})
            vl_enabled = vl_config.get("enabled", False)
            
            if not vl_enabled:
                logger.info(f"[{task_id}] VL 模块未启用，返回 vl_enabled=False")
                _persist_task_token_report({
                    "status": "vl_disabled",
                    "vl_enabled": False,
                    "used_fallback": False,
                    "routing_stats": {},
                    "token_stats": {},
                })
                return video_processing_pb2.VLAnalysisResponse(
                    success=True,
                    vl_enabled=False,
                    used_fallback=False,
                    error_msg=""
                )
            
            # 加载语义单元
            import json
            with open(semantic_units_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            if isinstance(data, dict) and "semantic_units" in data:
                semantic_units = data["semantic_units"]
            elif isinstance(data, list):
                semantic_units = data
            else:
                semantic_units = []
            
            if not semantic_units:
                logger.warning(f"[{task_id}] 无语义单元，跳过 VL 分析")
                _persist_task_token_report({
                    "status": "no_semantic_units",
                    "vl_enabled": True,
                    "used_fallback": False,
                    "routing_stats": {"total": 0},
                    "token_stats": {},
                })
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
            from MVP_Module2_HEANCING.module2_content_enhancement.resource_manager import get_io_executor
            from MVP_Module2_HEANCING.module2_content_enhancement.vl_material_generator import VLMaterialGenerator

            def _safe_float(value, default=0.0):
                try:
                    return float(value)
                except Exception:
                    return default

            def _normalize_knowledge_type(raw_value):
                kt = (str(raw_value).strip() if raw_value is not None else "").lower()
                if kt not in {"abstract", "concrete", "process"}:
                    return "process"
                return kt

            def _select_screenshots_sync(unit_id, start_sec, end_sec):
                """
                说明：在 IO 线程池中执行的同步截图选择。
                取舍：每次调用创建轻量级 selector，避免多线程共享状态引发不稳定。
                """
                try:
                    selector = ScreenshotSelector.create_lightweight()
                    results = selector.select_screenshots_for_range_sync(
                        video_path=video_path,
                        start_sec=start_sec,
                        end_sec=end_sec
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
                "process_short": 0,
                "process_long": 0,
                "process_preprocessed": 0,
                "unknown": 0
            }
            vl_units = []
            cv_screenshot_units = []
            cv_clip_units = []
            routing_cfg = vl_config.get("routing", {}) if isinstance(vl_config.get("routing", {}), dict) else {}
            duration_threshold_sec = max(
                0.0,
                _safe_float(routing_cfg.get("process_duration_threshold_sec", 20.0), 20.0)
            )
            force_process_preprocess = bool(routing_cfg.get("process_force_preprocess_before_routing", True))

            process_units = []
            for unit in semantic_units:
                raw_kt = unit.get("knowledge_type", "")
                if _normalize_knowledge_type(raw_kt) == "process":
                    process_units.append(unit)

            process_route_map = {}
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
                if kt == "process" and not (str(raw_kt).strip().lower() in {"process"}):
                    routing_stats["unknown"] += 1
                start_sec = _safe_float(unit.get("start_sec", 0.0))
                end_sec = _safe_float(unit.get("end_sec", 0.0))
                duration = max(0.0, end_sec - start_sec)

                if kt == "abstract":
                    routing_stats["abstract"] += 1
                    continue
                if kt == "concrete":
                    routing_stats["concrete"] += 1
                    cv_screenshot_units.append(unit)
                    continue

                # process: 按时长分流
                route_info = process_route_map.get(str(unit.get("unit_id", "") or ""), {})
                effective_duration = _safe_float(route_info.get("effective_duration_sec", duration), duration)
                if bool(route_info.get("preprocess_applied", False)):
                    routing_stats["process_preprocessed"] += 1
                unit["_routing_pre_prune"] = route_info.get("pre_prune_info", {})
                unit["_routing_effective_duration_sec"] = effective_duration

                if effective_duration <= duration_threshold_sec:
                    routing_stats["process_short"] += 1
                    cv_screenshot_units.append(unit)
                    if unit.get("mult_steps", False):
                        cv_clip_units.append(unit)
                else:
                    routing_stats["process_long"] += 1
                    vl_units.append(unit)

            logger.info(
                f"[{task_id}] VL 路由统计: total={routing_stats['total']}, "
                f"abstract={routing_stats['abstract']}, concrete={routing_stats['concrete']}, "
                f"process_short={routing_stats['process_short']}, process_long={routing_stats['process_long']}, "
                f"process_preprocessed={routing_stats['process_preprocessed']}, "
                f"threshold={duration_threshold_sec:.1f}s, "
                f"unknown={routing_stats['unknown']}"
            )

            # ==================================================================
            # 预启动 VL 任务（与路由侧截图并行，形成 IO/Compute 重叠）
            # ==================================================================
            vl_task = None
            vl_t0 = None
            vl_token_stats = {}
            if vl_units:
                vl_t0 = time.perf_counter()
                generator = VLMaterialGenerator(vl_config, cv_executor=self.cv_process_pool)
                vl_task = asyncio.create_task(generator.generate(video_path, vl_units, output_dir))

            # ==================================================================
            # 路由侧：截图选择（concrete + process<=20s）
            # ==================================================================
            vl_screenshot_requests = []
            vl_clip_requests = []
            cv_screenshot_requests = []
            if cv_screenshot_units:
                route_t0 = time.perf_counter()
                loop = asyncio.get_event_loop()
                executor = get_io_executor()
                cpu_count = os.cpu_count() or 4
                max_concurrency = max(1, min(4, cpu_count // 2))
                semaphore = asyncio.Semaphore(max_concurrency)
                batch_size = max(1, max_concurrency * 4)
                total_units = len(cv_screenshot_units)
                total_batches = (total_units + batch_size - 1) // batch_size

                def _consume_vl_result(vl_result):
                    if not vl_result.success:
                        return False, vl_result.error_msg
                    nonlocal vl_token_stats
                    vl_token_stats = getattr(vl_result, "token_stats", {}) or {}
                    vl_unit_ids = {u.get("unit_id", "") for u in vl_units}
                    for ss in vl_result.screenshot_requests:
                        if ss.get("semantic_unit_id", "") in vl_unit_ids:
                            vl_screenshot_requests.append(ss)
                    for clip in vl_result.clip_requests:
                        if clip.get("semantic_unit_id", "") in vl_unit_ids:
                            vl_clip_requests.append(clip)
                    return True, ""

                async def _run_cv_screenshot(unit):
                    unit_id = unit.get("unit_id", "")
                    start_sec = _safe_float(unit.get("start_sec", 0.0))
                    end_sec = _safe_float(unit.get("end_sec", 0.0))
                    async with semaphore:
                        results = await loop.run_in_executor(
                            executor,
                            functools.partial(_select_screenshots_sync, unit_id, start_sec, end_sec)
                        )
                    return unit_id, start_sec, end_sec, results

                vl_consumed = False
                for batch_idx in range(total_batches):
                    start = batch_idx * batch_size
                    end = min(start + batch_size, total_units)
                    batch_units = cv_screenshot_units[start:end]

                    if vl_task and vl_task.done() and not vl_consumed:
                        vl_result = vl_task.result()
                        ok, err = _consume_vl_result(vl_result)
                        if not ok:
                            logger.warning(f"[{task_id}] VL 分析失败，提前回退: {err}")
                            _persist_task_token_report({
                                "status": "fallback",
                                "vl_enabled": True,
                                "used_fallback": True,
                                "error_msg": err,
                                "routing_stats": routing_stats,
                                "token_stats": vl_token_stats,
                            })
                            return video_processing_pb2.VLAnalysisResponse(
                                success=True,
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
                                "screenshot_id": f"routed_ss_{unit_id}_{idx}",
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
                    f"concurrency={max_concurrency}, batch_size={batch_size}"
                )

            # ==================================================================
            # 路由侧：短过程 clip（process<=10s）
            # ==================================================================
            cv_clip_requests = []
            for unit in cv_clip_units:
                unit_id = unit.get("unit_id", "")
                start_sec = _safe_float(unit.get("start_sec", 0.0))
                end_sec = _safe_float(unit.get("end_sec", 0.0))
                cv_clip_requests.append({
                    "clip_id": f"routed_clip_{unit_id}",
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
                        logger.warning(f"[{task_id}] VL 鍒嗘瀽澶辫触锛岄渶瑕佸洖閫€: {vl_result.error_msg}")
                        _persist_task_token_report({
                            "status": "fallback",
                            "vl_enabled": True,
                            "used_fallback": True,
                            "error_msg": vl_result.error_msg,
                            "routing_stats": routing_stats,
                            "token_stats": vl_token_stats,
                        })
                        return video_processing_pb2.VLAnalysisResponse(
                            success=True,
                            vl_enabled=True,
                            used_fallback=True,
                            error_msg=vl_result.error_msg
                        )
                    vl_unit_ids = {u.get("unit_id", "") for u in vl_units}
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
                seen = set()
                deduped = []
                for item in items:
                    key = (
                        item.get("semantic_unit_id", ""),
                        float(item.get("timestamp_sec", 0.0)),
                        item.get("label", "")
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped.append(item)
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
                    semantic_unit_id=ss.get("semantic_unit_id", "")
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
            try:
                if vl_clip_requests:
                    has_updates = False
                    units_map = {u.get("unit_id"): u for u in semantic_units}
                    
                    # Group steps by unit
                    unit_steps = {} # uid -> list of step dicts
                    for clip in vl_clip_requests:
                        if clip.get("analysis_mode") == "tutorial_stepwise":
                            uid = clip.get("semantic_unit_id")
                            if not uid: continue
                            
                            if uid not in unit_steps:
                                unit_steps[uid] = []
                            
                            # Find matching screenshots
                            step_ss_ids = []
                            step_id = clip.get("step_id")
                            for ss in vl_screenshot_requests:
                                if ss.get("semantic_unit_id") == uid and ss.get("step_id") == step_id:
                                    step_ss_ids.append(ss.get("screenshot_id"))
                            
                            unit_steps[uid].append({
                                "step_id": step_id,
                                "description": clip.get("step_description", ""),
                                "timestamp_range": [clip.get("start_sec"), clip.get("end_sec")],
                                "materials": {
                                    "clip_id": clip.get("clip_id"),
                                    "screenshot_ids": step_ss_ids
                                }
                            })
                    
                    for uid, steps in unit_steps.items():
                        if uid in units_map:
                            # Sort by step_id
                            try:
                                steps.sort(key=lambda x: int(x["step_id"]))
                            except:
                                pass
                            units_map[uid]["instructional_steps"] = steps
                            has_updates = True
                    
                    if has_updates:
                        with open(semantic_units_path, "w", encoding="utf-8") as f:
                            # Re-use 'data' structure loaded earlier
                            if isinstance(data, dict):
                                # Ensure we don't lose other top-level keys
                                data["semantic_units"] = semantic_units 
                                json.dump(data, f, ensure_ascii=False, indent=2)
                            else:
                                json.dump(semantic_units, f, ensure_ascii=False, indent=2)
                        logger.info(f"[{task_id}] ✅ Persisted instructional_steps to {semantic_units_path}")

            except Exception as e:
                logger.error(f"[{task_id}] Failed to persist instructional_steps: {e}")

            _persist_task_token_report({
                "status": "success",
                "vl_enabled": True,
                "used_fallback": False,
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
            logger.error(f"[{task_id}] AnalyzeWithVL 异常: {e}", exc_info=True)
            _persist_task_token_report({
                "status": "exception",
                "vl_enabled": True,
                "used_fallback": True,
                "error_msg": str(e),
                "routing_stats": {},
                "token_stats": {},
            })
            return video_processing_pb2.VLAnalysisResponse(
                success=False,
                vl_enabled=True,
                used_fallback=True,
                error_msg=str(e)
            )
        finally:
            self._decrement_tasks()

    async def ReleaseCVResources(self, request, context):
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
    # 初始化 gRPC 服务器与 Servicer
    server = aio.server()
    logger.info("初始化 VideoProcessingServicer（首次 warmup 可能较慢）...")
    init_t0 = time.perf_counter()
    servicer = VideoProcessingServicer()
    logger.info(f"VideoProcessingServicer initialized in {time.perf_counter() - init_t0:.2f}s")
    video_processing_pb2_grpc.add_VideoProcessingServiceServicer_to_server(servicer, server)

    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    
    logger.info(f"Starting gRPC server on {listen_addr}")
    
    await server.start()
    
    # Graceful shutdown handler
    async def shutdown():
        """
        执行逻辑：
        1) 停止 gRPC 服务器并等待完成。
        2) 如存在进程池则执行关闭。
        实现方式：server.stop + 条件判断。
        核心价值：避免资源泄漏，保证优雅退出。
        决策逻辑：
        - 条件：hasattr(servicer, 'process_pool')
        依据来源（证据链）：
        - Servicer 属性：process_pool 是否存在。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如释放资源）。"""
        logger.info("Stopping server...")
        await server.stop(5)
        if hasattr(servicer, 'process_pool'):
            servicer.process_pool.shutdown()
            logger.info("Process pool shut down")
            
    try:
        await server.wait_for_termination()
    finally:
        await shutdown()


if __name__ == "__main__":
    # Windows下多进程必须在 if __name__ == "__main__" 保护下
    # 并通过 freeze_support 支持打包 (虽然这里不需要打包)
    from multiprocessing import freeze_support
    freeze_support()
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    asyncio.run(serve())
