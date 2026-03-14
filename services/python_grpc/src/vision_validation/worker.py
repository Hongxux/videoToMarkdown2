"""CV Worker 多进程执行模块。

负责在独立进程中执行 CV 相关重计算任务，降低主流程阻塞风险。
"""

import os
# Encoding fixed: CPU note.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
os.environ.setdefault("OPENCV_OPENCL_RUNTIME", "disabled")
import logging
import gc
import time
import re
import threading
import psutil
from typing import Dict, List, Tuple, Any, Optional
from multiprocessing import shared_memory
import numpy as np
import cv2
from services.python_grpc.src.common.utils.opencv_decode import (
    ensure_opencv_readable_video_path,
    open_video_capture_with_fallback,
)

# Worker 进程内的全局缓存
_validator_cache: Dict[str, Any] = {}
_initialized = False
_thread_local = threading.local()
_attached_shms: Dict[str, shared_memory.SharedMemory] = {}  # 宸查檮鍔犵殑鍏变韩鍐呭瓨

logger = logging.getLogger(__name__)

_TEXT_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9\u4e00-\u9fff]+")
_ASCII_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")
_CJK_RUN_PATTERN = re.compile(r"[\u4e00-\u9fff]+")
_CJK_PHRASE_CONNECTOR_PATTERN = re.compile(r"[的之]+")
_CJK_NGRAM_MIN_LEN = 2
_CJK_NGRAM_MAX_LEN = 4
_CJK_NGRAM_CHAR_CAP = 24


def _get_env_float(name: str, default: float, lower: float, upper: float) -> float:
    """方法说明：`_get_env_float` 工具方法。
    执行步骤：
    1) 步骤1：读取环境变量并尝试解析为浮点数。
    2) 步骤2：解析失败时使用默认值。
    3) 步骤3：按上下界裁剪后返回结果。"""
    try:
        value = float(os.getenv(name, str(default)))
    except Exception:
        value = float(default)
    return max(lower, min(upper, value))


def _resolve_worker_readable_video_path(
    video_path: str,
    *,
    decode_open_timeout_sec: int = 30,
    decode_allow_inline_transcode: bool = False,
    decode_enable_async_transcode: bool = True,
) -> str:
    """方法说明：`_resolve_worker_readable_video_path` 工具方法。
    执行步骤：
    1) 步骤1：读取 worker 进程内缓存，命中则直接返回。
    2) 步骤2：未命中时执行 OpenCV 可解码路径解析（必要时转码兜底）。
    3) 步骤3：写回缓存，减少同视频重复探测开销。"""
    global _validator_cache
    if not video_path:
        return video_path
    cache_key = f"decode_path::{video_path}"
    cached = _validator_cache.get(cache_key)
    if isinstance(cached, str) and cached:
        return cached

    resolved_path, _ = ensure_opencv_readable_video_path(
        video_path,
        timeout_sec=max(5, int(decode_open_timeout_sec)),
        logger=logger,
        allow_inline_transcode=bool(decode_allow_inline_transcode),
        enable_async_transcode=bool(decode_enable_async_transcode),
    )
    _validator_cache[cache_key] = resolved_path
    return resolved_path


def _is_truthy_env(name: str, default: str = "0") -> bool:
    """方法说明：`_is_truthy_env` 工具方法。
    执行步骤：
    1) 步骤1：读取指定环境变量并应用默认值兜底。
    2) 步骤2：对值做去空白与小写归一化。
    3) 步骤3：按真值集合判断并返回布尔结果。"""
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def init_cv_worker():
    """??? CV Worker ??????????"""
    global _initialized
    
    if _initialized:
        return

    # 配置子进程日志（尽早配置，避免初始化阶段日志丢失/格式不一致）
    env_level = os.getenv("CV_WORKER_LOG_LEVEL", "").strip().upper()
    level = getattr(logging, env_level, logging.INFO) if env_level else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - CV_WORKER[%(process)d] - %(levelname)s - %(message)s",
    )
    
    # 禁用嵌套并行，避免单进程内部抢占多核
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
    os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
    try:
        import cv2
        cv2.setNumThreads(1)
        try:
            cv2.ocl.setUseOpenCL(False)
        except Exception:
            pass

        if _is_truthy_env("CV_DISABLE_OPENCV_OPT", "0"):
            try:
                cv2.setUseOptimized(False)
            except Exception:
                pass
        logger.info("鉁?Nested parallelism disabled: cv2 threads=1")
    except Exception as e:
        logger.warning(f"鈿狅笍 Failed to set cv2 threads=1: {e}")
    
    logger.info(f"馃殌 CV Worker initialized with SharedMemory support (PID={os.getpid()})")
    _initialized = True
    
    
    # 🚀 Memory Limit (Dynamic Adjustment)
    global _soft_limit_bytes, _hard_limit_bytes
    
    # Get total system memory
    mem = psutil.virtual_memory()
    total_mem = mem.total
    
    # Strategy: Allow each worker up to 15% of total RAM
    _soft_limit_bytes = int(total_mem * 0.15)
    _hard_limit_bytes = int(total_mem * 0.20)
    
    try:
        import resource
        resource.setrlimit(resource.RLIMIT_AS, (_soft_limit_bytes, _hard_limit_bytes))
        logger.info(f"鉁?Memory limit set (RLIMIT): Soft={_soft_limit_bytes/1024**3:.1f}GB")
    except ImportError:
        logger.info(f"鈿狅笍 'resource' check skipped (Windows). Manual limit active: {_soft_limit_bytes/1024**3:.1f}GB")

def _check_memory_usage():
    """?? Worker ????????????? GC?"""
    try:
        process = psutil.Process(os.getpid())
        rss = process.memory_info().rss
        if rss > _soft_limit_bytes:
            import gc
            gc.collect()
            logger.warning(f"鈿狅笍 Worker Memory High ({rss/1024**3:.1f}GB). GC Triggered.")
    except Exception:
        pass


def _get_attached_shm_store() -> Dict[str, shared_memory.SharedMemory]:
    if threading.current_thread() is threading.main_thread():
        return _attached_shms
    store = getattr(_thread_local, "attached_shms", None)
    if not isinstance(store, dict):
        store = {}
        _thread_local.attached_shms = store
    return store


def _count_attached_shm_refs() -> int:
    """返回当前线程已附着的 SHM 句柄数量，便于任务级监控。"""
    return len(_get_attached_shm_store())


def _get_thread_local_screenshot_selector(selector_key: str):
    selector_cache = getattr(_thread_local, "screenshot_selector_cache", None)
    if not isinstance(selector_cache, dict):
        selector_cache = {}
        _thread_local.screenshot_selector_cache = selector_cache
    selector = selector_cache.get(selector_key)
    if selector is None:
        from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector
        selector = ScreenshotSelector.create_lightweight()
        selector_cache[selector_key] = selector
    return selector


def get_frame_from_shm(shm_ref: dict, copy: bool = False) -> Optional[np.ndarray]:
    """方法说明：`get_frame_from_shm` 核心方法。
    执行步骤：
    1) 步骤1：从共享内存描述中解析名称、形状与数据类型。
    2) 步骤2：附加或复用共享内存对象并构建 `numpy` 视图。
    3) 步骤3：按需返回拷贝或只读视图，异常时返回 `None`。"""
    attached_shms = _get_attached_shm_store()
    
    try:
        shm_name = shm_ref.get("shm_name")
        shape = shm_ref.get("shape")
        dtype = shm_ref.get("dtype")
        
        if not all([shm_name, shape, dtype]):
            return None
        
        # 复用已附加的共享内存
        if shm_name not in attached_shms:
            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                attached_shms[shm_name] = shm
            except FileNotFoundError:
                logger.warning(f"SharedMemory not found: {shm_name}")
                return None
        
        shm = attached_shms[shm_name]
        frame = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
        return frame.copy() if copy else frame
        
    except Exception as e:
        logger.warning(f"Failed to get frame from SharedMemory: {e}")
        return None


def release_attached_shm_refs(shm_names: Optional[List[str]] = None) -> None:
    """方法说明：`release_attached_shm_refs` 工具方法。
    执行步骤：
    1) 步骤1：确定需要释放的共享内存名称集合。
    2) 步骤2：关闭 worker 进程中已附加但不再需要的共享内存句柄。
    3) 步骤3：从本地缓存移除句柄，避免长期任务造成句柄累积。"""
    attached_shms = _get_attached_shm_store()

    if shm_names is None:
        target_names = list(attached_shms.keys())
    else:
        target_names = [name for name in shm_names if name]

    before_count = len(attached_shms)

    for shm_name in target_names:
        shm = attached_shms.pop(shm_name, None)
        if shm is None:
            continue
        try:
            # 仅 close 不 unlink：共享内存所有权在主进程 registry。
            shm.close()
        except Exception:
            pass

    if target_names:
        logger.info(
            "[SHM Worker] release requested=%s attached_before=%s attached_after=%s",
            len(target_names),
            before_count,
            len(attached_shms),
        )

def _normalize_roi(
    roi: Optional[Tuple[int, int, int, int]],
    frame_shape: Tuple[int, ...],
) -> Tuple[int, int, int, int]:
    """方法说明：`_normalize_roi` 工具方法。
    执行步骤：
    1) 步骤1：将 ROI 坐标约束到画面尺寸边界内。
    2) 步骤2：处理非法 ROI（x2<=x1 或 y2<=y1）并回退默认区域。
    3) 步骤3：返回稳定可用的标准化 ROI。"""
    h = int(frame_shape[0]) if len(frame_shape) > 0 else 0
    w = int(frame_shape[1]) if len(frame_shape) > 1 else 0
    if h <= 0 or w <= 0:
        return (0, 0, 1, 1)

    if roi is None:
        margin_x = int(w * 0.1)
        margin_y = int(h * 0.1)
        return (margin_x, margin_y, max(margin_x + 1, w - margin_x), max(margin_y + 1, h - margin_y))

    x1, y1, x2, y2 = [int(v) for v in roi]
    x1 = max(0, min(x1, w))
    x2 = max(0, min(x2, w))
    y1 = max(0, min(y1, h))
    y2 = max(0, min(y2, h))
    if x2 <= x1 or y2 <= y1:
        margin_x = int(w * 0.1)
        margin_y = int(h * 0.1)
        return (margin_x, margin_y, max(margin_x + 1, w - margin_x), max(margin_y + 1, h - margin_y))
    return (x1, y1, x2, y2)


def _crop_frame_by_roi(frame: np.ndarray, roi: Optional[Tuple[int, int, int, int]]) -> np.ndarray:
    """方法说明：`_crop_frame_by_roi` 工具方法。
    执行步骤：
    1) 步骤1：对 ROI 做边界标准化。
    2) 步骤2：执行图像裁剪。
    3) 步骤3：裁剪失败时回退原图，保证流程鲁棒。"""
    if frame is None:
        return frame
    norm = _normalize_roi(roi, frame.shape)
    x1, y1, x2, y2 = norm
    try:
        cropped = frame[y1:y2, x1:x2]
        return cropped if cropped is not None and cropped.size > 0 else frame
    except Exception:
        return frame



def _resize_frame_and_roi_for_analysis(
    frame: np.ndarray,
    roi: Optional[Tuple[int, int, int, int]],
    max_width: int,
) -> Tuple[np.ndarray, Optional[Tuple[int, int, int, int]]]:
    """方法说明：`_resize_frame_and_roi_for_analysis` 工具方法。
    执行步骤：
    1) 步骤1：根据分析宽度上限判断是否需要缩放整帧。
    2) 步骤2：若发生缩放，则同步按比例缩放 ROI，避免 OCR/形状分析坐标失真。
    3) 步骤3：返回可直接送入分析链路的帧与 ROI。"""
    if frame is None:
        return frame, roi

    target_width = max(0, int(max_width or 0))
    frame_h = int(frame.shape[0]) if len(frame.shape) > 0 else 0
    frame_w = int(frame.shape[1]) if len(frame.shape) > 1 else 0
    if target_width <= 0 or frame_h <= 0 or frame_w <= 0 or frame_w <= target_width:
        if roi is None:
            return frame, None
        return frame, _normalize_roi(roi, frame.shape)

    scale = float(target_width) / float(frame_w)
    target_height = max(1, int(round(frame_h * scale)))
    resized = cv2.resize(frame, (target_width, target_height), interpolation=cv2.INTER_AREA)
    if roi is None:
        return resized, None

    x1, y1, x2, y2 = _normalize_roi(roi, frame.shape)
    scaled_roi = (
        int(round(x1 * scale)),
        int(round(y1 * scale)),
        int(round(x2 * scale)),
        int(round(y2 * scale)),
    )
    return resized, _normalize_roi(scaled_roi, resized.shape)



def _empty_shape_signature() -> Dict[str, float]:
    """方法说明：`_empty_shape_signature` 工具方法。
    执行步骤：
    1) 步骤1：为无法取得分析帧的场景提供统一兜底结构。
    2) 步骤2：避免多处手写默认字典导致结构漂移。
    3) 步骤3：让增量截图过滤逻辑始终收到稳定字段。"""
    return {"rect_count": 0, "component_count": 0, "edge_density": 0.0}



def _analyze_frame_for_incremental_screenshot(
    frame: np.ndarray,
    roi: Optional[Tuple[int, int, int, int]],
    analysis_max_width: int,
) -> Tuple[set, Dict[str, float]]:
    """方法说明：`_analyze_frame_for_incremental_screenshot` 工具方法。
    执行步骤：
    1) 步骤1：先将候选帧压到统一分析分辨率，控制 OCR/形状分析成本。
    2) 步骤2：复用现有 `_extract_ocr_tokens` 与 `_extract_shape_signature`，不重复造轮子。
    3) 步骤3：若帧为空，则返回稳定的空分析结果。"""
    if frame is None:
        return set(), _empty_shape_signature()

    analysis_frame, analysis_roi = _resize_frame_and_roi_for_analysis(frame, roi, analysis_max_width)
    return _extract_ocr_tokens(analysis_frame, analysis_roi), _extract_shape_signature(analysis_frame, analysis_roi)



def _find_nearest_frame(
    frames: List[np.ndarray],
    timestamps: List[float],
    target_sec: float,
) -> Optional[np.ndarray]:
    """方法说明：`_find_nearest_frame` 工具方法。
    执行步骤：
    1) 步骤1：在已有共享帧时间轴里寻找最接近目标时间戳的帧。
    2) 步骤2：优先复用已降采样帧，减少 worker 再次打开视频的概率。
    3) 步骤3：找不到可用帧时返回 `None`，交由上层决定是否回退到视频解码。"""
    if not frames or not timestamps or len(frames) != len(timestamps):
        return None
    try:
        best_idx = min(range(len(timestamps)), key=lambda idx: abs(float(timestamps[idx]) - float(target_sec)))
    except Exception:
        return None
    if best_idx < 0 or best_idx >= len(frames):
        return None
    return frames[best_idx]



def _strip_private_screenshot_fields(screenshots: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """方法说明：`_strip_private_screenshot_fields` 工具方法。
    执行步骤：
    1) 步骤1：移除 worker 内部临时字段，避免泄漏实现细节到返回结果。
    2) 步骤2：仅清理以下划线开头的分析期字段，不影响业务字段。
    3) 步骤3：返回适合持久化与后续链路消费的截图结果。"""
    cleaned: List[Dict[str, Any]] = []
    for item in screenshots:
        if not isinstance(item, dict):
            continue
        cleaned.append({k: v for k, v in item.items() if not str(k).startswith('_')})
    return cleaned


def _get_route_roi(
    video_path: str,
    start_sec: float,
    end_sec: float,
    *,
    decode_open_timeout_sec: int = 30,
    decode_allow_inline_transcode: bool = False,
    decode_enable_async_transcode: bool = True,
) -> Optional[Tuple[int, int, int, int]]:
    """方法说明：`_get_route_roi` 核心方法。
    执行步骤：
    1) 步骤1：复用 `CVKnowledgeValidator._detect_roi`（已实现逻辑）在中点帧上检测 ROI。
    2) 步骤2：对 ROI 做字幕带抑制（底部裁切）。
    3) 步骤3：缓存 ROI，减少同视频重复检测开销。"""
    global _validator_cache

    roi_cache_key = f"route_roi::{video_path}::{round(float(start_sec), 2)}::{round(float(end_sec), 2)}"
    cached = _validator_cache.get(roi_cache_key)
    if isinstance(cached, tuple) and len(cached) == 4:
        return cached

    frame = None
    frame_shape = None
    validator = None
    try:
        validator_key = f"route_roi_validator::{video_path}"
        if validator_key not in _validator_cache:
            from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator

            _validator_cache[validator_key] = CVKnowledgeValidator(video_path, use_resource_manager=False)
        validator = _validator_cache[validator_key]

        mid_sec = max(0.0, (float(start_sec) + float(end_sec)) / 2.0)
        cap = getattr(validator, "cap", None)
        fps = float(getattr(validator, "fps", 0.0) or 0.0)
        if cap is not None and fps > 0:
            frame_idx = int(mid_sec * fps)
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
            ret, sampled = cap.read()
            if ret and sampled is not None:
                frame = sampled
    except Exception as e:
        logger.debug(f"Route ROI via validator failed: {e}")

    if frame is None:
        cap, effective_video_path, _ = open_video_capture_with_fallback(
            video_path,
            timeout_sec=max(5, int(decode_open_timeout_sec)),
            logger=logger,
            allow_inline_transcode=bool(decode_allow_inline_transcode),
            enable_async_transcode=bool(decode_enable_async_transcode),
        )
        if cap is not None:
            try:
                if cap.isOpened():
                    fps = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
                    frame_idx = int(max(0.0, (float(start_sec) + float(end_sec)) / 2.0) * fps)
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                    ret, sampled = cap.read()
                    if ret and sampled is not None:
                        frame = sampled
            finally:
                cap.release()
        else:
            logger.debug("Route ROI fallback capture open failed: source=%s, effective=%s", video_path, effective_video_path)

    if frame is None:
        return None

    frame_shape = frame.shape
    detected_roi = None
    if validator is not None:
        try:
            detected_roi = validator._detect_roi(frame, use_cache=True)
        except Exception as e:
            logger.debug(f"Route ROI detect failed, fallback default ROI: {e}")

    norm_roi = _normalize_roi(detected_roi, frame_shape)

    # 底部字幕带抑制：仅在保证 ROI 高度充足时执行底部裁切。
    trim_ratio = max(0.0, min(0.35, float(os.getenv("CV_ROUTE_ROI_BOTTOM_TRIM_RATIO", "0.12"))))
    x1, y1, x2, y2 = norm_roi
    h = frame_shape[0]
    trimmed_y2 = min(y2, int(h * (1.0 - trim_ratio)))
    min_roi_h = max(8, int(h * 0.08))
    if trimmed_y2 - y1 >= min_roi_h:
        norm_roi = (x1, y1, x2, trimmed_y2)

    _validator_cache[roi_cache_key] = norm_roi
    return norm_roi


def _get_ocr_extractor() -> Any:
    """方法说明：`_get_ocr_extractor` 工具方法。
    执行步骤：
    1) 步骤1：从 worker 级缓存读取 OCR 实例。
    2) 步骤2：不存在时懒加载创建。
    3) 步骤3：初始化失败返回 `None`，不阻断主流程。"""
    global _validator_cache

    key = "route_screenshot_ocr_extractor"
    if key in _validator_cache:
        return _validator_cache[key]

    try:
        from services.python_grpc.src.content_pipeline.infra.runtime.ocr_utils import OCRExtractor

        # 增量截图阶段优先按中英双语初始化，避免仅英文识别导致 token 漏检。
        ocr_lang = str(os.getenv("CV_ROUTE_OCR_LANG", "chi_sim+eng") or "chi_sim+eng").strip() or "chi_sim+eng"
        _validator_cache[key] = OCRExtractor(lang=ocr_lang)
    except Exception as e:
        logger.warning(f"Route screenshot OCR extractor init failed: {e}")
        _validator_cache[key] = None
    return _validator_cache[key]


def _get_region_ocr_engine() -> Tuple[str, Any]:
    """方法说明：`_get_region_ocr_engine` 工具方法。
    执行步骤：
    1) 步骤1：读取配置决定 OCR 引擎优先级（auto/rapidocr/paddle/tesseract）。
    2) 步骤2：懒加载并缓存区域 OCR 引擎实例。
    3) 步骤3：不可用时返回 `("none", None)` 并降级到后备路径。"""
    global _validator_cache

    key = "route_screenshot_region_ocr_engine"
    if key in _validator_cache:
        cached = _validator_cache[key]
        if isinstance(cached, tuple) and len(cached) == 2:
            return cached[0], cached[1]
        return "none", None

    mode = str(os.getenv("CV_ROUTE_OCR_ENGINE", "auto") or "auto").strip().lower()
    tried = []

    def _try_rapidocr() -> Optional[Tuple[str, Any]]:
        try:
            from rapidocr_onnxruntime import RapidOCR

            return ("rapidocr", RapidOCR())
        except Exception as exc:
            tried.append(f"rapidocr:{type(exc).__name__}")
            return None

    def _try_paddle() -> Optional[Tuple[str, Any]]:
        try:
            from paddleocr import PaddleOCR  # type: ignore

            kwargs = {"lang": "ch", "use_angle_cls": True, "show_log": False}
            try:
                engine = PaddleOCR(**kwargs)
            except TypeError:
                kwargs.pop("show_log", None)
                engine = PaddleOCR(**kwargs)
            return ("paddle", engine)
        except Exception as exc:
            tried.append(f"paddle:{type(exc).__name__}")
            return None

    selected: Optional[Tuple[str, Any]] = None
    if mode in {"auto", "rapidocr"}:
        selected = _try_rapidocr()
    if selected is None and mode in {"auto", "paddle"}:
        selected = _try_paddle()
    if selected is None and mode in {"auto", "tesseract"}:
        # tesseract 走 OCRExtractor 回退，不在区域引擎内初始化。
        selected = ("tesseract", None)

    if selected is None:
        _validator_cache[key] = ("none", None)
        logger.debug("Route region OCR engine unavailable, mode=%s, tried=%s", mode, ",".join(tried))
        return "none", None

    _validator_cache[key] = selected
    logger.info("Route region OCR engine selected: %s", selected[0])
    return selected[0], selected[1]


def _extract_ocr_regions_from_crop(crop: np.ndarray) -> List[Dict[str, Any]]:
    """方法说明：`_extract_ocr_regions_from_crop` 工具方法。
    执行步骤：
    1) 步骤1：优先使用区域 OCR 引擎抽取文本框及置信度。
    2) 步骤2：统一转换为 `x/y/w/h/text/confidence` 结构。
    3) 步骤3：失败时回退到 `OCRExtractor.extract_text_regions_from_frame`。"""
    if crop is None or getattr(crop, "size", 0) <= 0:
        return []

    conf_threshold = _get_env_float("CV_ROUTE_OCR_REGION_MIN_CONF", 0.35, 0.0, 1.0)
    engine_name, engine = _get_region_ocr_engine()
    regions: List[Dict[str, Any]] = []

    try:
        if engine_name == "rapidocr" and engine is not None:
            result, _ = engine(crop)
            for item in result or []:
                if not isinstance(item, (list, tuple)) or len(item) < 3:
                    continue
                bbox, text, conf = item[0], item[1], item[2]
                if not bbox:
                    continue
                try:
                    xs = [float(p[0]) for p in bbox]
                    ys = [float(p[1]) for p in bbox]
                    x1, y1, x2, y2 = min(xs), min(ys), max(xs), max(ys)
                except Exception:
                    continue
                text_norm = str(text or "").strip()
                confidence = float(conf or 0.0)
                if not text_norm or confidence < conf_threshold:
                    continue
                regions.append(
                    {
                        "text": text_norm,
                        "x": int(round(x1)),
                        "y": int(round(y1)),
                        "w": max(1, int(round(x2 - x1))),
                        "h": max(1, int(round(y2 - y1))),
                        "confidence": confidence,
                    }
                )
            return regions

        if engine_name == "paddle" and engine is not None:
            result = engine.ocr(crop, cls=True)
            for line in (result[0] if result else []):
                if not isinstance(line, (list, tuple)) or len(line) < 2:
                    continue
                bbox, payload = line[0], line[1]
                if not isinstance(payload, (list, tuple)) or len(payload) < 2:
                    continue
                text_norm = str(payload[0] or "").strip()
                confidence = float(payload[1] or 0.0)
                if not text_norm or confidence < conf_threshold:
                    continue
                try:
                    xs = [float(p[0]) for p in bbox]
                    ys = [float(p[1]) for p in bbox]
                    x1, y1, x2, y2 = min(xs), min(ys), max(xs), max(ys)
                except Exception:
                    continue
                regions.append(
                    {
                        "text": text_norm,
                        "x": int(round(x1)),
                        "y": int(round(y1)),
                        "w": max(1, int(round(x2 - x1))),
                        "h": max(1, int(round(y2 - y1))),
                        "confidence": confidence,
                    }
                )
            return regions
    except Exception as exc:
        logger.debug("Route region OCR extraction failed via %s: %s", engine_name, exc)

    extractor = _get_ocr_extractor()
    if extractor is None:
        return []
    try:
        fallback_regions = extractor.extract_text_regions_from_frame(crop)
        return fallback_regions if isinstance(fallback_regions, list) else []
    except Exception:
        return []


def _is_subtitle_like_region(region: Dict[str, Any], image_w: int, image_h: int) -> bool:
    """方法说明：`_is_subtitle_like_region` 工具方法。
    执行步骤：
    1) 步骤1：读取文本框几何信息并做合法性校验。
    2) 步骤2：按“靠近底部 + 横向较宽 + 行高较矮”规则判定字幕候选。
    3) 步骤3：返回是否应在增量 OCR token 中排除。"""
    if image_w <= 0 or image_h <= 0:
        return False

    x = int(region.get("x", 0) or 0)
    y = int(region.get("y", 0) or 0)
    w = int(region.get("w", 0) or 0)
    h = int(region.get("h", 0) or 0)
    if w <= 0 or h <= 0:
        return False

    bottom_band_ratio = _get_env_float("CV_ROUTE_OCR_SUBTITLE_BOTTOM_RATIO", 0.33, 0.05, 0.45)
    min_width_ratio = _get_env_float("CV_ROUTE_OCR_SUBTITLE_MIN_WIDTH_RATIO", 0.28, 0.1, 0.9)
    max_height_ratio = _get_env_float("CV_ROUTE_OCR_SUBTITLE_MAX_HEIGHT_RATIO", 0.14, 0.03, 0.4)

    lower_band_start = image_h * (1.0 - bottom_band_ratio)
    center_y = y + h * 0.5
    near_bottom = center_y >= lower_band_start
    horizontally_wide = (w / float(image_w)) >= min_width_ratio
    short_line = (h / float(image_h)) <= max_height_ratio

    return bool(near_bottom and horizontally_wide and short_line)


def _build_cjk_phrase_tokens(text: str) -> set:
    """方法说明：`_build_cjk_phrase_tokens` 工具方法。
    执行步骤：
    1) 步骤1：保留原始中文串，保证与旧行为兼容。
    2) 步骤2：按“的/之”结构拆出修饰词与中心词并组合短语。
    3) 步骤3：补充短 n-gram，降低“无空格中文串”漏匹配风险。"""
    phrase = str(text or "").strip()
    if not phrase:
        return set()

    tokens = {phrase}

    segments = [seg for seg in _CJK_PHRASE_CONNECTOR_PATTERN.split(phrase) if seg]
    if len(segments) > 1:
        for seg in segments:
            if len(seg) >= _CJK_NGRAM_MIN_LEN:
                tokens.add(seg)
        for idx in range(len(segments) - 1):
            left = segments[idx]
            right = segments[idx + 1]
            if left and right:
                tokens.add(f"{left}{right}")

    if len(phrase) >= _CJK_NGRAM_MIN_LEN and len(phrase) <= _CJK_NGRAM_CHAR_CAP:
        max_len = min(_CJK_NGRAM_MAX_LEN, len(phrase))
        for window in range(_CJK_NGRAM_MIN_LEN, max_len + 1):
            for start in range(0, len(phrase) - window + 1):
                tokens.add(phrase[start : start + window])

    return tokens


def _tokenize_text_for_incremental(text: str) -> set:
    """方法说明：`_tokenize_text_for_incremental` 工具方法。
    执行步骤：
    1) 步骤1：按兼容正则切出基础 token（英文/数字/中文串）。
    2) 步骤2：英文数字 token 直接保留，中文串走短语增强切分。
    3) 步骤3：返回小写去重集合，用于增量截图比较。"""
    normalized = str(text or "").strip().lower()
    if not normalized:
        return set()

    tokens = set()
    for raw_token in _TEXT_TOKEN_PATTERN.findall(normalized):
        token = str(raw_token or "").strip()
        if not token:
            continue
        tokens.add(token)

        for ascii_token in _ASCII_TOKEN_PATTERN.findall(token):
            if ascii_token:
                tokens.add(ascii_token)

        for cjk_run in _CJK_RUN_PATTERN.findall(token):
            tokens.update(_build_cjk_phrase_tokens(cjk_run))

    return tokens


def _extract_ocr_tokens(frame: np.ndarray, roi: Optional[Tuple[int, int, int, int]]) -> set:
    """方法说明：`_extract_ocr_tokens` 工具方法。
    执行步骤：
    1) 步骤1：在 ROI 内裁剪图像。
    2) 步骤2：调用 OCR 提取文本。
    3) 步骤3：将文本切分为 token 集合用于增量判定。"""
    extractor = _get_ocr_extractor()
    if frame is None:
        return set()
    try:
        crop = _crop_frame_by_roi(frame, roi)
        if crop is None or getattr(crop, "size", 0) <= 0:
            return set()

        regions = _extract_ocr_regions_from_crop(crop)
        if regions:
            kept_regions: List[Dict[str, Any]] = []
            h, w = int(crop.shape[0]), int(crop.shape[1])
            for region in regions:
                if not _is_subtitle_like_region(region, image_w=w, image_h=h):
                    kept_regions.append(region)
            tokens = set()
            for region in kept_regions:
                tokens.update(_tokenize_text_for_incremental(str(region.get("text", "") or "")))
            if tokens:
                return tokens

        # 若区域 OCR 未产出有效 token，回退整图 OCR 保持兼容。
        if extractor is None:
            return set()
        text = extractor.extract_text_from_frame(crop, preprocess=True)
        if not text:
            return set()
        return _tokenize_text_for_incremental(text)
    except Exception:
        return set()


def _extract_shape_signature(frame: np.ndarray, roi: Optional[Tuple[int, int, int, int]]) -> Dict[str, float]:
    """方法说明：`_extract_shape_signature` 工具方法。
    执行步骤：
    1) 步骤1：在 ROI 区域执行边缘检测与轮廓提取。
    2) 步骤2：统计矩形轮廓数与有效连通轮廓数。
    3) 步骤3：输出形状签名用于“增量截图”比较。"""
    if frame is None:
        return {"rect_count": 0, "component_count": 0, "edge_density": 0.0}

    crop = _crop_frame_by_roi(frame, roi)
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY) if len(crop.shape) == 3 else crop
    edges = cv2.Canny(gray, 50, 150)
    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

    min_area = max(20.0, gray.shape[0] * gray.shape[1] * 0.0002)
    rect_count = 0
    component_count = 0
    for cnt in contours:
        area = cv2.contourArea(cnt)
        if area < min_area:
            continue
        component_count += 1
        peri = cv2.arcLength(cnt, True)
        if peri <= 0:
            continue
        approx = cv2.approxPolyDP(cnt, 0.02 * peri, True)
        if len(approx) == 4:
            rect_count += 1

    edge_density = float(np.count_nonzero(edges) / edges.size) if edges.size > 0 else 0.0
    return {
        "rect_count": int(rect_count),
        "component_count": int(component_count),
        "edge_density": edge_density,
    }


def _is_incremental_screenshot(base: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    """方法说明：`_is_incremental_screenshot` 工具方法。
    执行步骤：
    1) 步骤1：检查文本增量关系（candidate 覆盖 base 全量文本且文本更多）。
    2) 步骤2：检查形状增量关系（candidate 覆盖 base 形状统计且更丰富）。
    3) 步骤3：满足任一增量条件则返回 `True`。"""
    base_tokens = set(base.get("ocr_tokens") or [])
    cand_tokens = set(candidate.get("ocr_tokens") or [])

    text_incremental = False
    if base_tokens:
        text_incremental = base_tokens.issubset(cand_tokens) and len(cand_tokens) > len(base_tokens)

    base_shape = base.get("shape_signature") or {}
    cand_shape = candidate.get("shape_signature") or {}
    b_rect = int(base_shape.get("rect_count", 0) or 0)
    b_comp = int(base_shape.get("component_count", 0) or 0)
    c_rect = int(cand_shape.get("rect_count", 0) or 0)
    c_comp = int(cand_shape.get("component_count", 0) or 0)

    shape_incremental = False
    if b_rect > 0 or b_comp > 0:
        shape_incremental = (c_rect >= b_rect and c_comp >= b_comp) and (c_rect > b_rect or c_comp > b_comp)

    return text_incremental or shape_incremental


def _filter_incremental_screenshots(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """方法说明：`_filter_incremental_screenshots` 核心方法。
    执行步骤：
    1) 步骤1：逐一检查每个候选是否被其它候选“增量覆盖”。
    2) 步骤2：仅保留未被覆盖的候选截图。
    3) 步骤3：若全部被覆盖（极端情况），回退保留分数最高截图。"""
    if len(candidates) <= 1:
        return candidates

    keep = [True for _ in candidates]
    for idx, base in enumerate(candidates):
        for jdx, cand in enumerate(candidates):
            if idx == jdx:
                continue
            if _is_incremental_screenshot(base, cand):
                keep[idx] = False
                break

    filtered = [item for i, item in enumerate(candidates) if keep[i]]
    if filtered:
        return filtered

    return [max(candidates, key=lambda item: float(item.get("score", 0.0)))]


def run_cv_validation_task(video_path: str, unit_data: dict, shm_frames: dict = None) -> dict:
    """方法说明：`run_cv_validation_task` 核心方法。
    执行步骤：
    1) 步骤1：执行内存保护检查并准备 `validator` 实例。
    2) 步骤2：优先使用共享帧执行 CV 校验，必要时回退常规路径。
    3) 步骤3：返回统一结构化结果，异常时返回降级错误结果。"""
    global _validator_cache
    
    # 🚀 Manual Memory Check (Windows Safety)
    _check_memory_usage()
    used_shm_names: set = set()
    injected_frame_indices: List[int] = []
    validator = None
    
    try:
        # 鑾峰彇鎴栧垱寤?Validator (杩涚▼鍐呯紦瀛?
        # 鑾峰彇鎴栧垱寤?Validator (杩涚▼鍐呯紦瀛?
        if video_path not in _validator_cache:
            from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator
            
            # Encoding fixed: PID note.
            logger.info(f"馃啎 [PID={os.getpid()}] Initializing CVKnowledgeValidator for: {os.path.basename(video_path)}")
            _validator_cache[video_path] = CVKnowledgeValidator(video_path, use_resource_manager=False)
        else:
            logger.debug(f"鈾伙笍 [PID={os.getpid()}] Cache HIT for validator: {os.path.basename(video_path)}")
        
        validator = _validator_cache[video_path]
        
        # Encoding fixed: Validator note.
        if shm_frames:
            injected_count = 0
            for frame_idx_str, shm_ref in shm_frames.items():
                frame = get_frame_from_shm(shm_ref)
                if frame is not None:
                    shm_name = shm_ref.get("shm_name") if isinstance(shm_ref, dict) else None
                    if shm_name:
                        used_shm_names.add(str(shm_name))
                    # Encoding fixed: Validator note.
                    if hasattr(validator, '_frame_cache'):
                        frame_idx = int(frame_idx_str)
                        validator._frame_cache[frame_idx] = frame.copy()
                        injected_frame_indices.append(frame_idx)
                        injected_count += 1
            if injected_count > 0:
                logger.debug(f"Injected {injected_count} frames from SharedMemory")
        
        # 执行 CV 验证
        stable_islands, action_units, redundancy_segments = validator.detect_visual_states(
            start_sec=unit_data["start_sec"],
            end_sec=unit_data["end_sec"]
        )
        
        # 搴忓垪鍖栫粨鏋?(Dataclass -> Dict)
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
        
        return {
            "unit_id": unit_data["unit_id"],
            "stable_islands": stable_islands_data,
            "action_segments": action_segments_data
        }
        
    except Exception as e:
        import traceback
        logger.error(f"鉂?CV validation failed for {unit_data['unit_id']}: {e}")
        logger.error(traceback.format_exc())
        return {
            "unit_id": unit_data["unit_id"],
            "stable_islands": [],
            "action_segments": [],
            "error": str(e)
        }
    finally:
        try:
            if validator is not None and hasattr(validator, "_frame_cache") and injected_frame_indices:
                for frame_idx in injected_frame_indices:
                    validator._frame_cache.pop(frame_idx, None)
            attached_before_release = _count_attached_shm_refs()
            if used_shm_names:
                release_attached_shm_refs(list(used_shm_names))
                logger.info(
                    "[SHM Worker] task=cv_validation unit=%s used=%s attached_before=%s attached_after=%s",
                    unit_data.get("unit_id", ""),
                    len(used_shm_names),
                    attached_before_release,
                    _count_attached_shm_refs(),
                )
            gc.collect()
        except Exception:
            pass


def run_detect_stable_islands_task(clip_path: str, unit_id: str = "", duration_sec: float = 0.0) -> List[Tuple[float, float]]:
    """方法说明：`run_detect_stable_islands_task` 核心方法。
    执行步骤：
    1) 步骤1：按 clip 路径复用或创建 `CVKnowledgeValidator`（worker 进程内缓存）。
    2) 步骤2：执行 `stable_only=True` 的视觉状态检测并抽取稳定区间。
    3) 步骤3：返回区间列表，异常时返回空列表并记录日志。"""
    global _validator_cache

    _check_memory_usage()

    try:
        if clip_path not in _validator_cache:
            from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator

            logger.info(
                f"🤖 [PID={os.getpid()}] Initializing CVKnowledgeValidator for pre-prune clip: {os.path.basename(clip_path)}"
            )
            _validator_cache[clip_path] = CVKnowledgeValidator(clip_path, use_resource_manager=False)

        validator = _validator_cache[clip_path]
        detected_duration = max(0.0, float(getattr(validator, "duration_sec", 0.0) or 0.0))
        requested_duration = max(0.0, float(duration_sec or 0.0))
        scan_duration = detected_duration if detected_duration > 0.0 else requested_duration
        if scan_duration <= 0.0:
            return []

        stable_islands, _, _ = validator.detect_visual_states(0.0, scan_duration, stable_only=True)

        result: List[Tuple[float, float]] = []
        for island in stable_islands:
            try:
                start_sec = float(getattr(island, "start_sec", 0.0) or 0.0)
                end_sec = float(getattr(island, "end_sec", start_sec) or start_sec)
            except (TypeError, ValueError):
                continue
            if end_sec > start_sec:
                result.append((start_sec, end_sec))

        logger.debug(
            f"[VL-PrePruneWorker] stable detect done: unit={unit_id}, clip={os.path.basename(clip_path)}, islands={len(result)}"
        )
        return result
    except Exception as error:
        logger.warning(
            f"[VL-PrePruneWorker] stable detect failed: unit={unit_id}, clip={os.path.basename(clip_path)}, error={error}"
        )
        return []


def cleanup_worker_resources():
    """?? Worker ????????????????"""
    global _validator_cache, _attached_shms
    
    # 关闭 Validators
    for path, validator in list(_validator_cache.items()):
        try:
            if hasattr(validator, 'close'):
                validator.close()
        except Exception:
            pass
    _validator_cache.clear()
    
    # Encoding fixed: corrupted comment cleaned.
    for shm_name, shm in list(_attached_shms.items()):
        try:
            shm.close()  # 鍙?close 涓?unlink (涓昏繘绋嬭礋璐?unlink)
        except Exception:
            pass
    _attached_shms.clear()


def run_screenshot_selection_task(
    video_path: str,
    unit_id: str,
    island_index: int,
    expanded_start: float,
    expanded_end: float,
    shm_frames: Dict[float, dict],
    fps: float = 30.0,
    static_island_min_ms: Optional[float] = None,
) -> dict:
    """方法说明：`run_screenshot_selection_task` 核心方法。
    执行步骤：
    1) 步骤1：读取共享内存帧并构建时间戳序列。
    2) 步骤2：调用轻量选择器挑选最佳截图时间点。
    3) 步骤3：输出选择结果，失败时回退到区间中点策略。"""
    try:
        _check_memory_usage()

        start_sec = float(expanded_start)
        end_sec = float(expanded_end)
        if static_island_min_ms is None:
            min_static_island_ms = _get_env_float("CV_ROUTE_STATIC_ISLAND_MIN_MS", 200.0, 0.0, 5000.0)
        else:
            try:
                min_static_island_ms = float(static_island_min_ms)
            except (TypeError, ValueError):
                min_static_island_ms = 200.0
            min_static_island_ms = max(0.0, min(5000.0, min_static_island_ms))
        used_shm_names: set = set()

        # 1. Read frame data from SharedMemory
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"鈻讹笍 Task start: {unit_id}_island{island_index} (PID={os.getpid()})")
        elif logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"鈻讹笍 Task start: {unit_id}_island{island_index} (PID={os.getpid()})")
        
        # 1. Read frame data from SharedMemory
        frames = []
        timestamps = []
        
        shm_names_sample = []
        for ts, shm_ref in sorted(shm_frames.items()):
            if len(shm_names_sample) < 3:
                try:
                    shm_name = shm_ref.get("shm_name")
                    if shm_name:
                        shm_names_sample.append(shm_name)
                except Exception:
                    pass
            frame = get_frame_from_shm(shm_ref)
            if frame is not None:
                frames.append(frame)
                timestamps.append(ts)
                shm_name = shm_ref.get("shm_name") if isinstance(shm_ref, dict) else None
                if shm_name:
                    used_shm_names.add(str(shm_name))
        
        if not frames:
            logger.warning(
                f"No frames read from SharedMemory for {unit_id}_island{island_index} "
                f"(PID={os.getpid()}, shm_sample={shm_names_sample})"
            )
            return {
                "unit_id": unit_id,
                "island_index": island_index,
                "selected_timestamp": (expanded_start + expanded_end) / 2,
                "quality_score": 0.0,
                "island_count": 0,
                "analyzed_frames": 0,
                "candidate_screenshots": [
                    {
                        "timestamp_sec": (expanded_start + expanded_end) / 2,
                        "score": 0.0,
                        "island_index": 0,
                        "island_start": start_sec,
                        "island_end": end_sec,
                    }
                ],
                "static_island_threshold_ms": min_static_island_ms,
            }
        
        # 2. 鍒涘缓杞婚噺绾?ScreenshotSelector
        from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector
        
        selector_key = "screenshot_selector_lightweight"
        selector = _get_thread_local_screenshot_selector(selector_key)
        
        # Encoding fixed: corrupted comment cleaned.
        # 璁＄畻鍒嗚鲸鐜囩郴鏁?
        res_factor = frames[0].shape[1] / 1920.0 if frames else 1.0
        
        result = selector.select_from_shared_frames(
            frames=frames,
            timestamps=timestamps,
            fps=fps,
            res_factor=res_factor,
            min_static_island_ms=min_static_island_ms,
        )

        candidate_screenshots: List[Dict[str, Any]] = []
        raw_candidates = result.get("candidates", []) if isinstance(result, dict) else []
        if isinstance(raw_candidates, list):
            for item in raw_candidates:
                if not isinstance(item, dict):
                    continue
                try:
                    ts = float(item.get("timestamp_sec", result.get("selected_timestamp", 0.0)))
                except (TypeError, ValueError):
                    continue
                candidate_screenshots.append(
                    {
                        "timestamp_sec": ts,
                        "score": float(item.get("score", 0.0) or 0.0),
                        "island_index": int(item.get("island_index", 0) or 0),
                        "island_start": float(item.get("island_start", ts) or ts),
                        "island_end": float(item.get("island_end", ts) or ts),
                    }
                )
        if not candidate_screenshots:
            candidate_screenshots = [
                {
                    "timestamp_sec": float(result.get("selected_timestamp", (expanded_start + expanded_end) / 2)),
                    "score": float(result.get("quality_score", 0.0) or 0.0),
                    "island_index": 0,
                    "island_start": start_sec,
                    "island_end": end_sec,
                }
            ]
        candidate_screenshots.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        
        logger.info(
            f"鉁?Screenshot selected for {unit_id}_island{island_index} (PID={os.getpid()}): "
            f"t={result['selected_timestamp']:.2f}s, score={result['quality_score']:.3f} "
            f"(islands={result['island_count']}, frames={result['analyzed_frames']})"
        )
        
        return {
            "unit_id": unit_id,
            "island_index": island_index,
            "selected_timestamp": result["selected_timestamp"],
            "quality_score": result["quality_score"],
            "island_count": result["island_count"],
            "analyzed_frames": result["analyzed_frames"],
            "candidate_screenshots": candidate_screenshots,
            "static_island_threshold_ms": min_static_island_ms,
        }
        
    except Exception as e:
        import traceback
        logger.error(f"鉂?Screenshot selection failed for {unit_id}_island{island_index}: {e}")
        logger.error(traceback.format_exc())
        
        # 鍥為€€锛氳繑鍥炰腑鐐规椂闂存埑
        fallback_timestamp = (expanded_start + expanded_end) / 2
        return {
            "unit_id": unit_id,
            "island_index": island_index,
            "selected_timestamp": fallback_timestamp,
            "quality_score": 0.0,
            "island_count": 0,
            "analyzed_frames": 0,
            "candidate_screenshots": [
                {
                    "timestamp_sec": fallback_timestamp,
                    "score": 0.0,
                    "island_index": 0,
                    "island_start": start_sec,
                    "island_end": end_sec,
                }
            ],
            "static_island_threshold_ms": min_static_island_ms,
            "error": str(e)
        }
    finally:
        try:
            if "frames" in locals():
                del frames
            if "timestamps" in locals():
                del timestamps
            attached_before_release = _count_attached_shm_refs()
            if "used_shm_names" in locals() and used_shm_names:
                release_attached_shm_refs(list(used_shm_names))
                logger.info(
                    "[SHM Worker] task=screenshot_selection unit=%s island=%s used=%s attached_before=%s attached_after=%s",
                    unit_id,
                    island_index,
                    len(used_shm_names),
                    attached_before_release,
                    _count_attached_shm_refs(),
                )
            gc.collect()
        except Exception:
            pass


def run_select_screenshots_for_range_task(
    video_path: str,
    unit_id: str,
    start_sec: float,
    end_sec: float,
    coarse_fps: float = 2.0,
    fine_fps: float = 10.0,
    stable_islands_override: Optional[List[Tuple[float, float]]] = None,
    action_segments_override: Optional[List[Tuple[float, float]]] = None,
    analysis_max_width: int = 640,
    long_window_fine_chunk_sec: float = 20.0,
    decode_open_timeout_sec: int = 30,
    decode_allow_inline_transcode: bool = False,
    decode_enable_async_transcode: bool = True,
) -> dict:
    """
    璺敱鎴浘涓撶敤 Worker 鍏ュ彛锛氬湪杩涚▼姹犲唴鎵ц瀹屾暣 coarse-fine 閫夋嫨銆?
    淇濈暀鍘熸湁 ScreenshotSelector 鐨勯€夋嫨閫昏緫锛屼粎鏀瑰彉璋冨害鏂瑰紡锛堜富杩涚▼ -> ProcessPool锛夈€?    """
    _check_memory_usage()
    started_at = time.perf_counter()

    try:
        from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector

        selector_key = f"screenshot_selector_range_cf_{coarse_fps}_{fine_fps}"
        selector = _get_thread_local_screenshot_selector(selector_key)
        effective_video_path = _resolve_worker_readable_video_path(
            video_path,
            decode_open_timeout_sec=decode_open_timeout_sec,
            decode_allow_inline_transcode=decode_allow_inline_transcode,
            decode_enable_async_transcode=decode_enable_async_transcode,
        )
        route_roi = _get_route_roi(
            effective_video_path,
            start_sec,
            end_sec,
            decode_open_timeout_sec=decode_open_timeout_sec,
            decode_allow_inline_transcode=decode_allow_inline_transcode,
            decode_enable_async_transcode=decode_enable_async_transcode,
        )
        screenshots = selector.select_screenshots_for_range_sync(
            video_path=effective_video_path,
            start_sec=start_sec,
            end_sec=end_sec,
            coarse_fps=coarse_fps,
            fine_fps=fine_fps,
            roi=route_roi,
            stable_islands_override=stable_islands_override,
            action_segments_override=action_segments_override,
            analysis_max_width=max(0, int(analysis_max_width or 0)),
            long_window_fine_chunk_sec=max(0.0, float(long_window_fine_chunk_sec or 0.0)),
            decode_open_timeout_sec=max(5, int(decode_open_timeout_sec)),
            decode_allow_inline_transcode=bool(decode_allow_inline_transcode),
            decode_enable_async_transcode=bool(decode_enable_async_transcode),
        )

        enhanced_candidates: List[Dict[str, Any]] = []
        if screenshots and effective_video_path:
            cap, _, _ = open_video_capture_with_fallback(
                effective_video_path,
                timeout_sec=max(5, int(decode_open_timeout_sec)),
                logger=logger,
                allow_inline_transcode=bool(decode_allow_inline_transcode),
                enable_async_transcode=bool(decode_enable_async_transcode),
            )
            try:
                if cap is not None:
                    fps_val = float(cap.get(cv2.CAP_PROP_FPS) or 30.0)
                    for item in screenshots:
                        ts = float(item.get("timestamp_sec", (start_sec + end_sec) / 2.0))
                        frame = None
                        if cap.isOpened() and fps_val > 0:
                            frame_idx = int(max(0.0, ts) * fps_val)
                            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                            ok, sampled = cap.read()
                            if ok and sampled is not None:
                                frame = sampled

                        ocr_tokens, shape_sig = _analyze_frame_for_incremental_screenshot(
                            frame,
                            route_roi,
                            analysis_max_width,
                        )
                        enriched = dict(item)
                        enriched["ocr_tokens"] = sorted(ocr_tokens)
                        enriched["shape_signature"] = shape_sig
                        enhanced_candidates.append(enriched)
            finally:
                if cap is not None:
                    cap.release()

        if enhanced_candidates:
            screenshots = _filter_incremental_screenshots(enhanced_candidates)

        if not screenshots:
            mid = (start_sec + end_sec) / 2 if end_sec >= start_sec else start_sec
            screenshots = [{"timestamp_sec": mid, "score": 0.0}]

        return {
            "unit_id": unit_id,
            "start_sec": float(start_sec),
            "end_sec": float(end_sec),
            "screenshots": screenshots,
            "route_roi": route_roi,
            "worker_pid": os.getpid(),
            "elapsed_ms": (time.perf_counter() - started_at) * 1000.0,
        }
    except Exception as e:
        logger.error(f"鉂?Routed screenshot selection failed for {unit_id}: {e}")
        fallback_mid = (start_sec + end_sec) / 2 if end_sec >= start_sec else start_sec
        return {
            "unit_id": unit_id,
            "start_sec": float(start_sec),
            "end_sec": float(end_sec),
            "screenshots": [{"timestamp_sec": fallback_mid, "score": 0.0}],
            "worker_pid": os.getpid(),
            "elapsed_ms": (time.perf_counter() - started_at) * 1000.0,
            "error": str(e),
        }


def warmup_worker() -> int:
    """
    鐢ㄤ簬璇婃柇 ProcessPool 鏄惁鐪熸鍒嗛厤浠诲姟鍒板涓?Worker銆?
    浣跨敤鏂瑰紡锛氫富杩涚▼鍦ㄥ紑濮嬪苟琛屾埅鍥鹃€夋嫨鍓嶆彁浜?N 涓?warmup_worker 浠诲姟锛屾敹闆嗚繑鍥炵殑 PID 闆嗗悎銆?    """
    pid = os.getpid()
    logger.info(f"馃敟 Warmup worker task executed (PID={pid})")
    return pid


def run_coarse_fine_screenshot_task(
    unit_id: str,
    start_sec: float,
    end_sec: float,
    coarse_shm_frames: Dict[float, dict],
    coarse_interval: float,
    fine_shm_frames_by_island: List[Dict[float, dict]] = None,
    video_path: str = "",
    stable_islands_override: Optional[List[Tuple[float, float]]] = None,
    analysis_max_width: int = 640,
    decode_open_timeout_sec: int = 30,
    decode_allow_inline_transcode: bool = False,
    decode_enable_async_transcode: bool = True,
) -> dict:
    """方法说明：`run_coarse_fine_screenshot_task` 核心方法。
    执行步骤：
    1) 步骤1：先在粗粒度共享帧中完成候选岛屿筛选。
    2) 步骤2：对候选岛屿执行细粒度评分与最优帧选择。
    3) 步骤3：汇总并返回多候选截图结果，必要时提供兜底结果。"""
    _check_memory_usage()
    used_shm_names: set = set()
    
    try:
        # 1. 浠?SharedMemory 璇诲彇绮楅噰鏍峰抚
        coarse_frames = []
        coarse_timestamps = []
        
        for ts, shm_ref in sorted(coarse_shm_frames.items()):
            frame = get_frame_from_shm(shm_ref)
            if frame is not None:
                coarse_frames.append(frame)
                coarse_timestamps.append(ts)
                shm_name = shm_ref.get("shm_name") if isinstance(shm_ref, dict) else None
                if shm_name:
                    used_shm_names.add(str(shm_name))
        
        if len(coarse_frames) < 2:
            logger.warning(f"Insufficient coarse frames for {unit_id}: {len(coarse_frames)}")
            return {
                "unit_id": unit_id,
                "stable_islands": [{"start_sec": start_sec, "end_sec": end_sec}],
                "screenshots": [{"timestamp_sec": (start_sec + end_sec) / 2, "score": 0.0, 
                                 "island_start": start_sec, "island_end": end_sec}],
                "start_sec": start_sec,
                "end_sec": end_sec
            }
        
        # 2. 获取 ScreenshotSelector
        from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector
        
        global _validator_cache
        if '_validator_cache' not in globals():
            globals()['_validator_cache'] = {}
        
        _validator_cache = globals()['_validator_cache']
        selector_key = "screenshot_selector_lightweight"
        
        if selector_key not in _validator_cache:
            logger.info(f"馃啎 [PID={os.getpid()}] Initializing ScreenshotSelector (lightweight)")
            _validator_cache[selector_key] = ScreenshotSelector.create_lightweight()
        
        selector = _validator_cache[selector_key]
        effective_video_path = (
            _resolve_worker_readable_video_path(
                video_path,
                decode_open_timeout_sec=decode_open_timeout_sec,
                decode_allow_inline_transcode=decode_allow_inline_transcode,
                decode_enable_async_transcode=decode_enable_async_transcode,
            )
            if video_path
            else ""
        )
        route_roi = (
            _get_route_roi(
                effective_video_path,
                start_sec,
                end_sec,
                decode_open_timeout_sec=decode_open_timeout_sec,
                decode_allow_inline_transcode=decode_allow_inline_transcode,
                decode_enable_async_transcode=decode_enable_async_transcode,
            )
            if effective_video_path
            else None
        )
        
        # Encoding fixed: Stage note.
        stable_islands = []
        if isinstance(stable_islands_override, list) and stable_islands_override:
            for item in stable_islands_override:
                if not isinstance(item, (list, tuple)) or len(item) < 2:
                    continue
                try:
                    s = float(item[0])
                    e = float(item[1])
                except (TypeError, ValueError):
                    continue
                s = max(start_sec, s)
                e = min(end_sec, e)
                if e > s:
                    stable_islands.append({"start_sec": s, "end_sec": e})
        else:
            stable_islands = selector.detect_stable_islands_from_frames(
                frames=coarse_frames,
                timestamps=coarse_timestamps,
                interval=coarse_interval,
                roi=route_roi,
            )
        
        if not stable_islands:
            stable_islands = [{"start_sec": start_sec, "end_sec": end_sec}]
        
        logger.info(f"Stage 1 complete for {unit_id}: {len(stable_islands)} stable islands")
        
        # 4. Stage 2: 濡傛灉鎻愪緵浜嗙粏閲囨牱甯э紝閫夋嫨鏈€浣冲抚
        screenshots = []
        
        if fine_shm_frames_by_island and len(fine_shm_frames_by_island) == len(stable_islands):
            for island_idx, (island, fine_shm_frames) in enumerate(zip(stable_islands, fine_shm_frames_by_island)):
                fine_frames = []
                fine_timestamps = []
                
                for ts, shm_ref in sorted(fine_shm_frames.items()):
                    frame = get_frame_from_shm(shm_ref)
                    if frame is not None:
                        fine_frames.append(frame)
                        fine_timestamps.append(ts)
                        shm_name = shm_ref.get("shm_name") if isinstance(shm_ref, dict) else None
                        if shm_name:
                            used_shm_names.add(str(shm_name))
                
                if not fine_frames:
                    fallback_ts = (island["start_sec"] + island["end_sec"]) / 2
                    screenshots.append({
                        "timestamp_sec": fallback_ts,
                        "island_index": island_idx,
                        "score": 0.0,
                        "island_start": island["start_sec"],
                        "island_end": island["end_sec"],
                        "_analysis_frame": _find_nearest_frame(coarse_frames, coarse_timestamps, fallback_ts),
                    })
                    continue
                
                best_ts, best_score, best_idx = selector.select_best_frame_from_frames(
                    frames=fine_frames,
                    timestamps=fine_timestamps,
                    roi=route_roi,
                    return_index=True,
                )
                analysis_frame = fine_frames[best_idx] if 0 <= int(best_idx) < len(fine_frames) else None
                
                screenshots.append({
                    "timestamp_sec": best_ts,
                    "island_index": island_idx,
                    "score": float(best_score),
                    "island_start": island["start_sec"],
                    "island_end": island["end_sec"],
                    "_analysis_frame": analysis_frame,
                })
        else:
            # Encoding fixed: corrupted comment cleaned.
            for island_idx, island in enumerate(stable_islands):
                fallback_ts = (island["start_sec"] + island["end_sec"]) / 2
                screenshots.append({
                    "timestamp_sec": fallback_ts,
                    "island_index": island_idx,
                    "score": 0.5,
                    "island_start": island["start_sec"],
                    "island_end": island["end_sec"],
                    "_analysis_frame": _find_nearest_frame(coarse_frames, coarse_timestamps, fallback_ts),
                })
        
        if screenshots:
            need_video_fallback = any(item.get("_analysis_frame") is None for item in screenshots if isinstance(item, dict))
            cap = None
            enhanced_candidates: List[Dict[str, Any]] = []
            try:
                if need_video_fallback and effective_video_path:
                    cap, _, _ = open_video_capture_with_fallback(
                        effective_video_path,
                        timeout_sec=max(5, int(decode_open_timeout_sec)),
                        logger=logger,
                        allow_inline_transcode=bool(decode_allow_inline_transcode),
                        enable_async_transcode=bool(decode_enable_async_transcode),
                    )
                fps_val = float(cap.get(cv2.CAP_PROP_FPS) or 30.0) if cap is not None else 0.0
                for item in screenshots:
                    ts = float(item.get("timestamp_sec", (start_sec + end_sec) / 2.0))
                    frame = item.get("_analysis_frame") if isinstance(item, dict) else None
                    if frame is None:
                        frame = _find_nearest_frame(coarse_frames, coarse_timestamps, ts)
                    if frame is None and cap is not None and cap.isOpened() and fps_val > 0:
                        frame_idx = int(max(0.0, ts) * fps_val)
                        cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                        ok, sampled = cap.read()
                        if ok and sampled is not None:
                            frame = sampled

                    ocr_tokens, shape_sig = _analyze_frame_for_incremental_screenshot(
                        frame,
                        route_roi,
                        analysis_max_width,
                    )
                    enriched = {k: v for k, v in dict(item).items() if not str(k).startswith('_')}
                    enriched["ocr_tokens"] = sorted(ocr_tokens)
                    enriched["shape_signature"] = shape_sig
                    enhanced_candidates.append(enriched)
            finally:
                if cap is not None:
                    cap.release()

            if enhanced_candidates:
                screenshots = _filter_incremental_screenshots(enhanced_candidates)
            else:
                screenshots = _strip_private_screenshot_fields(screenshots)

        logger.info(
            f"鉁?Coarse-Fine complete for {unit_id}: "
            f"{len(stable_islands)} islands, {len(screenshots)} screenshots"
        )
        
        return {
            "unit_id": unit_id,
            "stable_islands": stable_islands,
            "screenshots": screenshots,
            "start_sec": start_sec,
            "end_sec": end_sec,
            "route_roi": route_roi,
        }
        
    except Exception as e:
        import traceback
        logger.error(f"鉂?Coarse-Fine screenshot failed for {unit_id}: {e}")
        logger.error(traceback.format_exc())
        
        fallback_timestamp = (start_sec + end_sec) / 2
        return {
            "unit_id": unit_id,
            "stable_islands": [{"start_sec": start_sec, "end_sec": end_sec}],
            "screenshots": [{
                "timestamp_sec": fallback_timestamp,
                "island_index": 0,
                "score": 0.0,
                "island_start": start_sec,
                "island_end": end_sec
            }],
            "start_sec": start_sec,
            "end_sec": end_sec,
            "error": str(e)
        }
    finally:
        try:
            if "coarse_frames" in locals():
                del coarse_frames
            if "coarse_timestamps" in locals():
                del coarse_timestamps
            if "fine_frames" in locals():
                del fine_frames
            if "fine_timestamps" in locals():
                del fine_timestamps
            attached_before_release = _count_attached_shm_refs()
            if "used_shm_names" in locals() and used_shm_names:
                release_attached_shm_refs(list(used_shm_names))
                logger.info(
                    "[SHM Worker] task=coarse_fine_screenshot unit=%s used=%s attached_before=%s attached_after=%s",
                    unit_id,
                    len(used_shm_names),
                    attached_before_release,
                    _count_attached_shm_refs(),
                )
            gc.collect()
        except Exception:
            pass
