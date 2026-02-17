"""CV Worker ????????

??????????CV ????????????
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
_attached_shms: Dict[str, shared_memory.SharedMemory] = {}  # 宸查檮鍔犵殑鍏变韩鍐呭瓨

logger = logging.getLogger(__name__)

_TEXT_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9\u4e00-\u9fff]+")


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


def get_frame_from_shm(shm_ref: dict, copy: bool = False) -> Optional[np.ndarray]:
    """方法说明：`get_frame_from_shm` 核心方法。
    执行步骤：
    1) 步骤1：从共享内存描述中解析名称、形状与数据类型。
    2) 步骤2：附加或复用共享内存对象并构建 `numpy` 视图。
    3) 步骤3：按需返回拷贝或只读视图，异常时返回 `None`。"""
    global _attached_shms
    
    try:
        shm_name = shm_ref.get("shm_name")
        shape = shm_ref.get("shape")
        dtype = shm_ref.get("dtype")
        
        if not all([shm_name, shape, dtype]):
            return None
        
        # 复用已附加的共享内存
        if shm_name not in _attached_shms:
            try:
                shm = shared_memory.SharedMemory(name=shm_name)
                _attached_shms[shm_name] = shm
            except FileNotFoundError:
                logger.warning(f"SharedMemory not found: {shm_name}")
                return None
        
        shm = _attached_shms[shm_name]
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
    global _attached_shms

    if shm_names is None:
        target_names = list(_attached_shms.keys())
    else:
        target_names = [name for name in shm_names if name]

    for shm_name in target_names:
        shm = _attached_shms.pop(shm_name, None)
        if shm is None:
            continue
        try:
            # 仅 close 不 unlink：共享内存所有权在主进程 registry。
            shm.close()
        except Exception:
            pass


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

        _validator_cache[key] = OCRExtractor()
    except Exception as e:
        logger.warning(f"Route screenshot OCR extractor init failed: {e}")
        _validator_cache[key] = None
    return _validator_cache[key]


def _extract_ocr_tokens(frame: np.ndarray, roi: Optional[Tuple[int, int, int, int]]) -> set:
    """方法说明：`_extract_ocr_tokens` 工具方法。
    执行步骤：
    1) 步骤1：在 ROI 内裁剪图像。
    2) 步骤2：调用 OCR 提取文本。
    3) 步骤3：将文本切分为 token 集合用于增量判定。"""
    extractor = _get_ocr_extractor()
    if extractor is None or frame is None:
        return set()
    try:
        crop = _crop_frame_by_roi(frame, roi)
        text = extractor.extract_text_from_frame(crop, preprocess=True)
        if not text:
            return set()
        return {tok.lower() for tok in _TEXT_TOKEN_PATTERN.findall(text) if tok.strip()}
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
            if used_shm_names:
                release_attached_shm_refs(list(used_shm_names))
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
    fps: float = 30.0
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
                "analyzed_frames": 0
            }
        
        # 2. 鍒涘缓杞婚噺绾?ScreenshotSelector
        from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector
        
        global _validator_cache
        selector_key = "screenshot_selector_lightweight"
        
        if selector_key not in _validator_cache:
            _validator_cache[selector_key] = ScreenshotSelector.create_lightweight()
        
        selector = _validator_cache[selector_key]
        
        # Encoding fixed: corrupted comment cleaned.
        # 璁＄畻鍒嗚鲸鐜囩郴鏁?
        res_factor = frames[0].shape[1] / 1920.0 if frames else 1.0
        
        result = selector.select_from_shared_frames(
            frames=frames,
            timestamps=timestamps,
            fps=fps,
            res_factor=res_factor
        )
        
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
            "analyzed_frames": result["analyzed_frames"]
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
            "error": str(e)
        }
    finally:
        try:
            if "frames" in locals():
                del frames
            if "timestamps" in locals():
                del timestamps
            if "used_shm_names" in locals() and used_shm_names:
                release_attached_shm_refs(list(used_shm_names))
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
    璺敱鎴浘涓撶敤 Worker 鍏ュ彛锛氬湪杩涚▼姹犲唴鎵ц瀹屾暣 coarse-fine 閫夋嫨銆?
    淇濈暀鍘熸湁 ScreenshotSelector 鐨勯€夋嫨閫昏緫锛屼粎鏀瑰彉璋冨害鏂瑰紡锛堜富杩涚▼ -> ProcessPool锛夈€?    """
    _check_memory_usage()
    started_at = time.perf_counter()

    try:
        from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector

        global _validator_cache
        if '_validator_cache' not in globals():
            globals()['_validator_cache'] = {}
        _validator_cache = globals()['_validator_cache']

        selector_key = f"screenshot_selector_range_cf_{coarse_fps}_{fine_fps}"
        if selector_key not in _validator_cache:
            _validator_cache[selector_key] = ScreenshotSelector.create_lightweight()

        selector = _validator_cache[selector_key]
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
        if screenshots:
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

                        ocr_tokens = _extract_ocr_tokens(frame, route_roi) if frame is not None else set()
                        shape_sig = _extract_shape_signature(frame, route_roi) if frame is not None else {
                            "rect_count": 0,
                            "component_count": 0,
                            "edge_density": 0.0,
                        }
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
    鐢ㄤ簬璇婃柇 ProcessPool 鏄惁鐪熸鍒嗛厤浠诲姟鍒板涓?Worker銆?
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
                    # Fallback: 浣跨敤宀涗腑鐐?
                    screenshots.append({
                        "timestamp_sec": (island["start_sec"] + island["end_sec"]) / 2,
                        "island_index": island_idx,
                        "score": 0.0,
                        "island_start": island["start_sec"],
                        "island_end": island["end_sec"]
                    })
                    continue
                
                best_ts, best_score = selector.select_best_frame_from_frames(
                    frames=fine_frames,
                    timestamps=fine_timestamps,
                    roi=route_roi,
                )
                
                screenshots.append({
                    "timestamp_sec": best_ts,
                    "island_index": island_idx,
                    "score": float(best_score),
                    "island_start": island["start_sec"],
                    "island_end": island["end_sec"]
                })
        else:
            # Encoding fixed: corrupted comment cleaned.
            for island_idx, island in enumerate(stable_islands):
                screenshots.append({
                    "timestamp_sec": (island["start_sec"] + island["end_sec"]) / 2,
                    "island_index": island_idx,
                    "score": 0.5,
                    "island_start": island["start_sec"],
                    "island_end": island["end_sec"]
                })
        
        if screenshots and effective_video_path:
            cap, _, _ = open_video_capture_with_fallback(
                effective_video_path,
                timeout_sec=max(5, int(decode_open_timeout_sec)),
                logger=logger,
                allow_inline_transcode=bool(decode_allow_inline_transcode),
                enable_async_transcode=bool(decode_enable_async_transcode),
            )
            enhanced_candidates: List[Dict[str, Any]] = []
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

                        ocr_tokens = _extract_ocr_tokens(frame, route_roi) if frame is not None else set()
                        shape_sig = _extract_shape_signature(frame, route_roi) if frame is not None else {
                            "rect_count": 0,
                            "component_count": 0,
                            "edge_density": 0.0,
                        }
                        enriched = dict(item)
                        enriched["ocr_tokens"] = sorted(ocr_tokens)
                        enriched["shape_signature"] = shape_sig
                        enhanced_candidates.append(enriched)
            finally:
                if cap is not None:
                    cap.release()

            if enhanced_candidates:
                screenshots = _filter_incremental_screenshots(enhanced_candidates)

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
            if "used_shm_names" in locals() and used_shm_names:
                release_attached_shm_refs(list(used_shm_names))
            gc.collect()
        except Exception:
            pass
