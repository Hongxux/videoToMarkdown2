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
import psutil
from typing import Dict, List, Tuple, Any, Optional
from multiprocessing import shared_memory
import numpy as np

# Worker 进程内的全局缓存
_validator_cache: Dict[str, Any] = {}
_initialized = False
_attached_shms: Dict[str, shared_memory.SharedMemory] = {}  # 宸查檮鍔犵殑鍏变韩鍐呭瓨

logger = logging.getLogger(__name__)


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


def run_cv_validation_task(video_path: str, unit_data: dict, shm_frames: dict = None) -> dict:
    """方法说明：`run_cv_validation_task` 核心方法。
    执行步骤：
    1) 步骤1：执行内存保护检查并准备 `validator` 实例。
    2) 步骤2：优先使用共享帧执行 CV 校验，必要时回退常规路径。
    3) 步骤3：返回统一结构化结果，异常时返回降级错误结果。"""
    global _validator_cache
    
    # 🚀 Manual Memory Check (Windows Safety)
    _check_memory_usage()
    
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
                    # Encoding fixed: Validator note.
                    if hasattr(validator, '_frame_cache'):
                        validator._frame_cache[int(frame_idx_str)] = frame.copy()
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
        screenshots = selector.select_screenshots_for_range_sync(
            video_path=video_path,
            start_sec=start_sec,
            end_sec=end_sec,
            coarse_fps=coarse_fps,
            fine_fps=fine_fps,
        )

        if not screenshots:
            mid = (start_sec + end_sec) / 2 if end_sec >= start_sec else start_sec
            screenshots = [{"timestamp_sec": mid, "score": 0.0}]

        return {
            "unit_id": unit_id,
            "start_sec": float(start_sec),
            "end_sec": float(end_sec),
            "screenshots": screenshots,
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
    fine_shm_frames_by_island: List[Dict[float, dict]] = None
) -> dict:
    """方法说明：`run_coarse_fine_screenshot_task` 核心方法。
    执行步骤：
    1) 步骤1：先在粗粒度共享帧中完成候选岛屿筛选。
    2) 步骤2：对候选岛屿执行细粒度评分与最优帧选择。
    3) 步骤3：汇总并返回多候选截图结果，必要时提供兜底结果。"""
    _check_memory_usage()
    
    try:
        # 1. 浠?SharedMemory 璇诲彇绮楅噰鏍峰抚
        coarse_frames = []
        coarse_timestamps = []
        
        for ts, shm_ref in sorted(coarse_shm_frames.items()):
            frame = get_frame_from_shm(shm_ref)
            if frame is not None:
                coarse_frames.append(frame)
                coarse_timestamps.append(ts)
        
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
        
        # Encoding fixed: Stage note.
        stable_islands = selector.detect_stable_islands_from_frames(
            frames=coarse_frames,
            timestamps=coarse_timestamps,
            interval=coarse_interval
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
                    timestamps=fine_timestamps
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
        
        logger.info(
            f"鉁?Coarse-Fine complete for {unit_id}: "
            f"{len(stable_islands)} islands, {len(screenshots)} screenshots"
        )
        
        return {
            "unit_id": unit_id,
            "stable_islands": stable_islands,
            "screenshots": screenshots,
            "start_sec": start_sec,
            "end_sec": end_sec
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
            gc.collect()
        except Exception:
            pass
