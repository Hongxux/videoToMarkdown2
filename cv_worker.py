"""
模块说明：cv_worker 相关能力的封装。
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
import logging
import psutil
from typing import Dict, List, Tuple, Any, Optional
from multiprocessing import shared_memory
import numpy as np

# Worker 进程内的全局缓存
_validator_cache: Dict[str, Any] = {}
_initialized = False
_attached_shms: Dict[str, shared_memory.SharedMemory] = {}  # 已附加的共享内存

logger = logging.getLogger(__name__)


def init_cv_worker():
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过进程池并发实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：_initialized
    依据来源（证据链）：
    输入参数：
    - 无。
    输出参数：
    - 无（仅产生副作用，如日志/写盘/状态更新）。"""
    global _initialized
    
    if _initialized:
        return
    
    # 配置子进程日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - CV_WORKER[%(process)d] - %(levelname)s - %(message)s'
    )
    
    logger.info(f"🚀 CV Worker initialized with SharedMemory support (PID={os.getpid()})")
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
        logger.info(f"✅ Memory limit set (RLIMIT): Soft={_soft_limit_bytes/1024**3:.1f}GB")
    except ImportError:
        logger.info(f"⚠️ 'resource' check skipped (Windows). Manual limit active: {_soft_limit_bytes/1024**3:.1f}GB")

def _check_memory_usage():
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：rss > _soft_limit_bytes
    依据来源（证据链）：
    输入参数：
    - 无。
    输出参数：
    - 无（仅产生副作用，如日志/写盘/状态更新）。"""
    try:
        process = psutil.Process(os.getpid())
        rss = process.memory_info().rss
        if rss > _soft_limit_bytes:
            import gc
            gc.collect()
            logger.warning(f"⚠️ Worker Memory High ({rss/1024**3:.1f}GB). GC Triggered.")
    except Exception:
        pass


def get_frame_from_shm(shm_ref: dict) -> Optional[np.ndarray]:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过NumPy 数值计算实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：not all([shm_name, shape, dtype])
    - 条件：shm_name not in _attached_shms
    依据来源（证据链）：
    输入参数：
    - shm_ref: 函数入参（类型：dict）。
    输出参数：
    - copy 对象或调用结果。"""
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
        return frame.copy()  # 返回副本，避免共享内存被回收后访问
        
    except Exception as e:
        logger.warning(f"Failed to get frame from SharedMemory: {e}")
        return None


def run_cv_validation_task(video_path: str, unit_data: dict, shm_frames: dict = None) -> dict:
    """
    执行逻辑：
    1) 组织处理流程与依赖调用。
    2) 汇总中间结果并输出。
    实现方式：通过文件系统读写实现。
    核心价值：编排流程，保证步骤顺序与可追踪性。
    决策逻辑：
    - 条件：video_path not in _validator_cache
    - 条件：shm_frames
    - 条件：injected_count > 0
    依据来源（证据链）：
    - 输入参数：shm_frames, video_path。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - unit_data: 函数入参（类型：dict）。
    - shm_frames: 函数入参（类型：dict）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    global _validator_cache
    
    # 🚀 Manual Memory Check (Windows Safety)
    _check_memory_usage()
    
    try:
        # 获取或创建 Validator (进程内缓存)
        # 获取或创建 Validator (进程内缓存)
        if video_path not in _validator_cache:
            from MVP_Module2_HEANCING.module2_content_enhancement.cv_knowledge_validator import CVKnowledgeValidator
            
            # 使用 PID 标识不同进程的加载行为
            logger.info(f"🆕 [PID={os.getpid()}] Initializing CVKnowledgeValidator for: {os.path.basename(video_path)}")
            _validator_cache[video_path] = CVKnowledgeValidator(video_path, use_resource_manager=False)
        else:
            logger.debug(f"♻️ [PID={os.getpid()}] Cache HIT for validator: {os.path.basename(video_path)}")
        
        validator = _validator_cache[video_path]
        
        # 🚀 如果有共享内存帧，注入到 Validator 的帧缓存中
        if shm_frames:
            injected_count = 0
            for frame_idx_str, shm_ref in shm_frames.items():
                frame = get_frame_from_shm(shm_ref)
                if frame is not None:
                    # 注入到 Validator 的内部缓存 (如果支持)
                    if hasattr(validator, '_frame_cache'):
                        validator._frame_cache[int(frame_idx_str)] = frame
                        injected_count += 1
            if injected_count > 0:
                logger.debug(f"Injected {injected_count} frames from SharedMemory")
        
        # 执行 CV 验证
        stable_islands, action_units, redundancy_segments = validator.detect_visual_states(
            start_sec=unit_data["start_sec"],
            end_sec=unit_data["end_sec"]
        )
        
        # 序列化结果 (Dataclass -> Dict)
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
        logger.error(f"❌ CV validation failed for {unit_data['unit_id']}: {e}")
        logger.error(traceback.format_exc())
        return {
            "unit_id": unit_data["unit_id"],
            "stable_islands": [],
            "action_segments": [],
            "error": str(e)
        }


def cleanup_worker_resources():
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：hasattr(validator, 'close')
    依据来源（证据链）：
    输入参数：
    - 无。
    输出参数：
    - 无（仅产生副作用，如日志/写盘/状态更新）。"""
    global _validator_cache, _attached_shms
    
    # 关闭 Validators
    for path, validator in list(_validator_cache.items()):
        try:
            if hasattr(validator, 'close'):
                validator.close()
        except Exception:
            pass
    _validator_cache.clear()
    
    # 关闭附加的共享内存
    for shm_name, shm in list(_attached_shms.items()):
        try:
            shm.close()  # 只 close 不 unlink (主进程负责 unlink)
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
    """
    执行逻辑：
    1) 组织处理流程与依赖调用。
    2) 汇总中间结果并输出。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：编排流程，保证步骤顺序与可追踪性。
    决策逻辑：
    - 条件：not frames
    - 条件：selector_key not in _validator_cache
    - 条件：frame is not None
    依据来源（证据链）：
    输入参数：
    - video_path: 文件路径（类型：str）。
    - unit_id: 标识符（类型：str）。
    - island_index: 函数入参（类型：int）。
    - expanded_start: 起止时间/区间边界（类型：float）。
    - expanded_end: 起止时间/区间边界（类型：float）。
    - shm_frames: 函数入参（类型：Dict[float, dict]）。
    - fps: 函数入参（类型：float）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    调用 ScreenshotSelector.select_from_shared_frames()，保留完整的：
    - 波动容忍聚类
    - 岛屿博弈（过滤 + 去重）
    - 岛内择优
    video_path: 视频路径
    unit_id: 语义单元 ID
    island_index: 稳定岛索引
    expanded_start: 扩展后的起始时间
    expanded_end: 扩展后的结束时间
    fps: 视频帧率"""
    try:
        _check_memory_usage()
        
        # 1. 从 SharedMemory 读取帧
        frames = []
        timestamps = []
        
        for ts, shm_ref in sorted(shm_frames.items()):
            frame = get_frame_from_shm(shm_ref)
            if frame is not None:
                frames.append(frame.copy())  # 复制以避免 SharedMemory 生命周期问题
                timestamps.append(ts)
        
        if not frames:
            logger.warning(f"No frames read from SharedMemory for {unit_id}_island{island_index}")
            return {
                "unit_id": unit_id,
                "island_index": island_index,
                "selected_timestamp": (expanded_start + expanded_end) / 2,
                "quality_score": 0.0,
                "island_count": 0,
                "analyzed_frames": 0
            }
        
        # 2. 创建轻量级 ScreenshotSelector
        from MVP_Module2_HEANCING.module2_content_enhancement.screenshot_selector import ScreenshotSelector
        
        global _validator_cache
        selector_key = "screenshot_selector_lightweight"
        
        if selector_key not in _validator_cache:
            _validator_cache[selector_key] = ScreenshotSelector.create_lightweight()
        
        selector = _validator_cache[selector_key]
        
        # 3. 调用同步版本的截图选择（保留完整岛屿逻辑）
        # 计算分辨率系数
        res_factor = frames[0].shape[1] / 1920.0 if frames else 1.0
        
        result = selector.select_from_shared_frames(
            frames=frames,
            timestamps=timestamps,
            fps=fps,
            res_factor=res_factor
        )
        
        logger.info(
            f"✅ Screenshot selected for {unit_id}_island{island_index}: "
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
        logger.error(f"❌ Screenshot selection failed for {unit_id}_island{island_index}: {e}")
        logger.error(traceback.format_exc())
        
        # 回退：返回中点时间戳
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


def run_coarse_fine_screenshot_task(
    unit_id: str,
    start_sec: float,
    end_sec: float,
    coarse_shm_frames: Dict[float, dict],
    coarse_interval: float,
    fine_shm_frames_by_island: List[Dict[float, dict]] = None
) -> dict:
    """
    执行逻辑：
    1) 组织处理流程与依赖调用。
    2) 汇总中间结果并输出。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：编排流程，保证步骤顺序与可追踪性。
    决策逻辑：
    - 条件：len(coarse_frames) < 2
    - 条件：'_validator_cache' not in globals()
    - 条件：selector_key not in _validator_cache
    依据来源（证据链）：
    - 输入参数：fine_shm_frames_by_island。
    输入参数：
    - unit_id: 标识符（类型：str）。
    - start_sec: 起止时间/区间边界（类型：float）。
    - end_sec: 起止时间/区间边界（类型：float）。
    - coarse_shm_frames: 函数入参（类型：Dict[float, dict]）。
    - coarse_interval: 函数入参（类型：float）。
    - fine_shm_frames_by_island: 函数入参（类型：List[Dict[float, dict]]）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    _check_memory_usage()
    
    try:
        # 1. 从 SharedMemory 读取粗采样帧
        coarse_frames = []
        coarse_timestamps = []
        
        for ts, shm_ref in sorted(coarse_shm_frames.items()):
            frame = get_frame_from_shm(shm_ref)
            if frame is not None:
                coarse_frames.append(frame.copy())
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
        from MVP_Module2_HEANCING.module2_content_enhancement.screenshot_selector import ScreenshotSelector
        
        global _validator_cache
        if '_validator_cache' not in globals():
            globals()['_validator_cache'] = {}
        
        _validator_cache = globals()['_validator_cache']
        selector_key = "screenshot_selector_lightweight"
        
        if selector_key not in _validator_cache:
            logger.info(f"🆕 [PID={os.getpid()}] Initializing ScreenshotSelector (lightweight)")
            _validator_cache[selector_key] = ScreenshotSelector.create_lightweight()
        
        selector = _validator_cache[selector_key]
        
        # 3. Stage 1: 识别稳定岛 (纯计算)
        stable_islands = selector.detect_stable_islands_from_frames(
            frames=coarse_frames,
            timestamps=coarse_timestamps,
            interval=coarse_interval
        )
        
        if not stable_islands:
            stable_islands = [{"start_sec": start_sec, "end_sec": end_sec}]
        
        logger.info(f"Stage 1 complete for {unit_id}: {len(stable_islands)} stable islands")
        
        # 4. Stage 2: 如果提供了细采样帧，选择最佳帧
        screenshots = []
        
        if fine_shm_frames_by_island and len(fine_shm_frames_by_island) == len(stable_islands):
            for island_idx, (island, fine_shm_frames) in enumerate(zip(stable_islands, fine_shm_frames_by_island)):
                fine_frames = []
                fine_timestamps = []
                
                for ts, shm_ref in sorted(fine_shm_frames.items()):
                    frame = get_frame_from_shm(shm_ref)
                    if frame is not None:
                        fine_frames.append(frame.copy())
                        fine_timestamps.append(ts)
                
                if not fine_frames:
                    # Fallback: 使用岛中点
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
            # 未提供细采样帧，返回岛中点
            for island_idx, island in enumerate(stable_islands):
                screenshots.append({
                    "timestamp_sec": (island["start_sec"] + island["end_sec"]) / 2,
                    "island_index": island_idx,
                    "score": 0.5,
                    "island_start": island["start_sec"],
                    "island_end": island["end_sec"]
                })
        
        logger.info(
            f"✅ Coarse-Fine complete for {unit_id}: "
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
        logger.error(f"❌ Coarse-Fine screenshot failed for {unit_id}: {e}")
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
