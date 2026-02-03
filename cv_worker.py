"""
CV Validation Worker Module - 独立进程执行的 CV 验证函数

该模块被 ProcessPoolExecutor 的 Worker 进程导入和执行，
与主进程完全隔离，绕过 GIL 实现真正的 CPU 并行。

🚀 V2: 支持共享内存零拷贝帧传递
- 主进程预读帧并存入 SharedMemory
- Worker 通过 shm_name 直接访问，无需序列化/反序列化

设计原则:
1. 每个 Worker 进程有独立的 VideoCapture (作为 fallback)
2. 优先使用共享内存访问预读帧
3. Worker 内部使用全局缓存避免重复初始化
"""

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
    Worker 进程初始化函数 - ProcessPoolExecutor 的 initializer
    
    在每个子进程启动时调用一次
    """
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
    """Manual memory check for environments without 'resource' module (e.g., Windows)"""
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
    从共享内存获取帧 (零拷贝)
    
    Args:
        shm_ref: {"shm_name": str, "shape": tuple, "dtype": str}
    
    Returns:
        numpy 数组 (共享内存视图) 或 None
    """
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
    单个语义单元的 CV 验证 - 在 Worker 进程中执行
    
    🚀 V2: 支持共享内存帧传递
    
    Args:
        video_path: 视频文件路径
        unit_data: {"unit_id", "start_sec", "end_sec", "knowledge_type"}
        shm_frames: 可选，共享内存帧引用字典 {frame_idx: shm_ref}
    
    Returns:
        验证结果字典
    """
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
        
        # 确定模态
        if not action_units and stable_islands:
            modality = "screenshot"
            knowledge_subtype = "static"
        elif action_units:
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
        
        logger.info(f"✅ CV validation done for {unit_data['unit_id']} "
                   f"(stable={len(stable_islands)}, action={len(action_units)})")
        
        return {
            "unit_id": unit_data["unit_id"],
            "modality": modality,
            "knowledge_subtype": knowledge_subtype,
            "stable_islands": stable_islands_data,
            "action_segments": action_segments_data
        }
        
    except Exception as e:
        import traceback
        logger.error(f"❌ CV validation failed for {unit_data['unit_id']}: {e}")
        logger.error(traceback.format_exc())
        return {
            "unit_id": unit_data["unit_id"],
            "modality": "unknown",
            "knowledge_subtype": "unknown",
            "stable_islands": [],
            "action_segments": [],
            "error": str(e)
        }


def cleanup_worker_resources():
    """清理 Worker 进程内的资源"""
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

