import os
import psutil
from typing import Dict, Any


class ResourceOrchestrator:
    """
    V6 Adaptive Resource Orchestrator
    动态监控系统资源并建议并发/缓存参数，以平衡性能与稳定性。
    """
    
    @staticmethod
    def get_system_status() -> Dict[str, Any]:
        """获取当前系统资源状态"""
        vm = psutil.virtual_memory()
        return {
            "percent": vm.percent,
            "available_gb": vm.available / (1024**3),
            "total_gb": vm.total / (1024**3),
            "cpu_count": os.cpu_count() or 4
        }

    @staticmethod
    def get_adaptive_cache_size(base_size: int = 50, per_gb_increment: int = 25) -> int:
        """
        根据可用内存调整缓存大小。
        """
        status = ResourceOrchestrator.get_system_status()
        available_gb = status["available_gb"]
        
        # 每有 1GB 剩余内存，增加 25 帧缓存，上限 1000
        increment = int(available_gb * per_gb_increment)
        return min(base_size + increment, 1000)

