"""
模块说明：Module2 内容增强中的 resource_utils 模块。
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
import psutil
import logging
import asyncio
from typing import Dict, Any

logger = logging.getLogger(__name__)


class ResourceOrchestrator:
    """
    类说明：封装 ResourceOrchestrator 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    @staticmethod
    def get_system_status() -> Dict[str, Any]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        vm = psutil.virtual_memory()
        return {
            "percent": vm.percent,
            "available_gb": vm.available / (1024**3),
            "total_gb": vm.total / (1024**3),
            "cpu_count": os.cpu_count() or 4
        }

    @staticmethod
    def get_adaptive_concurrency(base_multiplier: float = 1.0, cap: int = 16) -> int:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：available_gb < 2.0 or percent > 90
        - 条件：available_gb < 6.0 or percent > 75
        依据来源（证据链）：
        输入参数：
        - base_multiplier: 函数入参（类型：float）。
        - cap: 函数入参（类型：int）。
        输出参数：
        - 数值型计算结果。"""
        status = ResourceOrchestrator.get_system_status()
        available_gb = status["available_gb"]
        percent = status["percent"]
        cpu_count = status["cpu_count"]

        # 1. ????????? (?????????????????? > 90%)
        if available_gb < 2.0 or percent > 90:
            logger.warning(f"?? [OOM Risk]: Available RAM {available_gb:.1f}GB ({percent}%). Enforcing minimal concurrency (1).")
            return 1
            
        # 2. ??????????(?????? 2-6GB ?????? 75-90%)
        if available_gb < 6.0 or percent > 75:
            logger.info(f"??? [Resource Constrained]: Available RAM {available_gb:.1f}GB. Limiting concurrency.")
            return min(2, cpu_count)

        # 3. ?????????
        # ?????? = CPU * 1.5 * (???GB/8GB)
        suggested = int(cpu_count * base_multiplier * min(2.0, available_gb / 8.0))
        final_concurrency = max(2, min(suggested, cap))
        
        logger.info(f"?? [Performance Advisory]: Suggested concurrency {final_concurrency} (RAM {percent}%, {available_gb:.1f}GB free)")
        return final_concurrency

    @staticmethod
    def get_adaptive_cache_size(base_size: int = 50, per_gb_increment: int = 25) -> int:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - base_size: 函数入参（类型：int）。
        - per_gb_increment: 函数入参（类型：int）。
        输出参数：
        - 数值型计算结果。"""
        status = ResourceOrchestrator.get_system_status()
        available_gb = status["available_gb"]
        
        # ??? 1GB ???????????25 ????????? 1000
        increment = int(available_gb * per_gb_increment)
        return min(base_size + increment, 1000)

class AdaptiveSemaphore:
    """
    类说明：封装 AdaptiveSemaphore 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    def __init__(self, base_cap: int = 4):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - base_cap: 函数入参（类型：int）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.base_cap = base_cap
        self._sem = asyncio.Semaphore(base_cap)
        self._current_limit = base_cap

    async def __aenter__(self):
        # ??????????????????????????????????????????
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        await self._sem.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - exc_type: 函数入参（类型：未标注）。
        - exc: 函数入参（类型：未标注）。
        - tb: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self._sem.release()

    @property
    def current_limit(self):
        """
        执行逻辑：
        1) 读取对象内部状态。
        2) 返回属性值。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：对外提供统一读路径，便于维护与扩展。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        return self._current_limit
