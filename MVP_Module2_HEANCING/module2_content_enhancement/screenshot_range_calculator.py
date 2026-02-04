"""
模块说明：Module2 内容增强中的 screenshot_range_calculator 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。
补充说明：
核心逻辑：
1. 收集所有稳定岛（内部+外部+被跨越的）
2. 对每个稳定岛扩展 ±1s
3. 指数回退处理重叠（优先缩小时长更长的）
4. 对于稳定岛本身重叠的情况，只在非重叠部分各取一个截图
5. 边界裁剪到 [0, video_duration]"""

import logging
from dataclasses import dataclass
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)


@dataclass
class ScreenshotRange:
    """
    类说明：封装 ScreenshotRange 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    start_sec: float          # 扩展后的起始时间
    end_sec: float            # 扩展后的结束时间
    island_start: float       # 原始稳定岛起始
    island_end: float         # 原始稳定岛结束
    island_duration: float    # 稳定岛持续时间
    expansion: float          # 当前扩展量
    semantic_unit_id: str = ""  # 所属语义单元
    
    @property
    def mid_sec(self) -> float:
        """
        执行逻辑：
        1) 读取对象内部状态。
        2) 返回属性值。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：对外提供统一读路径，便于维护与扩展。
        输入参数：
        - 无。
        输出参数：
        - 数值型计算结果。"""
        return (self.island_start + self.island_end) / 2


class ScreenshotRangeCalculator:
    """
    类说明：封装 ScreenshotRangeCalculator 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。
    补充说明：
    负责：
    1. 扩展稳定岛范围
    2. 指数回退处理重叠
    3. 处理稳定岛本身重叠（只在非重叠部分取截图）
    4. 边界裁剪"""
    
    def __init__(self, video_duration: float, initial_expansion: float = 1.0):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - video_duration: 函数入参（类型：float）。
        - initial_expansion: 函数入参（类型：float）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.video_duration = video_duration
        self.initial_expansion = initial_expansion
    
    def calculate_ranges(
        self, 
        stable_islands: List[Tuple[float, float, str]]  # [(start, end, semantic_unit_id), ...]
    ) -> List[ScreenshotRange]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not stable_islands
        依据来源（证据链）：
        - 输入参数：stable_islands。
        输入参数：
        - stable_islands: 函数入参（类型：List[Tuple[float, float, str]]）。
        输出参数：
        - ScreenshotRange 列表（与输入或处理结果一一对应）。"""
        if not stable_islands:
            return []
        
        # 1. 初始化范围（扩展 ±initial_expansion）
        ranges = []
        for start, end, unit_id in stable_islands:
            duration = end - start
            expanded_start = max(0, start - self.initial_expansion)
            expanded_end = min(self.video_duration, end + self.initial_expansion)
            
            ranges.append(ScreenshotRange(
                start_sec=expanded_start,
                end_sec=expanded_end,
                island_start=start,
                island_end=end,
                island_duration=duration,
                expansion=self.initial_expansion,
                semantic_unit_id=unit_id
            ))
        
        # 2. 按时间排序
        ranges.sort(key=lambda x: x.start_sec)
        
        # 3. 指数回退处理重叠
        self._resolve_overlaps(ranges)
        
        logger.info(f"Calculated {len(ranges)} screenshot ranges from {len(stable_islands)} stable islands")
        
        return ranges
    
    def _resolve_overlaps(self, ranges: List[ScreenshotRange]) -> None:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not overlap_found
        - 条件：r1.end_sec > r2.start_sec
        - 条件：r1.expansion < MIN_EXPANSION and r2.expansion < MIN_EXPANSION
        依据来源（证据链）：
        - 阈值常量：MIN_EXPANSION。
        输入参数：
        - ranges: 函数入参（类型：List[ScreenshotRange]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。
        补充说明：
        策略：
        1. 指数回退：优先缩小时长更长的稳定岛
        2. 如果扩展为 0 仍重叠（稳定岛本身重叠）：只在非重叠部分各取一个截图"""
        MAX_ITERATIONS = 10  # 防止无限循环
        MIN_EXPANSION = 0.01  # 最小扩展量
        
        for iteration in range(MAX_ITERATIONS):
            overlap_found = False
            
            for i in range(len(ranges) - 1):
                r1, r2 = ranges[i], ranges[i + 1]
                
                # 检查是否重叠
                if r1.end_sec > r2.start_sec:
                    overlap_found = True
                    
                    # 检查是否是稳定岛本身重叠（扩展已经接近 0）
                    if r1.expansion < MIN_EXPANSION and r2.expansion < MIN_EXPANSION:
                        # 稳定岛本身重叠，调整为非重叠部分
                        self._adjust_to_non_overlapping(r1, r2)
                        continue
                    
                    # 优先缩小时长更长的
                    if r1.island_duration > r2.island_duration:
                        target = r1
                    else:
                        target = r2
                    
                    # 指数回退
                    target.expansion /= 2
                    
                    # 重新计算范围
                    target.start_sec = max(0, target.island_start - target.expansion)
                    target.end_sec = min(self.video_duration, target.island_end + target.expansion)
                    
                    logger.debug(f"Overlap resolved: shrinking island [{target.island_start:.1f}s-{target.island_end:.1f}s] "
                               f"expansion={target.expansion:.3f}s")
            
            if not overlap_found:
                break
    
    def _adjust_to_non_overlapping(self, r1: ScreenshotRange, r2: ScreenshotRange) -> None:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：r1.island_start < overlap_start
        - 条件：r2.island_end > overlap_end
        依据来源（证据链）：
        - 输入参数：r1, r2。
        输入参数：
        - r1: 函数入参（类型：ScreenshotRange）。
        - r2: 函数入参（类型：ScreenshotRange）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        overlap_start = max(r1.island_start, r2.island_start)
        overlap_end = min(r1.island_end, r2.island_end)
        
        # r1 只覆盖到重叠开始点
        if r1.island_start < overlap_start:
            r1.end_sec = overlap_start
        else:
            # r1 完全在重叠区域内，缩小到最小
            r1.end_sec = r1.island_start + 0.1
        
        # r2 只从重叠结束点开始
        if r2.island_end > overlap_end:
            r2.start_sec = overlap_end
        else:
            # r2 完全在重叠区域内，缩小到最小
            r2.start_sec = r2.island_end - 0.1
        
        logger.debug(f"Adjusted non-overlapping: r1=[{r1.start_sec:.1f}s-{r1.end_sec:.1f}s], "
                    f"r2=[{r2.start_sec:.1f}s-{r2.end_sec:.1f}s]")
    
    def get_screenshot_timestamps(
        self, 
        ranges: List[ScreenshotRange],
        strategy: str = "mid"
    ) -> List[Tuple[float, str]]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：strategy == 'mid'
        - 条件：strategy == 'island_mid'
        - 条件：strategy == 'start'
        依据来源（证据链）：
        - 输入参数：strategy。
        输入参数：
        - ranges: 函数入参（类型：List[ScreenshotRange]）。
        - strategy: 函数入参（类型：str）。
        输出参数：
        - Tuple[float, str] 列表（与输入或处理结果一一对应）。"""
        timestamps = []
        
        for r in ranges:
            if strategy == "mid":
                ts = (r.start_sec + r.end_sec) / 2
            elif strategy == "island_mid":
                ts = r.mid_sec
            elif strategy == "start":
                ts = r.start_sec
            else:
                ts = r.mid_sec
            
            # 确保在有效范围内
            ts = max(0, min(ts, self.video_duration))
            timestamps.append((ts, r.semantic_unit_id))
        
        return timestamps
