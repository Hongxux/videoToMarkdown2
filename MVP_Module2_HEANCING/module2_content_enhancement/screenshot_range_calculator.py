"""
Screenshot Range Calculator - V9.0

负责计算截图提取范围，处理稳定岛的扩展和重叠解决。

核心逻辑：
1. 收集所有稳定岛（内部+外部+被跨越的）
2. 对每个稳定岛扩展 ±1s
3. 指数回退处理重叠（优先缩小时长更长的）
4. 对于稳定岛本身重叠的情况，只在非重叠部分各取一个截图
5. 边界裁剪到 [0, video_duration]
"""

import logging
from dataclasses import dataclass
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)


@dataclass
class ScreenshotRange:
    """截图提取范围"""
    start_sec: float          # 扩展后的起始时间
    end_sec: float            # 扩展后的结束时间
    island_start: float       # 原始稳定岛起始
    island_end: float         # 原始稳定岛结束
    island_duration: float    # 稳定岛持续时间
    expansion: float          # 当前扩展量
    semantic_unit_id: str = ""  # 所属语义单元
    
    @property
    def mid_sec(self) -> float:
        """稳定岛中点"""
        return (self.island_start + self.island_end) / 2


class ScreenshotRangeCalculator:
    """
    截图范围计算器
    
    负责：
    1. 扩展稳定岛范围
    2. 指数回退处理重叠
    3. 处理稳定岛本身重叠（只在非重叠部分取截图）
    4. 边界裁剪
    """
    
    def __init__(self, video_duration: float, initial_expansion: float = 1.0):
        """
        Args:
            video_duration: 视频总时长（秒）
            initial_expansion: 初始扩展量（秒），默认 1.0
        """
        self.video_duration = video_duration
        self.initial_expansion = initial_expansion
    
    def calculate_ranges(
        self, 
        stable_islands: List[Tuple[float, float, str]]  # [(start, end, semantic_unit_id), ...]
    ) -> List[ScreenshotRange]:
        """
        计算截图提取范围
        
        Args:
            stable_islands: 稳定岛列表 [(start, end, semantic_unit_id), ...]
            
        Returns:
            List[ScreenshotRange]: 处理后的截图范围列表
        """
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
        处理重叠：指数回退 + 非重叠部分提取
        
        策略：
        1. 指数回退：优先缩小时长更长的稳定岛
        2. 如果扩展为 0 仍重叠（稳定岛本身重叠）：只在非重叠部分各取一个截图
        """
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
        调整两个重叠的稳定岛，使其只在非重叠部分提取截图
        
        策略：r1 的范围结束于重叠开始点，r2 的范围开始于重叠结束点
        """
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
        从范围中获取截图时间戳
        
        Args:
            ranges: 截图范围列表
            strategy: 时间戳选择策略
                - "mid": 取范围中点（默认）
                - "island_mid": 取原始稳定岛中点
                - "start": 取范围起点
                
        Returns:
            List[(timestamp, semantic_unit_id)]
        """
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
