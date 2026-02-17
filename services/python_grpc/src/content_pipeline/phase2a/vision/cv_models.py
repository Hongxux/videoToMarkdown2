"""CV ????????????"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from collections import OrderedDict
from typing import List, Dict, Optional, Tuple

import numpy as np

from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics

class CVConfig:
    """类说明：CVConfig 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    # 稳定岛/动作单元检测阈值
    TH_SSIM_STABLE = 0.9           # 稳定岛SSIM阈值
    TH_STABLE_DURATION_MS = 500    # 稳定岛最小持续时长(ms)
    TH_DIFF_RATIO = 0.05           # 动作单元diff_ratio阈值
    TH_ACTION_DURATION_MS = 300    # 动作单元最小持续时长(ms)
    TH_ACTION_MERGE_GAP_SEC = 1.0  # 💥 动作单元合并阈值: 间隔<1s则合并
    
    # 主视觉类型判定阈值 (三级阶梯)
    TH_ABSOLUTE_LEAD = 0.6         # 绝对主导
    TH_RELATIVE_LEAD = 0.4         # 相对主导下限
    TH_RELATIVE_DIFF = 0.15        # 相对最小差值
    
    # 联合裁决阈值
    TH_ABS_CON_SUM = 0.7           # 抽象+具象之和
    TH_STABLE_RATIO = 0.5          # 稳定岛占比
    TH_ACTION_RATIO = 0.4          # 动作单元占比
    
    # 多级采样帧率
    FPS_ROI_DETECTION = 1.0        # ROI检测: 1fps
    FPS_STATE_DETECTION = 5.0      # 状态判定: 5fps
    FPS_BOUNDARY_REFINE = 10.0     # 边界精修: 10fps
    BOUNDARY_EXTEND_MS = 500       # 边界扩展范围(ms)
    
    # 性能优化配置
    ROI_CACHE_SIZE = 3             # 缓存前N个单元的ROI
    ROI_REUSE_SIM_THRESH = 0.9     # 视觉相似度≥0.9则复用ROI
    ROI_REUSE_FAIL_THRESH = 0.5    # ROI内有效特征占比<50%则回退
    
    FRAME_FEAT_CACHE_FRAMES = 3    # 缓存末尾N帧特征
    FEAT_ALIGN_OFFSET_THRESH = 0.1 # ROI偏移≤10%则对齐复用
    
    # 轻量冗余初筛阈值 (放宽以避免误判)
    RED_LOW_VAR_THRESH = 30        # 亮度方差≤30为空白帧 (纯黑/纯白)
    RED_EDGE_RATIO_THRESH = 0.01   # 边缘占比≤1%为无意义帧 (极简洁画面)
    RED_VALID_PIXEL_THRESH = 0.05  # ROI有效像素≤5%为冗余 (极单调)
    
    # 状态判定轻量校验
    STABLE_LIGHT_CHECK_AREAS = 5   # 稳定状态轻量校验的局部区域数
    ACTION_TRACK_CENTROID_THRESH = 5  # 动作状态重心偏移阈值(像素)
    
    # 动态采样率
    SAMPLE_FPS_LOW = 3.0           # 低复杂度采样率
    COMPLEX_LOW_STABLE_RATIO = 0.8 # 低复杂度判定: 稳定岛≥80%
    
    # ========== V6.9.4 边缘差分累积检测 (检测平移类动画) ==========
    # 死区触发条件: 仅在传统MSE/SSIM失效时启动边缘检测
    EDGE_DETECT_TRIGGER_MSE = 10.0     # MSE < 10 时触发边缘检测
    EDGE_DETECT_TRIGGER_SSIM_DROP = 0.05  # SSIM跌幅 < 5% 时触发
    
    # 边缘差分累积阈值
    TH_EDGE_DIFF_CUMULATIVE = 50.0     # 累积能量阈值
    TH_EDGE_DIFF_VARIANCE = 100.0      # 方差阈值(区分随机噪点和规律平移)
    TH_EDGE_DIFF_MIN_FRAMES = 3        # 最小累积帧数
    
    # MSE+SSIM双特征判定容器切换
    TH_SSIM_DROP_TRANSITION = 0.5      # SSIM跌幅>50%为容器切换
    TH_SSIM_DROP_KNOWLEDGE = 0.2       # SSIM跌幅<20%可能是知识生产
    
    # 知识生产型动态的绝对时长保底
    TH_KNOWLEDGE_MIN_DURATION_MS = 1500  # 知识生产型≥1.5s
    TH_TRANSITION_MAX_DURATION_MS = 1500 # 容器切换型<1.5s
    
    # ========== V8.0 智能干扰过滤 (鼠标/人物) ==========
    # 鼠标过滤: 形态学开运算 + 最小面积 (可靠，默认启用)
    MOTION_FILTER_ENABLED = True           # 启用干扰过滤
    MOTION_MORPH_KERNEL_SIZE = 5           # 形态学核大小 (3-7, 越大过滤越强)
    MOTION_MIN_AREA_RATIO = 0.005          # 最小变化区域占比 (0.5% = 忽略鼠标)
    
    # 人物过滤: 固定ROI排除模式 (用户配置，避免误判)
    # 格式: [(x1_ratio, y1_ratio, x2_ratio, y2_ratio), ...] 相对于ROI的比例坐标
    # 例: [(0.7, 0.6, 1.0, 1.0)] 表示排除右下角30%x40%区域
    PERSON_EXCLUDE_ROIS = []               # 默认不排除任何区域
    # PERSON_EXCLUDE_ROIS = [(0.7, 0.6, 1.0, 1.0)]  # 示例: 排除右下角 (Talking Head)
    
    # 启发式人物过滤 (不可靠，默认关闭)
    PERSON_HEURISTIC_ENABLED = False       # 关闭启发式边缘检测


# =============================================================================
# Data Structures
# =============================================================================

class VisualKnowledgeType(Enum):
    """类说明：VisualKnowledgeType 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    ABSTRACT = "abstract"    # 抽象: 纯人像/文字展示
    CONCRETE = "concrete"    # 具象: 静态图表/界面
    PROCESS = "process"      # 过程: 动态操作/动画
    MIXED = "mixed"          # 混杂: 无法判定


class RedundancyType(Enum):
    """类说明：RedundancyType 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    RED_TRANSITION = "transition"     # 转场冗余
    RED_IRRELEVANT = "irrelevant"     # 无关动作冗余
    RED_BLANK = "blank"               # 空白画面冗余
    RED_DECOR = "decor"               # 装饰动态冗余
    RED_OBSTACLE = "obstacle"         # 遮挡干扰冗余


class FrameState(Enum):
    """类说明：FrameState 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    STABLE = "stable"         # 稳定岛
    ACTION = "action"         # 动作单元
    REDUNDANT = "redundant"   # 冗余


@dataclass
class StableIsland:
    """类说明：StableIsland 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    start_sec: float
    end_sec: float
    avg_ssim: float
    
    @property
    def duration_ms(self) -> float:
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
        return (self.end_sec - self.start_sec) * 1000


class Modality(Enum):
    """类说明：Modality 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    DISCARD = "discard"           # 剔除，不生成素材
    SCREENSHOT = "screenshot"     # 纯截图 (K1/K2/呈现型)
    PRESENTATION = "presentation" # V7.2: 呈现型动态 (淡入/渐显) → 单张稳定截图
    VIDEO_SCREENSHOT = "video_screenshot"  # 视频+关键截图 (K3)
    VIDEO_ONLY = "video_only"     # 纯视频 (K4)


@dataclass
class ActionUnit:
    """类说明：ActionUnit 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    start_sec: float
    end_sec: float
    avg_diff_ratio: float
    action_type: str = "unknown"        # knowledge/transition/noise/mixed
    ssim_drop: float = 0.0              # SSIM跌幅 (用于分类)
    is_effective: bool = True           # 是否为有效动态
    # V7.0 新增
    knowledge_subtype: str = "unknown"  # K1/K2/K3/K4 (仅knowledge类型有效)
    modality: str = "discard"           # 最终模态: screenshot/video_screenshot/video_only/discard
    has_internal_stable: bool = False   # 是否有内部稳定岛 (用于K1/K2判定)
    # V9.0 新增（两阶段合并 + LLM分类）
    knowledge_type: str = ""            # LLM 分类结果: 过程性知识/实操/推演/讲解型
    confidence: float = 0.0             # LLM 分类置信度
    internal_stable_islands: List['StableIsland'] = field(default_factory=list)  # 内部稳定岛列表

    
    @property
    def duration_ms(self) -> float:
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
        return (self.end_sec - self.start_sec) * 1000
    
    def classify(self) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.ssim_drop > 0.5
        - 条件：duration_s >= 1.5 and self.ssim_drop < 0.2
        - 条件：duration_s < 0.3
        依据来源（证据链）：
        - 对象内部状态：self.ssim_drop。
        输入参数：
        - 无。
        输出参数：
        - 字符串结果。"""
        duration_s = self.duration_ms / 1000
        
        # 优先判断: SSIM大跌 = 容器切换 (场景突变)
        if self.ssim_drop > 0.5:
            return "transition"   # 容器切换型
        
        # 知识生产型: 结构稳定 + 足够时长
        if duration_s >= 1.5 and self.ssim_drop < 0.2:
            return "knowledge"    # 知识生产型
        
        # 噪点: 时长过短
        if duration_s < 0.3:
            return "noise"        # 无效变动
        
        return "mixed"            # 混合/待定
    
    def classify_modality(self, has_internal_stable: bool = False, 
                          is_continuous_derivation: bool = False,
                          is_continuous_operation: bool = False) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：action_type in ('transition', 'noise')
        - 条件：action_type == 'mixed'
        - 条件：action_type == 'knowledge'
        依据来源（证据链）：
        - 输入参数：has_internal_stable, is_continuous_derivation, is_continuous_operation。
        输入参数：
        - has_internal_stable: 函数入参（类型：bool）。
        - is_continuous_derivation: 开关/状态（类型：bool）。
        - is_continuous_operation: 开关/状态（类型：bool）。
        输出参数：
        - 字符串结果。"""
        action_type = self.classify()
        
        # 无效动态: 剔除
        if action_type in ("transition", "noise"):
            self.knowledge_subtype = "invalid"
            return Modality.DISCARD.value
        
        # mixed类型: 保守用截图
        if action_type == "mixed":
            self.knowledge_subtype = "mixed"
            return Modality.SCREENSHOT.value
        
        # 知识生产型: 进一步子分类
        if action_type == "knowledge":
            self.has_internal_stable = has_internal_stable
            
            if has_internal_stable:
                # K1/K2: 有内部稳定岛 → 可静态化 → 纯截图
                self.knowledge_subtype = "K1_K2_stepwise"
                return Modality.SCREENSHOT.value
            elif is_continuous_derivation:
                # K3: 连续推演 → 视频+关键截图
                self.knowledge_subtype = "K3_derivation"
                return Modality.VIDEO_SCREENSHOT.value
            elif is_continuous_operation:
                # K4: 连续操作 → 纯视频
                self.knowledge_subtype = "K4_operation"
                return Modality.VIDEO_ONLY.value
            else:
                # 无法归类的knowledge → 默认截图
                self.knowledge_subtype = "K_unknown"
                return Modality.SCREENSHOT.value
        
        return Modality.DISCARD.value



@dataclass
class RedundancySegment:
    """类说明：RedundancySegment 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    start_sec: float
    end_sec: float
    redundancy_type: RedundancyType
    processing_action: str  # 下游处理动作
    
    @property
    def duration_ms(self) -> float:
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
        return (self.end_sec - self.start_sec) * 1000


@dataclass
class VisionStats:
    """类说明：VisionStats 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    s_stable: float = 0.0      # 稳定岛占比
    s_action: float = 0.0      # 动作单元占比
    s_redundant: float = 0.0   # 冗余占比
    
    p_abstract: float = 0.0    # 抽象视觉占比
    p_concrete: float = 0.0    # 具象视觉占比
    p_process: float = 0.0     # 过程视觉占比


@dataclass
class CVValidationResult:
    """类说明：CVValidationResult 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    unit_id: str
    timeline: Tuple[float, float]
    
    # 视觉统计
    vision_stats: VisionStats
    
    # 主视觉类型判定
    main_vision_type: VisualKnowledgeType
    
    # 检测结果
    stable_islands: List[StableIsland] = field(default_factory=list)
    action_units: List[ActionUnit] = field(default_factory=list)
    redundancy_segments: List[RedundancySegment] = field(default_factory=list)
    
    # 视觉锚点
    vision_anchors: List[float] = field(default_factory=list)
    
    # 校验状态
    timeline_continuous: bool = True
    type_match: bool = True
    vision_unit_complete: bool = True
    
    # 异常信息
    abnormal_type: Optional[str] = None
    abnormal_timeline: Optional[Tuple[float, float]] = None
    abnormal_reason: Optional[str] = None
    
    @property
    def is_normal(self) -> bool:
        """
        执行逻辑：
        1) 读取对象内部状态。
        2) 返回属性值。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：对外提供统一读路径，便于维护与扩展。
        输入参数：
        - 无。
        输出参数：
        - 是否满足条件的判定结果（布尔标记）。"""
        return self.timeline_continuous and self.type_match and self.vision_unit_complete


@dataclass
class ConflictPackage:
    """类说明：ConflictPackage 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    conflict_unit_id: str
    conflict_timeline: Tuple[float, float]
    vision_anchors: List[float]
    vision_anchor_types: List[str]
    conflict_reason: str
    vision_valid_timelines: List[Tuple[float, float]]


# =============================================================================
# Performance Optimization: Caches
# =============================================================================

class ROICache:
    """类说明：ROICache 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, max_size: int = CVConfig.ROI_CACHE_SIZE):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - max_size: 函数入参（类型：int）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.cache: OrderedDict = OrderedDict()
        self.max_size = max_size
    
    def get(self, unit_end_sec: float) -> Optional[Tuple[int, int, int, int]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：abs(end_sec - unit_end_sec) < 10
        依据来源（证据链）：
        - 输入参数：unit_end_sec。
        输入参数：
        - unit_end_sec: 起止时间/区间边界（类型：float）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
        for end_sec, data in reversed(self.cache.items()):
            if abs(end_sec - unit_end_sec) < 10:  # 10秒内
                cache_metrics.hit("module2.cv_validator.roi_cache")
                return data['roi']
        cache_metrics.miss("module2.cv_validator.roi_cache")
        return None
    
    def put(self, unit_end_sec: float, roi: Tuple[int, int, int, int], 
            layout_feature: float, confidence: float):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(self.cache) >= self.max_size
        依据来源（证据链）：
        - 对象内部状态：self.cache, self.max_size。
        输入参数：
        - unit_end_sec: 起止时间/区间边界（类型：float）。
        - roi: 函数入参（类型：Tuple[int, int, int, int]）。
        - layout_feature: 函数入参（类型：float）。
        - confidence: 函数入参（类型：float）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        self.cache[unit_end_sec] = {
            'roi': roi,
            'layout_feature': layout_feature,
            'confidence': confidence
        }
    
    def get_last_layout_feature(self) -> Optional[float]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：self.cache
        依据来源（证据链）：
        - 对象内部状态：self.cache。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        if self.cache:
            return list(self.cache.values())[-1]['layout_feature']
        return None


class FrameFeatureCache:
    """类说明：FrameFeatureCache 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, max_frames: int = CVConfig.FRAME_FEAT_CACHE_FRAMES):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - max_frames: 函数入参（类型：int）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.cache: OrderedDict = OrderedDict()
        self.max_frames = max_frames
    
    def get(self, timestamp: float) -> Optional[Dict]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：abs(ts - timestamp) < 0.1
        依据来源（证据链）：
        - 输入参数：timestamp。
        输入参数：
        - timestamp: 函数入参（类型：float）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        for ts, feat in self.cache.items():
            if abs(ts - timestamp) < 0.1:  # 100ms容差
                cache_metrics.hit("module2.cv_validator.frame_feature_cache")
                return feat
        cache_metrics.miss("module2.cv_validator.frame_feature_cache")
        return None
    
    def put(self, timestamp: float, gray_roi: np.ndarray, 
            ssim_base: Optional[np.ndarray], diff_prev: float):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(self.cache) >= self.max_frames
        - 条件：ssim_base is not None
        依据来源（证据链）：
        - 输入参数：ssim_base。
        - 对象内部状态：self.cache, self.max_frames。
        输入参数：
        - timestamp: 函数入参（类型：float）。
        - gray_roi: 函数入参（类型：np.ndarray）。
        - ssim_base: 函数入参（类型：Optional[np.ndarray]）。
        - diff_prev: 函数入参（类型：float）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if len(self.cache) >= self.max_frames:
            self.cache.popitem(last=False)
        self.cache[timestamp] = {
            'gray_roi': gray_roi.copy(),
            'ssim_base': ssim_base.copy() if ssim_base is not None else None,
            'diff_prev': diff_prev
        }
    
