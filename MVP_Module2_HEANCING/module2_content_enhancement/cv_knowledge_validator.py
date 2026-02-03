"""
CV Knowledge Validator - LLM+CV Collaborative Semantic Unit Validation

基于第一性原理的视觉知识验证模块 v2.1：
- 稳定岛检测 (ROI内SSIM≥0.9, ≥500ms)
- 动作单元检测 (ROI内diff_ratio>0.05, ≥300ms)  
- 视觉冗余5类细分 (转场/无关动作/空白/装饰/遮挡)
- 视觉知识类型识别 (抽象/具象/过程)
- 跨模态一致性校验

性能优化:
- ROI增量复用缓存
- 帧特征增量缓存
- 轻量冗余初筛
- 状态判定轻量校验
- 动态采样率适配
- 批量增量并行处理

多级采样策略:
- 1fps: ROI检测
- 5fps: 状态判定
- 10fps: 边界精修
"""

import cv2
import numpy as np
import logging
import os
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
from collections import OrderedDict
import time
from .resource_manager import get_resource_manager
from .fast_metrics import fast_ssim, fast_diff_ratio, fast_mse

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration Constants
# =============================================================================

class CVConfig:
    """CV Knowledge Validator 配置常量"""
    
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
    """视觉知识类型"""
    ABSTRACT = "abstract"    # 抽象: 纯人像/文字展示
    CONCRETE = "concrete"    # 具象: 静态图表/界面
    PROCESS = "process"      # 过程: 动态操作/动画
    MIXED = "mixed"          # 混杂: 无法判定


class RedundancyType(Enum):
    """视觉冗余类型"""
    RED_TRANSITION = "transition"     # 转场冗余
    RED_IRRELEVANT = "irrelevant"     # 无关动作冗余
    RED_BLANK = "blank"               # 空白画面冗余
    RED_DECOR = "decor"               # 装饰动态冗余
    RED_OBSTACLE = "obstacle"         # 遮挡干扰冗余


class FrameState(Enum):
    """帧状态"""
    STABLE = "stable"         # 稳定岛
    ACTION = "action"         # 动作单元
    REDUNDANT = "redundant"   # 冗余


@dataclass
class StableIsland:
    """稳定岛"""
    start_sec: float
    end_sec: float
    avg_ssim: float
    
    @property
    def duration_ms(self) -> float:
        return (self.end_sec - self.start_sec) * 1000


class Modality(Enum):
    """素材模态类型"""
    DISCARD = "discard"           # 剔除，不生成素材
    SCREENSHOT = "screenshot"     # 纯截图 (K1/K2/呈现型)
    PRESENTATION = "presentation" # V7.2: 呈现型动态 (淡入/渐显) → 单张稳定截图
    VIDEO_SCREENSHOT = "video_screenshot"  # 视频+关键截图 (K3)
    VIDEO_ONLY = "video_only"     # 纯视频 (K4)


@dataclass
class ActionUnit:
    """
    动作单元 (V9.0 - 两阶段合并 + LLM分类)
    
    分类层级:
    1. 有效性分类: knowledge/transition/noise/mixed
    2. LLM 知识分类: 过程性知识/实操/推演/讲解型
    3. 模态子分类 (仅针对knowledge): K1/K2→截图, K3→视频+截图, K4→视频
    """
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
        return (self.end_sec - self.start_sec) * 1000
    
    def classify(self) -> str:
        """
        动态类型分类 (第一性原理 V6.9.5)
        
        分类优先级:
        1. SSIM跌幅>50% → 容器切换型 (不论时长)
        2. SSIM跌幅<20% + 时长≥1.5s → 知识生产型
        3. 时长<0.3s → 噪点
        4. 其他 → 混合/待定
        """
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
        模态分类 (V7.0 - 知识生产型内部子分类)
        
        决策树:
        - 无效动态 (transition/noise) → DISCARD
        - 有内部稳定岛 (K1/K2) → SCREENSHOT
        - 连续推演 (K3) → VIDEO_SCREENSHOT
        - 连续操作 (K4) → VIDEO_ONLY
        - mixed → SCREENSHOT (保守策略)
        """
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
    """冗余区间"""
    start_sec: float
    end_sec: float
    redundancy_type: RedundancyType
    processing_action: str  # 下游处理动作
    
    @property
    def duration_ms(self) -> float:
        return (self.end_sec - self.start_sec) * 1000


@dataclass
class VisionStats:
    """视觉统计"""
    s_stable: float = 0.0      # 稳定岛占比
    s_action: float = 0.0      # 动作单元占比
    s_redundant: float = 0.0   # 冗余占比
    
    p_abstract: float = 0.0    # 抽象视觉占比
    p_concrete: float = 0.0    # 具象视觉占比
    p_process: float = 0.0     # 过程视觉占比


@dataclass
class CVValidationResult:
    """CV校验结果"""
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
        return self.timeline_continuous and self.type_match and self.vision_unit_complete


@dataclass
class ConflictPackage:
    """冲突包 (用于LLM重判)"""
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
    """ROI增量复用缓存 (措施1)"""
    
    def __init__(self, max_size: int = CVConfig.ROI_CACHE_SIZE):
        self.cache: OrderedDict = OrderedDict()
        self.max_size = max_size
    
    def get(self, unit_end_sec: float) -> Optional[Tuple[int, int, int, int]]:
        """获取最近的ROI"""
        for end_sec, data in reversed(self.cache.items()):
            if abs(end_sec - unit_end_sec) < 10:  # 10秒内
                return data['roi']
        return None
    
    def put(self, unit_end_sec: float, roi: Tuple[int, int, int, int], 
            layout_feature: float, confidence: float):
        """缓存ROI"""
        if len(self.cache) >= self.max_size:
            self.cache.popitem(last=False)
        self.cache[unit_end_sec] = {
            'roi': roi,
            'layout_feature': layout_feature,
            'confidence': confidence
        }
    
    def get_last_layout_feature(self) -> Optional[float]:
        """获取上一个单元的布局特征"""
        if self.cache:
            return list(self.cache.values())[-1]['layout_feature']
        return None


class FrameFeatureCache:
    """帧特征增量缓存 (措施2)"""
    
    def __init__(self, max_frames: int = CVConfig.FRAME_FEAT_CACHE_FRAMES):
        self.cache: OrderedDict = OrderedDict()
        self.max_frames = max_frames
    
    def get(self, timestamp: float) -> Optional[Dict]:
        """获取缓存的帧特征"""
        for ts, feat in self.cache.items():
            if abs(ts - timestamp) < 0.1:  # 100ms容差
                return feat
        return None
    
    def put(self, timestamp: float, gray_roi: np.ndarray, 
            ssim_base: Optional[np.ndarray], diff_prev: float):
        """缓存帧特征"""
        if len(self.cache) >= self.max_frames:
            self.cache.popitem(last=False)
        self.cache[timestamp] = {
            'gray_roi': gray_roi.copy(),
            'ssim_base': ssim_base.copy() if ssim_base is not None else None,
            'diff_prev': diff_prev
        }
    


# =============================================================================
# Core Implementation
# =============================================================================

class CVKnowledgeValidator:
    """
    CV Knowledge Validator - LLM+CV协同校验器
    
    核心功能:
    1. 稳定岛检测 (ROI内SSIM≥0.9, ≥500ms)
    2. 动作单元检测 (ROI内diff_ratio>0.05, ≥300ms)
    3. 视觉冗余5类细分
    4. 视觉知识类型识别 (抽象/具象/过程/混杂)
    5. 跨模态一致性校验
    """
    
    def __init__(self, video_path: str, use_resource_manager: bool = True):
        self.video_path = video_path
        self.use_resource_manager = use_resource_manager
        self.cap: Optional[cv2.VideoCapture] = None
        self.fps: float = 30.0
        self.frame_count: int = 0
        self.duration_sec: float = 0.0
        
        # 性能优化: 缓存
        self.roi_cache = ROICache()
        self.frame_feat_cache = FrameFeatureCache()
        
        # 上一单元的复杂度 (用于动态采样率)
        self.last_unit_complexity = "medium"
        
        self._init_video()
    
    def _init_video(self):
        """初始化视频"""
        if self.use_resource_manager:
            rm = get_resource_manager()
            self.cap = rm.get_video_capture(self.video_path)
            info = rm.get_video_info(self.video_path)
            self.fps = info["fps"]
            self.frame_count = info["frame_count"]
            self.duration_sec = info["duration"]
        else:
            self.cap = cv2.VideoCapture(self.video_path)
            if not self.cap.isOpened():
                raise ValueError(f"Cannot open video: {self.video_path}")
            
            self.fps = self.cap.get(cv2.CAP_PROP_FPS)
            self.frame_count = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
            self.duration_sec = self.frame_count / self.fps if self.fps > 0 else 0
            
            logger.info(f"Video loaded (Direct): {self.video_path}, "
                       f"FPS={self.fps:.2f}, Duration={self.duration_sec:.2f}s")
        
        # 🚀 动态计算缩放比例 (最大宽度 640)
        # 缩小处理分辨率能大幅降低内存消耗 (1080p -> 640p 内存减少 ~84%)
        width = self.cap.get(cv2.CAP_PROP_FRAME_WIDTH) if self.cap else 1920
        self.target_width = 640
        self.processing_scale = self.target_width / width if width > self.target_width else 1.0
    
    def _resize_frame(self, frame: np.ndarray) -> np.ndarray:
        """统一调整帧大小以减少内存消耗"""
        if self.processing_scale < 1.0 and frame is not None:
             # Fast resize using INTER_AREA for downsampling or INTER_LINEAR for speed
             # INTER_LINEAR is faster and sufficient for CV metrics
             width = int(frame.shape[1] * self.processing_scale)
             height = int(frame.shape[0] * self.processing_scale)
             return cv2.resize(frame, (width, height), interpolation=cv2.INTER_LINEAR)
        return frame

    
    def close(self):
        """释放资源"""
        if self.cap:
            if not self.use_resource_manager:
                self.cap.release()
            self.cap = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # =========================================================================
    # 措施3: 轻量冗余初筛
    # =========================================================================
    
    def _quick_redundancy_check(self, frame: np.ndarray, 
                                 roi: Optional[Tuple[int, int, int, int]] = None) -> Optional[RedundancyType]:
        """
        轻量冗余初筛 (措施3)
        
        仅检测极端情况(纯黑/纯白)，避免误判正常画面
        注意: 边缘检测对PPT类视频不适用(边缘本来就少)
        """
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
        
        # 仅检查: 亮度方差 (纯黑/纯白屏)
        variance = np.var(gray)
        if variance <= CVConfig.RED_LOW_VAR_THRESH:
            logger.debug(f"Redundancy: LOW_VAR={variance:.1f}")
            return RedundancyType.RED_BLANK
        
        # 不再检查边缘占比 - PPT类视频边缘本来就很少
        # 不再检查ROI有效像素 - 可能误判
        return None

    
    # =========================================================================
    # 措施1: ROI增量复用
    # =========================================================================
    
    def _compute_layout_feature(self, frame: np.ndarray) -> float:
        """计算布局特征值 (用于ROI复用判定)"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
        # 8x8分块灰度均值
        h, w = gray.shape
        block_h, block_w = h // 8, w // 8
        feature = 0.0
        for i in range(8):
            for j in range(8):
                block = gray[i*block_h:(i+1)*block_h, j*block_w:(j+1)*block_w]
                feature += np.mean(block) * (i * 8 + j + 1)
        return feature
    
    def _detect_roi(self, frame: np.ndarray, 
                    use_cache: bool = True) -> Optional[Tuple[int, int, int, int]]:
        """
        检测核心知识ROI
        
        复用 VisualElementDetector.detect_structure_roi 逻辑:
        边缘检测 + 轮廓外接矩形
        """
        # 措施1: 尝试复用缓存
        if use_cache:
            layout_feat = self._compute_layout_feature(frame)
            prev_feat = self.roi_cache.get_last_layout_feature()
            if prev_feat and abs(layout_feat - prev_feat) / prev_feat < (1 - CVConfig.ROI_REUSE_SIM_THRESH):
                cached_roi = list(self.roi_cache.cache.values())[-1]['roi'] if self.roi_cache.cache else None
                if cached_roi:
                    logger.debug("ROI cache hit, reusing previous ROI")
                    return cached_roi
        
        # 新检测ROI
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
        h, w = gray.shape
        
        # 边缘检测
        edges = cv2.Canny(gray, 50, 150)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        if not contours:
            # 默认ROI: 中心80%区域
            margin_x, margin_y = int(w * 0.1), int(h * 0.1)
            return (margin_x, margin_y, w - margin_x, h - margin_y)
        
        # 计算包围盒
        all_points = []
        min_area = (h * w) * 0.001  # 最小面积阈值
        for cnt in contours:
            x, y, cw, ch = cv2.boundingRect(cnt)
            if cw * ch > min_area:
                all_points.append((x, y))
                all_points.append((x + cw, y + ch))
        
        if not all_points:
            margin_x, margin_y = int(w * 0.1), int(h * 0.1)
            return (margin_x, margin_y, w - margin_x, h - margin_y)
        
        points = np.array(all_points)
        x1, y1 = np.min(points, axis=0)
        x2, y2 = np.max(points, axis=0)
        
        # 添加边距
        margin = 10
        x1 = max(0, x1 - margin)
        y1 = max(0, y1 - margin)
        x2 = min(w, x2 + margin)
        y2 = min(h, y2 + margin)
        
        roi = (int(x1), int(y1), int(x2), int(y2))
        
        # 缓存ROI
        if use_cache:
            layout_feat = self._compute_layout_feature(frame)
            self.roi_cache.put(0, roi, layout_feat, 0.9)
        
        return roi
    
    # =========================================================================
    # SSIM计算 (ROI内)
    # =========================================================================
    
    def _calculate_ssim_roi(self, frame1: np.ndarray, frame2: np.ndarray,
                             roi: Tuple[int, int, int, int]) -> float:
        """计算ROI区域的SSIM"""
        x1, y1, x2, y2 = roi
        
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY) if len(frame1.shape) == 3 else frame1
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY) if len(frame2.shape) == 3 else frame2
        
        roi1 = gray1[y1:y2, x1:x2]
        roi2 = gray2[y1:y2, x1:x2]
        
        if roi1.size == 0 or roi2.size == 0:
            return 0.0
        
        # 使用 Numba 加速版本 (measures 7/10.2.4)
        return float(fast_ssim(roi1, roi2))
    
    # =========================================================================
    # Diff Ratio计算 (ROI内)
    # =========================================================================
    
    def _calculate_diff_ratio_roi(self, frame1: np.ndarray, frame2: np.ndarray,
                                   roi: Tuple[int, int, int, int]) -> float:
        """
        计算ROI区域的变化像素比例
        
        🚀 V8.0: 智能干扰过滤
        - 鼠标过滤: 形态学开运算 + 最小面积阈值
        - 人物过滤: 边缘区域变化忽略
        """
        x1, y1, x2, y2 = roi
        roi_w = x2 - x1
        roi_h = y2 - y1
        total_pixels = roi_w * roi_h
        
        if total_pixels == 0:
            return 0.0
        
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY) if len(frame1.shape) == 3 else frame1
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY) if len(frame2.shape) == 3 else frame2
        
        roi1 = gray1[y1:y2, x1:x2]
        roi2 = gray2[y1:y2, x1:x2]
        
        if roi1.size == 0 or roi2.size == 0:
            return 0.0
        
        # ========== V8.0 智能干扰过滤 ==========
        if not CVConfig.MOTION_FILTER_ENABLED:
            # 未启用过滤，使用原始快速版本
            return float(fast_diff_ratio(roi1, roi2, threshold=30))
        
        # Step 1: 计算差分 + 二值化
        diff = cv2.absdiff(roi1, roi2)
        _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
        
        # Step 2: 形态学开运算 (过滤鼠标光标和细小噪点)
        kernel_size = CVConfig.MOTION_MORPH_KERNEL_SIZE
        kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (kernel_size, kernel_size))
        cleaned = cv2.morphologyEx(thresh, cv2.MORPH_OPEN, kernel)
        
        # Step 3: 固定ROI排除 (人物区域 - 用户配置)
        if CVConfig.PERSON_EXCLUDE_ROIS:
            for roi_ratio in CVConfig.PERSON_EXCLUDE_ROIS:
                ex_x1 = int(roi_w * roi_ratio[0])
                ex_y1 = int(roi_h * roi_ratio[1])
                ex_x2 = int(roi_w * roi_ratio[2])
                ex_y2 = int(roi_h * roi_ratio[3])
                # 将排除区域置零
                cleaned[ex_y1:ex_y2, ex_x1:ex_x2] = 0
        
        # Step 4: 轮廓分析 + 小面积过滤 (鼠标)
        contours, _ = cv2.findContours(cleaned, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        min_area = total_pixels * CVConfig.MOTION_MIN_AREA_RATIO
        valid_change_pixels = 0
        
        for cnt in contours:
            area = cv2.contourArea(cnt)
            # 过滤: 忽略过小区域 (鼠标光标残留)
            if area < min_area:
                continue
            valid_change_pixels += area
        
        return valid_change_pixels / total_pixels
    
    # =========================================================================
    # 采样帧提取
    # =========================================================================
    
    def _sample_frames(self, start_sec: float, end_sec: float, 
                       fps: float) -> List[Tuple[float, np.ndarray]]:
        """按指定帧率采样 (优化版: 使用 ResourceManager 的顺序读取逻辑 + 自动缩放)"""
        raw_frames = []
        if self.use_resource_manager:
            # 兼容: 如果 ResourceManager 返回 None 或空，回退到本地 cap
            try:
                raw_frames = get_resource_manager().extract_frames(self.video_path, start_sec, end_sec, fps)
            except Exception as e:
                logger.warning(f"ResourceManager extract_frames failed: {e}, using local cap")
                raw_frames = []
        
        # 如果 ResourceManager 未启用或失败，使用本地 cap
        if not raw_frames and self.cap:
             # 回退逻辑 (不推荐)
            interval = 1.0 / fps
            t = start_sec
            while t <= end_sec:
                self.cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
                ret, frame = self.cap.read()
                if ret and frame is not None:
                    raw_frames.append((t, frame))
                t += interval
        
        # 🚀 统一缩放帧
        return [(t, self._resize_frame(f)) for t, f in raw_frames]
    
    # =========================================================================
    # 措施4: 状态判定轻量校验
    # =========================================================================
    
    def _light_stable_check(self, frame: np.ndarray, prev_frame: np.ndarray,
                            roi: Tuple[int, int, int, int]) -> bool:
        """轻量稳定状态校验 (仅检查5个局部区域)"""
        x1, y1, x2, y2 = roi
        w, h = x2 - x1, y2 - y1
        
        # 5个采样点: 4角 + 中心
        points = [
            (x1, y1, x1 + w//4, y1 + h//4),           # 左上
            (x2 - w//4, y1, x2, y1 + h//4),           # 右上
            (x1, y2 - h//4, x1 + w//4, y2),           # 左下
            (x2 - w//4, y2 - h//4, x2, y2),           # 右下
            (x1 + w//4, y1 + h//4, x2 - w//4, y2 - h//4)  # 中心
        ]
        
        for px1, py1, px2, py2 in points:
            diff_ratio = self._calculate_diff_ratio_roi(
                frame, prev_frame, (px1, py1, px2, py2))
            if diff_ratio > CVConfig.TH_DIFF_RATIO:
                return False
        
        return True
    
    # =========================================================================
    # V6.9.4: 边缘差分累积检测 (检测平移类动画)
    # =========================================================================
    

    


    
    def _should_trigger_edge_detection(self, ssim: float, diff_ratio: float) -> bool:
        """
        死区触发模式: 仅在MSE/SSIM失效的伪静止场景启动边缘检测
        
        触发条件:
        - diff_ratio极低 (MSE等效,像素变化微小)
        - SSIM极高 (结构完整)
        """
        ssim_drop = 1.0 - ssim
        return (
            diff_ratio < 0.01 and  # diff_ratio < 1% (等效MSE极低)
            ssim_drop < CVConfig.EDGE_DETECT_TRIGGER_SSIM_DROP
        )

    # =========================================================================
    # V8.0: 动作边界细化 (复用 VideoClipExtractor 核心逻辑)
    # =========================================================================
    
    def _calculate_mse(self, frame1: np.ndarray, frame2: np.ndarray, 
                       roi: Tuple[int, int, int, int] = None) -> float:
        """计算两帧之间的 MSE (均方误差)"""
        if roi:
            x1, y1, x2, y2 = roi
            f1 = frame1[y1:y2, x1:x2]
            f2 = frame2[y1:y2, x1:x2]
        else:
            f1, f2 = frame1, frame2
        
        # 转灰度
        if len(f1.shape) == 3:
            f1 = cv2.cvtColor(f1, cv2.COLOR_BGR2GRAY)
        if len(f2.shape) == 3:
            f2 = cv2.cvtColor(f2, cv2.COLOR_BGR2GRAY)
        
        diff = f1.astype(np.float32) - f2.astype(np.float32)
        mse = np.mean(diff ** 2)
        return mse
    
    def _refine_action_boundaries(self, action: ActionUnit, 
                                  roi: Tuple[int, int, int, int] = None) -> ActionUnit:
        """
        V8.0: 复用 VideoClipExtractor 的起止点细化逻辑
        
        核心原理:
        1. 起始点: 找到第一个 MSE 跳变帧 (视觉变化起点)
        2. 终止点: 找到最后一个 MSE 跳变帧 + 后续稳定区 (视觉变化终点)
        
        评分模型 = 0.7 * MSE强度 + 0.3 * 时序相似度
        """
        # 扩展扫描窗 (±2s)
        scan_start = max(0, action.start_sec - 2.0)
        scan_end = min(getattr(self, '_video_duration', action.end_sec + 10), action.end_sec + 2.0)
        
        # 采样帧 (2fps 足够)
        frames = self._sample_frames(scan_start, scan_end, fps=2.0)
        if len(frames) < 2:
            logger.debug(f"Boundary refinement skipped: insufficient frames")
            return action
        
        # 自适应 MSE 阈值
        mse_threshold = 80  # 默认阈值
        
        # 计算帧间 MSE
        mse_list = []
        for i in range(len(frames) - 1):
            _, f1 = frames[i]
            _, f2 = frames[i + 1]
            mse = self._calculate_mse(f1, f2, roi)
            mse_list.append((frames[i][0], mse, frames[i + 1][0]))
        
        if not mse_list:
            return action
        
        # 评分函数 (复用 VideoClipExtractor 逻辑)
        def calculate_anchor_score(mse_val: float, anchor_time: float, target_time: float) -> float:
            intensity = min(1.0, mse_val / mse_threshold)
            time_gap = abs(anchor_time - target_time)
            zone_weight = 1.0 if time_gap <= 1.5 else 0.7
            temporal_similarity = max(0.0, 1.0 - (time_gap / 5.0))
            return (0.7 * intensity + 0.3 * temporal_similarity) * zone_weight
        
        # 找起始锚点: 原始 start_sec 附近最高得分的 MSE 跳变
        best_start = action.start_sec
        max_start_score = -1.0
        for t, mse, _ in mse_list:
            if mse > mse_threshold:
                score = calculate_anchor_score(mse, t, action.start_sec)
                if score > max_start_score:
                    max_start_score = score
                    best_start = t
        
        # 找终止锚点: 原始 end_sec 附近最高得分的 MSE 跳变
        best_end = action.end_sec
        max_end_score = -1.0
        for t, mse, t_next in reversed(mse_list):
            if mse > mse_threshold * 0.8:  # 终止点阈值稍低
                score = calculate_anchor_score(mse, t_next, action.end_sec)
                if score > max_end_score:
                    max_end_score = score
                    best_end = t_next
        
        # 第一性原理: 起始点严守，终止点取语义终点与物理跳变的并集
        final_start = best_start
        final_end = max(action.end_sec, best_end)
        
        # 边界保护: 避免倒置或过长
        if final_end <= final_start:
            final_end = final_start + max(1.0, action.duration_ms / 1000)
        if final_end > action.end_sec + 3.0:
            final_end = action.end_sec + 0.5  # 适度缓冲
        
        # 更新 ActionUnit
        original_start, original_end = action.start_sec, action.end_sec
        action.start_sec = final_start
        action.end_sec = final_end
        
        if abs(original_start - final_start) > 0.1 or abs(original_end - final_end) > 0.1:
            logger.debug(f"Refined action boundaries: [{original_start:.2f}s-{original_end:.2f}s] → "
                        f"[{final_start:.2f}s-{final_end:.2f}s]")
        
        return action

    # =========================================================================
    # V7.0: 模态分类辅助方法
    # =========================================================================
    
    def _has_internal_stable_islands(self, action: ActionUnit, 
                                     all_stable_islands: List[StableIsland],
                                     min_duration_ms: float = 500.0) -> bool:
        """
        检测动作区间是否有内部稳定岛 (用于K1/K2判定)
        
        内部稳定岛定义: 
        - stable.start > action.start (不是首部衔接)
        - stable.end < action.end (不是尾部衔接)
        - duration >= 500ms
        
        返回: True表示可静态化 → K1/K2, False表示连续型 → K3/K4
        """
        internal_islands = []
        for stable in all_stable_islands:
            # 检查是否为内部稳定岛 (排除首尾衔接)
            if (stable.start_sec > action.start_sec + 0.1 and  # 排除首部
                stable.end_sec < action.end_sec - 0.1 and      # 排除尾部
                stable.duration_ms >= min_duration_ms):
                internal_islands.append(stable)
        
        has_internal = len(internal_islands) >= 1
        if has_internal:
            logger.debug(f"Found {len(internal_islands)} internal stable islands in action [{action.start_sec:.1f}s-{action.end_sec:.1f}s]")
        
        return has_internal
    
    # =========================================================================
    # V7.2: 呈现型动态检测 (淡入/渐显/弹出)
    # =========================================================================
    
    def _is_presentation_dynamic(self, action: ActionUnit,
                                  frames: List[Tuple[float, np.ndarray]],
                                  roi: Tuple[int, int, int, int],
                                  all_stable_islands: List[StableIsland]) -> bool:
        """
        检测是否为呈现型动态 (V7.2)
        
        呈现型定义: 内容在首帧已完整存在(仅低可见度)，末帧只是变成完全可见
        - 淡入/淡出、亮度渐变、透明度变化
        - 信息等价于末帧稳定截图，不需要视频
        
        判定规则 (全部满足):
        1. 空间分散度 > 0.75 (全局均匀变化)
        2. diff_ratio 单调平滑 (无创作型波动)
        3. 首尾帧内容IOU > 0.90 (二值化后)
        4. 动作后有稳定岛 (呈现完成即静止)
        
        返回: True=呈现型(强制截图), False=非呈现型(继续常规判定)
        """
        if len(frames) < 3:
            return False
        
        x1, y1, x2, y2 = roi
        
        # 提取动作区间内的帧
        action_frames = [(t, f) for t, f in frames 
                         if action.start_sec <= t <= action.end_sec]
        
        if len(action_frames) < 3:
            return False
        
        # 🚀 性能优化: 降采样 (提取动作区间帧后，最多保留 8 帧用于判断)
        if len(action_frames) > 8:
            step = len(action_frames) // 8
            analysis_frames = action_frames[::step][:8]
        else:
            analysis_frames = action_frames

        
        # ============ 规则1: 空间分散度检测 (最快，先做) ============
        # 计算变化像素的空间占比 (呈现型 > 0.75)
        spatial_spread = self._calculate_spatial_spread(analysis_frames, roi)
        if spatial_spread < 0.75:
            logger.debug(f"Presentation check failed: spatial_spread={spatial_spread:.2f} < 0.75")
            return False

        # ============ V7.3 反制层: 创作型强特征检测 (较重，后做) ============
        # 如果检测到创作型特征，直接排除呈现型判定
        # 防止手写/绘制/操作被误判为淡入淡出
        if self._has_creation_features(analysis_frames, roi):
            logger.debug(f"Presentation check blocked: creation features detected")
            return False
        
        # ============ 规则2: diff_ratio单调性检测 ============
        # 呈现型: 单调上升或下降; 创作型: 有波动
        diff_ratios = []
        prev_frame = None
        for t, frame in analysis_frames:
            if prev_frame is not None:
                diff = self._calculate_diff_ratio_roi(prev_frame, frame, roi)
                diff_ratios.append(diff)
            prev_frame = frame
        
        if not self._is_monotonic_smooth(diff_ratios, tolerance=0.02):
            logger.debug(f"Presentation check failed: diff_ratio not monotonic")
            return False
        
        # ============ 规则3: 首尾帧内容IOU检测 ============
        # 呈现型: 首尾内容一致 (仅可见度变化)
        first_frame = analysis_frames[0][1]
        last_frame = analysis_frames[-1][1]
        content_iou = self._calculate_frame_content_iou(first_frame, last_frame, roi)
        
        if content_iou < 0.90:
            logger.debug(f"Presentation check failed: content_iou={content_iou:.2f} < 0.90")
            return False
        
        # ============ 规则4: 动作后稳定岛检测 ============
        # 呈现完成后应立即进入稳定状态
        has_post_stable = False
        for stable in all_stable_islands:
            if stable.start_sec >= action.end_sec - 0.1 and stable.duration_ms >= 500:
                has_post_stable = True
                break
        
        if not has_post_stable:
            logger.debug(f"Presentation check failed: no stable island after action end")
            return False
        
        logger.debug(f"Detected PRESENTATION dynamic: spatial={spatial_spread:.2f}, "
                    f"content_iou={content_iou:.2f}")
        return True
    
    def _calculate_spatial_spread(self, frames: List[Tuple[float, np.ndarray]],
                                   roi: Tuple[int, int, int, int]) -> float:
        """计算变化像素的空间分散度 (0-1)"""
        if len(frames) < 2:
            return 0.0
        
        x1, y1, x2, y2 = roi
        total_changed_pixels = 0
        roi_area = (x2 - x1) * (y2 - y1)
        
        if roi_area == 0:
            return 0.0
        
        prev_frame = None
        for t, frame in frames:
            if prev_frame is not None:
                gray_curr = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
                gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY) if len(prev_frame.shape) == 3 else prev_frame
                
                roi_curr = gray_curr[y1:y2, x1:x2]
                roi_prev = gray_prev[y1:y2, x1:x2]
                
                diff = cv2.absdiff(roi_curr, roi_prev)
                changed = np.sum(diff > 10)  # 阈值10排除噪点
                ratio = changed / roi_area
                total_changed_pixels = max(total_changed_pixels, ratio)
            
            prev_frame = frame
        
        return total_changed_pixels
    
    def _is_monotonic_smooth(self, series: List[float], tolerance: float = 0.02) -> bool:
        """检测序列是否单调平滑 (允许小波动)"""
        if len(series) < 2:
            return True
        
        # 去极值
        arr = np.array(series)
        arr = np.clip(arr, 0, np.percentile(arr, 95) if len(arr) > 0 else 1)
        
        # 计算一阶差分
        diffs = np.diff(arr)
        
        # 允许小波动
        diffs[np.abs(diffs) <= tolerance] = 0
        
        # 检查单调性
        is_up = np.all(diffs >= 0)
        is_down = np.all(diffs <= 0)
        
        return is_up or is_down
    
    def _calculate_frame_content_iou(self, frame1: np.ndarray, frame2: np.ndarray,
                                      roi: Tuple[int, int, int, int]) -> float:
        """计算ROI内首尾帧内容重合度 (二值化后)"""
        x1, y1, x2, y2 = roi
        
        gray1 = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY) if len(frame1.shape) == 3 else frame1
        gray2 = cv2.cvtColor(frame2, cv2.COLOR_BGR2GRAY) if len(frame2.shape) == 3 else frame2
        
        roi1 = gray1[y1:y2, x1:x2]
        roi2 = gray2[y1:y2, x1:x2]
        
        # 二值化 (低阈值，捕捉内容轮廓)
        _, bin1 = cv2.threshold(roi1, 50, 255, cv2.THRESH_BINARY)
        _, bin2 = cv2.threshold(roi2, 50, 255, cv2.THRESH_BINARY)
        
        # 计算IOU
        intersection = np.logical_and(bin1, bin2).sum()
        union = np.logical_or(bin1, bin2).sum()
        
        return intersection / union if union > 0 else 0.0
    
    
    # =========================================================================
    # V7.3: 创作型强特征检测 (伪阳性规避)
    # =========================================================================
    
    def _has_creation_features(self, frames: List[Tuple[float, np.ndarray]],
                                roi: Tuple[int, int, int, int]) -> bool:
        """
        检测创作型强特征 (V7.3)
        
        如果检测到任意一个创作型特征，直接排除呈现型判定，
        防止真实创作型动态被误判为呈现型。
        
        创作型强特征:
        1. 局部像素增量 (创作型集中在局部区域，呈现型全局均匀)
        2. 非均匀变化模式 (创作型有轨迹/笔迹，呈现型是渐变)
        3. 内容边界扩展 (创作型边界逐渐扩大，呈现型边界不变)
        
        返回: True=有创作特征(非呈现型), False=无创作特征(可能是呈现型)
        """
        if len(frames) < 3:
            return False
        
        x1, y1, x2, y2 = roi
        
        # 🚀 性能优化: 降采样 (最多处理 10 帧)
        if len(frames) > 10:
            step = len(frames) // 10
            frames = frames[::step][:10]
        
        # 特征1: 检测局部集中变化 (创作型特征)
        has_local_increment = self._detect_local_pixel_increment(frames, roi)
        if has_local_increment:
            logger.debug("Creation feature detected: local pixel increment")
            return True
        
        # 特征2: 检测非均匀变化模式 (轨迹/笔迹)
        has_trace_pattern = self._detect_trace_pattern(frames, roi)
        if has_trace_pattern:
            logger.debug("Creation feature detected: trace pattern")
            return True
        
        # 特征3: 检测内容边界扩展
        has_boundary_expansion = self._detect_boundary_expansion(frames, roi)
        if has_boundary_expansion:
            logger.debug("Creation feature detected: boundary expansion")
            return True
        
        return False
    
    def _detect_local_pixel_increment(self, frames: List[Tuple[float, np.ndarray]],
                                       roi: Tuple[int, int, int, int]) -> bool:
        """
        检测局部像素增量 (创作型特征)
        
        创作型: 变化集中在某个局部区域 (如书写位置)
        呈现型: 变化均匀分布在整个ROI
        
        实现: 将ROI划分为4x4网格，检查变化是否集中在少数网格
        """
        x1, y1, x2, y2 = roi
        grid_h = (y2 - y1) // 4
        grid_w = (x2 - x1) // 4
        
        if grid_h < 10 or grid_w < 10:
            return False
        
        # 统计每个网格的累计变化
        grid_changes = np.zeros((4, 4))
        
        prev_frame = None
        for t, frame in frames:
            if prev_frame is not None:
                gray_curr = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
                gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY) if len(prev_frame.shape) == 3 else prev_frame
                
                for i in range(4):
                    for j in range(4):
                        gy1, gy2 = y1 + i * grid_h, y1 + (i + 1) * grid_h
                        gx1, gx2 = x1 + j * grid_w, x1 + (j + 1) * grid_w
                        
                        grid_curr = gray_curr[gy1:gy2, gx1:gx2]
                        grid_prev = gray_prev[gy1:gy2, gx1:gx2]
                        
                        diff = cv2.absdiff(grid_curr, grid_prev)
                        grid_changes[i, j] += np.sum(diff > 15)
            
            prev_frame = frame
        
        # 检查变化是否集中 (超过50%变化集中在<25%的网格)
        total_change = grid_changes.sum()
        if total_change == 0:
            return False
        
        sorted_changes = np.sort(grid_changes.flatten())[::-1]
        top_4_ratio = sorted_changes[:4].sum() / total_change  # 前4个网格占比
        
        # 如果前4个网格(25%)包含超过60%的变化 → 局部集中 → 创作型
        return top_4_ratio > 0.6
    
    def _detect_trace_pattern(self, frames: List[Tuple[float, np.ndarray]],
                               roi: Tuple[int, int, int, int]) -> bool:
        """
        检测轨迹/笔迹模式 (创作型特征)
        
        创作型: 变化呈连续轨迹状 (手写/绘制)
        呈现型: 变化是均匀渐变 (无轨迹)
        
        实现: 检测连续帧间变化区域的连通性
        """
        x1, y1, x2, y2 = roi
        
        trace_count = 0
        prev_frame = None
        
        for t, frame in frames[-5:]:  # 只检查最后5帧 (本来就是采样的，这里取最后5个样本)
            if prev_frame is not None:
                gray_curr = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
                gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY) if len(prev_frame.shape) == 3 else prev_frame
                
                roi_curr = gray_curr[y1:y2, x1:x2]
                roi_prev = gray_prev[y1:y2, x1:x2]
                
                diff = cv2.absdiff(roi_curr, roi_prev)
                _, binary = cv2.threshold(diff, 20, 255, cv2.THRESH_BINARY)
                
                # 寻找连通区域
                contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                
                # 检查是否有细长连通区域 (轨迹特征)
                for contour in contours:
                    if len(contour) > 10:
                        # 计算轮廓的细长度 (周长^2 / 面积)
                        area = cv2.contourArea(contour)
                        perimeter = cv2.arcLength(contour, True)
                        if area > 0 and perimeter > 0:
                            circularity = (perimeter ** 2) / (4 * np.pi * area)
                            # 细长轨迹的circularity > 3
                            if circularity > 3:
                                trace_count += 1
            
            prev_frame = frame
        
        # 如果检测到多个轨迹模式 → 创作型
        return trace_count >= 2
    
    def _detect_boundary_expansion(self, frames: List[Tuple[float, np.ndarray]],
                                    roi: Tuple[int, int, int, int]) -> bool:
        """
        检测内容边界扩展 (创作型特征)
        
        创作型: 内容边界随时间逐渐扩大 (新内容逐步添加)
        呈现型: 内容边界始终一致 (只是可见度变化)
        
        实现: 比较首帧和末帧的二值化内容边界框
        """
        if len(frames) < 2:
            return False
        
        x1, y1, x2, y2 = roi
        
        first_frame = frames[0][1]
        last_frame = frames[-1][1]
        
        gray_first = cv2.cvtColor(first_frame, cv2.COLOR_BGR2GRAY) if len(first_frame.shape) == 3 else first_frame
        gray_last = cv2.cvtColor(last_frame, cv2.COLOR_BGR2GRAY) if len(last_frame.shape) == 3 else last_frame
        
        roi_first = gray_first[y1:y2, x1:x2]
        roi_last = gray_last[y1:y2, x1:x2]
        
        # 自适应二值化
        _, bin_first = cv2.threshold(roi_first, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        _, bin_last = cv2.threshold(roi_last, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # 计算内容区域 (非零像素)
        first_pixels = np.sum(bin_first > 0)
        last_pixels = np.sum(bin_last > 0)
        
        if first_pixels == 0:
            # 首帧无内容，末帧有内容 → 明显的创作过程
            return last_pixels > (roi_first.shape[0] * roi_first.shape[1] * 0.01)
        
        # 如果末帧内容比首帧多20%以上 → 有新内容添加 → 创作型
        expansion_ratio = (last_pixels - first_pixels) / first_pixels if first_pixels > 0 else 0
        
        return expansion_ratio > 0.2

    def _classify_continuous_type(self, frames: List[Tuple[float, np.ndarray]],
                                   roi: Tuple[int, int, int, int]) -> str:
        """
        区分连续型动态的子类型 (K3/K4)
        
        纯CV方案:
        - 有UI控件特征 → K4 (连续操作)
        - 有手写/绘图轨迹特征 → K3 (连续推演)
        - 无法判定 → 默认K3 (保守: 视频+截图)
        
        返回: "derivation" (K3) / "operation" (K4) / "ambiguous"
        """
        if len(frames) < 2:
            return "ambiguous"
        
        x1, y1, x2, y2 = roi
        
        # 检测变化模式
        has_localized_change = False  # 局部定点变化 → K4特征
        has_traced_change = False     # 轨迹式变化 → K3特征
        
        prev_frame = None
        change_regions = []
        
        for t, frame in frames[-5:]:  # 只分析最后几帧
            if prev_frame is None:
                prev_frame = frame
                continue
            
            # 计算变化区域
            gray_curr = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
            gray_prev = cv2.cvtColor(prev_frame, cv2.COLOR_BGR2GRAY) if len(prev_frame.shape) == 3 else prev_frame
            
            roi_curr = gray_curr[y1:y2, x1:x2]
            roi_prev = gray_prev[y1:y2, x1:x2]
            
            diff = cv2.absdiff(roi_curr, roi_prev)
            _, thresh = cv2.threshold(diff, 30, 255, cv2.THRESH_BINARY)
            
            # 找到变化区域的轮廓
            contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if contours:
                # 检查变化区域的特征
                total_area = sum(cv2.contourArea(c) for c in contours)
                roi_area = (x2 - x1) * (y2 - y1)
                change_ratio = total_area / roi_area if roi_area > 0 else 0
                
                if len(contours) <= 3 and change_ratio < 0.1:
                    # 少量小区域变化 → 可能是UI点击
                    has_localized_change = True
                elif len(contours) > 5 or change_ratio > 0.15:
                    # 大量分散变化 → 可能是书写轨迹
                    has_traced_change = True
            
            prev_frame = frame
        
        # 决策
        if has_localized_change and not has_traced_change:
            return "operation"   # K4
        elif has_traced_change:
            return "derivation"  # K3
        else:
            return "derivation"  # 默认K3 (保守策略: 视频+截图)
    
    def _extract_key_screenshot_times(self, action: ActionUnit,
                                       diff_ratios: List[Tuple[float, float]] = None) -> List[float]:
        """
        提取K3类型的关键截图时间点 (3个锚点)
        
        规则:
        1. 起点: action.start_sec
        2. 终点: action.end_sec
        3. 中段: 视觉变化拐点帧 (diff_ratio峰值), 无则用时间中点
        
        返回: [start_time, inflection_time, end_time]
        """
        start_time = action.start_sec
        end_time = action.end_sec
        
        # 查找视觉拐点
        inflection_time = None
        if diff_ratios:
            # 找diff_ratio的峰值时间
            max_ratio = 0
            for t, ratio in diff_ratios:
                if action.start_sec < t < action.end_sec and ratio > max_ratio:
                    max_ratio = ratio
                    inflection_time = t
        
        # 无明显拐点则用时间中点
        if inflection_time is None:
            inflection_time = (start_time + end_time) / 2
        
        return [start_time, inflection_time, end_time]


    # 核心检测: 稳定岛/动作单元/冗余
    # =========================================================================

    
    def detect_visual_states(self, start_sec: float, end_sec: float
                              ) -> Tuple[List[StableIsland], List[ActionUnit], List[RedundancySegment]]:
        """
        检测视觉状态区间 (三类互斥)
        
        多级采样:
        - 1fps: ROI检测
        - 5fps: 状态判定
        - 10fps: 边界精修 (可选)
        """
        stable_islands: List[StableIsland] = []
        action_units: List[ActionUnit] = []
        redundancy_segments: List[RedundancySegment] = []
        
        # 动态采样率 (措施5)
        state_fps = CVConfig.FPS_STATE_DETECTION
        if self.last_unit_complexity == "low":
            state_fps = CVConfig.SAMPLE_FPS_LOW
        
        # 采样帧
        frames = self._sample_frames(start_sec, end_sec, state_fps)
        if len(frames) < 2:
            return stable_islands, action_units, redundancy_segments
        
        # ROI检测 (1fps采样)
        roi_frame = frames[len(frames) // 2][1]  # 中间帧
        roi = self._detect_roi(roi_frame)
        
        if not roi:
            return stable_islands, action_units, redundancy_segments
        
        # 状态序列: (timestamp, state, metric, ssim_drop)
        states: List[Tuple[float, FrameState, float, float]] = []
        
        prev_frame = None
        prev_state = None
        continuous_count = 0
        first_frame_of_segment = None  # 记录区间起始帧用于计算SSIM跌幅

        
        for i, (t, frame) in enumerate(frames):
            # 措施3: 轻量冗余初筛
            redundancy_type = self._quick_redundancy_check(frame, roi)
            if redundancy_type:
                states.append((t, FrameState.REDUNDANT, 0.0, 0.5))  # ssim=0.5 default
                prev_frame = frame
                prev_state = FrameState.REDUNDANT
                continuous_count = 0
                continue
            
            if prev_frame is None:
                prev_frame = frame
                continue
            
            # 措施4: 状态判定轻量校验
            if prev_state == FrameState.STABLE and continuous_count >= 2:
                if self._light_stable_check(frame, prev_frame, roi):
                    states.append((t, FrameState.STABLE, 0.95, 0.95))  # ssim=0.95 for stable
                    continuous_count += 1
                    prev_frame = frame
                    continue
            
            # 全量计算
            ssim = self._calculate_ssim_roi(prev_frame, frame, roi)
            diff_ratio = self._calculate_diff_ratio_roi(prev_frame, frame, roi)
            
            # 状态判定 (修正逻辑 v2)
            # 关键洞察: SSIM低 = 场景变化(可能是翻页), 应视为ACTION或边界
            if ssim >= CVConfig.TH_SSIM_STABLE:
                # 高相似度(≥0.9) = 稳定
                state = FrameState.STABLE
                metric = ssim
                if prev_state == FrameState.STABLE:
                    continuous_count += 1
                else:
                    continuous_count = 1
            elif ssim < 0.5:
                # 极低相似度(<0.5) = 场景突变(如PPT翻页) → 视为ACTION边界
                # 这是知识点切换的重要锚点
                state = FrameState.ACTION
                metric = 1.0 - ssim  # 用1-ssim作为变化程度
                if prev_state == FrameState.ACTION:
                    continuous_count += 1
                else:
                    continuous_count = 1
            elif diff_ratio > 0.03:  # 降低阈值: 3%变化就算动作
                # 中等变化(diff>3%) = 动作
                state = FrameState.ACTION
                metric = diff_ratio
                if prev_state == FrameState.ACTION:
                    continuous_count += 1
                else:
                    continuous_count = 1
            elif ssim >= 0.7:
                # 中等相似度(0.7-0.9) + 低变化 = 视为稳定(讲解/微动)
                state = FrameState.STABLE
                metric = ssim
                if prev_state == FrameState.STABLE:
                    continuous_count += 1
                else:
                    continuous_count = 1
            else:
                # 其他情况 = 可能是缓慢过渡
                state = FrameState.REDUNDANT
                metric = 0.0
                ssim = 0.5  # 默认SSIM
                continuous_count = 0
            
            # V6.9.5: 状态元组增加SSIM值 (timestamp, state, metric, ssim)
            states.append((t, state, metric, ssim))
            prev_frame = frame
            prev_state = state

        
        # V6.9.4: 帧级别平滑动画检测 (V7.1优化: 提高阈值减少误检)
        # 对于被判定为STABLE的帧，检查是否存在边缘位移
        # 只标记有实际动画的帧，而不是整个区间
        if len(frames) >= 3:
            x1, y1, x2, y2 = roi
            new_states = []
            prev_edges = None
            prev_centroid = None
            consecutive_animated = 0  # 连续动画帧计数
            
            for idx, (t, frame) in enumerate(frames):
                if idx >= len(states):
                    break
                    
                current_state = states[idx]
                
                # 只对STABLE帧进行动画检测
                if current_state[1] == FrameState.STABLE:
                    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
                    roi_gray = gray[y1:y2, x1:x2]
                    
                    # 边缘检测
                    blurred = cv2.GaussianBlur(roi_gray, (3, 3), 0)
                    edges = cv2.Canny(blurred, 50, 150)
                    
                    # 重心计算
                    contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                    centroid = None
                    if contours:
                        cx_sum, cy_sum, cnt = 0, 0, 0
                        for c in contours:
                            M = cv2.moments(c)
                            if M["m00"] > 0:
                                cx_sum += M["m10"] / M["m00"]
                                cy_sum += M["m01"] / M["m00"]
                                cnt += 1
                        if cnt > 0:
                            centroid = (cx_sum / cnt, cy_sum / cnt)
                    
                    is_frame_animated = False
                    
                    # V7.1: 提高阈值减少误检
                    # 检测边缘变化 (提高阈值: 500 → 1500)
                    if prev_edges is not None:
                        edge_diff = cv2.absdiff(edges, prev_edges)
                        diff_energy = np.sum(edge_diff > 0)
                        if diff_energy > 1500:  # 提高单帧边缘变化阈值
                            is_frame_animated = True
                    
                    # 检测重心位移 (提高阈值: 3.0 → 8.0)
                    if prev_centroid and centroid:
                        dx = centroid[0] - prev_centroid[0]
                        dy = centroid[1] - prev_centroid[1]
                        disp = np.sqrt(dx**2 + dy**2)
                        if disp > 8.0:  # 提高单帧位移阈值
                            is_frame_animated = True
                    
                    # V7.1: 需要连续2帧触发才判定为动画，减少噪点误判
                    if is_frame_animated:
                        consecutive_animated += 1
                        if consecutive_animated >= 2:
                            # 真正的动画帧
                            new_states.append((t, FrameState.ACTION, 0.5, 0.8))
                        else:
                            # 只有1帧触发，可能是噪点，保持STABLE
                            new_states.append(current_state)
                    else:
                        consecutive_animated = 0
                        new_states.append(current_state)
                    
                    prev_edges = edges
                    prev_centroid = centroid
                else:
                    new_states.append(current_state)
                    prev_edges = None
                    prev_centroid = None
                    consecutive_animated = 0
            
            states = new_states

        
        # 合并连续状态为区间 (V7.2: 传递frames和roi用于呈现型检测)
        stable_islands, action_units, redundancy_segments = self._merge_state_intervals(
            states, start_sec, end_sec, frames=frames, roi=roi)

        
        # 更新复杂度 (用于下一单元动态采样)
        total_duration = end_sec - start_sec
        stable_duration = sum(s.duration_ms for s in stable_islands) / 1000
        self.last_unit_complexity = "low" if stable_duration / total_duration >= CVConfig.COMPLEX_LOW_STABLE_RATIO else "medium"
        
        return stable_islands, action_units, redundancy_segments



    
    def _merge_state_intervals(self, states: List[Tuple[float, FrameState, float]],
                                start_sec: float, end_sec: float,
                                frames: List[Tuple[float, np.ndarray]] = None,
                                roi: Tuple[int, int, int, int] = None
                                ) -> Tuple[List[StableIsland], List[ActionUnit], List[RedundancySegment]]:
        """
        合并连续状态为区间
        
        V6.9.5增强: 
        - 计算ActionUnit的ssim_drop用于分类
        - 自动标记transition/noise为非有效动态
        - 将非有效动作移入冗余段
        
        V7.2增强:
        - 支持呈现型动态检测 (frames + roi)
        """
        stable_islands: List[StableIsland] = []
        action_units: List[ActionUnit] = []
        redundancy_segments: List[RedundancySegment] = []
        
        if not states:
            return stable_islands, action_units, redundancy_segments
        
        # 分组连续相同状态
        # 状态元组: (timestamp, state, metric, ssim)
        current_state = states[0][1]
        current_start = states[0][0]
        current_metrics: List[float] = [states[0][2]]
        current_ssims: List[float] = [states[0][3] if len(states[0]) > 3 else 0.9]
        
        for i in range(1, len(states)):
            # 解包4元组
            t = states[i][0]
            state = states[i][1]
            metric = states[i][2]
            ssim = states[i][3] if len(states[i]) > 3 else 0.9
            
            if state == current_state:
                current_metrics.append(metric)
                current_ssims.append(ssim)
            else:
                # 输出上一个区间
                duration_ms = (t - current_start) * 1000
                avg_metric = np.mean(current_metrics) if current_metrics else 0.0
                
                if current_state == FrameState.STABLE and duration_ms >= CVConfig.TH_STABLE_DURATION_MS:
                    stable_islands.append(StableIsland(current_start, t, avg_metric))
                elif current_state == FrameState.ACTION and duration_ms >= CVConfig.TH_ACTION_DURATION_MS:
                    # V6.9.5: 计算真正的SSIM跌幅 = 首帧SSIM - 末帧SSIM
                    # 如果跌幅大，说明是场景切换(transition)
                    first_ssim = current_ssims[0] if current_ssims else 0.9
                    last_ssim = current_ssims[-1] if current_ssims else 0.9
                    ssim_drop = max(0.0, first_ssim - last_ssim)
                    
                    # 另一种计算: 用最低SSIM衡量结构变化程度
                    min_ssim = min(current_ssims) if current_ssims else 0.9
                    if min_ssim < 0.5:
                        ssim_drop = 1.0 - min_ssim  # 大跌幅
                    
                    action_units.append(ActionUnit(
                        start_sec=current_start,
                        end_sec=t,
                        avg_diff_ratio=avg_metric,
                        ssim_drop=ssim_drop
                    ))
                elif current_state == FrameState.REDUNDANT:
                    redundancy_segments.append(RedundancySegment(
                        current_start, t, RedundancyType.RED_TRANSITION, "整段剔除"))
                
                # 开始新区间
                current_state = state
                current_start = t
                current_metrics = [metric]
                current_ssims = [ssim]

        
        # 处理最后一个区间
        if states:
            t = end_sec
            duration_ms = (t - current_start) * 1000
            avg_metric = np.mean(current_metrics) if current_metrics else 0.0
            
            if current_state == FrameState.STABLE and duration_ms >= CVConfig.TH_STABLE_DURATION_MS:
                stable_islands.append(StableIsland(current_start, t, avg_metric))
            elif current_state == FrameState.ACTION and duration_ms >= CVConfig.TH_ACTION_DURATION_MS:
                # 计算真正的SSIM跌幅
                first_ssim = current_ssims[0] if current_ssims else 0.9
                last_ssim = current_ssims[-1] if current_ssims else 0.9
                ssim_drop = max(0.0, first_ssim - last_ssim)
                min_ssim = min(current_ssims) if current_ssims else 0.9
                if min_ssim < 0.5:
                    ssim_drop = 1.0 - min_ssim
                    
                action_units.append(ActionUnit(
                    start_sec=current_start,
                    end_sec=t,
                    avg_diff_ratio=avg_metric,
                    ssim_drop=ssim_drop
                ))
            elif current_state == FrameState.REDUNDANT:
                redundancy_segments.append(RedundancySegment(
                    current_start, t, RedundancyType.RED_TRANSITION, "整段剔除"))

        
        # V6.9.5: 后处理 - 分类并过滤非有效动作
        # V7.0: 增加模态子分类 (K1-K4)
        effective_actions = []
        for action in action_units:
            action_type = action.classify()
            action.action_type = action_type
            
            if action_type == "knowledge":
                # 知识生产型: 有效，保留
                action.is_effective = True
                
                # V7.0: 模态子分类
                # 检查是否有内部稳定岛 → K1/K2 (截图)
                has_internal = self._has_internal_stable_islands(action, stable_islands)
                
                if has_internal:
                    # K1/K2: 可静态化 → 纯截图
                    action.modality = Modality.SCREENSHOT.value
                    action.knowledge_subtype = "K1_K2_stepwise"
                else:
                    # V7.2: 先检查呈现型动态 (淡入/渐显)
                    is_presentation = False
                    if frames is not None and roi is not None:
                        is_presentation = self._is_presentation_dynamic(
                            action, frames, roi, stable_islands)
                    
                    if is_presentation:
                        # 呈现型: 强制截图 (取末帧稳定帧)
                        action.modality = Modality.SCREENSHOT.value
                        action.knowledge_subtype = "presentation"
                        logger.debug(f"Presentation dynamic detected [{action.start_sec:.1f}s-{action.end_sec:.1f}s]: forced screenshot")
                    else:
                        # V7.4: K3/K4 精细区分
                        continuous_type = "derivation"  # 默认K3
                        if frames is not None and roi is not None:
                            # 提取动作区间帧
                            action_frames = [(t, f) for t, f in frames 
                                            if action.start_sec <= t <= action.end_sec]
                            if len(action_frames) >= 2:
                                continuous_type = self._classify_continuous_type(action_frames, roi)
                        
                        if continuous_type == "operation":
                            # K4: 连续操作 → 纯视频
                            action.modality = Modality.VIDEO_ONLY.value
                            action.knowledge_subtype = "K4_operation"
                        else:
                            # K3: 连续推演 → 视频+截图
                            action.modality = Modality.VIDEO_SCREENSHOT.value
                            action.knowledge_subtype = "K3_derivation"

                
                effective_actions.append(action)
                logger.debug(f"Knowledge action [{action.start_sec:.1f}s-{action.end_sec:.1f}s]: "
                           f"subtype={action.knowledge_subtype}, modality={action.modality}")
                
            elif action_type == "transition":
                # 容器切换型: 非有效，移入冗余段
                action.is_effective = False
                action.modality = Modality.DISCARD.value
                redundancy_segments.append(RedundancySegment(
                    action.start_sec, action.end_sec,
                    RedundancyType.RED_TRANSITION, "转场动画剔除"))
                logger.debug(f"Filtered transition action: [{action.start_sec:.1f}s-{action.end_sec:.1f}s]")
            elif action_type == "noise":
                # 噪点: 非有效，移入冗余段
                action.is_effective = False
                action.modality = Modality.DISCARD.value
                redundancy_segments.append(RedundancySegment(
                    action.start_sec, action.end_sec,
                    RedundancyType.RED_IRRELEVANT, "噪点动画剔除"))
                logger.debug(f"Filtered noise action: [{action.start_sec:.1f}s-{action.end_sec:.1f}s]")
            else:
                # mixed类型: 保守用截图
                action.is_effective = True
                action.modality = Modality.SCREENSHOT.value
                action.knowledge_subtype = "mixed"
                effective_actions.append(action)
        
        # V7.1: 合并相邻动作单元 (间隔 < TH_ACTION_MERGE_GAP_SEC 的视为连续动作)
        if len(effective_actions) >= 2:
            merged_actions = []
            current = effective_actions[0]
            
            for next_action in effective_actions[1:]:
                gap = next_action.start_sec - current.end_sec
                
                if gap < CVConfig.TH_ACTION_MERGE_GAP_SEC:  # 使用配置项
                    # 合并为一个更大的动作单元
                    merged = ActionUnit(
                        start_sec=current.start_sec,
                        end_sec=next_action.end_sec,
                        avg_diff_ratio=max(current.avg_diff_ratio, next_action.avg_diff_ratio),
                        ssim_drop=max(current.ssim_drop, next_action.ssim_drop),
                        is_effective=True,
                        has_internal_stable=current.has_internal_stable or next_action.has_internal_stable
                    )
                    # 重新分类合并后的动作
                    merged.action_type = merged.classify()
                    
                    # V7.0: 根据新类型更新模态
                    if merged.action_type == "knowledge":
                        merged.modality = Modality.VIDEO_SCREENSHOT.value
                        merged.knowledge_subtype = "K3_K4_continuous"
                    elif merged.action_type == "mixed":
                        merged.modality = Modality.SCREENSHOT.value
                        merged.knowledge_subtype = "mixed"
                    else:
                        merged.modality = current.modality
                        merged.knowledge_subtype = current.knowledge_subtype
                    
                    current = merged
                    logger.debug(f"Merged adjacent actions: gap={gap:.2f}s → [{current.start_sec:.1f}s-{current.end_sec:.1f}s] type={current.action_type}")
                else:
                    merged_actions.append(current)
                    current = next_action
            
            merged_actions.append(current)
            
            # 💥 日志: 显示合并效果
            if len(merged_actions) < len(effective_actions):
                logger.info(f"Action merge: {len(effective_actions)} → {len(merged_actions)} (threshold={CVConfig.TH_ACTION_MERGE_GAP_SEC}s)")
            
            effective_actions = merged_actions
        
        # V8.0: 边界细化 - 复用 VideoClipExtractor 逻辑
        # 仅对需要截取视频的动作单元进行细化
        for action in effective_actions:
            if action.modality in (Modality.VIDEO_SCREENSHOT.value, Modality.VIDEO_ONLY.value):
                self._refine_action_boundaries(action, roi)
                logger.debug(f"Boundary refined: [{action.start_sec:.2f}s-{action.end_sec:.2f}s] modality={action.modality}")

        return stable_islands, effective_actions, redundancy_segments


    # =========================================================================
    # V9.0: 两阶段动作单元合并 (新架构)
    # =========================================================================
    
    def _merge_action_units_stage1(
        self, 
        action_units: List[ActionUnit],
        all_stable_islands: List[StableIsland]
    ) -> Tuple[List[ActionUnit], List[StableIsland]]:
        """
        第一阶段合并：去碎片（适用于所有 ActionUnit）
        
        条件：间隔 < 1s
        目的：修正 CV 采样导致的碎片化
        
        Args:
            action_units: 所有检测到的动作单元
            all_stable_islands: 所有检测到的稳定岛（用于记录被跨越的）
            
        Returns:
            (merged_units, crossed_stable_islands)
        """
        if len(action_units) <= 1:
            return action_units, []
        
        MERGE_GAP_THRESHOLD = 1.0  # 第一阶段：1秒
        
        merged = []
        crossed_islands = []
        current = action_units[0]
        
        for next_unit in action_units[1:]:
            gap = next_unit.start_sec - current.end_sec
            
            if gap < MERGE_GAP_THRESHOLD:
                # 记录被跨越的稳定岛
                gap_islands = self._get_stable_islands_in_range(
                    current.end_sec, next_unit.start_sec, all_stable_islands
                )
                crossed_islands.extend(gap_islands)
                
                # 合并动作单元
                current = ActionUnit(
                    start_sec=current.start_sec,
                    end_sec=next_unit.end_sec,
                    avg_diff_ratio=max(current.avg_diff_ratio, next_unit.avg_diff_ratio),
                    ssim_drop=max(current.ssim_drop, next_unit.ssim_drop),
                    action_type=current.action_type,
                    is_effective=current.is_effective or next_unit.is_effective,
                    has_internal_stable=current.has_internal_stable or next_unit.has_internal_stable,
                    modality=current.modality,
                    knowledge_subtype=current.knowledge_subtype
                )
                logger.debug(f"Stage1 merge: gap={gap:.2f}s → [{current.start_sec:.1f}s-{current.end_sec:.1f}s]")
            else:
                merged.append(current)
                current = next_unit
        
        merged.append(current)
        
        if len(merged) < len(action_units):
            logger.info(f"Stage1 merge: {len(action_units)} → {len(merged)} actions, "
                       f"crossed {len(crossed_islands)} stable islands")
        
        return merged, crossed_islands
    
    def _merge_action_units_stage2(
        self, 
        action_units: List[ActionUnit],
        all_stable_islands: List[StableIsland],
        semantic_unit_id: str = ""
    ) -> Tuple[List[ActionUnit], List[StableIsland]]:
        """
        第二阶段合并：语义聚合（仅适用于通过 LLM 筛选的 ActionUnit）
        
        条件：
        - knowledge_type 相同
        - 间隔 < 5s
        
        Args:
            action_units: 通过 LLM 筛选的动作单元
            all_stable_islands: 所有检测到的稳定岛
            semantic_unit_id: 语义单元 ID（用于日志）
            
        Returns:
            (merged_units, crossed_stable_islands)
        """
        if len(action_units) <= 1:
            return action_units, []
        
        MERGE_GAP_THRESHOLD = 5.0  # 第二阶段：5秒
        
        merged = []
        crossed_islands = []
        current = action_units[0]
        
        for next_unit in action_units[1:]:
            gap = next_unit.start_sec - current.end_sec
            
            # 只有 knowledge_type 相同且间隔 < 5s 才合并
            same_type = getattr(current, 'knowledge_type', '') == getattr(next_unit, 'knowledge_type', '')
            
            if gap < MERGE_GAP_THRESHOLD and same_type:
                # 记录被跨越的稳定岛
                gap_islands = self._get_stable_islands_in_range(
                    current.end_sec, next_unit.start_sec, all_stable_islands
                )
                crossed_islands.extend(gap_islands)
                
                # 合并动作单元
                current = ActionUnit(
                    start_sec=current.start_sec,
                    end_sec=next_unit.end_sec,
                    avg_diff_ratio=max(current.avg_diff_ratio, next_unit.avg_diff_ratio),
                    ssim_drop=max(current.ssim_drop, next_unit.ssim_drop),
                    action_type=current.action_type,
                    is_effective=True,
                    has_internal_stable=current.has_internal_stable or next_unit.has_internal_stable,
                    modality=current.modality,
                    knowledge_subtype=current.knowledge_subtype
                )
                # 保留 knowledge_type
                if hasattr(current, 'knowledge_type'):
                    current.knowledge_type = getattr(action_units[0], 'knowledge_type', '')
                    
                logger.debug(f"Stage2 merge [{semantic_unit_id}]: gap={gap:.2f}s, type={getattr(current, 'knowledge_type', 'unknown')} → "
                           f"[{current.start_sec:.1f}s-{current.end_sec:.1f}s]")
            else:
                merged.append(current)
                current = next_unit
        
        merged.append(current)
        
        if len(merged) < len(action_units):
            logger.info(f"Stage2 merge [{semantic_unit_id}]: {len(action_units)} → {len(merged)} actions, "
                       f"crossed {len(crossed_islands)} stable islands")
        
        return merged, crossed_islands
    
    def _get_stable_islands_in_range(
        self, 
        start_sec: float, 
        end_sec: float,
        stable_islands: List[StableIsland]
    ) -> List[StableIsland]:
        """获取指定时间范围内的稳定岛"""
        return [
            island for island in stable_islands
            if island.end_sec > start_sec and island.start_sec < end_sec
        ]
    
    def _collect_all_stable_islands(
        self,
        action_units: List[ActionUnit],
        external_stable_islands: List[StableIsland],
        crossed_islands_stage1: List[StableIsland],
        crossed_islands_stage2: List[StableIsland]
    ) -> List[StableIsland]:
        """
        收集所有稳定岛用于截图提取：
        1. ActionUnit 内部的稳定岛
        2. ActionUnit 外部的稳定岛
        3. 两次合并中被跨越的稳定岛
        """
        all_islands = []
        seen_times = set()  # 去重
        
        def add_island(island: StableIsland):
            key = (round(island.start_sec, 2), round(island.end_sec, 2))
            if key not in seen_times:
                seen_times.add(key)
                all_islands.append(island)
        
        # 1. 内部稳定岛（从 ActionUnit 的 internal_stable_islands 属性）
        for unit in action_units:
            if hasattr(unit, 'internal_stable_islands') and unit.internal_stable_islands:
                for island in unit.internal_stable_islands:
                    add_island(island)
        
        # 2. 外部稳定岛
        for island in external_stable_islands:
            add_island(island)
        
        # 3. 被跨越的稳定岛
        for island in crossed_islands_stage1:
            add_island(island)
        for island in crossed_islands_stage2:
            add_island(island)
        
        # 按时间排序
        all_islands.sort(key=lambda x: x.start_sec)
        
        logger.debug(f"Collected {len(all_islands)} stable islands for screenshot extraction")
        return all_islands

    
    # =========================================================================
    # 视觉知识类型识别
    # =========================================================================
    
    def classify_visual_knowledge_type(self, 
                                        stable_islands: List[StableIsland],
                                        action_units: List[ActionUnit],
                                        redundancy_segments: List[RedundancySegment],
                                        total_duration: float
                                        ) -> Tuple[VisualKnowledgeType, VisionStats]:
        """
        视觉知识类型识别 (三级阶梯判定)
        """
        if total_duration <= 0:
            return VisualKnowledgeType.MIXED, VisionStats()
        
        # 计算各类区间占比
        stable_duration = sum(s.duration_ms for s in stable_islands) / 1000
        action_duration = sum(a.duration_ms for a in action_units) / 1000
        redundant_duration = sum(r.duration_ms for r in redundancy_segments) / 1000
        
        # 补全到total_duration
        accounted = stable_duration + action_duration + redundant_duration
        if accounted < total_duration:
            # 未分类区间归为抽象 (无明显视觉特征)
            abstract_duration = total_duration - accounted
        else:
            abstract_duration = 0.0
        
        # 三类互斥占比
        s_stable = stable_duration / total_duration
        s_action = action_duration / total_duration
        s_redundant = redundant_duration / total_duration
        
        # 知识类型占比映射
        # 具象 ← 稳定岛
        # 过程 ← 动作单元  
        # 抽象 ← 未分类区间
        p_abstract = abstract_duration / total_duration
        p_concrete = s_stable
        p_process = s_action
        
        stats = VisionStats(
            s_stable=s_stable,
            s_action=s_action,
            s_redundant=s_redundant,
            p_abstract=p_abstract,
            p_concrete=p_concrete,
            p_process=p_process
        )
        
        # 三级阶梯判定
        ratios = {'abstract': p_abstract, 'concrete': p_concrete, 'process': p_process}
        sorted_ratios = sorted(ratios.items(), key=lambda x: x[1], reverse=True)
        max_type, max_p = sorted_ratios[0]
        second_p = sorted_ratios[1][1]
        
        # Step 1: 绝对主导
        if max_p >= CVConfig.TH_ABSOLUTE_LEAD:
            return VisualKnowledgeType(max_type), stats
        
        # Step 2: 相对主导
        if max_p >= CVConfig.TH_RELATIVE_LEAD and (max_p - second_p) >= CVConfig.TH_RELATIVE_DIFF:
            return VisualKnowledgeType(max_type), stats
        
        # Step 3: 混杂
        return VisualKnowledgeType.MIXED, stats
    
    # =========================================================================
    # 批量校验 (措施6 + 方案B)
    # =========================================================================
    
    def validate_batch(self, units: List[Dict[str, Any]]) -> List[CVValidationResult]:
        """
        批量CV校验 (并行加速版)
        
        优化: 使用 ThreadPoolExecutor 并行处理多个语义单元
        注意: OpenCV 的很多操作释放 GIL，IO 操作也能并行
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        results: List[CVValidationResult] = []
        if not units:
            return results
            
        # 按 start_sec 排序，尽量保证缓存命中 (虽然并行可能会乱序，但ResourceManager可能有底层优化)
        sorted_units = sorted(units, key=lambda x: x.get("start_sec", 0.0))
        
        # 限制并发数 (避免过多 VideoCapture 竞争或内存爆炸)
        # 建议设置为 CPU 核心数 或 4-8
        max_workers = min(8, os.cpu_count() or 4)
        
        logger.info(f"Starting batch CV validation for {len(units)} units (workers={max_workers})")
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_unit = {
                executor.submit(
                    self.validate_single, 
                    unit.get("unit_id", ""), 
                    unit.get("start_sec", 0.0), 
                    unit.get("end_sec", 0.0),
                    unit.get("knowledge_type", "abstract")
                ): unit for unit in sorted_units
            }
            
            for future in as_completed(future_to_unit):
                unit = future_to_unit[future]
                unit_id = unit.get("unit_id", "")
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"CV validation failed for unit {unit_id}: {e}")
                    # 生成降级结果
                    results.append(CVValidationResult(
                        unit_id=unit_id,
                        timeline=(unit.get("start_sec", 0.0), unit.get("end_sec", 0.0)),
                        vision_stats=VisionStats(),
                        main_vision_type=VisualKnowledgeType.MIXED,
                        abnormal_type="validation_error",
                        abnormal_reason=str(e)
                    ))
        
        # 恢复原始顺序
        results_map = {r.unit_id: r for r in results}
        final_results = [results_map.get(u.get("unit_id", ""), None) for u in units]
        # 过滤 None (理论上不应发生)
        final_results = [r for r in final_results if r]
        
        logger.info(f"Batch CV validation completed in {time.time() - start_time:.2f}s")
        return final_results
    
    def validate_single(self, unit_id: str, start_sec: float, end_sec: float,
                         llm_type: str) -> CVValidationResult:
        """
        单语义单元CV校验
        """
        total_duration = end_sec - start_sec
        
        # 1. 检测三类区间
        stable_islands, action_units, redundancy_segments = \
            self.detect_visual_states(start_sec, end_sec)
        
        # 2. 视觉知识类型识别
        vision_type, stats = self.classify_visual_knowledge_type(
            stable_islands, action_units, redundancy_segments, total_duration)
        
        # 3. 提取视觉锚点
        anchors = []
        for si in stable_islands:
            anchors.extend([si.start_sec, si.end_sec])
        for au in action_units:
            anchors.extend([au.start_sec, au.end_sec])
        anchors = sorted(set(anchors))
        
        # 4. 跨模态一致性校验
        type_match = self._check_type_match(llm_type, vision_type, stats)
        
        # 5. 时序连续性校验 (简化: 检查是否有大段冗余断层)
        timeline_continuous = True
        for rs in redundancy_segments:
            if rs.duration_ms > 2000:  # 2秒以上冗余
                timeline_continuous = False
                break
        
        # 6. 视觉单元完整性校验
        vision_unit_complete = True
        # 检查稳定岛/动作单元是否被边界截断
        epsilon = 0.5  # 500ms容差
        for si in stable_islands:
            if abs(si.start_sec - start_sec) < epsilon or abs(si.end_sec - end_sec) < epsilon:
                if si.duration_ms < CVConfig.TH_STABLE_DURATION_MS * 1.5:
                    vision_unit_complete = False
                    break
        
        # 构建结果
        result = CVValidationResult(
            unit_id=unit_id,
            timeline=(start_sec, end_sec),
            vision_stats=stats,
            main_vision_type=vision_type,
            stable_islands=stable_islands,
            action_units=action_units,
            redundancy_segments=redundancy_segments,
            vision_anchors=anchors,
            timeline_continuous=timeline_continuous,
            type_match=type_match,
            vision_unit_complete=vision_unit_complete
        )
        
        # 异常信息
        if not type_match:
            result.abnormal_type = "类型不匹配"
            result.abnormal_reason = f"LLM={llm_type}, CV={vision_type.value}"
        elif not timeline_continuous:
            result.abnormal_type = "时序割裂"
            result.abnormal_reason = "存在>2s的冗余断层"
        elif not vision_unit_complete:
            result.abnormal_type = "视觉单元不完整"
            result.abnormal_reason = "稳定岛/动作单元被边界截断"
        
        return result
    
    def _check_type_match(self, llm_type: str, vision_type: VisualKnowledgeType,
                           stats: VisionStats) -> bool:
        """跨模态类型一致性校验"""
        if vision_type == VisualKnowledgeType.MIXED:
            # 联合裁决
            if llm_type == "abstract":
                return (stats.p_abstract + stats.p_concrete) >= CVConfig.TH_ABS_CON_SUM
            elif llm_type == "concrete":
                return stats.s_stable >= CVConfig.TH_STABLE_RATIO
            elif llm_type == "process":
                return stats.s_action >= CVConfig.TH_ACTION_RATIO
            return False
        
        return llm_type == vision_type.value
    
    # =========================================================================
    # 冲突包生成 (用于LLM重判)
    # =========================================================================
    
    def generate_conflict_packages(self, results: List[CVValidationResult]
                                    ) -> List[ConflictPackage]:
        """
        生成冲突包 (仅对异常单元)
        """
        packages: List[ConflictPackage] = []
        
        for result in results:
            if result.is_normal:
                continue
            
            # 提取有效区间
            valid_timelines = []
            for si in result.stable_islands:
                valid_timelines.append((si.start_sec, si.end_sec))
            for au in result.action_units:
                valid_timelines.append((au.start_sec, au.end_sec))
            
            # 锚点类型
            anchor_types = []
            for anchor in result.vision_anchors:
                for si in result.stable_islands:
                    if abs(anchor - si.start_sec) < 0.1:
                        anchor_types.append("稳定岛起始")
                    elif abs(anchor - si.end_sec) < 0.1:
                        anchor_types.append("稳定岛结束")
                for au in result.action_units:
                    if abs(anchor - au.start_sec) < 0.1:
                        anchor_types.append("动作单元起始")
                    elif abs(anchor - au.end_sec) < 0.1:
                        anchor_types.append("动作单元结束")
            
            packages.append(ConflictPackage(
                conflict_unit_id=result.unit_id,
                conflict_timeline=result.timeline,
                vision_anchors=result.vision_anchors,
                vision_anchor_types=anchor_types,
                conflict_reason=result.abnormal_reason or "",
                vision_valid_timelines=valid_timelines
            ))
        
        return packages
    
    # =========================================================================
    # 输出序列化
    # =========================================================================
    
    def to_dict(self, result: CVValidationResult) -> Dict[str, Any]:
        """转换为可序列化字典"""
        return {
            "unit_id": result.unit_id,
            "timeline": list(result.timeline),
            "vision_stats": {
                "s_stable": result.vision_stats.s_stable,
                "s_action": result.vision_stats.s_action,
                "s_redundant": result.vision_stats.s_redundant,
                "p_abstract": result.vision_stats.p_abstract,
                "p_concrete": result.vision_stats.p_concrete,
                "p_process": result.vision_stats.p_process
            },
            "main_vision_type": result.main_vision_type.value,
            "stable_islands": [
                {"start": s.start_sec, "end": s.end_sec, "ssim": s.avg_ssim}
                for s in result.stable_islands
            ],
            "action_units": [
                {"start": a.start_sec, "end": a.end_sec, "diff_ratio": a.avg_diff_ratio}
                for a in result.action_units
            ],
            "redundancy_segments": [
                {"start": r.start_sec, "end": r.end_sec, 
                 "type": r.redundancy_type.value, "action": r.processing_action}
                for r in result.redundancy_segments
            ],
            "vision_anchors": result.vision_anchors,
            "is_normal": result.is_normal,
            "abnormal_type": result.abnormal_type,
            "abnormal_reason": result.abnormal_reason
        }
