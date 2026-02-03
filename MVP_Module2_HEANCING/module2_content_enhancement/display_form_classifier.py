"""
Display Form Classifier - Phase 2: Semantic Unit Display Form Validation

基于CV特征分类视频画面的展示形式：
- PPT: 大面积单色背景 + 矩形文本框 + 结构化元素
- 实操 (hands_on): 代码编辑器/IDE界面特征 + 高文本密度
- 口头 (talking): 低边缘密度 + 低复杂度背景  
- 动画 (animation): 连续帧高变化率 + 非静态结构

与 SemanticUnitSegmenter 集成，确保语义单元内展示形式单一。
"""

import cv2
import numpy as np
import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# Enums and Data Structures
# =============================================================================

class DisplayForm(Enum):
    """展示形式枚举"""
    PPT = "ppt"              # PPT 幻灯片展示
    HANDS_ON = "hands_on"    # 实操演示 (代码/软件操作)
    TALKING = "talking"      # 纯口头讲解 (人物为主)
    ANIMATION = "animation"  # 动画展示
    MIXED = "mixed"          # 混合/无法确定
    

@dataclass
class DisplayFormResult:
    """单帧/区间的展示形式分析结果"""
    display_form: DisplayForm
    confidence: float
    features: Dict[str, Any]  # 支撑判定的CV特征
    

@dataclass
class DisplayFormSwitchPoint:
    """展示形式切换点"""
    timestamp_sec: float
    from_form: DisplayForm
    to_form: DisplayForm
    confidence: float


# =============================================================================
# Display Form Classifier
# =============================================================================

class DisplayFormClassifier:
    """
    展示形式分类器
    
    基于 CV 特征对视频帧进行分类：
    - PPT: 高矩形数 + 低边缘密度 + 大面积背景
    - 实操: 高边缘密度 + 高文本密度 + IDE特征
    - 口头: 低边缘密度 + 无结构化元素
    - 动画: 连续帧差异大 + 非静态结构
    """
    
    # 分类阈值 (可调参)
    PPT_RECT_THRESHOLD = 3           # PPT 至少有3个矩形框
    PPT_EDGE_DENSITY_MAX = 0.15      # PPT 边缘密度通常较低
    HANDS_ON_EDGE_DENSITY_MIN = 0.08 # 实操边缘密度较高
    HANDS_ON_LINE_THRESHOLD = 20     # 实操有大量直线(代码行)
    TALKING_EDGE_DENSITY_MAX = 0.05  # 口头边缘密度很低
    ANIMATION_FRAME_DIFF_MIN = 0.1   # 动画帧差变化大
    
    def __init__(self, video_path: str = None):
        """
        Args:
            video_path: 视频文件路径 (用于帧提取)
        """
        self.video_path = video_path
        self.cap = None
        self._init_video()
        
        # 导入视觉元素检测器
        from module2_content_enhancement.visual_element_detection_helpers import VisualElementDetector
        self.detector = VisualElementDetector
    
    def _init_video(self):
        """初始化视频读取器"""
        if self.video_path:
            self.cap = cv2.VideoCapture(self.video_path)
            if not self.cap.isOpened():
                logger.warning(f"Failed to open video: {self.video_path}")
                self.cap = None
    
    def close(self):
        """释放资源"""
        if self.cap:
            self.cap.release()
            self.cap = None
    
    def classify_frame(self, frame: np.ndarray) -> DisplayFormResult:
        """
        对单帧进行展示形式分类
        
        Args:
            frame: BGR格式的视频帧
            
        Returns:
            DisplayFormResult
        """
        # 使用现有检测器提取特征
        features = self.detector.analyze_frame(frame)
        
        # 判定逻辑
        display_form, confidence = self._classify_by_features(features)
        
        return DisplayFormResult(
            display_form=display_form,
            confidence=confidence,
            features=features
        )
    
    def classify_at_timestamp(self, timestamp_sec: float) -> DisplayFormResult:
        """
        对指定时间戳的帧进行分类
        
        Args:
            timestamp_sec: 时间戳(秒)
            
        Returns:
            DisplayFormResult
        """
        if not self.cap:
            return DisplayFormResult(
                display_form=DisplayForm.MIXED,
                confidence=0.0,
                features={"error": "Video not loaded"}
            )
        
        # 跳转到指定时间
        self.cap.set(cv2.CAP_PROP_POS_MSEC, timestamp_sec * 1000)
        ret, frame = self.cap.read()
        
        if not ret or frame is None:
            return DisplayFormResult(
                display_form=DisplayForm.MIXED,
                confidence=0.0,
                features={"error": "Frame read failed"}
            )
        
        return self.classify_frame(frame)
    
    def classify_segment(
        self,
        start_sec: float,
        end_sec: float,
        sample_interval: float = 2.0
    ) -> Tuple[DisplayForm, float, List[DisplayFormSwitchPoint]]:
        """
        对时间段进行分类，检测展示形式切换点
        
        Args:
            start_sec: 起始时间(秒)
            end_sec: 结束时间(秒)
            sample_interval: 采样间隔(秒)
            
        Returns:
            (主要展示形式, 置信度, 切换点列表)
        """
        if not self.cap:
            return DisplayForm.MIXED, 0.0, []
        
        # 采样并分类
        sample_results = []
        current_time = start_sec
        
        while current_time <= end_sec:
            result = self.classify_at_timestamp(current_time)
            sample_results.append((current_time, result))
            current_time += sample_interval
        
        if not sample_results:
            return DisplayForm.MIXED, 0.0, []
        
        # 统计各展示形式出现次数
        form_counts = {}
        for _, result in sample_results:
            form = result.display_form
            form_counts[form] = form_counts.get(form, 0) + 1
        
        # 找出主要展示形式
        dominant_form = max(form_counts, key=form_counts.get)
        dominant_ratio = form_counts[dominant_form] / len(sample_results)
        
        # 检测切换点
        switch_points = []
        for i in range(1, len(sample_results)):
            prev_time, prev_result = sample_results[i-1]
            curr_time, curr_result = sample_results[i]
            
            if prev_result.display_form != curr_result.display_form:
                switch_point = DisplayFormSwitchPoint(
                    timestamp_sec=(prev_time + curr_time) / 2,  # 取中点作为切换时间
                    from_form=prev_result.display_form,
                    to_form=curr_result.display_form,
                    confidence=(prev_result.confidence + curr_result.confidence) / 2
                )
                switch_points.append(switch_point)
        
        return dominant_form, dominant_ratio, switch_points
    
    def _classify_by_features(self, features: Dict[str, Any]) -> Tuple[DisplayForm, float]:
        """
        基于CV特征进行分类判定
        
        决策树逻辑:
        1. 高矩形 + 低边缘密度 → PPT
        2. 高边缘密度 + 高直线数 → 实操
        3. 低边缘密度 + 无结构元素 → 口头
        4. 其他 → 混合
        """
        rect_count = features.get("rectangles", 0)
        edge_density = features.get("edge_density", 0.0)
        line_count = features.get("lines", 0)
        has_architecture = features.get("has_architecture_elements", False)
        has_table = features.get("has_table", False)
        has_math = features.get("has_math_formula", False)
        total_elements = features.get("total", 0)
        
        # PPT判定: 多矩形 + 低边缘密度 + 有结构化元素
        if (rect_count >= self.PPT_RECT_THRESHOLD and 
            edge_density < self.PPT_EDGE_DENSITY_MAX and
            (has_architecture or has_table or has_math)):
            confidence = min(0.95, 0.6 + rect_count * 0.05 + (0.1 if has_table else 0))
            return DisplayForm.PPT, confidence
        
        # 实操判定: 高边缘密度 + 大量直线 (代码界面)
        if (edge_density >= self.HANDS_ON_EDGE_DENSITY_MIN and
            line_count >= self.HANDS_ON_LINE_THRESHOLD):
            confidence = min(0.9, 0.5 + edge_density * 2 + line_count * 0.01)
            return DisplayForm.HANDS_ON, confidence
        
        # 口头判定: 低边缘密度 + 无结构化元素
        if (edge_density < self.TALKING_EDGE_DENSITY_MAX and
            total_elements < 3 and
            not has_architecture):
            confidence = max(0.6, 0.9 - edge_density * 10)
            return DisplayForm.TALKING, confidence
        
        # 如果有结构化元素但不符合PPT标准，可能是动画或混合
        if has_architecture or has_math:
            return DisplayForm.PPT, 0.6  # 倾向于PPT但置信度低
        
        # 默认混合
        return DisplayForm.MIXED, 0.5
    
    def detect_animation(
        self,
        start_sec: float,
        end_sec: float,
        sample_count: int = 5
    ) -> Tuple[bool, float]:
        """
        检测时间段内是否为动画 (基于帧间差异)
        
        Args:
            start_sec: 起始时间
            end_sec: 结束时间
            sample_count: 采样帧数
            
        Returns:
            (是否为动画, 置信度)
        """
        if not self.cap or sample_count < 2:
            return False, 0.0
        
        duration = end_sec - start_sec
        interval = duration / (sample_count - 1)
        
        frames = []
        for i in range(sample_count):
            t = start_sec + i * interval
            self.cap.set(cv2.CAP_PROP_POS_MSEC, t * 1000)
            ret, frame = self.cap.read()
            if ret and frame is not None:
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                frames.append(gray)
        
        if len(frames) < 2:
            return False, 0.0
        
        # 计算帧间差异
        diffs = []
        for i in range(1, len(frames)):
            diff = cv2.absdiff(frames[i], frames[i-1])
            diff_ratio = np.sum(diff > 30) / diff.size  # 变化像素比例
            diffs.append(diff_ratio)
        
        avg_diff = np.mean(diffs) if diffs else 0.0
        
        # 判定动画
        is_animation = avg_diff >= self.ANIMATION_FRAME_DIFF_MIN
        confidence = min(0.95, avg_diff * 5) if is_animation else max(0.3, 1 - avg_diff * 10)
        
        return is_animation, confidence


# =============================================================================
# Integration with SemanticUnitSegmenter
# =============================================================================

def validate_display_form_consistency(
    semantic_units: List[Dict[str, Any]],
    video_path: str,
    sentence_timestamps: Dict[str, Dict[str, float]] = None
) -> List[Dict[str, Any]]:
    """
    验证语义单元的展示形式一致性，不一致时拆分
    
    Args:
        semantic_units: 语义单元列表 (来自 Phase 1)
        video_path: 视频文件路径
        sentence_timestamps: 句子时间戳映射
        
    Returns:
        更新后的语义单元列表 (可能拆分)
    """
    classifier = DisplayFormClassifier(video_path)
    result_units = []
    
    try:
        for unit in semantic_units:
            start_sec = unit.get("start_sec", 0.0)
            end_sec = unit.get("end_sec", 0.0)
            
            if end_sec <= start_sec:
                # 无有效时间范围，保持原样
                unit["display_form"] = "unknown"
                result_units.append(unit)
                continue
            
            # 分类并检测切换点
            dominant_form, confidence, switch_points = classifier.classify_segment(
                start_sec, end_sec
            )
            
            if not switch_points:
                # 无切换点，直接更新展示形式
                unit["display_form"] = dominant_form.value
                unit["display_form_confidence"] = confidence
                result_units.append(unit)
            else:
                # 有切换点，需要拆分
                logger.info(f"Unit {unit.get('unit_id')} has {len(switch_points)} switch points, splitting...")
                
                # 简化处理：取第一个切换点拆分
                # 完整实现可以递归拆分
                switch = switch_points[0]
                
                # 拆分为两个单元
                unit1 = unit.copy()
                unit1["unit_id"] = unit["unit_id"] + "a"
                unit1["end_sec"] = switch.timestamp_sec
                unit1["display_form"] = switch.from_form.value
                
                unit2 = unit.copy()
                unit2["unit_id"] = unit["unit_id"] + "b"
                unit2["start_sec"] = switch.timestamp_sec
                unit2["display_form"] = switch.to_form.value
                
                # 文本拆分 (简化: 按比例)
                total_duration = end_sec - start_sec
                split_ratio = (switch.timestamp_sec - start_sec) / total_duration if total_duration > 0 else 0.5
                
                text = unit.get("text", "")
                split_pos = int(len(text) * split_ratio)
                unit1["text"] = text[:split_pos]
                unit2["text"] = text[split_pos:]
                
                result_units.extend([unit1, unit2])
                
    finally:
        classifier.close()
    
    return result_units


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    import sys
    import os
    import json
    
    # Add parent directory to path for script mode
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) < 3:
        print("Usage: python display_form_classifier.py <video_path> <timestamp_sec>")
        print("   or: python display_form_classifier.py <video_path> <start_sec> <end_sec>")
        sys.exit(1)
    
    video_path = sys.argv[1]
    classifier = DisplayFormClassifier(video_path)
    
    try:
        if len(sys.argv) == 3:
            # 单帧分类
            timestamp = float(sys.argv[2])
            result = classifier.classify_at_timestamp(timestamp)
            print(f"Timestamp: {timestamp}s")
            print(f"Display Form: {result.display_form.value}")
            print(f"Confidence: {result.confidence:.2f}")
            print(f"Features: {json.dumps(result.features, indent=2, default=str)}")
        else:
            # 时间段分类
            start_sec = float(sys.argv[2])
            end_sec = float(sys.argv[3])
            dominant, confidence, switches = classifier.classify_segment(start_sec, end_sec)
            print(f"Segment: [{start_sec}s - {end_sec}s]")
            print(f"Dominant Form: {dominant.value}")
            print(f"Confidence: {confidence:.2f}")
            print(f"Switch Points: {len(switches)}")
            for sp in switches:
                print(f"  - {sp.timestamp_sec:.1f}s: {sp.from_form.value} → {sp.to_form.value}")
    finally:
        classifier.close()
