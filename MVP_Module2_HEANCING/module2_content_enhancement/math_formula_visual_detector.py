
import cv2
import numpy as np
import logging
from typing import Tuple, List, Dict, Optional, Any
from .visual_element_detection_helpers import VisualElementDetector
# from .ocr_utils import OCRExtractor # Delayed import to avoid circular dep

logger = logging.getLogger(__name__)

import json
from pathlib import Path
from .ocr_utils import ThreadSafeMathOCR

class MathSymbolMatcher:
    def __init__(self):
        self.templates = {
            "sqrt": [np.array([[0,15], [5,25], [15,0], [40,0]], dtype=np.int32)],
            "fraction_line": [np.array([[0,0], [50,0]], dtype=np.int32)]
        }
    def match_symbol(self, contour: np.ndarray, symbol_type: str) -> float:
        if symbol_type not in self.templates: return 1.0
        best_score = 1.0
        for tp in self.templates[symbol_type]:
            score = cv2.matchShapes(contour, tp, cv2.CONTOURS_MATCH_I2, 0.0)
            if score < best_score: best_score = score
        return best_score


class MathFormulaVisualDetector:
    """
    数学公式多模态判定器 (V4 - First Principles & Hybrid Recognition)
    
    核心特性:
    1. 4层漏斗过滤 (ROI -> 符号 -> 结构 -> 语义)
    2. 分辨率归一化 (res_factor)
    3. 混合匹配 (Hu矩 + 模板库)
    4. 重心锚定空间校验 (Gravity Anchor)
    """
    def __init__(self, visual_extractor):
        self.visual_extractor = visual_extractor
        self.visual_helper = VisualElementDetector() # 原子算子库
        self.ocr_engine = ThreadSafeMathOCR()
        
    def analyze_formula_confidence(self, frame: np.ndarray, fault_text: str = "") -> Dict:
        """
        V4 核心入口: 计算公式置信度及其分类 (Static/Dynamic)
        """
        h, w = frame.shape[:2]
        res_factor = w / 1920.0 # 1080P 基准
        
        logger.info(f"Math-V4 Analysis: Frame {w}x{h}, res_factor={res_factor:.2f}")
        
        # 1. L1: 候选区域筛选 (ROI Filter)
        candidates = self._filter_candidate_regions(frame, res_factor)
        if not candidates:
            logger.info("L1: No math candidates found.")
            return {"is_formula": False, "confidence": 0.0}
            
        max_score = 0.0
        best_region_data = None
        
        for region_img, bbox in candidates:
            # 2. L2 & L3: 深度特征评分
            s1 = self._calculate_symbol_score(region_img, res_factor)      # 符号特征 (40%)
            s2 = self._calculate_spatial_score(region_img, res_factor)     # 空间结构 (40%)
            
            # 3. L4: 语义校验 (方案A: 局部 OCR)
            ocr_results = self.ocr_engine.recognize_math(region_img)
            s3 = self._calculate_semantic_score(ocr_results, fault_text)   # 语义关联 (20%)
            
            # 综合评分 (Final_Score = 0.4*S1 + 0.4*S2 + 0.2*S3)
            final_score = 0.4 * s1 + 0.4 * s2 + 0.2 * s3
            
            if final_score > max_score:
                max_score = final_score
                best_region_data = {
                    "score": final_score,
                    "s1": s1, "s2": s2, "s3": s3,
                    "bbox": bbox,
                    "text": ocr_results[0]["text"] if ocr_results else ""
                }
                

        is_formula = max_score >= 40.0 # V4 判定阈值
        logger.info(f"Math-V4 Result: Max Score={max_score:.2f}, Is Formula={is_formula}")
        if is_formula and best_region_data:
            logger.info(f"Best ROI: {best_region_data['bbox']}, Text: {best_region_data['text']}, S1={best_region_data['s1']}, S2={best_region_data['s2']}, S3={best_region_data['s3']}")
        
        return {
            "is_formula": is_formula,
            "confidence": max_score,
            "details": best_region_data
        }

    def _filter_candidate_regions(self, frame: np.ndarray, res_factor: float) -> List[Tuple[np.ndarray, Tuple[int, int, int, int]]]:
        """L1: 基础过滤 (尺寸/密度/方差)"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        low, high = self._get_adaptive_canny_thresholds(gray, res_factor)
        edges = cv2.Canny(gray, low, high)
        
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        candidates = []
        
        for cnt in contours:
            x, y, w_box, h_box = cv2.boundingRect(cnt)
            area = w_box * h_box
            if not (50 * res_factor <= w_box <= 1000 * res_factor and 20 * res_factor <= h_box <= 500 * res_factor):
                continue
            region_edges = edges[y:y+h_box, x:x+w_box]
            density = np.sum(region_edges > 0) / (area + 1e-6)
            if not (0.05 <= density <= 0.95): continue
            region_gray = gray[y:y+h_box, x:x+w_box]
            if np.var(region_gray) < 40: continue
            expand = int(5 * res_factor)
            ex1, ey1 = max(0, x - expand), max(0, y - expand)
            ex2, ey2 = min(frame.shape[1], x + w_box + expand), min(frame.shape[0], y + h_box + expand)
            candidates.append((frame[ey1:ey2, ex1:ex2], (ex1, ey1, ex2, ey2)))
        return candidates

    def _get_adaptive_canny_thresholds(self, gray: np.ndarray, res_factor: float) -> Tuple[int, int]:
        img_mean = np.mean(gray)
        base_low, base_high = 50, 150
        dyn_low, dyn_high = int(base_low * res_factor), int(base_high * res_factor)
        adapt_low, adapt_high = max(10, int(img_mean * 0.3)), max(30, int(img_mean * 0.9))
        return max(dyn_low, adapt_low), max(dyn_high, adapt_high)

    def _calculate_symbol_score(self, region: np.ndarray, res_factor: float) -> float:
        """S1: 符号特征 (40%权重) - 混合匹配层"""
        gray = cv2.cvtColor(region, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 50, 150)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        score = 0.0
        matcher = MathSymbolMatcher()
        
        # 1. 检测分式线
        frac_hit = False
        lines = self.visual_helper.detect_lines_p(edges, min_length=int(20*res_factor))
        for x1, y1, x2, y2 in lines:
            length = np.sqrt((x2-x1)**2 + (y2-y1)**2)
            if (abs(y2-y1) <= 3 * res_factor and length >= 20 * res_factor):
                roi_up = edges[max(0, y1-int(10*res_factor)):y1, x1:x2]
                roi_down = edges[y1+3:min(edges.shape[0], y1+int(10*res_factor)), x1:x2]
                if np.sum(roi_up) > 0 and np.sum(roi_down) > 0:
                    score += 40
                    frac_hit = True; break
        
        # 2. 根号检测 (Hu矩)
        sqrt_hit = False
        for cnt in contours:
            if cv2.contourArea(cnt) < 50 * res_factor: continue
            if matcher.match_symbol(cnt, "sqrt") < 0.3: # 匹配阈值
                score += 40
                sqrt_hit = True; break
                
        # 3. 关系符号命中
        if not frac_hit and not sqrt_hit:
            # 弱匹配: 如果有多个小轮廓且排列规则
            if len(contours) >= 3: score += 20

        return min(100.0, score)

    def _calculate_spatial_score(self, region: np.ndarray, res_factor: float) -> float:
        """S2: 空间结构 (40%权重)"""
        gray = cv2.cvtColor(region, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 50, 150)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if len(contours) < 2: return 0.0
        
        areas = [cv2.contourArea(c) for c in contours]
        main_idx = np.argmax(areas)
        main_h = cv2.boundingRect(contours[main_idx])[3]
        anchor_center = self.visual_helper.calculate_gravity_center(contours[main_idx])
        if not anchor_center: return 0.0
        
        score = 0.0
        for i, cnt in enumerate(contours):
            if i == main_idx: continue
            center = self.visual_helper.calculate_gravity_center(cnt)
            if center and abs(center[1] - anchor_center[1]) / (main_h + 1e-6) >= 0.25:
                score += 30
                
        _, hierarchy = cv2.findContours(edges, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
        if hierarchy is not None:
            max_depth = 0
            for h_item in hierarchy[0]:
                depth, temp = 0, h_item
                while temp[3] != -1: 
                    depth += 1
                    temp = hierarchy[0][temp[3]]
                max_depth = max(max_depth, depth)
            score += max_depth * 20
        return min(100.0, score)

    def _calculate_semantic_score(self, ocr_results: List[Dict], fault_text: str) -> float:
        """S3: 语义关联 (20%权重)"""
        if not ocr_results: return 0.0
        text, base_score = ocr_results[0]["text"], ocr_results[0]["score"]
        score = base_score * 50
        math_chars = r"√∫∑∏+-×÷=<>≤≥≠()[]{}^_/\\"
        symbol_count = sum(1 for char in text if char in math_chars)
        if len(text) > 0:
            ratio = symbol_count / len(text)
            if 0.2 <= ratio <= 0.8: score += 30
        domain_keywords = ["积分", "根号", "推导", "公式", "矩阵", "分式", "等于", "变换"]
        if any(kw in fault_text for kw in domain_keywords): score += 20
        return min(100.0, score)

    def is_static_formula(self, frame: np.ndarray) -> tuple[bool, float]:
        res = self.analyze_formula_confidence(frame)
        return res["is_formula"], res["confidence"]

    def detect_formula_element_displacement(self, frames: List[np.ndarray]) -> bool:
        """检测公式元素的重心位移，判断是否为动画场景"""
        if len(frames) < 3: return False
        
        # 提取每帧公式区域的轮廓重心
        centroids = []
        res_factor = self._get_resolution_factor(frames[0])
        
        for frame in frames:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            low, high = self._get_adaptive_canny_thresholds(gray, res_factor)
            edges = cv2.Canny(gray, low, high)
            contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if not contours: continue
            
            # 计算所有轮廓的平均重心
            cx_sum, cy_sum, cnt_valid = 0, 0, 0
            for cnt in contours:
                M = cv2.moments(cnt)
                if M["m00"] == 0: continue
                cx = M["m10"] / M["m00"]
                cy = M["m01"] / M["m00"]
                cx_sum += cx; cy_sum += cy; cnt_valid += 1
                
            if cnt_valid > 0:
                centroids.append((cx_sum/cnt_valid, cy_sum/cnt_valid))
            else:
                 centroids.append(None)

        # 计算相邻帧重心的欧氏距离
        displacements = []
        for i in range(1, len(centroids)):
            if centroids[i] and centroids[i-1]:
                dx = centroids[i][0] - centroids[i-1][0]
                dy = centroids[i][1] - centroids[i-1][1]
                dist = np.sqrt(dx**2 + dy**2)
                displacements.append(dist)
        
        if not displacements: return False
        
        # 判定条件：平均位移＞阈值（随分辨率缩放）且变化率均匀
        avg_displacement = np.mean(displacements)
        # displacement_var = np.var(displacements) # Variance check can be strict for complex animations
        displacement_threshold = 5 * res_factor
        
        # logger.info(f"Math-V5 Anim Displace: Avg={avg_displacement:.2f} (Thresh {displacement_threshold})")
        return avg_displacement > displacement_threshold

    def detect_formula_element_fade(self, frames: List[np.ndarray]) -> bool:
        """检测公式元素的显隐变化，判断是否为动画场景"""
        if len(frames) < 3: return False
        
        # 计算每帧公式区域的像素均值（代表透明度变化）
        # Focus on ROI if possible, otherwise full frame (can be noisy)
        # Using full frame means we rely on significant content changes
        pixel_means = [np.mean(cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)) for frame in frames]
        
        # 计算相邻帧的像素均值变化率
        mean_changes = []
        for i in range(1, len(pixel_means)):
             if pixel_means[i-1] > 0:
                 change = abs(pixel_means[i] - pixel_means[i-1]) / pixel_means[i-1]
                 mean_changes.append(change)
        
        if not mean_changes: return False
        
        avg_change_rate = np.mean(mean_changes)
        # logger.info(f"Math-V5 Anim Fade: AvgRate={avg_change_rate:.4f}")
        
        # 判定条件：平均变化率＞阈值 (0.005, ie 0.5%) 且变化规律 (Not instant flash)
        return avg_change_rate > 0.005 and avg_change_rate < 0.1

    def _get_resolution_factor(self, frame):
        return frame.shape[1] / 1920.0

    def is_dynamic_formula(self, frames: List[np.ndarray]) -> tuple[bool, float]:
        """
        V5.2 Upgrade: Subtle Animation Intensity-Adaptive Logic
        Ref: 51.80s First Principles Analysis
        
        Logic:
        1. Multi-scale Flux: Instant (avg_mse) + Cumulative (Long-term MSE) + Local (Sliding Window)
        2. Scene Adaptive: Fast dynamics (focus on Instant) vs Subtle dynamics (focus on Cumulative)
        3. Semantic Guard: SSIM check to ensure structural evolution continuity
        """
        if len(frames) < 3: return False, 0.0
        
        # --- Stage 1: Multi-scale Flux Calculation ---
        # 1.1 Instantaneous Flux (Frame-by-frame)
        mse_changes = [self.visual_extractor.calculate_mse_diff(frames[i], frames[i+1]) for i in range(len(frames)-1)]
        avg_mse = np.mean(mse_changes)
        
        # 1.2 Long-term Flux (Sovereign anchor comparison)
        long_term_mse = self.visual_extractor.calculate_mse_diff(frames[0], frames[-1])
        
        # 1.3 Sliding Window Cumulative Flux (V5.2 Best Practice)
        # Window size = ~0.3s (approx 3-10 frames depending on sampling)
        win_size = max(3, len(frames) // 3)
        window_cumulative_mses = []
        for i in range(win_size, len(frames), win_size // 2): # Sliding window
             win_mse = self.visual_extractor.calculate_mse_diff(frames[0], frames[i])
             window_cumulative_mses.append(win_mse)
        max_window_mse = max(window_cumulative_mses) if window_cumulative_mses else long_term_mse

        # --- Stage 2: Scene-Adaptive Thresholding ---
        # Classify if this is a "Fast Dynamic" scene (High instant flux)
        is_fast_scene = avg_mse > 500
        
        # Baseline thresholds (Adapted from First Principles)
        # Slow/Subtle Scene: Rely on Cumulative (LT-MSE >= 2.0 or Win-MSE >= 1.5)
        # Fast Scene: Rely on Instant (Avg-MSE >= 250)
        
        # --- Stage 3: Feature Detectors ---
        has_displacement = self.detect_formula_element_displacement(frames)
        has_fade = self.detect_formula_element_fade(frames)
        
        # --- Stage 4: Semantic Guard (SSIM) ---
        # SSIM confirmed semantic continuity (Structural evolution)
        ssim = self.visual_extractor.calculate_ssim(frames[0], frames[-1])
        is_structurally_consistent = ssim >= 0.6 # V5.2 Guideline: 0.6 threshold

        # --- Stage 5: Decision Matrix (V5.2 Weighted) ---
        is_dynamic = False
        decision_score = 0.0
        reason = ""

        if is_fast_scene:
            # Fast Dynamics (Active handwriting/re-layout)
            if avg_mse > 300 or has_displacement:
                is_dynamic = True
                decision_score = min(1.0, avg_mse / 1000.0)
                reason = "Fast Dynamics detected (High Avg-MSE)"
        else:
            # Subtle Dynamics (Slow handwriting/transition)
            # Thresholds: LT-MSE >= 2.0 is strong, >= 1.5 is moderate
            if (long_term_mse >= 2.0 or max_window_mse >= 1.8) and is_structurally_consistent:
                is_dynamic = True
                decision_score = 0.6 if long_term_mse > 2.0 else 0.5
                reason = "Subtle Dynamics detected (High Long-term/Window Flux)"
            elif has_displacement and is_structurally_consistent:
                is_dynamic = True
                decision_score = 0.7
                reason = "Logical Displacement detected (Semantic Evolution)"

        # Handwriting Context Override (Legacy OCR Match)
        if not is_dynamic and (avg_mse > 150 or long_term_mse > 1.5):
             try:
                 ocr_start = self.ocr_engine.recognize_math(frames[0])
                 ocr_end = self.ocr_engine.recognize_math(frames[-1])
                 if ocr_start and ocr_end:
                     from .ocr_utils import OCRExtractor
                     match_rate = OCRExtractor().calculate_text_match_rate(ocr_start[0]["text"], ocr_end[0]["text"])
                     if match_rate < 0.8: # Semantic change detected
                         is_dynamic = True
                         decision_score = max(decision_score, 1.0 - match_rate)
                         reason = f"OCR Semantic Evolution (Match: {match_rate:.2f})"
             except: pass

        # Final Trace Logging (Consistency Feedback per V5.2 Recommendation)
        logger.info(f"[V5.2 Decision Trace] Anim: {is_dynamic} | Reason: {reason} | Score: {decision_score:.2f} | "
                    f"Inst-MSE: {avg_mse:.2f} | LT-MSE: {long_term_mse:.2f} | Win-MSE: {max_window_mse:.2f} | SSIM: {ssim:.2f}")

        # V6 Combination Hint: if dynamic, we usually want VIDEO_AND_SCREENSHOT
        return is_dynamic, max(decision_score, 0.8 if (has_displacement or has_fade) and is_dynamic else decision_score)

    def classify_dynamic_type(self, frames: List[np.ndarray], avg_mse: float, duration: float, ssim_seq: List[float], edge_flux: float) -> str:
        """
        V5: First Principles Classification of Dynamic Type
        Returns: 'DERIVATION' | 'TRANSITION' | 'STATIC' | 'UNKNOWN'
        
        Logic:
        1. TRANSITION: 
           - Global structural change (SSIM drops < 0.8)
           - Short duration (< 1.5s)
           - High MSE peak (Instant change)
        2. DERIVATION:
           - Local structural continuity (SSIM > 0.95)
           - Continuous edge growth (edge_flux > 0.1)
           - Sufficient duration (> 2.0s)
        """
        if len(frames) < 2: return "STATIC"
        
        # 1. Transition Check (Global Mutation)
        min_ssim = min(ssim_seq) if ssim_seq else 1.0
        if min_ssim < 0.8 and duration < 1.5:
             logger.info(f"V5 Classify: TRANSITION (Low SSIM {min_ssim:.2f}, Short Duration {duration:.2f}s)")
             return "TRANSITION"
             
        # 2. Derivation Check (Local Evolution)
        # Edge flux indicates writing activity.
        # SSIM should remain relatively high (background stable).
        # MSE should be moderate (not explosion).
        if duration >= 2.0 and min_ssim > 0.9 and edge_flux > 0.05: # Threshold tuned for writing
             logger.info(f"V5 Classify: DERIVATION (Stable SSIM {min_ssim:.2f}, Edge Flux {edge_flux:.2f}, Duration {duration:.2f}s)")
             return "DERIVATION"
             
        # 3. Fallback / Refinement
        if avg_mse < 50: return "STATIC"
        
        return "UNKNOWN"


    def is_static_result(self, frames: List[np.ndarray]) -> bool:
        """
        V6 Result Check: 判断动态过程后是否存在稳定结果帧
        Criteria:
        1. Low Flux (MSE < Threshold)
        2. High Sharpness (Optional but good)
        3. Sufficient Duration (> 0.3s)
        """
        # V6 Fix: Only check the "Result" part (e.g. last 1 second or 30%)
        # Determine check window
        scan_len = max(int(len(frames) * 0.3), int(self.visual_extractor.fps * 1.0))
        scan_len = min(scan_len, len(frames))
        
        if scan_len < 3: return False
        
        result_frames = frames[-scan_len:]
        
        mse_changes = [self.visual_extractor.calculate_mse_diff(result_frames[i], result_frames[i+1]) for i in range(len(result_frames)-1)]
        avg_mse = np.mean(mse_changes)
        
        # Stability check
        # For a static result, flux should be low
        if avg_mse > 300: # Slightly relaxed threshold for "stable enough" result
            return False
            
        return True

    def semantic_visual_match(self, visual_result: str, fault_text: str, semantic_extractor: Any = None) -> Dict:
        """
        语义-视觉匹配验证 (V4)
        """
        static_keywords = ["结构", "组成", "分子", "分母", "式子", "含义", "部分", "布局", "行列", "定义", "概念", "图示", "架构"]
        dynamic_keywords = ["推导", "变形", "步骤", "演算", "过程", "怎么", "如何", "变化", "生成", "得到", "执行", "操作", "流转", "计算", "证明"]
        
        sem_static = any(k in fault_text for k in static_keywords)
        sem_dynamic = any(k in fault_text for k in dynamic_keywords)
        
        demand_type = "mixed"
        if sem_static and not sem_dynamic: demand_type = "static"
        if sem_dynamic and not sem_static: demand_type = "dynamic"
        
        match = (visual_result == demand_type or demand_type == "mixed")
        
        return {
            "match": match,
            "final_type": visual_result,
            "demand_type": demand_type
        }
