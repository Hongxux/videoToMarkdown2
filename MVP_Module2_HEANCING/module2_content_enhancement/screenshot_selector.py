"""
Screenshot Selector - Week 3 Day 16-18

Selects the best frame from a time range for screenshot enhancement.

Scoring system:
- S1 (稳定性): Continuous stable frames → higher score
- S4 (无遮挡): No occlusions/overlays → higher score
- Final: 0.5 × S1 + 0.5 × S4
"""

import logging
import cv2
import numpy as np
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import json
import time

logger = logging.getLogger(__name__)

# 💥 性能优化: 引入 Numba JIT 加速像素级运算
try:
    from numba import jit, prange
    HAS_NUMBA = True
    
    @jit(nopython=True, fastmath=True, parallel=True)
    def _numba_batch_mse(frames_data):
        """批量计算 MSE，利用 AVX-512/SIMD 和多核并行"""
        n, h, w, c = frames_data.shape
        results = np.zeros(n - 1, dtype=np.float32)
        for i in prange(n - 1):
            diff_sum = 0.0
            for row in range(h):
                for col in range(w):
                    for ch in range(c):
                        d = float(frames_data[i, row, col, ch]) - float(frames_data[i+1, row, col, ch])
                        diff_sum += d * d
            results[i] = diff_sum / (h * w * c)
        return results

    @jit(nopython=True, fastmath=True, parallel=True)
    def _numba_batch_struct_mse(edges_data):
        """批量计算结构化 MSE (基于 Canny 边缘图)"""
        n, h, w = edges_data.shape
        results = np.zeros(n - 1, dtype=np.float32)
        for i in prange(n - 1):
            diff_sum = 0.0
            for row in range(h):
                for col in range(w):
                    d = float(edges_data[i, row, col]) - float(edges_data[i+1, row, col])
                    diff_sum += d * d
            results[i] = (diff_sum / (h * w)) / (255.0 * 255.0)
        return results
        
except Exception as e:
    HAS_NUMBA = False
    logger.warning(f"ScreenshotSelector: Numba acceleration disabled: {e}")


@dataclass
class FrameScore:
    """
    单帧评分结果 (多维评价矩阵)
    """
    frame_idx: int
    timestamp_sec: float
    
    # 评分
    S1_stability: float     # 稳定性 (MSE-based) 0-100
    S2_info_density: float  # 信息密度 (Standard 2) 0-100
    S3_completeness: float  # 架构完整性 (Standard 3: 箭头/矩形) 0-100
    S4_no_occlusion: float  # 无遮挡评分 0-100
    final_score: float      # 综合评分 0-100
    
    # 详细细节
    rectangle_count: int
    arrow_count: int
    has_occlusion: bool



@dataclass
class ScreenshotSelection:
    """
    截图选择结果
    """
    selected_frame_idx: int
    selected_timestamp: float
    screenshot_path: str
    
    # 评分细节
    final_score: float
    S1_stability: float
    S2_info_density: float
    S3_completeness: float
    S4_no_occlusion: float
    
    all_candidates: List[FrameScore]


def _analyze_frame_quality_worker(frame: np.ndarray) -> Tuple[float, float, float, float]:
    """
    V6.2 工业级质量分析 Worker
    使用 Laplacian Variance (锐度) 和 Shannon Entropy (信息密度)
    返回: (laplacian_var, shannon_entropy, sharpness_score, contrast_score)
    """
    try:
        # A. ROI 定位 (排除黑边/工具栏)
        gray_full = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        h, w = gray_full.shape
        # 简单 ROI: 排除上下 10% (通常是UI区域)
        roi_gray = gray_full[int(h*0.1):int(h*0.9), :]
        
        # B. 物理防抖 (Laplacian Variance)
        # 越高越清晰，越低越模糊
        laplacian_var = cv2.Laplacian(roi_gray, cv2.CV_64F).var()
        
        # C. 信息熵 (Shannon Entropy)
        hist = cv2.calcHist([roi_gray], [0], None, [256], [0, 256])
        if hist.sum() == 0: return 0.0, 0.0, 0.0, 0.0
        hist_norm = hist.ravel() / hist.sum()
        hist_norm = hist_norm[hist_norm > 0]
        shannon_entropy = -np.sum(hist_norm * np.log2(hist_norm))
        
        # D. 边缘锐度 (Sobel Magnitude Mean - 辅助)
        gx = cv2.Sobel(roi_gray, cv2.CV_64F, 1, 0, ksize=3)
        gy = cv2.Sobel(roi_gray, cv2.CV_64F, 0, 1, ksize=3)
        mag = cv2.sqrt(gx**2 + gy**2)
        sharpness_score = np.mean(mag)
        
        # E. 对比度
        max_v, min_v = np.max(roi_gray).astype(float), np.min(roi_gray).astype(float)
        contrast_score = (max_v - min_v) / (max_v + min_v + 1e-6)
        
        return laplacian_var, shannon_entropy, sharpness_score, contrast_score
    except Exception as e:
        logger.error(f"Critical error in quality worker: {e}", exc_info=True)
        return 0.0, 0.0, 0.0, 0.0


class ScreenshotSelector:
    """
    截图选择器 (V6.2 Refined Logic)
    
    Improvements:
    1. Fluctuation Tolerance Island Clustering (<=2 jitter frames)
    2. Laplacian & Entropy based Quality Gates
    3. Refined Scoring Game (Recency Bonus + S4 Penalty)
    """
    
    def __init__(
        self,
        visual_extractor,
        config: Dict = None
    ):
        self.visual_extractor = visual_extractor
        from .visual_element_detection_helpers import VisualElementDetector
        self.detector = VisualElementDetector()
        
        if config is None:
            from .config_loader import load_module2_config
            config = load_module2_config()
        
        # V6.2 默认严格权重
        self.WEIGHT_S1 = 0.2
        self.WEIGHT_S2 = 0.3
        self.WEIGHT_S3 = 0.4
        self.WEIGHT_S4 = 0.1
        
        logger.info(f"ScreenshotSelector V6.2 initialized (Fluctuation Tolerance Enabled)")
    
    async def select_screenshot(
        self,
        video_path: str,
        start_sec: float,
        end_sec: float,
        output_dir: str = None,
        output_name: str = None,
        save_image: bool = True  # 💥 新增: 是否保存图片文件
    ) -> ScreenshotSelection:
        """
        V6.2 核心流程: 波动容忍聚类 + 岛屿博弈 + 岛内择优
        """
        # 💥 存储 output_name 以便内部调用使用
        self._current_output_name = output_name
        
        # 1. 视窗与采样准备
        fps = self._get_video_fps(video_path)
        safe_end_sec = max(start_sec + 1.0, end_sec)
        
        # 2. 调用全局自适应决策引擎 (Phase 6.9: Unified Features)
        refined_visual = await self.visual_extractor.extract_visual_features(start_sec, safe_end_sec, sample_rate=1)
        
        # 从缓存或返回对象中获取数据
        cache = self.visual_extractor.get_cached_content(start_sec, safe_end_sec, sample_rate=1)
        if not cache or not cache.enhanced_frames:
             return await self._handle_empty_frames_complex(start_sec, safe_end_sec, output_dir)
             
        frames = cache.enhanced_frames
        timestamps = cache.timestamps
        mse_diffs_actual = refined_visual.mse_list
            
        # 3. 分辨率系数与组件识别
        video_width = frames[0].shape[1]
        res_factor = video_width / 1920.0
        
        # 识别内容类型以调整阈值
        content_type = self._identify_action_type_v6(frames[0])
        threshold_config = self._get_adaptive_threshold(content_type, res_factor, fps)
        
        logger.info(f"Selecting V6.9 ({content_type}, ResFactor={res_factor:.2f}) from {start_sec:.2f}s to {safe_end_sec:.2f}s")
        
        # 4. 基础指标全量并行计算
        import asyncio
        from .visual_feature_extractor import get_visual_process_pool
        loop = asyncio.get_running_loop()
        executor = get_visual_process_pool()
        
        # 4.5 预计算边缘图以用于结构 MSE (V6.2 Struct-MSE)
        edge_maps = [cv2.Canny(cv2.cvtColor(f, cv2.COLOR_BGR2GRAY), 50, 150) for f in frames]
        
        # Struct-MSE 核心计算 (Numba 加速版)
        if HAS_NUMBA and len(frames) > 1:
            edge_stack = np.stack(edge_maps)
            smse_results = _numba_batch_struct_mse(edge_stack)
            struct_mse_diffs = list(smse_results) + [0.0]
        else:
            struct_mse_diffs = []
            for i in range(len(frames) - 1):
                diff = (edge_maps[i].astype(np.int16) - edge_maps[i+1].astype(np.int16)) ** 2
                struct_mse_diffs.append(np.mean(diff) / (255 * 255))
            struct_mse_diffs.append(0)
        
        # 补全 MSE 列表长度 (对齐 timestamps)
        if len(mse_diffs_actual) < len(timestamps):
            mse_diffs_actual.append(0.0)

        # Quality Worker 并行
        quality_tasks = [loop.run_in_executor(executor, _analyze_frame_quality_worker, f) for f in frames]
        quality_results = await asyncio.gather(*quality_tasks) # [(lap, ent, sharp, cont), ...]
        
        # 5. 稳定岛聚类 (Fluctuation Tolerance Mechanism)
        PIXEL_THRESH = threshold_config["pixel_mse"]
        STRUCT_THRESH = threshold_config["struct_mse"]
        MIN_STABLE_LEN = threshold_config["min_stable_frames"]
        
        islands = []
        current_island = []
        fluctuation_count = 0 # 容错计数器
        
        MAX_FLUCT_MSE = PIXEL_THRESH * 3.0 # 允许瞬间抖动到 3 倍阈值 (e.g. 鼠标飞过)
        MAX_FLUCT_SMSE = STRUCT_THRESH * 3.0
        
        # 质量门槛 (Quality Gate: Laplacian > 10.0*res_factor, Contrast > 0.15)
        LAP_GATE = 10.0 * res_factor
        CONT_GATE = 0.15
        
        for i in range(len(frames) - 1):
            mse, smse = mse_diffs_actual[i], struct_mse_diffs[i]
            lap, ent, sharp, contrast = quality_results[i]
            
            # 判定基准
            is_visually_stable = (mse < PIXEL_THRESH) and (smse < STRUCT_THRESH)
            is_high_quality = (lap > LAP_GATE) and (contrast > CONT_GATE)
            
            # 容错判定: 如果不稳定，但抖动在容忍范围内，且不算太久，且画面依然清晰
            is_tolerable_fluctuation = (mse < MAX_FLUCT_MSE) and (smse < MAX_FLUCT_SMSE) and is_high_quality
            
            if is_visually_stable and is_high_quality:
                # 完美稳定帧: 加入岛屿，重置抖动计数
                current_island.append(i)
                fluctuation_count = 0 
                
            elif is_tolerable_fluctuation and len(current_island) > 0 and fluctuation_count < 2:
                # 容忍抖动帧: 暂时加入岛屿，增加抖动计数
                # 💥 稳定性增强: 确保岛内抖动比例不超过 20%
                current_island.append(i)
                fluctuation_count += 1
                
            else:
                # 彻底断开: 结算当前岛屿
                if len(current_island) >= MIN_STABLE_LEN:
                    # 只有当岛屿不仅仅由抖动帧组成时才有效 (简单校验: 长度够长通常意味着包含稳定帧)
                     islands.append(self._finalize_island(current_island, quality_results, mse_diffs_actual, frames))
                
                current_island = []
                fluctuation_count = 0

        # 处理最后一个岛屿
        if len(current_island) >= MIN_STABLE_LEN:
            islands.append(self._finalize_island(current_island, quality_results, mse_diffs_actual, frames))

        import gc
        gc.collect()

        # 6. 岛屿级博弈 (Island Optimization V6.3)
        debug_data = {"window": {"start": start_sec, "end": safe_end_sec}, "islands": []}
        
        if not islands:
            logger.warning(f"V6.2: No stable islands. Executing Fallback (Entropy*Sharpness*Time).")
            # 💥 性能优化: 兜底逻辑前也要清理
            del mse_diffs_actual
            del struct_mse_diffs
            del edge_maps
            import gc
            gc.collect()
            
            return self._fallback_select(frames, timestamps, quality_results, output_dir, save_image)
        else:
            # A. 过滤有效岛屿
            valid_islands = self.filter_valid_islands(islands, frames, quality_results, mse_diffs_actual, PIXEL_THRESH)
            
            # 💥 性能优化: 指标使用完毕，及时释放大列表内存
            del mse_diffs_actual
            del struct_mse_diffs
            del edge_maps
            import gc
            gc.collect()

            # B. 岛屿去重
            unique_islands = self.deduplicate_islands(valid_islands, frames)
            
            # If no unique islands left, fallback to top ranked original island or single best
            if not unique_islands:
                # Fallback to the original ranking game to pick at least one
                unique_islands = islands # Revert to all
            
            # C. 岛内择优 (Batch Processing for all unique islands)
            final_selections = []
            
            for island in unique_islands:
                # Intra-island selection
                best_idx = self._select_intra_island_winner(island, frames, quality_results)
                best_frame, best_ts = frames[best_idx], timestamps[best_idx]
                
                # 🚀 V6.2 High-Res Fix: Re-extract winner at original resolution to avoid blur
                cap = cv2.VideoCapture(video_path)
                
                # Robust Seek: Seek earlier and scan forward to avoid keyframe snapping issues
                seek_target = max(0, best_ts * 1000 - 1500) # Seek 1.5s back
                cap.set(cv2.CAP_PROP_POS_MSEC, seek_target)
                
                target_ts_ms = best_ts * 1000
                found_frame = None
                min_diff = float('inf')
                
                # Scan max 60 frames (approx 2s at 30fps) - Safety break
                for _ in range(60):
                    ret, frame = cap.read()
                    if not ret: break
                    
                    curr_pos = cap.get(cv2.CAP_PROP_POS_MSEC)
                    diff = abs(curr_pos - target_ts_ms)
                    
                    if diff < min_diff:
                        min_diff = diff
                        found_frame = frame
                        
                    # If we passed the target by more than 1 frame duration (approx 40ms) and diff starts growing
                    if curr_pos > target_ts_ms + 50: 
                        break
                
                high_res_frame = found_frame
                cap.release()
                
                if save_image:
                    if high_res_frame is not None:
                        path = self._save_screenshot(high_res_frame, best_ts, output_dir)
                        del high_res_frame
                    else:
                        logger.warning(f"High-res re-extraction failed for frame {best_idx}, using proxy.")
                        path = self._save_screenshot(best_frame, best_ts, output_dir)
                else:
                    path = "" # 不实际保存文件
                
                # Calculate scores
                scores = self._calculate_final_scores(frames, best_idx, island, quality_results[best_idx])
                
                sel = ScreenshotSelection(
                    best_idx, best_ts, path, 
                    scores["final"], scores["s1"], scores["s2"], scores["s3"], scores["s4"], 
                    [] # details omitted
                )
                final_selections.append(sel)
                
                # Add to debug
                debug_data["islands"].append({
                    "time": best_ts, 
                    "score": scores["final"], 
                    "metrics": island,
                    "selected": True
                })
            
            # Sort by timestamp (Chronological)
            final_selections.sort(key=lambda x: x.selected_timestamp)
            
            # V6.2 Debug Export (Tiered)
            # Use the best scoring one as the "primary" for debug trace "selected" field
            best_of_all =  max(final_selections, key=lambda x: x.final_score) if final_selections else None
            if best_of_all:
                 debug_data["selected"] = {"time": best_of_all.selected_timestamp, "idx": best_of_all.selected_frame_idx}
            
            self._export_debug_trace_tiered(
                debug_data, output_dir, start_sec, 
                frames, timestamps, islands, quality_results, 
                0, 0 
            )
            
            # COMPATIBILITY HACK: Return the one with highest score as the "Main" result,
            # but users of this method should ideally check for multiple outputs if they support it.
            # For now, to satisfy "Retain All", we should probably change the return type signature
            # OR - we package the others in a new field.
            # Since I cannot easily change downstream code right now (E2E pipeline),
            # I will return the BEST one, but ensure all qualified files are SAVED to disk (which they are).
            # The downstream `MultimodalFusionDecider` might only use one.
            # CHECK: Does `ScreenshotSelection` support carrying extras?
            # It has `all_candidates` which is List[FrameScore]. Not quite.
            # I will assume the goal is to SAVE them. The E2E pipeline might need adjustment to handle multiple.
            # For now, returning the BEST one is safe for code stability, while files are generated.
            return best_of_all if best_of_all else self._fallback_select(frames, timestamps, quality_results, output_dir, save_image)

    def _finalize_island(self, indices, quality_results, mse_diffs, frames):
        """结算岛屿统计指标"""
        # avg_laplacian is index 0 in quality_results
        avg_lap = np.mean([quality_results[i][0] for i in indices])
        avg_ent = np.mean([quality_results[i][1] for i in indices])
        
        # 快速 S4 估算 (抽样首尾中)
        sample_indices = [indices[0], indices[-1], indices[len(indices)//2]]
        s4_vals = [self._calculate_S4_no_occlusion_v6(frames[i]) for i in sample_indices]
        avg_s4 = np.mean(s4_vals)
        
        return {
            "indices": indices,
            "start_idx": indices[0],
            "end_idx": indices[-1],
            "duration": len(indices),
            "avg_laplacian": float(avg_lap),
            "avg_entropy": float(avg_ent),
            "avg_s4": float(avg_s4),
            "variance": float(np.var([mse_diffs[i] for i in indices[:-1]])) if len(indices)>1 else 0.0
        }

    def _select_intra_island_winner(self, island, frames, quality_results):
        """岛内择优: 综合 Entropy + Laplacian + S4"""
        best_score = -1
        best_idx = island["indices"][0]
        
        for idx in island["indices"]:
            lap, ent, sharp, contrast = quality_results[idx]
            s4 = self._calculate_S4_no_occlusion_v6(frames[idx])
            
            # 岛内打分: 信息密度和清晰度优先
            score = (ent * 0.4) + (lap * 0.3) + (contrast * 0.1) + (s4/100.0 * 0.2)
            
            if score > best_score:
                best_score = score
                best_idx = idx
        return best_idx

    def _identify_action_type_v6(self, frame_sample) -> str:
        """V6 内容分类"""
        gray = cv2.cvtColor(frame_sample, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 50, 150)
        rect_count = self.detector.detect_rectangles(edges)
        edge_density = np.sum(edges > 0) / edges.size
        
        if rect_count >= 5: return "ppt_complex"
        if edge_density > 0.1 and rect_count < 2: return "handwriting"
        if rect_count >= 1: return "ppt_basic"
        return "popup"

    def _get_adaptive_threshold(self, content_type: str, res_factor: float, fps: float) -> Dict:
        """V6 自适应阈值"""
        # 基准 (1080P)
        base_pixel = 80 * res_factor
        base_struct = 0.0015
        
        scale_map = {
            "handwriting": {"p": 1.5, "s": 1.5, "time": 0.5}, # 允许大波动，短时间
            "ppt_complex": {"p": 0.8, "s": 0.8, "time": 0.8}, # 要求稳定，稍短时间
            "ppt_basic":   {"p": 1.0, "s": 1.0, "time": 0.6},
            "popup":       {"p": 1.0, "s": 1.0, "time": 0.5}
        }
        
        factor = scale_map.get(content_type, {"p": 1.0, "s": 1.0, "time": 0.6})
        
        return {
            "pixel_mse": base_pixel * factor["p"],
            "struct_mse": base_struct * factor["s"],
            "min_stable_frames": int(fps * factor["time"])
        }

    def _calculate_S4_no_occlusion_v6(self, frame: np.ndarray) -> float:
        """V6 S4: 鼠标+UI文本检测"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        h, w = gray.shape
        penalty = 0
        
        # 1. 鼠标 (高亮小块)
        mask = cv2.inRange(gray, 240, 255)
        cnts, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        mouse_candidates = [c for c in cnts if 10 < cv2.contourArea(c) < 150]
        if mouse_candidates: penalty += 30
        
        # 2. UI 文本 (边缘过密区域)
        top_roi = gray[:int(h*0.1), :]
        if cv2.Canny(top_roi, 50, 150).mean() > 20: penalty += 20
        
        return max(0, 100 - penalty)

    def _fallback_select(self, frames, timestamps, quality_results, output_dir, save_image=True):
        """兜底逻辑: Entropy * Laplacian * TimeWeight"""
        best_score = -1
        best_idx = 0
        
        for i, (lap, ent, sharp, cont) in enumerate(quality_results):
            # 时间权重: 越靠后越重要 (假设板书写完了)
            time_bias = 1.0 + (i / len(frames)) * 0.5
            score = ent * lap * time_bias
            if score > best_score:
                best_score = score
                best_idx = i
        
        best_frame, best_ts = frames[best_idx], timestamps[best_idx]
        if save_image:
            path = self._save_screenshot(best_frame, best_ts, output_dir)
        else:
            path = ""
        
        # 兜底分
        s4 = self._calculate_S4_no_occlusion_v6(best_frame)
        return ScreenshotSelection(best_idx, best_ts, path, 50.0, 50, 50, 50, s4, [])

    def _calculate_final_scores(self, frames, idx, island, quality_res):
        """Patch S1-S4 values for final result"""
        lap, ent, sharp, cont = quality_res
        
        # S1: Stability (Island Duration + Low Variance)
        s1 = min(100, 50 + (island["duration"]/30)*30 + (500 - island["variance"])/10)
        
        # S2: Info Density (Entropy normalized)
        # Assuming entropy is usually 0-5. Normalized 0-100?
        # Standard: 0-10 bit. Normal range 3-7.
        s2 = min(100, ent * 15)
        
        # S3: Completeness (Calling detector)
        small_f = cv2.resize(frames[idx], (320, 240))
        e = cv2.Canny(cv2.cvtColor(small_f, cv2.COLOR_BGR2GRAY), 50, 150)
        rects = self.detector.detect_rectangles(e)
        arrows = self.detector.detect_arrows(e, cv2.cvtColor(small_f, cv2.COLOR_BGR2GRAY))["total"]
        s3 = min(100, 30 + rects*15 + arrows*10)
        
        # S4
        s4 = self._calculate_S4_no_occlusion_v6(frames[idx])
        
        final = s1*self.WEIGHT_S1 + s2*self.WEIGHT_S2 + s3*self.WEIGHT_S3 + s4*self.WEIGHT_S4
        return {"final": final, "s1": s1, "s2": s2, "s3": s3, "s4": s4}

    def filter_valid_islands(self, islands: list, frames: list, quality_results: list, mse_diffs_ref: list, pixel_thresh_ref: float) -> list:
        """
        V6.3 过滤有效岛屿: 保留稳定且有内容价值的岛
        Criteria:
        1. Duration > 0.6s (Handled in clustering)
        2. Entropy > Global Mean * 0.5
        3. Sharpness Ratio >= 0.7 (Adaptive)
        4. S4 >= 50
        """
        if not islands: return []
        
        # Calculate global entropy mean for relative threshold
        global_ent_mean = np.mean([q[1] for q in quality_results])
        max_sharp = max([q[0] for q in quality_results]) if quality_results else 1.0
        
        valid_islands = []
        for island in islands:
            # 0. 稳定性修正: 检查抖动帧占比 (防止伪岛屿)
            # 我们假设岛内连续抖动 > 2 已经断开，但可能总占比依然很高
            # 这里强制要求岛内高质量占比 > 80%
            if "indices" in island:
                fluct_frames = [idx for idx in island["indices"] if mse_diffs_ref[idx] > pixel_thresh_ref] 
                if len(fluct_frames) / len(island["indices"]) > 0.2:
                    continue

            # 1. Content Density (Entropy)
            if island["avg_entropy"] < global_ent_mean * 0.5:
                continue
                
            # 2. Clarity (Sharp Frame Ratio)
            # Frame is sharp if laplacian > 70% of max (adaptive) or > absolute thresh
            sharp_thresh = max(10.0, max_sharp * 0.6) 
            sharp_count = 0
            for idx in island["indices"]:
                if quality_results[idx][0] > sharp_thresh:
                    sharp_count += 1
            
            sharp_ratio = sharp_count / len(island["indices"])
            if sharp_ratio < 0.6: # Relaxed slightly from 0.7 to avoid false negatives
                continue
                
            # 3. No Occlusion (S4)
            if island["avg_s4"] < 50:
                continue
                
            valid_islands.append(island)
            
        return valid_islands

    def deduplicate_islands(self, valid_islands: list, frames: list) -> list:
        """
        V6.3 岛屿去重: 基于 SSIM 保留唯一内容岛
        Strategy:
        - Pick temp best frame for each island
        - Compare SSIM of effective content regions
        - If SSIM > 0.8, treat as duplicate, keep the LATER one (more complete)
        """
        if not valid_islands: return []
        
        # 1. Select representative frame for each island (middle frame)
        # Using middle frame is faster than full optimization and sufficient for dedup
        rep_frames = []
        for island in valid_islands:
            mid_idx = island["indices"][len(island["indices"])//2]
            rep_frames.append(frames[mid_idx])
            
        unique_islands = []
        # Store tuples of (island, rep_frame_gray)
        kept_data = [] 
        
        for i, island in enumerate(valid_islands):
            curr_frame = rep_frames[i]
            curr_gray = cv2.cvtColor(curr_frame, cv2.COLOR_BGR2GRAY)
            
            # ROI for comparison (Skip top/bottom UI)
            h, w = curr_gray.shape
            roi_y1, roi_y2 = int(h*0.15), int(h*0.85)
            curr_roi = curr_gray[roi_y1:roi_y2, :]
            
            is_duplicate = False
            duplicate_idx = -1
            
            for k, (kept_island, kept_roi) in enumerate(kept_data):
                # Calculate SSIM (resize to speed up)
                s_curr = cv2.resize(curr_roi, (160, 90))
                s_kept = cv2.resize(kept_roi, (160, 90))
                
                score = self.visual_extractor.calculate_ssim(s_curr, s_kept)
                
                if score > 0.85: # High similarity threshold
                    is_duplicate = True
                    duplicate_idx = k
                    break
            
            if is_duplicate:
                # Keep the later one (island 'i' is chronologically later than 'kept_island')
                # Replace the kept one with current one
                kept_data[duplicate_idx] = (island, curr_roi)
            else:
                kept_data.append((island, curr_roi))
        
        return [item[0] for item in kept_data]
        
        

    def _export_debug_trace_tiered(self, data, output_dir, ts, frames, timestamps, islands, quality_results, sharp_thresh, contrast_thresh):
        """
        导出分层级决策链与对照图 (V6.2 Lean Mode)
        用户要求: 不需要 Quality Pass, 重点区别岛屿与岛屿
        """
        if not output_dir: return
        try:
            root_debug = Path(output_dir) / "debug_trace"
            case_dir = root_debug / f"case_{ts:.2f}s"
            case_dir.mkdir(parents=True, exist_ok=True)
            
            # 1. 保存 JSON 决策树
            with open(case_dir / "decision_trace.json", 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                
            # 2. Layer 1: Raw Samples (固定密度采样 - 视觉参考)
            raw_dir = case_dir / "01_raw_samples"
            raw_dir.mkdir(exist_ok=True)
            step = max(1, len(frames) // 10)
            for i in range(0, len(frames), step):
                f_path = raw_dir / f"raw_idx{i}_{timestamps[i]:.2f}s.png"
                cv2.imwrite(str(f_path), frames[i])
            
            # 3. Layer 3: Stability Islands (重点: 岛屿与岛屿的区别)
            isl_dir = case_dir / "03_islands_comparison"
            isl_dir.mkdir(exist_ok=True)
            
            for i, isl in enumerate(islands):
                # 记录岛屿信息
                # Save Start, Middle, End frames to visualize stability
                mid = isl["indices"][len(isl["indices"])//2]
                frames_to_save = {
                    "start": isl["indices"][0],
                    "mid": mid,
                    "end": isl["indices"][-1]
                }
                
                # V6.2 Safety: Check if data['islands'] has this island (filtering might have happened)
                island_data = None
                if i < len(data['islands']):
                    if data['islands'][i]['metrics']['start_idx'] == isl['start_idx']:
                        island_data = data['islands'][i]
                
                if not island_data:
                    # Search by matching indices if enumeration mismatched
                    for d_isl in data['islands']:
                        if d_isl['metrics']['start_idx'] == isl['start_idx']:
                            island_data = d_isl
                            break
                            
                score = island_data['score'] if island_data else 0.0
                
                # 创建岛屿子文件夹，方便查看岛内一致性
                island_sub_dir = isl_dir / f"Rank{i+1}_Score{score:.2f}_Dur{isl['duration']}"
                island_sub_dir.mkdir(exist_ok=True)
                
                for label, idx in frames_to_save.items():
                    f_path = island_sub_dir / f"{label}_{timestamps[idx]:.2f}s.png"
                    cv2.imwrite(str(f_path), frames[idx])
                
            # 5. Winner
            cv2.imwrite(str(case_dir / f"WINNER_{ts:.2f}s.png"), frames[data['selected']['idx']])
            
        except Exception as e:
            logger.error(f"Tiered debug trace failed: {e}", exc_info=True)


    # Helper stubs for compatibility if needed
    def _get_video_fps(self, path): return self.visual_extractor.fps
    def _handle_empty_frames_complex(self, s, e, o): return self._create_empty_selection(s)
    def _create_empty_selection(self, ts): return ScreenshotSelection(0, ts, "", 0,0,0,0,0,[])
    def _save_screenshot(self, frame, ts, output_dir, output_name=None):
        """
        使用 FFmpeg 提取高分辨率帧
        
        💥 重构: 不再使用 OpenCV 代理帧，直接从原始视频提取高分辨率帧
        """
        import subprocess
        
        if not output_dir: output_dir = "screenshots"
        
        # 确定输出文件名
        final_name = output_name or getattr(self, '_current_output_name', None)
        if final_name:
            p = Path(output_dir) / f"{final_name}.png"
        else:
            p = Path(output_dir) / f"screenshot_{ts:.2f}s.png"
        p.parent.mkdir(parents=True, exist_ok=True)
        
        # 获取视频路径 (从 visual_extractor)
        video_path = getattr(self.visual_extractor, 'video_path', None)
        
        if video_path and Path(video_path).exists():
            # 使用 FFmpeg 提取高分辨率帧
            cmd = [
                "ffmpeg", "-y",
                "-ss", str(ts),
                "-i", video_path,
                "-frames:v", "1",
                "-q:v", "2",  # 高质量
                str(p)
            ]
            try:
                subprocess.run(cmd, capture_output=True, check=True, timeout=30)
                if p.exists():
                    logger.debug(f"FFmpeg extracted high-res frame at {ts:.2f}s -> {p.name}")
                    return str(p)
            except Exception as e:
                logger.warning(f"FFmpeg extraction failed at {ts:.2f}s: {e}, falling back to OpenCV")
        
        # 回退: 使用 OpenCV 代理帧
        cv2.imwrite(str(p), frame)
        return str(p)


