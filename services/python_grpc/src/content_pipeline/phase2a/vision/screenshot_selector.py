"""
?????Module2 ?????? screenshot_selector ???
??????????????????????
???????????????????
Screenshot Selector - Week 3 Day 16-18

Selects the best frame from a time range for screenshot enhancement.

Scoring system:
- S1 (绋冲畾鎬?: Continuous stable frames 鈫?higher score
- S4 (鏃犻伄鎸?: No occlusions/overlays 鈫?higher score
- Final: 0.5 脳 S1 + 0.5 脳 S4
"""

import logging
import cv2
import numpy as np
import os
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from pathlib import Path
import json
import time
from services.python_grpc.src.common.utils.opencv_decode import open_video_capture_with_fallback
from services.python_grpc.src.content_pipeline.infra.runtime.cv_runtime_config import CV_FLOAT_DTYPE, CV_FLOAT_DEPTH

logger = logging.getLogger(__name__)

# 馃挜 鎬ц兘浼樺寲: 寮曞叆 Numba JIT 鍔犻€熷儚绱犵骇杩愮畻
try:
    from numba import jit, prange
    HAS_NUMBA = True
    
    @jit(nopython=True, fastmath=True, parallel=True)
    def _numba_batch_mse(frames_data):
        """Batch-compute per-frame MSE for adjacent frames."""
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
        """Batch-compute structural MSE based on Canny edges."""
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
    鍗曞抚璇勫垎缁撴灉 (澶氱淮璇勪环鐭╅樀)
    """
    frame_idx: int
    timestamp_sec: float
    
    # 璇勫垎
    S1_stability: float     # 绋冲畾鎬?(MSE-based) 0-100
    S2_info_density: float  # 淇℃伅瀵嗗害 (Standard 2) 0-100
    S3_completeness: float  # 鏋舵瀯瀹屾暣鎬?(Standard 3: 绠ご/鐭╁舰) 0-100
    S4_no_occlusion: float  # 鏃犻伄鎸¤瘎鍒?0-100
    final_score: float      # 缁煎悎璇勫垎 0-100
    
    # 璇︾粏缁嗚妭
    rectangle_count: int
    arrow_count: int
    has_occlusion: bool



@dataclass
class ScreenshotSelection:
    """
    鎴浘閫夋嫨缁撴灉
    """
    selected_frame_idx: int
    selected_timestamp: float
    screenshot_path: str
    
    # 璇勫垎缁嗚妭
    final_score: float
    S1_stability: float
    S2_info_density: float
    S3_completeness: float
    S4_no_occlusion: float
    
    all_candidates: List[FrameScore]


def _analyze_frame_quality_worker(frame: np.ndarray) -> Tuple[float, float, float, float]:
    """
    V6.2 宸ヤ笟绾ц川閲忓垎鏋?Worker
    浣跨敤 Laplacian Variance (閿愬害) 鍜?Shannon Entropy (淇℃伅瀵嗗害)
    杩斿洖: (laplacian_var, shannon_entropy, sharpness_score, contrast_score)
    """
    try:
        # A. ROI 瀹氫綅 (鎺掗櫎榛戣竟/宸ュ叿鏍?
        gray_full = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        h, w = gray_full.shape
        # 绠€鍗?ROI: 鎺掗櫎涓婁笅 10% (閫氬父鏄疷I鍖哄煙)
        roi_gray = gray_full[int(h*0.1):int(h*0.9), :]
        
        # B. 鐗╃悊闃叉姈 (Laplacian Variance)
        # 瓒婇珮瓒婃竻鏅帮紝瓒婁綆瓒婃ā绯?
        laplacian_var = cv2.Laplacian(roi_gray, CV_FLOAT_DEPTH).var()
        
        # C. 淇℃伅鐔?(Shannon Entropy)
        hist = cv2.calcHist([roi_gray], [0], None, [256], [0, 256])
        if hist.sum() == 0: return 0.0, 0.0, 0.0, 0.0
        hist_norm = hist.ravel() / hist.sum()
        hist_norm = hist_norm[hist_norm > 0]
        shannon_entropy = -np.sum(hist_norm * np.log2(hist_norm))
        
        # D. 杈圭紭閿愬害 (Sobel Magnitude Mean - 杈呭姪)
        gx = cv2.Sobel(roi_gray, CV_FLOAT_DEPTH, 1, 0, ksize=3)
        gy = cv2.Sobel(roi_gray, CV_FLOAT_DEPTH, 0, 1, ksize=3)
        mag = cv2.sqrt(gx**2 + gy**2)
        sharpness_score = np.mean(mag)
        
        # E. 瀵规瘮搴?
        max_v = np.max(roi_gray).astype(CV_FLOAT_DTYPE)
        min_v = np.min(roi_gray).astype(CV_FLOAT_DTYPE)
        contrast_score = float((max_v - min_v) / (max_v + min_v + 1e-6))
        
        return laplacian_var, shannon_entropy, sharpness_score, contrast_score
    except Exception as e:
        logger.error(f"Critical error in quality worker: {e}", exc_info=True)
        return 0.0, 0.0, 0.0, 0.0


class ScreenshotSelector:
    """
    鎴浘閫夋嫨鍣?(V6.2 Refined Logic)
    
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
        """
        ?????????????????
        ???????????????????????????
        ????????????????????????
        """
        self.visual_extractor = visual_extractor
        from services.python_grpc.src.content_pipeline.phase2a.vision.visual_element_detection_helpers import VisualElementDetector
        self.detector = VisualElementDetector()
        
        if config is None:
            from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
            config = load_module2_config()
        
        # V6.2 榛樿涓ユ牸鏉冮噸
        self.WEIGHT_S1 = 0.2
        self.WEIGHT_S2 = 0.3
        self.WEIGHT_S3 = 0.4
        self.WEIGHT_S4 = 0.1
        
        logger.info(f"ScreenshotSelector V6.2 initialized (Fluctuation Tolerance Enabled)")
    
    @classmethod
    def create_lightweight(cls) -> 'ScreenshotSelector':
        """
        馃殌 宸ュ巶鏂规硶锛氬垱寤鸿交閲忕骇瀹炰緥锛堢敤浜?ProcessPool Worker锛?
        
        涓嶅垵濮嬪寲 visual_extractor锛堝湪 Worker 涓笉闇€瑕佽鍙栬棰戯級
        """
        instance = object.__new__(cls)
        instance.visual_extractor = None
        instance.detector = None  # 寤惰繜鍒濆鍖?
        instance.WEIGHT_S1 = 0.2
        instance.WEIGHT_S2 = 0.3
        instance.WEIGHT_S3 = 0.4
        instance.WEIGHT_S4 = 0.1
        return instance
    
    def _ensure_detector(self):
        """延迟初始化视觉元素检测器。"""
        if self.detector is None:
            from services.python_grpc.src.content_pipeline.phase2a.vision.visual_element_detection_helpers import VisualElementDetector
            self.detector = VisualElementDetector()
    
    def select_from_shared_frames(
        self,
        frames: List[np.ndarray],
        timestamps: List[float],
        fps: float = 30.0,
        res_factor: float = 1.0
    ) -> dict:
        """
        馃殌 ProcessPool 鍏煎鐗堟湰锛氫粠棰勮鍙栫殑甯т腑閫夋嫨鏈€浣虫埅鍥?
        
        淇濈暀瀹屾暣鐨勫矝灞胯仛绫?+ 鍗氬紙 + 鎷╀紭閫昏緫锛屼絾锛?
        1. 鎺ュ彈棰勮鍙栫殑甯э紙鑰岄潪浠庤棰戣鍙栵級
        2. 鍚屾鎵ц锛堣€岄潪 async锛?
        3. 涓嶄繚瀛樻枃浠讹紙浠呰繑鍥炴椂闂存埑锛?
        
        Args:
            frames: 棰勮鍙栫殑甯у垪琛?
            timestamps: 瀵瑰簲鐨勬椂闂存埑鍒楄〃
            fps: 瑙嗛甯х巼
            res_factor: 鍒嗚鲸鐜囩郴鏁帮紙鐩稿浜?1080p锛?
            
        Returns:
            {
                "selected_timestamp": float,
                "quality_score": float,
                "island_count": int,
                "analyzed_frames": int
            }
        """
        self._ensure_detector()
        
        if not frames or len(frames) == 0:
            return {
                "selected_timestamp": timestamps[0] if timestamps else 0.0,
                "quality_score": 0.0,
                "island_count": 0,
                "analyzed_frames": 0
            }
        
        # 1. 璇嗗埆鍐呭绫诲瀷浠ヨ皟鏁撮槇鍊?
        content_type = self._identify_action_type_v6(frames[0])
        threshold_config = self._get_adaptive_threshold(content_type, res_factor, fps)
        
        # 2. 璁＄畻甯ч棿 MSE 宸紓锛堝悓姝ョ増鏈級
        mse_diffs = []
        analysis_frames = frames
        for i in range(len(analysis_frames) - 1):
            f1 = analysis_frames[i].astype(CV_FLOAT_DTYPE, copy=False)
            f2 = analysis_frames[i + 1].astype(CV_FLOAT_DTYPE, copy=False)
            diff = np.mean((f1 - f2) ** 2)
            mse_diffs.append(diff)
        mse_diffs.append(0.0)  # 琛ラ綈鏈€鍚庝竴甯?
        
        # 3. 璁＄畻缁撴瀯鍖?MSE锛堣竟缂樺浘锛?
        edge_maps = [cv2.Canny(cv2.cvtColor(f, cv2.COLOR_BGR2GRAY), 50, 150) for f in frames]
        struct_mse_diffs = []
        for i in range(len(analysis_frames) - 1):
            diff = (edge_maps[i].astype(np.int16) - edge_maps[i+1].astype(np.int16)) ** 2
            struct_mse_diffs.append(np.mean(diff) / (255 * 255))
        struct_mse_diffs.append(0.0)
        
        # 4. 鍚屾璁＄畻璐ㄩ噺鎸囨爣
        quality_results = [_analyze_frame_quality_worker(f) for f in frames]
        
        # 5. 娉㈠姩瀹瑰繊鑱氱被
        PIXEL_THRESH = threshold_config["pixel_mse"]
        STRUCT_THRESH = threshold_config["struct_mse"]
        MIN_STABLE_LEN = threshold_config["min_stable_frames"]
        
        islands = []
        current_island = []
        fluctuation_count = 0
        
        MAX_FLUCT_MSE = PIXEL_THRESH * 3.0
        MAX_FLUCT_SMSE = STRUCT_THRESH * 3.0
        LAP_GATE = 10.0 * res_factor
        CONT_GATE = 0.15
        
        for i in range(len(analysis_frames) - 1):
            mse, smse = mse_diffs[i], struct_mse_diffs[i]
            lap, ent, sharp, contrast = quality_results[i]
            
            is_visually_stable = (mse < PIXEL_THRESH) and (smse < STRUCT_THRESH)
            is_high_quality = (lap > LAP_GATE) and (contrast > CONT_GATE)
            is_tolerable_fluctuation = (mse < MAX_FLUCT_MSE) and (smse < MAX_FLUCT_SMSE) and is_high_quality
            
            if is_visually_stable and is_high_quality:
                current_island.append(i)
                fluctuation_count = 0
            elif is_tolerable_fluctuation and len(current_island) > 0 and fluctuation_count < 2:
                current_island.append(i)
                fluctuation_count += 1
            else:
                if len(current_island) >= MIN_STABLE_LEN:
                    islands.append(self._finalize_island_sync(current_island, quality_results, mse_diffs, frames))
                current_island = []
                fluctuation_count = 0
        
        # 澶勭悊鏈€鍚庝竴涓矝灞?
        if len(current_island) >= MIN_STABLE_LEN:
            islands.append(self._finalize_island_sync(current_island, quality_results, mse_diffs, frames))
        
        # 6. 宀涘笨鍗氬紙
        if not islands:
            # 鍏滃簳锛氶€夋嫨 Entropy * Laplacian * TimeWeight 鏈€楂樼殑甯?
            best_score = -1
            best_idx = 0
            for i, (lap, ent, sharp, cont) in enumerate(quality_results):
                time_bias = 1.0 + (i / len(frames)) * 0.5
                score = ent * lap * time_bias
                if score > best_score:
                    best_score = score
                    best_idx = i
            
            return {
                "selected_timestamp": timestamps[best_idx],
                "quality_score": best_score,
                "island_count": 0,
                "analyzed_frames": len(frames)
            }
        
        # 7. 杩囨护鏈夋晥宀涘笨
        valid_islands = self._filter_valid_islands_sync(islands, frames, quality_results, mse_diffs, PIXEL_THRESH)
        
        if not valid_islands:
            valid_islands = islands  # 鍥為€€鍒版墍鏈夊矝灞?
        
        # 8. 宀涘笨鍘婚噸锛堢畝鍖栫増鏈紝閬垮厤 SSIM 璁＄畻寮€閿€锛?
        unique_islands = self._deduplicate_islands_simple(valid_islands, timestamps)
        
        if not unique_islands:
            unique_islands = valid_islands
        
        # 9. 宀涘唴鎷╀紭
        best_island = None
        best_island_score = -1
        best_frame_idx = 0
        
        for island in unique_islands:
            idx, score = self._select_intra_island_winner_sync(island, frames, quality_results)
            if score > best_island_score:
                best_island_score = score
                best_frame_idx = idx
                best_island = island
        
        return {
            "selected_timestamp": timestamps[best_frame_idx],
            "quality_score": best_island_score,
            "island_count": len(unique_islands),
            "analyzed_frames": len(frames)
        }
    
    def _finalize_island_sync(self, indices, quality_results, mse_diffs, frames):
        """同步版本：汇总稳定岛统计信息。"""
        avg_lap = np.mean([quality_results[i][0] for i in indices])
        avg_ent = np.mean([quality_results[i][1] for i in indices])
        
        # 蹇€?S4 浼扮畻锛堟娊鏍烽灏句腑锛?
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
            "variance": float(np.var([mse_diffs[i] for i in indices[:-1]])) if len(indices) > 1 else 0.0
        }
    
    def _filter_valid_islands_sync(self, islands, frames, quality_results, mse_diffs, pixel_thresh):
        """同步版本：过滤无效稳定岛。"""
        if not islands:
            return []
        
        global_ent_mean = np.mean([q[1] for q in quality_results])
        max_sharp = max([q[0] for q in quality_results]) if quality_results else 1.0
        
        valid_islands = []
        for island in islands:
            # 妫€鏌ユ姈鍔ㄥ抚鍗犳瘮
            if "indices" in island:
                fluct_frames = [idx for idx in island["indices"] if mse_diffs[idx] > pixel_thresh]
                if len(fluct_frames) / len(island["indices"]) > 0.2:
                    continue
            
            # 鍐呭瀵嗗害妫€鏌?
            if island["avg_entropy"] < global_ent_mean * 0.5:
                continue
            
            # 娓呮櫚搴︽鏌?
            sharp_thresh = max(10.0, max_sharp * 0.6)
            sharp_count = sum(1 for idx in island["indices"] if quality_results[idx][0] > sharp_thresh)
            if sharp_count / len(island["indices"]) < 0.6:
                continue
            
            # 閬尅妫€鏌?
            if island["avg_s4"] < 50:
                continue
            
            valid_islands.append(island)
        
        return valid_islands
    
    def _deduplicate_islands_simple(self, islands, timestamps):
        """简化版去重：基于时间间隔进行稳定岛去重。"""
        if len(islands) <= 1:
            return islands
        
        unique = [islands[0]]
        for island in islands[1:]:
            # 濡傛灉涓や釜宀涘笨鐨勪腑蹇冩椂闂村樊 > 2s锛岃涓烘槸涓嶅悓鍐呭
            last_mid = timestamps[unique[-1]["indices"][len(unique[-1]["indices"])//2]]
            curr_mid = timestamps[island["indices"][len(island["indices"])//2]]
            
            if abs(curr_mid - last_mid) > 2.0:
                unique.append(island)
            else:
                # 淇濈暀鍚庝竴涓紙閫氬父鍐呭鏇村畬鏁达級
                unique[-1] = island
        
        return unique
    
    def _select_intra_island_winner_sync(self, island, frames, quality_results):
        """同步版本：在稳定岛内选择最优帧。"""
        best_score = -1
        best_idx = island["indices"][0]
        
        for idx in island["indices"]:
            lap, ent, sharp, contrast = quality_results[idx]
            s4 = self._calculate_S4_no_occlusion_v6(frames[idx])
            
            score = (ent * 0.4) + (lap * 0.3) + (contrast * 0.1) + (s4 / 100.0 * 0.2)
            
            if score > best_score:
                best_score = score
                best_idx = idx
        
        return best_idx, best_score

    def _read_frames_at_timestamps_sequential(
        self,
        cap: cv2.VideoCapture,
        timestamps: List[float],
        video_fps: float,
        total_frames: int,
        max_width: Optional[int] = None,
    ) -> Tuple[List[float], List[np.ndarray]]:
        """
        椤哄簭璇诲抚妯″紡锛氭寜鏃堕棿鎴虫帓搴忓悗锛屽敖閲忎互杩炵画 read() 浠ｆ浛澶氭闅忔満 seek銆?

        鐩爣锛氶檷浣?cap.set(...) 楂橀璋冪敤甯︽潵鐨?IO/瑙ｇ爜寮€閿€銆?
        """
        if not timestamps:
            return [], []

        valid_pairs = []
        for ts in timestamps:
            frame_idx = int(max(0.0, ts) * video_fps)
            frame_idx = max(0, min(frame_idx, max(0, total_frames - 1)))
            valid_pairs.append((float(ts), frame_idx))

        valid_pairs.sort(key=lambda item: item[1])

        out_timestamps: List[float] = []
        out_frames: List[np.ndarray] = []

        current_idx = None
        for ts, target_idx in valid_pairs:
            if current_idx is None:
                cap.set(cv2.CAP_PROP_POS_FRAMES, target_idx)
                current_idx = target_idx
            elif target_idx < current_idx or (target_idx - current_idx) > 3:
                cap.set(cv2.CAP_PROP_POS_FRAMES, target_idx)
                current_idx = target_idx
            else:
                while current_idx < target_idx:
                    ret_skip, _ = cap.read()
                    if not ret_skip:
                        break
                    current_idx += 1

            ret, frame = cap.read()
            if ret and frame is not None:
                frame = self._resize_frame_max_width(frame, max_width=max_width)
                out_timestamps.append(ts)
                out_frames.append(frame)
                current_idx = (current_idx if current_idx is not None else target_idx) + 1
            else:
                current_idx = target_idx + 1

        return out_timestamps, out_frames

    @staticmethod
    def _resize_frame_max_width(
        frame: np.ndarray,
        max_width: Optional[int] = None,
    ) -> np.ndarray:
        """按最大宽度下采样帧，降低路由截图阶段的内存占用。"""
        if frame is None or not max_width or int(max_width) <= 0:
            return frame
        try:
            h, w = frame.shape[:2]
            if h <= 0 or w <= 0 or w <= int(max_width):
                return frame

            target_w = int(max_width)
            scale = float(target_w) / float(w)
            target_h = max(2, int(round(h * scale)))

            # 编码器/部分算子更偏好偶数尺寸
            if target_w % 2 != 0:
                target_w -= 1
            if target_h % 2 != 0:
                target_h += 1
            target_w = max(2, target_w)
            target_h = max(2, target_h)

            return cv2.resize(frame, (target_w, target_h), interpolation=cv2.INTER_AREA)
        except Exception:
            return frame

    @staticmethod
    def _split_time_range(
        start_sec: float,
        end_sec: float,
        max_span_sec: float,
    ) -> List[Tuple[float, float]]:
        """将长时间范围切分为多个子区间，复用 coarse-fine 的细采样逻辑。"""
        safe_start = float(start_sec)
        safe_end = float(end_sec)
        safe_span = max(0.1, float(max_span_sec))
        if safe_end <= safe_start:
            return [(safe_start, safe_start + safe_span)]

        if (safe_end - safe_start) <= safe_span:
            return [(safe_start, safe_end)]

        ranges: List[Tuple[float, float]] = []
        cur = safe_start
        while cur < safe_end:
            nxt = min(safe_end, cur + safe_span)
            if nxt > cur:
                ranges.append((cur, nxt))
            cur = nxt
        return ranges

    def _crop_frame_by_roi(
        self,
        frame: np.ndarray,
        roi: Optional[Tuple[int, int, int, int]] = None,
    ) -> np.ndarray:
        """
        鍦?ROI 鍐呰鍓抚锛岀敤浜庡噺灏戝瓧骞曠瓑杈圭紭鍣０骞叉壈銆?
        鍋氫粈涔堬細
        - 灏嗗悗缁ǔ瀹氬矝妫€娴嬩笌宀涘唴閫変紭闄愬畾鍒?ROI 鍖哄煙銆?        涓轰粈涔堬細
        - 璺敱鎴浘鍦烘櫙甯告湁搴曢儴瀛楀箷鏉★紝鐩存帴鍙備笌璇勫垎浼氭媺浣庣ǔ瀹氭€у垽鏂川閲忋€?        """
        if frame is None or roi is None:
            return frame
        try:
            x1, y1, x2, y2 = roi
            h, w = frame.shape[:2]
            x1 = max(0, min(int(x1), w))
            x2 = max(0, min(int(x2), w))
            y1 = max(0, min(int(y1), h))
            y2 = max(0, min(int(y2), h))
            if x2 <= x1 or y2 <= y1:
                return frame
            cropped = frame[y1:y2, x1:x2]
            return cropped if cropped.size > 0 else frame
        except Exception:
            return frame

    def select_screenshots_for_range_sync(
        self,
        video_path: str,
        start_sec: float,
        end_sec: float,
        coarse_fps: float = 2.0,
        fine_fps: float = 10.0,
        roi: Optional[Tuple[int, int, int, int]] = None,
        stable_islands_override: Optional[List[Tuple[float, float]]] = None,
        action_segments_override: Optional[List[Tuple[float, float]]] = None,
        analysis_max_width: Optional[int] = None,
        long_window_fine_chunk_sec: float = 0.0,
        decode_open_timeout_sec: int = 300,
        decode_allow_inline_transcode: Optional[bool] = None,
        decode_enable_async_transcode: Optional[bool] = None,
    ) -> List[Dict]:
        """
        馃殌 鍏堢矖鍚庣粏鎴浘閫夋嫨 (鍚屾鐗堟湰锛岀敤浜?ProcessPool - 浠嶉渶鑷璇诲彇甯?
        
        鈿狅笍 娉ㄦ剰: 姝ゆ柟娉曚粛鐒跺湪 Worker 涓鍙栧抚锛岀敤浜庣畝鍗曞満鏅€?
        瀵逛簬楂樻€ц兘鍦烘櫙锛屽簲浣跨敤 detect_stable_islands_from_frames + select_best_frame_from_frames
        """
        import time
        t0 = time.time()
        
        self._ensure_detector()
        
        # 杈圭晫淇濇姢
        if end_sec <= start_sec:
            end_sec = start_sec + 1.0
        
        cap, effective_video_path, used_decode_fallback = open_video_capture_with_fallback(
            video_path,
            logger=logger,
            timeout_sec=max(5, int(decode_open_timeout_sec)),
            allow_inline_transcode=decode_allow_inline_transcode,
            enable_async_transcode=decode_enable_async_transcode,
        )
        if cap is None or not cap.isOpened():
            logger.error(f"Cannot open video: source={video_path}, effective={effective_video_path}")
            return []
        if used_decode_fallback:
            logger.warning(
                "ScreenshotSelector decode fallback applied: source=%s, effective=%s",
                video_path,
                effective_video_path,
            )
        
        video_fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        video_duration = total_frames / video_fps
        end_sec = min(end_sec, video_duration)
        
        # Stage 1: 绮楅噰鏍凤紙椤哄簭璇诲抚锛屽噺灏戦殢鏈?seek锛?
        coarse_interval = 1.0 / coarse_fps
        coarse_ts_candidates = []
        t = start_sec
        while t < end_sec:
            coarse_ts_candidates.append(t)
            t += coarse_interval

        coarse_timestamps, coarse_frames = self._read_frames_at_timestamps_sequential(
            cap=cap,
            timestamps=coarse_ts_candidates,
            video_fps=video_fps,
            total_frames=total_frames,
            max_width=analysis_max_width,
        )
        
        if len(coarse_frames) < 2:
            cap.release()
            return [{"timestamp_sec": (start_sec + end_sec) / 2, "island_index": 0, "score": 0.5, 
                     "island_start": start_sec, "island_end": end_sec}]
        
        # Stage 1: 璇嗗埆绋冲畾宀?
        islands = []
        if isinstance(stable_islands_override, list) and stable_islands_override:
            for item in stable_islands_override:
                if not isinstance(item, (list, tuple)) or len(item) < 2:
                    continue
                try:
                    s = float(item[0])
                    e = float(item[1])
                except (TypeError, ValueError):
                    continue
                s = max(start_sec, s)
                e = min(end_sec, e)
                if e > s:
                    islands.append({"start_sec": s, "end_sec": e})
        else:
            islands = self.detect_stable_islands_from_frames(
                coarse_frames,
                coarse_timestamps,
                coarse_interval,
                roi=roi,
            )
        
        if not islands:
            islands = [{"start_sec": start_sec, "end_sec": end_sec}]

        # 若上游已给出动作单元区间，则将截图搜索范围约束到“稳定岛∩动作段”；
        # 这样短 process 走 CV 路由时也能复用 action 信息，避免整段扫描噪声。
        action_ranges: List[Tuple[float, float]] = []
        if isinstance(action_segments_override, list) and action_segments_override:
            for item in action_segments_override:
                if not isinstance(item, (list, tuple)) or len(item) < 2:
                    continue
                try:
                    a_start = float(item[0])
                    a_end = float(item[1])
                except (TypeError, ValueError):
                    continue
                a_start = max(start_sec, a_start)
                a_end = min(end_sec, a_end)
                if a_end > a_start:
                    action_ranges.append((a_start, a_end))

        if action_ranges:
            constrained_islands: List[Dict[str, float]] = []
            for island in islands:
                i_start = float(island.get("start_sec", start_sec))
                i_end = float(island.get("end_sec", end_sec))
                if i_end <= i_start:
                    continue
                for a_start, a_end in action_ranges:
                    inter_start = max(i_start, a_start)
                    inter_end = min(i_end, a_end)
                    if inter_end > inter_start:
                        constrained_islands.append({
                            "start_sec": inter_start,
                            "end_sec": inter_end,
                        })

            if constrained_islands:
                islands = constrained_islands
            else:
                islands = [{"start_sec": s, "end_sec": e} for s, e in action_ranges]
        
        # Stage 2: 缁嗛噰鏍烽€夋渶浣冲抚锛堥『搴忚甯?+ 骞惰璐ㄩ噺璇勫垎锛?
        results = []
        fine_interval = 1.0 / fine_fps
        
        for island_idx, island in enumerate(islands):
            island_start = island["start_sec"]
            island_end = island["end_sec"]

            # 长稳定岛分块细采样：避免一次性加载超长窗口 fine 帧列表。
            sub_ranges = [(island_start, island_end)]
            if float(long_window_fine_chunk_sec or 0.0) > 0.0 and (island_end - island_start) > float(long_window_fine_chunk_sec):
                sub_ranges = self._split_time_range(
                    start_sec=island_start,
                    end_sec=island_end,
                    max_span_sec=float(long_window_fine_chunk_sec),
                )

            best_ts = None
            best_score = -1.0
            for sub_start, sub_end in sub_ranges:
                fine_ts_candidates = []
                t = sub_start
                while t < sub_end:
                    fine_ts_candidates.append(t)
                    t += fine_interval
                if not fine_ts_candidates:
                    continue

                fine_timestamps, fine_frames = self._read_frames_at_timestamps_sequential(
                    cap=cap,
                    timestamps=fine_ts_candidates,
                    video_fps=video_fps,
                    total_frames=total_frames,
                    max_width=analysis_max_width,
                )
                if not fine_frames:
                    continue

                cand_ts, cand_score = self.select_best_frame_from_frames(
                    fine_frames,
                    fine_timestamps,
                    roi=roi,
                )
                if float(cand_score) > best_score:
                    best_ts = cand_ts
                    best_score = float(cand_score)

            if best_ts is None:
                continue

            results.append({
                "timestamp_sec": best_ts,
                "island_index": island_idx,
                "score": float(best_score),
                "island_start": island_start,
                "island_end": island_end
            })
        
        cap.release()
        
        elapsed = time.time() - t0
        logger.info(
            f"Coarse-Fine selection [{start_sec:.1f}s-{end_sec:.1f}s]: "
            f"{len(results)} screenshots in {elapsed:.2f}s (pid={os.getpid()})"
        )
        
        return results

    def detect_stable_islands_from_frames(
        self,
        frames: List[np.ndarray],
        timestamps: List[float],
        interval: float,
        stable_thresh: float = 0.005,
        min_island_len: int = 2,
        roi: Optional[Tuple[int, int, int, int]] = None,
    ) -> List[Dict]:
        """
        馃殌 Stage 1: 浠庨璇诲彇鐨勫抚涓瘑鍒ǔ瀹氬矝 (绾绠楋紝鏃?IO)
        
        鐢ㄤ簬 ProcessPool Worker锛屾帴鏀朵富杩涚▼閫氳繃 SharedMemory 浼犻€掔殑甯?
        
        Args:
            frames: 棰勮鍙栫殑甯у垪琛?
            timestamps: 瀵瑰簲鐨勬椂闂存埑鍒楄〃
            interval: 閲囨牱闂撮殧
            stable_thresh: MSE 绋冲畾闃堝€?
            min_island_len: 鏈€灏忓矝闀垮害
            
        Returns:
            绋冲畾宀涘垪琛?[{"start_sec": float, "end_sec": float}, ...]
        """
        if len(frames) < 2:
            return []

        analysis_frames = [self._crop_frame_by_roi(frame, roi) for frame in frames]
        
        # 璁＄畻鐩搁偦甯?MSE
        mse_values = []
        for i in range(len(analysis_frames) - 1):
            f1 = cv2.cvtColor(analysis_frames[i], cv2.COLOR_BGR2GRAY).astype(CV_FLOAT_DTYPE, copy=False)
            f2 = cv2.cvtColor(analysis_frames[i + 1], cv2.COLOR_BGR2GRAY).astype(CV_FLOAT_DTYPE, copy=False)
            mse = np.mean((f1 - f2) ** 2) / (255 * 255)
            mse_values.append(mse)
        mse_values.append(0.0)
        
        # 璇嗗埆绋冲畾宀?
        islands = []
        current_island_indices = []
        
        for i, mse in enumerate(mse_values[:-1]):
            if mse < stable_thresh:
                current_island_indices.append(i)
            else:
                if len(current_island_indices) >= min_island_len:
                    islands.append({
                        "start_sec": timestamps[current_island_indices[0]],
                        "end_sec": timestamps[current_island_indices[-1]] + interval
                    })
                current_island_indices = []
        
        # 澶勭悊鏈熬
        if len(current_island_indices) >= min_island_len:
            islands.append({
                "start_sec": timestamps[current_island_indices[0]],
                "end_sec": timestamps[current_island_indices[-1]] + interval
            })
        
        logger.debug(f"Detected {len(islands)} stable islands from {len(frames)} coarse frames")
        return islands

    def select_best_frame_from_frames(
        self,
        frames: List[np.ndarray],
        timestamps: List[float],
        roi: Optional[Tuple[int, int, int, int]] = None,
        return_index: bool = False,
    ) -> Tuple[float, float]:
        """
        馃殌 Stage 2: 浠庨璇诲彇鐨勫抚涓€夋嫨鏈€浣冲抚 (绾绠楋紝鏃?IO)
        
        鐢ㄤ簬 ProcessPool Worker锛屾帴鏀朵富杩涚▼閫氳繃 SharedMemory 浼犻€掔殑甯?
        
        Args:
            frames: 棰勮鍙栫殑甯у垪琛?
            timestamps: 瀵瑰簲鐨勬椂闂存埑鍒楄〃
            
        Returns:
            (best_timestamp, best_score)
        """
        if not frames:
            return (0.0, 0.0)
        
        if len(frames) == 1:
            if return_index:
                return (timestamps[0], 0.5, 0)
            return (timestamps[0], 0.5)

        analysis_frames = [self._crop_frame_by_roi(frame, roi) for frame in frames]
        
        # 璐ㄩ噺璇勪及锛堝苟琛岋級
        worker_count = max(1, min(4, len(analysis_frames)))
        if worker_count > 1:
            with ThreadPoolExecutor(max_workers=worker_count) as pool:
                quality_results = list(pool.map(_analyze_frame_quality_worker, analysis_frames))
        else:
            quality_results = [_analyze_frame_quality_worker(f) for f in analysis_frames]
        
        # 璁＄畻 MSE
        mse_values = []
        for i in range(len(analysis_frames) - 1):
            f1 = cv2.cvtColor(analysis_frames[i], cv2.COLOR_BGR2GRAY).astype(CV_FLOAT_DTYPE, copy=False)
            f2 = cv2.cvtColor(analysis_frames[i + 1], cv2.COLOR_BGR2GRAY).astype(CV_FLOAT_DTYPE, copy=False)
            mse = np.mean((f1 - f2) ** 2) / (255 * 255)
            mse_values.append(mse)
        if mse_values:
            mse_values.append(mse_values[-1])
        else:
            mse_values.append(0.0)
        
        # 閫夋嫨鏈€浣冲抚
        best_score = -1
        best_idx = 0
        
        for idx in range(len(analysis_frames)):
            lap, ent, sharp, contrast = quality_results[idx]
            s4 = self._calculate_S4_no_occlusion_v6(analysis_frames[idx])
            stability_bonus = max(0, 1.0 - mse_values[idx] * 100)
            
            score = (ent * 0.35) + (lap * 0.25) + (contrast * 0.1) + (s4 / 100.0 * 0.15) + (stability_bonus * 0.15)
            
            if score > best_score:
                best_score = score
                best_idx = idx
        
        if return_index:
            return (timestamps[best_idx], best_score, best_idx)
        return (timestamps[best_idx], best_score)



    async def select_screenshot(
        self,
        video_path: str,
        start_sec: float,
        end_sec: float,
        output_dir: str = None,
        output_name: str = None,
        save_image: bool = True  # 馃挜 鏂板: 鏄惁淇濆瓨鍥剧墖鏂囦欢
    ) -> ScreenshotSelection:
        """
        V6.2 鏍稿績娴佺▼: 娉㈠姩瀹瑰繊鑱氱被 + 宀涘笨鍗氬紙 + 宀涘唴鎷╀紭
        """
        # 馃挜 瀛樺偍 output_name 浠ヤ究鍐呴儴璋冪敤浣跨敤
        self._current_output_name = output_name
        
        # 1. 瑙嗙獥涓庨噰鏍峰噯澶?
        fps = self._get_video_fps(video_path)
        safe_end_sec = max(start_sec + 1.0, end_sec)
        
        # 2. 璋冪敤鍏ㄥ眬鑷€傚簲鍐崇瓥寮曟搸 (Phase 6.9: Unified Features)
        refined_visual = await self.visual_extractor.extract_visual_features(start_sec, safe_end_sec, sample_rate=1)
        
        # 浠庣紦瀛樻垨杩斿洖瀵硅薄涓幏鍙栨暟鎹?
        cache = self.visual_extractor.get_cached_content(start_sec, safe_end_sec, sample_rate=1)
        if not cache or not cache.enhanced_frames:
             return await self._handle_empty_frames_complex(start_sec, safe_end_sec, output_dir)
             
        frames = cache.enhanced_frames
        timestamps = cache.timestamps
        mse_diffs_actual = refined_visual.mse_list
            
        # 3. 鍒嗚鲸鐜囩郴鏁颁笌缁勪欢璇嗗埆
        video_width = frames[0].shape[1]
        res_factor = video_width / 1920.0
        
        # 璇嗗埆鍐呭绫诲瀷浠ヨ皟鏁撮槇鍊?
        content_type = self._identify_action_type_v6(frames[0])
        threshold_config = self._get_adaptive_threshold(content_type, res_factor, fps)
        
        logger.info(f"Selecting V6.9 ({content_type}, ResFactor={res_factor:.2f}) from {start_sec:.2f}s to {safe_end_sec:.2f}s")
        
        # 4. 鍩虹鎸囨爣鍏ㄩ噺骞惰璁＄畻
        import asyncio
        from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import get_visual_process_pool
        loop = asyncio.get_running_loop()
        executor = get_visual_process_pool()
        
        # 4.5 棰勮绠楄竟缂樺浘浠ョ敤浜庣粨鏋?MSE (V6.2 Struct-MSE)
        edge_maps = [cv2.Canny(cv2.cvtColor(f, cv2.COLOR_BGR2GRAY), 50, 150) for f in frames]
        
        # Struct-MSE 鏍稿績璁＄畻 (Numba 鍔犻€熺増)
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
        
        # 琛ュ叏 MSE 鍒楄〃闀垮害 (瀵归綈 timestamps)
        if len(mse_diffs_actual) < len(timestamps):
            mse_diffs_actual.append(0.0)

        # Quality Worker 骞惰
        quality_tasks = [loop.run_in_executor(executor, _analyze_frame_quality_worker, f) for f in frames]
        quality_results = await asyncio.gather(*quality_tasks) # [(lap, ent, sharp, cont), ...]
        
        # 5. 绋冲畾宀涜仛绫?(Fluctuation Tolerance Mechanism)
        PIXEL_THRESH = threshold_config["pixel_mse"]
        STRUCT_THRESH = threshold_config["struct_mse"]
        MIN_STABLE_LEN = threshold_config["min_stable_frames"]
        
        islands = []
        current_island = []
        fluctuation_count = 0 # 瀹归敊璁℃暟鍣?
        
        MAX_FLUCT_MSE = PIXEL_THRESH * 3.0 # 鍏佽鐬棿鎶栧姩鍒?3 鍊嶉槇鍊?(e.g. 榧犳爣椋炶繃)
        MAX_FLUCT_SMSE = STRUCT_THRESH * 3.0
        
        # 璐ㄩ噺闂ㄦ (Quality Gate: Laplacian > 10.0*res_factor, Contrast > 0.15)
        LAP_GATE = 10.0 * res_factor
        CONT_GATE = 0.15
        
        for i in range(len(frames) - 1):
            mse, smse = mse_diffs_actual[i], struct_mse_diffs[i]
            lap, ent, sharp, contrast = quality_results[i]
            
            # 鍒ゅ畾鍩哄噯
            is_visually_stable = (mse < PIXEL_THRESH) and (smse < STRUCT_THRESH)
            is_high_quality = (lap > LAP_GATE) and (contrast > CONT_GATE)
            
            # 瀹归敊鍒ゅ畾: 濡傛灉涓嶇ǔ瀹氾紝浣嗘姈鍔ㄥ湪瀹瑰繊鑼冨洿鍐咃紝涓斾笉绠楀お涔咃紝涓旂敾闈緷鐒舵竻鏅?
            is_tolerable_fluctuation = (mse < MAX_FLUCT_MSE) and (smse < MAX_FLUCT_SMSE) and is_high_quality
            
            if is_visually_stable and is_high_quality:
                # 瀹岀編绋冲畾甯? 鍔犲叆宀涘笨锛岄噸缃姈鍔ㄨ鏁?
                current_island.append(i)
                fluctuation_count = 0 
                
            elif is_tolerable_fluctuation and len(current_island) > 0 and fluctuation_count < 2:
                # 瀹瑰繊鎶栧姩甯? 鏆傛椂鍔犲叆宀涘笨锛屽鍔犳姈鍔ㄨ鏁?
                # 馃挜 绋冲畾鎬у寮? 纭繚宀涘唴鎶栧姩姣斾緥涓嶈秴杩?20%
                current_island.append(i)
                fluctuation_count += 1
                
            else:
                # 褰诲簳鏂紑: 缁撶畻褰撳墠宀涘笨
                if len(current_island) >= MIN_STABLE_LEN:
                    # 鍙湁褰撳矝灞夸笉浠呬粎鐢辨姈鍔ㄥ抚缁勬垚鏃舵墠鏈夋晥 (绠€鍗曟牎楠? 闀垮害澶熼暱閫氬父鎰忓懗鐫€鍖呭惈绋冲畾甯?
                     islands.append(self._finalize_island(current_island, quality_results, mse_diffs_actual, frames))
                
                current_island = []
                fluctuation_count = 0

        # 澶勭悊鏈€鍚庝竴涓矝灞?
        if len(current_island) >= MIN_STABLE_LEN:
            islands.append(self._finalize_island(current_island, quality_results, mse_diffs_actual, frames))

        import gc
        gc.collect()

        # 6. 宀涘笨绾у崥寮?(Island Optimization V6.3)
        debug_data = {"window": {"start": start_sec, "end": safe_end_sec}, "islands": []}
        
        if not islands:
            logger.warning(f"V6.2: No stable islands. Executing Fallback (Entropy*Sharpness*Time).")
            # 馃挜 鎬ц兘浼樺寲: 鍏滃簳閫昏緫鍓嶄篃瑕佹竻鐞?
            del mse_diffs_actual
            del struct_mse_diffs
            del edge_maps
            import gc
            gc.collect()
            
            return self._fallback_select(frames, timestamps, quality_results, output_dir, save_image)
        else:
            # A. 杩囨护鏈夋晥宀涘笨
            valid_islands = self.filter_valid_islands(islands, frames, quality_results, mse_diffs_actual, PIXEL_THRESH)
            
            # 馃挜 鎬ц兘浼樺寲: 鎸囨爣浣跨敤瀹屾瘯锛屽強鏃堕噴鏀惧ぇ鍒楄〃鍐呭瓨
            del mse_diffs_actual
            del struct_mse_diffs
            del edge_maps
            import gc
            gc.collect()

            # B. 宀涘笨鍘婚噸
            unique_islands = self.deduplicate_islands(valid_islands, frames)
            
            # If no unique islands left, fallback to top ranked original island or single best
            if not unique_islands:
                # Fallback to the original ranking game to pick at least one
                unique_islands = islands # Revert to all
            
            # C. 宀涘唴鎷╀紭 (Batch Processing for all unique islands)
            final_selections = []
            
            for island in unique_islands:
                # Intra-island selection
                best_idx = self._select_intra_island_winner(island, frames, quality_results)
                best_frame, best_ts = frames[best_idx], timestamps[best_idx]
                
                # 馃殌 V6.2 High-Res Fix: Re-extract winner at original resolution to avoid blur
                cap, effective_video_path, _ = open_video_capture_with_fallback(
                    video_path,
                    logger=logger,
                )
                if cap is None or not cap.isOpened():
                    logger.warning(
                        "High-res re-extract cannot open video: source=%s, effective=%s",
                        video_path,
                        effective_video_path,
                    )
                    high_res_frame = None
                    cap = None
                else:
                
                    # Robust Seek: Seek earlier and scan forward to avoid keyframe snapping issues
                    seek_target = max(0, best_ts * 1000 - 1500) # Seek 1.5s back
                    cap.set(cv2.CAP_PROP_POS_MSEC, seek_target)

                    target_ts_ms = best_ts * 1000
                    found_frame = None
                    min_diff = float('inf')

                    # Scan max 60 frames (approx 2s at 30fps) - Safety break
                    for _ in range(60):
                        ret, frame = cap.read()
                        if not ret:
                            break

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
                    path = "" # 涓嶅疄闄呬繚瀛樻枃浠?
                
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
        """缁撶畻宀涘笨缁熻鎸囨爣"""
        # avg_laplacian is index 0 in quality_results
        avg_lap = np.mean([quality_results[i][0] for i in indices])
        avg_ent = np.mean([quality_results[i][1] for i in indices])
        
        # 蹇€?S4 浼扮畻 (鎶芥牱棣栧熬涓?
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
        """宀涘唴鎷╀紭: 缁煎悎 Entropy + Laplacian + S4"""
        best_score = -1
        best_idx = island["indices"][0]
        
        for idx in island["indices"]:
            lap, ent, sharp, contrast = quality_results[idx]
            s4 = self._calculate_S4_no_occlusion_v6(frames[idx])
            
            # 宀涘唴鎵撳垎: 淇℃伅瀵嗗害鍜屾竻鏅板害浼樺厛
            score = (ent * 0.4) + (lap * 0.3) + (contrast * 0.1) + (s4/100.0 * 0.2)
            
            if score > best_score:
                best_score = score
                best_idx = idx
        return best_idx

    def _identify_action_type_v6(self, frame_sample) -> str:
        """V6 鍐呭鍒嗙被"""
        gray = cv2.cvtColor(frame_sample, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 50, 150)
        rect_count = self.detector.detect_rectangles(edges)
        edge_density = np.sum(edges > 0) / edges.size
        
        if rect_count >= 5: return "ppt_complex"
        if edge_density > 0.1 and rect_count < 2: return "handwriting"
        if rect_count >= 1: return "ppt_basic"
        return "popup"

    def _get_adaptive_threshold(self, content_type: str, res_factor: float, fps: float) -> Dict:
        """V6：根据内容类型与分辨率自适应阈值。"""
        # 鍩哄噯 (1080P)
        base_pixel = 80 * res_factor
        base_struct = 0.0015
        
        scale_map = {
            "handwriting": {"p": 1.5, "s": 1.5, "time": 0.5}, # 鍏佽澶ф尝鍔紝鐭椂闂?
            "ppt_complex": {"p": 0.8, "s": 0.8, "time": 0.8}, # 瑕佹眰绋冲畾锛岀◢鐭椂闂?
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
        """V6 S4：基于鼠标/UI痕迹估计遮挡惩罚。"""
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        h, w = gray.shape
        penalty = 0
        
        # 1. 榧犳爣 (楂樹寒灏忓潡)
        mask = cv2.inRange(gray, 240, 255)
        cnts, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        mouse_candidates = [c for c in cnts if 10 < cv2.contourArea(c) < 150]
        if mouse_candidates: penalty += 30
        
        # 2. UI 鏂囨湰 (杈圭紭杩囧瘑鍖哄煙)
        top_roi = gray[:int(h*0.1), :]
        if cv2.Canny(top_roi, 50, 150).mean() > 20: penalty += 20
        
        return max(0, 100 - penalty)

    def _fallback_select(self, frames, timestamps, quality_results, output_dir, save_image=True):
        """鍏滃簳閫昏緫: Entropy * Laplacian * TimeWeight"""
        best_score = -1
        best_idx = 0
        
        for i, (lap, ent, sharp, cont) in enumerate(quality_results):
            # 鏃堕棿鏉冮噸: 瓒婇潬鍚庤秺閲嶈 (鍋囪鏉夸功鍐欏畬浜?
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
        
        # 鍏滃簳鍒?
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
        V6.3 杩囨护鏈夋晥宀涘笨: 淇濈暀绋冲畾涓旀湁鍐呭浠峰€肩殑宀?
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
            # 0. 绋冲畾鎬т慨姝? 妫€鏌ユ姈鍔ㄥ抚鍗犳瘮 (闃叉浼矝灞?
            # 鎴戜滑鍋囪宀涘唴杩炵画鎶栧姩 > 2 宸茬粡鏂紑锛屼絾鍙兘鎬诲崰姣斾緷鐒跺緢楂?
            # 杩欓噷寮哄埗瑕佹眰宀涘唴楂樿川閲忓崰姣?> 80%
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
        V6.3 宀涘笨鍘婚噸: 鍩轰簬 SSIM 淇濈暀鍞竴鍐呭宀?
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
        瀵煎嚭鍒嗗眰绾у喅绛栭摼涓庡鐓у浘 (V6.2 Lean Mode)
        鐢ㄦ埛瑕佹眰: 涓嶉渶瑕?Quality Pass, 閲嶇偣鍖哄埆宀涘笨涓庡矝灞?
        """
        if not output_dir: return
        try:
            root_debug = Path(output_dir) / "debug_trace"
            case_dir = root_debug / f"case_{ts:.2f}s"
            case_dir.mkdir(parents=True, exist_ok=True)
            
            # 1. 淇濆瓨 JSON 鍐崇瓥鏍?
            with open(case_dir / "decision_trace.json", 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
                
            # 2. Layer 1: Raw Samples (鍥哄畾瀵嗗害閲囨牱 - 瑙嗚鍙傝€?
            raw_dir = case_dir / "01_raw_samples"
            raw_dir.mkdir(exist_ok=True)
            step = max(1, len(frames) // 10)
            for i in range(0, len(frames), step):
                f_path = raw_dir / f"raw_idx{i}_{timestamps[i]:.2f}s.png"
                cv2.imwrite(str(f_path), frames[i])
            
            # 3. Layer 3: Stability Islands (閲嶇偣: 宀涘笨涓庡矝灞跨殑鍖哄埆)
            isl_dir = case_dir / "03_islands_comparison"
            isl_dir.mkdir(exist_ok=True)
            
            for i, isl in enumerate(islands):
                # 璁板綍宀涘笨淇℃伅
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
                
                # 鍒涘缓宀涘笨瀛愭枃浠跺す锛屾柟渚挎煡鐪嬪矝鍐呬竴鑷存€?
                island_sub_dir = isl_dir / f"Rank{i+1}_Score{score:.2f}_Dur{isl['duration']}"
                island_sub_dir.mkdir(exist_ok=True)
                
                for label, idx in frames_to_save.items():
                    f_path = island_sub_dir / f"{label}_{timestamps[idx]:.2f}s.png"
                    cv2.imwrite(str(f_path), frames[idx])
                
            # 5. Winner
            cv2.imwrite(str(case_dir / f"WINNER_{ts:.2f}s.png"), frames[data['selected']['idx']])
            
        except Exception as e:
            logger.error(f"Tiered debug trace failed: {e}", exc_info=True)


    # 鍏煎鎬у厹搴曟柟娉曪細鍋氫粈涔堟槸淇濈暀鏃ф帴鍙ｏ紱涓轰粈涔堟槸閬垮厤璋冪敤鏂规柇瑁傦紱鏉冭　鏄€昏緫杈冪畝鍖?
    def _get_video_fps(self, path):
        """兼容旧接口：返回视频 FPS。"""
        return self.visual_extractor.fps

    # 绌哄抚鍏滃簳锛氬仛浠€涔堟槸杩斿洖绌洪€夋嫨锛涗负浠€涔堟槸淇濊瘉涓嬫父娴佺▼涓嶆柇锛涙潈琛℃槸涓嶅啀鍖哄垎澶嶆潅绌哄抚鍘熷洜
    def _handle_empty_frames_complex(self, s, e, o):
        """空帧兜底：返回默认空选择结果。"""
        return self._create_empty_selection(s)

    # 绌洪€夋嫨鏋勯€狅細鍋氫粈涔堟槸鐢熸垚榛樿 ScreenshotSelection锛涗负浠€涔堟槸缁熶竴杩斿洖缁撴瀯锛涙潈琛℃槸缁嗚妭淇℃伅缂哄け
    def _create_empty_selection(self, ts):
        """生成空的 `ScreenshotSelection`。"""
        return ScreenshotSelection(0, ts, "", 0, 0, 0, 0, 0, [])
    def _save_screenshot(self, frame, ts, output_dir, output_name=None):
        """
        浣跨敤 FFmpeg 鎻愬彇楂樺垎杈ㄧ巼甯?
        
        馃挜 閲嶆瀯: 涓嶅啀浣跨敤 OpenCV 浠ｇ悊甯э紝鐩存帴浠庡師濮嬭棰戞彁鍙栭珮鍒嗚鲸鐜囧抚
        """
        import subprocess
        
        if not output_dir: output_dir = "screenshots"
        
        # 纭畾杈撳嚭鏂囦欢鍚?
        final_name = output_name or getattr(self, '_current_output_name', None)
        if final_name:
            p = Path(output_dir) / f"{final_name}.png"
        else:
            p = Path(output_dir) / f"screenshot_{ts:.2f}s.png"
        p.parent.mkdir(parents=True, exist_ok=True)
        
        # 鑾峰彇瑙嗛璺緞 (浠?visual_extractor)
        video_path = getattr(self.visual_extractor, 'video_path', None)
        
        if video_path and Path(video_path).exists():
            # 浣跨敤 FFmpeg 鎻愬彇楂樺垎杈ㄧ巼甯?
            cmd = [
                "ffmpeg", "-y",
                "-ss", str(ts),
                "-i", video_path,
                "-frames:v", "1",
                "-q:v", "2",  # 楂樿川閲?
                str(p)
            ]
            try:
                subprocess.run(cmd, capture_output=True, check=True, timeout=30)
                if p.exists():
                    logger.debug(f"FFmpeg extracted high-res frame at {ts:.2f}s -> {p.name}")
                    return str(p)
            except Exception as e:
                logger.warning(f"FFmpeg extraction failed at {ts:.2f}s: {e}, falling back to OpenCV")
        
        # 鍥為€€: 浣跨敤 OpenCV 浠ｇ悊甯?
        cv2.imwrite(str(p), frame)
        return str(p)


