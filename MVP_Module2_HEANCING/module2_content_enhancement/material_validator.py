import logging
import numpy as np
import cv2
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path

from .data_structures import EnhancementType
from .visual_feature_extractor import VisualFeatureExtractor
from .visual_element_detection_helpers import VisualElementDetector
from .timestamp_mapper import TimestampMapper

from .cognitive_demand_classifier import CognitiveDemandClassifier

logger = logging.getLogger(__name__)

class MaterialValidator:
    """
    素材验证层 (Phase 4.2) - 认知需求驱动方案
    
    核心逻辑（第一性原理）：
    1. 认知需求解耦：先判定断层需要什么载体（文字/图片/视频）。
    2. 需求响应验证：针对具体需求进行针对性能力与语义双向评分。
    3. 匹配度导向：计算 MatchScore = 0.5*能力匹配 + 0.5*语义匹配。
    """
    
    def __init__(self, config: Dict = None, extractor: Any = None, visual_extractor: Any = None):
        self.config = config or {}
        self._bert_extractor = extractor
        self._visual_extractor = visual_extractor
        
        # 🚀 Phase 4.3: 传入 semantic_extractor 以启用零样本分类
        self.classifier = CognitiveDemandClassifier(semantic_extractor=self._bert_extractor)
        
        # 核心阈值 (可配置)
        self.ACTION_VALID_THRESHOLD = 0.6
        self.STRUCT_VALID_THRESHOLD = 0.7
        
        # 🚀 Performance: Cache validation results to avoid redundant LLM calls
        self._validation_cache = {}
        
    async def validate_for_demand(
        self,
        fault_text: str,
        video_path: str,
        time_window: Tuple[float, float],
        subtitles: List[Dict],
        screenshot: Optional[np.ndarray] = None,
        visual_features: Any = None
    ) -> Dict[str, Any]:
        """
        需求响应式验证入口 (Phase 4.2)
        根据认知需求类型，计算各素材的 MatchScore
        """
        # 💥 Cache Check
        cache_key = f"{fault_text}_{time_window}"
        if cache_key in self._validation_cache:
            return self._validation_cache[cache_key]

        # 1. 判定认知需求 (混合策略: 视觉特征优先 + V5 Context)
        # Construct Context from subtitles
        context_text = ""
        if subtitles:
             start_ts = time_window[0]
             # Get subs in window +/- 10s for broader context
             relevant_subs = [s['text'] for s in subtitles if s.get('start', 0) >= start_ts - 10 and s.get('end', 0) <= time_window[1] + 5]
             context_text = " ".join(relevant_subs)

        # 🚀 V5: Async Classification with Context & LLM
        if hasattr(self.classifier, 'classify_with_reason_async'):
             demand_type, demand_reason = await self.classifier.classify_with_reason_async(fault_text, visual_features, context_text)
        else:
             demand_type, demand_reason = self.classifier.classify_with_reason(fault_text, visual_features)
        
        logger.info(f"Targeting logic for Demand Type: {demand_type.upper()} ({demand_reason})")
        
        results = {}
        
        # 2. 验证视频 (如果是视频需或混合需)
        video_score, video_details = await self.validate_video_for_demand(video_path, time_window, fault_text, subtitles, demand_type)
        results["video"] = {"score": video_score, "details": video_details}
        
        # 3. 验证截图
        screenshot_score, screenshot_details = await self.validate_screenshot_for_demand(video_path, time_window[0], fault_text, subtitles, demand_type)
        results["screenshot"] = {"score": screenshot_score, "details": screenshot_details}
        
        # 🚀 Level 3: 重构文字得分 (Lowered Fallback)
        # 不匹配时得分降低 (0.6 -> 0.5)，匹配时保底分降低 (0.85 -> 0.7)
        text_score = 0.7 if demand_type == "text" else 0.5
        results["text"] = {"score": text_score, "details": {"type": "Text_Fallback"}}
        
        result_pkg = {
            "demand_type": demand_type,
            "demand_reason": demand_reason,
            "results": results
        }
        
        # Cache result
        self._validation_cache[cache_key] = result_pkg
        return result_pkg

    # =========================================================================
    # 🚀 V5 Optimization: Multi-Metric Scoring & Local Semantic Match
    # =========================================================================

    def expand_fuzzy_text(self, fault_text: str) -> str:
        """V5: Expand fuzzy text with synonyms for robust matching."""
        synonym_dict = {
            "关系": "层级关系、结构关系、逻辑关系",
            "步骤": "操作步骤、推导步骤、执行步骤",
            "图": "架构图、流程图、拓扑图"
        }
        text = fault_text
        for word, synonyms in synonym_dict.items():
            if word in text:
                text = text.replace(word, f"{word} ({synonyms})")
        return text

    def calculate_local_semantic_match(self, feature_roi: Any, fault_text: str) -> float:
        """
        V5: Local Semantic Match using ROI (via CLIP if available).
        """
        # If visual_extractor is available and has CLIP support
        if self.visual_extractor and hasattr(self.visual_extractor, 'calculate_clip_score'):
             prompt = f"a clear image of {fault_text[:30]}"
             # ensure feature_roi is a valid image
             if feature_roi is not None and hasattr(feature_roi, 'shape'):
                 return self.visual_extractor.calculate_clip_score(feature_roi, prompt)
        
        return 0.75 # Fallback

    def adjust_capability_weights(self, metrics: Dict[str, float], cognitive_type: str) -> float:
        """V5: Dynamically adjust weights based on cognitive demand."""
        if cognitive_type == "video" or cognitive_type == "process":
            # Video/Process: Focus on Action Strength
            return 0.6 * metrics.get("action_strength", 0) + 0.2 * metrics.get("continuity", 0) + 0.2 * metrics.get("element_completeness", 0)
        elif cognitive_type == "screenshot" or cognitive_type == "spatial":
            # Screenshot/Spatial: Focus on Structure
            return 0.7 * metrics.get("element_completeness", 0) + 0.1 * metrics.get("clarity", 0) + 0.2 * metrics.get("occlusion_score", 1.0) # Occlusion score 1.0 means NO occlusion
        else:
             # Abstract: Average
             return float(np.mean(list(metrics.values()))) if metrics else 0.0

    async def validate_video_for_demand(
        self,
        video_path: str,
        window: Tuple[float, float],
        fault_text: str,
        subtitles: List[Dict],
        demand_type: str
    ) -> Tuple[float, Dict]:
        """验证视频对需求的满足度 (V5 Optimized)"""
        # A. 基础视频动作验证 -> 获取各项指标
        # 暂时复用 _validate_video_action 获取 raw_confidence 作为 action_strength
        raw_confidence, details = await self._validate_video_action(video_path, window, fault_text, subtitles)
        
        # Construct metrics dict for V5 (Simulated from existing outputs)
        metrics = {
            "action_strength": raw_confidence, # derived from MSE
            "continuity": details.get("temporal_consistency", 0.5), # Assuming new field or using default
            "element_completeness": 0.5, # Video specific
        }
        
        # B. V5 Adaptive Capability Score
        # Map demand_type "process", "spatial" etc. to simplify
        cog_type = "process" if demand_type == "video" else demand_type
        capability_score = self.adjust_capability_weights(metrics, cog_type)
        
        # C. V5 Robust Semantic Match
        expanded_text = self.expand_fuzzy_text(fault_text)
        # Re-using existing semantic sim but with expanded text would be ideal, 
        # for now we use the one returned by _validate_video_action as baseline
        semantic_sim = details.get("semantic_sim", 0.5)
        
        # D. MatchScore Calculation
        demand_multiplier = 1.0
        if demand_type == "text": demand_multiplier = 0.4
        elif demand_type == "screenshot": demand_multiplier = 0.7
            
        match_score = (0.5 * capability_score + 0.5 * semantic_sim) * demand_multiplier
        
        details["v5_expanded_text"] = expanded_text
        return float(match_score), details

    async def validate_screenshot_for_demand(
        self,
        video_path: str,
        timestamp: float,
        fault_text: str,
        subtitles: List[Dict],
        demand_type: str
    ) -> Tuple[float, Dict]:
        """验证截图对需求的满足度 (V5 Optimized)"""
        # A. 基础截图结构验证
        raw_confidence, details = await self._validate_screenshot_struct(video_path, timestamp, fault_text, subtitles)
        
        # Construct metrics dict for V5
        metrics = {
            "element_completeness": raw_confidence, # derived from element detection
            "clarity": 0.8, # Placeholder or need to fetch from visual features
            "occlusion_score": 1.0 
        }
        
        # B. V5 Adaptive Capability Score
        cog_type = "spatial" if demand_type == "screenshot" else demand_type
        capability_score = self.adjust_capability_weights(metrics, cog_type)
        
        # C. V5 Robust Semantic Match
        semantic_sim = details.get("semantic_sim", 0.5)
        
        # D. MatchScore
        demand_multiplier = 1.0
        if demand_type == "video": demand_multiplier = 0.5
        elif demand_type == "text": demand_multiplier = 0.4
        
        # V5: Use new weights (0.7 cap + 0.1 sem + 0.2 context)
        # Note: 'details' might not have 'context_weight', defaulting
        match_score = (0.7 * capability_score + 0.1 * semantic_sim + 0.2 * details.get("context_weight", 1.0)) * demand_multiplier
        
        if demand_type == "screenshot" and any(k in fault_text for k in ["公式", "分式", "矩阵", "积分"]):
             match_score = min(1.0, match_score * 1.2)
             
        return float(match_score), details

    # --- 关键动作检测系统 ---

    async def _validate_video_action(
        self, 
        video_path: str, 
        window: Tuple[float, float], 
        fault_text: str,
        subtitles: List[Dict]
    ) -> Tuple[float, Dict]:
        """
        动作有效性打分 (全场景细化)
        $Score_{act} = 0.4*特征匹配 + 0.3*时序一致 + 0.2*语义关联 + 0.1*干扰排除$
        """
        extractor = self._get_visual_extractor(video_path)
        frames, timestamps = extractor.extract_frames_fast(window[0], window[1], sample_rate=5)
        if not frames: return 0.0, {"error": "No frames Extracted"}
        
        mse_list, _ = extractor.calculate_all_diffs(frames)
        
        # 1. 特征匹配度 (6类动作)
        match_score, action_type = self._detect_6_action_types(frames, mse_list, fault_text)
        
        # 2. 时序一致性
        temporal_consistency = self._check_temporal_consistency(mse_list)
        
        # 3. 语义关联性 (BERT Proxy)
        semantic_sim = await self._get_semantic_similarity(window, fault_text, subtitles)
        
        # 4. 干扰排除度 (Noise Filtering)
        noise_filter_score = self._calculate_noise_filter(mse_list)
        
        # 5. 音频/节奏特征分 (Audio Score - Proxy)
        audio_score = self.calculate_audio_score(window, subtitles, demand_type="video")
        
        # 6. Text Score (Consistency)
        text_score = semantic_sim # Use semantic similarity as text score proxy
        
        # 最终打分 (V3 Multimodal Fusion: 0.3*Text + 0.5*Visual + 0.2*Audio)
        visual_score = match_score # Action score is the visual component
        
        # Adjust weights based on demand type relevance
        total_score = (0.3 * text_score + 0.5 * visual_score + 0.2 * audio_score)
        
        return float(total_score), {
            "action_type": action_type,
            "match_score": match_score,
            "semantic_sim": semantic_sim,
            "noise_filtered": noise_filter_score > 0.5
        }

    def _detect_6_action_types(self, frames: List[np.ndarray], mse_list: List[float], fault_text: str) -> Tuple[float, str]:
        """核心：识别6类教学动作并打分"""
        max_mse = np.max(mse_list) if mse_list else 0
        avg_mse = np.mean(mse_list) if mse_list else 0
        
        # (1) 页面/帧切换 (全局 MSE > 500)
        if max_mse > 500:
            return 1.0, "Page_Turn"
            
        # (2) 数据结构操作 (有序高亮/变化) - 简化判定：连续多帧中等变化
        if any(m > 150 for m in mse_list) and np.std(mse_list) > 30:
            return 0.9, "Structure_Operation"
            
        # (3) 光标精准操作 (MSE 局部变化)
        if 80 < avg_mse < 200:
            return 0.7, "Cursor_Operation"
            
        # (4) 元素状态操控 (亮度/标记变化)
        if any(30 < m < 150 for m in mse_list):
            return 0.6, "State_Control"
            
        # (5) 手写/板书及 (6) 工具演示
        if avg_mse > 100:
            return 0.5, "Handwriting/Demo"
            
        return 0.2, "None"

    def _check_temporal_consistency(self, mse_list: List[float]) -> float:
        """验证动作是否按逻辑顺序发生（非随机爆炸）"""
        if not mse_list: return 0.0
        # 教学动作通常具有聚集性，而非全程均匀分布或单帧噪点
        peaks = [m for m in mse_list if m > 100]
        if 0 < len(peaks) <= len(mse_list) * 0.4: return 1.0 # 局部聚集动效
        if len(peaks) > 0: return 0.6
        return 0.2

    def _calculate_noise_filter(self, mse_list: List[float]) -> float:
        """排除误触/波动 (干扰排除度)"""
        if not mse_list: return 0.0
        avg_mse = np.mean(mse_list)
        # 过滤 logic：位移量太小或持续太短
        if avg_mse < 30: return 0.0 # 纯噪点
        if np.max(mse_list) < 80: return 0.3 # 微弱抖动
        return 1.0

    # --- 结构有效性检测系统 ---

    async def _validate_screenshot_struct(
        self,
        video_path: str,
        timestamp: float,
        fault_text: str,
        subtitles: List[Dict]
    ) -> Tuple[float, Dict]:
        """
        结构有效性打分 (5类结构)
        $Score_{struct} = 0.3*元素完整 + 0.4*空间关系 + 0.3*语义关联$
        """
        extractor = self._get_visual_extractor(video_path)
        frames, _ = extractor.extract_frames(timestamp, timestamp + 0.1)
        if not frames: return 0.0, {"error": "Frame extraction failed"}
        frame = frames[0]
        
        elements = VisualElementDetector.analyze_frame(frame)
        
        # 1. 元素完整性
        completion_score = self._calculate_element_completion(elements)
        
        # 2. 空间关系合理性 (5类结构)
        relation_score, struct_type = self._analyze_5_structural_relations(elements, fault_text)
        
        # 3. 语义关联性
        semantic_sim = await self._get_semantic_similarity((timestamp-1.0, timestamp+1.0), fault_text, subtitles)
        
        # 4. 音频/节奏特征分 (Audio Score - Proxy)
        audio_score = self.calculate_audio_score((timestamp, timestamp+1.0), subtitles, demand_type="screenshot")
        
        # 5. Text Score
        text_score = semantic_sim

        # 最终打分 (V3 Multimodal Fusion: 0.3*Text + 0.5*Visual + 0.2*Audio)
        visual_score = relation_score # Structural score is the visual component
        
        total_score = (0.3 * text_score + 0.5 * visual_score + 0.2 * audio_score)
        
        return float(total_score), {
            "struct_type": struct_type,
            "element_completion": completion_score,
            "relation_score": relation_score,
            "semantic_sim": semantic_sim
        }

    def _calculate_element_completion(self, elements: Dict) -> float:
        """判断核心元素是否缺失"""
        total = elements["total"]
        if total >= 8: return 1.0
        if total >= 4: return 0.7
        if elements["has_architecture_elements"]: return 0.5
        return 0.2

    def _analyze_5_structural_relations(self, elements: Dict, fault_text: str) -> Tuple[float, str]:
        """识别5类结构空间关系"""
        # (1) 链表/树结构 (节点 + 定向连线)
        if (elements["circles"] >= 2 or elements["rectangles"] >= 3) and elements["arrows"]["total"] >= 2:
            return 1.0, "Link/Tree"
            
        # (2) 流程图结构 (多种节点 + 逻辑流转)
        if elements["diamonds"] >= 1 or elements["connectors"]["intersections"] >= 1:
            return 0.9, "Flowchart"
            
        # (3) 层级架构图 (多个矩形 + 连接)
        if elements["rectangles"] >= 4 and elements["arrows"]["total"] >= 1:
            return 0.8, "Hierarchy"
            
        # (4) 对比表格/矩阵 (网格线 + 直线)
        if elements["lines"] >= 6 and elements["connectors"]["t_junctions"] >= 2:
            return 0.7, "Table/Grid"
            
        # (5) 标注型结构图
        if elements["total"] >= 2 and elements["arrows"]["confidence"] > 0.6:
            return 0.6, "Annotation"
            
        return 0.3, "General"

    # --- 辅助模块 ---

    async def _get_semantic_similarity(self, window: Tuple[float, float], fault_text: str, subtitles: List[Dict]) -> float:
        """利用 S-BERT 字幕代理进行语义校验"""
        try:
            from .subtitle_utils import extract_subtitle_text_in_range
            context_text = extract_subtitle_text_in_range(subtitles, window[0], window[1])
            if not context_text: return 0.3
            
            extractor = self._get_bert_extractor()
            if not extractor: return 0.5
            
            sim = await extractor.calculate_context_similarity(fault_text, context_text)
            return float(sim)
        except Exception as e:
            logger.warning(f"Semantic proxy failed: {e}")
            logger.warning(f"Semantic proxy failed: {e}")
            return 0.5
            
    def calculate_audio_score(self, window: Tuple[float, float], subtitles: List[Dict], demand_type: str) -> float:
        """
        计算音频模态得分 (Proxy: 语速/节奏)
        
        原理: 
        - 动态过程 (Video): 通常语速较快，包含指令性词汇，能量高 -> High Speech Rate Score
        - 静态空间 (Screenshot): 通常语速平缓，解释性强 -> Low/Stable Speech Rate Score
        """
        try:
            from .subtitle_utils import extract_subtitle_text_in_range
            text = extract_subtitle_text_in_range(subtitles, window[0], window[1])
            duration = window[1] - window[0]
            if duration <= 0: return 0.5
            
            # 简单语速 (字/秒)
            char_count = len(text.replace(" ", ""))
            speech_rate = char_count / duration
            
            # 标准化 (假设正常语速 3-5 字/秒)
            # High rate (>5) favors dynamic/video
            # Moderate rate (2-4) favors static/screenshot
            
            if demand_type == "video":
                # 越快分越高 (Demo usually fast)
                score = min(1.0, max(0.0, (speech_rate - 2) / 4)) 
            else: # screenshot
                # 适中最好 (Explanation)
                if 2 <= speech_rate <= 5:
                    score = 0.8
                elif speech_rate < 2: # Too slow (pause?)
                    score = 0.5
                else: # Too fast (skipping?)
                    score = 0.4
                    
            return score
        except:
            return 0.5

    def _get_visual_extractor(self, video_path: str):
        if self._visual_extractor:
            return self._visual_extractor
        return VisualFeatureExtractor(video_path)

    def _get_bert_extractor(self):
        if not hasattr(self, '_bert_extractor') or self._bert_extractor is None:
            try:
                from .semantic_feature_extractor import SemanticFeatureExtractor
                self._bert_extractor = SemanticFeatureExtractor(config=self.config)
            except: self._bert_extractor = None
        return self._bert_extractor
