"""
Confidence Calculator - Week 2 Day 11-12

Implements dual-dimension confidence calculation for text补充:
- C_text (内部语义置信度, 权重0.4)
- C_multi (外部多模态验证置信度, 权重0.6)

Follows user document: "LLM上下文补全置信度计算设计与多模态检验方案"
"""

import logging
from typing import Dict, Optional, List, Any
from dataclasses import dataclass
import numpy as np

from .subtitle_utils import extract_subtitle_text_in_range, calculate_subtitle_similarity
from collections import OrderedDict
import hashlib
import json

logger = logging.getLogger(__name__)


@dataclass
class ConfidenceCalculationResult:
    """
    置信度计算结果
    
    双维度模型:
    C_total = 0.4 × C_text + 0.6 × C_multi
    """
    # 双维度分数
    C_text: float  # 内部语义置信度 (0-1)
    C_multi: float  # 外部多模态置信度 (0-1)
    C_total: float  # 综合置信度 (0-1)
    
    # 等级
    confidence_level: str  # "high" | "medium" | "low"
    
    # 详细分数 (用于调试)
    S_ctx: float = 0.0  # 上下文相似度
    K_domain: float = 0.0  # 领域一致性
    P_norm: float = 0.5  # 困惑度 (MVP简化,暂用固定值)
    
    # 多模态验证详情
    multimodal_details: Dict = None
    
    # 处理建议
    processing_suggestion: str = ""  # 根据置信度等级的建议


class ConfidenceCalculator:
    """
    双维度置信度计算器
    
    核心公式:
    1. C_text = 0.5 × S_ctx + 0.3 × P_norm + 0.2 × K_domain
    2. C_multi = 根据断层类型选择验证方式
    3. C_total = 0.4 × C_text + 0.6 × C_multi
    
    等级划分:
    - [0.8, 1.0]: 高置信度
    - [0.6, 0.8): 中置信度
    - [0.0, 0.6): 低置信度
    """
    
    # 权重配置 (来自用户文档)
    WEIGHT_C_TEXT = 0.4
    WEIGHT_C_MULTI = 0.6
    
    # C_text内部权重
    WEIGHT_S_CTX = 0.5  # 上下文相似度
    WEIGHT_P_NORM = 0.3  # 困惑度
    WEIGHT_K_DOMAIN = 0.2  # 领域一致性
    
    # 置信度阈值
    THRESHOLD_HIGH = 0.8
    THRESHOLD_MEDIUM = 0.7  # Aligned with Design Doc 2 decision threshold
    
    def __init__(self, semantic_extractor=None, config: Dict = None):
        """
        Args:
            semantic_extractor: SemanticFeatureExtractor实例 (用于Sentence-BERT)
            config: Module2配置字典
        """
        self.semantic_extractor = semantic_extractor
        
        # 加载配置
        if config is None:
            from .config_loader import load_module2_config
            config = load_module2_config()
        
        conf_config = config.get("confidence_config", {})
        
        # 严格遵循设计文档权重
        self.WEIGHT_C_TEXT = 0.4
        self.WEIGHT_C_MULTI = 0.6
        
        # C_text内部权重 (50% / 30% / 20%)
        self.WEIGHT_S_CTX = 0.5
        self.WEIGHT_P_NORM = 0.3
        self.WEIGHT_K_DOMAIN = 0.2
        
        # C_multi分类别权重
        self.CLASS2_TEXT_WEIGHT = 0.6
        self.CLASS2_VISUAL_WEIGHT = 0.4
        self.CLASS3_STEP_WEIGHT = 0.5
        self.CLASS3_SEQUENCE_WEIGHT = 0.5
        
        # 🚀 Phase 5.0 Performance: 结果缓存 (遵循“一次计算全局复用”的第一性原理)
        self._result_cache = OrderedDict()
        self._max_cache_size = 500
        self._cache_hits = 0
        
        logger.info(f"ConfidenceCalculator initialized (Cache enabled, Strict Design Mode)")
    
    async def calculate_C_text(
        self,
        completion_text: str,
        context_before: str,
        context_after: str,
        domain: str,
        domain_keywords: Dict[str, list],
        logprobs: Any = None
    ) -> tuple[float, Dict]:
        """
        计算内部语义置信度 C_text (严格版)
        
        公式: C_text = 0.5 × S_ctx + 0.3 × P_norm + 0.2 × K_domain
        """
        import re
        # 1. S_ctx: 上下文相似度 (设计要求: 前后各3句)
        def slice_context(text, n=3, last=True):
            sentences = re.split(r'[。！？.!?]', text)
            sentences = [s.strip() for s in sentences if s.strip()]
            if last: return " ".join(sentences[-n:]) if sentences else ""
            return " ".join(sentences[:n]) if sentences else ""

        ctx_b = slice_context(context_before, 3, last=True)
        ctx_a = slice_context(context_after, 3, last=False)
        full_context = f"{ctx_b} {ctx_a}"
        
        if self.semantic_extractor:
            S_ctx = await self.semantic_extractor.calculate_context_similarity(completion_text, full_context)
        else:
            S_ctx = self._simple_text_similarity(completion_text, full_context)
        
        # 2. K_domain: 领域知识一致性 (要求: 错误则清零)
        K_domain = self._calculate_domain_consistency_strict(completion_text, domain, domain_keywords)
        
        # 3. P_norm: 困惑度 (要求: 使用LLM自回归概率)
        if logprobs:
            P_norm = self._calculate_perplexity_from_logprobs(logprobs)
        else:
            # 兜底 heuristic
            P_norm = self._estimate_perplexity_score(completion_text)
        
        # 4. 按权重合成 (🚀 V6.2 Safety: Ensure no None values)
        S_ctx = S_ctx if S_ctx is not None else 0.5
        P_norm = P_norm if P_norm is not None else 0.5
        K_domain = K_domain if K_domain is not None else 0.5
        
        C_text = (
            self.WEIGHT_S_CTX * S_ctx +
            self.WEIGHT_P_NORM * P_norm +
            self.WEIGHT_K_DOMAIN * K_domain
        )
        
        details = {"S_ctx": S_ctx, "P_norm": P_norm, "K_domain": K_domain}
        logger.info(f"C_text = {C_text:.3f} (S_ctx={S_ctx:.3f}, P_norm={P_norm:.3f}, K_domain={K_domain:.3f})")
        return C_text, details
    
    def _calculate_perplexity_from_logprobs(self, logprobs_obj) -> float:
        """从 OpenAI logprobs 对象计算归一化 P_norm"""
        try:
            # logprobs_obj.content 是 token 列表，每个 token 有 logprob
            if not hasattr(logprobs_obj, 'content'): return 0.5
            
            probs = [t.logprob for t in logprobs_obj.content if hasattr(t, 'logprob')]
            if not probs: return 0.5
            
            avg_logprob = sum(probs) / len(probs)
            # 映射 logprob [-5, 0] 到 [0, 1]
            # -5 左右约等于 0.006 概率 (很低)，0 是 1.0 概率
            p_norm = max(0.0, min(1.0, (avg_logprob + 4) / 4)) 
            return p_norm
        except:
            return 0.5

    def _estimate_perplexity_score(self, text: str) -> float:
        """
        估算困惑度分数 (当无法获取logprobs时的兜底方案)
        
        简单的启发式: 文本越长, 词汇越多样, 困惑度可能越低 (假设是流畅的)
        或者, 简单返回一个中性值
        """
        # 这是一个非常简化的占位符，实际应使用更复杂的语言模型或统计方法
        word_count = len(text.split())
        if word_count < 5:
            return 0.3 # 短文本可能信息量不足，困惑度高
        elif word_count < 20:
            return 0.5 # 中等长度，中性
        else:
            return 0.7 # 较长文本，假设流畅度较高
    
    def _simple_text_similarity(self, text1: str, text2: str) -> float:
        """
        简单的文本相似度 (降级方案,当Sentence-BERT不可用时)
        
        基于词汇重叠率
        """
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = words1 & words2
        union = words1 | words2
        
        return len(intersection) / len(union) if union else 0.0
    
    def _calculate_domain_consistency_strict(self, text: str, domain: str, domain_keywords: Dict[str, list]) -> float:
        """严格领域校验: 关键词覆盖 + 错误清零"""
        if domain not in domain_keywords: return 0.5
        keywords = domain_keywords[domain]
        
        matched = [kw for kw in keywords if kw in text]
        coverage = min(1.0, len(matched) / 5.0) # 命中5个关键术语即满分
        
        # 检查是否有跨领域严重错误 (如算法中出现会计词汇)
        for other_domain, kws in domain_keywords.items():
            if other_domain != domain:
                if any(kw in text for kw in kws[:10]): # 只取核心词
                    logger.warning(f"Domain conflict detected: {other_domain} terms in {domain} text")
                    return 0.0 # 严格按设计方案：扣至0分
        
        return coverage
    
    async def calculate_C_multi(
        self,
        completion_text: str,
        fault_class: int,
        video_path: str,
        timestamp_start: float,
        timestamp_end: float,
        screenshot_path: Optional[str] = None,
        corrected_subtitles: List[Dict] = None,
        visual_features: Any = None
    ) -> tuple[float, Dict]:
        """计算外部多模态验证置信度 C_multi (严格版)"""
        details = {}
        
        if fault_class == 1:
            # 抽象逻辑断层: 验证载体 = 视频语音 (使用校正后的字幕)
            C_multi = await self._verify_with_step2_subtitles(
                completion_text,
                timestamp_start,
                timestamp_end,
                corrected_subtitles,
                details
            )
        
        elif fault_class == 2 and screenshot_path:
            # 具象性断层: 验证载体 = 截图OCR + 视觉元素
            C_multi = await self._verify_with_screenshot(
                completion_text,
                screenshot_path,
                details
            )
        
        elif fault_class == 3:
            # 过程性断层: 视频片段动作时序 + 语音描述
            C_multi = await self._verify_procedural_fault(
                completion_text, 
                corrected_subtitles, 
                timestamp_start, 
                timestamp_end, 
                visual_features, 
                details
            )
        else:
            # 无多模态证据,兜底
            C_multi = self.calculate_C_text_fallback(details)
            
        logger.info(f"C_multi = {C_multi:.3f} for class {fault_class}")
        
        return C_multi, details
    
    async def _verify_with_step2_subtitles(
        self,
        completion_text: str,
        start_sec: float,
        end_sec: float,
        corrected_subtitles: List[Dict],
        details: Dict
    ) -> float:
        """
        使用校正后的字幕验证 (对应设计文档中的Step 2 ASR)
        
        C_multi = S_asr (补全内容与ASR文本的语义相似度)
        """
        try:
            if not corrected_subtitles:
                details["asr_failed"] = "No corrected subtitles provided"
                return 0.5
            
            asr_text = extract_subtitle_text_in_range(corrected_subtitles, start_sec, end_sec)
            
            if asr_text:
                if self.semantic_extractor:
                    S_asr = await self.semantic_extractor.calculate_context_similarity(
                        completion_text,
                        asr_text
                    )
                else:
                    S_asr = self._simple_text_similarity(completion_text, asr_text)
                
                details["asr_text"] = asr_text[:50]  # 截取前50字符
                details["S_asr"] = S_asr
                
                return S_asr
            else:
                details["asr_failed"] = "No ASR text extracted in range"
                return 0.5
        
        except Exception as e:
            logger.error(f"ASR verification failed: {e}")
            details["error"] = str(e)
            return 0.5
    
    async def _verify_with_screenshot(
        self,
        completion_text: str,
        screenshot_path: str,
        details: Dict
    ) -> float:
        """
        使用Tesseract OCR + 视觉元素检测验证
        
        C_multi = 0.6 × M_text + 0.4 × M_visual
        """
        try:
            # 实际使用OCR提取文本
            from .ocr_utils import OCRExtractor
            
            ocr = OCRExtractor(lang="chi_sim+eng")
            ocr_text = ocr.extract_text_from_image(screenshot_path, preprocess=True)
            
            logger.info(f"OCR extracted: {len(ocr_text)} chars")
            
            # M_text: 文本匹配率
            if ocr_text:
                M_text = ocr.calculate_text_match_rate(completion_text, ocr_text)
            else:
                logger.warning("OCR extracted no text")
                M_text = 0.3  # 兜底值
            
            # M_visual: 视觉元素准确率
            M_visual = self._calculate_visual_element_score(screenshot_path, details)
            
            C_multi = self.CLASS2_TEXT_WEIGHT * M_text + self.CLASS2_VISUAL_WEIGHT * M_visual
            
            details["ocr_text"] = ocr_text[:100] if ocr_text else "N/A"
            details["M_text"] = M_text
            details["M_visual"] = M_visual
            
            logger.info(f"Screenshot verification: M_text={M_text:.3f}, M_visual={M_visual:.3f}, C_multi={C_multi:.3f}")
            
            return C_multi
        
        except ImportError:
            logger.error("OCR not available (pytesseract not installed)")
            details["ocr_error"] = "pytesseract not installed"
            return 0.5
        
    def _calculate_visual_element_score(self, screenshot_path: str, details: Dict) -> float:
        """
        计算视觉元素评分 M_visual (严格版)
        
        策略: 检查截图中的形状是否支撑了语义描述
        """
        import cv2
        from .visual_element_detection_helpers import VisualElementDetector
        
        frame = cv2.imread(screenshot_path)
        if frame is None: return 0.4
        
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 50, 150)
        
        # 获取基础形状
        rects = VisualElementDetector.detect_rectangles(edges)
        circles = VisualElementDetector.detect_circles(edges)
        arrows = VisualElementDetector.detect_arrows(edges, [], gray)
        
        # 简单评分：如果存在结构化图形(框/箭头)
        score = 0.5
        if arrows["total"] > 0: score += 0.2
        if rects > 1: score += 0.2
        if circles > 0: score += 0.1
        
        details["visual_shapes"] = {"rects": rects, "circles": circles, "arrows": arrows["total"]}
        return min(1.0, score)

    async def _verify_procedural_fault(
        self, text: str, subtitles: List[Dict], start: float, end: float, visual: Any, details: Dict
    ) -> float:
        """
        过程性断层验证 (严格版 $T_{step}$)
        C_multi = 0.5 × M_step + 0.5 × T_step
        """
        # 1. M_step: 文本步骤匹配 (利用字幕)
        real_text = extract_subtitle_text_in_range(subtitles, start, end)
        M_step = self._simple_text_similarity(text, real_text) if real_text else 0.4
        
        # 2. T_step: 动作时序匹配 (利用 visual_features.mse_list)
        # 逻辑：如果文本提到"然后"、"第二步"等转折点, 检查视频是否有相应的峰值(MSE)变化
        T_step = 0.5 # 默认中性
        if visual and hasattr(visual, 'mse_list') and visual.mse_list:
            # 假设MSE列表是时间序列，每个值代表帧间变化
            # 寻找显著变化点作为“步骤”的指示
            # 阈值50.0是经验值，可能需要根据实际数据调整
            peaks = sum(1 for m in visual.mse_list if m > 50.0)
            
            # 估算文本中的步骤数，例如通过换行符或句号
            text_steps = text.count("\n") + text.count("。") + text.count("；")
            
            # 简单匹配：如果视频变化次数与文本描述步骤数接近, 则 T_step 高
            if text_steps > 0:
                # 计算相似度，例如1 - 归一化后的差值
                # 差值越小，相似度越高
                similarity = 1 - min(1, abs(peaks - text_steps) / max(peaks, text_steps))
                T_step = 0.3 + 0.7 * similarity # 基础分0.3，最高可达1.0
            else:
                # 如果文本没有明确步骤，但视频有变化，也给一定分数
                T_step = 0.5 + (0.5 if peaks > 0 else 0)
        
        C_multi = self.CLASS3_STEP_WEIGHT * M_step + self.CLASS3_SEQUENCE_WEIGHT * T_step
        details.update({"M_step": M_step, "T_step": T_step})
        return C_multi
    
    def calculate_C_text_fallback(self, details: Dict) -> float:
        """兜底措施: C_multi = C_text * 0.8"""
        details["fallback"] = True
        return 0.5 # 暂用中性值, 理想中应在 calculate_confidence 中处理
    
    def _calculate_text_match_rate(self, text1: str, text2: str) -> float:
        """计算文本匹配率"""
        # 简单的词汇重叠率
        return self._simple_text_similarity(text1, text2)
    
    async def calculate_confidence(
        self,
        completion_text: str,
        context_before: str,
        context_after: str,
        domain: str,
        domain_keywords: Dict[str, list],
        fault_class: int,
        timestamp_start: float,
        timestamp_end: float,
        video_path: Optional[str] = None, # Make it optional/kwarg
        screenshot_path: Optional[str] = None,
        corrected_subtitles: List[Dict] = None,
        logprobs: Any = None,
        visual_features: Any = None
    ) -> ConfidenceCalculationResult:
        """主入口: 完整的双维度置信度计算 (严格按公式)"""
        
        # 🚀 Phase 5.0 Performance: 缓存检查 (LRU)
        # 基于核心输入生成 Hash
        input_data = {
            "text": completion_text,
            "ctx_b": context_before,
            "ctx_a": context_after,
            "domain": domain, # Added domain to cache key
            "fault_class": fault_class,
            "ts": (timestamp_start, timestamp_end),
            "video_path": video_path, # Added video_path
            "screenshot_path": screenshot_path, # Added screenshot_path
            "subtitles_hash": hashlib.md5(str(corrected_subtitles).encode()).hexdigest() if corrected_subtitles else None, # Hash subtitles
            "logprobs_hash": hashlib.md5(str(logprobs).encode()).hexdigest() if logprobs else None, # Hash logprobs
            "vis_id": id(visual_features) if visual_features else None # visual_features might not be hashable directly, use id
        }
        input_hash = hashlib.md5(str(input_data).encode()).hexdigest()
        
        if input_hash in self._result_cache:
            self._cache_hits += 1
            logger.info(f"[PERF] Confidence Cache Hit: {input_hash[:8]} (Total Hits: {self._cache_hits})")
            return self._result_cache[input_hash]
        
        # 1. 计算 C_text (40%)
        C_text, text_details = await self.calculate_C_text(
            completion_text, context_before, context_after, domain, domain_keywords, logprobs
        )
        
        # 2. 计算 C_multi (60%)
        C_multi, multi_details = await self.calculate_C_multi(
            completion_text, fault_class, video_path, timestamp_start, timestamp_end, screenshot_path, corrected_subtitles, visual_features
        )
        
        # Safety check for None returns
        if C_text is None: C_text = 0.5
        if C_multi is None: C_multi = 0.5
        
        # 3. 综合置信度 C_total = 0.4*C_text + 0.6*C_multi
        C_total = self.WEIGHT_C_TEXT * C_text + self.WEIGHT_C_MULTI * C_multi
        
        # 4. 划分等级
        level = "low"
        if C_total >= self.THRESHOLD_HIGH: level = "high"
        elif C_total >= self.THRESHOLD_MEDIUM: level = "medium"
        
        # 5. 处理建议
        suggestion = ""
        if level == "high": suggestion = "直接纳入Markdown笔记,无需人工审核"
        elif level == "medium": suggestion = "纳入笔记并标注'中等置信度:建议核对视频/截图'"
        else: suggestion = "不直接纳入笔记,生成候选补全内容并嵌入人工修正入口"
        
        logger.info(f"Confidence: C_total={C_total:.3f} ({level}), "
                   f"C_text={C_text:.3f}, C_multi={C_multi:.3f}")
        
        result = ConfidenceCalculationResult(
            C_text=C_text,
            C_multi=C_multi,
            C_total=C_total,
            confidence_level=level,
            S_ctx=text_details["S_ctx"],
            K_domain=text_details["K_domain"],
            P_norm=text_details["P_norm"],
            multimodal_details=multi_details,
            processing_suggestion=suggestion
        )
        
        # 更新缓存
        if len(self._result_cache) >= self._max_cache_size:
            self._result_cache.popitem(last=False)
        self._result_cache[input_hash] = result
        
        return result

    def _calculate_visual_element_score(
        self,
        screenshot_path: str,
        details: Dict = None
    ) -> float:
        """
        计算视觉元素评分M_visual (增强版)
        
        基于增强的视觉元素检测结果:
        - Arrow confidence (箭头置信度)
        - Element diversity (元素多样性)
        - Diagram coherence (图表连贯性)
        """
        
        try:
            import cv2
            from .visual_element_detection_helpers import VisualElementDetector
            
            if details is None:
                details = {}
            
            # 读取图像
            frame = cv2.imread(screenshot_path)
            if frame is None:
                logger.warning(f"Cannot read screenshot: {screenshot_path}")
                return 0.5
            
            # Use detector directly (avoiding dummy video extractor)
            detector = VisualElementDetector()
            elements = detector.analyze_frame(frame)
            
            # 1. 箭头分 (0.4)
            arrow_data = elements.get("arrows", {"total": 0, "confidence": 0.5})
            arrow_count = arrow_data.get("total", 0)
            arrow_conf = arrow_data.get("confidence", 0.5)
            
            arrow_score = 0.0
            if arrow_count > 0:
                # 基础置信度 + 数量奖励 (每3个箭头+0.1)
                arrow_score = min(arrow_conf + (arrow_count // 3) * 0.1, 1.0)
            
            # 2. 多样性分 (0.3)
            # 矩形、圆形、菱形、云朵
            types = ["rectangles", "circles", "diamonds", "clouds"]
            active_types = sum(1 for t in types if elements.get(t, 0) > 0)
            diversity_score = active_types / len(types)
            
            # 3. 连贯性分 (0.3)
            # 连接符、线条
            connectors = elements.get("connectors", {"total": 0})
            total_elements = elements.get("total", 0)
            
            coherence_score = 0.3 # 基础
            if connectors.get("t_junctions", 0) > 0 or connectors.get("intersections", 0) > 0:
                coherence_score += 0.4
            if elements.get("lines", 0) > 5:
                coherence_score += 0.3
                
            coherence_score = min(coherence_score, 1.0)
            
            # 综合
            M_visual = 0.4 * arrow_score + 0.3 * diversity_score + 0.3 * coherence_score
            
            logger.info(f"Visual scoring for {screenshot_path}: arrows={arrow_count}, "
                       f"types={active_types}, M_visual={M_visual:.3f}")
            
            if details is not None:
                details["visual_elements"] = elements
            
            return M_visual
            
        except Exception as e:
            logger.error(f"Visual element scoring failed: {e}")
            return 0.5

