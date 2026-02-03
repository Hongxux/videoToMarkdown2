"""
Multimodal Fusion Decision - Week 2 Day 10

Implements the three-layer fusion system for enhancement type decision:
- Layer 1: Semantic features (weight 0.4)
- Layer 2: Visual features (weight 0.5)
- Layer 3: Domain prior (weight 0.1)

Follows user document "图文并茂的多模态判断的参数依据"
"""

import logging
from typing import Dict, Optional, Tuple, List, Any
from dataclasses import dataclass
import asyncio
from dataclasses import dataclass, field

from .semantic_feature_extractor import SemanticFeatures
from .visual_feature_extractor import VisualFeatures
from .data_structures import EnhancementType
from .visual_element_detection_helpers import VisualElementDetector

logger = logging.getLogger(__name__)


@dataclass
class MultimodalDecision:
    """
    多模态融合决策结果
    """
    enhancement_type: EnhancementType  # 最终决策
    
    # 置信度分数
    final_confidence: float  # 综合置信度
    semantic_contribution: float  # 语义贡献 (0.4权重后)
    visual_contribution: float  # 视觉贡献 (0.5权重后)
    domain_contribution: float  # 领域贡献 (0.1权重后)
    
    # 决策依据
    semantic_type: str  # 语义判断的类型
    visual_type: str  # 视觉判断的类型
    decision_reason: str  # 决策理由
    
    # 优化参数 (V6.4)
    visual_lag_seconds: float = 0.0 # 视觉内容滞后补偿 (秒)

    # 🚀 Phase 7.1: Full Decision Trace (Transpacency)
    decision_trace: List[str] = field(default_factory=list) # e.g. ["Step 0: Pass", "Step 1: Process Anchor", "Step 2: Valid"]


class MultimodalFusionDecider:
    """
    多模态融合决策器
    
    实现三层融合架构:
    - 语义特征 (权重0.4)
    - 画面特征 (权重0.5)
    - 领域先验 (权重0.1)
    
    所有参数从配置文件加载
    """
    
    def __init__(self, domain: str = "", config: Dict = None, semantic_extractor: Any = None, visual_extractor: Any = None):
        """
        Args:
            domain: 领域标签 (算法/AI框架/数学)
            config: Module2配置字典 (如果为None,自动加载)
            semantic_extractor: 共享的语义特征提取器 (可选, Phase 3.8 优化)
            visual_extractor: 共享的视觉特征提取器 (可选, Phase 3.8 优化)
        """
        self.domain = domain
        self.visual_extractor = visual_extractor
        
        # 加载配置
        if config is None:
            from .config_loader import load_module2_config
            config = load_module2_config()
        
        self.config = config
        fusion_config = config.get("multimodal_fusion_config", {})
        
        # 从配置加载权重 (Phase 3.8 动态权重)
        self.WEIGHT_VIDEO = 1.2
        self.WEIGHT_SCREENSHOT = 1.0
        self.WEIGHT_TEXT = 0.8
        
        # 从配置加载领域规则
        self.DOMAIN_RULES = fusion_config.get("domain_rules", {})
        
        # 从配置加载决策阈值
        thresholds = fusion_config.get("decision_thresholds", {})
        self.HIGH_CONFIDENCE = thresholds.get("high_confidence", 0.7)
        self.LOW_CONFIDENCE = thresholds.get("low_confidence", 0.5)
        
        # 🚀 Phase 3.8: 初始化素材验证器 (支持共享 Extractor)
        from .material_validator import MaterialValidator
        self.validator = MaterialValidator(config=config, extractor=semantic_extractor, visual_extractor=visual_extractor)
        
        # 🚀 Phase 4.3: 数学公式专用检测器 (V3 Logic)
        from .math_formula_visual_detector import MathFormulaVisualDetector
        self.math_detector = None
        if visual_extractor:
             self.math_detector = MathFormulaVisualDetector(visual_extractor)
        
        logger.info(f"MultimodalFusionDecider optimized for Phase 3.8 (Material Validation Enabled)")

    async def decide_enhancement_type(
        self,
        semantic: SemanticFeatures,
        visual: VisualFeatures,
        domain: Optional[str] = None,
        **kwargs
    ) -> MultimodalDecision:
        """
        [Standard Entry Point] Redirects to First Principles Refined Logic.
        Supports extra arguments: enh_id, source_video, time_window, subtitles, fault_text
        """
        # Extract new arguments with defaults if missing (Backward Compatibility)
        enh_id = kwargs.get("enh_id", "LEGACY_CALL")
        source_video = kwargs.get("source_video", "")
        time_window = kwargs.get("time_window", (0.0, 1.0))
        subtitles = kwargs.get("subtitles", [])
        fault_text = kwargs.get("fault_text", "")

        return await self.decide_enhancement_type_refined(
            enh_id=enh_id,
            semantic=semantic,
            visual=visual,
            source_video=source_video,
            time_window=time_window,
            subtitles=subtitles,
            fault_text=fault_text
        )

    def _calculate_visual_lag(
        self,
        has_formula: bool, 
        has_structure: bool, 
        is_dynamic: bool = False,
        animation_end_time: Optional[float] = None,
        voice_end: float = 0.0
    ) -> float:
        """
        Calculate visual lag based on Scenario Distinction (V6.4 Best Practice)
        """
        # Scenario 3: Dynamic Animation (Align to Animation End)
        if is_dynamic and has_structure:
             if animation_end_time is not None:
                 # Lag = (AnimationEnd - VoiceEnd) + 0.2s Stable Time
                 # Note: If Animation ends BEFORE voice, lag is just 0.2s
                 lag = animation_end_time - voice_end + 0.2 
                 return max(0.2, lag)
             else:
                 return 3.0 # Fallback
        # Scenario 2: Formula (Write delay)
        elif has_formula:
             return 2.5
        # Scenario 2: Static Structure (Display delay)
        elif has_structure:
             return 1.5
        # Scenario 1: Text (Small buffer)
        else:
             return 0.5

    def _find_animation_end_time(self, frames: List[Any], timestamps: List[float]) -> Optional[float]:
        """
        Identify the timestamp where the animation stabilizes.
        Logic: Find the first frame index i where frames[i:] have low short-term MSE.
        """
        if len(frames) < 2 or len(timestamps) != len(frames):
            return None
            
        import numpy as np
        import cv2
        
        # 1. Calculate Short-Term MSEs
        mses = []
        for i in range(len(frames)-1):
            f1 = frames[i]
            f2 = frames[i+1]
            if f1.shape != f2.shape: 
                mses.append(100.0)
                continue
            
            # Convert to gray if needed
            if len(f1.shape) == 3:
                g1 = cv2.cvtColor(f1, cv2.COLOR_BGR2GRAY).astype(np.float32)
                g2 = cv2.cvtColor(f2, cv2.COLOR_BGR2GRAY).astype(np.float32)
            else:
                g1 = f1.astype(np.float32)
                g2 = f2.astype(np.float32)
                
            mse = np.mean((g1 - g2) ** 2)
            mses.append(mse)
            
        # 2. Find Stable Suffix
        # Threshold 0.8 (same as judge_structure_dynamic)
        threshold = 0.8
        
        # We look for the START of the stable suffix
        # If mses = [50, 40, 0.1, 0.2, 0.1], stable starts at index 2 (frame 3 -> timestamp[2+1]?)
        # Frame i, i+1 correspond to mse[i].
        # If mse[k] < thresh, it means frame k and k+1 are similar.
        # If ALL mse[k:] < thresh, then frame k is the first stable frame.
        
        search_range = range(len(mses))
        for k in search_range:
            # Check if all subsequent MSEs are low
            is_stable = True
            for j in range(k, len(mses)):
                if mses[j] >= threshold:
                    is_stable = False
                    break
            
            if is_stable:
                # Frame k+1 matches Frame k, and Frame k+2 matches k+1...
                # So Frame k is the start of stability? 
                # Actually if mse[k] is low, change stopped between k and k+1.
                # So Frame k+1 is definitely stable relative to k.
                # We want the FIRST frame where change stops.
                # If mse[k] is the first low value in a chain of lows.
                # Then Frame k+1 is the stable state.
                return timestamps[k+1]
                
        # If no stable suffix found (e.g. dynamic until the very end)
        # Return the last timestamp
        return timestamps[-1]


    async def decide_enhancement_type_refined(
        self,
        enh_id: str,
        semantic: SemanticFeatures,
        visual: VisualFeatures,
        source_video: str,
        time_window: Tuple[float, float],
        subtitles: List[Dict],
        fault_text: str
    ) -> MultimodalDecision:
        """
        🚀 Phase 7.0: First Principles Decision Chain (Noise Filter -> Type Anchoring -> Quality Check)
        Strictly aligned with user's "Cognitive Gain" and "Noise Removal" definitions.
        """
        start_t, end_t = time_window
        duration = end_t - start_t
        
        # =========================================================================
        # 🟢 Step 0: Noise Filter (Pre-cleaning)
        # =========================================================================
        is_noise, noise_reason = self._check_noise_filter(semantic, visual, duration, fault_text)
        if is_noise:
             # Discard by returning TEXT with low confidence (or a special NOISE type if supported, but TEXT is safest fallback)
             # "Text of the noise" is just the ASR which is already there, so we effectively discard the enhancement.
             logger.info(f"🗑️ [Noise Filter] {enh_id} discarded: {noise_reason}")
             return MultimodalDecision(
                enhancement_type=EnhancementType.TEXT,
                final_confidence=0.1, # V.Low confidence -> likely skipped by downstream
                semantic_contribution=0.0, visual_contribution=0.0, domain_contribution=0.0,
                semantic_type="noise", visual_type="noise",
                decision_reason=f"Noise Filter: {noise_reason}",
                decision_trace=[
                     f"Step 0 (Noise): REJECT ({noise_reason})",
                     "Step 1: Skipped",
                     "Step 2: Skipped"
                ],
                visual_lag_seconds=0.0
             )

        # =========================================================================
        # 🔵 Step 1: First Principles Type Anchoring (Understanding Purpose)
        # =========================================================================
        # Based on "Cognitive Purpose" (semantic.knowledge_type)
        # We assume semantic extractor is now accurate (abstract/spatial/process)
        
        target_type = EnhancementType.TEXT # Default
        anchor_reason = ""
        
        if self.math_detector:
             # V5 Upgrade: Use new metrics for classification
             is_formula, conf = self.math_detector.is_static_formula(mid_frame)
             if is_formula:
                 visual.has_math_formula = True
                 
                 # Logic Inline (Mirroring MathDetector.classify_dynamic_type)
                 ssim_seq = getattr(visual, "ssim_seq", [])
                 edge_flux = getattr(visual, "avg_edge_flux", 0.0)
                 avg_mse = visual.avg_mse
                 duration = time_window[1] - time_window[0]
                 min_ssim = min(ssim_seq) if ssim_seq else 1.0
                 
                 dynamic_type = "UNKNOWN"
                 if min_ssim < 0.8 and duration < 1.5:
                      dynamic_type = "TRANSITION"
                 elif duration >= 2.0 and min_ssim > 0.9 and edge_flux > 0.05:
                      dynamic_type = "DERIVATION"
                 elif avg_mse < 50:
                      dynamic_type = "STATIC"
                 
                 if dynamic_type == "TRANSITION":
                      # Downgrade to Static
                      visual.is_dynamic = False
                      visual.is_static = True # Artificial override
                      anchor_reason = f"Math Transition Detected ({dynamic_type})"
                 elif dynamic_type == "DERIVATION":
                      visual.is_dynamic = True
                      anchor_reason = f"Math Derivation Detected ({dynamic_type})"
                 
        # 1. Understanding Evolution (Process)
        if semantic.knowledge_type == "process":
            # Cross-Check: Visual MUST be dynamic
            # "Pseudo-Process" Risk: Semantic says "traverse", Visual is static code.
            if visual.is_dynamic or visual.action_density > 0.05 or visual.has_math_formula: # Math is process
                 target_type = EnhancementType.VIDEO
                 # Double Insurance: If result is complex structure, add Screenshot
                 if visual.has_static_visual_structure and visual.element_count > 3:
                     target_type = EnhancementType.VIDEO_AND_SCREENSHOT
                     anchor_reason = "Process Anchor (Evolution + Structure Result)"
                 else:
                     anchor_reason = "Process Anchor (Dynamic Evolution)"
            else:
                 # Degrade: Visual didn't support process.
                 # Check if it's "Hidden-Process" (Visual Flux high but not marked dynamic?) -> unlikely if VFE is good.
                 # Fallback to Text (Concept) or Screenshot (if structure exists)
                 if visual.has_static_visual_structure:
                     target_type = EnhancementType.SCREENSHOT
                     anchor_reason = "Process Anchor Vetoed (Static Visual) -> Degraded to Spatial"
                 else:
                     target_type = EnhancementType.TEXT
                     anchor_reason = "Process Anchor Vetoed (No Visual Gain) -> Degraded to Text"

        # 2. Understanding Structure (Spatial)
        elif semantic.knowledge_type == "spatial":
            # Cross-Check: Visual MUST have structure
            if visual.has_static_visual_structure or visual.element_count >= 2 or visual.has_math_formula:
                target_type = EnhancementType.SCREENSHOT
                anchor_reason = "Spatial Anchor (Structure/Logic)"
            else:
                # Degrade: "Pseudo-Structure" (Teacher waving)
                target_type = EnhancementType.TEXT
                anchor_reason = "Spatial Anchor Vetoed (No Visual Structure) -> Degraded to Text"
                
        # 3. Understanding Concept (Abstract)
        else: # abstract
            # Cross-Check: Even if visual is dynamic, does it hold information?
            # If visual is just teacher, ignore.
            # But if visual is Math Formula, override to Video/Screenshot because Math is never just "abstract concept" in this context.
            if visual.has_math_formula:
                 # V5 Logic: Only separate "Derivation" (Video) vs "Transition" (Screenshot)
                 if visual.is_dynamic: # Already filtered by dynamic_type check above
                     target_type = EnhancementType.VIDEO_AND_SCREENSHOT
                     anchor_reason = f"Concept Anchor Override (Math Derivation is Process)"
                     # V5 Optimization: Add lag for writing completion
                     lag = self._calculate_visual_lag(True, True, True, voice_end=time_window[1])
                 else:
                     target_type = EnhancementType.SCREENSHOT
                     anchor_reason = f"Concept Anchor Override (Math Static/Transition)"
                     lag = 0.0
                 
                 return self._finalize_decision(semantic, visual, target_type, 1.0, anchor_reason, target_type, lag)
            elif visual.has_static_visual_structure and visual.element_count > 5:
                 # High density chart might be useful even for abstract talk
                 target_type = EnhancementType.SCREENSHOT
                 anchor_reason = "Concept Anchor Override (High Density Information)"
            else:
                 target_type = EnhancementType.TEXT
                 anchor_reason = "Concept Anchor (Abstract/Definition)"

        # =========================================================================
        # 🟠 Step 2: Quality & Cognitive Threshold Validation
        # =========================================================================
        # Validate the selected target_type. If bad, degrade.
        
        final_type = target_type
        final_conf = 0.8 # Base high
        val_reason = ""
        
        # Calculate Visual Lag (Scenario Distinction)
        lag = self._calculate_visual_lag(
            has_formula=visual.has_math_formula,
            has_structure=visual.has_static_visual_structure,
            is_dynamic=(final_type in [EnhancementType.VIDEO, EnhancementType.VIDEO_AND_SCREENSHOT])
        )
        
        if final_type in [EnhancementType.VIDEO, EnhancementType.VIDEO_AND_SCREENSHOT]:
            # Validation: Clarity, Duration, Action Completeness
            if duration < 1.0 and not visual.has_math_formula: 
                 # Too short for video (unless math stroke)
                 final_type = EnhancementType.TEXT
                 val_reason = "Video too short (<1s)"
            elif visual.avg_mse > 500: # Too blurry/noisy? (Simple heuristic)
                 # Maybe keep but lower confidence
                 final_conf = 0.6
                 val_reason = "High Visual Noise"
            elif visual.confidence < 0.5:
                 final_type = EnhancementType.SCREENSHOT if visual.has_static_visual_structure else EnhancementType.TEXT
                 val_reason = "Low Video Confidence (Degraded)"
                 
        elif final_type == EnhancementType.SCREENSHOT:
            # Validation: Structure Completeness, Blur
            # Assuming VisualFeatures has 'confidence' that reflects blur/completeness
            if visual.confidence < 0.6:
                 final_type = EnhancementType.TEXT
                 val_reason = "Low Screenshot Quality (Blur/Incomplete)"
                 
        # Text is always safe fallback
        
        return MultimodalDecision(
            enhancement_type=final_type,
            final_confidence=final_conf,
            semantic_contribution=0.0, visual_contribution=0.0, domain_contribution=0.0,
            semantic_type=semantic.knowledge_type,
            visual_type=visual.visual_type,
            decision_trace=[
                "Step 0 (Noise): Pass (Clean signal)",
                f"Step 1 (First Principles): {anchor_reason} (Type={target_type.value})",
                f"Step 2 (Quality): {'Pass' if not val_reason else 'Degraded'} ({val_reason if val_reason else 'Meets thresholds'})",
                f"Meta (Lag): Applied +{lag}s lag"
            ],
            decision_reason=f"{anchor_reason} -> {val_reason if val_reason else 'Quality Pass'}",
            visual_lag_seconds=lag
        )

    def _check_noise_filter(self, semantic: SemanticFeatures, visual: VisualFeatures, duration: float, fault_text: str) -> Tuple[bool, str]:
        """
        Step 0: Pre-cleaning "Visual/Statistical Noise"
        Returns (is_noise, reason)
        """
        # 1. Visual Noise: Dynamic action but no Gain (e.g. cursor hovering, teacher waving)
        # Rule: Action Density > 0 but Semantic is Abstract AND Visual Structure is low
        if visual.is_dynamic:
             if semantic.knowledge_type == "abstract" and not visual.has_math_formula and visual.element_count < 2:
                 # Teacher waving or cursor moving over nothing
                 return True, "Visual Noise: Dynamic Abstract Content (e.g. waving) without Structure"
             
             if visual.action_density < 0.1 and duration > 5.0 and not visual.has_math_formula:
                 # Very sparse action in long clip
                 return True, "Visual Noise: Sparse Action (<10%) in long clip"

        # 2. Statistical Noise: Low visual info
        if not visual.is_dynamic and not visual.has_static_visual_structure and visual.element_count == 0:
             # Empty frame or just background
             if semantic.knowledge_type != "abstract": # If it's abstract, Text is fine. If Process/Spatial, this is noise/missing.
                 return True, "Visual Noise: Empty Visuals for Non-Abstract content"

        # 3. Text Noise: (Can be expanded with LLM entropy check)
        # For now, simplistic length/repetition check
        if len(fault_text) < 2:
             return True, "Text Noise: Too short"
        
        return False, ""
    
    def _extract_semantic_score(
        self,
        semantic: SemanticFeatures
    ) -> tuple[float, str]:
        """
        从语义特征提取分数和类型
        
        Returns:
            (score, type)
        """
        # 直接使用语义特征的置信度
        score = semantic.confidence
        type_label = semantic.knowledge_type  # "process" | "spatial" | "abstract"
        
        return score, type_label
    
    def _extract_visual_score(
        self,
        visual: VisualFeatures
    ) -> tuple[float, str]:
        """
        从视觉特征提取分数和类型
        
        Returns:
            (score, type)
        """
        # 直接使用视觉特征的置信度
        score = visual.confidence
        type_label = visual.visual_type  # "static" | "dynamic" | "mixed"
        
        return score, type_label
    
    def _get_domain_weight(
        self,
        domain: str,
        semantic_type: str,
        visual_type: str
    ) -> float:
        """
        获取领域先验权重
        
        Args:
            domain: 领域名称
            semantic_type: 语义类型
            visual_type: 视觉类型
        
        Returns:
            领域权重分数
        """
        if domain not in self.DOMAIN_RULES:
            return 0.5  # 未知领域返回中性值
        
        rules = self.DOMAIN_RULES[domain]
        
        # 根据语义和视觉类型选择对应权重
        if semantic_type == "process" or visual_type == "dynamic":
            return rules.get("process", 0.5)
        elif semantic_type == "spatial" or visual_type == "static":
            return rules.get("spatial", 0.5)
        else:
            return 0.5
    
    def _make_final_decision(
        self,
        semantic: SemanticFeatures,
        visual: VisualFeatures,
        semantic_type: str,
        visual_type: str,
        final_confidence: float
    ) -> tuple[EnhancementType, str]:
        """
        根据融合结果做最终决策
        
        决策规则 (来自用户文档):
        1. 语义和视觉都指向动态过程 → 视频
        2. 语义和视觉都指向静态结构 → 截图
        3. 两者置信度都低 → 文字补充
        4. 冲突场景 → 视觉特征优先 (权重更高)
        
        Args:
            semantic: 语义特征
            visual: 视觉特征
            semantic_type: 语义类型
            visual_type: 视觉类型
            final_confidence: 综合置信度
        
        Returns:
            (enhancement_type, reason)
        """
        # 💥 逻辑修正: 获取断层类别 (如果是1类逻辑断层,优先文字)
        fault_class = semantic.knowledge_type # 这里的 mapping 稍后校验
        # 检查真正的类别 (从 semantic 对象中提取, 假设已经在 extraction 阶段注入)
        
        # 规则0: 针对 Class 1 (逻辑断层) 的特殊处理
        # 如果是抽象逻辑问题，即使有画面波动，也优先生成文字解释
        if semantic_type == "abstract":
            if visual.confidence < 0.8: # 除非画面特征极其显著
                return EnhancementType.TEXT, "逻辑断层优先文字补充"
                
        # 规则1: 强冲突保护 - 语义建议截图 vs 视觉动态
        # 在教学视频中，鼠标移动或局部刷新经常导致视觉误判为 dynamic。
        # 如果语义明确要求空间结构 (Screenshot)，应无视中等强度的视觉波动。
        if semantic_type == "spatial":
            if visual.confidence < 0.85: # 除非视觉极其确定是实质性的动态过程
                return EnhancementType.SCREENSHOT, "语义强暗示为空间结构, 覆盖视觉波动"

        # 规则2: 都指向动态过程
        if semantic_type == "process" and visual_type == "dynamic":
            return EnhancementType.VIDEO, "语义和视觉都指向动态过程"
        
        # 规则3: 都指向静态结构
        if semantic_type == "spatial" and visual_type == "static":
            return EnhancementType.SCREENSHOT, "语义和视觉都指向静态结构"
        
        # 规则4: 两者置信度都低
        if semantic.confidence < 0.5 and visual.confidence < 0.5:
            return EnhancementType.TEXT, "语义和视觉置信度都低,降级为文字补充"
        
        # 规则5: 视觉特征极强优先
        if visual.confidence >= 0.8:
            if visual_type == "dynamic":
                return EnhancementType.VIDEO, "视觉特征极强, 确认为实质性动态过程"
            elif visual_type == "static":
                return EnhancementType.SCREENSHOT, "视觉特征极强, 确认为静态画面"
        
        # 规则6: 语义特征优先 (降级场景)
        if semantic.confidence >= 0.7:
            if semantic_type == "process":
                return EnhancementType.VIDEO, "语义特征优先, 判断为过程性知识"
            elif semantic_type == "spatial":
                return EnhancementType.SCREENSHOT, "语义特征优先, 判断为空间性知识"
        
        # 默认: 根据综合置信度决策
        if final_confidence >= 0.7:
            # 根据更强的信号
            if visual.confidence > semantic.confidence:
                if visual_type == "dynamic":
                    return EnhancementType.VIDEO, "综合判断倾向视频"
                else:
                    return EnhancementType.SCREENSHOT, "综合判断倾向截图"
            else:
                if semantic_type == "process":
                    return EnhancementType.VIDEO, "综合判断倾向视频"
                else:
                    return EnhancementType.SCREENSHOT, "综合判断倾向截图"
        
        # 兜底: 文字补充
        return EnhancementType.TEXT, "置信度不足,降级为文字补充"
