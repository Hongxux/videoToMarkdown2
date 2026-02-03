"""
Text Supplement Generator - Week 3 Day 13-15

Generates text supplements for detected faults using LLM.

Key requirements from user document:
1. Based on merged_segment for natural fusion
2. Mimic original text style and terminology
3. Integrate confidence calculation
4. Length control (50-300 chars)
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TextSupplement:
    """
    文字补充结果
    
    对应data_structures.py中的TextSupplement
    """
    supplement_id: str
    fault_id: str
    
    # 补充内容
    generated_text: str
    fusion_position: str  # "before" | "after" | "replace"
    
    # 基于哪个merged_segment
    source_segment_id: str
    original_segment_text: str
    
    # 融合后效果
    fused_text: str  # merged_segment融合补充后的完整文本
    
    # 置信度
    C_text: float
    C_multi: float
    C_total: float
    confidence_level: str  # "high" | "medium" | "low"
    
    # 原始Logprobs (用于后续置信度精确计算)
    logprobs: Optional[Any] = None


class TextGenerator:
    """
    文字补充生成器
    
    核心流程:
    1. 接收FaultCandidate
    2. 使用LLM生成补充文字
    3. 基于merged_segment自然融合
    4. 计算置信度
    5. 返回TextSupplement
    """
    
    def __init__(
        self,
        llm_client,
        confidence_calculator,
        config: Dict = None
    ):
        """
        Args:
            llm_client: LLM客户端
            confidence_calculator: ConfidenceCalculator实例
            config: 配置字典
        """
        self.llm = llm_client
        self.confidence_calc = confidence_calculator
        
        # 加载配置
        if config is None:
            from .config_loader import load_module2_config
            config = load_module2_config()
        
        text_config = config.get("text_generation_config", {})
        
        # LLM参数
        llm_config = text_config.get("llm", {})
        self.model = llm_config.get("model", "deepseek-chat")
        self.temperature = llm_config.get("temperature", 0.3)
        self.max_tokens = llm_config.get("max_tokens", 500)
        
        # 长度限制
        length_config = text_config.get("length_constraints", {})
        self.min_length = length_config.get("min_length", 10)
        self.max_length = length_config.get("max_length", 300)
        self.target_length = length_config.get("target_length", 150)
        
        logger.info(f"TextGenerator initialized")
        logger.info(f"Length constraints: {self.min_length}-{self.max_length} chars")
    
    async def generate_supplement(
        self,
        fault_candidate,
        merged_segment,
        corrected_subtitles: List,
        context_before: str,
        context_after: str,
        domain: str,
        domain_keywords: Dict[str, List[str]],
        video_path: Optional[str] = None
    ) -> TextSupplement:
        """
        为单个断层生成文字补充
        
        Args:
            fault_candidate: FaultCandidate对象
            merged_segment: CrossSentenceMergedSegment对象 (用于融合)
            corrected_subtitles: 字幕列表 (用于置信度验证)
            context_before: 前文上下文
            context_after: 后文上下文
            domain: 领域
            domain_keywords: 领域关键词
            video_path: 视频路径 (用于多模态校验)
        
        Returns:
            TextSupplement对象
        """
        logger.info(f"Generating supplement for fault {fault_candidate.fault_id}")
        
        # 1. 构建LLM prompt
        prompt = self._build_generation_prompt(
            fault_candidate,
            merged_segment,
            context_before,
            context_after,
            domain
        )
        
        # 2. 调用LLM生成 (包含logprobs)
        generated_text, lprobs = await self._call_llm_generate(prompt)
        
        # 3. 质量检查和修正
        generated_text = self._quality_check(generated_text, merged_segment)
       
        # 4. 决定融合位置 (增强版)
        fusion_position, anchor_text = self._decide_fusion_position(
            fault_candidate,
            merged_segment
        )
        
        # 5. 融合到merged_segment
        fused_text = self._fuse_with_segment(
            generated_text,
            merged_segment.full_text,
            fusion_position,
            anchor_text  # 传递锚点
        )
        
        # 6. 计算置信度 (传递 logprobs 供 P_norm 计算)
        confidence_result = await self.confidence_calc.calculate_confidence(
            completion_text=generated_text,
            context_before=context_before,
            context_after=context_after,
            domain=domain,
            domain_keywords=domain_keywords,
            fault_class=fault_candidate.fault_class.value if hasattr(fault_candidate.fault_class, 'value') else fault_candidate.fault_class,
            video_path=video_path,
            corrected_subtitles=corrected_subtitles,
            timestamp_start=fault_candidate.timestamp_start,
            timestamp_end=fault_candidate.timestamp_end,
            logprobs=lprobs
        )
        
        # 7. 构建结果
        supplement = TextSupplement(
            supplement_id=f"{fault_candidate.fault_id}_SUP",
            fault_id=fault_candidate.fault_id,
            generated_text=generated_text,
            fusion_position=fusion_position,
            source_segment_id=fault_candidate.source_segment_id,
            original_segment_text=merged_segment.full_text,
            fused_text=fused_text,
            C_text=confidence_result.C_text,
            C_multi=confidence_result.C_multi,
            C_total=confidence_result.C_total,
            confidence_level=confidence_result.confidence_level,
            logprobs=lprobs
        )
        
        logger.info(f"Generated supplement: {len(generated_text)} chars, "
                   f"confidence={confidence_result.C_total:.3f} ({confidence_result.confidence_level})")
        
        return supplement
    
    def _build_generation_prompt(
        self,
        fault_candidate,
        merged_segment,
        context_before: str,
        context_after: str,
        domain: str
    ) -> str:
        """
        构建LLM生成prompt
        
        关键要求:
        1. 模仿原文用词风格
        2. 基于merged_segment自然融合
        3. 控制长度
        """
        fault_class_desc = {
            1: "抽象逻辑缺失或指代不明",
            2: "视觉信息缺失(但本次生成文字描述)",
            3: "过程步骤缺失"
        }
        
        prompt = f"""你是知识讲解视频的文字补充专家。请为以下断层生成补充文字。

【原文段落】
{merged_segment.full_text}

【上下文】
前文: {context_before if context_before else "无"}
后文: {context_after if context_after else "无"}

【断层类型】{fault_class_desc.get(fault_candidate.fault_class.value, "未知")}

【断层文本】{fault_candidate.fault_text}

【检测理由】{fault_candidate.detection_reason}

【领域】{domain}

【补充要求】
1. **模仿原文风格**: 用词、句式与原文段落保持一致
2. **自然融合**: 补充内容应能无缝插入原文段落
3. **长度控制**: {self.min_length}-{self.max_length}字符,目标约{self.target_length}字符
4. **准确性**: 基于视频内容(通过上下文推断),不要胡编乱造
5. **完整性**: 补充完整的概念/步骤/解释,不要半句话

【输出要求】
直接输出补充的文字内容,不要包含任何前缀、后缀或解释。
补充内容:"""

        return prompt
    
    async def _call_llm_generate(self, prompt: str) -> tuple[str, Any]:
        """
        调用LLM生成文本并获取logprobs
        
        Returns:
            (生成的文本, logprobs)
        """
        try:
            # 调用LLM (handle 3 values: content, metadata, lprobs)
            response_text, metadata, lprobs = await self.llm.complete_text(
                prompt,
                system_message="你是专业的知识讲解文字补充专家,擅长模仿原文风格生成自然流畅的补充内容。"
            )
            
            # 清理输出
            generated_text = response_text.strip()
            
            # 移除可能的前缀
            prefixes_to_remove = ["补充内容:", "补充:", "答:", "生成:", "文字:"]
            for prefix in prefixes_to_remove:
                if generated_text.startswith(prefix):
                    generated_text = generated_text[len(prefix):].strip()
            
            logger.info(f"LLM generated {len(generated_text)} chars")
            
            return generated_text, lprobs
        
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return f"[补充内容生成失败: {str(e)}]", None
    
    def _quality_check(
        self,
        generated_text: str,
        merged_segment
    ) -> str:
        """
        质量检查和修正
        
        检查:
        1. 长度是否合理
        2. 是否包含异常字符
        3. 是否过于重复
        """
        # 长度检查
        if len(generated_text) < self.min_length:
            logger.warning(f"Generated text too short: {len(generated_text)} chars")
            # 可以考虑重新生成,这里暂时保留
        
        if len(generated_text) > self.max_length:
            logger.warning(f"Generated text too long: {len(generated_text)} chars, truncating")
            generated_text = generated_text[:self.max_length] + "..."
        
        # 移除异常字符 (控制字符等)
        generated_text = ''.join(char for char in generated_text if char.isprintable() or char.isspace())
        
        return generated_text.strip()
    
    def _decide_fusion_position(
        self,
        fault_candidate,
        merged_segment
    ) -> Tuple[str, Optional[str]]:
        """
        决定补充内容的融合位置
        
        策略:
        - 第1类(抽象逻辑): 通常在断层文本之后补充
        - 第2类(视觉信息): 如果生成文字,通常在相关描述后补充
        - 第3类(过程步骤): 在步骤描述处补充
        
        Returns:
            (position, anchor_text)
            - position: "before" | "after" | "replace"
            - anchor_text: 定位锚点
        """
        # 简化策略: 默认都是after (在fault_text后面补充)
        # 后续可以根据fault_class精细化
        return "after", fault_candidate.fault_text
    
    def _fuse_with_segment(
        self,
        generated_text: str,
        original_segment_text: str,
        position: str,
        anchor_text: Optional[str] = None
    ) -> str:
        """
        将补充内容融合到原segment
        
        Args:
            generated_text: 生成的补充文本
            original_segment_text: 原始segment文本
            position: 融合位置
            anchor_text: 定位锚点
        
        Returns:
            融合后的完整文本
        """
        if position == "before":
            fused = f"{generated_text} {original_segment_text}"
        elif position == "replace":
            fused = generated_text
        elif position == "after" and anchor_text and anchor_text in original_segment_text:
            # 💥 高级特性: 如果有锚点, 在锚点后插入, 否则在末尾
            parts = original_segment_text.split(anchor_text, 1)
            fused = f"{parts[0]}{anchor_text} {generated_text}{parts[1]}"
        else:  # "after" (默认)
            fused = f"{original_segment_text} {generated_text}"
        
        return fused
