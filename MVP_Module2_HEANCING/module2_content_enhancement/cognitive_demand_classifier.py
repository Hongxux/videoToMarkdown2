import logging
from typing import Dict, List, Set, Any

logger = logging.getLogger(__name__)

class CognitiveDemandClassifier:
    """
    认知需求分类器 (Phase 4.2)
    
    核心逻辑：基于断层文本的语义特征，判定知识的认知需求类型。
    分类结果：
    - text: 抽象逻辑类 (概念、定义、原因等)
    - screenshot: 静态空间类 (架构、结构、对比等)
    - video: 动态过程类 (操作、演示、遍历等)
    - mixed: 混合类
    """
    
    
    def __init__(self, llm_client=None, semantic_extractor=None):
        self.llm_client = llm_client
        self.semantic_extractor = semantic_extractor
        
        # 泛化语义标签 (用于零样本分类)
        self.STATIC_TAGS = [
            "数学公式静态查看", "公式空间结构分析", "符号位置识别", "公式组成理解",
            "静态结构", "组成部分", "空间布局"
        ]
        self.DYNAMIC_TAGS = [
            "数学公式动态推导", "公式步骤计算", "符号变形操作", "推导过程演示",
            "动态演变", "步骤推演", "变形过程"
        ]
        self.tag_embeddings_computed = False
        self._static_embeddings = None
        self._dynamic_embeddings = None
        
        
        # 0. Load Dictionaries
        from .config_loader import get_config_loader
        self.dicts = get_config_loader().load_dictionaries().get("cognitive_demand", {})
        
        # 1. 抽象逻辑类 (Text) - 侧重理论、背景、因果、定义
        self.logic_keywords = set(self.dicts.get("keywords", {}).get("logic", []))
        
        # 匹配模式 (用于区分 "什么是..." vs "如何操作...")
        self.logic_patterns = self.dicts.get("patterns", {}).get("logic", [])
        
        # 2. 空间结构类 (Screenshot) - 侧重组成、分布、层级
        self.spatial_keywords = set(self.dicts.get("keywords", {}).get("spatial", []))
        
        # 匹配模式
        self.spatial_patterns = self.dicts.get("patterns", {}).get("spatial", [])
        
        # 3. 动态过程类 (Video) - 侧重演示、步骤、变化
        self.process_keywords = set(self.dicts.get("keywords", {}).get("process", []))
        
        # 匹配模式
        self.process_patterns = self.dicts.get("patterns", {}).get("process", [])
    

    def classify_cognitive_level(self, fault_text: str) -> tuple[str, str, str]:
        """
        基于认知目标分层判定主辅需求 (V3 Optimization)
        
        Layers:
        - Memory (记忆): 定义/概念 -> Abstract -> Text
        - Understanding (理解): 结构/原理 -> Spatial -> Screenshot
        - Application (应用): 操作/遍历 -> Process -> Video
        
        Returns:
            (main_type, auxiliary_type, reason_str)
        """
        fault_text_lower = fault_text.lower()
        
        # 1. 认知目标关键词映射        # 0. Load from config if available
        layer_map = self.dicts.get("layer_mapping", {})
        memory_keywords = set(layer_map.get("memory", []))
        understand_keywords = set(layer_map.get("understand", []))
        apply_keywords = set(layer_map.get("apply", []))
        
        if not memory_keywords: # Fallback
            memory_keywords = {"定义", "概念", "记住", "背诵", "含义", "名词", "术语"}
            understand_keywords = {"结构", "组成", "原理", "为什么", "逻辑", "关系", "本质"}
            apply_keywords = {"操作", "遍历", "怎么做", "演示", "流程", "执行", "推导"}
        
        memory_score = sum(1 for kw in memory_keywords if kw in fault_text_lower)
        understand_score = sum(1 for kw in understand_keywords if kw in fault_text_lower)
        apply_score = sum(1 for kw in apply_keywords if kw in fault_text_lower)
        
        # 2. 确定最高认知目标
        max_score = max(memory_score, understand_score, apply_score)
        
        if max_score == 0:
            return "unknown", "none", "No cognitive keywords matched"
            
        main_type = "text"
        reason = f"Cognitive Layer: Memory (score={memory_score})"
        
        if max_score == apply_score:
            main_type = "video"
            reason = f"Cognitive Layer: Application (score={apply_score})"
        elif max_score == understand_score:
            main_type = "screenshot"
            reason = f"Cognitive Layer: Understanding (score={understand_score})"
            
        # 3. 确定混合类辅需求
        total_score = memory_score + understand_score + apply_score
        aux_type = "none"
        if total_score > max_score:
            # 寻找次高分
            scores = {"video": apply_score, "screenshot": understand_score, "text": memory_score}
            sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            if sorted_scores[1][1] > 0:
                aux_type = sorted_scores[1][0]
                reason += f", Aux: {aux_type} (score={sorted_scores[1][1]})"
                
        return main_type, aux_type, reason

    def detect_srl_structure(self, fault_text: str) -> tuple[str, str]:
        """
        模拟 SRL (语义角色标注) 分析 (V3 Optimization)
        无关键词时的兜底逻辑: 分析谓词-论元结构
        """
        # 简化版规则库 (Predicate -> Type)
        srl_map = self.dicts.get("srl_predicates", {})
        action_predicates = srl_map.get("action", ["遍历", "操作", "点击", "绘制", "执行", "运行", "生成"])
        spatial_predicates = srl_map.get("spatial", ["是", "包含", "组成", "属于", "位于", "连接"])
        logic_predicates = srl_map.get("logic", ["推导", "证明", "因为", "所以", "导致"])
        
        for pred in action_predicates:
            if pred in fault_text: return "video", f"SRL Heuristic: Action Predicate '{pred}' Found"
            
        for pred in spatial_predicates:
            # 需要结合论元 (简单启发式: 后面跟着结构名词)
            if pred in fault_text and any(n in fault_text for n in ["结构", "图", "表", "树", "节点"]):
                match_arg = next(n for n in ["结构", "图", "表", "树", "节点"] if n in fault_text)
                return "screenshot", f"SRL Heuristic: Spatial Predicate '{pred}' + Argument '{match_arg}' Found"
                
        for pred in logic_predicates:
            return "text", f"SRL Heuristic: Logic Predicate '{pred}' Found"
            
        return "unknown", "No SRL structure matched"

    def classify(self, fault_text: str) -> str:
        """
        根据断层文本返回认知需求类型 (V3 Integrated Pipeline)
        Pipeline: SRL -> Cognitive Layer -> Keyword Dict -> LLM
        Note: Return just type string to maintain backward compatibility, unless modified upstream.
        Actually, let's change return to tuple if possible, but safer to check usage.
        
        UPDATE: To support 'reasoning trace', we add a new method classify_with_reason 
        and keep classify as wrapper.
        """
        t, _ = self.classify_with_reason(fault_text)
        return t

    def judge_math_demand_type(self, text: str) -> tuple[str, str]:
        """
        [V3.5] 利用预训练模型进行零样本数学需求分类 (无需自训练)
        """
        if not self.semantic_extractor:
            return "unknown", "No model"
            
        try:
            model = self.semantic_extractor.model # Lazy load
            if not model: return "unknown", "Model load failed"
            
            # 首次运行计算标签嵌入
            import torch
            from sentence_transformers import util
            
            if not self.tag_embeddings_computed:
                self._static_embeddings = model.encode(self.STATIC_TAGS, convert_to_tensor=True)
                self._dynamic_embeddings = model.encode(self.DYNAMIC_TAGS, convert_to_tensor=True)
                self.tag_embeddings_computed = True
                
            # 计算相似度
            text_embedding = model.encode(text, convert_to_tensor=True)
            
            static_sim = util.cos_sim(text_embedding, self._static_embeddings).mean().item()
            dynamic_sim = util.cos_sim(text_embedding, self._dynamic_embeddings).mean().item()
            
            # 阈值判定 (V2 Doc建议阈值 0.15~0.2)
            THRESHOLD = 0.05 # 微调为更敏感，因为 tags 和 text 差异可能较大
            
            reason = f"Zero-shot Semantic: Static={static_sim:.3f}, Dynamic={dynamic_sim:.3f}"
            
            if static_sim > dynamic_sim + THRESHOLD:
                return "screenshot", reason
            elif dynamic_sim > static_sim + THRESHOLD:
                return "video", reason
            
            return "unknown", reason
            
        except Exception as e:
            logger.warning(f"Zero-shot classification failed: {e}")
            return "unknown", f"Error: {e}"

    # =========================================================================
    # 🚀 V5 Optimization: Context & Domain Enhanced Classification
    # =========================================================================

    async def complete_fault_text_with_llm(self, fault_text: str, context_text: str) -> str:
        """
        [Legacy Wrapper] V5: LLM Semantic Completion (Single Item)
        """
        results = await self.batch_complete_fault_text_with_llm([(fault_text, context_text)])
        return results[0] if results else "unknown"

    async def batch_complete_fault_text_with_llm(self, items: List[Tuple[str, str]]) -> List[str]:
        """
        [Optimized] V5: Batch LLM Semantic Completion
        """
        if not self.llm_client or not items:
             return ["unknown"] * len(items)

        import math
        import json
        
        # 1. Dynamic Batching
        avg_len = sum(len(f) + len(c) for f, c in items) / len(items)
        BATCH_SIZE = 10 if avg_len < 100 else (5 if avg_len < 500 else 2)
        
        batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
        final_results = []
        
        for batch in batches:
            # Build Batch Prompt
            batch_content = ""
            for i, (fault, ctx) in enumerate(batch):
                batch_content += f"Item {i}:\nContext: {ctx}\nFragment: {fault}\n---\n"
                
            prompt = f"""
            Batch Task: Infer implied cognitive demand for multiple items.
            Options: Process, Spatial, Abstract.
            
            Items:
            {batch_content}
            
            Output strictly JSON array of strings, e.g. ["video", "screenshot", ...].
            """
            
            try:
                # Use batch call logic (simplified here using simple ask for demonstration, should be pool-based in prod)
                response = await self.llm_client.chat.completions.create(
                    model="llama3-8b-8192", 
                    messages=[{"role": "user", "content": prompt}],
                    response_format={"type": "json_object"},
                    max_tokens=200,
                    temperature=0.1
                )
                content = response.choices[0].message.content
                # Parse
                try:
                    # Try parsing json array directly
                    batch_res = json.loads(content)
                    if isinstance(batch_res, dict) and "items" in batch_res: batch_res = batch_res["items"]
                    if not isinstance(batch_res, list): raise ValueError("Not a list")
                except:
                    # Fallback parsing
                    batch_res = []
                    lower_content = content.lower()
                    # heuristic mapping if json fails
                    current_idx = 0
                    while len(batch_res) < len(batch):
                         # Just fill unknown on failure to be safe
                         batch_res.append("unknown")
                
                # Normalize results
                normalized = []
                for res in batch_res:
                    r = str(res).lower()
                    if "process" in r or "video" in r: normalized.append("video")
                    elif "spatial" in r or "screenshot" in r: normalized.append("screenshot")
                    elif "abstract" in r or "text" in r: normalized.append("text")
                    else: normalized.append("unknown")
                
                # Padding if LLM returned fewer items
                while len(normalized) < len(batch):
                    normalized.append("unknown")
                    
                final_results.extend(normalized)
                
            except Exception as e:
                logger.error(f"Batch completion failed: {e}")
                final_results.extend(["unknown"] * len(batch))
                
        return final_results

    def enhance_domain_classification(self, text: str, domain: str = "computer_science") -> str:
        """
        V5: Domain Vocabulary Enhancement
        Prioritize domain-specific terminology for verification.
        """
        # Simplified Domain Vocab (Expand in config/ or separate file)
        VOCAB = {
            "computer_science": {
                "process": ["flow", "call", "transmit", "execute", "run", "pipeline", "traverse", "sort", "find"],
                "spatial": ["topology", "architecture", "hierarchy", "diagram", "structure", "tree", "graph", "matrix", "array"]
            },
            "math": {
                "process": ["derive", "transform", "calculate", "solve", "proof step"],
                "spatial": ["geometry", "matrix", "coordinate", "plot", "figure", "graph"]
            }
        }
        
        if domain not in VOCAB: return "unknown"
        
        text_lower = text.lower()
        
        # Check Process terms
        if any(term in text_lower for term in VOCAB[domain]["process"]):
            return "video" # Process -> Video
            
        # Check Spatial terms
        if any(term in text_lower for term in VOCAB[domain]["spatial"]):
            return "screenshot" # Spatial -> Screenshot
            
        return "unknown"

    def check_short_text_rules(self, text: str) -> str:
        """
        V5: Hard rules for short text (< 8 chars) to prevent random classification.
        """
        if len(text) > 8: return "unknown" 
        
        text = text.strip()
        
        spatial_triggers = ["如图", "见图", "看这里", "如下图", "这张图", "Figure", "Fig", "结构", "分布"]
        process_triggers = ["步骤", "操作", "流程", "Step", "Flow", "执行", "运行"]
        abstract_triggers = ["概念", "原理", "逻辑", "Concept", "Why", "定义", "什么是"]
        
        if any(t in text for t in spatial_triggers): return "screenshot"
        if any(t in text for t in process_triggers): return "video"
        if any(t in text for t in abstract_triggers): return "text" 
        
        return "unknown"

    async def classify_with_reason_async(self, fault_text: str, visual_features: Any = None, context_text: str = "") -> tuple[str, str]:
        """
        Async version of classification for V5 LLM support.
        """
        # 0. Reuse Sync Logic for specific checks
        sync_res, sync_reason = self.classify_with_reason(fault_text, visual_features, context_text)
        
        # If Sync logic is very confident (Visual or Short Rule), return immediately
        if "Visual-Fact" in sync_reason or "Short Text Rule" in sync_reason:
            return sync_res, sync_reason
            
        # 🚀 V5: LLM Semantic Completion for ambiguous cases (Mixed or Unknown or Abstract but maybe missing context)
        # Only trigger if context is present and result is not definitive
        if context_text and (sync_res in ["mixed", "unknown", "text"]):
             llm_suggestion = await self.complete_fault_text_with_llm(fault_text, context_text)
             if llm_suggestion != "unknown":
                 return llm_suggestion, f"V5 LLM Context Inference: {llm_suggestion.upper()} (Context: {context_text[:20]}...)"
                 
        return sync_res, sync_reason

    def classify_with_reason(self, fault_text: str, visual_features: Any = None, context_text: str = "") -> tuple[str, str]:
        """
        Return classification with reason for traceability.
        🚀 V5 Optimization: Context-Aware + Domain-Enhanced.
        """
        if not fault_text: return "text", "Empty text"
        
        # 0. 🚀 V5: Short Text Hard Rules (Highest Priority for stability)
        short_rule = self.check_short_text_rules(fault_text)
        if short_rule != "unknown":
             return short_rule, f"V5 Short Text Rule: {short_rule.upper()} (Keyword Trigger)"
        
        # 1. 🚀 Level 2 Correction: 视觉事实修正 (Vision-Aware Demand Correction)
        if visual_features:
            if getattr(visual_features, 'has_static_visual_structure', False):
                return "screenshot", "Vision-Fact: Static visual structure (Arch/Diagram/Table) detected"
            if getattr(visual_features, 'is_dynamic', False) and getattr(visual_features, 'has_math_formula', False):
                 # Math Dynamic -> Video (handled in First Principle Override usually, but here as backup)
                 pass

        # 2. 🚀 V5: Context Enhancement (Validation)
        # If context is provided, append it for semantic checking logic (Zero-Shot & Keyword)
        # For now, we concat to text for analysis, or use it to resolve ambiguity.
        analysis_text = f"{context_text} {fault_text}" if context_text else fault_text
        
        # 3. 🚀 V5: Domain Vocabulary Check (Computer Science Default)
        domain_res = self.enhance_domain_classification(analysis_text, "computer_science")
        if domain_res != "unknown":
             return domain_res, f"V5 Domain Vocab Match: {domain_res.upper()}"

        # 4. [V3.5] Zero-Shot Semantic Classification (Updated to use analysis_text)
        if self.semantic_extractor:
            # Use the richer analysis text
            zero_shot_type, zero_shot_reason = self.judge_math_demand_type(analysis_text)
            if zero_shot_type != "unknown":
                return zero_shot_type, f"[Zero-Shot] {zero_shot_reason}"
        
        # 5. Cognitive Layer Priority (Legacy V3)
        cog_main, cog_aux, cog_reason = self.classify_cognitive_level(analysis_text)
        if cog_main != "unknown":
            if cog_aux != "none":
                return "mixed", cog_reason 
            return cog_main, cog_reason
            
        # 6. SRL Structure Analysis
        srl_type, srl_reason = self.detect_srl_structure(analysis_text)
        if srl_type != "unknown":
            return srl_type, srl_reason
 
        # 7. Legacy Fallback
        legacy_type = self._classify_v2_legacy(analysis_text)
        return legacy_type, f"Legacy Keyword Dict (Fallback): {legacy_type}"

    def _classify_v2_legacy(self, fault_text: str) -> str:
        """原有的基于计数分类逻辑"""
        fault_text_lower = fault_text.lower()
        
        # 1. 关键词匹配计数
        logic_count = sum(1 for kw in self.logic_keywords if kw in fault_text_lower)
        spatial_count = sum(1 for kw in self.spatial_keywords if kw in fault_text_lower)
        process_count = sum(1 for kw in self.process_keywords if kw in fault_text_lower)
        
        # 2. 句式匹配计数
        logic_pattern_count = sum(1 for p in self.logic_patterns if p in fault_text_lower)
        spatial_pattern_count = sum(1 for p in self.spatial_patterns if p in fault_text_lower)
        process_pattern_count = sum(1 for p in self.process_patterns if p in fault_text_lower)
        
        # 3. 综合判定
        total_logic = logic_count + logic_pattern_count
        total_spatial = spatial_count + spatial_pattern_count
        total_process = process_count + process_pattern_count
        
        # 精准分类判定逻辑
        if total_process >= 2:
            if total_spatial >= 1:
                return "mixed"
            return "video"
        elif total_spatial >= 2:
            return "screenshot"
        elif total_logic >= 2:
            return "text"
        elif total_spatial >= 1 and total_process >= 1:
            return "mixed"
            
        # 兜底判定
        if total_process > total_spatial and total_process > total_logic:
            return "video"
        if total_spatial > total_process and total_spatial > total_logic:
            return "screenshot"
            
        # 最终兜底：默认文字类（最安全）
        return "text"

    async def classify_with_llm(self, fault_text: str) -> str:
        """规则无法判定时，可选调用 LLM 进行深度判定"""
        if not self.llm_client:
            return self.classify(fault_text)
            
        prompt = f"请分析以下断层文本的认知需求类型。如果是关于概念、原理的选 text；如果是关于结构、组成的选 screenshot；如果是关于操作、过程的选 video。仅返回 text/screenshot/video 之一：\n{fault_text}"
        try:
            result = await self.llm_client.ask_simple(prompt)
            result = result.strip().lower()
            if result in ["text", "screenshot", "video"]:
                return result
        except Exception as e:
            logger.warning(f"LLM Classification failed: {e}")
            
        return self.classify(fault_text)
