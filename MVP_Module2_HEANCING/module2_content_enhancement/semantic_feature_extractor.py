"""
Semantic Feature Extractor - Week 2 Day 9

Implements NLP-based semantic feature analysis:
- Context similarity (Sentence-BERT)
- Domain keyword matching
- Semantic role labeling

Part of the three-layer multimodal fusion system.
"""

import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
import asyncio
import numpy as np

logger = logging.getLogger(__name__)
from collections import OrderedDict

# 💥 性能优化: 全局共享嵌入缓存 (跨组件复用)
_GLOBAL_EMBEDDING_CACHE = OrderedDict()
_MAX_GLOBAL_CACHE_SIZE = 2000


@dataclass
class SemanticFeatures:
    """
    语义特征分析结果
    
    用于多模态融合决策的语义维度
    """
    knowledge_type: str  # "process" | "spatial" | "abstract"
    
    # 上下文相似度 (S_ctx)
    context_similarity: float  # 0-1
    
    # 领域知识一致性 (K_domain)
    domain_consistency: float  # 0-1
    matched_keywords: List[str]  # 匹配到的关键词
    
    # 语义特征
    has_sequence_pattern: bool  # A→B→C
    has_hierarchy_pattern: bool  # A包含B、C
    has_process_words: bool  # 包含过程类词汇
    has_spatial_words: bool  # 包含空间类词汇
    
    # 置信度
    confidence: float  # 0-1


class SemanticFeatureExtractor:
    """
    语义特征提取器
    
    基于Sentence-BERT和关键词匹配实现
    """
    
    def __init__(
        self,
        config: Dict = None,
        domain_keywords: Dict[str, List[str]] = None
    ):
        """
        Args:
            config: Module2配置字典 (如果为None,自动加载)
            domain_keywords: 领域关键词字典
        """
        # 加载配置
        if config is None:
            from .config_loader import load_module2_config
            config = load_module2_config()
        
        self.config = config
        
        # 0. Load Dictionaries
        from .config_loader import get_config_loader
        self.dicts = get_config_loader().load_dictionaries().get("semantic_feature", {})
        
        logger.info(f"Submodule dictionaries loaded")
        
        semantic_config = config.get("semantic_feature_config", {})
        
        # 从配置加载模型名称
        self.model_name = semantic_config.get(
            "model_name",
            "paraphrase-multilingual-MiniLM-L12-v2"
        )
        
        # 🚀 Phase 5.0 Performance: ONNX Runtime Check
        self.use_onnx = semantic_config.get("use_onnx", True)
        self._onnx_model = None
        
        # 从配置加载模式检测关键词 (优先用 dictionaries.yaml)
        # 兼容旧逻辑 pattern_config, 但若 dictionaries.yaml 有值则覆盖
        self.sequence_indicators = self.dicts.get("indicators", {}).get("sequence", [])
        if not self.sequence_indicators:
            pattern_config = semantic_config.get("pattern_detection", {})
            self.sequence_indicators = pattern_config.get("sequence_indicators", [])
            
        self.hierarchy_indicators = self.dicts.get("indicators", {}).get("hierarchy", [])
        if not self.hierarchy_indicators:
            pattern_config = semantic_config.get("pattern_detection", {})
            self.hierarchy_indicators = pattern_config.get("hierarchy_indicators", [])
        
        self.domain_keywords = domain_keywords or {}
        
        # 💥 性能优化: 嵌入缓存，避免对相同文本重复推理 (尤其是上下文)
        self._embedding_cache = _GLOBAL_EMBEDDING_CACHE
        self._cache_hits = 0
        self._cache_misses = 0
        
        # 💥 性能优化: 强制限制 PyTorch 线程数 (防止与视觉进程争抢 CPU)
        try:
            import torch
            torch.set_num_threads(1)
            # logger.info("SemanticExtractor: Enforced torch.set_num_threads(1)")
        except:
            pass
        
        # 延迟加载模型 (避免启动时就加载)
        self._model = None
        self._src_model = None # BERT-tiny for SRC
        
        # 语义角色原型定义 (用于零样本分类)
        self.role_prototypes = {
            "G": ["现在我们来看", "接下来讲解", "下面开始演示", "我们试一下这个", "注意看这里"],
            "R": ["这样就完成了", "搞定了", "大家可以看到结果", "这就是最终效果", "确认没问题", "对吧"]
        }
        
        logger.info(f"SemanticFeatureExtractor initialized with SRC Prototypes")
        logger.info(f"Sequence indicators: {len(self.sequence_indicators)}")
        logger.info(f"Hierarchy indicators: {len(self.hierarchy_indicators)}")
    
    @property
    def model(self):
        """懒加载Sentence-BERT模型 (🚀 Phase 5.1: Fixed UnboundLocalError & Redundancy)"""
        if self._model is not None:
            return self._model
            
        try:
            from sentence_transformers import SentenceTransformer, util
            self._util = util
            
            # 🚀 Phase 5.0 Performance: Auto-switch to ONNX if available
            if self.use_onnx:
                try:
                    logger.info(f"Attempting to load {self.model_name} with ONNX backend...")
                    self._model = SentenceTransformer(self.model_name, backend="onnx")
                    logger.info("ONNX Runtime backend enabled for SentenceTransformer")
                except Exception as e:
                    logger.warning(f"Failed to load ONNX backend, falling back to Pytorch: {e}")
                    self._model = SentenceTransformer(self.model_name)
            else:
                self._model = SentenceTransformer(self.model_name)
            
            logger.info(f"Loaded Sentence-BERT model: {self.model_name}")
            
            # 💥 性能优化: 限制 PyTorch 内部线程
            import torch
            if torch.get_num_threads() > 1:
                torch.set_num_threads(1)
                logger.info("Set torch threads to 1 for parallel efficiency")
                
        except Exception as e:
            logger.error(f"SentenceTransformer load failed: {e}")
            raise 
            
        return self._model
    
    async def classify_semantic_role(self, text: str) -> str:
        """
        使用 BERT-tiny 判定句子的语义角色 (G/C/R/N)
        基于原型向量的余弦相似度进行零样本判定
        """
        if not text.strip(): return "N"
        
        try:
            # 1. 获取目标文本的 Embedding
            target_emb = await self.get_embedding(text)
            
            # 2. 与 G/R 原型进行比对
            max_sim = 0.0
            best_role = "C" # 默认核心内容
            
            for role, prototypes in self.role_prototypes.items():
                for proto in prototypes:
                    proto_emb = await self.get_embedding(proto)
                    
                    import numpy as np
                    sim = np.dot(target_emb, proto_emb) / (
                        np.linalg.norm(target_emb) * np.linalg.norm(proto_emb) + 1e-6
                    )
                    
                    if sim > max_sim:
                        max_sim = sim
                        if sim > 0.65: # 阈值阈值
                            best_role = role
            
            # 3. 结果修正：如果相似度都很低，归为 C (或 N 如果文本极短)
            if max_sim < 0.4:
                return "C" if len(text) > 5 else "N"
                
            return best_role
            
        except Exception as e:
            logger.error(f"SRC Classification failed: {e}")
            return "C"

    async def get_embedding(self, text: str):
        """获取文本 Embedding (含缓存支持)"""
        # 尝试从全局缓存获取
        if text in _GLOBAL_EMBEDDING_CACHE:
            return _GLOBAL_EMBEDDING_CACHE[text]
            
        model = self.model
        loop = asyncio.get_running_loop()
        new_emb = await loop.run_in_executor(
            None, 
            lambda: model.encode([text], convert_to_numpy=True, show_progress_bar=False)[0]
        )
        
        # 更新缓存
        self._update_cache(text, new_emb)
        return new_emb

    async def batch_get_embeddings(self, texts: List[str]) -> List[np.ndarray]:
        """🚀 Phase 5.0 Performance: 批量获取 Embedding (深度优化点)"""
        if not texts: return []
        
        results = [None] * len(texts)
        to_encode_indices = []
        to_encode_texts = []
        
        # 1. 检查缓存
        for i, text in enumerate(texts):
            if text in _GLOBAL_EMBEDDING_CACHE:
                results[i] = _GLOBAL_EMBEDDING_CACHE[text]
            else:
                to_encode_indices.append(i)
                to_encode_texts.append(text)
        
        # 2. 批量推理
        if to_encode_texts:
            model = self.model
            loop = asyncio.get_running_loop()
            new_embs = await loop.run_in_executor(
                None,
                lambda: model.encode(to_encode_texts, batch_size=32, convert_to_numpy=True, show_progress_bar=False)
            )
            
            for i, emb in zip(to_encode_indices, new_embs):
                results[i] = emb
                self._update_cache(to_encode_texts[to_encode_indices.index(i)], emb)
        
        return results

    def _update_cache(self, text, emb):
        if len(_GLOBAL_EMBEDDING_CACHE) >= _MAX_GLOBAL_CACHE_SIZE:
             _GLOBAL_EMBEDDING_CACHE.popitem(last=False)
        _GLOBAL_EMBEDDING_CACHE[text] = emb

    async def calculate_context_similarity(
        self,
        text: str,
        context: str
    ) -> float:
        """
        计算文本与上下文的语义相似度 (异步入口, 同步推理保证稳定性)
        """
        if not text.strip() or not context.strip():
            return 0.0
        
        try:
            # 💥 性能优化: 嵌入缓存逻辑
            model = self.model
            
            # 准备待编码文本
            strings_to_encode = []
            cached_vectors = {}
            
            for s in [text, context]:
                if s in self._embedding_cache:
                    cached_vectors[s] = self._embedding_cache[s]
                    self._cache_hits += 1
                else:
                    strings_to_encode.append(s)
                    self._cache_misses += 1
            
            # 如果有需要编码的文本
            if strings_to_encode:
                import time
                t0 = time.time()
                
                # 🚀 性能优化: 移至线程池执行模型推理，防止阻塞事件循环
                loop = asyncio.get_running_loop()
                new_embeddings = await loop.run_in_executor(
                    None, 
                    lambda: model.encode(strings_to_encode, convert_to_numpy=True, show_progress_bar=False)
                )
                
                elapsed = (time.time() - t0) * 1000
                logger.info(f"[PERF] BERT Inference (Async): encoded {len(strings_to_encode)} itms in {elapsed:.1f}ms")
                
                # 更新缓存 (LRU)
                for i, s in enumerate(strings_to_encode):
                    if len(_GLOBAL_EMBEDDING_CACHE) >= _MAX_GLOBAL_CACHE_SIZE:
                        _GLOBAL_EMBEDDING_CACHE.popitem(last=False)
                    _GLOBAL_EMBEDDING_CACHE[s] = new_embeddings[i]
                    cached_vectors[s] = new_embeddings[i]

            emb1 = cached_vectors[text]
            emb2 = cached_vectors[context]
            
            import numpy as np
            similarity = np.dot(emb1, emb2) / (
                np.linalg.norm(emb1) * np.linalg.norm(emb2) + 1e-6
            )
            
            # 偶尔打印统计数据
            if (self._cache_hits + self._cache_misses) % 10 == 0:
                logger.info(f"[PERF] Embedding Cache: {self._cache_hits} hits, {self._cache_misses} misses")
                
            return float((similarity + 1) / 2)
            
        except Exception as model_err:
            logger.warning(f"Model similarity failed: {model_err}")
            return 0.5
                
        except Exception as e:
            logger.error(f"Failed to calculate similarity: {e}")
            return 0.5
    
    def calculate_domain_consistency(
        self,
        text: str,
        domain: str
    ) -> tuple[float, List[str]]:
        """
        计算领域知识一致性
        
        基于领域关键词匹配率:
        K_domain = matched_count / total_keywords
        
        Args:
            text: 目标文本
            domain: 领域名称
        
        Returns:
            (一致性分数, 匹配到的关键词列表)
        """
        if domain not in self.domain_keywords:
            logger.warning(f"Unknown domain: {domain}")
            return 0.5, []
        
        keywords = self.domain_keywords[domain]
        if not keywords:
            return 0.5, []
        
        # 统计匹配的关键词
        matched = [kw for kw in keywords if kw in text]
        
        # 计算匹配率
        consistency = len(matched) / len(keywords)
       
        return consistency, matched
    
    def detect_pattern(self, text: str) -> Dict[str, bool]:
        """
        检测文本中的模式
        
        - 序列模式: "A→B→C", "首先...然后...最后"
        - 层级模式: "A包含B", "分为", "组成"
        
        Returns:
            模式检测结果
        """
        # 序列模式关键词 (Already loaded in __init__)
        sequence_indicators = self.sequence_indicators
        
        # 层级模式关键词 (Already loaded in __init__)
        hierarchy_indicators = self.hierarchy_indicators
        
        has_sequence = any(ind in text for ind in sequence_indicators)
        has_hierarchy = any(ind in text for ind in hierarchy_indicators)
        
        return {
            "has_sequence_pattern": has_sequence,
            "has_hierarchy_pattern": has_hierarchy
        }
    
    def classify_knowledge_type(
        self,
        text: str,
        class1_indicators: Dict[str, List[str]],
        class2_indicators: Dict[str, List[str]],
        llm_suggestion: str = None
    ) -> tuple[str, bool, bool]:
        """
        分类知识类型 (混合动力: LLM 建议优先 + 关键词校验)
        """
        # 1. 基础关键词统计
        spatial_keywords = class2_indicators.get("空间结构", [])
        spatial_count = sum(1 for kw in spatial_keywords if kw in text)
        process_keywords = class2_indicators.get("动态过程", [])
        process_count = sum(1 for kw in process_keywords if kw in text)
        
        has_spatial = spatial_count > 0
        has_process = process_count > 0
        
        # 2. 融合决策
        # 如果 LLM 已经明确建议了类型，赋予极高权重
        if llm_suggestion == "screenshot":
            knowledge_type = "spatial"
        elif llm_suggestion == "video":
            knowledge_type = "process"
        elif llm_suggestion == "text":
            knowledge_type = "abstract"
        else:
            # 兜底：关键词匹配
            if process_count > spatial_count:
                knowledge_type = "process"
            elif spatial_count > process_count:
                knowledge_type = "spatial"
            else:
                knowledge_type = "abstract"
        
        # 特殊保护：如果包含强过程动词（如“演示”、“操作”），即使建议是截图也保留过程属性
        if has_process and knowledge_type == "spatial":
            knowledge_type = "process"
            
        return knowledge_type, has_process, has_spatial
    
    async def extract_semantic_features(
        self,
        text: str,
        context_before: str,
        context_after: str,
        domain: str,
        class1_indicators: Dict,
        class2_indicators: Dict,
        llm_suggestion: str = None
    ) -> SemanticFeatures:
        """
        提取完整的语义特征
        
        这是主入口方法
        
        Args:
            text: 目标文本
            context_before: 前文上下文
            context_after: 后文上下文
            domain: 领域
            class1_indicators: 第1类断层关键词
            class2_indicators: 第2类断层关键词
            llm_suggestion: LLM关于增强方式的建议 (Optional)
        
        Returns:
            SemanticFeatures对象
        """
        # 1. 上下文相似度 (现在是 await 调用)
        full_context = f"{context_before} {context_after}"
        S_ctx = await self.calculate_context_similarity(text, full_context)
        
        # 2. 领域一致性
        K_domain, matched_kw = self.calculate_domain_consistency(text, domain)
        
        # 3. 模式检测
        patterns = self.detect_pattern(text)
        
        # 4. 知识类型分类
        knowledge_type, has_process, has_spatial = self.classify_knowledge_type(
            text,
            class1_indicators,
            class2_indicators,
            llm_suggestion=llm_suggestion
        )
        
        # 5. 计算置信度
        confidence = self._calculate_semantic_confidence(
            S_ctx,
            K_domain,
            has_process or has_spatial
        )
        
        return SemanticFeatures(
            knowledge_type=knowledge_type,
            context_similarity=S_ctx,
            domain_consistency=K_domain,
            matched_keywords=matched_kw,
            has_sequence_pattern=patterns["has_sequence_pattern"],
            has_hierarchy_pattern=patterns["has_hierarchy_pattern"],
            has_process_words=has_process,
            has_spatial_words=has_spatial,
            confidence=confidence
        )
    
    def _calculate_semantic_confidence(
        self,
        S_ctx: float,
        K_domain: float,
        has_keywords: bool
    ) -> float:
        """
        计算语义特征的置信度
        
        基于:
        - 上下文相似度
        - 领域一致性
        - 是否匹配关键词
        
        Returns:
            置信度 0-1
        """
        # 按照用户文档的内部语义置信度公式简化版
        # C_text = 0.5 × S_ctx + 0.2 × K_domain (省略困惑度)
        confidence = 0.5 * S_ctx + 0.2 * K_domain + 0.3 * (1.0 if has_keywords else 0.5)
        
        return min(1.0, confidence)
