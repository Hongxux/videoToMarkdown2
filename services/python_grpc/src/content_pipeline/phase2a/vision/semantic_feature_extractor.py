"""
模块说明：Module2 内容增强中的 semantic_feature_extractor 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
import asyncio
import numpy as np

logger = logging.getLogger(__name__)
from collections import OrderedDict
from services.python_grpc.src.content_pipeline.infra.runtime import cache_metrics

# 💥 性能优化: 全局共享嵌入缓存 (跨组件复用)
_GLOBAL_EMBEDDING_CACHE = OrderedDict()
_MAX_GLOBAL_CACHE_SIZE = 2000


@dataclass
class SemanticFeatures:
    """类说明：SemanticFeatures 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
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
    """类说明：SemanticFeatureExtractor 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(
        self,
        config: Dict = None,
        domain_keywords: Dict[str, List[str]] = None
    ):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、Torch 推理/张量计算、ONNX 推理、YAML 解析实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：config is None
        - 条件：not self.sequence_indicators
        - 条件：not self.hierarchy_indicators
        依据来源（证据链）：
        - 输入参数：config。
        - 对象内部状态：self.hierarchy_indicators, self.sequence_indicators。
        输入参数：
        - config: 配置对象/字典（类型：Dict）。
        - domain_keywords: 函数入参（类型：Dict[str, List[str]]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        # 加载配置
        if config is None:
            from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
            config = load_module2_config()
        
        self.config = config
        
        # 0. Load Dictionaries
        from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import get_config_loader
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
        """
        执行逻辑：
        1) 读取对象内部状态。
        2) 返回属性值。
        实现方式：通过内部方法调用/状态更新、Torch 推理/张量计算、ONNX 推理实现。
        核心价值：对外提供统一读路径，便于维护与扩展。
        决策逻辑：
        - 条件：self._model is not None
        - 条件：self.use_onnx
        - 条件：torch.get_num_threads() > 1
        依据来源（证据链）：
        - 对象内部状态：self._model, self.use_onnx。
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not text.strip()
        - 条件：max_sim < 0.4
        - 条件：sim > max_sim
        依据来源（证据链）：
        - 输入参数：text。
        输入参数：
        - text: 文本内容（类型：str）。
        输出参数：
        - 字符串结果。"""
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
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算、asyncio 异步调度实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：text in _GLOBAL_EMBEDDING_CACHE
        依据来源（证据链）：
        - 输入参数：text。
        - 阈值常量：_GLOBAL_EMBEDDING_CACHE。
        输入参数：
        - text: 文本内容（类型：str）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算、asyncio 异步调度实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not texts
        - 条件：to_encode_texts
        - 条件：text in _GLOBAL_EMBEDDING_CACHE
        依据来源（证据链）：
        - 输入参数：texts。
        - 阈值常量：_GLOBAL_EMBEDDING_CACHE。
        输入参数：
        - texts: 函数入参（类型：List[str]）。
        输出参数：
        - np.ndarray 列表（与输入或处理结果一一对应）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(_GLOBAL_EMBEDDING_CACHE) >= _MAX_GLOBAL_CACHE_SIZE
        依据来源（证据链）：
        - 阈值常量：_GLOBAL_EMBEDDING_CACHE, _MAX_GLOBAL_CACHE_SIZE。
        输入参数：
        - text: 文本内容（类型：未标注）。
        - emb: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if len(_GLOBAL_EMBEDDING_CACHE) >= _MAX_GLOBAL_CACHE_SIZE:
             _GLOBAL_EMBEDDING_CACHE.popitem(last=False)
        _GLOBAL_EMBEDDING_CACHE[text] = emb

    async def calculate_context_similarity(
        self,
        text: str,
        context: str
    ) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算、asyncio 异步调度实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not text.strip() or not context.strip()
        - 条件：strings_to_encode
        - 条件：(self._cache_hits + self._cache_misses) % 10 == 0
        依据来源（证据链）：
        - 输入参数：context, text。
        - 阈值常量：_GLOBAL_EMBEDDING_CACHE, _MAX_GLOBAL_CACHE_SIZE。
        - 对象内部状态：self._cache_hits, self._cache_misses, self._embedding_cache。
        输入参数：
        - text: 文本内容（类型：str）。
        - context: 函数入参（类型：str）。
        输出参数：
        - 数值型计算结果。"""
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
                    cache_metrics.hit("module2.semantic.embedding_cache")
                else:
                    strings_to_encode.append(s)
                    self._cache_misses += 1
                    cache_metrics.miss("module2.semantic.embedding_cache")
            
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：domain not in self.domain_keywords
        - 条件：not keywords
        依据来源（证据链）：
        - 输入参数：domain。
        - 对象内部状态：self.domain_keywords。
        输入参数：
        - text: 文本内容（类型：str）。
        - domain: 函数入参（类型：str）。
        输出参数：
        - float, List[str] 列表（与输入或处理结果一一对应）。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - text: 文本内容（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：llm_suggestion == 'screenshot'
        - 条件：has_process and knowledge_type == 'spatial'
        - 条件：llm_suggestion == 'video'
        依据来源（证据链）：
        - 输入参数：llm_suggestion。
        输入参数：
        - text: 文本内容（类型：str）。
        - class1_indicators: 函数入参（类型：Dict[str, List[str]]）。
        - class2_indicators: 函数入参（类型：Dict[str, List[str]]）。
        - llm_suggestion: 函数入参（类型：str）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
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
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        输入参数：
        - text: 文本内容（类型：str）。
        - context_before: 函数入参（类型：str）。
        - context_after: 函数入参（类型：str）。
        - domain: 函数入参（类型：str）。
        - class1_indicators: 函数入参（类型：Dict）。
        - class2_indicators: 函数入参（类型：Dict）。
        - llm_suggestion: 函数入参（类型：str）。
        输出参数：
        - SemanticFeatures 对象（包含字段：knowledge_type, context_similarity, domain_consistency, matched_keywords, has_sequence_pattern, has_hierarchy_pattern, has_process_words, has_spatial_words, confidence）。"""
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
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：has_keywords
        依据来源（证据链）：
        - 输入参数：has_keywords。
        输入参数：
        - S_ctx: 函数入参（类型：float）。
        - K_domain: 函数入参（类型：float）。
        - has_keywords: 函数入参（类型：bool）。
        输出参数：
        - 数值型计算结果。"""
        # 按照用户文档的内部语义置信度公式简化版
        # C_text = 0.5 × S_ctx + 0.2 × K_domain (省略困惑度)
        confidence = 0.5 * S_ctx + 0.2 * K_domain + 0.3 * (1.0 if has_keywords else 0.5)
        
        return min(1.0, confidence)
