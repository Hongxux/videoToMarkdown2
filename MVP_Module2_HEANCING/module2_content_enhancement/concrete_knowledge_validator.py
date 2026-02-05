"""
模块说明：具象性知识验证器，负责判断截图是否包含可插入的教学内容。
执行逻辑：
1) 加载 Vision AI 与 pHash 缓存配置，建立可复用的检测基础。
2) 对截图执行公式检测、图形区域提取与视觉验证。
3) 输出具象知识判定结果供上游筛选与组装。
实现方式：OpenCV + NumPy 做图像特征分析，Vision AI 做语义判定，pHash 做重复帧过滤。
核心价值：在不牺牲召回的前提下，减少纯文字截图与重复截图的进入。
输入：
- 截图路径与 OCR 文本（可选）。
- config.yaml 中 vision_ai 配置（可选）。
输出：
- ConcreteKnowledgeResult（是否具象、置信度、类型、理由、是否应保留）。"""

import os
import cv2
import json
import base64
import logging
import httpx
import yaml
import numpy as np
import hashlib
import time
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


# ==============================================================================
# Vision AI Prompt (V2 - 简化版，仅图片输入)
# ==============================================================================

CONCRETE_KNOWLEDGE_PROMPT = """# 角色
你是专业的教育多媒体分析专家，精通教学知识分类，严格按照「具象性知识」的学术定义判定。

# 核心判定规则
判定当前截图是否包含**具象性知识**：

**阳性（存在具象性知识）** - 满足任一条件：
- 截图中存在和教学知识点强相关的：实物照片、标本图、实验装置、解剖图、结构图、地图、实操界面、具体事物示意图，抽象框图、逻辑流程图、思维导图等
- 截图中存在**数学公式**（包括方程式、推导过程、符号表达式等）
- 该图形是用于讲解知识点的功能性元素，非装饰、水印、花边、无关插画
- 图形能明确对应现实中的具体事物、现象、操作步骤、数学关系
- 截图是否存在图表显示数据或者变化
- 截图是否能帮助初学者直观认知某个事物的视觉形式
- 截图是否能作为记忆点，用于学习者后续回顾和复习
**阴性（不存在具象性知识）**：
- 纯文字、无功能性图形
- 仅有装饰图片，无教学用具象图形或数学公式
- 仅仅是讲解者的人物图片

**混合型**：同时包含具象知识和抽象知识，仍标记为「存在具象性知识」

# 输出要求（严格JSON格式）
{{
    "has_concrete_knowledge": "是/否",
    "confidence": 0.0-1.0,
    "concrete_type": "实物图/装置图/解剖图/实操界面/地形图/模型图/等等/无",
    "reason": "简明判定依据"
}}

请只输出JSON，不要有其他内容。"""


# ==============================================================================
# Data Classes
# ==============================================================================

@dataclass
class ConcreteKnowledgeResult:
    """
    类说明：具象性知识判定的结构化结果。
    执行逻辑：
    1) 记录是否具象、是否公式、置信度与判定理由。
    2) 提供是否应保留截图的最终结论。
    实现方式：使用 dataclass 存储字段值。
    核心价值：统一输出格式，便于上游直接消费与统计。
    输入：
    - 构造参数：has_concrete/has_formula/confidence/concrete_type/reason/is_mixed/non_text_ratio/should_include。
    输出：
    - 结果对象字段，用于描述具象知识判定与保留建议。"""
    has_concrete: bool              # 是否包含具象性知识
    has_formula: bool               # 是否包含数学公式
    confidence: float               # 置信度 (0-1)
    concrete_type: str              # 具象类型
    reason: str                     # 判定依据
    is_mixed: bool                  # 是否混合型
    non_text_ratio: float           # 非文本区域占比
    should_include: bool            # 是否应该插入截图


# ==============================================================================
# Main Class
# ==============================================================================

class ConcreteKnowledgeValidator:
    """
    类说明：具象性知识验证器，组合 OCR/CV/Vision AI 进行截图筛选。
    执行逻辑：
    1) 初始化 Vision AI 客户端与 pHash 缓存（可选）。
    2) 对截图执行公式检测、图形区域提取与视觉判定。
    3) 输出 ConcreteKnowledgeResult 并写入缓存。
    实现方式：YAML 读取 vision_ai 配置，OpenCV/NumPy 提取图形区域，Vision AI 语义判定。
    核心价值：减少纯文字与重复截图进入富文本，提高插图质量。
    输入：
    - image_path/ocr_text（验证输入）。
    - config_path/output_dir（初始化配置与缓存路径）。
    输出：
    - ConcreteKnowledgeResult 或其列表（批量模式）。"""
    
    def __init__(self, config_path: Optional[str] = None, output_dir: Optional[str] = None):
        """
        执行逻辑：
        1) 读取 vision_ai 配置并按需初始化 VisionAIClient。
        2) 基于 similarity_threshold 初始化 pHash 缓存与签名。
        3) 如提供 output_dir，则建立持久化缓存并加载历史结果。
        4) 初始化 ThreadSafeMathOCR 作为公式检测首选方案。
        实现方式：VisionAIClient/HashCacheManager/PerceptualHasher + 本地 JSON 缓存。
        核心价值：减少重复调用与误判，降低外部 API 成本。
        决策逻辑：
        - 条件：vision_config 存在时才初始化 VisionAIClient。
        - 条件：output_dir 提供时才开启持久化缓存。
        - 条件：ThreadSafeMathOCR 加载失败则降级到文本规则检测。
        依据来源（证据链）：
        - 配置字段：vision_ai.enabled、vision_ai.bearer_token、vision_ai.similarity_threshold。
        - 输入参数：output_dir。
        - 运行状态：OCR 初始化异常。
        输入参数：
        - config_path: 配置文件路径（Optional[str]）。
        - output_dir: 中间产物目录（Optional[str]）。
        输出参数：
        - 无（仅更新内部状态与缓存）。"""
        # V3: 使用模块化 VisionAIClient (带感知哈希缓存)
        from .vision_ai_client import VisionAIClient, VisionAIConfig, PerceptualHasher, HashCacheManager
        
        self._vision_enabled = False
        self._vision_client: Optional[VisionAIClient] = None
        self._cache_signature = ""
        self._persistent_cache_path: Optional[Path] = None
        
        # 加载配置并初始化 VisionAIClient
        vision_config = self._load_vision_config(config_path)
        if vision_config:
            self._vision_client = VisionAIClient(vision_config)
            self._vision_enabled = vision_config.enabled
            self._cache_signature = self._build_cache_signature(vision_config)
        
        # 独立的感知哈希缓存 (用于非 Vision API 场景)
        similarity_threshold = vision_config.similarity_threshold if vision_config else 0.95
        self._hash_cache = HashCacheManager(similarity_threshold=similarity_threshold)
        self._hasher = PerceptualHasher

        # 持久化缓存：按 url_hash + 关键配置 复用 VisionAI 结果
        if output_dir:
            intermediates_dir = Path(output_dir) / "intermediates"
            intermediates_dir.mkdir(parents=True, exist_ok=True)
            suffix = self._cache_signature[:12] if self._cache_signature else "default"
            self._persistent_cache_path = intermediates_dir / f"vision_ai_cache_{suffix}.json"
            self._load_persistent_cache()
        
        # V2: 集成 ThreadSafeMathOCR 进行精准公式检测 (用户建议)
        self._math_ocr = None
        try:
            from .ocr_utils import ThreadSafeMathOCR
            self._math_ocr = ThreadSafeMathOCR()
            logger.info("ThreadSafeMathOCR initialized for formula detection")
        except Exception as e:
            logger.warning(f"ThreadSafeMathOCR not available, using fallback formula detection: {e}")
    
    def _load_vision_config(self, config_path: Optional[str] = None):
        """
        执行逻辑：
        1) 确定 config.yaml 路径并读取配置。
        2) 校验 vision_ai 是否启用与 bearer_token 是否存在。
        3) 组装 VisionAIConfig 返回给初始化流程。
        实现方式：YAML 解析 + 基本字段校验。
        核心价值：集中管理 Vision AI 的开关与参数来源。
        决策逻辑：
        - 条件：config_path is None（回退到项目根目录 config.yaml）
        - 条件：not config_path.exists()（找不到配置则关闭 Vision AI）
        - 条件：not vision_config.get('enabled', False)
        - 条件：not vision_config.get('bearer_token')
        依据来源（证据链）：
        - 输入参数：config_path。
        - 配置字段：vision_ai.enabled、vision_ai.bearer_token、vision_ai.base_url、vision_ai.vision_model。
        输入参数：
        - config_path: 配置文件路径（Optional[str]）。
        输出参数：
        - VisionAIConfig 或 None（表示使用纯 CV 路径）。"""
        from .vision_ai_client import VisionAIConfig
        
        # 查找 config.yaml
        if config_path is None:
            # 默认路径: 项目根目录/config.yaml
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config.yaml"
        else:
            config_path = Path(config_path)
        
        if not config_path.exists():
            logger.warning(f"config.yaml not found at {config_path}, using CV-only mode")
            return None
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            vision_config = config.get("vision_ai", {})
            
            if not vision_config.get("enabled", False):
                logger.info("Vision AI disabled in config, using CV-only mode")
                return None
            
            bearer_token = vision_config.get("bearer_token", "")
            if not bearer_token:
                logger.warning("vision_ai.bearer_token not set in config.yaml, using CV-only mode")
                return None
            
            # 构建 VisionAIConfig
            ai_config = VisionAIConfig(
                enabled=True,
                bearer_token=bearer_token,
                base_url=vision_config.get("base_url", "https://qianfan.baidubce.com/v2/chat/completions"),
                model=vision_config.get("vision_model", "ernie-4.5-turbo-vl-32k"),
                temperature=vision_config.get("temperature", 0.3),
                duplicate_detection_enabled=vision_config.get("duplicate_detection", True),
                similarity_threshold=vision_config.get("similarity_threshold", 0.95)
            )
            
            logger.info(f"ERNIE Vision API enabled: model={ai_config.model}, duplicate_detection={ai_config.duplicate_detection_enabled}")
            return ai_config
            
        except Exception as e:
            logger.error(f"Failed to load vision config: {e}")
            return None

    def _build_cache_signature(self, vision_config) -> str:
        """
        执行逻辑：
        1) 抽取关键配置字段形成稳定字典。
        2) 计算 SHA256 签名用于缓存隔离。
        实现方式：json.dumps + hashlib.sha256。
        核心价值：让缓存与模型/阈值绑定，避免跨配置污染。
        输入参数：
        - vision_config: VisionAIConfig 或同结构对象。
        输出参数：
        - 配置签名字符串（sha256 十六进制）。"""
        try:
            payload = {
                "model": getattr(vision_config, "model", ""),
                "temperature": getattr(vision_config, "temperature", 0.0),
                "duplicate_detection": getattr(vision_config, "duplicate_detection_enabled", True),
                "similarity_threshold": getattr(vision_config, "similarity_threshold", 0.95)
            }
            raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
            return hashlib.sha256(raw.encode("utf-8")).hexdigest()
        except Exception as e:
            logger.warning(f"Failed to build cache signature: {e}")
            return ""

    def _load_persistent_cache(self):
        """
        执行逻辑：
        1) 读取本地持久化缓存文件。
        2) 校验签名一致后加载到 hash_cache。
        实现方式：JSON 解析 + HashCacheManager.load_results。
        核心价值：跨任务复用 Vision AI 结果，减少重复调用。
        决策逻辑：
        - 条件：not self._persistent_cache_path or not self._persistent_cache_path.exists()
        - 条件：self._cache_signature and meta.get('signature') != self._cache_signature
        - 条件：self._hash_cache and items
        依据来源（证据链）：
        - 缓存文件：meta.signature。
        - 内部状态：self._cache_signature、self._hash_cache、self._persistent_cache_path。
        输入参数：
        - 无。
        输出参数：
        - 无（仅更新内部缓存）。"""
        if not self._persistent_cache_path or not self._persistent_cache_path.exists():
            return
        try:
            with open(self._persistent_cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            meta = data.get("meta", {})
            if self._cache_signature and meta.get("signature") != self._cache_signature:
                logger.info("Persistent cache signature mismatch, skip loading")
                return
            items = data.get("items", {})
            if self._hash_cache and items:
                self._hash_cache.load_results(items)
                logger.info(f"Persistent cache loaded: {len(items)} items")
        except Exception as e:
            logger.warning(f"Failed to load persistent cache: {e}")

    def _save_persistent_cache(self):
        """
        执行逻辑：
        1) 从 hash_cache 导出结果。
        2) 写入本地 JSON 文件并附加签名与更新时间。
        实现方式：JSON 序列化 + 文件系统写入。
        核心价值：让缓存可跨进程/跨任务复用。
        决策逻辑：
        - 条件：not self._persistent_cache_path or not self._hash_cache
        依据来源（证据链）：
        - 对象内部状态：self._hash_cache, self._persistent_cache_path。
        输入参数：
        - 无。
        输出参数：
        - 无（仅更新本地缓存文件）。"""
        if not self._persistent_cache_path or not self._hash_cache:
            return
        try:
            items = self._hash_cache.export_results()
            url_hash = ""
            try:
                url_hash = self._persistent_cache_path.parent.parent.name
            except Exception:
                url_hash = ""
            data = {
                "meta": {
                    "signature": self._cache_signature,
                    "url_hash": url_hash,
                    "updated_at": int(time.time())
                },
                "items": items
            }
            with open(self._persistent_cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Failed to save persistent cache: {e}")
    
    @property
    def enabled(self) -> bool:
        """
        执行逻辑：
        1) 返回当前验证器的启用状态。
        2) 保持接口一致性（当前实现固定为 True）。
        实现方式：直接返回固定值。
        核心价值：为上层提供统一的开关语义。
        输入参数：
        - 无。
        输出参数：
        - bool：是否启用验证器。"""
        return True
    
    def validate(self, image_path: str, ocr_text: str = "") -> ConcreteKnowledgeResult:
        """
        执行逻辑：
        1) 校验图片存在性，缺失则直接返回默认结果。
        2) 通过 pHash 检测重复帧，命中则复用缓存结果。
        3) 进行公式检测，命中则直接保留截图。
        4) 提取图形区域，失败则判为纯文字页并剔除。
        5) Vision AI 可用时调用 _vision_validate_v3，否则走 CV-only 保守保留。
        实现方式：HashCacheManager + OCR/文本规则 + OpenCV 图形区域提取 + VisionAIClient。
        核心价值：减少重复调用并提高具象知识筛选准确度。
        决策逻辑：
        - 条件：not os.path.exists(image_path)
        - 条件：self._hash_cache 命中重复帧
        - 条件：has_formula
        - 条件：graphic_region is None
        - 条件：self._vision_enabled and self._vision_client
        依据来源（证据链）：
        - 文件系统状态：image_path 是否存在。
        - 缓存阈值：self._hash_cache.threshold（相似度阈值）。
        - OCR/文本：ocr_text 与 _detect_math_formula 结果。
        - 配置状态：vision_ai.enabled 与 bearer_token 是否生效。
        输入参数：
        - image_path: 文件路径（类型：str）。
        - ocr_text: 文本内容（类型：str）。
        输出参数：
        - ConcreteKnowledgeResult（包含具象判定、置信度、类型、理由与保留建议）。"""
        if not os.path.exists(image_path):
            logger.warning(f"Image not found: {image_path}")
            return self._default_result(False)
        
        # 🚀 Step 0: 感知哈希重复帧检测 (快速跳过相似帧)
        if self._hash_cache:
            is_duplicate, cached_result = self._hash_cache.check_duplicate(image_path)
            if is_duplicate and cached_result:
                logger.info(f"Duplicate frame skipped (pHash): {Path(image_path).name}")
                # 将缓存的字典转换为 ConcreteKnowledgeResult
                if isinstance(cached_result, dict):
                    return ConcreteKnowledgeResult(
                        has_concrete=cached_result.get("has_concrete", True),
                        has_formula=cached_result.get("has_formula", False),
                        confidence=cached_result.get("confidence", 0.9),
                        concrete_type=cached_result.get("concrete_type", "缓存结果"),
                        reason=f"重复帧 (pHash相似度>{self._hash_cache.threshold:.0%})",
                        is_mixed=cached_result.get("is_mixed", False),
                        non_text_ratio=cached_result.get("non_text_ratio", 0.0),
                        should_include=cached_result.get("should_include", True)
                    )
                elif isinstance(cached_result, ConcreteKnowledgeResult):
                    return cached_result
        
        # Step 1: 检测数学公式 (需要 conda whisper_env 中的 PaddleOCR)
        has_formula = self._detect_math_formula(ocr_text)
        if has_formula:
            logger.info(f"Formula detected in {Path(image_path).name}, including screenshot")
            result = ConcreteKnowledgeResult(
                has_concrete=False,
                has_formula=True,
                confidence=0.9,
                concrete_type="公式",
                reason="检测到数学公式，保留截图",
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=True
            )
            self._cache_result(image_path, result)
            return result
        
        # Step 2: CV 分析 - 提取图形区域
        graphic_region = self._extract_graphic_region(image_path)
        
        if graphic_region is None:
            # 无法提取图形区域，判定为纯文字页
            logger.info(f"No graphic region in {Path(image_path).name}, skipping screenshot")
            result = ConcreteKnowledgeResult(
                has_concrete=False,
                has_formula=False,
                confidence=0.9,
                concrete_type="无",
                reason="未检测到图形区域，判定为纯文字页",
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=False
            )
            self._cache_result(image_path, result)
            return result
        
        # Step 3: ERNIE Vision AI 验证 (只发送裁剪后的图形区域)
        if self._vision_enabled and self._vision_client:
            result = self._vision_validate_v3(image_path, graphic_region)
            self._cache_result(image_path, result)
            return result
        
        # Fallback: 无 Vision API 时，有图形区域则保留
        logger.info(f"Vision API disabled, including {Path(image_path).name} with graphic region")
        result = ConcreteKnowledgeResult(
            has_concrete=True,
            has_formula=False,
            confidence=0.6,
            concrete_type="未知图形",
            reason="检测到图形区域，Vision API 不可用，默认保留",
            is_mixed=False,
            non_text_ratio=0.0,
            should_include=True
        )
        self._cache_result(image_path, result)
        return result
    
    def _cache_result(self, image_path: str, result: ConcreteKnowledgeResult):
        """
        执行逻辑：
        1) 将结果转换为可序列化字典。
        2) 写入 hash_cache 并同步到持久化缓存。
        实现方式：HashCacheManager.store_result + 本地 JSON。
        核心价值：跨帧复用具象判定，减少重复调用。
        决策逻辑：
        - 条件：self._hash_cache
        依据来源（证据链）：
        - 对象内部状态：self._hash_cache。
        输入参数：
        - image_path: 文件路径（类型：str）。
        - result: 函数入参（类型：ConcreteKnowledgeResult）。
        输出参数：
        - 无（仅更新缓存）。"""
        if self._hash_cache:
            result_dict = {
                "has_concrete": result.has_concrete,
                "has_formula": result.has_formula,
                "confidence": result.confidence,
                "concrete_type": result.concrete_type,
                "is_mixed": result.is_mixed,
                "non_text_ratio": result.non_text_ratio,
                "should_include": result.should_include
            }
            self._hash_cache.store_result(image_path, result_dict)
            self._save_persistent_cache()
    
    def _vision_validate_v3(self, image_path: str, graphic_region: np.ndarray) -> ConcreteKnowledgeResult:
        """
        执行逻辑：
        1) 将裁剪图形区域写入临时文件。
        2) 以异步方式调用 VisionAIClient.validate_image。
        3) 解析 has_concrete_knowledge/confidence/concrete_type/reason 并生成结果。
        4) 清理临时文件，异常时返回保守结果。
        实现方式：OpenCV 写临时图像 + asyncio/线程池封装异步调用。
        核心价值：仅上传图形区域，提高判定效率与准确度。
        决策逻辑：
        - 条件：loop.is_running()
        - 条件：has_concrete or confidence < 0.5（低置信度时保留）
        依据来源（证据链）：
        - 运行状态：asyncio 事件循环是否在运行。
        - API 字段：has_concrete_knowledge、confidence、concrete_type、reason。
        输入参数：
        - image_path: 文件路径（类型：str）。
        - graphic_region: 函数入参（类型：np.ndarray）。
        输出参数：
        - ConcreteKnowledgeResult（基于 Vision AI 判定的具象结论）。"""
        import asyncio
        
        # 保存裁剪区域为临时文件
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            cv2.imwrite(tmp.name, graphic_region)
            temp_path = tmp.name
        
        try:
            # 调用异步 API (同步包装)
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # 如果已在异步上下文，创建新任务
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self._vision_client.validate_image(temp_path, CONCRETE_KNOWLEDGE_PROMPT)
                    )
                    api_result = future.result(timeout=60)
            else:
                api_result = asyncio.run(
                    self._vision_client.validate_image(temp_path, CONCRETE_KNOWLEDGE_PROMPT)
                )
            
            # 解析结果
            has_concrete = api_result.get("has_concrete_knowledge", "否") == "是"
            confidence = float(api_result.get("confidence", 0.5))
            concrete_type = api_result.get("concrete_type", "未知")
            reason = api_result.get("reason", "Vision AI 判定")
            
            return ConcreteKnowledgeResult(
                has_concrete=has_concrete,
                has_formula=False,
                confidence=confidence,
                concrete_type=concrete_type,
                reason=reason,
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=has_concrete or confidence < 0.5  # 低置信度时保留
            )
            
        except Exception as e:
            logger.error(f"VisionAIClient validation failed: {e}")
            return ConcreteKnowledgeResult(
                has_concrete=True,
                has_formula=False,
                confidence=0.5,
                concrete_type="未知",
                reason=f"Vision AI 调用失败: {e}",
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=True  # 失败时保守保留
            )
        finally:
            # 清理临时文件
            try:
                os.unlink(temp_path)
            except:
                pass
    
    def validate_batch(self, tasks: List[Dict]) -> List[ConcreteKnowledgeResult]:
        """
        执行逻辑：
        1) 将任务列表映射到线程池并发执行。
        2) 保持结果顺序与输入一致。
        3) 单个任务异常时返回默认结果。
        实现方式：ThreadPoolExecutor + validate。
        核心价值：批量验证提升吞吐，同时保持失败可控。
        决策逻辑：
        - 条件：任务执行异常时回退到 _default_result(True)。
        依据来源（证据链）：
        - 运行状态：future.result() 抛出的异常。
        输入参数：
        - tasks: 数据列表/集合（类型：List[Dict]）。
        输出参数：
        - ConcreteKnowledgeResult 列表（顺序与输入 tasks 对齐）。"""
        import concurrent.futures
        results = [None] * len(tasks)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            future_to_idx = {}
            for i, task in enumerate(tasks):
                future = executor.submit(
                    self.validate,
                    image_path=task.get("image_path", ""),
                    ocr_text=task.get("ocr_text", "")
                )
                future_to_idx[future] = i
                
            for future in concurrent.futures.as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    logger.error(f"Batch validation failed for item {idx}: {e}")
                    results[idx] = self._default_result(True)
        
        return results
    
    def _detect_math_formula(self, text: str = "", image: Optional[np.ndarray] = None) -> bool:
        """
        执行逻辑：
        1) 若 ThreadSafeMathOCR 可用且提供图像，则优先使用 OCR 检测公式。
        2) 若无 OCR 结果，则使用文本规则匹配公式特征。
        3) 满足阈值条件即认为存在数学公式。
        实现方式：OCR 结果阈值 + 文本特征计数。
        核心价值：在无 OCR 或失败时仍能保证公式召回。
        决策逻辑：
        - 条件：self._math_ocr and image is not None
        - 条件：not text
        - 条件：OCR score >= 0.7
        - 条件：formula_count >= 2 or math_char_count >= 3
        依据来源（证据链）：
        - OCR 结果：res.score 字段。
        - 文本特征：公式模式命中数与数学字符数。
        - 输入参数：image、text。
        输入参数：
        - text: 文本内容（类型：str）。
        - image: 函数入参（类型：Optional[np.ndarray]）。
        输出参数：
        - bool：是否检测到数学公式。"""
        # 策略1: 使用 ThreadSafeMathOCR 进行图像级检测 (高精度)
        if self._math_ocr and image is not None:
            try:
                math_results = self._math_ocr.recognize_math(image)
                if math_results:
                    for res in math_results:
                        if res.get("score", 0) >= 0.7:
                            logger.debug(f"MathOCR detected: {res.get('text', '')[:50]}")
                            return True
            except Exception as e:
                logger.debug(f"MathOCR detection failed: {e}")
        
        # 策略2: 文本特征检测 (fallback)
        if not text:
            return False
        
        # 数学符号和结构 (来自 ocr_utils.py ThreadSafeMathOCR 的字符集)
        math_chars = set(r"√∫∑∏+-×÷=<>≤≥≠()[]{}^_/\\")
        formula_patterns = [
            "=",           # 等式
            "∑", "∫", "∏", # 求和、积分、连乘
            "√", "∛",      # 根号
            "∞", "≈", "≠", "≤", "≥",  # 数学符号
            "α", "β", "γ", "δ", "θ", "λ", "μ", "π", "σ",  # 希腊字母
            "²", "³", "⁴", "ⁿ",  # 上标
            "÷", "×",      # 运算符
            "lim", "log", "ln", "sin", "cos", "tan",  # 函数
            "d/dx", "dy/dx",  # 微分
            "n+1", "n-1", "2n",  # 变量表达式
            "ASL", "O(n)",  # 算法复杂度
        ]
        
        # 分数表达式模式
        fraction_patterns = ["/", "分之", "比"]
        
        # 检查公式模式
        text_lower = text.lower()
        formula_count = sum(1 for p in formula_patterns if p in text or p.lower() in text_lower)
        
        # 检查数学字符
        math_char_count = sum(1 for c in text if c in math_chars)
        
        # 如果有2个以上公式特征，或者有足够多的数学字符
        if formula_count >= 2 or math_char_count >= 3:
            return True
        
        # 检查分数表达式
        import re
        for p in fraction_patterns:
            if p in text:
                if re.search(r'\d+[/÷]\d+', text) or re.search(r'\d+分之\d+', text):
                    return True
        
        return False
    
    def _analyze_cv_features(self, image_path: str) -> Tuple[float, str, Optional[np.ndarray]]:
        """
        执行逻辑：
        1) 读取图像并计算边缘与形态学区域。
        2) 估算非文本区域占比并判定页面类型。
        3) 返回非文本区域掩膜以供后续使用。
        实现方式：OpenCV Canny/形态学操作 + NumPy 统计。
        核心价值：以轻量 CV 特征评估图文结构。
        决策逻辑：
        - 条件：img is None
        - 条件：total_area > 0
        - 条件：non_text_ratio < 0.1 / 0.3 / 0.6（页面类型划分）
        依据来源（证据链）：
        - 计算指标：non_text_ratio、total_area。
        输入参数：
        - image_path: 文件路径（类型：str）。
        输出参数：
        - (non_text_ratio, page_type, non_text_region)：
          非文本占比、页面类型、非文本区域图像。"""
        try:
            img = cv2.imread(image_path)
            if img is None:
                return 0.0, "unknown", None
            
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            height, width = gray.shape
            
            # 边缘检测
            edges = cv2.Canny(gray, 50, 150)
            
            # 文本区域检测 (使用形态学操作)
            # 文字通常是密集的小边缘
            kernel_text = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 3))
            text_regions = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel_text)
            
            # 图形区域检测 (使用较大的结构元素)
            kernel_graphic = cv2.getStructuringElement(cv2.MORPH_RECT, (20, 20))
            graphic_regions = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel_graphic)
            
            # 计算非文本区域占比
            text_area = np.sum(text_regions > 0)
            graphic_area = np.sum(graphic_regions > 0)
            total_area = height * width
            
            # 非文本占比 = (图形区域 - 文本区域) / 总面积
            non_text_area = max(0, graphic_area - text_area * 0.5)
            non_text_ratio = non_text_area / total_area if total_area > 0 else 0.0
            
            # 页面类型判断
            edge_density = np.sum(edges > 0) / total_area
            
            if non_text_ratio < 0.1:
                page_type = "text_only"
            elif non_text_ratio < 0.3:
                page_type = "text_with_diagram"
            elif non_text_ratio < 0.6:
                page_type = "mixed"
            else:
                page_type = "graphic_heavy"
            
            # 提取非文本区域
            non_text_mask = cv2.subtract(graphic_regions, text_regions)
            non_text_region = cv2.bitwise_and(img, img, mask=non_text_mask)
            
            return non_text_ratio, page_type, non_text_region
            
        except Exception as e:
            logger.error(f"CV analysis failed: {e}")
            return 0.0, "unknown", None
    
    def _extract_graphic_region(self, image_path: str) -> Optional[np.ndarray]:
        """
        执行逻辑：
        1) 读取图像并检测边缘与连通区域。
        2) 过滤过小/过大的轮廓并合并边界框。
        3) 扩展边界并裁剪图形区域返回。
        实现方式：OpenCV 轮廓检测 + 简单面积阈值过滤。
        核心价值：提取可能承载具象知识的图形区域。
        决策逻辑：
        - 条件：img is None
        - 条件：not contours
        - 条件：not valid_contours
        - 条件：裁剪区域尺寸 < 50px
        依据来源（证据链）：
        - 轮廓面积阈值：1% ~ 95% 总面积。
        - 裁剪区域尺寸阈值：50px。
        输入参数：
        - image_path: 文件路径（类型：str）。
        输出参数：
        - np.ndarray 图形区域或 None（无有效图形）。"""
        try:
            img = cv2.imread(image_path)
            if img is None:
                return None
            
            height, width = img.shape[:2]
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # 边缘检测
            edges = cv2.Canny(gray, 50, 150)
            
            # 膨胀边缘找到连通区域
            kernel = np.ones((5, 5), np.uint8)
            dilated = cv2.dilate(edges, kernel, iterations=3)
            
            # 查找轮廓
            contours, _ = cv2.findContours(dilated, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if not contours:
                return None
            
            # 找最大轮廓区域
            total_area = height * width
            valid_contours = []
            
            for cnt in contours:
                area = cv2.contourArea(cnt)
                # 过滤太小或太大的区域
                if area > total_area * 0.01 and area < total_area * 0.95:
                    valid_contours.append(cnt)
            
            if not valid_contours:
                # 无有效图形区域
                return None
            
            # 合并所有有效轮廓的边界框
            all_points = np.vstack(valid_contours)
            x, y, w, h = cv2.boundingRect(all_points)
            
            # 扩展边界 10%
            margin_x = int(w * 0.1)
            margin_y = int(h * 0.1)
            x = max(0, x - margin_x)
            y = max(0, y - margin_y)
            w = min(width - x, w + 2 * margin_x)
            h = min(height - y, h + 2 * margin_y)
            
            # 裁剪图形区域
            graphic_region = img[y:y+h, x:x+w]
            
            # 检查裁剪区域是否足够大
            if graphic_region.shape[0] < 50 or graphic_region.shape[1] < 50:
                return None
            
            logger.debug(f"Extracted graphic region: {w}x{h} from {width}x{height}")
            return graphic_region
            
        except Exception as e:
            logger.error(f"Failed to extract graphic region: {e}")
            return None
    
    def _vision_validate(self, image: np.ndarray) -> ConcreteKnowledgeResult:
        """
        执行逻辑：
        1) 将图像编码为 base64 并调用 ERNIE Vision API。
        2) 从响应中提取 JSON 结果并解析字段。
        3) 根据置信度与具象标记生成结果。
        实现方式：HTTP 调用 + JSON 解析 + 置信度阈值判断。
        核心价值：提供 Vision AI 的直接判定路径（旧版兼容）。
        决策逻辑：
        - 条件：'```json' in content
        - 条件：'```' in content
        - 条件：has_concrete_knowledge == "是"
        - 条件：confidence >= 0.5
        依据来源（证据链）：
        - API 字段：choices[0].message.content。
        - 解析字段：has_concrete_knowledge、confidence、concrete_type、reason。
        输入参数：
        - image: 函数入参（类型：np.ndarray）。
        输出参数：
        - ConcreteKnowledgeResult（基于 Vision API 的具象判定）。"""
        try:
            # 编码图片为 base64
            _, buffer = cv2.imencode('.png', image)
            image_base64 = base64.b64encode(buffer).decode("utf-8")
            
            # ERNIE Vision 消息格式 (只发送图片和简化 prompt)
            messages = [{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{image_base64}"
                        }
                    },
                    {
                        "type": "text",
                        "text": CONCRETE_KNOWLEDGE_PROMPT
                    }
                ]
            }]
            
            payload = {
                "model": self._vision_model,
                "messages": messages,
                "temperature": self._temperature,
            }
            
            # 调用 ERNIE Vision API
            with httpx.Client(timeout=60.0) as client:
                response = client.post(
                    self._base_url,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {self._bearer_token}",
                        "Content-Type": "application/json"
                    }
                )
                response.raise_for_status()
                data = response.json()
            
            content = data["choices"][0]["message"]["content"].strip()
            
            # 解析 JSON
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0].strip()
            elif "```" in content:
                content = content.split("```")[1].split("```")[0].strip()
            
            result = json.loads(content)
            
            has_concrete = result.get("has_concrete_knowledge") == "是"
            confidence = float(result.get("confidence", 0.5))
            
            # 置信度阈值判定
            should_include = has_concrete and confidence >= 0.5
            
            return ConcreteKnowledgeResult(
                has_concrete=has_concrete,
                has_formula=False,
                confidence=confidence,
                concrete_type=result.get("concrete_type", "无"),
                reason=result.get("reason", ""),
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=should_include
            )
            
        except Exception as e:
            logger.error(f"ERNIE Vision validation failed: {e}")
            # Fallback: 默认保留
            return ConcreteKnowledgeResult(
                has_concrete=True,
                has_formula=False,
                confidence=0.5,
                concrete_type="未知",
                reason=f"Vision API 调用失败: {e}，默认保留",
                is_mixed=False,
                non_text_ratio=0.0,
                should_include=True
            )
    
    def _cv_only_validate(self, non_text_ratio: float, cv_page_type: str) -> ConcreteKnowledgeResult:
        """
        执行逻辑：
        1) 根据 non_text_ratio 粗分页面类型。
        2) 按阈值给出保留/剔除建议。
        实现方式：简单阈值规则。
        核心价值：在无 Vision AI 时提供可解释的回退策略。
        决策逻辑：
        - 条件：non_text_ratio >= 0.4
        - 条件：non_text_ratio >= 0.2
        依据来源（证据链）：
        - CV 指标：non_text_ratio。
        输入参数：
        - non_text_ratio: 函数入参（类型：float）。
        - cv_page_type: 函数入参（类型：str）。
        输出参数：
        - ConcreteKnowledgeResult（基于非文本占比的判定）。"""
        # 基于非文本区域比例判断
        if non_text_ratio >= 0.4:
            return ConcreteKnowledgeResult(
                has_concrete=True,
                has_formula=False,
                confidence=0.6,
                concrete_type="图形",
                reason=f"CV检测: 非文本区域占比 {non_text_ratio:.1%}，可能包含图形",
                is_mixed=True,
                non_text_ratio=non_text_ratio,
                should_include=True
            )
        elif non_text_ratio >= 0.2:
            return ConcreteKnowledgeResult(
                has_concrete=False,
                has_formula=False,
                confidence=0.5,
                concrete_type="不确定",
                reason=f"CV检测: 非文本区域占比 {non_text_ratio:.1%}，建议保留",
                is_mixed=True,
                non_text_ratio=non_text_ratio,
                should_include=True  # 模糊时保留
            )
        else:
            return ConcreteKnowledgeResult(
                has_concrete=False,
                has_formula=False,
                confidence=0.7,
                concrete_type="无",
                reason=f"CV检测: 非文本区域占比仅 {non_text_ratio:.1%}，判定为纯文字",
                is_mixed=False,
                non_text_ratio=non_text_ratio,
                should_include=False
            )
    
    def _default_result(self, should_include: bool) -> ConcreteKnowledgeResult:
        """
        执行逻辑：
        1) 使用默认字段构造兜底结果。
        2) 由 should_include 决定是否保留。
        实现方式：直接实例化 ConcreteKnowledgeResult。
        核心价值：异常/缺失场景下保持可控输出。
        输入参数：
        - should_include: 函数入参（类型：bool）。
        输出参数：
        - ConcreteKnowledgeResult（默认判定结果）。"""
        return ConcreteKnowledgeResult(
            has_concrete=False,
            has_formula=False,
            confidence=0.0,
            concrete_type="无",
            reason="默认判定",
            is_mixed=False,
            non_text_ratio=0.0,
            should_include=should_include
        )


# ==============================================================================
# Test Entry
# ==============================================================================
