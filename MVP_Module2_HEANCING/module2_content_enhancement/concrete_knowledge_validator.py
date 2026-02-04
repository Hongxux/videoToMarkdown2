"""
具象性知识验证器

用于判定截图中是否包含具象性知识（实物图、装置图、解剖图、实操界面等）
用于决定是否在 Markdown 中插入截图

流程:
1. 检测数学公式 → 有则保留
2. 检测连续色块/线条图形 → 有则裁剪非文字区进行 Vision AI 验证
3. Vision AI 判定是否为功能性教学图形

V2.0 - 使用 ERNIE Vision API (千帆平台)
"""

import os
import cv2
import json
import base64
import logging
import httpx
import yaml
import numpy as np
from pathlib import Path
from typing import Dict, Optional, Tuple, List
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
    """具象性知识验证结果"""
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
    具象性知识验证器
    
    判定截图是否包含应该插入的教学内容
    
    Vision AI 验证使用 ERNIE Vision API (千帆平台)
    配置从 config.yaml 读取 (vision_ai 部分)
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Args:
            config_path: config.yaml 路径 (可选，默认自动查找)
        """
        # V3: 使用模块化 VisionAIClient (带感知哈希缓存)
        from .vision_ai_client import VisionAIClient, VisionAIConfig, PerceptualHasher, HashCacheManager
        
        self._vision_enabled = False
        self._vision_client: Optional[VisionAIClient] = None
        
        # 加载配置并初始化 VisionAIClient
        vision_config = self._load_vision_config(config_path)
        if vision_config:
            self._vision_client = VisionAIClient(vision_config)
            self._vision_enabled = vision_config.enabled
        
        # 独立的感知哈希缓存 (用于非 Vision API 场景)
        self._hash_cache = HashCacheManager(similarity_threshold=0.95)
        self._hasher = PerceptualHasher
        
        # V2: 集成 ThreadSafeMathOCR 进行精准公式检测 (用户建议)
        self._math_ocr = None
        try:
            from .ocr_utils import ThreadSafeMathOCR
            self._math_ocr = ThreadSafeMathOCR()
            logger.info("ThreadSafeMathOCR initialized for formula detection")
        except Exception as e:
            logger.warning(f"ThreadSafeMathOCR not available, using fallback formula detection: {e}")
    
    def _load_vision_config(self, config_path: Optional[str] = None):
        """从 config.yaml 加载 ERNIE Vision 配置，返回 VisionAIConfig"""
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
    
    @property
    def enabled(self) -> bool:
        """CV + 公式检测始终可用，Vision AI 验证可选"""
        return True
    
    def validate(self, image_path: str, ocr_text: str = "") -> ConcreteKnowledgeResult:
        """
        验证截图是否包含具象性知识
        
        V3: 增加感知哈希重复帧检测
        
        Args:
            image_path: 截图路径
            ocr_text: OCR 识别文本 (可选)
            
        Returns:
            ConcreteKnowledgeResult
        """
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
        """缓存验证结果到哈希缓存"""
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
    
    def _vision_validate_v3(self, image_path: str, graphic_region: np.ndarray) -> ConcreteKnowledgeResult:
        """使用 VisionAIClient (V3) 进行验证"""
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
        批量并发验证
        Args:
            tasks: [{"image_path": str, "ocr_text": str}, ...]
        """
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
        检测是否包含数学公式
        
        V2: 集成 ThreadSafeMathOCR (PaddleOCR) 进行图像级公式检测
        
        Args:
            text: OCR 文本 (可选)
            image: 图像数组 (可选，用于 PaddleOCR 检测)
        """
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
        CV 分析图像特征
        
        Returns:
            (非文本区域占比, 页面类型, 非文本区域图像)
        """
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
        提取图像中的图形区域（非文字区域）
        
        Args:
            image_path: 图像路径
            
        Returns:
            裁剪后的图形区域 (numpy array)，如果无图形区域则返回 None
        """
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
        使用 ERNIE Vision AI 验证具象性知识
        
        Args:
            image: 图形区域图像 (numpy array, BGR format)
            
        Returns:
            ConcreteKnowledgeResult
        """
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
        """仅使用 CV 特征判断"""
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
        """默认结果"""
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
