"""
模块说明：Module2 内容增强中的 ocr_utils 模块。
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
from typing import Optional, Dict, List, Tuple
from pathlib import Path
import cv2
import numpy as np

logger = logging.getLogger(__name__)


class OCRExtractor:
    """类说明：OCRExtractor 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, lang: str = "chi_sim+eng"):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - lang: 函数入参（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.lang = lang
        
        # 1. 尝试初始化并绑定 Tesseract 二进制路径 (🚀 Windows 鲁棒化)
        self._setup_tesseract_path()
        
        # 2. 检查 Tesseract 是否可用
        self._check_tesseract()
    
    def _setup_tesseract_path(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：sys.platform != 'win32'
        - 条件：shutil.which('tesseract')
        - 条件：p.exists()
        依据来源（证据链）：
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        import sys
        if sys.platform != "win32":
            return
            
        try:
            import pytesseract
            import shutil
            
            # 如果已经能直接在 PATH 找到, 则无需手动绑定
            if shutil.which("tesseract"):
                logger.debug("Tesseract found in system PATH.")
                return
                
            # 常见 Windows 安装路径
            common_paths = [
                r"C:\Program Files\Tesseract-OCR\tesseract.exe",
                r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
                r"E:\Program Files\Tesseract-OCR\tesseract.exe",
                r"C:\Users\{user}\AppData\Local\Tesseract-OCR\tesseract.exe".format(user=Path.home().name),
                # 针对特定用户环境的推测路径
            ]
            
            for path_str in common_paths:
                p = Path(path_str)
                if p.exists():
                    pytesseract.pytesseract.tesseract_cmd = str(p)
                    logger.info(f"🚀 Tesseract binary bound to: {str(p)}")
                    return
            
            logger.warning("Tesseract binary NOT found in common paths. OCR will likely fail.")
        except Exception as e:
            logger.error(f"Error during Tesseract path setup: {e}")

    def _check_tesseract(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        try:
            import pytesseract
            # 尝试获取版本以验证 binary 可用性
            version = pytesseract.get_tesseract_version()
            logger.info(f"✅ Tesseract OCR ready (Version: {version})")
        except ImportError:
            logger.error("pytesseract package not installed. Please pip install pytesseract.")
        except Exception as e:
            logger.warning(f"⚠️ Tesseract binary not detected or not in PATH: {e}")
            logger.warning("Please install Tesseract-OCR and ensure it's in your PATH or common installation dirs.")
    
    def extract_text_from_image(
        self,
        image_path: str,
        preprocess: bool = True
    ) -> str:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理、文件系统读写实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：image is None
        - 条件：preprocess
        依据来源（证据链）：
        - 输入参数：preprocess。
        输入参数：
        - image_path: 文件路径（类型：str）。
        - preprocess: 函数入参（类型：bool）。
        输出参数：
        - 字符串结果。"""
        try:
            import pytesseract
            
            # 读取图片
            image = cv2.imread(image_path)
            if image is None:
                raise ValueError(f"Cannot read image: {image_path}")
            
            # 预处理
            if preprocess:
                image = self._preprocess_image(image)
            
            # OCR提取
            text = pytesseract.image_to_string(
                image,
                lang=self.lang,
                config='--psm 6'  # PSM 6: 假设文本块
            )
            
            # 清理文本
            text = self._clean_ocr_text(text)
            
            logger.info(f"OCR extracted {len(text)} chars from {Path(image_path).name}")
            
            return text
        
        except Exception as e:
            logger.error(f"OCR extraction failed: {e}")
            return ""
    
    def extract_text_from_frame(
        self,
        frame: np.ndarray,
        preprocess: bool = True
    ) -> str:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：preprocess
        依据来源（证据链）：
        - 输入参数：preprocess。
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        - preprocess: 函数入参（类型：bool）。
        输出参数：
        - 字符串结果。"""
        try:
            import pytesseract
            
            # 预处理
            if preprocess:
                frame = self._preprocess_image(frame)
            
            # OCR提取
            text = pytesseract.image_to_string(
                frame,
                lang=self.lang,
                config='--psm 6'
            )
            
            # 清理文本
            text = self._clean_ocr_text(text)
            
            logger.info(f"OCR extracted {len(text)} chars from frame")
            
            return text
        
        except Exception as e:
            logger.error(f"OCR extraction from frame failed: {e}")
            return ""
    
    def extract_text_regions_from_frame(
        self,
        frame: np.ndarray
    ) -> List[Dict]:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：text and confidence > 30
        依据来源（证据链）：
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        输出参数：
        - Dict 列表（与输入或处理结果一一对应）。"""
        try:
            import pytesseract
            
            # 提取文字和位置
            data = pytesseract.image_to_data(
                frame,
                lang=self.lang,
                config='--psm 6',
                output_type=pytesseract.Output.DICT
            )
            
            # 解析结果
            regions = []
            n_boxes = len(data['text'])
            
            for i in range(n_boxes):
                text = data['text'][i].strip()
                confidence = float(data['conf'][i])
                
                # 过滤低置信度和空文本
                if text and confidence > 30:  # 置信度阈值
                    regions.append({
                        "text": text,
                        "x": data['left'][i],
                        "y": data['top'][i],
                        "w": data['width'][i],
                        "h": data['height'][i],
                        "confidence": confidence
                    })
            
            logger.debug(f"OCR found {len(regions)} text regions in frame")
            
            return regions
        
        except Exception as e:
            logger.error(f"Text region extraction from frame failed: {e}")
            return []
    
    def extract_text_regions(
        self,
        image_path: str
    ) -> List[Dict]:
        """
        执行逻辑：
        1) 扫描输入内容。
        2) 过滤并提取目标子集。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理实现。
        核心价值：聚焦关键信息，减少后续处理成本。
        决策逻辑：
        - 条件：image is None
        - 条件：text
        依据来源（证据链）：
        输入参数：
        - image_path: 文件路径（类型：str）。
        输出参数：
        - Dict 列表（与输入或处理结果一一对应）。"""
        try:
            import pytesseract
            
            # 读取图片
            image = cv2.imread(image_path)
            if image is None:
                raise ValueError(f"Cannot read image: {image_path}")
            
            # 提取文字和位置
            data = pytesseract.image_to_data(
                image,
                lang=self.lang,
                config='--psm 6',
                output_type=pytesseract.Output.DICT
            )
            
            # 解析结果
            regions = []
            n_boxes = len(data['text'])
            
            for i in range(n_boxes):
                text = data['text'][i].strip()
                if text:  # 过滤空文本
                    regions.append({
                        "text": text,
                        "x": data['left'][i],
                        "y": data['top'][i],
                        "w": data['width'][i],
                        "h": data['height'][i],
                        "confidence": data['conf'][i]
                    })
            
            logger.info(f"OCR found {len(regions)} text regions")
            
            return regions
        
        except Exception as e:
            logger.error(f"Text region extraction failed: {e}")
            return []
    
    def _preprocess_image(self, image: np.ndarray) -> np.ndarray:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - image: 函数入参（类型：np.ndarray）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        # 灰度化
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        
        # 去噪
        denoised = cv2.GaussianBlur(gray, (5, 5), 0)
        
        # 二值化 (Otsu自适应阈值)
        _, binary = cv2.threshold(
            denoised,
            0, 255,
            cv2.THRESH_BINARY + cv2.THRESH_OTSU
        )
        
        return binary
    
    def _clean_ocr_text(self, text: str) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - text: 文本内容（类型：str）。
        输出参数：
        - 字符串结果。"""
        # 移除多余空白
        text = ' '.join(text.split())
        
        # 移除明显的OCR错误字符
        # (可以根据实际情况调整)
        
        return text.strip()
    
    def calculate_text_match_rate(
        self,
        text1: str,
        text2: str
    ) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not text1 or not text2
        - 条件：not words1 or not words2
        - 条件：union
        依据来源（证据链）：
        - 输入参数：text1, text2。
        输入参数：
        - text1: 函数入参（类型：str）。
        - text2: 函数入参（类型：str）。
        输出参数：
        - 数值型计算结果。"""
        if not text1 or not text2:
            return 0.0
        
        # 简单分词 (按字符+空格)
        words1 = set(text1.lower().replace(' ', ''))
        words2 = set(text2.lower().replace(' ', ''))
        
        if not words1 or not words2:
            return 0.0
        
        # 计算Jaccard相似度
        intersection = words1 & words2
        union = words1 | words2
        
        match_rate = len(intersection) / len(union) if union else 0.0
        return match_rate
        

import threading

class ThreadSafeMathOCR:
    """类说明：ThreadSafeMathOCR 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not cls._instance
        依据来源（证据链）：
        输入参数：
        - 无。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_engines()
        return cls._instance
        
    def _init_engines(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、Paddle 相关能力实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.paddle = None
        self.tesseract_available = False
        
        # 1. 尝试初始化 PaddleOCR
        try:
            from paddleocr import PaddleOCR
            paddle_kwargs = {
                "use_angle_cls": True,
                "lang": "ch",
                "det_db_score_mode": "slow",
                "use_space_char": True,
                "show_log": False,
            }
            try:
                self.paddle = PaddleOCR(**paddle_kwargs)
            except TypeError as type_err:
                # 兼容不同版本 PaddleOCR：部分版本不支持 show_log 参数
                if "show_log" in str(type_err):
                    paddle_kwargs.pop("show_log", None)
                    self.paddle = PaddleOCR(**paddle_kwargs)
                    logger.info("PaddleOCR 当前版本不支持 show_log，已自动降级兼容初始化。")
                else:
                    raise
            logger.info("PaddleOCR engine initialized successfully for Math detection.")
        except Exception as e:
            logger.warning(f"PaddleOCR not available, math detection may be degraded: {e}")
            
        # 2. 验证 Tesseract (作为通用文本或数学兜底)
        try:
            import pytesseract
            pytesseract.get_tesseract_version()
            self.tesseract_available = True
        except:
            pass

    def recognize_math(self, image: np.ndarray, roi_box: Optional[Tuple[int, int, int, int]] = None) -> List[Dict]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算、Paddle 相关能力实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：roi_box
        - 条件：self.paddle
        - 条件：not results and self.tesseract_available
        依据来源（证据链）：
        - 输入参数：roi_box。
        - 对象内部状态：self.paddle, self.tesseract_available。
        输入参数：
        - image: 函数入参（类型：np.ndarray）。
        - roi_box: 函数入参（类型：Optional[Tuple[int, int, int, int]]）。
        输出参数：
        - Dict 列表（与输入或处理结果一一对应）。"""
        with self._lock:
            # 局部裁剪 (方案A)
            crop_img = image
            if roi_box:
                x1, y1, x2, y2 = roi_box
                crop_img = image[y1:y2, x1:x2]
                
            results = []
            
            # 优先使用 PaddleOCR
            if self.paddle:
                try:
                    ocr_res = self.paddle.ocr(crop_img, cls=True)
                    if ocr_res and len(ocr_res) > 0 and ocr_res[0]:
                        for line in ocr_res[0]:
                            text = line[1][0]
                            score = line[1][1]
                            # 过滤数学字符集
                            math_chars = set(r"√∫∑∏+-×÷=<>≤≥≠()[]{}^_/\\")
                            if any(char in math_chars for char in text) or score > 0.8:
                                results.append({
                                    "text": text,
                                    "score": score,
                                    "roi": roi_box if roi_box else (0,0,image.shape[1],image.shape[0])
                                })
                except Exception as e:
                    logger.error(f"PaddleOCR recognition failed: {e}")
            
            # 如果 Paddle 识别失败或不可用, 且 Tesseract 可用, 进行轻量级兜底
            if not results and self.tesseract_available:
                try:
                    import pytesseract
                    txt = pytesseract.image_to_string(crop_img, config='--psm 7') # PSM 7: 单行文本
                    if txt.strip():
                        results.append({
                            "text": txt.strip(),
                            "score": 0.5, # Tesseract for math is low confidence
                            "roi": roi_box
                        })
                except:
                    pass
                    
            return results
