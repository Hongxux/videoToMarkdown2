"""
OCR Utilities - Tesseract Wrapper

Wraps Tesseract OCR for screenshot text extraction.
Used in Class 2 fault verification (C_multi calculation).
"""

import logging
from typing import Optional, Dict, List, Tuple
from pathlib import Path
import cv2
import numpy as np

logger = logging.getLogger(__name__)


class OCRExtractor:
    """
    OCR文字提取器
    
    包装Tesseract OCR,用于从截图中提取文字
    
    用途:
    - 第2类断层(具象性)的C_multi验证
    - 计算补全内容与截图OCR文本的匹配率
    """
    
    def __init__(self, lang: str = "chi_sim+eng"):
        """
        Args:
           lang: Tesseract语言包 (chi_sim=简体中文, eng=英文)
        """
        self.lang = lang
        
        # 1. 尝试初始化并绑定 Tesseract 二进制路径 (🚀 Windows 鲁棒化)
        self._setup_tesseract_path()
        
        # 2. 检查 Tesseract 是否可用
        self._check_tesseract()
    
    def _setup_tesseract_path(self):
        """自动搜寻并绑定 Tesseract 二进制路径 (Windows 特化)"""
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
                r"D:\Program Files\Tesseract-OCR\tesseract.exe",
                r"E:\Program Files\Tesseract-OCR\tesseract.exe",
                r"C:\Users\{user}\AppData\Local\Tesseract-OCR\tesseract.exe".format(user=Path.home().name),
                # 针对特定用户环境的推测路径
                r"D:\New_ANACONDA\envs\whisper_env\Library\bin\tesseract.exe"
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
        """检查Tesseract是否安装"""
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
        从图片提取文字
        
        Args:
            image_path: 图片路径
            preprocess: 是否预处理 (灰度化、二值化)
        
        Returns:
            提取的文本
        """
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
        从视频帧提取文字 (for in-memory frames)
        
        Args:
            frame: OpenCV图像 (np.ndarray)
            preprocess: 是否预处理
        
        Returns:
            提取的文本
        """
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
        从视频帧提取文字区域 (for screenshot selection)
        
        Args:
            frame: OpenCV图像 (np.ndarray)
        
        Returns:
            [{"text": "...", "x": 10, "y": 20, "w": 100, "h": 30, "confidence": 85}, ...]
        """
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
        提取文字区域 (带位置信息)
        
        Returns:
            [{"text": "...", "x": 10, "y": 20, "w": 100, "h": 30}, ...]
        """
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
        图像预处理,提高OCR准确率
        
        步骤:
        1. 灰度化
        2. 去噪 (高斯模糊)
        3. 二值化 (Otsu阈值)
        """
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
        清理OCR提取的文本
        
        - 移除多余空白
        - 移除特殊字符
        - 保留中英文和标点
        """
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
        计算两段文本的匹配率
        
        用于比较补全内容与OCR文本的相似度
        
        策略:
        1. 分词 (简单空格分割)
        2. 计算词汇重叠率
        
        Returns:
            匹配率 0-1
        """
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
    """
    数学公式专用的线程安全OCR单例 (V4)
    支持 PaddleOCR 局部识别与 Tesseract 兜底
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_engines()
        return cls._instance
        
    def _init_engines(self):
        """初始化OCR引擎 (延迟加载)"""
        self.paddle = None
        self.tesseract_available = False
        
        # 1. 尝试初始化 PaddleOCR
        try:
            from paddleocr import PaddleOCR
            self.paddle = PaddleOCR(
                use_angle_cls=True,
                lang="ch",
                det_db_score_mode="slow",
                use_space_char=True,
                show_log=False
            )
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
        进行数学符号识别 (带锁)
        :return: [{"text": "...", "score": 0.95, "roi": (x1,y1,x2,y2)}]
        """
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
