"""
confidence_calculator.py 的 _verify_with_screenshot 方法更新补丁

请将以下代码替换confidence_calculator.py中的_verify_with_screenshot方法:
"""

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
        # TODO: 实现更精确的视觉元素检测 (架构图节点、箭头等)
        # 目前使用简化评分
        M_visual = 0.5
        
        C_multi = self.CLASS2_TEXT_WEIGHT * M_text + self.CLASS2_VISUAL_WEIGHT * M_visual
        
        details["ocr_text"] = ocr_text[:100] if ocr_text else "N/A"  # 截取前100字符
        details["M_text"] = M_text
        details["M_visual"] = M_visual
        details["ocr_char_count"] = len(ocr_text)
        
        logger.info(f"Screenshot verification: M_text={M_text:.3f}, M_visual={M_visual:.3f}, C_multi={C_multi:.3f}")
        
        return C_multi
    
    except ImportError:
        logger.error("OCR not available (pytesseract not installed)")
        details["ocr_error"] = "pytesseract not installed"
        return 0.5
    
    except Exception as e:
        logger.error(f"Screenshot verification failed: {e}")
        details["error"] = str(e)
        return 0.5
