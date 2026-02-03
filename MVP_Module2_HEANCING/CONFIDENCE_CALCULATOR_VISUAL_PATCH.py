"""
confidence_calculator.py - 视觉元素评分方法补充

在confidence_calculator.py类中添加此方法:
"""

def _calculate_visual_element_score(
    self,
    screenshot_path: str
) -> float:
    """
    计算视觉元素评分M_visual (for Class 2 faults)
    
    策略:
    1. 检测截图中的视觉元素 (矩形、圆形、箭头、线条)
    2. 元素越多 → 架构图越复杂 → 分数越高
    
    Returns:
        M_visual评分 0-1
    """
    try:
        # 读取截图
        import cv2
        import numpy as np
        
        image = cv2.imread(screenshot_path)
        if image is None:
            logger.warning(f"Cannot read screenshot: {screenshot_path}")
            return 0.5
        
        # 使用visual_feature_extractor检测元素
        # 需要临时创建extractor (或从self获取if available)
        from .visual_feature_extractor import VisualFeatureExtractor
        
        extractor = VisualFeatureExtractor()
        elements = extractor.detect_visual_elements(image)
        
        # 评分策略:
        # 根据元素数量归一化到0-1
        # 假设:
        # - 0-5个元素: 低信息量 (0.2-0.4)
        # - 6-15个元素: 中信息量 (0.4-0.7)
        # - 16+个元素: 高信息量 (0.7-1.0)
        
        total_elements = elements.get('total', 0)
        
        if total_elements == 0:
            score = 0.2  # 无视觉元素
        elif total_elements <= 5:
            score = 0.2 + (total_elements / 5) * 0.2  # 0.2-0.4
        elif total_elements <= 15:
            score = 0.4 + ((total_elements - 5) / 10) * 0.3  # 0.4-0.7
        else:
            score = 0.7 + min((total_elements - 15) / 20, 0.3)  # 0.7-1.0
        
        logger.info(f"Visual elements: {total_elements}, M_visual={score:.3f}")
        logger.debug(f"Element breakdown: {elements}")
        
        return score
    
    except Exception as e:
        logger.error(f"Visual element score calculation failed: {e}")
        return 0.5  # 中性分
