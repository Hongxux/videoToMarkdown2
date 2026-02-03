"""
screenshot_selector.py - 视觉元素评分补充

在S4评分中加入视觉元素作为信息量的补充指标

将以下方法添加到screenshot_selector.py:
"""

def _calculate_visual_element_score(
    self,
    frame: np.ndarray
) -> float:
    """
    计算视觉元素评分 (for screenshot selection)
    
    检测架构图/流程图的视觉元素
    元素越多 → 信息越丰富 → 分数越高
    
    Returns:
        视觉元素评分 0-100
    """
    try:
        elements = self.visual_extractor.detect_visual_elements(frame)
        
        total_elements = elements.get('total', 0)
        
        # 归一化到0-100
        # 假设15个元素为满分基准
        score = min(total_elements / 15 * 100, 100)
        
        logger.debug(f"Visual elements: {total_elements}, score={score:.1f}")
        
        return score
    
    except Exception as e:
        logger.warning(f"Visual element scoring failed: {e}")
        return 50  # 中性分


# 更新_calculate_S4_no_occlusion方法,加入视觉元素评分:

def _calculate_S4_no_occlusion(
    self,
    frame: np.ndarray
) -> float:
    """
    计算S4无遮挡评分 (完整版)
    
    评分组成:
    1. 边缘密度 (15%) - 内容清晰度
    2. 颜色分布 (15%) - 无大面积遮挡
    3. OCR信息量 (35%) - 文字内容丰富度
    4. 视觉元素 (25%) - 架构图元素丰富度 (新增!)
    5. OCR遮挡惩罚 (扣分) - 弹幕字幕
    
    Returns:
        S4评分 0-100
    """
    # 转换为灰度
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    
    # 1. 边缘密度检测 (15%)
    edges = cv2.Canny(gray, 50, 150)
    edge_density = np.sum(edges > 0) / edges.size
    edge_score = edge_density * 100
    
    # 2. 颜色分布检测 (15%)
    hist = cv2.calcHist([gray], [0], None, [256], [0, 256])
    hist_std = np.std(hist)
    color_score = min(hist_std / 10, 100)
    
    # 3. OCR信息量评分 (35%)
    ocr_info_score = self._calculate_ocr_information_score(frame)
    
    # 4. 视觉元素评分 (25%) - 新增!
    visual_element_score = self._calculate_visual_element_score(frame)
    
    # 5. OCR遮挡惩罚
    occlusion_penalty = self._detect_text_overlay_with_ocr(frame)
    
    # 综合评分
    S4 = (
        edge_score * 0.15 + 
        color_score * 0.15 + 
        ocr_info_score * 0.35 +
        visual_element_score * 0.25  # 视觉元素占25%
    ) - occlusion_penalty * 0.1
    
    S4 = min(100, max(0, S4))
    
    logger.debug(f"S4 breakdown: edge={edge_score:.1f}, color={color_score:.1f}, "
                f"ocr_info={ocr_info_score:.1f}, visual_elem={visual_element_score:.1f}, "
                f"occlusion={occlusion_penalty:.1f}, final={S4:.1f}")
    
    return S4
