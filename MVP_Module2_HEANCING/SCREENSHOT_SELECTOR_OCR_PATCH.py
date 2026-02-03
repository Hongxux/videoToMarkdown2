"""
screenshot_selector.py OCR信息量评分增强版

S4评分新设计:
- 20% 边缘密度 (内容清晰度)
- 20% 颜色分布 (无大面积遮挡)
- 40% OCR信息量 (内容区域的文字量) ← 新增正面指标!
- -20% OCR遮挡惩罚 (顶部/底部弹幕字幕)

将以下代码替换screenshot_selector.py的相关方法:
"""

def _calculate_S4_no_occlusion(
    self,
    frame: np.ndarray
) -> float:
    """
    计算S4无遮挡评分 (OCR信息量增强版)
    
    评分组成:
    1. 边缘密度 (20%) - 内容清晰度
    2. 颜色分布 (20%) - 无大面积遮挡
    3. **OCR信息量 (40%)** - 内容区域的文字量 (新增!)
    4. OCR遮挡惩罚 (扣分) - 顶部/底部的弹幕字幕
    
    Returns:
        S4评分 0-100
    """
    # 转换为灰度
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    
    # 1. 边缘密度检测 (20%)
    edges = cv2.Canny(gray, 50, 150)
    edge_density = np.sum(edges > 0) / edges.size
    edge_score = edge_density * 100
    
    # 2. 颜色分布检测 (20%)
    hist = cv2.calcHist([gray], [0], None, [256], [0, 256])
    hist_std = np.std(hist)
    color_score = min(hist_std / 10, 100)
    
    # 3. OCR信息量评分 (40%) - 新增!
    ocr_info_score = self._calculate_ocr_information_score(frame)
    
    # 4. OCR遮挡惩罚 - 扣分
    occlusion_penalty = self._detect_text_overlay_with_ocr(frame)
    
    # 综合评分
    S4 = (
        edge_score * 0.2 + 
        color_score * 0.2 + 
        ocr_info_score * 0.4  # OCR信息量占40%权重
    ) - occlusion_penalty * 0.2
    
    S4 = min(100, max(0, S4))
    
    logger.debug(f"S4 breakdown: edge={edge_score:.1f}, color={color_score:.1f}, "
                f"ocr_info={ocr_info_score:.1f}, occlusion_penalty={occlusion_penalty:.1f}")
    
    return S4

def _calculate_ocr_information_score(
    self,
    frame: np.ndarray
) -> float:
    """
    计算OCR信息量评分 (新增!)
    
    策略:
    1. 提取所有OCR文字区域
    2. 筛选出**内容区域**的文字 (中间60%区域)
    3. 统计文字量: 字符数、区域数、覆盖面积
    4. 信息量越多 → 分数越高
    
    Returns:
        OCR信息量评分 0-100
    """
    try:
        from .ocr_utils import OCRExtractor
        
        ocr = OCRExtractor(lang="chi_sim+eng")
        text_regions = ocr.extract_text_regions_from_frame(frame)
        
        if not text_regions:
            # 没有OCR文字,信息量低
            return 20  # 基础分
        
        height = frame.shape[0]
        width = frame.shape[1]
        
        # 定义内容区域 (中间60%,避开顶部/底部的弹幕字幕区域)
        content_top = height * 0.2
        content_bottom = height * 0.8
        
        # 筛选内容区域的文字
        content_text = []
        content_area = 0
        total_chars = 0
        
        for region in text_regions:
            y = region['y']
            h = region['h']
            
            # 检查是否在内容区域
            if y >= content_top and (y + h) <= content_bottom:
                content_text.append(region['text'])
                content_area += region['w'] * region['h']
                total_chars += len(region['text'])
        
        # 计算信息量评分
        if not content_text:
            return 20  # 内容区域无文字
        
        # 三个维度:
        # 1. 字符数量 (归一化到0-40)
        char_score = min(total_chars / 50 * 40, 40)  # 假设50字符为满分
        
        # 2. 文字区域数量 (归一化到0-30)
        region_score = min(len(content_text) / 10 * 30, 30)  # 假设10个区域为满分
        
        # 3. 文字覆盖率 (归一化到0-30)
        coverage_ratio = content_area / (width * height)
        coverage_score = min(coverage_ratio * 1000, 30)  # 缩放系数
        
        ocr_info_score = char_score + region_score + coverage_score
        
        logger.debug(f"OCR info: {total_chars} chars, {len(content_text)} regions, "
                    f"score={ocr_info_score:.1f}")
        
        return ocr_info_score
    
    except ImportError:
        logger.debug("OCR not available, using fallback info score")
        # 降级: 使用边缘密度作为信息量的粗略估计
        edges = cv2.Canny(cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY), 50, 150)
        edge_density = np.sum(edges > 0) / edges.size
        return edge_density * 80  # 简化评分
    
    except Exception as e:
        logger.warning(f"OCR info score calculation failed: {e}")
        return 50  # 中性分

def _detect_text_overlay_with_ocr(
    self,
    frame: np.ndarray
) -> float:
    """
    使用OCR检测文字遮挡 (弹幕/字幕)
    
    只检测**顶部/底部**区域的文字 → 认为是遮挡
    
    Returns:
        遮挡惩罚分 0-100
    """
    try:
        from .ocr_utils import OCRExtractor
        
        ocr = OCRExtractor(lang="chi_sim+eng")
        text_regions = ocr.extract_text_regions_from_frame(frame)
        
        if not text_regions:
            return 0
        
        height = frame.shape[0]
        width = frame.shape[1]
        
        # 定义遮挡区域 (顶部20% + 底部20%)
        top_zone = height * 0.2
        bottom_zone = height * 0.8
        
        overlay_penalty = 0
        
        for region in text_regions:
            y = region['y']
            h = region['h']
            region_area = region['w'] * region['h']
            
            # 只惩罚顶部/底部的文字
            if y < top_zone or (y + h) > bottom_zone:
                area_ratio = region_area / (width * height)
                overlay_penalty += area_ratio * 500
        
        overlay_penalty = min(100, overlay_penalty)
        
        return overlay_penalty
    
    except:
        # 降级到方差检测
        return self._detect_text_overlay_fallback(frame)

def _detect_text_overlay_fallback(
    self,
    frame: np.ndarray
) -> float:
    """降级的遮挡检测"""
    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    height = frame.shape[0]
    
    top_region = gray[:height // 5, :]
    bottom_region = gray[height * 4 // 5:, :]
    
    top_var = np.var(top_region)
    bottom_var = np.var(bottom_region)
    
    penalty = 0
    if top_var > 5000:
        penalty += 20
    if bottom_var > 5000:
        penalty += 20
    
    return penalty
