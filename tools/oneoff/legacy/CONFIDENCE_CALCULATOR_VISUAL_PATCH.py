"""
模块说明：CONFIDENCE_CALCULATOR_VISUAL_PATCH 相关能力的封装。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

def _calculate_visual_element_score(
    self,
    screenshot_path: str
) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：image is None
    - 条件：total_elements == 0
    - 条件：total_elements <= 5
    依据来源（证据链）：
    输入参数：
    - screenshot_path: 文件路径（类型：str）。
    输出参数：
    - 数值型计算结果。"""
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
        from services.python_grpc.src.content_pipeline.phase2a.vision.visual_feature_extractor import VisualFeatureExtractor
        
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
