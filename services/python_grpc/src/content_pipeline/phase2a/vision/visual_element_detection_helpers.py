"""
模块说明：Module2 内容增强中的 visual_element_detection_helpers 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import cv2
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import logging
import gc
from services.python_grpc.src.content_pipeline.infra.runtime.cv_runtime_config import CV_FLOAT_DTYPE

# 🚀 Performance: Enable Numba JIT
try:
    from numba import jit
    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False
    # Dummy decorator
    def jit(*args, **kwargs):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - *args: 可变参数，含义由调用方决定。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        def decorator(func):
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            输入参数：
            - func: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            return func
        return decorator

logger = logging.getLogger(__name__)

# --- JIT Optimized Math Helpers ---
@jit(nopython=True, fastmath=True)
def _fast_cosine_angle(v1: np.ndarray, v2: np.ndarray) -> float:
    # Numba np.linalg.norm requires float input
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过NumPy 数值计算实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：norm_prod < 1e-06
    - 条件：cos_angle > 1.0
    - 条件：cos_angle < -1.0
    依据来源（证据链）：
    输入参数：
    - v1: 函数入参（类型：np.ndarray）。
    - v2: 函数入参（类型：np.ndarray）。
    输出参数：
    - 数值型计算结果。"""
    v1_f = v1.astype(CV_FLOAT_DTYPE)
    v2_f = v2.astype(CV_FLOAT_DTYPE)
    
    norm_prod = np.linalg.norm(v1_f) * np.linalg.norm(v2_f)
    if norm_prod < 1e-6:
        return 0.0
    # Use the float vectors for dot product too for consistency
    cos_angle = np.dot(v1_f, v2_f) / norm_prod
    # Clip for safety
    if cos_angle > 1.0: cos_angle = 1.0
    if cos_angle < -1.0: cos_angle = -1.0
    return np.degrees(np.arccos(cos_angle))

@jit(nopython=True, fastmath=True)
def _fast_point_dist(p1: np.ndarray, p2: np.ndarray) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过NumPy 数值计算实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - p1: 函数入参（类型：np.ndarray）。
    - p2: 函数入参（类型：np.ndarray）。
    输出参数：
    - 数值型计算结果。"""
    return np.linalg.norm(p1 - p2)

class VisualElementDetector:
    """类说明：VisualElementDetector 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    @staticmethod
    def detect_rectangles(edges: np.ndarray) -> int:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：area < 100
        - 条件：len(approx) == 4
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        contours, _ = cv2.findContours(
            edges,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE
        )
        
        rectangle_count = 0
        for contour in contours:
            # 轮廓面积过滤 (避免噪声)
            area = cv2.contourArea(contour)
            if area < 100:  # 最小面积阈值
                continue
            
            # 近似多边形
            epsilon = 0.02 * cv2.arcLength(contour, True)
            approx = cv2.approxPolyDP(contour, epsilon, True)
            
            # 4个顶点且接近矩形
            if len(approx) == 4:
                rectangle_count += 1
        
        return rectangle_count
    
    @staticmethod
    def detect_circles(gray: np.ndarray) -> int:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：circles is not None
        依据来源（证据链）：
        输入参数：
        - gray: 函数入参（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        try:
            # 高斯模糊去噪
            blurred = cv2.GaussianBlur(gray, (9, 9), 2)
            
            # 霍夫圆检测
            # 在执行 HoughCircles 前强制清理一次
            gc.collect()
            circles = cv2.HoughCircles(
                blurred,
                cv2.HOUGH_GRADIENT,
                dp=1.2, # Changed dp from 1 to 1.2
                minDist=30,  # Changed minDist from 50 to 30
                param1=50,   # Canny边缘检测高阈值
                param2=30,   # 圆心检测阈值
                minRadius=10,
                maxRadius=100 # Changed maxRadius from 200 to 100
            )
            
            if circles is not None:
                return len(circles[0])
        except Exception as e:
            logger.warning(f"Circle detection failed (likely OOM): {e}") # Modified warning message
        
        return 0
    
    @staticmethod
    def detect_lines(edges: np.ndarray, line_type: str = "all") -> List[Tuple[float, float]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：lines is None
        - 条件：line_type == 'horizontal' and abs(theta - np.pi / 2) > 0.1
        - 条件：line_type == 'vertical' and abs(theta) > 0.1 and (abs(theta - np.pi) > 0.1)
        依据来源（证据链）：
        - 输入参数：line_type。
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        - line_type: 函数入参（类型：str）。
        输出参数：
        - Tuple[float, float] 列表（与输入或处理结果一一对应）。"""
        lines = cv2.HoughLines(edges, rho=1, theta=np.pi / 180, threshold=100)
        if lines is None: return []
        
        unique_lines = []
        for line in lines:
            rho, theta = line[0]
            # 过滤类型
            if line_type == "horizontal" and abs(theta - np.pi/2) > 0.1: continue
            if line_type == "vertical" and abs(theta) > 0.1 and abs(theta - np.pi) > 0.1: continue
            
            # 去重
            is_duplicate = False
            for u_rho, u_theta in unique_lines:
                if abs(rho - u_rho) < 20 and abs(theta - u_theta) < 0.1:
                    is_duplicate = True
                    break
            if not is_duplicate:
                unique_lines.append((rho, theta))
        return unique_lines

    @staticmethod
    def detect_lines_p(edges: np.ndarray, min_length: int = 20, max_gap: int = 5) -> List[Tuple[int, int, int, int]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：lines is None
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        - min_length: 函数入参（类型：int）。
        - max_gap: 函数入参（类型：int）。
        输出参数：
        - Tuple[int, int, int, int] 列表（与输入或处理结果一一对应）。"""
        lines = cv2.HoughLinesP(edges, 1, np.pi/180, threshold=50, minLineLength=min_length, maxLineGap=max_gap)
        if lines is None: return []
        return [tuple(l[0]) for l in lines]

    @staticmethod
    def calculate_gravity_center(contour: np.ndarray) -> Optional[Tuple[float, float]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：m['m00'] == 0
        依据来源（证据链）：
        - 配置字段：m00。
        输入参数：
        - contour: 函数入参（类型：np.ndarray）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
        m = cv2.moments(contour)
        if m["m00"] == 0: return None
        return (m["m10"] / m["m00"], m["m01"] / m["m00"])
    
    @staticmethod
    def detect_arrows(
        edges: np.ndarray,
        gray: np.ndarray,
        lines: Optional[List[Tuple[float, float]]] = None
    ) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：total_arrows > 0
        - 条件：area < 50 or area > 10000
        - 条件：len(approx) == 3
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        - gray: 函数入参（类型：np.ndarray）。
        - lines: 函数入参（类型：Optional[List[Tuple[float, float]]]）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        contours, _ = cv2.findContours(
            edges,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE
        )
        
        arrow_candidates = []
        
        for contour in contours:
            area = cv2.contourArea(contour)
            if area < 50 or area > 10000:  # 箭头头部面积范围
                continue
            
            # 近似为多边形
            epsilon = 0.04 * cv2.arcLength(contour, True)
            approx = cv2.approxPolyDP(contour, epsilon, True)
            
            # 三角形 (箭头头部特征)
            if len(approx) == 3:
                # 计算三角形的方向
                points = approx.reshape(-1, 2)
                direction = VisualElementDetector._classify_arrow_direction(points)
                arrow_candidates.append(direction)
        
        # 统计各方向箭头数量
        direction_counts = {
            "up": 0,
            "down": 0,
            "left": 0,
            "right": 0,
            "diagonal": 0
        }
        
        for direction in arrow_candidates:
            if direction in direction_counts:
                direction_counts[direction] += 1
        
        total_arrows = len(arrow_candidates)
        
        # 计算置信度 (基于检测到的箭头数量和线条一致性)
        confidence = 0.5
        if total_arrows > 0:
            confidence = min(1.0, 0.5 + (total_arrows / 20) * 0.5)
        
        return {
            "total": total_arrows,
            "up": direction_counts["up"],
            "down": direction_counts["down"],
            "left": direction_counts["left"],
            "right": direction_counts["right"],
            "diagonal": direction_counts["diagonal"],
            "confidence": confidence
        }
    
    @staticmethod
    def _classify_arrow_direction(points: np.ndarray) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(points) != 3
        - 条件：abs(dx) < abs(dy) * 0.5
        - 条件：abs(dy) < abs(dx) * 0.5
        依据来源（证据链）：
        - 输入参数：points。
        输入参数：
        - points: 函数入参（类型：np.ndarray）。
        输出参数：
        - 字符串结果。"""
        if len(points) != 3:
            return "diagonal"
        
        # 计算每个角的角度
        angles = []
        for i in range(3):
            p1 = points[i]
            p2 = points[(i + 1) % 3]
            p3 = points[(i + 2) % 3]
            
            # 计算向量夹角
            v1 = p1 - p2
            v2 = p3 - p2
            
            # 🚀 Optimization: Use JIT math
            angle = _fast_cosine_angle(v1, v2)
            angles.append((i, angle))
        
        # 找到最小角度的顶点 (箭头尖端)
        tip_idx = min(angles, key=lambda x: x[1])[0]
        tip_point = points[tip_idx]
        
        # 计算质心
        centroid = np.mean(points, axis=0)
        
        # 相对于质心的方向向量
        direction_vector = tip_point - centroid
        
        # 根据方向向量分类
        dx, dy = direction_vector
        
        # 主方向判断 (角度容差: 45度)
        if abs(dx) < abs(dy) * 0.5:  # 主要是垂直方向
            return "up" if dy < 0 else "down"
        elif abs(dy) < abs(dx) * 0.5:  # 主要是水平方向
            return "right" if dx > 0 else "left"
        else:
            return "diagonal"
    
    @staticmethod
    def detect_connectors(edges: np.ndarray, lines: List[Tuple[float, float]]) -> Dict[str, int]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：density > 0.4
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        - lines: 函数入参（类型：List[Tuple[float, float]]）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        # 简化实现: 基于边缘密度的启发式
        # 寻找高密度交叉区域
        kernel = np.ones((5, 5), np.uint8)
        dilated = cv2.dilate(edges, kernel, iterations=1)
        
        # 检测交叉点 (高密度区域)
        intersection_points = []
        h, w = edges.shape
        grid_size = 20
        
        for y in range(0, h - grid_size, grid_size):
            for x in range(0, w - grid_size, grid_size):
                region = dilated[y:y+grid_size, x:x+grid_size]
                density = np.sum(region > 0) / (grid_size * grid_size)
                
                if density > 0.4:  # 高密度阈值
                    intersection_points.append((x + grid_size//2, y + grid_size//2))
        
        # 简化分类: 将交叉点数量的一半作为T型连接
        total_intersections = len(intersection_points)
        t_junctions = total_intersections // 2
        intersections = total_intersections - t_junctions
        
        return {
            "t_junctions": t_junctions,
            "intersections": intersections
        }
    
    @staticmethod
    def detect_diamonds(edges: np.ndarray) -> int:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：area < 200
        - 条件：len(approx) == 4
        - 条件：0.6 < ratio < 1.0
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        contours, _ = cv2.findContours(
            edges,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE
        )
        
        diamond_count = 0
        for contour in contours:
            area = cv2.contourArea(contour)
            if area < 200:  # 最小面积
                continue
            
            epsilon = 0.02 * cv2.arcLength(contour, True)
            approx = cv2.approxPolyDP(contour, epsilon, True)
            
            # 4个顶点
            if len(approx) == 4:
                # 检查是否为菱形 (对角线近似相等且垂直)
                points = approx.reshape(-1, 2)
                
                # 计算对角线长度
                diag1 = np.linalg.norm(points[0] - points[2])
                diag2 = np.linalg.norm(points[1] - points[3])
                
                # 菱形特征: 对角线长度比例在一定范围内
                ratio = min(diag1, diag2) / (max(diag1, diag2) + 1e-6)
                
                if 0.6 < ratio < 1.0:  # 不是过于扁平的矩形
                    # 检查旋转角度 (菱形通常旋转45度)
                    rect = cv2.minAreaRect(contour)
                    angle = abs(rect[2])
                    
                    if 35 < angle < 55:  # 接近45度旋转
                        diamond_count += 1
        
        return diamond_count
    
    @staticmethod
    def detect_clouds(edges: np.ndarray) -> int:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：area < 500
        - 条件：0.5 < circularity < 0.8 and len(approx) > 6
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        contours, _ = cv2.findContours(
            edges,
            cv2.RETR_EXTERNAL,
            cv2.CHAIN_APPROX_SIMPLE
        )
        
        cloud_count = 0
        for contour in contours:
            area = cv2.contourArea(contour)
            if area < 500:  # 云形状通常较大
                continue
            
            # 云形状特征:
            # 1. 圆度较高但不是完美圆形
            # 2. 轮廓复杂度中等
            
            perimeter = cv2.arcLength(contour, True)
            circularity = 4 * np.pi * area / (perimeter ** 2 + 1e-6)
            
            # 多边形近似
            epsilon = 0.02 * perimeter
            approx = cv2.approxPolyDP(contour, epsilon, True)
            
            # 云形状: 圆度在0.5-0.8之间,顶点数较多(波浪状)
            if 0.5 < circularity < 0.8 and len(approx) > 6:
                cloud_count += 1
        
        return cloud_count

    @staticmethod
    def detect_math_formula(edges: np.ndarray, lines: List[Tuple[float, float]], rect_count: int) -> Dict[str, bool]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：w > 5 * res_factor and h > 5 * res_factor
        - 条件：bw > bh * aspect_ratio_thresh and bw >= frac_min_len and (bh <= frac_max_height) and (bw < w_img * 0.4)
        - 条件：0 < dist < spacing_thresh and has_overlap
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        - lines: 函数入参（类型：List[Tuple[float, float]]）。
        - rect_count: 函数入参（类型：int）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        has_fraction = False
        has_script = False
        
        h_img, w_img = edges.shape
        # 分辨率因子 (基准 1920)
        res_factor = max(0.5, min(2.0, w_img / 1920.0))
        # print(f"DEBUG: Frame {w_img}x{h_img}, res_factor={res_factor}")
        
        # 0. 预计算轮廓与边界框
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        bounding_boxes = []
        for c in contours:
            x, y, w, h = cv2.boundingRect(c)
            # 过滤极小噪点 (绝对长度>=5px)
            if w > 5 * res_factor and h > 5 * res_factor:
                bounding_boxes.append((x, y, w, h, cv2.contourArea(c), c))
        
        # print(f"DEBUG: Found {len(bounding_boxes)} valid bounding boxes")

        # 1. 优化: 分数线检测 (Fraction Line)
        # 动态阈值准备
        frac_min_len = 20 * res_factor
        # 宽度约束修正: 主要是高度要小 (扁平), 且不能太占满全屏(那是表格线/分割线)
        # Relaxed to 12*res to accomodate Canny expansion (3px line -> 5px bbox)
        frac_max_height = max(2, 12 * res_factor) 
        
        # 寻找候选
        for x, y, bw, bh, area, cnt in bounding_boxes:
            # A. 几何特征: 扁长 + 绝对长度达标
            # 动态宽长比: >= 5 + res_factor
            aspect_ratio_thresh = 5.0 + res_factor
            
            # print(f"DEBUG Fract Check: {bw}x{bh}, ratio={bw/bh:.2f} (req {aspect_ratio_thresh:.2f}), max_h={frac_max_height}")
            
            if (bw > bh * aspect_ratio_thresh) and \
               (bw >= frac_min_len) and \
               (bh <= frac_max_height) and \
               (bw < w_img * 0.4): # 宽度约束: <屏幕40% (避免是表格横线)
                
                # B. 方向约束: 旋转角度 < 2度
                rect = cv2.minAreaRect(cnt)
                angle = abs(rect[2])
                if angle > 45: angle = 90 - angle # 归一化到水平
                if angle > 2.0: 
                    # print(f"DEBUG Fract Reject: Angle {angle:.2f}")
                    continue
                
                # C. 上下文约束: 有效内容密度 >= 30%
                # 检查区域: 上下各延伸 2倍自身高度
                check_h = max(10, bh * 2) 
                
                roi_up = edges[max(0, int(y-check_h)):y, x:x+bw]
                roi_down = edges[y+bh:min(h_img, int(y+bh+check_h)), x:x+bw]
                
                up_cnt = np.count_nonzero(roi_up)
                down_cnt = np.count_nonzero(roi_down)
                # print(f"DEBUG Fract Context: up={up_cnt}, down={down_cnt}, req={bw*0.1}")
                
                if up_cnt > bw * 0.1 and down_cnt > bw * 0.1:
                    has_fraction = True
                    # print("DEBUG: Fraction Found!")
                    break

        # 2. 优化: 上下标检测 (Superscript/Subscript)
        # 过滤过大的框 (背景框) & 排序
        # 字符级轮廓: 100 < area < 5000 (scaled)
        # print(f"DEBUG: Char Box Filter Range: {(10 * res_factor)**2} - {(200 * res_factor)**2}")
        char_boxes = [b for b in bounding_boxes if (10 * res_factor)**2 < b[4] < (200 * res_factor)**2]
        char_boxes.sort(key=lambda b: b[0]) # 按 x 排序
        # print(f"DEBUG: {len(char_boxes)} char boxes for script detection")
        
        for i in range(len(char_boxes)-1):
            x1, y1, w1, h1, area1, _ = char_boxes[i]
            x2, y2, w2, h2, area2, _ = char_boxes[i+1]
            
            # 动态距离约束: 间距 < 主字符宽度的 30%
            spacing_thresh = max(w1, w2) * 0.3
            dist = x2 - (x1 + w1) # 水平间距
            
            # 必须水平紧邻 (dist < thresh) 且 垂直有重叠 (排除完全分行的)
            # 垂直无重叠判断: y1+h1 < y2 或 y2+h2 < y1
            has_overlap = not (y1 + h1 < y2 or y2 + h2 < y1)
            
            # print(f"DEBUG Script Pair: Box1({x1},{y1},{w1}x{h1}) Box2({x2},{y2},{w2}x{h2}) Dist={dist} (Thresh {spacing_thresh:.1f}) Overlap={has_overlap}")
            
            if 0 < dist < spacing_thresh and has_overlap:
                # 面积比约束: 主 >= 1.5倍 附, 且 附 <= 50% 主
                big_area, small_area = max(area1, area2), min(area1, area2)
                # 区分主附
                is_right_small = area2 < area1
                
                # print(f"DEBUG Script Area: Big={big_area}, Small={small_area}, Ratio={big_area/small_area:.2f}")

                if big_area > small_area * 1.5 and small_area < big_area * 0.5:
                    # 垂直偏移趋势 (Vertical Offset Trend)
                    # 附相对于主中心的偏移 >= 主高度的 15%
                    cy1 = y1 + h1/2
                    cy2 = y2 + h2/2
                    
                    main_h = h1 if is_right_small else h2
                    main_cy = cy1 if is_right_small else cy2
                    sub_cy = cy2 if is_right_small else cy1
                    
                    offset_ratio = abs(sub_cy - main_cy) / (main_h + 1e-6)
                    # print(f"DEBUG Script Offset: {offset_ratio:.2f} (Req 0.15)")
                    
                    if offset_ratio >= 0.15:
                        has_script = True
                        # print("DEBUG: Script Found!")
                        break
                        
        return {
            "fraction_line_count": 1 if has_fraction else 0,
            "superscript_count": 1 if has_script else 0,
            "matrix_bracket_count": 0, 
            "sqrt_count": 0, 
            "is_formula_likely": has_fraction or has_script
        }
    
    @staticmethod
    def detect_matrix_brackets(edges: np.ndarray, contours: List[np.ndarray]) -> int:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：h < 40 or h < w * 2.5
        - 条件：hull_area == 0
        - 条件：0.2 < solidity < 0.7
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        - contours: 函数入参（类型：List[np.ndarray]）。
        输出参数：
        - 数值型计算结果。"""
        count = 0
        h_img, w_img = edges.shape
        
        for c in contours:
            x, y, w, h = cv2.boundingRect(c)
            
            # 1. 基础尺寸过滤
            # 高度至少 40px (1080p下), 高宽比 > 2.5 (细长)
            if h < 40 or h < w * 2.5:
                continue
                
            # 2. 几何特征: 括号是空心的弯曲形状
            area = cv2.contourArea(c)
            hull = cv2.convexHull(c)
            hull_area = cv2.contourArea(hull)
            
            # Solidity (实心度): 括号的 solidity 通常较低 (0.3 - 0.6)
            # 直线/矩形的 solidity 接近 1.0
            if hull_area == 0: continue
            solidity = area / hull_area
            
            if 0.2 < solidity < 0.7:
                # 3. 形状描述符 (HuMoments) 过滤噪声
                # 这里使用简化版: 填满率 (Extent)
                # 括号的 bounding rect 填满率应该适中 (不像直线那么低, 也不像矩形那么高)
                extent = area / (w * h)
                
                if extent < 0.6:
                    # 4. 垂直连续性检查 (防止是虚线)
                    # 提取 ROI 检查垂直投影
                    mask = np.zeros((h, w), dtype=np.uint8)
                    cv2.drawContours(mask, [c], -1, 255, -1, offset=(-x, -y))
                    proj = np.sum(mask, axis=1) / 255
                    # 至少 80% 的行有像素
                    vertical_coverage = np.count_nonzero(proj) / h
                    
                    if vertical_coverage > 0.8:
                        count += 1
        return count

    @staticmethod
    def detect_sqrt(edges: np.ndarray, contours: List[np.ndarray]) -> int:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：w < 30 or h < 20
        - 条件：3 <= len(approx) <= 8
        - 条件：len(points) >= 3
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        - contours: 函数入参（类型：List[np.ndarray]）。
        输出参数：
        - 数值型计算结果。"""
        count = 0
        for c in contours:
            # 1. 尺寸过滤
            x, y, w, h = cv2.boundingRect(c)
            if w < 30 or h < 20: continue # 太小
            
            # 2. 多边形拟合
            epsilon = 0.03 * cv2.arcLength(c, True)
            approx = cv2.approxPolyDP(c, epsilon, False) # Open contour often better for strokes
            
            # 根号通常是 3-5 个关键点 (提笔处，折点，顶盖末端)
            if 3 <= len(approx) <= 8:
                # 3. 寻找 "V" 型结构 (根号左侧)
                # 遍历顶点寻找锐角
                points = approx.reshape(-1, 2)
                has_check_mark = False
                has_roof = False
                
                # 按 x 坐标排序点 (通常从左到右书写)
                points = points[np.argsort(points[:, 0])]
                
                if len(points) >= 3:
                     # 检查前三个点是否构成 "V" (下-上 趋势)
                     # p0 -> p1 (down), p1 -> p2 (up)
                     # 简化: 寻找最低点 (y最大) 的索引
                     bottom_idx = np.argmax(points[:, 1])
                     
                     if 0 < bottom_idx < len(points)-1:
                         p_prev = points[bottom_idx-1]
                         p_curr = points[bottom_idx]
                         p_next = points[bottom_idx+1]
                         
                         # 计算向量
                         v1 = p_prev - p_curr # 指向左上
                         v2 = p_next - p_curr # 指向右上
                         
                         # 计算夹角
                         # 🚀 Optimization: Use JIT math
                         angle = _fast_cosine_angle(v1, v2)
                         
                         # 根号的折角通常在 30-80 度之间
                         if 20 < angle < 90:
                             has_check_mark = True
                
                # 4. 检查顶盖 (Roof) -> 也就是长横线
                # 检查最右侧的点是否具有较大的 x 和 且 y 接近 p_next 的 y (或更高)
                # 简单检查 bounding box 的宽高比，根号通常宽大于高，或者接近
                if has_check_mark:
                    # 进一步确认顶部有一条长横线
                    # 这里的 approx 可能是闭合的 (Canny结果)，也可能是非闭合
                    # 检查 bounding box 顶部区域的像素占比
                    roi = edges[y:y+max(5, h//5), x+w//3:x+w] # 顶部右侧
                    if np.count_nonzero(roi) > w * 0.3: # 有横线
                        count += 1
                        
        return count

    @staticmethod
    def detect_tables(edges: np.ndarray) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：lines is None
        - 条件：len(h_lines) >= 3 and len(v_lines) >= 2
        - 条件：abs(y1 - y2) < 3
        依据来源（证据链）：
        输入参数：
        - edges: 函数入参（类型：np.ndarray）。
        输出参数：
        - 布尔判断结果。"""
        h_img, w_img = edges.shape
        # 1. 检测直线
        # 使用概率霍夫变换寻找长线段
        lines = cv2.HoughLinesP(edges, 1, np.pi/180, threshold=100, 
                               minLineLength=w_img//10, maxLineGap=10)
        
        if lines is None: return False
        
        h_lines = []
        v_lines = []
        
        for line in lines:
            x1, y1, x2, y2 = line[0]
            # 水平线: y 相近
            if abs(y1 - y2) < 3:
                h_lines.append(line[0])
            # 垂直线: x 相近
            elif abs(x1 - x2) < 3:
                v_lines.append(line[0])
                
        # 2. 判定逻辑: 至少 3 条水平线和 2 条垂直线 (或者反之) 构成的栅格
        if len(h_lines) >= 3 and len(v_lines) >= 2:
            # 进一步验证交叉性 (简单启发: 线条分布跨度)
            h_y = sorted([l[1] for l in h_lines])
            v_x = sorted([l[0] for l in v_lines])
            
            h_span = h_y[-1] - h_y[0]
            v_span = v_x[-1] - v_x[0]
            
            if h_span > 50 and v_span > 50:
                return True
                
        return False
    @staticmethod
    def analyze_frame(frame: np.ndarray) -> Dict[str, any]:
        """在 worker 进程中分析单帧视觉元素，支持 SHM 输入。"""
        import cv2
        import os
        import time

        gc.collect()

        start_time = time.time()
        pid = os.getpid()
        shm = None
        if isinstance(frame, dict) and "shm_name" in frame:
            from multiprocessing import shared_memory
            try:
                shm = shared_memory.SharedMemory(name=frame["shm_name"])
                frame = np.ndarray(frame["shape"], dtype=frame["dtype"], buffer=shm.buf)
            except Exception as e:
                return {"error": f"SHM access failed: {e}", "process_id": pid}

        try:
            if isinstance(frame, (bytes, np.ndarray)) and not isinstance(frame, np.ndarray):
                frame = cv2.imdecode(np.frombuffer(frame, np.uint8), cv2.IMREAD_COLOR)

            if frame is None:
                return {"error": "Invalid frame data", "process_id": pid}

            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            edges = cv2.Canny(gray, 50, 150, apertureSize=3)
            rectangles = VisualElementDetector.detect_rectangles(edges)
            circles = VisualElementDetector.detect_circles(gray)
            lines = VisualElementDetector.detect_lines(edges)
            arrows = VisualElementDetector.detect_arrows(edges, lines, gray)
            connectors = VisualElementDetector.detect_connectors(edges, lines)
            diamonds = VisualElementDetector.detect_diamonds(edges)
            clouds = VisualElementDetector.detect_clouds(edges)
            contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            math_features = VisualElementDetector.detect_math_formula(edges, lines, rectangles)
            matrix_count = VisualElementDetector.detect_matrix_brackets(edges, contours)
            math_features["matrix_bracket_count"] = matrix_count
            math_features["is_formula_likely"] = math_features["is_formula_likely"] or (matrix_count > 0)
            has_table = VisualElementDetector.detect_tables(edges)
            total_elements = rectangles + circles + arrows["total"] + diamonds + clouds
            has_architecture = (rectangles >= 3 or circles >= 2 or arrows["total"] >= 3 or diamonds >= 1)
            has_static_structure = has_architecture or has_table or math_features["is_formula_likely"]
            elapsed = (time.time() - start_time) * 1000

            return {
                "rectangles": rectangles,
                "circles": circles,
                "arrows": arrows,
                "lines": len(lines),
                "connectors": connectors,
                "diamonds": diamonds,
                "clouds": clouds,
                "total": total_elements,
                "has_architecture_elements": has_architecture,
                "has_table": has_table,
                "has_static_visual_structure": has_static_structure,
                "has_math_formula": math_features["is_formula_likely"],
                "math_details": math_features,
                "edge_density": np.sum(edges > 0) / edges.size,
                "rectangle_count": rectangles,
                "process_id": pid,
                "latency_ms": elapsed,
            }
        finally:
            if shm:
                shm.close()

    def detect_structure_roi(self, frame: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not contours
        - 条件：not all_points
        - 条件：cw * ch > min_area
        依据来源（证据链）：
        输入参数：
        - frame: 函数入参（类型：np.ndarray）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
        try:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            edges = cv2.Canny(gray, 50, 150)
            
            # 使用现有方法检测元素，或者简单地基于边缘密度
            # 这里简单起见，使用轮廓外接矩形
            contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if not contours:
                return None
                
            # 过滤微小噪声
            valid_rects = []
            h, w = frame.shape[:2]
            min_area = (h * w) * 0.05 # 至少占屏 5%
            
            all_points = []
            for cnt in contours:
                x, y, cw, ch = cv2.boundingRect(cnt)
                if cw * ch > min_area:
                     all_points.append((x, y))
                     all_points.append((x + cw, y + ch))
            
            if not all_points:
                # Fallback: take all non-trivial contours
                for cnt in contours:
                    if cv2.contourArea(cnt) > 100:
                         x,y,cw,ch = cv2.boundingRect(cnt)
                         all_points.append((x,y))
                         all_points.append((x+cw,y+ch))
            
            if not all_points: return None
            
            all_points = np.array(all_points)
            min_x = np.min(all_points[:, 0])
            min_y = np.min(all_points[:, 1])
            max_x = np.max(all_points[:, 0])
            max_y = np.max(all_points[:, 1])
            
            # Padding
            pad = 20
            min_x = max(0, min_x - pad)
            min_y = max(0, min_y - pad)
            max_x = min(w, max_x + pad)
            max_y = min(h, max_y + pad)
            
            return (min_x, min_y, max_x, max_y)
            
        except Exception as e:
            logger.warning(f"ROI detection failed: {e}")
            return None

    def judge_structure_dynamic(self, frame_sequence: List[np.ndarray], structure_roi: Tuple[int, int, int, int]) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not frame_sequence or not structure_roi
        - 条件：len(roi_frames) < 2
        - 条件：short_term_mean < 0.8 and long_term_mse < 1.5
        依据来源（证据链）：
        - 输入参数：frame_sequence, structure_roi。
        输入参数：
        - frame_sequence: 函数入参（类型：List[np.ndarray]）。
        - structure_roi: 函数入参（类型：Tuple[int, int, int, int]）。
        输出参数：
        - 字符串结果。"""
        if not frame_sequence or not structure_roi:
            return "static"
            
        x1, y1, x2, y2 = structure_roi
        # 1. 提取ROI内的帧序列
        roi_frames = []
        for frame in frame_sequence:
            if frame is None: continue
            roi = frame[y1:y2, x1:x2]
            if roi.size == 0: continue
            # Convert to gray for MSE
            if len(roi.shape) == 3:
                roi = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
            roi_frames.append(roi.astype(CV_FLOAT_DTYPE, copy=False))
            
        if len(roi_frames) < 2: return "static"
        
        # 2. 短期波动 (Short-term Flux)
        short_term_mse = []
        for i in range(len(roi_frames)-1):
            mse = np.mean((roi_frames[i] - roi_frames[i+1]) ** 2)
            short_term_mse.append(mse)
        short_term_mean = np.mean(short_term_mse) if short_term_mse else 0.0
        
        # 3. 长期通量 (Long-term Flux)
        long_term_mse = np.mean((roi_frames[0] - roi_frames[-1]) ** 2)
        
        logger.info(f"[Dynamic Check] ROI ShortMSE: {short_term_mean:.3f}, ROI LongMSE: {long_term_mse:.3f}")

        # 🚀 V6.4 Safety Net: Global Flux Check
        # If ROI is static (e.g. chart doesn't move), check if there is GLOBAL activity (e.g. mouse, writing)
        if short_term_mean < 0.8 and long_term_mse < 1.5:
             # Calculate GLOBAL flux on the 360p or 720p frames
             global_short_flux = []
             for i in range(len(frame_sequence)-1):
                 # Fast MSE on whole frame (downsample to 360p for speed if large)
                 f1 = frame_sequence[i]
                 f2 = frame_sequence[i+1]
                 
                 # Convert to gray for MSE if not already
                 if len(f1.shape) == 3:
                     f1 = cv2.cvtColor(f1, cv2.COLOR_BGR2GRAY)
                 if len(f2.shape) == 3:
                     f2 = cv2.cvtColor(f2, cv2.COLOR_BGR2GRAY)

                 f1_f = f1.astype(CV_FLOAT_DTYPE, copy=False)
                 f2_f = f2.astype(CV_FLOAT_DTYPE, copy=False)
                 mse = np.mean((f1_f - f2_f) ** 2)
                 global_short_flux.append(mse)
             
             avg_global = np.mean(global_short_flux) if global_short_flux else 0.0
             logger.info(f"[Dynamic Check] Global ShortMSE: {avg_global:.3f}")
             
             # Global Threshold: Slightly higher to avoid compression noise (2.0?)
             if avg_global > 2.0:
                 logger.info("✨ [Dynamic Override] Global activity detected despite static ROI.")
                 return "dynamic"

        # Thresholds: Long >= 1.5 (Trend), Short < 0.8 (Stability)
        # Note: Short flux measures jitter. If jitter is high, it might be camera shake?
        # But for screen recording, high short flux means fast animation.
        # We classify as dynamic if changes are significant.
        if long_term_mse >= 1.5:
            return "dynamic"
            
        return "static"
