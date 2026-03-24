"""
模块说明：Module2 内容增强中的 fast_metrics 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import numpy as np

# =============================================================================
# Vectorized CV Metrics (Pure Numpy, No Loops)
# =============================================================================

def fast_mse(arr1: np.ndarray, arr2: np.ndarray) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过NumPy 数值计算实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - arr1: 函数入参（类型：np.ndarray）。
    - arr2: 函数入参（类型：np.ndarray）。
    输出参数：
    - 数值型计算结果。"""
    diff = arr1.astype(np.float32) - arr2.astype(np.float32)
    return float(np.mean(diff * diff))

def fast_ssim(arr1: np.ndarray, arr2: np.ndarray, c1=6.5025, c2=58.5225) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过NumPy 数值计算实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - arr1: 函数入参（类型：np.ndarray）。
    - arr2: 函数入参（类型：np.ndarray）。
    - c1: 函数入参（类型：未标注）。
    - c2: 函数入参（类型：未标注）。
    输出参数：
    - 数值型计算结果。"""
    im1 = arr1.astype(np.float32)
    im2 = arr2.astype(np.float32)
    
    mu1 = np.mean(im1)
    mu2 = np.mean(im2)
    
    # 优化: 向量化计算方差和协方差
    # var = mean(x^2) - mean(x)^2
    var1 = np.var(im1)
    var2 = np.var(im2)
    cov12 = np.mean((im1 - mu1) * (im2 - mu2))
    
    numerator = (2 * mu1 * mu2 + c1) * (2 * cov12 + c2)
    denominator = (mu1**2 + mu2**2 + c1) * (var1 + var2 + c2)
    
    return float(numerator / denominator)


def fast_diff_ratio(arr1: np.ndarray, arr2: np.ndarray, threshold=30) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过NumPy 数值计算实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - arr1: 函数入参（类型：np.ndarray）。
    - arr2: 函数入参（类型：np.ndarray）。
    - threshold: 阈值（类型：未标注）。
    输出参数：
    - 数值型计算结果。"""
    diff = np.abs(arr1.astype(np.float32) - arr2.astype(np.float32))
    return float(np.mean(diff > threshold))
