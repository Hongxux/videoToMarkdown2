"""
Fast CV Metrics using pure vectorized NumPy (C-level speed, no GIL loops).
Memory optimized: Uses float32 to reduce memory footprint.
"""

import numpy as np

# =============================================================================
# Vectorized CV Metrics (Pure Numpy, No Loops)
# =============================================================================

def fast_ssim(arr1: np.ndarray, arr2: np.ndarray, c1=6.5025, c2=58.5225) -> float:
    """
    Vectorized SSIM (Simplified)
    """
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
    Vectorized Diff Ratio
    Returns percentage of pixels with difference > threshold
    """
    diff = np.abs(arr1.astype(np.float32) - arr2.astype(np.float32))
    return float(np.mean(diff > threshold))
