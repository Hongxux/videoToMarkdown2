"""
模块说明：Module2 CV 运行时配置（精度与 OpenCV 行为）。
执行逻辑：
1) 读取环境变量并解析为稳定配置。
2) 对外提供统一常量，避免各模块重复解析。
实现方式：启动时一次性解析 + 模块级常量。
核心价值：统一精度开关，减少不一致与重复实现。
输入：
- CV_FLOAT_DTYPE: "32" | "64"
输出：
- CV_FLOAT_DTYPE: np.float32 或 np.float64
- CV_FLOAT_DEPTH: cv2.CV_32F 或 cv2.CV_64F
"""

from __future__ import annotations

import os
import cv2
import numpy as np


def _resolve_cv_float_dtype() -> type:
    raw = (os.getenv("CV_FLOAT_DTYPE", "32") or "32").strip()
    if raw == "64":
        return np.float64
    return np.float32


CV_FLOAT_DTYPE = _resolve_cv_float_dtype()
CV_FLOAT_DEPTH = cv2.CV_64F if CV_FLOAT_DTYPE is np.float64 else cv2.CV_32F
