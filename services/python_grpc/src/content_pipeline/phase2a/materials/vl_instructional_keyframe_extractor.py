"""
教程关键帧提取辅助模块。

职责：
1) 统一规范化 VL 返回的 bbox（[ymin, xmin, ymax, xmax]，0-1000）。
2) 按 bbox 对关键帧图片进行原地裁剪，失败时保留原图以确保流程可回退。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import cv2


def normalize_bbox_1000(value: Any) -> Optional[List[int]]:
    """将任意 bbox 输入归一为 [ymin, xmin, ymax, xmax] 的 0-1000 整数坐标。"""
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        return None
    try:
        ymin = int(round(float(value[0])))
        xmin = int(round(float(value[1])))
        ymax = int(round(float(value[2])))
        xmax = int(round(float(value[3])))
    except Exception:
        return None

    ymin = max(0, min(1000, ymin))
    xmin = max(0, min(1000, xmin))
    ymax = max(0, min(1000, ymax))
    xmax = max(0, min(1000, xmax))

    if ymax < ymin:
        ymin, ymax = ymax, ymin
    if xmax < xmin:
        xmin, xmax = xmax, xmin
    return [ymin, xmin, ymax, xmax]


def crop_keyframe_inplace_by_bbox_1000(image_path: Path, bbox_1000: Optional[List[int]]) -> bool:
    """
    按 0-1000 归一化 bbox 对关键帧图片做原地裁剪。

    返回：
    - True: 成功裁剪，或不需要裁剪（bbox 为空）
    - False: 裁剪失败（保留原图）
    """
    if bbox_1000 is None:
        return True
    if not image_path.exists():
        return False

    image = cv2.imread(str(image_path), cv2.IMREAD_UNCHANGED)
    if image is None:
        return False
    if len(image.shape) < 2:
        return False

    height, width = int(image.shape[0]), int(image.shape[1])
    if height <= 0 or width <= 0:
        return False

    ymin, xmin, ymax, xmax = bbox_1000
    y0 = int(round((ymin / 1000.0) * height))
    x0 = int(round((xmin / 1000.0) * width))
    y1 = int(round((ymax / 1000.0) * height))
    x1 = int(round((xmax / 1000.0) * width))

    y0 = max(0, min(height - 1, y0))
    x0 = max(0, min(width - 1, x0))
    y1 = max(0, min(height, y1))
    x1 = max(0, min(width, x1))

    # 保障裁剪区域至少 1 像素，避免 bbox 过窄导致空图。
    if y1 <= y0:
        y1 = min(height, y0 + 1)
    if x1 <= x0:
        x1 = min(width, x0 + 1)
    if y1 <= y0 or x1 <= x0:
        return False

    if y0 == 0 and x0 == 0 and y1 == height and x1 == width:
        return True

    cropped = image[y0:y1, x0:x1]
    if cropped.size == 0:
        return False

    tmp_path = image_path.parent / f"{image_path.stem}.tmp_crop{image_path.suffix}"
    try:
        if not cv2.imwrite(str(tmp_path), cropped):
            return False
        tmp_path.replace(image_path)
        return True
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        return False
