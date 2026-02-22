"""
Tutorial keyframe bbox normalization and enhanced crop utilities.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import cv2
import numpy as np


def normalize_bbox_1000(value: Any) -> Optional[List[int]]:
    """Normalize bbox input into [xmin, ymin, xmax, ymax] in 0-1000 coordinates."""
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        return None
    try:
        xmin = int(round(float(value[0])))
        ymin = int(round(float(value[1])))
        xmax = int(round(float(value[2])))
        ymax = int(round(float(value[3])))
    except Exception:
        return None

    xmin = max(0, min(1000, xmin))
    ymin = max(0, min(1000, ymin))
    xmax = max(0, min(1000, xmax))
    ymax = max(0, min(1000, ymax))

    if xmax < xmin:
        xmin, xmax = xmax, xmin
    if ymax < ymin:
        ymin, ymax = ymax, ymin
    return [xmin, ymin, xmax, ymax]


def expand_bbox_1000(
    bbox_1000: Optional[List[int]],
    *,
    expand_ratio: float = 0.15,
    min_border_span_1000: int = 20,
) -> Optional[List[int]]:
    """
    Expand normalized bbox outward in 0-1000 coordinates.

    Rules:
    1) Expand by ratio of bbox width/height.
    2) Keep minimum expansion per side.
    3) Clamp final bbox to [0, 1000].
    """
    bbox = normalize_bbox_1000(bbox_1000)
    if bbox is None:
        return None
    xmin, ymin, xmax, ymax = bbox

    width = max(1, xmax - xmin)
    height = max(1, ymax - ymin)

    ratio = max(0.0, float(expand_ratio))
    min_pad = max(0, int(min_border_span_1000))
    pad_x = max(int(round(width * ratio)), min_pad)
    pad_y = max(int(round(height * ratio)), min_pad)

    expanded_xmin = max(0, xmin - pad_x)
    expanded_ymin = max(0, ymin - pad_y)
    expanded_xmax = min(1000, xmax + pad_x)
    expanded_ymax = min(1000, ymax + pad_y)

    if expanded_xmax < expanded_xmin:
        expanded_xmin, expanded_xmax = expanded_xmax, expanded_xmin
    if expanded_ymax < expanded_ymin:
        expanded_ymin, expanded_ymax = expanded_ymax, expanded_ymin
    return [expanded_xmin, expanded_ymin, expanded_xmax, expanded_ymax]


def _resolve_interpolation(name: str) -> int:
    mapping = {
        "lanczos4": cv2.INTER_LANCZOS4,
        "lanczos": cv2.INTER_LANCZOS4,
        "cubic": cv2.INTER_CUBIC,
        "linear": cv2.INTER_LINEAR,
        "area": cv2.INTER_AREA,
        "nearest": cv2.INTER_NEAREST,
    }
    key = str(name or "").strip().lower()
    return mapping.get(key, cv2.INTER_LANCZOS4)


def _to_bgr(image: np.ndarray) -> np.ndarray:
    if image is None or image.size == 0:
        return image
    if len(image.shape) == 2:
        return cv2.cvtColor(image, cv2.COLOR_GRAY2BGR)
    if len(image.shape) == 3 and int(image.shape[2]) == 4:
        return cv2.cvtColor(image, cv2.COLOR_BGRA2BGR)
    return image


def _apply_unsharp_mask(
    image: np.ndarray,
    *,
    sigma: float,
    amount: float,
    threshold: int = 0,
) -> np.ndarray:
    if image is None or image.size == 0:
        return image
    if sigma <= 0.0 or amount <= 0.0:
        return image

    blurred = cv2.GaussianBlur(image, ksize=(0, 0), sigmaX=float(sigma), sigmaY=float(sigma))
    sharpened = cv2.addWeighted(image, 1.0 + float(amount), blurred, -float(amount), 0)
    if threshold <= 0:
        return sharpened

    diff = cv2.absdiff(image, blurred)
    low_contrast_mask = diff < int(threshold)
    merged = np.where(low_contrast_mask, image, sharpened)
    return merged.astype(image.dtype, copy=False)


def _bbox_1000_to_pixel_box(
    *,
    bbox_1000: List[int],
    width: int,
    height: int,
) -> Tuple[int, int, int, int]:
    xmin, ymin, xmax, ymax = bbox_1000
    x0 = int(round((xmin / 1000.0) * width))
    y0 = int(round((ymin / 1000.0) * height))
    x1 = int(round((xmax / 1000.0) * width))
    y1 = int(round((ymax / 1000.0) * height))

    x0 = max(0, min(width - 1, x0))
    y0 = max(0, min(height - 1, y0))
    x1 = max(0, min(width, x1))
    y1 = max(0, min(height, y1))

    if x1 <= x0:
        x1 = min(width, x0 + 1)
    if y1 <= y0:
        y1 = min(height, y0 + 1)
    return x0, y0, x1, y1


def _compute_halfscreen_crop_region(
    *,
    image_width: int,
    image_height: int,
    bbox_pixels: Tuple[int, int, int, int],
) -> Tuple[int, int, int, int]:
    x0, y0, x1, y1 = bbox_pixels
    bbox_w = max(1, x1 - x0)
    bbox_h = max(1, y1 - y0)

    half_scale = 0.5 ** 0.5
    crop_w = int(round(image_width * half_scale))
    crop_h = int(round(image_height * half_scale))
    crop_w = max(crop_w, bbox_w + 2)
    crop_h = max(crop_h, bbox_h + 2)
    crop_w = max(1, min(image_width, crop_w))
    crop_h = max(1, min(image_height, crop_h))

    center_x = (x0 + x1) / 2.0
    center_y = (y0 + y1) / 2.0
    crop_x0 = int(round(center_x - crop_w / 2.0))
    crop_y0 = int(round(center_y - crop_h / 2.0))

    if crop_x0 > x0:
        crop_x0 = x0
    if crop_x0 + crop_w < x1:
        crop_x0 = x1 - crop_w
    if crop_y0 > y0:
        crop_y0 = y0
    if crop_y0 + crop_h < y1:
        crop_y0 = y1 - crop_h

    crop_x0 = max(0, min(image_width - crop_w, crop_x0))
    crop_y0 = max(0, min(image_height - crop_h, crop_y0))
    crop_x1 = crop_x0 + crop_w
    crop_y1 = crop_y0 + crop_h
    return crop_x0, crop_y0, crop_x1, crop_y1


def crop_keyframe_inplace_by_bbox_1000(
    image_path: Path,
    bbox_1000: Optional[List[int]],
    *,
    expand_ratio: float = 0.15,
    min_border_span_1000: int = 20,
    halfscreen_crop: bool = True,
    upscale_factor: float = 2.0,
    upscale_interpolation: str = "lanczos4",
    usm_sigma: float = 1.2,
    usm_amount: float = 1.0,
    usm_threshold: int = 0,
    draw_bbox_red: bool = True,
    draw_bbox_use_expanded: bool = False,
    draw_on_original_frame: bool = False,
    original_draw_crop_expand_ratio: float = 0.30,
    original_draw_crop_min_border_span_1000: int = 20,
    red_box_thickness_ratio: float = 0.01,
    skip_post_draw_processing: bool = True,
) -> bool:
    """
    Crop keyframe by bbox and apply readability enhancements.

    Return:
    - True: processed successfully, or no bbox supplied.
    - False: processing failed.
    """
    if bbox_1000 is None:
        return True
    if not image_path.exists():
        return False

    image = cv2.imread(str(image_path), cv2.IMREAD_UNCHANGED)
    if image is None or len(image.shape) < 2:
        return False

    image = _to_bgr(image)
    height, width = int(image.shape[0]), int(image.shape[1])
    if height <= 0 or width <= 0:
        return False

    normalized_bbox = normalize_bbox_1000(bbox_1000)
    if normalized_bbox is None:
        return True
    expanded_bbox = expand_bbox_1000(
        bbox_1000,
        expand_ratio=expand_ratio,
        min_border_span_1000=min_border_span_1000,
    )
    if expanded_bbox is None:
        return True

    raw_bbox_pixels = _bbox_1000_to_pixel_box(
        bbox_1000=normalized_bbox,
        width=width,
        height=height,
    )
    bbox_pixels = _bbox_1000_to_pixel_box(
        bbox_1000=expanded_bbox,
        width=width,
        height=height,
    )

    def _save_image_inplace(image_to_save: np.ndarray) -> bool:
        tmp_path = image_path.parent / f"{image_path.stem}.tmp_crop{image_path.suffix}"
        write_params: List[int] = []
        suffix = image_path.suffix.lower()
        if suffix in {".jpg", ".jpeg"}:
            write_params = [cv2.IMWRITE_JPEG_QUALITY, 95, cv2.IMWRITE_JPEG_OPTIMIZE, 1]
        elif suffix == ".png":
            write_params = [cv2.IMWRITE_PNG_COMPRESSION, 1]
        try:
            if write_params:
                ok = cv2.imwrite(str(tmp_path), image_to_save, write_params)
            else:
                ok = cv2.imwrite(str(tmp_path), image_to_save)
            if not ok:
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

    if bool(draw_on_original_frame):
        rendered = image.copy()
        bx0, by0, bx1, by1 = bbox_pixels
        if bool(draw_bbox_red) and bx1 > bx0 and by1 > by0:
            thickness = max(3, int(round(min(height, width) * max(0.0, float(red_box_thickness_ratio)))))
            cv2.rectangle(
                rendered,
                (bx0, by0),
                (max(bx0, bx1 - 1), max(by0, by1 - 1)),
                (0, 0, 255),
                thickness=thickness,
                lineType=cv2.LINE_AA,
            )

        # v5 默认语义：仅原图画框，不做后处理。
        if bool(skip_post_draw_processing):
            return _save_image_inplace(rendered)
        if bx1 <= bx0 or by1 <= by0:
            return _save_image_inplace(rendered)

        crop_expand_ratio = max(0.0, float(original_draw_crop_expand_ratio))
        box_w = max(1, bx1 - bx0)
        box_h = max(1, by1 - by0)
        min_crop_border = max(0, int(original_draw_crop_min_border_span_1000))
        min_pad_x = int(round(width * (min_crop_border / 1000.0)))
        min_pad_y = int(round(height * (min_crop_border / 1000.0)))
        pad_x = max(int(round(box_w * crop_expand_ratio)), min_pad_x)
        pad_y = max(int(round(box_h * crop_expand_ratio)), min_pad_y)
        crop_x0 = max(0, bx0 - pad_x)
        crop_y0 = max(0, by0 - pad_y)
        crop_x1 = min(width, bx1 + pad_x)
        crop_y1 = min(height, by1 + pad_y)
        if crop_x1 <= crop_x0:
            crop_x1 = min(width, crop_x0 + 1)
        if crop_y1 <= crop_y0:
            crop_y1 = min(height, crop_y0 + 1)
        cropped = rendered[crop_y0:crop_y1, crop_x0:crop_x1]
        if cropped is None or cropped.size == 0:
            return _save_image_inplace(rendered)

        factor = max(1.0, float(upscale_factor))
        if factor > 1.0:
            target_w = max(1, int(round(cropped.shape[1] * factor)))
            target_h = max(1, int(round(cropped.shape[0] * factor)))
            cropped = cv2.resize(
                cropped,
                (target_w, target_h),
                interpolation=_resolve_interpolation(upscale_interpolation),
            )
        cropped = _apply_unsharp_mask(
            cropped,
            sigma=float(usm_sigma),
            amount=float(usm_amount),
            threshold=max(0, int(usm_threshold)),
        )
        return _save_image_inplace(cropped)
    if halfscreen_crop:
        crop_x0, crop_y0, crop_x1, crop_y1 = _compute_halfscreen_crop_region(
            image_width=width,
            image_height=height,
            bbox_pixels=bbox_pixels,
        )
    else:
        crop_x0, crop_y0, crop_x1, crop_y1 = bbox_pixels

    cropped = image[crop_y0:crop_y1, crop_x0:crop_x1]
    if cropped is None or cropped.size == 0:
        return False

    # 先把原图坐标系下的 bbox 转成当前裁剪图坐标；后续再按放大倍数映射到最终输出图坐标。
    red_bbox_pixels = bbox_pixels if bool(draw_bbox_use_expanded) else raw_bbox_pixels
    bx0, by0, bx1, by1 = red_bbox_pixels
    local_x0 = max(0, min(cropped.shape[1] - 1, bx0 - crop_x0))
    local_y0 = max(0, min(cropped.shape[0] - 1, by0 - crop_y0))
    local_x1 = max(0, min(cropped.shape[1], bx1 - crop_x0))
    local_y1 = max(0, min(cropped.shape[0], by1 - crop_y0))

    factor = max(1.0, float(upscale_factor))
    if factor > 1.0:
        target_w = max(1, int(round(cropped.shape[1] * factor)))
        target_h = max(1, int(round(cropped.shape[0] * factor)))
        cropped = cv2.resize(
            cropped,
            (target_w, target_h),
            interpolation=_resolve_interpolation(upscale_interpolation),
        )
    scale_x = float(cropped.shape[1]) / max(1.0, float(crop_x1 - crop_x0))
    scale_y = float(cropped.shape[0]) / max(1.0, float(crop_y1 - crop_y0))

    cropped = _apply_unsharp_mask(
        cropped,
        sigma=float(usm_sigma),
        amount=float(usm_amount),
        threshold=max(0, int(usm_threshold)),
    )

    if draw_bbox_red and local_x1 > local_x0 and local_y1 > local_y0:
        rx0 = int(round(local_x0 * scale_x))
        ry0 = int(round(local_y0 * scale_y))
        rx1 = int(round(local_x1 * scale_x))
        ry1 = int(round(local_y1 * scale_y))
        rx0 = max(0, min(cropped.shape[1] - 1, rx0))
        ry0 = max(0, min(cropped.shape[0] - 1, ry0))
        rx1 = max(0, min(cropped.shape[1], rx1))
        ry1 = max(0, min(cropped.shape[0], ry1))
        if rx1 > rx0 and ry1 > ry0:
            thickness = max(3, int(round(min(cropped.shape[0], cropped.shape[1]) * 0.012)))
            cv2.rectangle(
                cropped,
                (rx0, ry0),
                (max(rx0, rx1 - 1), max(ry0, ry1 - 1)),
                (0, 0, 255),
                thickness=thickness,
                lineType=cv2.LINE_AA,
            )

    return _save_image_inplace(cropped)


def save_grid_overlay_image(
    *,
    source_image_path: Path,
    output_image_path: Path,
    grid_rows: int = 20,
    grid_cols: int = 20,
    alpha: float = 0.38,
    line_thickness: int = 1,
    variant: str = "full",
) -> bool:
    if not source_image_path.exists():
        return False

    image = cv2.imread(str(source_image_path), cv2.IMREAD_UNCHANGED)
    if image is None or len(image.shape) < 2:
        return False
    image = _to_bgr(image)
    h, w = int(image.shape[0]), int(image.shape[1])
    if h <= 0 or w <= 0:
        return False

    rows = max(2, int(grid_rows))
    cols = max(2, int(grid_cols))
    overlay_outline = np.zeros_like(image)
    overlay_main = np.zeros_like(image)

    alpha_value = max(0.0, min(1.0, float(alpha)))
    thickness = max(1, min(2, int(line_thickness)))
    outline_thickness = thickness + 2
    bright_color = (0, 255, 255)
    outline_color = (0, 0, 0)

    variant_text = str(variant or "full").strip().lower()
    use_edge_ticks = variant_text in {"edge", "edge_ticks", "ticks"}
    use_crosshair = variant_text in {"cross", "crosshair", "center"}

    def _draw_line(x1: int, y1: int, x2: int, y2: int) -> None:
        cv2.line(
            overlay_outline,
            (x1, y1),
            (x2, y2),
            outline_color,
            thickness=outline_thickness,
            lineType=cv2.LINE_AA,
        )
        cv2.line(
            overlay_main,
            (x1, y1),
            (x2, y2),
            bright_color,
            thickness=thickness,
            lineType=cv2.LINE_AA,
        )

    cell_h = float(h) / float(rows)
    cell_w = float(w) / float(cols)

    if use_crosshair:
        cx = int(round(w / 2.0))
        cy = int(round(h / 2.0))
        _draw_line(cx, 0, cx, h - 1)
        _draw_line(0, cy, w - 1, cy)
    elif use_edge_ticks:
        tick_len = max(8, int(round(min(h, w) * 0.02)))
        for col in range(cols + 1):
            x = int(round(col * cell_w))
            x = max(0, min(w - 1, x))
            _draw_line(x, 0, x, min(h - 1, tick_len))
            _draw_line(x, max(0, h - 1 - tick_len), x, h - 1)
        for row in range(rows + 1):
            y = int(round(row * cell_h))
            y = max(0, min(h - 1, y))
            _draw_line(0, y, min(w - 1, tick_len), y)
            _draw_line(max(0, w - 1 - tick_len), y, w - 1, y)
    else:
        for col in range(cols + 1):
            x = int(round(col * cell_w))
            x = max(0, min(w - 1, x))
            _draw_line(x, 0, x, h - 1)
        for row in range(rows + 1):
            y = int(round(row * cell_h))
            y = max(0, min(h - 1, y))
            _draw_line(0, y, w - 1, y)

    rendered = cv2.addWeighted(image, 1.0, overlay_outline, alpha_value * 0.55, 0.0)
    rendered = cv2.addWeighted(rendered, 1.0, overlay_main, alpha_value, 0.0)

    output_image_path.parent.mkdir(parents=True, exist_ok=True)
    return bool(cv2.imwrite(str(output_image_path), rendered))


def _column_label_to_index(label: str) -> Optional[int]:
    text = str(label or "").strip().upper()
    if not text or not text.isalpha():
        return None
    result = 0
    for char in text:
        value = ord(char) - ord("A") + 1
        if value < 1 or value > 26:
            return None
        result = result * 26 + value
    return result


def parse_grid_cell_label(cell: Any) -> Optional[Tuple[int, int]]:
    text = str(cell or "").strip().upper()
    if not text:
        return None
    matched = re.match(r"^([A-Z]{1,3})(\d{1,3})$", text)
    if not matched:
        return None
    row_label = matched.group(1)
    col_label = matched.group(2)
    row_index = _column_label_to_index(row_label)
    if row_index is None:
        return None
    try:
        col_index = int(col_label)
    except Exception:
        return None
    if col_index <= 0:
        return None
    return row_index, col_index


def crop_keyframe_inplace_by_grid_range(
    *,
    image_path: Path,
    grid_start: Any,
    grid_end: Any,
    grid_rows: int = 20,
    grid_cols: int = 20,
    expand_ratio: float = 0.15,
    min_border_px: int = 12,
) -> Optional[Dict[str, int]]:
    if not image_path.exists():
        return None

    start_cell = parse_grid_cell_label(grid_start)
    end_cell = parse_grid_cell_label(grid_end)
    if start_cell is None or end_cell is None:
        return None

    image = cv2.imread(str(image_path), cv2.IMREAD_UNCHANGED)
    if image is None or len(image.shape) < 2:
        return None
    image = _to_bgr(image)
    h, w = int(image.shape[0]), int(image.shape[1])
    if h <= 0 or w <= 0:
        return None

    rows = max(2, int(grid_rows))
    cols = max(2, int(grid_cols))

    row1 = max(1, min(rows, int(start_cell[0])))
    col1 = max(1, min(cols, int(start_cell[1])))
    row2 = max(1, min(rows, int(end_cell[0])))
    col2 = max(1, min(cols, int(end_cell[1])))

    top_row = min(row1, row2)
    bottom_row = max(row1, row2)
    left_col = min(col1, col2)
    right_col = max(col1, col2)

    cell_h = float(h) / float(rows)
    cell_w = float(w) / float(cols)

    raw_x0 = int(np.floor((left_col - 1) * cell_w))
    raw_y0 = int(np.floor((top_row - 1) * cell_h))
    raw_x1 = int(np.ceil(right_col * cell_w))
    raw_y1 = int(np.ceil(bottom_row * cell_h))

    raw_x0 = max(0, min(w - 1, raw_x0))
    raw_y0 = max(0, min(h - 1, raw_y0))
    raw_x1 = max(raw_x0 + 1, min(w, raw_x1))
    raw_y1 = max(raw_y0 + 1, min(h, raw_y1))

    box_w = max(1, raw_x1 - raw_x0)
    box_h = max(1, raw_y1 - raw_y0)
    ratio = max(0.0, float(expand_ratio))
    min_border = max(0, int(min_border_px))
    pad_x = max(int(round(box_w * ratio)), min_border)
    pad_y = max(int(round(box_h * ratio)), min_border)

    crop_x0 = max(0, raw_x0 - pad_x)
    crop_y0 = max(0, raw_y0 - pad_y)
    crop_x1 = min(w, raw_x1 + pad_x)
    crop_y1 = min(h, raw_y1 + pad_y)
    if crop_x1 <= crop_x0:
        crop_x1 = min(w, crop_x0 + 1)
    if crop_y1 <= crop_y0:
        crop_y1 = min(h, crop_y0 + 1)

    cropped = image[crop_y0:crop_y1, crop_x0:crop_x1]
    if cropped is None or cropped.size == 0:
        return None
    if not cv2.imwrite(str(image_path), cropped):
        return None

    return {
        "raw_x0": int(raw_x0),
        "raw_y0": int(raw_y0),
        "raw_x1": int(raw_x1),
        "raw_y1": int(raw_y1),
        "crop_x0": int(crop_x0),
        "crop_y0": int(crop_y0),
        "crop_x1": int(crop_x1),
        "crop_y1": int(crop_y1),
    }
