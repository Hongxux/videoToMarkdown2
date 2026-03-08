from pathlib import Path

import cv2
import numpy as np

from services.python_grpc.src.content_pipeline.phase2a.materials.vl_instructional_keyframe_extractor import (
    _build_top_reason_banner_layout,
    crop_keyframe_inplace_by_grid_range,
    crop_keyframe_inplace_by_bbox_1000,
    expand_bbox_1000,
    normalize_bbox_1000,
    parse_grid_cell_label,
    save_grid_overlay_image,
    save_top_reason_banner_image,
)


def test_normalize_bbox_1000_clamps_and_sorts():
    assert normalize_bbox_1000([1200, -20, 300, 1800]) == [300, 0, 1000, 1000]
    assert normalize_bbox_1000([100, 200, 900, 800]) == [100, 200, 900, 800]
    assert normalize_bbox_1000("bad") is None


def test_expand_bbox_1000_applies_ratio_and_min_border():
    assert expand_bbox_1000([500, 500, 510, 510], expand_ratio=0.15, min_border_span_1000=20) == [480, 480, 530, 530]


def test_expand_bbox_1000_clamps_to_image_bounds():
    assert expand_bbox_1000([0, 0, 100, 100], expand_ratio=0.15, min_border_span_1000=20) == [0, 0, 120, 120]


def test_crop_keyframe_inplace_by_bbox_1000_legacy_mode(tmp_path):
    image_path = Path(tmp_path) / "frame_legacy.png"
    image = np.full((100, 200, 3), 255, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    bbox = [250, 250, 750, 750]
    assert (
        crop_keyframe_inplace_by_bbox_1000(
            image_path,
            bbox,
            halfscreen_crop=False,
            upscale_factor=1.0,
            usm_sigma=0.0,
            usm_amount=0.0,
            draw_bbox_red=False,
        )
        is True
    )

    cropped = cv2.imread(str(image_path), cv2.IMREAD_UNCHANGED)
    assert cropped is not None
    assert int(cropped.shape[0]) == 64
    assert int(cropped.shape[1]) == 130


def test_crop_keyframe_uses_xyxy_axis_order_in_legacy_mode(tmp_path):
    image_path = Path(tmp_path) / "frame_axis.png"
    image = np.full((100, 200, 3), 255, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    bbox = [100, 200, 400, 300]
    assert (
        crop_keyframe_inplace_by_bbox_1000(
            image_path,
            bbox,
            halfscreen_crop=False,
            upscale_factor=1.0,
            usm_sigma=0.0,
            usm_amount=0.0,
            draw_bbox_red=False,
        )
        is True
    )

    cropped = cv2.imread(str(image_path), cv2.IMREAD_UNCHANGED)
    assert cropped is not None
    assert int(cropped.shape[1]) == 78
    assert int(cropped.shape[0]) == 14


def test_crop_keyframe_enhanced_mode_halfscreen_red_box_and_upscale(tmp_path):
    image_path = Path(tmp_path) / "frame_enhanced.png"
    image = np.full((100, 200, 3), 220, dtype=np.uint8)
    cv2.line(image, (0, 50), (199, 50), (40, 40, 40), 1)
    cv2.line(image, (100, 0), (100, 99), (40, 40, 40), 1)
    assert cv2.imwrite(str(image_path), image)

    bbox = [400, 300, 600, 700]
    assert (
        crop_keyframe_inplace_by_bbox_1000(
            image_path,
            bbox,
            expand_ratio=0.15,
            min_border_span_1000=20,
            halfscreen_crop=True,
            upscale_factor=2.0,
            upscale_interpolation="lanczos4",
            usm_sigma=1.2,
            usm_amount=1.0,
            usm_threshold=0,
            draw_bbox_red=True,
        )
        is True
    )

    enhanced = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert enhanced is not None
    assert int(enhanced.shape[0]) >= 140
    assert int(enhanced.shape[1]) >= 280

    red_pixels = np.where(
        (enhanced[:, :, 2] > 220)
        & (enhanced[:, :, 1] < 80)
        & (enhanced[:, :, 0] < 80)
    )
    assert int(red_pixels[0].size) > 0


def test_crop_keyframe_draw_on_original_frame_without_scaling(tmp_path):
    image_path = Path(tmp_path) / "frame_original_draw.png"
    image = np.full((120, 220, 3), 180, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    bbox = [100, 100, 900, 500]
    assert (
        crop_keyframe_inplace_by_bbox_1000(
            image_path,
            bbox,
            expand_ratio=0.15,
            min_border_span_1000=20,
            draw_bbox_red=True,
            draw_on_original_frame=True,
            original_draw_crop_expand_ratio=0.30,
            upscale_factor=1.0,
            usm_sigma=0.0,
            usm_amount=0.0,
        )
        is True
    )

    rendered = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert rendered is not None
    assert int(rendered.shape[0]) == 120
    assert int(rendered.shape[1]) == 220
    red_pixels = np.where(
        (rendered[:, :, 2] > 220)
        & (rendered[:, :, 1] < 80)
        & (rendered[:, :, 0] < 80)
    )
    assert int(red_pixels[0].size) > 0


def test_crop_keyframe_draw_on_original_frame_with_upscale_and_usm(tmp_path):
    image_path = Path(tmp_path) / "frame_original_draw_enhance.png"
    image = np.full((120, 220, 3), 170, dtype=np.uint8)
    cv2.putText(image, "A", (35, 45), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (30, 30, 30), 2, cv2.LINE_AA)
    assert cv2.imwrite(str(image_path), image)

    assert (
        crop_keyframe_inplace_by_bbox_1000(
            image_path,
            [100, 100, 900, 500],
            draw_bbox_red=True,
            draw_on_original_frame=True,
            original_draw_crop_expand_ratio=0.30,
            upscale_factor=2.0,
            upscale_interpolation="lanczos4",
            usm_sigma=1.0,
            usm_amount=1.2,
            usm_threshold=0,
        )
        is True
    )
    rendered = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert rendered is not None
    assert int(rendered.shape[0]) == 120
    assert int(rendered.shape[1]) == 220


def test_crop_keyframe_draw_on_original_frame_skip_post_processing(tmp_path):
    image_path = Path(tmp_path) / "frame_original_draw_skip.png"
    image = np.full((120, 220, 3), 170, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    assert (
        crop_keyframe_inplace_by_bbox_1000(
            image_path,
            [100, 100, 900, 500],
            draw_bbox_red=True,
            draw_on_original_frame=True,
            skip_post_draw_processing=True,
            original_draw_crop_expand_ratio=0.30,
            upscale_factor=3.0,
            usm_sigma=1.0,
            usm_amount=1.6,
        )
        is True
    )
    rendered = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert rendered is not None
    assert int(rendered.shape[0]) == 120
    assert int(rendered.shape[1]) == 220


def test_crop_keyframe_draw_on_original_frame_second_crop_only(tmp_path):
    image_path = Path(tmp_path) / "frame_original_draw_second_crop.png"
    image = np.full((120, 220, 3), 170, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    assert (
        crop_keyframe_inplace_by_bbox_1000(
            image_path,
            [100, 100, 900, 500],
            draw_bbox_red=True,
            draw_on_original_frame=True,
            skip_post_draw_processing=False,
            original_draw_crop_expand_ratio=0.0,
            upscale_factor=1.0,
            usm_sigma=0.0,
            usm_amount=0.0,
        )
        is True
    )
    rendered = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert rendered is not None
    assert int(rendered.shape[0]) < 120
    assert int(rendered.shape[1]) <= 220


def test_crop_keyframe_draw_on_original_frame_second_crop_has_min_expand(tmp_path):
    image_path = Path(tmp_path) / "frame_original_draw_second_crop_min_expand.png"
    image = np.full((100, 100, 3), 170, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    assert (
        crop_keyframe_inplace_by_bbox_1000(
            image_path,
            [500, 500, 510, 510],
            draw_bbox_red=True,
            draw_on_original_frame=True,
            skip_post_draw_processing=False,
            original_draw_crop_expand_ratio=0.0,
            original_draw_crop_min_border_span_1000=20,
            upscale_factor=1.0,
            usm_sigma=0.0,
            usm_amount=0.0,
        )
        is True
    )
    rendered = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert rendered is not None
    assert int(rendered.shape[1]) >= 9
    assert int(rendered.shape[0]) >= 9


def test_parse_grid_cell_label_supports_letter_number_format():
    assert parse_grid_cell_label("C4") == (3, 4)
    assert parse_grid_cell_label("t20") == (20, 20)
    assert parse_grid_cell_label("bad") is None


def test_save_grid_overlay_image_and_crop_by_grid_range(tmp_path):
    image_path = Path(tmp_path) / "frame_grid.png"
    overlay_path = Path(tmp_path) / "frame_grid_overlay.png"
    image = np.full((200, 300, 3), 180, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    assert save_grid_overlay_image(
        source_image_path=image_path,
        output_image_path=overlay_path,
        grid_rows=20,
        grid_cols=20,
        alpha=0.4,
        line_thickness=1,
        variant="full",
    )
    assert overlay_path.exists()

    meta = crop_keyframe_inplace_by_grid_range(
        image_path=image_path,
        grid_start="C4",
        grid_end="E7",
        grid_rows=20,
        grid_cols=20,
        expand_ratio=0.15,
        min_border_px=6,
    )
    assert meta is not None
    assert int(meta["crop_x1"]) > int(meta["crop_x0"])
    assert int(meta["crop_y1"]) > int(meta["crop_y0"])

    cropped = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert cropped is not None
    assert int(cropped.shape[0]) < 200
    assert int(cropped.shape[1]) < 300


def test_save_top_reason_banner_image_renders_top_overlay(tmp_path):
    image_path = Path(tmp_path) / "frame_banner_input.jpg"
    output_path = Path(tmp_path) / "frame_banner_output.jpg"
    image = np.full((1080, 1920, 3), 235, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    assert save_top_reason_banner_image(
        source_image_path=image_path,
        output_image_path=output_path,
        text="大家请看画面左侧的题目描述部分，这里明确提出了需要实现一个支持 push、pop、top 和 getMin 操作的栈。",
    )
    assert output_path.exists()

    rendered = cv2.imread(str(output_path), cv2.IMREAD_COLOR)
    assert rendered is not None
    assert int(rendered.shape[0]) == 1080
    assert int(rendered.shape[1]) == 1920

    top_sample = rendered[55:135, 120:1800]
    assert top_sample.size > 0
    assert int(np.mean(top_sample)) < 235


def test_save_top_reason_banner_image_supports_same_path_output(tmp_path):
    image_path = Path(tmp_path) / "frame_banner_same_path.jpg"
    image = np.full((720, 1280, 3), 228, dtype=np.uint8)
    assert cv2.imwrite(str(image_path), image)

    assert save_top_reason_banner_image(
        source_image_path=image_path,
        output_image_path=image_path,
        text="请关注这里的关键状态变化。",
    )

    rendered = cv2.imread(str(image_path), cv2.IMREAD_COLOR)
    assert rendered is not None
    top_sample = rendered[30:120, 80:1200]
    assert top_sample.size > 0
    assert int(np.mean(top_sample)) < 228


def test_build_top_reason_banner_layout_uses_height_div_40_formula():
    hd_layout = _build_top_reason_banner_layout(image_width=1920, image_height=1080)
    qhd_layout = _build_top_reason_banner_layout(image_width=2560, image_height=1440)
    uhd_layout = _build_top_reason_banner_layout(image_width=3840, image_height=2160)

    assert hd_layout["font_size"] == 27
    assert hd_layout["top_margin"] == 54
    assert hd_layout["padding_y"] == 22
    assert hd_layout["padding_x"] == 38

    assert qhd_layout["font_size"] == 36
    assert uhd_layout["font_size"] == 54
