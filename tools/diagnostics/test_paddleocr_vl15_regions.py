#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
PaddleOCR-VL-1.5 功能测试（带 PPStructure 兼容回退）。

输出字段：
- 区域类型
- 坐标
- 识别文本
- semantic_tag
- layout_relation
- multimodal_score
- format_preserved_text
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_IMAGE = PROJECT_ROOT / 'var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg'

_DETECTION_SCORE_THRESHOLD = 0.2
_IMAGE_LABELS: set[str] = {
    'header_image',
    'figure',
    'image',
    'stamp',
    'seal',
    'chart',
    'diagram',
    'photo',
    'picture',
    'img',
}
_TEXT_LABELS: set[str] = {'text', 'title', 'paragraph', 'formula', 'table'}


def _to_builtin(obj: Any) -> Any:
    try:
        import numpy as np  # type: ignore
    except Exception:
        np = None  # type: ignore

    if np is not None and isinstance(obj, (np.generic,)):
        return obj.item()
    if np is not None and isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (list, tuple)):
        return [_to_builtin(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _to_builtin(v) for k, v in obj.items()}
    return obj


def _extract_text(region: dict[str, Any]) -> str:
    if region.get('type') == 'ignore':
        return ''

    res = region.get('res')
    if res is None:
        return ''

    if isinstance(res, dict):
        text = res.get('text')
        if isinstance(text, str):
            return text
        if isinstance(text, list):
            parts = [str(x) for x in text if str(x).strip()]
            return '\n'.join(parts)
        if 'html' in res and isinstance(res.get('html'), str):
            return res['html']

    if isinstance(res, list):
        parts: list[str] = []
        for item in res:
            if isinstance(item, dict):
                t = item.get('text')
                if isinstance(t, str) and t.strip():
                    parts.append(t.strip())
                continue
            if isinstance(item, (list, tuple)):
                if len(item) >= 2 and isinstance(item[1], (list, tuple)) and item[1]:
                    t = item[1][0]
                    if isinstance(t, str) and t.strip():
                        parts.append(t.strip())
                        continue
                if len(item) >= 2 and isinstance(item[1], str) and item[1].strip():
                    parts.append(item[1].strip())
                    continue
                if len(item) >= 1 and isinstance(item[0], str) and item[0].strip():
                    parts.append(item[0].strip())
                    continue
            if isinstance(item, str) and item.strip():
                parts.append(item.strip())
        return '\n'.join(parts)

    if isinstance(res, str):
        return res.strip()

    return str(res)


def _extract_semantic_tag(region: dict[str, Any]) -> Any:
    for key in ('semantic_tag', 'semanticTag', 'sem_tag', 'tag'):
        if key in region:
            return _to_builtin(region.get(key))
    res = region.get('res')
    if isinstance(res, dict):
        for key in ('semantic_tag', 'semanticTag', 'sem_tag', 'tag'):
            if key in res:
                return _to_builtin(res.get(key))
    return None


def _extract_layout_relation(region: dict[str, Any]) -> Any:
    for key in ('layout_relation', 'layoutRelation', 'relation', 'relations'):
        if key in region:
            return _to_builtin(region.get(key))
    res = region.get('res')
    if isinstance(res, dict):
        for key in ('layout_relation', 'layoutRelation', 'relation', 'relations'):
            if key in res:
                return _to_builtin(res.get(key))
    return None


def _extract_multimodal_score(region: dict[str, Any]) -> Any:
    for key in ('multimodal_score', 'multimodalScore', 'score', 'confidence'):
        if key in region:
            return _to_builtin(region.get(key))
    res = region.get('res')
    if isinstance(res, dict):
        for key in ('multimodal_score', 'multimodalScore', 'score', 'confidence'):
            if key in res:
                return _to_builtin(res.get(key))
    return None


def _extract_format_preserved_text(region: dict[str, Any]) -> str:
    if region.get('type') == 'ignore':
        return ''
    res = region.get('res')
    if isinstance(res, dict):
        for key in (
            'format_preserved_text',
            'formatPreservedText',
            'formatted_text',
            'markdown',
            'html',
            'latex',
        ):
            v = res.get(key)
            if isinstance(v, str) and v.strip():
                return v
        text = res.get('text')
        if isinstance(text, str):
            return text
    return _extract_text(region)


def _norm_bbox(bbox: Any) -> list[int]:
    """规范化 BBox 坐标，返回 [x1, y1, x2, y2]。"""
    data = _to_builtin(bbox)
    if not isinstance(data, (list, tuple)) or len(data) < 4:
        return []
    out: list[int] = []
    for x in data[:4]:
        try:
            out.append(int(round(float(x))))
        except Exception:
            out.append(0)
    x1, y1, x2, y2 = out
    x1, x2 = sorted([x1, x2])
    y1, y2 = sorted([y1, y2])
    return [x1, y1, x2, y2]


def _bbox_fuzzy_match(
    target_bbox: list[int],
    parse_map: dict[tuple[str, tuple[int, ...]], dict[str, Any]],
    label: str,
    tolerance: int = 2,
) -> dict[str, Any]:
    """在 parse_map 中按标签做 BBox 模糊匹配。"""
    if not target_bbox or len(target_bbox) != 4:
        return {}
    for (map_label, map_bbox), map_block in parse_map.items():
        if map_label != label or len(map_bbox) != 4:
            continue
        diffs = [abs(a - b) for a, b in zip(target_bbox, map_bbox)]
        if all(d <= tolerance for d in diffs):
            return map_block
    return {}


def _paddle_block_to_dict(block: Any) -> dict[str, Any]:
    if isinstance(block, dict):
        return _to_builtin(block)
    return {
        'label': getattr(block, 'label', None),
        'bbox': _to_builtin(getattr(block, 'bbox', [])),
        'content': getattr(block, 'content', ''),
        'group_id': getattr(block, 'group_id', None),
        'global_group_id': getattr(block, 'global_group_id', None),
        'global_block_id': getattr(block, 'global_block_id', None),
    }


def _preprocess_image(image_path: Path) -> Path:
    """对图片做轻量预处理并输出临时图路径。"""
    import cv2  # type: ignore
    import tempfile

    img = cv2.imread(str(image_path))
    if img is None:
        return image_path

    lab = cv2.cvtColor(img, cv2.COLOR_BGR2LAB)
    l_ch, a_ch, b_ch = cv2.split(lab)
    clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
    l_ch = clahe.apply(l_ch)
    lab = cv2.merge([l_ch, a_ch, b_ch])
    enhanced = cv2.cvtColor(lab, cv2.COLOR_LAB2BGR)

    enhanced = cv2.medianBlur(enhanced, 3)

    h, w = enhanced.shape[:2]
    min_size = 600
    if h < min_size or w < min_size:
        scale = min_size / min(h, w)
        enhanced = cv2.resize(
            enhanced,
            (int(w * scale), int(h * scale)),
            interpolation=cv2.INTER_CUBIC,
        )

    suffix = image_path.suffix or '.jpg'
    fd, tmp_path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    cv2.imwrite(tmp_path, enhanced)
    logger.debug('预处理图片已保存: %s -> %s', image_path, tmp_path)
    return Path(tmp_path)


def _safe_token(text: str) -> str:
    token = re.sub(r'[^0-9A-Za-z\-_.]+', '_', text).strip('_')
    return token or 'unknown'


def _dump_crop_debug(
    image_path: Path,
    bbox: list[int],
    *,
    label: str,
    text: str,
    region_index: int,
    debug_dir: Path,
) -> None:
    """落盘图片区域裁剪图和 OCR 文本，便于定位问题。"""
    import cv2  # type: ignore

    img = cv2.imread(str(image_path))
    if img is None:
        return

    h_img, w_img = img.shape[:2]
    x1, y1, x2, y2 = bbox
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(w_img, x2)
    y2 = min(h_img, y2)
    if x2 <= x1 or y2 <= y1:
        return

    crop = img[y1:y2, x1:x2]
    debug_dir.mkdir(parents=True, exist_ok=True)

    stem = f"region_{region_index:04d}_{_safe_token(label)}_{x1}_{y1}_{x2}_{y2}"
    img_path = debug_dir / f'{stem}.jpg'
    txt_path = debug_dir / f'{stem}.txt'

    cv2.imwrite(str(img_path), crop)
    txt_path.write_text(text or '', encoding='utf-8')


def _ocr_crop_region(
    image_path: Path,
    bbox: list[int],
    *,
    vl_pipeline: Any | None = None,
) -> str:
    """裁剪指定区域并复用 VL pipeline 做 OCR。"""
    import cv2  # type: ignore
    import tempfile

    if vl_pipeline is None:
        logger.warning('图片区域 OCR 缺少可用 pipeline，跳过。bbox=%s', bbox)
        return ''

    img = cv2.imread(str(image_path))
    if img is None:
        return ''

    h_img, w_img = img.shape[:2]
    x1, y1, x2, y2 = bbox
    x1 = max(0, x1)
    y1 = max(0, y1)
    x2 = min(w_img, x2)
    y2 = min(h_img, y2)
    if x2 <= x1 or y2 <= y1:
        return ''

    crop = img[y1:y2, x1:x2]
    fd, crop_path = tempfile.mkstemp(suffix='.jpg')
    os.close(fd)
    cv2.imwrite(crop_path, crop)

    try:
        raw = vl_pipeline.predict(str(crop_path))
        if isinstance(raw, (list, tuple)):
            pages = raw
        elif hasattr(raw, '__iter__') and not isinstance(raw, (dict, str, bytes)):
            pages = list(raw)
        else:
            pages = [raw]

        lines: list[str] = []
        for page in pages:
            if not page:
                continue
            page_dict = _to_builtin(page)
            if not isinstance(page_dict, dict):
                continue

            parsing = page_dict.get('parsing_res_list', [])
            if isinstance(parsing, list):
                for block in parsing:
                    b = _paddle_block_to_dict(block)
                    label = str(b.get('label', ''))
                    content = str(b.get('content', '') or '').strip()
                    if label in _TEXT_LABELS and content:
                        lines.append(content)

            spotting = page_dict.get('spotting_res')
            if isinstance(spotting, list):
                for spot in spotting:
                    if not isinstance(spot, dict):
                        continue
                    txt = str(spot.get('rec_text', '') or '').strip()
                    if txt:
                        lines.append(txt)

        uniq: list[str] = []
        seen: set[str] = set()
        for line in lines:
            if line not in seen:
                seen.add(line)
                uniq.append(line)
        return '\n'.join(uniq).strip()
    except Exception as e:
        logger.warning('图片区域 OCR 失败 (bbox=%s): %s', bbox, e)
        return ''
    finally:
        try:
            os.unlink(crop_path)
        except Exception:
            pass


def run_with_paddleocr_vl(
    image_path: Path,
    *,
    preprocess: bool = True,
    crop_debug_dir: Path | None = None,
) -> list[dict[str, Any]]:
    import paddlex as pdx  # type: ignore

    timing: dict[str, float] = {}

    t0 = time.perf_counter()
    processed_path = _preprocess_image(image_path) if preprocess else image_path
    timing['preprocess'] = time.perf_counter() - t0

    t_load = time.perf_counter()
    pipeline = pdx.create_pipeline(pipeline='PaddleOCR-VL-1.5')
    timing['model_load'] = time.perf_counter() - t_load

    t_inf = time.perf_counter()
    raw_results = list(pipeline.predict(str(processed_path)))
    timing['inference'] = time.perf_counter() - t_inf

    outputs: list[dict[str, Any]] = []
    t_post = time.perf_counter()
    region_index = 0

    for page in raw_results:
        page_dict = _to_builtin(page)
        if not isinstance(page_dict, dict):
            continue

        layout_res = page_dict.get('layout_det_res', {})
        boxes = layout_res.get('boxes', []) if isinstance(layout_res, dict) else []
        parsing = page_dict.get('parsing_res_list', [])

        parse_map: dict[tuple[str, tuple[int, ...]], dict[str, Any]] = {}
        if isinstance(parsing, list):
            for block in parsing:
                b = _paddle_block_to_dict(block)
                label = str(b.get('label', 'unknown'))
                bbox = _norm_bbox(b.get('bbox', []))
                if bbox:
                    parse_map[(label, tuple(bbox))] = b

        if isinstance(boxes, list):
            for box in boxes:
                if not isinstance(box, dict):
                    continue

                label = str(box.get('label', 'unknown'))
                bbox = _norm_bbox(box.get('coordinate', box.get('bbox', [])))
                if not bbox:
                    continue

                region_index += 1

                raw_score = box.get('score')
                if raw_score is not None:
                    try:
                        if float(raw_score) < _DETECTION_SCORE_THRESHOLD:
                            logger.debug(
                                '低置信度区域(保留): label=%s, score=%.4f, bbox=%s',
                                label,
                                float(raw_score),
                                bbox,
                            )
                    except Exception:
                        pass

                block = parse_map.get((label, tuple(bbox)), {})
                if not block:
                    block = _bbox_fuzzy_match(bbox, parse_map, label, tolerance=2)

                text = str(block.get('content', '') or '')
                if label == 'ignore' and not text.strip():
                    text = ''

                if label in _IMAGE_LABELS and not text.strip():
                    ocr_text = _ocr_crop_region(processed_path, bbox, vl_pipeline=pipeline)
                    if ocr_text:
                        text = ocr_text

                if label in _IMAGE_LABELS and crop_debug_dir is not None:
                    _dump_crop_debug(
                        processed_path,
                        bbox,
                        label=label,
                        text=text,
                        region_index=region_index,
                        debug_dir=crop_debug_dir,
                    )

                relation = {
                    'order': _to_builtin(box.get('order')),
                    'group_id': _to_builtin(block.get('group_id')),
                    'global_group_id': _to_builtin(block.get('global_group_id')),
                    'global_block_id': _to_builtin(block.get('global_block_id')),
                }

                outputs.append(
                    {
                        '区域类型': label,
                        '坐标': bbox,
                        '识别文本': text,
                        'semantic_tag': label,
                        'layout_relation': relation,
                        'multimodal_score': _to_builtin(box.get('score')),
                        'format_preserved_text': text,
                    }
                )

        if not boxes and isinstance(parsing, list):
            for block in parsing:
                b = _paddle_block_to_dict(block)
                label = str(b.get('label', 'unknown'))
                bbox = _norm_bbox(b.get('bbox', []))
                text = str(b.get('content', '') or '')
                if label == 'ignore' and not text.strip():
                    text = ''
                outputs.append(
                    {
                        '区域类型': label,
                        '坐标': bbox,
                        '识别文本': text,
                        'semantic_tag': label,
                        'layout_relation': {
                            'order': None,
                            'group_id': _to_builtin(b.get('group_id')),
                            'global_group_id': _to_builtin(b.get('global_group_id')),
                            'global_block_id': _to_builtin(b.get('global_block_id')),
                        },
                        'multimodal_score': None,
                        'format_preserved_text': text,
                    }
                )

    timing['postprocess'] = time.perf_counter() - t_post
    timing['total_excl_model'] = timing['preprocess'] + timing['inference'] + timing['postprocess']
    timing['total'] = timing['model_load'] + timing['total_excl_model']

    if processed_path != image_path:
        try:
            processed_path.unlink(missing_ok=True)
        except Exception:
            pass

    print('\n' + '=' * 50, file=sys.stderr)
    print('耗时统计 (Processing Time)', file=sys.stderr)
    print('=' * 50, file=sys.stderr)
    print(f"预处理:              {timing['preprocess']:.3f}s", file=sys.stderr)
    print(f"模型加载:            {timing['model_load']:.3f}s", file=sys.stderr)
    print(f"推理 (predict):      {timing['inference']:.3f}s", file=sys.stderr)
    print(f"后处理:              {timing['postprocess']:.3f}s", file=sys.stderr)
    print('-' * 50, file=sys.stderr)
    print(f"总计:                {timing['total']:.3f}s", file=sys.stderr)
    print(f"总计(不含模型加载):  {timing['total_excl_model']:.3f}s", file=sys.stderr)
    print('=' * 50, file=sys.stderr)

    return outputs


def run_with_ppstructure(image_path: Path) -> list[dict[str, Any]]:
    from paddleocr import PPStructure  # type: ignore

    engine = PPStructure(
        layout=True,
        table=True,
        ocr=True,
        show_log=False,
        use_angle_cls=False,
        lang='ch',
    )
    regions = _to_builtin(engine(str(image_path)))

    outputs: list[dict[str, Any]] = []
    for region in regions:
        if not isinstance(region, dict):
            continue
        outputs.append(
            {
                '区域类型': region.get('type', 'unknown'),
                '坐标': _to_builtin(region.get('bbox', [])),
                '识别文本': _extract_text(region),
                'semantic_tag': _extract_semantic_tag(region),
                'layout_relation': _extract_layout_relation(region),
                'multimodal_score': _extract_multimodal_score(region),
                'format_preserved_text': _extract_format_preserved_text(region),
            }
        )
    return outputs


def main() -> int:
    parser = argparse.ArgumentParser(description='PaddleOCR-VL-1.5 功能测试（区域输出）')
    parser.add_argument(
        '--image',
        type=Path,
        default=DEFAULT_IMAGE,
        help=f'待测试图片路径，默认: {DEFAULT_IMAGE}',
    )
    parser.add_argument(
        '--no-preprocess',
        action='store_true',
        default=False,
        help='禁用图片预处理。',
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        default=False,
        help='启用调试日志。',
    )
    parser.add_argument(
        '--dump-crop-debug',
        type=Path,
        default=None,
        help='保存图片类区域裁剪图和对应 OCR 文本的目录。',
    )
    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    image_path = args.image.resolve()
    if not image_path.exists():
        print(f'图片不存在: {image_path}', file=sys.stderr)
        return 1

    crop_debug_dir: Path | None = None
    if args.dump_crop_debug is not None:
        crop_debug_dir = args.dump_crop_debug.resolve()
        crop_debug_dir.mkdir(parents=True, exist_ok=True)
        logger.info('图片区域调试输出目录: %s', crop_debug_dir)

    os.environ.setdefault('PADDLE_HOME', str(PROJECT_ROOT / 'var/models/paddle'))
    os.environ.setdefault('PPNLP_HOME', str(PROJECT_ROOT / 'var/models/ppnlp'))
    os.environ.setdefault('HUGGINGFACE_HUB_CACHE', str(PROJECT_ROOT / 'var/models/hf'))
    os.environ.setdefault('PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK', 'True')
    Path(os.environ['PADDLE_HOME']).mkdir(parents=True, exist_ok=True)
    Path(os.environ['PPNLP_HOME']).mkdir(parents=True, exist_ok=True)
    Path(os.environ['HUGGINGFACE_HUB_CACHE']).mkdir(parents=True, exist_ok=True)

    backend = ''
    regions: list[dict[str, Any]]
    try:
        regions = run_with_paddleocr_vl(
            image_path,
            preprocess=not args.no_preprocess,
            crop_debug_dir=crop_debug_dir,
        )
        backend = 'PaddleOCR-VL-1.5'
    except Exception as e:
        print(f'[WARN] PaddleOCR-VL-1.5 不可用，自动回退 PPStructure。原因: {e}', file=sys.stderr)
        regions = run_with_ppstructure(image_path)
        backend = 'PPStructure(兼容回退)'

    print(json.dumps({'backend': backend, 'image': str(image_path), 'regions': regions}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
