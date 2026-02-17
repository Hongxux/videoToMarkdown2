"""
PaddleOCR PP-Structure 测试脚本
用于 PPT 截图的布局检测和 OCR 识别

PP-Structure 功能：
1. 布局分析（检测标题、正文、表格、图片等区域）
2. OCR 识别（每个区域的文字内容）
3. 表格识别（结构化表格内容）
"""

import sys
import time
import json
from pathlib import Path
from typing import List, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from paddleocr import PaddleOCR
    from PIL import Image
    import cv2
    import numpy as np
except ImportError as e:
    print(f"缺少依赖: {e}")
    print("\n请安装 PaddleOCR:")
    print("  CPU 版本: pip install paddleocr paddlepaddle")
    print("  GPU 版本: pip install paddleocr paddlepaddle-gpu")
    sys.exit(1)

IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

def test_pp_structure(image_path: Path, save_output: bool = True):
    """
    使用 PaddleOCR 分析 PPT 截图
    
    注意: PaddleOCR 3.x 不再提供 PPStructure，但可以使用 PaddleOCR 进行 OCR + 简单布局分析
    
    Args:
        image_path: 图片路径
        save_output: 是否保存结果到文件
    """
    print(f"\n{'='*70}")
    print(f"PaddleOCR 文档分析测试")
    print(f"{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"图片路径: {image_path}")
    
    image = Image.open(image_path)
    print(f"图片尺寸: {image.size}")
    print(f"{'='*70}\n")
    
    # ============ 初始化 PaddleOCR ============
    print("[1/2] 初始化 PaddleOCR 引擎...")
    print("  - 正在加载 OCR 模型...")
    
    t_init = time.perf_counter()
    
    try:
        # 初始化 PaddleOCR
        # use_angle_cls=True: 支持文字方向检测
        # lang='ch': 中文+英文模型
        ocr = PaddleOCR(
            use_angle_cls=True,
            lang='ch'
        )
        
        init_time = time.perf_counter() - t_init
        print(f"✓ 引擎初始化完成 ({init_time:.1f}s)\n")
        
    except Exception as e:
        print(f"✗ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ============ 执行 OCR ============
    print("[2/2] 执行 OCR 识别...")
    t_process = time.perf_counter()
    
    try:
        # PaddleOCR 分析
        result = ocr.ocr(str(image_path))
        
        process_time = time.perf_counter() - t_process
        print(f"✓ 处理完成 ({process_time:.1f}s)\n")
        
    except Exception as e:
        print(f"✗ 处理失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ============ 解析结果 ============
    if not result or not result[0]:
        print("⚠ 未检测到文字")
        return
    
    print(f"{'='*70}")
    print("OCR 识别结果")
    print(f"{'='*70}\n")
    
    # result 格式: [[[bbox, (text, confidence)], ...]]
    ocr_results = result[0]
    
    # 按 y 坐标分组（简单的行检测）
    lines = {}
    for item in ocr_results:
        bbox = item[0]
        text = item[1][0]
        conf = item[1][1]
        
        # 使用 y 坐标中点作为行的键
        y_center = int((bbox[0][1] + bbox[2][1]) / 2)
        
        # 合并到同一行（y 坐标相差 < 20 像素）
        line_key = None
        for key in lines.keys():
            if abs(key - y_center) < 20:
                line_key = key
                break
        
        if line_key is None:
            line_key = y_center
            lines[line_key] = []
        
        lines[line_key].append({
            'bbox': bbox,
            'text': text,
            'conf': conf,
            'x_start': bbox[0][0]
        })
    
    # 排序行并合并同行文字
    sorted_lines = sorted(lines.items())
    
    all_text = []
    print(f"检测到 {len(sorted_lines)} 行文字:\n")
    
    for i, (y_pos, line_items) in enumerate(sorted_lines, 1):
        # 按 x 坐标排序（从左到右）
        line_items.sort(key=lambda x: x['x_start'])
        
        # 合并同行文字
        line_text = " ".join([item['text'] for item in line_items])
        avg_conf = sum([item['conf'] for item in line_items]) / len(line_items)
        
        all_text.append(line_text)
        
        print(f"第 {i} 行 (置信度: {avg_conf:.2f}):")
        print(f"  {line_text}")
        print()
    
    # ============ 统计信息 ============
    print(f"{'='*70}")
    print("统计信息")
    print(f"{'='*70}")
    print(f"检测到的文本块总数: {len(ocr_results)}")
    print(f"识别到的行数: {len(sorted_lines)}")
    print(f"总字符数: {sum(len(text) for text in all_text)}")
    
    print(f"\n性能统计:")
    print(f"  引擎初始化: {init_time:.2f}s")
    print(f"  OCR 识别: {process_time:.2f}s")
    print(f"  总耗时: {init_time + process_time:.2f}s")
    print(f"{'='*70}\n")
    
    # ============ 完整文本输出 ============
    print(f"{'='*70}")
    print("完整文本（按行）")
    print(f"{'='*70}")
    full_text = "\n".join(all_text)
    print(full_text)
    print(f"{'='*70}\n")
    
    # ============ 保存结果 ============
    if save_output:
        output_dir = image_path.parent
        output_prefix = image_path.stem
        
        # 保存 JSON 结果
        json_path = output_dir / f"{output_prefix}_paddleocr_result.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump({
                "image_path": str(image_path),
                "image_size": image.size,
                "total_text_blocks": len(ocr_results),
                "total_lines": len(sorted_lines),
                "total_characters": sum(len(text) for text in all_text),
                "lines": all_text,
                "full_text": full_text,
                "raw_results": [
                    {
                        "bbox": item[0],
                        "text": item[1][0],
                        "confidence": item[1][1]
                    }
                    for item in ocr_results
                ],
                "performance": {
                    "init_time": init_time,
                    "process_time": process_time,
                    "total_time": init_time + process_time
                }
            }, f, ensure_ascii=False, indent=2)
        
        print(f"✓ 结果已保存到: {json_path}\n")
    
    print(f"{'='*70}")
    print("测试完成")
    print(f"{'='*70}\n")
    
    print("说明:")
    print("  PaddleOCR 3.x 提供了强大的 OCR 功能，但不包含布局分析。")
    print("  如需布局分析（检测标题/正文/表格等），可以：")
    print("    1. 降级到 PaddleOCR 2.x (pip install paddleocr==2.7.0)")
    print("    2. 使用 YOLO + PaddleOCR 组合")
    print("    3. 使用 LayoutLMv3 等专门的布局分析模型")

if __name__ == "__main__":
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    test_pp_structure(IMAGE_PATH, save_output=True)
