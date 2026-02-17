"""
PaddleOCR 2.x PP-Structure 测试脚本
用于 PPT 截图的布局检测和 OCR 识别
"""

import sys
import time
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from paddleocr import PPStructure, save_structure_res
    from PIL import Image
except ImportError as e:
    print(f"缺少依赖: {e}")
    print("\n请安装 PaddleOCR 2.x:")
    print("  pip install paddleocr==2.7.0")
    sys.exit(1)

IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

def test_pp_structure(image_path: Path, save_output: bool = True):
    """使用 PP-Structure 分析 PPT 截图"""
    print(f"\n{'='*70}")
    print(f"PaddleOCR 2.x PP-Structure 测试")
    print(f"{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"图片路径: {image_path}")
    
    image = Image.open(image_path)
    print(f"图片尺寸: {image.size}")
    print(f"{'='*70}\n")
    
    # 初始化 PP-Structure
    print("[1/2] 初始化 PP-Structure 引擎...")
    t_init = time.perf_counter()
    
    try:
        engine = PPStructure(
            layout=True,
            table=True,
            ocr=True,
            show_log=False,
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
    
    # 执行布局分析和 OCR
    print("[2/2] 执行布局分析和 OCR...")
    t_process = time.perf_counter()
    
    try:
        result = engine(str(image_path))
        process_time = time.perf_counter() - t_process
        print(f"✓ 处理完成 ({process_time:.1f}s)\n")
        
    except Exception as e:
        print(f"✗ 处理失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 解析和展示结果
    print(f"{'='*70}")
    print("布局分析和 OCR 结果")
    print(f"{'='*70}\n")
    
    type_counts = {}
    structured_results = {
        "title": [],
        "text": [],
        "table": [],
        "figure": [],
        "reference": [],
        "other": []
    }
    
    for i, item in enumerate(result):
        layout_type = item.get('type', 'unknown')
        bbox = item.get('bbox', [])
        res = item.get('res', '')
        
        type_counts[layout_type] = type_counts.get(layout_type, 0) + 1
        
        # 提取文字内容
        if isinstance(res, list):
            text_content = "\n".join([line[1] for line in res if len(line) >= 2])
        elif isinstance(res, dict):
            text_content = str(res)
        else:
            text_content = str(res)
        
        region_data = {
            "index": i + 1,
            "type": layout_type,
            "bbox": bbox,
            "text": text_content
        }
        
        if layout_type == "title":
            structured_results["title"].append(region_data)
        elif layout_type == "text":
            structured_results["text"].append(region_data)
        elif layout_type == "table":
            structured_results["table"].append(region_data)
        elif layout_type == "figure":
            structured_results["figure"].append(region_data)
        elif layout_type == "reference":
            structured_results["reference"].append(region_data)
        else:
            structured_results["other"].append(region_data)
        
        print(f"【区域 #{i+1}】")
        print(f"  类型: {layout_type}")
        print(f"  位置: {bbox}")
        if text_content.strip():
            print(f"  内容:")
            display_text = text_content[:200] + ("..." if len(text_content) > 200 else "")
            for line in display_text.split('\n'):
                if line.strip():
                    print(f"    {line}")
        print()
    
    # 统计信息
    print(f"{'='*70}")
    print("统计信息")
    print(f"{'='*70}")
    print(f"检测到的区域总数: {len(result)}")
    print(f"\n区域类型分布:")
    for layout_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {layout_type}: {count} 个")
    
    print(f"\n性能统计:")
    print(f"  引擎初始化: {init_time:.2f}s")
    print(f"  布局分析+OCR: {process_time:.2f}s")
    print(f"  总耗时: {init_time + process_time:.2f}s")
    print(f"{'='*70}\n")
    
    # 按类型汇总
    print(f"{'='*70}")
    print("按类型汇总")
    print(f"{'='*70}\n")
    
    if structured_results["title"]:
        print(f"【标题区域】({len(structured_results['title'])} 个)")
        for item in structured_results["title"]:
            if item["text"].strip():
                print(f"  {item['text'][:100]}")
        print()
    
    if structured_results["text"]:
        print(f"【文本区域】({len(structured_results['text'])} 个)")
        for item in structured_results["text"][:3]:
            if item["text"].strip():
                print(f"  区域 #{item['index']}: {item['text'][:100]}")
        if len(structured_results["text"]) > 3:
            print(f"  ... (还有 {len(structured_results['text']) - 3} 个文本区域)")
        print()
    
    if structured_results["table"]:
        print(f"【表格区域】({len(structured_results['table'])} 个)")
        for item in structured_results["table"]:
            print(f"  区域 #{item['index']}: 表格内容已识别")
        print()
    
    if structured_results["figure"]:
        print(f"【图片区域】({len(structured_results['figure'])} 个)")
        for item in structured_results["figure"]:
            print(f"  区域 #{item['index']}: {item['bbox']}")
        print()
    
    # 保存结果
    if save_output:
        output_dir = image_path.parent
        output_prefix = image_path.stem
        
        json_path = output_dir / f"{output_prefix}_ppstructure_result.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump({
                "image_path": str(image_path),
                "image_size": image.size,
                "total_regions": len(result),
                "type_counts": type_counts,
                "regions": structured_results,
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

if __name__ == "__main__":
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    test_pp_structure(IMAGE_PATH, save_output=True)
