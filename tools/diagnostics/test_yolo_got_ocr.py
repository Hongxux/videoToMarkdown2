"""
OmniParser 改进版 - YOLO + Got-OCR2.0
使用专业 OCR 模型替代 Florence-2，提高文字识别准确度
"""

import sys
import time
import os
from pathlib import Path
from typing import List, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import torch
    from transformers import AutoModel, AutoTokenizer
    from ultralytics import YOLO
    from PIL import Image
except ImportError as e:
    print(f"Missing dependencies: {e}")
    sys.exit(1)

IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

# YOLO UI 元素类别映射
UI_CLASS_MAPPING = {
    0: "button", 1: "input", 2: "icon", 3: "text", 4: "image",
    5: "checkbox", 6: "radio", 7: "dropdown", 8: "slider", 
    9: "progress_bar", 10: "modal", 11: "navigation", 12: "card",
    13: "list_item", 14: "tab", 15: "menu", 16: "toolbar", 
    17: "table", 18: "other"
}

def test_yolo_got_ocr(image_path: Path):
    """
    YOLO + Got-OCR2.0 混合方案
    - YOLO: 检测 UI 元素位置
    - Got-OCR2.0: 精确文字识别
    """
    print(f"\n{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"方案: YOLO (位置检测) + Got-OCR2.0 (文字识别)")
    print(f"{'='*70}\n")
    
    # ============ 1. 加载 YOLO 模型 ============
    print("[1/3] 加载 YOLO 模型...")
    t_load = time.perf_counter()
    
    try:
        yolo_path = PROJECT_ROOT / "var/models/omniparser-v2/icon_detect/model.pt"
        if not yolo_path.exists():
            print(f"✗ YOLO 模型未找到: {yolo_path}")
            return
        
        yolo = YOLO(str(yolo_path))
        print(f"✓ YOLO 加载完成")
    except Exception as e:
        print(f"✗ YOLO 加载失败: {e}")
        return
    
    # ============ 2. 加载 Got-OCR2.0 模型 ============
    print("\n[2/3] 加载 Got-OCR2.0 模型...")
    
    try:
        got_path = PROJECT_ROOT / "var/models/got-ocr2"
        if not got_path.exists():
            print(f"✗ Got-OCR2.0 模型未找到: {got_path}")
            return
        
        tokenizer = AutoTokenizer.from_pretrained(got_path, trust_remote_code=True)
        model = AutoModel.from_pretrained(
            got_path,
            trust_remote_code=True,
            use_safetensors=True,
            pad_token_id=tokenizer.eos_token_id
        )
        model = model.eval()
        
        device = "cuda" if torch.cuda.is_available() else "cpu"
        if torch.cuda.is_available():
            model = model.cuda()
        
        print(f"✓ Got-OCR2.0 加载完成 (设备: {device})")
    except Exception as e:
        print(f"✗ Got-OCR2.0 加载失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    load_ms = (time.perf_counter() - t_load) * 1000
    print(f"✓ 所有模型加载完成 ({load_ms:.0f}ms)\n")
    
    # ============ 3. 检测并识别 ============
    print("[3/3] 检测 UI 元素并提取文字...")
    t0 = time.perf_counter()
    
    try:
        image = Image.open(image_path).convert("RGB")
        
        # YOLO 检测
        det_results = yolo(image, verbose=False)
        detections = []
        
        if len(det_results) > 0:
            boxes = det_results[0].boxes
            for box in boxes:
                xyxy = box.xyxy[0].tolist()
                conf = box.conf[0].item()
                cls = int(box.cls[0].item())
                detections.append({
                    "box": xyxy, 
                    "conf": conf, 
                    "cls": cls,
                    "class_name": UI_CLASS_MAPPING.get(cls, 'other')
                })
        
        # 按位置排序
        detections.sort(key=lambda x: (x["box"][1], x["box"][0]))
        
        print(f"✓ YOLO 检测到 {len(detections)} 个 UI 元素\n")
        
        if len(detections) == 0:
            print("未检测到任何 UI 元素")
            return
        
        # 为每个元素进行 OCR
        print("正在使用 Got-OCR2.0 提取文字...\n")
        results = []
        temp_dir = PROJECT_ROOT / "temp"
        temp_dir.mkdir(exist_ok=True)
        
        for i, d in enumerate(detections):
            print(f"处理元素 {i+1}/{len(detections)}...", end=" ")
            t_elem = time.perf_counter()
            
            box = d["box"]
            crop = image.crop((int(box[0]), int(box[1]), int(box[2]), int(box[3])))
            
            # 保存临时文件供 Got-OCR2.0 使用
            temp_path = temp_dir / f"crop_{i}.jpg"
            crop.save(temp_path)
            
            try:
                # Got-OCR2.0 OCR
                ocr_text = model.chat(tokenizer, str(temp_path), ocr_type='ocr')
                ocr_text = ocr_text.strip()
            except Exception as e:
                ocr_text = f"[OCR失败: {str(e)[:50]}]"
            finally:
                # 删除临时文件
                if temp_path.exists():
                    temp_path.unlink()
            
            elem_ms = (time.perf_counter() - t_elem) * 1000
            
            results.append({
                "index": i + 1,
                "class": d["class_name"],
                "yolo_conf": d["conf"],
                "position": f"({int(box[0])}, {int(box[1])})",
                "size": f"{int(box[2]-box[0])}x{int(box[3]-box[1])}",
                "ocr_text": ocr_text,
                "time_ms": elem_ms
            })
            
            print(f"{elem_ms:.0f}ms")
        
        # 清理临时目录
        try:
            temp_dir.rmdir()
        except:
            pass
        
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"\n✓ 处理完成 (总耗时: {elapsed_ms:.0f}ms)\n")
        
        # ============ 输出结果 ============
        print(f"{'='*70}")
        print("识别结果:")
        print(f"{'='*70}\n")
        
        for r in results:
            print(f"【元素 #{r['index']}】")
            print(f"  类别: {r['class']} (YOLO 置信度: {r['yolo_conf']:.2f})")
            print(f"  位置: {r['position']}")
            print(f"  大小: {r['size']}")
            print(f"  识别文字: {r['ocr_text']}")
            print(f"  耗时: {r['time_ms']:.0f}ms")
            print()
        
        # ============ 性能统计 ============
        print(f"{'='*70}")
        print("性能统计:")
        print(f"{'='*70}")
        print(f"  模型加载: {load_ms:.0f}ms")
        print(f"  YOLO 检测: {elapsed_ms - sum(r['time_ms'] for r in results):.0f}ms")
        print(f"  OCR 识别: {sum(r['time_ms'] for r in results):.0f}ms")
        print(f"    → 平均每元素: {sum(r['time_ms'] for r in results) / len(results):.0f}ms")
        print(f"  总耗时: {elapsed_ms:.0f}ms")
        print(f"\n{'='*70}\n")
        
        # ============ 与 Florence-2 对比 ============
        print("💡 与 Florence-2 OCR 对比:")
        print("  - Got-OCR2.0 专为 OCR 设计，中文识别准确度显著提高")
        print("  - 可准确识别终端输出、代码、中英文混排等复杂文本")
        print("  - 速度较慢（CPU 推理），但准确度优势明显")
        print()
        
    except Exception as e:
        print(f"✗ 处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    test_yolo_got_ocr(IMAGE_PATH)
