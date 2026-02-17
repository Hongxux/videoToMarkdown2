"""
OmniParser 增强版 - 使用 Florence-2 辅助 UI 分类
解决 YOLO 分类不准确的问题
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import torch
    from transformers import AutoModelForCausalLM, AutoProcessor
    from ultralytics import YOLO
    from PIL import Image
except ImportError as e:
    print(f"Missing dependencies: {e}")
    sys.exit(1)

IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

# YOLO 原始类别映射
UI_CLASS_MAPPING = {
    0: "button", 1: "input", 2: "icon", 3: "text", 4: "image",
    5: "checkbox", 6: "radio", 7: "dropdown", 8: "slider", 
    9: "progress_bar", 10: "modal", 11: "navigation", 12: "card",
    13: "list_item", 14: "tab", 15: "menu", 16: "toolbar", 
    17: "table", 18: "other"
}

def classify_from_description(desc):
    """从 Florence-2 描述中推断 UI 类别"""
    desc_lower = desc.lower()
    
    # 关键词匹配规则（按优先级排序）
    rules = [
        (['card', 'panel', 'container', 'box'], 'card'),
        (['input', 'text field', 'entry', 'textbox', 'text box'], 'input'),
        (['button', 'click', 'submit', 'confirm'], 'button'),
        (['text', 'label', 'paragraph', 'description', 'caption'], 'text'),
        (['icon', 'symbol', 'logo'], 'icon'),
        (['image', 'picture', 'photo'], 'image'),
        (['menu', 'navigation', 'nav'], 'navigation'),
        (['table', 'grid', 'spreadsheet'], 'table'),
        (['list', 'item'], 'list_item'),
    ]
    
    for keywords, class_name in rules:
        if any(kw in desc_lower for kw in keywords):
            return class_name
    
    return 'other'

def test_omniparser_enhanced(image_path: Path):
    """
    OmniParser 增强版测试
    使用 Florence-2 辅助分类，提高准确度
    """
    print(f"\n{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"模型: OmniParser v2.0 + Florence-2 辅助分类")
    print(f"{'='*70}\n")
    
    print("[1/4] 加载 YOLO 模型...")
    t_load = time.perf_counter()
    
    try:
        yolo_path = PROJECT_ROOT / "var/models/omniparser-v2/icon_detect/model.pt"
        yolo = YOLO(str(yolo_path))
        print(f"✓ YOLO 加载完成")
    except Exception as e:
        print(f"✗ YOLO 加载失败: {e}")
        return
    
    print("\n[2/4] 加载 Florence-2 模型...")
    
    try:
        caption_path = PROJECT_ROOT / "var/models/omniparser-v2/icon_caption"
        device = "cuda" if torch.cuda.is_available() else "cpu"
        torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
        
        caption_model = AutoModelForCausalLM.from_pretrained(
            caption_path, 
            trust_remote_code=True,
            torch_dtype=torch_dtype
        ).to(device)
        
        sys.path.insert(0, str(caption_path))
        from processing_florence2 import Florence2Processor
        caption_processor = Florence2Processor.from_pretrained(caption_path)
        sys.path.pop(0)
        
        print(f"✓ Florence-2 加载完成 (设备: {device})")
    except Exception as e:
        print(f"✗ Florence-2 加载失败: {e}")
        return
    
    load_ms = (time.perf_counter() - t_load) * 1000
    print(f"✓ 所有模型加载完成 ({load_ms:.0f}ms)\n")
    
    print("[3/4] 检测 UI 元素...")
    t0 = time.perf_counter()
    
    try:
        image = Image.open(image_path).convert("RGB")
        det_results = yolo(image, verbose=False)
        detections = []
        
        if len(det_results) > 0:
            boxes = det_results[0].boxes
            for box in boxes:
                xyxy = box.xyxy[0].tolist()
                conf = box.conf[0].item()
                cls = int(box.cls[0].item())
                detections.append({"box": xyxy, "conf": conf, "cls": cls})
        
        detections.sort(key=lambda x: (x["box"][1], x["box"][0]))
        print(f"✓ 检测到 {len(detections)} 个 UI 元素\n")
        
        if len(detections) == 0:
            print("未检测到任何 UI 元素")
            return
    except Exception as e:
        print(f"✗ UI 元素检测失败: {e}")
        return
    
    print("[4/4] 增强分类（YOLO + Florence-2）...\n")
    
    def get_enhanced_classification(crop, yolo_class_id, yolo_conf):
        """增强版分类：结合 YOLO 和 Florence-2"""
        yolo_class = UI_CLASS_MAPPING.get(yolo_class_id, 'other')
        
        # 高置信度：直接使用 YOLO
        if yolo_conf > 0.7:
            return yolo_class, yolo_conf, 'yolo', ''
        
        # 低置信度：使用 Florence-2 辅助
        try:
            prompt = "<DETAILED_CAPTION>"
            inputs = caption_processor(text=prompt, images=crop, return_tensors="pt").to(device, torch_dtype)
            outputs = caption_model.generate(
                input_ids=inputs["input_ids"],
                pixel_values=inputs["pixel_values"],
                max_new_tokens=100,
                num_beams=3
            )
            generated_text = caption_processor.batch_decode(outputs, skip_special_tokens=False)[0]
            parsed = caption_processor.post_process_generation(
                generated_text, 
                task=prompt, 
                image_size=(crop.width, crop.height)
            )
            description = parsed.get(prompt, "")
            
            # 从描述推断类别
            florence_class = classify_from_description(description)
            
            return florence_class, 0.8, 'florence2', description
        except Exception as e:
            print(f"  ⚠ Florence-2 分类失败: {e}")
            return yolo_class, yolo_conf, 'yolo_fallback', ''
    
    try:
        results = []
        for i, d in enumerate(detections):
            box = d["box"]
            crop = image.crop((int(box[0]), int(box[1]), int(box[2]), int(box[3])))
            
            yolo_class = UI_CLASS_MAPPING.get(d["cls"], 'other')
            final_class, final_conf, method, description = get_enhanced_classification(crop, d["cls"], d["conf"])
            
            results.append({
                "index": i + 1,
                "yolo_class": yolo_class,
                "yolo_conf": d["conf"],
                "final_class": final_class,
                "final_conf": final_conf,
                "method": method,
                "description": description,
                "position": f"({int(box[0])}, {int(box[1])})",
                "size": f"{int(box[2]-box[0])}x{int(box[3]-box[1])}"
            })
            
            print(f"元素 #{i+1}:")
            print(f"  位置: {results[-1]['position']}")
            print(f"  大小: {results[-1]['size']}")
            print(f"  YOLO 分类: {yolo_class} (置信度: {d['conf']:.2f})")
            print(f"  最终分类: {final_class} (置信度: {final_conf:.2f})")
            print(f"  分类方法: {method}")
            if description:
                print(f"  Florence-2 描述: {description}")
            print()
        
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"✓ 处理完成 ({elapsed_ms:.0f}ms)\n")
        
        # 统计
        print(f"{'='*70}")
        print("分类方法统计:")
        print(f"{'='*70}")
        method_counts = {}
        for r in results:
            method_counts[r['method']] = method_counts.get(r['method'], 0) + 1
        
        for method, count in method_counts.items():
            print(f"  {method}: {count} 个元素")
        
        print(f"\n{'='*70}\n")
        
    except Exception as e:
        print(f"✗ 处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    test_omniparser_enhanced(IMAGE_PATH)
