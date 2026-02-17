"""
单图片 OmniParser 测试脚本（简化版）
使用 YOLO 检测 UI 元素 + Florence-2-base 生成描述
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
    print("需要安装: pip install torch transformers ultralytics pillow")
    sys.exit(1)

# 测试图片路径
IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

def test_omniparser_simple(image_path: Path):
    """
    测试 OmniParser 核心功能（简化版）
    
    使用：
    1. YOLO (OmniParser): 检测 UI 元素
    2. Florence-2-base: 生成元素描述
    """
    print(f"\n{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"模型: YOLO (OmniParser) + Florence-2-base")
    print(f"{'='*70}\n")
    
    print("[1/3] 加载 YOLO 模型 (UI 元素检测)...")
    t_load = time.perf_counter()
    
    try:
        yolo_path = PROJECT_ROOT / "var/models/omniparser-v2/icon_detect/model.pt"
        if not yolo_path.exists():
            print(f"错误: YOLO 模型未找到 ({yolo_path})")
            return
        
        yolo = YOLO(str(yolo_path))
        print(f"✓ YOLO 加载完成")
        
    except Exception as e:
        print(f"✗ YOLO 加载失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n[2/3] 加载 Florence-2-base 模型 (元素描述生成)...")
    
    try:
        # 使用 Florence-2-base 而非 OmniParser 微调版
        device = "cuda" if torch.cuda.is_available() else "cpu"
        torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
        
        model_id = "microsoft/Florence-2-base"
        caption_model = AutoModelForCausalLM.from_pretrained(
            model_id, 
            trust_remote_code=True,
            torch_dtype=torch_dtype
        ).to(device)
        caption_processor = AutoProcessor.from_pretrained(model_id, trust_remote_code=True)
        
        print(f"✓ Florence-2-base 加载完成 (设备: {device})")
        
    except Exception as e:
        print(f"✗ Florence-2 加载失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    load_ms = (time.perf_counter() - t_load) * 1000
    print(f"✓ 所有模型加载完成 ({load_ms:.0f}ms)\n")
    
    print("[3/3] 检测 UI 元素并生成描述...")
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
                detections.append({"box": xyxy, "conf": conf, "cls": cls})
        
        # 按位置排序（从上到下，从左到右）
        detections.sort(key=lambda x: (x["box"][1], x["box"][0]))
        
        print(f"✓ 检测到 {len(detections)} 个 UI 元素")
        
        if len(detections) == 0:
            print("\n未检测到任何 UI 元素，可能原因：")
            print("  - 图片不是 UI 截图")
            print("  - UI 元素太小或模糊")
            print("  - 图片对比度太低")
            print("\n尝试生成全局描述...")
        
        # 生成全局描述
        def get_caption(img):
            """为图片生成描述"""
            prompt = "<MORE_DETAILED_CAPTION>"
            inputs = caption_processor(text=prompt, images=img, return_tensors="pt").to(device, torch_dtype)
            generated_ids = caption_model.generate(
                input_ids=inputs["input_ids"],
                pixel_values=inputs["pixel_values"],
                max_new_tokens=1024,
                num_beams=3
            )
            generated_text = caption_processor.batch_decode(generated_ids, skip_special_tokens=False)[0]
            parsed_answer = caption_processor.post_process_generation(
                generated_text, 
                task=prompt, 
                image_size=(img.width, img.height)
            )
            return parsed_answer.get(prompt, "")
        
        global_cap = get_caption(image)
        
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"✓ 处理完成 ({elapsed_ms:.0f}ms)\n")
        
        # 输出结果
        print(f"{'='*70}")
        print("全局描述:")
        print(f"{'='*70}")
        print(global_cap)
        print()
        
        if len(detections) > 0:
            print(f"{'='*70}")
            print(f"检测到的 UI 元素 ({len(detections)} 个):")
            print(f"{'='*70}")
            
            # 只为前 5 个元素生成描述（节省时间）
            max_elements = min(5, len(detections))
            for i in range(max_elements):
                d = detections[i]
                box = d["box"]
                crop = image.crop((int(box[0]), int(box[1]), int(box[2]), int(box[3])))
                cap = get_caption(crop)
                
                print(f"\n元素 #{i+1}:")
                print(f"  位置: ({int(box[0])}, {int(box[1])})")
                print(f"  大小: {int(box[2]-box[0])}x{int(box[3]-box[1])}")
                print(f"  置信度: {d['conf']:.2f}")
                print(f"  描述: {cap}")
            
            if len(detections) > max_elements:
                print(f"\n... 还有 {len(detections) - max_elements} 个元素未显示")
        
        print(f"\n{'='*70}\n")
        
    except Exception as e:
        print(f"✗ 处理失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    test_omniparser_simple(IMAGE_PATH)
