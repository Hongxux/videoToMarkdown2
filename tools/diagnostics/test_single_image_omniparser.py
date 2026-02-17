"""
单图片 OmniParser 测试脚本 (增强版)
测试 OmniParser (YOLO + Florence-2) 对单张图片的 UI 元素识别效果
增强输出：元素分类、OCR文本提取、视觉属性、详细性能指标、丰富的描述生成
"""

import sys
import time
import json
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import torch
    import numpy as np
    from transformers import AutoModelForCausalLM, AutoProcessor
    from ultralytics import YOLO
    from PIL import Image, ImageStat
    import easyocr
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("需要安装: pip install torch transformers ultralytics pillow easyocr numpy")
    sys.exit(1)

# 测试图片路径
IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

# YOLO UI元素类别映射（根据OmniParser v2的类别定义）
UI_CLASS_MAPPING = {
    0: "button",          # 按钮
    1: "input",           # 输入框
    2: "icon",            # 图标
    3: "text",            # 文本区域
    4: "image",           # 图片元素
    5: "checkbox",        # 复选框
    6: "radio",           # 单选框
    7: "dropdown",        # 下拉框
    8: "slider",          # 滑块
    9: "progress_bar",    # 进度条
    10: "modal",          # 弹窗
    11: "navigation",     # 导航栏
    12: "card",           # 卡片
    13: "list_item",      # 列表项
    14: "tab",            # 标签页
    15: "menu",           # 菜单
    16: "toolbar",        # 工具栏
    17: "table",          # 表格
    18: "other"           # 其他元素
}

def get_image_color_info(img: Image.Image) -> Dict:
    """获取图片/元素的颜色信息"""
    stat = ImageStat.Stat(img)
    rgb_mean = stat.mean  # RGB均值
    rgb_std = stat.stddev    # RGB标准差（正确的属性名是 stddev）
    
    # 主色调判断（简化版）
    if rgb_mean[0] > 200 and rgb_mean[1] > 200 and rgb_mean[2] > 200:
        main_color = "白色/浅色调"
    elif rgb_mean[0] < 50 and rgb_mean[1] < 50 and rgb_mean[2] < 50:
        main_color = "黑色/深色调"
    elif rgb_mean[0] > rgb_mean[1] and rgb_mean[0] > rgb_mean[2]:
        main_color = "红色调"
    elif rgb_mean[1] > rgb_mean[0] and rgb_mean[1] > rgb_mean[2]:
        main_color = "绿色调"
    elif rgb_mean[2] > rgb_mean[0] and rgb_mean[2] > rgb_mean[1]:
        main_color = "蓝色调"
    else:
        main_color = "混合色调"
    
    return {
        "main_color": main_color,
        "rgb_mean": f"R:{rgb_mean[0]:.0f}, G:{rgb_mean[1]:.0f}, B:{rgb_mean[2]:.0f}",
        "brightness": f"{np.mean(rgb_mean):.0f} (0-255)",
        "contrast": f"{np.mean(rgb_std):.0f} (越高对比度越强)"
    }

def extract_text_with_ocr(img: Image.Image, lang_list: List[str] = ["ch_sim", "en"]) -> List[str]:
    """使用OCR提取图片中的文本"""
    try:
        reader = easyocr.Reader(lang_list, gpu=torch.cuda.is_available())
        results = reader.readtext(np.array(img), detail=0)
        return [text.strip() for text in results if text.strip()]
    except Exception as e:
        print(f"OCR提取文本失败: {e}")
        return []

def get_enhanced_caption(img_crop: Image.Image, processor, model, device, torch_dtype) -> Dict:
    """生成增强版的元素描述（多维度提示词）"""
    # 定义多个提示词任务，获取更丰富的描述
    tasks = {
        "basic_desc": "<CAPTION>",
        "detailed_desc": "<DETAILED_CAPTION>",
        "attribute": "<ATTRIBUTE>",
        "ocr": "<OCR>",
        "scene": "<SCENE_DESCRIPTION>"
    }
    
    results = {}
    for task_name, prompt in tasks.items():
        try:
            inputs = processor(text=prompt, images=img_crop, return_tensors="pt").to(device, torch_dtype)
            generated_ids = model.generate(
                input_ids=inputs["input_ids"],
                pixel_values=inputs["pixel_values"],
                max_new_tokens=100,
                num_beams=5,
                temperature=0.7,
                top_p=0.9
            )
            generated_text = processor.batch_decode(generated_ids, skip_special_tokens=True)[0]
            results[task_name] = generated_text.strip()
        except Exception as e:
            results[task_name] = f"生成失败: {str(e)[:50]}"
    
    # 合并描述
    combined_desc = f"""
基础描述: {results['basic_desc']}
详细描述: {results['detailed_desc']}
属性特征: {results['attribute']}
场景描述: {results['scene']}
""".strip()
    
    return {
        "combined": combined_desc,
        "raw": results
    }

def test_omniparser(image_path: Path):
    """
    测试 OmniParser v2.0 (增强版)
    
    增强特性：
    1. 详细的模型信息输出
    2. UI元素分类识别
    3. 元素颜色/亮度/对比度分析
    4. OCR文本提取
    5. 多维度的元素描述生成
    6. 细分的性能计时
    7. 结构化的输出格式
    """
    print(f"\n{'='*80}")
    print(f"测试图片: {image_path.name}")
    print(f"图片路径: {image_path}")
    print(f"图片大小: {Image.open(image_path).size}")
    print(f"模型: OmniParser v2.0 (YOLO + Florence-2) [增强版]")
    print(f"{'='*80}\n")
    
    # --------------------------
    # 1. 加载模型（详细信息）
    # --------------------------
    print("[1/5] 加载 YOLO 模型 (UI 元素检测)...")
    t_load_start = time.perf_counter()
    
    try:
        yolo_path = PROJECT_ROOT / "var/models/omniparser-v2/icon_detect/model.pt"
        if not yolo_path.exists():
            print(f"错误: YOLO 模型未找到 ({yolo_path})")
            return
        
        yolo = YOLO(str(yolo_path))
        # 输出YOLO模型详细信息
        print(f"✓ YOLO 加载完成")
        print(f"  - 模型路径: {yolo_path}")
        print(f"  - 模型类型: {yolo.model.__class__.__name__}")
        print(f"  - 输入尺寸: {yolo.model.args['imgsz'] if 'imgsz' in yolo.model.args else '默认'}")
        
    except Exception as e:
        print(f"✗ YOLO 加载失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    print("\n[2/5] 加载 Florence-2 模型 (元素描述生成)...")
    
    try:
        caption_path = PROJECT_ROOT / "var/models/omniparser-v2/icon_caption"
        if not caption_path.exists():
            print(f"错误: Florence-2 模型未找到 ({caption_path})")
            return
        
        # 设备和精度详细信息
        device = "cuda" if torch.cuda.is_available() else "cpu"
        torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
        
        # 输出设备信息
        if device == "cuda":
            print(f"  - GPU: {torch.cuda.get_device_name(0)}")
            print(f"  - CUDA版本: {torch.version.cuda}")
        
        caption_model = AutoModelForCausalLM.from_pretrained(
            caption_path, 
            trust_remote_code=True,
            torch_dtype=torch_dtype
        ).to(device)
        
        # 直接从本地文件导入 Florence2Processor
        sys.path.insert(0, str(caption_path))
        from processing_florence2 import Florence2Processor
        caption_processor = Florence2Processor.from_pretrained(caption_path)
        sys.path.pop(0)
        
        print(f"✓ Florence-2 加载完成")
        print(f"  - 模型路径: {caption_path}")
        print(f"  - 设备: {device}")
        print(f"  - 数据类型: {torch_dtype}")
        print(f"  - 模型参数: {sum(p.numel() for p in caption_model.parameters()):,}")
        
    except Exception as e:
        print(f"✗ Florence-2 加载失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    load_duration = (time.perf_counter() - t_load_start) * 1000
    print(f"✓ 所有模型加载完成 ({load_duration:.0f}ms)\n")
    
    # --------------------------
    # 2. 加载图片并检测UI元素
    # --------------------------
    print("[3/5] 检测 UI 元素...")
    t_detect_start = time.perf_counter()
    
    try:
        # 加载图片并获取基础信息
        image = Image.open(image_path).convert("RGB")
        img_width, img_height = image.size
        
        # YOLO 检测（带计时）
        t_yolo_start = time.perf_counter()
        det_results = yolo(image, verbose=False)
        yolo_duration = (time.perf_counter() - t_yolo_start) * 1000
        
        detections = []
        if len(det_results) > 0:
            boxes = det_results[0].boxes
            for box in boxes:
                xyxy = box.xyxy[0].tolist()
                conf = box.conf[0].item()
                cls = int(box.cls[0].item())
                
                # 计算元素相对位置和尺寸
                rel_x = (xyxy[0] + xyxy[2]) / 2 / img_width
                rel_y = (xyxy[1] + xyxy[3]) / 2 / img_height
                rel_width = (xyxy[2] - xyxy[0]) / img_width
                rel_height = (xyxy[3] - xyxy[1]) / img_height
                
                detections.append({
                    "box": xyxy,
                    "conf": conf,
                    "cls": cls,
                    "cls_name": UI_CLASS_MAPPING.get(cls, f"未知({cls})"),
                    "relative_position": (rel_x, rel_y),
                    "relative_size": (rel_width, rel_height)
                })
        
        # 按位置排序（从上到下，从左到右）
        detections.sort(key=lambda x: (x["box"][1], x["box"][0]))
        
        detect_duration = (time.perf_counter() - t_detect_start) * 1000
        print(f"✓ 检测完成 (总耗时: {detect_duration:.0f}ms, YOLO耗时: {yolo_duration:.0f}ms)")
        print(f"  - 检测到 {len(detections)} 个 UI 元素")
        print(f"  - 元素类别分布: {dict(Counter([d['cls_name'] for d in detections]))}")
        
        if len(detections) == 0:
            print("\n未检测到任何 UI 元素，可能原因：")
            print("  - 图片不是 UI 截图")
            print("  - UI 元素太小或模糊")
            print("  - 图片对比度太低")
            print("  - YOLO模型未适配该类型UI")
            return
        
    except Exception as e:
        print(f"✗ UI 元素检测失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # --------------------------
    # 3. 生成增强版元素描述
    # --------------------------
    print("\n[4/5] 生成增强版元素描述 (含OCR和视觉分析)...")
    t_desc_start = time.perf_counter()
    
    try:
        # 初始化OCR阅读器（全局一次）
        ocr_reader = easyocr.Reader(["ch_sim", "en"], gpu=torch.cuda.is_available())
        
        element_details = []
        for i, d in enumerate(detections):
            print(f"  处理元素 {i+1}/{len(detections)}: {d['cls_name']} (置信度: {d['conf']:.2f})")
            
            # 裁剪元素
            box = d["box"]
            crop = image.crop((int(box[0]), int(box[1]), int(box[2]), int(box[3])))
            crop_width, crop_height = crop.size
            
            # 计时单个元素处理
            t_element_start = time.perf_counter()
            
            # 1. 视觉属性分析
            color_info = get_image_color_info(crop)
            
            # 2. OCR文本提取
            ocr_text = extract_text_with_ocr(crop)
            
            # 3. 增强版描述生成
            caption_info = get_enhanced_caption(crop, caption_processor, caption_model, device, torch_dtype)
            
            # 4. 计算元素信息
            element_detail = {
                "index": i + 1,
                # 基础位置信息
                "position": {
                    "absolute": f"({int(box[0])}, {int(box[1])})",
                    "relative": f"X:{d['relative_position'][0]:.2f}, Y:{d['relative_position'][1]:.2f}",
                    "bbox": f"[{int(box[0])}, {int(box[1])}, {int(box[2])}, {int(box[3])}]"
                },
                "size": {
                    "absolute": f"{crop_width}x{crop_height}",
                    "relative": f"W:{d['relative_size'][0]:.2f}, H:{d['relative_size'][1]:.2f}",
                    "area_pct": f"{(crop_width * crop_height) / (img_width * img_height) * 100:.1f}%"
                },
                # 置信度和分类
                "confidence": f"{d['conf']:.2f}",
                "classification": {
                    "class_id": d["cls"],
                    "class_name": d["cls_name"]
                },
                # 视觉属性
                "visual_attributes": color_info,
                # 文本信息
                "text_content": {
                    "ocr_text": ocr_text,
                    "has_text": len(ocr_text) > 0
                },
                # 多维度描述
                "descriptions": caption_info,
                # 性能信息
                "processing_time_ms": f"{(time.perf_counter() - t_element_start) * 1000:.0f}"
            }
            
            element_details.append(element_detail)
        
        # 生成全局描述
        print("\n  生成全局图片描述...")
        global_caption = get_enhanced_caption(image, caption_processor, caption_model, device, torch_dtype)
        
        desc_duration = (time.perf_counter() - t_desc_start) * 1000
        print(f"✓ 描述生成完成 (总耗时: {desc_duration:.0f}ms)\n")
        
    except Exception as e:
        print(f"✗ 描述生成失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # --------------------------
    # 4. 输出结构化结果
    # --------------------------
    print("[5/5] 输出详细分析结果...")
    print(f"\n{'='*80}")
    print("全局图片分析:")
    print(f"{'='*80}")
    print(f"图片基本信息:")
    print(f"  - 文件名: {image_path.name}")
    print(f"  - 分辨率: {img_width}x{img_height}")
    print(f"  - 总像素: {img_width * img_height:,}")
    print(f"  - 主色调: {get_image_color_info(image)['main_color']}")
    print(f"\n全局描述:")
    print(global_caption['combined'])
    
    print(f"\n{'='*80}")
    print(f"UI元素详细分析 ({len(element_details)} 个):")
    print(f"{'='*80}")
    
    for elem in element_details:
        print(f"\n【元素 #{elem['index']}】")
        # 基础信息
        print(f"  1. 基础信息:")
        print(f"     - 类别: {elem['classification']['class_name']} (ID: {elem['classification']['class_id']})")
        print(f"     - 置信度: {elem['confidence']}")
        print(f"     - 处理耗时: {elem['processing_time_ms']}ms")
        
        # 位置信息
        print(f"  2. 位置信息:")
        print(f"     - 绝对坐标: {elem['position']['absolute']}")
        print(f"     - 相对位置: {elem['position']['relative']}")
        print(f"     - 完整BBOX: {elem['position']['bbox']}")
        
        # 尺寸信息
        print(f"  3. 尺寸信息:")
        print(f"     - 绝对尺寸: {elem['size']['absolute']}")
        print(f"     - 相对尺寸: {elem['size']['relative']}")
        print(f"     - 占图片比例: {elem['size']['area_pct']}")
        
        # 视觉属性
        print(f"  4. 视觉属性:")
        print(f"     - 主色调: {elem['visual_attributes']['main_color']}")
        print(f"     - RGB均值: {elem['visual_attributes']['rgb_mean']}")
        print(f"     - 亮度: {elem['visual_attributes']['brightness']}")
        print(f"     - 对比度: {elem['visual_attributes']['contrast']}")
        
        # 文本内容
        print(f"  5. 文本内容 (OCR):")
        if elem['text_content']['has_text']:
            for idx, text in enumerate(elem['text_content']['ocr_text'], 1):
                print(f"     - 文本{idx}: {text}")
        else:
            print(f"     - 无可识别文本")
        
        # 详细描述
        print(f"  6. 多维度描述:")
        print(f"     {elem['descriptions']['combined'].replace('\n', '\n     ')}")
        
        print(f"  {'-'*60}")
    
    # 性能汇总
    total_duration = (time.perf_counter() - t_load_start) * 1000
    print(f"\n{'='*80}")
    print(f"性能汇总:")
    print(f"{'='*80}")
    print(f"  - 模型加载耗时: {load_duration:.0f}ms")
    print(f"  - 元素检测耗时: {detect_duration:.0f}ms (其中YOLO: {yolo_duration:.0f}ms)")
    print(f"  - 描述生成耗时: {desc_duration:.0f}ms")
    print(f"  - 总处理耗时: {total_duration:.0f}ms")
    print(f"  - 平均每个元素处理耗时: {desc_duration/len(element_details):.0f}ms (共{len(element_details)}个元素)")
    
    # 可选：保存结果到JSON文件
    save_results = input("\n是否将结果保存为JSON文件？(y/n): ").strip().lower()
    if save_results == 'y':
        output_file = image_path.parent / f"{image_path.stem}_omniparser_result.json"
        result_data = {
            "image_info": {
                "filename": image_path.name,
                "path": str(image_path),
                "resolution": [img_width, img_height]
            },
            "performance": {
                "model_load_ms": load_duration,
                "detection_ms": detect_duration,
                "description_ms": desc_duration,
                "total_ms": total_duration
            },
            "global_description": global_caption,
            "elements": element_details
        }
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, ensure_ascii=False, indent=2)
        print(f"结果已保存到: {output_file}")

if __name__ == "__main__":
    # 补充Counter导入（用于类别统计）
    from collections import Counter
    
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    test_omniparser(IMAGE_PATH)