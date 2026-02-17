"""
结构化 OCR 提取工具
使用 YOLO + RapidOCR 提取图片中的区域信息
输出格式：区域编号、区域类型、区域坐标、识别文本
"""

import sys
import time
import json
from pathlib import Path
from typing import List, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from PIL import Image
    from rapidocr_onnxruntime import RapidOCR
    from ultralytics import YOLO
except ImportError as e:
    print(f"缺少依赖: {e}")
    print("\n请安装:")
    print("  pip install rapidocr-onnxruntime ultralytics pillow")
    sys.exit(1)

# 配置
IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"
YOLO_MODEL = PROJECT_ROOT / "var/models/omniparser-v2/icon_detect/model.pt"

# 类型映射（根据 YOLO 模型）
TYPE_MAPPING = {
    0: "text",      # 文本
    1: "icon",      # 图标
    2: "button",    # 按钮
    3: "input",     # 输入框
    4: "label",     # 标签
    # 可根据实际 YOLO 模型调整
}

def detect_regions(image_path: Path, model_path: Path, conf_threshold: float = 0.3) -> List[Dict]:
    """
    使用 YOLO 检测图片中的不同区域
    
    Returns:
        List[Dict]: 检测到的区域列表，每个区域包含 type 和 bbox
    """
    print(f"[1/3] 加载 YOLO 模型...")
    model = YOLO(str(model_path))
    print(f"✓ YOLO 加载完成\n")
    
    print(f"[2/3] 检测 UI 区域...")
    results = model(str(image_path), conf=conf_threshold, verbose=False)
    
    regions = []
    for result in results:
        boxes = result.boxes
        for i, box in enumerate(boxes):
            # 获取坐标
            x1, y1, x2, y2 = box.xyxy[0].cpu().numpy()
            
            # 获取类别
            cls_id = int(box.cls[0].cpu().numpy())
            confidence = float(box.conf[0].cpu().numpy())
            
            # 类型名称
            type_name = TYPE_MAPPING.get(cls_id, f"unknown_{cls_id}")
            
            regions.append({
                "type": type_name,
                "bbox": [float(x1), float(y1), float(x2), float(y2)],
                "conf": confidence,
                "cls_id": cls_id
            })
    
    print(f"✓ 检测到 {len(regions)} 个区域\n")
    return regions

def extract_text_from_region(image: Image.Image, bbox: List[float], ocr_engine: RapidOCR) -> str:
    """
    从指定区域提取文字
    
    Args:
        image: PIL Image 对象
        bbox: [x1, y1, x2, y2]
        ocr_engine: RapidOCR 实例
    
    Returns:
        str: 识别的文字
    """
    x1, y1, x2, y2 = bbox
    
    # 裁剪区域（添加小边距）
    margin = 5
    crop_box = (
        max(0, x1 - margin),
        max(0, y1 - margin),
        min(image.width, x2 + margin),
        min(image.height, y2 + margin)
    )
    
    cropped = image.crop(crop_box)
    
    # OCR 识别
    result, _ = ocr_engine(cropped)
    
    if result:
        # 提取所有文字并合并
        texts = [line[1] for line in result]
        return " ".join(texts)
    else:
        return ""

def extract_structured_ocr(
    image_path: Path,
    yolo_model_path: Path,
    output_json: Path = None,
    conf_threshold: float = 0.3
) -> List[Dict[str, Any]]:
    """
    主函数：提取结构化 OCR 结果
    
    Args:
        image_path: 图片路径
        yolo_model_path: YOLO 模型路径
        output_json: 输出 JSON 文件路径（可选）
        conf_threshold: YOLO 置信度阈值
    
    Returns:
        List[Dict]: 结构化结果列表
    """
    print(f"\n{'='*70}")
    print(f"结构化 OCR 提取")
    print(f"{'='*70}")
    print(f"图片: {image_path.name}")
    print(f"YOLO 模型: {yolo_model_path.name}")
    print(f"置信度阈值: {conf_threshold}")
    print(f"{'='*70}\n")
    
    t_start = time.perf_counter()
    
    # 打开图片
    image = Image.open(image_path)
    print(f"图片尺寸: {image.size}\n")
    
    # 1. YOLO 检测区域
    regions = detect_regions(image_path, yolo_model_path, conf_threshold)
    
    if not regions:
        print("⚠️ 未检测到任何区域")
        return []
    
    # 2. 加载 RapidOCR
    print(f"[3/3] 使用 RapidOCR 提取文字...")
    ocr_engine = RapidOCR()
    
    # 3. 提取每个区域的文字
    final_result = []
    for idx, region in enumerate(regions):
        print(f"  处理区域 {idx+1}/{len(regions)}...", end=" ")
        
        text = extract_text_from_region(image, region['bbox'], ocr_engine)
        
        final_result.append({
            "区域编号": idx + 1,
            "区域类型": region['type'],
            "区域坐标": region['bbox'],  # [x1, y1, x2, y2]
            "识别文本": text if text else "无",
            "置信度": region['conf']
        })
        
        print(f"✓ ({len(text)} 字符)")
    
    total_time = time.perf_counter() - t_start
    
    # 4. 输出结果
    print(f"\n{'='*70}")
    print(f"提取完成")
    print(f"{'='*70}")
    print(f"总区域数: {len(final_result)}")
    print(f"总耗时: {total_time:.2f}s")
    print(f"{'='*70}\n")
    
    # 5. 显示结果
    print("结构化结果:")
    print("-" * 70)
    for item in final_result:
        print(f"\n【区域 #{item['区域编号']}】")
        print(f"  类型: {item['区域类型']}")
        print(f"  坐标: [{item['区域坐标'][0]:.0f}, {item['区域坐标'][1]:.0f}, "
              f"{item['区域坐标'][2]:.0f}, {item['区域坐标'][3]:.0f}]")
        print(f"  置信度: {item['置信度']:.2f}")
        if item['识别文本'] and item['识别文本'] != "无":
            text_preview = item['识别文本'][:50] + ("..." if len(item['识别文本']) > 50 else "")
            print(f"  文本: {text_preview}")
        else:
            print(f"  文本: (无)")
    
    # 6. 保存 JSON
    if output_json:
        with open(output_json, 'w', encoding='utf-8') as f:
            json.dump(final_result, f, ensure_ascii=False, indent=2)
        print(f"\n✓ 结果已保存到: {output_json}")
    
    print(f"\n{'='*70}\n")
    
    return final_result

if __name__ == "__main__":
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    if not YOLO_MODEL.exists():
        print(f"错误: YOLO 模型不存在 ({YOLO_MODEL})")
        sys.exit(1)
    
    # 输出 JSON 路径
    output_json = IMAGE_PATH.parent / f"{IMAGE_PATH.stem}_structured_ocr.json"
    
    # 执行提取
    result = extract_structured_ocr(
        image_path=IMAGE_PATH,
        yolo_model_path=YOLO_MODEL,
        output_json=output_json,
        conf_threshold=0.3
    )
    
    print(f"提取了 {len(result)} 个区域的结构化信息")
