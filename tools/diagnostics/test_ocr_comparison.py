"""
OCR 模型综合对比测试
对比三种 OCR 方案：Florence-2-Large、Got-OCR2.0、RapidOCR
"""

import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import torch
    from PIL import Image
except ImportError as e:
    print(f"Missing dependencies: {e}")
    sys.exit(1)

IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

def test_florence2_large_ocr(image_path: Path) -> Tuple[str, float, str]:
    """测试 Florence-2-Large OCR"""
    print("\n[1/3] Florence-2-Large OCR")
    print("-" * 70)
    
    try:
        from transformers import AutoModelForCausalLM, AutoProcessor
        
        print("  加载模型...", end=" ")
        t_load = time.perf_counter()
        
        model_id = "microsoft/Florence-2-large"
        device = "cuda" if torch.cuda.is_available() else "cpu"
        torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
        
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            trust_remote_code=True,
            torch_dtype=torch_dtype
        ).to(device)
        processor = AutoProcessor.from_pretrained(model_id, trust_remote_code=True)
        
        load_time = time.perf_counter() - t_load
        print(f"完成 ({load_time:.1f}s)")
        
        print("  执行 OCR...", end=" ")
        t_ocr = time.perf_counter()
        
        image = Image.open(image_path).convert("RGB")
        
        # 使用 <OCR> 任务
        prompt = "<OCR>"
        inputs = processor(text=prompt, images=image, return_tensors="pt").to(device, torch_dtype)
        
        generated_ids = model.generate(
            input_ids=inputs["input_ids"],
            pixel_values=inputs["pixel_values"],
            max_new_tokens=1024,
            num_beams=3
        )
        
        generated_text = processor.batch_decode(generated_ids, skip_special_tokens=False)[0]
        parsed = processor.post_process_generation(
            generated_text,
            task=prompt,
            image_size=image.size
        )
        
        result = parsed.get(prompt, "")
        ocr_time = time.perf_counter() - t_ocr
        
        print(f"完成 ({ocr_time:.1f}s)")
        
        return result, ocr_time, "成功"
        
    except Exception as e:
        return "", 0.0, f"失败: {str(e)[:100]}"

def test_got_ocr2(image_path: Path) -> Tuple[str, float, str]:
    """测试 Got-OCR2.0"""
    print("\n[2/3] Got-OCR2.0")
    print("-" * 70)
    
    try:
        from transformers import AutoModel, AutoTokenizer
        
        print("  加载模型...", end=" ")
        t_load = time.perf_counter()
        
        model_path = PROJECT_ROOT / "var/models/got-ocr2"
        if not model_path.exists():
            return "", 0.0, f"模型未找到: {model_path}"
        
        tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
        model = AutoModel.from_pretrained(
            model_path,
            trust_remote_code=True,
            use_safetensors=True,
            pad_token_id=tokenizer.eos_token_id
        )
        model = model.eval()
        
        if torch.cuda.is_available():
            model = model.cuda()
        
        load_time = time.perf_counter() - t_load
        print(f"完成 ({load_time:.1f}s)")
        
        print("  执行 OCR...", end=" ")
        t_ocr = time.perf_counter()
        
        result = model.chat(tokenizer, str(image_path), ocr_type='ocr')
        
        ocr_time = time.perf_counter() - t_ocr
        print(f"完成 ({ocr_time:.1f}s)")
        
        return result.strip(), ocr_time, "成功"
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return "", 0.0, f"失败: {str(e)[:100]}"

def test_rapid_ocr(image_path: Path) -> Tuple[str, float, str]:
    """测试 RapidOCR"""
    print("\n[3/3] RapidOCR")
    print("-" * 70)
    
    try:
        # 尝试导入 RapidOCR
        try:
            from rapidocr_onnxruntime import RapidOCR
        except ImportError:
            try:
                from rapidocr_openvino import RapidOCR
            except ImportError:
                return "", 0.0, "未安装 RapidOCR (需要: pip install rapidocr-onnxruntime 或 rapidocr-openvino)"
        
        print("  加载模型...", end=" ")
        t_load = time.perf_counter()
        
        engine = RapidOCR()
        
        load_time = time.perf_counter() - t_load
        print(f"完成 ({load_time:.1f}s)")
        
        print("  执行 OCR...", end=" ")
        t_ocr = time.perf_counter()
        
        result, elapse = engine(str(image_path))
        
        ocr_time = time.perf_counter() - t_ocr
        print(f"完成 ({ocr_time:.1f}s)")
        
        if result:
            # RapidOCR 返回格式: [[bbox, text, conf], ...]
            # 提取所有文本并按位置排序
            texts = []
            for item in result:
                if len(item) >= 2:
                    bbox, text = item[0], item[1]
                    # bbox 格式: [[x1,y1], [x2,y2], [x3,y3], [x4,y4]]
                    y = bbox[0][1]  # 使用左上角 y 坐标排序
                    texts.append((y, text))
            
            # 按 y 坐标排序
            texts.sort(key=lambda x: x[0])
            combined_text = "\n".join([t[1] for t in texts])
            
            return combined_text, ocr_time, "成功"
        else:
            return "", ocr_time, "未检测到文字"
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return "", 0.0, f"失败: {str(e)[:100]}"

def print_comparison_table(results: Dict[str, Tuple[str, float, str]]):
    """打印对比结果表格"""
    print("\n" + "=" * 70)
    print("OCR 识别结果对比")
    print("=" * 70)
    
    for model_name, (text, time_cost, status) in results.items():
        print(f"\n【{model_name}】")
        print(f"状态: {status}")
        print(f"耗时: {time_cost:.2f}s")
        print(f"字符数: {len(text)}")
        if text:
            print(f"识别结果:")
            print("-" * 70)
            # 只显示前 500 个字符
            display_text = text[:500] + ("..." if len(text) > 500 else "")
            print(display_text)
        print("-" * 70)

def analyze_results(results: Dict[str, Tuple[str, float, str]]):
    """分析对比结果"""
    print("\n" + "=" * 70)
    print("性能和准确度分析")
    print("=" * 70)
    
    # 速度排名
    speed_ranking = sorted(
        [(name, time_cost) for name, (_, time_cost, status) in results.items() if status == "成功"],
        key=lambda x: x[1]
    )
    
    print("\n速度排名（快→慢）:")
    for i, (name, time_cost) in enumerate(speed_ranking, 1):
        print(f"  {i}. {name}: {time_cost:.2f}s")
    
    # 字符数对比
    print("\n识别字符数:")
    for name, (text, _, status) in results.items():
        if status == "成功":
            print(f"  {name}: {len(text)} 字符")
    
    # 特点总结
    print("\n模型特点总结:")
    print("  Florence-2-Large:")
    print("    - 通用视觉-语言模型，OCR 是其众多能力之一")
    print("    - 适合英文，中文识别能力一般")
    print("    - 模型较大（~1.7GB），推理较慢")
    
    print("\n  Got-OCR2.0:")
    print("    - 专业 OCR 模型，专注于文字识别")
    print("    - 中文识别准确度高，支持公式、表格等")
    print("    - CPU 推理较慢，GPU 加速效果显著")
    
    print("\n  RapidOCR:")
    print("    - 轻量级 OCR，基于 PaddleOCR")
    print("    - 速度快，适合生产环境")
    print("    - 返回文字和位置信息")

def main():
    """主函数"""
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        return
    
    print("=" * 70)
    print("OCR 模型对比测试")
    print("=" * 70)
    print(f"测试图片: {IMAGE_PATH.name}")
    print(f"图片路径: {IMAGE_PATH}")
    
    image = Image.open(IMAGE_PATH)
    print(f"图片尺寸: {image.size}")
    print("=" * 70)
    
    results = {}
    
    # 测试 Florence-2-Large
    text, time_cost, status = test_florence2_large_ocr(IMAGE_PATH)
    results["Florence-2-Large"] = (text, time_cost, status)
    
    # 测试 Got-OCR2.0
    text, time_cost, status = test_got_ocr2(IMAGE_PATH)
    results["Got-OCR2.0"] = (text, time_cost, status)
    
    # 测试 RapidOCR
    text, time_cost, status = test_rapid_ocr(IMAGE_PATH)
    results["RapidOCR"] = (text, time_cost, status)
    
    # 打印对比结果
    print_comparison_table(results)
    
    # 分析结果
    analyze_results(results)
    
    print("\n" + "=" * 70)
    print("测试完成")
    print("=" * 70)

if __name__ == "__main__":
    main()
