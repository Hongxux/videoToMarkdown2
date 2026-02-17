"""
PaddleOCR-VL-1.5 测试脚本
测试百度 PaddleOCR-VL 多模态 OCR 模型
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from PIL import Image
    import torch
except ImportError as e:
    print(f"Missing dependencies: {e}")
    sys.exit(1)

IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

def test_paddleocr_vl(image_path: Path):
    """测试 PaddleOCR-VL-1.5"""
    print(f"\n{'='*70}")
    print(f"PaddleOCR-VL-1.5 测试")
    print(f"{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"图片路径: {image_path}")
    
    image = Image.open(image_path)
    print(f"图片尺寸: {image.size}")
    print(f"{'='*70}\n")
    
    print("PaddleOCR-VL-1.5 特点:")
    print("  - 多模态视觉模型")
    print("  - 支持 OCR、文档理解、表格识别")
    print("  - 基于 PaddlePaddle 和视觉语言模型")
    print(f"{'='*70}\n")
    
    # 加载模型
    print("[1/2] 加载 PaddleOCR-VL-1.5 模型...")
    t_load = time.perf_counter()
    
    try:
        # 方式1: 通过 transformers
        try:
            from transformers import AutoModel, AutoTokenizer
            
            model_id = "paddlepaddle/PaddleOCR-VL-1.5"  # 模型 ID
            
            print(f"  正在从 HuggingFace 加载: {model_id}")
            
            tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
            model = AutoModel.from_pretrained(
                model_id,
                trust_remote_code=True,
                device_map="auto" if torch.cuda.is_available() else "cpu"
            ).eval()
            
            load_time = time.perf_counter() - t_load
            device = "GPU" if torch.cuda.is_available() else "CPU"
            print(f"✓ 模型加载完成 ({load_time:.1f}s)")
            print(f"  设备: {device}\n")
            
            # 执行 OCR
            print("[2/2] 执行 OCR 识别...")
            t_ocr = time.perf_counter()
            
            # PaddleOCR-VL 的使用方式
            prompt = "请识别图片中的所有文字内容"
            
            # 根据模型 API 调用
            inputs = tokenizer(prompt, return_tensors="pt")
            
            with torch.no_grad():
                outputs = model.generate(
                    **inputs,
                    images=image,
                    max_new_tokens=1024
                )
                result = tokenizer.decode(outputs[0], skip_special_tokens=True)
            
            ocr_time = time.perf_counter() - t_ocr
            print(f"✓ OCR 完成 ({ocr_time:.1f}s)\n")
            
            # 显示结果
            print(f"{'='*70}")
            print("OCR 识别结果")
            print(f"{'='*70}\n")
            print(result)
            print(f"\n{'='*70}")
            
            # 性能统计
            print("\n性能统计:")
            print(f"  模型加载: {load_time:.2f}s")
            print(f"  OCR 识别: {ocr_time:.2f}s")
            print(f"  总耗时: {load_time + ocr_time:.2f}s")
            print(f"  字符数: {len(result)}")
            print(f"{'='*70}\n")
            
        except Exception as e1:
            print(f"✗ transformers 加载失败: {e1}")
            
            # 方式2: 通过 paddlepaddle
            print("\n尝试通过 PaddlePaddle 加载...")
            try:
                import paddle
                from paddlenlp import Taskflow
                
                # 使用 PaddleNLP 的 Taskflow
                ocr = Taskflow("document Understanding", model="paddleocr-vl-1.5")
                
                load_time = time.perf_counter() - t_load
                print(f"✓ 模型加载完成 ({load_time:.1f}s)\n")
                
                print("[2/2] 执行 OCR 识别...")
                t_ocr = time.perf_counter()
                
                result = ocr(str(image_path))
                
                ocr_time = time.perf_counter() - t_ocr
                print(f"✓ OCR 完成 ({ocr_time:.1f}s)\n")
                
                # 显示结果
                print(f"{'='*70}")
                print("OCR 识别结果")
                print(f"{'='*70}\n")
                print(result)
                print(f"\n{'='*70}")
                
            except Exception as e2:
                print(f"✗ PaddlePaddle 加载失败: {e2}")
                
                print("\n" + "="*70)
                print("PaddleOCR-VL-1.5 使用说明")
                print("="*70)
                print("\nPaddleOCR-VL 是百度推出的多模态文档理解模型")
                print("\n安装方式:")
                print("\n方式1: 通过 transformers")
                print("  pip install transformers torch")
                print("  # 从 HuggingFace 下载")
                print("\n方式2: 通过 PaddlePaddle (推荐)")
                print("  pip install paddlepaddle paddlenlp")
                print("  # 使用 PaddleNLP Taskflow")
                print("\n方式3: 通过 Ollama")
                print("  # 需要更新 Ollama 版本")
                print("  ollama pull paddleocr-vl")
                print("\n注意: 模型可能需要 GPU 和较大内存")
                return
            
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    test_paddleocr_vl(IMAGE_PATH)
