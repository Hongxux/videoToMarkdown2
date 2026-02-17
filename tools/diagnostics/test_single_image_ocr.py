"""
单图片 OCR 测试脚本
测试 Got-OCR2.0 对单张图片的识别效果
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import torch
    from transformers import AutoModel, AutoTokenizer
    from PIL import Image
except ImportError:
    print("Missing dependencies: pip install torch transformers pillow")
    sys.exit(1)

# 测试图片路径
IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

def test_got_ocr2(image_path: Path, ocr_type: str = 'ocr'):
    """
    测试 Got-OCR2.0
    
    Args:
        image_path: 图片路径
        ocr_type: OCR 类型
            - 'format': 输出 LaTeX/Markdown 格式（适合表格、公式）
            - 其他值: 输出纯文本（更易读）
    """
    print(f"\n{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"OCR 模式: {ocr_type}")
    print(f"{'='*70}\n")
    
    print("[1/3] 加载 Got-OCR2.0 模型...")
    t_load = time.perf_counter()
    
    try:
        model_path = PROJECT_ROOT / "var/models/got-ocr2"
        if not model_path.exists():
            print(f"错误: 模型未找到 ({model_path})")
            return
        
        tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
        kw = {"trust_remote_code": True, "use_safetensors": True, "pad_token_id": tokenizer.eos_token_id}
        if torch.cuda.is_available():
            kw["device_map"] = "cuda"
            kw["low_cpu_mem_usage"] = True
        
        model = AutoModel.from_pretrained(model_path, **kw)
        model = model.eval()
        
        if torch.cuda.is_available():
            model = model.cuda()
            print("✓ 使用 GPU")
        else:
            print("✓ 使用 CPU")
            
    except Exception as e:
        print(f"✗ 模型加载失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    load_ms = (time.perf_counter() - t_load) * 1000
    print(f"✓ 模型加载完成 ({load_ms:.0f}ms)\n")
    
    print("[2/3] 执行 OCR 识别...")
    t0 = time.perf_counter()
    
    try:
        result = model.chat(tokenizer, str(image_path), ocr_type=ocr_type)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        print(f"✓ OCR 完成 ({elapsed_ms:.0f}ms)\n")
        
        print("[3/3] OCR 结果:")
        print(f"{'='*70}")
        print(result)
        print(f"{'='*70}\n")
        
        print(f"字符数: {len(result)}")
        print(f"行数: {result.count(chr(10)) + 1}")
        
    except Exception as e:
        print(f"✗ OCR 失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        sys.exit(1)
    
    # 测试纯文本模式
    test_got_ocr2(IMAGE_PATH, ocr_type='ocr')
    
    # 如果需要测试格式化模式，取消下面的注释
    # print("\n\n")
    # test_got_ocr2(IMAGE_PATH, ocr_type='format')
