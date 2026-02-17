"""
GLM-OCR 测试脚本 - 智谱 AI OCR 模型
支持 API 调用和本地部署两种方式
"""

import sys
import time
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from PIL import Image
    import base64
    import io
except ImportError as e:
    print(f"Missing dependencies: {e}")
    sys.exit(1)

IMAGE_PATH = PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg"

def image_to_base64(image_path: Path) -> str:
    """将图片转换为 base64"""
    with open(image_path, 'rb') as f:
        return base64.b64encode(f.read()).decode('utf-8')

def test_glm_ocr_api(image_path: Path, api_key: str = None):
    """
    使用 GLM-OCR API 进行测试
    
    需要：
    1. 安装 SDK: pip install zhipuai
    2. 获取 API Key: https://open.bigmodel.cn/
    """
    print(f"\n{'='*70}")
    print(f"GLM-OCR API 测试")
    print(f"{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"图片尺寸: {Image.open(image_path).size}")
    print(f"{'='*70}\n")
    
    # 检查 API Key
    if not api_key:
        api_key = os.getenv('ZHIPUAI_API_KEY')
    
    if not api_key:
        print("❌ 未找到 API Key")
        print("\n请设置环境变量或传入 API Key:")
        print("  方式1: export ZHIPUAI_API_KEY='your_api_key'")
        print("  方式2: test_glm_ocr_api(image_path, api_key='your_key')")
        print("\n获取 API Key: https://open.bigmodel.cn/")
        return None, 0, "缺少 API Key"
    
    try:
        from zhipuai import ZhipuAI
    except ImportError:
        print("❌ 未安装 zhipuai SDK")
        print("\n请安装: pip install zhipuai")
        return None, 0, "缺少 SDK"
    
    # 初始化客户端
    print("[1/2] 初始化 GLM-OCR 客户端...")
    client = ZhipuAI(api_key=api_key)
    print("✓ 客户端初始化完成\n")
    
    # 转换图片为 base64
    print("[2/2] 执行 OCR 识别...")
    t_start = time.perf_counter()
    
    try:
        img_base64 = image_to_base64(image_path)
        
        response = client.chat.completions.create(
            model="glm-ocr-2m",  # GLM-OCR 专用模型
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/jpeg;base64,{img_base64}"
                            }
                        },
                        {
                            "type": "text",
                            "text": "请识别图片中的所有文字，按照从上到下、从左到右的顺序输出。"
                        }
                    ]
                }
            ],
            stream=False
        )
        
        result = response.choices[0].message.content
        elapsed = time.perf_counter() - t_start
        
        print(f"✓ OCR 完成 ({elapsed:.1f}s)\n")
        
        return result, elapsed, "成功"
        
    except Exception as e:
        elapsed = time.perf_counter() - t_start
        print(f"✗ OCR 失败 ({elapsed:.1f}s)")
        print(f"  错误: {e}")
        return None, elapsed, f"失败: {str(e)[:100]}"

def test_glm_ocr_local(image_path: Path):
    """
    使用本地部署的 GLM-OCR 模型
    
    需要：
    1. 下载模型: ollama pull glm-ocr
    2. 或使用 transformers 加载模型
    """
    print(f"\n{'='*70}")
    print(f"GLM-OCR 本地测试")
    print(f"{'='*70}")
    print(f"测试图片: {image_path.name}")
    print(f"{'='*70}\n")
    
    # 尝试 Ollama
    try:
        import subprocess
        import json
        
        print("[方式1] 尝试使用 Ollama...")
        
        # 检查 ollama 是否可用
        result = subprocess.run(
            ["ollama", "list"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0:
            print("✓ Ollama 可用")
            
            # 检查 glm-ocr 模型是否已下载
            if "glm-ocr" in result.stdout:
                print("✓ GLM-OCR 模型已安装")
                
                # 转换图片为 base64
                img_base64 = image_to_base64(image_path)
                
                t_start = time.perf_counter()
                
                # 调用 glm-ocr
                cmd = ["ollama", "run", "glm-ocr", "--", f"data:image/jpeg;base64,{img_base64}"]
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                
                elapsed = time.perf_counter() - t_start
                
                if result.returncode == 0:
                    print(f"✓ OCR 完成 ({elapsed:.1f}s)\n")
                    return result.stdout, elapsed, "成功"
                else:
                    return None, elapsed, f"Ollama 执行失败: {result.stderr}"
            else:
                print("❌ GLM-OCR 模型未安装")
                print("\n请安装: ollama pull glm-ocr")
                return None, 0, "模型未安装"
        else:
            print("  Ollama 不可用")
    except Exception as e:
        print(f"  方式1 失败: {e}")
    
    # 提示其他方式
    print("\n本地部署方式:")
    print("1. Ollama (推荐):")
    print("   curl https://ollama.ai/install.sh | sh")
    print("   ollama pull glm-ocr")
    print("\n2. vLLM (生产级):")
    print("   pip install vllm")
    print("   # 从 HuggingFace 下载模型")
    print("\n3. SGLang:")
    print("   pip install sglang")
    
    return None, 0, "本地部署需要额外配置"

def main():
    """主函数"""
    if not IMAGE_PATH.exists():
        print(f"错误: 图片不存在 ({IMAGE_PATH})")
        return
    
    print(f"\n{'='*70}")
    print(f"GLM-OCR 综合测试")
    print(f"{'='*70}")
    print("\nGLM-OCR 特点:")
    print("  - 参数量: 0.9B (轻量级)")
    print("  - 支持: 文本、表格、公式、手写体、多语言")
    print("  - 输出格式: 文本、Markdown、JSON、HTML、Excel、PDF")
    print(f"{'='*70}\n")
    
    results = {}
    
    # 测试 API 调用
    print("=" * 70)
    print("测试1: GLM-OCR API")
    print("=" * 70)
    
    text, time_cost, status = test_glm_ocr_api(IMAGE_PATH)
    results["GLM-OCR API"] = {
        "text": text,
        "time": time_cost,
        "status": status
    }
    
    if text:
        print("\n识别结果:")
        print("-" * 70)
        print(text[:500] + ("..." if len(text) > 500 else ""))
        print("-" * 70)
    
    # 测试本地部署
    print("\n" + "=" * 70)
    print("测试2: GLM-OCR 本地部署")
    print("=" * 70)
    
    text, time_cost, status = test_glm_ocr_local(IMAGE_PATH)
    results["GLM-OCR Local"] = {
        "text": text,
        "time": time_cost,
        "status": status
    }
    
    if text:
        print("\n识别结果:")
        print("-" * 70)
        print(text[:500] + ("..." if len(text) > 500 else ""))
        print("-" * 70)
    
    # 总结
    print("\n" + "=" * 70)
    print("测试总结")
    print("=" * 70)
    
    for method, result in results.items():
        print(f"\n{method}:")
        print(f"  状态: {result['status']}")
        if result['time'] > 0:
            print(f"  耗时: {result['time']:.2f}s")
        if result['text']:
            print(f"  字符数: {len(result['text'])}")
    
    print(f"\n{'='*70}")
    print("GLM-OCR 使用建议:")
    print("="*70)
    print("\n✅ 推荐使用 GLM-OCR API (最简单)")
    print("  - 优点: 无需部署、开箱即用、持续更新")
    print("  - 适合: 小规模调用、快速验证")
    print("  - 获取 API Key: https://open.bigmodel.cn/")
    print("\n✅ 本地部署 (大规模/隐私需求)")
    print("  - 优点: 数据安全、成本可控、高并发")
    print("  - 适合: 生产环境、大规模处理")
    print("  - 推荐: Ollama (简单) / vLLM (高性能)")
    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    main()
