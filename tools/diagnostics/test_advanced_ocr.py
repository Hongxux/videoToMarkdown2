"""
Advanced OCR & UI Understanding Test Script
===========================================
Phase 3: Testing Got-OCR2.0 and OmniParser (Experimental)

Usage:
    python tools/diagnostics/test_advanced_ocr.py
"""

import asyncio
import base64
import json
import os
import sys
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Try imports
try:
    import torch
    import torch
    from transformers import AutoModel, AutoTokenizer, AutoProcessor, AutoModelForCausalLM
    from PIL import Image
except ImportError:
    print("Missing dependencies: pip install torch transformers pillow")
    sys.exit(1)

try:
    from services.python_grpc.src.content_pipeline.infra.llm.llm_client import LLMClient
except ImportError:
    LLMClient = None

# Test Images
TEST_IMAGES: List[Path] = [
    PROJECT_ROOT / "var/storage/storage/5dd689b51667d593eb2e36d8b2f8d204/assets/SU005/SU005_ss_route_008.jpg",
    PROJECT_ROOT / "var/storage/storage/5dd689b51667d593eb2e36d8b2f8d204/assets/SU005/SU005_ss_route_009.jpg",
    PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU018/SU018_ss_island_004.jpg",
    PROJECT_ROOT / "var/storage/storage/65453b3e35f62593c19f79150a89c929/assets/SU023/SU023_ss_island_005.jpg",
]

@dataclass
class ImageTestResult:
    image: str = ""
    method: str = ""
    has_concrete_knowledge: Optional[bool] = None
    confidence: float = 0.0
    img_description: str = ""
    elapsed_ms: float = 0.0
    ok: bool = False
    error: str = ""
    raw_output: str = ""
    prompt_sent_to_llm: str = ""

# -------------------------------------------------------------------------
# Helper: DeepSeek Judgment
# -------------------------------------------------------------------------
async def assess_concrete_knowledge_with_llm(description: str) -> Tuple[bool, str, str]:
    if not LLMClient:
        return False, "LLMClient not available", ""

    client = LLMClient(model="deepseek-chat", temperature=0.0)
    prompt = f"""
    Based on the image description/content provided below, determine whether the image contains **Concrete Knowledge**.
    
    Image Content (OCR/Description):
    {description}
    
    Judgment Rules:
    **POSITIVE (Contains Concrete Knowledge)**:
    - Educational content: photos of real objects, specimens, lab equipment, anatomical diagrams, structural diagrams, maps.
    - Mathematical formulas: equations, derivation steps.
    - Functional diagrams: block diagrams, logic flowcharts, mind maps.
    - Specific interfaces: software screenshots, IDEs, terminals showing actual code/ops.
    - Data visualizations: charts, graphs.
    
    **NEGATIVE (No Concrete Knowledge)**:
    - Pure text without functional graphics.
    - Decorative images only.
    - Simple UI frames without specific content.
    
    Return a JSON object with:
    - "has_concrete_knowledge": boolean
    - "reason": string
    """
    try:
        data, _, _ = await client.complete_json(prompt)
        return data.get("has_concrete_knowledge", False), data.get("reason", ""), prompt
    except Exception as e:
        return False, f"LLM Error: {e}", prompt

# -------------------------------------------------------------------------
# Method H: Got-OCR2.0
# -------------------------------------------------------------------------
async def run_got_ocr2_test(image_paths: List[Path]) -> List[ImageTestResult]:
    """方案 H：Got-OCR2.0 (Stepfun-AI)"""
    results = []
    print("[方案H] 正在加载 Got-OCR2.0 模型 (需下载 ~1.4GB)...")
    t_load = time.perf_counter()
    
    try:
        model_path = PROJECT_ROOT / "var/models/got-ocr2"
        if not model_path.exists():
            # Fallback to HF hub if local not found
            model_path = "stepfun-ai/GOT-OCR2_0"
            print(f"[方案H] 本地模型未找到 ({model_path}), 尝试从 HF 加载...")
        else:
            print(f"[方案H] 加载本地模型: {model_path}")

        tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
        # Simplified loading for higher compatibility
        kw = {"trust_remote_code": True, "use_safetensors": True, "pad_token_id": tokenizer.eos_token_id}
        if torch.cuda.is_available():
            kw["device_map"] = "cuda"
            kw["low_cpu_mem_usage"] = True
        
        model = AutoModel.from_pretrained(model_path, **kw)

        model = model.eval()
        if torch.cuda.is_available():
            model = model.cuda()
        else:
            print("[方案H] Running on CPU")
    except Exception as e:
        import traceback
        traceback.print_exc()
        return [ImageTestResult(image=p.name, method="got_ocr2", error=f"加载失败: {e}") for p in image_paths]

    load_ms = (time.perf_counter() - t_load) * 1000
    print(f"[方案H] 模型加载完成: {load_ms:.0f}ms")

    for path in image_paths:
        r = ImageTestResult(image=path.name, method="got_ocr2")
        t0 = time.perf_counter()
        try:
            # Got-OCR2.0 chat interface
            # 使用 'ocr' 模式获取纯文本输出（更易读）
            res = model.chat(tokenizer, str(path), ocr_type='ocr')
            
            r.img_description = res
            r.raw_output = res
            r.has_concrete_knowledge = None
            r.confidence = 1.0
            r.prompt_sent_to_llm = ""
            
            # 直接打印 OCR 结果
            print(f"\n{'='*60}")
            print(f"图片: {path.name}")
            print(f"{'='*60}")
            print(res)
            print(f"{'='*60}\n")

            r.ok = True
        except Exception as e:
            r.error = str(e)
            import traceback
            traceback.print_exc()
        
        r.elapsed_ms = (time.perf_counter() - t0) * 1000
        results.append(r)
    
    return results

# -------------------------------------------------------------------------
# Method G: OmniParser (Placeholder / Experimental)
# -------------------------------------------------------------------------
# -------------------------------------------------------------------------
# Method G: OmniParser (OmniParser-v2.0)
# -------------------------------------------------------------------------
async def run_omniparser_test(image_paths: List[Path]) -> List[ImageTestResult]:
    """方案 G：OmniParser v2 (YOLO + Florence-2)"""
    results = []
    print("[方案G] 正在加载 OmniParser v2...")
    t_load = time.perf_counter()

    # check dependencies
    try:
        from ultralytics import YOLO
    except ImportError:
        return [ImageTestResult(method="omniparser", error="Missing ultralytics. pip install ultralytics")]

    try:
        # 1. Load YOLO (Icon Detect)
        yolo_path = PROJECT_ROOT / "var/models/omniparser-v2/icon_detect/model.pt"
        if not yolo_path.exists():
             return [ImageTestResult(method="omniparser", error=f"YOLO model not found at {yolo_path}")]
        
        yolo = YOLO(str(yolo_path))
        
        # 2. Load Florence-2 (Icon Caption)
        caption_path = PROJECT_ROOT / "var/models/omniparser-v2/icon_caption"
        if not caption_path.exists():
             return [ImageTestResult(method="omniparser", error=f"Caption model not found at {caption_path}")]

        # Use CPU/GPU logic
        device = "cuda" if torch.cuda.is_available() else "cpu"
        torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32

        caption_model = AutoModelForCausalLM.from_pretrained(
            caption_path, 
            trust_remote_code=True,
            torch_dtype=torch_dtype
        ).to(device)
        caption_processor = AutoProcessor.from_pretrained(caption_path, trust_remote_code=True)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return [ImageTestResult(method="omniparser", error=f"Load Error: {e}")]
    
    load_ms = (time.perf_counter() - t_load) * 1000
    print(f"[方案G] 模型加载完成: {load_ms:.0f}ms")

    # Helper for captioning
    def get_caption(img_crop):
        inputs = caption_processor(text="<CAPTION>", images=img_crop, return_tensors="pt").to(device, torch_dtype)
        generated_ids = caption_model.generate(
            input_ids=inputs["input_ids"],
            pixel_values=inputs["pixel_values"],
            max_new_tokens=50,
            num_beams=3
        )
        generated_text = caption_processor.batch_decode(generated_ids, skip_special_tokens=False)[0]
        parsed_answer = caption_processor.post_process_generation(
            generated_text, 
            task="<CAPTION>", 
            image_size=(img_crop.width, img_crop.height)
        )
        return parsed_answer.get("<CAPTION>", "")

    for path in image_paths:
        r = ImageTestResult(image=path.name, method="omniparser")
        t0 = time.perf_counter()
        
        try:
            image = Image.open(path).convert("RGB")
            
            # Step 1: Detect
            # YOLO inference
            det_results = yolo(image, verbose=False) 
            # Parse results
            detections = []
            if len(det_results) > 0:
                boxes = det_results[0].boxes
                for box in boxes:
                    xyxy = box.xyxy[0].tolist()
                    conf = box.conf[0].item()
                    cls = int(box.cls[0].item())
                    # OmniParser classes? usually 0.. but let's assume it detects 'icon'
                    detections.append({"box": xyxy, "conf": conf, "cls": cls})

            # Sort by Y then X to emulate reading order
            detections.sort(key=lambda x: (x["box"][1], x["box"][0]))

            # Step 2: Crop & Caption
            descriptions = []
            for i, d in enumerate(detections):
                box = d["box"]
                crop = image.crop((int(box[0]), int(box[1]), int(box[2]), int(box[3])))
                cap = get_caption(crop)
                descriptions.append(f"- Item {i+1} [Pos: {int(box[0])},{int(box[1])}]: {cap}")

            # Step 3: Global Caption 
            # (Optional: use Florence-2 Base for global caption too?)
            # Or just use the element list. User wants "Detail".
            # Let's add a global caption too.
            global_cap = get_caption(image)
            
            full_desc = f"Global Description: {global_cap}\n\nDetected UI Elements ({len(detections)}):\n" + "\n".join(descriptions)
            
            r.img_description = full_desc
            r.raw_output = str(detections)

            # 不使用 DeepSeek 判定
            r.has_concrete_knowledge = None
            r.confidence = 1.0
            
            # 直接打印 OmniParser 结果
            print(f"\n{'='*60}")
            print(f"图片: {path.name}")
            print(f"{'='*60}")
            print(full_desc)
            print(f"{'='*60}\n")

            r.ok = True

        except Exception as e:
            r.error = str(e)
            import traceback
            traceback.print_exc()

        r.elapsed_ms = (time.perf_counter() - t0) * 1000
        results.append(r)

    return results

async def main():
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    print(f"[{time.strftime('%H:%M:%S')}] 开始 Advanced OCR 测试...")
    
    # Method H: Got-OCR2
    got_results = await run_got_ocr2_test(TEST_IMAGES)
    
    # Method G: OmniParser
    omni_results = await run_omniparser_test(TEST_IMAGES)
    
    all_results = got_results + omni_results
    
    # Save results to JSON
    output_dir = PROJECT_ROOT / "var/artifacts/benchmarks/advanced_ocr"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"comparison_advanced_{timestamp}.json"
    
    # helper to serializable
    def serialize_result(r):
        return {
            "image": r.image,
            "method": r.method,
            "has_concrete_knowledge": r.has_concrete_knowledge,
            "confidence": r.confidence,
            "elapsed_ms": r.elapsed_ms,
            "img_description": r.img_description,
            "error": r.error,
            "raw_output": r.raw_output,
            "prompt_sent_to_llm": r.prompt_sent_to_llm
        }
        
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump([serialize_result(r) for r in all_results], f, ensure_ascii=False, indent=2)
        print(f"\n📄 结果已保存至: {json_path}")
    except Exception as e:
        print(f"\n❌ 保存结果失败: {e}")

    print("\n" + "="*50)
    print(f"📊 总结 (Got-OCR2.0 + OmniParser):")
    for r in all_results:
        status = "✅" if r.ok else "❌"
        print(f"{status} {r.image} | {r.method} | {r.has_concrete_knowledge} | {r.elapsed_ms:.0f}ms")
        if not r.ok:
            print(f"   Error: {r.error}")

if __name__ == "__main__":
    asyncio.run(main())
