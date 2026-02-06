import asyncio
import sys
import json
from pathlib import Path

# 添加项目路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from MVP_Module2_HEANCING.module2_content_enhancement.vl_video_analyzer import VLVideoAnalyzer
from MVP_Module2_HEANCING.module2_content_enhancement.config_loader import load_module2_config

async def verify():
    video_path = r"D:\videoToMarkdownTest2\storage\20225626c2a19253c4121f684ecdff12\semantic_unit_clips\007_SU007_Quickstart模式配置步骤_130.00-150.00.mp4"
    config = load_module2_config()
    vl_config = config.get("vl_material_generation", {})
    analyzer = VLVideoAnalyzer(vl_config)
    
    print(f"Analyzer Temperature: {analyzer.temperature}")
    
    response = await analyzer.analyze_clip(
        clip_path=video_path,
        semantic_unit_start_sec=130.0,
        semantic_unit_id="SU007"
    )
    
    print(f"Success: {response.success}")
    if response.clip_requests:
        for clip in response.clip_requests:
            print(f"Clip: {clip['clip_id']}, Start: {clip['start_sec']}, End: {clip['end_sec']}, Type: {clip['knowledge_type']}")
    else:
        print("No clip requests.")

if __name__ == "__main__":
    asyncio.run(verify())
