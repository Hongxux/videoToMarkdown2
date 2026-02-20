import asyncio
import sys
import json
from pathlib import Path

# Add project root to sys path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import VLVideoAnalyzer
from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config

async def main():
    video_path = sys.argv[1] if len(sys.argv) > 1 else r"d:\videoToMarkdownTest2\var\storage\storage\204064deaadedd8e7f073f509e1916bd\semantic_unit_clips_vl\001_SU001_访问官网并下载 Android Studio_0.00-135.00.mp4"
    if not Path(video_path).exists():
        print(f"Error: Video file not found at {video_path}")
        return

    print("Loading config...")
    config = load_module2_config()
    vl_config = config.get("vl_material_generation", {})
    
    print("Initializing VLVideoAnalyzer...")
    analyzer = VLVideoAnalyzer(vl_config)
    
    print(f"🚀 Calling VL Model in tutorial_stepwise mode on:\n{video_path}")
    print("This may take 1-3 minutes depending on network and video size...\n")
    
    try:
        result = await analyzer.analyze_clip(
            clip_path=video_path,
            semantic_unit_start_sec=0.0,
            semantic_unit_id="SU001",
            analysis_mode="tutorial_stepwise",
        )
        
        print("\n✅ API Response received successfully!")
        
        if result.raw_response_json:
            print("\n" + "="*80)
            print("📦 Raw JSON from Model:")
            print("="*80)
            print(json.dumps(result.raw_response_json, indent=2, ensure_ascii=False))
        else:
            print("\n⚠️ No structured JSON data was parsed from the model.")
            print(result)
            
        if hasattr(result, 'token_usage'):
            print(f"\n🪙 Token Usage: {result.token_usage}")
            
    except Exception as e:
        print(f"\n❌ Exception during analysis analysis:\n{e}")

if __name__ == "__main__":
    # Workaround Windows console encoding issue
    if sys.stdout.encoding.lower() != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    asyncio.run(main())
