
import json
import logging
import sys
import os
import asyncio
import subprocess
import shutil
from dataclasses import dataclass
from typing import List, Dict, Optional

# Adjust path to include src and project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../services/python_grpc/src")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

try:
    from content_pipeline.infra.llm.llm_client import LLMClient
    from content_pipeline.phase2a.materials.vl_video_analyzer import VLVideoAnalyzer
except ImportError as e:
    print(f"Could not import modules: {e}")
    sys.exit(1)

# Mock or Minimal implementation of the filters for testing logic
# In a real scenario, this would import from the actual codebase

@dataclass
class SemanticUnit:
    title: str
    full_text: str
    unit_id: str
    start_sec: float
    end_sec: float
    video_path: str # Added video path

@dataclass
class ActionClip:
    clip_id: str
    start_sec: float
    end_sec: float
    knowledge_type: str

class ContentExtractor:
    @staticmethod
    def extract_transcript(full_text: str, unit_start: float, clip_start: float, clip_end: float, buffer_sec: float = 10.0) -> str:
        """
        Extracts the transcript relevant to the clip with some buffer context.
        """
        return full_text

class SemanticActionFilter:
    def __init__(self):
        self.system_prompt_template = "" # Will be loaded from file

    def construct_prompt(self, unit: SemanticUnit, clip: ActionClip) -> str:
        transcript = ContentExtractor.extract_transcript(unit_start=unit.start_sec, full_text=unit.full_text, clip_start=clip.start_sec, clip_end=clip.end_sec)
        duration = clip.end_sec - clip.start_sec
        
        user_prompt = f"""
Input Data:
1. Semantic Unit Title: {unit.title}
2. Clip Transcript: {transcript}
3. Clip Duration: {duration}s
"""
        return user_prompt.strip()

class VideoSlicer:
    @staticmethod
    def slice_video(input_path: str, start_sec: float, end_sec: float, output_path: str):
        """
        Slices a video clip using ffmpeg.
        """
        duration = end_sec - start_sec
        cmd = [
            "ffmpeg",
            "-y", # Overwrite output
            "-ss", str(start_sec),
            "-i", input_path,
            "-t", str(duration),
            "-c:v", "libx264", # Re-encode to ensure keyframes are correct or use copy if precise enough? 
            # Re-encoding is safer for VL but slower. Copy is fast but might have keyframe issues.
            # Let's use re-encoding with fast preset for testing.
            "-preset", "ultrafast",
            "-c:a", "aac",
            output_path
        ]
        # Hide output unless error
        process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if process.returncode != 0:
            raise RuntimeError(f"FFmpeg failed: {process.stderr.decode()}")
        return output_path

def get_test_cases():
    su015_text = "但是还有个问题，假如这个问题我们再变一下，比如说处理原始文档不只有PDF，还有可能是Word文档、TXT文档。\n输出的格式也可能是HTML、PDF甚至是一张图片，难道你要给这些所有的排列组合都写一套处理流程吗？这显然是不合适的。\n你也可以写一堆if-else做判断，但是如果你仍然希望用函数以自然语言的方式触发这个任务，不形成这个体验，这个时候就又不好用程序来判断分支了，那该怎么办呢？\n你可以这样设计：准备一个目录，把所有可能涉及到的转换脚本全都写好放在这，然后写个统一的调用文件，把整体的流程描述清楚，并且告诉AI根据文件的格式。\n再给AI下达任务之前，加上这么一句话：先读取刚刚我们写好的那一大串要求，再按照要求完成任务。灵活选择指定的脚本，再给AI的下达任务之前加上这么一句话：先读取刚刚我们写好的那一大串要求，再按照要求完成任务。这样整个过程就既保证了一定的灵活性，同时又变得比较可控。\n但是这不就又来了优化空间吗？我可以提前约定好某个指定的位置，在AIM中写死一段程序去读取这个位置的skill的md。还是想到与把这句话固化成了一段程序，这样就不用每次都加这么一句废话了。\n虽然你也知道这破坏人好像就是把提示词放两个地方存起来，但小小还是给他写个新名字，就叫做skill即AIM的技能。\n好家伙，这是往游戏里的英雄方向设计了呀。"
    
    # Update this path to the actual video file location
    video_path = r"d:\videoToMarkdownTest2\var\storage\storage\65453b3e35f62593c19f79150a89c929\video.mp4"

    su015 = SemanticUnit(
        title="实现灵活文件格式处理",
        full_text=su015_text,
        unit_id="SU015",
        start_sec=450.0,
        end_sec=533.0,
        video_path=video_path
    )
    
    clips = [
        ActionClip("SU015_clip_action_001", 461.6, 473.0, "推演"), 
        ActionClip("SU015_clip_action_003", 486.6, 492.0, "实操"),
        ActionClip("SU015_clip_action_007", 512.0, 518.0, "过程性知识") 
    ]
    
    return su015, clips

async def main():
    # Setup LLM Client
    try:
        llm_client = LLMClient() 
    except Exception as e:
        print(f"Failed to init LLMClient: {e}")
        return

    # Setup VL Analyzer
    try:
        from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import load_module2_config
        
        config = load_module2_config()
        vl_config = config.get("vl_material_generation", {})
        
        if not vl_config.get("api", {}).get("api_key"):
            print("WARNING: 'api_key' not found in config['vl_material_generation']['api']. VL calls may fail.")

        vl_analyzer = VLVideoAnalyzer(config=vl_config)
        
        # Override prompts
        semantic_prompt_path = os.path.join(os.path.dirname(__file__), "../services/python_grpc/src/content_pipeline/prompts/vl/video_analysis/semantic_action_filter.md")
        vl_prompt_path = os.path.join(os.path.dirname(__file__), "../services/python_grpc/src/content_pipeline/prompts/vl/video_analysis/legacy_action_filter_system.md")
        
        if os.path.exists(vl_prompt_path):
            with open(vl_prompt_path, "r", encoding="utf-8") as f:
                vl_analyzer.prompt_template = f.read()
            # print(f"Overrode VL prompt template with content from {vl_prompt_path}")
        
        # IMPORTANT: Disable hardcoded constraints that conflict with legacy prompt schema
        vl_analyzer._constraints_default = ""
        vl_analyzer._constraints_tutorial = ""
        
        print(f"VL Analyzer initialized with model: {vl_config.get('api', {}).get('model')}")

    except Exception as e:
        print(f"Failed to init VLVideoAnalyzer from config: {e}")
        return

    filter_logic = SemanticActionFilter()
    
    # Load semantic prompt
    if os.path.exists(semantic_prompt_path):
         with open(semantic_prompt_path, "r", encoding="utf-8") as f:
            filter_logic.system_prompt_template = f.read()
    
    unit, clips = get_test_cases()
    
    print(f"Testing Semantic Filter & VL Filter for Unit: {unit.title} ({unit.unit_id})")
    print("-" * 50)
    
    temp_dir = os.path.join(os.path.dirname(__file__), "temp_clips")
    os.makedirs(temp_dir, exist_ok=True)

    for clip in clips:
        print(f"\n>>> Clip ID: {clip.clip_id}")
        print(f"Time: {clip.start_sec} - {clip.end_sec} ({clip.end_sec - clip.start_sec}s)")
        print(f"Knowledge Type (Reference): {clip.knowledge_type}")
        
        # 1. Semantic Filter
        print("\n[Semantic Filter] Running...")
        user_prompt = filter_logic.construct_prompt(unit, clip)
        try:
            response_data, metadata, _ = await llm_client.complete_json(
                prompt=user_prompt,
                system_message=filter_logic.system_prompt_template,
                model="deepseek-chat",
            )
            print("Semantic Response:")
            print(json.dumps(response_data, indent=2, ensure_ascii=False))
            # print(f"Latency: {metadata.latency_ms:.2f}ms")
        except Exception as e:
            # print(f"Semantic Filter Failed: {e}")
            pass

        # 2. VL Filter
        print("\n[VL Filter] Running...")
        
        # Slice video
        temp_output_path = os.path.join(temp_dir, f"{clip.clip_id}.mp4")
        try:
            # print(f"Slicing video to {temp_output_path}...")
            VideoSlicer.slice_video(unit.video_path, clip.start_sec, clip.end_sec, temp_output_path)
            
            # Prepare extra prompt for VL
            vl_extra_prompt = f"Semantic Unit Title: {unit.title}\nClip Duration: {clip.end_sec - clip.start_sec}s"

            # Call VL
            # print("Calling VL Video Analyzer...")
            vl_result = await vl_analyzer.analyze_clip(
                clip_path=temp_output_path,
                semantic_unit_id=unit.unit_id,
                semantic_unit_start_sec=unit.start_sec,
                extra_prompt=vl_extra_prompt
            )
            
            if vl_result.success:
                print("VL Response (Raw JSON):")
                print(json.dumps(vl_result.raw_response_json, indent=2, ensure_ascii=False))
                
                # Check for legacy 'keep' field
                keep_val = "N/A"
                if vl_result.raw_response_json and isinstance(vl_result.raw_response_json, list):
                    item = vl_result.raw_response_json[0]
                    if isinstance(item, dict):
                        keep_val = item.get("keep", item.get("keep_flag", "N/A"))
                elif isinstance(vl_result.raw_response_json, dict):
                    keep_val = vl_result.raw_response_json.get("keep", "N/A")
                
                print(f"Final Decision -> VL Keep: {keep_val}")
                # print(f"Token Usage: {vl_result.token_usage}")
            else:
                print(f"VL Failed: {vl_result.error_msg}")

        except Exception as e:
            # print(f"VL Filter Failed: {e}")
            import traceback
            traceback.print_exc()
        
        print("-" * 30)

    # Cleanup temp dir
    # shutil.rmtree(temp_dir)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
