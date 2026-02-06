"""
增强调试脚本 - 详细追踪时间戳转换过程 (验证版)
"""

import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime

# 添加项目路径
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from MVP_Module2_HEANCING.module2_content_enhancement.vl_video_analyzer import VLVideoAnalyzer
from MVP_Module2_HEANCING.module2_content_enhancement.config_loader import load_module2_config


async def debug_timestamp_conversion():
    """详细追踪时间戳转换过程"""
    
    print(f"\n{'='*80}")
    print(f"SU007 时间戳转换验证")
    print(f"{'='*80}\n")
    
    # 视频路径
    video_path = r"D:\videoToMarkdownTest2\storage\20225626c2a19253c4121f684ecdff12\semantic_unit_clips\007_SU007_Quickstart模式配置步骤_130.00-150.00.mp4"
    
    if not Path(video_path).exists():
        print(f"❌ 视频文件不存在: {video_path}")
        return
    
    # 语义单元信息
    semantic_unit_start_sec = 130.0
    semantic_unit_id = "SU007"
    
    # 加载配置
    config = load_module2_config()
    vl_config = config.get("vl_material_generation", {})
    
    # 初始化分析器
    analyzer = VLVideoAnalyzer(vl_config)
    
    print(f"🔍 调用 VL API (Temperature: {analyzer.temperature})...")
    
    try:
        # 使用完整的 analyze_clip 方法
        response = await analyzer.analyze_clip(
            clip_path=video_path,
            semantic_unit_start_sec=semantic_unit_start_sec,
            semantic_unit_id=semantic_unit_id
        )
        
        print(f"\n✅ analyze_clip 完成")
        print(f"   - 成功: {response.success}")
        
        if response.clip_requests:
            for idx, clip_req in enumerate(response.clip_requests):
                print(f"\nclip_request {idx}:")
                print(f"  - clip_id: {clip_req['clip_id']}")
                print(f"  - start_sec: {clip_req['start_sec']}")
                print(f"  - end_sec: {clip_req['end_sec']}")
                print(f"  - 知识类型: {clip_req['knowledge_type']}")
                
                if clip_req['start_sec'] == clip_req['end_sec']:
                    print(f"  ⚠️  警告: start_sec 和 end_sec 仍相同!")
                else:
                    print(f"  ✅ 成功: 识别到有效范围 (时长 {clip_req['end_sec'] - clip_req['start_sec']:.2f}s)")
        else:
            print(f"\n⚠️  未发现视频片段请求")
            
        if response.analysis_results:
            print(f"\n提示词/模型推理细节:")
            for idx, ar in enumerate(response.analysis_results):
                print(f"  - Reasoning: {ar.reasoning}")
        
    except Exception as e:
        print(f"\n❌ 调试过程中出错: {e}")
        import traceback
        traceback.print_exc()


async def main():
    await debug_timestamp_conversion()


if __name__ == "__main__":
    asyncio.run(main())
