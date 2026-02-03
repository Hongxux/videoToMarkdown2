
import asyncio
import json
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from stage1_pipeline.nodes.phase4_screenshot_gen import step9_node, step10_node, step11_node
from stage1_pipeline.nodes.phase5_capture import step12_node, step13_node, step14_node, step15_node
from stage1_pipeline.monitoring.logger import get_logger

async def run_test():
    base_dir = r"D:\videoToMarkdownTest2\videoToMarkdown\worker_output\1e580021-7932-4004-a200-55204a5443ee"
    intermediates_dir = os.path.join(base_dir, "intermediates")
    video_path = os.path.join(base_dir, "video.mp4")
    
    # Load Step 8b output
    # Load Step 8b output
    step8b_path = os.path.join(intermediates_dir, "step8b_fault_locate_output.json")
    with open(step8b_path, 'r', encoding='utf-8') as f:
        step8b_data = json.load(f)
        
    # Correctly extract semantic_faults from the output or input section
    # Based on the file content, it is in step8b_data["output"]["semantic_faults"]
    semantic_faults = step8b_data.get("output", {}).get("semantic_faults", [])
    if not semantic_faults:
        # Fallback to input if output is empty (just in case)
        semantic_faults = step8b_data.get("input", {}).get("fault_candidates", {}).get("_sample", [])

    state = {
        "video_path": video_path,
        "output_dir": base_dir,
        "semantic_faults": semantic_faults,
        "visualization_candidates": [], 
    }
    
    # Need to check if step8b_data actually has "semantic_faults" or "fault_candidates"
    # Based on file check earlier, it seemed to imply "fault_candidates" -> "semantic_faults" mapping in step 8
    # Let me check step8b output structure if possible, but I'll assume standard flow.
    # Actually, phase4 expects "semantic_faults". Step 8b output likely provides "fault_candidates".
    # I might need a small bridge or check step 8b output again.
    
    print("--- Starting Test Run ---")
    
    # Step 9
    print("\n[Step 9] Strategy Matching...")
    res9 = await step9_node(state)
    state.update(res9)
    print(f"Matched {len(state.get('strategy_matches', []))} strategies")
    
    # Step 10
    print("\n[Step 10] Timing Calculation...")
    res10 = await step10_node(state)
    state.update(res10)
    print(f"Calculated {len(state.get('capture_times', []))} timing points")
    
    # Step 11
    print("\n[Step 11] Instruction Generation...")
    res11 = await step11_node(state)
    state.update(res11)
    print(f"Generated {len(state.get('screenshot_instructions', []))} instructions")
    
    # Step 12
    print("\n[Step 12] Capture Execution (with Peak Detection)...")
    res12 = await step12_node(state)
    state.update(res12)
    print(f"Captured {len(state.get('captured_frames', []))} frames")
    
    # Step 13
    print("\n[Step 13] Frame Validation...")
    res13 = await step13_node(state)
    state.update(res13)
    print(f"Valid frames: {len(state.get('valid_frames', []))}")
    
    # Step 14
    print("\n[Step 14] Vision QA (Tiered Verification)...")
    res14 = await step14_node(state)
    state.update(res14)
    qualified = state.get("qualified_frames", [])
    print(f"Qualified frames: {len(qualified)}")
    for f in qualified:
        print(f"  - {f['frame_id']}: Grade {f.get('grade')}, Tier {f.get('verification_tier')}")
        
    # Step 15
    print("\n[Step 15] Retry Loop...")
    res15 = await step15_node(state)
    state.update(res15)
    
    # Save final state
    final_output = os.path.join(base_dir, "test_strategy_final_output.json")
    # Helper to serialize non-serializable objects if any
    with open(final_output, 'w', encoding='utf-8') as f:
        json.dump(state, f, default=str, indent=2, ensure_ascii=False)
        
    print(f"\nTest Complete. Output saved to {final_output}")

if __name__ == "__main__":
    asyncio.run(run_test())
