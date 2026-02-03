"""
Step 23: 可视化决策总结
生成可视化选择的详细报告，记录截图和视频片段的选择原因
"""

import asyncio
from typing import Dict, Any, List
from pathlib import Path

from ..state import PipelineState
from ..monitoring.logger import get_logger


async def step23_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤23：可视化决策总结
    
    类型：代码规则
    核心动作：生成可视化选择的详细报告
    """
    logger = get_logger("step23_summary", state.get("output_dir", "output/logs"))
    logger.start()
    
    # 获取所有相关数据
    reconstructed_materials = state.get("reconstructed_materials", [])
    semantic_faults = state.get("semantic_faults", [])
    qualified_frames = state.get("qualified_frames", [])
    processed_frames = state.get("processed_frames", [])
    auxiliary_information = state.get("auxiliary_information", [])
    visualization_forms = state.get("visualization_forms", [])
    core_content_judgment = state.get("core_content_judgment", [])
    strategy_matches = state.get("strategy_matches", [])
    
    # 构建映射
    fault_map = {}
    for fault in semantic_faults:
        sid = fault.get("segment_id", "")
        if sid not in fault_map:
            fault_map[sid] = []
        fault_map[sid].append(fault)
    
    form_map = {v["segment_id"]: v for v in visualization_forms}
    core_map = {c["segment_id"]: c for c in core_content_judgment}
    aux_map = {a["segment_id"]: a for a in auxiliary_information}
    strategy_map = {s["segment_id"]: s for s in strategy_matches}
    
    # 按 segment 组织截图
    screenshot_map = {}
    for frame in (processed_frames if processed_frames else qualified_frames):
        sid = frame.get("segment_id", "")
        if sid not in screenshot_map:
            screenshot_map[sid] = []
        screenshot_map[sid].append(frame)
    
    logger.log_input({"material_count": len(reconstructed_materials)})
    
    try:
        # 生成总结报告
        md_lines = []
        md_lines.append("# 可视化决策总结")
        md_lines.append("")
        md_lines.append("> 本报告记录了所有截图和视频片段的选择依据，用于质量监控和问题追溯。")
        md_lines.append("")
        md_lines.append("---")
        md_lines.append("")
        
        # 统计信息
        total_screenshots = sum(len(frames) for frames in screenshot_map.values())
        total_videos = sum(1 for mat in reconstructed_materials if mat.get("video_time_range"))
        
        md_lines.append("## 总体统计")
        md_lines.append("")
        md_lines.append(f"- **知识点数量**: {len(reconstructed_materials)}")
        md_lines.append(f"- **截图总数**: {total_screenshots}")
        md_lines.append(f"- **视频片段数**: {total_videos}")
        md_lines.append("")
        md_lines.append("---")
        md_lines.append("")
        
        # 按知识点生成详细报告
        for idx, material in enumerate(reconstructed_materials, 1):
            kp_title = material.get("knowledge_point", f"知识点 {idx}")
            segment_ids = material.get("segment_ids", [material.get("segment_id", "")])
            
            md_lines.append(f"## {idx}. {kp_title}")
            md_lines.append("")
            
            # 可视化形式判断
            form_info = form_map.get(segment_ids[0], {}) if segment_ids else {}
            form = form_info.get("form", "none")
            
            if form != "none":
                md_lines.append(f"**可视化形式**: `{form}`")
                md_lines.append("")
            
            # 截图选择
            has_screenshots = False
            for sid in segment_ids:
                screenshots = screenshot_map.get(sid, [])
                if screenshots:
                    if not has_screenshots:
                        md_lines.append("### 📸 截图选择")
                        md_lines.append("")
                        has_screenshots = True
                    
                    for ss_idx, ss in enumerate(screenshots, 1):
                        filename = Path(ss.get("processed_path") or ss.get("frame_path", "")).name
                        timestamp = ss.get("timestamp", 0)
                        
                        md_lines.append(f"#### 截图 {ss_idx}: {filename}")
                        md_lines.append(f"- **时间点**: {timestamp:.1f}s")
                        
                        # 断层信息
                        fault_id = ss.get("fault_id", "")
                        matching_fault = next((f for f in fault_map.get(sid, []) if f.get("fault_id") == fault_id), None)
                        
                        if matching_fault:
                            fault_type = matching_fault.get("fault_type_name", "未知")
                            fault_type_id = matching_fault.get("fault_type_id", 0)
                            md_lines.append(f"- **断层类型**: {fault_type} (Type {fault_type_id})")
                            
                            missing_content = matching_fault.get("missing_content", {})
                            must_supplement = missing_content.get("must_supplement", "")
                            if must_supplement:
                                md_lines.append(f"- **缺失内容**: {must_supplement}")
                        
                        # 采样策略
                        strategy = strategy_map.get(sid, {})
                        if strategy:
                            strategy_name = strategy.get("strategy", "")
                            anchor = strategy.get("anchor", "")
                            mode = strategy.get("mode", "")
                            count = strategy.get("count", 0)
                            md_lines.append(f"- **采样策略**: {strategy_name}，锚点：{anchor}，{mode}（{count}帧）")
                        
                        # 质量验证
                        grade = ss.get("grade", "")
                        if grade:
                            md_lines.append(f"- **质量等级**: {grade}")
                        
                        md_lines.append("")
            
            # 视频片段选择
            video_range = material.get("video_time_range")
            if video_range:
                start_sec = video_range.get("start_sec", 0)
                end_sec = video_range.get("end_sec", 0)
                duration = end_sec - start_sec
                
                md_lines.append("### 🎬 视频片段选择")
                md_lines.append("")
                md_lines.append(f"**时间范围**: {start_sec:.1f}s - {end_sec:.1f}s ({duration:.1f}s)")
                md_lines.append("")
                
                # 选择原因
                md_lines.append("**选择原因**:")
                
                # 核心内容判断
                core_info = core_map.get(segment_ids[0], {}) if segment_ids else {}
                if core_info.get("video_needed"):
                    core_type = core_info.get("core_type", "")
                    reason = core_info.get("reason", "")
                    md_lines.append(f"- **核心内容判断**: {core_type}")
                    if reason:
                        md_lines.append(f"  - {reason}")
                
                # 断层类型
                all_faults = []
                for sid in segment_ids:
                    all_faults.extend(fault_map.get(sid, []))
                
                if all_faults:
                    fault_types = list(set(f.get("fault_type_name", "") for f in all_faults))
                    md_lines.append(f"- **断层类型**: {', '.join(fault_types)}")
                
                md_lines.append("")
                
                # 边界检测过程（从 auxiliary_information 获取）
                aux_info = aux_map.get(segment_ids[0], {}) if segment_ids else {}
                if aux_info:
                    md_lines.append("**边界检测过程**:")
                    md_lines.append(f"- 最终边界: {start_sec:.1f}s - {end_sec:.1f}s")
                    
                    # 如果有引导语，说明经过了精细化
                    video_transition = aux_info.get("video_transition", "")
                    if video_transition:
                        md_lines.append("- 经过 Vision AI 边界精细化")
                    
                    md_lines.append("")
            
            md_lines.append("---")
            md_lines.append("")
        
        # 写入文件
        output_dir = state.get("output_dir", "output")
        notes_dir = Path(output_dir) / "notes"
        notes_dir.mkdir(parents=True, exist_ok=True)
        
        summary_path = notes_dir / "visualization_summary.md"
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("\n".join(md_lines))
        
        output = {
            "visualization_summary_path": str(summary_path),
            "current_step": "step23_summary",
            "current_step_status": "completed"
        }
        
        logger.log_output({
            "summary_path": str(summary_path),
            "total_screenshots": total_screenshots,
            "total_videos": total_videos
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step23_summary": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"visualization_summary_path": None, "errors": [{"step": "step23", "error": str(e)}]}
