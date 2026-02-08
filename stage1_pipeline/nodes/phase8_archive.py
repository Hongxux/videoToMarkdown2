"""
模块说明：阶段流程节点 phase8_archive 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import subprocess
from pathlib import Path
from typing import Dict, Any, List
import shutil

from ..state import PipelineState
from ..tools.storage import LocalStorage
from ..monitoring.logger import get_logger


async def step23_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过子进程调用、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not core.get('video_needed')
    - 条件：not time_range
    - 条件：end <= start
    依据来源（证据链）：
    - 配置字段：video_needed。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step23_video_name", state.get("output_dir", "output/logs"))
    logger.start()
    
    core_content_judgment = state.get("core_content_judgment", [])
    knowledge_segments = state.get("knowledge_segments", [])
    auxiliary_information = state.get("auxiliary_information", [])
    video_path = state.get("video_path", "")
    output_dir = state.get("output_dir", "output")
    
    # 构建映射
    segment_map = {s["segment_id"]: s for s in knowledge_segments}
    aux_map = {a["segment_id"]: a for a in auxiliary_information}
    
    logger.log_input({"core_count": len([c for c in core_content_judgment if c.get("video_needed")])})
    
    try:
        named_clips = []
        clips_dir = Path(output_dir) / "video_clips"
        clips_dir.mkdir(parents=True, exist_ok=True)
        
        for core in core_content_judgment:
            if not core.get("video_needed"):
                continue
            
            sid = core["segment_id"]
            segment = segment_map.get(sid, {})
            aux = aux_map.get(sid, {})
            
            time_range = aux.get("video_time_range", {})
            if not time_range:
                continue
            
            start = time_range.get("start_sec", 0)
            end = time_range.get("end_sec", 0)
            
            if end <= start:
                continue
            
            # 生成文件名
            label = segment.get("core_semantic", {}).get("label", sid)
            core_type = core.get("core_type", "核心")
            safe_label = "".join(c if c.isalnum() or c in "-_" else "_" for c in label)
            
            clip_name = f"{safe_label}-{core_type}-{start:.0f}s.mp4"
            clip_path = clips_dir / clip_name
            
            # 使用 FFmpeg 截取
            try:
                duration = end - start
                # 快速通道：流拷贝（快，关键帧精度）
                fast_cmd = [
                    "ffmpeg", "-y",
                    "-ss", str(start),
                    "-i", video_path,
                    "-t", str(duration),
                    "-c", "copy",
                    "-avoid_negative_ts", "make_zero",
                    str(clip_path)
                ]

                result = subprocess.run(
                    fast_cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )

                # 回退通道：重编码（慢，但兼容性更高）
                if result.returncode != 0 or not clip_path.exists() or clip_path.stat().st_size == 0:
                    fallback_cmd = [
                        "ffmpeg", "-y",
                        "-ss", str(start),
                        "-i", video_path,
                        "-t", str(duration),
                        "-c:v", "libx264",
                        "-preset", "veryfast",
                        "-crf", "23",
                        "-c:a", "aac",
                        "-b:a", "128k",
                        str(clip_path)
                    ]
                    result = subprocess.run(
                        fallback_cmd,
                        capture_output=True,
                        text=True,
                        timeout=120
                    )
                
                if result.returncode == 0 and clip_path.exists():
                    named_clips.append({
                        "segment_id": sid,
                        "clip_name": clip_name,
                        "clip_path": str(clip_path),
                        "start_sec": start,
                        "end_sec": end,
                        "core_type": core_type
                    })
                    logger.info(f"Created clip: {clip_name}")
                else:
                    logger.log_warning(f"FFmpeg failed for {sid}: {result.stderr[:200]}")
                    
            except subprocess.TimeoutExpired:
                logger.log_warning(f"FFmpeg timeout for {sid}")
            except FileNotFoundError:
                logger.log_warning("FFmpeg not found, skipping video clip creation")
                break
        
        output = {
            "named_video_clips": named_clips,
            "current_step": "step23_video_name",
            "current_step_status": "completed"
        }
        
        logger.log_output({"clip_count": len(named_clips)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step23_video_name": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"named_video_clips": [], "errors": [{"step": "step23", "error": str(e)}]}


async def step24_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：processed_frames
    - 条件：sid
    - 条件：not knowledge_point
    依据来源（证据链）：
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step24_screenshot_name", state.get("output_dir", "output/logs"))
    logger.start()
    
    # 优先使用 processed_frames（step15b输出），否则 qualified_frames
    processed_frames = state.get("processed_frames", [])
    qualified_frames = state.get("qualified_frames", [])
    frames_to_use = processed_frames if processed_frames else qualified_frames
    
    visualization_candidates = state.get("visualization_candidates", [])
    knowledge_segments = state.get("knowledge_segments", [])
    strategy_matches = state.get("strategy_matches", [])
    semantic_faults = state.get("semantic_faults", [])
    output_dir = state.get("output_dir", "output")
    
    # 构建映射
    segment_map = {s["segment_id"]: s for s in knowledge_segments}
    viz_candidate_map = {v["segment_id"]: v for v in visualization_candidates}
    
    # 构建 fault_id -> segment_id 映射
    fault_to_segment = {f["fault_id"]: f["segment_id"] for f in semantic_faults}
    
    # 构建 segment_id -> source_type/scene_type 映射
    segment_source = {}
    for s in strategy_matches:
        sid = s.get("segment_id", "")
        if sid:
            segment_source[sid] = {
                "source_type": s.get("source_type", "fault"),
                "scene_type": s.get("scene_type", "")
            }
    
    logger.log_input({
        "frame_count": len(frames_to_use),
        "using_processed_frames": len(processed_frames) > 0
    })
    
    try:
        named_screenshots = []
        screenshots_dir = Path(output_dir) / "screenshots"
        screenshots_dir.mkdir(parents=True, exist_ok=True)
        
        # 按 segment_id 分组统计序号
        segment_sequence = {}
        
        for frame in frames_to_use:
            fault_id = frame.get("fault_id", "")
            segment_id = frame.get("segment_id", "") or fault_to_segment.get(fault_id, "")
            
            segment = segment_map.get(segment_id, {})
            viz_candidate = viz_candidate_map.get(segment_id, {})
            source_info = segment_source.get(segment_id, {})
            
            # 获取知识点名称
            knowledge_point = segment.get("knowledge_point", "")
            if not knowledge_point:
                knowledge_point = segment.get("core_semantic", {}).get("label", segment_id)
            
            # 获取场景类型
            # 优先从 visualization_candidates 获取，否则从 strategy_matches
            scene_type = viz_candidate.get("scene_type", "")
            if not scene_type:
                scene_type = source_info.get("scene_type", "")
            if not scene_type:
                # 默认使用断层类型作为场景类型
                scene_type = "断层补全类"
            
            # 计算序号
            if segment_id not in segment_sequence:
                segment_sequence[segment_id] = 0
            segment_sequence[segment_id] += 1
            sequence = segment_sequence[segment_id]
            
            # 安全化文件名（移除特殊字符）
            safe_kp = "".join(c if c.isalnum() or c in "-_" else "_" for c in knowledge_point)
            safe_scene = scene_type.replace("/", "")  # 移除斜杠
            safe_scene = "".join(c if c.isalnum() or c in "-_" else "_" for c in safe_scene)
            
            # v7命名格式: {knowledge_point}_{scene_type}_{sequence}.png
            new_name = f"{safe_kp}_{safe_scene}_{sequence}.png"
            new_path = screenshots_dir / new_name
            
            # 获取源路径（优先 processed_path，否则 frame_path）
            old_path = Path(frame.get("processed_path", frame.get("frame_path", "")))
            
            if old_path.exists():
                shutil.copy2(old_path, new_path)
                
                named_screenshots.append({
                    "frame_id": frame.get("frame_id", ""),
                    "original_path": str(frame.get("original_path", old_path)),
                    "processed_path": str(old_path),
                    "new_name": new_name,
                    "new_path": str(new_path),
                    "segment_id": segment_id,
                    "knowledge_point": knowledge_point,
                    "scene_type": scene_type,
                    "sequence": sequence
                })
                
                logger.debug(f"Renamed: {old_path.name} -> {new_name}")
        
        output = {
            "named_screenshots": named_screenshots,
            "current_step": "step24_screenshot_name",
            "current_step_status": "completed"
        }
        
        logger.log_output({"screenshot_count": len(named_screenshots)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step24_screenshot_name": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"named_screenshots": [], "errors": [{"step": "step24", "error": str(e)}]}

