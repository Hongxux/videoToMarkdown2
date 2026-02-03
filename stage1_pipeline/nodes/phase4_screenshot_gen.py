"""
Phase 4: 截图指令生成
Steps 9, 10, 11
"""

import re
from typing import Dict, Any, List

from ..state import PipelineState
from ..monitoring.logger import get_logger
from ..tools.opencv_capture import calculate_capture_times
from .screenshot_helpers import generate_simplified_questions


# ============================================================================
# 截帧策略映射表（硬编码）- Step 9
# ============================================================================

STRATEGY_MAP = {
    1: {  # 显性指引类
        "strategy": "显性指引",
        "anchor": "后0.5-2秒",
        "anchor_offset": (0.5, 2.0),
        "mode": "多帧采样",
        "count": 3,
        "enhance": "default"
    },
    2: {  # 结论无推导类
        "strategy": "过程公式",
        "anchor": "前1-3秒",
        "anchor_offset": (-3.0, -1.0),
        "mode": "peak_detect",  # Changed to peak_detect
        "count": 1,             # Single best frame
        "enhance": "default"
    },
    3: {  # 概念无定义类
        "strategy": "显性指引",
        "anchor": "后0.5-2.5秒",  # 扩大范围以覆盖PPT切换延迟
        "anchor_offset": (0.5, 2.5),
        "mode": "多帧采样",  # 改为多帧采样
        "count": 3,  # 采样3帧
        "enhance": "default"
    },
    4: {  # 实操步骤断裂类
        "strategy": "显性指引",
        "anchor": "同步",
        "anchor_offset": (0.0, 0.0),
        "mode": "多帧采样",
        "count": 3,
        "enhance": "default"
    },
    5: {  # 分层分类无内容类
        "strategy": "显性指引",
        "anchor": "后0.5-1.5秒",
        "anchor_offset": (0.5, 1.5),
        "mode": "双帧采样",
        "count": 2,
        "enhance": "default"
    },
    6: {  # 量化数据缺失类
        "strategy": "量化数据",
        "anchor": "前1-2秒",
        "anchor_offset": (-2.0, -1.0),
        "mode": "单帧精准",
        "count": 1,
        "enhance": "sharpen"
    },
    7: {  # 指代模糊类
        "strategy": "显性指引",
        "anchor": "前2-3秒至后1秒",
        "anchor_offset": (-3.0, 1.0),
        "mode": "多帧采样",
        "count": 3,
        "enhance": "default"
    },
    8: {  # 动态过程空白类
        "strategy": "过程公式",
        "anchor": "全程区间",
        "anchor_offset": (0.0, 0.0),
        "mode": "peak_detect",  # Changed to peak_detect
        "count": 1,             # Single best frame
        "enhance": "default"
    },
    9: {  # 符号编号缺失类
        "strategy": "符号编号",
        "anchor": "后0-1秒",
        "anchor_offset": (0.0, 1.0),
        "mode": "单帧精准",
        "count": 1,
        "enhance": "local_zoom"
    },
    10: {  # 对比逻辑缺失类
        "strategy": "量化数据",
        "anchor": "后0.5-1.5秒",
        "anchor_offset": (0.5, 1.5),
        "mode": "双帧采样",
        "count": 2,
        "enhance": "default"
    }
}


# ============================================================================
# 可视化场景策略映射表（硬编码）- Step 9 新增
# ============================================================================

VISUALIZATION_STRATEGY_MAP = {
    "层级/结构类": {
        "strategy": "显性指引",
        "anchor": "片段中间",
        "anchor_offset": (0.0, 0.0),
        "mode": "单帧精准",
        "count": 1,
        "enhance": "default"
    },
    "流程/流转类": {
        "strategy": "过程公式",
        "anchor": "片段全程",
        "anchor_offset": (0.0, 0.0),
        "mode": "多帧采样",
        "count": 5,
        "enhance": "default"
    },
    "实操/界面类": {
        "strategy": "显性指引",
        "anchor": "同步",
        "anchor_offset": (0.0, 0.0),
        "mode": "多帧采样",
        "count": 3,
        "enhance": "default"
    },
    "对比/差异类": {
        "strategy": "量化数据",
        "anchor": "片段中间",
        "anchor_offset": (0.0, 0.0),
        "mode": "双帧采样",
        "count": 2,
        "enhance": "default"
    },
    "复杂逻辑关系类": {
        "strategy": "显性指引",
        "anchor": "片段中间",
        "anchor_offset": (0.0, 0.0),
        "mode": "单帧精准",
        "count": 1,
        "enhance": "default"
    }
}


async def step9_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤9：截帧策略匹配
    
    类型：代码规则（硬编码映射表）
    核心动作：
    1. 根据断层类型匹配截帧策略 (STRATEGY_MAP)
    2. 根据可视化场景类型匹配截帧策略 (VISUALIZATION_STRATEGY_MAP)
    """
    logger = get_logger("step9_strategy", state.get("output_dir", "output/logs"))
    logger.start()
    
    semantic_faults = state.get("semantic_faults", [])
    visualization_candidates = state.get("visualization_candidates", [])
    
    logger.log_input({
        "fault_count": len(semantic_faults),
        "viz_candidate_count": len(visualization_candidates)
    })
    
    try:
        strategy_matches = []
        
        # 1. 处理断层类型
        for fault in semantic_faults:
            fault_type = fault.get("fault_type", 1)
            strategy = STRATEGY_MAP.get(fault_type, STRATEGY_MAP[1])
            
            strategy_matches.append({
                "source_type": "fault",
                "fault_id": fault["fault_id"],
                "segment_id": fault["segment_id"],
                "fault_type": fault_type,
                "strategy": strategy["strategy"],
                "anchor": strategy["anchor"],
                "anchor_offset": strategy["anchor_offset"],
                "mode": strategy["mode"],
                "count": strategy["count"],
                "enhance": strategy["enhance"],
                # 来源于断层，无key_elements
                "expected_visual_forms": [],
                "key_elements": [],
                "min_completeness": 0.7
            })
        
        # 2. 处理可视化场景（不与断层重复的segment）
        fault_segment_ids = {f["segment_id"] for f in semantic_faults}
        
        for viz in visualization_candidates:
            segment_id = viz.get("segment_id", "")
            
            # 如果已经有断层，跳过（断层优先）
            if segment_id in fault_segment_ids:
                continue
            
            scene_type = viz.get("scene_type", "")
            strategy = VISUALIZATION_STRATEGY_MAP.get(scene_type, VISUALIZATION_STRATEGY_MAP.get("层级/结构类"))
            
            if strategy:
                strategy_matches.append({
                    "source_type": "visualization",
                    "id": viz.get("viz_id", f"VIZ_{segment_id}"), # 统一ID字段
                    "viz_id": viz.get("viz_id", ""),  # 保留特定ID
                    "segment_id": segment_id,
                    "scene_type": scene_type,
                    "strategy": strategy["strategy"],
                    "anchor": strategy["anchor"],
                    "anchor_offset": strategy["anchor_offset"],
                    "mode": strategy["mode"],
                    "count": strategy["count"],
                    "enhance": strategy["enhance"],
                    # 来源于可视化场景，包含key_elements
                    "expected_visual_forms": viz.get("expected_visual_forms", []),
                    "key_elements": viz.get("key_elements", []),
                    "min_completeness": viz.get("min_completeness", 0.7),
                    "timestamp": viz.get("timestamp")  # 传递精确时间戳
                })
        
        output = {
            "strategy_matches": strategy_matches,
            "current_step": "step9_strategy",
            "current_step_status": "completed"
        }
        
        logger.log_output({
            "matched_count": len(strategy_matches),
            "fault_matches": sum(1 for s in strategy_matches if s.get("source_type") == "fault"),
            "viz_matches": sum(1 for s in strategy_matches if s.get("source_type") == "visualization")
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step9_strategy": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"strategy_matches": [], "errors": [{"step": "step9", "error": str(e)}]}


async def step10_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤10：截帧时间计算
    
    类型：代码规则
    核心动作：根据时间锚点规则计算精确截帧时间
    """
    logger = get_logger("step10_timing", state.get("output_dir", "output/logs"))
    logger.start()
    
    strategy_matches = state.get("strategy_matches", [])
    semantic_faults = state.get("semantic_faults", [])
    visualization_candidates = state.get("visualization_candidates", [])
    
    # 构建映射
    fault_map = {f["fault_id"]: f for f in semantic_faults}
    viz_map = {v["viz_id"]: v for v in visualization_candidates if "viz_id" in v}
    
    logger.log_input({"strategy_count": len(strategy_matches)})
    
    try:
        capture_times = []
        
        for match in strategy_matches:
            source_type = match.get("source_type", "fault")
            location = {"start_sec": 0, "end_sec": 10}
            
            # 获取位置信息
            if source_type == "fault":
                fault = fault_map.get(match.get("fault_id"), {})
                location = fault.get("fault_location") or location
            elif source_type == "visualization":
                # 优先使用传递过来的精确时间戳
                if match.get("timestamp"):
                    location = match["timestamp"]
                else:
                    viz = viz_map.get(match.get("viz_id"), {})
                    location = (viz.get("timestamp") if viz else None) or location
            
            # 安全检查：确保 location 不是 None
            if location is None:
                location = {"start_sec": 0, "end_sec": 10}
            
            start = location.get("start_sec", 0)
            end = location.get("end_sec", 10)
            mode = match["mode"]
            count = match["count"]
            anchor_offset = match.get("anchor_offset", (0, 0))
            
            # Unified calculation using tool function
            # Construct a fault-like object for visualization matches to be compatible
            fault_loc = {"start_sec": start, "end_sec": end}
            calc_result = calculate_capture_times(match, fault_loc)
            
            preferred = calc_result["capture_times"]
            fallback = calc_result["fallback_range"]
            peak_params = calc_result.get("peak_detect_params")
            
            capture_times.append({
                "source_type": source_type,
                "fault_id": match.get("fault_id"), # 仅fault有
                "viz_id": match.get("viz_id"),     # 仅viz有
                "segment_id": match["segment_id"],
                "capture_times": preferred,
                "fallback_range": fallback,
                "peak_detect_params": peak_params
            })
        
        output = {
            "capture_times": capture_times,
            "current_step": "step10_timing",
            "current_step_status": "completed"
        }
        
        logger.log_output({"calculated_count": len(capture_times)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step10_timing": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"capture_times": [], "errors": [{"step": "step10", "error": str(e)}]}


async def step11_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤11：标准化JSON指令生成
    
    类型：代码规则
    核心动作：生成含执行层和校验层的截帧任务包
    """
    logger = get_logger("step11_instruction", state.get("output_dir", "output/logs"))
    logger.start()
    
    capture_times = state.get("capture_times", [])
    strategy_matches = state.get("strategy_matches", [])
    semantic_faults = state.get("semantic_faults", [])
    visualization_candidates = state.get("visualization_candidates", [])
    
    # 构建映射
    # 策略映射：尝试使用通用ID或特定ID
    strategy_map = {}
    for s in strategy_matches:
        if s.get("fault_id"):
            strategy_map[s["fault_id"]] = s
        if s.get("viz_id"):
            strategy_map[s["viz_id"]] = s
            
    fault_map = {f["fault_id"]: f for f in semantic_faults}
    viz_map = {v["viz_id"]: v for v in visualization_candidates if "viz_id" in v}
    
    logger.log_input({"capture_count": len(capture_times)})
    
    try:
        instructions = []
        instruction_counter = 1
        
        for ct in capture_times:
            source_type = ct.get("source_type", "fault")
            
            # 获取策略和源数据
            if source_type == "fault":
                source_id = ct["fault_id"]
                strategy = strategy_map.get(source_id, {})
                source_data = fault_map.get(source_id, {})
                missing = source_data.get("missing_content", {})
                visual_form = source_data.get("visual_form", "")
                
                # Fault特定的must/secondary
                must_content = missing.get("must_supplement", "")
                secondary_content = missing.get("secondary_supplement", "")
                
            else: # visualization
                source_id = ct["viz_id"]
                strategy = strategy_map.get(source_id, {})
                source_data = viz_map.get(source_id, {})
                visual_form = ", ".join(source_data.get("expected_visual_forms", []))
                
                # Viz特定的key_elements
                key_elements = source_data.get("key_elements", [])
                must_content = key_elements[0] if key_elements else ""
                secondary_content = key_elements[1] if len(key_elements) > 1 else ""
            
            # 生成校验问题（简化版，提高Vision准确率）
            questions = generate_simplified_questions(
                source_type=source_type,
                source_data=source_data,
                must_content=must_content,
                secondary_content=secondary_content
            )
            
            instruction = {
                "instruction_id": f"INS{instruction_counter:03d}",
                "source_type": source_type,
                "source_id": source_id,
                "fault_id": ct.get("fault_id"), # 兼容旧字段
                "viz_id": ct.get("viz_id"),     # 新增字段
                "segment_id": ct["segment_id"],
                "opencv_params": {
                    "primary_times": ct["capture_times"],
                    "fallback_range": ct["fallback_range"],
                    "enhance_params": {
                        "sharpen": strategy.get("enhance") == "sharpen",
                        "local_zoom": strategy.get("enhance") == "local_zoom",
                        "contrast_boost": 1.0
                    },
                    "peak_detect_params": ct.get("peak_detect_params")
                },
                "validation_questions": questions,
                "verification_tier": "L2", # Default to L2 (Vision Grounding)
                "expected_visual_form": visual_form
            }
            
            instructions.append(instruction)
            instruction_counter += 1
        
        output = {
            "screenshot_instructions": instructions,
            "current_step": "step11_instruction",
            "current_step_status": "completed"
        }
        
        output_metrics = {"instruction_count": len(instructions)}
        logger.log_output(output_metrics)
        timing = logger.end(success=True)
        output["step_timings"] = {"step11_instruction": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"screenshot_instructions": [], "errors": [{"step": "step11", "error": str(e)}]}
