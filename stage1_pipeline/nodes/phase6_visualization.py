"""
模块说明：阶段流程节点 phase6_visualization 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import asyncio
from typing import Dict, Any, List

from ..state import PipelineState
from ..llm.client import create_llm_client
from ..llm.vision import ERNIEVisionClient
from ..tools.storage import LocalStorage
from ..tools.opencv_capture import FrameCapture
from ..tools.frame_analyzer import FrameBoundaryAnalyzer, BoundaryAnalysisResult
from ..monitoring.logger import get_logger

# ============================================================================
# 视频边界精细化 Prompt (新增)
# ============================================================================

VIDEO_BOUNDARY_PROMPT = """请分析以下视频帧序列，判断视频的有效演示是否已经结束或尚未开始。

【场景描述】
知识点：{title}
演示动作：{summary}

【任务目标】
1. 检测画面演示是否仍在继续（解决结束太早的问题）。
2. 检测开始处是否包含上一页PPT残留或转场（解决开始太早的问题）。

【提供帧序列】
- Frame Start ({t_start}s): 当前设定的开始时刻
- Frame End+1s ({t_end_1}s): 结束时刻后1秒
- Frame End+3s ({t_end_3}s): 结束时刻后3秒
- Frame End+5s ({t_end_5}s): 结束时刻后5秒

【判断标准】
- 结束点延展：如果 End+1s/End+3s 的画面内容与演示动作紧密相关（如鼠标移动、代码输入、界面变化），且未进入明显的新知识点页面，则应延长。
- 开始点调整：如果 Start 帧显示的是上一页PPT、黑屏或明显的转场动画，则应向后调整。

【输出格式】
{{
    "start_adjustment_sec": 0.0,  // 建议开始时间向后调整的秒数（如 1.5，若无需调整则为 0）
    "end_extension_sec": 0.0,     // 建议结束时间向后延长的秒数（根据画面动作持续情况，最大 8.0）
    "reason": "判断依据..."
}}"""


# ============================================================================
# 可视化形式映射（硬编码）- Step 17
# ============================================================================

VISUALIZATION_FORM_MAP = {
    # 动态内容 -> 视频片段
    8: "video",   # 动态过程空白类
    4: "video",   # 实操步骤断裂类
    
    # 静态内容 -> 关键截图
    1: "screenshot",   # 显性指引类
    3: "screenshot",   # 概念无定义类
    5: "screenshot",   # 分层分类无内容类
    6: "screenshot",   # 量化数据缺失类
    9: "screenshot",   # 符号编号缺失类
    10: "screenshot",  # 对比逻辑缺失类
    
    # 混合内容 -> 视频+截图
    2: "video_screenshot",   # 结论无推导类
    7: "video_screenshot",   # 指代模糊类
}


# ============================================================================
# Step 16: 可视化必要性判定 Prompt
# ============================================================================

VISUALIZATION_NEED_PROMPT = """请判断以下知识点片段是否需要可视化补充。

【片段信息】
- 内容摘要：{summary}
- DARPA问题：{darpa_question}
- 语义维度：{semantic_dimension}

【断层信息】
{faults_info}

【已有截图数量】: {frame_count}

【判断依据】
1. 内容复杂度是否需要视觉辅助
2. 是否存在需要补全的断层
3. 已有截图是否足够

【输出格式】
{{
  "need_visualization": true/false,
  "judgment_basis": ["理由1", "理由2"]
}}"""


async def step16_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：knowledge_points
    - 条件：sid not in seg_to_faults
    - 条件：sid not in seg_to_frames
    依据来源（证据链）：
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    改造点：
    1. 优先处理 knowledge_points，聚合其下所有 segment 的断层和截图
    2. 如果 knowledge_points 为空，回退到处理 knowledge_segments"""
    logger = get_logger("step16_viz_need", state.get("output_dir", "output/logs"))
    logger.start()
    
    knowledge_points = state.get("knowledge_points", [])
    segments = state.get("knowledge_segments", [])
    semantic_faults = state.get("semantic_faults", [])
    qualified_frames = state.get("qualified_frames", [])
    visualization_candidates = state.get("visualization_candidates", [])
    
    # 确定处理单元：优先使用 KP，否则使用 Segment
    units = knowledge_points if knowledge_points else segments
    is_kp_mode = len(knowledge_points) > 0
    
    # 构建基础映射
    # segment_id -> faults
    seg_to_faults = {}
    for fault in semantic_faults:
        sid = fault["segment_id"]
        if sid not in seg_to_faults:
            seg_to_faults[sid] = []
        seg_to_faults[sid].append(fault)
        
    # segment_id -> frames
    seg_to_frames = {}
    for frame in qualified_frames:
        sid = frame.get("segment_id", "")
        if sid not in seg_to_frames:
            seg_to_frames[sid] = []
        seg_to_frames[sid].append(frame)
        
    # segment_id -> visualization_candidate
    seg_to_viz_candidates = {v["segment_id"]: v for v in visualization_candidates}
    
    logger.log_input({
        "unit_count": len(units),
        "mode": "KP" if is_kp_mode else "Segment",
        "fault_count": len(semantic_faults)
    })
    
    try:
        llm = create_llm_client(purpose="analysis")
        
        async def process_unit(unit):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            决策逻辑：
            - 条件：is_kp_mode
            - 条件：unit_viz_candidates
            - 条件：not unit_faults
            依据来源（证据链）：
            输入参数：
            - unit: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            unit_id = unit.get("kp_id", unit.get("segment_id", ""))
            
            # 获取该单元包含的所有 segment IDs
            if is_kp_mode:
                sids = unit.get("segment_ids", [])
                summary = unit.get("merge_reason", "")  # 合并理由作为摘要
                title = unit.get("kp_title", "")
                darpa_q = " / ".join(unit.get("darpa_questions", []))
                sem_dim = unit.get("hierarchy_type", "")
            else:
                sids = [unit.get("segment_id")]
                summary = unit.get("core_semantic", {}).get("summary", "")
                title = unit.get("knowledge_point", "")
                darpa_q = f"{unit.get('darpa_question', '')} - {unit.get('darpa_question_name', '')}"
                sem_dim = unit.get("semantic_dimension", {}).get("description", "")

            # 聚合该单元下所有 segment 的关联数据
            unit_faults = []
            unit_frames = []
            unit_viz_candidates = []
            
            for sid in sids:
                unit_faults.extend(seg_to_faults.get(sid, []))
                unit_frames.extend(seg_to_frames.get(sid, []))
                vc = seg_to_viz_candidates.get(sid)
                if vc:
                    unit_viz_candidates.append(vc)
            
            # 1. 优先策略：如果有识别出的可视化场景
            if unit_viz_candidates:
                # 选取第一个场景类型作为主要依据
                vc = unit_viz_candidates[0]
                return {
                    "segment_id": unit_id,  # 这里的 key 仍保持 segment_id 以兼容下游
                    "unit_id": unit_id,
                    "need_visualization": True,
                    "source": "visualization_candidate",
                    "scene_type": vc.get("scene_type", ""),
                    "judgment_basis": [f"包含可视化场景: {vc.get('scene_type', '')}"]
                }, 0
            
            # 2. 基础策略：如果没有断层，则不需要可视化
            if not unit_faults:
                return {
                    "segment_id": unit_id,
                    "unit_id": unit_id,
                    "need_visualization": False,
                    "source": "no_fault",
                    "judgment_basis": ["无断层"]
                }, 0
            
            # 3. LLM 判定策略：存在断层，需分析必要性
            faults_info = "\n".join([
                f"- {f['fault_type_name']}: {f.get('missing_content', {}).get('must_supplement', '')}"
                for f in unit_faults
            ])
            
            prompt = VISUALIZATION_NEED_PROMPT.format(
                summary=f"【{title}】{summary}",
                darpa_question=darpa_q,
                semantic_dimension=sem_dim,
                faults_info=faults_info or "无",
                frame_count=len(unit_frames)
            )
            
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Unit {unit_id}",
                    response=f"Need Viz: {result.get('need_visualization', False)}",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                return {
                    "segment_id": unit_id,
                    "unit_id": unit_id,
                    "need_visualization": result.get("need_visualization", len(unit_faults) > 0),
                    "source": "fault",
                    "judgment_basis": result.get("judgment_basis", [])
                }, response.total_tokens
            except Exception as e:
                logger.log_warning(f"Viz judgment failed for {unit_id}: {e}")
                return {
                    "segment_id": unit_id,
                    "unit_id": unit_id,
                    "need_visualization": len(unit_faults) > 0,
                    "source": "fault_fallback",
                    "judgment_basis": [f"Error: {e}"]
                }, 0

        logger.info(f"并发判定 {len(units)} 个 {('KP' if is_kp_mode else 'segment')} 的可视化必要性...")
        results = await asyncio.gather(*[process_unit(u) for u in units])
        
        # 聚合结果
        visualization_needed = []
        total_tokens = 0
        for res, tokens in results:
            total_tokens += tokens
            visualization_needed.append(res)
        
        output = {
            "visualization_needed": visualization_needed,
            "current_step": "step16_viz_need",
            "current_step_status": "completed",
            "token_usage": {"step16_viz_need": total_tokens}
        }
        
        need_count = sum(1 for v in visualization_needed if v["need_visualization"])
        logger.log_output({"need_visualization_count": need_count})
        timing = logger.end(success=True)
        output["step_timings"] = {"step16_viz_need": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"visualization_needed": [], "errors": [{"step": "step16", "error": str(e)}]}


async def step17_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：sid not in seg_to_faults
    - 条件：not viz['need_visualization']
    - 条件：is_kp_mode and unit_id in kp_map
    依据来源（证据链）：
    - 配置字段：need_visualization, source。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step17_viz_form", state.get("output_dir", "output/logs"))
    logger.start()
    
    knowledge_points = state.get("knowledge_points", [])
    visualization_needed = state.get("visualization_needed", [])
    semantic_faults = state.get("semantic_faults", [])
    visualization_candidates = state.get("visualization_candidates", [])
    
    # 基础映射
    # segment_id -> faults
    seg_to_faults = {}
    for fault in semantic_faults:
        sid = fault["segment_id"]
        if sid not in seg_to_faults:
            seg_to_faults[sid] = []
        seg_to_faults[sid].append(fault)
    
    # segment_id -> visualization_candidate
    seg_to_viz_candidates = {v["segment_id"]: v for v in visualization_candidates}
    
    # KP 映射 (如果存在)
    kp_map = {kp["kp_id"]: kp for kp in knowledge_points}
    is_kp_mode = len(knowledge_points) > 0
    
    logger.log_input({
        "visualization_needed_count": len(visualization_needed),
        "mode": "KP" if is_kp_mode else "Segment"
    })
    
    try:
        visualization_forms = []
        
        for viz in visualization_needed:
            unit_id = viz.get("unit_id", viz.get("segment_id"))
            
            if not viz["need_visualization"]:
                visualization_forms.append({
                    "segment_id": unit_id,  # 兼容下游
                    "unit_id": unit_id,
                    "form": "none",
                    "reason": "不需要可视化"
                })
                continue
            
            # 获取该单元包含的所有 segment IDs
            if is_kp_mode and unit_id in kp_map:
                sids = kp_map[unit_id].get("segment_ids", [])
            else:
                sids = [unit_id]
                
            # 聚合数据
            all_unit_faults = []
            all_unit_viz_candidates = []
            for sid in sids:
                all_unit_faults.extend(seg_to_faults.get(sid, []))
                vc = seg_to_viz_candidates.get(sid)
                if vc:
                    all_unit_viz_candidates.append(vc)
                    
            # 1. 优先根据可视化场景确定形式
            if viz.get("source") == "visualization_candidate" and all_unit_viz_candidates:
                # 选取优先级最高或第一个场景
                vc = all_unit_viz_candidates[0]
                scene_type = vc.get("scene_type", "")
                scene_form_map = {
                    "层级/结构类": "screenshot",
                    "流程/流转类": "video_screenshot",
                    "实操/界面类": "video",
                    "对比/差异类": "screenshot",
                    "复杂逻辑关系类": "screenshot"
                }
                final_form = scene_form_map.get(scene_type, "screenshot")
                
                visualization_forms.append({
                    "segment_id": unit_id,
                    "unit_id": unit_id,
                    "form": final_form,
                    "scene_type": scene_type,
                    "reason": f"聚合场景来源: {scene_type}"
                })
                continue
            
            # 2. 根据断层类型确定形式
            forms_needed = set()
            for fault in all_unit_faults:
                fault_type = fault.get("fault_type", 1)
                form = VISUALIZATION_FORM_MAP.get(fault_type, "screenshot")
                forms_needed.add(form)
            
            # 优先级：video_screenshot > video > screenshot
            if "video_screenshot" in forms_needed:
                final_form = "video_screenshot"
            elif "video" in forms_needed:
                final_form = "video"
            elif "screenshot" in forms_needed:
                final_form = "screenshot"
            else:
                final_form = "screenshot" # 默认回退
            
            visualization_forms.append({
                "segment_id": unit_id,
                "unit_id": unit_id,
                "form": final_form,
                "fault_types": list(set([f.get("fault_type") for f in all_unit_faults])),
                "reason": f"聚合断层来源 (共{len(all_unit_faults)}个断层)"
            })
        
        output = {
            "visualization_forms": visualization_forms,
            "current_step": "step17_viz_form",
            "current_step_status": "completed"
        }
        
        logger.log_output({"form_count": len(visualization_forms)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step17_viz_form": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"visualization_forms": [], "errors": [{"step": "step17", "error": str(e)}]}


# ============================================================================
# Step 18: 核心内容判定 Prompt
# ============================================================================

CORE_CONTENT_PROMPT = """请判断以下知识点片段是否为需保留的核心视频片段。

【片段信息】
- 知识点：{knowledge_point}
- DARPA问题：{darpa_question}
- 内容摘要：{summary}

【断层类型】
{fault_types}

【核心内容条件（满足任一即为核心）】
1. 核心知识点可视化：展示核心概念/原理的动画或演示
2. 实操连贯步骤：需要连续观看才能理解的操作步骤
3. 动画展现核心机制：动态过程演示

【输出格式】
{{
  "is_core": true/false,
  "core_type": "核心知识点可视化/实操连贯步骤/动画展现核心机制/非核心",
  "video_needed": true/false,
  "reason": "判断理由"
}}"""


async def step18_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：knowledge_points
    - 条件：sid not in seg_to_faults
    - 条件：is_kp_mode
    依据来源（证据链）：
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step18_core_content", state.get("output_dir", "output/logs"))
    logger.start()
    
    knowledge_points = state.get("knowledge_points", [])
    segments = state.get("knowledge_segments", [])
    semantic_faults = state.get("semantic_faults", [])
    
    # 确定处理单元
    units = knowledge_points if knowledge_points else segments
    is_kp_mode = len(knowledge_points) > 0
    
    # 基础映射
    # segment_id -> faults
    seg_to_faults = {}
    for fault in semantic_faults:
        sid = fault["segment_id"]
        if sid not in seg_to_faults:
            seg_to_faults[sid] = []
        seg_to_faults[sid].append(fault)
    
    logger.log_input({
        "unit_count": len(units),
        "mode": "KP" if is_kp_mode else "Segment"
    })
    
    try:
        llm = create_llm_client(purpose="analysis")
        
        async def process_unit(unit):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            决策逻辑：
            - 条件：is_kp_mode
            依据来源（证据链）：
            输入参数：
            - unit: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            unit_id = unit.get("kp_id", unit.get("segment_id", ""))
            
            # 获取该单元包含的所有数据
            if is_kp_mode:
                sids = unit.get("segment_ids", [])
                summary = unit.get("merge_reason", "")
                title = unit.get("kp_title", "")
                darpa_q = " / ".join(unit.get("darpa_questions", []))
            else:
                sids = [unit.get("segment_id")]
                summary = unit.get("core_semantic", {}).get("summary", "")
                title = unit.get("knowledge_point", "")
                darpa_q = f"{unit.get('darpa_question', '')} - {unit.get('darpa_question_name', '')}"
            
            # 聚合断层类型
            unit_faults = []
            for sid in sids:
                unit_faults.extend(seg_to_faults.get(sid, []))
            
            fault_types_str = "\n".join([f"- {f['fault_type_name']}" for f in unit_faults]) or "无"
            
            prompt = CORE_CONTENT_PROMPT.format(
                knowledge_point=title,
                darpa_question=darpa_q,
                summary=summary,
                fault_types=fault_types_str
            )
            
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Unit {unit_id}",
                    response=f"Is Core: {result.get('is_core', False)}",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                return {
                    "segment_id": unit_id,  # 兼容下游
                    "unit_id": unit_id,
                    "is_core": result.get("is_core", False),
                    "core_type": result.get("core_type", "非核心"),
                    "video_needed": result.get("video_needed", False),
                    "reason": result.get("reason", "")
                }, response.total_tokens
            except Exception as e:
                logger.log_warning(f"Core judgment failed for {unit_id}: {e}")
                return {
                    "segment_id": unit_id,
                    "unit_id": unit_id,
                    "is_core": False,
                    "core_type": "错误推断",
                    "video_needed": False,
                    "reason": f"Error: {e}"
                }, 0

        logger.info(f"并发判定 {len(units)} 个 {('KP' if is_kp_mode else 'segment')} 的核心内容属性...")
        results = await asyncio.gather(*[process_unit(u) for u in units])
        
        # 聚合结果
        core_content_judgment = []
        total_tokens = 0
        for res, tokens in results:
            total_tokens += tokens
            core_content_judgment.append(res)
        
        output = {
            "core_content_judgment": core_content_judgment,
            "current_step": "step18_core_content",
            "current_step_status": "completed",
            "token_usage": {"step18_core_content": total_tokens}
        }
        
        core_count = sum(1 for c in core_content_judgment if c["is_core"])
        logger.log_output({"core_count": core_count})
        timing = logger.end(success=True)
        output["step_timings"] = {"step18_core_content": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"core_content_judgment": [], "errors": [{"step": "step18", "error": str(e)}]}


# ============================================================================
# Step 19: 辅助信息生成 Prompt (媒体衔接引导语)
# ============================================================================

AUXILIARY_INFO_PROMPT = """请为以下知识点生成媒体衔接引导语，用于从文字过渡到视频/截图。

【知识点信息】
- 知识点：{knowledge_point}
- 内容摘要：{summary}
- 断层信息：{fault_info}

【可视化形式】{visualization_form}

【需要生成的衔接语类型】
{transition_types}

【生成要求】
1. **核心目的**：引导语是文字稿和可视化内容之间的**桥梁**，帮助读者从文字理解平滑过渡到视觉观看
2. **衔接功能**：
   - 承接前文：简要呼应文字稿中刚讲过的内容
   - 引导观看：告诉读者接下来的视频/截图会展示什么
   - 明确重点：指出观看时应该关注的要点
3. **video_transition 必须使用分点格式**，每个要点单独一行，使用 "- " 开头
4. **避免纯粹重复**：可以呼应原文关键词，但要提供观看指引，而非照搬原文
5. post_media_summary 用于在媒体之后总结视觉内容补充了什么信息

【输出格式】
{{
  "video_transition": "请观看以下视频片段：\\n- 承接+引导：xxxx\\n- 观看重点：xxxx\\n- 预期收获：xxxx（仅当需要视频时生成，必须分点）",
  "screenshot_transition": "下图展示了xxxx（承接文字，引导观看）",
  "post_media_summary": "通过上述视频/截图，我们可以看到xxxx（总结视觉补充的信息）"
}}

【video_transition 示例】
✅ 好的衔接（承接文字 + 引导观看）：
"请观看以下视频片段：\\n- 它将动态演示刚才提到的顺序查找执行过程\\n- 重点关注从头到尾逐个比较的步骤\\n- 观察查找成功和失败时的不同终止条件"

❌ 纯粹重复（缺少衔接和引导）：
"请观看以下视频片段，它将动态演示顺序查找在单向链表中的完整执行过程。视频会从头部节点开始，逐步展示如何依次比较每个节点的值。"
"""


# ============================================================================
# [DEPRECATED] 旧的边界精细化函数 - 已被 _verify_candidates_with_vision 替代
# 保留供参考，不再使用
# ============================================================================

async def _refine_video_boundary(
    video_path: str,
    time_range: Dict[str, float],
    title: str,
    summary: str,
    output_dir: str,
    unit_id: str = "default"
) -> Dict[str, float]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not video_path or end <= start
    - 条件：not valid_paths
    - 条件：new_end <= new_start
    依据来源（证据链）：
    - 输入参数：video_path。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - time_range: 函数入参（类型：Dict[str, float]）。
    - title: 函数入参（类型：str）。
    - summary: 函数入参（类型：str）。
    - output_dir: 目录路径（类型：str）。
    - unit_id: 标识符（类型：str）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    import warnings
    warnings.warn("_refine_video_boundary is deprecated, use _verify_candidates_with_vision instead", DeprecationWarning)
    start = time_range["start_sec"]
    end = time_range["end_sec"]
    
    # 简单的有效性检查
    if not video_path or end <= start:
        return time_range
        
    # 定义截帧时间点：Start, End+1, End+3, End+5
    # 如果 End+ 后续时间超出视频长度，FrameCapture 会返回空或最后一帧，需注意
    capture_times = [start, end + 1.0, end + 3.0, end + 5.0]
    
    try:
        # 使用 run_in_executor 避免阻塞事件循环
        loop = asyncio.get_running_loop()
        
        def _capture():
            # 临时目录：使用ID确保唯一
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            决策逻辑：
            - 条件：res.is_valid
            依据来源（证据链）：
            输入参数：
            - 无。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            temp_dir = f"{output_dir}/temp_boundary_{unit_id}"
            with FrameCapture(video_path, temp_dir) as cap:
                paths = []
                for t in capture_times:
                    # 尝试截取
                    frame_id = f"b_check_{t:.1f}".replace(".", "_")
                    # 忽略 enhance 以提高速度
                    res = cap.capture_frame(t, frame_id, {"sharpen": False}) 
                    if res.is_valid:
                        paths.append(res.frame_path)
                    else:
                        paths.append(None)
                return paths
                
        frame_paths = await loop.run_in_executor(None, _capture)
        
        # 过滤无效帧
        valid_paths = [p for p in frame_paths if p]
        if not valid_paths:
            return time_range
            
        vision_client = create_vision_client()
        
        t_start = f"{start:.1f}"
        t_end_1 = f"{end+1.0:.1f}"
        t_end_3 = f"{end+3.0:.1f}"
        t_end_5 = f"{end+5.0:.1f}"
        
        prompt = VIDEO_BOUNDARY_PROMPT.format(
            title=title,
            summary=summary,
            t_start=t_start,
            t_end_1=t_end_1,
            t_end_3=t_end_3,
            t_end_5=t_end_5
        )
        
        result, _ = await vision_client.complete_json(prompt, image_paths=valid_paths)
        
        # 应用调整
        adj_start = result.get("start_adjustment_sec", 0.0)
        ext_end = result.get("end_extension_sec", 0.0)
        
        # 限制
        adj_start = max(0.0, min(adj_start, 5.0)) # 最多向后调整5秒
        ext_end = max(0.0, min(ext_end, 8.0))     # 最多延长8秒
        
        new_start = start + adj_start
        new_end = end + ext_end
        
        # 简单的校验
        if new_end <= new_start:
            return time_range
            
        return {"start_sec": new_start, "end_sec": new_end}
        
    except Exception as e:
        get_logger("viz_refine").log_warning(f"Boundary refinement failed: {e}")
        return time_range


async def step19_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：knowledge_points
    - 条件：sid not in seg_to_faults
    - 条件：form == 'none'
    依据来源（证据链）：
    - 配置字段：screenshot_transition, start_sec, video_needed, video_time_range, video_transition。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    改造点：
    1. 适配 KP 模式，聚合子片段的所有待补断层和场景信息
    2. 对齐视频范围逻辑：显式遵守 Step 18 的 video_needed 结果"""
    logger = get_logger("step19_auxiliary", state.get("output_dir", "output/logs"))
    logger.start()
    
    knowledge_points = state.get("knowledge_points", [])
    segments = state.get("knowledge_segments", [])
    visualization_forms = state.get("visualization_forms", [])
    core_content_judgment = state.get("core_content_judgment", [])
    semantic_faults = state.get("semantic_faults", [])
    visualization_candidates = state.get("visualization_candidates", [])
    
    # 确定处理单元
    units = knowledge_points if knowledge_points else segments
    is_kp_mode = len(knowledge_points) > 0
    
    # 基础映射
    form_map = {v["segment_id"]: v for v in visualization_forms}
    core_map = {c["segment_id"]: c for c in core_content_judgment}
    viz_candidate_map = {v["segment_id"]: v for v in visualization_candidates}
    
    # segment_id -> faults
    seg_to_faults = {}
    for fault in semantic_faults:
        sid = fault["segment_id"]
        if sid not in seg_to_faults:
            seg_to_faults[sid] = []
        seg_to_faults[sid].append(fault)
        
    video_path = state.get("video_path", "")
    output_dir = state.get("output_dir", "output")
    storage = LocalStorage(output_dir + "/local_storage")
    
    logger.log_input({
        "unit_count": len(units),
        "mode": "KP" if is_kp_mode else "Segment"
    })
    
    # 预加载时间戳数据
    all_subtitle_ts = storage.load_subtitle_timestamps()
    all_sentence_ts = storage.load_sentence_timestamps()
    
    try:
        llm = create_llm_client(purpose="analysis")
        
        async def process_unit(unit):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            决策逻辑：
            - 条件：form == 'none'
            - 条件：is_kp_mode
            - 条件：form in ['video', 'video_screenshot']
            依据来源（证据链）：
            - 配置字段：screenshot_transition, start_sec, video_needed, video_transition。
            输入参数：
            - unit: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            unit_id = unit.get("kp_id", unit.get("segment_id", ""))
            
            # 获取对应的 form 和核心判定 (Step 17 和 Step 18 的结果已按 unit_id 索引)
            form_info = form_map.get(unit_id, {})
            core = core_map.get(unit_id, {})
            
            form = form_info.get("form", "none")
            
            # 策略：如果不需要媒体，直接返回
            if form == "none":
                return {
                    "segment_id": unit_id,
                    "unit_id": unit_id,
                    "video_transition": None,
                    "screenshot_transition": None,
                    "post_media_summary": None,
                    "video_time_range": None
                }, 0
            
            # 准备聚合数据供提示词使用
            if is_kp_mode:
                sids = unit.get("segment_ids", [])
                summary = unit.get("merge_reason", "")
                title = unit.get("kp_title", "")
                # 聚合 KP 下的所有断层
                unit_faults = []
                unit_viz_candidates = []
                for sid in sids:
                    unit_faults.extend(seg_to_faults.get(sid, []))
                    vc = viz_candidate_map.get(sid)
                    if vc: unit_viz_candidates.append(vc)
                
                # 计算视频范围：改用 Fault 级别精细定位
                # 只有当可视化形式包含视频时才进行边界精细化
                if core.get("video_needed") and "video" in form:
                    # 新逻辑：遍历 KP 下的每个 fault，使用 fault 级别的精确时间范围
                    refined_ranges = []
                    
                    if unit_faults:
                        # 有明确的断层，使用断层位置作为搜索范围（最小语义单元）
                        for fault in unit_faults:
                            fault_loc = fault.get("fault_location", {})
                            if fault_loc.get("start_sec") is not None:
                                fault_range = {
                                    "start_sec": fault_loc.get("start_sec", 0),
                                    "end_sec": fault_loc.get("end_sec", fault_loc.get("start_sec", 0) + 10)
                                }
                                # 使用 fault 的精确范围进行边界精细化
                                refined = await _refine_with_retry(
                                    video_path, fault_range, 
                                    title=fault.get("fault_type_name", title),
                                    summary=fault.get("visual_form", summary), 
                                    output_dir=output_dir,
                                    max_retries=2,  # 降低重试次数以减少 Vision API 调用
                                    logger=logger, 
                                    unit_id=f"{unit_id}_{fault.get('fault_id', 'F')}",
                                    video_duration=state.get("video_duration", 3600.0)
                                )
                                refined_ranges.append(refined)
                    
                    if refined_ranges:
                        # 合并所有 fault 的精细化范围
                        video_time_range = {
                            "start_sec": min(r["start_sec"] for r in refined_ranges),
                            "end_sec": max(r["end_sec"] for r in refined_ranges)
                        }
                    else:
                        # 降级：无 fault 时使用 KP 范围
                        kp_ts = unit.get("time_range", {})
                        rough_range = _calculate_rough_video_range(
                            unit_id, kp_ts, all_subtitle_ts, all_sentence_ts
                        )
                        video_time_range = await _refine_with_retry(
                            video_path, rough_range, title, summary, output_dir, 
                            max_retries=2, logger=logger, unit_id=unit_id,
                            video_duration=state.get("video_duration", 3600.0)
                        )
                else:
                    video_time_range = None
            else:
                # 回退：单段模式
                sids = [unit.get("segment_id")]
                summary = unit.get("core_semantic", {}).get("summary", "")
                title = unit.get("knowledge_point", "")
                unit_faults = seg_to_faults.get(unit_id, [])
                viz_candidate = viz_candidate_map.get(unit_id)
                
                if core.get("video_needed"):
                    # 阶段1: 使用字幕粗定位
                    segment_ts = storage.get_segment_timestamp(unit_id) or {}
                    rough_range = _calculate_rough_video_range(
                        unit_id, segment_ts, all_subtitle_ts, all_sentence_ts
                    )
                    
                    # 阶段2: 使用视觉分析精细化（带重试）
                    video_time_range = await _refine_with_retry(
                        video_path, rough_range, title, summary, output_dir,
                        max_retries=2, logger=logger, unit_id=unit_id,
                        video_duration=state.get("video_duration", 3600.0)
                    )
                else:
                    video_time_range = None
            
            # 注意：_refine_with_retry 已经包含了视觉边界精细化逻辑
            # 不需要再调用 _refine_video_boundary
            
            # 确定引导语类型
            transition_types = []
            if form in ["video", "video_screenshot"]:
                transition_types.append("- video_transition: 视频引导语")
            if form in ["screenshot", "video_screenshot"]:
                transition_types.append("- screenshot_transition: 截图引导语")
            transition_types.append("- post_media_summary: 媒体后总结")
            
            # 格式化断层信息
            fault_info = "\n".join([
                f"- {f['fault_type_name']}: {f.get('missing_content', {}).get('must_supplement', '需要补充')}"
                for f in unit_faults
            ]) or "无特定断层"
            
            prompt = AUXILIARY_INFO_PROMPT.format(
                knowledge_point=title,
                summary=summary,
                fault_info=fault_info,
                visualization_form=form,
                transition_types="\n".join(transition_types)
            )
            
            try:
                result, response = await llm.complete_json(prompt)
                tokens = response.total_tokens if response else 0
                
                logger.log_llm_call(
                    prompt=f"Unit {unit_id}",
                    response=f"V:{'Y' if result.get('video_transition') else 'N'} S:{'Y' if result.get('screenshot_transition') else 'N'}",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model
                )
            except Exception as e_llm:
                logger.log_warning(f"LLM Call Failed for {unit_id}: {e_llm}")
                result = {}
                tokens = 0
            
            # 默认补全
            v_trans = result.get("video_transition")
            if not v_trans and form in ["video", "video_screenshot"]:
                v_trans = "请观看以下视频片段，了解具体演示过程。"
                
            s_trans = result.get("screenshot_transition")
            if not s_trans and form in ["screenshot", "video_screenshot"]:
                s_trans = "下图展示了关键步骤的截图，请结合文字理解。"
                
            p_summary = result.get("post_media_summary")
            if not p_summary:
                p_summary = "通过上述演示，我们可以更直观地理解该知识点的核心逻辑。"

            return {
                "segment_id": unit_id,
                "unit_id": unit_id,
                "form": form,
                "video_transition": v_trans if form in ["video", "video_screenshot"] else None,
                "screenshot_transition": s_trans if form in ["screenshot", "video_screenshot"] else None,
                "post_media_summary": p_summary,
                "video_time_range": video_time_range
            }, tokens

        results = await asyncio.gather(*[process_unit(u) for u in units])
        
        # 收集辅导信息和视频片段
        auxiliary_information = []
        video_clips = []  # 新增：收集视频片段
        total_tokens = 0
        
        from ..tools.video_utils import cut_video_segment
        
        for res, tokens in results:
            total_tokens += tokens
            auxiliary_information.append(res)
            
            # 新增：如果有视频时间范围，切割视频
            if res.get("video_time_range"):
                unit_id = res.get("unit_id")
                time_range = res["video_time_range"]
                
                # 切割视频片段
                video_clip_path = cut_video_segment(
                    source_video=video_path,
                    start_sec=time_range["start_sec"],
                    end_sec=time_range["end_sec"],
                    output_path=f"{output_dir}/video_clips/{unit_id}.mp4",
                    log_prefix=f"[Step19:{unit_id}] "
                )
                
                if video_clip_path:
                    video_clips.append({
                        "kp_id": unit_id,
                        "video_path": video_clip_path,
                        "start_sec": time_range["start_sec"],
                        "end_sec": time_range["end_sec"],
                        "duration": time_range["end_sec"] - time_range["start_sec"],
                        "scene_type": res.get("scene_type", "unknown")
                    })
                    logger.info(f"✓ Video clip created for {unit_id}: {video_clip_path}")
                else:
                    logger.warning(f"✗ Failed to create video clip for {unit_id}")
        
        output = {
            "auxiliary_information": auxiliary_information,
            "video_clips": video_clips,  # 新增：视频片段列表
            "current_step": "step19_auxiliary",
            "current_step_status": "completed",
            "token_usage": {"step19_auxiliary": total_tokens}
        }
        
        logger.log_output({
            "aux_count": len(auxiliary_information),
            "video_clip_count": len(video_clips)  # 新增：记录视频片段数量
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step19_auxiliary": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"auxiliary_information": [], "errors": [{"step": "step19", "error": str(e)}]}


def _calculate_optimized_video_range(segment_id: str, segment_ts: Dict, faults: List[Dict], viz_candidate: Dict, subtitle_ts: Dict = None, sentence_ts: Dict = None) -> Dict:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：sentence_ts and source_sentence_ids
    - 条件：viz_candidate
    - 条件：sentence_ts
    依据来源（证据链）：
    - 输入参数：sentence_ts, viz_candidate。
    输入参数：
    - segment_id: 标识符（类型：str）。
    - segment_ts: 函数入参（类型：Dict）。
    - faults: 函数入参（类型：List[Dict]）。
    - viz_candidate: 函数入参（类型：Dict）。
    - subtitle_ts: 函数入参（类型：Dict）。
    - sentence_ts: 函数入参（类型：Dict）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    策略（用户反馈优化）：
    1. 优先使用阶段2生成的句子时间戳（sentence_ts）作为精确边界
    2. 通过segment_ts.source_sentence_ids获取相关句子
    3. 只在结束时适当延长（后2秒），开始时精确使用句子边界
    4. 可选：使用磁吸对齐到最近的句子边界"""
    # 获取segment的source_sentence_ids
    source_sentence_ids = segment_ts.get("source_sentence_ids", [])
    
    # 如果有sentence_ts和source_sentence_ids，使用句子时间戳计算精确边界
    if sentence_ts and source_sentence_ids:
        sentence_starts = []
        sentence_ends = []
        
        for sid in source_sentence_ids:
            if sid in sentence_ts:
                sentence_starts.append(sentence_ts[sid].get("start_sec", 0))
                sentence_ends.append(sentence_ts[sid].get("end_sec", 0))
        
        if sentence_starts and sentence_ends:
            # 使用句子的精确时间范围
            final_s = min(sentence_starts)
            final_e = max(sentence_ends) + 2.0  # 结束时间加2秒缓冲
        else:
            # 回退到segment时间
            final_s = segment_ts.get("start_sec", 0)
            final_e = segment_ts.get("end_sec", 0) + 2.0
    else:
        # 回退到segment时间
        final_s = segment_ts.get("start_sec", 0)
        final_e = segment_ts.get("end_sec", 0) + 2.0
    
    # 如果有可视化场景的timestamp，可以用来微调结束时间
    if viz_candidate:
        ts = viz_candidate.get("timestamp")
        if ts:
            if isinstance(ts, dict):
                viz_end = ts.get("end_sec", final_e)
                # 确保结束时间覆盖可视化场景
                final_e = max(final_e, viz_end + 2.0)
            else:
                # float类型：单一时间点
                final_e = max(final_e, float(ts) + 2.0)
    
    # 确保不超出合理范围
    final_s = max(0, final_s)
    
    # 磁吸对齐到最近的句子边界（如果有sentence_ts）
    if sentence_ts:
        sorted_sents = []
        for sid, data in sentence_ts.items():
            sorted_sents.append({
                "sent_id": sid,
                "start": data.get("start_sec", 0),
                "end": data.get("end_sec", 0)
            })
        sorted_sents.sort(key=lambda x: x["start"])
        
        # 寻找最近的开始点
        nearest_start = final_s
        min_start_diff = float("inf")
        
        # 寻找最近的结束点
        nearest_end = final_e
        min_end_diff = float("inf")
        
        for sent in sorted_sents:
            # 检查开始时间（1秒吸附阈值）
            diff_s = abs(sent["start"] - final_s)
            if diff_s < 1.0 and diff_s < min_start_diff:
                min_start_diff = diff_s
                nearest_start = sent["start"]
            
            # 检查结束时间（2秒吸附阈值）
            diff_e = abs(sent["end"] - final_e)
            if diff_e < 2.0 and diff_e < min_end_diff:
                min_end_diff = diff_e
                nearest_end = sent["end"]
        
        final_s = nearest_start
        final_e = nearest_end
    
    return {"start_sec": float(final_s), "end_sec": float(final_e)}


def _calculate_rough_video_range(
    segment_id: str, 
    segment_ts: Dict,
    subtitle_ts: Dict = None,
    sentence_ts: Dict = None
) -> Dict:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not subtitle_ids or not subtitle_ts
    - 条件：not starts
    - 条件：sentence_ts and sent_id in sentence_ts
    依据来源（证据链）：
    - 输入参数：sentence_ts, subtitle_ts。
    - 阈值常量：TRANSITION_WORDS。
    输入参数：
    - segment_id: 标识符（类型：str）。
    - segment_ts: 函数入参（类型：Dict）。
    - subtitle_ts: 函数入参（类型：Dict）。
    - sentence_ts: 函数入参（类型：Dict）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    策略（基于第一性原理）：
    1. 使用subtitle_timestamps获取更细腻的时间粒度
    2. 以触发词为锚点，向前后扩展
    3. 遇到过渡词或达到上限时停止（不使用语义相似度判断）
    注意：这只是粗定位，真正的边界由视觉分析决定"""
    # 获取segment关联的subtitle_ids
    source_sentence_ids = segment_ts.get("source_sentence_ids", [])
    subtitle_ids = []
    for sent_id in source_sentence_ids:
        if sentence_ts and sent_id in sentence_ts:
            sent_data = sentence_ts.get(sent_id, {})
            subtitle_ids.extend(sent_data.get("source_subtitle_ids", []))
    
    # 如果没有subtitle数据，回退到sentence时间戳
    if not subtitle_ids or not subtitle_ts:
        if sentence_ts and source_sentence_ids:
            starts = [sentence_ts[sid].get("start_sec", 0) for sid in source_sentence_ids if sid in sentence_ts]
            ends = [sentence_ts[sid].get("end_sec", 0) for sid in source_sentence_ids if sid in sentence_ts]
            if starts and ends:
                return {"start_sec": min(starts), "end_sec": max(ends)}
        return {"start_sec": segment_ts.get("start_sec", 0), "end_sec": segment_ts.get("end_sec", 0)}
    
    # 获取所有字幕ID（按时间排序）
    all_subtitle_ids = list(subtitle_ts.keys())
    all_subtitle_ids.sort(key=lambda x: subtitle_ts[x]["start_sec"])
    
    # 触发词锚点（取segment字幕的中间位置）
    anchor_sub_id = subtitle_ids[len(subtitle_ids) // 2]
    try:
        anchor_idx = all_subtitle_ids.index(anchor_sub_id)
    except ValueError:
        # 回退
        return {"start_sec": segment_ts.get("start_sec", 0), "end_sec": segment_ts.get("end_sec", 0)}
    
    # 扩展参数
    MAX_EXPAND_BEFORE = 30
    MAX_EXPAND_AFTER = 45
    TRANSITION_WORDS = ["接下来", "下面我们看", "那么", "现在开始", "我们来看", "回到", "刚才", "好的", "OK"]
    
    start_idx = anchor_idx
    end_idx = anchor_idx
    
    # 向前扩展（遇过渡词或上限停止）
    for i in range(MAX_EXPAND_BEFORE):
        if start_idx <= 0:
            break
        prev_idx = start_idx - 1
        prev_text = subtitle_ts[all_subtitle_ids[prev_idx]].get("text", "")
        if any(word in prev_text for word in TRANSITION_WORDS):
            break
        start_idx = prev_idx
    
    # 向后扩展（遇过渡词或上限停止）
    for i in range(MAX_EXPAND_AFTER):
        if end_idx >= len(all_subtitle_ids) - 1:
            break
        next_idx = end_idx + 1
        next_text = subtitle_ts[all_subtitle_ids[next_idx]].get("text", "")
        if any(word in next_text for word in TRANSITION_WORDS):
            break
        end_idx = next_idx
    
    # 输出粗略时间范围
    window_ids = all_subtitle_ids[start_idx:end_idx+1]
    starts = [subtitle_ts[sid]["start_sec"] for sid in window_ids if sid in subtitle_ts]
    ends = [subtitle_ts[sid]["end_sec"] for sid in window_ids if sid in subtitle_ts]
    
    if not starts:
        return {"start_sec": 0, "end_sec": 0}
    
    return {"start_sec": min(starts), "end_sec": max(ends)}


async def _verify_candidates_with_vision(
    video_path: str,
    best_start,  # 保留但不使用（兼容性）
    best_end,    # 保留但不使用（兼容性）
    alt_starts: list,  # 保留但不使用（兼容性）
    alt_ends: list,    # 保留但不使用（兼容性）
    all_frames: List[Dict],
    title: str,
    summary: str,
    output_dir: str,
    unit_id: str,
    logger,
    candidate_frames: List[str] = None,  # 新增：直接传递的帧路径列表
    frame_labels: List[str] = None,      # 新增：直接传递的帧标签列表
    subtitle_context: str = ""           # 新增：字幕上下文
) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：candidate_frames is None or frame_labels is None
    - 条件：not candidate_frames
    - 条件：subtitle_context
    依据来源（证据链）：
    - 输入参数：candidate_frames, frame_labels, logger, subtitle_context。
    - 配置字段：frame_path。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - best_start: 起止时间/区间边界（类型：未标注）。
    - best_end: 起止时间/区间边界（类型：未标注）。
    - alt_starts: 起止时间/区间边界（类型：list）。
    - alt_ends: 起止时间/区间边界（类型：list）。
    - all_frames: 函数入参（类型：List[Dict]）。
    - title: 函数入参（类型：str）。
    - summary: 函数入参（类型：str）。
    - output_dir: 目录路径（类型：str）。
    - unit_id: 标识符（类型：str）。
    - logger: 函数入参（类型：未标注）。
    - candidate_frames: 函数入参（类型：List[str]）。
    - frame_labels: 函数入参（类型：List[str]）。
    - subtitle_context: 函数入参（类型：str）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    from ..llm.client import create_vision_client
    
    # 如果调用方未提供帧列表，从 all_frames 构建（向后兼容）
    if candidate_frames is None or frame_labels is None:
        candidate_frames = []
        frame_labels = []
        for idx, fd in enumerate(all_frames):
            if fd.get("frame_path"):
                label = f"frame_{idx}@{fd['timestamp']:.1f}s"
                candidate_frames.append(fd["frame_path"])
                frame_labels.append(label)
    
    if not candidate_frames:
        return {
            "boundary_status": "need_expand_both",
            "reason": "No frames to validate",
            "expand_suggestion": "2.0"
        }
    
    # 构建Vision AI验证提示词（融入用户提供的场景标准）
    # 添加字幕上下文（如果提供）
    # subtitle_context 已作为参数传递
    subtitle_section = ""
    if subtitle_context:
        subtitle_section = f"""
【字幕上下文】
{subtitle_context}

提示：字幕可以帮助你理解视频内容的语义边界，判断动画/操作的开始和结束时机。
"""
    
    prompt = f"""请从以下截图序列中，判断"{title}"的视频片段的精确起止边界。

【知识点信息】
- 标题：{title}
- 摘要：{summary}
{subtitle_section}
【提供的帧序列】
{chr(10).join([f"- {label}" for label in frame_labels])}

【场景识别】
首先判断该片段属于以下哪种场景：
1. **动画场景**：流程图、柱状图、分步文字动画等视觉元素的动态呈现
2. **实操场景**：IDE编码、PS绘图、Excel计算等软件工具的实时操作

---

## 一、动画场景标准

### （一）开始帧判断标准（扩展版）

**核心逻辑：基准静帧 → 扰动发生 → 趋势确认**

**模式 A：生成型（无中生有）**
1. **基准静帧**：确认某一帧为动画启动前的纯静态页（无动画载体）。
2. **变化帧1**：下一帧出现「未完全呈现的动画元素雏形」（如流程图仅1个节点、柱状图仅坐标轴）。
3. **变化帧2**：雏形出现可识别的扩展（如新增箭头、柱形增高）。

**模式 B：突变型（状态改变）**
1. **基准静帧**：画面处于常态（如黑色代码字）。
2. **变化帧1**：属性发生跳变（如某行代码突然变红、背景高亮）。
3. **变化帧2**：该状态持续保持或引发后续变化。
*适用：节点变色、代码高亮、关键词加粗*

**模式 C：位移型（位置移动）**
1. **基准静帧**：物体位于起始位置 A。
2. **变化帧1**：由于移动产生的残影、中间态或位置略微偏移。
3. **变化帧2**：明确的位置变化。
*适用：数据包传输、指针移动*

### （二）结束帧判断标准（扩展版）

**核心逻辑：动态持续 → 最终稳态 → 完整性/结果验证**

**模式 A：完成型（结构完整）**
1. **最后变化帧**：动画元素最后一次扩展。
2. **连续稳定帧**：之后连续帧像素差异<1%，形态完全一致。
3. **元素完整性**：所有节点、箭头、数值齐全。

**模式 B：响应型（反馈明确）**
1. **最后变化帧**：属性变化完成（如变色完成）。
2. **结果验证**：出现明确的视觉反馈（如弹出提示框、数值更新、对钩图标）。
3. **连续稳定帧**：画面静止。

**模式 C：抵达型（到达目标）**
1. **最后变化帧**：物体到达目标位置 B。
2. **停止运动**：物体不再发生位移。
3. **交互反馈**：与目标对象发生交互（可选，如目标节点变亮）。

### （三）实操场景标准（复用原版并微调）

**开始帧**：基准静帧（无工具） → 出现工具界面（可操作） → 有效操作痕迹（光标/输入）。
**结束帧**：最后操作帧 → 连续稳定帧（无动作） → 实操结果达成（Output/UI反馈）。

---

【判断规则】
1. **精确定位**：如果能从提供的帧中找到符合标准的起止帧，返回对应的帧标签
2. **区间定位**：如果真正的边界在两帧之间，且间隔<1s：
   - 开始边界：选择前一帧的标签（宁早勿晚，确保不遗漏动画开始）
   - 结束边界：选择后一帧的标签（宁晚勿早，确保动画完整呈现）
3. **范围扩展**：如果真正的开始点应该在最早帧之前，或结束点应该在最晚帧之后，明确指出需要扩展

【输出格式】
{{
  "scene_type": "动画场景" 或 "实操场景" 或 "其他",
  "boundary_status": "found" 或 "need_resample",
  "start_frame_label": "具体的帧标签，如 'frame_3@46.4s'" (仅当 boundary_status="found" 时),
  "end_frame_label": "具体的帧标签，如 'frame_8@51.2s'" (仅当 boundary_status="found" 时),
  "reason": "详细说明判断依据，引用上述标准的具体条款",
  "resample_points": [45.0, 47.5, 50.0, 52.5, 55.0]  (数组，单位：秒。如果 boundary_status="need_resample"，提供具体的重采样时间点列表)
}}

**特别说明**：
- **核心警告：请严格区分『中间停顿』和『最终结束』**。
  - 如果画面暂时静止，但字幕上下文显示仍在讲解当前动作的后续步骤（如"接下来..."），或者画面元素处于明显未完成状态（如流程图箭头悬空、只有"Step 1"没有"Step 2"），**请判定为 need_expand_end**，绝不要过早结束！
  - 只有当画面静止**且**字幕话题明确切换或当前动作逻辑闭环时，才判定为结束。
- 如果场景类型为"其他"（纯讲解场景），仍应返回 boundary_status="found"，并选择提供的第一帧和最后一帧作为起止帧
- start_frame_label 和 end_frame_label 必须是具体的帧标签（如 "frame_0@45.2s"），不能是"无"或其他文字描述
- 如果当前帧序列不足以判断边界，返回 boundary_status="need_resample"，并在 resample_points 中提供5-10个具体的重采样时间点（秒）
- resample_points 应该是一个数字数组，例如 [45.0, 47.5, 50.0, 52.5, 55.0]，覆盖你认为可能包含边界的关键时间点

注意：resample_points 必须是数字数组，不要使用文字描述。"""

    try:
        vision_client = create_vision_client()
        result, _ = await vision_client.complete_json(prompt, image_paths=candidate_frames)
        await vision_client.close()
        
        if logger:
            status = result.get('boundary_status', 'unknown')
            scene = result.get('scene_type', 'unknown')
            logger.info(f"[VISION VERIFY] Status: {status}, Scene: {scene}")
            logger.info(f"[VISION VERIFY] Reason: {result.get('reason', '')[:100]}")
            if status == "found":
                logger.info(f"[VISION VERIFY] Selected: START={result.get('start_frame_label')}, END={result.get('end_frame_label')}")
            elif "expand" in status:
                logger.info(f"[VISION VERIFY] Expand suggestion: {result.get('expand_suggestion')}s")
        
        return {
            "boundary_status": result.get("boundary_status", "found"),
            "start_frame_label": result.get("start_frame_label"),
            "end_frame_label": result.get("end_frame_label"),
            "scene_type": result.get("scene_type"),
            "reason": result.get("reason", ""),
            "expand_suggestion": result.get("expand_suggestion")
        }
    except Exception as e:
        if logger:
            logger.log_warning(f"Vision AI validation failed: {e}")
        # 降级：如果 Vision AI 失败，返回需要扩展的状态
        return {
            "boundary_status": "need_expand_both",
            "reason": f"Vision API error: {e}",
            "expand_suggestion": "2.0"
        }


async def _deprecated_refine_with_retry(
    video_path: str,
    rough_range: Dict[str, float],
    title: str,
    summary: str,
    output_dir: str,
    max_retries: int = 3,
    logger = None,
    unit_id: str = "default"
) -> Dict[str, float]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过OpenCV 图像处理、HTTP 调用、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：logger
    - 条件：all_subtitles
    - 条件：subtitle_context
    依据来源（证据链）：
    - 输入参数：logger。
    - 配置字段：boundary_status, frame_path。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - rough_range: 函数入参（类型：Dict[str, float]）。
    - title: 函数入参（类型：str）。
    - summary: 函数入参（类型：str）。
    - output_dir: 目录路径（类型：str）。
    - max_retries: 函数入参（类型：int）。
    - logger: 函数入参（类型：未标注）。
    - unit_id: 标识符（类型：str）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    重试触发条件：
    1. Vision AI 返回 need_resample (需要重新采样特定时间点)
    2. 无法采集到足够的帧数据
    改进：
    - 提供字幕上下文给 Vision AI
    - 处理 Vision AI 返回的具体重采样时间点"""
    from ..tools.storage import LocalStorage
    
    current_range = rough_range.copy()
    retry_history = []
    
    # 获取字幕上下文
    storage = LocalStorage(output_dir)
    subtitle_context = ""
    try:
        # 获取时间范围内的所有字幕
        timestamps_map = storage.load_subtitle_timestamps()
        all_subtitles = list(timestamps_map.values()) if timestamps_map else []
        if all_subtitles:
            start_sec = rough_range["start_sec"]
            end_sec = rough_range["end_sec"]
            # 扩展范围以获取上下文（前后各5秒）
            context_start = max(0, start_sec - 5)
            context_end = end_sec + 5
            
            relevant_subs = [
                sub for sub in all_subtitles
                if context_start <= sub.get("start_sec", 0) <= context_end
            ]
            
            if relevant_subs:
                subtitle_lines = []
                for sub in relevant_subs[:10]:  # 最多10条字幕
                    start_time = sub.get("start_sec", 0)
                    text = sub.get("text", "")
                    subtitle_lines.append(f"[{start_time:.1f}s] {text}")
                subtitle_context = "\n".join(subtitle_lines)
    except Exception as e:
        if logger:
            logger.log_warning(f"Failed to fetch subtitle context: {e}")
    
    if logger:
        logger.info(f"[REFINE] Starting boundary refinement for '{title}'")
        logger.info(f"[REFINE] Initial range: {rough_range['start_sec']:.1f}s - {rough_range['end_sec']:.1f}s ({rough_range['end_sec']-rough_range['start_sec']:.1f}s)")
        if subtitle_context:
            logger.info(f"[REFINE] Subtitle context: {len(subtitle_context)} chars")
    
    for retry_round in range(max_retries):
        # 使用 unit_id 作为 session_id 确保并发安全
        session_id = f"{unit_id}_r{retry_round}"
        analyzer = FrameBoundaryAnalyzer(video_path, output_dir, session_id=session_id)
        try:
            analysis_result = analyzer.analyze_boundary(current_range, title, summary)
        finally:
            analyzer.cleanup()
        
        all_frames = analysis_result.all_frames
        
        # 1. 如果没有采集到帧，扩展范围后重试
        if not all_frames or len(all_frames) < 3:
            if logger:
                logger.log_warning(f"[RETRY {retry_round+1}] Insufficient frames ({len(all_frames) if all_frames else 0}), expanding range")
            current_range["start_sec"] = max(0, current_range["start_sec"] - 2.0)
            current_range["end_sec"] = current_range["end_sec"] + 3.0
            continue
        
        # 2. 准备帧序列供 Vision AI 分析
        candidate_frames = []
        frame_labels = []
        frame_label_map = {}  # 标签 -> 帧数据的映射
        
        for idx, fd in enumerate(all_frames):
            if fd.get("frame_path"):
                label = f"frame_{idx}@{fd['timestamp']:.1f}s"
                candidate_frames.append(fd["frame_path"])
                frame_labels.append(label)
                frame_label_map[label] = fd
        
        if not candidate_frames:
            if logger:
                logger.info(f"Round {retry_round+1}: 无有效帧路径，扩展范围")
            current_range["start_sec"] = max(0, current_range["start_sec"] - 2.0)
            current_range["end_sec"] = current_range["end_sec"] + 3.0
            continue
        
        # 3. Vision AI 验证（传递字幕上下文）
        vision_validated = await _verify_candidates_with_vision(
            video_path, 
            None,  # 不再传递 best_start
            None,  # 不再传递 best_end
            [],    # 不再传递 alt_starts
            [],    # 不再传递 alt_ends
            all_frames,
            title, 
            summary,
            output_dir,
            unit_id,
            logger,
            candidate_frames,  # 新增：直接传递帧列表
            frame_labels,      # 新增：直接传递标签列表
            subtitle_context=subtitle_context  # 新增：传递字幕上下文
        )
        
        boundary_status = vision_validated.get("boundary_status", "found")
        
        # 4. 根据 Vision AI 的判断决定下一步
        if boundary_status == "found":
            # 成功找到边界，解析帧标签
            start_label = vision_validated.get("start_frame_label")
            end_label = vision_validated.get("end_frame_label")
            
            if start_label in frame_label_map and end_label in frame_label_map:
                start_frame = frame_label_map[start_label]
                end_frame = frame_label_map[end_label]
                
                # 检查是否选择了边界帧（可能需要继续扩展）
                all_timestamps = [f["timestamp"] for f in all_frames]
                min_timestamp = min(all_timestamps)
                max_timestamp = max(all_timestamps)
                
                # 如果选择的开始帧接近最早帧（误差 < 0.5s），可能需要向前扩展
                is_at_start_boundary = abs(start_frame["timestamp"] - min_timestamp) < 0.5
                # 如果选择的结束帧接近最晚帧（误差 < 0.5s），可能需要向后扩展
                is_at_end_boundary = abs(end_frame["timestamp"] - max_timestamp) < 0.5
                
                if is_at_start_boundary and is_at_end_boundary:
                    # 两端都在边界，触发双向扩展
                    if logger:
                        logger.info(f"[RETRY {retry_round+1}] ⚠ Selected frames at both boundaries, expanding range")
                    boundary_status = "need_expand_both"
                    # 继续到扩展逻辑
                elif is_at_start_boundary:
                    # 开始帧在边界，触发向前扩展
                    if logger:
                        logger.info(f"[RETRY {retry_round+1}] ⚠ Start frame at boundary ({start_frame['timestamp']:.1f}s ≈ {min_timestamp:.1f}s), expanding start")
                    boundary_status = "need_expand_start"
                    # 继续到扩展逻辑
                elif is_at_end_boundary:
                    # 结束帧在边界，触发向后扩展
                    if logger:
                        logger.info(f"[RETRY {retry_round+1}] ⚠ End frame at boundary ({end_frame['timestamp']:.1f}s ≈ {max_timestamp:.1f}s), expanding end")
                    boundary_status = "need_expand_end"
                    # 继续到扩展逻辑
                else:
                    # === POST-END VERIFICATION (First Principles: Visual Stability Check) ===
                    # 即使 AI 认为找到了，我们也要向后探测，确保画面已经稳定下来
                    
                    # 动态步长策略 (User Defined):
                    # - 动画场景: 0.5s (节奏快，快速确认)
                    # - 实操场景: 1.5s (节奏慢，容忍短暂思考)
                    # - 其他: 1.0s (默认)
                    
                    scene_type = vision_validated.get("scene_type", "")
                    if "动画" in scene_type:
                        check_offset = 0.5
                    elif "实操" in scene_type:
                        check_offset = 1.5
                    else:
                        check_offset = 1.0
                    
                    try:
                        check_timestamp = end_frame["timestamp"] + check_offset
                        duration_limit = state.get("video_duration", 3600.0)
                        
                        if check_timestamp < duration_limit:
                            from ..tools.opencv_capture import FrameCapture
                            from ..tools.frame_analyzer import FrameBoundaryAnalyzer
                            
                            check_session = f"{unit_id}_postcheck_r{retry_round}"
                            check_temp_dir = f"{output_dir}/temp_frames_{check_session}"
                            
                            with FrameCapture(video_path, check_temp_dir) as checker:
                                check_res = checker.capture_frame(check_timestamp, "post_check_frame")
                                
                                if check_res.is_valid:
                                    # 计算 SSIM: End_Frame vs Post_Check_Frame
                                    from skimage.metrics import structural_similarity as ssim
                                    import cv2
                                    
                                    img_end = cv2.imread(end_frame["frame_path"])
                                    img_check = cv2.imread(check_res.frame_path)
                                    
                                    if img_end is not None and img_check is not None:
                                        # Resize to same logic as analyzer (320x180 for speed)
                                        img_end_s = cv2.resize(img_end, (320, 180))
                                        img_check_s = cv2.resize(img_check, (320, 180))
                                        img_end_s = cv2.cvtColor(img_end_s, cv2.COLOR_BGR2GRAY)
                                        img_check_s = cv2.cvtColor(img_check_s, cv2.COLOR_BGR2GRAY)
                                        
                                        similarity = ssim(img_end_s, img_check_s)
                                        
                                        if similarity < 0.95:
                                            # 画面显著变化，说明未结束
                                            if logger:
                                                logger.info(f"[RETRY {retry_round+1}] ⚠ Post-End Check FAILED ({scene_type}): SSIM={similarity:.3f} (<0.95) at +{check_offset}s")
                                                logger.info(f"[RETRY {retry_round+1}] 🛑 Animation is likely continuing. Overriding to 'need_expand_end'")
                                            
                                            boundary_status = "need_expand_end"
                                            # 清理临时文件
                                            import shutil
                                            if os.path.exists(check_temp_dir):
                                                shutil.rmtree(check_temp_dir, ignore_errors=True)
                                            
                                            # 跳转到扩展逻辑，不返回
                                            if boundary_status == "need_expand_end":
                                                # 手动模拟扩展逻辑的触发
                                                pass 
                                        else:
                                            if logger:
                                                logger.info(f"[RETRY {retry_round+1}] ✓ Post-End Check PASSED ({scene_type}): SSIM={similarity:.3f} (Stable at +{check_offset}s)")
                                    
                            # 清理临时文件 (Checker 会自动处理吗？ context manager 可能只关资源)
                            import shutil
                            if os.path.exists(check_temp_dir):
                                shutil.rmtree(check_temp_dir, ignore_errors=True)
                                
                    except Exception as e:
                        if logger:
                            logger.log_warning(f"[RETRY {retry_round+1}] Post-End Check Error: {e}")
                    
                    if boundary_status == "need_expand_end":
                        # 直接复用下方的扩展逻辑
                        pass
                    else:
                        refined_range = {
                            "start_sec": start_frame["timestamp"],
                            "end_sec": end_frame["timestamp"]
                        }
                        
                        if logger:
                            duration = refined_range['end_sec'] - refined_range['start_sec']
                            logger.info(f"[RETRY {retry_round+1}] ✓ Vision AI successfully located boundary (Verified)")
                            logger.info(f"[REFINE] Final range: {refined_range['start_sec']:.1f}s - {refined_range['end_sec']:.1f}s ({duration:.1f}s)")
                            logger.info(f"[REFINE] Frames: START=[{start_label}] END=[{end_label}]")
                        
                        return refined_range
            else:
                if logger:
                    logger.log_warning(f"Vision AI 返回的帧标签无效: {start_label}, {end_label}")
                # 降级：返回当前范围
                return current_range
        
        elif boundary_status == "need_resample":
            # Vision AI 提出了具体的重采样时间点
            resample_points = vision_validated.get("resample_points", [])
            
            if resample_points and isinstance(resample_points, list) and len(resample_points) >= 3:
                if logger:
                    logger.info(f"[RETRY {retry_round+1}] 🎯 Vision AI requests targeted resampling at {len(resample_points)} points")
                    logger.info(f"[RETRY {retry_round+1}] Points: {[f'{p:.1f}s' for p in resample_points[:5]]}...")
                
                # 使用 FrameCapture 在指定时间点精确采样
                from ..tools.opencv_capture import FrameCapture
                session_id = f"{unit_id}_resample_r{retry_round}"
                temp_dir = f"{output_dir}/temp_frames_{session_id}"
                
                try:
                    with FrameCapture(video_path, temp_dir) as capture:
                        resampled_frames = []
                        for idx, timestamp in enumerate(resample_points):
                            frame_id = f"resample_{idx}_{timestamp:.1f}s".replace(".", "_")
                            result = capture.capture_frame(timestamp, frame_id, {"sharpen": False})
                            if result.is_valid:
                                resampled_frames.append({
                                    "timestamp": timestamp,
                                    "frame_path": result.frame_path,
                                    "frame_idx": idx
                                })
                        
                        if len(resampled_frames) >= 3:
                            # 准备新的帧序列
                            candidate_frames = [f["frame_path"] for f in resampled_frames]
                            frame_labels = [f"frame_{f['frame_idx']}@{f['timestamp']:.1f}s" for f in resampled_frames]
                            frame_label_map = {
                                f"frame_{f['frame_idx']}@{f['timestamp']:.1f}s": f 
                                for f in resampled_frames
                            }
                            
                            if logger:
                                logger.info(f"[RETRY {retry_round+1}] Successfully resampled {len(resampled_frames)} frames")
                            
                            # 重新验证
                            vision_validated_2 = await _verify_candidates_with_vision(
                                video_path, None, None, [], [],
                                resampled_frames, title, summary,
                                output_dir, unit_id, logger,
                                candidate_frames, frame_labels,
                                subtitle_context=subtitle_context
                            )
                            
                            if vision_validated_2.get("boundary_status") == "found":
                                start_label = vision_validated_2.get("start_frame_label")
                                end_label = vision_validated_2.get("end_frame_label")
                                
                                if start_label in frame_label_map and end_label in frame_label_map:
                                    refined_range = {
                                        "start_sec": frame_label_map[start_label]["timestamp"],
                                        "end_sec": frame_label_map[end_label]["timestamp"]
                                    }
                                    
                                    if logger:
                                        duration = refined_range['end_sec'] - refined_range['start_sec']
                                        logger.info(f"[RETRY {retry_round+1}] ✓ Targeted resampling successful")
                                        logger.info(f"[REFINE] Final range: {refined_range['start_sec']:.1f}s - {refined_range['end_sec']:.1f}s ({duration:.1f}s)")
                                    
                                    return refined_range
                        else:
                            if logger:
                                logger.log_warning(f"[RETRY {retry_round+1}] Insufficient resampled frames ({len(resampled_frames)})")
                except Exception as e:
                    if logger:
                        logger.log_warning(f"[RETRY {retry_round+1}] Resampling failed: {e}")
            
            # 如果重采样失败或点数不足，扩展范围重试
            if logger:
                logger.log_warning(f"[RETRY {retry_round+1}] Falling back to range expansion")
            current_range["start_sec"] = max(0, current_range["start_sec"] - 3.0)
            current_range["end_sec"] = current_range["end_sec"] + 3.0
            continue
        
        elif boundary_status == "need_expand_start":
            # 需要向前扩展
            try:
                expand_sec = float(vision_validated.get("expand_suggestion", 2.0))
            except (ValueError, TypeError):
                expand_sec = 2.0  # 默认扩展2秒
            old_start = current_range["start_sec"]
            current_range["start_sec"] = max(0, current_range["start_sec"] - expand_sec)
            if logger:
                logger.info(f"[RETRY {retry_round+1}] ← Expanding START: {old_start:.1f}s → {current_range['start_sec']:.1f}s (-{expand_sec}s)")
            continue
        
        elif boundary_status == "need_expand_end":
            # 需要向后扩展
            try:
                expand_sec = float(vision_validated.get("expand_suggestion", 3.0))
            except (ValueError, TypeError):
                expand_sec = 3.0  # 默认扩展3秒
            old_end = current_range["end_sec"]
            current_range["end_sec"] = current_range["end_sec"] + expand_sec
            if logger:
                logger.info(f"[RETRY {retry_round+1}] → Expanding END: {old_end:.1f}s → {current_range['end_sec']:.1f}s (+{expand_sec}s)")
            continue
        
        elif boundary_status == "need_expand_both":
            # 两端都需要扩展
            try:
                expand_sec = float(vision_validated.get("expand_suggestion", 2.0))
            except (ValueError, TypeError):
                expand_sec = 2.0  # 默认扩展2秒
            old_start = current_range["start_sec"]
            old_end = current_range["end_sec"]
            current_range["start_sec"] = max(0, current_range["start_sec"] - expand_sec)
            current_range["end_sec"] = current_range["end_sec"] + expand_sec
            if logger:
                logger.info(f"[RETRY {retry_round+1}] ↔ Expanding BOTH: {old_start:.1f}s-{old_end:.1f}s → {current_range['start_sec']:.1f}s-{current_range['end_sec']:.1f}s")
            continue
        
        else:
            # 未知状态，降级返回当前范围
            if logger:
                logger.log_warning(f"Vision AI 返回未知状态: {boundary_status}")
            return current_range
    
    # 达到最大重试次数，返回当前范围
    if logger:
        logger.log_warning(f"[REFINE] ✗ Max retries ({max_retries}) reached, returning current range")
        logger.log_warning(f"[REFINE] Fallback range: {current_range['start_sec']:.1f}s - {current_range['end_sec']:.1f}s")
    return current_range


async def _refine_with_retry(
    video_path: str,
    rough_range: Dict[str, float],
    title: str,
    summary: str,
    output_dir: str,
    max_retries: int = 3,
    logger = None,
    unit_id: str = "default",
    video_duration: float = 3600.0,
    scene_type: str = "unknown"
) -> Dict[str, float]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过OpenCV 图像处理、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：all_subtitles
    - 条件：scene_type not in step_config
    - 条件：not all_frames or len(all_frames) < 3
    依据来源（证据链）：
    - 输入参数：logger, scene_type, video_duration。
    - 配置字段：end_sec, frame_path, ssim_threshold, start_sec, timestamp。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - rough_range: 函数入参（类型：Dict[str, float]）。
    - title: 函数入参（类型：str）。
    - summary: 函数入参（类型：str）。
    - output_dir: 目录路径（类型：str）。
    - max_retries: 函数入参（类型：int）。
    - logger: 函数入参（类型：未标注）。
    - unit_id: 标识符（类型：str）。
    - video_duration: 函数入参（类型：float）。
    - scene_type: 函数入参（类型：str）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    修复后的带重试边界精细化：
    1. 动态步长适配场景
    2. 反向验证边界
    3. 修复Post-End Check Bug
    4. 优化SSIM判断逻辑"""
    from ..tools.storage import LocalStorage
    from ..tools.frame_analyzer import FrameBoundaryAnalyzer
    
    current_range = rough_range.copy()
    
    # 1. 场景化步长配置（核心修复）
    step_config = {
        "动画场景": {"sample_step": 0.2, "expand_start": 0.5, "expand_end": 1.0, "ssim_threshold": 0.90},
        "实操场景": {"sample_step": 2.0, "expand_start": 2.0, "expand_end": 3.0, "ssim_threshold": 0.92},
        "其他": {"sample_step": 0.5, "expand_start": 1.0, "expand_end": 2.0, "ssim_threshold": 0.93}
    }
    
    # 获取字幕上下文
    subtitle_context = ""
    try:
        storage = LocalStorage(output_dir)
        timestamps_map = storage.load_subtitle_timestamps()
        all_subtitles = list(timestamps_map.values()) if timestamps_map else []
        if all_subtitles:
            start_sec = rough_range["start_sec"]
            end_sec = rough_range["end_sec"]
            context_start = max(0, start_sec - 5)
            context_end = end_sec + 5
            relevant_subs = [
                sub for sub in all_subtitles
                if context_start <= sub.get("start_sec", 0) <= context_end
            ]
            if relevant_subs:
                subtitle_lines = []
                for sub in relevant_subs[:10]:
                    start_time = sub.get("start_sec", 0)
                    text = sub.get("text", "")
                    subtitle_lines.append(f"[{start_time:.1f}s] {text}")
                subtitle_context = "\n".join(subtitle_lines)
    except Exception as e:
        if logger:
            logger.log_warning(f"Failed to fetch subtitle context: {e}")

    for retry_round in range(max_retries):
        # 动态获取当前可能的 scene_type 对应的配置
        config = step_config.get(scene_type, step_config["其他"])
        if scene_type not in step_config:
            if "动画" in scene_type: config = step_config["动画场景"]
            elif "实操" in scene_type: config = step_config["实操场景"]
        
        session_id = f"{unit_id}_r{retry_round}"
        analyzer = FrameBoundaryAnalyzer(video_path, output_dir, session_id=session_id)
        
        try:
            if logger:
                logger.info(f"[RETRY {retry_round+1}] Analyzing range {current_range['start_sec']:.1f}-{current_range['end_sec']:.1f} with step {config['sample_step']}s (Scene: {scene_type})")
                
            analysis_result = analyzer.analyze_boundary(
                current_range, title, summary,
                sample_step=config["sample_step"]
            )
        finally:
            analyzer.cleanup()
        
        all_frames = analysis_result.all_frames
        
        # 1. 检查帧数量
        if not all_frames or len(all_frames) < 3:
            if logger:
                logger.log_warning(f"[RETRY {retry_round+1}] Insufficient frames, expanding range")
            current_range["start_sec"] = max(0, current_range["start_sec"] - config["expand_start"])
            current_range["end_sec"] = current_range["end_sec"] + config["expand_end"]
            continue
        
        # 2. 准备帧数据
        candidate_frames, frame_labels, frame_label_map = [], [], {}
        for idx, fd in enumerate(all_frames):
            if fd.get("frame_path"):
                label = f"frame_{idx}@{fd['timestamp']:.1f}s"
                candidate_frames.append(fd["frame_path"])
                frame_labels.append(label)
                frame_label_map[label] = fd
        
        # 3. Vision AI验证
        vision_validated = await _verify_candidates_with_vision(
            video_path, None, None, [], [], all_frames, title, summary, output_dir, unit_id, logger,
            candidate_frames=candidate_frames, frame_labels=frame_labels, subtitle_context=subtitle_context
        )
        
        new_scene = vision_validated.get("scene_type")
        if new_scene:
            scene_type = new_scene

        boundary_status = vision_validated.get("boundary_status", "found")
        
        if boundary_status == "found":
            start_label = vision_validated.get("start_frame_label")
            end_label = vision_validated.get("end_frame_label")
            
            if start_label in frame_label_map and end_label in frame_label_map:
                start_frame = frame_label_map[start_label]
                end_frame = frame_label_map[end_label]
                
                # 4. 反向验证开始边界
                start_verify_range = {
                    "start_sec": max(0, start_frame["timestamp"] - 2.0),
                    "end_sec": start_frame["timestamp"]
                }
                if start_verify_range["end_sec"] - start_verify_range["start_sec"] > 0.5:
                    start_verify_analyzer = FrameBoundaryAnalyzer(video_path, output_dir, session_id=f"{session_id}_start_verify")
                    try:
                        if logger:
                             logger.info(f"[RETRY {retry_round+1}] 🔍 Verifying Start Boundary")
                        start_verify_result = start_verify_analyzer.analyze_boundary(
                            start_verify_range, title, summary, sample_step=0.3
                        )
                        verified_frames = start_verify_result.all_frames
                        if verified_frames:
                             earliest = verified_frames[0]
                             if earliest["timestamp"] < start_frame["timestamp"] - 0.5:
                                 if logger:
                                     logger.info(f"[RETRY {retry_round+1}] ← Found earlier start frame: {earliest['timestamp']:.1f}s")
                                 start_frame = earliest
                    finally:
                        start_verify_analyzer.cleanup()
                
                # 5. Post-End Check
                check_offset = 0.5 if "动画" in scene_type else 1.5
                check_timestamp = end_frame["timestamp"] + check_offset
                
                if check_timestamp < video_duration:
                    try:
                        from ..tools.opencv_capture import FrameCapture
                        from skimage.metrics import structural_similarity as ssim
                        import cv2
                        import shutil
                        import os
                        
                        check_temp_dir = f"{output_dir}/temp_frames_{session_id}_post_check"
                        with FrameCapture(video_path, check_temp_dir) as checker:
                            check_res = checker.capture_frame(check_timestamp, "post_check_frame")
                            if check_res.is_valid:
                                img_end = cv2.imread(end_frame["frame_path"])
                                img_check = cv2.imread(check_res.frame_path)
                                if img_end is not None and img_check is not None:
                                    img_end_s = cv2.resize(img_end, (320, 180), cv2.INTER_AREA)
                                    img_check_s = cv2.resize(img_check, (320, 180), cv2.INTER_AREA)
                                    img_end_s = cv2.cvtColor(img_end_s, cv2.COLOR_BGR2GRAY)
                                    img_check_s = cv2.cvtColor(img_check_s, cv2.COLOR_BGR2GRAY)
                                    similarity = ssim(img_end_s, img_check_s)
                                    
                                    if similarity < config["ssim_threshold"]:
                                        if logger:
                                            logger.info(f"[RETRY {retry_round+1}] ⚠ Post-End Check FAILED ({scene_type}): SSIM={similarity:.3f}")
                                        boundary_status = "need_expand_end"
                                    else:
                                        if logger:
                                             logger.info(f"[RETRY {retry_round+1}] ✓ Post-End Check PASSED")
                        
                        if os.path.exists(check_temp_dir):
                            shutil.rmtree(check_temp_dir, ignore_errors=True)
                            
                    except Exception as e:
                        if logger:
                             logger.log_warning(f"Post-End Check Error: {e}")

                if boundary_status == "need_expand_end":
                    current_range["end_sec"] += config["expand_end"]
                    continue

                refined_range = {
                    "start_sec": start_frame["timestamp"],
                    "end_sec": end_frame["timestamp"]
                }
                if logger:
                    logger.info(f"[REFINE] Final range: {refined_range['start_sec']:.1f}s - {refined_range['end_sec']:.1f}s")
                return refined_range
        
        if boundary_status == "need_expand_start":
             current_range["start_sec"] = max(0, current_range["start_sec"] - config["expand_start"])
        elif boundary_status == "need_expand_end":
             current_range["end_sec"] += config["expand_end"]
        elif boundary_status == "need_expand_both":
             current_range["start_sec"] = max(0, current_range["start_sec"] - config["expand_start"])
             current_range["end_sec"] += config["expand_end"]
        elif boundary_status == "need_resample":
             current_range["start_sec"] = max(0, current_range["start_sec"] - config["expand_start"])
             current_range["end_sec"] += config["expand_end"]
        
    return current_range


