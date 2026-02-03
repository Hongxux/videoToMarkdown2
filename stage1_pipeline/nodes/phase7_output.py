"""
Phase 7: 语义重构与最终输出
Steps 20, 21, 22
"""

import asyncio
from typing import Dict, Any, List
from pathlib import Path

from ..state import PipelineState
from ..llm.client import create_llm_client
from ..tools.storage import LocalStorage
from ..monitoring.logger import get_logger


async def step20_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤20：素材整合
    
    类型：代码规则
    核心动作：按知识点整合文字、视频、截图、辅助信息
    
    截图分类：
    - fault_screenshots: 断层补全截图
    - viz_screenshots: 可视化场景截图
    - 使用 processed_frames（如有）优先于 qualified_frames
    """
    logger = get_logger("step20_integrate", state.get("output_dir", "output/logs"))
    logger.start()
    
    segments = state.get("knowledge_segments", [])
    knowledge_points = state.get("knowledge_points", [])  # 新增：获取合并后的知识点
    core_content_judgment = state.get("core_content_judgment", [])
    qualified_frames = state.get("qualified_frames", [])
    processed_frames = state.get("processed_frames", [])  # 新增: step15b输出
    auxiliary_information = state.get("auxiliary_information", [])
    video_clips = state.get("video_clips", [])  # 新增：从step19读取视频片段
    visualization_forms = state.get("visualization_forms", [])
    strategy_matches = state.get("strategy_matches", [])  # 用于判断来源类型
    
    # 构建映射
    core_map = {c["segment_id"]: c for c in core_content_judgment}
    aux_map = {a["segment_id"]: a for a in auxiliary_information}
    form_map = {v["segment_id"]: v for v in visualization_forms}
    video_clips_map = {v["kp_id"]: v for v in video_clips}  # 新增：视频片段映射
    
    # 构建 segment -> source_type 映射
    segment_source_type = {}
    for s in strategy_matches:
        sid = s.get("segment_id", "")
        if sid:
            segment_source_type[sid] = s.get("source_type", "fault")
    
    # 获取 semantic_faults 用于构建 fault_id -> segment_id 映射
    semantic_faults = state.get("semantic_faults", [])
    fault_to_segment = {f["fault_id"]: f["segment_id"] for f in semantic_faults}

    # 获取 screenshot_instructions 用于构建 instruction_id -> segment_id 映射
    screenshot_instructions = state.get("screenshot_instructions", [])
    instruction_to_segment = {ins["instruction_id"]: ins["segment_id"] for ins in screenshot_instructions}
    
    # 优先使用 processed_frames，否则使用 qualified_frames
    frames_to_use = processed_frames if processed_frames else qualified_frames
    
    # 构建 segment -> frames 映射
    segment_frames = {}
    for frame in frames_to_use:
        fault_id = frame.get("fault_id", "")
        instruction_id = frame.get("instruction_id", "")
        
        # 依次尝试通过 fault_id、instruction_id 或 frame 自身字段找到 segment_id
        sid = fault_to_segment.get(fault_id)
        if not sid:
            sid = instruction_to_segment.get(instruction_id)
        if not sid:
            sid = frame.get("segment_id", "")
            
        if sid:
            if sid not in segment_frames:
                segment_frames[sid] = []
            segment_frames[sid].append(frame)
    
    logger.log_input({
        "segment_count": len(segments),
        "kp_count": len(knowledge_points),
        "processed_frames_count": len(processed_frames),
        "qualified_frames_count": len(qualified_frames),
        "video_clips_count": len(video_clips)  # 新增：记录视频片段数量
    })
    
    try:
        integrated_materials = []
        
        # 如果有knowledge_points，按KP组织；否则回退到按segment组织
        if knowledge_points:
            # 构建 segment_id -> segment 映射 (用于提取子元素)
            segment_map = {s["segment_id"]: s for s in segments}
            
            for kp in knowledge_points:
                kp_id = kp.get("kp_id", "")
                # 下游 Steps 16-19 已经产出了以 kp_id 为索引的结果
                core = core_map.get(kp_id, {})
                aux = aux_map.get(kp_id, {})
                form_info = form_map.get(kp_id, {})
                
                kp_segment_ids = kp.get("segment_ids", [])
                
                # 聚合截图 (KP 模式下，截图仍关联在原始 segment_id 上)
                all_fault_screenshots = []
                all_viz_screenshots = []
                combined_extracted = {"examples": [], "analogies": [], "concrete_words": [], "insights": []}
                # 优先使用 KP 已经聚合好的信息，避免 lookup 失败
                kp_full_text = kp.get("full_text")
                kp_extracted = kp.get("extracted_elements")
                full_text_parts = []
                
                for sid in kp_segment_ids:
                    # 累积截图 (基于原子 segment_id)
                    frames = segment_frames.get(sid, [])
                    source_type = segment_source_type.get(sid, "fault")
                    for f in frames:
                        frame_info = {
                            "frame_id": f.get("frame_id", ""),
                            "frame_path": f.get("processed_path", f.get("frame_path", "")),
                            "original_path": f.get("original_path", f.get("frame_path", "")),
                            "timestamp": f.get("timestamp", 0),
                            "grade": f.get("grade", "C"),
                            "crop_applied": f.get("crop_applied", False)
                        }
                        if source_type == "visualization":
                            all_viz_screenshots.append(frame_info)
                        else:
                            all_fault_screenshots.append(frame_info)
                    
                    # 如果 KP 没有预先聚合好的信息，则手动聚合
                    if not kp_full_text or not kp_extracted:
                        seg = segment_map.get(sid, {})
                        if not kp_full_text:
                            txt = seg.get("full_text", "")
                            if txt: full_text_parts.append(txt)
                        if not kp_extracted:
                            extracted = seg.get("extracted_elements", {})
                            for key in combined_extracted:
                                combined_extracted[key].extend(extracted.get(key, []))

                final_full_text = kp_full_text if kp_full_text else "\n\n".join(full_text_parts)
                final_extracted = kp_extracted if kp_extracted else combined_extracted
                
                # 聚合媒体地图 (Granular Media)
                media_list = []
                media_counter = 1
                
                # 新增：从 video_clips_map 获取视频片段
                video_clip = video_clips_map.get(kp_id)
                if video_clip:
                    media_list.append({
                        "id": f"MEDIA_{media_counter}",
                        "type": "video_file",  # 区分于 video_time_range
                        "description": f"视频演示 ({video_clip['duration']:.1f}秒)",
                        "path": video_clip["video_path"],
                        "start_sec": video_clip["start_sec"],
                        "end_sec": video_clip["end_sec"],
                        "duration": video_clip["duration"],
                        "scene_type": video_clip.get("scene_type", "unknown")
                    })
                    media_counter += 1
                    logger.debug(f"  Added video_file for {kp_id}: {video_clip['video_path']}")
                
                # 从 auxiliary_information 中提取与 kp_segment_ids 匹配的项
                for aid in auxiliary_information:
                    aid_id = aid.get("segment_id")
                    if aid_id == kp_id or aid_id in kp_segment_ids:
                        v_range = aid.get("video_time_range")
                        if v_range and (aid.get("video_transition") or aid.get("post_media_summary")):
                            media_list.append({
                                "id": f"MEDIA_{media_counter}",
                                "type": "video",
                                "description": aid.get("video_transition", "演示视频"),
                                "range": v_range,
                                "aux": aid
                            })
                            media_counter += 1

                # 将截图也加入媒体列表
                # 优先加入 viz_screenshots (可视化截图)
                for ss in all_viz_screenshots:
                    path = ss.get("frame_path", "")
                    filename = Path(path).name if path else "unknown.png"
                    media_list.append({
                        "id": f"MEDIA_{media_counter}",
                        "type": "image",
                        "description": f"场景截图: {filename}",
                        "path": path,
                        "filename": filename,
                        "ss_info": ss
                    })
                    media_counter += 1

                # 然后加入 fault_screenshots (断层补全截图)
                for ss in all_fault_screenshots:
                    path = ss.get("frame_path", "")
                    filename = Path(path).name if path else "unknown.png"
                    media_list.append({
                        "id": f"MEDIA_{media_counter}",
                        "type": "image",
                        "description": f"补充截图: {filename}",
                        "path": path,
                        "filename": filename,
                        "ss_info": ss
                    })
                    media_counter += 1

                material = {
                    "kp_id": kp_id,
                    "segment_id": kp_id,
                    "segment_ids": kp_segment_ids,
                    "knowledge_point": kp.get("kp_title", ""),
                    "darpa_questions": kp.get("darpa_questions", []),
                    "darpa_question": " / ".join(kp.get("darpa_questions", [])),
                    "full_text": final_full_text,
                    "extracted_elements": final_extracted,
                    "core_semantic": {"summary": kp.get("merge_reason", "")},
                    "text_content": final_full_text,
                    
                    # 粒度化媒体
                    "media_list": media_list,
                    
                    # 过渡语 (新增：整合video_clips和aux信息)
                    "transition": {
                        "video_clips": [video_clips_map.get(kp_id)] if video_clips_map.get(kp_id) else [],
                        "screenshots": all_fault_screenshots + all_viz_screenshots,
                        "auxiliary_info": aux_map.get(kp_id, {})
                    } if (video_clips_map.get(kp_id) or all_fault_screenshots or all_viz_screenshots) else None,
                    "transition_source": kp.get("transition_source"),
                    
                    # 截图
                    "screenshot_info": {
                        "fault_screenshots": all_fault_screenshots,
                        "viz_screenshots": all_viz_screenshots
                    },
                    "screenshots": all_fault_screenshots + all_viz_screenshots
                }
                integrated_materials.append(material)
        else:
            # 回退：按原segment逻辑处理
            for segment in segments:
                sid = segment["segment_id"]
                core = core_map.get(sid, {})
                aux = aux_map.get(sid, {})
                form = form_map.get(sid, {})
                frames = segment_frames.get(sid, [])
                source_type = segment_source_type.get(sid, "fault")
                
                fault_screenshots = []
                viz_screenshots = []
                
                for f in frames:
                    frame_info = {
                        "frame_id": f.get("frame_id", ""),
                        "frame_path": f.get("processed_path", f.get("frame_path", "")),
                        "original_path": f.get("original_path", f.get("frame_path", "")),
                        "timestamp": f.get("timestamp", 0),
                        "grade": f.get("grade", "C"),
                        "crop_applied": f.get("crop_applied", False)
                    }
                    if source_type == "visualization":
                        viz_screenshots.append(frame_info)
                    else:
                        fault_screenshots.append(frame_info)
                
                material = {
                    "segment_id": sid,
                    "knowledge_point": segment.get("knowledge_point", ""),
                    "darpa_question": segment.get("darpa_question", ""),
                    "darpa_question_name": segment.get("darpa_question_name", ""),
                    "core_semantic": segment.get("core_semantic", {}),
                    "text_content": segment.get("core_semantic", {}).get("summary", ""),
                    "full_text": segment.get("full_text", ""),
                    "extracted_elements": segment.get("extracted_elements", {}),
                    
                    "visualization_form": form.get("form", "none"),
                    "is_core_video": core.get("is_core", False),
                    "core_type": core.get("core_type", ""),
                    "video_needed": core.get("video_needed", False),
                    
                    "video_transition": aux.get("video_transition"),
                    "screenshot_transition": aux.get("screenshot_transition"),
                    "post_media_summary": aux.get("post_media_summary"),
                    "video_time_range": aux.get("video_time_range"),
                    
                    "screenshot_info": {
                        "fault_screenshots": fault_screenshots,
                        "viz_screenshots": viz_screenshots
                    },
                    "screenshots": fault_screenshots + viz_screenshots
                }
                
                integrated_materials.append(material)
        
        output = {
            "integrated_materials": integrated_materials,
            "current_step": "step20_integrate",
            "current_step_status": "completed"
        }
        
        logger.log_output({"material_count": len(integrated_materials)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step20_integrate": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"integrated_materials": [], "errors": [{"step": "step20", "error": str(e)}]}


# ============================================================================
# Step 21: 语义重构 Prompt（结构化笔记生成）
# ============================================================================

STRUCTURE_ANALYSIS_PROMPT = """请将以下原文按语义结构重新组织，生成层次化笔记。

【原文】
{full_text}

【已提取的元素】
- 例子：{examples}
- 类比：{analogies}
- 具象词语：{concrete_words}
- 个人理解：{insights}

【逻辑关系】{logic_relation}
【层次类型】{hierarchy_type}
【知识点】{knowledge_point}

【媒体插入信息】
{media_info}

【核心核心要求：严格遵循 note-format.xml 规范】
1. **格式规则 (Format Rules)**
   - **强制列表标记**：严禁只有缩进没有标记。所有缩进层级必须使用 `- ` (无序) 或 `1. ` (有序) 开头。
     - 正确：`\t- 内容` (Tab缩进)
     - 错误：`  - 内容` (空格缩进)
   - **严格缩进**：列表嵌套**必须严格使用 Tab (制表符)** 进行缩进。严禁使用空格。
   - **严禁空父节点**：父列表项必须包含核心内容，不能只有标题而无内容直接跟子项。
     - 错误：
       - `- **核心概念**` (无内容)
         `\t- 定义...`
     - 正确：
       - `- **核心概念**：这里写核心概括...`
         `\t- 定义...`
   - **标题限制**：内部仅允许使用 `###` (子章节/钩子) 和 `####` (支撑/细节)。严禁使用 `#####` 或更深层级。
   - **禁用表格**：Markdown表格在缩进中渲染不兼容，**绝对禁止使用表格**。如有对比，请使用“维度列表”形式（例如：`**维度名**：内容`）。
   - **语义标签**：使用明确的语义标签（如 `原因：`、`后果：`、`机制：`、`场景：`、`代价：`），而非泛泛的描述。

2. **逻辑规则 (Logic Rules)**
   - **消灭简单并列**：如果出现并列项，必须推断并标注其分类维度。
   - **层次明确**：使用缩进表达逻辑从属关系（展开/支撑/细化）。如果是新话题，请返回上一级。

3. **内容规则 (Content Rules)**
   - **加粗规范**：仅加粗定义性核心概念（`**概念**：定义`）。禁止过度加粗。
   - **完整性**：保留原文所有知识点，不增不删核心信息。
   - **媒体嵌入**：在需要插入媒体的位置添加占位符 `{{MEDIA}}`。
     - **重要**：如果提供了多个媒体（如 MEDIA_1, MEDIA_2），请按顺序使用 `{{MEDIA_1}}`, `{{MEDIA_2}}` 等占位符来精准控制插入位置。
     - 不要自己在正文中写“请看视频”、“如下图所示”等引导语。只需放置占位符，系统会自动生成标准格式的引导语和媒体链接。

4. **风格与内容修正规则 (Style & Content Modification)**
   - **允许修改 (Allowed)**：
     - **清理口语废话**：删除无意义的语气词（如“嗯...然后”、“接下来呢”、“其实啊”），可替换为专业的衔接词（如“接下来”、“需要注意的是”、“同理”）。
     - **优化衔接**：使句子之间的逻辑转换更流畅。
   - **绝对禁止修改 (Strictly Prohibited)**：
     - **保留讲解者人称**：严禁更改讲解者的习惯用语（例如：如果原文用“咱们”，必须保留“咱们”，**禁止**改成“大家”或“我们”）。
     - **保留核心事实**：严禁修改任何核心定义、分类标准、数据数值。
     - **保留类比本体**：严禁更改类比的喻体（例如：如果原文用“快递分拣”类比“数据筛选”，**禁止**改成“邮件分拣”或其他比喻）。

5. **输出清洗**
   - 只输出 JSON 内容，不要包含 Markdown 代码块标记。

【输出格式】JSON
{{
  "structured_text": "重组后的 Markdown 文本"
}}"""


async def step21_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤21：语义重构（结构化笔记生成）
    
    类型：LLM
    核心动作：
    1. 将原文按语义结构重新组织
    2. 嵌入例子/类比/个人理解
    3. 在断层位置插入媒体占位符
    
    注：处理核心已升级为 KP。
    """
    logger = get_logger("step21_reconstruct", state.get("output_dir", "output/logs"))
    logger.start()
    
    integrated_materials = state.get("integrated_materials", [])
    knowledge_segments = state.get("knowledge_segments", [])
    semantic_faults = state.get("semantic_faults", [])
    auxiliary_information = state.get("auxiliary_information", [])
    
    # 构建映射（用于回退场景）
    segment_map = {s["segment_id"]: s for s in knowledge_segments}
    aux_map = {a["segment_id"]: a for a in auxiliary_information}
    
    # 构建 segment -> faults 映射
    segment_faults = {}
    for fault in semantic_faults:
        sid = fault["segment_id"]
        if sid not in segment_faults:
            segment_faults[sid] = []
        segment_faults[sid].append(fault)
    
    logger.log_input({"material_count": len(integrated_materials)})
    
    try:
        llm = create_llm_client(purpose="analysis")
        
        async def process_material(material):
            """并行处理单个material的语义重构"""
            sid = material.get("segment_id", material.get("kp_id", ""))
            segment_ids = material.get("segment_ids", [sid])
            form = material.get("visualization_form", "none")
            
            # 优先使用material中已聚合的信息
            full_text = material.get("full_text", material.get("text_content", ""))
            if not full_text:
                segment = segment_map.get(sid, {})
                full_text = segment.get("full_text", "")
            
            # 收集所有相关的断层
            faults = []
            for seg_id in segment_ids:
                faults.extend(segment_faults.get(seg_id, []))
            
            # 如果不需要可视化且无断层，使用简化处理
            if form == "none" and not faults:
                return {
                    "segment_id": sid,
                    "segment_ids": segment_ids,
                    "knowledge_point": material.get("knowledge_point", ""),
                    "structured_text": full_text,
                    "visualization_form": form,
                    "video_time_range": None,
                    "screenshots": [],
                    "video_transition": None,
                    "screenshot_transition": None,
                    "post_media_summary": None,
                    "darpa_question": material.get("darpa_question", ""),
                    "transition": material.get("transition"),
                    "_tokens": 0
                }
            
            # 获取提取元素
            extracted = material.get("extracted_elements", {})
            if not extracted or not any(extracted.values()):
                segment = segment_map.get(sid, {})
                extracted = segment.get("extracted_elements", {})
            
            examples = extracted.get("examples", [])
            analogies = extracted.get("analogies", [])
            concrete_words = extracted.get("concrete_words", [])
            insights = extracted.get("insights", [])
            
            # 语义维度（从material或segment获取）
            segment_for_dim = segment_map.get(sid, {})
            sem_dim = segment_for_dim.get("semantic_dimension", {})
            logic_relation = sem_dim.get("logic_relation", "")
            hierarchy_type = sem_dim.get("hierarchy_type", "")
            
            # 构建媒体插入信息 (支持多 ID)
            media_list = material.get("media_list", [])
            media_info_lines = []
            for m in media_list:
                media_info_lines.append(f"- {{{{{m['id']}}}}}: {m['description']}")
            
            media_info = "\n".join(media_info_lines) if media_info_lines else "无需插入媒体"
            
            # 格式化提取元素
            examples_str = ", ".join([f"「{e['content']}」({e.get('position', '')})" for e in examples]) or "无"
            analogies_str = ", ".join([f"「{a['content']}」({a.get('position', '')})" for a in analogies]) or "无"
            concrete_str = ", ".join([f"{c['word']}→{c['abstract_concept']}" for c in concrete_words]) or "无"
            insights_str = ", ".join([f"「{i['content']}」" for i in insights]) or "无"
            
            prompt = STRUCTURE_ANALYSIS_PROMPT.format(
                full_text=full_text,
                examples=examples_str,
                analogies=analogies_str,
                concrete_words=concrete_str,
                insights=insights_str,
                logic_relation=logic_relation,
                hierarchy_type=hierarchy_type,
                knowledge_point=material.get("knowledge_point", ""),
                media_info=media_info
            )
            
            try:
                result, response = await llm.complete_json(prompt)
                tokens = response.total_tokens if response else 0
            except Exception as e:
                logger.log_warning(f"LLM call failed for {sid}: {e}")
                result = {"structured_text": full_text}
                tokens = 0
            
            # 获取结构化文本
            structured_text = result.get("structured_text", full_text)
            
            # 替换媒体占位符 (多 ID 支持)
            structured_text = _replace_media_placeholders(
                structured_text, 
                material
            )
            
            # --- 增加：强制缩进标准化 (Step 21 JSON 输出) ---
            # 确保中间状态也使用 Tab 缩进
            text_lines = structured_text.split('\n')
            norm_lines = []
            for line in text_lines:
                leading_part = line[:-len(line.lstrip())]
                content = line.lstrip()
                if not leading_part:
                    if ' ' in line: # Clean empty lines
                        norm_lines.append("")
                    else:
                        norm_lines.append(line)
                    continue
                
                # Tab 转换逻辑
                tab_count = leading_part.count('\t') + leading_part.count(' ') // 2
                norm_lines.append('\t' * tab_count + content)
            structured_text = '\n'.join(norm_lines)
            
            return {
                "segment_id": sid,
                "segment_ids": segment_ids,
                "knowledge_point": material.get("knowledge_point", ""),
                "structured_text": structured_text,
                "darpa_question": material.get("darpa_question", ""),
                "transition": material.get("transition"),
                "_tokens": tokens
            }
        
        # 并发执行所有material的语义重构
        logger.info(f"并发重构 {len(integrated_materials)} 个material的语义结构...")
        results = await asyncio.gather(*[process_material(m) for m in integrated_materials])
        
        # 收集结果
        reconstructed_materials = []
        total_tokens = 0
        for r in results:
            tokens = r.pop("_tokens", 0)
            total_tokens += tokens
            reconstructed_materials.append(r)
        
        output = {
            "reconstructed_materials": reconstructed_materials,
            "current_step": "step21_reconstruct",
            "current_step_status": "completed",
            "token_usage": {"step21_reconstruct": total_tokens}
        }
        
        logger.log_output({"reconstructed_count": len(reconstructed_materials)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step21_reconstruct": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"reconstructed_materials": [], "errors": [{"step": "step21", "error": str(e)}]}


def _replace_media_placeholders(text: str, material: Dict) -> str:
    """替换媒体占位符为实际的 Obsidian 嵌入代码 (支持 MEDIA_ID 和通用 MEDIA)"""
    import re
    
    media_list = material.get("media_list", [])
    if not media_list:
        return text
    
    # 记录已使用的媒体索引，用于通用 {{MEDIA}} 占位符按序消费
    used_indices = set()
    
    lines = text.split('\n')
    new_lines = []
    
    for line in lines:
        # 1. 优先尝试匹配带编号的占位符 (支持 {{MEDIA_1}}, {MEDIA_1}, etc.)
        match_id = re.search(r"\{\{?\s*(MEDIA_(\d+))\s*\}?\}", line)
        
        # 2. 尝试匹配通用占位符 (支持 {{MEDIA}}, {MEDIA})
        match_generic = re.search(r"\{\{?\s*MEDIA\s*\}?\}", line)
        
        m_data = None
        current_match = None
        
        if match_id:
            current_match = match_id
            media_id = match_id.group(1)
            idx = int(match_id.group(2)) - 1 # MEDIA_1 对应索引 0
            if 0 <= idx < len(media_list):
                m_data = media_list[idx]
                used_indices.add(idx)
        elif match_generic:
            current_match = match_generic
            # 找到第一个未被使用的媒体
            for i in range(len(media_list)):
                if i not in used_indices:
                    m_data = media_list[i]
                    used_indices.add(i)
                    break
        
        if m_data and current_match:
            # 提取前导缩进
            # 处理规则：如果行首是 "- {{MEDIA}}"，去掉标记保留缩进
            indent_match = re.match(r"^(\s*)(?:- |1\. )?", line)
            indent = indent_match.group(1) if indent_match else ""
            
            # 生成嵌入内容
            embed = _generate_media_embed_v2(m_data, indent)
            new_lines.append(embed)
        elif current_match:
            # 匹配到了但没找到对应媒体，移除该行
            continue
        else:
            new_lines.append(line)
            
    return '\n'.join(new_lines)


def _generate_media_embed_v2(media: Dict, parent_indent: str) -> str:
    """生成符合 Obsidian 规范的媒体嵌入 (使用 Tab 缩进)"""
    lines = []
    aux = media.get("aux", {})
    
    # 统一转换 parent_indent 为 Tab
    tab_count = 0
    if parent_indent:
        tab_count = parent_indent.count('\t') + parent_indent.count(' ') // 2
    base_indent = '\t' * tab_count
    media_indent = base_indent + '\t'
    
    if media["type"] == "video" or media["type"] == "video_file":
        if media["type"] == "video":
            v_range = media["range"]
            start = int(v_range.get("start_sec", 0))
            end = int(v_range.get("end_sec", 0))
            desc = media.get("description", "演示视频")
        else:
            # video_file 类型 (由 step19_auxiliary 产出，用于核心视频片段)
            start = int(media.get("start_sec", 0))
            end = int(media.get("end_sec", 0))
            desc = media.get("description", "核心视频演示")
            
        lines.append(f"{base_indent}- {desc}")
        lines.append(f"{media_indent}- ![[video.mp4#t={start},{end}]]")
        
        summary = aux.get("post_media_summary")
        if summary:
            lines.append(f"{media_indent}- 💡 {summary}")
            
    elif media["type"] == "image":
        filename = media.get("filename", "")
        description = media.get("description", "截图")
        
        lines.append(f"{base_indent}- {description}")
        if filename:
            lines.append(f"{media_indent}- ![[{filename}]]")
            
    return '\n'.join(lines)


async def step22_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤22：Markdown笔记生成
    
    类型：代码规则
    核心动作：生成最终的Markdown格式笔记（Obsidian兼容格式）
          包含相邻章节合并、标签清洗等后处理
    """
    import shutil
    import re
    
    logger = get_logger("step22_markdown", state.get("output_dir", "output/logs"))
    logger.start()
    
    reconstructed_materials = state.get("reconstructed_materials", [])
    video_path = state.get("video_path", "")
    main_topic = state.get("main_topic", "")
    domain = state.get("domain", "")
    output_dir = state.get("output_dir", "output")
    
    logger.log_input({"material_count": len(reconstructed_materials)})
    
    try:
        output_dir_path = Path(output_dir)
        notes_dir = output_dir_path / "notes"
        notes_dir.mkdir(parents=True, exist_ok=True)
        
        # 复制资源
        _copy_resources(video_path, reconstructed_materials, notes_dir)
        
        # --- 核心逻辑优化：合并同名章节 ---
        merged_materials = []
        if reconstructed_materials:
            current_group = {
                "knowledge_point": reconstructed_materials[0].get("knowledge_point", ""),
                "darpa_questions": set(),
                "texts": []
            }
            if reconstructed_materials[0].get("darpa_question"):
                current_group["darpa_questions"].add(reconstructed_materials[0]["darpa_question"])
            current_group["texts"].append(reconstructed_materials[0].get("structured_text", ""))
            
            for i in range(1, len(reconstructed_materials)):
                mat = reconstructed_materials[i]
                kp = mat.get("knowledge_point", "")
                
                # 如果知识点名称相同（忽略空格），则合并
                if kp.strip() == current_group["knowledge_point"].strip():
                    if mat.get("darpa_question"):
                        current_group["darpa_questions"].add(mat["darpa_question"])
                    current_group["texts"].append(mat.get("structured_text", ""))
                else:
                    # 保存上一组
                    merged_materials.append(current_group)
                    # 开启新组
                    current_group = {
                        "knowledge_point": kp,
                        "darpa_questions": set(),
                        "texts": []
                    }
                    if mat.get("darpa_question"):
                        current_group["darpa_questions"].add(mat["darpa_question"])
                    current_group["texts"].append(mat.get("structured_text", ""))
            
            # 保存最后一组
            merged_materials.append(current_group)
            
        # --- 生成 Markdown ---
        md_lines = []
        
        # 头部元数据
        safe_title = main_topic.replace("/", "-").replace("\\", "-").replace(":", "-")
        md_lines.append(f"# {safe_title}")
        md_lines.append("")
        md_lines.append(f"> **领域**: {domain}")
        md_lines.append(f"> **主题**: {main_topic}")
        md_lines.append("")
        md_lines.append("---")
        md_lines.append("")
        
        # 目录
        md_lines.append("## 目录")
        md_lines.append("")
        for i, group in enumerate(merged_materials, 1):
            kp = group["knowledge_point"]
            # Obsidian 锚点规则：保留特殊字符，空格转 -
            anchor = _slugify_obsidian(kp)
            md_lines.append(f"{i}. [{kp}](#{anchor})")
        md_lines.append("")
        md_lines.append("---")
        md_lines.append("")
        
        # 正文
        for i, group in enumerate(merged_materials, 1):
            kp = group["knowledge_point"]
            md_lines.append(f"## {i}. {kp}")
            md_lines.append("")
            
            # DARPA 问题（去重展示）
            dq_list = list(group["darpa_questions"])
            if dq_list:
                dq_str = " / ".join(dq_list)
                md_lines.append(f"> **DARPA问题**: {dq_str}")
                md_lines.append("")
            
            # 合并文本内容
            full_text = "\n\n".join(group["texts"])
            
            # 最终清洗：移除未替换的占位符 {MEDIA} {{MEDIA}}
            full_text = re.sub(r"\{\{?MEDIA\}?\}", "", full_text)
            
            # 统一缩进风格：强制使用 Tab
            lines = full_text.split('\n')
            normalized_lines = []
            for line in lines:
                # 获取行首空白
                leading_part = line[:-len(line.lstrip())]
                content = line.lstrip()
                
                if not leading_part:
                     normalized_lines.append(line)
                     continue

                # 将空格转换为 Tab
                # 假设 2 空格 = 1 Tab
                tab_count = leading_part.count('\t') + leading_part.count(' ') // 2
                
                # 重新构建行
                new_indent = '\t' * tab_count
                normalized_lines.append(new_indent + content)
            full_text = '\n'.join(normalized_lines)
            
            # --- 增加：媒体去重逻辑 ---
            # 同一章节内，相同的视频范围只保留第一次出现
            seen_videos = set()
            def deduplicate_video(match):
                video_link = match.group(0)
                if video_link in seen_videos:
                    return "" # 移除重复
                seen_videos.add(video_link)
                return video_link
            
            # 我们针对 ![[video.mp4#t=...]] 进行去重
            full_text = re.sub(r"!\[\[video\.mp4#t=\d+,\d+\]\]", deduplicate_video, full_text)
            
            md_lines.append(full_text)
            md_lines.append("")
            md_lines.append("---")
            md_lines.append("")
            
        # 写入文件
        output_filename = f"{safe_title}.md"
        output_path = notes_dir / output_filename
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(md_lines))
        
        output = {
            "output_markdown_path": str(output_path),
            "current_step": "step22_markdown",
            "current_step_status": "completed"
        }
        
        logger.log_output({"merged_sections": len(merged_materials), "output_path": str(output_path)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step22_markdown": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        # Fallback to empty file creation/cleanup?
        return {"output_markdown_path": "", "errors": [{"step": "step22", "error": str(e)}]}

def _slugify_obsidian(text: str) -> str:
    """
    生成 Obsidian 兼容的锚点 Slug
    规则：
    1. 保留中文和数字
    2. 空格转换为短横线 (-)
    3. 移除大部分标点符号，但保留某些特殊字符（Obsidian 比较宽松，但为了稳健，我们移除常见标点）
    """
    import re
    # 替换空格为 -
    text = text.replace(' ', '-')
    # 移除常见标点 (保留 - 和 _ 和中文)
    # Obsidian 实际上是非常宽松的，通常只是替换空格。
    # 为了兼容性，我们只处理空格，如果用户遇到问题再加严
    # 参考用户反馈：中文 / 特殊字符会被直接保留；空格会被替换为 -
    return text

def _copy_resources(video_path: str, materials: List[Dict], notes_dir: Path):
    """复制视频和图片资源"""
    import shutil
    
    # 复制视频
    if video_path:
        src_video = Path(video_path)
        if src_video.exists():
            dst_video = notes_dir / "video.mp4"
            if not dst_video.exists():
                shutil.copy2(src_video, dst_video)
                
    # 复制图片
    for mat in materials:
        for ss in mat.get("screenshots", []):
            # 优先使用 processed_path (裁剪后的)
            src_str = ss.get("processed_path") or ss.get("frame_path")
            if src_str:
                src_path = Path(src_str)
                if src_path.exists():
                    dst_path = notes_dir / src_path.name
                    if not dst_path.exists():
                        shutil.copy2(src_path, dst_path)


async def step22b_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤22b：可视化决策总结
    
    类型：代码规则
    核心动作：生成可视化选择的详细报告
    """
    logger = get_logger("step22b_viz_summary", state.get("output_dir", "output/logs"))
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
            "current_step": "step22b_viz_summary",
            "current_step_status": "completed"
        }
        
        logger.log_output({
            "summary_path": str(summary_path),
            "total_screenshots": total_screenshots,
            "total_videos": total_videos
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step22b_viz_summary": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"visualization_summary_path": None, "errors": [{"step": "step22b", "error": str(e)}]}
