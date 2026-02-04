"""
模块说明：阶段流程节点 phase3_segmentation 的实现。
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
from ..tools.storage import LocalStorage
from ..monitoring.logger import get_logger


# ============================================================================
# DARPA八问框架定义
# ============================================================================

DARPA_QUESTIONS = {
    "Q1": "要解决什么问题",
    "Q2": "旧方法及局限",
    "Q3": "创新之处与核心原理",
    "Q4": "应用场景与价值",
    "Q5": "代价与风险",
    "Q6": "最小验证案例",
    "Q7": "与其他知识的关联",
    "Q8": "易误解之处"
}

DARPA_KEYWORDS = {
    "Q1": ["为什么需要", "痛点", "问题", "困难", "挑战"],
    "Q2": ["以前", "传统方式", "局限", "过去", "原来"],
    "Q3": ["创新", "改进", "核心机制", "原理", "本质"],
    "Q4": ["应用场景", "适合", "用在", "谁用", "什么时候用"],
    "Q5": ["代价", "成本", "约束", "缺点", "风险", "局限"],
    "Q6": ["验证", "案例", "示例", "演示", "举例"],
    "Q7": ["关联", "依赖", "对比", "联系", "类似"],
    "Q8": ["误区", "容易混淆", "注意", "常见错误", "陷阱"]
}


# ============================================================================
# Step 7: 知识点分片 Prompt
# ============================================================================

SEGMENT_PROMPT = """请将以下文本按DARPA八问框架进行细粒度分片。

【文本内容】
{paragraphs}

【DARPA八问】
Q1: 要解决什么问题
Q2: 旧方法及局限
Q3: 创新之处与核心原理
Q4: 应用场景与价值
Q5: 代价与风险
Q6: 最小验证案例
Q7: 与其他知识的关联
Q8: 易误解之处

【分片要求】
1. 每个片段只对应一个DARPA问题的一个语义维度
2. 识别知识点名称
3. 确定逻辑关系（因果/对比/递进/并列/条件）
4. 确定分类分层（定义层/原理层/实现层/应用层/边界层）
5. 提取例子、类比、具象词语、个人洞察

【输出格式】
{{
  "knowledge_segments": [
    {{
      "segment_id": "SEG001",
      "full_text": "完整文本",
      "knowledge_point": "知识点名称",
      "darpa_question": "Q1",
      "darpa_question_name": "要解决什么问题",
      "semantic_dimension": {{
        "logic_relation": "因果关系",
        "hierarchy_type": "原理层",
        "description": "Q1-xxx的核心问题"
      }},
      "core_semantic": {{
        "summary": "20-50字摘要",
        "label": "8字以内标签"
      }},
      "extracted_elements": {{
        "examples": [{{"content": "...", "position": "第X句"}}],
        "analogies": [{{"content": "...", "position": "第X句"}}],
        "concrete_words": [{{"word": "...", "abstract_concept": "..."}}],
        "insights": [{{"content": "...", "insight_type": "个人理解"}}]
      }},
      "source_paragraph_ids": ["P001", "P002"]
    }}
  ]
}}"""


async def step7_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：sentence_ids
    - 条件：p['paragraph_id'] in source_pids
    依据来源（证据链）：
    - 配置字段：paragraph_id。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step7_segment", state.get("output_dir", "output/logs"))
    logger.start()
    
    paragraphs = state.get("pure_text_script", [])
    logger.log_input({"paragraph_count": len(paragraphs)})
    
    try:
        llm = create_llm_client(purpose="analysis")
        storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
        
        # 批处理
        batch_size = 5
        all_segments = []
        segment_counter = 1
        total_tokens = 0
        
        # 并行批处理
        batch_size = 5
        async def process_batch(idx, batch):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            输入参数：
            - idx: 函数入参（类型：未标注）。
            - batch: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            paragraphs_text = "\n\n".join([
                f"[{p['paragraph_id']}]\n{p['text']}" 
                for p in batch
            ])
            
            prompt = SEGMENT_PROMPT.format(paragraphs=paragraphs_text)
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Batch {idx + 1}",
                    response=f"{len(result.get('knowledge_segments', []))} segments",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                return result.get("knowledge_segments", []), response.total_tokens
            except Exception as e:
                logger.log_warning(f"Batch {idx + 1} failed: {e}")
                return [], 0

        logger.info(f"并发执行 {len(paragraphs)} 个段落的分片处理...")
        batches = [paragraphs[i:i + batch_size] for i in range(0, len(paragraphs), batch_size)]
        tasks = [process_batch(i, batch) for i, batch in enumerate(batches)]
        
        results = await asyncio.gather(*tasks)
        
        # 聚合结果并分配唯一ID
        all_segments = []
        total_tokens = 0
        segment_counter = 1
        
        for batch_segments, tokens in results:
            total_tokens += tokens
            for item in batch_segments:
                item["segment_id"] = f"SEG{segment_counter:03d}"
                segment_counter += 1
                all_segments.append(item)
        
        # 计算并存储段落级时间戳
        sentence_timestamps = storage.load_sentence_timestamps()
        segment_timestamps = {}
        
        for segment in all_segments:
            source_pids = segment.get("source_paragraph_ids", [])
            # 从段落找回句子ID
            sentence_ids = []
            for p in paragraphs:
                if p["paragraph_id"] in source_pids:
                    sentence_ids.extend(p.get("source_sentence_ids", []))
            
            if sentence_ids:
                time_range = storage.get_sentence_time_range(sentence_ids)
                segment_timestamps[segment["segment_id"]] = {
                    "start_sec": time_range["start_sec"],
                    "end_sec": time_range["end_sec"],
                    "source_sentence_ids": sentence_ids
                }
        
        storage.save_segment_timestamps(segment_timestamps)
        logger.info(f"Saved {len(segment_timestamps)} segment timestamps")
        
        output = {
            "knowledge_segments": all_segments,
            "current_step": "step7_segment",
            "current_step_status": "completed",
            "token_usage": {"step7_segment": total_tokens}
        }
        
        logger.log_output({"segment_count": len(all_segments)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step7_segment": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"knowledge_segments": [], "errors": [{"step": "step7", "error": str(e)}]}


# ============================================================================
# Step 7b: 可视化场景识别 Prompt (与步骤8a并行)
# ============================================================================

VISUALIZATION_SCENE_PROMPT = """请分析以下知识点片段是否适合用可视化（截图/视频）呈现。

【片段信息】
- 片段ID：{segment_id}
- 内容：{full_text}
- DARPA问题：{darpa_question}

【5种可视化场景】
1. 层级/结构类：涉及架构分层、目录结构、数据结构组成
2. 流程/流转类：逻辑有分支、闭环或顺序依赖
3. 实操/界面类：涉及软件操作、命令输出、界面配置
4. 对比/差异类：需区分不同状态、方案的差异
5. 复杂逻辑关系类：多元素关联（ER图、依赖关系图等）

【判断原则】
- 判断依据是"可视化效果是否优于纯文字"
- 不需要关键词触发，基于语义理解判断
- 即使文字描述完整，如果图更直观也应标记

【输出格式】
{{
  "is_visualization_candidate": true/false,
  "scene_type": "层级/结构类/流程/流转类/实操/界面类/对比/差异类/复杂逻辑关系类/null",
  "expected_visual_forms": ["可视化形态1", "可视化形态2"],
  "key_elements": ["截图必须包含的关键元素1", "关键元素2"],
  "visual_anchor_text": "片段中最能代表该可视化场景的一句话或短语（用于精确时间定位）",
  "min_completeness": 0.7,
  "judgment_basis": "判断依据说明"
}}"""


async def step7b_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：response
    - 条件：result and result.get('is_visualization_candidate', False)
    - 条件：anchor_text
    依据来源（证据链）：
    - 配置字段：is_visualization_candidate。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step7b_viz_scene", state.get("output_dir", "output/logs"))
    logger.start()
    
    segments = state.get("knowledge_segments", [])
    logger.log_input({"segment_count": len(segments)})
    
    try:
        llm = create_llm_client(purpose="analysis")
        storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
        
        # 构建所有prompt
        async def process_segment(segment):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            输入参数：
            - segment: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            prompt = VISUALIZATION_SCENE_PROMPT.format(
                segment_id=segment.get("segment_id", ""),
                full_text=segment.get("full_text", "")[:800],
                darpa_question=f"{segment.get('darpa_question', '')} - {segment.get('darpa_question_name', '')}"
            )
            try:
                result, response = await llm.complete_json(prompt)
                return segment, result, response
            except Exception as e:
                logger.log_warning(f"Failed for {segment['segment_id']}: {e}")
                return segment, None, None
        
        # 并发执行所有segment
        logger.info(f"并发处理 {len(segments)} 个segment...")
        results = await asyncio.gather(*[process_segment(s) for s in segments])
        
        visualization_candidates = []
        total_tokens = 0
        candidate_counter = 1
        
        for segment, result, response in results:
            if response:
                total_tokens += response.total_tokens
                logger.log_llm_call(
                    prompt=f"Segment {segment['segment_id']}",
                    response=f"Viz Candidate: {result.get('is_visualization_candidate', False) if result else 'None'}",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
             
            if result and result.get("is_visualization_candidate", False):
                # 尝试查找精确时间戳
                anchor_text = result.get("visual_anchor_text", "")
                precise_timestamp = None
                
                if anchor_text:
                    precise_subs = storage.find_subtitle_by_text(anchor_text)
                    if precise_subs:
                        match = precise_subs[0]
                        precise_timestamp = {
                            "start_sec": match["start_sec"],
                            "end_sec": match["end_sec"]
                        }
                
                # 如果找不到精确时间，降级使用段落时间
                if not precise_timestamp:
                     segment_ts = storage.get_segment_timestamp(segment["segment_id"])
                     precise_timestamp = segment_ts
                
                visualization_candidates.append({
                    "viz_id": f"VIZ{candidate_counter:03d}",  # 新增ID
                    "segment_id": segment["segment_id"],
                    "scene_type": result.get("scene_type", ""),
                    "expected_visual_forms": result.get("expected_visual_forms", []),
                    "key_elements": result.get("key_elements", []),
                    "visual_anchor_text": anchor_text,
                    "timestamp": precise_timestamp,  # 新增时间戳
                    "min_completeness": result.get("min_completeness", 0.7),
                    "judgment_basis": result.get("judgment_basis", "")
                })
                candidate_counter += 1
        
        output = {
            "visualization_candidates": visualization_candidates,
            "token_usage": {"step7b_viz_scene": total_tokens}
        }
        
        logger.log_output({"candidate_count": len(visualization_candidates)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step7b_viz_scene": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"visualization_candidates": [], "errors": [{"step": "step7b", "error": str(e)}]}


# ============================================================================
# 10类断层定义（硬编码）
# ============================================================================

FAULT_DEFINITIONS = {
    1: {
        "name": "显性指引类断层",
        "description": "文字稿出现显性指引词，且指引词后无任何核心内容描述",
        "trigger_keywords": ["看这个", "如图所示", "PPT上", "动画里", "黑板上", "看这里"],
        "visual_form": "PPT静态页/动画定格帧/板书完整页",
        "time_anchor": "指引词后0.5-2秒"
    },
    2: {
        "name": "结论无推导类断层",
        "description": "突然给出公式/定理/结论，但无推导步骤",
        "trigger_keywords": ["所以公式是", "最终结论是", "由此得出", "因此"],
        "visual_form": "PPT推导步骤页/动画推演帧/公式手写步骤",
        "time_anchor": "结论前1-3秒"
    },
    3: {
        "name": "概念无定义类断层",
        "description": "提及专业术语/陌生概念，但无定义解释",
        "trigger_keywords": ["我们用XX", "这里的XX", "所谓的XX", "什么是"],
        "visual_form": "PPT术语卡片/概念动画演示帧/定义板书",
        "time_anchor": "概念出现后0-1秒"
    },
    4: {
        "name": "实操步骤断裂类断层",
        "description": "步骤描述跳步，缺失关键界面/参数",
        "trigger_keywords": ["点击", "设置", "输入", "选择", "运行"],
        "visual_form": "软件操作界面/实验器材摆放/步骤演示动画",
        "time_anchor": "同步截帧"
    },
    5: {
        "name": "分层分类无内容类断层",
        "description": "提及分层/分类，但未列出具体内容",
        "trigger_keywords": ["分为三层", "有四种类型", "包含五个模块", "分成"],
        "visual_form": "PPT层级框架图/分类对比表/模块关系图",
        "time_anchor": "提及后0.5-1.5秒"
    },
    6: {
        "name": "量化数据缺失类断层",
        "description": "定性描述但无具体数值支撑",
        "trigger_keywords": ["效率提升了", "性能更好", "数据显示", "对比"],
        "visual_form": "PPT柱状图/折线图/数据对比表",
        "time_anchor": "定性描述前1-2秒"
    },
    7: {
        "name": "指代模糊类断层",
        "description": "出现模糊指代词，但无明确指代对象",
        "trigger_keywords": ["这个结构", "那个方法", "该模型", "这里"],
        "visual_form": "PPT结构示意图/模型动画/方法流程图",
        "time_anchor": "前2-3秒至后1秒"
    },
    8: {
        "name": "动态过程空白类断层",
        "description": "提及动态过程，但无关键节点描述",
        "trigger_keywords": ["变化过程", "演变", "迭代", "流动", "传递"],
        "visual_form": "参数变化动画/算法迭代步骤帧/系统演变演示",
        "time_anchor": "全程区间"
    },
    9: {
        "name": "符号编号缺失类断层",
        "description": "提及带编号的内容，但未给出具体内容",
        "trigger_keywords": ["公式(", "图3-", "表2", "如式", "第X步"],
        "visual_form": "PPT编号公式页/编号图表页/板书编号内容",
        "time_anchor": "提及后0-1秒"
    },
    10: {
        "name": "对比逻辑缺失类断层",
        "description": "提及对比，但无对比维度/差异点",
        "trigger_keywords": ["对比", "差异", "区别", "不同", "相比"],
        "visual_form": "PPT对比表格/方案差异图/动画效果对比帧",
        "time_anchor": "提及后0.5-1.5秒"
    }
}


# ============================================================================
# Step 8a: 断层分类粗定位 Prompt
# ============================================================================

FAULT_DETECT_PROMPT = """请识别以下知识点片段中的语义断层。

【知识点片段】
{segment_text}

【片段上下文】
- 知识点：{knowledge_point}
- DARPA问题：{darpa_question} - {darpa_question_name}
- 语义维度：{semantic_dimension}

【10类断层类型及特征】
1. 显性指引类：出现"看这个PPT/动画/如图所示"等指引词，后无内容描述
2. 结论无推导类：突然给出公式/结论，无推导过程
3. 概念无定义类：提及专业术语，无定义解释
4. 实操步骤断裂类：步骤描述跳步，缺失关键界面/参数
5. 分层分类无内容类：提及"分X层/类"，未列出具体内容
6. 量化数据缺失类：定性描述无具体数值支撑
7. 指代模糊类："这个结构/那个方法"等模糊指代
8. 动态过程空白类：提及动态过程，无关键节点描述
9. 符号编号缺失类：提及"公式(1)/图3-2"，无具体内容
10. 对比逻辑缺失类：提及对比，无对比维度/差异点

【输出要求】
如果发现断层，输出：
{{
  "has_fault": true,
  "faults": [
    {{
      "fault_type": 1,
      "fault_type_name": "显性指引类断层",
      "trigger_text": "触发断层的原文",
      "trigger_keywords": ["关键词"]
    }}
  ]
}}

如果没有断层：
{{"has_fault": false, "faults": []}}"""


async def step8a_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：response
    - 条件：result and result.get('has_fault')
    - 条件：result
    依据来源（证据链）：
    - 配置字段：has_fault。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step8a_fault_detect", state.get("output_dir", "output/logs"))
    logger.start()
    
    segments = state.get("knowledge_segments", [])
    logger.log_input({"segment_count": len(segments)})
    
    try:
        llm = create_llm_client(purpose="analysis")
        
        async def detect_faults(segment):
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            输入参数：
            - segment: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            prompt = FAULT_DETECT_PROMPT.format(
                segment_text=segment.get("full_text", ""),
                knowledge_point=segment.get("knowledge_point", ""),
                darpa_question=segment.get("darpa_question", ""),
                darpa_question_name=segment.get("darpa_question_name", ""),
                semantic_dimension=segment.get("semantic_dimension", {}).get("description", "")
            )
            try:
                result, response = await llm.complete_json(prompt)
                return segment, result, response
            except Exception as e:
                logger.log_warning(f"Failed for {segment['segment_id']}: {e}")
                return segment, None, None
        
        # 并发执行所有segment
        logger.info(f"并发检测 {len(segments)} 个segment的断层...")
        results = await asyncio.gather(*[detect_faults(s) for s in segments])
        
        all_faults = []
        fault_counter = 1
        total_tokens = 0
        
        for segment, result, response in results:
            if response:
                total_tokens += response.total_tokens
                logger.log_llm_call(
                    prompt=f"Segment {segment['segment_id']}",
                    response=f"Faults: {len(result.get('faults', [])) if result else 0}",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
            if result and result.get("has_fault"):
                for fault in result.get("faults", []):
                    all_faults.append({
                        "fault_id": f"FAULT{fault_counter:03d}",
                        "segment_id": segment["segment_id"],
                        "fault_type": fault["fault_type"],
                        "fault_type_name": fault.get("fault_type_name", ""),
                        "trigger_text": fault.get("trigger_text", ""),
                        "trigger_keywords": fault.get("trigger_keywords", []),
                        "fault_context": {
                            "knowledge_point": segment.get("knowledge_point"),
                            "darpa_question": segment.get("darpa_question")
                        }
                    })
                    fault_counter += 1
        
        output = {
            "fault_candidates": all_faults,
            "token_usage": {"step8a_fault_detect": total_tokens}
        }
        
        logger.log_output({"fault_count": len(all_faults)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step8a_fault_detect": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"fault_candidates": [], "errors": [{"step": "step8a", "error": str(e)}]}


# ============================================================================
# Step 8b: 断层精确定位 Prompt
# ============================================================================

FAULT_LOCATE_PROMPT = """请对以下断层进行精确定位和缺失内容分析。

【断层信息】
- 断层类型：{fault_type_name}
- 触发文本：{trigger_text}
- 所属片段：{segment_text}

【触发句子时间戳】
{trigger_timestamp}

【时间锚点规则】
{time_anchor_rule}

【分层校验标准】
- 核心必含项：{core_requirements}
- 次要加分项：{secondary_requirements}

【输出格式】
{{
  "fault_location": {{
    "start_sec": 10.5,
    "end_sec": 15.2
  }},
  "visual_form": "预期的可视化形态",
  "missing_content": {{
    "must_supplement": "必须补全的核心内容",
    "secondary_supplement": "次要补全内容"
  }}
}}"""


async def step8b_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：precise_subs
    - 条件：segment
    - 条件：loc and 'start_sec' in loc
    依据来源（证据链）：
    - 配置字段：start_sec。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step8b_fault_locate", state.get("output_dir", "output/logs"))
    logger.start()
    
    fault_candidates = state.get("fault_candidates", [])
    segments = state.get("knowledge_segments", [])
    logger.log_input({"fault_count": len(fault_candidates)})
    
    try:
        llm = create_llm_client(purpose="analysis")
        storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
        
        semantic_faults = []
        total_tokens = 0
        
        async def process_fault(fault):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过JSON 解析/序列化实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            决策逻辑：
            - 条件：precise_subs
            - 条件：segment
            - 条件：loc and 'start_sec' in loc
            依据来源（证据链）：
            - 配置字段：start_sec。
            输入参数：
            - fault: 函数入参（类型：未标注）。
            输出参数：
            - 结构化字典结果（包含字段：fault_id, segment_id, fault_type, fault_type_name, fault_location, visual_form, missing_content, _tokens）。"""
            fault_type = fault["fault_type"]
            fault_def = FAULT_DEFINITIONS.get(fault_type, {})
            
            # 获取片段信息
            segment = next((s for s in segments if s["segment_id"] == fault["segment_id"]), None)
            segment_text = segment.get("full_text", "") if segment else ""
            
            # 获取时间戳 - 优先尝试精确匹配原始字幕
            segment_ts = storage.get_segment_timestamp(fault["segment_id"]) or {}
            
            # 尝试根据触发词精确查找原始字幕
            trigger_text = fault["trigger_text"]
            precise_subs = storage.find_subtitle_by_text(trigger_text)
            
            if precise_subs:
                # 找到了精确对应的原始字幕
                match = precise_subs[0]
                trigger_timestamp = f"start: {match['start_sec']}s, end: {match['end_sec']}s (Precise Match)"
                start_base = match['start_sec']
                end_base = match['end_sec']
            else:
                # 降级到段落时间戳
                trigger_timestamp = f"start: {segment_ts.get('start_sec', 0)}s, end: {segment_ts.get('end_sec', 0)}s (Segment Range)"
                start_base = segment_ts.get('start_sec', 0)
                end_base = segment_ts.get('end_sec', 0)
            
            prompt = FAULT_LOCATE_PROMPT.format(
                fault_type_name=fault["fault_type_name"],
                trigger_text=fault["trigger_text"],
                segment_text=segment_text[:500],
                trigger_timestamp=trigger_timestamp,
                time_anchor_rule=fault_def.get("time_anchor", "同步"),
                core_requirements=fault_def.get("core_requirements", "核心内容"),
                secondary_requirements=fault_def.get("secondary_requirements", "补充内容")
            )
            
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Fault {fault['fault_id']}",
                    response=f"Loc: {result.get('fault_location')}",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                # 如果LLM返回了具体时间，进行合理性校验
                loc = result.get("fault_location") or {}
                if loc and "start_sec" in loc:
                    # 简单校验：不能偏离基准时间太远（例如超过30秒）
                    if abs(loc["start_sec"] - start_base) > 30:
                        logger.log_warning(f"Fault location deviation too large: {loc} vs base {start_base}")
                        # 如果偏离太大，可能幻觉，优先使用基准时间
                        loc = {"start_sec": start_base, "end_sec": end_base}
                else:
                    loc = {"start_sec": start_base, "end_sec": end_base}
                
                return {
                    "fault_id": fault["fault_id"],
                    "segment_id": fault["segment_id"],
                    "fault_type": fault_type,
                    "fault_type_name": fault["fault_type_name"],
                    "fault_location": loc,
                    "visual_form": result.get("visual_form", fault_def.get("visual_form", "")),
                    "missing_content": result.get("missing_content", {}),
                    "_tokens": response.total_tokens
                }
            except Exception as e:
                logger.log_warning(f"LLM call failed for fault {fault['fault_id']}: {e}")
                # Fallback on LLM failure: use base timestamps
                return {
                    "fault_id": fault["fault_id"],
                    "segment_id": fault["segment_id"],
                    "fault_type": fault_type,
                    "fault_type_name": fault["fault_type_name"],
                    "fault_location": {"start_sec": start_base, "end_sec": end_base},
                    "visual_form": fault_def.get("visual_form", ""),
                    "missing_content": {},
                    "_tokens": 0
                }

        logger.info(f"并发定位 {len(fault_candidates)} 个潜在断层...")
        results = await asyncio.gather(*[process_fault(f) for f in fault_candidates])
        
        # 聚合结果
        semantic_faults = []
        total_tokens = 0
        for r in results:
            total_tokens += r.pop("_tokens", 0)
            semantic_faults.append(r)
        
        output = {
            "semantic_faults": semantic_faults,
            "current_step": "step8b_fault_locate",
            "current_step_status": "completed",
            "token_usage": {"step8b_fault_locate": total_tokens}
        }
        
        logger.log_output({"semantic_fault_count": len(semantic_faults)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step8b_fault_locate": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"semantic_faults": [], "errors": [{"step": "step8b", "error": str(e)}]}


# ============================================================================
# Step 7c: 知识点识别与合并 (新增)
# ============================================================================

KNOWLEDGE_POINT_MERGE_PROMPT = """请分析以下知识点片段，判断哪些属于同一个知识点的不同维度，并识别原文中的过渡语。

【原始片段列表】
{segments_info}

【合并原则】
1. **主题一致性**：内容围绕同一个核心概念
   - 例如："顺序查找的定义"和"顺序查找的比较次数规律"都是关于"顺序查找"
   - 例如："ASL的推导"、"ASL的含义"、"ASL的术语解释"都是关于"ASL"

2. **DARPA问题互补**：不同的DARPA问题从不同角度描述同一知识点
   - Q1（是什么）+ Q3（怎么做）+ Q7（与什么关联）→ 同一知识点的不同维度
   - 单一Q1或单一Q3 → 可能是独立知识点

3. **语义层次相同**：hierarchy_type相同的更可能属于同一知识点
   - 都是"定义层" → 可能是同一知识点的基础部分
   - 都是"原理层" → 可能是同一知识点的深入部分

4. **位置连续性**：相邻的segment更可能属于同一知识点

5. **适度粒度**：合并后的知识点数应该在3-5个之间（原{segment_count}个）

【过渡语识别规则】
过渡语只存在于知识点与知识点之间。请识别每个知识点开头是否有原文中的过渡语句：
1. 主题过渡："接下来"、"下面我们来看"、"那么"、"现在我们讨论"
2. 层次过渡："首先"、"其次"、"最后"、"第一个方面"
3. 总结过渡："总结一下"、"综上所述"、"所以说"

**重要**：如果原文有过渡语句，直接提取；如果没有，transition设为null

【输出格式】
{{
  "knowledge_points": [
    {{
      "kp_title": "顺序查找的基础概念",
      "segment_ids": ["SEG001", "SEG002"],
      "merge_reason": "两个segment都是介绍顺序查找的基础知识，Q1定义+Q7关联，逻辑连贯",
      "darpa_questions": ["Q1", "Q7"],
      "hierarchy_type": "定义层",
      "transition": "接下来我们来看顺序查找算法",
      "transition_source": "original"
    }}
  ]
}}

【注意事项】
- 如果某个segment独立性强，可以不合并，单独作为一个知识点
- 确保所有segment_id都被包含在合并计划中
- kp_title应该概括所有合并segment的共同主题
- transition_source只能是"original"（原文提取）或null（原文无过渡语）
"""


async def step7c_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：time_ranges
    - 条件：seg_ts
    依据来源（证据链）：
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step7c_kp_merge", state.get("output_dir", "output/logs"))
    logger.start()
    
    segments = state.get("knowledge_segments", [])
    logger.log_input({"segment_count": len(segments)})
    
    try:
        llm = create_llm_client(purpose="analysis")
        storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
        
        # 格式化segment信息
        segments_info = _format_segments_for_merge(segments)
        
        # 调用LLM分析
        prompt = KNOWLEDGE_POINT_MERGE_PROMPT.format(
            segments_info=segments_info,
            segment_count=len(segments)
        )
        
        result, response = await llm.complete_json(prompt)
        
        logger.log_llm_call(
            prompt="Knowledge Point Merge Analysis",
            response=f"Merged into {len(result.get('knowledge_points', []))} KPs",
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            model=response.model,
            latency_ms=response.latency_ms
        )
        
        merge_plan = result.get("knowledge_points", [])
        
        # 验证并自动补全合并计划
        merge_plan = _validate_kp_merge_plan(merge_plan, segments)
        
        # 构建knowledge_points
        knowledge_points = []
        kp_counter = 1
        
        for kp_plan in merge_plan:
            segment_ids = kp_plan.get("segment_ids", [])
            
            # 收集这些segment的信息
            kp_segments = [s for s in segments if s["segment_id"] in segment_ids]
            
            # 合并full_text
            full_text = "\n\n".join([s["full_text"] for s in kp_segments])
            
            # 合并extracted_elements
            merged_elements = {
                "examples": [],
                "analogies": [],
                "concrete_words": [],
                "insights": []
            }
            for seg in kp_segments:
                elements = seg.get("extracted_elements", {})
                for key in merged_elements:
                    merged_elements[key].extend(elements.get(key, []))
            
            # 计算时间范围
            time_ranges = []
            for sid in segment_ids:
                seg_ts = storage.get_segment_timestamp(sid)
                if seg_ts:
                    time_ranges.append((seg_ts["start_sec"], seg_ts["end_sec"]))
            
            if time_ranges:
                kp_start = min(t[0] for t in time_ranges)
                kp_end = max(t[1] for t in time_ranges)
            else:
                kp_start, kp_end = 0, 0
            
            # 构建knowledge_point
            kp_id = f"KP{kp_counter:03d}"
            kp = {
                "kp_id": kp_id,
                "segment_id": kp_id,  # 添加alias以兼容下游节点
                "kp_title": kp_plan.get("kp_title", ""),
                "segments": kp_segments,  # 保留原始segment信息
                "segment_ids": segment_ids,
                "darpa_questions": kp_plan.get("darpa_questions", []),
                "hierarchy_type": kp_plan.get("hierarchy_type", ""),
                "merge_reason": kp_plan.get("merge_reason", ""),
                
                # 过渡语（优先采用原文，无则为null）
                "transition": kp_plan.get("transition"),
                "transition_source": kp_plan.get("transition_source"),
                
                # 聚合信息
                "full_text": full_text,
                "extracted_elements": merged_elements,
                "time_range": {
                    "start_sec": kp_start,
                    "end_sec": kp_end
                }
            }
            
            knowledge_points.append(kp)
            kp_counter += 1
        
        # 存储knowledge_point时间戳
        kp_timestamps = {}
        for kp in knowledge_points:
            kp_timestamps[kp["kp_id"]] = {
                "start_sec": kp["time_range"]["start_sec"],
                "end_sec": kp["time_range"]["end_sec"],
                "segment_ids": kp["segment_ids"]
            }
        storage.save_kp_timestamps(kp_timestamps)
        logger.info(f"Saved {len(kp_timestamps)} KP timestamps")
        
        output = {
            "knowledge_points": knowledge_points,
            "current_step": "step7c_kp_merge",
            "current_step_status": "completed",
            "token_usage": {"step7c_kp_merge": response.total_tokens}
        }
        
        logger.log_output({
            "kp_count": len(knowledge_points),
            "original_segment_count": len(segments)
        })
        
        timing = logger.end(success=True)
        output["step_timings"] = {"step7c_kp_merge": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        # 回退：不合并，每个segment作为独立knowledge_point
        return {
            "knowledge_points": _create_no_merge_kps(segments, storage),
            "errors": [{"step": "step7c", "error": str(e)}]
        }


def _format_segments_for_merge(segments: List[Dict]) -> str:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - segments: 数据列表/集合（类型：List[Dict]）。
    输出参数：
    - 字符串结果。"""
    info_lines = []
    for seg in segments:
        info_lines.append(f"""
【{seg['segment_id']}】
- 知识点：{seg['knowledge_point']}
- DARPA问题：{seg['darpa_question']} - {seg['darpa_question_name']}
- 语义维度：{seg['semantic_dimension']['hierarchy_type']} / {seg['semantic_dimension']['logic_relation']}
- 层次描述：{seg['semantic_dimension']['description']}
- 内容摘要：{seg['core_semantic']['summary']}
""")
    return "\n".join(info_lines)


def _validate_kp_merge_plan(merge_plan: List[Dict], segments: List[Dict]) -> List[Dict]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：missing
    - 条件：not seg
    依据来源（证据链）：
    输入参数：
    - merge_plan: 函数入参（类型：List[Dict]）。
    - segments: 数据列表/集合（类型：List[Dict]）。
    输出参数：
    - Dict 列表（与输入或处理结果一一对应）。"""
    all_seg_ids = {seg["segment_id"] for seg in segments}
    planned_seg_ids = set()
    
    for kp in merge_plan:
        planned_seg_ids.update(kp.get("segment_ids", []))
    
    missing = all_seg_ids - planned_seg_ids
    if missing:
        # 自动补全：为缺失的每个segment创建一个独立的KP
        for sid in missing:
            seg = next((s for s in segments if s["segment_id"] == sid), None)
            if not seg: continue
            merge_plan.append({
                "kp_title": seg["knowledge_point"],
                "segment_ids": [sid],
                "merge_reason": "LLM遗漏自动补全",
                "darpa_questions": [seg["darpa_question"]],
                "hierarchy_type": seg.get("semantic_dimension", {}).get("hierarchy_type", ""),
                "transition": None,
                "transition_source": None
            })
    
    return merge_plan


def _create_no_merge_kps(segments: List[Dict], storage) -> List[Dict]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - segments: 数据列表/集合（类型：List[Dict]）。
    - storage: 函数入参（类型：未标注）。
    输出参数：
    - Dict 列表（与输入或处理结果一一对应）。"""
    kps = []
    for i, seg in enumerate(segments):
        seg_ts = storage.get_segment_timestamp(seg["segment_id"]) or {}
        kps.append({
            "kp_id": f"KP{i+1:03d}",
            "kp_title": seg["knowledge_point"],
            "segments": [seg],
            "segment_ids": [seg["segment_id"]],
            "darpa_questions": [seg["darpa_question"]],
            "hierarchy_type": seg["semantic_dimension"]["hierarchy_type"],
            "merge_reason": "独立知识点",
            "full_text": seg["full_text"],
            "extracted_elements": seg.get("extracted_elements", {}),
            "time_range": {
                "start_sec": seg_ts.get("start_sec", 0),
                "end_sec": seg_ts.get("end_sec", 0)
            }
        })
    return kps
