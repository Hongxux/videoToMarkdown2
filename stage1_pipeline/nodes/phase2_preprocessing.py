"""
Phase 2: 文字稿预处理
Steps 2-6
"""

import asyncio
from typing import Dict, Any, List

from ..state import PipelineState
from ..llm.client import create_llm_client
from ..tools.storage import LocalStorage
from ..monitoring.logger import get_logger


# ============================================================================
# Step 2: 智能纠错 Prompt
# ============================================================================

CORRECTION_PROMPT = """你是一个专业的ASR纠错助手，请修正以下字幕中的同音字错误。

【视频领域】{domain}

【字幕列表】
{subtitles}

【纠错范围】
仅纠正同音字错误，如：
- "维新者"→"唯心者"（哲学领域，上下文有"唯物主义"）
- "行而上学"→"形而上学"
- "的/得/地"、"在/再"混用
- 计算机领域："栈针"→"栈帧"

【纠错原则】
- 只纠正明显的同音字错误
- 不确定时保留原文
- 不纠正语法错误或标点错误
- 基于领域推断专业术语

【输出格式】
{{
  "corrected_subtitles": [
    {{
      "subtitle_id": "SUB001",
      "corrected_text": "纠错后文本",
      "corrections": [
        {{"original": "原文", "corrected": "纠正", "reason": "判断依据"}}
      ]
    }}
  ]
}}"""


async def step2_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤2：DeepSeek智能纠错
    
    类型：LLM
    核心动作：基于领域上下文，修正ASR语音识别的同音字错误
    """
    logger = get_logger("step2_correction", state.get("output_dir", "output/logs"))
    logger.start()
    
    # 读取字幕（从 read_subtitle_sample 的完整版本）
    from ..tools.file_validator import read_subtitle_sample
    subtitles = read_subtitle_sample(state["subtitle_path"], count=1000)  # 读取全部
    
    logger.log_input({
        "domain": state.get("domain", ""),
        "subtitle_count": len(subtitles)
    })
    
    try:
        llm = create_llm_client(purpose="refinement")
        storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
        
        # 分批处理（减小批次从100到50，防止输出过长导致截断）
        batch_size = 15
        async def process_batch(idx, batch):
            subtitles_text = "\n".join([
                f"[{s['subtitle_id']}] {s['text']}" 
                for s in batch
            ])
            
            prompt = CORRECTION_PROMPT.format(
                domain=state.get("domain", "通用"),
                subtitles=subtitles_text
            )
            
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Batch {idx + 1}",
                    response=f"{len(result.get('corrected_subtitles', []))} corrections",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                batch_corrected = []
                batch_corrections = []
                for item in result.get("corrected_subtitles", []):
                    original = next((s for s in batch if s["subtitle_id"] == item["subtitle_id"]), None)
                    if original:
                        batch_corrected.append({
                            "subtitle_id": item["subtitle_id"],
                            "corrected_text": item.get("corrected_text", original["text"]),
                            "start_sec": original["start_sec"],
                            "end_sec": original["end_sec"],
                            "corrections": item.get("corrections", [])
                        })
                        batch_corrections.extend(item.get("corrections", []))
                
                # 对于未在结果中返回的字幕，保留原文
                processed_ids = {c["subtitle_id"] for c in batch_corrected}
                for s in batch:
                    if s["subtitle_id"] not in processed_ids:
                        batch_corrected.append({
                            "subtitle_id": s["subtitle_id"],
                            "corrected_text": s["text"],
                            "start_sec": s["start_sec"],
                            "end_sec": s["end_sec"],
                            "corrections": []
                        })
                
                return batch_corrected, batch_corrections, response.total_tokens
            except Exception as e:
                logger.log_warning(f"Batch {idx + 1} failed: {e}")
                # 失败回退：保留原文
                fallback = []
                for s in batch:
                    fallback.append({
                        "subtitle_id": s["subtitle_id"],
                        "corrected_text": s["text"],
                        "start_sec": s["start_sec"],
                        "end_sec": s["end_sec"],
                        "corrections": []
                    })
                return fallback, [], 0

        # 构建任务列表
        logger.info(f"并发执行 {len(subtitles)} 条字幕的纠错处理...")
        batches = [subtitles[i:i + batch_size] for i in range(0, len(subtitles), batch_size)]
        tasks = [process_batch(i, batch) for i, batch in enumerate(batches)]
        
        # 执行并行任务
        results = await asyncio.gather(*tasks)
        
        # 收集结果
        all_corrected = []
        all_corrections = []
        total_tokens = 0
        for batch_corrected, batch_corrections, tokens in results:
            all_corrected.extend(batch_corrected)
            all_corrections.extend(batch_corrections)
            total_tokens += tokens
        
        # 排序，确保顺序正确
        all_corrected.sort(key=lambda x: int(x["subtitle_id"].replace("SUB", "")))
        
        # 存储纠错后的字幕时间戳到本地（更精确的时间定位，且与后续步骤文本一致）
        subtitle_timestamps = {
            s["subtitle_id"]: {
                "start_sec": s["start_sec"],
                "end_sec": s["end_sec"],
                "text": s["corrected_text"][:50]  # 存储纠错后文本用于匹配
            }
            for s in all_corrected
        }
        storage.save_subtitle_timestamps(subtitle_timestamps)
        logger.info(f"Saved {len(subtitle_timestamps)} subtitle timestamps to local storage")
        
        output = {
            "corrected_subtitles": all_corrected,
            "correction_summary": all_corrections,
            "current_step": "step2_correction",
            "current_step_status": "completed",
            "token_usage": {"step2_correction": total_tokens},
            "llm_calls": [{
                "step_name": "step2_correction",
                "model": "deepseek-chat",
                "total_tokens": total_tokens
            }]
        }
        
        logger.log_output({"corrected_count": len(all_corrected), "corrections_made": len(all_corrections)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step2_correction": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.log_warning("LLM failed, using original subtitles as fallback")
        
        # Fallback: 使用原始字幕作为 corrected_subtitles
        from ..tools.file_validator import read_subtitle_sample
        subtitles = read_subtitle_sample(state["subtitle_path"], count=1000)
        
        fallback_corrected = [
            {
                "subtitle_id": s["subtitle_id"],
                "corrected_text": s["text"],  # 原始文本
                "start_sec": s["start_sec"],
                "end_sec": s["end_sec"],
                "corrections": []
            }
            for s in subtitles
        ]
        
        logger.end(success=False)
        return {
            "corrected_subtitles": fallback_corrected,
            "errors": [{"step": "step2", "error": str(e), "fallback_used": True}],
            "current_step_status": "fallback"
        }


# ============================================================================
# Step 3: 自然语义合并 Prompt
# ============================================================================

MERGE_PROMPT = """请将以下ASR细切字幕合并为语法完整、语义通顺的句子。

【字幕列表】
{subtitles}

【合并规则】
1. 相邻字幕如果语义连贯，应合并为一句
2. 遇到句号、问号、感叹号等结束标点，作为句子边界
3. 遇到明显的话题转换，作为句子边界
4. 单句不超过80字

【输出格式】
{{
  "merged_sentences": [
    {{
      "sentence_id": "S001",
      "text": "合并后的完整句子",
      "source_subtitle_ids": ["SUB001", "SUB002"]
    }}
  ]
}}"""


async def step3_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤3：自然语义合并
    
    类型：LLM
    核心动作：将ASR细切字幕碎片句合并为语法完整、语义通顺的句子
    约束：单句不超过80字
    """
    logger = get_logger("step3_merge", state.get("output_dir", "output/logs"))
    logger.start()
    
    corrected = state.get("corrected_subtitles", [])
    logger.log_input({"subtitle_count": len(corrected)})
    
    try:
        llm = create_llm_client(purpose="refinement")
        
        # 并行滑动窗口处理
        window_size = 10
        async def process_window(idx, i):
            batch = corrected[i:i + window_size]
            
            subtitles_text = "\n".join([
                f"[{s['subtitle_id']}] {s['corrected_text']}" 
                for s in batch
            ])
            
            prompt = MERGE_PROMPT.format(subtitles=subtitles_text)
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Window {idx + 1}",
                    response=f"{len(result.get('merged_sentences', []))} merged",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                enriched_sentences = []
                for item in result.get("merged_sentences", []):
                    # 计算时间戳 (在窗口局部计算)
                    source_ids = item.get("source_subtitle_ids", [])
                    sub_map = {s["subtitle_id"]: s for s in batch}
                    relevant_subs = [sub_map[sid] for sid in source_ids if sid in sub_map]
                    
                    start_sec = min((s["start_sec"] for s in relevant_subs), default=0)
                    end_sec = max((s["end_sec"] for s in relevant_subs), default=0)
                    
                    enriched_sentences.append({
                        "text": item["text"],
                        "start_sec": start_sec,
                        "end_sec": end_sec,
                        "source_subtitle_ids": source_ids
                    })
                
                return enriched_sentences, response.total_tokens
            except Exception as e:
                logger.log_warning(f"Window {idx + 1} failed: {e}")
                return [], 0

        logger.info(f"并发执行 {len(corrected)} 条字幕的语义合并...")
        window_starts = list(range(0, len(corrected), window_size - 5))
        tasks = [process_window(idx, i) for idx, i in enumerate(window_starts)]
        
        results = await asyncio.gather(*tasks)
        
        # 聚合结果并重新统一编号
        all_merged = []
        total_tokens = 0
        sentence_counter = 1
        
        for enriched_sentences, tokens in results:
            total_tokens += tokens
            for sent in enriched_sentences:
                sent["sentence_id"] = f"S{sentence_counter:03d}"
                sentence_counter += 1
                all_merged.append(sent)
        
        output = {
            "merged_sentences": all_merged,
            "current_step": "step3_merge",
            "current_step_status": "completed",
            "token_usage": {"step3_merge": total_tokens}
        }
        
        logger.log_output({"merged_count": len(all_merged)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step3_merge": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"merged_sentences": [], "errors": [{"step": "step3", "error": str(e)}]}


# ============================================================================
# Step 4: 局部冗余删除 Prompt
# ============================================================================

CLEAN_LOCAL_PROMPT = """请清理以下句子中的无价值冗余内容。

【句子列表】
{sentences}

【清理类型（全部直接删除）】
1. 结巴类：单句内连续重复词汇/音节，如"我我我想说"→"我想说"
2. 单句内无意义重复：如"这个这个方案可行"→"这个方案可行"
3. 口语填充词：如"那个"、"就是说"、"然后"、"嗯"、"啊"、"呃"
4. 同音/近音词误判：如"产品的的质量"→"产品的质量"
5. 背景噪音误判：如句末的"呃嘶"等无意义音
6. 单句语义赘述：如"我个人认为我觉得"→"我认为"

【注意】
- 保留有意义的重复（如强调性重复）
- 保留有表达作用的语气词
- 仅处理单句内的冗余

【输出格式】
{{
  "cleaned_sentences": [
    {{
      "sentence_id": "S001",
      "cleaned_text": "清理后文本",
      "removed_items": ["删除的内容1", "删除的内容2"]
    }}
  ]
}}"""


async def step4_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤4：局部冗余删除 + 时间戳存储
    
    类型：LLM + 本地存储
    核心动作：清理单句内无价值冗余，并将时间戳存储到本地
    """
    logger = get_logger("step4_clean_local", state.get("output_dir", "output/logs"))
    logger.start()
    
    merged = state.get("merged_sentences", [])
    logger.log_input({"sentence_count": len(merged)})
    
    try:
        llm = create_llm_client(purpose="refinement")
        storage = LocalStorage(state.get("output_dir", "output") + "/local_storage")
        
        async def process_batch(idx, batch):
            sentences_text = "\n".join([
                f"[{s['sentence_id']}] {s['text']}" 
                for s in batch
            ])
            
            prompt = CLEAN_LOCAL_PROMPT.format(sentences=sentences_text)
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Batch {idx + 1}",
                    response=f"{len(result.get('cleaned_sentences', []))} cleaned",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                batch_results = []
                for item in result.get("cleaned_sentences", []):
                    batch_results.append({
                        "sentence_id": item["sentence_id"],
                        "cleaned_text": item.get("cleaned_text", ""),
                        "removed_items": item.get("removed_items", [])
                    })
                return batch_results, response.total_tokens
            except Exception as e:
                logger.log_warning(f"Batch {idx + 1} failed: {e}")
                return [], 0

        logger.info(f"并发执行 {len(merged)} 条字幕的局部冗余处理...")
        batch_size = 10
        batches = [merged[i:i + batch_size] for i in range(0, len(merged), batch_size)]
        tasks = [process_batch(i, batch) for i, batch in enumerate(batches)]
        
        results = await asyncio.gather(*tasks)
        
        # 收集结果
        all_cleaned = []
        total_tokens = 0
        for batch_results, tokens in results:
            all_cleaned.extend(batch_results)
            total_tokens += tokens
        
        # 存储时间戳到本地
        timestamps = {
            s["sentence_id"]: {
                "start_sec": s["start_sec"],
                "end_sec": s["end_sec"]
            }
            for s in merged
        }
        storage.save_sentence_timestamps(timestamps)
        logger.info(f"Saved {len(timestamps)} sentence timestamps to local storage")
        
        output = {
            "cleaned_sentences": all_cleaned,
            "current_step": "step4_clean_local",
            "current_step_status": "completed",
            "token_usage": {"step4_clean_local": total_tokens}
        }
        
        logger.log_output({"cleaned_count": len(all_cleaned)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step4_clean_local": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"cleaned_sentences": [], "errors": [{"step": "step4", "error": str(e)}]}


# ============================================================================
# Step 5: 跨句冗余删除 Prompt
# ============================================================================

CLEAN_CROSS_PROMPT = """请识别以下句子中需要直接删除的跨句冗余内容。

【核心主题】{main_topic}

【句子列表】
{sentences}

【冗余类型（全部直接删除，无增量价值）】
1. 跨句完全重复：与前面句子内容完全相同或高度相似（相似度>0.95），无任何新信息
2. 离题性冗余：与核心主题完全无关的内容
3. 高频口头禅（无意义）：局部窗口内高频重复且与语义无关

【判断原则】
- 删除的内容必须是"无增量价值"的
- 如果句子虽然重复但有新信息补充，不要删除
- 离题判断需结合核心主题，不要误删相关内容

【输出格式】
{{
  "keep_sentence_ids": ["S001", "S003", "S004"],
  "deleted_sentences": [
    {{"sentence_id": "S002", "reason": "与S001完全重复"}}
  ]
}}"""


async def step5_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤5：跨句冗余删除
    
    类型：LLM
    核心动作：删除跨句无价值冗余（完全重复/离题/无意义口头禅）
    """
    logger = get_logger("step5_clean_cross", state.get("output_dir", "output/logs"))
    logger.start()
    
    cleaned = state.get("cleaned_sentences", [])
    main_topic = state.get("main_topic", "")
    logger.log_input({"sentence_count": len(cleaned), "main_topic": main_topic})
    
    try:
        llm = create_llm_client(purpose="analysis")
        
        async def process_window(idx, i):
            batch = cleaned[i:i + window_size]
            
            sentences_text = "\n".join([
                f"[{s['sentence_id']}] {s['cleaned_text']}" 
                for s in batch
            ])
            
            prompt = CLEAN_CROSS_PROMPT.format(
                main_topic=main_topic,
                sentences=sentences_text
            )
            
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Window {idx + 1}",
                    response=f"Kept {len(result.get('keep_sentence_ids', []))}",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                return result.get("keep_sentence_ids", []), result.get("deleted_sentences", []), response.total_tokens
            except Exception as e:
                logger.log_warning(f"Window {idx + 1} failed: {e}")
                return [], [], 0

        
        logger.info(f"并发执行 {len(cleaned)} 条字幕的跨句冗余处理...")
        window_size = 8
        window_starts = list(range(0, len(cleaned), window_size - 3))
        tasks = [process_window(idx, i) for idx, i in enumerate(window_starts)]
        
        results = await asyncio.gather(*tasks)
        
        # 聚合结果
        keep_ids = set()
        deleted_info = []
        total_tokens = 0
        for k_ids, d_info, tokens in results:
            keep_ids.update(k_ids)
            deleted_info.extend(d_info)
            total_tokens += tokens
        
        # 过滤保留的句子
        non_redundant = [s for s in cleaned if s["sentence_id"] in keep_ids]
        
        output = {
            "non_redundant_sentences": non_redundant,
            "current_step": "step5_clean_cross",
            "current_step_status": "completed",
            "token_usage": {"step5_clean_cross": total_tokens}
        }
        
        logger.log_output({
            "original_count": len(cleaned),
            "kept_count": len(non_redundant),
            "deleted_count": len(deleted_info)
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step5_clean_cross": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"non_redundant_sentences": [], "errors": [{"step": "step5", "error": str(e)}]}


# ============================================================================
# Step 6: 跨句冗余合并 Prompt
# ============================================================================

MERGE_CROSS_PROMPT = """请将以下句子中语义重叠但含增量信息的内容合并为段落。

【句子列表】
{sentences}

【需要合并的冗余类型（有增量价值）】
1. 断句错误重复：ASR断句错误导致的拆分，后句有新内容
2. 跨句同义转述：语义重叠但有不同角度的补充
3. 跨句部分重复：部分内容重复，其余为有效补充

【合并规则】
1. 语义重叠但各有增量信息的句子，合并为一个段落
2. 保留所有增量信息，不丢失细节
3. 合并后的段落应语义连贯
4. 保持讲解者的表达风格
5. 不需要合并的句子单独作为一个段落

【输出格式】
{{
  "paragraphs": [
    {{
      "paragraph_id": "P001",
      "text": "合并后的段落文本",
      "source_sentence_ids": ["S001", "S002"],
      "merge_type": "同义转述"
    }}
  ]
}}"""



def _deduplicate_paragraphs(paragraphs: List[Dict]) -> List[Dict]:
    """
    贪婪去重算法：优先保留长段落，剔除子集段落
    """
    if not paragraphs:
        return []

    # 1. 预处理：构造辅助列表 (index, ids_set, count, item)
    candidates = []
    for i, p in enumerate(paragraphs):
        ids = set(p.get("source_sentence_ids", []))
        candidates.append({
            "index": i,
            "ids": ids,
            "count": len(ids),
            "item": p
        })
    
    # 2. 排序：按句子数量降序 (贪婪策略：保留信息量大的)
    candidates.sort(key=lambda x: x["count"], reverse=True)
    
    kept = []
    
    for cand in candidates:
        is_subset = False
        for k in kept:
            # 如果当前候选是已保留段落的子集，剔除
            if cand["ids"].issubset(k["ids"]):
                is_subset = True
                break
        
        if not is_subset:
            kept.append(cand)
    
    # 3. 恢复原始顺序 (为了保持文本的时间连贯性)
    kept.sort(key=lambda x: x["index"])
    
    return [k["item"] for k in kept]


async def step6_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤6：跨句冗余合并
    
    类型：LLM
    核心动作：整合跨句语义重叠且含增量信息的内容
    """
    logger = get_logger("step6_merge_cross", state.get("output_dir", "output/logs"))
    logger.start()
    
    non_redundant = state.get("non_redundant_sentences", [])
    logger.log_input({"sentence_count": len(non_redundant)})
    
    try:
        llm = create_llm_client(purpose="analysis")
        
        async def process_window(idx, i):
            batch = non_redundant[i:i + window_size]
            
            sentences_text = "\n".join([
                f"[{s['sentence_id']}] {s['cleaned_text']}" 
                for s in batch
            ])
            
            prompt = MERGE_CROSS_PROMPT.format(sentences=sentences_text)
            try:
                result, response = await llm.complete_json(prompt)
                
                logger.log_llm_call(
                    prompt=f"Window {idx + 1}",
                    response=f"{len(result.get('paragraphs', []))} paragraphs",
                    prompt_tokens=response.prompt_tokens,
                    completion_tokens=response.completion_tokens,
                    model=response.model,
                    latency_ms=response.latency_ms
                )
                
                return result.get("paragraphs", []), response.total_tokens
            except Exception as e:
                logger.log_warning(f"Window {idx + 1} failed: {e}")
                return [], 0

        logger.info(f"并发执行 {len(non_redundant)} 条字幕的语义合并(Phase 2)...")
        window_size = 8
        window_starts = list(range(0, len(non_redundant), window_size - 4))
        tasks = [process_window(idx, i) for idx, i in enumerate(window_starts)]
        
        results = await asyncio.gather(*tasks)
        
        # 聚合结果
        all_paragraphs = []
        total_tokens = 0
        for window_paragraphs, tokens in results:
            total_tokens += tokens
            for item in window_paragraphs:
                # 暂不生成ID，待去重后再生成
                all_paragraphs.append({
                    "text": item["text"],
                    "source_sentence_ids": item.get("source_sentence_ids", []),
                    "merge_type": item.get("merge_type", "无合并")
                })
        
        # 执行贪婪去重
        deduplicated = _deduplicate_paragraphs(all_paragraphs)
        
        # 重新生成ID
        final_paragraphs = []
        for idx, item in enumerate(deduplicated):
            item["paragraph_id"] = f"P{idx + 1:03d}"
            final_paragraphs.append(item)
            
        output = {
            "pure_text_script": final_paragraphs,
            "current_step": "step6_merge_cross",
            "current_step_status": "completed",
            "token_usage": {"step6_merge_cross": total_tokens}
        }
        
        logger.log_output({"paragraph_count": len(all_paragraphs)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step6_merge_cross": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"pure_text_script": [], "errors": [{"step": "step6", "error": str(e)}]}
