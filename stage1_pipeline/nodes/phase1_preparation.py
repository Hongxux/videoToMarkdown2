"""
Phase 1: 前期准备
Step 1: 原材料确认与主题识别
"""

import time
from typing import Dict, Any

from ..state import PipelineState
from ..tools.file_validator import (
    validate_video, 
    validate_subtitle, 
    read_subtitle_sample,
    extract_video_title
)
from ..llm.client import create_llm_client
from ..monitoring.logger import get_logger


# Step 1 主题推断 Prompt
TOPIC_INFERENCE_PROMPT = """请根据以下视频字幕样本，推断视频的领域和主题：

【字幕样本】
{sample_subtitles}

【视频标题（如有）】
{video_title}

【输出要求】
1. domain：视频所属领域，如"计算机科学"、"数学"、"物理"、"经济学"、"哲学"等
2. main_topic：核心主题，20字以内，概括视频讲解的核心内容

【输出格式】
{{"domain": "string", "main_topic": "string"}}"""


async def step1_node(state: PipelineState) -> Dict[str, Any]:
    """
    步骤1：原材料确认与主题识别
    
    类型：Tool + LLM
    
    核心动作：
    1. 校验视频和字幕文件有效性
    2. 读取字幕样本，推断视频领域和主题
    
    输入：video_path, subtitle_path
    输出：is_valid, domain, main_topic, video_title
    """
    logger = get_logger("step1_validate", state.get("output_dir", "output/logs"))
    logger.start()
    
    logger.log_input({
        "video_path": state["video_path"],
        "subtitle_path": state["subtitle_path"]
    })
    
    errors = []
    
    try:
        # 1. [Tool] 校验视频文件
        logger.info("Validating video file...")
        video_valid, video_error = validate_video(state["video_path"])
        logger.log_tool_call("validate_video", {"path": state["video_path"]}, video_valid)
        
        if not video_valid:
            errors.append({"step": "step1", "type": "video_validation", "error": video_error})
            logger.log_warning(f"Video validation failed: {video_error}")
        
        # 2. [Tool] 校验字幕文件
        logger.info("Validating subtitle file...")
        subtitle_valid, subtitle_error = validate_subtitle(state["subtitle_path"])
        logger.log_tool_call("validate_subtitle", {"path": state["subtitle_path"]}, subtitle_valid)
        
        if not subtitle_valid:
            errors.append({"step": "step1", "type": "subtitle_validation", "error": subtitle_error})
            logger.log_warning(f"Subtitle validation failed: {subtitle_error}")
        
        # 如果文件校验失败，返回错误
        if not video_valid or not subtitle_valid:
            output = {
                "is_valid": False,
                "domain": "",
                "main_topic": "",
                "video_title": "",
                "errors": errors,
                "current_step": "step1_validate",
                "current_step_status": "failed"
            }
            logger.log_output(output, summary_only=True)
            logger.end(success=False)
            return output
        
        # 3. [Tool] 提取视频标题
        video_title = extract_video_title(state["video_path"])
        logger.info(f"Extracted video title: {video_title}")
        
        # 4. [Tool] 读取字幕样本
        logger.info("Reading subtitle sample...")
        sample_subtitles = read_subtitle_sample(state["subtitle_path"], count=20)
        logger.log_tool_call(
            "read_subtitle_sample", 
            {"path": state["subtitle_path"], "count": 20}, 
            f"{len(sample_subtitles)} subtitles"
        )
        
        # 格式化字幕样本
        sample_text = "\n".join([
            f"[{s['start_sec']:.1f}s] {s['text']}" 
            for s in sample_subtitles
        ])
        
        # 5. [LLM] 推断领域和主题
        logger.info("Inferring domain and topic with LLM...")
        llm = create_llm_client(purpose="topic")
        
        prompt = TOPIC_INFERENCE_PROMPT.format(
            sample_subtitles=sample_text,
            video_title=video_title or "(无)"
        )
        
        result, response = await llm.complete_json(prompt)
        
        logger.log_llm_call(
            prompt=prompt,
            response=response.content,
            prompt_tokens=response.prompt_tokens,
            completion_tokens=response.completion_tokens,
            model=response.model,
            latency_ms=response.latency_ms
        )
        
        domain = result.get("domain", "未知")
        main_topic = result.get("main_topic", "")
        
        logger.info(f"Inferred domain: {domain}, topic: {main_topic}")
        
        # 6. 构建输出
        output = {
            "is_valid": True,
            "domain": domain,
            "main_topic": main_topic,
            "video_title": video_title,
            "current_step": "step1_validate",
            "current_step_status": "completed",
            "execution_trace": [{
                "step_name": "step1_validate",
                "status": "success",
                "duration_ms": 0  # Will be filled by logger.end()
            }],
            "llm_calls": [{
                "step_name": "step1_validate",
                "model": response.model,
                "prompt_tokens": response.prompt_tokens,
                "completion_tokens": response.completion_tokens,
                "total_tokens": response.total_tokens,
                "latency_ms": response.latency_ms
            }],
            "token_usage": {
                "step1_validate": response.total_tokens
            }
        }
        
        logger.log_output(output, summary_only=True)
        timing = logger.end(success=True)
        output["step_timings"] = {"step1_validate": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e, {"video_path": state["video_path"]})
        logger.end(success=False)
        
        return {
            "is_valid": False,
            "domain": "",
            "main_topic": "",
            "video_title": "",
            "errors": [{"step": "step1", "type": "exception", "error": str(e)}],
            "current_step": "step1_validate",
            "current_step_status": "error"
        }
