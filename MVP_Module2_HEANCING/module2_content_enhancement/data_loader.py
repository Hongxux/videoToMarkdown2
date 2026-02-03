"""
Data Loader - Week 1 Day 1-2

Loads input data from JSON files:
1. corrected_subtitles from step2 output
2. merged_segments (pure_text_script) from step6 output

References existing implementation for correct JSON structure.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any

from .data_structures import (
    CorrectedSubtitle,
    CrossSentenceMergedSegment,
    Module2Input
)

logger = logging.getLogger(__name__)


def load_corrected_subtitles(json_path: str) -> List[CorrectedSubtitle]:
    """
    从JSON文件解析corrected_subtitles
    
    参考: stage1_pipeline/nodes/phase2_preprocessing.py step2_node输出格式
    
    Expected JSON structure:
    {
        "corrected_subtitles": [
            {
                "subtitle_id": "SUB001",
                "corrected_text": "纠错后文本",
                "start_sec": 10.5,
                "end_sec": 12.3,
                "corrections": [...]
            }
        ]
    }
    
    Args:
        json_path: Path to step2 output JSON file
    
    Returns:
        List of CorrectedSubtitle objects
    
    Raises:
        FileNotFoundError: If JSON file doesn't exist
        ValueError: If JSON structure is invalid
    """
    if not json_path:
        logger.debug("load_corrected_subtitles: empty path provided, returning empty list")
        return []
        
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"corrected_subtitles JSON not found: {json_path}")
    
    logger.info(f"Loading corrected_subtitles from {json_path}")
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if "corrected_subtitles" not in data:
            # Support both flat and nested 'output' structure
            if "output" in data and "corrected_subtitles" in data["output"]:
                data = data["output"]
            else:
                raise ValueError("JSON must contain 'corrected_subtitles' key")
        
        subtitles = []
        for item in data["corrected_subtitles"]:
            # Validate required fields
            required_fields = ["subtitle_id", "start_sec", "end_sec"]
            missing = [f for f in required_fields if f not in item]
            if missing:
                raise ValueError(f"Missing required fields: {missing}")
            
            subtitle = CorrectedSubtitle(
                subtitle_id=item["subtitle_id"],
                text=item.get("corrected_text", item.get("text", "")),  # Support both field names
                start_sec=float(item["start_sec"]),
                end_sec=float(item["end_sec"]),
                corrections=item.get("corrections", [])
            )
            subtitles.append(subtitle)
        
        logger.info(f"Loaded {len(subtitles)} corrected subtitles")
        
        # Validate time ordering
        for i, sub in enumerate(subtitles):
            if sub.start_sec > sub.end_sec:
                logger.warning(f"Invalid time range in {sub.subtitle_id}: {sub.start_sec} > {sub.end_sec}")
            if sub.start_sec < 0:
                raise ValueError(f"Negative timestamp in {sub.subtitle_id}")
        
        return subtitles
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")
    except Exception as e:
        logger.error(f"Error loading corrected_subtitles: {e}")
        raise


def load_merged_segments(json_path: str) -> List[CrossSentenceMergedSegment]:
    """
    从JSON文件解析merged_segments (步骤6的pure_text_script输出)
    
    参考: stage1_pipeline/nodes/phase2_preprocessing.py step6_node输出格式
    
    Expected JSON structure:
    {
        "pure_text_script": [
            {
                "paragraph_id": "P001",
                "text": "完整的语义段落",
                "source_sentence_ids": ["S001", "S002"],
                "merge_type": "同义转述"
            }
        ]
    }
    
    Args:
        json_path: Path to step6 output JSON file
    
    Returns:
        List of CrossSentenceMergedSegment objects
    """
    if not json_path:
        logger.debug("load_merged_segments: empty path provided, returning empty list")
        return []
        
    path = Path(json_path)
    if not path.exists():
        raise FileNotFoundError(f"merged_segments JSON not found: {json_path}")
    
    logger.info(f"Loading merged_segments from {json_path}")
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if "pure_text_script" not in data:
            # Support both flat and nested 'output' structure
            if "output" in data and "pure_text_script" in data["output"]:
                data = data["output"]
            else:
                raise ValueError("JSON must contain 'pure_text_script' key")
        
        segments = []
        for item in data["pure_text_script"]:
            # Validate required fields
            required_fields = ["paragraph_id", "text", "source_sentence_ids"]
            missing = [f for f in required_fields if f not in item]
            if missing:
                raise ValueError(f"Missing required fields: {missing}")
            
            segment = CrossSentenceMergedSegment(
                segment_id=item["paragraph_id"],
                full_text=item["text"],
                source_sentence_ids=item["source_sentence_ids"],
                merge_type=item.get("merge_type", "无合并")
            )
            segments.append(segment)
        
        logger.info(f"Loaded {len(segments)} merged segments")
        
        # Validate
        for seg in segments:
            if not seg.full_text.strip():
                logger.warning(f"Empty text in segment {seg.segment_id}")
            if not seg.source_sentence_ids:
                logger.warning(f"No source sentence IDs in {seg.segment_id}")
        
        return segments
        
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format: {e}")
    except Exception as e:
        logger.error(f"Error loading merged_segments: {e}")
        raise


def _get_video_duration(video_path: str) -> float:
    """获取视频物理时长(秒)"""
    import cv2
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return 0.0
    fps = cap.get(cv2.CAP_PROP_FPS)
    frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
    duration = frame_count / fps if fps > 0 else 0.0
    cap.release()
    return duration

def sanitize_module_input(module_input: Module2Input) -> Module2Input:
    """
    清理 ASR 时间戳漂移 (Hallucination Sanitizer)
    """
    duration = _get_video_duration(module_input.video_path)
    if duration <= 0:
        return module_input
        
    logger.info(f"Sanitizing timeline against video duration: {duration:.2f}s")
    
    # 1. 字幕清理 (物理锚定)
    orig_sub_count = len(module_input.corrected_subtitles)
    module_input.corrected_subtitles = [
        s for s in module_input.corrected_subtitles 
        if s.start_sec < duration
    ]
    for s in module_input.corrected_subtitles:
        if s.end_sec > duration:
            s.end_sec = duration
            
    pruned_subs = orig_sub_count - len(module_input.corrected_subtitles)
    if pruned_subs > 0:
        logger.warning(f"Pruned {pruned_subs} ghost subtitles (hallucinated after EOF)")
        
    return module_input

def load_sentence_timestamps(json_path: str) -> Dict[str, Dict[str, float]]:
    """从 local_storage 加载预计算的句子时间戳"""
    path = Path(json_path)
    if not path.exists():
        logger.warning(f"sentence_timestamps JSON not found at {json_path}. Falling back to search.")
        return {}
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading sentence_timestamps: {e}")
        return {}


def create_module2_input(
    corrected_subtitles_path: str,
    merged_segments_path: str,
    video_path: str,
    output_dir: str,
    domain: str,
    main_topic: str = "",
    sentence_timestamps_path: str = None
) -> Module2Input:
    """
    Create complete Module2Input from JSON files (with Duration Sanitization)
    """
    logger.info("Creating Module2Input...")
    
    # Load data
    corrected_subtitles = load_corrected_subtitles(corrected_subtitles_path)
    merged_segments = load_merged_segments(merged_segments_path)
    
    # 💥 全新数据源: 加载预计算的时间戳 (解决偏移核心)
    sentence_timestamps = {}
    if sentence_timestamps_path:
        sentence_timestamps = load_sentence_timestamps(sentence_timestamps_path)
    
    # Validate paths
    if not Path(video_path).exists():
        raise FileNotFoundError(f"Video file not found: {video_path}")
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    module_input = Module2Input(
        corrected_subtitles=corrected_subtitles,
        merged_segments=merged_segments,
        video_path=video_path,
        output_dir=output_dir,
        domain=domain,
        main_topic=main_topic,
        sentence_timestamps=sentence_timestamps
    )
    
    # 💥 核心修复: 执行物理时长锚定，消除 ASR 漂移幻觉
    module_input = sanitize_module_input(module_input)
    
    return module_input


def validate_input_consistency(module_input: Module2Input) -> Dict[str, Any]:
    """
    Validate consistency between corrected_subtitles and merged_segments
    
    Returns:
        Validation report dict
    """
    report = {
        "valid": True,
        "warnings": [],
        "errors": []
    }
    
    # Check 1: Time range coverage
    if module_input.corrected_subtitles:
        sub_start = min(s.start_sec for s in module_input.corrected_subtitles)
        sub_end = max(s.end_sec for s in module_input.corrected_subtitles)
        
        logging.info(f"Subtitle time range: {sub_start:.2f}s - {sub_end:.2f}s")
    
    # Check 2: Segment-to-subtitle mapping
    all_subtitle_ids = {s.subtitle_id for s in module_input.corrected_subtitles}
    
    referenced_ids = set()
    for seg in module_input.merged_segments:
        referenced_ids.update(seg.source_sentence_ids)
    
    # Note: source_sentence_ids refer to sentence IDs (S001), not subtitle IDs (SUB001)
    # This is expected as step6 uses output from step3-5
    
    report["subtitle_count"] = len(module_input.corrected_subtitles)
    report["segment_count"] = len(module_input.merged_segments)
    
    logger.info(f"Validation complete: {report}")
    
    return report
