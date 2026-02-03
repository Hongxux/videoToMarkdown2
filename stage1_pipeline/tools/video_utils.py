"""
视频处理工具

提供视频切割、验证等功能，支持详细的日志记录
"""
import subprocess
import os
from pathlib import Path
from typing import Tuple, Optional
import logging

logger = logging.getLogger(__name__)


def cut_video_segment(
    source_video: str,
    start_sec: float,
    end_sec: float,
    output_path: str,
    log_prefix: str = ""
) -> Optional[str]:
    """
    切割视频片段
    
    Args:
        source_video: 源视频路径
        start_sec: 开始时间（秒）
        end_sec: 结束时间（秒）
        output_path: 输出文件路径
        log_prefix: 日志前缀（便于追踪）
    
    Returns:
        成功返回输出路径，失败返回None
    """
    try:
        # 参数验证
        if not os.path.exists(source_video):
            logger.error(f"{log_prefix}Source video not found: {source_video}")
            return None
        
        if end_sec <= start_sec:
            logger.error(f"{log_prefix}Invalid time range: start={start_sec}, end={end_sec}")
            return None
        
        duration = end_sec - start_sec
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_path)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"{log_prefix}Cutting video: {start_sec:.2f}s - {end_sec:.2f}s (duration: {duration:.2f}s)")
        logger.debug(f"{log_prefix}  Source: {source_video}")
        logger.debug(f"{log_prefix}  Output: {output_path}")
        
        # 构建ffmpeg命令
        # -ss: 开始时间
        # -t: 持续时间
        # -c copy: 复制编码（快速，但可能不精确）
        # -avoid_negative_ts make_zero: 避免时间戳问题
        cmd = [
            "ffmpeg",
            "-y",  # 覆盖输出文件
            "-ss", str(start_sec),
            "-i", source_video,
            "-t", str(duration),
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            output_path
        ]
        
        logger.debug(f"{log_prefix}  Command: {' '.join(cmd)}")
        
        # 执行命令
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60  # 60秒超时
        )
        
        if result.returncode != 0:
            logger.error(f"{log_prefix}ffmpeg failed with code {result.returncode}")
            logger.error(f"{log_prefix}  stderr: {result.stderr[:500]}")
            return None
        
        # 验证输出文件
        if not os.path.exists(output_path):
            logger.error(f"{log_prefix}Output file not created: {output_path}")
            return None
        
        file_size = os.path.getsize(output_path)
        if file_size == 0:
            logger.error(f"{log_prefix}Output file is empty: {output_path}")
            return None
        
        logger.info(f"{log_prefix}✓ Video cut successful: {output_path} ({file_size} bytes)")
        return output_path
        
    except subprocess.TimeoutExpired:
        logger.error(f"{log_prefix}ffmpeg timeout (>60s)")
        return None
    except Exception as e:
        logger.error(f"{log_prefix}Unexpected error cutting video: {e}", exc_info=True)
        return None


def validate_video_file(video_path: str) -> Tuple[bool, Optional[dict]]:
    """
    验证视频文件有效性并获取元信息
    
    Returns:
        (is_valid, metadata)
    """
    try:
        if not os.path.exists(video_path):
            return False, None
        
        # 使用ffprobe获取视频信息
        cmd = [
            "ffprobe",
            "-v", "quiet",
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            video_path
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode != 0:
            return False, None
        
        import json
        data = json.loads(result.stdout)
        
        # 提取关键信息
        video_streams = [s for s in data.get("streams", []) if s.get("codec_type") == "video"]
        if not video_streams:
            return False, None
        
        video_stream = video_streams[0]
        format_info = data.get("format", {})
        
        metadata = {
            "duration": float(format_info.get("duration", 0)),
            "width": video_stream.get("width"),
            "height": video_stream.get("height"),
            "codec": video_stream.get("codec_name"),
            "fps": eval(video_stream.get("r_frame_rate", "0/1"))
        }
        
        return True, metadata
        
    except Exception as e:
        logger.error(f"Error validating video: {e}")
        return False, None


def get_video_duration(video_path: str) -> Optional[float]:
    """
    快速获取视频时长
    """
    is_valid, metadata = validate_video_file(video_path)
    if is_valid and metadata:
        return metadata["duration"]
    return None
