"""
模块说明：阶段工具 video_utils 的实现。
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
import os
from pathlib import Path
from typing import Tuple, Optional
import logging
from services.python_grpc.src.common.utils.video import get_video_duration as _get_video_duration

logger = logging.getLogger(__name__)


def cut_video_segment(
    source_video: str,
    start_sec: float,
    end_sec: float,
    output_path: str,
    log_prefix: str = ""
) -> Optional[str]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过子进程调用、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not os.path.exists(source_video)
    - 条件：end_sec <= start_sec
    - 条件：result.returncode != 0
    依据来源（证据链）：
    - 输入参数：end_sec, output_path, source_video, start_sec。
    输入参数：
    - source_video: 函数入参（类型：str）。
    - start_sec: 起止时间/区间边界（类型：float）。
    - end_sec: 起止时间/区间边界（类型：float）。
    - output_path: 文件路径（类型：str）。
    - log_prefix: 函数入参（类型：str）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
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
    执行逻辑：
    1) 整理待校验数据。
    2) 按规则逐项校验并返回结果。
    实现方式：通过JSON 解析/序列化、子进程调用、文件系统读写实现。
    核心价值：提前发现数据/状态问题，降低运行风险。
    决策逻辑：
    - 条件：not os.path.exists(video_path)
    - 条件：result.returncode != 0
    - 条件：not video_streams
    依据来源（证据链）：
    - 输入参数：video_path。
    输入参数：
    - video_path: 文件路径（类型：str）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
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
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：is_valid and metadata
    依据来源（证据链）：
    输入参数：
    - video_path: 文件路径（类型：str）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    return _get_video_duration(video_path, default=None, use_cv2_fallback=True)
