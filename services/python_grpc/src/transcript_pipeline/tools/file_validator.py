"""
模块说明：阶段工具 file_validator 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import os
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any

import cv2

from services.python_grpc.src.common.utils.opencv_decode import (
    probe_capture_readable,
    probe_primary_video_codec,
)


def _apply_count_limit(items: List[Any], count: Optional[int]) -> List[Any]:
    """根据 count 对列表限长；count 为空或 <=0 时返回全部。"""
    if count is None or count <= 0:
        return items
    return items[:count]


def _is_av1_codec(codec_name: Optional[str]) -> bool:
    """判断 ffprobe 编码结果是否属于 AV1/AV01 变体。"""
    normalized = str(codec_name or "").strip().lower()
    return normalized in {"av1", "av01"} or normalized.startswith("av1.")


def validate_video(video_path: str) -> Tuple[bool, Optional[str]]:
    """
    执行逻辑：
    1) 整理待校验数据。
    2) 按规则逐项校验并返回结果。
    实现方式：通过OpenCV 图像处理、文件系统读写实现。
    核心价值：提前发现数据/状态问题，降低运行风险。
    决策逻辑：
    - 条件：not path.exists()
    - 条件：not path.is_file()
    - 条件：path.stat().st_size == 0
    依据来源（证据链）：
    输入参数：
    - video_path: 文件路径（类型：str）。
    输出参数：
    - 多值结果元组（各元素含义见实现）。"""
    path = Path(video_path)
    
    if not path.exists():
        return False, f"Video file not found: {video_path}"
    
    if not path.is_file():
        return False, f"Not a file: {video_path}"
    
    # 检查文件大小
    if path.stat().st_size == 0:
        return False, f"Video file is empty: {video_path}"
    
    # 检查扩展名
    valid_extensions = {'.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm'}
    if path.suffix.lower() not in valid_extensions:
        return False, f"Invalid video extension: {path.suffix}"
    
    # 尝试用 OpenCV 打开
    try:
        codec_name = probe_primary_video_codec(str(path))

        # step1 入口只做轻量校验：AV1/AV01 先短路放行，避免触发 OpenCV 底层噪声报错。
        if _is_av1_codec(codec_name):
            return True, None

        cap = cv2.VideoCapture(str(path))
        try:
            if probe_capture_readable(cap):
                return True, None
        finally:
            cap.release()

        codec_suffix = f" (codec={codec_name})" if codec_name else ""
        return False, f"Cannot decode video file{codec_suffix}: {video_path}"

    except Exception as e:
        return False, f"Error validating video: {str(e)}"


def validate_subtitle(subtitle_path: str) -> Tuple[bool, Optional[str]]:
    """
    执行逻辑：
    1) 整理待校验数据。
    2) 按规则逐项校验并返回结果。
    实现方式：通过JSON 解析/序列化、文件系统读写实现。
    核心价值：提前发现数据/状态问题，降低运行风险。
    决策逻辑：
    - 条件：not path.exists()
    - 条件：not path.is_file()
    - 条件：path.stat().st_size == 0
    依据来源（证据链）：
    输入参数：
    - subtitle_path: 文件路径（类型：str）。
    输出参数：
    - 多值结果元组（各元素含义见实现）。"""
    path = Path(subtitle_path)
    
    if not path.exists():
        return False, f"Subtitle file not found: {subtitle_path}"
    
    if not path.is_file():
        return False, f"Not a file: {subtitle_path}"
    
    # 检查文件大小
    if path.stat().st_size == 0:
        return False, f"Subtitle file is empty: {subtitle_path}"
    
    # 检查扩展名
    valid_extensions = {'.srt', '.vtt', '.ass', '.ssa', '.txt', '.json'}
    if path.suffix.lower() not in valid_extensions:
        return False, f"Invalid subtitle extension: {path.suffix}"
    
    # 尝试读取
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
            if len(content.strip()) == 0:
                return False, f"Subtitle file is empty: {subtitle_path}"
        return True, None
    except UnicodeDecodeError:
        # 尝试其他编码
        try:
            with open(path, 'r', encoding='gbk') as f:
                content = f.read()
            return True, None
        except Exception as e:
            return False, f"Cannot read subtitle file: {str(e)}"
    except Exception as e:
        return False, f"Error validating subtitle: {str(e)}"


def read_subtitle_sample(
    subtitle_path: str, 
    count: Optional[int] = 20
) -> List[Dict[str, any]]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：path.suffix.lower() == '.srt'
    - 条件：path.suffix.lower() == '.vtt'
    - 条件：path.suffix.lower() == '.txt'
    依据来源（证据链）：
    输入参数：
    - subtitle_path: 文件路径（类型：str）。
    - count: 函数入参（类型：int）。
    输出参数：
    - Dict[str, any] 列表（与输入或处理结果一一对应）。"""
    path = Path(subtitle_path)
    
    # 根据扩展名选择解析方式
    if path.suffix.lower() == '.srt':
        return _parse_srt(path, count)
    elif path.suffix.lower() == '.vtt':
        return _parse_vtt(path, count)
    elif path.suffix.lower() == '.txt':
        return _parse_txt(path, count)
    elif path.suffix.lower() == '.json':
        return _parse_json(path, count)
    else:
        # 默认按纯文本处理
        return _parse_txt(path, count)


def _parse_srt(path: Path, count: Optional[int]) -> List[Dict]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - path: 文件路径（类型：Path）。
    - count: 函数入参（类型：int）。
    输出参数：
    - Dict 列表（与输入或处理结果一一对应）。"""
    subtitles = []
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        with open(path, 'r', encoding='gbk') as f:
            content = f.read()
    
    # SRT 格式: 序号 + 时间码 + 文本 + 空行
    pattern = r'(\d+)\n(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})\n(.+?)(?=\n\n|\Z)'
    matches = re.findall(pattern, content, re.DOTALL)
    
    for i, (idx, start, end, text) in enumerate(_apply_count_limit(matches, count)):
        subtitles.append({
            "subtitle_id": f"SUB{int(idx):03d}",
            "text": text.strip().replace('\n', ' '),
            "start_sec": _time_to_sec(start),
            "end_sec": _time_to_sec(end)
        })
    
    return subtitles


def _parse_vtt(path: Path, count: Optional[int]) -> List[Dict]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - path: 文件路径（类型：Path）。
    - count: 函数入参（类型：int）。
    输出参数：
    - Dict 列表（与输入或处理结果一一对应）。"""
    subtitles = []
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        with open(path, 'r', encoding='gbk') as f:
            content = f.read()
    
    # VTT 格式: 时间码 + 文本
    pattern = r'(\d{2}:\d{2}:\d{2}\.\d{3}) --> (\d{2}:\d{2}:\d{2}\.\d{3})\n(.+?)(?=\n\n|\Z)'
    matches = re.findall(pattern, content, re.DOTALL)
    
    for i, (start, end, text) in enumerate(_apply_count_limit(matches, count)):
        subtitles.append({
            "subtitle_id": f"SUB{i+1:03d}",
            "text": text.strip().replace('\n', ' '),
            "start_sec": _time_to_sec_vtt(start),
            "end_sec": _time_to_sec_vtt(end)
        })
    
    return subtitles


def _parse_txt(path: Path, count: Optional[int]) -> List[Dict]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not line
    - 条件：match
    依据来源（证据链）：
    输入参数：
    - path: 文件路径（类型：Path）。
    - count: 函数入参（类型：int）。
    输出参数：
    - Dict 列表（与输入或处理结果一一对应）。"""
    subtitles = []
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(path, 'r', encoding='gbk') as f:
            lines = f.readlines()
    
    # 匹配 [HH:MM:SS -> HH:MM:SS] 或者 [MM:SS -> MM:SS]
    range_pattern = re.compile(r'^\[([\d:.]+) -> ([\d:.]+)\]\s*(.*)$')
    # 匹配 [HH:MM:SS] 文本 或 [MM:SS] 文本
    single_ts_pattern = re.compile(r'^\[([\d:.]+)\]\s*(.*)$')

    parsed_rows: List[Dict] = []
    for line_index, raw_line in enumerate(_apply_count_limit(lines, count)):
        line = raw_line.strip()
        if not line:
            continue

        range_match = range_pattern.match(line)
        if range_match:
            start_str, end_str, text = range_match.groups()
            start_sec = _hms_to_sec(start_str)
            end_sec = _hms_to_sec(end_str)
            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec
            parsed_rows.append(
                {
                    "mode": "range",
                    "line_index": line_index,
                    "text": text.strip(),
                    "start_sec": start_sec,
                    "end_sec": end_sec,
                }
            )
            continue

        single_match = single_ts_pattern.match(line)
        if single_match:
            start_str, text = single_match.groups()
            parsed_rows.append(
                {
                    "mode": "single",
                    "line_index": line_index,
                    "text": text.strip(),
                    "start_sec": _hms_to_sec(start_str),
                }
            )
            continue

        # 兼容无时间戳格式：保留原有估算逻辑
        parsed_rows.append(
            {
                "mode": "plain",
                "line_index": line_index,
                "text": line,
            }
        )

    for row_index, row in enumerate(parsed_rows):
        mode = row.get("mode")
        if mode == "range":
            start_sec = row["start_sec"]
            end_sec = row["end_sec"]
        elif mode == "single":
            start_sec = row["start_sec"]
            end_sec = start_sec + 2.0

            # 如果后续存在更大的时间戳，则以它作为当前字幕结束时间
            for next_row in parsed_rows[row_index + 1:]:
                next_mode = next_row.get("mode")
                if next_mode == "range":
                    next_start = float(next_row.get("start_sec", start_sec))
                elif next_mode == "single":
                    next_start = float(next_row.get("start_sec", start_sec))
                else:
                    continue

                if next_start > start_sec:
                    end_sec = next_start
                    break
        else:
            source_index = int(row.get("line_index", row_index))
            start_sec = source_index * 2.0
            end_sec = (source_index + 1) * 2.0

        subtitles.append(
            {
                "subtitle_id": f"SUB{len(subtitles)+1:03d}",
                "text": str(row.get("text", "")).strip(),
                "start_sec": start_sec,
                "end_sec": end_sec,
            }
        )
    
    return subtitles


def _hms_to_sec(time_str: str) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：len(parts) == 3
    - 条件：len(parts) == 2
    依据来源（证据链）：
    输入参数：
    - time_str: 函数入参（类型：str）。
    输出参数：
    - 数值型计算结果。"""
    parts = time_str.split(':')
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
        else:
            return float(time_str)
    except:
        return 0.0


def _parse_json(path: Path, count: Optional[int]) -> List[Dict]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：isinstance(data, list)
    - 条件：isinstance(data, dict)
    - 条件：isinstance(item, dict)
    依据来源（证据链）：
    输入参数：
    - path: 文件路径（类型：Path）。
    - count: 函数入参（类型：int）。
    输出参数：
    - Dict 列表（与输入或处理结果一一对应）。"""
    import json
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 支持多种 JSON 格式
    if isinstance(data, list):
        items = _apply_count_limit(data, count)
    elif isinstance(data, dict):
        raw_items = data.get("subtitles", data.get("segments", data.get("items", [])))
        items = _apply_count_limit(raw_items, count)
    else:
        return []
    
    subtitles = []
    for i, item in enumerate(items):
        if isinstance(item, dict):
            subtitles.append({
                "subtitle_id": item.get("id", item.get("subtitle_id", f"SUB{i+1:03d}")),
                "text": item.get("text", item.get("content", "")),
                "start_sec": float(item.get("start", item.get("start_sec", i * 2.0))),
                "end_sec": float(item.get("end", item.get("end_sec", (i + 1) * 2.0)))
            })
        elif isinstance(item, str):
            subtitles.append({
                "subtitle_id": f"SUB{i+1:03d}",
                "text": item,
                "start_sec": i * 2.0,
                "end_sec": (i + 1) * 2.0
            })
    
    return subtitles


def _time_to_sec(time_str: str) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - time_str: 函数入参（类型：str）。
    输出参数：
    - 数值型计算结果。"""
    parts = time_str.replace(',', '.').split(':')
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])


def _time_to_sec_vtt(time_str: str) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - time_str: 函数入参（类型：str）。
    输出参数：
    - 数值型计算结果。"""
    parts = time_str.split(':')
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])


def extract_video_title(video_path: str) -> str:
    """
    执行逻辑：
    1) 扫描输入内容。
    2) 过滤并提取目标子集。
    实现方式：通过文件系统读写实现。
    核心价值：聚焦关键信息，减少后续处理成本。
    输入参数：
    - video_path: 文件路径（类型：str）。
    输出参数：
    - 字符串结果。"""
    path = Path(video_path)
    stem = path.stem
    
    # 移除常见的前缀后缀
    patterns_to_remove = [
        r'^\d+[-_\.]',  # 开头的序号
        r'[-_]\d{8,}$',  # 结尾的日期
        r'[-_]?[hH][dD]$',  # HD标记
        r'[-_]?\d{3,4}[pP]$',  # 分辨率
        r'[-_]?[v]\d+$',  # 版本号
    ]
    
    title = stem
    for pattern in patterns_to_remove:
        title = re.sub(pattern, '', title)
    
    # 将下划线和连字符转为空格
    title = re.sub(r'[-_]+', ' ', title)
    
    return title.strip() or stem
