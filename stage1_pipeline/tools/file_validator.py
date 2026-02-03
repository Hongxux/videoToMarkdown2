"""
文件校验工具
用于 Step 1: 原材料确认
"""

import os
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple


def validate_video(video_path: str) -> Tuple[bool, Optional[str]]:
    """
    校验视频文件有效性
    
    Args:
        video_path: 视频文件路径
        
    Returns:
        (is_valid, error_message)
    """
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
        import cv2
        cap = cv2.VideoCapture(str(path))
        if not cap.isOpened():
            return False, f"Cannot open video file: {video_path}"
        
        # 检查帧数
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if frame_count == 0:
            return False, f"Video has no frames: {video_path}"
        
        cap.release()
        return True, None
        
    except Exception as e:
        return False, f"Error validating video: {str(e)}"


def validate_subtitle(subtitle_path: str) -> Tuple[bool, Optional[str]]:
    """
    校验字幕文件有效性
    
    Args:
        subtitle_path: 字幕文件路径
        
    Returns:
        (is_valid, error_message)
    """
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
    count: int = 20
) -> List[Dict[str, any]]:
    """
    读取字幕样本
    
    Args:
        subtitle_path: 字幕文件路径
        count: 读取条数
        
    Returns:
        字幕列表 [{"subtitle_id": "SUB001", "text": "...", "start_sec": 0.0, "end_sec": 1.0}]
    """
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


def _parse_srt(path: Path, count: int) -> List[Dict]:
    """解析 SRT 字幕"""
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
    
    for i, (idx, start, end, text) in enumerate(matches[:count]):
        subtitles.append({
            "subtitle_id": f"SUB{int(idx):03d}",
            "text": text.strip().replace('\n', ' '),
            "start_sec": _time_to_sec(start),
            "end_sec": _time_to_sec(end)
        })
    
    return subtitles


def _parse_vtt(path: Path, count: int) -> List[Dict]:
    """解析 VTT 字幕"""
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
    
    for i, (start, end, text) in enumerate(matches[:count]):
        subtitles.append({
            "subtitle_id": f"SUB{i+1:03d}",
            "text": text.strip().replace('\n', ' '),
            "start_sec": _time_to_sec_vtt(start),
            "end_sec": _time_to_sec_vtt(end)
        })
    
    return subtitles


def _parse_txt(path: Path, count: int) -> List[Dict]:
    """解析纯文本字幕（逐行，支持 Whisper 括号格式）"""
    subtitles = []
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(path, 'r', encoding='gbk') as f:
            lines = f.readlines()
    
    # 匹配 [HH:MM:SS -> HH:MM:SS] 或者 [MM:SS -> MM:SS]
    bracket_pattern = re.compile(r'\[([\d:.]+) -> ([\d:.]+)\]\s*(.*)')
    
    for i, line in enumerate(lines[:count]):
        line = line.strip()
        if not line:
            continue
            
        match = bracket_pattern.match(line)
        if match:
            start_str, end_str, text = match.groups()
            subtitles.append({
                "subtitle_id": f"SUB{len(subtitles)+1:03d}",
                "text": text.strip(),
                "start_sec": _hms_to_sec(start_str),
                "end_sec": _hms_to_sec(end_str)
            })
        else:
            # 兼容无时间戳格式：原有估算逻辑
            subtitles.append({
                "subtitle_id": f"SUB{len(subtitles)+1:03d}",
                "text": line,
                "start_sec": i * 2.0,
                "end_sec": (i + 1) * 2.0
            })
    
    return subtitles


def _hms_to_sec(time_str: str) -> float:
    """通用的 HMS 转秒工具 (支持 00:01:23 或 01:23)"""
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


def _parse_json(path: Path, count: int) -> List[Dict]:
    """解析 JSON 字幕"""
    import json
    
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 支持多种 JSON 格式
    if isinstance(data, list):
        items = data[:count]
    elif isinstance(data, dict):
        items = data.get("subtitles", data.get("segments", data.get("items", [])))[:count]
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
    """SRT 时间转秒 (00:00:00,000)"""
    parts = time_str.replace(',', '.').split(':')
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])


def _time_to_sec_vtt(time_str: str) -> float:
    """VTT 时间转秒 (00:00:00.000)"""
    parts = time_str.split(':')
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])


def extract_video_title(video_path: str) -> str:
    """
    从文件名提取视频标题
    
    Args:
        video_path: 视频文件路径
        
    Returns:
        视频标题
    """
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
