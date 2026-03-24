"""
common.utils 公共工具包。

说明：集中放置跨模块复用的纯函数工具，避免重复实现。
"""

from .numbers import safe_int, safe_float, to_float
from .time import format_hhmmss
from .video import probe_video_duration_ffprobe, get_video_duration
from .path import safe_filename, sanitize_filename_component
from .text_patch import (
    find_all_occurrences,
    find_contextual_match_positions,
    replace_by_index,
    find_add_insert_positions,
    extract_first_json_dict,
)
from .patch_protocol import (
    normalize_replace_add_patch_item,
    collect_patch_ops,
    pick_full_text_fallback,
    normalize_removal_patch_item,
)

__all__ = [
    "safe_int",
    "safe_float",
    "to_float",
    "format_hhmmss",
    "probe_video_duration_ffprobe",
    "get_video_duration",
    "safe_filename",
    "sanitize_filename_component",
    "find_all_occurrences",
    "find_contextual_match_positions",
    "replace_by_index",
    "find_add_insert_positions",
    "extract_first_json_dict",
    "normalize_replace_add_patch_item",
    "collect_patch_ops",
    "pick_full_text_fallback",
    "normalize_removal_patch_item",
]
