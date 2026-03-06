"""
模块说明：VL 截图预取并发辅助工具。

执行逻辑：
1) 解析并发 worker 数。
2) 将截图请求聚合成预取 chunk。
3) 从共享帧映射构造 worker 任务参数。

核心价值：把预取并发策略从 VLMaterialGenerator 主类剥离。
"""

import os
import math
from typing import Any, Dict, List, Optional


def resolve_max_workers(
    request_count: int,
    *,
    cv_executor: Any,
    screenshot_config: Dict[str, Any],
    hard_cap: int = 6,
) -> int:
    """解析截图优化阶段的并发 worker 数。"""
    if cv_executor is not None:
        injected_workers = getattr(cv_executor, "_max_workers", None)
        if isinstance(injected_workers, int) and injected_workers > 0:
            return max(1, min(injected_workers, request_count))

    max_workers_config = screenshot_config.get("max_workers", "auto")

    if isinstance(max_workers_config, int):
        desired = max_workers_config
    else:
        config_str = str(max_workers_config).strip().lower()
        if config_str == "auto":
            desired = max(1, (os.cpu_count() or 2) - 1)
        else:
            desired = int(config_str)

    return max(1, min(desired, hard_cap, request_count))


def build_screenshot_prefetch_chunks(
    *,
    screenshot_requests: List[Dict[str, Any]],
    max_span_seconds: float,
    max_requests: int,
    time_window: Optional[float] = None,
    time_window_before: Optional[float] = None,
    time_window_after: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """将截图请求按时间聚类为多个可预取 chunk。"""
    if not screenshot_requests:
        return []

    try:
        legacy_window = float(time_window if time_window is not None else 1.0)
    except (TypeError, ValueError):
        legacy_window = 1.0
    if legacy_window < 0.0:
        legacy_window = 0.0
    try:
        resolved_before = float(time_window_before if time_window_before is not None else legacy_window)
    except (TypeError, ValueError):
        resolved_before = legacy_window
    try:
        resolved_after = float(time_window_after if time_window_after is not None else legacy_window)
    except (TypeError, ValueError):
        resolved_after = legacy_window
    if resolved_before < 0.0:
        resolved_before = 0.0
    if resolved_after < 0.0:
        resolved_after = 0.0

    windows = []
    for index, request in enumerate(screenshot_requests):
        original_ts = float(request.get("timestamp_sec", 0) or 0.0)
        default_start = max(0.0, original_ts - resolved_before)
        default_end = original_ts + resolved_after

        # 允许请求级窗口覆盖全局 time_window（用于动作头尾定向截图）。
        raw_window_start = request.get("_window_start_sec")
        raw_window_end = request.get("_window_end_sec")
        if raw_window_start is None and raw_window_end is None:
            search_start = default_start
            search_end = default_end
        else:
            try:
                search_start = float(raw_window_start) if raw_window_start is not None else default_start
            except (TypeError, ValueError):
                search_start = default_start
            try:
                search_end = float(raw_window_end) if raw_window_end is not None else default_end
            except (TypeError, ValueError):
                search_end = default_end
            search_start = max(0.0, search_start)
            if search_end < search_start:
                search_end = search_start

        unit_id = (
            request.get("semantic_unit_id")
            or request.get("unit_id")
            or request.get("screenshot_id")
            or f"req_{index}"
        )
        windows.append(
            {
                "req": request,
                "order_idx": index,
                "unit_id": unit_id,
                "island_index": index,
                "original_ts": original_ts,
                "expanded_start": search_start,
                "expanded_end": search_end,
            }
        )

    windows.sort(key=lambda item: item["original_ts"])

    chunks: List[Dict[str, Any]] = []
    current: List[Dict[str, Any]] = []
    union_start: Optional[float] = None
    union_end: Optional[float] = None

    def flush() -> None:
        nonlocal current, union_start, union_end
        if not current:
            return
        chunks.append(
            {
                "union_start": float(union_start or 0.0),
                "union_end": float(union_end or 0.0),
                "windows": current,
            }
        )
        current = []
        union_start = None
        union_end = None

    for window in windows:
        if not current:
            current = [window]
            union_start = window["expanded_start"]
            union_end = window["expanded_end"]
            continue

        candidate_start = min(union_start, window["expanded_start"])  # type: ignore[arg-type]
        candidate_end = max(union_end, window["expanded_end"])  # type: ignore[arg-type]
        candidate_span = candidate_end - candidate_start

        if (len(current) >= max_requests) or (candidate_span > max_span_seconds):
            flush()
            current = [window]
            union_start = window["expanded_start"]
            union_end = window["expanded_end"]
            continue

        current.append(window)
        union_start = candidate_start
        union_end = candidate_end

    flush()
    return chunks


def build_task_params_from_ts_map(
    *,
    windows: List[Dict[str, Any]],
    ts_to_shm_ref: Dict[float, Any],
    fps: float,
) -> List[Dict[str, Any]]:
    """将窗口与共享内存帧映射转换为 worker 任务参数。"""
    task_params: List[Dict[str, Any]] = []
    for window in windows:
        search_start = float(window["expanded_start"])
        search_end = float(window["expanded_end"])
        shm_frames = {
            ts: ref
            for ts, ref in ts_to_shm_ref.items()
            if search_start <= ts <= search_end
        }
        if not shm_frames:
            task_params.append({"req": window["req"], "skip": True})
            continue

        task_params.append(
            {
                "req": window["req"],
                "skip": False,
                "unit_id": window["unit_id"],
                "island_index": window["island_index"],
                "expanded_start": search_start,
                "expanded_end": search_end,
                "shm_frames": shm_frames,
                "fps": fps,
            }
        )
    return task_params


def resolve_adaptive_prefetch_step(
    *,
    start_frame: int,
    end_frame: int,
    sample_rate: int,
    max_frames_per_chunk: int,
) -> int:
    """
    解析预读采样步长：在基础采样率之上，按单 chunk 帧数上限自适应放大步长。

    设计目标：
    - 保持原有 `sample_rate` 作为“质量优先”的基础步长；
    - 当跨度过大时自动增大步长，避免单 chunk 预读过多帧导致内存峰值上升；
    - 保证最小步长为 1，并保持可预测的整数行为。
    """
    safe_sample_rate = max(1, int(sample_rate))
    safe_max_frames = max(1, int(max_frames_per_chunk))

    if end_frame < start_frame:
        return safe_sample_rate

    span_frames = int(end_frame - start_frame + 1)
    if span_frames <= safe_max_frames:
        return safe_sample_rate

    # 先按帧数上限推导“至少需要多大的步长”，再与基础采样率取 max。
    cap_step = int(math.ceil(span_frames / float(safe_max_frames)))
    return max(safe_sample_rate, cap_step)
