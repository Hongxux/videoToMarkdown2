"""
模块说明：VL 截图预取并发辅助工具。

执行逻辑：
1) 解析并发 worker 数。
2) 将截图请求聚合成预取 chunk。
3) 从共享帧映射构造 worker 任务参数。

核心价值：把预取并发策略从 VLMaterialGenerator 主类剥离。
"""

import os
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
    time_window: float,
    max_span_seconds: float,
    max_requests: int,
) -> List[Dict[str, Any]]:
    """将截图请求按时间聚类为多个可预取 chunk。"""
    if not screenshot_requests:
        return []

    windows = []
    for index, request in enumerate(screenshot_requests):
        original_ts = float(request.get("timestamp_sec", 0) or 0.0)
        search_start = max(0.0, original_ts - time_window)
        search_end = original_ts + time_window
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

