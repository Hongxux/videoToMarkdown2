from __future__ import annotations

import csv
import json
import statistics
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import psutil


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_int_list(raw: str, field_name: str) -> List[int]:
    values: List[int] = []
    for item in (raw or "").split(","):
        token = item.strip()
        if not token:
            continue
        try:
            value = int(token)
        except ValueError as exc:
            raise ValueError(f"{field_name} 包含非法整数: {token}") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0: {token}")
        values.append(value)
    dedup = sorted(set(values))
    if not dedup:
        raise ValueError(f"{field_name} 至少需要一个正整数")
    return dedup


def parse_float_list(raw: str, field_name: str) -> List[float]:
    values: List[float] = []
    for item in (raw or "").split(","):
        token = item.strip()
        if not token:
            continue
        try:
            value = float(token)
        except ValueError as exc:
            raise ValueError(f"{field_name} 包含非法浮点数: {token}") from exc
        if value <= 0:
            raise ValueError(f"{field_name} 必须大于 0: {token}")
        values.append(value)
    dedup = sorted(set(values))
    if not dedup:
        raise ValueError(f"{field_name} 至少需要一个正浮点数")
    return dedup


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return float(sorted_values[f])
    d0 = sorted_values[f] * (c - k)
    d1 = sorted_values[c] * (k - f)
    return float(d0 + d1)


def jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [jsonable(v) for v in value]
    return str(value)


def ensure_benchmark_dirs(output_root: str, task_name: str) -> Tuple[Path, Path, Path]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = Path(output_root) / f"{task_name}_{ts}"
    raw_dir = output_dir / "raw"
    charts_dir = output_dir / "charts"
    raw_dir.mkdir(parents=True, exist_ok=True)
    charts_dir.mkdir(parents=True, exist_ok=True)
    return output_dir, raw_dir, charts_dir


def write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(jsonable(payload), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


class SystemSampler:
    def __init__(self, interval_sec: float = 0.5) -> None:
        self.interval_sec = max(0.2, float(interval_sec))
        self.samples: List[Dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.samples = []
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> List[Dict[str, Any]]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.interval_sec * 3)
        return self.samples

    def _run(self) -> None:
        psutil.cpu_percent(interval=None)
        while not self._stop.is_set():
            cpu_percent = float(psutil.cpu_percent(interval=self.interval_sec))
            vm = psutil.virtual_memory()
            self.samples.append(
                {
                    "ts": now_iso(),
                    "cpu_percent": cpu_percent,
                    "memory_percent": float(vm.percent),
                    "memory_used_gb": float((vm.total - vm.available) / (1024**3)),
                    "memory_available_gb": float(vm.available / (1024**3)),
                }
            )


def summarize_system_samples(samples: List[Dict[str, Any]]) -> Dict[str, float]:
    if not samples:
        return {
            "cpu_mean_percent": 0.0,
            "cpu_p95_percent": 0.0,
            "memory_mean_percent": 0.0,
            "memory_p95_percent": 0.0,
            "memory_used_gb_mean": 0.0,
            "memory_available_gb_min": 0.0,
        }

    cpu_values = [float(item["cpu_percent"]) for item in samples]
    mem_values = [float(item["memory_percent"]) for item in samples]
    mem_used_values = [float(item["memory_used_gb"]) for item in samples]
    mem_available_values = [float(item["memory_available_gb"]) for item in samples]
    return {
        "cpu_mean_percent": float(statistics.fmean(cpu_values)),
        "cpu_p95_percent": percentile(cpu_values, 95),
        "memory_mean_percent": float(statistics.fmean(mem_values)),
        "memory_p95_percent": percentile(mem_values, 95),
        "memory_used_gb_mean": float(statistics.fmean(mem_used_values)),
        "memory_available_gb_min": float(min(mem_available_values)),
    }

