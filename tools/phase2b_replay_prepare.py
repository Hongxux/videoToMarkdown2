"""
基于现有存储目录生成 Phase2B 重放输入：
1) 从 semantic_units_phase2a.json 读取单元；
2) 从 vl_analysis_cache.json 提取 screenshot_requests；
3) 按 semantic_unit_id 回填到 material_requests.screenshot_requests；
4) 输出 semantic_units_phase2a.replay.json。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load_json(path: Path) -> Any:
    with open(path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def _dump_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)


def _collect_screenshot_requests(vl_cache: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    by_unit: Dict[str, List[Dict[str, Any]]] = {}

    def _normalize_request(item: Dict[str, Any], unit_id: str, shot_id: str) -> Dict[str, Any]:
        """标准化截图请求字段，避免写入非 Phase2B dataclass 字段。"""
        label = str(item.get("label") or item.get("type") or "stable").strip() or "stable"
        try:
            ts = float(item.get("timestamp_sec", 0.0) or 0.0)
        except Exception:
            ts = 0.0
        return {
            "screenshot_id": shot_id,
            "timestamp_sec": ts,
            "label": label,
            "semantic_unit_id": unit_id,
        }

    def _append(item: Dict[str, Any]) -> None:
        unit_id = str(item.get("semantic_unit_id") or "").strip()
        shot_id = str(item.get("screenshot_id") or "").strip()
        if not unit_id or not shot_id:
            return
        if "/" not in shot_id:
            shot_id = f"{unit_id}/{shot_id}"
        bucket = by_unit.setdefault(unit_id, [])
        cloned = _normalize_request(item, unit_id, shot_id)
        bucket.append(cloned)

    aggregated = vl_cache.get("aggregated_screenshots", [])
    if isinstance(aggregated, list):
        for item in aggregated:
            if isinstance(item, dict):
                _append(item)

    analysis_results = vl_cache.get("analysis_results", [])
    if isinstance(analysis_results, list):
        for result in analysis_results:
            if not isinstance(result, dict):
                continue
            requests = result.get("screenshot_requests", [])
            if not isinstance(requests, list):
                continue
            for item in requests:
                if isinstance(item, dict):
                    _append(item)

    deduped: Dict[str, List[Dict[str, Any]]] = {}
    for unit_id, items in by_unit.items():
        seen: set[Tuple[str, float]] = set()
        stable: List[Dict[str, Any]] = []
        for item in items:
            shot_id = str(item.get("screenshot_id") or "").strip()
            try:
                ts = float(item.get("timestamp_sec", 0.0) or 0.0)
            except Exception:
                ts = 0.0
            key = (shot_id, ts)
            if key in seen:
                continue
            seen.add(key)
            stable.append(item)
        deduped[unit_id] = stable

    return deduped


def build_replay_input(storage_dir: Path) -> Path:
    source_units = storage_dir / "semantic_units_phase2a.json"
    vl_cache_path = storage_dir / "vl_analysis_cache.json"
    output_path = storage_dir / "intermediates" / "semantic_units_phase2a.replay.json"

    units = _load_json(source_units)
    if not isinstance(units, list):
        raise ValueError(f"semantic_units_phase2a.json format invalid: {source_units}")

    vl_cache = _load_json(vl_cache_path)
    if not isinstance(vl_cache, dict):
        raise ValueError(f"vl_analysis_cache.json format invalid: {vl_cache_path}")

    by_unit = _collect_screenshot_requests(vl_cache)

    updated = 0
    for unit in units:
        if not isinstance(unit, dict):
            continue
        unit_id = str(unit.get("unit_id") or "").strip()
        if not unit_id:
            continue

        requests = by_unit.get(unit_id, [])
        material_requests = unit.get("material_requests")
        if not isinstance(material_requests, dict):
            material_requests = {}
            unit["material_requests"] = material_requests

        existing = material_requests.get("screenshot_requests", [])
        if not isinstance(existing, list):
            existing = []

        merged = list(existing)
        seen: set[Tuple[str, float]] = set()
        for item in merged:
            if not isinstance(item, dict):
                continue
            shot_id = str(item.get("screenshot_id") or "").strip()
            try:
                ts = float(item.get("timestamp_sec", 0.0) or 0.0)
            except Exception:
                ts = 0.0
            if shot_id:
                seen.add((shot_id, ts))

        for item in requests:
            shot_id = str(item.get("screenshot_id") or "").strip()
            try:
                ts = float(item.get("timestamp_sec", 0.0) or 0.0)
            except Exception:
                ts = 0.0
            key = (shot_id, ts)
            if not shot_id or key in seen:
                continue
            seen.add(key)
            merged.append(item)

        material_requests["screenshot_requests"] = merged
        if requests:
            updated += 1

    _dump_json(output_path, units)
    print(f"Replay semantic units written: {output_path}")
    print(f"Units with merged screenshot requests: {updated}")
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare Phase2B replay semantic_units JSON")
    parser.add_argument("--storage-dir", required=True, help="Path to storage/<task_id> directory")
    args = parser.parse_args()

    storage_dir = Path(args.storage_dir).resolve()
    build_replay_input(storage_dir)


if __name__ == "__main__":
    main()
