"""
语义单元 payload 工具。

目标：
1) 统一 grouped/legacy 双格式读写逻辑。
2) 避免不同调用链重复实现，降低协议漂移风险。
"""

from typing import Any, Dict, List, Tuple


def _normalize_group_key(group_name: str) -> str:
    """规范化分组名，用于回退分组聚合。"""
    cleaned = "".join(ch.lower() for ch in str(group_name or "").strip() if ch.isalnum())
    return cleaned or str(group_name or "").strip().lower()


def iter_semantic_unit_nodes(data: Any) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    遍历语义单元节点并附带分组元信息。

    返回每个元素为 `(unit_node, group_meta)`：
    - unit_node: 可原地修改的 unit dict 引用。
    - group_meta: group_id/group_name/group_reason（若可用）。
    """
    nodes: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    if isinstance(data, list):
        for item in list(data or []):
            if isinstance(item, dict):
                nodes.append((item, {}))
        return nodes

    if not isinstance(data, dict):
        return nodes

    grouped_payload = data.get("knowledge_groups")
    if isinstance(grouped_payload, list):
        fallback_group_id = 1
        for raw_group in list(grouped_payload or []):
            if not isinstance(raw_group, dict):
                continue
            group_name = str(raw_group.get("group_name", "") or "").strip()
            group_reason = str(raw_group.get("reason", "") or "").strip()
            try:
                group_id = int(raw_group.get("group_id", 0) or 0)
            except Exception:
                group_id = 0
            if group_id <= 0:
                group_id = fallback_group_id
            fallback_group_id = max(fallback_group_id, group_id + 1)

            group_meta = {
                "group_id": group_id,
                "group_name": group_name,
                "group_reason": group_reason,
            }
            for raw_unit in list(raw_group.get("units") or []):
                if isinstance(raw_unit, dict):
                    nodes.append((raw_unit, group_meta))
        return nodes

    legacy_units = data.get("semantic_units")
    if isinstance(legacy_units, list):
        for item in list(legacy_units or []):
            if isinstance(item, dict):
                nodes.append((item, {}))
    return nodes


def build_semantic_unit_index(data: Any) -> Dict[str, Dict[str, Any]]:
    """建立 `unit_id -> unit_node` 索引。"""
    units_index: Dict[str, Dict[str, Any]] = {}
    for unit_node, _group_meta in iter_semantic_unit_nodes(data):
        unit_id = str(unit_node.get("unit_id", "") or "").strip()
        if unit_id:
            units_index[unit_id] = unit_node
    return units_index


def normalize_semantic_units_payload(data: Any) -> List[Dict[str, Any]]:
    """拉平 grouped/legacy 载荷为扁平 unit 列表，并继承 group 元信息。"""
    normalized_units: List[Dict[str, Any]] = []
    for unit_node, group_meta in iter_semantic_unit_nodes(data):
        normalized = dict(unit_node)
        group_id = int(group_meta.get("group_id", 0) or 0)
        group_name = str(group_meta.get("group_name", "") or "").strip()
        group_reason = str(group_meta.get("group_reason", "") or "").strip()
        try:
            current_group_id = int(normalized.get("group_id", 0) or 0)
        except Exception:
            current_group_id = 0
        if group_id > 0 and current_group_id <= 0:
            normalized["group_id"] = group_id
        if group_name and not str(normalized.get("group_name", "") or "").strip():
            normalized["group_name"] = group_name
        if group_reason and not str(normalized.get("group_reason", "") or "").strip():
            normalized["group_reason"] = group_reason
        normalized_units.append(normalized)
    return normalized_units


def build_grouped_semantic_units_payload(
    semantic_units: List[Dict[str, Any]],
    *,
    schema_version: str = "phase2a.grouped.v1",
    default_group_reason: str = "同一核心论点聚合",
    strip_unit_group_fields: bool = True,
) -> Dict[str, Any]:
    """
    将扁平语义单元重建为 `knowledge_groups` 结构。

    - 缺失 group_id 时，按 group_name 回退聚合。
    - 默认剥离 unit 层 group 元信息，保持 Group/Unit 职责分离。
    """
    grouped_payload: Dict[int, Dict[str, Any]] = {}
    group_name_to_fallback_id: Dict[str, int] = {}
    next_fallback_group_id = 1

    for raw_unit in list(semantic_units or []):
        if not isinstance(raw_unit, dict):
            continue

        unit = dict(raw_unit)
        group_name = str(unit.get("group_name", "") or "").strip()
        if not group_name:
            group_name = (
                str(unit.get("knowledge_topic", "") or "").strip()
                or str(unit.get("title", "") or "").strip()
                or "未命名知识点"
            )

        try:
            group_id = int(unit.get("group_id", 0) or 0)
        except Exception:
            group_id = 0
        if group_id <= 0:
            normalized_group_key = _normalize_group_key(group_name)
            if normalized_group_key not in group_name_to_fallback_id:
                group_name_to_fallback_id[normalized_group_key] = next_fallback_group_id
                next_fallback_group_id += 1
            group_id = group_name_to_fallback_id[normalized_group_key]

        group_reason = str(unit.get("group_reason", "") or "").strip() or default_group_reason

        if strip_unit_group_fields:
            unit.pop("group_id", None)
            unit.pop("group_name", None)
            unit.pop("group_reason", None)

        group_bucket = grouped_payload.setdefault(
            group_id,
            {
                "group_id": group_id,
                "group_name": group_name,
                "reason": group_reason,
                "units": [],
            },
        )
        if not str(group_bucket.get("group_name", "") or "").strip():
            group_bucket["group_name"] = group_name
        if not str(group_bucket.get("reason", "") or "").strip():
            group_bucket["reason"] = group_reason
        group_bucket["units"].append(unit)

    knowledge_groups: List[Dict[str, Any]] = []
    for gid in sorted(grouped_payload.keys()):
        group_bucket = grouped_payload[gid]
        sorted_units = sorted(
            list(group_bucket.get("units", [])),
            key=lambda item: (
                float(item.get("start_sec", 0.0) or 0.0),
                str(item.get("unit_id", "") or ""),
            ),
        )
        knowledge_groups.append(
            {
                "group_id": int(group_bucket.get("group_id", gid) or gid),
                "group_name": str(group_bucket.get("group_name", "") or "").strip() or f"知识点分组{gid}",
                "reason": str(group_bucket.get("reason", "") or "").strip() or default_group_reason,
                "units": sorted_units,
            }
        )

    return {
        "schema_version": schema_version,
        "knowledge_groups": knowledge_groups,
        "total_units_output": sum(len(group.get("units", [])) for group in knowledge_groups),
    }
