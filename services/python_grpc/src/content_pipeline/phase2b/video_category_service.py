"""
模块说明：Phase2B 视频级分类服务。
执行逻辑：
1) 在 Phase2B 最终产物落盘后，抽取标题、首段正文和大纲组名。
2) 复用 category_classifier prompt 调用 LLM，并通过二次校验压制示例串台。
3) 立即回写分类路径库、任务级分类结果、video_meta.json 和 var/storage 汇总 JSON。
实现方式：prompt_loader + llm_gateway + 文件级原子回写。
核心价值：把视频分类正式纳入 Phase2B 末尾，而不是依赖离线脚本补跑。
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from services.python_grpc.src.content_pipeline.common.utils.json_payload_repair import (
    extract_top_level_objects,
    parse_json_payload,
)
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt, render_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys


logger = logging.getLogger(__name__)

_DEFAULT_DEEPSEEK_BASE_URL = "https://api.deepseek.com/v1"
_DEFAULT_DEEPSEEK_MODEL = "deepseek-chat"
_CATEGORY_LIBRARY_FILE = "category_paths.txt"
_TASK_CLASSIFICATION_FILE = "category_classification.json"
_SUMMARY_JSON_FILE = "category_classification_results.json"


@dataclass
class VideoCategoryInput:
    """封装单个视频分类所需的最小输入。"""

    task_dir: Path
    title: str
    first_unit_text: str
    group_names: List[str]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _normalize_lines(lines: Iterable[str]) -> List[str]:
    values: List[str] = []
    seen: set[str] = set()
    for line in lines:
        value = str(line or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        values.append(value)
    return values


def _load_category_library(path: Path) -> List[str]:
    if not path.exists():
        return []
    return _normalize_lines(path.read_text(encoding="utf-8").splitlines())


def _write_category_library(path: Path, categories: Iterable[str]) -> None:
    normalized = sorted(_normalize_lines(categories))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(normalized) + ("\n" if normalized else ""),
        encoding="utf-8",
    )


def _extract_first_unit_text(result_payload: Dict[str, Any]) -> str:
    groups = result_payload.get("knowledge_groups")
    if not isinstance(groups, list):
        return ""
    for group in groups:
        if not isinstance(group, dict):
            continue
        units = group.get("units")
        if not isinstance(units, list):
            continue
        for unit in units:
            if not isinstance(unit, dict):
                continue
            text = str(unit.get("body_text") or unit.get("text") or "").strip()
            if text:
                return text[:2000]
    return ""


def _extract_group_names(result_payload: Dict[str, Any]) -> List[str]:
    groups = result_payload.get("knowledge_groups")
    if not isinstance(groups, list):
        return []
    return _normalize_lines(
        str(group.get("group_name") or "").strip()
        for group in groups
        if isinstance(group, dict)
    )


def _build_video_input(task_dir: Path, title: str = "") -> VideoCategoryInput:
    video_meta_path = task_dir / "video_meta.json"
    result_path = task_dir / "result.json"
    result_payload = _read_json(result_path)
    title_candidates = [
        str(title or "").strip(),
        str(result_payload.get("title") or "").strip(),
    ]
    if video_meta_path.exists():
        video_meta = _read_json(video_meta_path)
        title_candidates.insert(1, str(video_meta.get("title") or "").strip())

    resolved_title = next((item for item in title_candidates if item), "")
    first_unit_text = _extract_first_unit_text(result_payload)
    group_names = _extract_group_names(result_payload)
    if not resolved_title or not first_unit_text or not group_names:
        raise ValueError(
            f"分类输入不完整: title={bool(resolved_title)}, "
            f"first_unit_text={bool(first_unit_text)}, group_names={bool(group_names)}"
        )
    return VideoCategoryInput(
        task_dir=task_dir,
        title=resolved_title,
        first_unit_text=first_unit_text,
        group_names=group_names,
    )


def _extract_json_object(text: str) -> Dict[str, Any]:
    payload, error = parse_json_payload(text)
    if isinstance(payload, dict):
        return payload
    for candidate in extract_top_level_objects(text):
        payload, error = parse_json_payload(candidate)
        if isinstance(payload, dict):
            return payload
    raise ValueError(f"模型输出不是合法 JSON: {error or text[:200]}")


def _normalize_category_result(payload: Dict[str, Any], categories: List[str]) -> Dict[str, Any]:
    category_path = str(payload.get("category_path") or "").strip()
    reasoning = str(payload.get("reasoning") or "").strip()
    if not category_path or "/" not in category_path:
        raise ValueError(f"非法分类路径: {category_path!r}")

    actual_is_new = category_path not in set(categories)
    is_new_value = payload.get("is_new")
    if not isinstance(is_new_value, bool) or bool(is_new_value) != actual_is_new:
        is_new_value = actual_is_new

    return {
        "category_path": category_path,
        "is_new": bool(is_new_value),
        "reasoning": reasoning,
    }


def _task_dir_from_output_dir(output_dir: str) -> Path:
    resolved = Path(str(output_dir or "")).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"output_dir not found: {resolved}")
    return resolved


def _storage_root_from_task_dir(task_dir: Path) -> Path:
    return task_dir.parent


def _summary_json_path(storage_root: Path) -> Path:
    return storage_root.parent / _SUMMARY_JSON_FILE


def _bootstrap_existing_categories(storage_root: Path) -> List[str]:
    categories: List[str] = []
    for child in sorted(storage_root.iterdir(), key=lambda item: item.name):
        if not child.is_dir() or child.name.startswith("."):
            continue
        classification_path = child / _TASK_CLASSIFICATION_FILE
        video_meta_path = child / "video_meta.json"
        candidate = ""
        try:
            if classification_path.exists():
                candidate = str(_read_json(classification_path).get("category_path") or "").strip()
            elif video_meta_path.exists():
                candidate = str(_read_json(video_meta_path).get("category_path") or "").strip()
        except Exception:
            candidate = ""
        if candidate:
            categories.append(candidate)
    return _normalize_lines(categories)


def _collect_existing_classifications(storage_root: Path) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for child in sorted(storage_root.iterdir(), key=lambda item: item.name):
        if not child.is_dir() or child.name.startswith("."):
            continue
        classification_path = child / _TASK_CLASSIFICATION_FILE
        if not classification_path.exists():
            continue
        try:
            payload = _read_json(classification_path)
        except Exception:
            continue
        if isinstance(payload, dict):
            results.append(payload)
    return results


def _write_summary_json(summary_file: Path, classifications: List[Dict[str, Any]]) -> None:
    existing_summary: Dict[str, Any] = {}
    if summary_file.exists():
        try:
            loaded = _read_json(summary_file)
            if isinstance(loaded, dict):
                existing_summary = loaded
        except Exception:
            existing_summary = {}

    category_counts: Dict[str, int] = {}
    for item in classifications:
        category_path = str(item.get("category_path") or "").strip()
        if not category_path:
            continue
        category_counts[category_path] = category_counts.get(category_path, 0) + 1

    existing_bindings = _normalize_collection_bindings(existing_summary.get("collectionBindings"))
    old_auto_bindings = _build_automatic_bindings(existing_summary.get("results"))
    new_auto_bindings = _build_automatic_bindings(classifications)
    merged_bindings = dict(existing_bindings)
    for task_path, category_path in new_auto_bindings.items():
        current_binding = str(merged_bindings.get(task_path) or "").strip()
        previous_auto = str(old_auto_bindings.get(task_path) or "").strip()
        if not current_binding or current_binding == previous_auto:
            merged_bindings[task_path] = category_path

    payload = {
        "updated_at": _utc_now_iso(),
        "total_videos": len(classifications),
        "category_counts": dict(sorted(category_counts.items(), key=lambda pair: pair[0])),
        "results": classifications,
        "collectionBindings": dict(sorted(merged_bindings.items(), key=lambda pair: pair[0])),
    }
    _write_json(summary_file, payload)


def _update_video_meta(task_dir: Path, classification: Dict[str, Any]) -> None:
    meta_path = task_dir / "video_meta.json"
    payload: Dict[str, Any] = {}
    if meta_path.exists():
        try:
            loaded = _read_json(meta_path)
            if isinstance(loaded, dict):
                payload = dict(loaded)
        except Exception as exc:
            logger.warning("Failed to read existing video_meta.json from %s: %s", meta_path, exc)

    category_path = str(classification["category_path"])
    domain, subdomain = category_path.split("/", 1)
    payload["category_path"] = category_path
    payload["category_domain"] = domain
    payload["category_subdomain"] = subdomain
    payload["category_is_new"] = bool(classification["is_new"])
    payload["category_reasoning"] = str(classification["reasoning"] or "")
    payload["category_classified_at"] = str(classification["generated_at"] or _utc_now_iso())
    _write_json(meta_path, payload)


def _write_task_classification(task_input: VideoCategoryInput, classification: Dict[str, Any]) -> None:
    task_path = f"storage/{task_input.task_dir.name}"
    artifact = {
        "video_id": task_input.task_dir.name,
        "task_path": task_path,
        "video_title": task_input.title,
        "category_path": classification["category_path"],
        "is_new": classification["is_new"],
        "reasoning": classification["reasoning"],
        "generated_at": classification["generated_at"],
        "usage": classification.get("usage", {}),
        "input_snapshot": {
            "first_unit_text": task_input.first_unit_text,
            "group_names": task_input.group_names,
        },
        "raw_response": classification.get("raw_response", ""),
        "verified_raw_response": classification.get("verified_raw_response", ""),
    }
    _write_json(task_input.task_dir / _TASK_CLASSIFICATION_FILE, artifact)


def _normalize_collection_bindings(payload: Any) -> Dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    normalized: Dict[str, str] = {}
    for raw_task_path, raw_collection_path in payload.items():
        task_path = str(raw_task_path or "").strip().replace("\\", "/").lstrip("/")
        collection_path = str(raw_collection_path or "").strip().replace("\\", "/")
        if not task_path or not collection_path:
            continue
        task_path = "/".join(part for part in task_path.split("/") if part)
        collection_path = "/".join(part for part in collection_path.split("/") if part)
        if not task_path or not collection_path:
            continue
        normalized[task_path] = collection_path
    return normalized


def _resolve_task_path_from_summary_item(item: Dict[str, Any]) -> str:
    task_path = str(item.get("task_path") or item.get("taskPath") or "").strip()
    if task_path:
        return "/".join(part for part in task_path.replace("\\", "/").split("/") if part)
    video_id = str(item.get("video_id") or "").strip()
    if not video_id:
        return ""
    return f"storage/{video_id}"


def _build_automatic_bindings(results: Any) -> Dict[str, str]:
    if not isinstance(results, list):
        return {}
    bindings: Dict[str, str] = {}
    for item in results:
        if not isinstance(item, dict):
            continue
        task_path = _resolve_task_path_from_summary_item(item)
        category_path = str(item.get("category_path") or "").strip()
        if not task_path or not category_path:
            continue
        bindings[task_path] = category_path
    return bindings


async def _verify_classification(
    *,
    task_input: VideoCategoryInput,
    categories: List[str],
    system_prompt: str,
    candidate: Dict[str, Any],
    api_key: str,
    base_url: str,
    model: str,
) -> Dict[str, Any]:
    verify_prompt = f"""请审查下面这个视频分类结果是否严格基于输入事实。

如果候选结果存在任一问题，你必须直接纠正：
1. `category_path` 与标题、第一段核心正文、大纲组名的核心主题不一致；
2. `reasoning` 引用了输入中不存在的证据词、示例内容或串台信息；
3. `is_new` 与当前分类路径库是否包含该 `category_path` 不一致；
4. `category_path` 不是唯一、互斥、两层结构的 `领域/子领域`。

你必须自己重新核对，不要盲从候选结果。
请只输出修正后的合法 JSON。

## 视频标题
{task_input.title}

## 第一段核心正文
{task_input.first_unit_text}

## 大纲组名
{json.dumps(task_input.group_names, ensure_ascii=False)}

## 当前分类路径库
{json.dumps(categories, ensure_ascii=False)}

## 候选结果
{json.dumps(candidate, ensure_ascii=False)}
"""
    content, _metadata, _ = await llm_gateway.deepseek_complete_text(
        prompt=verify_prompt,
        system_message=system_prompt,
        api_key=api_key,
        base_url=base_url,
        model=model,
        temperature=0.0,
        cache_enabled=False,
        inflight_dedup_enabled=False,
        hedge_context={"batch_text_chars": len(verify_prompt)},
    )
    normalized = _normalize_category_result(_extract_json_object(content), categories)
    normalized["verified_raw_response"] = content
    return normalized


async def classify_phase2b_output(
    *,
    output_dir: str,
    title: str,
    result_json_path: str,
) -> Optional[Dict[str, Any]]:
    """
    在 Phase2B 最终产物落盘后执行视频级分类。
    失败策略：记录告警并返回 None，不阻塞 Phase2B 主链路。
    """
    api_key = str(os.getenv("DEEPSEEK_API_KEY", "") or "").strip()
    if not api_key:
        logger.info("[Phase2B-Category] skip: DEEPSEEK_API_KEY not set")
        return None

    try:
        task_dir = _task_dir_from_output_dir(output_dir)
        if not Path(result_json_path).exists():
            logger.warning("[Phase2B-Category] skip: result.json missing, path=%s", result_json_path)
            return None

        storage_root = _storage_root_from_task_dir(task_dir)
        library_path = storage_root / _CATEGORY_LIBRARY_FILE
        summary_json_path = _summary_json_path(storage_root)

        task_input = _build_video_input(task_dir=task_dir, title=title)
        categories = _load_category_library(library_path)
        categories.extend(_bootstrap_existing_categories(storage_root))
        categories = _normalize_lines(categories)

        system_prompt = get_prompt(PromptKeys.DEEPSEEK_CATEGORY_CLASSIFIER_SYSTEM)
        user_prompt = render_prompt(
            PromptKeys.DEEPSEEK_CATEGORY_CLASSIFIER_USER,
            context={
                "video_title": task_input.title,
                "first_unit_text": task_input.first_unit_text,
                "group_names": "\n".join(f"- {name}" for name in task_input.group_names),
                "categories": "\n".join(categories),
            },
        )
        base_url = str(os.getenv("MODULE2_CATEGORY_CLASSIFIER_BASE_URL", _DEFAULT_DEEPSEEK_BASE_URL) or "").strip()
        model = str(os.getenv("MODULE2_CATEGORY_CLASSIFIER_MODEL", _DEFAULT_DEEPSEEK_MODEL) or "").strip()

        content, metadata, _ = await llm_gateway.deepseek_complete_text(
            prompt=user_prompt,
            system_message=system_prompt,
            api_key=api_key,
            base_url=base_url,
            model=model,
            temperature=0.1,
            cache_enabled=False,
            inflight_dedup_enabled=False,
            hedge_context={"batch_text_chars": len(user_prompt)},
        )
        candidate = _normalize_category_result(_extract_json_object(content), categories)
        verified = await _verify_classification(
            task_input=task_input,
            categories=categories,
            system_prompt=system_prompt,
            candidate=candidate,
            api_key=api_key,
            base_url=base_url,
            model=model,
        )
        if isinstance(metadata, dict):
            verified["usage"] = {
                key: metadata.get(key)
                for key in (
                    "model",
                    "prompt_tokens",
                    "completion_tokens",
                    "total_tokens",
                    "latency_ms",
                    "cache_hit",
                )
                if key in metadata
            }
        else:
            verified["usage"] = {}
        verified["raw_response"] = content
        verified["generated_at"] = _utc_now_iso()

        updated_categories = sorted(set(categories) | {verified["category_path"]})
        _write_category_library(library_path, updated_categories)
        _update_video_meta(task_dir, verified)
        _write_task_classification(task_input, verified)
        _write_summary_json(summary_json_path, _collect_existing_classifications(storage_root))
        logger.info(
            "[Phase2B-Category] classified task=%s category=%s is_new=%s",
            task_dir.name,
            verified["category_path"],
            verified["is_new"],
        )
        return verified
    except Exception as exc:
        logger.warning("[Phase2B-Category] failed for output_dir=%s: %s", output_dir, exc)
        return None
