"""RichTextPipeline ???????????"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List

from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnit

from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import (
    ClipRequest,
    MaterialRequests,
    ScreenshotRequest,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.pipeline_material_request_utils import (
    create_clip_request,
    create_screenshot_request,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_document import MaterialSet

logger = logging.getLogger(__name__)

async def generate_materials(pipeline, unit: SemanticUnit):
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：action_segments
    - 条件：len(action_segments) >= 2
    - 条件：stable_islands
    依据来源（证据链）：
    输入参数：
    - unit: 函数入参（类型：SemanticUnit）。
    输出参数：
    - 无（仅产生副作用，如日志/写盘/状态更新）。"""
    materials = MaterialSet(modality=unit.modality)
    
    screenshot_paths = []
    screenshot_labels = []
    clip_paths = []
    
    # 获取稳定岛和动作单元信息
    stable_islands = getattr(unit, 'stable_islands', [])
    action_segments = getattr(unit, 'action_segments', [])
    
    if action_segments:
        # ==== 有动作单元: 规则一 + 规则二 ====
        # 规则一: 不提取语义单元级 stable 部分 (跳过)
        
        # 💥 后处理: 多动作融合 (同一语义单元同主题，放宽合并间隔)
        if len(action_segments) >= 2:
            merged_actions = pipeline._merge_action_segments(action_segments, gap_threshold_sec=5.0)
            if len(merged_actions) < len(action_segments):
                logger.info(
                    f"{unit.unit_id}: Post-merge (gap<5.0s) {len(action_segments)} → {len(merged_actions)} actions"
                )
            action_segments = merged_actions
            unit.action_segments = merged_actions
        
        # 🚀 优化：优先复用预分类/上游 knowledge_type，缺失时才调用 LLM
        for a in action_segments:
            if isinstance(a.get("classification"), dict) and a.get("classification", {}).get("knowledge_type"):
                continue
            kt = str(a.get("knowledge_type", "") or "").strip()
            if kt:
                a["classification"] = {
                    "knowledge_type": kt,
                    "confidence": float(a.get("confidence", 0.5) or 0.5),
                    "key_evidence": a.get("key_evidence", ""),
                    "reasoning": a.get("reasoning", ""),
                }

        if all(
            isinstance(a.get("classification"), dict) and a.get("classification", {}).get("knowledge_type")
            for a in action_segments
        ):
            batch_classifications = [a.get("classification", {}) for a in action_segments]
        else:
            batch_classifications = await pipeline._knowledge_classifier.classify_batch(
                semantic_unit_title=getattr(unit, 'knowledge_topic', '未知主题'),
                semantic_unit_text=getattr(unit, 'full_text', getattr(unit, 'text', '')),
                action_segments=action_segments
            )
            for action, classification in zip(action_segments, batch_classifications):
                action["classification"] = classification
        
        for i, (action, classification) in enumerate(zip(action_segments, batch_classifications)):
            action_start = action.get("start_sec", unit.start_sec)
            action_end = action.get("end_sec", unit.end_sec)
            # 获取该动作单元内部的稳定岛
            action_internal_islands = action.get("internal_stable_islands", [])
            
            # 💥 Sentence：与动作重叠的那句（按字幕时间戳定位）
            sentence_start = pipeline._align_to_sentence_start(action_start)
            sentence_end = pipeline._align_to_sentence_end(action_end)
            
            # Classification already done in batch
            knowledge_type = classification.get("knowledge_type", "过程性知识")
            confidence = classification.get("confidence", 0.5)
            action_brief = pipeline._build_action_brief(action, classification, i + 1)
            asset_base = f"{unit.unit_id}/{pipeline._build_request_base_name(unit, f'action_{i+1:02d}_{action_brief}')}"

            
            logger.info(f"{unit.unit_id} action_{i+1}: {knowledge_type} (conf={confidence:.0%}) - {classification.get('key_evidence', '')[:30]}")

            # 🚀 Adaptive Action Envelope: 语义单元短时可整段；且 clip 结束不跨越 unit.end_sec
            envelope_start, envelope_end = pipeline._compute_action_envelope(
                unit=unit,
                action_start=action_start,
                action_end=action_end,
                sentence_start=sentence_start,
                sentence_end=sentence_end,
                knowledge_type=knowledge_type
            )
            logger.warning(
                f"{unit.unit_id} action_{i+1}: envelope [{envelope_start:.2f}s-{envelope_end:.2f}s] "
                f"(knowledge_type={knowledge_type})"
            )
            
            # 根据分类决定素材策略
            if knowledge_type == "讲解型":
                # 💥 降级: 讲解型只截取首尾帧 + 稳定岛截图，不提取视频
                logger.info("  → Downgrade to screenshots only (讲解型)")
                
                # 首帧截图: 查找窗口为 [包络起点, 动作起点]
                head_window_end = min(max(envelope_start + 0.5, action_start), envelope_end)
                head_ss = await pipeline._select_screenshot(
                    start_sec=envelope_start,
                    end_sec=head_window_end,
                    name=f"{asset_base}_head"
                )
                if head_ss:
                    screenshot_paths.append(head_ss)
                    screenshot_labels.append(f"动作{i+1}首帧")
                
                # 稳定岛截图
                for j, island in enumerate(action_internal_islands):
                    island_start = island.get("start", action_start)
                    island_end = island.get("end", action_end)
                    
                    island_ss = await pipeline._select_screenshot(
                        start_sec=island_start,
                        end_sec=island_end,
                        name=f"{asset_base}_island_{j+1:02d}"
                    )
                    if island_ss:
                        screenshot_paths.append(island_ss)
                        screenshot_labels.append(f"动作{i+1}稳定帧{j+1}")
                
                # 末帧截图: 查找窗口为 [动作终点, 包络终点]
                tail_window_start = max(min(envelope_end - 0.5, action_end), envelope_start)
                tail_ss = await pipeline._select_screenshot(
                    start_sec=tail_window_start,
                    end_sec=envelope_end,
                    name=f"{asset_base}_tail"
                )
                if tail_ss:
                    screenshot_paths.append(tail_ss)
                    screenshot_labels.append(f"动作{i+1}末帧")
            
            else:
                # 非讲解型: 提取视频 + 首尾帧 + 稳定岛截图
                
                # 1. 提取视频片段 (使用自适应动作包络时间范围)
                clip_path = await pipeline._extract_action_clip(
                    start_sec=envelope_start,
                    end_sec=envelope_end,
                    name=f"{asset_base}"
                )
                if clip_path:
                    clip_paths.append(clip_path)
                
                # 2. 提取首帧截图: 查找窗口为 [包络起点, 动作起点]
                head_window_end = min(max(envelope_start + 0.5, action_start), envelope_end)
                head_ss = await pipeline._select_screenshot(
                    start_sec=envelope_start,
                    end_sec=head_window_end,
                    name=f"{asset_base}_head"
                )
                if head_ss:
                    screenshot_paths.append(head_ss)
                    screenshot_labels.append(f"动作{i+1}首帧")
                
                # 3. 稳定岛截图
                for j, island in enumerate(action_internal_islands):
                    island_start = island.get("start", action_start)
                    island_end = island.get("end", action_end)
                    
                    island_ss = await pipeline._select_screenshot(
                        start_sec=island_start,
                        end_sec=island_end,
                        name=f"{asset_base}_island_{j+1:02d}"
                    )
                    if island_ss:
                        screenshot_paths.append(island_ss)
                        screenshot_labels.append(f"动作{i+1}稳定帧{j+1}")
                
                # 4. 提取末帧截图: 查找窗口为 [动作终点, 包络终点]
                tail_window_start = max(min(envelope_end - 0.5, action_end), envelope_start)
                tail_ss = await pipeline._select_screenshot(
                    start_sec=tail_window_start,
                    end_sec=envelope_end,
                    name=f"{asset_base}_tail"
                )
                if tail_ss:
                    screenshot_paths.append(tail_ss)
                    screenshot_labels.append(f"动作{i+1}末帧")
    
    elif stable_islands:
        # ==== 无动作单元，仅稳定岛: 提取中间帧 ====
        for i, island in enumerate(stable_islands):
            island_start = island.get("start", unit.start_sec)
            island_end = island.get("end", unit.end_sec)
            
            ss_path = await pipeline._select_screenshot(
                start_sec=island_start,
                end_sec=island_end,
                name=f"{unit.unit_id}/{pipeline._build_request_base_name(unit, f'stable_{i+1:02d}')}"
            )
            if ss_path:
                screenshot_paths.append(ss_path)
                screenshot_labels.append(f"稳定帧{i+1}")
    
    else:
        # ==== 回退: 无任何检测结果 ====
        fallback_ss = await pipeline._select_screenshot(
            start_sec=unit.start_sec,
            end_sec=unit.end_sec,
            name=f"{unit.unit_id}/{pipeline._build_request_base_name(unit, 'fallback')}"
        )
        if fallback_ss:
            screenshot_paths.append(fallback_ss)
            screenshot_labels.append("截图")
    
    # ==== 组装素材集合 ====
    materials.screenshot_paths = screenshot_paths
    materials.screenshot_labels = screenshot_labels
    materials.screenshot_items = [
        {
            "img_id": f"{unit.unit_id}_img_{idx + 1:02d}",
            "img_path": path,
            "img_description": screenshot_labels[idx] if idx < len(screenshot_labels) else f"image_{idx + 1}",
            "img_desription": screenshot_labels[idx] if idx < len(screenshot_labels) else f"image_{idx + 1}",
            "label": screenshot_labels[idx] if idx < len(screenshot_labels) else "",
            "source_id": Path(path).stem,
        }
        for idx, path in enumerate(screenshot_paths)
    ]
    materials.clip_paths = clip_paths
    materials.clip_path = clip_paths[0] if clip_paths else ""
    
    # 💥 V7.4: 提取动作单元分类结果
    action_classifications = []
    for action in action_segments:
        if "classification" in action:
            action_classifications.append({
                "time_range": [action.get("start_sec", 0), action.get("end_sec", 0)],
                **action["classification"]
            })
    materials.action_classifications = action_classifications
    
    unit.materials = materials
    
    logger.debug(f"{unit.unit_id}: {len(action_segments)} actions, {len(stable_islands)} islands → "
                 f"{len(clip_paths)} clips + {len(screenshot_paths)} screenshots")


async def collect_material_requests(pipeline, unit: SemanticUnit) -> MaterialRequests:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部方法调用/状态更新、HTTP 调用实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：len(action_segments) >= 2
    - 条件：action_segments
    - 条件：stable_islands
    依据来源（证据链）：
    输入参数：
    - unit: 函数入参（类型：SemanticUnit）。
    输出参数：
    - MaterialRequests 对象（包含字段：screenshot_requests, clip_requests, action_classifications）。"""
    screenshot_requests: List[ScreenshotRequest] = []
    clip_requests: List[ClipRequest] = []
    action_classifications: List[Dict[str, Any]] = []
    
    # 获取稳定岛和动作单元信息
    stable_islands = getattr(unit, 'stable_islands', [])
    action_segments = getattr(unit, 'action_segments', [])
    
    # 💥 后处理: 多动作融合 (与_generate_materials保持一致)
    if len(action_segments) >= 2:
        action_segments = pipeline._merge_action_segments(action_segments, gap_threshold_sec=5.0)
        unit.action_segments = action_segments
    
    if action_segments:
        # 🚀 优化：优先复用预分类/上游 knowledge_type，缺失时才调用 LLM
        for a in action_segments:
            if isinstance(a.get("classification"), dict) and a.get("classification", {}).get("knowledge_type"):
                continue
            kt = str(a.get("knowledge_type", "") or "").strip()
            if kt:
                a["classification"] = {
                    "knowledge_type": kt,
                    "confidence": float(a.get("confidence", 0.5) or 0.5),
                    "key_evidence": a.get("key_evidence", ""),
                    "reasoning": a.get("reasoning", ""),
                }

        if all(
            isinstance(a.get("classification"), dict) and a.get("classification", {}).get("knowledge_type")
            for a in action_segments
        ):
            batch_classifications = [a.get("classification", {}) for a in action_segments]
        else:
            batch_classifications = await pipeline._knowledge_classifier.classify_batch(
                semantic_unit_title=getattr(unit, 'knowledge_topic', '未知主题'),
                semantic_unit_text=getattr(unit, 'full_text', getattr(unit, 'text', '')),
                action_segments=action_segments
            )
            for action, classification in zip(action_segments, batch_classifications):
                action["classification"] = classification

        # ==== 有动作单元 ====
        for i, (action, classification) in enumerate(zip(action_segments, batch_classifications)):
            action_start = action.get("start_sec", unit.start_sec)
            action_end = action.get("end_sec", unit.end_sec)
            action_internal_islands = action.get("internal_stable_islands", [])
            
            # 💥 Sentence：与动作重叠的那句（按字幕时间戳定位）
            sentence_start = pipeline._align_to_sentence_start(action_start)
            sentence_end = pipeline._align_to_sentence_end(action_end)
            
            # Classification already done in batch
            knowledge_type = classification.get("knowledge_type", "过程性知识")
            confidence = classification.get("confidence", 0.5)
            action_brief = pipeline._build_action_brief(action, classification, i + 1)
            request_base = pipeline._build_unit_relative_request_id(
                unit,
                f"action_{i+1:02d}_{action_brief}",
            )

            
            # 存储分类结果
            action_classifications.append({
                "time_range": [action_start, action_end],
                **classification
            })
            
            logger.info(f"{unit.unit_id} action_{i+1}: {knowledge_type} (conf={confidence:.0%})")

            # 🚀 Adaptive Action Envelope: 语义单元短时可整段；且 clip 结束不跨越 unit.end_sec
            envelope_start, envelope_end = pipeline._compute_action_envelope(
                unit=unit,
                action_start=action_start,
                action_end=action_end,
                sentence_start=sentence_start,
                sentence_end=sentence_end,
                knowledge_type=knowledge_type
            )
            logger.warning(
                f"{unit.unit_id} action_{i+1}: envelope [{envelope_start:.2f}s-{envelope_end:.2f}s] "
                f"(knowledge_type={knowledge_type})"
            )
            
            # 根据分类决定素材策略
            if knowledge_type == "讲解型":
                # 讲解型: 只需要截图，不需要视频
                # 首帧截图: 搜索窗口扩大为 包络起点 ±1.0s
                head_search_start, head_search_end = pipeline._clamp_time_range(envelope_start - 1.0, envelope_start + 1.0)
                fallback_head_ts = envelope_start
                head_ts = await pipeline._select_screenshot_timestamp(head_search_start, head_search_end, fallback_head_ts)
                
                screenshot_requests.append(
                    create_screenshot_request(
                        screenshot_request_type=ScreenshotRequest,
                        screenshot_id=f"{request_base}_head",
                        timestamp_sec=head_ts,
                        label="head",
                        semantic_unit_id=unit.unit_id,
                    )
                )
                
                # 稳定岛截图
                for j, island in enumerate(action_internal_islands):
                    island_start = island.get("start", action_start)
                    island_end = island.get("end", action_end)
                    island_mid_fallback = (island_start + island_end) / 2
                    island_start, island_end = pipeline._clamp_time_range(island_start, island_end)
                    island_mid = await pipeline._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                    screenshot_requests.append(
                        create_screenshot_request(
                            screenshot_request_type=ScreenshotRequest,
                            screenshot_id=f"{request_base}_island_{j+1:02d}",
                            timestamp_sec=island_mid,
                            label="stable",
                            semantic_unit_id=unit.unit_id,
                        )
                    )
                
                # 末帧截图: 搜索窗口扩大为 包络终点 ±1.0s
                tail_search_start, tail_search_end = pipeline._clamp_time_range(envelope_end - 1.0, envelope_end + 1.0)
                tail_search_end = min(tail_search_end, float(getattr(unit, "end_sec", tail_search_end)))
                tail_search_start, tail_search_end = pipeline._clamp_time_range(tail_search_start, tail_search_end)
                fallback_tail_ts = envelope_end
                tail_ts = await pipeline._select_screenshot_timestamp(tail_search_start, tail_search_end, fallback_tail_ts)
                
                screenshot_requests.append(
                    create_screenshot_request(
                        screenshot_request_type=ScreenshotRequest,
                        screenshot_id=f"{request_base}_tail",
                        timestamp_sec=tail_ts,
                        label="tail",
                        semantic_unit_id=unit.unit_id,
                    )
                )
            
            else:
                # 非讲解型: 需要视频切片 + 首尾帧截图
                # 视频切片
                clip_requests.append(
                    create_clip_request(
                        clip_request_type=ClipRequest,
                        clip_id=request_base,
                        start_sec=envelope_start,
                        end_sec=envelope_end,
                        knowledge_type=knowledge_type,
                        semantic_unit_id=unit.unit_id,
                    )
                )
                
                # 首帧截图: 搜索窗口扩大为 包络起点 ±1.0s
                head_search_start, head_search_end = pipeline._clamp_time_range(envelope_start - 1.0, envelope_start + 1.0)
                fallback_head_ts = envelope_start
                head_ts = await pipeline._select_screenshot_timestamp(head_search_start, head_search_end, fallback_head_ts)
                
                screenshot_requests.append(
                    create_screenshot_request(
                        screenshot_request_type=ScreenshotRequest,
                        screenshot_id=f"{request_base}_head",
                        timestamp_sec=head_ts,
                        label="head",
                        semantic_unit_id=unit.unit_id,
                    )
                )
                
                # 稳定岛截图
                for j, island in enumerate(action_internal_islands):
                    island_start = island.get("start", action_start)
                    island_end = island.get("end", action_end)
                    island_mid_fallback = (island_start + island_end) / 2
                    island_start, island_end = pipeline._clamp_time_range(island_start, island_end)
                    island_mid = await pipeline._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                    screenshot_requests.append(
                        create_screenshot_request(
                            screenshot_request_type=ScreenshotRequest,
                            screenshot_id=f"{request_base}_island_{j+1:02d}",
                            timestamp_sec=island_mid,
                            label="stable",
                            semantic_unit_id=unit.unit_id,
                        )
                    )
                
                # 末帧截图: 搜索窗口扩大为 包络终点 ±1.0s
                tail_search_start, tail_search_end = pipeline._clamp_time_range(envelope_end - 1.0, envelope_end + 1.0)
                tail_search_end = min(tail_search_end, float(getattr(unit, "end_sec", tail_search_end)))
                tail_search_start, tail_search_end = pipeline._clamp_time_range(tail_search_start, tail_search_end)
                fallback_tail_ts = envelope_end
                tail_ts = await pipeline._select_screenshot_timestamp(tail_search_start, tail_search_end, fallback_tail_ts)
                
                screenshot_requests.append(
                    create_screenshot_request(
                        screenshot_request_type=ScreenshotRequest,
                        screenshot_id=f"{request_base}_tail",
                        timestamp_sec=tail_ts,
                        label="tail",
                        semantic_unit_id=unit.unit_id,
                    )
                )
    
    elif stable_islands:
        # ==== 无动作单元，仅稳定岛: 提取中间帧 ====
        for i, island in enumerate(stable_islands):
            island_start = island.get("start", unit.start_sec)
            island_end = island.get("end", unit.end_sec)
            island_mid_fallback = (island_start + island_end) / 2
            island_mid = await pipeline._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

            screenshot_requests.append(
                create_screenshot_request(
                    screenshot_request_type=ScreenshotRequest,
                    screenshot_id=pipeline._build_unit_relative_request_id(unit, f"stable_{i+1:02d}"),
                    timestamp_sec=island_mid,
                    label="stable",
                    semantic_unit_id=unit.unit_id,
                )
            )
    
    else:
        # ==== 回退: 无任何检测结果 ====
        fallback_ts = (unit.start_sec + unit.end_sec) / 2
        best_ts = await pipeline._select_screenshot_timestamp(unit.start_sec, unit.end_sec, fallback_ts)
        
        screenshot_requests.append(
            create_screenshot_request(
                screenshot_request_type=ScreenshotRequest,
                screenshot_id=pipeline._build_unit_relative_request_id(unit, "fallback"),
                timestamp_sec=best_ts,
                label="fallback",
                semantic_unit_id=unit.unit_id,
            )
        )
    
    logger.debug(f"{unit.unit_id}: collected {len(screenshot_requests)} screenshot requests, "
                 f"{len(clip_requests)} clip requests")
    
    return MaterialRequests(
        screenshot_requests=screenshot_requests,
        clip_requests=clip_requests,
        action_classifications=action_classifications
    )


def apply_external_materials(
    self,
    unit: SemanticUnit,
    screenshots_dir: str,
    clips_dir: str,
    material_requests: MaterialRequests
):
    """方法说明：RichTextPipeline._apply_external_materials 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    materials = MaterialSet()
    screenshot_paths: List[str] = []
    screenshot_labels: List[str] = []
    screenshot_items: List[Dict[str, Any]] = []
    sentence_timestamps = pipeline._build_sentence_timestamps()

    def _normalize_knowledge_type(raw_type: str) -> str:
        lowered = (raw_type or "").strip().lower()
        if any(key in lowered for key in ["process", "??", "??", "procedural"]):
            return "process"
        if any(key in lowered for key in ["concrete", "??", "??", "??", "??"]):
            return "concrete"
        if any(key in lowered for key in ["abstract", "??", "??", "??", "explanation"]):
            return "abstract"
        return lowered or "abstract"

    normalized_kt = _normalize_knowledge_type(str(getattr(unit, "knowledge_type", "") or ""))
    should_validate_screenshot = normalized_kt in {"abstract", "concrete"}
    allow_clip = normalized_kt == "process"

    def _deduplicate_paths(paths: List[str]) -> List[str]:
        ordered: List[str] = []
        seen: set[str] = set()
        for path_item in paths:
            try:
                key = str(Path(path_item).resolve())
            except Exception:
                key = os.path.abspath(path_item)
            if key in seen:
                continue
            seen.add(key)
            ordered.append(path_item)
        return ordered

    def _collect_candidates_by_id(base_dir: str, req_id: str, exts: List[str]) -> List[str]:
        candidates: List[str] = []
        raw_id = str(req_id or "").strip().replace("\\", "/")
        if not raw_id:
            return candidates

        raw_id = raw_id.strip("/")
        if "/" not in raw_id:
            logger.warning(
                "Skip legacy material request id without unit folder: unit=%s id=%s",
                unit.unit_id,
                req_id,
            )
            return candidates

        raw_path = Path(raw_id)
        base_name = raw_path.name
        stem_name = raw_path.stem if raw_path.suffix else base_name

        checks: List[Path] = [Path(base_dir) / raw_id]
        if raw_path.suffix:
            checks.append(Path(base_dir) / raw_path.parent / base_name)
        for ext in exts:
            checks.append(Path(base_dir) / raw_path.parent / f"{stem_name}{ext}")

        for check in checks:
            if check.exists():
                candidates.append(str(check))
        return _deduplicate_paths(candidates)

    assets_root = Path(pipeline.assets_dir)

    def _normalize_existing_asset_path(source_path: str, kind: str, source_id: str) -> str:
        candidate = Path(source_path)
        if not candidate.exists():
            return ""

        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = Path(os.path.abspath(str(candidate)))

        try:
            resolved.relative_to(assets_root.resolve())
        except Exception:
            logger.warning(
                "Skip %s outside assets in no-copy mode: unit=%s id=%s path=%s",
                kind,
                unit.unit_id,
                source_id,
                source_path,
            )
            return ""

        return str(resolved)

    screenshot_candidates: List[Tuple[str, str, str, Optional[float]]] = []
    request_meta_by_id: Dict[str, ScreenshotRequest] = {}
    if material_requests.screenshot_requests:
        for req in material_requests.screenshot_requests:
            if req.semantic_unit_id != unit.unit_id:
                continue
            req_id = str(req.screenshot_id or "").strip()
            if req_id:
                request_meta_by_id[req_id] = req
            req_paths = _collect_candidates_by_id(
                screenshots_dir,
                req.screenshot_id,
                [".png", ".jpg", ".jpeg"],
            )
            for path_item in req_paths:
                screenshot_candidates.append((path_item, req.label, req.screenshot_id, float(req.timestamp_sec)))

    deduped_screenshot_candidates: List[Tuple[str, str, str, Optional[float]]] = []
    seen_screenshot_paths: set[str] = set()
    for raw_path, label, sid, request_ts in screenshot_candidates:
        try:
            candidate_key = str(Path(raw_path).resolve())
        except Exception:
            candidate_key = os.path.abspath(raw_path)
        if candidate_key in seen_screenshot_paths:
            continue
        seen_screenshot_paths.add(candidate_key)
        deduped_screenshot_candidates.append((raw_path, label, sid, request_ts))
    screenshot_candidates = deduped_screenshot_candidates

    rejected_screenshot_count = 0
    for _idx, (raw_path, label, sid, request_ts) in enumerate(screenshot_candidates, start=1):
        is_valid = True
        img_description = ""

        req_meta = request_meta_by_id.get(str(sid or "").strip())
        if request_ts is None and req_meta is not None:
            try:
                request_ts = float(req_meta.timestamp_sec)
            except Exception:
                request_ts = None

        sentence_id = ""
        sentence_text = ""
        if request_ts is not None:
            sentence_id = pipeline._map_timestamp_to_sentence_id(float(request_ts), sentence_timestamps)
            if sentence_id:
                sentence_text = pipeline._get_sentence_text_by_id(sentence_id)

        if should_validate_screenshot and pipeline._concrete_validator:
            try:
                pre_key = str(Path(raw_path).resolve())
            except Exception:
                pre_key = os.path.abspath(raw_path)

            if pre_key in pipeline._prevalidated_concrete_results:
                res = pipeline._prevalidated_concrete_results[pre_key]
            else:
                res = pipeline._concrete_validator.validate(raw_path)
            img_description = str(getattr(res, "img_description", "") or getattr(res, "reason", "")).strip()
            if not res.should_include:
                logger.info(f"Removing negative screenshot: {sid} ({res.reason})")
                is_valid = False
                rejected_screenshot_count += 1

        if not is_valid:
            continue

        normalized_path = _normalize_existing_asset_path(raw_path, "img", sid or label)
        if not normalized_path:
            continue

        screenshot_paths.append(normalized_path)
        screenshot_labels.append(label or sid)
        resolved_desc = img_description or label or sid
        img_index = len(screenshot_items) + 1
        img_id = f"{unit.unit_id}_img_{img_index:02d}"
        mapping_status = "mapped"
        if request_ts is None:
            mapping_status = "no_timestamp"
        elif not sentence_id:
            mapping_status = "unmapped"

        screenshot_items.append({
            "img_id": img_id,
            "img_path": normalized_path,
            "img_description": resolved_desc,
            "img_desription": resolved_desc,
            "label": label,
            "source_id": sid,
            "timestamp_sec": float(request_ts) if request_ts is not None else None,
            "sentence_id": sentence_id,
            "sentence_text": sentence_text,
        })

        pipeline._record_image_match_audit(
            unit_id=unit.unit_id,
            img_id=img_id,
            source_id=sid,
            timestamp_sec=float(request_ts) if request_ts is not None else None,
            sentence_id=sentence_id,
            sentence_text=sentence_text,
            img_description=resolved_desc,
            mapping_status=mapping_status,
        )

    if should_validate_screenshot:
        logger.info(
            f"{unit.unit_id}: screenshot validation kept={len(screenshot_paths)}, "
            f"rejected={rejected_screenshot_count}"
        )

    clip_paths: List[str] = []
    if allow_clip:
        clip_candidates: List[Tuple[str, str]] = []
        if material_requests.clip_requests:
            for req in material_requests.clip_requests:
                if req.semantic_unit_id != unit.unit_id:
                    continue
                for path_item in _collect_candidates_by_id(clips_dir, req.clip_id, [".mp4", ".webm", ".mkv"]):
                    clip_candidates.append((path_item, req.clip_id))

        deduped_clip_candidates: List[Tuple[str, str]] = []
        seen_clip_paths: set[str] = set()
        for clip_candidate_path, clip_candidate_label in clip_candidates:
            try:
                candidate_key = str(Path(clip_candidate_path).resolve())
            except Exception:
                candidate_key = os.path.abspath(clip_candidate_path)
            if candidate_key in seen_clip_paths:
                continue
            seen_clip_paths.add(candidate_key)
            deduped_clip_candidates.append((clip_candidate_path, clip_candidate_label))
        clip_candidates = deduped_clip_candidates
        for selected, selected_label in clip_candidates:
            normalized_clip_path = _normalize_existing_asset_path(selected, "clip", selected_label)
            if normalized_clip_path:
                clip_paths.append(normalized_clip_path)
    else:
        logger.info(f"Skip clip for non-process unit: {unit.unit_id} ({normalized_kt})")

    materials.screenshot_paths = screenshot_paths
    materials.screenshot_labels = screenshot_labels
    materials.screenshot_items = screenshot_items
    materials.clip_paths = clip_paths
    materials.clip_path = clip_paths[0] if clip_paths else ""
    materials.action_classifications = material_requests.action_classifications

    unit.materials = materials

    logger.debug(
        f"{unit.unit_id}: applied {len(screenshot_paths)} external screenshots, "
        f"clips={len(clip_paths)}"
    )
    if not screenshot_paths and not clip_paths:
        logger.warning(f"{unit.unit_id}: no external materials matched in Phase2B")

