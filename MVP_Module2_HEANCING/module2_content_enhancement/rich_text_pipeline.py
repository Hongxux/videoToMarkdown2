"""
模块说明：Module2 内容增强中的 rich_text_pipeline 模块。
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
import difflib
import cv2
import json
import logging
import asyncio
import yaml
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict

from .data_loader import load_corrected_subtitles, load_merged_segments
from .semantic_unit_segmenter import SemanticUnitSegmenter, SemanticUnit
from .cv_knowledge_validator import CVKnowledgeValidator
from .concrete_knowledge_validator import ConcreteKnowledgeValidator
from .screenshot_selector import ScreenshotSelector
from .knowledge_classifier import KnowledgeClassifier
from .resource_manager import get_resource_manager, get_io_executor
from .rich_text_document import (
    RichTextDocument, 
    RichTextSection, 
    MaterialSet, 
    create_section_from_semantic_unit
)

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """
    类说明：封装 PipelineConfig 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    # 素材生成
    screenshot_quality: int = 95              # JPEG 质量
    clip_crf: int = 23                        # 视频压缩质量 (越小越好)
    
    # 采样
    head_offset_sec: float = 0.2              # 首帧偏移
    tail_offset_sec: float = 0.2              # 末帧偏移
    
    # ScreenshotSelector 配置
    screenshot_sample_interval: float = 0.5   # 候选帧采样间隔
    
    # 输出
    assets_subdir: str = "assets"             # 素材子目录名


@dataclass
class ScreenshotRequest:
    """
    类说明：封装 ScreenshotRequest 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    screenshot_id: str          # 截图ID，如 "SU001_action_1_head"
    timestamp_sec: float        # 截图时间点（秒）
    label: str                  # 标签 (head/tail/stable/fallback)
    semantic_unit_id: str       # 所属语义单元ID


@dataclass
class ClipRequest:
    """
    类说明：封装 ClipRequest 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    clip_id: str                # 切片ID，如 "SU001_action_1"
    start_sec: float            # 起始时间（秒）
    end_sec: float              # 结束时间（秒）
    knowledge_type: str         # 知识类型 (过程性/讲解型/结构性/概念性)
    semantic_unit_id: str       # 所属语义单元ID
    segments: Optional[List[Dict[str, float]]] = None  # 多段拼接切片（用于 VL 多段合并）


@dataclass
class MaterialRequests:
    """
    类说明：封装 MaterialRequests 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    screenshot_requests: List[ScreenshotRequest]
    clip_requests: List[ClipRequest]
    action_classifications: List[Dict[str, Any]]  # 动作分类结果


class RichTextPipeline:
    """
    类说明：封装 RichTextPipeline 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    def __init__(
        self,
        video_path: str,
        step2_path: str,
        step6_path: str,
        output_dir: str,
        config: PipelineConfig = None,
        sentence_timestamps_path: str = None,
        segmenter: SemanticUnitSegmenter = None,
    ):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：step2_path
        - 条件：step6_path
        - 条件：self._knowledge_classifier.enabled
        依据来源（证据链）：
        - 输入参数：step2_path, step6_path。
        - 对象内部状态：self._concrete_validator, self._knowledge_classifier。
        输入参数：
        - video_path: 文件路径（类型：str）。
        - step2_path: 文件路径（类型：str）。
        - step6_path: 文件路径（类型：str）。
        - output_dir: 目录路径（类型：str）。
        - config: 配置对象/字典（类型：PipelineConfig）。
        - sentence_timestamps_path: 文件路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.video_path = video_path
        self.output_dir = output_dir
        self.config = config or PipelineConfig()

        # 先解析可用的中间产物路径：显式传参优先，其次自动回退到 output_dir/intermediates
        self.step2_path = self._resolve_intermediate_path(
            provided_path=step2_path,
            candidate_names=[
                "step2_correction_output.json",
                "step2_output.json",
            ],
        )
        self.step6_path = self._resolve_intermediate_path(
            provided_path=step6_path,
            candidate_names=[
                "step6_merge_cross_output.json",
                "step6_output.json",
            ],
        )
        self.sentence_timestamps_path = self._resolve_intermediate_path(
            provided_path=sentence_timestamps_path,
            candidate_names=["sentence_timestamps.json"],
        )
        
        # 创建输出目录
        self.assets_dir = os.path.join(output_dir, self.config.assets_subdir)
        Path(self.assets_dir).mkdir(parents=True, exist_ok=True)
        
        # 加载数据 (Phase 2B 组装模式下可能为空)
        if self.step2_path:
            logger.info(f"Loading step2: {self.step2_path}")
            self.subtitles = load_corrected_subtitles(self.step2_path)
        else:
            logger.info("Skip loading step2 (empty path)")
            self.subtitles = []

        if self.step6_path:
            logger.info(f"Loading step6: {self.step6_path}")
            self.paragraphs = self._load_paragraphs(self.step6_path)
        else:
            logger.info("Skip loading step6 (empty path)")
            self.paragraphs = []
        
        # 初始化组件（优先注入单例，避免热路径重复构建）
        self.segmenter = segmenter if segmenter is not None else SemanticUnitSegmenter()
        
        # ScreenshotSelector (懒加载，需要 visual_extractor 依赖)
        self._screenshot_selector = None
        
        # VideoClipExtractor (懒加载，需要依赖注入)
        self._clip_extractor = None
        
        # 视觉特征提取器 (共享依赖)
        self._visual_extractor = None
        
        # 获取视频信息
        self.video_duration = self._get_video_duration()
        
        # 💥 V7.4: 知识分类器 (用于动作单元四分类)
        # 💥 V7.4: KnowledgeClassifier 直接从 Step2 读取字幕，需显式注入 step2_path（避免空字幕导致分类质量退化）
        self._knowledge_classifier = KnowledgeClassifier(step2_path=self.step2_path)
        if self._knowledge_classifier.enabled:
            logger.info("Knowledge classifier enabled (DeepSeek API)")
        else:
            logger.warning("Knowledge classifier disabled (API key not set)")
            
        # 💥 V7.5: 具象知识验证器 (用于过滤无效截图)
        self._concrete_validator = ConcreteKnowledgeValidator(output_dir=self.output_dir)
        if self._concrete_validator.enabled:
            logger.info("Concrete knowledge validator enabled (CV/Vision)")

        # 指代断层预补全阶段复用缓存（image_abs_path -> ConcreteKnowledgeResult）
        self._prevalidated_concrete_results: Dict[str, Any] = {}

        # 图片匹配审计：默认关闭，可通过 config.yaml / 环境变量开启
        self._image_match_audit_enabled = self._load_image_match_audit_switch(default_value=False)
        self._image_match_audit_records: List[Dict[str, Any]] = []
        
        logger.info(f"Pipeline initialized: {len(self.subtitles)} subtitles, {len(self.paragraphs)} paragraphs")

    def _resolve_config_path(self) -> Optional[Path]:
        env_path = str(os.getenv("MODULE2_CONFIG_PATH", "") or "").strip()
        if env_path:
            candidate = Path(env_path)
            if candidate.exists():
                return candidate
            logger.warning(f"MODULE2_CONFIG_PATH not found: {candidate}")

        project_root = Path(__file__).parent.parent.parent
        candidates = [
            project_root / "config.yaml",
            project_root / "videoToMarkdown" / "config.yaml",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    @staticmethod
    def _parse_bool(value: Any, default: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            raw = value.strip().lower()
            if raw in ("1", "true", "yes", "y", "on"):
                return True
            if raw in ("0", "false", "no", "n", "off"):
                return False
        return bool(default)

    def _load_image_match_audit_switch(self, default_value: bool = False) -> bool:
        enabled = bool(default_value)

        config_path = self._resolve_config_path()
        if config_path is not None:
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
                module2_cfg = config.get("module2", {}) if isinstance(config, dict) else {}
                observability_cfg = module2_cfg.get("observability", {}) if isinstance(module2_cfg, dict) else {}
                image_cfg = observability_cfg.get("image_match_audit", {}) if isinstance(observability_cfg, dict) else {}
                enabled = self._parse_bool(image_cfg.get("enabled", enabled), enabled)
            except Exception as exc:
                logger.warning(f"Failed to load image-match-audit switch from config: {exc}")

        env_raw = os.getenv("MODULE2_IMAGE_MATCH_AUDIT_ENABLED")
        if env_raw is not None and str(env_raw).strip() != "":
            enabled = self._parse_bool(env_raw, enabled)

        return enabled

    def _record_image_match_audit(
        self,
        *,
        unit_id: str,
        img_id: str,
        source_id: str,
        timestamp_sec: Optional[float],
        sentence_id: str,
        sentence_text: str,
        img_description: str,
        mapping_status: str,
    ) -> None:
        if not self._image_match_audit_enabled:
            return

        self._image_match_audit_records.append(
            {
                "unit_id": str(unit_id or ""),
                "img_id": str(img_id or ""),
                "source_id": str(source_id or ""),
                "timestamp_sec": float(timestamp_sec) if timestamp_sec is not None else None,
                "sentence_id": str(sentence_id or ""),
                "sentence_text": str(sentence_text or ""),
                "img_description": str(img_description or ""),
                "mapping_status": str(mapping_status or ""),
            }
        )

    def _flush_image_match_audit(self) -> str:
        if not self._image_match_audit_enabled:
            return ""

        output_path = Path(self.output_dir) / "intermediates" / "phase2b_image_match_audit.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as file_obj:
            json.dump(
                {
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "total_records": len(self._image_match_audit_records),
                    "records": self._image_match_audit_records,
                },
                file_obj,
                ensure_ascii=False,
                indent=2,
            )
        return str(output_path)

    def _resolve_intermediate_path(self, provided_path: Optional[str], candidate_names: List[str]) -> str:
        """解析中间产物路径：显式路径优先，其次 output_dir/intermediates 回退。"""
        raw = str(provided_path or "").strip()
        if raw:
            if os.path.exists(raw):
                return raw
            logger.warning(f"Provided intermediate path not found: {raw}")

        candidates: List[Path] = []
        base_dir = Path(self.output_dir)
        intermediates_dir = base_dir / "intermediates"
        for name in candidate_names:
            if not name:
                continue
            candidates.append(intermediates_dir / name)
            candidates.append(base_dir / name)

        for candidate in candidates:
            if candidate.exists():
                logger.info(f"Auto-discovered intermediate file: {candidate}")
                return str(candidate)

        return ""
    
    def set_visual_extractor(self, visual_extractor):
        """
        执行逻辑：
        1) 校验输入值。
        2) 更新内部状态或持久化。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：集中更新状态，保证一致性。
        输入参数：
        - visual_extractor: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self._visual_extractor = visual_extractor
        # 初始化 ScreenshotSelector
        self._screenshot_selector = ScreenshotSelector(
            visual_extractor=visual_extractor,
            config=None  # 使用默认配置
        )
    
    def set_clip_extractor(self, extractor):
        """
        执行逻辑：
        1) 校验输入值。
        2) 更新内部状态或持久化。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：集中更新状态，保证一致性。
        输入参数：
        - extractor: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self._clip_extractor = extractor
        self._clip_extractor.set_subtitles(self.subtitles)
    

    def _slugify_text(self, value: str, max_len: int = 48) -> str:
        """
        ??????????????????

        ???
        - ?????????????????????
        - ?????????????????? max_len?
        """
        raw = str(value or "").strip().lower()
        if not raw:
            return "item"

        normalized: List[str] = []
        for ch in raw:
            if ch.isalnum():
                normalized.append(ch)
            else:
                normalized.append("_")

        slug = "".join(normalized)
        while "__" in slug:
            slug = slug.replace("__", "_")
        slug = slug.strip("_")
        if not slug:
            slug = "item"

        if len(slug) > max_len:
            slug = slug[:max_len].rstrip("_")
        return slug or "item"

    def _build_unit_asset_prefix(self, unit: SemanticUnit) -> str:
        """
        ???????????`{unit_id}_{title_slug}`?
        """
        unit_title = str(
            getattr(unit, "knowledge_topic", "")
            or getattr(unit, "title", "")
            or getattr(unit, "full_text", "")
        ).strip()
        title_slug = self._slugify_text(unit_title, max_len=40)
        return f"{unit.unit_id}_{title_slug}"

    def _build_action_brief(self, action: Dict[str, Any], classification: Dict[str, Any], index: int) -> str:
        """
        ?????????????????
        """
        candidates = [
            classification.get("description", "") if isinstance(classification, dict) else "",
            classification.get("subject", "") if isinstance(classification, dict) else "",
            action.get("description", "") if isinstance(action, dict) else "",
            action.get("type", "") if isinstance(action, dict) else "",
        ]
        for item in candidates:
            slug = self._slugify_text(str(item or ""), max_len=36)
            if slug and slug != "item":
                return slug
        return f"action_{index:02d}"

    def _build_request_base_name(self, unit: SemanticUnit, suffix: str) -> str:
        """
        ???? ID / ???????????? + ???????????
        """
        return f"{self._build_unit_asset_prefix(unit)}_{self._slugify_text(suffix, max_len=48)}"

    def _build_unit_relative_request_id(self, unit: SemanticUnit, suffix: str) -> str:
        """
        生成用于外部提取阶段的相对路径 ID，确保素材在提取时直接写入 `assets/{unit_id}/`。

        为什么：Phase2A 先生成请求 ID，Java/FFmpeg 按该 ID 落盘；若 ID 不带语义单元目录，
        会导致素材扁平化堆叠，Phase2B 再匹配时需要大量兜底逻辑，且易串单元。
        """
        return f"{unit.unit_id}/{self._build_request_base_name(unit, suffix)}"

    def _resolve_asset_output_path(self, name: str, ext: str) -> str:
        """
        ????????? assets ???????????????
        """
        clean_name = str(name or "").strip().replace("\\", "/").strip("/")
        if clean_name.lower().endswith(f".{ext.lower()}"):
            clean_name = clean_name[: -(len(ext) + 1)]
        abs_path = Path(self.assets_dir) / f"{clean_name}.{ext}"
        abs_path.parent.mkdir(parents=True, exist_ok=True)
        return str(abs_path)

    def _align_to_sentence_start(self, timestamp: float) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：sub.start_sec <= timestamp
        依据来源（证据链）：
        - 输入参数：timestamp。
        输入参数：
        - timestamp: 函数入参（类型：float）。
        输出参数：
        - 数值型计算结果。"""
        best_start = 0.0
        for sub in self.subtitles:
            if sub.start_sec <= timestamp:
                best_start = sub.start_sec
            else:
                break
        return best_start
    
    def _align_to_sentence_end(self, timestamp: float) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：sub.end_sec >= timestamp
        - 条件：self.subtitles
        依据来源（证据链）：
        - 输入参数：timestamp。
        - 对象内部状态：self.subtitles。
        输入参数：
        - timestamp: 函数入参（类型：float）。
        输出参数：
        - 数值型计算结果。"""
        for sub in self.subtitles:
            if sub.end_sec >= timestamp:
                return sub.end_sec
        # 如果没找到，返回最后一个字幕的结束时间
        return self.subtitles[-1].end_sec if self.subtitles else timestamp

    def _clamp_time_range(self, start_sec: float, end_sec: float) -> Tuple[float, float]:
        """
        做什么：对时间区间做安全裁剪与归一化（对齐 visual_feature_extractor.py 的边界策略）。
        为什么：动作包络扩边/整段回退可能产生负数/越界/反向区间，导致 FFmpeg/OpenCV 读帧失败或空素材。
        权衡：当 video_duration 不可用时，仅保证 start>=0 且 end>=start，无法完全阻止越界，但不破坏主流程。
        """
        try:
            start = float(start_sec)
        except Exception:
            start = 0.0

        try:
            end = float(end_sec)
        except Exception:
            end = start

        # 先做基本约束，避免反向区间
        start = max(0.0, start)
        end = max(start, end)

        # 再做视频边界裁剪（尽量模拟 visual_feature_extractor.py 对帧边界的处理）
        if getattr(self, "video_duration", 0) and self.video_duration > 0:
            max_end = float(self.video_duration)
            start = max(0.0, min(start, max_end))
            end = max(start, min(end, max_end))

        return start, end

    def _merge_action_segments(
        self,
        action_segments: List[Dict[str, Any]],
        gap_threshold_sec: float = 5.0
    ) -> List[Dict[str, Any]]:
        """
        做什么：将同一语义单元内、间隔小于阈值的多个动作段合并为一个动作段。
        为什么：当前链路最终只保留第一个 clip（materials.clip_path），动作被切碎会导致“只截到其中一段”。
        权衡：阈值放宽会让 clip 变长并包含更多间隙，但可换取动作语义单元的完整性。
        """
        if not action_segments:
            return []
        if len(action_segments) == 1:
            return [action_segments[0].copy()]

        merged_actions: List[Dict[str, Any]] = []
        current = action_segments[0].copy()

        for next_action in action_segments[1:]:
            next_copy = next_action.copy()
            gap = float(next_copy.get("start_sec", 0)) - float(current.get("end_sec", 0))

            if gap < gap_threshold_sec:
                # 扩展当前动作的结束时间
                current["end_sec"] = max(float(current.get("end_sec", 0)), float(next_copy.get("end_sec", 0)))
                # 合并内部稳定岛
                current_islands = current.get("internal_stable_islands", [])
                next_islands = next_copy.get("internal_stable_islands", [])
                current["internal_stable_islands"] = current_islands + next_islands
            else:
                merged_actions.append(current)
                current = next_copy

        merged_actions.append(current)
        return merged_actions

    def _compute_action_envelope(
        self,
        unit: SemanticUnit,
        action_start: float,
        action_end: float,
        sentence_start: float,
        sentence_end: float,
        knowledge_type: str,
        short_unit_threshold_sec: float = 20.0,
        pre_buffer_sec: float = 0.4,
        post_buffer_sec: float = 1.0
    ) -> Tuple[float, float]:
        """
        做什么：根据知识类型计算动作截取范围（Adaptive Action Envelope）。
        为什么：对“实操/推演/配置”类动作，需覆盖定位准备→执行→结果确认，避免只截到像素变化瞬间。
        权衡：短单元整段会增加片段长度；长单元扩边可能引入少量非核心画面，但提升闭环可理解性。
        """
        k_type = (knowledge_type or "").strip()
        target_keywords = ("实操", "推演", "环境配置", "配置")
        is_target_type = any(kw in k_type for kw in target_keywords)
        logger.info("k_type: {}, is_target_type: {}".format(k_type, is_target_type))
        unit_start = float(getattr(unit, "start_sec", 0.0))
        unit_end = float(getattr(unit, "end_sec", 0.0))
        unit_duration = unit_end - unit_start

        # Sentence：与动作重叠的那句（由 _align_to_sentence_* 在字幕序列中定位）
        base_start = min(float(action_start), float(sentence_start))
        base_end = max(float(action_end), float(sentence_end))

        # 1) 短单元：直接取整段
        if unit_duration > 0 and unit_duration <= short_unit_threshold_sec and is_target_type:
            start_sec, end_sec = unit_start, unit_end
        elif is_target_type:
            # 2) 长单元：在 Union(Action, Sentence) 基础上扩边
            start_sec = base_start - pre_buffer_sec
            end_sec = base_end + post_buffer_sec
        else:
            # 非目标类型：保持原策略，仅做句子边界对齐
            start_sec, end_sec = base_start, base_end

        # 约束：暂不跨越下一个语义单元（结束时间不超过 unit.end_sec）
        end_sec = min(end_sec, unit_end)

        # 最终安全裁剪（视频边界 + 反向区间）
        return self._clamp_time_range(start_sec, end_sec)
    
    def _load_paragraphs(self, step6_path: str) -> List[Dict]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：'output' in data and 'pure_text_script' in data['output']
        - 条件：'pure_text_script' in data
        依据来源（证据链）：
        - 配置字段：output。
        输入参数：
        - step6_path: 文件路径（类型：str）。
        输出参数：
        - Dict 列表（与输入或处理结果一一对应）。"""
        with open(step6_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if "output" in data and "pure_text_script" in data["output"]:
            return data["output"]["pure_text_script"]
        elif "pure_text_script" in data:
            return data["pure_text_script"]
        else:
            raise ValueError("Invalid step6 format: missing pure_text_script")
    
    def _get_video_duration(self) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、OpenCV 图像处理实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not cap.isOpened()
        - 条件：fps > 0
        依据来源（证据链）：
        输入参数：
        - 无。
        输出参数：
        - 数值型计算结果。"""
        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            return 0.0
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        cap.release()
        return frame_count / fps if fps > 0 else 0.0
    
    async def run(self, title: str = "") -> RichTextDocument:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - title: 函数入参（类型：str）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        logger.info("="*60)
        logger.info("Starting Rich Text Pipeline V2")
        logger.info("="*60)
        
        # Stage 1: 语义单元切分
        logger.info("[Stage 1] Semantic Unit Segmentation")
        sentence_timestamps = self._build_sentence_timestamps()
        
        # 🚀 缓存路径: 保存到 storage/{task_id}/intermediates/
        intermediates_dir = os.path.join(os.path.dirname(self.output_dir), "intermediates")
        os.makedirs(intermediates_dir, exist_ok=True)
        segment_cache = os.path.join(intermediates_dir, "semantic_segmentation_cache.json")
        modality_cache = os.path.join(intermediates_dir, "modality_classification_cache.json")
        
        result = await self.segmenter.segment(
            self.paragraphs, 
            sentence_timestamps,
            cache_path=segment_cache
        )
        units = result.semantic_units
        logger.info(f"  → {len(units)} semantic units created")
        

        
        # Stage 3: 素材生成 (并行优化)
        logger.info("[Stage 3] Material Generation (Parallel)")
        await self._generate_materials_parallel(units)
        
        # Stage 4: 富文本组装
        logger.info("[Stage 4] Rich Text Assembly")
        document = self._assemble_document(units, title)
        
        logger.info("="*60)
        logger.info(f"Pipeline completed: {len(document.sections)} sections")
        logger.info("="*60)
        
        return document
    
    # =========================================================================
    # 🔑 gRPC 入口方法 (供Java编排调用)
    # =========================================================================
    
    async def analyze_only(self) -> Tuple[List[ScreenshotRequest], List[ClipRequest], str]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、HTTP 调用、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - List[ScreenshotRequest], List[ClipRequest], str 列表（与输入或处理结果一一对应）。"""
        import os
        logger.info("="*60)
        logger.info("RichTextPipeline.analyze_only() - Phase2A (Segmentation only)")
        logger.info("="*60)
        
        # Stage 1: 语义单元切分
        logger.info("[Phase2A-1] Semantic Unit Segmentation")
        sentence_timestamps = self._build_sentence_timestamps()
        
        # 🚀 缓存路径
        intermediates_dir = os.path.join(self.output_dir, "intermediates")
        os.makedirs(intermediates_dir, exist_ok=True)
        segment_cache = os.path.join(intermediates_dir, "semantic_segmentation_cache.json")
        
        result = await self.segmenter.segment(
            self.paragraphs, 
            sentence_timestamps, 
            cache_path=segment_cache
        )
        units = result.semantic_units
        logger.info(f"  → {len(units)} semantic units created")
        
        # 🚀 Phase2A 现在跳过 Modality Classification 和 Material Requests
        # 这些任务已拆分到 ValidateCVBatch 和 GenerateMaterialRequests
        all_screenshot_requests: List[ScreenshotRequest] = []
        all_clip_requests: List[ClipRequest] = []
        
        # Stage 4: 保存中间结果
        semantic_units_path = os.path.join(self.output_dir, "semantic_units_phase2a.json")
        self._save_semantic_units(units, semantic_units_path)
        
        logger.info("="*60)
        logger.info(f"Phase2A completed (Segmentation only): {semantic_units_path}")
        logger.info("="*60)
        
        return all_screenshot_requests, all_clip_requests, semantic_units_path
    
    async def assemble_only(
        self,
        semantic_units_json_path: str,
        screenshots_dir: str,
        clips_dir: str,
        title: str = "视频内容",
        subject: str = "数据结构与算法"
    ) -> Tuple[str, str]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、HTTP 调用、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：os.path.exists(cv_results_path)
        - 条件：enhancer.enabled
        - 条件：requests
        依据来源（证据链）：
        输入参数：
        - semantic_units_json_path: 文件路径（类型：str）。
        - screenshots_dir: 目录路径（类型：str）。
        - clips_dir: 目录路径（类型：str）。
        - title: 函数入参（类型：str）。
        - subject: 函数入参（类型：str）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
        from .markdown_enhancer import MarkdownEnhancer
        
        logger.info("="*60)
        logger.info("RichTextPipeline.assemble_only() - Phase2B Start")
        logger.info("="*60)
        if self._image_match_audit_enabled:
            self._image_match_audit_records.clear()
        
        # Stage 1: 加载Phase2A保存的语义单元
        logger.info("[Phase2B-1] Load Semantic Units")
        units, material_requests_map = self._load_semantic_units(semantic_units_json_path)
        logger.info(f"  → Loaded {len(units)} semantic units")
        
        # 🚀 Stage 1.5: 合并外部 CV 验证结果 (来自 Java 并行编排)
        cv_results_path = semantic_units_json_path.replace(".json", ".cv_results.json")
        if os.path.exists(cv_results_path):
            logger.info(f"[Phase2B-1.5] Merging External CV Results from {cv_results_path}")
            self._merge_cv_results(units, cv_results_path)
        else:
            logger.warning(f"[Phase2B-1.5] External CV Results not found at {cv_results_path}, using existing modality")
        
        # Stage 2: 应用外部素材
        logger.info("[Phase2B-2] Apply External Materials")

        for unit in units:
            requests = material_requests_map.get(unit.unit_id)
            if not requests:
                requests = MaterialRequests([], [], [])
            self._apply_external_materials(unit, screenshots_dir, clips_dir, requests)
        
        # Stage 3: 富文本组装 (生成基础文档)
        logger.info("[Phase2B-3] Rich Text Assembly")
        document = self._assemble_document(units, title)
        
        # Stage 4: 导出 result.json
        logger.info("[Phase2B-4] Export result.json")
        json_path = os.path.join(self.output_dir, "result.json")
        document.to_json(json_path)
        
        # Stage 5: 调用 MarkdownEnhancer 增强 (模拟测试调用逻辑)
        logger.info("[Phase2B-5] Markdown Enhancement")
        enhancer = MarkdownEnhancer()
        markdown_path = os.path.join(self.output_dir, "enhanced_output.md")
        
        if not enhancer.enabled:
            logger.warning("  → MarkdownEnhancer disabled (DEEPSEEK_API_KEY not set), using rule-based hierarchy")
        try:
            # 🚀 Stage 5: 直接 await 异步增强
            enhanced_md = await enhancer.enhance(
                json_path,
                subject,
                markdown_dir=os.path.dirname(markdown_path)
            )
            with open(markdown_path, 'w', encoding='utf-8') as f:
                f.write(enhanced_md)
            logger.info(f"  → Enhanced markdown exported: {markdown_path}")
        except Exception as e:
            logger.error(f"  → Markdown enhancement failed: {e}, using fallback")
            # 回退: 使用基础Markdown
            document.to_markdown(markdown_path)
        
        logger.info("="*60)
        logger.info(f"Phase2B completed: {len(document.sections)} sections")
        logger.info(f"  → Markdown: {markdown_path}")
        logger.info(f"  → JSON: {json_path}")
        if self._image_match_audit_enabled:
            audit_path = self._flush_image_match_audit()
            if audit_path:
                logger.info(f"  → Image match audit: {audit_path}")
        logger.info("="*60)

        return markdown_path, json_path
    
    def _assemble_document(self, units, title: str):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：materials is None
        依据来源（证据链）：
        输入参数：
        - units: 函数入参（类型：未标注）。
        - title: 函数入参（类型：str）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        doc = RichTextDocument(
            title=title,
            source_video=self.video_path,
            total_duration_sec=self._get_video_duration()
        )
        
        for unit in units:
            # 使用 unit.materials 或创建空的 MaterialSet
            materials = getattr(unit, 'materials', None)
            if materials is None:
                materials = MaterialSet()
            
            # 创建 section，确保 title 使用 knowledge_topic
            section = create_section_from_semantic_unit(unit, materials)
            doc.add_section(section)
        
        return doc
    
    def _save_semantic_units(self, units: List[SemanticUnit], output_path: str):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、HTTP 调用、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：hasattr(unit, '_material_requests')
        依据来源（证据链）：
        输入参数：
        - units: 函数入参（类型：List[SemanticUnit]）。
        - output_path: 文件路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        data = []
        for unit in units:
            # 基础字段
            unit_data = {
                "unit_id": unit.unit_id,
                "start_sec": unit.start_sec,
                "end_sec": unit.end_sec,
                "full_text": getattr(unit, 'full_text', ''),
                "text": getattr(unit, 'full_text', ''),  # 兼容性字段
                "stable_islands": getattr(unit, 'stable_islands', []),
                "action_segments": getattr(unit, 'action_segments', []),
                # 保存素材需求 (用于Phase2B匹配外部素材)
                "material_requests": {
                    "screenshot_requests": [
                        {"screenshot_id": r.screenshot_id, "timestamp_sec": r.timestamp_sec, 
                         "label": r.label, "semantic_unit_id": r.semantic_unit_id}
                        for r in getattr(unit, '_material_requests', MaterialRequests([], [], [])).screenshot_requests
                    ] if hasattr(unit, '_material_requests') else [],
                    "clip_requests": [
                        {
                            **{
                                "clip_id": r.clip_id,
                                "start_sec": r.start_sec,
                                "end_sec": r.end_sec,
                                "knowledge_type": r.knowledge_type,
                                "semantic_unit_id": r.semantic_unit_id
                            },
                            **({"segments": r.segments} if getattr(r, "segments", None) else {})
                        }
                        for r in getattr(unit, '_material_requests', MaterialRequests([], [], [])).clip_requests
                    ] if hasattr(unit, '_material_requests') else [],
                },
                # V9.0 新增字段
                "knowledge_type": getattr(unit, 'knowledge_type', ''),
                "knowledge_topic": getattr(unit, 'knowledge_topic', ''),
                "mult_steps": getattr(unit, 'mult_steps', False),
                "cv_validated": getattr(unit, 'cv_validated', False),
                "instructional_steps": getattr(unit, 'instructional_steps', []),
                # V9.0: 带有 LLM 分类结果的动作单元列表
                "action_units": getattr(unit, 'action_units', []),
                # V9.0: 两阶段合并过程中被跨越的稳定岛
                "crossed_stable_islands": getattr(unit, 'crossed_stable_islands', {
                    "stage1": [],
                    "stage2": []
                }),
            }
            data.append(unit_data)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved {len(units)} semantic units to {output_path}")
    
    def _load_semantic_units(
        self, 
        json_path: str
    ) -> Tuple[List[SemanticUnit], Dict[str, MaterialRequests]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、HTTP 调用、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - json_path: 文件路径（类型：str）。
        输出参数：
        - List[SemanticUnit], Dict[str, MaterialRequests] 列表（与输入或处理结果一一对应）。"""
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        units = []
        material_requests_map: Dict[str, MaterialRequests] = {}
        
        for item in data:
            unit = SemanticUnit(
                unit_id=item["unit_id"],
                knowledge_type=item.get("knowledge_type", "abstract"),
                knowledge_topic=item.get("knowledge_topic", "未知主题"),
                full_text=item.get("full_text", item.get("text", "")),
                source_paragraph_ids=item.get("source_paragraph_ids", []),
                source_sentence_ids=item.get("source_sentence_ids", []),
                start_sec=item["start_sec"],
                end_sec=item["end_sec"],
                mult_steps=item.get("mult_steps", False)
            )
            unit.stable_islands = item.get("stable_islands", [])
            unit.action_segments = item.get("action_segments", [])
            
            # V9.0: 恢复新字段
            unit.instructional_steps = item.get("instructional_steps", [])
            unit.cv_validated = item.get("cv_validated", False)
            unit.action_units = item.get("action_units", [])
            unit.crossed_stable_islands = item.get("crossed_stable_islands", {
                "stage1": [],
                "stage2": []
            })
            
            # 恢复素材需求
            mr_data = item.get("material_requests", {})
            screenshot_requests = [
                ScreenshotRequest(**sr) for sr in mr_data.get("screenshot_requests", [])
            ]
            clip_requests = [
                ClipRequest(**cr) for cr in mr_data.get("clip_requests", [])
            ]
            material_requests_map[unit.unit_id] = MaterialRequests(
                screenshot_requests=screenshot_requests,
                clip_requests=clip_requests,
                action_classifications=[]
            )
            
            units.append(unit)
        
        return units, material_requests_map

    def _merge_cv_results(self, units: List[SemanticUnit], cv_results_path: str):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：unit_id in unit_map
        依据来源（证据链）：
        输入参数：
        - units: 函数入参（类型：List[SemanticUnit]）。
        - cv_results_path: 文件路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        try:
            with open(cv_results_path, 'r', encoding='utf-8') as f:
                cv_results = json.load(f)
            
            unit_map = {u.unit_id: u for u in units}
            merged_count = 0
            
            for unit_id, result_data in cv_results.items():
                if unit_id in unit_map:
                    unit = unit_map[unit_id]
                    
                    # 更新稳定岛 (从驼峰转下划线)
                    pb_islands = result_data.get("stableIslands", [])
                    unit.stable_islands = [
                        {
                            "start_sec": i.get("startSec", 0),
                            "end_sec": i.get("endSec", 0),
                            "mid_sec": i.get("midSec", 0),
                            "duration_sec": i.get("durationSec", 0)
                        } for i in pb_islands
                    ]
                    
                    # 更新动作单元
                    pb_actions = result_data.get("actionSegments", [])
                    unit.action_segments = [
                        {
                            "start_sec": a.get("startSec", 0),
                            "end_sec": a.get("endSec", 0),
                            "modality": a.get("actionType", "unknown"),
                            "stable_islands": [
                                {
                                    "start_sec": si.get("startSec", 0),
                                    "end_sec": si.get("endSec", 0),
                                    "mid_sec": si.get("midSec", 0),
                                    "duration_sec": si.get("durationSec", 0)
                                } for si in a.get("internalStableIslands", [])
                            ]
                        } for a in pb_actions
                    ]
                    
                    merged_count += 1
            
            logger.info(f"  → Successfully merged {merged_count} CV results")
            
        except Exception as e:
            logger.error(f"  → Failed to merge CV results: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _classify_and_filter_actions(
        self, 
        units: List[SemanticUnit],
        classifier: 'KnowledgeClassifier'
    ) -> Dict[str, Dict]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not action_segments
        - 条件：not is_explainable and (not is_noise)
        - 条件：i < len(classification_results)
        依据来源（证据链）：
        输入参数：
        - units: 函数入参（类型：List[SemanticUnit]）。
        - classifier: 函数入参（类型：'KnowledgeClassifier'）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。
        补充说明：
        流程：
        1. 第一阶段合并（所有 ActionUnit，间隔 < 1s）
        2. LLM 分类（过程性知识/讲解型/结构性/概念性）
        3. 过滤：只保留过程性知识、实操、推演（不为视频生成讲解型）
        4. 第二阶段合并（筛选后的 ActionUnit，相同 knowledge_type，间隔 < 5s）
        5. 收集所有稳定岛用于截图提取
        units: 语义单元列表（已合并 CV 结果）
        classifier: KnowledgeClassifier 实例
        'clip_actions': [...],      # 需要生成视频的 ActionUnit
        'all_stable_islands': [...], # 所有稳定岛用于截图"""
        from .screenshot_range_calculator import ScreenshotRangeCalculator
        
        results = {}
        STAGE1_GAP_THRESHOLD = 1.0  # 第一阶段：1秒
        STAGE2_GAP_THRESHOLD = 5.0  # 第二阶段：5秒
        
        for unit in units:
            unit_id = unit.unit_id
            action_segments = unit.action_segments or []
            stable_islands = unit.stable_islands or []
            
            if not action_segments:
                # 无动作单元，只用稳定岛生成截图
                results[unit_id] = {
                    'clip_actions': [],
                    'all_stable_islands': stable_islands,
                    'crossed_islands_stage1': [],
                    'crossed_islands_stage2': []
                }
                continue
            
            # ==== 第一阶段合并（所有 ActionUnit，间隔 < 1s）====
            sorted_actions = sorted(action_segments, key=lambda x: x.get('start_sec', 0))
            merged_stage1, crossed_stage1 = self._merge_actions_local(
                sorted_actions, stable_islands, STAGE1_GAP_THRESHOLD
            )
            
            logger.debug(f"[{unit_id}] Stage1 merge: {len(action_segments)} → {len(merged_stage1)} actions")
            
            # ==== LLM 分类 ====
            try:
                # 为每个动作单元准备字幕
                subtitles = self.subtitles  # 已加载的字幕
                classification_results = await classifier.classify_batch(
                    semantic_unit_title=getattr(unit, "knowledge_topic", "未知主题"),
                    semantic_unit_text=getattr(unit, "full_text", ""),
                    action_segments=[
                        {"start": a.get('start_sec', 0), "end": a.get('end_sec', 0), "id": f"action_{i}"}
                        for i, a in enumerate(merged_stage1)
                    ],
                    subtitles=[
                        {"start_sec": s.start_sec, "end_sec": s.end_sec, "corrected_text": s.corrected_text}
                        for s in subtitles
                    ] if hasattr(subtitles[0], 'start_sec') else subtitles
                )
                
                # 将分类结果附加到动作单元
                for i, a in enumerate(merged_stage1):
                    if i < len(classification_results):
                        res = classification_results[i]
                        a['knowledge_type'] = res.get('knowledge_type', '过程性知识')
                        a['confidence'] = res.get('confidence', 0.5)
                    else:
                        a['knowledge_type'] = '过程性知识'
                        a['confidence'] = 0.5
                        
            except Exception as e:
                logger.warning(f"[{unit_id}] LLM classification failed: {e}, using default type")
                for a in merged_stage1:
                    a['knowledge_type'] = '过程性知识'
                    a['confidence'] = 0.5
            
            # ==== 过滤：只保留需要视频的类型 ====
            # 讲解型 / Noise / Transition → 不生成视频（但保留截图）
            EXPLAINABLE_TYPES = ['讲解', '概念', '原理', '定义', '背景', '解释', 'Concept', 'Principle', 'explanation']
            NOISE_TYPES = ['noise', 'transition', '噪点', '转场']
            
            video_worthy_actions = []
            for a in merged_stage1:
                k_type = a.get('knowledge_type', '')
                is_explainable = any(t in k_type for t in EXPLAINABLE_TYPES)
                is_noise = any(t in k_type.lower() for t in NOISE_TYPES)
                
                if not is_explainable and not is_noise:
                    video_worthy_actions.append(a)
                else:
                    logger.debug(f"[{unit_id}] Filtered action [{a.get('start_sec', 0):.1f}s-{a.get('end_sec', 0):.1f}s]: type={k_type}")
            
            logger.debug(f"[{unit_id}] After LLM filter: {len(merged_stage1)} → {len(video_worthy_actions)} actions")
            
            # ==== 第二阶段合并（筛选后的 ActionUnit，相同 knowledge_type，间隔 < 5s）====
            merged_stage2, crossed_stage2 = self._merge_actions_local_stage2(
                video_worthy_actions, stable_islands, STAGE2_GAP_THRESHOLD
            )
            
            logger.debug(f"[{unit_id}] Stage2 merge: {len(video_worthy_actions)} → {len(merged_stage2)} actions")
            
            # ==== 收集所有稳定岛 ====
            all_stable = self._collect_all_stable_islands_local(
                merged_stage2, stable_islands, crossed_stage1, crossed_stage2
            )
            
            results[unit_id] = {
                'clip_actions': merged_stage2,
                'all_stable_islands': all_stable,
                'crossed_islands_stage1': crossed_stage1,
                'crossed_islands_stage2': crossed_stage2
            }
            
            logger.info(f"[{unit_id}] Final: {len(merged_stage2)} clip actions, {len(all_stable)} stable islands for screenshots")
        
        return results
    
    def _merge_actions_local(
        self, 
        actions: List[Dict], 
        stable_islands: List[Dict],
        gap_threshold: float
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(actions) <= 1
        - 条件：gap < gap_threshold
        - 条件：i_start >= current.get('end_sec', 0) and i_end <= next_action.get('start_sec', 0)
        依据来源（证据链）：
        - 输入参数：actions, gap_threshold。
        - 配置字段：end_sec, start_sec。
        输入参数：
        - actions: 函数入参（类型：List[Dict]）。
        - stable_islands: 函数入参（类型：List[Dict]）。
        - gap_threshold: 阈值（类型：float）。
        输出参数：
        - List[Dict], List[Dict] 列表（与输入或处理结果一一对应）。"""
        if len(actions) <= 1:
            return actions, []
        
        merged = []
        crossed = []
        current = actions[0].copy()
        
        for next_action in actions[1:]:
            gap = next_action.get('start_sec', 0) - current.get('end_sec', 0)
            
            if gap < gap_threshold:
                # 记录被跨越的稳定岛
                for island in stable_islands:
                    i_start = island.get('start_sec', 0)
                    i_end = island.get('end_sec', 0)
                    if i_start >= current.get('end_sec', 0) and i_end <= next_action.get('start_sec', 0):
                        crossed.append(island)
                
                # 合并
                current['end_sec'] = next_action.get('end_sec', 0)
            else:
                merged.append(current)
                current = next_action.copy()
        
        merged.append(current)
        return merged, crossed
    
    def _merge_actions_local_stage2(
        self, 
        actions: List[Dict], 
        stable_islands: List[Dict],
        gap_threshold: float
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(actions) <= 1
        - 条件：gap < gap_threshold and same_type
        - 条件：i_start >= current.get('end_sec', 0) and i_end <= next_action.get('start_sec', 0)
        依据来源（证据链）：
        - 输入参数：actions, gap_threshold。
        - 配置字段：end_sec, start_sec。
        输入参数：
        - actions: 函数入参（类型：List[Dict]）。
        - stable_islands: 函数入参（类型：List[Dict]）。
        - gap_threshold: 阈值（类型：float）。
        输出参数：
        - List[Dict], List[Dict] 列表（与输入或处理结果一一对应）。"""
        if len(actions) <= 1:
            return actions, []
        
        merged = []
        crossed = []
        current = actions[0].copy()
        
        for next_action in actions[1:]:
            gap = next_action.get('start_sec', 0) - current.get('end_sec', 0)
            same_type = current.get('knowledge_type', '') == next_action.get('knowledge_type', '')
            
            if gap < gap_threshold and same_type:
                # 记录被跨越的稳定岛
                for island in stable_islands:
                    i_start = island.get('start_sec', 0)
                    i_end = island.get('end_sec', 0)
                    if i_start >= current.get('end_sec', 0) and i_end <= next_action.get('start_sec', 0):
                        crossed.append(island)
                
                # 合并
                current['end_sec'] = next_action.get('end_sec', 0)
            else:
                merged.append(current)
                current = next_action.copy()
        
        merged.append(current)
        return merged, crossed
    
    def _collect_all_stable_islands_local(
        self,
        actions: List[Dict],
        external_islands: List[Dict],
        crossed_stage1: List[Dict],
        crossed_stage2: List[Dict]
    ) -> List[Dict]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：key not in seen
        依据来源（证据链）：
        输入参数：
        - actions: 函数入参（类型：List[Dict]）。
        - external_islands: 函数入参（类型：List[Dict]）。
        - crossed_stage1: 函数入参（类型：List[Dict]）。
        - crossed_stage2: 函数入参（类型：List[Dict]）。
        输出参数：
        - Dict 列表（与输入或处理结果一一对应）。"""
        all_islands = []
        seen = set()
        
        def add_island(island: Dict):
            """
            执行逻辑：
            1) 准备必要上下文与参数。
            2) 执行核心处理并返回结果。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：封装逻辑单元，提升复用与可维护性。
            决策逻辑：
            - 条件：key not in seen
            依据来源（证据链）：
            输入参数：
            - island: 函数入参（类型：Dict）。
            输出参数：
            - 无（仅产生副作用，如日志/写盘/状态更新）。"""
            key = (round(island.get('start_sec', 0), 2), round(island.get('end_sec', 0), 2))
            if key not in seen:
                seen.add(key)
                all_islands.append(island)
        
        # 1. 动作单元内部的稳定岛
        for a in actions:
            for island in a.get('stable_islands', []):
                add_island(island)
        
        # 2. 外部稳定岛
        for island in external_islands:
            add_island(island)
        
        # 3. 被跨越的稳定岛
        for island in crossed_stage1:
            add_island(island)
        for island in crossed_stage2:
            add_island(island)
        
        # 按时间排序
        all_islands.sort(key=lambda x: x.get('start_sec', 0))
        return all_islands


    # ❌ Removed: _align_paragraphs_to_subtitles method
    # 文本对齐逻辑已废弃，KnowledgeClassifier 现在直接从 Step 2 读取字幕
    
    def _build_sentence_timestamps(self) -> Dict[str, Dict[str, float]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.sentence_timestamps_path and os.path.exists(self.sentence_timestamps_path)
        依据来源（证据链）：
        - 对象内部状态：self.sentence_timestamps_path。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        # 1. 优先从外部文件加载
        if self.sentence_timestamps_path and os.path.exists(self.sentence_timestamps_path):
            try:
                with open(self.sentence_timestamps_path, 'r', encoding='utf-8') as f:
                    timestamps = json.load(f)
                logger.info(f"Loaded sentence timestamps from external file: {len(timestamps)} mappings")
                return timestamps
            except Exception as e:
                logger.warning(f"Failed to load external sentence_timestamps: {e}, falling back to index mapping")
        
        # 2. 回退: 使用索引规则 S001 → subtitles[0]
        timestamps = {}
        for i, sub in enumerate(self.subtitles):
            sid = f"S{i+1:03d}"
            timestamps[sid] = {
                "start_sec": sub.start_sec,
                "end_sec": sub.end_sec
            }
        logger.info(f"Built sentence timestamps via index mapping: {len(timestamps)} mappings")
        return timestamps

    def _map_timestamp_to_sentence_id(
        self,
        timestamp_sec: float,
        sentence_timestamps: Dict[str, Dict[str, float]],
    ) -> str:
        """根据时间戳映射最匹配的字幕句子 ID。"""
        try:
            target = float(timestamp_sec)
        except Exception:
            return ""

        if not isinstance(sentence_timestamps, dict) or not sentence_timestamps:
            return ""

        in_range_hits: List[Tuple[float, str]] = []
        nearest_hit: Optional[Tuple[float, str]] = None

        for sid, meta in sentence_timestamps.items():
            if not isinstance(meta, dict):
                continue

            try:
                start_sec = float(meta.get("start_sec", 0.0))
                end_sec = float(meta.get("end_sec", start_sec))
            except Exception:
                continue

            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec

            if start_sec <= target <= end_sec:
                in_range_hits.append((end_sec - start_sec, str(sid)))
                continue

            center = (start_sec + end_sec) / 2.0
            distance = abs(center - target)
            if nearest_hit is None or distance < nearest_hit[0]:
                nearest_hit = (distance, str(sid))

        if in_range_hits:
            in_range_hits.sort(key=lambda item: item[0])
            return in_range_hits[0][1]

        if nearest_hit:
            return nearest_hit[1]

        return ""

    def _get_sentence_text_by_id(self, sentence_id: str) -> str:
        """按 sentence_id 获取字幕文本，支持 S001 索引与 subtitle_id。"""
        sid = str(sentence_id or "").strip()
        if not sid:
            return ""

        by_subtitle_id: Dict[str, Any] = {}
        for idx, sub in enumerate(self.subtitles or [], start=1):
            if sid == f"S{idx:03d}":
                return str(getattr(sub, "text", "") or getattr(sub, "corrected_text", "") or "").strip()
            subtitle_id = str(getattr(sub, "subtitle_id", "") or "").strip()
            if subtitle_id:
                by_subtitle_id[subtitle_id] = sub

        matched = by_subtitle_id.get(sid)
        if matched is None:
            return ""

        return str(getattr(matched, "text", "") or getattr(matched, "corrected_text", "") or "").strip()
    
    async def _apply_modality_classification(
        self, 
        units: List[SemanticUnit],
        cache_path: str = None
    ) -> List[SemanticUnit]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not units
        - 条件：cache_path and os.path.exists(cache_path)
        - 条件：cache_path
        依据来源（证据链）：
        - 输入参数：cache_path, units。
        输入参数：
        - units: 函数入参（类型：List[SemanticUnit]）。
        - cache_path: 文件路径（类型：str）。
        输出参数：
        - SemanticUnit 列表（与输入或处理结果一一对应）。"""
        if not units:
            return units
            
        # 🚀 缓存检查
        if cache_path and os.path.exists(cache_path):
            try:
                cached_units = self._load_modality_cache(cache_path)
                logger.info(f"Loaded cached modality classification result from {cache_path}")
                # 简单验证: 数量一致
                if len(cached_units) == len(units):
                    # 还需要验证 unit_id 是否匹配? 
                    # 假设 paragraphs 没变，segment 结果也没变，则匹配。
                    # 如果 segment 变了，limit 可能会不同。
                    # 严格来说应该 match unit_ids. 但这里作为 user 要求的复用，先假设一致.
                    return cached_units
                else:
                    logger.warning(f"Cache size mismatch ({len(cached_units)} vs {len(units)}), re-computing...")
            except Exception as e:
                logger.warning(f"Failed to load modality cache: {e}, re-computing...")
            
        try:
            # 性能优化: 提取到循环外，复用同一 Validator 实例 (及其 VideoCapture 句柄)
            with CVKnowledgeValidator(self.video_path) as validator:
                for unit in units:
                    try:
                        stable_islands, action_units, redundancy = await asyncio.to_thread(
                            validator.detect_visual_states, unit.start_sec, unit.end_sec
                        )
                        
                        # 为每个动作单元计算其内部的稳定岛
                        def get_internal_islands(action_start, action_end, all_islands):
                            """
                            执行逻辑：
                            1) 读取内部状态或外部资源。
                            2) 返回读取结果。
                            实现方式：通过内部函数组合与条件判断实现。
                            核心价值：提供一致读取接口，降低调用耦合。
                            决策逻辑：
                            - 条件：si.start_sec >= action_start and si.end_sec <= action_end
                            依据来源（证据链）：
                            - 输入参数：action_end, action_start。
                            输入参数：
                            - action_start: 起止时间/区间边界（类型：未标注）。
                            - action_end: 起止时间/区间边界（类型：未标注）。
                            - all_islands: 函数入参（类型：未标注）。
                            输出参数：
                            - 函数计算/封装后的结果对象。"""
                            internal = []
                            for si in all_islands:
                                # 稳定岛在动作区间内部
                                if si.start_sec >= action_start and si.end_sec <= action_end:
                                    internal.append({
                                        "start": si.start_sec,
                                        "end": si.end_sec,
                                        "mid": (si.start_sec + si.end_sec) / 2,
                                        "duration": si.duration_ms / 1000.0
                                    })
                            return internal
                        
                        # 保存检测到的所有区间信息 (用于后续素材提取)
                        # 稳定岛信息 (语义单元级)
                        unit.stable_islands = [{
                            "start": si.start_sec,
                            "end": si.end_sec,
                            "mid": (si.start_sec + si.end_sec) / 2,
                            "duration": si.duration_ms / 1000.0
                        } for si in stable_islands] if stable_islands else []
                        
                        # 动作单元信息 (包含内部稳定岛)
                        unit.action_segments = [{
                            "start": au.start_sec,
                            "end": au.end_sec,
                            "type": au.knowledge_subtype,
                            "internal_stable_islands": get_internal_islands(au.start_sec, au.end_sec, stable_islands)
                        } for au in action_units] if action_units else []
                        
                        # 模态决策
                        if not action_units:
                            # 纯静态: 只有稳定岛
                            unit.modality = "screenshot"
                            unit.knowledge_subtype = "stable"
                            
                        elif stable_islands:
                            # 混合: 有动作单元也有稳定岛
                            unit.modality = "video_screenshot"
                            unit.knowledge_subtype = action_units[0].knowledge_subtype if action_units else "K3_derivation"
                            
                        else:
                            # 纯动态: 只有动作单元
                            unit.modality = "video_only"
                            unit.knowledge_subtype = action_units[0].knowledge_subtype if action_units else "K4_operation"
                            
                    except Exception as e:
                        logger.warning(f"Modality classification failed for {unit.unit_id}: {e}")
                        # 回退到静态
                        unit.modality = "screenshot"
                        unit.stable_islands = []
                        unit.action_segments = []
                        unit.knowledge_subtype = "fallback"
        except Exception as e:
            logger.error(f"Global modality classification failed: {e}")
            # 全部回退到静态
            for unit in units:
                if unit.modality == "unknown":
                    unit.modality = "screenshot"
        
        # 统计
        modality_counts = {}
        for u in units:
            modality_counts[u.modality] = modality_counts.get(u.modality, 0) + 1
        logger.info(f"  Modality distribution: {modality_counts}")
        
        # 🚀 保存缓存
        if cache_path:
            try:
                self._save_modality_cache(units, cache_path)
                logger.info(f"Saved modality classification cache: {cache_path}")
            except Exception as e:
                logger.warning(f"Failed to save modality cache: {e}")
        
        return units

    def _save_modality_cache(self, units: List[SemanticUnit], path: str):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - units: 函数入参（类型：List[SemanticUnit]）。
        - path: 文件路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        data = {
            "units": [asdict(u) for u in units]
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_modality_cache(self, path: str) -> List[SemanticUnit]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过JSON 解析/序列化、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：'materials' in filtered_data and isinstance(filtered_data['materials'], dict)
        依据来源（证据链）：
        - 配置字段：materials。
        输入参数：
        - path: 文件路径（类型：str）。
        输出参数：
        - SemanticUnit 列表（与输入或处理结果一一对应）。"""
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        units = []
        for u_data in data.get("units", []):
            # 重建 SemanticUnit
            # 过滤 dataclass 未定义的字段 (向后兼容)
            valid_keys = SemanticUnit.__dataclass_fields__.keys()
            filtered_data = {k: v for k, v in u_data.items() if k in valid_keys}
            
            # 手动处理 materials (dict -> MaterialSet)
            if "materials" in filtered_data and isinstance(filtered_data["materials"], dict):
                mat_data = filtered_data["materials"]
                # 过滤 MaterialSet 未定义字段
                mat_valid_keys = MaterialSet.__dataclass_fields__.keys()
                mat_filtered = {k: v for k, v in mat_data.items() if k in mat_valid_keys}
                filtered_data["materials"] = MaterialSet(**mat_filtered)
                
            units.append(SemanticUnit(**filtered_data))
        return units
    
    async def _generate_materials_parallel(self, units: List[SemanticUnit]):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、asyncio 异步调度实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - units: 函数入参（类型：List[SemanticUnit]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        # 🚀 LLM 调用优化：跨 unit 预分类（参考 LLM调用优化.md「批量请求合并」）
        # 做什么：将多个 unit 的动作单元合并批处理，显著减少 DeepSeek 请求次数。
        # 为什么：单 unit 调一次 classify_batch 会把网络往返与调度开销放大，成为瓶颈。
        # 权衡：单次 prompt 更长，依赖 KnowledgeClassifier 的 token_budget 动态分块与解析回退策略。
        await self._preclassify_action_segments_multi_unit(units)

        MAX_CONCURRENT = 4  # 最大并发数
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
        async def process_unit(idx: int, unit: SemanticUnit):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过内部方法调用/状态更新实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            输入参数：
            - idx: 函数入参（类型：int）。
            - unit: 函数入参（类型：SemanticUnit）。
            输出参数：
            - 无（仅产生副作用，如日志/写盘/状态更新）。"""
            async with semaphore:
                logger.info(f"  [{idx+1}/{len(units)}] {unit.unit_id}: {unit.modality}")
                await self._generate_materials(unit)
        
        # 创建所有任务
        tasks = [
            process_unit(i, unit) 
            for i, unit in enumerate(units)
        ]
        
        # 并行执行
        await asyncio.gather(*tasks)
        
        logger.info(f"  → All {len(units)} units processed in parallel")

    async def _preclassify_action_segments_multi_unit(self, units: List[SemanticUnit]) -> None:
        """
        做什么：对多个语义单元的动作单元做“跨 unit”批量知识分类，并将结果回填到 action_segments[*].classification。
        为什么：减少 LLM 调用次数与调度开销（参考 LLM调用优化.md），同时避免对已存在 knowledge_type 的动作重复分类。
        权衡：需要构建批处理 payload 与索引映射；当批处理关闭/失败时不影响主流程（回退到 per-unit classify_batch）。
        """
        if not units:
            return

        classifier = getattr(self, "_knowledge_classifier", None)
        if not classifier or not getattr(classifier, "enabled", False):
            return

        raw = (os.getenv("MODULE2_KC_MULTI_UNIT_ENABLED", "1") or "").strip().lower()
        multi_unit_enabled = raw in ("1", "true", "yes", "y", "on")
        if not (multi_unit_enabled and hasattr(classifier, "classify_units_batch")):
            return

        # 1) 预处理：动作融合（与 _generate_materials/_collect_material_requests 一致），并尽量复用已有 knowledge_type
        units_payload = []
        index_map: Dict[str, List[int]] = {}

        for unit in units:
            action_segments = getattr(unit, "action_segments", None) or []
            if not action_segments:
                continue

            # 与 _generate_materials 相同的融合策略：gap<5s
            if len(action_segments) >= 2:
                merged_actions = self._merge_action_segments(action_segments, gap_threshold_sec=5.0)
                action_segments = merged_actions
                unit.action_segments = merged_actions

            missing_indices: List[int] = []
            missing_segments: List[Dict[str, Any]] = []
            for idx, action in enumerate(action_segments):
                # 已有 classification 或 knowledge_type → 直接回填（避免重复 LLM 调用）
                cls = action.get("classification")
                if isinstance(cls, dict) and cls.get("knowledge_type"):
                    continue

                kt = str(action.get("knowledge_type", "") or "").strip()
                if kt:
                    action["classification"] = {
                        "knowledge_type": kt,
                        "confidence": float(action.get("confidence", 0.5) or 0.5),
                        "key_evidence": action.get("key_evidence", ""),
                        "reasoning": action.get("reasoning", ""),
                    }
                    continue

                missing_indices.append(idx)
                missing_segments.append(
                    {
                        "start_sec": action.get("start_sec", getattr(unit, "start_sec", 0.0)),
                        "end_sec": action.get("end_sec", getattr(unit, "end_sec", 0.0)),
                        "id": action.get("id", f"action_{idx}"),
                    }
                )

            if not missing_segments:
                continue

            index_map[unit.unit_id] = missing_indices
            units_payload.append(
                {
                    "unit_id": unit.unit_id,
                    "title": getattr(unit, "knowledge_topic", None) or "未知主题",
                    "full_text": getattr(unit, "full_text", getattr(unit, "text", "")) or "",
                    "action_segments": missing_segments,
                }
            )

        if not units_payload:
            return

        # 2) 跨 unit 批量分类（内部包含 token_budget 动态分块 + JSON 解析回退）
        try:
            results_map = await classifier.classify_units_batch(units_payload)
        except Exception as e:
            logger.warning(f"Multi-unit preclassification failed: {e} -> fallback per-unit later")
            return

        # 3) 回填：仅填充缺失项，避免覆盖已有 classification
        if not isinstance(results_map, dict):
            return

        for unit in units:
            unit_id = unit.unit_id
            if unit_id not in index_map:
                continue

            missing_indices = index_map[unit_id]
            batch_results = results_map.get(unit_id, []) or []

            for j, orig_idx in enumerate(missing_indices):
                res = batch_results[j] if j < len(batch_results) else {}
                if not isinstance(res, dict):
                    res = {}
                unit.action_segments[orig_idx]["classification"] = res
    
    async def _generate_materials(self, unit: SemanticUnit):
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
                merged_actions = self._merge_action_segments(action_segments, gap_threshold_sec=5.0)
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
                batch_classifications = await self._knowledge_classifier.classify_batch(
                    semantic_unit_title=getattr(unit, 'knowledge_topic', '未知主题'),
                    semantic_unit_text=getattr(unit, 'full_text', getattr(unit, 'text', '')),
                    action_segments=action_segments
                )
                for action, classification in zip(action_segments, batch_classifications):
                    action["classification"] = classification
            
            for i, (action, classification) in enumerate(zip(action_segments, batch_classifications)):
                action_start = action.get("start_sec", unit.start_sec)
                action_end = action.get("end_sec", unit.end_sec)
                action_type = action.get("type", "K4_operation")
                
                # 获取该动作单元内部的稳定岛
                action_internal_islands = action.get("internal_stable_islands", [])
                
                # 💥 Sentence：与动作重叠的那句（按字幕时间戳定位）
                sentence_start = self._align_to_sentence_start(action_start)
                sentence_end = self._align_to_sentence_end(action_end)
                
                # Classification already done in batch
                knowledge_type = classification.get("knowledge_type", "过程性知识")
                confidence = classification.get("confidence", 0.5)
                action_brief = self._build_action_brief(action, classification, i + 1)
                asset_base = f"{unit.unit_id}/{self._build_request_base_name(unit, f'action_{i+1:02d}_{action_brief}')}"

                
                logger.info(f"{unit.unit_id} action_{i+1}: {knowledge_type} (conf={confidence:.0%}) - {classification.get('key_evidence', '')[:30]}")

                # 🚀 Adaptive Action Envelope: 语义单元短时可整段；且 clip 结束不跨越 unit.end_sec
                envelope_start, envelope_end = self._compute_action_envelope(
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
                    logger.info(f"  → Downgrade to screenshots only (讲解型)")
                    
                    # 首帧截图: 查找窗口为 [包络起点, 动作起点]
                    head_window_end = min(max(envelope_start + 0.5, action_start), envelope_end)
                    head_ss = await self._select_screenshot(
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
                        
                        island_ss = await self._select_screenshot(
                            start_sec=island_start,
                            end_sec=island_end,
                            name=f"{asset_base}_island_{j+1:02d}"
                        )
                        if island_ss:
                            screenshot_paths.append(island_ss)
                            screenshot_labels.append(f"动作{i+1}稳定帧{j+1}")
                    
                    # 末帧截图: 查找窗口为 [动作终点, 包络终点]
                    tail_window_start = max(min(envelope_end - 0.5, action_end), envelope_start)
                    tail_ss = await self._select_screenshot(
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
                    clip_path = await self._extract_action_clip(
                        start_sec=envelope_start,
                        end_sec=envelope_end,
                        name=f"{asset_base}"
                    )
                    if clip_path:
                        clip_paths.append(clip_path)
                    
                    # 2. 提取首帧截图: 查找窗口为 [包络起点, 动作起点]
                    head_window_end = min(max(envelope_start + 0.5, action_start), envelope_end)
                    head_ss = await self._select_screenshot(
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
                        
                        island_ss = await self._select_screenshot(
                            start_sec=island_start,
                            end_sec=island_end,
                            name=f"{asset_base}_island_{j+1:02d}"
                        )
                        if island_ss:
                            screenshot_paths.append(island_ss)
                            screenshot_labels.append(f"动作{i+1}稳定帧{j+1}")
                    
                    # 4. 提取末帧截图: 查找窗口为 [动作终点, 包络终点]
                    tail_window_start = max(min(envelope_end - 0.5, action_end), envelope_start)
                    tail_ss = await self._select_screenshot(
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
                
                ss_path = await self._select_screenshot(
                    start_sec=island_start,
                    end_sec=island_end,
                    name=f"{unit.unit_id}/{self._build_request_base_name(unit, f'stable_{i+1:02d}')}"
                )
                if ss_path:
                    screenshot_paths.append(ss_path)
                    screenshot_labels.append(f"稳定帧{i+1}")
        
        else:
            # ==== 回退: 无任何检测结果 ====
            fallback_ss = await self._select_screenshot(
                start_sec=unit.start_sec,
                end_sec=unit.end_sec,
                name=f"{unit.unit_id}/{self._build_request_base_name(unit, 'fallback')}"
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
    
    async def _collect_material_requests(self, unit: SemanticUnit) -> MaterialRequests:
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
            action_segments = self._merge_action_segments(action_segments, gap_threshold_sec=5.0)
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
                batch_classifications = await self._knowledge_classifier.classify_batch(
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
                sentence_start = self._align_to_sentence_start(action_start)
                sentence_end = self._align_to_sentence_end(action_end)
                
                # Classification already done in batch
                knowledge_type = classification.get("knowledge_type", "过程性知识")
                confidence = classification.get("confidence", 0.5)
                action_brief = self._build_action_brief(action, classification, i + 1)
                request_base = self._build_unit_relative_request_id(
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
                envelope_start, envelope_end = self._compute_action_envelope(
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
                    head_search_start, head_search_end = self._clamp_time_range(envelope_start - 1.0, envelope_start + 1.0)
                    fallback_head_ts = envelope_start
                    head_ts = await self._select_screenshot_timestamp(head_search_start, head_search_end, fallback_head_ts)
                    
                    screenshot_requests.append(ScreenshotRequest(
                        screenshot_id=f"{request_base}_head",
                        timestamp_sec=head_ts,
                        label="head",
                        semantic_unit_id=unit.unit_id
                    ))
                    
                    # 稳定岛截图
                    for j, island in enumerate(action_internal_islands):
                        island_start = island.get("start", action_start)
                        island_end = island.get("end", action_end)
                        island_mid_fallback = (island_start + island_end) / 2
                        island_start, island_end = self._clamp_time_range(island_start, island_end)
                        island_mid = await self._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                        screenshot_requests.append(ScreenshotRequest(
                            screenshot_id=f"{request_base}_island_{j+1:02d}",
                            timestamp_sec=island_mid,
                            label="stable",
                            semantic_unit_id=unit.unit_id
                        ))
                    
                    # 末帧截图: 搜索窗口扩大为 包络终点 ±1.0s
                    tail_search_start, tail_search_end = self._clamp_time_range(envelope_end - 1.0, envelope_end + 1.0)
                    tail_search_end = min(tail_search_end, float(getattr(unit, "end_sec", tail_search_end)))
                    tail_search_start, tail_search_end = self._clamp_time_range(tail_search_start, tail_search_end)
                    fallback_tail_ts = envelope_end
                    tail_ts = await self._select_screenshot_timestamp(tail_search_start, tail_search_end, fallback_tail_ts)
                    
                    screenshot_requests.append(ScreenshotRequest(
                        screenshot_id=f"{request_base}_tail",
                        timestamp_sec=tail_ts,
                        label="tail",
                        semantic_unit_id=unit.unit_id
                    ))
                
                else:
                    # 非讲解型: 需要视频切片 + 首尾帧截图
                    # 视频切片
                    clip_requests.append(ClipRequest(
                        clip_id=request_base,
                        start_sec=envelope_start,
                        end_sec=envelope_end,
                        knowledge_type=knowledge_type,
                        semantic_unit_id=unit.unit_id
                    ))
                    
                    # 首帧截图: 搜索窗口扩大为 包络起点 ±1.0s
                    head_search_start, head_search_end = self._clamp_time_range(envelope_start - 1.0, envelope_start + 1.0)
                    fallback_head_ts = envelope_start
                    head_ts = await self._select_screenshot_timestamp(head_search_start, head_search_end, fallback_head_ts)
                    
                    screenshot_requests.append(ScreenshotRequest(
                        screenshot_id=f"{request_base}_head",
                        timestamp_sec=head_ts,
                        label="head",
                        semantic_unit_id=unit.unit_id
                    ))
                    
                    # 稳定岛截图
                    for j, island in enumerate(action_internal_islands):
                        island_start = island.get("start", action_start)
                        island_end = island.get("end", action_end)
                        island_mid_fallback = (island_start + island_end) / 2
                        island_start, island_end = self._clamp_time_range(island_start, island_end)
                        island_mid = await self._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                        screenshot_requests.append(ScreenshotRequest(
                            screenshot_id=f"{request_base}_island_{j+1:02d}",
                            timestamp_sec=island_mid,
                            label="stable",
                            semantic_unit_id=unit.unit_id
                        ))
                    
                    # 末帧截图: 搜索窗口扩大为 包络终点 ±1.0s
                    tail_search_start, tail_search_end = self._clamp_time_range(envelope_end - 1.0, envelope_end + 1.0)
                    tail_search_end = min(tail_search_end, float(getattr(unit, "end_sec", tail_search_end)))
                    tail_search_start, tail_search_end = self._clamp_time_range(tail_search_start, tail_search_end)
                    fallback_tail_ts = envelope_end
                    tail_ts = await self._select_screenshot_timestamp(tail_search_start, tail_search_end, fallback_tail_ts)
                    
                    screenshot_requests.append(ScreenshotRequest(
                        screenshot_id=f"{request_base}_tail",
                        timestamp_sec=tail_ts,
                        label="tail",
                        semantic_unit_id=unit.unit_id
                    ))
        
        elif stable_islands:
            # ==== 无动作单元，仅稳定岛: 提取中间帧 ====
            for i, island in enumerate(stable_islands):
                island_start = island.get("start", unit.start_sec)
                island_end = island.get("end", unit.end_sec)
                island_mid_fallback = (island_start + island_end) / 2
                island_mid = await self._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                screenshot_requests.append(ScreenshotRequest(
                    screenshot_id=self._build_unit_relative_request_id(unit, f"stable_{i+1:02d}"),
                    timestamp_sec=island_mid,
                    label="stable",
                    semantic_unit_id=unit.unit_id
                ))
        
        else:
            # ==== 回退: 无任何检测结果 ====
            fallback_ts = (unit.start_sec + unit.end_sec) / 2
            best_ts = await self._select_screenshot_timestamp(unit.start_sec, unit.end_sec, fallback_ts)
            
            screenshot_requests.append(ScreenshotRequest(
                screenshot_id=self._build_unit_relative_request_id(unit, "fallback"),
                timestamp_sec=best_ts,
                label="fallback",
                semantic_unit_id=unit.unit_id
            ))
        
        logger.debug(f"{unit.unit_id}: collected {len(screenshot_requests)} screenshot requests, "
                     f"{len(clip_requests)} clip requests")
        
        return MaterialRequests(
            screenshot_requests=screenshot_requests,
            clip_requests=clip_requests,
            action_classifications=action_classifications
        )
    
    def _apply_external_materials(
        self,
        unit: SemanticUnit,
        screenshots_dir: str,
        clips_dir: str,
        material_requests: MaterialRequests
    ):
        """
        ???????????????? `assets/{unit_id}/`?

        ???
        - ???????????????????/???
        - ???????????? unit ????????????????
        """
        materials = MaterialSet()
        screenshot_paths: List[str] = []
        screenshot_labels: List[str] = []
        screenshot_items: List[Dict[str, Any]] = []
        sentence_timestamps = self._build_sentence_timestamps()

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
        tutorial_steps = getattr(unit, "instructional_steps", []) or []
        is_tutorial_process = (
            normalized_kt == "process"
            and (
                bool(getattr(unit, "mult_steps", False))
                or len(tutorial_steps) > 1
            )
        )
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

        assets_root = Path(self.assets_dir)

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
                sentence_id = self._map_timestamp_to_sentence_id(float(request_ts), sentence_timestamps)
                if sentence_id:
                    sentence_text = self._get_sentence_text_by_id(sentence_id)

            if should_validate_screenshot and self._concrete_validator:
                try:
                    pre_key = str(Path(raw_path).resolve())
                except Exception:
                    pre_key = os.path.abspath(raw_path)

                if pre_key in self._prevalidated_concrete_results:
                    res = self._prevalidated_concrete_results[pre_key]
                else:
                    res = self._concrete_validator.validate(raw_path)
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

            self._record_image_match_audit(
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

    async def _select_screenshot(
        self,
        start_sec: float,
        end_sec: float,
        name: str
    ) -> str:
        """
        ?????????????????? assets ??????
        """
        output_path = self._resolve_asset_output_path(name, "png")
    
        if not self._screenshot_selector:
            logger.warning("ScreenshotSelector not available, using fallback ffmpeg direct")
            return await self._extract_frame_ffmpeg_fallback(start_sec, end_sec, name)
    
        try:
            result = await self._screenshot_selector.select_screenshot(
                video_path=self.video_path,
                start_sec=start_sec,
                end_sec=end_sec,
                output_dir=str(Path(output_path).parent),
                output_name=Path(output_path).stem,
            )
    
            if result and result.screenshot_path and os.path.exists(result.screenshot_path):
                if os.path.abspath(result.screenshot_path) == os.path.abspath(output_path):
                    return output_path
                logger.warning(
                    "ScreenshotSelector returned non-target path in no-copy mode, fallback ffmpeg direct: %s",
                    result.screenshot_path,
                )
                return await self._extract_frame_ffmpeg_fallback(start_sec, end_sec, name)
            return ""
    
        except Exception as e:
            logger.error(f"Screenshot selection failed: {e}")
            return await self._extract_frame_ffmpeg_fallback(start_sec, end_sec, name)
    
    async def _select_screenshot_timestamp(
        self,
        start_sec: float,
        end_sec: float,
        fallback_ts: float
    ) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._screenshot_selector
        依据来源（证据链）：
        - 对象内部状态：self._screenshot_selector。
        输入参数：
        - start_sec: 起止时间/区间边界（类型：float）。
        - end_sec: 起止时间/区间边界（类型：float）。
        - fallback_ts: 函数入参（类型：float）。
        输出参数：
        - 数值型计算结果。"""
        if self._screenshot_selector:
            try:
                # 调用 ScreenshotSelector，设置 save_image=False
                selection = await self._screenshot_selector.select_screenshot(
                    self.video_path,
                    start_sec,
                    end_sec,
                    save_image=False
                )
                return selection.selected_timestamp
            except Exception as e:
                logger.debug(f"Visual-aided timestamp selection failed, falling back to {fallback_ts}: {e}")
        
        return fallback_ts

    

    async def _extract_frame_ffmpeg_fallback(self, start_sec: float, end_sec: float, name: str) -> str:
        """
        ScreenshotSelector ??????? ffmpeg ?????????
        """
        import subprocess
    
        output_path = self._resolve_asset_output_path(name, "png")
        timestamp = (start_sec + end_sec) / 2
        timestamp = max(0.1, min(timestamp, self.video_duration - 0.1))
    
        cmd = [
            "ffmpeg", "-y",
            "-ss", str(timestamp),
            "-i", self.video_path,
            "-frames:v", "1",
            "-q:v", "2",
            output_path
        ]
    
        def run_ffmpeg():
            try:
                subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                return output_path if os.path.exists(output_path) else ""
            except Exception as e:
                logger.error(f"FFmpeg frame extraction failed: {e}")
                return ""
    
        loop = asyncio.get_running_loop()
        executor = get_io_executor()
        return await loop.run_in_executor(executor, run_ffmpeg)
    
    
    async def _extract_action_clip(
        self,
        start_sec: float,
        end_sec: float,
        name: str
    ) -> str:
        """
        ??????????????? assets ???
        """
        output_path = self._resolve_asset_output_path(name, "mp4")
    
        if not self._clip_extractor:
            logger.info(f"VideoClipExtractor not available for {name}, using ffmpeg fallback")
            return await self._extract_action_clip_ffmpeg(start_sec, end_sec, name)
    
        try:
            clip_result = await self._clip_extractor.extract_video_clip(
                timestamp_start=start_sec,
                timestamp_end=end_sec,
                output_dir=str(Path(output_path).parent),
                video_path=self.video_path,
                output_name=Path(output_path).stem,
            )
    
            if clip_result and clip_result.clip_path and os.path.exists(clip_result.clip_path):
                if os.path.abspath(clip_result.clip_path) == os.path.abspath(output_path):
                    return output_path
                logger.warning(
                    "VideoClipExtractor returned non-target path in no-copy mode, fallback ffmpeg direct: %s",
                    clip_result.clip_path,
                )
                return await self._extract_action_clip_ffmpeg(start_sec, end_sec, name)
    
            logger.warning(f"VideoClipExtractor returned no result for {name}")
            return await self._extract_action_clip_ffmpeg(start_sec, end_sec, name)
    
        except Exception as e:
            logger.error(f"Action clip extraction failed for {name}: {e}")
            return await self._extract_action_clip_ffmpeg(start_sec, end_sec, name)
    
    
    async def _extract_action_clip_ffmpeg(self, start_sec: float, end_sec: float, name: str) -> str:
        """
        Action ClipExtractor ??????? ffmpeg ?????????
        """
        import subprocess

        output_path = self._resolve_asset_output_path(name, "mp4")
        duration = end_sec - start_sec

        safe_start = max(0, start_sec - 0.2)
        safe_duration = duration + 0.3

        cmd = [
            "ffmpeg", "-y",
            "-ss", str(safe_start),
            "-i", self.video_path,
            "-t", str(safe_duration),
            "-c:v", "libx264",
            "-crf", str(self.config.clip_crf),
            "-c:a", "aac",
            "-b:a", "128k",
            output_path
        ]

        def run_ffmpeg():
            try:
                subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                return output_path if os.path.exists(output_path) else ""
            except Exception as e:
                logger.error(f"FFmpeg action clip extraction failed: {e}")
                return ""

        loop = asyncio.get_running_loop()
        executor = get_io_executor()
        return await loop.run_in_executor(executor, run_ffmpeg)

    async def _extract_video_clip(self, unit: SemanticUnit) -> str:
        """
        ???????????????? ClipExtractor????? ffmpeg??
        """
        if not self._clip_extractor:
            logger.warning("VideoClipExtractor not available, using fallback ffmpeg")
            return await self._extract_clip_ffmpeg_fallback(unit)

        try:
            if unit.action_segments:
                action_start = min(seg["start"] for seg in unit.action_segments)
                action_end = max(seg["end"] for seg in unit.action_segments)
            else:
                action_start = unit.start_sec
                action_end = unit.end_sec

            clip_result = await self._clip_extractor.extract_video_clip(
                timestamp_start=action_start,
                timestamp_end=action_end,
                output_dir=self.assets_dir,
                video_path=self.video_path,
                fault_text=unit.text,
                source_subtitle_ids=unit.source_subtitle_ids,
            )

            if clip_result and clip_result.clip_path:
                return clip_result.clip_path

            logger.warning(f"VideoClipExtractor returned no result for {unit.unit_id}")
            return await self._extract_clip_ffmpeg_fallback(unit)

        except Exception as e:
            logger.error(f"Video clip extraction failed for {unit.unit_id}: {e}")
            return await self._extract_clip_ffmpeg_fallback(unit)

    async def _extract_clip_ffmpeg_fallback(self, unit: SemanticUnit) -> str:
        """
        ClipExtractor ??????? ffmpeg ??????????????
        """
        import subprocess

        base_name = self._build_request_base_name(unit, "unit_clip")
        output_path = self._resolve_asset_output_path(f"{unit.unit_id}/{base_name}", "mp4")
        duration = unit.end_sec - unit.start_sec

        cmd = [
            "ffmpeg", "-y",
            "-ss", str(unit.start_sec),
            "-i", self.video_path,
            "-t", str(duration),
            "-c:v", "libx264",
            "-crf", str(self.config.clip_crf),
            "-c:a", "aac",
            "-b:a", "128k",
            output_path
        ]

        def run_ffmpeg():
            try:
                subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                return output_path if os.path.exists(output_path) else ""
            except Exception as e:
                logger.error(f"FFmpeg fallback failed: {e}")
                return ""

        loop = asyncio.get_running_loop()
        executor = get_io_executor()
        return await loop.run_in_executor(executor, run_ffmpeg)


# =============================================================================
# CLI 入口
# =============================================================================
