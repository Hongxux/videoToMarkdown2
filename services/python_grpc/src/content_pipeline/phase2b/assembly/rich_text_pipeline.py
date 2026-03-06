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
import json
import logging
import asyncio
import yaml
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import asdict

from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import SemanticUnitSegmenter, SemanticUnit
from services.python_grpc.src.content_pipeline.phase2a.vision.cv_knowledge_validator import CVKnowledgeValidator
from services.python_grpc.src.content_pipeline.phase2a.segmentation.concrete_knowledge_validator import ConcreteKnowledgeValidator
from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import ScreenshotSelector
from services.python_grpc.src.content_pipeline.phase2a.segmentation.knowledge_classifier import KnowledgeClassifier
from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_repository import SubtitleRepository
from services.python_grpc.src.content_pipeline.shared.semantic_payload import (
    build_grouped_semantic_units_payload,
    normalize_semantic_units_payload,
)
from services.python_grpc.src.content_pipeline.infra.runtime.resource_manager import get_io_executor
from services.python_grpc.src.content_pipeline.phase2b.assembly.pipeline_asset_utils import (
    slugify_text,
    build_unit_asset_prefix,
    build_action_brief,
    build_request_base_name,
    build_unit_relative_request_id,
    resolve_asset_output_path,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.pipeline_timeline_utils import (
    merge_action_segments,
    compute_action_envelope,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.pipeline_material_request_utils import (
    create_screenshot_request,
    create_clip_request,
)
from services.python_grpc.src.content_pipeline.phase2b.assembly.rich_text_document import (
    RichTextDocument,
    KnowledgeGroup,
    MaterialSet,
    create_section_from_semantic_unit,
)
from services.python_grpc.src.config_paths import resolve_video_config_path

from services.python_grpc.src.content_pipeline.phase2b.assembly.request_models import (
    PipelineConfig,
    ScreenshotRequest,
    ClipRequest,
    MaterialRequests,
)

from services.python_grpc.src.content_pipeline.phase2b.assembly.material_flow import (
    generate_materials,
    collect_material_requests,
    apply_external_materials,
)
from services.python_grpc.src.common.utils.video import get_video_duration
from services.python_grpc.src.common.utils.path import sanitize_filename_component

logger = logging.getLogger(__name__)


class RichTextPipeline:
    """类说明：RichTextPipeline 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(
        self,
        video_path: str,
        step2_path: str,
        step6_path: str,
        output_dir: str,
        config: PipelineConfig = None,
        sentence_timestamps_path: str = None,
        segmenter: SemanticUnitSegmenter = None,
        step2_subtitles: Optional[List[Any]] = None,
        step6_paragraphs: Optional[List[Dict[str, Any]]] = None,
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

        # 统一字幕仓储：解析 step2/step6/sentence_timestamps 路径并承载字幕检索能力
        self.subtitle_repo = SubtitleRepository.from_output_dir(
            output_dir=output_dir,
            step2_path=step2_path,
            step6_path=step6_path,
            sentence_timestamps_path=sentence_timestamps_path or "",
        )
        # 非复用链路优先使用内存态产物，避免刚入队的异步 JSON 读写时序依赖。
        if step2_subtitles is not None:
            self.subtitle_repo.set_raw_subtitles(step2_subtitles, clear_sentence_timestamps=False)
        if step6_paragraphs is not None:
            self.subtitle_repo.set_raw_paragraphs(step6_paragraphs)

        self.step2_path = self.subtitle_repo.step2_path
        self.step6_path = self.subtitle_repo.step6_path
        self.sentence_timestamps_path = self.subtitle_repo.sentence_timestamps_path
        
        # 创建输出目录
        self.assets_dir = os.path.join(output_dir, self.config.assets_subdir)
        Path(self.assets_dir).mkdir(parents=True, exist_ok=True)
        
        # 加载数据 (Phase 2B 组装模式下可能为空)
        if step2_subtitles is not None:
            logger.info(f"Loading step2 from memory payload: items={len(step2_subtitles)}")
        elif self.step2_path:
            logger.info(f"Loading step2: {self.step2_path}")
        else:
            logger.info("Skip loading step2 (empty path)")
        self.subtitles = self.subtitle_repo.load_step2_subtitles()

        if step6_paragraphs is not None:
            logger.info(f"Loading step6 from memory payload: paragraphs={len(step6_paragraphs)}")
        elif self.step6_path:
            logger.info(f"Loading step6: {self.step6_path}")
        else:
            logger.info("Skip loading step6 (empty path)")
        self.paragraphs = self.subtitle_repo.load_step6_paragraphs()
        # 记录最近一次 Phase2A 语义单元序列化结果，供上层服务在同进程内做内存直传。
        self.latest_phase2a_semantic_units_payload: List[Dict[str, Any]] = []
        
        # 初始化组件（优先注入单例，避免热路径重复构建）
        self.segmenter = segmenter if segmenter is not None else SemanticUnitSegmenter()
        
        # ScreenshotSelector (懒加载，需要 visual_extractor 依赖)
        self._screenshot_selector = None
        
        # VideoClipExtractor (懒加载，需要依赖注入)
        self._clip_extractor = None
        
        # 视觉特征提取器 (共享依赖)
        self._visual_extractor = None
        
        # 获取视频信息
        self.video_duration = get_video_duration(self.video_path, default=0.0, use_cv2_fallback=True)
        
        # 💥 V7.4: 知识分类器 (用于动作单元四分类)
        # 💥 V7.4: KnowledgeClassifier 直接从 Step2 读取字幕，需显式注入 step2_path（避免空字幕导致分类质量退化）
        self._knowledge_classifier = KnowledgeClassifier(step2_path=self.step2_path, subtitle_repo=self.subtitle_repo)
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
        # PP-Structure 提取前全局去重：跨语义单元复用原图签名，避免重复提取。
        self._prestructure_seen_raw_signatures: Dict[str, str] = {}

        # 图片匹配审计：默认关闭，可通过 config.yaml / 环境变量开启
        self._image_match_audit_enabled = self._load_image_match_audit_switch(default_value=False)
        self._image_match_audit_records: List[Dict[str, Any]] = []
        # concrete 截图在 Phase2B 默认跳过 AI Vision（含预处理），按配置可开启。
        self._phase2b_concrete_ai_vision_enabled = self._load_phase2b_concrete_ai_vision_switch(
            default_value=False
        )
        
        logger.info(f"Pipeline initialized: {len(self.subtitles)} subtitles, {len(self.paragraphs)} paragraphs")

    def _resolve_config_path(self) -> Optional[Path]:
        """方法说明：RichTextPipeline._resolve_config_path 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        env_path = str(os.getenv("MODULE2_CONFIG_PATH", "") or "").strip()
        if env_path:
            candidate = Path(env_path)
            if candidate.exists():
                return candidate
            logger.warning(f"MODULE2_CONFIG_PATH not found: {candidate}")
        return resolve_video_config_path(anchor_file=__file__)

    @staticmethod
    def _parse_bool(value: Any, default: bool) -> bool:
        """方法说明：RichTextPipeline._parse_bool 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：RichTextPipeline._load_image_match_audit_switch 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        enabled = bool(default_value)

        config_path = self._resolve_config_path()
        if config_path is not None:
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
                content_pipeline_cfg = config.get("content_pipeline", {}) if isinstance(config, dict) else {}
                observability_cfg = content_pipeline_cfg.get("observability", {}) if isinstance(content_pipeline_cfg, dict) else {}
                image_cfg = observability_cfg.get("image_match_audit", {}) if isinstance(observability_cfg, dict) else {}
                enabled = self._parse_bool(image_cfg.get("enabled", enabled), enabled)
            except Exception as exc:
                logger.warning(f"Failed to load image-match-audit switch from config: {exc}")

        env_raw = os.getenv("MODULE2_IMAGE_MATCH_AUDIT_ENABLED")
        if env_raw is not None and str(env_raw).strip() != "":
            enabled = self._parse_bool(env_raw, enabled)

        return enabled

    def _load_phase2b_concrete_ai_vision_switch(self, default_value: bool = False) -> bool:
        enabled = bool(default_value)

        config_path = self._resolve_config_path()
        if config_path is not None:
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f) or {}
                content_pipeline_cfg = config.get("content_pipeline", {}) if isinstance(config, dict) else {}
                phase2b_cfg = content_pipeline_cfg.get("phase2b", {}) if isinstance(content_pipeline_cfg, dict) else {}
                concrete_cfg = phase2b_cfg.get("concrete_ai_vision", {}) if isinstance(phase2b_cfg, dict) else {}
                enabled = self._parse_bool(concrete_cfg.get("enabled", enabled), enabled)
            except Exception as exc:
                logger.warning(f"Failed to load phase2b concrete-ai-vision switch from config: {exc}")

        env_raw = os.getenv("MODULE2_PHASE2B_CONCRETE_AI_VISION_ENABLED")
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
        """方法说明：RichTextPipeline._record_image_match_audit 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：RichTextPipeline._flush_image_match_audit 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """兼容保留：委托 SubtitleRepository 统一解析中间产物路径。"""
        return SubtitleRepository.resolve_intermediate_path(
            provided_path=provided_path,
            output_dir=self.output_dir,
            candidate_names=candidate_names,
        )
    
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
        """方法说明：RichTextPipeline._slugify_text 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return slugify_text(value, max_len=max_len)

    def _build_enhanced_markdown_filename(self, title: str) -> str:
        raw_title = str(title or "").strip()
        if raw_title.lower().endswith(".md"):
            raw_title = raw_title[:-3]
        filename_stem = sanitize_filename_component(raw_title, max_len=120)
        if not filename_stem:
            filename_stem = "enhanced_output"
        return f"{filename_stem}.md"

    def _build_unit_asset_prefix(self, unit: SemanticUnit) -> str:
        """方法说明：RichTextPipeline._build_unit_asset_prefix 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return build_unit_asset_prefix(unit)

    def _build_action_brief(self, action: Dict[str, Any], classification: Dict[str, Any], index: int) -> str:
        """方法说明：RichTextPipeline._build_action_brief 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return build_action_brief(action, classification, index)

    def _build_request_base_name(self, unit: SemanticUnit, suffix: str) -> str:
        """方法说明：RichTextPipeline._build_request_base_name 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return build_request_base_name(unit, suffix)

    def _build_unit_relative_request_id(self, unit: SemanticUnit, suffix: str) -> str:
        """
        生成用于外部提取阶段的相对路径 ID，确保素材在提取时直接写入 `assets/{unit_id}/`。

        为什么：Phase2A 先生成请求 ID，Java/FFmpeg 按该 ID 落盘；若 ID 不带语义单元目录，
        会导致素材扁平化堆叠，Phase2B 再匹配时需要大量兜底逻辑，且易串单元。
        """
        return build_unit_relative_request_id(unit, suffix)

    def _resolve_asset_output_path(self, name: str, ext: str) -> str:
        """方法说明：RichTextPipeline._resolve_asset_output_path 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return resolve_asset_output_path(self.assets_dir, name, ext)

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
        return self.subtitle_repo.align_to_sentence_start(timestamp)
    
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
        return self.subtitle_repo.align_to_sentence_end(timestamp)

    def _clamp_time_range(self, start_sec: float, end_sec: float) -> Tuple[float, float]:
        """
        做什么：对时间区间做安全裁剪与归一化（对齐 visual_feature_extractor.py 的边界策略）。
        为什么：动作包络扩边/整段回退可能产生负数/越界/反向区间，导致 FFmpeg/OpenCV 读帧失败或空素材。
        权衡：当 video_duration 不可用时，仅保证 start>=0 且 end>=start，无法完全阻止越界，但不破坏主流程。
        """
        return self.subtitle_repo.clamp_time_range(
            start_sec,
            end_sec,
            video_duration=float(getattr(self, "video_duration", 0.0) or 0.0),
        )

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
        return merge_action_segments(action_segments, gap_threshold_sec=gap_threshold_sec)

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
        return compute_action_envelope(
            unit=unit,
            action_start=action_start,
            action_end=action_end,
            sentence_start=sentence_start,
            sentence_end=sentence_end,
            knowledge_type=knowledge_type,
            short_unit_threshold_sec=short_unit_threshold_sec,
            pre_buffer_sec=pre_buffer_sec,
            post_buffer_sec=post_buffer_sec,
            video_duration=float(getattr(self, "video_duration", 0.0) or 0.0),
        )
    
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
        self.subtitle_repo.set_paths(step6_path=step6_path, clear_cache=True)
        self.step6_path = self.subtitle_repo.step6_path
        return self.subtitle_repo.load_step6_paragraphs()
    
    
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
            total_duration_sec=self.video_duration,
        )

        group_name_to_fallback_id: Dict[str, int] = {}
        next_fallback_group_id = 1
        grouped_units: Dict[int, Dict[str, Any]] = {}
        ordered_units = sorted(
            list(units or []),
            key=lambda unit_item: (
                float(getattr(unit_item, "start_sec", 0.0) or 0.0),
                str(getattr(unit_item, "unit_id", "") or ""),
            ),
        )

        for unit in ordered_units:
            group_name = str(getattr(unit, "group_name", "") or "").strip()
            if not group_name:
                group_name = str(getattr(unit, "knowledge_topic", "") or "").strip() or "未命名知识点"
            unit.group_name = group_name

            group_id = int(getattr(unit, "group_id", 0) or 0)
            if group_id <= 0:
                normalized_group_key = group_name.lower()
                if normalized_group_key not in group_name_to_fallback_id:
                    group_name_to_fallback_id[normalized_group_key] = next_fallback_group_id
                    next_fallback_group_id += 1
                group_id = group_name_to_fallback_id[normalized_group_key]
                unit.group_id = group_id

            group_bucket = grouped_units.setdefault(
                group_id,
                {
                    "group_name": group_name,
                    "reason": "",
                    "units": [],
                },
            )
            if not str(group_bucket.get("group_name", "") or "").strip():
                group_bucket["group_name"] = group_name
            group_reason = str(getattr(unit, "group_reason", "") or "").strip()
            if group_reason and not str(group_bucket.get("reason", "") or "").strip():
                group_bucket["reason"] = group_reason
            group_bucket["units"].append(unit)

        for group_id in sorted(grouped_units.keys()):
            group_bucket = grouped_units[group_id]
            group_units = sorted(
                list(group_bucket.get("units", [])),
                key=lambda unit_item: (
                    float(getattr(unit_item, "start_sec", 0.0) or 0.0),
                    str(getattr(unit_item, "unit_id", "") or ""),
                ),
            )
            section_items = []
            for unit in group_units:
                materials = getattr(unit, "materials", None)
                if materials is None:
                    materials = MaterialSet()
                section_items.append(create_section_from_semantic_unit(unit, materials))

            group_name = str(group_bucket.get("group_name", "") or "").strip() or f"知识点分组{group_id}"
            group_reason = str(group_bucket.get("reason", "") or "").strip() or "同一核心论点聚合"
            doc.add_group(
                KnowledgeGroup(
                    group_id=group_id,
                    group_name=group_name,
                    reason=group_reason,
                    units=section_items,
                )
            )

        return doc

    def _serialize_semantic_units(self, units: List[SemanticUnit]) -> Dict[str, Any]:
        """将语义单元按 knowledge_groups 结构序列化，避免在 unit 层重复 group 元信息。"""
        group_name_to_fallback_id: Dict[str, int] = {}
        next_fallback_group_id = 1
        ordered_units = sorted(
            list(units or []),
            key=lambda unit_item: (
                float(getattr(unit_item, "start_sec", 0.0) or 0.0),
                str(getattr(unit_item, "unit_id", "") or ""),
            ),
        )
        flat_units_with_group_meta: List[Dict[str, Any]] = []

        for unit in ordered_units:
            group_name = str(getattr(unit, "group_name", "") or "").strip()
            if not group_name:
                group_name = str(getattr(unit, "knowledge_topic", "") or "").strip() or "未命名知识点"
            unit.group_name = group_name

            group_id = int(getattr(unit, "group_id", 0) or 0)
            if group_id <= 0:
                normalized_group_key = group_name.lower()
                if normalized_group_key not in group_name_to_fallback_id:
                    group_name_to_fallback_id[normalized_group_key] = next_fallback_group_id
                    next_fallback_group_id += 1
                group_id = group_name_to_fallback_id[normalized_group_key]
                unit.group_id = group_id

            group_reason = str(getattr(unit, "group_reason", "") or "").strip() or "同一核心论点聚合"
            material_requests = (
                getattr(unit, "_material_requests", MaterialRequests([], [], []))
                if hasattr(unit, "_material_requests")
                else MaterialRequests([], [], [])
            )
            unit_payload = {
                "unit_id": unit.unit_id,
                "start_sec": unit.start_sec,
                "end_sec": unit.end_sec,
                "full_text": getattr(unit, "full_text", ""),
                "text": getattr(unit, "full_text", ""),
                "stable_islands": getattr(unit, "stable_islands", []),
                "action_segments": getattr(unit, "action_segments", []),
                "material_requests": {
                    "screenshot_requests": [
                        {
                            "screenshot_id": request_item.screenshot_id,
                            "timestamp_sec": request_item.timestamp_sec,
                            "label": request_item.label,
                            "semantic_unit_id": request_item.semantic_unit_id,
                            "frame_reason": str(getattr(request_item, "frame_reason", "") or ""),
                            "ocr_text": str(getattr(request_item, "ocr_text", "") or ""),
                        }
                        for request_item in list(material_requests.screenshot_requests or [])
                    ],
                    "clip_requests": [
                        {
                            **{
                                "clip_id": request_item.clip_id,
                                "start_sec": request_item.start_sec,
                                "end_sec": request_item.end_sec,
                                "knowledge_type": request_item.knowledge_type,
                                "semantic_unit_id": request_item.semantic_unit_id,
                            },
                            **({"segments": request_item.segments} if getattr(request_item, "segments", None) else {}),
                        }
                        for request_item in list(material_requests.clip_requests or [])
                    ],
                },
                "knowledge_type": getattr(unit, "knowledge_type", ""),
                "knowledge_topic": getattr(unit, "knowledge_topic", ""),
                "mult_steps": getattr(unit, "mult_steps", False),
                "cv_validated": getattr(unit, "cv_validated", False),
                "instructional_steps": getattr(unit, "instructional_steps", []),
                "action_units": getattr(unit, "action_units", []),
                "crossed_stable_islands": getattr(
                    unit,
                    "crossed_stable_islands",
                    {
                        "stage1": [],
                        "stage2": [],
                    },
                ),
                "group_id": group_id,
                "group_name": group_name,
                "group_reason": group_reason,
            }
            flat_units_with_group_meta.append(unit_payload)

        return build_grouped_semantic_units_payload(
            flat_units_with_group_meta,
            schema_version="phase2a.grouped.v1",
            default_group_reason="同一核心论点聚合",
            strip_unit_group_fields=True,
        )

    def _extract_semantic_units_from_payload(self, payload: Any) -> List[Dict[str, Any]]:
        """统一拉平语义单元 payload，兼容 grouped 与 legacy 两种结构。"""
        return normalize_semantic_units_payload(payload)

    def _save_semantic_units(
        self,
        units: List[SemanticUnit],
        output_path: str,
        mirror_output_path: Optional[str] = None,
    ):
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
        data = self._serialize_semantic_units(units)
        self.latest_phase2a_semantic_units_payload = self._extract_semantic_units_from_payload(data)
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        async_write_enabled = str(os.getenv("PHASE2_ASYNC_PERSIST_WRITES", "1")).strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }

        if async_write_enabled:
            from services.python_grpc.src.common.utils.async_disk_writer import enqueue_json_write

            enqueue_json_write(str(output_file), data, ensure_ascii=False, indent=2)
            logger.info(f"Queued {len(units)} semantic units to async writer: {output_file}")
        else:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(units)} semantic units to {output_file}")

        if mirror_output_path:
            try:
                mirror_file = Path(mirror_output_path)
                mirror_file.parent.mkdir(parents=True, exist_ok=True)
                if async_write_enabled:
                    from services.python_grpc.src.common.utils.async_disk_writer import enqueue_json_write

                    enqueue_json_write(str(mirror_file), data, ensure_ascii=False, indent=2)
                    logger.info(f"Queued mirrored semantic units to async writer: {mirror_file}")
                else:
                    with open(mirror_file, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    logger.info(f"Mirrored semantic units to {mirror_file}")
            except Exception as error:
                logger.warning(f"Mirror semantic units failed: {error}")
    
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
            raw_payload = json.load(f)

        data = self._extract_semantic_units_from_payload(raw_payload)
        
        units = []
        material_requests_map: Dict[str, MaterialRequests] = {}
        group_name_to_id: Dict[str, int] = {}
        next_group_id = 1
        
        for item in data:
            group_name = str(item.get("group_name", "") or "").strip()
            if not group_name:
                group_name = str(item.get("knowledge_topic", "") or "").strip() or "未命名知识点"
            group_id = int(item.get("group_id", 0) or 0)
            if group_id <= 0:
                normalized_group_name = group_name.lower()
                if normalized_group_name not in group_name_to_id:
                    group_name_to_id[normalized_group_name] = next_group_id
                    next_group_id += 1
                group_id = group_name_to_id[normalized_group_name]

            unit = SemanticUnit(
                unit_id=item["unit_id"],
                knowledge_type=item.get("knowledge_type", "abstract"),
                knowledge_topic=item.get("knowledge_topic", "未知主题"),
                full_text=item.get("full_text", item.get("text", "")),
                source_paragraph_ids=item.get("source_paragraph_ids", []),
                source_sentence_ids=item.get("source_sentence_ids", []),
                group_id=group_id,
                group_name=group_name,
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
            unit.group_reason = str(item.get("group_reason", "") or "").strip()
            
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

        self._merge_material_requests_from_vl_cache(
            semantic_units_json_path=json_path,
            material_requests_map=material_requests_map,
        )

        return units, material_requests_map

    def _merge_material_requests_from_vl_cache(
        self,
        semantic_units_json_path: str,
        material_requests_map: Dict[str, MaterialRequests],
    ) -> None:
        """
        做什么：当 semantic_units 中 material_requests 为空时，尝试从 VL 产物回填。
        为什么：恢复“请求驱动匹配”，避免 assemble_only 主要依赖目录扫描兜底。
        权衡：仅补空，不覆盖已存在请求；缓存损坏时静默降级，不阻断主流程。
        """
        if not material_requests_map:
            return

        semantic_dir = Path(semantic_units_json_path).resolve().parent
        candidate_dirs: List[Path] = [semantic_dir]
        if semantic_dir.name.lower() in {"intermediates", "immediates"}:
            candidate_dirs.append(semantic_dir.parent)
        for root_dir in list(candidate_dirs):
            candidate_dirs.append(root_dir / "intermediates")
            candidate_dirs.append(root_dir / "immediates")

        ordered_dirs: List[Path] = []
        seen_dir_keys: set[str] = set()
        for directory in candidate_dirs:
            try:
                dir_key = str(directory.resolve())
            except Exception:
                dir_key = os.path.abspath(str(directory))
            if dir_key in seen_dir_keys:
                continue
            seen_dir_keys.add(dir_key)
            if directory.exists() and directory.is_dir():
                ordered_dirs.append(directory)

        candidate_paths: List[Path] = []
        for directory in ordered_dirs:
            candidate_paths.append(directory / "vl_analysis_cache.json")
            candidate_paths.append(directory / "vl_analysis_output_latest.json")
            latest_outputs = sorted(
                directory.glob("vl_analysis_output_*.json"),
                key=lambda path_item: path_item.stat().st_mtime,
                reverse=True,
            )
            if latest_outputs:
                candidate_paths.append(latest_outputs[0])

        existing_sources: List[Path] = []
        seen_source_keys: set[str] = set()
        for candidate in candidate_paths:
            try:
                source_key = str(candidate.resolve())
            except Exception:
                source_key = os.path.abspath(str(candidate))
            if source_key in seen_source_keys:
                continue
            seen_source_keys.add(source_key)
            if candidate.exists() and candidate.is_file():
                existing_sources.append(candidate)

        if not existing_sources:
            return

        source_payloads: List[Tuple[Path, Dict[str, Any]]] = []
        for source_path in existing_sources:
            try:
                with open(source_path, "r", encoding="utf-8") as file_obj:
                    source_data = json.load(file_obj)
            except Exception as error:
                logger.warning(f"[Phase2B] failed to read VL source for material backfill: path={source_path}, err={error}")
                continue
            if isinstance(source_data, dict):
                source_payloads.append((source_path, source_data))

        if not source_payloads:
            return

        screenshot_payloads: List[Dict[str, Any]] = []
        clip_payloads: List[Dict[str, Any]] = []
        for _, source_data in source_payloads:
            for key in ("aggregated_screenshots", "merged_screenshots"):
                raw_items = source_data.get(key, [])
                if isinstance(raw_items, list):
                    screenshot_payloads.extend([item for item in raw_items if isinstance(item, dict)])
            for key in ("aggregated_clips", "merged_clips"):
                raw_items = source_data.get(key, [])
                if isinstance(raw_items, list):
                    clip_payloads.extend([item for item in raw_items if isinstance(item, dict)])

        def _should_skip_cache_item(payload: Dict[str, Any]) -> bool:
            mode = str(payload.get("analysis_mode", "") or "").strip().lower()
            # legacy_action_units 产物命名不稳定，保持原策略跳过，避免污染请求驱动匹配。
            return mode == "legacy_action_units"

        by_unit_screenshots: Dict[str, List[ScreenshotRequest]] = {}
        by_unit_frame_reason: Dict[str, Dict[str, str]] = {}
        by_unit_ocr_text: Dict[str, Dict[str, str]] = {}
        seen_ss_ids: Dict[str, set] = {}
        for item in screenshot_payloads:
            if _should_skip_cache_item(item):
                continue
            unit_id = str(item.get("semantic_unit_id", "") or "").strip()
            screenshot_id = str(item.get("screenshot_id", "") or "").strip()
            if not unit_id or not screenshot_id:
                continue
            if unit_id not in material_requests_map:
                continue
            try:
                timestamp_sec = float(item.get("timestamp_sec", 0.0) or 0.0)
            except Exception:
                timestamp_sec = 0.0
            label = str(item.get("label", "") or Path(screenshot_id).name).strip()
            if not label:
                label = Path(screenshot_id).name
            frame_reason = str(item.get("frame_reason", "") or "").strip()
            ocr_text = str(item.get("ocr_text", "") or "").strip()
            if frame_reason:
                by_unit_frame_reason.setdefault(unit_id, {}).setdefault(screenshot_id, frame_reason)
            if ocr_text:
                by_unit_ocr_text.setdefault(unit_id, {}).setdefault(screenshot_id, ocr_text)

            unit_seen = seen_ss_ids.setdefault(unit_id, set())
            if screenshot_id in unit_seen:
                continue
            unit_seen.add(screenshot_id)
            by_unit_screenshots.setdefault(unit_id, []).append(
                ScreenshotRequest(
                    screenshot_id=screenshot_id,
                    timestamp_sec=timestamp_sec,
                    label=label,
                    semantic_unit_id=unit_id,
                    frame_reason=frame_reason,
                    ocr_text=ocr_text,
                )
            )

        by_unit_clips: Dict[str, List[ClipRequest]] = {}
        seen_clip_ids: Dict[str, set] = {}
        for item in clip_payloads:
            if _should_skip_cache_item(item):
                continue
            unit_id = str(item.get("semantic_unit_id", "") or "").strip()
            clip_id = str(item.get("clip_id", "") or "").strip()
            if not unit_id or not clip_id:
                continue
            if unit_id not in material_requests_map:
                continue
            try:
                start_sec = float(item.get("start_sec", 0.0) or 0.0)
                end_sec = float(item.get("end_sec", start_sec) or start_sec)
            except Exception:
                continue
            if end_sec < start_sec:
                start_sec, end_sec = end_sec, start_sec

            raw_segments = item.get("segments", None)
            segments: Optional[List[Dict[str, float]]] = None
            if isinstance(raw_segments, list):
                normalized_segments: List[Dict[str, float]] = []
                for seg in raw_segments:
                    if not isinstance(seg, dict):
                        continue
                    try:
                        seg_start = float(seg.get("start_sec", 0.0) or 0.0)
                        seg_end = float(seg.get("end_sec", seg_start) or seg_start)
                    except Exception:
                        continue
                    if seg_end < seg_start:
                        seg_start, seg_end = seg_end, seg_start
                    normalized_segments.append(
                        {
                            "start_sec": seg_start,
                            "end_sec": seg_end,
                        }
                    )
                if normalized_segments:
                    segments = normalized_segments

            knowledge_type = str(item.get("knowledge_type", "process") or "process").strip() or "process"

            unit_seen = seen_clip_ids.setdefault(unit_id, set())
            if clip_id in unit_seen:
                continue
            unit_seen.add(clip_id)
            by_unit_clips.setdefault(unit_id, []).append(
                ClipRequest(
                    clip_id=clip_id,
                    start_sec=start_sec,
                    end_sec=end_sec,
                    knowledge_type=knowledge_type,
                    semantic_unit_id=unit_id,
                    segments=segments,
                    source_action_ids=item.get("source_action_ids"),
                    merged_from_count=item.get("merged_from_count"),
                )
            )

        merged_units = 0
        merged_screenshot_count = 0
        merged_clip_count = 0
        for unit_id, requests in list(material_requests_map.items()):
            if requests is None:
                continue

            filled_ss = False
            filled_clip = False
            cached_ss = by_unit_screenshots.get(unit_id, [])
            if cached_ss:
                existing_ss = list(requests.screenshot_requests or [])
                if not existing_ss:
                    requests.screenshot_requests = list(cached_ss)
                    merged_screenshot_count += len(cached_ss)
                    filled_ss = True
                else:
                    existing_ss_ids = {
                        str(getattr(req_item, "screenshot_id", "") or "").strip()
                        for req_item in existing_ss
                    }
                    appended_count = 0
                    for cached_req in cached_ss:
                        cached_id = str(getattr(cached_req, "screenshot_id", "") or "").strip()
                        if not cached_id or cached_id in existing_ss_ids:
                            continue
                        existing_ss.append(cached_req)
                        existing_ss_ids.add(cached_id)
                        appended_count += 1
                    if appended_count > 0:
                        requests.screenshot_requests = existing_ss
                        merged_screenshot_count += appended_count
                        filled_ss = True

            # 不覆盖已有请求，仅补齐缺失的 frame_reason/ocr_text。
            frame_reason_by_id = by_unit_frame_reason.get(unit_id, {})
            ocr_text_by_id = by_unit_ocr_text.get(unit_id, {})
            if requests.screenshot_requests:
                for request_item in requests.screenshot_requests:
                    screenshot_id = str(getattr(request_item, "screenshot_id", "") or "").strip()
                    if not screenshot_id:
                        continue
                    if not str(getattr(request_item, "frame_reason", "") or "").strip():
                        cached_reason = str(frame_reason_by_id.get(screenshot_id, "") or "").strip()
                        if cached_reason:
                            request_item.frame_reason = cached_reason
                    if not str(getattr(request_item, "ocr_text", "") or "").strip():
                        cached_ocr = str(ocr_text_by_id.get(screenshot_id, "") or "").strip()
                        if cached_ocr:
                            request_item.ocr_text = cached_ocr

            cached_clips = by_unit_clips.get(unit_id, [])
            if cached_clips:
                existing_clips = list(requests.clip_requests or [])
                if not existing_clips:
                    requests.clip_requests = list(cached_clips)
                    merged_clip_count += len(cached_clips)
                    filled_clip = True
                else:
                    existing_clip_ids = {
                        str(getattr(req_item, "clip_id", "") or "").strip()
                        for req_item in existing_clips
                    }
                    appended_count = 0
                    for cached_req in cached_clips:
                        cached_id = str(getattr(cached_req, "clip_id", "") or "").strip()
                        if not cached_id or cached_id in existing_clip_ids:
                            continue
                        existing_clips.append(cached_req)
                        existing_clip_ids.add(cached_id)
                        appended_count += 1
                    if appended_count > 0:
                        requests.clip_requests = existing_clips
                        merged_clip_count += appended_count
                        filled_clip = True

            if filled_ss or filled_clip:
                merged_units += 1

        if merged_units > 0:
            source_preview = [str(path_item) for path_item, _ in source_payloads[:3]]
            if len(source_payloads) > 3:
                source_preview.append(f"...(+{len(source_payloads) - 3} more)")
            logger.info(
                "[Phase2B] backfilled material requests from VL sources: units=%s, screenshots=%s, clips=%s, sources=%s",
                merged_units,
                merged_screenshot_count,
                merged_clip_count,
                ",".join(source_preview),
            )

    async def analyze_only(self) -> Tuple[List[ScreenshotRequest], List[ClipRequest], str]:
        """仅执行 Phase2A：语义切分 + 模态分析 + 素材请求生成。"""
        logger.info("[Phase2A] analyze_only start")
        semantic_units_path = os.path.join(self.output_dir, "semantic_units_phase2a.json")
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        semantic_units_intermediate_path = os.path.join(
            self.output_dir,
            "intermediates",
            "semantic_units_phase2a.json",
        )

        if not self.paragraphs:
            logger.warning("[Phase2A] step6 paragraphs is empty, saving empty semantic units")
            self._save_semantic_units(
                [],
                semantic_units_path,
                mirror_output_path=semantic_units_intermediate_path,
            )
            return [], [], semantic_units_path

        sentence_timestamps = self._build_sentence_timestamps()
        segmentation_result = await self.segmenter.segment(
            paragraphs=self.paragraphs,
            sentence_timestamps=sentence_timestamps,
        )
        units = list(getattr(segmentation_result, "semantic_units", []) or [])

        if not units:
            logger.warning("[Phase2A] semantic segmentation produced no units")
            self._save_semantic_units(
                [],
                semantic_units_path,
                mirror_output_path=semantic_units_intermediate_path,
            )
            return [], [], semantic_units_path

        self._save_semantic_units(
            units,
            semantic_units_path,
            mirror_output_path=semantic_units_intermediate_path,
        )
        logger.info(
            "[Phase2A] checkpoint saved after segmentation: "
            f"units={len(units)}, path={semantic_units_path}"
        )

        units = await self._apply_modality_classification(units)

        screenshot_requests: List[ScreenshotRequest] = []
        clip_requests: List[ClipRequest] = []

        for unit in units:
            try:
                normalized_actions: List[Dict[str, Any]] = []
                for action in list(getattr(unit, "action_segments", []) or []):
                    if not isinstance(action, dict):
                        continue
                    item = dict(action)
                    item["start_sec"] = item.get("start_sec", item.get("start", unit.start_sec))
                    item["end_sec"] = item.get("end_sec", item.get("end", unit.end_sec))

                    internal_islands = []
                    for island in list(item.get("internal_stable_islands", []) or []):
                        if not isinstance(island, dict):
                            continue
                        island_item = dict(island)
                        if "start" not in island_item:
                            island_item["start"] = island_item.get("start_sec", item["start_sec"])
                        if "end" not in island_item:
                            island_item["end"] = island_item.get("end_sec", item["end_sec"])
                        internal_islands.append(island_item)
                    if internal_islands:
                        item["internal_stable_islands"] = internal_islands
                    normalized_actions.append(item)
                if normalized_actions:
                    unit.action_segments = normalized_actions

                normalized_islands: List[Dict[str, Any]] = []
                for island in list(getattr(unit, "stable_islands", []) or []):
                    if not isinstance(island, dict):
                        continue
                    island_item = dict(island)
                    island_item["start_sec"] = island_item.get("start_sec", island_item.get("start", unit.start_sec))
                    island_item["end_sec"] = island_item.get("end_sec", island_item.get("end", unit.end_sec))
                    if "start" not in island_item:
                        island_item["start"] = island_item["start_sec"]
                    if "end" not in island_item:
                        island_item["end"] = island_item["end_sec"]
                    normalized_islands.append(island_item)
                if normalized_islands:
                    unit.stable_islands = normalized_islands

                material_requests = await self._collect_material_requests(unit)
                unit._material_requests = material_requests
                screenshot_requests.extend(material_requests.screenshot_requests)
                clip_requests.extend(material_requests.clip_requests)
            except Exception as error:
                logger.exception(
                    f"[Phase2A] unit processing failed: unit_id={getattr(unit, 'unit_id', '')}, error={error}"
                )
                unit._material_requests = MaterialRequests([], [], [])
            finally:
                self._save_semantic_units(
                    units,
                    semantic_units_path,
                    mirror_output_path=semantic_units_intermediate_path,
                )

        self._save_semantic_units(
            units,
            semantic_units_path,
            mirror_output_path=semantic_units_intermediate_path,
        )
        logger.info(
            f"[Phase2A] analyze_only done: units={len(units)}, screenshots={len(screenshot_requests)}, clips={len(clip_requests)}"
        )
        return screenshot_requests, clip_requests, semantic_units_path

    async def analyze_segmentation_only(self) -> str:
        """仅执行 Phase2A 语义切分并落盘，不生成任何素材请求。"""
        logger.info("[Phase2A] analyze_segmentation_only start")
        semantic_units_path = os.path.join(self.output_dir, "semantic_units_phase2a.json")
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        semantic_units_intermediate_path = os.path.join(
            self.output_dir,
            "intermediates",
            "semantic_units_phase2a.json",
        )

        if not self.paragraphs:
            logger.warning("[Phase2A] step6 paragraphs is empty, saving empty semantic units")
            self._save_semantic_units(
                [],
                semantic_units_path,
                mirror_output_path=semantic_units_intermediate_path,
            )
            return semantic_units_path

        sentence_timestamps = self._build_sentence_timestamps()
        segmentation_result = await self.segmenter.segment(
            paragraphs=self.paragraphs,
            sentence_timestamps=sentence_timestamps,
        )
        units = list(getattr(segmentation_result, "semantic_units", []) or [])

        if not units:
            logger.warning("[Phase2A] semantic segmentation produced no units")
            self._save_semantic_units(
                [],
                semantic_units_path,
                mirror_output_path=semantic_units_intermediate_path,
            )
            return semantic_units_path

        self._save_semantic_units(
            units,
            semantic_units_path,
            mirror_output_path=semantic_units_intermediate_path,
        )
        logger.info(
            "[Phase2A] checkpoint saved after segmentation: "
            f"units={len(units)}, path={semantic_units_path}"
        )
        logger.info(f"[Phase2A] analyze_segmentation_only done: units={len(units)}")
        return semantic_units_path

    async def assemble_only(
        self,
        semantic_units_json_path: str,
        screenshots_dir: str,
        clips_dir: str,
        title: str,
    ) -> Tuple[str, str]:
        """仅执行 Phase2B：外部素材映射 + 富文本文档组装。"""
        from services.python_grpc.src.content_pipeline.markdown_enhancer import MarkdownEnhancer

        logger.info("[Phase2B] assemble_only start")

        if not semantic_units_json_path or not os.path.exists(semantic_units_json_path):
            raise FileNotFoundError(f"semantic_units_json not found: {semantic_units_json_path}")

        self._refresh_subtitle_context_from_semantic_units(semantic_units_json_path)
        units, material_requests_map = self._load_semantic_units(semantic_units_json_path)

        for unit in units:
            material_requests = material_requests_map.get(
                unit.unit_id,
                MaterialRequests([], [], []),
            )
            self._apply_external_materials(
                unit=unit,
                screenshots_dir=screenshots_dir,
                clips_dir=clips_dir,
                material_requests=material_requests,
            )

        self._flush_image_match_audit()

        document_title = str(title or "视频内容").strip() or "视频内容"
        document = self._assemble_document(units, document_title)

        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        markdown_filename = self._build_enhanced_markdown_filename(document_title)
        markdown_path = os.path.join(self.output_dir, markdown_filename)
        json_path = os.path.join(self.output_dir, "result.json")

        document.to_json(json_path)
        enhancer = MarkdownEnhancer()
        if not enhancer.enabled:
            logger.warning("[Phase2B] MarkdownEnhancer disabled (DEEPSEEK_API_KEY not set), using rule-based flow")
        try:
            enhanced_markdown = await enhancer.enhance(
                json_path,
                subject="数据结构与算法",
                markdown_dir=self.output_dir,
            )
            with open(markdown_path, "w", encoding="utf-8") as file_obj:
                file_obj.write(str(enhanced_markdown or ""))
        except Exception as error:
            logger.error(f"[Phase2B] Markdown enhancement failed, fallback to base markdown: {error}")
            document.to_markdown(markdown_path, assets_relative_dir=self.config.assets_subdir)

        groups = list(getattr(document, "knowledge_groups", []) or [])
        if hasattr(document, "total_sections"):
            total_sections = int(document.total_sections())
        else:
            total_sections = len(getattr(document, "sections", []) or [])
        logger.info(
            f"[Phase2B] assemble_only done: groups={len(groups)}, sections={total_sections}, markdown={markdown_path}, json={json_path}"
        )
        return markdown_path, json_path

    def _refresh_subtitle_context_from_semantic_units(self, semantic_units_json_path: str) -> None:
        """
        做什么：在 Phase2B 组装前，按 semantic_units 所在目录刷新 step2/step6/句子时间戳上下文。
        为什么：Assemble 入口可能与 Phase2A 输出目录不一致，导致句子映射出现“字幕找不到”。
        权衡：仅在能发现真实文件时覆盖当前路径，避免破坏已有可用上下文。
        """
        semantic_path = str(semantic_units_json_path or "").strip()
        if not semantic_path:
            return

        semantic_base_dir = str(Path(semantic_path).resolve().parent)
        resolved_step2 = SubtitleRepository.resolve_intermediate_path(
            provided_path=self.step2_path,
            output_dir=semantic_base_dir,
            candidate_names=SubtitleRepository.DEFAULT_STEP2_CANDIDATES,
        )
        resolved_step6 = SubtitleRepository.resolve_intermediate_path(
            provided_path=self.step6_path,
            output_dir=semantic_base_dir,
            candidate_names=SubtitleRepository.DEFAULT_STEP6_CANDIDATES,
        )
        resolved_sentence_ts = SubtitleRepository.resolve_intermediate_path(
            provided_path=self.sentence_timestamps_path,
            output_dir=semantic_base_dir,
            candidate_names=SubtitleRepository.DEFAULT_SENTENCE_TS_CANDIDATES,
        )

        current_step2 = str(self.step2_path or "").strip()
        current_step6 = str(self.step6_path or "").strip()
        current_sentence_ts = str(self.sentence_timestamps_path or "").strip()

        if (
            resolved_step2 == current_step2
            and resolved_step6 == current_step6
            and resolved_sentence_ts == current_sentence_ts
        ):
            return

        self.subtitle_repo.set_paths(
            step2_path=resolved_step2,
            step6_path=resolved_step6,
            sentence_timestamps_path=resolved_sentence_ts,
            clear_cache=True,
        )
        self.step2_path = self.subtitle_repo.step2_path
        self.step6_path = self.subtitle_repo.step6_path
        self.sentence_timestamps_path = self.subtitle_repo.sentence_timestamps_path
        self.subtitles = self.subtitle_repo.load_step2_subtitles()
        self.paragraphs = self.subtitle_repo.load_step6_paragraphs()
        logger.info(
            "[Phase2B] subtitle context refreshed from semantic_units dir: "
            f"step2={bool(self.step2_path)}, step6={bool(self.step6_path)}, "
            f"sentence_ts={bool(self.sentence_timestamps_path)}"
        )

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
        return self.subtitle_repo.build_sentence_timestamps(prefer_external=True)

    def _map_timestamp_to_sentence_id(
        self,
        timestamp_sec: float,
        sentence_timestamps: Dict[str, Dict[str, float]],
    ) -> str:
        """根据时间戳映射最匹配的字幕句子 ID。"""
        # sentence_timestamps 参数保留兼容签名，实际由仓储统一维护。
        return self.subtitle_repo.map_timestamp_to_sentence_id(timestamp_sec)

    def _get_sentence_text_by_id(self, sentence_id: str) -> str:
        """按 sentence_id 获取字幕文本，支持 S001 索引与 subtitle_id。"""
        return self.subtitle_repo.get_sentence_text(sentence_id)
    
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
                    # 历史乱码注释已清理。
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
        """委托到 `material_flow`，保持主类职责聚焦。"""
        return await generate_materials(self, unit)
    
    async def _collect_material_requests(self, unit: SemanticUnit) -> MaterialRequests:
        """委托到 `material_flow`，保持主类职责聚焦。"""
        return await collect_material_requests(self, unit)
    
    def _apply_external_materials(
        self,
        unit: SemanticUnit,
        screenshots_dir: str,
        clips_dir: str,
        material_requests: MaterialRequests
    ):
        """委托到 `material_flow`，保持主类职责聚焦。"""
        return apply_external_materials(self, unit, screenshots_dir, clips_dir, material_requests)

    async def _select_screenshot(
        self,
        start_sec: float,
        end_sec: float,
        name: str
    ) -> str:
        """方法说明：RichTextPipeline._select_screenshot 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：RichTextPipeline._extract_frame_ffmpeg_fallback 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：RichTextPipeline._extract_action_clip 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：RichTextPipeline._extract_action_clip_ffmpeg 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：RichTextPipeline._extract_video_clip 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
        """方法说明：RichTextPipeline._extract_clip_ffmpeg_fallback 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
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
