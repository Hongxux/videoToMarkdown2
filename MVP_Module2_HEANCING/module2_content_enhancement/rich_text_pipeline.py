"""
Rich Text Pipeline - 完整语义单元到富文本流水线 (V2)

输入:
- video_path: 视频文件路径
- step2_path: step2_correction_output.json (字幕+时间戳)
- step6_path: step6_merge_cross_output.json (去噪段落)

输出:
- RichTextDocument (可导出 Markdown/HTML/JSON)

流程:
1. 语义单元切分 (LLM聚合)
2. V7.x 模态分类 (CV检测)
3. 素材生成 (使用 ScreenshotSelector 和 VideoClipExtractor)
4. 富文本组装

V2 变更:
- 视频片段: 使用 VideoClipExtractor 传递动作单元起止时间
- 截图提取: 使用 ScreenshotSelector 传递字幕对应时间范围
"""

import os
import difflib
import cv2
import json
import logging
import asyncio
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
    """流水线配置"""
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
    """截图请求 (Phase2A 输出)"""
    screenshot_id: str          # 截图ID，如 "SU001_action_1_head"
    timestamp_sec: float        # 截图时间点（秒）
    label: str                  # 标签 (head/tail/stable/fallback)
    semantic_unit_id: str       # 所属语义单元ID


@dataclass
class ClipRequest:
    """视频切片请求 (Phase2A 输出)"""
    clip_id: str                # 切片ID，如 "SU001_action_1"
    start_sec: float            # 起始时间（秒）
    end_sec: float              # 结束时间（秒）
    knowledge_type: str         # 知识类型 (过程性/讲解型/结构性/概念性)
    semantic_unit_id: str       # 所属语义单元ID


@dataclass
class MaterialRequests:
    """素材需求集合 (Phase2A 输出，交给Java执行)"""
    screenshot_requests: List[ScreenshotRequest]
    clip_requests: List[ClipRequest]
    action_classifications: List[Dict[str, Any]]  # 动作分类结果


class RichTextPipeline:
    """
    完整语义单元到富文本流水线 (V2)
    
    设计原则:
    - 使用 ScreenshotSelector 选择最佳帧 (传递字幕时间范围)
    - 使用 VideoClipExtractor 提取视频片段 (传递动作单元起止时间)
    - 稳定岛位置由 CVKnowledgeValidator 计算
    """
    
    def __init__(
        self,
        video_path: str,
        step2_path: str,
        step6_path: str,
        output_dir: str,
        config: PipelineConfig = None,
        sentence_timestamps_path: str = None
    ):
        self.video_path = video_path
        self.step2_path = step2_path
        self.step6_path = step6_path
        self.output_dir = output_dir
        self.config = config or PipelineConfig()
        self.sentence_timestamps_path = sentence_timestamps_path
        
        # 创建输出目录
        self.assets_dir = os.path.join(output_dir, self.config.assets_subdir)
        Path(self.assets_dir).mkdir(parents=True, exist_ok=True)
        
        # 加载数据 (Phase 2B 组装模式下可能为空)
        if step2_path:
            logger.info(f"Loading step2: {step2_path}")
            self.subtitles = load_corrected_subtitles(step2_path)
        else:
            logger.info("Skip loading step2 (empty path)")
            self.subtitles = []
        
        if step6_path:
            logger.info(f"Loading step6: {step6_path}")
            self.paragraphs = self._load_paragraphs(step6_path)
        else:
            logger.info("Skip loading step6 (empty path)")
            self.paragraphs = []
        
        # 初始化组件
        self.segmenter = SemanticUnitSegmenter()
        
        # ScreenshotSelector (懒加载，需要 visual_extractor 依赖)
        self._screenshot_selector = None
        
        # VideoClipExtractor (懒加载，需要依赖注入)
        self._clip_extractor = None
        
        # 视觉特征提取器 (共享依赖)
        self._visual_extractor = None
        
        # 获取视频信息
        self.video_duration = self._get_video_duration()
        
        # 💥 V7.4: 知识分类器 (用于动作单元四分类)
        self._knowledge_classifier = KnowledgeClassifier()
        if self._knowledge_classifier.enabled:
            logger.info("Knowledge classifier enabled (DeepSeek API)")
        else:
            logger.warning("Knowledge classifier disabled (API key not set)")
            
        # 💥 V7.5: 具象知识验证器 (用于过滤无效截图)
        self._concrete_validator = ConcreteKnowledgeValidator()
        if self._concrete_validator.enabled:
            logger.info("Concrete knowledge validator enabled (CV/Vision)")
        
        logger.info(f"Pipeline initialized: {len(self.subtitles)} subtitles, {len(self.paragraphs)} paragraphs")
    
    def set_visual_extractor(self, visual_extractor):
        """设置视觉特征提取器 (共享依赖)"""
        self._visual_extractor = visual_extractor
        # 初始化 ScreenshotSelector
        self._screenshot_selector = ScreenshotSelector(
            visual_extractor=visual_extractor,
            config=None  # 使用默认配置
        )
    
    def set_clip_extractor(self, extractor):
        """设置视频片段提取器 (依赖注入)"""
        self._clip_extractor = extractor
        self._clip_extractor.set_subtitles(self.subtitles)
    
    def _align_to_sentence_start(self, timestamp: float) -> float:
        """
        对齐到句子边界 (起点)
        
        找到 ≤ timestamp 的最近句子开始时间
        确保不会从句子中间开始
        """
        best_start = 0.0
        for sub in self.subtitles:
            if sub.start_sec <= timestamp:
                best_start = sub.start_sec
            else:
                break
        return best_start
    
    def _align_to_sentence_end(self, timestamp: float) -> float:
        """
        对齐到句子边界 (终点)
        
        找到 ≥ timestamp 的最近句子结束时间
        确保不会在句子中间结束
        """
        for sub in self.subtitles:
            if sub.end_sec >= timestamp:
                return sub.end_sec
        # 如果没找到，返回最后一个字幕的结束时间
        return self.subtitles[-1].end_sec if self.subtitles else timestamp
    
    def _load_paragraphs(self, step6_path: str) -> List[Dict]:
        """加载 step6 段落"""
        with open(step6_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if "output" in data and "pure_text_script" in data["output"]:
            return data["output"]["pure_text_script"]
        elif "pure_text_script" in data:
            return data["pure_text_script"]
        else:
            raise ValueError("Invalid step6 format: missing pure_text_script")
    
    def _get_video_duration(self) -> float:
        """获取视频时长"""
        cap = cv2.VideoCapture(self.video_path)
        if not cap.isOpened():
            return 0.0
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        cap.release()
        return frame_count / fps if fps > 0 else 0.0
    
    async def run(self, title: str = "") -> RichTextDocument:
        """
        执行完整流水线
        
        Returns:
            RichTextDocument
        """
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
        🔑 Phase2A: 仅执行语义分析（切分），返回基础语义单元
        
        供 gRPC AnalyzeSemanticUnits 接口调用。
        🚀 优化: 跳过视觉验证和素材策划，由 Java 并行编排模块接管。
        """
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
        🔑 Phase2B: 加载中间结果，应用外部素材，组装文档
        
        供 gRPC AssembleRichText 接口调用。
        使用Java FFmpeg生成的截图和切片进行富文本组装，并调用MarkdownEnhancer增强。
        
        Args:
            semantic_units_json_path: Phase2A 输出的语义单元JSON路径
            screenshots_dir: Java FFmpeg 生成的截图目录
            clips_dir: Java FFmpeg 生成的切片目录
            title: 文档标题
            subject: 学科名称 (用于MarkdownEnhancer层级划分)
            
        Returns:
            Tuple[str, str]: (markdown_path, json_path)
        """
        from .markdown_enhancer import MarkdownEnhancer
        
        logger.info("="*60)
        logger.info("RichTextPipeline.assemble_only() - Phase2B Start")
        logger.info("="*60)
        
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
            if requests:
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
        
        if enhancer.enabled:
            try:
                # 🚀 Stage 5: 直接 await 异步增强
                enhanced_md = await enhancer.enhance(json_path, subject)
                with open(markdown_path, 'w', encoding='utf-8') as f:
                    f.write(enhanced_md)
                logger.info(f"  → Enhanced markdown exported: {markdown_path}")
            except Exception as e:
                logger.error(f"  → Markdown enhancement failed: {e}, using fallback")
                # 回退: 使用基础Markdown
                document.to_markdown(markdown_path)
        else:
            logger.warning("  → MarkdownEnhancer disabled (DEEPSEEK_API_KEY not set)")
            # 回退: 使用基础Markdown
            document.to_markdown(markdown_path)
        
        logger.info("="*60)
        logger.info(f"Phase2B completed: {len(document.sections)} sections")
        logger.info(f"  → Markdown: {markdown_path}")
        logger.info(f"  → JSON: {json_path}")
        logger.info("="*60)
        
        return markdown_path, json_path
    
    def _apply_external_materials(self, unit, screenshots_dir: str, clips_dir: str, requests):
        """
        应用外部素材（Java FFmpeg 生成的截图和切片）
        
        Args:
            unit: SemanticUnit 对象
            screenshots_dir: 截图目录
            clips_dir: 切片目录
            requests: MaterialRequests 对象
        """
        materials = MaterialSet()
        
        # 1. 加载截图
        for ss_req in requests.screenshot_requests:
            # 尝试多种可能的文件名模式
            patterns = [
                os.path.join(screenshots_dir, f"{ss_req.screenshot_id}.jpg"),
                os.path.join(screenshots_dir, f"{ss_req.screenshot_id}.png"),
                os.path.join(screenshots_dir, f"screenshot_{ss_req.screenshot_id}.jpg"),
            ]
            
            for pattern in patterns:
                if os.path.exists(pattern):
                    materials.screenshots.append(pattern)
                    materials.labels.append(ss_req.label)
                    break
        
        # 2. 加载视频切片
        for clip_req in requests.clip_requests:
            patterns = [
                os.path.join(clips_dir, f"{clip_req.clip_id}.mp4"),
                os.path.join(clips_dir, f"clip_{clip_req.clip_id}.mp4"),
            ]
            
            for pattern in patterns:
                if os.path.exists(pattern):
                    materials.clip = pattern
                    break
        
        # 3. 保存到 unit 的 materials 属性
        unit.materials = materials
    
    def _assemble_document(self, units, title: str):
        """
        组装富文本文档
        
        Args:
            units: List[SemanticUnit]
            title: 文档标题
            
        Returns:
            RichTextDocument
        """
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
        保存语义单元到JSON (供Phase2B加载)
        
        V9.0: 新增字段
        - action_units: 带有 knowledge_type 和 confidence 的动作单元列表
        - crossed_stable_islands: 被合并跨越的稳定岛
        - cv_validated: CV 验证完成标记
        """
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
                        {"clip_id": r.clip_id, "start_sec": r.start_sec, "end_sec": r.end_sec,
                         "knowledge_type": r.knowledge_type, "semantic_unit_id": r.semantic_unit_id}
                        for r in getattr(unit, '_material_requests', MaterialRequests([], [], [])).clip_requests
                    ] if hasattr(unit, '_material_requests') else [],
                },
                # V9.0 新增字段
                "knowledge_type": getattr(unit, 'knowledge_type', ''),
                "knowledge_topic": getattr(unit, 'knowledge_topic', ''),
                "cv_validated": getattr(unit, 'cv_validated', False),
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
        加载语义单元和素材需求
        
        V9.0: 新增字段恢复
        - action_units: 带有 knowledge_type 和 confidence 的动作单元列表
        - crossed_stable_islands: 被合并跨越的稳定岛
        - cv_validated: CV 验证完成标记
        """
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
                end_sec=item["end_sec"]
            )
            unit.stable_islands = item.get("stable_islands", [])
            unit.action_segments = item.get("action_segments", [])
            
            # V9.0: 恢复新字段
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
        🚀 合并来自 Java 端的并行 CV 验证结果
        
        Args:
            units: 待处理的语义单元列表
            cv_results_path: .cv_results.json 路径
        """
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
        🚀 V9.0: 两阶段合并 + LLM分类过滤
        
        架构约束：保留现有 Java KnowledgeClassificationOrchestrator → Python 架构
        
        此方法用于本地处理，如需通过 Java 调用，应使用 ClassifyKnowledgeBatch gRPC。
        
        流程：
        1. 第一阶段合并（所有 ActionUnit，间隔 < 1s）
        2. LLM 分类（过程性知识/讲解型/结构性/概念性）
        3. 过滤：只保留过程性知识、实操、推演（不为视频生成讲解型）
        4. 第二阶段合并（筛选后的 ActionUnit，相同 knowledge_type，间隔 < 5s）
        5. 收集所有稳定岛用于截图提取
        
        Args:
            units: 语义单元列表（已合并 CV 结果）
            classifier: KnowledgeClassifier 实例
            
        Returns:
            Dict[unit_id, {
                'clip_actions': [...],      # 需要生成视频的 ActionUnit
                'all_stable_islands': [...], # 所有稳定岛用于截图
                'crossed_islands_stage1': [...],
                'crossed_islands_stage2': [...]
            }]
        """
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
        """第一阶段本地合并"""
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
        """第二阶段本地合并（需要相同 knowledge_type）"""
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
        """收集所有稳定岛用于截图"""
        all_islands = []
        seen = set()
        
        def add_island(island: Dict):
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
        构建 sentence_id → 时间戳 映射
        
        优先使用外部 sentence_timestamps.json，
        否则回退到索引映射 S001 → subtitles[0]
        
        ❌ 已移除: 文本对齐逻辑 (_align_paragraphs_to_subtitles)
        现在 KnowledgeClassifier 直接从 Step 2 读取字幕，不再依赖语义单元的时间戳正确性
        """
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
    
    async def _apply_modality_classification(
        self, 
        units: List[SemanticUnit],
        cache_path: str = None
    ) -> List[SemanticUnit]:
        """
        对每个语义单元应用 V7.x 模态分类
        """
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
                            """获取落在动作区间内的稳定岛"""
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
        """保存模态分类缓存"""
        data = {
            "units": [asdict(u) for u in units]
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def _load_modality_cache(self, path: str) -> List[SemanticUnit]:
        """加载模态分类缓存"""
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
        并行提取所有语义单元的素材
        
        使用 asyncio.Semaphore 控制并发数，避免资源竞争
        """
        MAX_CONCURRENT = 4  # 最大并发数
        semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        
        async def process_unit(idx: int, unit: SemanticUnit):
            """带信号量控制的单元素材提取"""
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
    
    async def _generate_materials(self, unit: SemanticUnit):
        """
        根据检测到的稳定岛和动作单元生成素材
        
        提取规则:
        - 规则一: 如果有 action 部分，不提取语义单元级 stable 部分的中间帧
        - 规则二: 如果 action 内部有稳定岛，提取 视频首帧 + action内部稳定岛帧 + 视频末帧
        - 无 action: 提取 stable 部分的中间帧
        """
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
            
            # 💥 后处理: 合并间隔 < 1.0s 的动作单元
            if len(action_segments) >= 2:
                merged_actions = []
                current = action_segments[0].copy()
                
                for next_action in action_segments[1:]:
                    gap = next_action.get("start_sec", 0) - current.get("end_sec", 0)
                    
                    if gap < 1.0:  # 间隔小于1秒，合并
                        # 扩展当前动作的结束时间
                        current["end_sec"] = next_action.get("end_sec", current.get("end_sec", 0))
                        # 合并内部稳定岛
                        current_islands = current.get("internal_stable_islands", [])
                        next_islands = next_action.get("internal_stable_islands", [])
                        current["internal_stable_islands"] = current_islands + next_islands
                        logger.info(f"Merged actions: gap={gap:.2f}s → [{current['start_sec']:.1f}s-{current['end_sec']:.1f}s]")
                    else:
                        merged_actions.append(current)
                        current = next_action.copy()
                
                merged_actions.append(current)
                
                if len(merged_actions) < len(action_segments):
                    logger.info(f"{unit.unit_id}: Post-merge {len(action_segments)} → {len(merged_actions)} actions")
                
                action_segments = merged_actions
            
            # 🚀 批量并行分类 (优化速度)
            batch_classifications = await self._knowledge_classifier.classify_batch(
                semantic_unit_title=getattr(unit, 'knowledge_topic', '未知主题'),
                semantic_unit_text=getattr(unit, 'full_text', getattr(unit, 'text', '')),
                action_segments=action_segments
            )
            
            for i, (action, classification) in enumerate(zip(action_segments, batch_classifications)):
                action_start = action.get("start_sec", unit.start_sec)
                action_end = action.get("end_sec", unit.end_sec)
                action_type = action.get("type", "K4_operation")
                
                # 获取该动作单元内部的稳定岛
                action_internal_islands = action.get("internal_stable_islands", [])
                
                # 💥 句子边界对齐: 确保不会从句子/动作中间截断
                # 策略: 扩展范围，不缩小 (确保完整包含动作)
                sentence_aligned_start = self._align_to_sentence_start(action_start)
                sentence_aligned_end = self._align_to_sentence_end(action_end)
                
                # 起点取更早的 (min), 终点取更晚的 (max)
                aligned_start = min(action_start, sentence_aligned_start)
                aligned_end = max(action_end, sentence_aligned_end)
                
                # Classification already done in batch
                knowledge_type = classification.get("knowledge_type", "过程性知识")
                confidence = classification.get("confidence", 0.5)
                
                # 存储分类结果到 action 中 (用于后续输出到 result.json)
                action["classification"] = classification
                
                logger.info(f"{unit.unit_id} action_{i+1}: {knowledge_type} (conf={confidence:.0%}) - {classification.get('key_evidence', '')[:30]}")
                
                # 根据分类决定素材策略
                if knowledge_type == "讲解型":
                    # 💥 降级: 讲解型只截取首尾帧 + 稳定岛截图，不提取视频
                    logger.info(f"  → Downgrade to screenshots only (讲解型)")
                    
                    # 首帧截图: 查找窗口为 [对齐起点, 动作起点]
                    head_window_end = max(aligned_start + 0.5, action_start)
                    head_ss = await self._select_screenshot(
                        start_sec=aligned_start,
                        end_sec=head_window_end,
                        name=f"{unit.unit_id}_action_{i+1}_head"
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
                            name=f"{unit.unit_id}_action_{i+1}_island_{j+1}"
                        )
                        if island_ss:
                            screenshot_paths.append(island_ss)
                            screenshot_labels.append(f"动作{i+1}稳定帧{j+1}")
                    
                    # 末帧截图: 查找窗口为 [动作终点, 对齐终点]
                    tail_window_start = min(aligned_end - 0.5, action_end)
                    tail_ss = await self._select_screenshot(
                        start_sec=tail_window_start,
                        end_sec=aligned_end,
                        name=f"{unit.unit_id}_action_{i+1}_tail"
                    )
                    if tail_ss:
                        screenshot_paths.append(tail_ss)
                        screenshot_labels.append(f"动作{i+1}末帧")
                
                else:
                    # 非讲解型: 提取视频 + 首尾帧 + 稳定岛截图
                    
                    # 1. 提取视频片段 (使用对齐后的时间范围)
                    clip_path = await self._extract_action_clip(
                        start_sec=aligned_start,
                        end_sec=aligned_end,
                        name=f"{unit.unit_id}_action_{i+1}"
                    )
                    if clip_path:
                        clip_paths.append(clip_path)
                    
                    # 2. 提取首帧截图: 查找窗口为 [对齐起点, 动作起点]
                    head_window_end = max(aligned_start + 0.5, action_start)
                    head_ss = await self._select_screenshot(
                        start_sec=aligned_start,
                        end_sec=head_window_end,
                        name=f"{unit.unit_id}_action_{i+1}_head"
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
                            name=f"{unit.unit_id}_action_{i+1}_island_{j+1}"
                        )
                        if island_ss:
                            screenshot_paths.append(island_ss)
                            screenshot_labels.append(f"动作{i+1}稳定帧{j+1}")
                    
                    # 4. 提取末帧截图: 查找窗口为 [动作终点, 对齐终点]
                    tail_window_start = min(aligned_end - 0.5, action_end)
                    tail_ss = await self._select_screenshot(
                        start_sec=tail_window_start,
                        end_sec=aligned_end,
                        name=f"{unit.unit_id}_action_{i+1}_tail"
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
                    name=f"{unit.unit_id}_stable_{i+1}"
                )
                if ss_path:
                    screenshot_paths.append(ss_path)
                    screenshot_labels.append(f"稳定帧{i+1}")
        
        else:
            # ==== 回退: 无任何检测结果 ====
            fallback_ss = await self._select_screenshot(
                start_sec=unit.start_sec,
                end_sec=unit.end_sec,
                name=f"{unit.unit_id}_fallback"
            )
            if fallback_ss:
                screenshot_paths.append(fallback_ss)
                screenshot_labels.append("截图")
        
        # ==== 组装素材集合 ====
        materials.screenshot_paths = screenshot_paths
        materials.screenshot_labels = screenshot_labels
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
        🔑 Phase2A: 收集素材需求（不执行FFmpeg）
        
        分析语义单元，返回需要的截图和切片列表，但不实际执行提取。
        这些需求将返回给Java由FFmpeg并行执行。
        
        Returns:
            MaterialRequests: 包含 screenshot_requests 和 clip_requests
        """
        screenshot_requests: List[ScreenshotRequest] = []
        clip_requests: List[ClipRequest] = []
        action_classifications: List[Dict[str, Any]] = []
        
        # 获取稳定岛和动作单元信息
        stable_islands = getattr(unit, 'stable_islands', [])
        action_segments = getattr(unit, 'action_segments', [])
        
        # 💥 后处理: 合并间隔 < 1.0s 的动作单元 (与_generate_materials保持一致)
        if len(action_segments) >= 2:
            merged_actions = []
            current = action_segments[0].copy()
            
            for next_action in action_segments[1:]:
                gap = next_action.get("start_sec", 0) - current.get("end_sec", 0)
                
                if gap < 1.0:
                    current["end_sec"] = next_action.get("end_sec", current.get("end_sec", 0))
                    current_islands = current.get("internal_stable_islands", [])
                    next_islands = next_action.get("internal_stable_islands", [])
                    current["internal_stable_islands"] = current_islands + next_islands
                else:
                    merged_actions.append(current)
                    current = next_action.copy()
            
            merged_actions.append(current)
            action_segments = merged_actions
        
        if action_segments:
            # 🚀 批量并行分类 (优化速度)
            batch_classifications = await self._knowledge_classifier.classify_batch(
                semantic_unit_title=getattr(unit, 'knowledge_topic', '未知主题'),
                semantic_unit_text=getattr(unit, 'full_text', getattr(unit, 'text', '')),
                action_segments=action_segments
            )

            # ==== 有动作单元 ====
            for i, (action, classification) in enumerate(zip(action_segments, batch_classifications)):
                action_start = action.get("start_sec", unit.start_sec)
                action_end = action.get("end_sec", unit.end_sec)
                action_internal_islands = action.get("internal_stable_islands", [])
                
                # 💥 句子边界对齐
                sentence_aligned_start = self._align_to_sentence_start(action_start)
                sentence_aligned_end = self._align_to_sentence_end(action_end)
                aligned_start = min(action_start, sentence_aligned_start)
                aligned_end = max(action_end, sentence_aligned_end)
                
                # Classification already done in batch
                knowledge_type = classification.get("knowledge_type", "过程性知识")
                confidence = classification.get("confidence", 0.5)
                
                # 存储分类结果
                action_classifications.append({
                    "time_range": [action_start, action_end],
                    **classification
                })
                
                logger.info(f"{unit.unit_id} action_{i+1}: {knowledge_type} (conf={confidence:.0%})")
                
                # 根据分类决定素材策略
                if knowledge_type == "讲解型":
                    # 讲解型: 只需要截图，不需要视频
                    # 首帧截图: 搜索窗口扩大为 动作起点 ±1.0s
                    head_search_start = max(0, aligned_start - 1.0)
                    head_search_end = aligned_start + 1.0
                    fallback_head_ts = aligned_start
                    head_ts = await self._select_screenshot_timestamp(head_search_start, head_search_end, fallback_head_ts)
                    
                    screenshot_requests.append(ScreenshotRequest(
                        screenshot_id=f"{unit.unit_id}_action_{i+1}_head",
                        timestamp_sec=head_ts,
                        label="head",
                        semantic_unit_id=unit.unit_id
                    ))
                    
                    # 稳定岛截图
                    for j, island in enumerate(action_internal_islands):
                        island_start = island.get("start", action_start)
                        island_end = island.get("end", action_end)
                        island_mid_fallback = (island_start + island_end) / 2
                        island_mid = await self._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                        screenshot_requests.append(ScreenshotRequest(
                            screenshot_id=f"{unit.unit_id}_action_{i+1}_island_{j+1}",
                            timestamp_sec=island_mid,
                            label="stable",
                            semantic_unit_id=unit.unit_id
                        ))
                    
                    # 末帧截图: 搜索窗口扩大为 动作结束点 ±1.0s
                    tail_search_start = max(0, aligned_end - 1.0)
                    tail_search_end = aligned_end + 1.0
                    fallback_tail_ts = aligned_end
                    tail_ts = await self._select_screenshot_timestamp(tail_search_start, tail_search_end, fallback_tail_ts)
                    
                    screenshot_requests.append(ScreenshotRequest(
                        screenshot_id=f"{unit.unit_id}_action_{i+1}_tail",
                        timestamp_sec=tail_ts,
                        label="tail",
                        semantic_unit_id=unit.unit_id
                    ))
                
                else:
                    # 非讲解型: 需要视频切片 + 首尾帧截图
                    # 视频切片
                    clip_requests.append(ClipRequest(
                        clip_id=f"{unit.unit_id}_action_{i+1}",
                        start_sec=aligned_start,
                        end_sec=aligned_end,
                        knowledge_type=knowledge_type,
                        semantic_unit_id=unit.unit_id
                    ))
                    
                    # 首帧截图: 搜索窗口扩大为 动作起点 ±1.0s
                    head_search_start = max(0, aligned_start - 1.0)
                    head_search_end = aligned_start + 1.0
                    fallback_head_ts = aligned_start
                    head_ts = await self._select_screenshot_timestamp(head_search_start, head_search_end, fallback_head_ts)
                    
                    screenshot_requests.append(ScreenshotRequest(
                        screenshot_id=f"{unit.unit_id}_action_{i+1}_head",
                        timestamp_sec=head_ts,
                        label="head",
                        semantic_unit_id=unit.unit_id
                    ))
                    
                    # 稳定岛截图
                    for j, island in enumerate(action_internal_islands):
                        island_start = island.get("start", action_start)
                        island_end = island.get("end", action_end)
                        island_mid_fallback = (island_start + island_end) / 2
                        island_mid = await self._select_screenshot_timestamp(island_start, island_end, island_mid_fallback)

                        screenshot_requests.append(ScreenshotRequest(
                            screenshot_id=f"{unit.unit_id}_action_{i+1}_island_{j+1}",
                            timestamp_sec=island_mid,
                            label="stable",
                            semantic_unit_id=unit.unit_id
                        ))
                    
                    # 末帧截图: 搜索窗口扩大为 动作结束点 ±1.0s
                    tail_search_start = max(0, aligned_end - 1.0)
                    tail_search_end = aligned_end + 1.0
                    fallback_tail_ts = aligned_end 
                    tail_ts = await self._select_screenshot_timestamp(tail_search_start, tail_search_end, fallback_tail_ts)
                    
                    screenshot_requests.append(ScreenshotRequest(
                        screenshot_id=f"{unit.unit_id}_action_{i+1}_tail",
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
                    screenshot_id=f"{unit.unit_id}_stable_{i+1}",
                    timestamp_sec=island_mid,
                    label="stable",
                    semantic_unit_id=unit.unit_id
                ))
        
        else:
            # ==== 回退: 无任何检测结果 ====
            fallback_ts = (unit.start_sec + unit.end_sec) / 2
            best_ts = await self._select_screenshot_timestamp(unit.start_sec, unit.end_sec, fallback_ts)
            
            screenshot_requests.append(ScreenshotRequest(
                screenshot_id=f"{unit.unit_id}_fallback",
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
        🔑 Phase2B: 应用外部素材（使用Java FFmpeg生成的截图和切片）
        
        Args:
            unit: 语义单元
            screenshots_dir: Java FFmpeg 生成的截图目录
            clips_dir: Java FFmpeg 生成的切片目录
            material_requests: Phase2A 收集的需求（用于匹配文件）
        """
        materials = MaterialSet()
        
        screenshot_paths = []
        screenshot_labels = []
        
        # 从外部目录查找截图
        for req in material_requests.screenshot_requests:
            if req.semantic_unit_id != unit.unit_id:
                continue
            
            # 尝试多种可能的文件扩展名
            for ext in [".png", ".jpg", ".jpeg"]:
                expected_path = os.path.join(screenshots_dir, f"{req.screenshot_id}{ext}")
                if os.path.exists(expected_path):
                    # 💥 V7.5: 验证截图有效性
                    is_valid = True
                    if self._concrete_validator and req.label != "head" and req.label != "tail":
                         # 首尾帧通常必须保留用于定位，中间帧/稳定岛帧需验证
                         res = self._concrete_validator.validate(expected_path)
                         if not res.should_include:
                             logger.info(f"Removing negative screenshot: {req.screenshot_id} ({res.reason})")
                             try:
                                 os.remove(expected_path)
                             except:
                                 pass
                             is_valid = False
                    
                    if is_valid:
                        screenshot_paths.append(expected_path)
                        screenshot_labels.append(req.label)
                    break
            else:
                logger.debug(f"Screenshot not found: {req.screenshot_id}")
        
        # 从外部目录查找视频切片
        clip_path = ""
        # 💥 V7.5: 严格模态检查
        # 切片的唯一依据: 是否为非讲解型
        # 如果是 "讲解型" (Explanation)，则禁止切片，只保留截图
        allow_clip = True
        k_type = str(unit.knowledge_type)
        if "讲解" in k_type or "Explanation" in k_type:
             allow_clip = False
             logger.info(f"{unit.unit_id}: Suppressed clip for Explanation type ({k_type})")

        if allow_clip:
            for req in material_requests.clip_requests:
                if req.semantic_unit_id != unit.unit_id:
                    continue
                
                for ext in [".mp4", ".webm", ".mkv"]:
                    expected_path = os.path.join(clips_dir, f"{req.clip_id}{ext}")
                    if os.path.exists(expected_path):
                        clip_path = expected_path
                        break
                
                if clip_path:
                    break
        else:
            # 如果禁止切片但文件存在，清理之
             for req in material_requests.clip_requests:
                if req.semantic_unit_id != unit.unit_id: continue
                for ext in [".mp4", ".webm", ".mkv"]:
                    p = os.path.join(clips_dir, f"{req.clip_id}{ext}")
                    if os.path.exists(p):
                        try:
                            os.remove(p)
                            logger.info(f"Cleaned up suppressed clip: {p}")
                        except: pass
        
        materials.screenshot_paths = screenshot_paths
        materials.screenshot_labels = screenshot_labels
        materials.clip_path = clip_path
        materials.action_classifications = material_requests.action_classifications
        
        unit.materials = materials
        
        logger.debug(f"{unit.unit_id}: applied {len(screenshot_paths)} external screenshots, "
                     f"clip={'Yes' if clip_path else 'No'}")
    
    # ❌ Removed: _get_subtitles_in_range() and _parse_subtitle() methods (45 lines)
    # These methods are no longer used after subtitle refactoring.
    # KnowledgeClassifier now handles subtitle retrieval directly from Step 2.
    
    async def _select_screenshot(
        self, 
        start_sec: float, 
        end_sec: float, 
        name: str
    ) -> str:
        """
        使用 ScreenshotSelector 选择最佳帧
        
        传递字幕对应的时间范围
        """
        if not self._screenshot_selector:
            logger.warning("ScreenshotSelector not available, using fallback ffmpeg direct")
            return await self._extract_frame_ffmpeg_fallback(start_sec, end_sec, name)
        
        try:
            # 调用 ScreenshotSelector (V6.2 逻辑)
            result = await self._screenshot_selector.select_screenshot(
                video_path=self.video_path,
                start_sec=start_sec,
                end_sec=end_sec,
                output_dir=self.assets_dir,
                output_name=name  # 💥 直接传递规范名称
            )
            
            if result and result.screenshot_path:
                # 💥 修复: 使用 move 而不是 copy，避免重复文件
                import shutil
                target_path = os.path.join(self.assets_dir, f"{name}.png")
                if os.path.exists(result.screenshot_path):
                    # 如果源文件和目标不同，移动文件
                    if os.path.abspath(result.screenshot_path) != os.path.abspath(target_path):
                        shutil.move(result.screenshot_path, target_path)
                    return target_path
                return result.screenshot_path
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
        🔑 视觉择优时间戳 (Phase2A 专用)
        
        选择指定范围内的最佳帧时间戳，但不实际保存图片。
        用于给 Java 提帧提供更高质量的参考点。
        """
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
        """FFmpeg 直接提取帧 (回退方案) - 异步版本"""
        import subprocess
        
        output_path = os.path.join(self.assets_dir, f"{name}.png")
        # 取中点时间
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
        
        # 使用 IO 线程池执行阻塞操作
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
        提取单个动作单元的视频片段
        
        使用 VideoClipExtractor 传递动作单元的精确起止时间
        """
        if not self._clip_extractor:
            logger.info(f"VideoClipExtractor not available for {name}, using ffmpeg fallback")
            return await self._extract_action_clip_ffmpeg(start_sec, end_sec, name)
        
        try:
            # 调用 VideoClipExtractor
            clip_result = await self._clip_extractor.extract_video_clip(
                timestamp_start=start_sec,
                timestamp_end=end_sec,
                output_dir=self.assets_dir,
                video_path=self.video_path,
                output_name=name  # 💥 传递规范名称
            )
            
            if clip_result and clip_result.clip_path:
                return clip_result.clip_path
            else:
                logger.warning(f"VideoClipExtractor returned no result for {name}")
                return await self._extract_action_clip_ffmpeg(start_sec, end_sec, name)
                
        except Exception as e:
            logger.error(f"Action clip extraction failed for {name}: {e}")
            return await self._extract_action_clip_ffmpeg(start_sec, end_sec, name)
    
    async def _extract_action_clip_ffmpeg(self, start_sec: float, end_sec: float, name: str) -> str:
        """FFmpeg 直接提取动作单元视频片段 (回退方案) - 异步版本"""
        import subprocess
        
        output_path = os.path.join(self.assets_dir, f"{name}_clip.mp4")
        duration = end_sec - start_sec
        
        # 安全边界
        safe_start = max(0, start_sec - 0.2)  # 稍微提前开始
        safe_duration = duration + 0.3  # 稍微延长结束
        
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
        
        # 使用 IO 线程池执行阻塞操作
        loop = asyncio.get_running_loop()
        executor = get_io_executor()
        return await loop.run_in_executor(executor, run_ffmpeg)
    
    async def _extract_video_clip(self, unit: SemanticUnit) -> str:
        """
        使用 VideoClipExtractor 提取视频片段
        
        传递动作单元的起止时间
        """
        if not self._clip_extractor:
            logger.warning("VideoClipExtractor not available, using fallback ffmpeg")
            return await self._extract_clip_ffmpeg_fallback(unit)
        
        try:
            # 获取动作区间
            if unit.action_segments:
                # 使用动作单元的起止时间
                action_start = min(seg["start"] for seg in unit.action_segments)
                action_end = max(seg["end"] for seg in unit.action_segments)
            else:
                # 使用语义单元时间
                action_start = unit.start_sec
                action_end = unit.end_sec
            
            # 调用 VideoClipExtractor (V3 Dual-Anchor recalibration)
            clip_result = await self._clip_extractor.extract_video_clip(
                timestamp_start=action_start,
                timestamp_end=action_end,
                output_dir=self.assets_dir,
                video_path=self.video_path,
                fault_text=unit.text,  # 传递语义文本以进行精修 (Standard 2)
                source_subtitle_ids=unit.source_subtitle_ids # 辅助对齐 (Standard 1)
            )
            
            if clip_result and clip_result.clip_path:
                return clip_result.clip_path
            else:
                logger.warning(f"VideoClipExtractor returned no result for {unit.unit_id}")
                return await self._extract_clip_ffmpeg_fallback(unit)
                
        except Exception as e:
            logger.error(f"Video clip extraction failed for {unit.unit_id}: {e}")
            return await self._extract_clip_ffmpeg_fallback(unit)
    
    async def _extract_clip_ffmpeg_fallback(self, unit: SemanticUnit) -> str:
        """FFmpeg 回退方案 (简化版) - 异步版本"""
        import subprocess
        
        output_path = os.path.join(self.assets_dir, f"{unit.unit_id}_clip.mp4")
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
        
        # 使用 IO 线程池执行阻塞操作
        loop = asyncio.get_running_loop()
        executor = get_io_executor()
        return await loop.run_in_executor(executor, run_ffmpeg)
    
    def _assemble_document(
        self, 
        units: List[SemanticUnit], 
        title: str
    ) -> RichTextDocument:
        """组装富文本文档"""
        doc = RichTextDocument(
            title=title or "视频知识文档",
            source_video=self.video_path,
            total_duration_sec=self.video_duration,
            generated_at=datetime.now().isoformat()
        )
        
        for unit in units:
            materials = getattr(unit, 'materials', MaterialSet())
            section = create_section_from_semantic_unit(unit, materials)
            doc.add_section(section)
        
        return doc


# =============================================================================
# CLI 入口
# =============================================================================
