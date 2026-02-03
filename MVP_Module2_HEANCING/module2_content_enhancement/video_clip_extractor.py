"""
Video Clip Extractor - Week 3 Day 19-21
Extracts video clips for process-based faults (Class 3) with Dual-Anchor recalibration.
"""

import logging
import subprocess
import math
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from pathlib import Path
import numpy as np
import cv2
import asyncio

logger = logging.getLogger(__name__)


@dataclass
class VideoClip:
    """视频片段结果"""
    clip_id: str
    fault_id: str
    
    # 时间信息
    original_start: float  # ASR/断层起始时间
    original_end: float    # ASR/断层结束时间
    extended_start: float  # 最终物理起始时间 (已标定)
    extended_end: float    # 最终物理结束时间 (已标定)
    
    # 输出文件
    clip_path: str
    
    # 物理锚点
    action_start_detected: float  # 视觉探测到的动作起点
    action_end_detected: float    # 视觉探测到的动作终点
    
    # 过渡语
    transition_text: str

@dataclass
class RichMediaMetadata:
    """
    🚀 V6.9.5: 富媒体元数据结构 (Rich Media Knowledge Component)
    契合公理：表现形式经济性 + 知识载体完整性
    """
    layout_type: str = "interactive_card"  # poster_with_clips, interactive_card
    poster_path: str = ""
    poster_timestamp: float = 0.0
    clips: List[Dict[str, Any]] = None # [{"url":..., "start":..., "end":..., "type":..., "description":...}]
    transcript: str = ""

@dataclass
class VideoClip:
    clip_id: str
    fault_id: str
    original_start: float
    original_end: float
    extended_start: float
    extended_end: float
    clip_path: str
    action_start_detected: float
    action_end_detected: float
    transition_text: str
    rich_media: Optional[RichMediaMetadata] = None # V6.9.5 新增


class VideoClipExtractor:
    """
    视频片段提取器 (第一性原理高精度版)
    
    核心策略:
    1. 双锚点重标定 (Dual-Anchor): 不盲信 ASR，通过搜索窗发现物理跳变
    2. 双向 MSE 极值扫描: 起始点向后找第一个峰值，结束点向前找最后一个定格点
    3. 物理硬约束: 视频 EOF 强制截断
    """
    
    def __init__(self, visual_extractor, llm_client, config: Dict = None, semantic_extractor = None):
        self.visual_extractor = visual_extractor
        self.llm = llm_client
        self.config = config or {}
        
        clip_config = self.config.get("video_clip_config", {})
        action_config = clip_config.get("action_detection", {})
        
        # 阈值配置
        self.ACTION_START_THRESHOLD = action_config.get("action_start_threshold", 100)
        self.ACTION_END_THRESHOLD = action_config.get("action_end_threshold", 80)
        
        # 扩展参数
        self.MAX_CLIP_DURATION = clip_config.get("max_clip_duration", 60.0)
        self.MIN_CLIP_DURATION = clip_config.get("min_clip_duration", 5.0)
        
        # FFmpeg 路径
        import shutil
        self.ffmpeg_path = clip_config.get("ffmpeg_path", "ffmpeg")
        if not shutil.which(self.ffmpeg_path):
            alt_path = r"D:\New_ANACONDA\envs\whisper_env\Library\bin\ffmpeg.exe"
            if Path(alt_path).exists():
                self.ffmpeg_path = alt_path
                
        # 💥 缓存机制: 避免重复截取相同区间的视频
        self._clip_cache: Dict[Tuple[float, float], str] = {}
        
        # 0. Load Dictionaries
        from .config_loader import get_config_loader
        self.dicts = get_config_loader().load_dictionaries().get("video_clip", {})
        
        # Load Transition Keywords from config or default
        trans_dict = self.dicts.get("transitions", {})
        self.TRANSITION_KEYWORDS = (
            trans_dict.get("strong_contrast", []) +
            trans_dict.get("flow", []) +
            trans_dict.get("summary", []) +
            trans_dict.get("parallel", []) +
            trans_dict.get("guidance", [])
        )
        # If config is empty, fallback to hardcoded (safety)
        if not self.TRANSITION_KEYWORDS:
             self.TRANSITION_KEYWORDS = [
                "但是", "然而", "那么", "但是呢", "其实", "事实上", "所以", "因此", "于是",
                "接下来", "下面我们看", "再看", "来看一下", "然后", "接着", "随后", "转到",
                "讲完", "之后", "综上所述", "总的来说", "刚才说到", "我们刚才讲了", "总结一下",
                "另外", "此外", "同时", "除了", "还有", "首先", "其次", "第三", "最后",
                "注意", "请看", "大家看", "我们可以看到", "看一下这个", "比如", "例如"
            ]
        
        self.semantic_extractor = semantic_extractor
        self.subtitles = []
        
        # 🚀 V3: Fragment boundary cache for mutual exclusion
        self.confirmed_segments: List[Dict[str, float]] = []
        
        logger.info(f"VideoClipExtractor initialized (Dual-Anchor Mode active)")

    def set_subtitles(self, subtitles: List):
        """Set subtitles for semantic boundary refinement"""
        self.subtitles = subtitles

    async def extract_video_clip(self, timestamp_start, timestamp_end, output_dir=None, video_path=None, 
                                fault_text="", source_subtitle_ids=None, output_name=None):
        """Shim for backward compatibility (V3 enhanced)
        
        Args:
            output_name: 规范化输出文件名 (如 SU001_action_1)
        """
        from dataclasses import dataclass
        @dataclass
        class MockFault:
            fault_id: str
            timestamp_start: float
            timestamp_end: float
            fault_text: str = ""
            source_subtitle_ids: list = None
            output_name: str = None  # 💥 新增
            original_start: float = 0.0  # 💥 新增: 原始起点约束

        fault = MockFault(
            fault_id=output_name or f"EXT_{timestamp_start}",  # 💥 优先使用规范名称
            timestamp_start=timestamp_start,
            timestamp_end=timestamp_end,
            fault_text=fault_text,
            source_subtitle_ids=source_subtitle_ids or [],
            output_name=output_name,
            original_start=timestamp_start  # 💥 保存原始起点用于约束
        )
        return await self.extract_clip(fault, video_path, output_dir)

    async def extract_clip(self, fault_candidate, video_path: str, output_dir: str = None) -> VideoClip:
        """提取视频片段 (包含物理-语义双向重标定)"""
        logger.info(f"Process starting: Re-anchoring clip for {fault_candidate.fault_id}")
        
        # 0. 获取物理时长事实 (优先使用 ffprobe)
        video_duration = self._get_video_duration(video_path)
        
        # ASR/断层起始时间的原始界限 (语义起止点)
        logger.info(f"🎤 [ASR Original Range]: {float(fault_candidate.timestamp_start):.2f}s - {float(fault_candidate.timestamp_end):.2f}s")
        
        # 🚀 V2 Core: 从 ASR 升级为“完整语义单元基线”
        sem_start, sem_end = await self._get_complete_semantic_baseline(
            fault_candidate.timestamp_start, fault_candidate.timestamp_end, fault_candidate.fault_text
        )
        logger.info(f"🎤 [Semantic V2 Baseline]: {sem_start:.2f}s - {sem_end:.2f}s")

        # 1. 执行视觉锚点重标定 (物理辅助)
        # 🚀 V3 Core: 采用 Termination Union 逻辑，起始点严守句头，终止点包含延迟翻页
        vis_start, vis_end = await self._recalibrate_physical_anchor(
            video_path, sem_start, sem_end, fault_text=fault_candidate.fault_text
        )
        logger.info(f"🌐 [Physical recalibrated (V3-Union)]: {vis_start:.2f}s - {vis_end:.2f}s")
        
        # 🚀 V3 Core: 语义精修 (BERT + 句式完整性 + 边界互斥)
        refined_start, refined_end = await self._refine_boundaries_semantically(
            vis_start, vis_end, fault_candidate.fault_text, fault_candidate.source_subtitle_ids
        )
        
        # 🚀 V3 Core: 添加口语语流缓冲 (Speech Flow Padding)
        final_s, final_e = self._add_speech_flow_padding(refined_start, refined_end)
        logger.info(f"🎯 [Final V3 Anchor]: {final_s:.2f}s - {final_e:.2f}s")
        
        # 🚀 Overlap Check: If padding/clamping resulted in invalid duration
        if final_s >= final_e - 0.1:
            logger.warning(f"🚫 [Overlap Filtered] {fault_candidate.fault_id}: Fully covered by previous clip. Range: {final_s:.2f}-{final_e:.2f}")
            return None
        
        # 记录边界以防止后续重叠
        self.confirmed_segments.append({"start": final_s, "end": final_e})
        
        # 🚀 Phase 4.4: Animation Validator (通用过滤)
        # 在确定物理/语义边界后，执行最终的第一性原理校验
        # 🚀 Phase 4.4: Animation Validator (通用过滤)
        # 在确定物理/语义边界后，执行最终的第一性原理校验 (V6.9.7: Returns action_windows)
        is_valid_anim, anim_reason, action_windows = await self.validate_animation(
            video_path, final_s, final_e, fault_candidate.fault_text
        )
        
        if not is_valid_anim:
            logger.warning(f"🚫 [Animation Filtered] {fault_candidate.fault_id}: {anim_reason}")
            return None
        
        # 🚀 V3 Logic: Respect final_s/final_e as definitive boundaries
        # These points already incorporate G/R expansion and Speech Flow Padding
        safe_start = final_s
        safe_end = final_e
        
        # 💥 用户约束: 视频起点不能晚于提供的动作单元起点
        original_start = getattr(fault_candidate, 'original_start', fault_candidate.timestamp_start)
        if safe_start > original_start:
            logger.warning(f"⚠️ [Start Constraint] Adjusting start from {safe_start:.2f}s to {original_start:.2f}s (cannot be later than action start)")
            safe_start = original_start
        
        # 4. 物理硬约束与时长保护
        safe_end = min(safe_end, video_duration)
        safe_start = max(0, min(safe_start, safe_end - 2.0)) # 保证至少 2s
        
        # 时长上限校验 (默认 60s)
        if (safe_end - safe_start) > self.MAX_CLIP_DURATION:
            # 如果太长，优先缩减结束点
            safe_end = safe_start + self.MAX_CLIP_DURATION
        
        # 物理事实保护：截取范围不能超越视频实际时长
        safe_end = min(safe_end, video_duration)
        
        logger.info(f"✂️ [Final V3 Export Range]: {safe_start:.2f}s - {safe_end:.2f}s")
        
        # 4. 检查缓存与 FFmpeg 导出
        # 规格化时间戳用于缓存 Key
        cache_key = (round(safe_start, 2), round(safe_end, 2))
        
        if cache_key in self._clip_cache:
            clip_path = self._clip_cache[cache_key]
            if Path(clip_path).exists():
                logger.info(f"♻️ [Cache Hit]: Reusing existing clip for {cache_key} -> {clip_path}")
            else:
                # 缓存失效，重新生成
                clip_path = self._export_clip_with_ffmpeg(video_path, safe_start, safe_end, fault_candidate.fault_id, output_dir)
                self._clip_cache[cache_key] = clip_path
        else:
            clip_path = self._export_clip_with_ffmpeg(video_path, safe_start, safe_end, fault_candidate.fault_id, output_dir)
            if clip_path:
                self._clip_cache[cache_key] = clip_path
                
        if not clip_path: return None
            
        # 5. Transition Generation
        transition_text = await self._generate_transition_text(fault_candidate, safe_start, safe_end)

        # 6. 🚀 V6.9.5: 生成结构化富媒体元数据
        # 💥 用户反馈: 不需要 poster，已禁用
        poster_t = min(safe_start + 1.0, safe_end - 0.5) # 保护时间戳
        poster_f = ""  # self._export_poster_at_timestamp(video_path, poster_t, fault_candidate.fault_id, output_dir)
        
        # 将 Engine 返回的 Action Windows 转换为前端可用的 Clips
        dynamic_clips = []
        if action_windows:
            for i, w in enumerate(action_windows):
                # 兼容 Dict 或 Object
                w_start = w.get("start") if isinstance(w, dict) else w.start
                w_end = w.get("end") if isinstance(w, dict) else w.end
                w_type = w.get("type", "knowledge") if isinstance(w, dict) else getattr(w, "type", "knowledge")
                
                # 相对时间戳
                dynamic_clips.append({
                    "id": f"clip_{i}",
                    "start": w_start, 
                    "end": w_end,
                    "duration": w_end - w_start,
                    "type": w_type,
                    "description": f"Dynamic segment {i+1}"
                })
        
        rich_media = RichMediaMetadata(
            layout_type="poster_with_clips",
            poster_path=poster_f if poster_f else "", 
            poster_timestamp=poster_t,
            clips=dynamic_clips,
            transcript=fault_candidate.fault_text 
        )
        
        # 7. 🚀 V6.9.7 (The Brain): Cognitive Value Check
        # 如果有动态 Clip，但不确定是否值得展示，由 LLM 最终裁决
        if dynamic_clips:
            # 只有当非“Knowledge”类型（如纯 Smooth Flow 或 Transition）时才敏感
            # Knowledge (书写) 默认都是 Essential 的
            has_knowledge = any(c["type"] == "knowledge" for c in dynamic_clips)
            if not has_knowledge:
                is_essential = await self._cognitive_value_check(fault_candidate.fault_text, len(dynamic_clips))
                if not is_essential:
                    logger.info("🧠 [Cognitive Filter] LLM decided motion is NOT essential. Downgrading to Poster Only.")
                    rich_media.layout_type = "poster_only"
                    rich_media.clips = [] # Clear clips
        
        return VideoClip(
            clip_id=f"{fault_candidate.fault_id}_CLIP",
            fault_id=fault_candidate.fault_id,
            original_start=float(fault_candidate.timestamp_start),
            original_end=float(fault_candidate.timestamp_end),
            extended_start=safe_start,
            extended_end=safe_end,
            clip_path=clip_path,
            action_start_detected=vis_start,
            action_end_detected=vis_end,
            transition_text=transition_text,
            rich_media=rich_media
        )

    async def _detect_best_physical_anchors(self, video_path, s_scan, e_scan, asr_s, asr_e, fault_text=""):
        """
        高阶对齐算法：多模态评分驱动的视觉锚点选择 (性能优化版)
        评分模型 = 0.5 * MSE强度 + 0.3 * 语义相似度(OCR) + 0.2 * 时序相似度
        """
        # 1. 自适应阈值设定
        base_start_threshold = self.ACTION_START_THRESHOLD
        base_end_threshold = self.ACTION_END_THRESHOLD
        
        # 🚀 性能优化: 使用极速低分辨率模式 (360P) 进行基准扫描
        # 采样率 = 2 (0.5s 一帧)
        frames, timestamps = self.visual_extractor.extract_frames_fast(
            s_scan, e_scan, sample_rate=2, target_height=360
        )
        if len(frames) < 2: return asr_s, asr_e
        
        # 执行视觉探测 (采样第一帧)
        try:
            # 💥 性能优化: 这里的分析已经是并行的
            from .visual_feature_extractor import get_visual_process_pool
            loop = asyncio.get_running_loop()
            executor = get_visual_process_pool()
            
            # 使用目前的低分辨率帧进行探测 (360P 已足够检测 PPT 元素)
            _, buf = cv2.imencode('.jpg', frames[0], [cv2.IMWRITE_JPEG_QUALITY, 85])
            sample_node = await loop.run_in_executor(executor, self.visual_extractor.visual_detector.analyze_frame, buf.tobytes())
            
            if sample_node.get("rect_count", 0) > 3 or sample_node.get("rectangle_count", 0) > 3:
                base_start_threshold = 80
                base_end_threshold = 64
                logger.info("📺 [Video Type]: PPT/Diagram detected, lowering MSE threshold")
            elif sample_node.get("total", 0) < 1:
                base_start_threshold = 120
                base_end_threshold = 96
                logger.info("🖌️ [Video Type]: Blackboard/Operation detected, raising MSE threshold")
        except Exception as e:
            logger.warning(f"Video type detection skipped: {e}")

        # 🚀 性能优化: MSE 现在在 360P 层面运行，速度提升 ~4 倍
        mse_list, _ = self.visual_extractor.calculate_all_diffs(frames)
        
        # 2. 动态采样逻辑 (核心区 vs 边缘区权重分配)
        def calculate_anchor_score(mse_val, anchor_time, target_time):
            # A. MSE 归一化
            intensity = min(1.0, mse_val / base_start_threshold)
            
            # B. 时序相似度与区域权重
            time_gap = abs(anchor_time - target_time)
            # 核心区 (±1.5s) 权重更高
            zone_weight = 1.0 if time_gap <= 1.5 else 0.7
            
            temporal_similarity = max(0.0, 1.0 - (time_gap / 5.0))
            
            # 由于 OCR 计算成本高，扫描阶段暂时仅使用 (0.7 * MSE + 0.3 * Temporal) * Zone
            return (0.7 * intensity + 0.3 * temporal_similarity) * zone_weight

        # 3. 寻找最佳起始锚点
        best_vis_start = asr_s
        max_start_score = -1.0
        
        for i, mse in enumerate(mse_list):
            if mse > base_start_threshold:
                cur_score = calculate_anchor_score(mse, timestamps[i], asr_s)
                if cur_score > max_start_score:
                    max_start_score = cur_score
                    best_vis_start = timestamps[i]

        # 4. 寻找最佳结束锚点
        best_vis_end = asr_e
        max_end_score = -1.0
        for i in range(len(mse_list)-1, -1, -1):
            if mse_list[i] > base_end_threshold:
                cur_score = calculate_anchor_score(mse_list[i], timestamps[i], asr_e)
                if cur_score > max_end_score:
                    max_end_score = cur_score
                    best_vis_end = timestamps[i]

        return best_vis_start, best_vis_end

    # 🚀 Phase 3.7: 语义边界修剪核心逻辑 (扩展转折/连接词库)
    # TRANSITION_KEYWORDS initialized in __init__

    async def _expand_logic_chain(self, anchor_time: float, is_start: bool) -> tuple[float, bool]:
        """
        基于模型驱动的逻辑链拓宽 (Guide/Confirm 识别)
        """
        if not self.semantic_extractor:
            return anchor_time, False
            
        # 1. 向前/向后 扫描 5s 内的字幕
        window = 5.0
        target_role = "G" if is_start else "R"
        
        # 获取相关字幕
        subs = self._get_subtitles_near(anchor_time, window if is_start else 0, 0 if is_start else window)
        if not subs: return anchor_time, False
        
        # 2. 依次分类判定
        # 如果是起点，从当前时间点向前找第一个 G
        # 如果是终点，从当前时间点向后找第一个 R
        search_list = reversed(subs) if is_start else subs
        
        for sub in search_list:
            # 🚀 Fix: Safe attribute/item access
            if isinstance(sub, dict):
                text = sub.get("corrected_text", sub.get("text", ""))
            else:
                text = getattr(sub, "corrected_text", getattr(sub, "text", ""))
            
            role = await self.semantic_extractor.classify_semantic_role(text)
            
            if role == target_role:
                # 命中地标
                new_time = sub["start_sec"] if is_start else sub["end_sec"]
                logger.info(f"✨ [Logic Chain Hook]: Found {target_role} at {new_time:.2f}s: '{text[:15]}...'")
                return new_time, True
                
        return anchor_time, False

    async def _get_dynamic_padding(self, has_trans: bool, semantic_time: float, is_start: bool) -> float:
        """
        动态计算冗余量 (A/B 分类)
        A类 (Chapter): 转折词 + 视觉跳变 -> 0.3s
        B类 (Process): 过程描述 -> 2.0s
        """
        if not is_start: return 0.2 # 结束点逻辑较简单
        if not has_trans: return 2.0 # 无转折词默认保守
        
        # 1. 视觉跳变校验 (Scene Switch)
        # 检查前后 1s 是否有力学/场景跳变
        # 利用 visual_extractor 的 MSE 判定
        # 注意: 这里的判定是定性的，MSE > Threshold 即可
        try:
            # 简单采样检测 (0.5s 窗口)
            has_visual_jump = await self._check_scene_switch(semantic_time)
            
            if has_visual_jump:
                logger.info(f"🎬 [Multimodal Logic]: Visual Jump detected at {semantic_time:.2f}s -> Using 0.3s padding (A-Class)")
                return 0.3
            else:
                logger.info(f"🎬 [Multimodal Logic]: No Visual Jump -> Using 2.0s padding (B-Class / Process)")
                return 2.0
        except:
            return 0.3 if has_trans else 2.0

    async def _check_scene_switch(self, timestamp: float) -> bool:
        """🚀 V6.9: 检测指定时间点附近是否有场景切换 (通过自适应引擎)"""
        # 采样前后 0.5s，使用高精度引擎判定
        feat = await self.visual_extractor.extract_visual_features(
            max(0, timestamp - 0.5), timestamp + 0.5, sample_rate=4
        )
        # 如果引擎判定为 dynamic 且置信度较高，或者 MSE 突变极大，则认为存在场景切换
        return feat.is_dynamic and feat.confidence > 0.6

    def _get_subtitles_near(self, t: float, before_s: float, after_s: float):
        """获取时间点附近的字幕"""
        if not self.subtitles:
            return []
            
        start_search = t - before_s
        end_search = t + after_s
        
        results = []
        for s in self.subtitles:
            # 🚀 Fix: Safe attribute/item access
            if isinstance(s, dict):
                s_start = float(s.get('start_sec', 0))
                s_end = float(s.get('end_sec', 0))
                text = s.get('text', '')
                corrected = s.get('corrected_text', '')
            else:
                s_start = float(getattr(s, 'start_sec', 0))
                s_end = float(getattr(s, 'end_sec', 0))
                text = getattr(s, 'text', '')
                corrected = getattr(s, 'corrected_text', '')
            
            # 判断重叠
            if s_start < end_search and s_end > start_search:
                results.append({
                    "text": text,
                    "corrected_text": corrected,
                    "start_sec": s_start,
                    "end_sec": s_end
                })
        return results

    async def _has_transition_at_boundary(self, timestamp: float, is_start: bool) -> bool:
        """检测指定时间点附近是否存在转折词"""
        # 搜索半径 1.0s
        from .subtitle_utils import extract_subtitle_text_in_range
        subs = self.subtitles
        if not subs: return False
        
        # 对于起始点，看 [t-1, t+0.5]；对于结束点，看 [t-0.5, t+1]
        s = timestamp - 1.0 if is_start else timestamp - 0.5
        e = timestamp + 0.5 if is_start else timestamp + 1.0
        
        text = extract_subtitle_text_in_range(subs, s, e)
        return any(kw in text for kw in self.TRANSITION_KEYWORDS)

    async def _refine_boundaries_semantically(self, v_start, v_end, fault_text, source_sub_ids) -> Tuple[float, float]:
        """
        🚀 V3 Core: 语义精修升级——扩大窗口 + 句式完整性校验 + 边界互斥
        """
        try:
            extractor = self._get_semantic_extractor()
            if not extractor or not fault_text: return v_start, v_end
            
            from .subtitle_utils import extract_subtitle_text_in_range
            subs = self.subtitles
            
            # 1. 3秒搜索窗基础配置 (V3 Optimization)
            s_search_start = v_start - 1.5
            s_search_end = v_start + 1.5
            e_search_start = v_end - 1.5
            e_search_end = v_end + 1.5
            
            # 2. 边界互斥校验 (Boundary Mutual Exclusion)
            s_overlap, s_overlap_seg = self._check_boundary_overlap(s_search_start, s_search_end)
            if s_overlap:
                s_search_start = max(s_search_start, s_overlap_seg["end"])
                s_search_end = max(s_search_end, s_overlap_seg["end"])
            
            e_overlap, e_overlap_seg = self._check_boundary_overlap(e_search_start, e_search_end)
            if e_overlap:
                e_search_end = min(e_search_end, e_overlap_seg["start"])
                e_search_start = min(e_search_start, e_overlap_seg["start"])

            async def get_sim(t):
                # 🚀 V3: 扩大文本提取范围 (1.5s) 且限制在搜索窗内
                text = extract_subtitle_text_in_range(subs, max(s_search_start, t - 1.5), min(s_search_end, t + 1.5))
                if not text: return 0.0, False
                sim_score = await extractor.calculate_context_similarity(fault_text, text)
                is_complete = self._judge_sentence_completeness_no_punc(text)
                return sim_score, is_complete

            # 3. 起始点修正 (相似度 + 完整性)
            s_sim, s_complete = await get_sim(v_start)
            refined_s = v_start
            if s_sim < 0.3 or not s_complete:
                max_offset = s_search_end - v_start
                for offset in [0.5, 1.0, 1.5]:
                    if offset > max_offset: break
                    side_sim, side_complete = await get_sim(v_start + offset)
                    if side_sim > s_sim and side_complete:
                        refined_s = v_start + offset
                        s_sim = side_sim
                    else:
                        break

            # 4. 验证结束点语义
            e_sim, e_complete = await get_sim(v_end)
            refined_e = v_end
            if e_sim < 0.2 and e_complete:
                next_text = extract_subtitle_text_in_range(subs, v_end, v_end + 1.0)
                if await self._is_next_topic(fault_text, next_text):
                    # 仅在搜索窗内修剪
                    refined_e = max(e_search_start, v_end - 0.5)

            # 5. 终极保护：确保时间区间有效且不缩减为 0
            if refined_e <= refined_s + 0.5:
                refined_s = v_start
                refined_e = v_end

            return refined_s, refined_e
        except Exception as e:
            logger.warning(f"V3 Semantic refinement failed: {e}")
            return v_start, v_end

    async def _get_complete_semantic_baseline(self, asr_s, asr_e, fault_text) -> Tuple[float, float]:
        """
        🚀 V3 Core: 适配无标点subs：基于Pause Detection模拟完整句，锚定包含关键词的完整口语句
        """
        subs = self.subtitles
        if not subs: return asr_s, asr_e

        # 1. 配置：口语句间有效停顿阈值 (s)
        PAUSE_THRESHOLD = 0.3
        
        # 2. 第一步：基于停顿分割完整口语句 (Pause Detection)
        complete_sentences = []
        current_sentence = {"text": "", "start": None, "end": None}
        
        for s in subs:
            if isinstance(s, dict):
                s_start = float(s.get('start_sec', 0))
                s_end = float(s.get('end_sec', 0))
                text = s.get('text', '')
            else:
                s_start = float(getattr(s, 'start_sec', 0))
                s_end = float(getattr(s, 'end_sec', 0))
                text = getattr(s, 'text', '')

            if current_sentence["start"] is None:
                current_sentence = {"text": text, "start": s_start, "end": s_end}
            else:
                pause_gap = s_start - current_sentence["end"]
                if pause_gap >= PAUSE_THRESHOLD:
                    complete_sentences.append(current_sentence)
                    current_sentence = {"text": text, "start": s_start, "end": s_end}
                else:
                    current_sentence["text"] += text
                    current_sentence["end"] = s_end
        
        if current_sentence["start"] is not None:
            complete_sentences.append(current_sentence)

        # 3. 第二步：在完整句中匹配引导词/确认语
        guide_words = {"下面", "接下来", "我们来看", "首先", "然后", "看", "讲", "看下"}
        confirm_words = {"好", "这就是", "总结一下", "也就是说", "这就是为什么", "结果是"}
        
        start_sentence = None
        end_sentence = None
        search_window = 8.0
        
        for sentence in complete_sentences:
            # 起始句判定
            if abs(sentence["start"] - asr_s) <= search_window:
                if any(word in sentence["text"] for word in guide_words) and not start_sentence:
                    start_sentence = {
                        "start": max(0, sentence["start"] - 0.2),
                        "end": sentence["end"]
                    }
            
            # 终止句判定
            if abs(sentence["end"] - asr_e) <= search_window:
                if any(word in sentence["text"] for word in confirm_words) and not end_sentence:
                    end_sentence = {
                        "start": sentence["start"],
                        "end": sentence["end"] + 0.2
                    }
        
        # 4. 融合与回退
        final_start = start_sentence["start"] if start_sentence else asr_s
        final_end = end_sentence["end"] if end_sentence else asr_e
        
        if not start_sentence:
            cands = await self._search_semantic_boundary(asr_s, is_start=True, window=search_window)
            if cands: final_start = min(cands)
        if not end_sentence:
            cands = await self._search_semantic_boundary(asr_e, is_start=False, window=search_window)
            if cands: final_end = max(cands)
            
        return final_start, final_end

    async def _recalibrate_physical_anchor(self, video_path, sem_start, sem_end, fault_text="") -> Tuple[float, float]:
        """
        🚀 V3: 优化终止点逻辑：起始点严守句头，终止点取句尾+物理跳变的并集
        基于第一性原理：语义优先，视觉辅助补充延迟。
        """
        video_duration = self._get_video_duration(video_path)
        
        # 扩展扫描窗
        scan_start = max(0, sem_start - 5.0)
        scan_end = min(video_duration, sem_end + 5.0)
        
        # 使用原有的视觉探测逻辑获取物理跳变
        vis_s, vis_e = await self._detect_best_physical_anchors(
            video_path, scan_start, scan_end, sem_start, sem_end, fault_text=fault_text
        )
        
        # 2. 起始点分析: 严守语义句头 (V3)
        # 不再向后追物理点，防止切断老师开头的引导语
        final_start = sem_start
        
        # 3. 结束点分析: 取语义结束与物理跳变的长者 (V3 Union)
        # 如果老师讲完了，但之后才翻页，我们要覆盖到翻页动作
        final_end = max(sem_end, vis_e)
        
        # 💥 Robustness Fix: Enforce Start < End
        # If visual detection found a "jump" before the start (false positive), ignore it.
        # If semantic data itself is inverted (Start > End), force a valid duration.
        if final_end <= final_start:
            logger.warning(f"⚠️ [Logic Fix] Inverted/Zero range detected: {final_start:.2f}-{final_end:.2f}. Forcing {self.MIN_CLIP_DURATION}s duration.")
            final_end = final_start + max(5.0, self.MIN_CLIP_DURATION)
        
        # 终止点边界保护: 避免跳变点拉得过晚导致无关内容进入
        if final_end > sem_end + 3.0:
            final_end = sem_end + 0.5 # 适度缓冲
            
        return final_start, final_end

    def _check_boundary_overlap(self, target_start, target_end) -> Tuple[bool, Optional[Dict]]:
        """🚀 V3: 校验目标窗口是否与已确定片段重叠"""
        for seg in self.confirmed_segments:
            if not (target_end <= seg["start"] or target_start >= seg["end"]):
                return True, seg
        return False, None

    def _judge_sentence_completeness_no_punc(self, text: str) -> bool:
        """🚀 V3: 无标点场景下的句式完整性判断"""
        if not text: return False
        subject_words = {"我们", "我", "这个", "该", "算法", "公式", "它", "大家"}
        predicate_words = {"看", "讲", "分析", "总结", "推导", "理解", "做", "写", "求"}
        has_sub_pred = any(s in text for s in subject_words) and any(p in text for p in predicate_words)
        return has_sub_pred and len(text) >= 5

    def _add_speech_flow_padding(self, start_time: float, end_time: float) -> Tuple[float, float]:
        """🚀 V3: 为起止点添加口语语流缓冲 (0.2-0.3s)"""
        # 起始点预留缓冲，向前 0.2s
        final_s = max(0, start_time - 0.2)
        # 结束点预留缓冲，向后 0.3s
        final_e = end_time + 0.3
        
        # 校验缓冲是否与下一句/上一句重叠 (V3 公理)
        if self.confirmed_segments:
            prev_seg = self.confirmed_segments[-1]
            if final_s < prev_seg["end"]:
                # Check for FULL coverage
                if prev_seg["end"] >= final_e - 0.1:
                    logger.warning(f"⚠️ [Overlap Detected] Current segment {final_s:.2f}-{final_e:.2f} is fully covered by previous segment (End: {prev_seg['end']:.2f}).")
                    # Force a minimal valid duration? Or allow negative to let caller filter?
                    # Letting caller filter is safer if we just clamp.
                    final_s = prev_seg["end"] 
                else:
                    # Partial overlap, clamp start
                    logger.info(f"✂️ [Overlap Adjusted] Start clamped {final_s:.2f} -> {prev_seg['end']:.2f}")
                    final_s = max(final_s, prev_seg["end"])
                
        return final_s, final_e

    async def _search_semantic_boundary(self, anchor_time, is_start, window) -> List[float]:
        """在窗口内搜索所有潜在的语义边界点"""
        subs = self._get_subtitles_near(anchor_time, window if is_start else 0, 0 if is_start else window)
        candidates = []
        if not subs: return []
        
        for sub in subs:
            text = sub.get("corrected_text", sub.get("text", ""))
            # 如果包含强转折词或话题起始词，计为候选点
            if is_start:
                if any(kw in text for kw in self.TRANSITION_KEYWORDS):
                    candidates.append(sub["start_sec"])
            else:
                # 寻找收尾特征 (好、总结、这就是)
                if any(kw in text for kw in ["好", "总结", "这就是", "讲完", "之后"]):
                    candidates.append(sub["end_sec"])
        return candidates

    async def _expand_logic_chain_v2(self, anchor_time: float, is_start: bool) -> tuple[float, bool]:
        """🚀 V2 Core: 逻辑链拓宽 (10s Window + 0.5s Buffer)"""
        if not self.semantic_extractor:
            return anchor_time, False
            
        window = 10.0
        target_role = "G" if is_start else "R"
        
        subs = self._get_subtitles_near(anchor_time, window if is_start else 0, 0 if is_start else window)
        if not subs: return anchor_time, False
        
        search_list = reversed(subs) if is_start else subs
        
        for sub in search_list:
            if isinstance(sub, dict):
                text = sub.get("corrected_text", sub.get("text", ""))
            else:
                text = getattr(sub, "corrected_text", getattr(sub, "text", ""))
            
            role = await self.semantic_extractor.classify_semantic_role(text)
            
            if role == target_role:
                # 命中地标，额外增加 0.5s 语气缓冲
                if is_start:
                    new_time = max(0, sub["start_sec"] - 0.5)
                else:
                    new_time = sub["end_sec"] + 0.5
                return new_time, True
        return anchor_time, False

    async def _get_dynamic_padding_v2(self, start_t, end_t, is_start) -> float:
        """🚀 V2 Core: 基于语义单元分类的动态冗余"""
        unit_type = self._classify_semantic_unit(start_t, end_t)
        
        # 基础缓冲 (0.5s)
        base_buffer = 0.5
        
        if unit_type == "chapter":
            padding = 0.3
        elif unit_type == "process":
            padding = 2.0
        elif unit_type == "summary":
            padding = 1.5
        else:
            padding = 1.0
            
        return base_buffer + padding if is_start else 0.5 # 结束点稍短

    def _classify_semantic_unit(self, start, end) -> str:
        """判定语义单元类型"""
        duration = end - start
        # 简单 heuristic: 结合时长和关键词
        from .subtitle_utils import extract_subtitle_text_in_range
        text = extract_subtitle_text_in_range(self.subtitles, start, end)
        
        if any(kw in text for kw in ["首先", "其次", "第三", "总结"]):
            return "chapter"
        if duration > 10.0 or any(kw in text for kw in ["点击", "输入", "这里", "可以看到"]):
            return "process"
        if any(kw in text for kw in ["这就是", "算出来", "结果是"]):
            return "summary"
        return "general"

    async def _is_next_topic(self, current_fault, next_text) -> bool:
        """判定一段文本是否已经是“下一话题”"""
        if not next_text or not self.semantic_extractor: return False
        # 如果下一段文本与当前断层文本的相似度极低 (< 0.2)，则认为是下一话题
        sim = await self.semantic_extractor.calculate_context_similarity(current_fault, next_text)
        return sim < 0.2

    async def validate_animation(self, video_path: str, start: float, end: float, fault_text: str) -> Tuple[bool, str]:
        """
        Phase 4.4: 动画演示验证器 (通用过滤)
        区分 'Semantic Demonstration' (保留) 和 'Transition/Decorative Animation' (丢弃)
        """
        duration = end - start
        
        # Layer 1: 时序目的性筛查
        # 瞬时转场通常非常快
        if duration < 0.8:
            return False, f"Duration too short ({duration:.2f}s < 0.8s)", []
            
        try:
            # 提取关键帧用于校验 (Start, End)
            # 使用高分辨率以保证 SSIM 准确
            # 🚀 V6.9.7: 调用完整视觉特征提取以获取 Action Windows
            # 这也是对 Clip 有效性的最强校验 (Engine Check)
            features = await self.visual_extractor.extract_visual_features(video_path, start, end)
            
            # 1. 引擎判定
            # 如果 Engine 认为是动态，那就是最大置信度
            if features.is_dynamic:
                 logger.info(f"✨ [Engine Verified] Clip verified as DYNAMIC ({len(features.action_windows)} windows)")
                 # 返回: Valid, Reason, Windows
                 return True, "Engine Verified Dynamic", features.action_windows
            
            # 2. 如果 Engine 认为是 Static，再看 SSIM 兜底 (原有逻辑)
            # 有时候平滑移动 Engine 没捕获到 (V6.9.4 已修复但防万一)
            frames, _ = self.visual_extractor.extract_frames(start, end, sample_rate=5)
            # ... (Existing SSIM check logic)
            
            start_frame = frames[0]
            end_frame = frames[-1]
            ssim = self.visual_extractor.calculate_ssim(start_frame, end_frame)
            content_ratio = self.visual_extractor.calculate_content_increment(start_frame, end_frame)
            
            try:
                ssim = float(ssim)
            except:
                ssim = 0.0
            
            try:
                content_ratio = float(content_ratio)
            except:
                content_ratio = 0.0
            
            logger.info(f"Animation Check [{start:.2f}-{end:.2f}]: SSIM={ssim:.2f}, ContentRatio={content_ratio:.2f}")

            is_math = any(kw in fault_text for kw in ["公式", "推导", "计算", "方程", "=", "+", "−", "×", "÷"])
            ssim_threshold = 0.4 if is_math else 0.6
            
            if ssim < ssim_threshold:
                 if is_math and content_ratio > 0.8:
                     logger.info(f"✨ [Math/Logic Exception] SSIM {ssim:.2f} Low/Ratio High. Bypassing.")
                 else:
                     return False, f"Semantic Break (SSIM {ssim:.2f})", []
            
            # 如果是 Static 但 SSIM 通过，可能是微小的展示。
            # 这时候没有 action_windows。
            return True, "Valid Semantic Animation (Static)", []
            
        except Exception as e:
            logger.warning(f"Animation validation error: {e}")
            return True, "Validation Error (Pass Safe)", []
            
    def _get_semantic_extractor(self):
        """延迟获取语义提取器实例"""
        if not hasattr(self, '_semantic_extractor') or self._semantic_extractor is None:
            try:
                from .semantic_feature_extractor import SemanticFeatureExtractor
                self._semantic_extractor = SemanticFeatureExtractor(config=self.config)
            except:
                self._semantic_extractor = None
        return self._semantic_extractor

    def _get_video_duration(self, video_path: str) -> float:
        """多方案获取时长，支持 ffprobe (主) 和 cv2 (备)"""
        # 1. 尝试 ffprobe
        try:
            cmd = [
                'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1', video_path
            ]
            # 解决 ffmpeg 路径问题
            if "ffmpeg.exe" in self.ffmpeg_path:
                ffprobe_path = self.ffmpeg_path.replace("ffmpeg.exe", "ffprobe.exe")
                cmd[0] = ffprobe_path
                
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode().strip()
            return float(out)
        except:
            # 2. 备选 cv2
            cap = cv2.VideoCapture(video_path)
            fps = cap.get(cv2.CAP_PROP_FPS)
            cnt = cap.get(cv2.CAP_PROP_FRAME_COUNT)
            dur = cnt / fps if fps > 0 else 3600.0
            cap.release()
            return dur

    def _export_clip_with_ffmpeg(self, video_path, start, end, fid, out_dir) -> str:
        out_dir = Path(out_dir or "video_clips")
        out_dir.mkdir(parents=True, exist_ok=True)
        dur = end - start
        
        # 💥 规范化命名: 如果 fid 是语义单元 ID (如 SU001_action_1)，使用简洁命名
        if fid and fid.startswith("SU"):
            fname = f"{fid}_clip.mp4"
        else:
            fname = f"clip_{fid}_{start:.2f}s-{end:.2f}s.mp4"
        opath = out_dir / fname
        
        # 💥 如果规范化文件已存在，直接返回
        if opath.exists():
            logger.info(f"💾 [Disk reuse]: Found existing file -> {fname}")
            return str(opath)
        
        # 💥 磁盘预检: 如果同一个时间区间的视频已存在（不论 fid），则直接复用
        # 搜索规律: clip_*_{start:.2f}s-{end:.2f}s.mp4
        pattern = f"clip_*_{start:.2f}s-{end:.2f}s.mp4"
        existing_files = list(out_dir.glob(pattern))
        if existing_files:
            logger.info(f"💾 [Disk reuse]: Found existing file for time range -> {existing_files[0].name}")
            return str(existing_files[0])
        
        # 使用 Slow Seek 模型 (优化兼容性)
        cmd = [self.ffmpeg_path, '-i', video_path, '-ss', str(start), '-t', str(dur), 
               '-c:v', 'libx264', '-preset', 'superfast', '-crf', '23', '-c:a', 'aac', '-y', str(opath)]
        
        try:
            logger.info(f"Exporting Physical Anchor: {fname}")
            subprocess.run(cmd, capture_output=True, check=True)
            return str(opath)
        except Exception as e:
            logger.error(f"FFmpeg Error for {fid}: {e}")
            return ""

    def _export_poster_at_timestamp(self, video_path: str, timestamp: float, fid: str, output_dir: str) -> str:
        """
        🚀 V6.9.5: 导出指定时间戳的海报帧
        """
        out_dir = Path(output_dir or "video_clips")
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # 💥 规范化命名
        if fid and fid.startswith("SU"):
            fname = f"{fid}_poster.png"
        else:
            fname = f"poster_{fid}_{timestamp:.2f}s.png"
        opath = out_dir / fname
        
        if opath.exists(): return str(opath)
        
        # 使用 ffmpeg 截图
        cmd = [self.ffmpeg_path, '-ss', str(timestamp), '-i', video_path, '-vframes', '1', '-q:v', '2', '-y', str(opath)]
        try:
             subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
             logger.info(f"🖼️ [Poster Export]: {fname}")
             return str(opath)
        except Exception as e:
             logger.error(f"Poster Export Error: {e}")
             return ""

    async def _cognitive_value_check(self, text: str, clip_count: int) -> bool:
        """
        🚀 V6.9.7 (The Brain): 调用 LLM 判断动效的认知价值
        """
        if not self.llm: return True
        
        prompt = (
            f"Context: Educational video segment. ASR Text: \"{text}\". "
            f"Visual: Detected {clip_count} smooth motion segments (e.g. cursor movement, sliding chart). "
            f"Question: Is seeing the DYNAMIC MOTION essential to understand this knowledge, or is a STATIC POSTER sufficient? "
            f"If the text implies motion (e.g. 'watch how it moves', 'observe the trend'), return ESSENTIAL. "
            f"If the text is static explanation, return OPTIONAL. "
            f"Answer (ESSENTIAL/OPTIONAL):"
        )
        
        try:
            res, _, _ = await self.llm.complete_text(prompt, system_message="Judge educational value of motion.")
            is_essential = "ESSENTIAL" in res.upper()
            return is_essential
        except Exception as e:
            logger.warning(f"Cognitive Check Failed: {e}. Defaulting to ESSENTIAL.")
            return True

    async def _generate_transition_text(self, fault, s, e) -> str:
        dur = e - s
        prompt = f"你是教育编辑。请为 {s:.1f}s-{e:.1f}s (时长{dur:.1f}s) 的教学视频生成一句 20 字以内的引导语。内容: {fault.fault_text}。输出格式: 过渡语: [内容]"
        try:
            res, _, _ = await self.llm.complete_text(prompt, system_message="简洁引导观看。")
            txt = res.split(":")[-1].strip() if ":" in res else res.strip()
            if not txt.endswith(('.', ':', '。', '！')): txt += ":"
            return txt
        except:
            return f"下面通过视频演示相关操作 ({dur:.1f}秒):"

    async def extract_result_screenshot(self, video_path: str, start: float, end: float, output_dir: str = None) -> Tuple[Optional[str], float]:
        """
        V6 Optimization: Re-use ScreenshotSelector for high-fidelity result extraction.
        Instead of a simplified internal check, we treat the 'result window' as a candidate
        for the full ScreenshotSelector logic (Stability Islands + Peak Info).
        """
        try:
            # 1. Define Result Window (Last 2.0s or 30% of clip)
            # Ensure window is valid
            duration = end - start
            scan_duration = min(2.0, duration * 0.4) 
            scan_start = max(start, end - scan_duration)
            
            logger.info(f"📸 [Result Extraction] Delegating to ScreenshotSelector for range [{scan_start:.2f}-{end:.2f}]")
            
            # 2. Initialize ScreenshotSelector (if not already cached/passed)
            # Ideally this should be injected, but for now we instantiate lightly
            # Note: ScreenshotSelector needs visual_extractor and config
            # We assume importing here to avoid circular dep at top level if any, 
            # though semantic_feature_extractor is already imported.
            from .screenshot_selector import ScreenshotSelector
            selector = ScreenshotSelector(self.visual_extractor, self.config)
            
            # 3. Use select_screenshot (which runs the full Island/Game logic)
            # 💥 Fix: Ensure subfolder is created for better organization
            final_out_dir = Path(output_dir or "screenshots") / "screenshots_from_clips"
            final_out_dir.mkdir(parents=True, exist_ok=True)
            
            selection = await selector.select_screenshot(
                video_path, scan_start, end, str(final_out_dir)
            )
            
            if selection and selection.screenshot_path:
                logger.info(f"✅ [Result Extraction] Success: {selection.screenshot_path} (Score: {selection.final_score:.2f})")
                return selection.screenshot_path, selection.final_score
            else:
                logger.warning("❌ [Result Extraction] ScreenshotSelector returned no result.")
                return None, 0.0
            
        except Exception as e:
            logger.error(f"Failed to extract result screenshot: {e}")
            return None, 0.0
