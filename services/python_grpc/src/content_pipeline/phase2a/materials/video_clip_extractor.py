"""Video clip extraction for phase2a materials."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.infra.llm.prompt_loader import get_prompt
from services.python_grpc.src.content_pipeline.infra.llm.prompt_registry import PromptKeys
from services.python_grpc.src.content_pipeline.phase2a.materials.clip_anchor_detection import (
    detect_best_physical_anchors,
)
from services.python_grpc.src.content_pipeline.phase2a.materials.clip_boundary_utils import (
    add_speech_flow_padding,
    check_boundary_overlap,
    judge_sentence_completeness_no_punc,
)
from services.python_grpc.src.content_pipeline.phase2a.materials.clip_export import (
    export_clip_with_ffmpeg,
    export_poster_at_timestamp,
    get_video_duration_seconds,
)
from services.python_grpc.src.content_pipeline.phase2a.materials.clip_models import (
    RichMediaMetadata,
    VideoClip,
)
from services.python_grpc.src.content_pipeline.phase2a.materials.clip_subtitle_ops import (
    check_scene_switch,
    get_subtitles_near,
    has_transition_at_boundary,
)

logger = logging.getLogger(__name__)


class VideoClipExtractor:
    """Extract clips around fault candidates with semantic/visual refinement."""

    def __init__(self, visual_extractor, llm_client, config: Dict = None, semantic_extractor=None):
        self.visual_extractor = visual_extractor
        self.llm = llm_client
        self.config = config or {}

        clip_config = self.config.get("video_clip_config", {})
        action_config = clip_config.get("action_detection", {})

        self.ACTION_START_THRESHOLD = action_config.get("action_start_threshold", 100)
        self.ACTION_END_THRESHOLD = action_config.get("action_end_threshold", 80)
        self.MAX_CLIP_DURATION = clip_config.get("max_clip_duration", 60.0)
        self.MIN_CLIP_DURATION = clip_config.get("min_clip_duration", 5.0)

        import shutil

        self.ffmpeg_path = clip_config.get("ffmpeg_path", "ffmpeg")
        if not shutil.which(self.ffmpeg_path):
            alt_path = r"D:\New_ANACONDA\envs\whisper_env\Library\bin\ffmpeg.exe"
            if Path(alt_path).exists():
                self.ffmpeg_path = alt_path

        self._clip_cache: Dict[Tuple[float, float], str] = {}

        from services.python_grpc.src.content_pipeline.infra.runtime.config_loader import get_config_loader

        self.dicts = get_config_loader().load_dictionaries().get("video_clip", {})
        trans_dict = self.dicts.get("transitions", {})
        self.TRANSITION_KEYWORDS = (
            trans_dict.get("strong_contrast", [])
            + trans_dict.get("flow", [])
            + trans_dict.get("summary", [])
            + trans_dict.get("parallel", [])
            + trans_dict.get("guidance", [])
        )
        if not self.TRANSITION_KEYWORDS:
            self.TRANSITION_KEYWORDS = [
                "但是",
                "然后",
                "那么",
                "其实",
                "因此",
                "接下来",
                "下面我们看",
                "再看",
                "之后",
                "另外",
                "此外",
                "同时",
                "首先",
                "其次",
                "最后",
            ]

        self.semantic_extractor = semantic_extractor
        self.subtitles = []
        self._motion_value_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_VIDEO_CLIP_MOTION_VALUE_SYSTEM,
            fallback="Judge educational value of motion.",
        )
        self._transition_system_prompt = get_prompt(
            PromptKeys.DEEPSEEK_VIDEO_CLIP_TRANSITION_SYSTEM,
            fallback="请给出简短的引导语。",
        )
        self.confirmed_segments: List[Dict[str, float]] = []

        logger.info("VideoClipExtractor initialized")

    def set_subtitles(self, subtitles: List):
        self.subtitles = subtitles

    async def extract_video_clip(
        self,
        timestamp_start,
        timestamp_end,
        output_dir=None,
        video_path=None,
        fault_text="",
        source_subtitle_ids=None,
        output_name=None,
    ):
        from dataclasses import dataclass

        @dataclass
        class MockFault:
            fault_id: str
            timestamp_start: float
            timestamp_end: float
            fault_text: str = ""
            source_subtitle_ids: list = None
            output_name: Optional[str] = None
            original_start: Optional[float] = None

        fault = MockFault(
            fault_id=output_name or "fault",
            timestamp_start=float(timestamp_start),
            timestamp_end=float(timestamp_end),
            fault_text=fault_text,
            source_subtitle_ids=source_subtitle_ids or [],
            output_name=output_name,
            original_start=timestamp_start,
        )
        return await self.extract_clip(fault, video_path, output_dir)

    async def extract_clip(self, fault_candidate, video_path: str, output_dir: str = None) -> VideoClip:
        logger.info("Process starting: %s", fault_candidate.fault_id)

        video_duration = self._get_video_duration(video_path)
        asr_start = float(fault_candidate.timestamp_start)
        asr_end = float(fault_candidate.timestamp_end)
        if asr_end <= asr_start:
            asr_end = asr_start + self.MIN_CLIP_DURATION

        sem_start, sem_end = await self._get_complete_semantic_baseline(
            asr_start,
            asr_end,
            getattr(fault_candidate, "fault_text", ""),
        )
        vis_start, vis_end = await self._recalibrate_physical_anchor(
            video_path,
            sem_start,
            sem_end,
            getattr(fault_candidate, "fault_text", ""),
        )
        refined_start, refined_end = await self._refine_boundaries_semantically(
            vis_start,
            vis_end,
            getattr(fault_candidate, "fault_text", ""),
            getattr(fault_candidate, "source_subtitle_ids", []) or [],
        )

        final_start, final_end = self._add_speech_flow_padding(refined_start, refined_end)
        final_start = max(0.0, final_start)
        final_end = min(video_duration, max(final_start + self.MIN_CLIP_DURATION, final_end))
        if final_end - final_start > self.MAX_CLIP_DURATION:
            final_end = final_start + self.MAX_CLIP_DURATION

        clip_id = getattr(fault_candidate, "output_name", None) or getattr(fault_candidate, "fault_id", "clip")
        clip_path = self._export_clip_with_ffmpeg(video_path, final_start, final_end, clip_id, output_dir)
        transition_text = await self._generate_transition_text(fault_candidate, final_start, final_end)

        poster_time = min(final_end, max(final_start, final_end - 0.3))
        poster_path = self._export_poster_at_timestamp(video_path, poster_time, clip_id, output_dir or "video_clips")

        rich_media = RichMediaMetadata(
            layout_type="poster_with_clips",
            poster_path=poster_path or "",
            poster_timestamp=poster_time,
            clips=[
                {
                    "url": clip_path,
                    "start": final_start,
                    "end": final_end,
                    "type": "knowledge",
                    "description": "main clip",
                }
            ]
            if clip_path
            else [],
            transcript=getattr(fault_candidate, "fault_text", ""),
        )

        self.confirmed_segments.append({"start": final_start, "end": final_end})

        return VideoClip(
            clip_id=f"{clip_id}_CLIP",
            fault_id=getattr(fault_candidate, "fault_id", clip_id),
            original_start=asr_start,
            original_end=asr_end,
            extended_start=final_start,
            extended_end=final_end,
            clip_path=clip_path,
            action_start_detected=vis_start,
            action_end_detected=vis_end,
            transition_text=transition_text,
            rich_media=rich_media,
        )

    async def _detect_best_physical_anchors(self, video_path, s_scan, e_scan, asr_s, asr_e, fault_text=""):
        return await detect_best_physical_anchors(
            self,
            video_path,
            s_scan,
            e_scan,
            asr_s,
            asr_e,
            fault_text=fault_text,
        )

    async def _expand_logic_chain(self, anchor_time: float, is_start: bool) -> tuple[float, bool]:
        if not self.semantic_extractor:
            return anchor_time, False
        window = 5.0
        target_role = "G" if is_start else "R"
        subtitles = self._get_subtitles_near(anchor_time, window if is_start else 0, 0 if is_start else window)
        if not subtitles:
            return anchor_time, False
        search_list = reversed(subtitles) if is_start else subtitles
        for subtitle in search_list:
            text = subtitle.get("corrected_text", subtitle.get("text", ""))
            role = await self.semantic_extractor.classify_semantic_role(text)
            if role == target_role:
                new_time = subtitle["start_sec"] if is_start else subtitle["end_sec"]
                return new_time, True
        return anchor_time, False

    async def _get_dynamic_padding(self, has_trans: bool, semantic_time: float, is_start: bool) -> float:
        if not is_start:
            return 0.2
        if not has_trans:
            return 2.0
        try:
            has_visual_jump = await self._check_scene_switch(semantic_time)
            return 0.3 if has_visual_jump else 2.0
        except Exception:  # noqa: BLE001
            return 0.3 if has_trans else 2.0

    async def _check_scene_switch(self, timestamp: float) -> bool:
        return await check_scene_switch(self, timestamp)

    def _get_subtitles_near(self, t: float, before_s: float, after_s: float):
        return get_subtitles_near(self, t, before_s, after_s)

    async def _has_transition_at_boundary(self, timestamp: float, is_start: bool) -> bool:
        return has_transition_at_boundary(self, timestamp, is_start)

    async def _refine_boundaries_semantically(self, v_start, v_end, fault_text, source_sub_ids) -> Tuple[float, float]:
        _ = source_sub_ids
        extractor = self._get_semantic_extractor()
        if not extractor or not fault_text:
            return v_start, v_end

        from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_utils import (
            extract_subtitle_text_in_range,
        )

        subtitles = self.subtitles
        start_window = (v_start - 1.5, v_start + 1.5)
        end_window = (v_end - 1.5, v_end + 1.5)

        overlap, overlap_seg = self._check_boundary_overlap(*start_window)
        if overlap and overlap_seg:
            start_window = (max(start_window[0], overlap_seg["end"]), max(start_window[1], overlap_seg["end"]))

        overlap, overlap_seg = self._check_boundary_overlap(*end_window)
        if overlap and overlap_seg:
            end_window = (min(end_window[0], overlap_seg["start"]), min(end_window[1], overlap_seg["start"]))

        async def score_at(ts: float, low: float, high: float):
            text = extract_subtitle_text_in_range(subtitles, max(low, ts - 1.5), min(high, ts + 1.5))
            if not text:
                return 0.0, False
            sim = await extractor.calculate_context_similarity(fault_text, text)
            complete = self._judge_sentence_completeness_no_punc(text)
            return sim, complete

        start_sim, start_complete = await score_at(v_start, *start_window)
        refined_start = v_start
        if start_sim < 0.3 or not start_complete:
            for offset in [0.5, 1.0, 1.5]:
                candidate = v_start + offset
                if candidate > start_window[1]:
                    break
                sim, complete = await score_at(candidate, *start_window)
                if sim > start_sim and complete:
                    start_sim = sim
                    refined_start = candidate

        end_sim, end_complete = await score_at(v_end, *end_window)
        refined_end = v_end
        if end_sim < 0.2 and end_complete:
            next_text = extract_subtitle_text_in_range(subtitles, v_end, v_end + 1.0)
            if await self._is_next_topic(fault_text, next_text):
                refined_end = max(end_window[0], min(end_window[1], v_end - 0.5))

        return refined_start, max(refined_start + 0.2, refined_end)

    async def _get_complete_semantic_baseline(self, asr_s, asr_e, fault_text) -> Tuple[float, float]:
        _ = fault_text
        search_window = 3.0

        start_sentence = None
        end_sentence = None

        guide_words = {"下面", "接下来", "我们来看", "首先", "然后", "看", "讲", "看下"}
        confirm_words = {"好", "总结", "这就是", "讲完", "之后"}

        current_sentence = {"text": "", "start": None, "end": None}
        sentence_list: List[Dict] = []
        for subtitle in self.subtitles:
            text = subtitle.get("corrected_text", subtitle.get("text", "")) if isinstance(subtitle, dict) else getattr(subtitle, "text", "")
            st = float(subtitle.get("start_sec", 0)) if isinstance(subtitle, dict) else float(getattr(subtitle, "start_sec", 0))
            et = float(subtitle.get("end_sec", 0)) if isinstance(subtitle, dict) else float(getattr(subtitle, "end_sec", 0))
            if current_sentence["start"] is None:
                current_sentence["start"] = st
            current_sentence["text"] += text
            current_sentence["end"] = et
            if text.endswith(("。", "！", "？", ".", "!", "?")):
                sentence_list.append(current_sentence)
                current_sentence = {"text": "", "start": None, "end": None}

        if current_sentence["text"]:
            sentence_list.append(current_sentence)

        for sentence in sentence_list:
            if abs(sentence["start"] - asr_s) <= search_window and any(word in sentence["text"] for word in guide_words) and not start_sentence:
                start_sentence = {"start": max(0, sentence["start"] - 0.2), "end": sentence["end"]}
            if abs(sentence["end"] - asr_e) <= search_window and any(word in sentence["text"] for word in confirm_words) and not end_sentence:
                end_sentence = {"start": sentence["start"], "end": sentence["end"] + 0.2}

        final_start = start_sentence["start"] if start_sentence else asr_s
        final_end = end_sentence["end"] if end_sentence else asr_e

        if not start_sentence:
            candidates = await self._search_semantic_boundary(asr_s, is_start=True, window=search_window)
            if candidates:
                final_start = min(candidates)
        if not end_sentence:
            candidates = await self._search_semantic_boundary(asr_e, is_start=False, window=search_window)
            if candidates:
                final_end = max(candidates)

        return final_start, final_end

    async def _recalibrate_physical_anchor(self, video_path, sem_start, sem_end, fault_text="") -> Tuple[float, float]:
        video_duration = self._get_video_duration(video_path)
        scan_start = max(0, sem_start - 5.0)
        scan_end = min(video_duration, sem_end + 5.0)
        vis_start, vis_end = await self._detect_best_physical_anchors(
            video_path,
            scan_start,
            scan_end,
            sem_start,
            sem_end,
            fault_text=fault_text,
        )

        final_start = sem_start
        final_end = max(sem_end, vis_end)
        if final_end <= final_start:
            final_end = final_start + max(5.0, self.MIN_CLIP_DURATION)
        if final_end > sem_end + 3.0:
            final_end = sem_end + 0.5
        return final_start, final_end

    def _check_boundary_overlap(self, target_start, target_end) -> Tuple[bool, Optional[Dict]]:
        return check_boundary_overlap(self, target_start, target_end)

    def _judge_sentence_completeness_no_punc(self, text: str) -> bool:
        return judge_sentence_completeness_no_punc(text)

    def _add_speech_flow_padding(self, start_time: float, end_time: float) -> Tuple[float, float]:
        return add_speech_flow_padding(self, start_time, end_time)

    async def _search_semantic_boundary(self, anchor_time, is_start, window) -> List[float]:
        subtitles = self._get_subtitles_near(anchor_time, window if is_start else 0, 0 if is_start else window)
        if not subtitles:
            return []

        candidates: List[float] = []
        end_keywords = ["好", "总结", "这就是", "讲完", "之后"]
        for subtitle in subtitles:
            text = subtitle.get("corrected_text", subtitle.get("text", ""))
            if is_start:
                if any(keyword in text for keyword in self.TRANSITION_KEYWORDS):
                    candidates.append(subtitle["start_sec"])
            else:
                if any(keyword in text for keyword in end_keywords):
                    candidates.append(subtitle["end_sec"])
        return candidates

    async def _expand_logic_chain_v2(self, anchor_time: float, is_start: bool) -> tuple[float, bool]:
        if not self.semantic_extractor:
            return anchor_time, False

        window = 10.0
        target_role = "G" if is_start else "R"
        subtitles = self._get_subtitles_near(anchor_time, window if is_start else 0, 0 if is_start else window)
        if not subtitles:
            return anchor_time, False

        search_list = reversed(subtitles) if is_start else subtitles
        for subtitle in search_list:
            text = subtitle.get("corrected_text", subtitle.get("text", ""))
            role = await self.semantic_extractor.classify_semantic_role(text)
            if role == target_role:
                if is_start:
                    return max(0, subtitle["start_sec"] - 0.5), True
                return subtitle["end_sec"] + 0.5, True
        return anchor_time, False

    async def _get_dynamic_padding_v2(self, start_t, end_t, is_start) -> float:
        unit_type = self._classify_semantic_unit(start_t, end_t)
        base_buffer = 0.5
        if unit_type == "chapter":
            padding = 0.3
        elif unit_type == "process":
            padding = 2.0
        elif unit_type == "summary":
            padding = 1.5
        else:
            padding = 1.0
        return base_buffer + padding if is_start else 0.5

    def _classify_semantic_unit(self, start, end) -> str:
        duration = end - start
        from services.python_grpc.src.content_pipeline.shared.subtitle.subtitle_utils import (
            extract_subtitle_text_in_range,
        )

        text = extract_subtitle_text_in_range(self.subtitles, start, end)
        if any(keyword in text for keyword in ["首先", "其次", "第三", "总结"]):
            return "chapter"
        if duration > 10.0 or any(keyword in text for keyword in ["点击", "输入", "这里", "可以看到"]):
            return "process"
        if any(keyword in text for keyword in ["这就是", "算出来", "结果是"]):
            return "summary"
        return "general"

    async def _is_next_topic(self, current_fault, next_text) -> bool:
        if not next_text or not self.semantic_extractor:
            return False
        similarity = await self.semantic_extractor.calculate_context_similarity(current_fault, next_text)
        return similarity < 0.2

    async def validate_animation(self, video_path: str, start: float, end: float, fault_text: str) -> Tuple[bool, str, List[Tuple[float, float, str]]]:
        _ = fault_text
        try:
            duration = end - start
            if duration < 0.8:
                return False, "Too short", []

            features = await self.visual_extractor.extract_visual_features(start, end, sample_rate=3)
            if features.is_dynamic:
                windows = getattr(features, "action_windows", []) or []
                return True, "Valid Dynamic Animation", windows

            ssim = float(getattr(features, "ssim_score", 1.0))
            content_ratio = float(getattr(features, "content_ratio", 1.0))
            is_math = any(token in fault_text for token in ["公式", "推导", "计算", "方程", "=", "+", "-", "×", "÷"])
            threshold = 0.4 if is_math else 0.6

            if ssim < threshold and not (is_math and content_ratio > 0.8):
                return False, f"Semantic Break (SSIM {ssim:.2f})", []
            return True, "Valid Semantic Animation (Static)", []
        except Exception as error:  # noqa: BLE001
            logger.warning("Animation validation error: %s", error)
            return True, "Validation Error (Pass Safe)", []

    def _get_semantic_extractor(self):
        if not hasattr(self, "_semantic_extractor") or self._semantic_extractor is None:
            try:
                from services.python_grpc.src.content_pipeline.phase2a.vision.semantic_feature_extractor import (
                    SemanticFeatureExtractor,
                )

                self._semantic_extractor = SemanticFeatureExtractor(config=self.config)
            except Exception:  # noqa: BLE001
                self._semantic_extractor = None
        return self._semantic_extractor

    def _get_video_duration(self, video_path: str) -> float:
        return get_video_duration_seconds(self, video_path)

    def _export_clip_with_ffmpeg(self, video_path, start, end, fid, out_dir) -> str:
        return export_clip_with_ffmpeg(self, video_path, start, end, fid, out_dir)

    def _export_poster_at_timestamp(self, video_path: str, timestamp: float, fid: str, output_dir: str) -> str:
        return export_poster_at_timestamp(self, video_path, timestamp, fid, output_dir)

    async def _cognitive_value_check(self, text: str, clip_count: int) -> bool:
        if not self.llm:
            return True
        prompt = (
            f"Context: Educational video segment. ASR Text: \"{text}\". "
            f"Visual: Detected {clip_count} smooth motion segments. "
            "Question: Is dynamic motion essential or optional? "
            "Answer only ESSENTIAL or OPTIONAL."
        )
        try:
            response, _, _ = await llm_gateway.deepseek_complete_text(
                prompt=prompt,
                system_message=self._motion_value_system_prompt,
                client=self.llm,
            )
            return "ESSENTIAL" in str(response).upper()
        except Exception:  # noqa: BLE001
            return True

    async def _generate_transition_text(self, fault, s, e) -> str:
        duration = max(0.0, e - s)
        content = getattr(fault, "fault_text", "")
        if not self.llm:
            return f"下面通过视频演示相关操作（{duration:.1f}s）："
        prompt = (
            f"请根据以下知识点生成一句短过渡语。\n"
            f"知识点：{content}\n"
            f"视频时长：{duration:.1f}秒\n"
            "要求：一句话，不超过20个汉字。"
        )
        try:
            response, _, _ = await llm_gateway.deepseek_complete_text(
                prompt=prompt,
                system_message=self._transition_system_prompt,
                client=self.llm,
            )
            text = response.split(":")[-1].strip() if ":" in response else response.strip()
            if not text.endswith(("。", "：", ":")):
                text += "："
            return text
        except Exception:  # noqa: BLE001
            return f"下面通过视频演示相关操作（{duration:.1f}s）："

    async def extract_result_screenshot(
        self,
        video_path: str,
        start: float,
        end: float,
        output_dir: str = None,
    ) -> Tuple[Optional[str], float]:
        try:
            duration = end - start
            scan_duration = min(2.0, duration * 0.4)
            scan_start = max(start, end - scan_duration)

            from services.python_grpc.src.content_pipeline.phase2a.vision.screenshot_selector import (
                ScreenshotSelector,
            )

            selector = ScreenshotSelector(self.visual_extractor, self.config)
            final_out_dir = Path(output_dir or "screenshots") / "screenshots_from_clips"
            final_out_dir.mkdir(parents=True, exist_ok=True)

            selection = await selector.select_screenshot(video_path, scan_start, end, str(final_out_dir))
            if selection and selection.screenshot_path:
                return selection.screenshot_path, selection.final_score
            return None, 0.0
        except Exception as error:  # noqa: BLE001
            logger.error("Failed to extract result screenshot: %s", error)
            return None, 0.0
