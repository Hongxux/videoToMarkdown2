"""
VL-Based Video Analyzer - 基于视觉语言模型的视频分析器

功能：
1. 使用 Qwen3-VL-Plus 分析语义单元视频片段
2. 解析 AI 返回的知识类型、视频截取区间、截图时间戳
3. 将相对时间戳（片段内）转换为绝对时间戳（原视频）

使用方法：
    from vl_video_analyzer import VLVideoAnalyzer
    
    analyzer = VLVideoAnalyzer(config)
    result = await analyzer.analyze_clip(
        clip_path="path/to/clip.mp4",
        semantic_unit_start_sec=100.0,
        semantic_unit_id="SU001"
    )
"""

import os
import re
import json
import base64
import asyncio
import logging
import io
import httpx
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


_DASHSCOPE_DATA_URI_ITEM_MAX_BYTES = 10 * 1024 * 1024  # 来自 DashScope 400 错误信息
_DATA_URI_SAFETY_RATIO = 0.90  # 留出协议/编码冗余，避免卡边界导致 400
_MAX_RAW_BYTES_FOR_BASE64_DATA_URI = int(_DASHSCOPE_DATA_URI_ITEM_MAX_BYTES * 3 / 4 * _DATA_URI_SAFETY_RATIO)


@dataclass
class VLAnalysisResult:
    """单个视频片段的分析结果"""
    id: int = 0
    knowledge_type: str = ""
    confidence: float = 0.0
    reasoning: str = ""
    key_evidence: str = ""
    clip_start_sec: float = 0.0  # 相对时间（片段内）
    clip_end_sec: float = 0.0    # 相对时间（片段内）
    suggested_screenshoot_timestamps: List[float] = field(default_factory=list)  # 相对时间

    # 教程模式新增字段
    step_id: int = 0
    step_description: str = ""
    analysis_mode: str = "default"

    # 绝对时间（由 VLVideoAnalyzer.convert_timestamps 计算）
    absolute_clip_start_sec: float = 0.0
    absolute_clip_end_sec: float = 0.0
    absolute_screenshot_timestamps: List[float] = field(default_factory=list)


@dataclass
class VLClipAnalysisResponse:
    """视频片段分析的完整响应"""
    success: bool = False
    error_msg: str = ""
    analysis_results: List[VLAnalysisResult] = field(default_factory=list)
    clip_requests: List[Dict[str, Any]] = field(default_factory=list)
    screenshot_requests: List[Dict[str, Any]] = field(default_factory=list)
    token_usage: Dict[str, int] = field(default_factory=dict)
    analysis_mode: str = "default"
    raw_response_json: List[Dict[str, Any]] = field(default_factory=list)


class VLVideoAnalyzer:
    """基于 Qwen3-VL-Plus 的视频分析器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化分析器
        
        Args:
            config: VL 配置，包含 api 配置、截图优化配置等
        """
        api_config = config.get("api", {})
        
        # 优先使用 api_key，其次从环境变量读取
        api_key = api_config.get("api_key", "")
        if not api_key:
            api_key_env = api_config.get("api_key_env", "DASHSCOPE_API_KEY")
            api_key = os.environ.get(api_key_env, "")
        self._api_key = api_key
        
        self.base_url = api_config.get("base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        self.model = api_config.get("model", "qwen3-vl-plus")
        self.max_retries = api_config.get("max_retries", 2)
        
        # 视频压缩配置 (API 限制 10MB)
        self.max_video_size_mb = api_config.get("max_video_size_mb", 8)  # 留 2MB buffer
        self.compression_crf = api_config.get("compression_crf", 28)  # 0-51, 越大压缩越多
        self.max_tokens = api_config.get("max_tokens", 4096)
        self.temperature = api_config.get("temperature", 0.2)

        # 兼容 DashScope data-uri 限制的输入策略
        # auto: data-uri(小文件) -> DashScope File.upload(若可用) -> 关键帧降级
        self.video_input_mode = api_config.get("video_input_mode", "auto")
        self.max_input_frames = int(api_config.get("max_input_frames", 6))
        self.max_image_dim = int(api_config.get("max_image_dim", 1024))
        
        # 初始化 HTTP 客户端 (带连接池和压缩)
        self.http_client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            headers={"Accept-Encoding": "gzip, deflate"},
            timeout=httpx.Timeout(120.0, connect=10.0)
        )
        
        # 初始化 OpenAI 兼容客户端
        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url=self.base_url,
            http_client=self.http_client
        )
        
        # 截图优化配置
        self.screenshot_optimization = config.get("screenshot_optimization", {})
        self.optimize_screenshots = self.screenshot_optimization.get("enabled", True)
        self.search_window_sec = self.screenshot_optimization.get("search_window_sec", 1.0)
        
        # 回退配置
        self.fallback_config = config.get("fallback", {})
        
        # 加载提示词模板
        self.prompt_template = self._load_prompt_template()

        # Tutorial mode settings for long multi-step process units
        tutorial_cfg = config.get("tutorial_mode", {}) if isinstance(config.get("tutorial_mode", {}), dict) else {}
        self.tutorial_min_step_duration_sec = float(tutorial_cfg.get("min_step_duration_sec", 5.0))
        
        logger.info(f"VLVideoAnalyzer 初始化完成: model={self.model}")

    def __del__(self):
        """析构时确保资源释放 (注意: 在异步环境中，建议显式调用 close)"""
        if hasattr(self, 'http_client') and not self.http_client.is_closed:
            # 由于 __del__ 不支持 await，这里只能记录日志，
            # 完整资源释放应调用 await self.close()
            pass

    async def close(self):
        """显式关闭资源池"""
        if hasattr(self, 'http_client'):
            await self.http_client.aclose()
            logger.info("VLVideoAnalyzer HTTP 客户端已关闭")

    def _normalize_analysis_mode(self, analysis_mode: Optional[str]) -> str:
        mode = str(analysis_mode or "default").strip().lower()
        if mode in {"tutorial", "tutorial_stepwise", "teaching"}:
            return "tutorial_stepwise"
        return "default"

    def _get_output_constraints(self, analysis_mode: str = "default") -> str:
        """
        追加到提示词尾部的输出约束。

        目标：降低模型输出 JSON 被 Markdown/自然语言污染、以及字段格式漂移导致的解析失败。
        """
        mode = self._normalize_analysis_mode(analysis_mode)
        if mode == "tutorial_stepwise":
            return (
                "\n\n"
                "[Hard Constraints - Tutorial Stepwise Mode]\n"
                "1) Output exactly one valid JSON array. No markdown, no prefix/suffix text, no explanations.\n"
                "2) Each array item must be one complete step.\n"
                "3) Required fields per item: step_id (Integer), step_description (String), "
                "clip_start_sec (Float), clip_end_sec (Float), instructional_keyframe_timestamp (List[Float]).\n"
                "4) Do not output reasoning, key_evidence, or knowledge_type fields.\n"
                "5) Segmentation rules:\n"
                "   - Keep explanation + execution + result of the same step together.\n"
                "   - Remove thinking/hesitation time (mouse wandering, idle pause, no new information).\n"
                "   - No step shorter than 5 seconds. Merge short steps with adjacent steps.\n"
                "6) instructional_keyframe_timestamp must be true instructional keyframes, "
                "prefer final state or just-before-submit moments.\n"
                "7) Avoid -1 for timestamps; if action spans whole clip use [0.0, clip_duration].\n"
            )

        return (
            "\n\n"
            "【输出硬性约束】\n"
            "1) 只输出一个标准的 JSON，不要任何 Markdown 代码块标签、不要解释、不要前后缀文字。\n"
            "2) 顶层必须是一个平铺的 JSON 数组：[{...}, {...}]。\n"
            "3) 每个对象必须包含字段：id, knowledge_type, confidence, "
            "clip_start_sec, clip_end_sec, suggested_screenshoot_timestamps。\n"
            "4) 严禁输出 reasoning / key_evidence 字段，避免无关文本增加 token。\n"
            "5) 时间边界判断规则：\n"
            "   - 对于非【讲解型】内容，**禁止**随意输出 -1。请根据视觉变化（如：菜单出现/消失、鼠标点击、窗口切换、公式书写开始/结束）尽力估算起止时间。\n"
            "   - 如果该知识类型贯穿整个视频片段，起始可设为 0.0，结束可设为片段总时长（或最后一个显著变化的时间戳）。\n"
            "   - 只有在视觉信息完全无法支撑任何时间判断时，才允许对该项输出 -1。\n"
        )

    def _load_prompt_template(self) -> str:
        """加载视频分析提示词模板"""
        # 尝试从文件加载
        prompt_paths = [
            Path(__file__).parent.parent.parent / "prompt_video_analysis.md",
            Path(__file__).parent / "prompts" / "video_analysis.md",
        ]
        
        for path in prompt_paths:
            if path.exists():
                try:
                    return path.read_text(encoding="utf-8")
                except Exception as e:
                    logger.warning(f"读取提示词文件失败 {path}: {e}")
        
        # 兜底：使用内置提示词
        return self._get_default_prompt()
    
    def _get_default_prompt(self) -> str:
        """获取默认提示词"""
        return '''请分析这段视频，识别其中的知识片段。

对于每个识别出的知识片段，请输出以下信息：
1. id: 片段序号（从0开始）
2. knowledge_type: 知识类型，必须是以下之一：
   - "实操" - 实际操作演示
   - "推演" - 推理演示过程
   - "环境配置" - 环境或配置设置
   - "过程性知识" - 过程性知识展示
   - "讲解型" - 纯讲解无视觉操作
3. confidence: 置信度（0-1）
4. clip_start_sec: 片段起始时间（秒，相对于视频开头）
5. clip_end_sec: 片段结束时间（秒）
6. suggested_screenshoot_timestamps: 建议的截图时间点数组（秒）

请以 JSON 数组格式输出，格式如下：
```json
[
  {
    "id": 0,
    "knowledge_type": "实操",
    "confidence": 0.9,
    "clip_start_sec": 0.0,
    "clip_end_sec": 10.0,
    "suggested_screenshoot_timestamps": [2.0, 5.0, 8.0]
  }
]
```'''
    
    def convert_timestamps(
        self, 
        relative_timestamps: List[float], 
        semantic_unit_start_sec: float
    ) -> List[float]:
        """
        将相对时间戳转换为绝对时间戳
        
        Args:
            relative_timestamps: 相对于片段开头的时间戳列表
            semantic_unit_start_sec: 语义单元在原视频中的起始时间
            
        Returns:
            绝对时间戳列表（相对于原视频开头）
        """
        return [semantic_unit_start_sec + ts for ts in relative_timestamps]
    
    async def analyze_clip(
        self,
        clip_path: str,
        semantic_unit_start_sec: float,
        semantic_unit_id: str,
        extra_prompt: Optional[str] = None,
        analysis_mode: str = "default"
    ) -> VLClipAnalysisResponse:
        """
        分析单个视频片段。

        Args:
            clip_path: 视频片段文件路径
            semantic_unit_start_sec: 语义单元在原视频中的起始时间（秒）
            semantic_unit_id: 语义单元 ID
            extra_prompt: 附加提示词（可选）
            analysis_mode: default / tutorial_stepwise

        Returns:
            VLClipAnalysisResponse: 包含切片与关键帧请求的完整结果
        """
        result = VLClipAnalysisResponse()
        normalized_mode = self._normalize_analysis_mode(analysis_mode)
        result.analysis_mode = normalized_mode

        try:
            # 调用 VL API
            analysis_results, token_usage, raw_json = await self._call_vl_api(
                clip_path,
                extra_prompt=extra_prompt,
                analysis_mode=normalized_mode,
            )
            result.token_usage = token_usage
            result.raw_response_json = raw_json or []

            if not analysis_results:
                result.success = False
                result.error_msg = "VL API returned empty result"
                return result

            # 计算并补齐绝对时间字段
            for i, ar in enumerate(analysis_results):
                ar.analysis_mode = normalized_mode

                # 将相对时间转换为原视频绝对时间
                ar.absolute_clip_start_sec = semantic_unit_start_sec + ar.clip_start_sec
                ar.absolute_clip_end_sec = semantic_unit_start_sec + ar.clip_end_sec
                ar.absolute_screenshot_timestamps = [
                    semantic_unit_start_sec + ts
                    for ts in ar.suggested_screenshoot_timestamps
                ]

                result.analysis_results.append(ar)

                step_id = int(ar.step_id) if int(ar.step_id) > 0 else (i + 1)
                action_brief = self._sanitize_action_brief(ar.step_description)

                if normalized_mode == "tutorial_stepwise":
                    # 教程模式仅关注步骤切分与关键帧，不依赖 VL 返回 knowledge_type。
                    result.clip_requests.append({
                        "clip_id": f"{semantic_unit_id}_step_{step_id:02d}_{action_brief}",
                        "start_sec": ar.absolute_clip_start_sec,
                        "end_sec": ar.absolute_clip_end_sec,
                        "knowledge_type": "process",
                        "semantic_unit_id": semantic_unit_id,
                        "step_id": step_id,
                        "step_description": ar.step_description,
                        "action_brief": action_brief,
                        "analysis_mode": normalized_mode,
                    })
                else:
                    # 默认模式保留旧行为：讲解型不生成视频切片。
                    k_type = str(ar.knowledge_type or "").strip("[]() \"'").lower()
                    if k_type not in {"\u8bb2\u89e3\u578b", "explanation", "abstract_explanation"}:
                        result.clip_requests.append({
                            "clip_id": f"vl_clip_{semantic_unit_id}_{i}",
                            "start_sec": ar.absolute_clip_start_sec,
                            "end_sec": ar.absolute_clip_end_sec,
                            "knowledge_type": ar.knowledge_type,
                            "semantic_unit_id": semantic_unit_id,
                            "step_id": step_id,
                            "step_description": ar.step_description,
                            "action_brief": action_brief,
                            "analysis_mode": normalized_mode,
                        })

                for j, ts in enumerate(ar.absolute_screenshot_timestamps):
                    screenshot_id = f"vl_ss_{semantic_unit_id}_{i}_{j}"
                    label = f"{ar.knowledge_type}_screenshot_{j+1}"
                    if normalized_mode == "tutorial_stepwise":
                        screenshot_id = f"{semantic_unit_id}_step_{step_id:02d}_{action_brief}_key_{j+1:02d}"
                        label = f"step_{step_id:02d}:{ar.step_description or action_brief}_keyframe_{j+1}"

                    result.screenshot_requests.append({
                        "screenshot_id": screenshot_id,
                        "timestamp_sec": ts,
                        "label": label,
                        "semantic_unit_id": semantic_unit_id,
                        "_relative_timestamp": ar.suggested_screenshoot_timestamps[j],
                        "_semantic_unit_start": semantic_unit_start_sec,
                        "step_id": step_id,
                        "step_description": ar.step_description,
                        "action_brief": action_brief,
                        "analysis_mode": normalized_mode,
                        "is_instructional_keyframe": normalized_mode == "tutorial_stepwise",
                        "keyframe_index": j + 1,
                    })

            result.success = True
            logger.info(
                f"VL analysis completed: {semantic_unit_id}, mode={normalized_mode}, "
                f"clips={len(result.clip_requests)}, screenshots={len(result.screenshot_requests)}, "
                f"prompt_tokens={result.token_usage.get('prompt_tokens', 0)}, "
                f"total_tokens={result.token_usage.get('total_tokens', 0)}"
            )

        except Exception as e:
            logger.error(f"VL analysis failed ({semantic_unit_id}): {e}")
            result.success = False
            result.error_msg = str(e)

        return result
    def _extract_token_usage(self, response: Any) -> Dict[str, int]:
        """
        从 OpenAI 兼容响应中提取 token 使用量。

        兼容对象/字典两种网关返回形态，缺失字段时兜底为 0。
        """
        usage = getattr(response, "usage", None)
        if usage is None and isinstance(response, dict):
            usage = response.get("usage")

        def _as_int(value: Any) -> int:
            try:
                return int(value)
            except Exception:
                return 0

        if usage is None:
            return {
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0,
            }

        if isinstance(usage, dict):
            prompt_tokens = _as_int(usage.get("prompt_tokens", 0))
            completion_tokens = _as_int(usage.get("completion_tokens", 0))
            total_tokens = _as_int(usage.get("total_tokens", prompt_tokens + completion_tokens))
        else:
            prompt_tokens = _as_int(getattr(usage, "prompt_tokens", 0))
            completion_tokens = _as_int(getattr(usage, "completion_tokens", 0))
            total_tokens = _as_int(getattr(usage, "total_tokens", prompt_tokens + completion_tokens))

        return {
            "prompt_tokens": max(0, prompt_tokens),
            "completion_tokens": max(0, completion_tokens),
            "total_tokens": max(0, total_tokens),
        }

    async def _call_vl_api(
        self,
        video_path: str,
        extra_prompt: Optional[str] = None,
        analysis_mode: str = "default",
    ) -> tuple[List[VLAnalysisResult], Dict[str, int], List[Dict[str, Any]]]:
        """
        调用 Qwen3-VL-Plus API 并解析结果。

        Returns:
            tuple: (分析结果列表, token 使用量, 归一化 JSON 结果)
        """
        normalized_mode = self._normalize_analysis_mode(analysis_mode)
        messages = await self._build_messages(
            video_path,
            extra_prompt=extra_prompt,
            analysis_mode=normalized_mode,
        )

        # 调用 API（含重试）
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                response_kwargs = {
                    "model": self.model,
                    "messages": messages,
                    "max_tokens": self.max_tokens,
                    "temperature": self.temperature,
                }

                if attempt > 0:
                    response_kwargs["temperature"] = 0.0
                    # DashScope 网关对 response_format 的兼容性不一致，需要容错。
                    # 使用 json_object 可提升重试场景的 JSON 稳定性。
                    response_kwargs["response_format"] = {"type": "json_object"}

                try:
                    response = await self.client.chat.completions.create(**response_kwargs)
                except Exception as e:
                    # 兼容非 OpenAI 官方网关：不支持 response_format 时回退。
                    err_str = str(e).lower()
                    if "response_format" in response_kwargs and ("response_format" in err_str or "unknown" in err_str):
                        response_kwargs.pop("response_format", None)
                        response = await self.client.chat.completions.create(**response_kwargs)
                    else:
                        raise

                content = response.choices[0].message.content
                finish_reason = getattr(response.choices[0], "finish_reason", None)
                token_usage = self._extract_token_usage(response)
                parsed_results, raw_json = self._parse_response_with_payload(
                    content,
                    finish_reason=finish_reason,
                    analysis_mode=normalized_mode,
                )
                return parsed_results, token_usage, raw_json

            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"VL API call failed (attempt {attempt+1}/{self.max_retries+1}): {e}, wait {wait_time}s")

                    # 若解析失败，附加更严格约束以提升下次 JSON 成功率。
                    err_str = str(e).lower()
                    if "json" in err_str or "parse" in err_str or "decode" in err_str or "unterminated" in err_str:
                        messages = await self._build_messages(
                            video_path,
                            extra_prompt=extra_prompt,
                            override_prompt=(
                                "You must output only a JSON array. No explanation text."
                                "Do not output reasoning or key_evidence fields."
                            ),
                            analysis_mode=normalized_mode,
                        )
                    await asyncio.sleep(wait_time)

        raise last_error

    def _get_tutorial_system_prompt(self) -> str:
        """
        教程模式系统提示词：仅做步骤切分与关键帧抽取。
        """
        return (
            "You are an instructional video editor for 1-on-1 teaching replication.\n"
            "Your only task is to split the clip into complete procedural steps and choose instructional keyframes.\n"
            "Do NOT classify knowledge types.\n"
            "For each step, output only: step_id, step_description, clip_start_sec, clip_end_sec, instructional_keyframe_timestamp.\n"
            "Keep explanation + execution + result in the same step.\n"
            "Remove hesitation/thinking-only intervals with no new information.\n"
            "Each step should be at least 5 seconds; merge overly short steps with neighbors."
        )

    async def _build_messages(
        self,
        video_path: str,
        extra_prompt: Optional[str] = None,
        override_prompt: Optional[str] = None,
        analysis_mode: str = "default"
    ) -> List[Dict[str, Any]]:
        """
        构建多模态消息。

        处理 DashScope 的 data-uri 单项 10MB 限制：
        - 小视频：直接 data-uri video_url
        - 大视频：优先尝试 DashScope File.upload 获取临时 URL（若安装了 dashscope）
        - 仍不可用：降级为抽取关键帧（image_url），并把每帧的时间戳作为文本标注提供给模型
        """
        # 统一构建系统提示词：教程模式不复用知识分类提示词。
        normalized_mode = self._normalize_analysis_mode(analysis_mode)
        if normalized_mode == "tutorial_stepwise":
            system_content = (
                self._get_tutorial_system_prompt()
                + self._get_output_constraints(normalized_mode)
                + "\n\n[Task] Split the procedural clip into steps and output stepwise JSON only."
            )
        else:
            system_content = (
                self.prompt_template
                + self._get_output_constraints(normalized_mode)
                + "\n\n[Task] Analyze the input video (or keyframes) and return JSON in the required schema."
            )
        
        user_text = ""
        if extra_prompt:
            user_text += extra_prompt.strip() + "\n"
        if override_prompt:
            user_text = "【重试补充要求】\n" + override_prompt + "\n" + user_text

        mode = (self.video_input_mode or "auto").lower()
        if mode not in ("auto", "data_uri", "dashscope_upload", "keyframes"):
            mode = "auto"

        video_file_size = 0
        try:
            video_file_size = Path(video_path).stat().st_size
        except Exception:
            video_file_size = 0

        # 1) data-uri（仅小文件安全）
        if mode in ("auto", "data_uri") and video_file_size and video_file_size <= _MAX_RAW_BYTES_FOR_BASE64_DATA_URI:
            video_base64 = self._encode_video_base64(video_path)
            if video_base64:
                return [
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": [
                        {"type": "video_url", "video_url": {"url": f"data:video/mp4;base64,{video_base64}"}},
                        {"type": "text", "text": user_text},
                    ]}
                ]

        # 2) DashScope File.upload 获取临时 URL（需要 dashscope SDK）
        if mode in ("auto", "dashscope_upload"):
            temp_url = await self._try_get_dashscope_temp_url(video_path)
            if temp_url:
                return [
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": [
                        {"type": "video_url", "video_url": {"url": temp_url}},
                        {"type": "text", "text": user_text},
                    ]}
                ]

        # 3) 降级为关键帧
        frames = await self._extract_keyframes(video_path, max_frames=self.max_input_frames)
        if not frames:
            raise ValueError(f"无法读取视频文件或抽帧失败: {video_path}")

        system_content += (
            "\n\n【关键帧输入说明】\n"
            "当输入为关键帧与时间戳时，请根据帧变化估算 clip_start_sec 与 clip_end_sec。\n"
            "If adjacent frames belong to the same step, estimate boundaries using their time span.\n"
        )
        content_items: List[Dict[str, Any]] = [{
            "type": "text",
            "text": "关键帧如下（含时间戳）："
        }]
        for idx, frame in enumerate(frames):
            content_items.append({"type": "text", "text": f"Frame {idx+1} @ {frame['timestamp_sec']:.2f}s"})
            content_items.append({"type": "image_url", "image_url": {"url": frame["data_uri"]}})

        if user_text:
            content_items.append({"type": "text", "text": user_text})
        
        return [
            {"role": "system", "content": system_content},
            {"role": "user", "content": content_items}
        ]

    async def _try_get_dashscope_temp_url(self, video_path: str) -> Optional[str]:
        """
        使用 DashScope SDK 上传本地文件，获取临时 URL。

        如果 dashscope SDK 不存在或上传失败，返回 None（由上层降级到关键帧）。
        """
        try:
            import dashscope  # type: ignore
        except Exception as e:
            logger.debug(f"dashscope SDK 不可用，跳过临时 URL 上传: {e}")
            return None

        if not self._api_key:
            return None

        def _upload() -> Optional[str]:
            dashscope.api_key = self._api_key
            # Files.upload 需要 file_path 字符串参数
            resp = dashscope.Files.upload(
                file_path=video_path,
                purpose="file-extract"
            )
            status_code = getattr(resp, "status_code", None)
            output = getattr(resp, "output", None)
            if status_code == 200 and output and isinstance(output, dict):
                return output.get("url")
            # 兼容 dict 形式返回
            if isinstance(resp, dict) and resp.get("status_code") == 200:
                return (resp.get("output") or {}).get("url")
            message = getattr(resp, "message", None) or str(resp)
            raise RuntimeError(f"DashScope Files.upload 失败: {message}")

        try:
            return await asyncio.to_thread(_upload)
        except Exception as e:
            logger.warning(f"DashScope 临时 URL 上传失败，降级关键帧: {e}")
            return None
    
    def _encode_video_base64(self, video_path: str) -> Optional[str]:
        """将视频文件编码为 base64（仅适用于小文件）"""
        try:
            with open(video_path, "rb") as f:
                return base64.b64encode(f.read()).decode("utf-8")
        except Exception as e:
            logger.error(f"视频编码失败: {e}")
            return None
    
    async def _extract_keyframes(self, video_path: str, max_frames: int = 6) -> List[Dict[str, Any]]:
        """
        从视频中抽取少量关键帧并编码为 data-uri（image/jpeg）。

        目标：绕过 DashScope 对单个 data-uri item 10MB 的限制。
        """
        try:
            import cv2  # type: ignore
            from PIL import Image  # type: ignore
        except Exception as e:
            logger.warning(f"关键帧抽取依赖不可用（opencv/pillow）：{e}")
            return []

        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return []

        fps = cap.get(cv2.CAP_PROP_FPS) or 0.0
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0.0
        duration = (frame_count / fps) if fps > 0 and frame_count > 0 else 0.0

        # 均匀采样，避免过多帧
        if max_frames <= 0:
            max_frames = 1
        if duration <= 0:
            timestamps = [0.0]
        else:
            # 避免取到末尾导致 seek 失败
            end = max(0.0, duration - 0.05)
            if max_frames == 1:
                timestamps = [max(0.0, end * 0.5)]
            else:
                step = end / (max_frames - 1)
                timestamps = [i * step for i in range(max_frames)]

        frames: List[Dict[str, Any]] = []
        for ts in timestamps:
            try:
                cap.set(cv2.CAP_PROP_POS_MSEC, ts * 1000.0)
                ok, frame_bgr = cap.read()
                if not ok or frame_bgr is None:
                    continue
                frame_rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
                image = Image.fromarray(frame_rgb)
                data_uri = self._encode_image_as_jpeg_data_uri(image)
                if not data_uri:
                    continue
                frames.append({"timestamp_sec": float(ts), "data_uri": data_uri})
            except Exception:
                continue

        cap.release()
        return frames

    def _encode_image_as_jpeg_data_uri(self, image) -> Optional[str]:
        """将 PIL.Image 编码为满足 10MB 限制的 JPEG data-uri。"""
        try:
            from PIL import Image  # type: ignore
        except Exception:
            return None

        if not isinstance(image, Image.Image):
            return None

        # 先按最大边缩放
        w, h = image.size
        max_dim = max(1, int(self.max_image_dim))
        if max(w, h) > max_dim:
            if w >= h:
                new_w = max_dim
                new_h = max(1, int(h * (max_dim / w)))
            else:
                new_h = max_dim
                new_w = max(1, int(w * (max_dim / h)))
            image = image.resize((new_w, new_h))

        quality = 82
        scale_rounds = 0
        while True:
            buf = io.BytesIO()
            try:
                image.save(buf, format="JPEG", quality=quality, optimize=True)
            except Exception:
                image.save(buf, format="JPEG", quality=quality)

            raw = buf.getvalue()
            if len(raw) <= int(_DASHSCOPE_DATA_URI_ITEM_MAX_BYTES * _DATA_URI_SAFETY_RATIO):
                b64 = base64.b64encode(raw).decode("utf-8")
                return f"data:image/jpeg;base64,{b64}"

            # 先降质量，再缩放
            if quality > 45:
                quality = max(45, quality - 10)
                continue

            if scale_rounds >= 4:
                return None

            w, h = image.size
            image = image.resize((max(1, int(w * 0.85)), max(1, int(h * 0.85))))
            scale_rounds += 1
            quality = 82

    def _sanitize_action_brief(self, text_value: str, max_len: int = 48) -> str:
        """Shorten a step description into a filename-safe action brief."""
        raw = str(text_value or "").strip().lower()
        raw = re.sub(r"[^a-z0-9]+", "_", raw)
        raw = re.sub(r"_+", "_", raw).strip("_")
        if not raw:
            return "action"
        if len(raw) > max_len:
            return raw[:max_len].rstrip("_") or "action"
        return raw

    def _safe_int(self, value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return int(default)

    def _safe_float_value(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return float(default)

    def _normalize_timestamp_list(self, value: Any) -> List[float]:
        if value is None:
            return []
        if not isinstance(value, list):
            value = [value]
        normalized: List[float] = []
        for item in value:
            ts = self._safe_float_value(item, 0.0)
            if ts >= 0.0:
                normalized.append(ts)
        normalized.sort()
        return normalized

    def _enforce_tutorial_step_constraints(self, results: List[VLAnalysisResult]) -> List[VLAnalysisResult]:
        """
        教程模式步骤后处理约束：
        - 按时间排序并重排 step_id
        - 将时长小于阈值的步骤与相邻步骤合并
        """
        if not results:
            return []

        min_step_duration = max(0.0, float(self.tutorial_min_step_duration_sec))
        ordered = sorted(results, key=lambda x: (float(x.clip_start_sec), float(x.clip_end_sec)))

        merged: List[VLAnalysisResult] = []
        idx = 0
        while idx < len(ordered):
            current = ordered[idx]
            current_duration = max(0.0, float(current.clip_end_sec) - float(current.clip_start_sec))

            # 优先并入前一步，避免打断语义连续性
            if min_step_duration > 0.0 and current_duration < min_step_duration and len(ordered) > 1:
                if merged:
                    prev = merged[-1]
                    prev.clip_end_sec = max(float(prev.clip_end_sec), float(current.clip_end_sec))
                    combined_desc = " / ".join(filter(None, [prev.step_description, current.step_description]))
                    prev.step_description = combined_desc[:160]
                    prev.suggested_screenshoot_timestamps = sorted(
                        set(prev.suggested_screenshoot_timestamps + current.suggested_screenshoot_timestamps)
                    )
                    idx += 1
                    continue
                if idx + 1 < len(ordered):
                    nxt = ordered[idx + 1]
                    nxt.clip_start_sec = min(float(nxt.clip_start_sec), float(current.clip_start_sec))
                    combined_desc = " / ".join(filter(None, [current.step_description, nxt.step_description]))
                    nxt.step_description = combined_desc[:160]
                    nxt.suggested_screenshoot_timestamps = sorted(
                        set(current.suggested_screenshoot_timestamps + nxt.suggested_screenshoot_timestamps)
                    )
                    idx += 1
                    continue

            merged.append(current)
            idx += 1

        for i, step in enumerate(merged, start=1):
            step.step_id = i
            step.id = i
            step.analysis_mode = "tutorial_stepwise"
            if not step.step_description:
                step.step_description = f"step_{i}"
            if step.clip_end_sec < step.clip_start_sec:
                step.clip_start_sec, step.clip_end_sec = step.clip_end_sec, step.clip_start_sec

        return merged

    def _parse_response_with_payload(
        self,
        content: str,
        finish_reason: Optional[str] = None,
        analysis_mode: str = "default",
    ) -> tuple[List[VLAnalysisResult], List[Dict[str, Any]]]:
        """
        解析 VL API 返回内容并提取可用 JSON。
        """
        normalized_mode = self._normalize_analysis_mode(analysis_mode)
        json_str = self._extract_json_candidate(content)

        data = None
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                data = json.loads(json_str)
                break
            except json.JSONDecodeError as e:
                last_err = e
                # 去除尾随逗号等常见 JSON 错误
                json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
                # 修复 key_evidence 字段的转义异常
                json_str = self._repair_key_evidence_field(json_str)
                if attempt == 2:
                    break

        if data is None:
            finish_hint = f", finish_reason={finish_reason}" if finish_reason else ""
            logger.error(f"JSON parse failed: {last_err}{finish_hint}, raw: {content[:500]}")
            raise ValueError(f"JSON parse failed: {last_err}{finish_hint}")

        if isinstance(data, dict):
            if isinstance(data.get("results"), list):
                items = data.get("results", [])
            elif isinstance(data.get("steps"), list):
                items = data.get("steps", [])
            else:
                items = [data]
        elif isinstance(data, list):
            items = data
        else:
            items = [data]

        results: List[VLAnalysisResult] = []
        normalized_payload: List[Dict[str, Any]] = []
        has_step_schema = False

        for index, item in enumerate(items):
            if not isinstance(item, dict):
                continue

            # 兼容教程 schema 与默认 schema
            step_id = self._safe_int(item.get("step_id", item.get("id", index + 1)), index + 1)
            step_description = str(
                item.get("step_description", item.get("description", item.get("title", ""))) or ""
            ).strip()

            raw_timestamps = item.get("instructional_keyframe_timestamp", None)
            if raw_timestamps is None:
                raw_timestamps = item.get("instructional_keyframe_timestamps", None)
            if raw_timestamps is None:
                raw_timestamps = item.get("suggested_screenshoot_timestamps", None)
            if raw_timestamps is None:
                raw_timestamps = item.get("suggested_screenshot_timestamps", [])
            timestamps = self._normalize_timestamp_list(raw_timestamps)

            clip_start_sec = self._safe_float_value(item.get("clip_start_sec", 0.0), 0.0)
            clip_end_sec = self._safe_float_value(item.get("clip_end_sec", 0.0), 0.0)
            if clip_end_sec < clip_start_sec:
                clip_start_sec, clip_end_sec = clip_end_sec, clip_start_sec

            key_evidence = item.get("key_evidence", "")
            if isinstance(key_evidence, list):
                key_evidence = "; ".join([str(x) for x in key_evidence if x is not None])
            else:
                key_evidence = str(key_evidence) if key_evidence is not None else ""

            tutorial_like = bool(
                ("step_id" in item)
                or ("step_description" in item)
                or ("instructional_keyframe_timestamp" in item)
                or normalized_mode == "tutorial_stepwise"
            )
            has_step_schema = has_step_schema or tutorial_like

            knowledge_type = str(item.get("knowledge_type", "") or "").strip()
            if tutorial_like:
                # 教程模式忽略模型返回的 knowledge_type，仅保留步骤结构
                knowledge_type = "process"

            result = VLAnalysisResult(
                id=self._safe_int(item.get("id", step_id), step_id),
                knowledge_type=knowledge_type,
                confidence=self._safe_float_value(item.get("confidence", 0.0), 0.0),
                reasoning=str(item.get("reasoning", "") or ""),
                key_evidence=key_evidence,
                clip_start_sec=clip_start_sec,
                clip_end_sec=clip_end_sec,
                suggested_screenshoot_timestamps=timestamps,
                step_id=step_id if tutorial_like else 0,
                step_description=step_description,
                analysis_mode="tutorial_stepwise" if tutorial_like else "default",
            )
            results.append(result)

            if tutorial_like:
                normalized_payload.append({
                    "step_id": step_id,
                    "step_description": step_description,
                    "clip_start_sec": clip_start_sec,
                    "clip_end_sec": clip_end_sec,
                    "instructional_keyframe_timestamp": timestamps,
                })
            else:
                normalized_payload.append({
                    "id": self._safe_int(item.get("id", index), index),
                    "knowledge_type": knowledge_type,
                    "confidence": self._safe_float_value(item.get("confidence", 0.0), 0.0),
                    "clip_start_sec": clip_start_sec,
                    "clip_end_sec": clip_end_sec,
                    "suggested_screenshoot_timestamps": timestamps,
                })

        if normalized_mode == "tutorial_stepwise" or has_step_schema:
            results = self._enforce_tutorial_step_constraints(results)
            normalized_payload = [
                {
                    "step_id": int(r.step_id),
                    "step_description": str(r.step_description or "").strip(),
                    "clip_start_sec": float(r.clip_start_sec),
                    "clip_end_sec": float(r.clip_end_sec),
                    "instructional_keyframe_timestamp": list(r.suggested_screenshoot_timestamps or []),
                }
                for r in results
            ]

        return results, normalized_payload

    def _parse_response(
        self,
        content: str,
        finish_reason: Optional[str] = None,
        analysis_mode: str = "default",
    ) -> List[VLAnalysisResult]:
        results, _ = self._parse_response_with_payload(
            content,
            finish_reason=finish_reason,
            analysis_mode=analysis_mode,
        )
        return results
    def _extract_json_candidate(self, content: str) -> str:
        """
        从模型回复中提取最可能的 JSON 片段。

        处理场景：
        - ```json ... ``` 代码块包裹
        - 回复前后带自然语言
        - 顶层为数组或对象
        """
        if not content:
            return ""

        # 1) 优先提取第一个代码块
        json_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", content, flags=re.IGNORECASE)
        if json_match:
            return json_match.group(1).strip()

        # 2) 尝试从第一个 { 或 [ 开始做括号配对提取
        start_idx = None
        for i, ch in enumerate(content):
            if ch in "[{":
                start_idx = i
                break
        if start_idx is None:
            return content.strip()

        extracted = self._extract_balanced_json(content[start_idx:])
        return (extracted or content[start_idx:]).strip()

    def _extract_balanced_json(self, s: str) -> Optional[str]:
        """从字符串开头提取括号配对的 JSON（忽略字符串内部的括号）。"""
        if not s or s[0] not in "[{":
            return None

        stack = []
        in_str = False
        escape = False
        for i, ch in enumerate(s):
            if in_str:
                if escape:
                    escape = False
                    continue
                if ch == "\\\\":
                    escape = True
                    continue
                if ch == "\"":
                    in_str = False
                continue

            if ch == "\"":
                in_str = True
                continue

            if ch in "[{":
                stack.append(ch)
            elif ch in "]}":
                if not stack:
                    return None
                left = stack.pop()
                if (left == "[" and ch != "]") or (left == "{" and ch != "}"):
                    return None
                if not stack:
                    return s[: i + 1]

        return None

    def _repair_key_evidence_field(self, json_str: str) -> str:
        """
        修复模型常见输出：
        "key_evidence": "a", "b", "c", "clip_start_sec": ...
        将其改写为：
        "key_evidence": ["a", "b", "c"], "clip_start_sec": ...
        """
        pattern = re.compile(
            r"\"key_evidence\"\s*:\s*(?P<vals>\"(?:\\.|[^\"\\])*\"(?:\s*,\s*\"(?:\\.|[^\"\\])*\")+)(?=\s*,\s*\"[A-Za-z_][^\"]*\"\s*:)",
            flags=re.MULTILINE,
        )

        def _repl(m: re.Match) -> str:
            vals = m.group("vals")
            parts = re.findall(r"\"(?:\\\\.|[^\"\\\\])*\"", vals)
            return "\"key_evidence\": [" + ", ".join(parts) + "]"

        return pattern.sub(_repl, json_str)
