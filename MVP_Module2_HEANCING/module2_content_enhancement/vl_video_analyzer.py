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
from pathlib import Path
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)


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
        
        self.base_url = api_config.get("base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1")
        self.model = api_config.get("model", "qwen3-vl-plus")
        self.max_retries = api_config.get("max_retries", 2)
        
        # 初始化 OpenAI 兼容客户端
        self.client = AsyncOpenAI(
            api_key=api_key,
            base_url=self.base_url
        )
        
        # 截图优化配置
        self.screenshot_optimization = config.get("screenshot_optimization", {})
        self.optimize_screenshots = self.screenshot_optimization.get("enabled", True)
        self.search_window_sec = self.screenshot_optimization.get("search_window_sec", 1.0)
        
        # 回退配置
        self.fallback_config = config.get("fallback", {})
        
        # 加载提示词模板
        self.prompt_template = self._load_prompt_template()
        
        logger.info(f"VLVideoAnalyzer 初始化完成: model={self.model}")
    
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
4. reasoning: 判断理由
5. key_evidence: 关键证据描述
6. clip_start_sec: 片段起始时间（秒，相对于视频开头）
7. clip_end_sec: 片段结束时间（秒）
8. suggested_screenshoot_timestamps: 建议的截图时间点数组（秒）

请以 JSON 数组格式输出，格式如下：
```json
[
  {
    "id": 0,
    "knowledge_type": "实操",
    "confidence": 0.9,
    "reasoning": "...",
    "key_evidence": "...",
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
        semantic_unit_id: str
    ) -> VLClipAnalysisResponse:
        """
        分析单个视频片段
        
        Args:
            clip_path: 视频片段文件路径
            semantic_unit_start_sec: 语义单元在原视频中的起始时间
            semantic_unit_id: 语义单元 ID
            
        Returns:
            VLClipAnalysisResponse 包含分析结果和素材请求
        """
        result = VLClipAnalysisResponse()
        
        try:
            # 调用 VL API
            analysis_results = await self._call_vl_api(clip_path)
            
            if not analysis_results:
                result.success = False
                result.error_msg = "VL API 返回空结果"
                return result
            
            # 转换时间戳并生成素材请求
            for i, ar in enumerate(analysis_results):
                # 转换相对时间戳为绝对时间戳
                ar.absolute_clip_start_sec = semantic_unit_start_sec + ar.clip_start_sec
                ar.absolute_clip_end_sec = semantic_unit_start_sec + ar.clip_end_sec
                ar.absolute_screenshot_timestamps = [
                    semantic_unit_start_sec + ts 
                    for ts in ar.suggested_screenshoot_timestamps
                ]
                
                result.analysis_results.append(ar)
                
                # 生成视频片段请求
                # 规则：讲解型不截取视频片段（只截图），其他类型截取视频片段
                if ar.knowledge_type != "讲解型":
                    result.clip_requests.append({
                        "clip_id": f"vl_clip_{semantic_unit_id}_{i}",
                        "start_sec": ar.absolute_clip_start_sec,
                        "end_sec": ar.absolute_clip_end_sec,
                        "knowledge_type": ar.knowledge_type,
                        "semantic_unit_id": semantic_unit_id
                    })
                
                # 生成截图请求（所有类型都截图，包括讲解型）
                for j, ts in enumerate(ar.absolute_screenshot_timestamps):
                    result.screenshot_requests.append({
                        "screenshot_id": f"vl_ss_{semantic_unit_id}_{i}_{j}",
                        "timestamp_sec": ts,
                        "label": f"{ar.knowledge_type}_截图{j+1}",
                        "semantic_unit_id": semantic_unit_id,
                        # 保存相对时间戳用于后续优化
                        "_relative_timestamp": ar.suggested_screenshoot_timestamps[j],
                        "_semantic_unit_start": semantic_unit_start_sec
                    })
            
            result.success = True
            logger.info(
                f"VL 分析完成: {semantic_unit_id}, "
                f"clips={len(result.clip_requests)}, screenshots={len(result.screenshot_requests)}"
            )
            
        except Exception as e:
            logger.error(f"VL 分析失败 ({semantic_unit_id}): {e}")
            result.success = False
            result.error_msg = str(e)
        
        return result
    
    async def _call_vl_api(self, video_path: str) -> List[VLAnalysisResult]:
        """
        调用 Qwen3-VL-Plus API 分析视频
        
        Args:
            video_path: 视频文件路径
            
        Returns:
            VLAnalysisResult 列表
        """
        # 将视频编码为 base64
        video_base64 = self._encode_video_base64(video_path)
        
        if not video_base64:
            raise ValueError(f"无法读取视频文件: {video_path}")
        
        # 构建消息
        # 注意：Qwen3-VL-Plus 的 OpenAI 兼容接口对视频使用 'video_url' 类型
        # 格式: data:video/mp4;base64,{base64_content}
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "video_url",
                        "video_url": {
                            "url": f"data:video/mp4;base64,{video_base64}"
                        }
                    },
                    {
                        "type": "text",
                        "text": self.prompt_template
                    }
                ]
            }
        ]
        
        # 调用 API（带重试）
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=messages
                )
                
                content = response.choices[0].message.content
                return self._parse_response(content)
                
            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"VL API 调用失败 (尝试 {attempt+1}/{self.max_retries+1}): {e}, 等待 {wait_time}s 重试")
                    await asyncio.sleep(wait_time)
        
        raise last_error
    
    def _encode_video_base64(self, video_path: str) -> Optional[str]:
        """将视频文件编码为 base64"""
        try:
            with open(video_path, "rb") as f:
                return base64.b64encode(f.read()).decode("utf-8")
        except Exception as e:
            logger.error(f"视频编码失败: {e}")
            return None
    
    def _parse_response(self, content: str) -> List[VLAnalysisResult]:
        """
        解析 VL API 返回的内容
        
        Args:
            content: API 返回的文本内容
            
        Returns:
            VLAnalysisResult 列表
        """
        # 尝试提取 JSON 代码块
        json_match = re.search(r'```(?:json)?\s*([\s\S]*?)```', content)
        if json_match:
            json_str = json_match.group(1).strip()
        else:
            # 尝试直接解析整个内容
            json_str = content.strip()
        
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失败: {e}, 原内容: {content[:500]}")
            raise ValueError(f"JSON 解析失败: {e}")
        
        # 确保是列表
        if not isinstance(data, list):
            data = [data]
        
        results = []
        for item in data:
            result = VLAnalysisResult(
                id=item.get("id", 0),
                knowledge_type=item.get("knowledge_type", ""),
                confidence=item.get("confidence", 0.0),
                reasoning=item.get("reasoning", ""),
                key_evidence=item.get("key_evidence", ""),
                clip_start_sec=item.get("clip_start_sec", 0.0),
                clip_end_sec=item.get("clip_end_sec", 0.0),
                suggested_screenshoot_timestamps=item.get("suggested_screenshoot_timestamps", [])
            )
            results.append(result)
        
        return results
