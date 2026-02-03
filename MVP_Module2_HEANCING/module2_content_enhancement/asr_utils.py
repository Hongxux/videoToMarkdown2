"""
ASR Utilities - Wrapper for existing Whisper implementation

Wraps videoToMarkdown's Whisper implementation for Module 2 use.
Provides simple interface for audio-to-text conversion.
"""

import sys
import os
import logging
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class ASRExtractor:
    """
    语音识别提取器
    
    包装videoToMarkdown的Whisper实现,用于Module 2的多模态验证
    """
    
    def __init__(
        self,
        model_size: str = "small",
        device: str = "cpu",
        compute_type: str = "int8"
    ):
        """
        Args:
            model_size: Whisper模型大小 (tiny/base/small/medium/large)
            device: 设备 (cpu/cuda)
            compute_type: 计算类型 (int8/float16/float32)
        """
        self.model_size = model_size
        self.device = device
        self.compute_type = compute_type
        
        # 添加videoToMarkdown到路径
        project_root = Path(__file__).parent.parent.parent.parent
        videotomarkdown_path = project_root / "videoToMarkdown"
        
        if videotomarkdown_path.exists():
            sys.path.insert(0, str(videotomarkdown_path))
            logger.info(f"Added videoToMarkdown to path: {videotomarkdown_path}")
        else:
            logger.warning(f"videoToMarkdown not found at {videotomarkdown_path}")
    
    async def extract_audio_text(
        self,
        video_path: str,
        start_sec: float,
        end_sec: float
    ) -> str:
        """
        提取视频片段的语音文字
        
        用于C_multi验证中的ASR文本匹配
        
        Args:
            video_path: 视频文件路径
            start_sec: 起始时间(秒)
            end_sec: 结束时间(秒)
        
        Returns:
            转录的文本
        """
        try:
            # 导入现有实现
            from knowledge_engine.core.parallel_transcription import transcribe_segment
            
            # 构建segment参数
            segment = {
                'id': 0,
                'start': start_sec,
                'end': end_sec,
                'duration': end_sec - start_sec
            }
            
            # 调用现有实现
            args = (
                video_path,
                segment,
                self.model_size,  # 传递model_size字符串,让它自己加载
                self.device,
                self.compute_type,
                "zh"  # 中文
            )
            
            result = transcribe_segment(args)
            
            if result['success']:
                # 提取文本
                texts = [sub['text'] for sub in result['subtitles']]
                full_text = " ".join(texts)
                
                logger.info(f"ASR extracted {len(texts)} segments, "
                           f"{len(full_text)} chars")
                
                return full_text
            else:
                logger.error(f"ASR failed: {result.get('error', 'Unknown error')}")
                return ""
        
        except Exception as e:
            logger.error(f"ASR extraction failed: {e}")
            return ""
    
    def extract_full_transcript(
        self,
        video_path: str
    ) -> List[Dict[str, any]]:
        """
        提取完整视频的转录
        
        返回带时间戳的字幕列表
        
        Args:
            video_path: 视频路径
        
        Returns:
            [{'start': float, 'end': float, 'text': str}, ...]
        """
        try:
            from knowledge_engine.core.transcription import Transcriber
            import asyncio
            
            # 创建转录器
            transcriber = Transcriber(
                model_size=self.model_size,
                device=self.device,
                compute_type=self.compute_type,
                parallel=False  # MVP使用串行,速度足够
            )
            
            # 执行转录
            loop = asyncio.get_event_loop()
            subtitle_text = loop.run_until_complete(
                transcriber.transcribe(video_path)
            )
            
            # 解析字幕文本为结构化数据
            subtitles = self._parse_subtitle_text(subtitle_text)
            
            logger.info(f"Extracted {len(subtitles)} subtitle segments")
            
            return subtitles
        
        except Exception as e:
            logger.error(f"Full transcript extraction failed: {e}")
            return []
    
    def _parse_subtitle_text(self, text: str) -> List[Dict]:
        """
        解析字幕文本
        
        格式: [00:00:10 -> 00:00:15] 文本内容
        """
        import re
        
        subtitles = []
        
        # 正则匹配: [HH:MM:SS -> HH:MM:SS] 文本
        pattern = r'\[(\d{2}):(\d{2}):(\d{2})\s*->\s*(\d{2}):(\d{2}):(\d{2})\]\s*(.+)'
        
        for line in text.split('\n'):
            match = re.match(pattern, line)
            if match:
                h1, m1, s1, h2, m2, s2, text_content = match.groups()
                
                start_sec = int(h1) * 3600 + int(m1) * 60 + int(s1)
                end_sec = int(h2) * 3600 + int(m2) * 60 + int(s2)
                
                subtitles.append({
                    'start': start_sec,
                    'end': end_sec,
                    'text': text_content.strip()
                })
        
        return subtitles
