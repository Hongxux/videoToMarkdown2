"""
本地存储管理
管理 sentence_timestamps.json 和 segment_timestamps.json
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime


class LocalStorage:
    """
    本地存储管理器
    
    管理时间戳和临时文件的存储
    """
    
    def __init__(self, storage_dir: str = "local_storage"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # 时间戳文件路径
        self.subtitle_timestamps_file = self.storage_dir / "subtitle_timestamps.json"  # 原始字幕时间戳
        self.sentence_timestamps_file = self.storage_dir / "sentence_timestamps.json"
        self.segment_timestamps_file = self.storage_dir / "segment_timestamps.json"
        
        # 临时帧目录
        self.temp_frames_dir = self.storage_dir / "temp_frames"
        self.temp_frames_dir.mkdir(parents=True, exist_ok=True)
        
        # 缓存
        self._subtitle_timestamps: Optional[Dict] = None
        self._sentence_timestamps: Optional[Dict] = None
        self._segment_timestamps: Optional[Dict] = None
    
    # ========== 原始字幕时间戳 (Step 2 写入, Step 8b/10 读取 - 最精确) ==========
    
    def save_subtitle_timestamps(self, timestamps: Dict[str, Dict[str, Any]]):
        """
        保存原始字幕时间戳（最精确的时间定位）
        
        Args:
            timestamps: {
                "SUB001": {"start_sec": 0.5, "end_sec": 3.2, "text": "前50字..."},
                "SUB002": {"start_sec": 3.2, "end_sec": 6.1, "text": "前50字..."}
            }
        """
        with open(self.subtitle_timestamps_file, 'w', encoding='utf-8') as f:
            json.dump(timestamps, f, ensure_ascii=False, indent=2)
        self._subtitle_timestamps = timestamps
    
    def load_subtitle_timestamps(self) -> Dict[str, Dict[str, Any]]:
        """加载原始字幕时间戳"""
        if self._subtitle_timestamps is not None:
            return self._subtitle_timestamps
            
        if not self.subtitle_timestamps_file.exists():
            return {}
            
        with open(self.subtitle_timestamps_file, 'r', encoding='utf-8') as f:
            self._subtitle_timestamps = json.load(f)
        return self._subtitle_timestamps
    
    def get_subtitle_timestamp(self, subtitle_id: str) -> Optional[Dict[str, Any]]:
        """获取单个原始字幕的时间戳"""
        timestamps = self.load_subtitle_timestamps()
        return timestamps.get(subtitle_id)
    
    def find_subtitle_by_text(self, text_fragment: str, max_results: int = 3) -> List[Dict[str, Any]]:
        """
        根据文本片段查找原始字幕ID和时间戳
        
        Returns: [{"subtitle_id": "SUB001", "start_sec": 0.5, "end_sec": 3.2, "match_score": 0.95}, ...]
        """
        timestamps = self.load_subtitle_timestamps()
        results = []
        
        for sub_id, data in timestamps.items():
            stored_text = data.get("text", "")
            # 简单的包含匹配
            if text_fragment[:20] in stored_text or stored_text in text_fragment:
                results.append({
                    "subtitle_id": sub_id,
                    "start_sec": data["start_sec"],
                    "end_sec": data["end_sec"],
                    "text": stored_text
                })
        
        return results[:max_results]
        
    # ========== 句子时间戳 (Step 4 写入, Step 8a/8b/9-11 读取) ==========
    
    def save_sentence_timestamps(self, timestamps: Dict[str, Dict[str, float]]):
        """
        保存句子时间戳
        
        Args:
            timestamps: {
                "S001": {"start_sec": 10.5, "end_sec": 15.2},
                "S002": {"start_sec": 15.2, "end_sec": 20.1}
            }
        """
        with open(self.sentence_timestamps_file, 'w', encoding='utf-8') as f:
            json.dump(timestamps, f, ensure_ascii=False, indent=2)
        self._sentence_timestamps = timestamps
        
    def load_sentence_timestamps(self) -> Dict[str, Dict[str, float]]:
        """加载句子时间戳"""
        if self._sentence_timestamps is not None:
            return self._sentence_timestamps
            
        if not self.sentence_timestamps_file.exists():
            return {}
            
        with open(self.sentence_timestamps_file, 'r', encoding='utf-8') as f:
            self._sentence_timestamps = json.load(f)
        return self._sentence_timestamps
    
    def get_sentence_timestamp(self, sentence_id: str) -> Optional[Dict[str, float]]:
        """获取单个句子的时间戳"""
        timestamps = self.load_sentence_timestamps()
        return timestamps.get(sentence_id)
    
    def get_sentence_time_range(self, sentence_ids: List[str]) -> Dict[str, float]:
        """
        获取多个句子的时间范围
        
        Returns:
            {"start_sec": min_start, "end_sec": max_end}
        """
        timestamps = self.load_sentence_timestamps()
        starts = []
        ends = []
        
        for sid in sentence_ids:
            if sid in timestamps:
                starts.append(timestamps[sid]["start_sec"])
                ends.append(timestamps[sid]["end_sec"])
        
        if not starts:
            return {"start_sec": 0, "end_sec": 0}
            
        return {
            "start_sec": min(starts),
            "end_sec": max(ends)
        }
    
    # ========== 段落时间戳 (Step 7 写入, Step 19/23 读取) ==========
    
    def save_segment_timestamps(self, timestamps: Dict[str, Dict[str, Any]]):
        """
        保存段落/片段时间戳
        
        Args:
            timestamps: {
                "SEG001": {
                    "start_sec": 10.5,
                    "end_sec": 45.3,
                    "source_sentence_ids": ["S001", "S002", "S003"]
                }
            }
        """
        with open(self.segment_timestamps_file, 'w', encoding='utf-8') as f:
            json.dump(timestamps, f, ensure_ascii=False, indent=2)
        self._segment_timestamps = timestamps
        
    def load_segment_timestamps(self) -> Dict[str, Dict[str, Any]]:
        """加载段落时间戳"""
        if self._segment_timestamps is not None:
            return self._segment_timestamps
            
        if not self.segment_timestamps_file.exists():
            return {}
            
        with open(self.segment_timestamps_file, 'r', encoding='utf-8') as f:
            self._segment_timestamps = json.load(f)
        return self._segment_timestamps
    
    def get_segment_timestamp(self, segment_id: str) -> Optional[Dict[str, Any]]:
        """获取单个片段的时间戳"""
        timestamps = self.load_segment_timestamps()
        return timestamps.get(segment_id)
    
    # ========== Knowledge Point时间戳 (Step 7c 写入) ==========
    
    def save_kp_timestamps(self, timestamps: Dict[str, Dict[str, Any]]):
        """
        保存知识点时间戳
        
        Args:
            timestamps: {
                "KP001": {
                    "start_sec": 10.5,
                    "end_sec": 65.3,
                    "segment_ids": ["SEG001", "SEG002"]
                }
            }
        """
        kp_file = self.storage_dir / "kp_timestamps.json"
        with open(kp_file, "w", encoding="utf-8") as f:
            json.dump(timestamps, f, ensure_ascii=False, indent=2)
    
    def load_kp_timestamps(self) -> Dict[str, Dict[str, Any]]:
        """加载知识点时间戳"""
        kp_file = self.storage_dir / "kp_timestamps.json"
        if not kp_file.exists():
            return {}
        with open(kp_file, "r", encoding="utf-8") as f:
            return json.load(f)
    
    def get_kp_timestamp(self, kp_id: str) -> Optional[Dict[str, Any]]:
        """获取单个知识点的时间戳"""
        timestamps = self.load_kp_timestamps()
        return timestamps.get(kp_id)
    
    # ========== 临时帧管理 ==========
    
    def get_temp_frame_path(self, instruction_id: str, frame_index: int) -> Path:
        """获取临时帧保存路径"""
        return self.temp_frames_dir / f"{instruction_id}_{frame_index}.png"
    
    def list_temp_frames(self, instruction_id: Optional[str] = None) -> List[Path]:
        """列出临时帧"""
        if instruction_id:
            return list(self.temp_frames_dir.glob(f"{instruction_id}_*.png"))
        return list(self.temp_frames_dir.glob("*.png"))
    
    def cleanup_temp_frames(self, instruction_id: Optional[str] = None):
        """清理临时帧"""
        frames = self.list_temp_frames(instruction_id)
        for frame in frames:
            frame.unlink()
    
    # ========== 检查点管理 ==========
    
    def save_checkpoint(self, step_name: str, data: Dict[str, Any]):
        """保存检查点"""
        checkpoint_dir = self.storage_dir / "checkpoints"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        checkpoint_file = checkpoint_dir / f"{step_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        
        return checkpoint_file
    
    def load_latest_checkpoint(self, step_name: str) -> Optional[Dict[str, Any]]:
        """加载最新检查点"""
        checkpoint_dir = self.storage_dir / "checkpoints"
        if not checkpoint_dir.exists():
            return None
            
        checkpoints = sorted(checkpoint_dir.glob(f"{step_name}_*.json"), reverse=True)
        if not checkpoints:
            return None
            
        with open(checkpoints[0], 'r', encoding='utf-8') as f:
            return json.load(f)
    
    # ========== 工具方法 ==========
    
    def clear_all(self):
        """清除所有存储"""
        if self.sentence_timestamps_file.exists():
            self.sentence_timestamps_file.unlink()
        if self.segment_timestamps_file.exists():
            self.segment_timestamps_file.unlink()
        self.cleanup_temp_frames()
        self._sentence_timestamps = None
        self._segment_timestamps = None
        
    def get_storage_info(self) -> Dict[str, Any]:
        """获取存储信息"""
        return {
            "storage_dir": str(self.storage_dir),
            "sentence_timestamps_count": len(self.load_sentence_timestamps()),
            "segment_timestamps_count": len(self.load_segment_timestamps()),
            "temp_frames_count": len(self.list_temp_frames())
        }
