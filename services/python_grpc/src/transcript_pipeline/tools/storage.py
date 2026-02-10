"""
模块说明：阶段工具 storage 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime


class LocalStorage:
    """类说明：LocalStorage 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, storage_dir: str = "local_storage"):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - storage_dir: 目录路径（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
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
        执行逻辑：
        1) 组织输出结构与格式。
        2) 写入目标路径并处理异常。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：统一输出格式，降低落盘与格式错误。
        输入参数：
        - timestamps: 函数入参（类型：Dict[str, Dict[str, Any]]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        with open(self.subtitle_timestamps_file, 'w', encoding='utf-8') as f:
            json.dump(timestamps, f, ensure_ascii=False, indent=2)
        self._subtitle_timestamps = timestamps
    
    def load_subtitle_timestamps(self) -> Dict[str, Dict[str, Any]]:
        """
        执行逻辑：
        1) 校验输入路径与参数。
        2) 读取并解析为结构化对象。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：将外部数据转为内部结构，统一输入口径。
        决策逻辑：
        - 条件：self._subtitle_timestamps is not None
        - 条件：not self.subtitle_timestamps_file.exists()
        依据来源（证据链）：
        - 对象内部状态：self._subtitle_timestamps, self.subtitle_timestamps_file。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        if self._subtitle_timestamps is not None:
            return self._subtitle_timestamps
            
        if not self.subtitle_timestamps_file.exists():
            return {}
            
        with open(self.subtitle_timestamps_file, 'r', encoding='utf-8') as f:
            self._subtitle_timestamps = json.load(f)
        return self._subtitle_timestamps
    
    def get_subtitle_timestamp(self, subtitle_id: str) -> Optional[Dict[str, Any]]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - subtitle_id: 标识符（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        timestamps = self.load_subtitle_timestamps()
        return timestamps.get(subtitle_id)
    
    def find_subtitle_by_text(self, text_fragment: str, max_results: int = 3) -> List[Dict[str, Any]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：text_fragment[:20] in stored_text or stored_text in text_fragment
        依据来源（证据链）：
        - 输入参数：text_fragment。
        输入参数：
        - text_fragment: 函数入参（类型：str）。
        - max_results: 函数入参（类型：int）。
        输出参数：
        - Dict[str, Any] 列表（与输入或处理结果一一对应）。"""
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
        执行逻辑：
        1) 组织输出结构与格式。
        2) 写入目标路径并处理异常。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：统一输出格式，降低落盘与格式错误。
        输入参数：
        - timestamps: 函数入参（类型：Dict[str, Dict[str, float]]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        with open(self.sentence_timestamps_file, 'w', encoding='utf-8') as f:
            json.dump(timestamps, f, ensure_ascii=False, indent=2)
        self._sentence_timestamps = timestamps
        
    def load_sentence_timestamps(self) -> Dict[str, Dict[str, float]]:
        """
        执行逻辑：
        1) 校验输入路径与参数。
        2) 读取并解析为结构化对象。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：将外部数据转为内部结构，统一输入口径。
        决策逻辑：
        - 条件：self._sentence_timestamps is not None
        - 条件：not self.sentence_timestamps_file.exists()
        依据来源（证据链）：
        - 对象内部状态：self._sentence_timestamps, self.sentence_timestamps_file。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        if self._sentence_timestamps is not None:
            return self._sentence_timestamps
            
        if not self.sentence_timestamps_file.exists():
            return {}
            
        with open(self.sentence_timestamps_file, 'r', encoding='utf-8') as f:
            self._sentence_timestamps = json.load(f)
        return self._sentence_timestamps
    
    def get_sentence_timestamp(self, sentence_id: str) -> Optional[Dict[str, float]]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - sentence_id: 标识符（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        timestamps = self.load_sentence_timestamps()
        return timestamps.get(sentence_id)
    
    def get_sentence_time_range(self, sentence_ids: List[str]) -> Dict[str, float]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        决策逻辑：
        - 条件：not starts
        - 条件：sid in timestamps
        依据来源（证据链）：
        输入参数：
        - sentence_ids: 函数入参（类型：List[str]）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
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
        执行逻辑：
        1) 组织输出结构与格式。
        2) 写入目标路径并处理异常。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：统一输出格式，降低落盘与格式错误。
        输入参数：
        - timestamps: 函数入参（类型：Dict[str, Dict[str, Any]]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        with open(self.segment_timestamps_file, 'w', encoding='utf-8') as f:
            json.dump(timestamps, f, ensure_ascii=False, indent=2)
        self._segment_timestamps = timestamps
        
    def load_segment_timestamps(self) -> Dict[str, Dict[str, Any]]:
        """
        执行逻辑：
        1) 校验输入路径与参数。
        2) 读取并解析为结构化对象。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：将外部数据转为内部结构，统一输入口径。
        决策逻辑：
        - 条件：self._segment_timestamps is not None
        - 条件：not self.segment_timestamps_file.exists()
        依据来源（证据链）：
        - 对象内部状态：self._segment_timestamps, self.segment_timestamps_file。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        if self._segment_timestamps is not None:
            return self._segment_timestamps
            
        if not self.segment_timestamps_file.exists():
            return {}
            
        with open(self.segment_timestamps_file, 'r', encoding='utf-8') as f:
            self._segment_timestamps = json.load(f)
        return self._segment_timestamps
    
    def get_segment_timestamp(self, segment_id: str) -> Optional[Dict[str, Any]]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - segment_id: 标识符（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        timestamps = self.load_segment_timestamps()
        return timestamps.get(segment_id)
    
    # ========== Knowledge Point时间戳 (Step 7c 写入) ==========
    
    def save_kp_timestamps(self, timestamps: Dict[str, Dict[str, Any]]):
        """
        执行逻辑：
        1) 组织输出结构与格式。
        2) 写入目标路径并处理异常。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：统一输出格式，降低落盘与格式错误。
        输入参数：
        - timestamps: 函数入参（类型：Dict[str, Dict[str, Any]]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        kp_file = self.storage_dir / "kp_timestamps.json"
        with open(kp_file, "w", encoding="utf-8") as f:
            json.dump(timestamps, f, ensure_ascii=False, indent=2)
    
    def load_kp_timestamps(self) -> Dict[str, Dict[str, Any]]:
        """
        执行逻辑：
        1) 校验输入路径与参数。
        2) 读取并解析为结构化对象。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：将外部数据转为内部结构，统一输入口径。
        决策逻辑：
        - 条件：not kp_file.exists()
        依据来源（证据链）：
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        kp_file = self.storage_dir / "kp_timestamps.json"
        if not kp_file.exists():
            return {}
        with open(kp_file, "r", encoding="utf-8") as f:
            return json.load(f)
    
    def get_kp_timestamp(self, kp_id: str) -> Optional[Dict[str, Any]]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - kp_id: 标识符（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        timestamps = self.load_kp_timestamps()
        return timestamps.get(kp_id)
    
    # ========== 临时帧管理 ==========
    
    def get_temp_frame_path(self, instruction_id: str, frame_index: int) -> Path:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - instruction_id: 标识符（类型：str）。
        - frame_index: 函数入参（类型：int）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        return self.temp_frames_dir / f"{instruction_id}_{frame_index}.png"
    
    def list_temp_frames(self, instruction_id: Optional[str] = None) -> List[Path]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：instruction_id
        依据来源（证据链）：
        - 输入参数：instruction_id。
        输入参数：
        - instruction_id: 标识符（类型：Optional[str]）。
        输出参数：
        - Path 列表（与输入或处理结果一一对应）。"""
        if instruction_id:
            return list(self.temp_frames_dir.glob(f"{instruction_id}_*.png"))
        return list(self.temp_frames_dir.glob("*.png"))
    
    def cleanup_temp_frames(self, instruction_id: Optional[str] = None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - instruction_id: 标识符（类型：Optional[str]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        frames = self.list_temp_frames(instruction_id)
        for frame in frames:
            frame.unlink()
    
    # ========== 检查点管理 ==========
    
    def save_checkpoint(self, step_name: str, data: Dict[str, Any]):
        """
        执行逻辑：
        1) 组织输出结构与格式。
        2) 写入目标路径并处理异常。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：统一输出格式，降低落盘与格式错误。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - data: 数据列表/集合（类型：Dict[str, Any]）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        checkpoint_dir = self.storage_dir / "checkpoints"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        checkpoint_file = checkpoint_dir / f"{step_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        
        return checkpoint_file
    
    def load_latest_checkpoint(self, step_name: str) -> Optional[Dict[str, Any]]:
        """
        执行逻辑：
        1) 校验输入路径与参数。
        2) 读取并解析为结构化对象。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：将外部数据转为内部结构，统一输入口径。
        决策逻辑：
        - 条件：not checkpoint_dir.exists()
        - 条件：not checkpoints
        依据来源（证据链）：
        输入参数：
        - step_name: 函数入参（类型：str）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
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
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.sentence_timestamps_file.exists()
        - 条件：self.segment_timestamps_file.exists()
        依据来源（证据链）：
        - 对象内部状态：self.segment_timestamps_file, self.sentence_timestamps_file。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if self.sentence_timestamps_file.exists():
            self.sentence_timestamps_file.unlink()
        if self.segment_timestamps_file.exists():
            self.segment_timestamps_file.unlink()
        self.cleanup_temp_frames()
        self._sentence_timestamps = None
        self._segment_timestamps = None
        
    def get_storage_info(self) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 读取内部状态或外部资源。
        2) 返回读取结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：提供一致读取接口，降低调用耦合。
        输入参数：
        - 无。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        return {
            "storage_dir": str(self.storage_dir),
            "sentence_timestamps_count": len(self.load_sentence_timestamps()),
            "segment_timestamps_count": len(self.load_segment_timestamps()),
            "temp_frames_count": len(self.list_temp_frames())
        }
