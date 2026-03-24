"""
模块说明：视频转Markdown流程中的 alignment 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import numpy as np
import webrtcvad
import wave
import contextlib
from typing import List, Tuple, Dict

class LightweightVAD:
    """类说明：LightweightVAD 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    def __init__(self, mode=2):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - mode: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.vad = webrtcvad.Vad(mode)
        self.sample_rate = 16000
        self.frame_duration = 30  # ms
        self.frame_size = int(self.sample_rate * self.frame_duration / 1000)

    def read_wav(self, audio_path: str) -> np.ndarray:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过NumPy 数值计算、文件系统读写实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - audio_path: 文件路径（类型：str）。
        输出参数：
        - frombuffer 对象或调用结果。"""
        with wave.open(audio_path, "rb") as wf:
            frames = wf.readframes(wf.getnframes())
            return np.frombuffer(frames, dtype=np.int16)

    def detect_speech(self, audio: np.ndarray) -> List[Tuple[int, int]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：is_speech
        - 条件：len(frame) < self.frame_size
        - 条件：not is_speech and frame_speech
        依据来源（证据链）：
        - 对象内部状态：self.frame_size。
        输入参数：
        - audio: 函数入参（类型：np.ndarray）。
        输出参数：
        - Tuple[int, int] 列表（与输入或处理结果一一对应）。"""
        frames = [audio[i:i+self.frame_size] for i in range(0, len(audio), self.frame_size)]
        speech_segments = []
        is_speech = False
        start_ms = 0

        for i, frame in enumerate(frames):
            if len(frame) < self.frame_size:
                break
            
            # WebRTC VAD process_frame
            current_ms = i * self.frame_duration
            try:
                frame_speech = self.vad.is_speech(frame.tobytes(), self.sample_rate)
            except:
                frame_speech = False
            
            if not is_speech and frame_speech:
                is_speech = True
                start_ms = current_ms
            elif is_speech and not frame_speech:
                is_speech = False
                speech_segments.append((start_ms, current_ms))
        
        if is_speech:
            speech_segments.append((start_ms, len(audio) * 1000 / self.sample_rate))
            
        return speech_segments

class DTWCalibrator:
    """类说明：DTWCalibrator 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    def __init__(self, step_pattern="symmetric2"):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - step_pattern: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.step_pattern = step_pattern

    def text_similarity(self, s1: str, s2: str) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not s1 or not s2
        依据来源（证据链）：
        - 输入参数：s1, s2。
        输入参数：
        - s1: 函数入参（类型：str）。
        - s2: 函数入参（类型：str）。
        输出参数：
        - 数值型计算结果。"""
        s1, s2 = s1.strip(), s2.strip()
        if not s1 or not s2:
            return 0.0
        set1 = set(s1)
        set2 = set(s2)
        common = len(set1 & set2)
        total = len(set1 | set2)
        return common / total

    def compute_dtw_matrix(self, asr_texts: List[str], ref_texts: List[str]) -> np.ndarray:
        """
        执行逻辑：
        1) 准备输入数据。
        2) 执行计算并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：提供量化结果，为上游决策提供依据。
        输入参数：
        - asr_texts: 函数入参（类型：List[str]）。
        - ref_texts: 函数入参（类型：List[str]）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        n, m = len(asr_texts), len(ref_texts)
        dist_matrix = np.zeros((n, m))
        for i in range(n):
            for j in range(m):
                dist_matrix[i][j] = 1 - self.text_similarity(asr_texts[i], ref_texts[j])
        return dist_matrix

    def find_optimal_path(self, dist_matrix: np.ndarray) -> List[Tuple[int, int]]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：min_prev == cost_matrix[i - 1, j - 1]
        - 条件：min_prev == cost_matrix[i - 1, j]
        依据来源（证据链）：
        输入参数：
        - dist_matrix: 函数入参（类型：np.ndarray）。
        输出参数：
        - Tuple[int, int] 列表（与输入或处理结果一一对应）。"""
        n, m = dist_matrix.shape
        cost_matrix = np.full((n+1, m+1), np.inf)
        cost_matrix[0, 0] = 0.0

        for i in range(1, n+1):
            for j in range(1, m+1):
                cost = dist_matrix[i-1, j-1]
                # Symmetric2: Match(1,1), Insert(1,0), Delete(0,1)
                cost_matrix[i, j] = cost + min(
                    cost_matrix[i-1, j],
                    cost_matrix[i, j-1],
                    cost_matrix[i-1, j-1]
                )

        # Backtrack
        path = []
        i, j = n, m
        while i > 0 and j > 0:
            path.append((i-1, j-1))
            # Greedy backtrack
            min_prev = min(
                cost_matrix[i-1, j],
                cost_matrix[i, j-1],
                cost_matrix[i-1, j-1]
            )
            if min_prev == cost_matrix[i-1, j-1]:
                i, j = i-1, j-1
            elif min_prev == cost_matrix[i-1, j]:
                i -= 1
            else:
                j -= 1
        return path[::-1]

    def calibrate_timestamps(self, asr_segments: List[dict], ref_texts: List[str], ref_timestamps: List[Tuple[float, float]]) -> List[dict]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：ref_idx >= len(ref_timestamps)
        依据来源（证据链）：
        - 输入参数：ref_timestamps。
        输入参数：
        - asr_segments: 函数入参（类型：List[dict]）。
        - ref_texts: 函数入参（类型：List[str]）。
        - ref_timestamps: 函数入参（类型：List[Tuple[float, float]]）。
        输出参数：
        - dict 列表（与输入或处理结果一一对应）。"""
        asr_texts = [seg.get("text", "") for seg in asr_segments]
        dist_matrix = self.compute_dtw_matrix(asr_texts, ref_texts)
        path = self.find_optimal_path(dist_matrix)
        
        calibrated = [s.copy() for s in asr_segments]
        
        for asr_idx, ref_idx in path:
            if ref_idx >= len(ref_timestamps): continue
            
            ref_start, ref_end = ref_timestamps[ref_idx]
            original = asr_segments[asr_idx]
            orig_dur = original["end"] - original["start"]
            
            # Linear interpolation / Anchor lock
            # We trust Ref start/end more?
            # Weighted average: 0.7 Ref + 0.3 Orig?
            # Or hard lock? Doc says "Align".
            # Let's do partial Correction.
            
            # Simple policy: Fit ASR segment into Ref window
            calibrated[asr_idx]["start"] = ref_start
            # End is tricky if multiple ASR segments map to one Ref.
            # But simple DTW maps 1-to-1 in path? 
            # Actually path can skip.
            # I will just set start anchor.
            
        return calibrated
