"""
模块说明：视频转Markdown流程中的 transcription 模块。
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
import time
import asyncio
import subprocess
import tempfile
import statistics
import inspect
import numpy as np
from collections import namedtuple
from .processing import BaseProcessor, ProgressUpdate
from .alignment import LightweightVAD
from .language_normalizer import normalize_whisper_language
from services.python_grpc.src.common.utils.time import format_hhmmss

# Dummy Segment data structure for manual construction
Segment = namedtuple('Segment', ['start', 'end', 'text'])

# 模块级模型缓存：存储已校验的模型路径，避免重复校验
_MODEL_CACHE = {}  # {model_size: model_dir}

class Transcriber(BaseProcessor):
    """类说明：Transcriber 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, model_size="small", device="cpu", compute_type="int8", 
                 parallel=False, num_workers=3, segment_duration=600, config=None):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - model_size: 模型/推理配置（类型：未标注）。
        - device: 函数入参（类型：未标注）。
        - compute_type: 函数入参（类型：未标注）。
        - parallel: 函数入参（类型：未标注）。
        - num_workers: 函数入参（类型：未标注）。
        - segment_duration: 函数入参（类型：未标注）。
        - config: 配置对象/字典（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        super().__init__()
        self.model_size = model_size
        self.device = device
        self.compute_type = compute_type
        self.parallel = parallel
        self.num_workers = num_workers
        self.segment_duration = segment_duration
        self.config = config
        self.model = None
        self.transcribe_params = {}
        
        # 回调函数，用于暂停和恢复外部 UI（如 Rich Live）
        self.on_manual_output_start = None
        self.on_manual_output_end = None

    def _load_model(self, config=None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not self.model
        - 条件：self.on_manual_output_start
        - 条件：self.model_size in _MODEL_CACHE
        依据来源（证据链）：
        - 阈值常量：_MODEL_CACHE。
        - 对象内部状态：self.model, self.model_size, self.on_manual_output_end, self.on_manual_output_start。
        输入参数：
        - config: 配置对象/字典（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        global _MODEL_CACHE
        
        # 优先使用传入的 config，否则使用初始化时的 config
        cfg = config or self.config
        
        if not self.model:
            # 如果有外部 UI，先暂停
            if self.on_manual_output_start:
                self.on_manual_output_start()
                
            from .model_downloader import download_whisper_model
            from faster_whisper import WhisperModel
            
            # 1. 检查缓存：如果该模型已在本次运行中校验过，直接使用缓存路径
            if self.model_size in _MODEL_CACHE:
                model_dir = _MODEL_CACHE[self.model_size]
                print(f"⚡ 使用缓存模型路径: {self.model_size} (跳过校验)", flush=True)
            else:
                # 2. 首次使用：执行完整校验和下载
                hf_endpoint = None
                use_mirror = True
                proxy = None
                skip_integrity_check_on_failure = True
                skip_reverify_after_success = True
                if cfg:
                    w_cfg = cfg.get("whisper", {})
                    hf_endpoint = w_cfg.get("hf_endpoint")
                    use_mirror = w_cfg.get("use_mirror", True)
                    proxy = w_cfg.get("download_proxy")
                    skip_integrity_check_on_failure = bool(
                        w_cfg.get("skip_integrity_check_on_failure", True)
                    )
                    skip_reverify_after_success = bool(
                        w_cfg.get("skip_reverify_after_success", True)
                    )
                
                try:
                    # 显式检查并下载模型（带进度条和哈希校验）
                    model_dir = download_whisper_model(
                        self.model_size, 
                        hf_endpoint=hf_endpoint,
                        use_mirror=use_mirror,
                        proxy=proxy,
                        skip_integrity_check_on_failure=skip_integrity_check_on_failure,
                        skip_reverify_after_success=skip_reverify_after_success,
                    )
                    # 缓存校验通过的模型路径
                    _MODEL_CACHE[self.model_size] = model_dir
                except Exception as e:
                    print(f"⚠️  模型校验失败 ({e})，尝试 CPU/INT8 模式...", flush=True)
                    self.model = WhisperModel(self.model_size, device="cpu", compute_type="int8")
                    if self.on_manual_output_end:
                        self.on_manual_output_end()
                    return
            
            try:
                # 3. 加载模型到内存 (使用绝对路径 + 强制本地模式)
                self.emit_progress("transcription", 0.05, f"加载模型 ({self.model_size})...")
                print(f"正在加载 Whisper 模型到内存 ({self.device})...", flush=True)
                
                self.model = WhisperModel(
                    model_dir, 
                    device=self.device, 
                    compute_type=self.compute_type,
                    local_files_only=True  # 核心：不再尝试联网校验
                )
                self.emit_progress("transcription", 0.08, f"✅ 模型加载完成")
            except Exception as e:
                # Fallback to cpu/int8 if cuda/float16 fails
                print(f"⚠️  加载失败 ({e})，尝试 CPU/INT8 模式...", flush=True)
                self.model = WhisperModel(self.model_size, device="cpu", compute_type="int8")
            finally:
                # 恢复 UI
                if self.on_manual_output_end:
                    self.on_manual_output_end()

    async def transcribe(self, video_path, language="auto", progress_callback=None):
        """
        异步转录接口：供 gRPC 服务调用。
        内部委托给 parallel_transcription.transcribe_parallel，
        通过 asyncio.to_thread 避免阻塞事件循环。

        输入参数：
        - video_path: 视频文件路径（类型：str）。
        - language: 语言代码（类型：str，默认 "zh"）。
        输出参数：
        - subtitle_text: 格式化后的字幕文本（类型：str）。
        """
        from .parallel_transcription import transcribe_parallel

        # 从 config 中提取 hf_endpoint（如有）
        hf_endpoint = None
        if self.config:
            hf_endpoint = self.config.get("whisper", {}).get("hf_endpoint")

        normalized_language = normalize_whisper_language(language)

        subtitle_text = await asyncio.to_thread(
            transcribe_parallel,
            video_path=video_path,
            model_size=self.model_size,
            device=self.device,
            compute_type=self.compute_type,
            language=normalized_language,
            segment_duration=self.segment_duration,
            num_workers=self.num_workers,
            hf_endpoint=hf_endpoint,
            config=self.config,
            progress_callback=progress_callback,
        )
        return subtitle_text
