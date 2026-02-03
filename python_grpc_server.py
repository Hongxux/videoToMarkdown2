"""
Python gRPC Server for Video Processing

🔑 V2 架构: 支持 Java-Python 分层协作
- 全局单例资源管理
- Phase2A: analyze_only (语义分析)
- Phase2B: assemble_only (富文本组装)
"""

import os
import sys
print("🚀 PYTHON GRPC SERVER IS STARTING - VERSION V3.1 (PARALLEL CV) 🚀", flush=True)
import logging
import asyncio
import threading
import psutil
import traceback
from concurrent import futures
from typing import Optional

import grpc
import gc
from grpc import aio

# 添加项目路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
sys.path.insert(0, os.path.dirname(current_dir))
# 🔑 添加 MVP_Module2_HEANCING 目录，使内部 'from module2_content_enhancement.xxx' 导入生效
sys.path.insert(0, os.path.join(current_dir, "MVP_Module2_HEANCING"))
# 🔑 添加 proto 目录，解决 gRPC 生成代码内部导入 pb2 失败的问题
sys.path.insert(0, os.path.join(current_dir, "proto"))

# gRPC 生成的代码 (需要先运行 protoc 生成)
from proto import video_processing_pb2
from proto import video_processing_pb2_grpc

# 模块导入
from stage1_pipeline.graph import run_pipeline
from videoToMarkdown.knowledge_engine.core.video import VideoProcessor
from videoToMarkdown.knowledge_engine.core.transcription import Transcriber
from MVP_Module2_HEANCING.module2_content_enhancement import (
    RichTextPipeline,
    PipelineConfig,
    ScreenshotRequest,
    ClipRequest,
    MaterialRequests
)
from MVP_Module2_HEANCING.module2_content_enhancement.visual_feature_extractor import (
    VisualFeatureExtractor,
    get_visual_process_pool,
    get_shared_frame_registry
)
# 🔑 Import tools for GenerateMaterialRequests
from MVP_Module2_HEANCING.module2_content_enhancement.screenshot_selector import ScreenshotSelector
from MVP_Module2_HEANCING.module2_content_enhancement.video_clip_extractor import VideoClipExtractor

logger = logging.getLogger(__name__)


# =============================================================================
# 🚀 CV 验证模块级函数 (ThreadPool 兼容)
# =============================================================================

# 进程/线程内 Validator 缓存 (避免重复创建)
_cv_validator_cache = {}
_cv_validator_lock = threading.Lock()


def run_cv_validation_unit(video_path: str, unit_data: dict) -> dict:
    """
    单个语义单元的 CV 验证 (在 ThreadPool Worker 中执行)
    
    使用线程内全局缓存避免重复创建 CVKnowledgeValidator
    
    Args:
        video_path: 视频路径
        unit_data: {"unit_id", "start_sec", "end_sec", "knowledge_type"}
    
    Returns:
        验证结果字典
    """
    global _cv_validator_cache, _cv_validator_lock
    
    try:
        # 线程内 Validator 复用 (避免重复打开视频)
        with _cv_validator_lock:
            if video_path not in _cv_validator_cache:
                from MVP_Module2_HEANCING.module2_content_enhancement.cv_knowledge_validator import CVKnowledgeValidator
                logger.info(f"🔄 Creating CVKnowledgeValidator for ThreadPool: {video_path}")
                _cv_validator_cache[video_path] = CVKnowledgeValidator(video_path, use_resource_manager=True)
            validator = _cv_validator_cache[video_path]
        
        # 执行验证 - 使用正确的 API: detect_visual_states
        stable_islands, action_units, redundancy_segments = validator.detect_visual_states(
            start_sec=unit_data["start_sec"],
            end_sec=unit_data["end_sec"]
        )
        
        # 构建结果字典
        # 将 StableIsland 和 ActionUnit 对象序列化为 dict
        stable_islands_data = []
        for si in stable_islands:
            stable_islands_data.append({
                "start_sec": si.start_sec,
                "end_sec": si.end_sec,
                "mid_sec": (si.start_sec + si.end_sec) / 2,
                "duration_sec": si.end_sec - si.start_sec
            })
        
        action_segments_data = []
        for au in action_units:
            # ActionUnit 可能有 internal_stable_islands 属性
            internal_islands = []
            if hasattr(au, 'internal_stable_islands') and au.internal_stable_islands:
                for isi in au.internal_stable_islands:
                    internal_islands.append({
                        "start_sec": isi.start_sec,
                        "end_sec": isi.end_sec,
                        "mid_sec": (isi.start_sec + isi.end_sec) / 2,
                        "duration_sec": isi.end_sec - isi.start_sec
                    })
            
            action_segments_data.append({
                "start_sec": au.start_sec,
                "end_sec": au.end_sec,
                "action_type": getattr(au, 'action_type', au.classify() if hasattr(au, 'classify') else 'knowledge'),
                "internal_stable_islands": internal_islands
            })
        
        # 确定主要模态 (基于 action_units 的分析)
        if not action_units and stable_islands:
            modality = "screenshot"  # 纯稳定 → 截图
            knowledge_subtype = "static"
        elif action_units:
            # 根据第一个 action_unit 的分类决定
            first_action = action_units[0]
            if hasattr(first_action, 'classify_modality'):
                modality_result = first_action.classify_modality()
                modality = modality_result.value if hasattr(modality_result, 'value') else str(modality_result)
            else:
                modality = "video_screenshot"
            knowledge_subtype = getattr(first_action, 'action_type', 'mixed')
        else:
            modality = "unknown"
            knowledge_subtype = "unknown"
        
        return {
            "unit_id": unit_data["unit_id"],
            "modality": modality,
            "knowledge_subtype": knowledge_subtype,
            "stable_islands": stable_islands_data,
            "action_segments": action_segments_data
        }
        
    except Exception as e:
        logger.error(f"CV validation failed for {unit_data['unit_id']}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "unit_id": unit_data["unit_id"],
            "modality": "unknown",
            "knowledge_subtype": "unknown",
            "stable_islands": [],
            "action_segments": [],
            "error": str(e)
        }



class GlobalResourceManager:
    """
    全局资源管理器 - 单例模式
    
    🔑 确保 Whisper/LLM/Vision 等模型只加载一次，
    多次 gRPC 调用复用，避免重复加载。
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def initialize(self, config: dict = None):
        """仅保存配置，资源改为延迟加载"""
        if self._initialized:
            return
        
        with self._lock:
            if self._initialized:
                return
            
            self.config = config or {}
            self._llm_client = None
            self._vision_client = None
            self._transcriber = None
            self._knowledge_classifier = None
            
            # 🚀 CV Validators Cache
            self._cv_validators = {}
            self._cv_validator_lock = threading.Lock()
            
            self._initialized = True
            logger.info("✅ Global resources config saved (lazy loading enabled)")

    @property
    def transcriber(self):
        if self._transcriber is None:
            with self._lock:
                if self._transcriber is None:
                    try:
                        from videoToMarkdown.knowledge_engine.core.transcription import Transcriber
                        self._transcriber = Transcriber()
                        logger.info("  → Transcriber loaded lazily")
                    except Exception as e:
                        logger.error(f"Transcriber init failed: {e}")
        return self._transcriber

    @property
    def knowledge_classifier(self):
        if self._knowledge_classifier is None:
            with self._lock:
                if self._knowledge_classifier is None:
                    try:
                        from MVP_Module2_HEANCING.module2_content_enhancement.knowledge_classifier import KnowledgeClassifier
                        # LLMClient 内部现在也是延迟加载 httpx 客户端的，所以这里初始化是安全的
                        self._knowledge_classifier = KnowledgeClassifier()
                        logger.info("  → KnowledgeClassifier loaded lazily")
                    except Exception as e:
                        logger.error(f"KnowledgeClassifier init failed: {e}")
        return self._knowledge_classifier

    @property
    def vision_client(self):
        if self._vision_client is None:
            with self._lock:
                if self._vision_client is None:
                    try:
                        from MVP_Module2_HEANCING.module2_content_enhancement.vision_ai_client import get_vision_ai_client
                        self._vision_client = get_vision_ai_client(self.config)
                        logger.info("  → Vision AI client loaded lazily")
                    except Exception as e:
                        logger.error(f"Vision client init failed: {e}")
        return self._vision_client
    
    def _init_llm_client(self, config):
        """初始化 LLM 客户端"""
        try:
            from openai import OpenAI
            api_key = config.get("deepseek_api_key") or os.getenv("DEEPSEEK_API_KEY")
            if api_key:
                self.llm_client = OpenAI(
                    api_key=api_key,
                    base_url=config.get("deepseek_base_url", "https://api.deepseek.com")
                )
                logger.info("  → LLM client initialized (DeepSeek)")
            else:
                self.llm_client = None
                logger.warning("  → LLM client not available (no API key)")
        except Exception as e:
            logger.error(f"  → LLM client init failed: {e}")
            self.llm_client = None
    
    def _init_media_tools(self, config):
        """初始化 Vision AI 客户端 (Media Tools 现改为按视频初始化)"""
        try:
            # Vision AI Client is global as it handles connections/pool
            from MVP_Module2_HEANCING.module2_content_enhancement.vision_ai_client import get_vision_ai_client
            self.vision_client = get_vision_ai_client(config)
            logger.info("  → Vision AI client initialized (Global Pool)")
            
            # visual_extractor and screenshot_selector are VIDEO-SPECIFIC.
            # They will be initialized per-video in the handlers or via a cache.
            self.visual_extractor = None 
            self.screenshot_selector = None
            
        except Exception as e:
            logger.error(f"  → Media tools init failed: {e}")
            self.vision_client = None

    def _init_transcriber(self, config):
        """初始化 Whisper Transcriber 并预加载模型"""
        try:
            logger.info("  → Initializing Whisper Transcriber (this may take a while)...")
            # 默认配置
            model_size = config.get("whisper_model", "medium")
            device = config.get("whisper_device", "cpu")
            
            # 🔑 性能优化配置
            # 自动检测 CPU 核心数，设置合理的并行数
            import multiprocessing
            cpu_count = multiprocessing.cpu_count()
            
            # 使用 CPU 核心数的一半作为 workers（最少2，最多8）
            num_workers = max(2, min(cpu_count // 2, 8))
            
            logger.info(f"  → CPU cores: {cpu_count}, using {num_workers} workers for parallel transcription")
            
            self.transcriber = Transcriber(
                model_size=model_size,
                device=device,
                compute_type="int8",     # 🔑 启用 int8 量化加速
                parallel=True,           # 🔑 启用并行转录
                num_workers=num_workers, # 🔑 根据 CPU 核心数动态设置
                config=config
            )
            # 🔑 预加载模型 (异步加载以便不阻塞启动，但此处保持同步以确保就绪)
            # 添加更强健的初始化逻辑
            try:
                self.transcriber._load_model()
                logger.info(f"  → Whisper Transcriber initialized (parallel={True}, int8={True}, workers={num_workers})")
            except Exception as e:
                logger.warning(f"  → Whisper Transcriber model pre-load failed (will retry on first task): {e}")
        except Exception as e:
            logger.error(f"  → Whisper Transcriber component init failed: {e}")
            self.transcriber = None

    # 🚀 CV Validators Cache (global singleton per video)
    def _init_video_tools_cache(self):
        if not hasattr(self, "_video_tools"):
            self._video_tools = {} # video_path -> {extractor, selector}
            self._video_tools_lock = threading.Lock()

    def get_screenshot_selector(self, video_path: str):
        """获取或创建该视频专用的 ScreenshotSelector"""
        self._init_video_tools_cache()
        with self._video_tools_lock:
            if video_path not in self._video_tools:
                logger.info(f"🔄 Initializing ScreenshotSelector for: {video_path}")
                extractor = VisualFeatureExtractor(video_path)
                selector = ScreenshotSelector(visual_extractor=extractor, config=self.config)
                self._video_tools[video_path] = {
                    "extractor": extractor,
                    "selector": selector
                }
            return self._video_tools[video_path]["selector"]

    def get_cv_validator(self, video_path: str):
        """
        获取或创建 CV 验证器（全局单例，按视频路径缓存）
        
        避免重复加载视频和模型，提高性能
        """
        with self._cv_validator_lock:
            if video_path not in self._cv_validators:
                logger.info(f"🔄 Creating CVKnowledgeValidator for: {video_path}")
                try:
                    from MVP_Module2_HEANCING.module2_content_enhancement.cv_knowledge_validator import CVKnowledgeValidator
                    self._cv_validators[video_path] = CVKnowledgeValidator(video_path)
                    logger.info(f"✅ CVKnowledgeValidator created for: {video_path}")
                except Exception as e:
                    logger.error(f"❌ Failed to create CVKnowledgeValidator: {e}")
                    raise
            return self._cv_validators[video_path]
    
    def cleanup_cv_validators(self):
        """清理所有 CV 验证器（释放资源）"""
        with self._cv_validator_lock:
            for video_path, validator in self._cv_validators.items():
                try:
                    if hasattr(validator, 'cleanup'):
                        validator.cleanup()
                except Exception as e:
                    logger.warning(f"Failed to cleanup validator for {video_path}: {e}")
            self._cv_validators.clear()
            logger.info("🧹 All CV validators cleaned up")


class VideoProcessingServicer(video_processing_pb2_grpc.VideoProcessingServiceServicer):
    """
    gRPC Service 实现
    
    实现 video_processing.proto 中定义的所有 RPC 方法
    """
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        
        # 🔑 使用全局资源管理器
        self.resources = GlobalResourceManager()
        self.resources.initialize(config)
        
        # 活跃任务计数
        self._active_tasks = 0
        self._task_lock = threading.Lock()
        
        # 🚀 V6: Java 控制并发 + Python ProcessPool + SharedMemory
        # - Java 控制发送多少并行请求 (熔断/重试)
        # - Python 使用 ProcessPool 绕过 GIL
        # - SharedFrameRegistry 实现帧共享
        from concurrent.futures import ProcessPoolExecutor
        import multiprocessing
        
        # 🚀 释放物理算力: 设为 CPU 核心数
        # 实际负载由 Java 端 Semaphore 精确控制，Python 端只提供最大能力底座
        # [FIX] 动态计算 Worker 数量防止 OOM
        # 策略: 至少保留 4GB 给系统，剩余内存每 3GB 允许一个 Worker (Windows Spawn 模式开销大)
        mem = psutil.virtual_memory()
        available_ram_gb = mem.transferable if hasattr(mem, 'transferable') else mem.available / (1024**3)
        # 保底 1 个, 上限 8 个 (或 CPU 核心数-1)
        cpu_cores = multiprocessing.cpu_count()
        # [User Request] 1.5GB per worker
        max_workers_by_ram = max(1, int((available_ram_gb - 4) / 1.5))
        self.cv_worker_count = min(max(1, cpu_cores-1), max_workers_by_ram, 8)
        logger.info(f"🚀 CV ProcessPool Config: {self.cv_worker_count} workers (Limit by RAM: {max_workers_by_ram}, CPU: {cpu_cores})") 

        
        # 创建 ProcessPool (使用 spawn 方式确保 Windows 兼容)
        from cv_worker import init_cv_worker
        self.cv_process_pool = ProcessPoolExecutor(
            max_workers=self.cv_worker_count,
            initializer=init_cv_worker
        )
        
        # SharedFrameRegistry 用于主进程预读帧
        self.frame_registry = get_shared_frame_registry()
        
        logger.info(f"🚀 CV ProcessPool created: {self.cv_worker_count} workers + SharedMemory")
        logger.info("VideoProcessingServicer initialized (Java controls concurrency)")



    
    async def HealthCheck(self, request, context):
        """健康检查"""
        # 获取系统状态
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        
        status = video_processing_pb2.ServerStatus(
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            available_memory_mb=memory.available // (1024 * 1024),
            gpu_available=False  # TODO: 检测GPU
        )

        return video_processing_pb2.HealthCheckResponse(
            healthy=True,
            version="2.0.0",
            active_tasks=self._active_tasks,
            status=status
        )

    async def DownloadVideo(self, request, context):
        """步骤1: 下载视频"""
        task_id = request.task_id
        video_url = request.video_url
        # output_dir 从 request 中获取，但我们会覆盖为统一的 storage 目录
        
        logger.info(f"[{task_id}] DownloadVideo: {video_url}")
        
        try:
            self._increment_tasks()

            # 🔑 生成基于 URL 的哈希值作为目录名
            import hashlib
            url_hash = hashlib.md5(video_url.encode('utf-8')).hexdigest()
            
            # 🔑 统一存储目录: storage/{url_hash}/
            storage_root = os.path.join(os.path.dirname(__file__), "storage")
            task_dir = os.path.join(storage_root, url_hash)
            os.makedirs(task_dir, exist_ok=True)
            
            # 视频固定命名为 video (VideoProcessor会自动添加扩展名)
            video_filename = "video"
            
            # 使用 VideoProcessor 下载
            downloader = VideoProcessor()
            video_path = await asyncio.to_thread(
                downloader.download,
                url=video_url,
                output_dir=task_dir,
                filename=video_filename
            )
            
            # 获取视频时长
            duration_sec = self._get_video_duration(video_path)
            file_size = os.path.getsize(video_path)
            
            logger.info(f"[{task_id}] Video saved to: {video_path}")
            
            return video_processing_pb2.DownloadResponse(
                success=True,
                video_path=video_path,
                file_size_bytes=file_size,
                duration_sec=duration_sec,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] DownloadVideo failed: {e}")
            return video_processing_pb2.DownloadResponse(
                success=False,
                video_path="",
                file_size_bytes=0,
                duration_sec=0.0,
                error_msg=str(e)
            )
        finally:
            self._decrement_tasks()
    
    async def TranscribeVideo(self, request, context):
        """步骤2: Whisper 转录"""
        task_id = request.task_id
        video_path = request.video_path
        language = request.language or "zh"
        
        logger.info(f"[{task_id}] TranscribeVideo: {video_path}")
        
        try:
            self._increment_tasks()
            
            # 🔑 统一存储: 使用视频所在目录作为输出目录
            # video_path 已经是绝对路径，所以 dirname 也是绝对路径
            output_dir = os.path.dirname(video_path)
            
            # 确保目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 🔑 检查是否已存在字幕文件（缓存复用）
            subtitle_path = os.path.join(output_dir, "subtitles.txt")
            
            if os.path.exists(subtitle_path):
                # 复用已有字幕
                with open(subtitle_path, "r", encoding="utf-8") as f:
                    subtitle_text = f.read()
                logger.info(f"[{task_id}] ✅ Reusing existing subtitles: {subtitle_path}")
            else:
                # 🔑 使用全局单例 Transcriber
                transcriber = self.resources.transcriber
                if not transcriber:
                    raise RuntimeError("Global Transcriber not initialized")
                
                # transcribe 是异步方法
                subtitle_text = await transcriber.transcribe(video_path)
                
                # 🔑 保存字幕文件为 subtitles.txt
                with open(subtitle_path, "w", encoding="utf-8") as f:
                    f.write(subtitle_text)
                
                logger.info(f"[{task_id}] Subtitles saved to: {subtitle_path}")
            
            return video_processing_pb2.TranscribeResponse(
                success=True,
                subtitle_path=subtitle_path,
                subtitle_text=subtitle_text[:100] + "..." if len(subtitle_text) > 100 else subtitle_text, 
                segments=[],
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] TranscribeVideo failed: {e}")
            return video_processing_pb2.TranscribeResponse(
                success=False,
                subtitle_path="",
                subtitle_text="",
                segments=[],
                error_msg=str(e)
            )
        finally:
            self._decrement_tasks()
    
    async def ProcessStage1(self, request, context):
        """步骤3: Stage1 处理"""
        task_id = request.task_id
        video_path = os.path.abspath(request.video_path) # Convert to absolute path immediately
        subtitle_path = os.path.abspath(request.subtitle_path) # Convert to absolute path immediately
        max_step = request.max_step or 24
        
        # 🔑 统一存储: 使用视频所在目录作为输出目录
        # video_path 已经是绝对路径，所以 dirname 也是绝对路径
        output_dir = os.path.dirname(video_path)
        intermediates_dir = os.path.join(output_dir, "intermediates")
        
        # 确保目录存在
        os.makedirs(intermediates_dir, exist_ok=True)
        
        # 输出文件路径
        step2_path = os.path.join(intermediates_dir, "step2_correction_output.json")
        step6_path = os.path.join(intermediates_dir, "step6_merge_cross_output.json")
        
        logger.info(f"[{task_id}] ProcessStage1: max_step={max_step}, output_dir={output_dir}")
        
        try:
            self._increment_tasks()
            
            # 🔑 检查是否已存在输出文件（缓存复用）
            if os.path.exists(step2_path) and os.path.exists(step6_path):
                logger.info(f"[{task_id}] ✅ Reusing existing Stage1 outputs")
            else:
                # 🔑 调用 Stage1 Pipeline (支持 max_step)
                await run_pipeline(
                   video_path=video_path,
                   subtitle_path=subtitle_path,
                   output_dir=output_dir,
                   max_step=max_step
                )
            
            return video_processing_pb2.Stage1Response(
                success=True,
                step2_json_path=step2_path,
                step6_json_path=step6_path,
                sentence_timestamps_path=os.path.join(output_dir, "sentence_timestamps.json"),
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] ProcessStage1 failed: {e}")
            return video_processing_pb2.Stage1Response(
                success=False,
                step2_json_path="",
                step6_json_path="",
                sentence_timestamps_path="",
                error_msg=str(e)
            )
        finally:
            self._decrement_tasks()
    
    async def AnalyzeSemanticUnits(self, request, context):
        """
        🔑 V2 步骤4: Phase2A - 语义分析 + 时间戳提取
        
        不执行 FFmpeg，只收集素材需求
        """
        import os  # Explicit local import
        import sys
        print(f"DEBUG: Entering AnalyzeSemanticUnits with os={os} and sys.modules.get('os')={sys.modules.get('os')}", flush=True)
        task_id = request.task_id
        video_path = os.path.abspath(request.video_path) # Convert to absolute path immediately
        step2_json_path = os.path.abspath(request.step2_json_path) if request.step2_json_path else "" # Convert to absolute path immediately
        step6_json_path = os.path.abspath(request.step6_json_path) if request.step6_json_path else "" # Convert to absolute path immediately
        
        # 🔑 统一存储: 使用视频所在目录，并补全为绝对路径
        output_dir = os.path.dirname(video_path)
        semantic_units_path = os.path.join(output_dir, "semantic_units_phase2a.json")
        
        logger.info(f"[{task_id}] AnalyzeSemanticUnits (Phase2A), output_dir={output_dir}")
        
        try:
            self._increment_tasks()
            
            # 🔑 检查是否已存在 Phase2A 输出（缓存复用）
            if os.path.exists(semantic_units_path):
                logger.info(f"[{task_id}] ✅ Reusing existing Phase2A output: {semantic_units_path}")
                
                # 从已有文件中提取 screenshot 和 clip 请求
                import json
                with open(semantic_units_path, "r", encoding="utf-8") as f:
                    cached_data = json.load(f)
                
                pb_screenshots = []
                pb_clips = []
                
                # 解析 semantic_units 提取素材需求
                # JSON 格式是一个列表，每个单元有 material_requests.screenshot_requests 和 material_requests.clip_requests
                semantic_units = cached_data if isinstance(cached_data, list) else cached_data.get("semantic_units", [])
                
                for su in semantic_units:
                    material_reqs = su.get("material_requests", {})
                    unit_id = su.get("unit_id", "")
                    
                    # 获取 screenshots
                    for ss in material_reqs.get("screenshot_requests", []):
                        pb_screenshots.append(video_processing_pb2.ScreenshotRequest(
                            screenshot_id=ss.get("screenshot_id", f"ss_{unit_id}"),
                            timestamp_sec=ss.get("timestamp_sec", 0.0),
                            label=ss.get("label", ""),
                            semantic_unit_id=ss.get("semantic_unit_id", unit_id)
                        ))
                    
                    # 获取 clips
                    for clip in material_reqs.get("clip_requests", []):
                        pb_clips.append(video_processing_pb2.ClipRequest(
                            clip_id=clip.get("clip_id", f"clip_{unit_id}"),
                            start_sec=clip.get("start_sec", 0.0),
                            end_sec=clip.get("end_sec", 0.0),
                            knowledge_type=clip.get("knowledge_type", ""),
                            semantic_unit_id=clip.get("semantic_unit_id", unit_id)
                        ))
                
                logger.info(f"[{task_id}] Loaded from cache: {len(pb_screenshots)} screenshots, {len(pb_clips)} clips")
                
                return video_processing_pb2.AnalyzeResponse(
                    success=True,
                    screenshot_requests=pb_screenshots,
                    clip_requests=pb_clips,
                    semantic_units_json_path=semantic_units_path,
                    error_msg=""
                )
            
            # 🔑 统一存储: 使用视频所在目录作为输出目录，并补全为绝对路径
            output_dir = os.path.dirname(video_path) # video_path is already absolute
            semantic_units_path = os.path.join(output_dir, "semantic_units_phase2a.json")
            
            # 确保目录存在
            os.makedirs(output_dir, exist_ok=True)
            
            # 🔑 创建 RichTextPipeline (使用正确的构造函数签名)
            pipeline = RichTextPipeline(
                video_path=video_path,
                step2_path=step2_json_path,
                step6_path=step6_json_path,
                output_dir=output_dir
            )
            
            # 🚀 注入视觉提取器，使 Phase2A 能够执行视觉打分推荐最佳时间戳
            visual_extractor = VisualFeatureExtractor(video_path)
            pipeline.set_visual_extractor(visual_extractor)
            
            # 🔑 调用 Phase2A: analyze_only
            screenshot_requests, clip_requests, semantic_units_path = await pipeline.analyze_only()
            
            # 转换为 protobuf 格式
            pb_screenshots = [
                video_processing_pb2.ScreenshotRequest(
                    screenshot_id=r.screenshot_id,
                    timestamp_sec=r.timestamp_sec,
                    label=r.label,
                    semantic_unit_id=r.semantic_unit_id
                )
                for r in screenshot_requests
            ]
            
            pb_clips = [
                video_processing_pb2.ClipRequest(
                    clip_id=r.clip_id,
                    start_sec=r.start_sec,
                    end_sec=r.end_sec,
                    knowledge_type=r.knowledge_type,
                    semantic_unit_id=r.semantic_unit_id
                )
                for r in clip_requests
            ]
            
            return video_processing_pb2.AnalyzeResponse(
                success=True,
                screenshot_requests=pb_screenshots,
                clip_requests=pb_clips,
                semantic_units_json_path=semantic_units_path,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] AnalyzeSemanticUnits failed: {e}")
            logger.exception(e)  # Log full traceback
            return video_processing_pb2.AnalyzeResponse(
                success=False,
                screenshot_requests=[],
                clip_requests=[],
                semantic_units_json_path="",
                error_msg=str(e)
            )
        finally:
            self._decrement_tasks()

    async def ClassifyKnowledgeBatch(self, request, context):
        """
        🚀 V3: Phase2A - Step 2: Knowledge Classification (Parallel)
        """
        task_id = request.task_id
        try:
            self._increment_tasks()
            classifier = self.resources.knowledge_classifier
            
            if not classifier:
                return video_processing_pb2.KnowledgeClassificationResponse(
                    success=False, error_msg="KnowledgeClassifier not initialized"
                )
            
            async def process_unit(u):
                action_segments = [
                    {"start": au.start_sec, "end": au.end_sec, "id": au.id} 
                    for au in u.action_units
                ]
                subtitles = [
                    {"start_sec": s.start_sec, "end_sec": s.end_sec, "corrected_text": s.text} 
                    for s in u.subtitles
                ]
                
                try:
                    batch_results = await classifier.classify_batch(
                        semantic_unit_title=u.title,
                        semantic_unit_text=u.text,
                        action_segments=action_segments,
                        subtitles=subtitles
                    )
                    
                    unit_results_proto = []
                    for i, res in enumerate(batch_results):
                        if i >= len(action_segments): break
                        action_id = action_segments[i]["id"]
                        unit_results_proto.append(video_processing_pb2.KnowledgeClassificationResult(
                            unit_id=u.unit_id,
                            action_id=action_id,
                            knowledge_type=res.get("knowledge_type", "过程性知识"),
                            confidence=res.get("confidence", 0.5),
                            key_evidence=res.get("key_evidence", ""),
                            reasoning=res.get("reasoning", "")
                        ))
                    return unit_results_proto
                except Exception as e:
                    logger.error(f"Unit {u.unit_id} classification failed: {e}")
                    return []

            tasks = [process_unit(u) for u in request.units]
            all_unit_results = await asyncio.gather(*tasks)
            flat_results = [r for sublist in all_unit_results for r in sublist]
            
            return video_processing_pb2.KnowledgeClassificationResponse(
                success=True, results=flat_results, error_msg=""
            )
        except Exception as e:
            logger.error(f"[{task_id}] ClassifyKnowledgeBatch failed: {e}")
            return video_processing_pb2.KnowledgeClassificationResponse(success=False, error_msg=str(e))
        finally:
            self._decrement_tasks()

    async def GenerateMaterialRequests(self, request, context):
        """
        🚀 V3: Phase2A - Step 3: Material Request Generation (Smart Timestamp)
        
        基于 CV 验证结果 (action_units) 和知识分类结果 (knowledge_type) 策划素材。
        """
        task_id = request.task_id
        video_path = request.video_path
        
        try:
            self._increment_tasks()
            # 获取该视频专用的选择器
            selector = self.resources.get_screenshot_selector(video_path)
            
            async def process_unit_requests(u):
                pb_ss_list = []
                pb_clip_list = []
                
                # 1. 检查是否有动作单元 (CV 探测结果)
                if u.action_units:
                    for i, au in enumerate(u.action_units):
                        # 判断该动作单元是否为讲解型 (基于分类结果)
                        k_type = au.knowledge_type or u.knowledge_type or "过程性知识"
                        is_explainable = any(k in k_type for k in ["概念", "原理", "定义", "背景", "讲解", "Concept", "Principle"])
                        
                        # 讲解型策略: 仅截图 (首中尾)
                        if is_explainable:
                            # 首帧
                            ss_head = await selector.select_screenshot(
                                video_path=video_path,
                                start_sec=float(au.start_sec),
                                end_sec=min(float(au.end_sec), float(au.start_sec) + 1.5),
                                output_name=f"{u.unit_id}_au{i}_head"
                            )
                            if ss_head:
                                pb_ss_list.append(video_processing_pb2.ScreenshotRequest(
                                    screenshot_id=ss_head.screenshot_id if hasattr(ss_head, 'screenshot_id') else f"{u.unit_id}_au{i}_head",
                                    timestamp_sec=ss_head.selected_timestamp,
                                    label="动作首帧", semantic_unit_id=u.unit_id
                                ))
                            # 中间关键帧 (Use full range to find best frame)
                            ss_mid = await selector.select_screenshot(
                                video_path=video_path,
                                start_sec=float(au.start_sec),
                                end_sec=float(au.end_sec),
                                output_name=f"{u.unit_id}_au{i}_mid"
                            )
                            if ss_mid:
                                pb_ss_list.append(video_processing_pb2.ScreenshotRequest(
                                    screenshot_id=ss_mid.screenshot_id if hasattr(ss_mid, 'screenshot_id') else f"{u.unit_id}_au{i}_mid",
                                    timestamp_sec=ss_mid.selected_timestamp,
                                    label="动作关键帧", semantic_unit_id=u.unit_id
                                ))
                        # 过程型策略: 提取视频 + 关键帧截图
                        else:
                            # 视频切片
                            pb_clip_list.append(video_processing_pb2.ClipRequest(
                                clip_id=f"clip_{u.unit_id}_au{i}",
                                start_sec=float(au.start_sec),
                                end_sec=float(au.end_sec),
                                knowledge_type=k_type,
                                semantic_unit_id=u.unit_id
                            ))
                            # 配套关键帧截图 (Use full range)
                            ss_action = await selector.select_screenshot(
                                video_path=video_path,
                                start_sec=float(au.start_sec),
                                end_sec=float(au.end_sec),
                                output_name=f"{u.unit_id}_au{i}_action"
                            )
                            if ss_action:
                                pb_ss_list.append(video_processing_pb2.ScreenshotRequest(
                                    screenshot_id=ss_action.screenshot_id if hasattr(ss_action, 'screenshot_id') else f"{u.unit_id}_au{i}_action",
                                    timestamp_sec=ss_action.selected_timestamp,
                                    label="动作演示帧", semantic_unit_id=u.unit_id
                                ))
                
                # 2. 回退策略 (无动作单元)
                else:
                    k_type = u.knowledge_type or "过程性知识"
                    is_explainable = any(k in k_type for k in ["概念", "原理", "定义", "背景", "讲解", "Concept", "Principle"])
                    
                    if is_explainable:
                        # 纯截图
                        ss_fallback = await selector.select_screenshot(
                            video_path=video_path,
                            start_sec=float(u.start_sec),
                            end_sec=min(float(u.end_sec), float(u.start_sec) + 2.0),
                            output_name=f"{u.unit_id}_fallback"
                        )
                        if ss_fallback:
                            pb_ss_list.append(video_processing_pb2.ScreenshotRequest(
                                screenshot_id=ss_fallback.screenshot_id if hasattr(ss_fallback, 'screenshot_id') else f"{u.unit_id}_fallback",
                                timestamp_sec=ss_fallback.selected_timestamp,
                                label="背景截图", semantic_unit_id=u.unit_id
                            ))
                    else:
                        # 整个单元作为视频
                        pb_clip_list.append(video_processing_pb2.ClipRequest(
                            clip_id=f"clip_{u.unit_id}_full",
                            start_sec=u.start_sec,
                            end_sec=u.end_sec,
                            knowledge_type=k_type,
                            semantic_unit_id=u.unit_id
                        ))
                
                return pb_ss_list, pb_clip_list

            tasks = [process_unit_requests(u) for u in request.units]
            all_results = await asyncio.gather(*tasks)
            
            final_ss = []
            final_clips = []
            for ss_list, clip_list in all_results:
                final_ss.extend(ss_list)
                final_clips.extend(clip_list)
                
            # 🚀 V9.0: 更新 semantic_units_phase2a.json 包含完整的素材信息
            try:
                output_dir = os.path.dirname(video_path)
                semantic_units_path = os.path.join(output_dir, "semantic_units_phase2a.json")
                
                if os.path.exists(semantic_units_path):
                    import json
                    with open(semantic_units_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # 构建 unit_id -> 素材请求映射
                    unit_ss_map = {}
                    unit_clip_map = {}
                    unit_action_map = {}
                    
                    for ss in final_ss:
                        unit_id = ss.semantic_unit_id
                        if unit_id not in unit_ss_map:
                            unit_ss_map[unit_id] = []
                        unit_ss_map[unit_id].append({
                            "screenshot_id": ss.screenshot_id,
                            "timestamp_sec": ss.timestamp_sec,
                            "label": ss.label,
                            "semantic_unit_id": ss.semantic_unit_id
                        })
                    
                    for clip in final_clips:
                        unit_id = clip.semantic_unit_id
                        if unit_id not in unit_clip_map:
                            unit_clip_map[unit_id] = []
                        unit_clip_map[unit_id].append({
                            "clip_id": clip.clip_id,
                            "start_sec": clip.start_sec,
                            "end_sec": clip.end_sec,
                            "knowledge_type": clip.knowledge_type,
                            "semantic_unit_id": clip.semantic_unit_id
                        })
                    
                    # 从请求中提取 action_units 信息
                    for u in request.units:
                        if u.action_units:
                            unit_action_map[u.unit_id] = [
                                {
                                    "id": i,
                                    "start_sec": au.start_sec,
                                    "end_sec": au.end_sec,
                                    "action_type": au.action_type,
                                    "knowledge_type": au.knowledge_type,
                                    "confidence": au.confidence,
                                    "reasoning": au.reasoning if hasattr(au, 'reasoning') else ""
                                }
                                for i, au in enumerate(u.action_units)
                            ]
                    
                    # 更新 JSON 数据
                    for item in data:
                        unit_id = item.get("unit_id", "")
                        
                        # 更新素材请求
                        if "material_requests" not in item:
                            item["material_requests"] = {}
                        item["material_requests"]["screenshot_requests"] = unit_ss_map.get(unit_id, [])
                        item["material_requests"]["clip_requests"] = unit_clip_map.get(unit_id, [])
                        
                        # 更新 action_units
                        if unit_id in unit_action_map:
                            item["action_units"] = unit_action_map[unit_id]
                        
                        # 标记 CV 验证完成
                        item["cv_validated"] = True
                    
                    # 保存更新后的 JSON
                    with open(semantic_units_path, 'w', encoding='utf-8') as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    
                    logger.info(f"[{task_id}] Updated semantic_units_phase2a.json with {len(final_ss)} screenshots, {len(final_clips)} clips")
            except Exception as e:
                logger.warning(f"[{task_id}] Failed to update semantic_units_phase2a.json: {e}")
            
            return video_processing_pb2.GenerateMaterialRequestsResponse(
                success=True,
                screenshot_requests=final_ss,
                clip_requests=final_clips,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] GenerateMaterialRequests failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return video_processing_pb2.GenerateMaterialRequestsResponse(success=False, error_msg=str(e))
        finally:
            self._decrement_tasks()
    
    async def AssembleRichText(self, request, context):
        """
        🔑 V2 步骤6: Phase2B - Vision AI 验证 + 富文本组装
        
        使用 Java FFmpeg 生成的截图和切片
        """
        task_id = request.task_id
        semantic_units_json_path = os.path.abspath(request.semantic_units_json_path) # Convert to absolute path immediately
        screenshots_dir = os.path.abspath(request.screenshots_dir) # Convert to absolute path immediately
        clips_dir = os.path.abspath(request.clips_dir) # Convert to absolute path immediately
        video_path = os.path.abspath(request.video_path) # Convert to absolute path immediately
        title = request.title or "视频内容"
        
        # 🔑 检查还补全输出目录为绝对路径 (解决 Permission denied: '.' 问题)
        output_dir = request.output_dir
        if not output_dir or output_dir == ".":
            output_dir = os.path.dirname(video_path) # video_path is already absolute
        else:
            output_dir = os.path.abspath(output_dir)
        
        # 确保目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"[{task_id}] AssembleRichText (Phase2B)")
        logger.info(f"  → video_path: {video_path}")
        logger.info(f"  → semantic_units_json_path: {semantic_units_json_path}")
        logger.info(f"  → screenshots_dir: {screenshots_dir}")
        logger.info(f"  → clips_dir: {clips_dir}")
        logger.info(f"  → output_dir: {output_dir}")
        
        try:
            self._increment_tasks()
            
            # 🔑 创建 RichTextPipeline
            # 注意: Phase2B 主要使用 semantic_units_json，step2/step6 在 Phase2A 已处理
            # 此处使用占位值，实际逻辑在 assemble_only 中加载 semantic_units_json
            pipeline = RichTextPipeline(
                video_path=video_path,
                step2_path="",  # Phase2B 不需要
                step6_path="",  # Phase2B 不需要
                output_dir=output_dir
            )
            
            # 🔑 调用 Phase2B: assemble_only
            markdown_path, json_path = await pipeline.assemble_only(
                semantic_units_json_path=semantic_units_json_path,
                screenshots_dir=screenshots_dir,
                clips_dir=clips_dir,
                title=title
            )
            
            # 统计信息
            stats = video_processing_pb2.AssembleStats(
                total_sections=0,
                video_clips_count=len(os.listdir(clips_dir)) if os.path.exists(clips_dir) else 0,
                screenshots_count=len(os.listdir(screenshots_dir)) if os.path.exists(screenshots_dir) else 0,
                text_only_count=0,
                vision_validated_count=0
            )
            
            return video_processing_pb2.AssembleResponse(
                success=True,
                markdown_path=markdown_path,
                json_path=json_path,
                stats=stats,
                error_msg=""
            )
            
        except Exception as e:
            import traceback # Import traceback for detailed error logging
            logger.error(f"[{task_id}] AssembleRichText failed: {e}")
            logger.error(traceback.format_exc()) # Log full traceback
            return video_processing_pb2.AssembleResponse(
                success=False,
                markdown_path="",
                json_path="",
                stats=video_processing_pb2.AssembleStats(), # Keep stats field, but initialize empty
                error_msg=str(e)
            )
        finally:
            self._decrement_tasks()
    
    def _get_video_duration(self, video_path: str) -> float:
        """获取视频时长"""
        try:
            if not os.path.exists(video_path):
                return 300.0
            import subprocess
            result = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", video_path],
                capture_output=True, text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                 return float(result.stdout.strip())
            return 300.0
        except:
            return 300.0  # 默认5分钟
    
    def _increment_tasks(self):
        with self._task_lock:
            self._active_tasks += 1
    
    def _decrement_tasks(self):
        with self._task_lock:
            self._active_tasks -= 1
            
    def _batch_read_frames_to_shm(self, video_path: str, units_data: list) -> dict:
        """
        Helper to read frames for a batch of units into SharedMemory (Thread-Safe).
        Executed in asyncio thread pool.
        """
        shm_map = {} # unit_id -> {frame_idx: shm_ref}
        import cv2
        
        # 1. Collect Requests
        frame_requests = [] # (time, unit_id)
        for u in units_data:
            uid = u['unit_id']
            # Start, Mid, End frames strategy
            start, end = u['start_sec'], u['end_sec']
            frame_requests.append((start, uid))
            frame_requests.append(((start+end)/2, uid))
            frame_requests.append((end, uid))
            
        # Sort by time to optimize seeking
        frame_requests.sort(key=lambda x: x[0])
        
        cap = cv2.VideoCapture(video_path)
        try:
            if not cap.isOpened(): return {}
            fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
            total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            
            valid_shm_refs = {}
            
            for req_time, uid in frame_requests:
                frame_idx = int(req_time * fps)
                frame_idx = max(0, min(frame_idx, total_frames - 1))
                
                if frame_idx in valid_shm_refs: continue
                
                # Seek & Read
                curr = cap.get(cv2.CAP_PROP_POS_FRAMES)
                # If frame_idx matches current, read direct. Else seek.
                if int(curr) != frame_idx:
                     cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
                
                ret, frame = cap.read()
                if ret and frame is not None:
                     # 🚀 Write to SharedMemory
                     self.frame_registry.register_frame(frame_idx, frame)
                     ref = self.frame_registry.get_shm_ref(frame_idx)
                     if ref: valid_shm_refs[frame_idx] = ref
                     
                     # 🚀 Immediate release from local heap
                     del frame
            
            # Re-map results to Unit ID
            for u in units_data:
                uid = u['unit_id']
                u_map = {}
                start, end = u['start_sec'], u['end_sec']
                for t in [start, (start+end)/2, end]:
                    idx = int(t * fps)
                    idx = max(0, min(idx, total_frames - 1))
                    if idx in valid_shm_refs:
                        u_map[str(idx)] = valid_shm_refs[idx]
                if u_map:
                    shm_map[uid] = u_map
                    
            return shm_map
        except Exception as e:
            logger.warning(f"Batch read failed: {e}")
            return {}
        finally:
            cap.release()
    
    async def ValidateCVBatch(self, request, context):
        """
        🚀 V6: CV验证 (Java 控制 + ProcessPool + SharedMemory + Chunked Processing)
        
        优化策略:
        1. Batch Reading: 分批预读帧到 SharedMemory (IO优化)
        2. ProcessPool: 提交纯计算任务 (CPU优化)
        3. Memory Guard: 每批次处理完强制 GC，防止 OOM
        """
        from cv_worker import run_cv_validation_task
        import functools
        
        task_id = request.task_id
        video_path = os.path.abspath(request.video_path)
        semantic_units = request.semantic_units
        
        logger.info(f"[{task_id}] ValidateCVBatch Start: {len(semantic_units)} units")
        
        try:
            self._increment_tasks()
            
            # 序列化所有数据
            all_units_data = []
            for u in semantic_units:
                all_units_data.append({
                    "unit_id": u.unit_id,
                    "start_sec": u.start_sec,
                    "end_sec": u.end_sec,
                    "knowledge_type": u.knowledge_type
                })
            
            # 🚀 Chunked Processing
            # [FIX] 动态计算 Batch Size
            # 基础: 50. 依据内存向上调整.
            mem = psutil.virtual_memory()
            avail_gb = mem.available / (1024**3)
            # 策略: 每 4GB 空闲内存增加 30 任务量. 
            # 16GB 空闲 -> +120 -> 170. 4GB 空闲 -> 50.
            dynamic_batch = 50 + int((max(0, avail_gb - 4) / 4) * 30)
            BATCH_SIZE = min(max(32, dynamic_batch), 200) # 限制范围 [32, 200]
            
            logger.info(f"[{task_id}] Dynamic Batch Config: Size={BATCH_SIZE} (Available RAM={avail_gb:.1f}GB)")
            results_data = []
            
            loop = asyncio.get_running_loop()
            
            for i in range(0, len(all_units_data), BATCH_SIZE):
                batch_units = all_units_data[i : i + BATCH_SIZE]
                current_batch_idx = i // BATCH_SIZE + 1
                logger.info(f"[{task_id}] Processing Batch {current_batch_idx} ({len(batch_units)} units)...")
                
                # 1. IO: Batch Read Frames (Thread Pool)
                # 使用 run_in_executor 避免阻塞 Asyncio Loop
                shm_map = await loop.run_in_executor(
                    None, 
                    self._batch_read_frames_to_shm, 
                    video_path, 
                    batch_units
                )
                
                # 2. CPU: Submit Tasks (Process Pool)
                futures = []
                for unit_data in batch_units:
                    unit_id = unit_data["unit_id"]
                    shm_frames = shm_map.get(unit_id, None)
                    
                    task_func = functools.partial(
                        run_cv_validation_task,
                        video_path,
                        unit_data,
                        shm_frames
                    )
                    futures.append(loop.run_in_executor(self.cv_process_pool, task_func))
                
                if futures:
                    # 并行等待当前批次
                    batch_results = await asyncio.gather(*futures, return_exceptions=True)
                    results_data.extend(batch_results)
                
                # 3. Memory Guard: Cleanup
                # SharedMemory 生命周期由 SharedFrameRegistry 管理 (LRU)
                # 但我们需要强制清理主进程的临时对象和碎片
                del shm_map
                del futures
                gc.collect() # 🚀 Critical for OOM prevention
            
            # 4. 构建响应
            pb_results = []
            for res in results_data:
                if isinstance(res, Exception):
                    logger.error(f"CV worker exception: {res}")
                    continue
                if not isinstance(res, dict): continue
                
                # Result Construction
                pb_islands = []
                for si in res.get("stable_islands", []):
                    if isinstance(si, dict):
                        pb_islands.append(video_processing_pb2.StableIsland(
                            start_sec=float(si.get("start_sec", 0.0)),
                            end_sec=float(si.get("end_sec", 0.0)),
                            mid_sec=float(si.get("mid_sec", 0.0)),
                            duration_sec=float(si.get("duration_sec", 0.0))
                        ))
                
                pb_actions = []
                for act in res.get("action_segments", []):
                    if isinstance(act, dict):
                        internal_islands = []
                        for Isi in act.get("internal_stable_islands", []):
                            internal_islands.append(video_processing_pb2.StableIsland(
                                start_sec=float(Isi.get("start_sec", 0.0)),
                                end_sec=float(Isi.get("end_sec", 0.0)),
                                mid_sec=float(Isi.get("mid_sec", 0.0)),
                                duration_sec=float(Isi.get("duration_sec", 0.0))
                            ))
                        
                        pb_actions.append(video_processing_pb2.ActionSegment(
                            start_sec=float(act.get("start_sec", 0.0)),
                            end_sec=float(act.get("end_sec", 0.0)),
                            action_type=str(act.get("action_type", "")),
                            internal_stable_islands=internal_islands
                        ))
                
                pb_results.append(video_processing_pb2.CVValidationResult(
                    unit_id=str(res.get("unit_id", "")),
                    modality=str(res.get("modality", "unknown")),
                    knowledge_subtype=str(res.get("knowledge_subtype", "unknown")),
                    stable_islands=pb_islands,
                    action_segments=pb_actions
                ))
            
            logger.info(f"[{task_id}] All Batches Completed. Total Results: {len(pb_results)}")
            
            return video_processing_pb2.CVValidationResponse(
                success=True,
                results=pb_results,
                error_msg=""
            )
            
        except Exception as e:
            logger.error(f"[{task_id}] ValidateCVBatch Failed: {e}")
            logger.error(traceback.format_exc())
            return video_processing_pb2.CVValidationResponse(
                success=False,
                results=[],
                error_msg=str(e)
            )

async def serve(host: str = "0.0.0.0", port: int = 50051):
    """启动 gRPC 服务器"""
    server = aio.server(
        futures.ThreadPoolExecutor(max_workers=50),
        options=[
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024)
        ]
    )
    
    # 添加服务
    servicer = VideoProcessingServicer()
    video_processing_pb2_grpc.add_VideoProcessingServiceServicer_to_server(servicer, server)
    
    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    
    logger.info(f"Starting gRPC server on {listen_addr}")
    
    await server.start()
    
    # Graceful shutdown handler
    async def shutdown():
        logger.info("Stopping server...")
        await server.stop(5)
        if hasattr(servicer, 'process_pool'):
            servicer.process_pool.shutdown()
            logger.info("Process pool shut down")
            
    try:
        await server.wait_for_termination()
    finally:
        await shutdown()


if __name__ == "__main__":
    # Windows下多进程必须在 if __name__ == "__main__" 保护下
    # 并通过 freeze_support 支持打包 (虽然这里不需要打包)
    from multiprocessing import freeze_support
    freeze_support()
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    asyncio.run(serve())
