"""
模块说明：视频转Markdown流程中的 parallel_transcription 模块。
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
import subprocess
import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from faster_whisper import WhisperModel
import json
import sys

from services.python_grpc.src.common.utils.numbers import safe_int, safe_float
from services.python_grpc.src.common.utils.time import format_hhmmss
from services.python_grpc.src.common.utils.video import get_video_duration as _get_video_duration

# 启用 HuggingFace 下载进度条
os.environ['HF_HUB_DISABLE_PROGRESS_BARS'] = '0'
os.environ['HF_HUB_ENABLE_HF_TRANSFER'] = '0'


def get_video_duration(video_path):
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过JSON 解析/序列化、子进程调用实现。
    核心价值：提供一致读取接口，降低调用耦合。
    输入参数：
    - video_path: 文件路径（类型：未标注）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    return _get_video_duration(video_path, default=0.0, use_cv2_fallback=True, raise_on_failure=True)



def split_video_segments(video_path, segment_duration=600, output_dir=None, num_workers=3, split_threshold_sec=300, config=None):
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：duration <= split_threshold_sec
    依据来源（证据链）：
    - 阈值来源：函数入参 split_threshold_sec。
    输入参数：
    - video_path: 文件路径（类型：未标注）。
    - segment_duration: 函数入参（类型：未标注）。
    - output_dir: 目录路径（类型：未标注）。
    - num_workers: 函数入参（类型：未标注）。
    输出参数：
    - 函数计算/封装后的结果对象。
    补充说明：
    策略：
    - 视频时长 <= 分段阈值：不分段，单路转录
    - 视频时长 > 分段阈值：按“基准分段 + worker目标段数 + 最小时长约束”联合切分
    video_path: 视频路径
    segment_duration: 最大分段时长（秒），默认 600 秒 = 10 分钟（作为上限参考）
    output_dir: 输出目录
    num_workers: 并行工作线程数，用于均分计算"""
    duration = get_video_duration(video_path)
    parallel_cfg = {}
    if config:
        parallel_cfg = config.get("whisper", {}).get("parallel", {})

    split_threshold_sec = max(60, safe_int(split_threshold_sec, 300))
    safe_segment_duration = max(60, safe_int(segment_duration, 600))
    target_segments_per_worker = max(1, safe_int(parallel_cfg.get("segments_per_worker", 2), 2))
    min_segment_duration_sec = max(30, safe_int(parallel_cfg.get("min_segment_duration_sec", 90), 90))
    max_segment_count_cfg = max(0, safe_int(parallel_cfg.get("max_segment_count", 0), 0))

    segments = []

    if duration <= split_threshold_sec:
        # 短视频：不分段
        segments.append({
            'id': 0,
            'start': 0,
            'end': duration,
            'duration': duration
        })
        print(
            f"[分段策略] 视频 {duration/60:.1f} 分钟 <= {split_threshold_sec/60:.1f} 分钟，不分段处理"
        )
    else:
        # 长视频：优先保证线程池“有活干”，避免 worker 提前空闲
        # 1) 基准分段：按 segment_duration 切分
        base_count = max(1, int(math.ceil(duration / safe_segment_duration)))
        # 2) 目标分段：每个 worker 至少 target_segments_per_worker 段，提升负载均衡
        target_count = max(1, safe_int(num_workers, 1)) * target_segments_per_worker
        # 3) 最小时长约束：避免分段过细导致 ffmpeg/调度开销占比过高
        max_count_by_min_duration = max(1, int(duration // min_segment_duration_sec))

        segment_count = max(base_count, target_count)
        segment_count = min(segment_count, max_count_by_min_duration)
        if max_segment_count_cfg > 0:
            segment_count = min(segment_count, max_segment_count_cfg)
        segment_count = max(1, segment_count)
        segment_length = duration / segment_count

        for i in range(segment_count):
            start = i * segment_length
            end = min((i + 1) * segment_length, duration)
            segments.append({
                'id': i,
                'start': start,
                'end': end,
                'duration': end - start
            })

        print(
            f"[分段策略] 视频 {duration/60:.1f} 分钟 > {split_threshold_sec/60:.1f} 分钟，"
            f"基准分段={base_count}, 目标分段={target_count}, 最小时长约束上限={max_count_by_min_duration}, "
            f"最终切分={segment_count} 段"
        )

    return segments


def build_parallel_plan(requested_workers, segment_count, device="cpu", config=None):
    """
    执行逻辑：
    1) 基于配置与机器资源估算并发上限。
    2) 输出线程池/进程池可执行计划。
    实现方式：CPU 核心预算 + 可用内存预算双重约束。
    核心价值：避免过度并发导致抢占、抖动和 OOM。
    输入参数：
    - requested_workers: 请求并发数（类型：未标注）。
    - segment_count: 分段任务数（类型：未标注）。
    - device: 设备类型（类型：未标注）。
    - config: 配置对象/字典（类型：未标注）。
    输出参数：
    - 并发计划字典。"""
    parallel_cfg = {}
    if config:
        parallel_cfg = config.get("whisper", {}).get("parallel", {})

    requested_workers = max(1, safe_int(requested_workers, 3))
    segment_count = max(1, safe_int(segment_count, 1))
    task_cap = min(requested_workers, segment_count)

    total_cores = os.cpu_count() or 1
    reserve_cpu_cores = max(0, safe_int(parallel_cfg.get("reserve_cpu_cores", 1), 1))
    reserve_cpu_ratio = safe_float(parallel_cfg.get("reserve_cpu_ratio", 0.1), 0.1)
    reserve_cpu_ratio = min(max(reserve_cpu_ratio, 0.0), 0.8)
    cpu_budget_by_count = max(1, total_cores - reserve_cpu_cores)
    cpu_budget_by_ratio = max(1, int(total_cores * (1 - reserve_cpu_ratio)))
    cpu_budget = min(cpu_budget_by_count, cpu_budget_by_ratio)
    cpu_worker_cap = max(1, cpu_budget)

    available_mem_gb = None
    reserve_memory_gb = max(0.0, safe_float(parallel_cfg.get("reserve_memory_gb", 2.0), 2.0))
    memory_per_worker_gb = max(0.1, safe_float(parallel_cfg.get("memory_per_worker_gb", 1.5), 1.5))
    memory_worker_cap = task_cap
    try:
        import psutil
        available_mem_gb = psutil.virtual_memory().available / (1024 ** 3)
        if available_mem_gb <= reserve_memory_gb:
            memory_worker_cap = 1
        else:
            memory_worker_cap = max(1, int((available_mem_gb - reserve_memory_gb) / memory_per_worker_gb))
    except Exception:
        # psutil 不可用时退化为仅按 CPU 约束
        memory_worker_cap = task_cap

    auto_schedule = bool(parallel_cfg.get("auto_resource_scheduling", True))
    gpu_worker_cap = max(1, safe_int(parallel_cfg.get("gpu_max_workers", 1), 1))

    if auto_schedule:
        if device == "cpu":
            effective_workers = max(1, min(task_cap, cpu_worker_cap, memory_worker_cap))
        else:
            effective_workers = max(1, min(task_cap, gpu_worker_cap, memory_worker_cap))
    else:
        effective_workers = max(1, task_cap)

    cpu_threads_per_worker = max(1, cpu_budget // effective_workers)

    return {
        "effective_workers": effective_workers,
        "cpu_threads_per_worker": cpu_threads_per_worker,
        "total_cores": total_cores,
        "cpu_budget": cpu_budget,
        "available_mem_gb": available_mem_gb,
        "memory_worker_cap": memory_worker_cap,
        "requested_workers": requested_workers,
        "segment_count": segment_count,
        "auto_schedule": auto_schedule,
    }


# 进程级模型缓存：每个 Worker 进程只加载一次模型，后续段复用
_worker_model = None
_worker_model_key = None


def _get_or_load_model(model_path, device, compute_type, cpu_threads):
    """进程内模型缓存：同一 Worker 处理多段时仅加载一次模型。"""
    global _worker_model, _worker_model_key
    key = (model_path, device, compute_type, cpu_threads)
    if _worker_model is not None and _worker_model_key == key:
        print(f"[进程 {os.getpid()}] ⚡ 复用已加载模型", flush=True)
        return _worker_model
    print(f"[进程 {os.getpid()}] 正在加载 Whisper 模型: {model_path} (Threads={cpu_threads})", flush=True)
    _worker_model = WhisperModel(model_path, device=device, compute_type=compute_type, cpu_threads=cpu_threads)
    _worker_model_key = key
    return _worker_model


def transcribe_segment(args):
    """
    单段转录函数，在子进程中执行。
    每个 Worker 进程通过 _get_or_load_model 缓存模型实例，
    同一 Worker 处理多段时只加载一次模型。
    输入：args 元组 (video_path, segment, model_path, device, compute_type, language, cpu_threads)
    输出：dict { segment_id, subtitles, success }
    """
    # Unpack args
    video_path, segment, model_path, device, compute_type, language, cpu_threads = args
    
    try:
        # 显示进度
        print(f"\n{'='*60}", flush=True)
        print(f"[进程 {os.getpid()}] 开始处理段 {segment['id']+1}", flush=True)
        print(f"  时间范围: {segment['start']:.0f}s - {segment['end']:.0f}s ({segment['duration']:.0f}s)", flush=True)
        print(f"{'='*60}\n", flush=True)
        
        # 获取或加载模型实例（进程级缓存，同一 Worker 复用）
        model = _get_or_load_model(model_path, device, compute_type, cpu_threads)
        
        # 临时提取音频片段
        temp_audio = f"temp_segment_{os.getpid()}_{segment['id']}.wav"
        
        print(f"[进程 {os.getpid()}] 提取音频片段...", flush=True)
        # 使用 ffmpeg 提取音频片段
        cmd = [
            'ffmpeg',
            '-i', video_path,
            '-ss', str(segment['start']),
            '-t', str(segment['duration']),
            '-vn',  # 不要视频
            '-acodec', 'pcm_s16le',
            '-ar', '16000',
            '-ac', '1',
            '-y',
            '-loglevel', 'error',  # 减少 ffmpeg 输出
            temp_audio
        ]
        
        subprocess.run(cmd, capture_output=True, check=True)
        print(f"[进程 {os.getpid()}] ✓ 音频提取完成", flush=True)
        
        # ת¼
        print(f"[进程 {os.getpid()}] 开始转录...", flush=True)
        segments_result, info = model.transcribe(
            temp_audio,
            language=language,
            beam_size=5,
            vad_filter=True
        )
        
        # 收集结果并调整时间戳
        subtitles = []
        for seg in segments_result:
            subtitles.append({
                'start': seg.start + segment['start'],
                'end': seg.end + segment['start'],
                'text': seg.text.strip()
            })
        
        # 清理临时文件
        if os.path.exists(temp_audio):
            os.remove(temp_audio)
        
        print(f"[进程 {os.getpid()}] ✓ 段 {segment['id']+1} 转录完成 ({len(subtitles)} 条字幕)", flush=True)
        
        return {
            'segment_id': segment['id'],
            'subtitles': subtitles,
            'success': True
        }
        
    except Exception as e:
        # 清理临时文件
        temp_audio = f"temp_segment_{os.getpid()}_{segment['id']}.wav"
        if os.path.exists(temp_audio):
            os.remove(temp_audio)
        
        print(f"[进程 {os.getpid()}] ✗ 段 {segment['id']+1} 失败: {e}", flush=True)
        import traceback
        traceback.print_exc()
        
        return {
            'segment_id': segment['id'],
            'error': str(e),
            'success': False
        }


def transcribe_parallel(video_path, model_size="small", device="cpu", 
                       compute_type="int8", language="zh",
                       segment_duration=600, num_workers=3, hf_endpoint=None, config=None):
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过线程池并发、进程池并发、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：config
    - 条件：len(segments) == 1
    - 条件：device == 'cpu'
    依据来源（证据链）：
    - 输入参数：config, device。
    - 配置字段：success。
    输入参数：
    - video_path: 文件路径（类型：未标注）。
    - model_size: 模型/推理配置（类型：未标注）。
    - device: 函数入参（类型：未标注）。
    - compute_type: 函数入参（类型：未标注）。
    - language: 函数入参（类型：未标注）。
    - segment_duration: 函数入参（类型：未标注）。
    - num_workers: 函数入参（类型：未标注）。
    - hf_endpoint: 起止时间/区间边界（类型：未标注）。
    - config: 配置对象/字典（类型：未标注）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    print(f"\n[并行转录] 开始处理: {os.path.basename(video_path)}")
    print(f"[并行转录] 模块校验: file={__file__}", flush=True)
    
    # 0. 显式下载模型（主进程先下载/通过校验）
    from .model_downloader import download_whisper_model
    
    use_mirror = True
    proxy = None
    if config:
        w_cfg = config.get("whisper", {})
        use_mirror = w_cfg.get("use_mirror", True)
        proxy = w_cfg.get("download_proxy")
        
    model_path = download_whisper_model(
        model_size, 
        hf_endpoint=hf_endpoint,
        use_mirror=use_mirror,
        proxy=proxy
    )
    
    parallel_cfg = {}
    if config:
        parallel_cfg = config.get("whisper", {}).get("parallel", {})

    # 若 config 中声明，则配置优先（保证统一入口可控）
    if "num_workers" in parallel_cfg:
        num_workers = safe_int(parallel_cfg.get("num_workers"), num_workers)
    if "segment_duration" in parallel_cfg:
        segment_duration = safe_int(parallel_cfg.get("segment_duration"), segment_duration)
    split_threshold_sec = safe_int(parallel_cfg.get("segment_split_threshold_sec", 300), 300)
    print(
        f"[并行转录] 参数校验: threshold={split_threshold_sec}s, "
        f"segment_duration={segment_duration}s, requested_workers={num_workers}, auto_schedule={parallel_cfg.get('auto_resource_scheduling', True)}",
        flush=True,
    )

    # 1. 预规划并发（不受任务段数限制），得到“资源可承载并发”
    preplan_virtual_segments = max(num_workers * 4, 16)
    pre_plan = build_parallel_plan(
        num_workers,
        preplan_virtual_segments,
        device=device,
        config=config,
    )
    segment_planning_workers = pre_plan["effective_workers"]

    # 2. 分割视频段（阈值可配置 + 以资源可承载并发为目标生成足够任务）
    print(f"[并行转录] 分析视频时长...")
    segments = split_video_segments(
        video_path,
        segment_duration=segment_duration,
        num_workers=segment_planning_workers,
        split_threshold_sec=split_threshold_sec,
        config=config,
    )

    # 3. 基于真实任务段数计算最终并发
    plan = build_parallel_plan(num_workers, len(segments), device=device, config=config)
    effective_workers = plan["effective_workers"]
    cpu_threads_per_worker = plan["cpu_threads_per_worker"]
    
    if len(segments) == 1:
        print(f"[并行转录] 短视频模式，不分段处理")
    else:
        print(f"[并行转录] 分割为 {len(segments)} 段，并行Worker {effective_workers}")

    # 4. 资源规划结果（线程池/进程池复用同一计划）
    mem_text = "未知"
    if plan["available_mem_gb"] is not None:
        mem_text = f"{plan['available_mem_gb']:.1f}GB"
    print(
        f"[并行转录] 资源规划: 请求Worker={plan['requested_workers']}, "
        f"预估Worker={segment_planning_workers}, "
        f"任务段数={plan['segment_count']}, "
        f"生效Worker={effective_workers}, "
        f"CPU预算={plan['cpu_budget']}/{plan['total_cores']}核, "
        f"可用内存={mem_text}, "
        f"每Worker线程={cpu_threads_per_worker}"
    )

    # 3. 执行并行转录
    all_subtitles = []
    completed = 0
    
    # CPU / GPU 统一使用 ProcessPoolExecutor（真正多进程并行）
    if device == "cpu":
        print(f"[并行转录] 检测到 CPU 模式，采用多进程池 (ProcessPoolExecutor, workers={effective_workers})")
        print(f"[并行转录] 每个Worker将独立加载模型 (Threads={cpu_threads_per_worker})")
    else:
        print(f"[并行转录] 检测到 {device} 模式，采用多进程池 (ProcessPoolExecutor, workers={effective_workers})")

    # 所有模式统一：传递 model_path (字符串，可序列化)，每个子进程独立加载模型
    tasks_args = [
        (video_path, seg, model_path, device, compute_type, language, cpu_threads_per_worker)
        for seg in segments
    ]

    failed_tasks_args = []
    with ProcessPoolExecutor(max_workers=effective_workers) as executor:
        futures = {executor.submit(transcribe_segment, args): args for args in tasks_args}
        for future in as_completed(futures):
            task_args = futures[future]
            segment = task_args[1]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    'segment_id': segment['id'],
                    'error': str(exc),
                    'success': False
                }

            if result['success']:
                all_subtitles.extend(result['subtitles'])
                completed += 1
                print(f"[并行转录] ✓ 段 {result['segment_id']+1}/{len(segments)} 完成 "
                      f"({completed}/{len(segments)})")
            else:
                failed_tasks_args.append(task_args)
                print(f"[并行转录] ✗ 段 {result['segment_id']+1} 失败: {result['error']}")

    if failed_tasks_args:
        print(f"[并行转录] 进入串行补偿: {len(failed_tasks_args)} 段")
        for task_args in failed_tasks_args:
            segment = task_args[1]
            fallback_result = transcribe_segment(task_args)
            if fallback_result['success']:
                all_subtitles.extend(fallback_result['subtitles'])
                completed += 1
                print(f"[并行转录] ✓ 段 {segment['id']+1}/{len(segments)} 串行补偿完成 "
                      f"({completed}/{len(segments)})")
            else:
                print(f"[并行转录] ✗ 段 {segment['id']+1}/{len(segments)} 串行补偿失败: {fallback_result['error']}")

    if completed == 0:
        raise RuntimeError("并行转录失败：所有分段均未成功")

    if completed < len(segments):
        failed_count = len(segments) - completed
        raise RuntimeError(f"并行转录失败：仍有 {failed_count}/{len(segments)} 个分段失败")
    
    # 4. 按时间排序
    all_subtitles.sort(key=lambda x: x['start'])
    
    # 5. 格式化为字幕文本
    subtitle_text = format_subtitles(all_subtitles)
    
    print(f"[并行转录] 完成！共 {len(all_subtitles)} 条字幕")
    
    return subtitle_text


def format_subtitles(subtitles):
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - subtitles: 数据列表/集合（类型：未标注）。
    输出参数：
    - join 对象或调用结果。"""
    lines = []
    for sub in subtitles:
        start_time = format_hhmmss(sub['start'])
        lines.append(f"[{start_time}] {sub['text']}")
    return "\n".join(lines)


