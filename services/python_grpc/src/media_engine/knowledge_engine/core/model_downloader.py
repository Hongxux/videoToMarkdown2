"""
模块说明：视频转Markdown流程中的 model_downloader 模块。
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
import json
import time
from contextlib import contextmanager
from huggingface_hub import hf_hub_download


_RETRYABLE_ERROR_KEYWORDS = (
    "server disconnected without sending a response",
    "remoteprotocolerror",
    "timeout",
    "timed out",
    "temporarily unavailable",
    "connection reset",
    "connection aborted",
    "connection refused",
    "network is unreachable",
    "name or service not known",
    "max retries exceeded",
    "connection error",
    "read error",
    "broken pipe",
)


def _build_verify_state_path(cache_dir, repo_id):
    safe_repo = repo_id.replace("/", "__")
    state_dir = os.path.join(cache_dir, ".video_to_markdown")
    os.makedirs(state_dir, exist_ok=True)
    return os.path.join(state_dir, f"{safe_repo}.verify_state.json")


def _load_verify_state(cache_dir, repo_id):
    state_path = _build_verify_state_path(cache_dir, repo_id)
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_verify_state(cache_dir, repo_id, payload):
    state_path = _build_verify_state_path(cache_dir, repo_id)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _resolve_cached_files_local_only(repo_id, cache_dir, core_files):
    resolved = {}
    for filename in core_files:
        try:
            path = hf_hub_download(
                repo_id=repo_id,
                filename=filename,
                cache_dir=cache_dir,
                local_files_only=True,
            )
            resolved[filename] = path
        except Exception:
            if filename == "vocabulary.txt":
                continue
            return {}
    return resolved


def _try_reuse_preverified_model_dir(repo_id, model_size, cache_dir, core_files, essential_files):
    state = _load_verify_state(cache_dir, repo_id)
    if not state:
        return ""
    if state.get("repo_id") != repo_id:
        return ""
    if state.get("model_size") != model_size:
        return ""

    cached_paths = _resolve_cached_files_local_only(repo_id, cache_dir, core_files)
    if not cached_paths:
        return ""
    if any(name not in cached_paths for name in essential_files):
        return ""

    for name in essential_files:
        path = cached_paths.get(name, "")
        if (not path) or (not os.path.exists(path)) or os.path.getsize(path) <= 0:
            return ""
    return os.path.dirname(cached_paths["config.json"])


def _is_retryable_download_error(exc):
    message = str(exc).lower()
    return any(keyword in message for keyword in _RETRYABLE_ERROR_KEYWORDS)


def _build_endpoint_plan(use_mirror, hf_endpoint, enable_endpoint_fallback):
    plan = [
        {
            "use_mirror": bool(use_mirror),
            "hf_endpoint": hf_endpoint,
            "label": "primary",
        }
    ]
    if not enable_endpoint_fallback:
        return plan

    if use_mirror:
        plan.append(
            {
                "use_mirror": False,
                "hf_endpoint": None,
                "label": "official_fallback",
            }
        )
    else:
        plan.append(
            {
                "use_mirror": True,
                "hf_endpoint": hf_endpoint or "https://hf-mirror.com",
                "label": "mirror_fallback",
            }
        )
    return plan


def _download_with_retry(
    repo_id,
    filename,
    cache_dir,
    *,
    max_retries=3,
    retry_base_delay_sec=1.5,
    etag_timeout_sec=20,
    force_download=False,
):
    if max_retries < 1:
        max_retries = 1

    for attempt in range(1, max_retries + 1):
        try:
            return hf_hub_download(
                repo_id=repo_id,
                filename=filename,
                cache_dir=cache_dir,
                force_download=force_download,
                etag_timeout=etag_timeout_sec,
            )
        except Exception as e:
            is_retryable = _is_retryable_download_error(e)
            if (not is_retryable) or attempt >= max_retries:
                raise

            wait_sec = max(0.0, float(retry_base_delay_sec)) * (2 ** (attempt - 1))
            print(
                f"⚠️  下载失败: {filename} (第 {attempt}/{max_retries} 次) -> {e}; "
                f"{wait_sec:.1f}s 后重试",
                flush=True,
            )
            time.sleep(wait_sec)

@contextmanager
def set_hf_env(use_mirror=True, hf_endpoint=None, proxy=None):
    """
    执行逻辑：
    1) 校验输入值。
    2) 更新内部状态或持久化。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：集中更新状态，保证一致性。
    决策逻辑：
    - 条件：proxy
    - 条件：use_mirror
    - 条件：value is None
    依据来源（证据链）：
    - 输入参数：proxy, use_mirror。
    输入参数：
    - use_mirror: 函数入参（类型：未标注）。
    - hf_endpoint: 起止时间/区间边界（类型：未标注）。
    - proxy: 函数入参（类型：未标注）。
    输出参数：
    - 无（仅产生副作用，如日志/写盘/状态更新）。"""
    old_env = {
        "HF_ENDPOINT": os.environ.get("HF_ENDPOINT"),
        "HTTP_PROXY": os.environ.get("HTTP_PROXY"),
        "HTTPS_PROXY": os.environ.get("HTTPS_PROXY"),
        "http_proxy": os.environ.get("http_proxy"),
        "https_proxy": os.environ.get("https_proxy"),
    }
    
    try:
        # 1. 设置代理
        if proxy:
            os.environ["HTTP_PROXY"] = proxy
            os.environ["HTTPS_PROXY"] = proxy
            os.environ["http_proxy"] = proxy
            os.environ["https_proxy"] = proxy
            print(f"🛰️  已临时配置代理: {proxy}", flush=True)
        else:
            for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
                os.environ.pop(key, None)
        
        # 2. 设置镜像端点
        if use_mirror:
            target = hf_endpoint or "https://hf-mirror.com"
            os.environ["HF_ENDPOINT"] = target
            print(f"🌐 正在通过镜像站加速: {target}", flush=True)
        else:
            # 强制回退官方
            os.environ.pop("HF_ENDPOINT", None)
            print(f"🌍 正在连接官方 HuggingFace 枢纽", flush=True)
            
        yield
    finally:
        # 恢复环境
        for key, value in old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _resolve_whisper_cache_dir():
    """
    执行逻辑：
    1) 优先读取显式缓存目录环境变量。
    2) 若未配置，则回退到 HuggingFace 默认缓存目录。
    实现方式：按 `WHISPER_MODEL_CACHE_DIR` -> `HUGGINGFACE_HUB_CACHE` -> `HF_HOME` -> 默认路径 依次解析。
    核心价值：确保 Docker 构建期预装模型与运行期读取模型使用同一路径，避免重复下载。
    输入参数：无。
    输出参数：
    - str：Whisper/HuggingFace Hub 缓存目录。
    """
    explicit_cache = str(
        os.getenv("WHISPER_MODEL_CACHE_DIR", "")
        or os.getenv("HUGGINGFACE_HUB_CACHE", "")
    ).strip()
    if explicit_cache:
        return os.path.expanduser(explicit_cache)

    hf_home = str(os.getenv("HF_HOME", "")).strip()
    if hf_home:
        return os.path.join(os.path.expanduser(hf_home), "hub")

    return os.path.expanduser("~/.cache/huggingface/hub")

def _verify_file_integrity(file_path, filename):
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not os.path.exists(file_path)
    - 条件：size == 0
    - 条件：filename.endswith('.json')
    依据来源（证据链）：
    - 输入参数：file_path, filename。
    输入参数：
    - file_path: 文件路径（类型：未标注）。
    - filename: 函数入参（类型：未标注）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    if not os.path.exists(file_path):
        return False
        
    size = os.path.getsize(file_path)
    if size == 0:
        return False
        
    if filename.endswith(".json"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json.load(f)
        except:
            print(f"⚠️  错误: {filename} 格式损坏，将尝试重新拉取。", flush=True)
            return False
            
    return True

def download_whisper_model(
    model_size="medium",
    hf_endpoint=None,
    use_mirror=True,
    proxy=None,
    max_retries=3,
    retry_base_delay_sec=1.5,
    etag_timeout_sec=20,
    enable_endpoint_fallback=True,
    skip_integrity_check_on_failure=True,
    skip_reverify_after_success=True,
):
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化、文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：model_size not in valid_model_sizes
    - 条件：f not in downloaded_paths
    - 条件：_verify_file_integrity(path, filename)
    依据来源（证据链）：
    - 输入参数：model_size。
    输入参数：
    - model_size: 模型/推理配置（类型：未标注）。
    - hf_endpoint: 起止时间/区间边界（类型：未标注）。
    - use_mirror: 函数入参（类型：未标注）。
    - proxy: 函数入参（类型：未标注）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    # 0. 模型尺寸校验
    valid_model_sizes = ["tiny", "base", "small", "medium", "large", "medium.en", "large-v1", "large-v2", "large-v3", "distil-large-v3"]
    if model_size not in valid_model_sizes:
        raise ValueError(f"❌ 无效的模型尺寸 '{model_size}'，可选范围：{valid_model_sizes}")

    # 1. 设置路径与仓库
    repo_id = f"Systran/faster-whisper-{model_size}"
    cache_dir = _resolve_whisper_cache_dir()
    
    # 2. 官方核心文件清单
    # 注意：纯净回装模式下，我们不再手动补齐 vocabulary.json
    core_files = ["config.json", "model.bin", "tokenizer.json", "vocabulary.txt"]
    essential = ["config.json", "model.bin", "tokenizer.json"]
    
    print(f"\n{'='*70}", flush=True)
    print(f"🚀 [纯净模式] 准备模型: {model_size}")
    print(f"📍 缓存根目录: {cache_dir}")
    print(f"{'='*70}", flush=True)
    
    downloaded_paths = {}
    all_integrity_passed = True
    
    # 设置符号链接警告抑制
    os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

    if skip_reverify_after_success:
        preverified_dir = _try_reuse_preverified_model_dir(
            repo_id=repo_id,
            model_size=model_size,
            cache_dir=cache_dir,
            core_files=core_files,
            essential_files=essential,
        )
        if preverified_dir:
            print("♻️ 检测到本地已校验模型，跳过重复校验与下载", flush=True)
            print(f"{'='*70}\n", flush=True)
            return preverified_dir
     
    endpoint_plan = _build_endpoint_plan(use_mirror, hf_endpoint, enable_endpoint_fallback)
    last_error = None

    for index, endpoint_cfg in enumerate(endpoint_plan):
        plan_use_mirror = endpoint_cfg["use_mirror"]
        plan_endpoint = endpoint_cfg["hf_endpoint"]
        plan_label = endpoint_cfg["label"]
        downloaded_paths = {}

        if index > 0:
            print(
                f"🔁 主下载端点失败，切换到备用端点: {plan_label}",
                flush=True,
            )

        try:
            with set_hf_env(plan_use_mirror, plan_endpoint, proxy):
                for filename in core_files:
                    print(f"📦 校验核心文件: {filename}...", flush=True)
                    try:
                        path = _download_with_retry(
                            repo_id=repo_id,
                            filename=filename,
                            cache_dir=cache_dir,
                            max_retries=max_retries,
                            retry_base_delay_sec=retry_base_delay_sec,
                            etag_timeout_sec=etag_timeout_sec,
                            force_download=False,
                        )
                        if _verify_file_integrity(path, filename):
                            downloaded_paths[filename] = path
                        elif skip_integrity_check_on_failure:
                            all_integrity_passed = False
                            print(
                                f"⚠️  文件校验未通过，按配置跳过校验: {filename}",
                                flush=True,
                            )
                            downloaded_paths[filename] = path
                        else:
                            all_integrity_passed = False
                            print(f"⚠️  文件校验失败，强制重拉: {filename}", flush=True)
                            path = _download_with_retry(
                                repo_id=repo_id,
                                filename=filename,
                                cache_dir=cache_dir,
                                max_retries=max_retries,
                                retry_base_delay_sec=retry_base_delay_sec,
                                etag_timeout_sec=etag_timeout_sec,
                                force_download=True,
                            )
                            downloaded_paths[filename] = path
                    except Exception as e:
                        if filename == "vocabulary.txt":
                            print(f"ℹ️  提示: {filename} 在仓库中不存在，跳过。", flush=True)
                            continue
                        raise e
            break
        except Exception as e:
            last_error = e
            has_more_endpoint = index < len(endpoint_plan) - 1
            if has_more_endpoint and _is_retryable_download_error(e):
                print(f"⚠️  端点 {plan_label} 下载失败: {e}", flush=True)
                continue
            raise e

    if not downloaded_paths and last_error:
        raise last_error
            
    # 最终完整性检查
    for f in essential:
        if f not in downloaded_paths:
            raise RuntimeError(f"❌ 核心文件缺失: {f}。模型将无法加载。")
            
    final_dir = os.path.dirname(downloaded_paths["config.json"])
    if skip_reverify_after_success:
        state_payload = {
            "repo_id": repo_id,
            "model_size": model_size,
            "verified_at_epoch": int(time.time()),
            "integrity_result": "passed" if all_integrity_passed else "skipped_on_failure",
            "essential_files": essential,
        }
        try:
            _write_verify_state(cache_dir, repo_id, state_payload)
        except Exception as e:
            print(f"[warn] write verify state failed: {e}", flush=True)
    print(f"\n✅ 模型物理准备完成！")
    print(f"📍 加载路径: {final_dir}")
    
    # 打印目录快照（过滤隐藏文件）
    try:
        files = [f for f in os.listdir(final_dir) if not f.startswith('.') and os.path.isfile(os.path.join(final_dir, f))]
        print(f"📁 目录清单: {', '.join(files)}")
    except:
        pass
        
    print(f"{'='*70}\n", flush=True)
    return final_dir
