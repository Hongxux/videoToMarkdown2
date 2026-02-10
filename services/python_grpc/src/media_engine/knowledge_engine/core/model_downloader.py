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
from contextlib import contextmanager
from huggingface_hub import hf_hub_download

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

def download_whisper_model(model_size="medium", hf_endpoint=None, use_mirror=True, proxy=None):
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
    cache_dir = os.path.expanduser("~/.cache/huggingface/hub")
    
    # 2. 官方核心文件清单
    # 注意：纯净回装模式下，我们不再手动补齐 vocabulary.json
    core_files = ["config.json", "model.bin", "tokenizer.json", "vocabulary.txt"]
    
    print(f"\n{'='*70}", flush=True)
    print(f"🚀 [纯净模式] 准备模型: {model_size}")
    print(f"📍 缓存根目录: {cache_dir}")
    print(f"{'='*70}", flush=True)
    
    downloaded_paths = {}
    
    # 设置符号链接警告抑制
    os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
    
    with set_hf_env(use_mirror, hf_endpoint, proxy):
        for filename in core_files:
            print(f"📦 校验核心文件: {filename}...", flush=True)
            try:
                path = hf_hub_download(repo_id=repo_id, filename=filename, cache_dir=cache_dir)
                if _verify_file_integrity(path, filename):
                    downloaded_paths[filename] = path
                else:
                    path = hf_hub_download(repo_id=repo_id, filename=filename, cache_dir=cache_dir, force_download=True)
                    downloaded_paths[filename] = path
            except Exception as e:
                if filename == "vocabulary.txt":
                    print(f"ℹ️  提示: {filename} 在仓库中不存在，跳过。", flush=True)
                    continue
                raise e
            
    # 最终完整性检查
    essential = ["config.json", "model.bin", "tokenizer.json"]
    for f in essential:
        if f not in downloaded_paths:
            raise RuntimeError(f"❌ 核心文件缺失: {f}。模型将无法加载。")
            
    final_dir = os.path.dirname(downloaded_paths["config.json"])
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
