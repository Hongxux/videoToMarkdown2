import time
import os
import torch
from faster_whisper import WhisperModel

def test_model(model_size, device="cpu", compute_type="int8", audio_path=None):
    print(f"\n--- 测试模型: {model_size} ({device}/{compute_type}) ---")
    
    # 启用镜像加速
    os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"
    
    start_load = time.time()
    try:
        # 优先指定本地缓存目录或直接通过名称加载
        model = WhisperModel(model_size, device=device, compute_type=compute_type)
    except Exception as e:
        print(f"尝试模型名称失败: {e}")
        # 如果是 distil 模型，手动指定仓库
        if "distil" in model_size:
            repo_id = f"Systran/faster-distil-whisper-large-v3" # 修正仓库路径
            print(f"尝试从 {repo_id} 加载...")
            model = WhisperModel(repo_id, device=device, compute_type=compute_type)
        else:
            raise e
            
    load_time = time.time() - start_load
    print(f"模型加载耗时: {load_time:.2f}s")

    start_transcribe = time.time()
    segments, info = model.transcribe(audio_path, beam_size=5, language="zh")
    
    # 强制迭代生成器以完成转录
    results = list(segments)
    transcribe_time = time.time() - start_transcribe
    
    print(f"转录完成！")
    print(f"音频时长: {info.duration:.2f}s")
    print(f"转录耗时: {transcribe_time:.2f}s (速度比: {info.duration/transcribe_time:.2f}x)")
    print(f"首条结果: {results[0].text if results else '无'}")
    
    return transcribe_time, results

if __name__ == "__main__":
    video_path = r"d:\videoToMarkdownTest2\storage\95bf71bd0768fa4d2a0b2968c775c312\video.mp4"
    
    # 转换为音频以减少干扰（可选，这里直接用视频测试以贴近实际情况）
    
    models_to_test = ["large-v2", "distil-large-v3"]
    
    results_summary = {}
    
    for m in models_to_test:
        try:
            t, res = test_model(m, audio_path=video_path)
            results_summary[m] = t
        except Exception as e:
            print(f"模型 {m} 测试出错: {e}")

    print("\n" + "="*30)
    print("性能汇总报告")
    print("="*30)
    for m, t in results_summary.items():
        print(f"{m}: {t:.2f}s")
    
    if len(results_summary) == 2:
        speedup = results_summary["large-v2"] / results_summary["distil-large-v3"]
        print(f"\n加速比: {speedup:.2f}x")
