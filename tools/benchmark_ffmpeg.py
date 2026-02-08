import subprocess
import time
import os
import shutil
import sys

# Log to file
log_file = open("benchmark.log", "w", encoding="utf-8")

def log(msg):
    print(msg)
    log_file.write(msg + "\n")
    log_file.flush()

VIDEO_PATH = r"d:\videoToMarkdownTest2\storage\20225626c2a19253c4121f684ecdff12\semantic_unit_clips\003_SU003_Cloudbot功能描述_64.00-80.00.mp4"
OUTPUT_DIR = "bench_out"
os.makedirs(OUTPUT_DIR, exist_ok=True)

FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
log(f"Using FFmpeg: {FFMPEG}")

def run_bench(name, cmd_args):
    start = time.time()
    try:
        # Capture stderr to see errors
        result = subprocess.run([FFMPEG, "-y"] + cmd_args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        end = time.time()
        
        if result.returncode != 0:
            log(f"[{name}] FAILED. Return code: {result.returncode}")
            log(f"Error output:\n{result.stderr[:500]}") # First 500 chars of error
        else:
            log(f"[{name}] Time: {end - start:.4f}s")
    except Exception as e:
        log(f"[{name}] Exception: {e}")

log(f"Benchmarking video: {VIDEO_PATH}")

# 1. Java Legacy (Fast Seek + CPU Medium)
log("\n--- 1. Java Legacy (Current) ---")
run_bench("Java Legacy", [
    "-ss", "5", "-i", VIDEO_PATH, "-t", "5",
    "-c:v", "libx264", "-crf", "23", "-c:a", "aac",
    f"{OUTPUT_DIR}/java_legacy.mp4"
])

# 2. Java Optimized CPU
log("\n--- 2. Java Optimization A (CPU Ultrafast) ---")
run_bench("Java CPU Ultrafast", [
    "-ss", "5", "-i", VIDEO_PATH, "-t", "5",
    "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23", "-c:a", "aac",
    f"{OUTPUT_DIR}/java_cpu_ultra.mp4"
])

# 3. Java Optimized GPU
log("\n--- 3. Java Optimization B (GPU NVENC) ---")
run_bench("Java GPU NVENC", [
    "-ss", "5", "-i", VIDEO_PATH, "-t", "5",
    "-c:v", "h264_nvenc", "-preset", "p4", "-cq", "23", "-c:a", "aac",
    f"{OUTPUT_DIR}/java_gpu.mp4"
])

# 4. Python Legacy (Slow Seek + CPU Superfast)
log("\n--- 4. Python Legacy (Current) ---")
run_bench("Python Legacy", [
    "-i", VIDEO_PATH, "-ss", "5", "-t", "5",
    "-c:v", "libx264", "-preset", "superfast", "-crf", "23", "-c:a", "aac",
    f"{OUTPUT_DIR}/py_legacy.mp4"
])

# 5. Python Optimized (Fast Seek + GPU)
log("\n--- 5. Python Optimized (Fast Seek + GPU) ---")
run_bench("Python GPU", [
    "-ss", "5", "-i", VIDEO_PATH, "-t", "5",
    "-c:v", "h264_nvenc", "-preset", "p4", "-cq", "23", "-c:a", "aac",
    f"{OUTPUT_DIR}/py_gpu.mp4"
])

log_file.close()
