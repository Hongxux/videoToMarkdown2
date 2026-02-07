import asyncio
import time
from proto import video_processing_pb2
from python_grpc_server import VideoProcessingServicer

VIDEO_PATH = r"D:\videoToMarkdownTest2\storage\99efb7c15a9121f4e29113821d5c9c73\video.mp4"
SEM_PATH = r"D:\videoToMarkdownTest2\storage\99efb7c15a9121f4e29113821d5c9c73\semantic_units_phase2a.json"
OUT_DIR = r"D:\videoToMarkdownTest2\storage\99efb7c15a9121f4e29113821d5c9c73"

async def run_once():
    servicer = VideoProcessingServicer()
    req = video_processing_pb2.VLAnalysisRequest(
        task_id=f"BENCH_AFTER_FILE_{int(time.time()*1000)}",
        video_path=VIDEO_PATH,
        semantic_units_json_path=SEM_PATH,
        output_dir=OUT_DIR,
    )
    t0 = time.perf_counter()
    resp = await servicer.AnalyzeWithVL(req, None)
    elapsed = (time.perf_counter() - t0) * 1000.0
    print("===BENCH_RESULT===")
    print(f"success={resp.success}")
    print(f"vl_enabled={resp.vl_enabled}")
    print(f"used_fallback={resp.used_fallback}")
    print(f"units_analyzed={resp.units_analyzed}")
    print(f"vl_screenshots_generated={resp.vl_screenshots_generated}")
    print(f"vl_clips_generated={resp.vl_clips_generated}")
    print(f"elapsed_ms={elapsed:.1f}")

if __name__ == "__main__":
    asyncio.run(run_once())
