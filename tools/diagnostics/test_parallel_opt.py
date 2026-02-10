import os
import sys
import time
# Add project root to path
sys.path.append(os.getcwd())

# Mock whisper model to avoid large download/load if possible, or just use base
# But we want to test the threading logic which happens inside transcribe_parallel

from services.python_grpc.src.media_engine.knowledge_engine.core.parallel_transcription import transcribe_parallel

# Use the video path from previous context
video_path = r"d:\videoToMarkdownTest2\storage\95bf71bd0768fa4d2a0b2968c775c312\video.mp4"

if not os.path.exists(video_path):
    # Try to find any mp4 file
    print(f"Video not found: {video_path}, searching for others...")
    found = False
    for root, dirs, files in os.walk("d:\\videoToMarkdownTest2"):
        for file in files:
            if file.endswith(".mp4"):
                video_path = os.path.join(root, file)
                print(f"Found video: {video_path}")
                found = True
                break
        if found: break
    
    if not found:
        print("No video found for testing.")
        sys.exit(0)

print(f"Testing parallel transcription on {video_path}")
print("-" * 50)

start_time = time.time()
# Use base model and cpu for testing logic
try:
    result = transcribe_parallel(
        video_path, 
        model_size="base", 
        device="cpu", 
        num_workers=2,
        compute_type="int8"
    )
    print("-" * 50)
    print(f"Transcription complete in {time.time() - start_time:.2f}s")
    print(f"Result length: {len(result)} chars")
except Exception as e:
    print(f"Test failed: {e}")
    import traceback
    traceback.print_exc()


