
import asyncio
import numpy as np
from module2_content_enhancement.visual_feature_extractor import VisualFeatureExtractor

async def main():
    video_path = r"d:\videoToMarkdownTest2\find_alg\video_01\downloads\video.mp4"
    extractor = VisualFeatureExtractor(video_path)
    
    print("=== Scanning 30s to 40s (0.5s intervals) ===")
    frames, ts = extractor.extract_frames_fast(30.0, 40.0, sample_rate=2, target_height=360)
    
    for i in range(len(frames)-1):
        mse = np.mean((frames[i].astype(np.float32) - frames[i+1].astype(np.float32))**2)
        print(f"{ts[i]:.2f}s - {ts[i+1]:.2f}s: MSE={mse:.5f}")

if __name__ == "__main__":
    asyncio.run(main())
