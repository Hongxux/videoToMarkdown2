import cv2
import logging
import numpy as np
from module2_content_enhancement.visual_element_detection_helpers import VisualElementDetector
from module2_content_enhancement.visual_feature_extractor import VisualFeatureExtractor

# Configure logging to stdout
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    video_path = r"d:\videoToMarkdownTest2\find_alg\video_01\downloads\video.mp4"
    detector = VisualElementDetector()
    try:
        extractor = VisualFeatureExtractor(video_path=video_path)
    except:
        extractor = VisualFeatureExtractor(video_path)
    
    # Case P003: 36.10s screenshot (Range estimate ~34.5-36.5s)
    print("\n=== Checking Case 34.50s-36.50s (P003 Refined) ===")
    frames, _ = extractor.extract_frames_fast(34.5, 36.5, sample_rate=4, target_height=720)
    if frames:
        print(f"Extracted {len(frames)} frames.")
        
        if len(frames) > 1:
            mse_manual = np.mean((frames[0].astype(float) - frames[1].astype(float))**2)
            print(f"Manual MSE (0 vs 1): {mse_manual:.5f}")

        roi = detector.detect_structure_roi(frames[0])
        print(f"ROI: {roi}")
        if roi:
            res = detector.judge_structure_dynamic(frames, roi)
            print(f"Result: {res}")

import asyncio
if __name__ == "__main__":
    asyncio.run(main())
