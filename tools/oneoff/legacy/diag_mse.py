"""
模块说明：diag_mse 相关能力的封装。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""


import asyncio
import numpy as np
from module2_content_enhancement.visual_feature_extractor import VisualFeatureExtractor

async def main():
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过NumPy 数值计算实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - 无。
    输出参数：
    - 无（仅产生副作用，如日志/写盘/状态更新）。"""
    video_path = r"d:\videoToMarkdownTest2\find_alg\video_01\downloads\video.mp4"
    extractor = VisualFeatureExtractor(video_path)
    
    print("=== Scanning 30s to 40s (0.5s intervals) ===")
    frames, ts = extractor.extract_frames_fast(30.0, 40.0, sample_rate=2, target_height=360)
    
    for i in range(len(frames)-1):
        mse = np.mean((frames[i].astype(np.float32) - frames[i+1].astype(np.float32))**2)
        print(f"{ts[i]:.2f}s - {ts[i+1]:.2f}s: MSE={mse:.5f}")

if __name__ == "__main__":
    asyncio.run(main())
