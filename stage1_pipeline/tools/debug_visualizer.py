
import cv2
import numpy as np
from typing import List, Dict, Tuple
from pathlib import Path
import json

class DebugVisualizer:
    """
    可视化调试工具
    用于生成峰值检测条带和校验覆盖图
    """
    
    @staticmethod
    def draw_peak_strip(
        output_path: str,
        metrics_history: List[Dict[str, float]],
        frames: List[Tuple[float, str]], # (timestamp, frame_path)
        peak_time: float
    ):
        """
        绘制峰值检测条带
        
        Args:
            output_path: 输出图片路径
            metrics_history: [{"t": 10.0, "score": 0.5}, ...]
            frames: 候选帧列表 [(10.0, "path1.png"), ...]
            peak_time: 最终选中的峰值时间
        """
        if not frames or not metrics_history:
            return
            
        # 1. 准备画布
        thumb_size = (160, 90) # 16:9 缩略图
        padding = 10
        margin_bottom = 100 # 留给曲线图的空间
        
        width = len(frames) * (thumb_size[0] + padding) + padding
        height = thumb_size[1] + 2 * padding + margin_bottom
        
        canvas = np.ones((height, width, 3), dtype=np.uint8) * 255
        
        # 2. 绘制缩略图
        for i, (t, f_path) in enumerate(frames):
            x = padding + i * (thumb_size[0] + padding)
            y = padding
            
            try:
                img = cv2.imread(f_path)
                if img is not None:
                    thumb = cv2.resize(img, thumb_size)
                    canvas[y:y+thumb_size[1], x:x+thumb_size[0]] = thumb
                    
                    # 如果是峰值帧，画红框
                    if abs(t - peak_time) < 0.01:
                        cv2.rectangle(canvas, (x, y), (x+thumb_size[0], y+thumb_size[1]), (0, 0, 255), 3)
                        
                    # 标注时间
                    cv2.putText(canvas, f"{t:.1f}s", (x, y + thumb_size[1] + 15), 
                                cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1)
            except Exception:
                pass

        # 3. 绘制曲线图
        # 提取时间轴和分数
        times = [m["t"] for m in metrics_history]
        scores = [m.get("score", 0) for m in metrics_history]
        
        if not scores:
            return
            
        graph_x_star = padding + thumb_size[0] // 2
        graph_width = width - 2 * padding - thumb_size[0]
        graph_y_base = height - padding
        graph_height = margin_bottom - 20
        
        min_t, max_t = min(times), max(times)
        max_score = max(scores) if scores else 1.0
        if max_score == 0: max_score = 1.0
        
        def t_to_x(t):
            ratio = (t - min_t) / (max_t - min_t) if max_t > min_t else 0
            # 简单映射到整个宽度，实际应该对齐缩略图，这里简化处理
            return int(graph_x_star + ratio * graph_width) if len(frames) > 1 else padding + thumb_size[0]//2

        pts = []
        # 改用对齐缩略图的方式绘制
        # 假设 frames 的时间点涵盖了 range
        if len(frames) > 1:
           frame_times = [f[0] for f in frames]
           min_f_t, max_f_t = min(frame_times), max(frame_times)
           
           for t, s in zip(times, scores):
               # 映射 t 到 x 坐标 (基于 min_f_t 和 max_f_t 线性插值)
               if max_f_t > min_f_t:
                   ratio = (t - min_f_t) / (max_f_t - min_f_t)
                   # x范围从第一个缩略图中心到最后一个缩略图中心
                   start_x = padding + thumb_size[0] // 2
                   total_w = (len(frames) - 1) * (thumb_size[0] + padding)
                   x = int(start_x + ratio * total_w)
               else:
                   x = padding + thumb_size[0] // 2
                   
               y = int(graph_y_base - (s / max_score) * graph_height)
               pts.append((x, y))
               
               # 绘制点
               color = (0, 0, 255) if abs(t - peak_time) < 0.01 else (200, 200, 200)
               cv2.circle(canvas, (x, y), 3, color, -1)
           
           # 连线
           if len(pts) > 1:
               cv2.polylines(canvas, [np.array(pts)], False, (100, 100, 100), 1)

        cv2.imwrite(output_path, canvas)

    @staticmethod
    def draw_verification_overlay(
        input_path: str,
        output_path: str,
        result: Dict
    ):
        """
        绘制校验覆盖图 (拒信单)
        
        Args:
            input_path: 原始图片路径
            output_path: 输出图片路径
            result: 校验结果 {"is_qualified": False, "missing_elements": [...], "grade": "C"}
        """
        img = cv2.imread(input_path)
        if img is None:
            return
            
        h, w = img.shape[:2]
        
        # 1. 绘制半透明蒙层
        overlay = img.copy()
        cv2.rectangle(overlay, (0, h-100), (w, h), (0, 0, 0), -1)
        cv2.addWeighted(overlay, 0.7, img, 0.3, 0, img)
        
        # 2. 绘制文字
        grade = result.get("grade", "Unknown")
        color = (0, 255, 0) if result.get("is_qualified") else (0, 0, 255)
        
        cv2.putText(img, f"Grade: {grade}", (20, h-60), 
                    cv2.FONT_HERSHEY_SIMPLEX, 1, color, 2)
                    
        if not result.get("is_qualified"):
            # 显示缺失原因
            missing = result.get("extracted_content", {}).get("missing_reason", "Verification Failed")
            # 简单截断防止溢出
            if len(missing) > 50: missing = missing[:47] + "..."
            cv2.putText(img, f"Reason: {missing}", (20, h-25), 
                        cv2.FONT_HERSHEY_SIMPLEX, 0.7, (200, 200, 255), 1)
                        
        cv2.imwrite(output_path, img)

