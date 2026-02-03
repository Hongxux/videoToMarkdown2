    def detect_structure_roi(self, frame: np.ndarray) -> Optional[Tuple[int, int, int, int]]:
        """
        检测结构化内容的ROI区域 (Bounding Box)
        
        Args:
            frame: 输入帧
            
        Returns:
            (x1, y1, x2, y2) 或 None
        """
        try:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            edges = cv2.Canny(gray, 50, 150)
            
            # 使用现有方法检测元素，或者简单地基于边缘密度
            # 这里简单起见，使用轮廓外接矩形
            contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            if not contours:
                return None
                
            # 过滤微小噪声
            valid_rects = []
            h, w = frame.shape[:2]
            min_area = (h * w) * 0.05 # 至少占屏 5%
            
            all_points = []
            for cnt in contours:
                x, y, cw, ch = cv2.boundingRect(cnt)
                if cw * ch > min_area:
                     all_points.append((x, y))
                     all_points.append((x + cw, y + ch))
            
            if not all_points:
                # Fallback: take all non-trivial contours
                for cnt in contours:
                    if cv2.contourArea(cnt) > 100:
                         x,y,cw,ch = cv2.boundingRect(cnt)
                         all_points.append((x,y))
                         all_points.append((x+cw,y+ch))
            
            if not all_points: return None
            
            all_points = np.array(all_points)
            min_x = np.min(all_points[:, 0])
            min_y = np.min(all_points[:, 1])
            max_x = np.max(all_points[:, 0])
            max_y = np.max(all_points[:, 1])
            
            # Padding
            pad = 20
            min_x = max(0, min_x - pad)
            min_y = max(0, min_y - pad)
            max_x = min(w, max_x + pad)
            max_y = min(h, max_y + pad)
            
            return (min_x, min_y, max_x, max_y)
            
        except Exception as e:
            logger.warning(f"ROI detection failed: {e}")
            return None

    def judge_structure_dynamic(self, frame_sequence: List[np.ndarray], structure_roi: Tuple[int, int, int, int]) -> str:
        """
        结构化图表的动静判定：区分静态图表和动态动画图表 (V6.3)
        
        Args:
            frame_sequence: 核心区域的连续帧序列
            structure_roi: (x1, y1, x2, y2)
        
        Returns:
            "static" / "dynamic"
        """
        if not frame_sequence or not structure_roi:
            return "static"
            
        x1, y1, x2, y2 = structure_roi
        # 1. 提取ROI内的帧序列
        roi_frames = []
        for frame in frame_sequence:
            if frame is None: continue
            roi = frame[y1:y2, x1:x2]
            if roi.size == 0: continue
            # Convert to gray for MSE
            if len(roi.shape) == 3:
                roi = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
            roi_frames.append(roi.astype(np.float32))
            
        if len(roi_frames) < 2: return "static"
        
        # 2. 短期波动 (Short-term Flux)
        short_term_mse = []
        for i in range(len(roi_frames)-1):
            mse = np.mean((roi_frames[i] - roi_frames[i+1]) ** 2)
            short_term_mse.append(mse)
        short_term_mean = np.mean(short_term_mse) if short_term_mse else 0.0
        
        # 3. 长期通量 (Long-term Flux)
        long_term_mse = np.mean((roi_frames[0] - roi_frames[-1]) ** 2)
        
        logger.info(f"[Structure Dynamic Check] ShortMSE: {short_term_mean:.3f}, LongMSE: {long_term_mse:.3f}")
        
        # 4. 判定阈值 (V6.3 Config)
        # 动态判定条件：长期累积变化显著(>=1.5)，且短期波动平稳(<2.0, 提高一点宽容度防止误判)
        # 原始建议 <0.8, 但考虑到压缩噪声, 2.0可能更安全, 我们可以先用 1.0
        if long_term_mse >= 1.5 and short_term_mean < 2.0:
            return "dynamic"
        else:
            return "static"
