"""CV ?????????"""

from __future__ import annotations

from typing import List, Tuple

import cv2
import numpy as np

from services.python_grpc.src.content_pipeline.phase2a.vision.cv_models import (
    ActionUnit,
    CVConfig,
    FrameState,
    RedundancySegment,
    StableIsland,
)


def detect_visual_states(validator, start_sec: float, end_sec: float, stable_only: bool = False):
    stable_islands: List[StableIsland] = []
    action_units: List[ActionUnit] = []
    redundancy_segments: List[RedundancySegment] = []
    
    # 动态采样率 (措施5)
    state_fps = CVConfig.FPS_STATE_DETECTION
    if validator.last_unit_complexity == "low":
        state_fps = CVConfig.SAMPLE_FPS_LOW
    
    # 采样帧
    frames = validator._sample_frames(start_sec, end_sec, state_fps)
    if len(frames) < 2:
        return stable_islands, action_units, redundancy_segments
    
    # ROI检测 (1fps采样)
    roi_frame = frames[len(frames) // 2][1]  # 中间帧
    roi = validator._detect_roi(roi_frame)
    
    if not roi:
        return stable_islands, action_units, redundancy_segments
    
    # 状态序列: (timestamp, state, metric, ssim_drop)
    states: List[Tuple[float, FrameState, float, float]] = []
    
    prev_frame = None
    prev_state = None
    continuous_count = 0
    first_frame_of_segment = None  # 记录区间起始帧用于计算SSIM跌幅

    
    for i, (t, frame) in enumerate(frames):
        # 措施3: 轻量冗余初筛
        redundancy_type = validator._quick_redundancy_check(frame, roi)
        if redundancy_type:
            states.append((t, FrameState.REDUNDANT, 0.0, 0.5))  # ssim=0.5 default
            prev_frame = frame
            prev_state = FrameState.REDUNDANT
            continuous_count = 0
            continue
        
        if prev_frame is None:
            prev_frame = frame
            continue
        
        # 措施4: 状态判定轻量校验
        if prev_state == FrameState.STABLE and continuous_count >= 2:
            if validator._light_stable_check(frame, prev_frame, roi):
                states.append((t, FrameState.STABLE, 0.95, 0.95))  # ssim=0.95 for stable
                continuous_count += 1
                prev_frame = frame
                continue
        
        # 全量计算
        ssim = validator._calculate_ssim_roi(prev_frame, frame, roi)
        diff_ratio = validator._calculate_diff_ratio_roi(prev_frame, frame, roi)
        
        # 状态判定 (修正逻辑 v2)
        # 关键洞察: SSIM低 = 场景变化(可能是翻页), 应视为ACTION或边界
        if ssim >= CVConfig.TH_SSIM_STABLE:
            # 高相似度(≥0.9) = 稳定
            state = FrameState.STABLE
            metric = ssim
            if prev_state == FrameState.STABLE:
                continuous_count += 1
            else:
                continuous_count = 1
        elif ssim < 0.5:
            # 极低相似度(<0.5) = 场景突变(如PPT翻页) → 视为ACTION边界
            # 这是知识点切换的重要锚点
            state = FrameState.ACTION
            metric = 1.0 - ssim  # 用1-ssim作为变化程度
            if prev_state == FrameState.ACTION:
                continuous_count += 1
            else:
                continuous_count = 1
        elif diff_ratio > 0.03:  # 降低阈值: 3%变化就算动作
            # 中等变化(diff>3%) = 动作
            state = FrameState.ACTION
            metric = diff_ratio
            if prev_state == FrameState.ACTION:
                continuous_count += 1
            else:
                continuous_count = 1
        elif ssim >= 0.7:
            # 中等相似度(0.7-0.9) + 低变化 = 视为稳定(讲解/微动)
            state = FrameState.STABLE
            metric = ssim
            if prev_state == FrameState.STABLE:
                continuous_count += 1
            else:
                continuous_count = 1
        else:
            # 其他情况 = 可能是缓慢过渡
            state = FrameState.REDUNDANT
            metric = 0.0
            ssim = 0.5  # 默认SSIM
            continuous_count = 0
        
        # V6.9.5: 状态元组增加SSIM值 (timestamp, state, metric, ssim)
        states.append((t, state, metric, ssim))
        prev_frame = frame
        prev_state = state

    
    # V6.9.4: 帧级别平滑动画检测 (V7.1优化: 提高阈值减少误检)
    # 对于被判定为STABLE的帧，检查是否存在边缘位移
    # 只标记有实际动画的帧，而不是整个区间
    if len(frames) >= 3:
        x1, y1, x2, y2 = roi
        new_states = []
        prev_edges = None
        prev_centroid = None
        consecutive_animated = 0  # 连续动画帧计数
        
        for idx, (t, frame) in enumerate(frames):
            if idx >= len(states):
                break
                
            current_state = states[idx]
            
            # 只对STABLE帧进行动画检测
            if current_state[1] == FrameState.STABLE:
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) if len(frame.shape) == 3 else frame
                roi_gray = gray[y1:y2, x1:x2]
                
                # 边缘检测
                blurred = cv2.GaussianBlur(roi_gray, (3, 3), 0)
                edges = cv2.Canny(blurred, 50, 150)
                
                # 重心计算
                contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                centroid = None
                if contours:
                    cx_sum, cy_sum, cnt = 0, 0, 0
                    for c in contours:
                        M = cv2.moments(c)
                        if M["m00"] > 0:
                            cx_sum += M["m10"] / M["m00"]
                            cy_sum += M["m01"] / M["m00"]
                            cnt += 1
                    if cnt > 0:
                        centroid = (cx_sum / cnt, cy_sum / cnt)
                
                is_frame_animated = False
                
                # V7.1: 提高阈值减少误检
                # 检测边缘变化 (提高阈值: 500 → 1500)
                if prev_edges is not None:
                    edge_diff = cv2.absdiff(edges, prev_edges)
                    diff_energy = np.sum(edge_diff > 0)
                    if diff_energy > 1500:  # 提高单帧边缘变化阈值
                        is_frame_animated = True
                
                # 检测重心位移 (提高阈值: 3.0 → 8.0)
                if prev_centroid and centroid:
                    dx = centroid[0] - prev_centroid[0]
                    dy = centroid[1] - prev_centroid[1]
                    disp = np.sqrt(dx**2 + dy**2)
                    if disp > 8.0:  # 提高单帧位移阈值
                        is_frame_animated = True
                
                # V7.1: 需要连续2帧触发才判定为动画，减少噪点误判
                if is_frame_animated:
                    consecutive_animated += 1
                    if consecutive_animated >= 2:
                        # 真正的动画帧
                        new_states.append((t, FrameState.ACTION, 0.5, 0.8))
                    else:
                        # 只有1帧触发，可能是噪点，保持STABLE
                        new_states.append(current_state)
                else:
                    consecutive_animated = 0
                    new_states.append(current_state)
                
                prev_edges = edges
                prev_centroid = centroid
            else:
                new_states.append(current_state)
                prev_edges = None
                prev_centroid = None
                consecutive_animated = 0
        
        states = new_states

    
    # 合并连续状态为区间 (V7.2: 传递frames和roi用于呈现型检测)
    stable_islands, action_units, redundancy_segments = validator._merge_state_intervals(
        states,
        start_sec,
        end_sec,
        frames=frames,
        roi=roi,
        stable_only=stable_only,
    )

    
    # 更新复杂度 (用于下一单元动态采样)
    total_duration = end_sec - start_sec
    stable_duration = sum(s.duration_ms for s in stable_islands) / 1000
    validator.last_unit_complexity = "low" if stable_duration / total_duration >= CVConfig.COMPLEX_LOW_STABLE_RATIO else "medium"
    
    return stable_islands, action_units, redundancy_segments




def merge_state_intervals(validator, states, start_sec: float, end_sec: float, frames=None, roi=None, stable_only: bool = False):
    stable_islands: List[StableIsland] = []
    action_units: List[ActionUnit] = []
    redundancy_segments: List[RedundancySegment] = []
    
    if not states:
        return stable_islands, action_units, redundancy_segments
    
    # 分组连续相同状态
    # 状态元组: (timestamp, state, metric, ssim)
    current_state = states[0][1]
    current_start = states[0][0]
    current_metrics: List[float] = [states[0][2]]
    current_ssims: List[float] = [states[0][3] if len(states[0]) > 3 else 0.9]
    
    for i in range(1, len(states)):
        # 解包4元组
        t = states[i][0]
        state = states[i][1]
        metric = states[i][2]
        ssim = states[i][3] if len(states[i]) > 3 else 0.9
        
        if state == current_state:
            current_metrics.append(metric)
            current_ssims.append(ssim)
        else:
            # 输出上一个区间
            duration_ms = (t - current_start) * 1000
            avg_metric = np.mean(current_metrics) if current_metrics else 0.0
            
            if current_state == FrameState.STABLE and duration_ms >= CVConfig.TH_STABLE_DURATION_MS:
                stable_islands.append(StableIsland(current_start, t, avg_metric))
            elif current_state == FrameState.ACTION and duration_ms >= CVConfig.TH_ACTION_DURATION_MS:
                # V6.9.5: 计算真正的SSIM跌幅 = 首帧SSIM - 末帧SSIM
                # 如果跌幅大，说明是场景切换(transition)
                first_ssim = current_ssims[0] if current_ssims else 0.9
                last_ssim = current_ssims[-1] if current_ssims else 0.9
                ssim_drop = max(0.0, first_ssim - last_ssim)
                
                # 另一种计算: 用最低SSIM衡量结构变化程度
                min_ssim = min(current_ssims) if current_ssims else 0.9
                if min_ssim < 0.5:
                    ssim_drop = 1.0 - min_ssim  # 大跌幅
                
                action_units.append(ActionUnit(
                    start_sec=current_start,
                    end_sec=t,
                    avg_diff_ratio=avg_metric,
                    ssim_drop=ssim_drop
                ))
            elif current_state == FrameState.REDUNDANT:
                redundancy_segments.append(RedundancySegment(
                    current_start, t, RedundancyType.RED_TRANSITION, "整段剔除"))
            
            # 开始新区间
            current_state = state
            current_start = t
            current_metrics = [metric]
            current_ssims = [ssim]

    
    # 处理最后一个区间
    if states:
        t = end_sec
        duration_ms = (t - current_start) * 1000
        avg_metric = np.mean(current_metrics) if current_metrics else 0.0
        
        if current_state == FrameState.STABLE and duration_ms >= CVConfig.TH_STABLE_DURATION_MS:
            stable_islands.append(StableIsland(current_start, t, avg_metric))
        elif current_state == FrameState.ACTION and duration_ms >= CVConfig.TH_ACTION_DURATION_MS:
            # 计算真正的SSIM跌幅
            first_ssim = current_ssims[0] if current_ssims else 0.9
            last_ssim = current_ssims[-1] if current_ssims else 0.9
            ssim_drop = max(0.0, first_ssim - last_ssim)
            min_ssim = min(current_ssims) if current_ssims else 0.9
            if min_ssim < 0.5:
                ssim_drop = 1.0 - min_ssim
                
            action_units.append(ActionUnit(
                start_sec=current_start,
                end_sec=t,
                avg_diff_ratio=avg_metric,
                ssim_drop=ssim_drop
            ))
        elif current_state == FrameState.REDUNDANT:
            redundancy_segments.append(RedundancySegment(
                current_start, t, RedundancyType.RED_TRANSITION, "整段剔除"))

    
    # 预处理场景：仅需要 stable 区间时，跳过动作单元分类/边界细化等昂贵阶段。
    # 这样可复用前面的动态采样+ROI+帧级状态+边缘动画检测链路，同时降低计算开销。
    if stable_only:
        return stable_islands, action_units, redundancy_segments

    # V6.9.5: 后处理 - 分类并过滤非有效动作
    # V7.0: 增加模态子分类 (K1-K4)
    effective_actions = []
    for action in action_units:
        action_type = action.classify()
        action.action_type = action_type
        
        if action_type == "knowledge":
            # 知识生产型: 有效，保留
            action.is_effective = True
            
            # V7.0: 模态子分类
            # 检查是否有内部稳定岛 → K1/K2 (截图)
            has_internal = validator._has_internal_stable_islands(action, stable_islands)
            
            if has_internal:
                # K1/K2: 可静态化 → 纯截图
                action.modality = Modality.SCREENSHOT.value
                action.knowledge_subtype = "K1_K2_stepwise"
            else:
                # V7.2: 先检查呈现型动态 (淡入/渐显)
                is_presentation = False
                if frames is not None and roi is not None:
                    is_presentation = validator._is_presentation_dynamic(
                        action, frames, roi, stable_islands)
                
                if is_presentation:
                    # 呈现型: 强制截图 (取末帧稳定帧)
                    action.modality = Modality.SCREENSHOT.value
                    action.knowledge_subtype = "presentation"
                    logger.debug(f"Presentation dynamic detected [{action.start_sec:.1f}s-{action.end_sec:.1f}s]: forced screenshot")
                else:
                    # V7.4: K3/K4 精细区分
                    continuous_type = "derivation"  # 默认K3
                    if frames is not None and roi is not None:
                        # 提取动作区间帧
                        action_frames = [(t, f) for t, f in frames 
                                        if action.start_sec <= t <= action.end_sec]
                        if len(action_frames) >= 2:
                            continuous_type = validator._classify_continuous_type(action_frames, roi)
                    
                    if continuous_type == "operation":
                        # K4: 连续操作 → 纯视频
                        action.modality = Modality.VIDEO_ONLY.value
                        action.knowledge_subtype = "K4_operation"
                    else:
                        # K3: 连续推演 → 视频+截图
                        action.modality = Modality.VIDEO_SCREENSHOT.value
                        action.knowledge_subtype = "K3_derivation"

            
            effective_actions.append(action)
            logger.debug(f"Knowledge action [{action.start_sec:.1f}s-{action.end_sec:.1f}s]: "
                       f"subtype={action.knowledge_subtype}, modality={action.modality}")
            
        elif action_type == "transition":
            # 容器切换型: 非有效，移入冗余段
            action.is_effective = False
            action.modality = Modality.DISCARD.value
            redundancy_segments.append(RedundancySegment(
                action.start_sec, action.end_sec,
                RedundancyType.RED_TRANSITION, "转场动画剔除"))
            logger.debug(f"Filtered transition action: [{action.start_sec:.1f}s-{action.end_sec:.1f}s]")
        elif action_type == "noise":
            # 噪点: 非有效，移入冗余段
            action.is_effective = False
            action.modality = Modality.DISCARD.value
            redundancy_segments.append(RedundancySegment(
                action.start_sec, action.end_sec,
                RedundancyType.RED_IRRELEVANT, "噪点动画剔除"))
            logger.debug(f"Filtered noise action: [{action.start_sec:.1f}s-{action.end_sec:.1f}s]")
        else:
            # mixed类型: 保守用截图
            action.is_effective = True
            action.modality = Modality.SCREENSHOT.value
            action.knowledge_subtype = "mixed"
            effective_actions.append(action)
    
    # V7.1: 合并相邻动作单元 (间隔 < TH_ACTION_MERGE_GAP_SEC 的视为连续动作)
    if len(effective_actions) >= 2:
        merged_actions = []
        current = effective_actions[0]
        
        for next_action in effective_actions[1:]:
            gap = next_action.start_sec - current.end_sec
            
            if gap < CVConfig.TH_ACTION_MERGE_GAP_SEC:  # 使用配置项
                # 合并为一个更大的动作单元
                merged = ActionUnit(
                    start_sec=current.start_sec,
                    end_sec=next_action.end_sec,
                    avg_diff_ratio=max(current.avg_diff_ratio, next_action.avg_diff_ratio),
                    ssim_drop=max(current.ssim_drop, next_action.ssim_drop),
                    is_effective=True,
                    has_internal_stable=current.has_internal_stable or next_action.has_internal_stable
                )
                # 重新分类合并后的动作
                merged.action_type = merged.classify()
                
                # V7.0: 根据新类型更新模态
                if merged.action_type == "knowledge":
                    merged.modality = Modality.VIDEO_SCREENSHOT.value
                    merged.knowledge_subtype = "K3_K4_continuous"
                elif merged.action_type == "mixed":
                    merged.modality = Modality.SCREENSHOT.value
                    merged.knowledge_subtype = "mixed"
                else:
                    merged.modality = current.modality
                    merged.knowledge_subtype = current.knowledge_subtype
                
                current = merged
                logger.debug(f"Merged adjacent actions: gap={gap:.2f}s → [{current.start_sec:.1f}s-{current.end_sec:.1f}s] type={current.action_type}")
            else:
                merged_actions.append(current)
                current = next_action
        
        merged_actions.append(current)
        
        # 💥 日志: 显示合并效果
        if len(merged_actions) < len(effective_actions):
            logger.info(f"Action merge: {len(effective_actions)} → {len(merged_actions)} (threshold={CVConfig.TH_ACTION_MERGE_GAP_SEC}s)")
        
        effective_actions = merged_actions
    
    # V8.0: 边界细化 - 复用 VideoClipExtractor 逻辑
    # 仅对需要截取视频的动作单元进行细化
    for action in effective_actions:
        if action.modality in (Modality.VIDEO_SCREENSHOT.value, Modality.VIDEO_ONLY.value):
            validator._refine_action_boundaries(action, roi)
            logger.debug(f"Boundary refined: [{action.start_sec:.2f}s-{action.end_sec:.2f}s] modality={action.modality}")

    return stable_islands, effective_actions, redundancy_segments
