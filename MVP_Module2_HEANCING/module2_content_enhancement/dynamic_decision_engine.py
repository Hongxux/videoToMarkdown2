"""
模块说明：Module2 内容增强中的 dynamic_decision_engine 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import numpy as np
import cv2
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class ActionWindow:
    """
    类说明：封装 ActionWindow 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    start_t: float
    end_t: float
    peak_mse: float
    certainty: float
    profile_type: str

class GlobalAnalysisCache:
    """
    类说明：封装 GlobalAnalysisCache 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    def __init__(self, clip_id: str):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - clip_id: 标识符（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.clip_id = clip_id
        self.enhanced_frames: List[np.ndarray] = []
        self.timestamps: List[float] = []
        self.mse_list: List[float] = []
        self.mse_base: float = 0.0
        self.ssim_drop: float = 0.0
        self.visual_elements: List[Dict[str, Any]] = []
        self.is_analyzed = False

    def clear(self):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.enhanced_frames.clear()
        self.timestamps.clear()
        self.mse_list.clear()
        self.visual_elements.clear()
        self.is_analyzed = False

class DynamicDecisionEngine:
    """
    类说明：封装 DynamicDecisionEngine 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    def __init__(self):
        # 默认 Profile 配置 (对齐第一性原理)
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - 无。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.profiles = {
            "formula": {
                "mse_spike_multiplier": 3.0,    # 降低倍率要求 (V6.9.3 sensitivity boost)
                "min_spike_mse": 2.0,           # 捕捉微小笔迹 (原 3.0)
                "certainty_threshold": 0.65,    # 降低置信度门槛
                "fusion_rule": "visual_first"
            },
            "ppt_slide": {
                "mse_spike_multiplier": 6.0,    # PPT 切换较明显，阈值稍高
                "min_spike_mse": 10.0,
                "certainty_threshold": 0.85,
                "fusion_rule": "balanced"
            },
            "mouse_cursor": {
                "mse_spike_multiplier": 10.0,   # 过滤鼠标，阈值极高
                "min_spike_mse": 30.0,
                "certainty_threshold": 0.95,
                "fusion_rule": "strict_visual"
            },
            "generic": {
                "mse_spike_multiplier": 5.0,
                "min_spike_mse": 5.0,
                "certainty_threshold": 0.8,
                "fusion_rule": "balanced"
            }
        }

    def preprocess_frames_adaptive(self, frames: List[np.ndarray]) -> List[np.ndarray]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        输出参数：
        - np.ndarray 列表（与输入或处理结果一一对应）。"""
        enhanced = []
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        
        for frame in frames:
            # 1. 转为 LAB 空间，对 L 通道应用 CLAHE
            lab = cv2.cvtColor(frame, cv2.COLOR_BGR2LAB)
            l, a, b = cv2.split(lab)
            l_enh = clahe.apply(l)
            lab_enh = cv2.merge((l_enh, a, b))
            f_enh = cv2.cvtColor(lab_enh, cv2.COLOR_LAB2BGR)
            
            # 2. 轻微去噪 (Gaussian) 减少压缩噪声带来的 MSE 干扰
            f_denoised = cv2.GaussianBlur(f_enh, (3, 3), 0)
            enhanced.append(f_denoised)
            
        return enhanced

    def compute_base_features(self, frames: List[np.ndarray], timestamps: List[float]) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 准备输入数据。
        2) 执行计算并返回结果。
        实现方式：通过内部方法调用/状态更新、NumPy 数值计算实现。
        核心价值：提供量化结果，为上游决策提供依据。
        决策逻辑：
        - 条件：len(frames) < 2
        - 条件：len(mse_list) >= 3
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        - timestamps: 函数入参（类型：List[float]）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        if len(frames) < 2:
            return {"mse_list": [], "mse_base": 0.0, "ssim_drop": 0.0}
            
        mse_list = []
        for i in range(len(frames) - 1):
            f1 = frames[i].astype(np.float32)
            f2 = frames[i+1].astype(np.float32)
            mse = np.mean((f1 - f2) ** 2)
            mse_list.append(mse)
            
        # 以开头两帧作为底噪基准
        mse_base = max(0.5, np.mean(mse_list[:3]) if len(mse_list) >= 3 else mse_list[0])
        
        # 🚀 物理特征增强: 计算全段结构置换率 (SSIM Proxy using Edge Diff)
        # 第一性原理：容器切换 = 全局结构重置 -> SSIM 暴跌
        ssim_drop = self.calculate_ssim_feature(frames)
        
        return {
            "mse_list": mse_list,
            "mse_base": mse_base,
            "ssim_drop": ssim_drop
        }

    def detect_action_windows(self, mse_list: List[float], timestamps: List[float], 
                             mse_base: float, profile_name: str = "generic") -> List[ActionWindow]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：is_spike and (not in_action)
        - 条件：not is_spike and in_action
        - 条件：is_spike and in_action
        依据来源（证据链）：
        - 输入参数：timestamps。
        输入参数：
        - mse_list: 数据列表/集合（类型：List[float]）。
        - timestamps: 函数入参（类型：List[float]）。
        - mse_base: 函数入参（类型：float）。
        - profile_name: 函数入参（类型：str）。
        输出参数：
        - ActionWindow 列表（与输入或处理结果一一对应）。"""
        profile = self.profiles.get(profile_name, self.profiles["generic"])
        multiplier = profile["mse_spike_multiplier"]
        min_mse = profile["min_spike_mse"]
        
        windows = []
        in_action = False
        start_t = 0.0
        peak_mse = 0.0
        
        for i, mse in enumerate(mse_list):
            is_spike = mse >= max(min_mse, mse_base * multiplier)
            
            if is_spike and not in_action:
                in_action = True
                start_t = timestamps[i]
                peak_mse = mse
            elif not is_spike and in_action:
                # 动作结束 (简单逻辑：MSE 回落)
                in_action = False
                end_t = timestamps[i+1] if i+1 < len(timestamps) else timestamps[i]
                windows.append(ActionWindow(
                    start_t=start_t,
                    end_t=end_t,
                    peak_mse=peak_mse,
                    certainty=min(1.0, peak_mse / (mse_base * 20)), # 示例确定性评分
                    profile_type=profile_name
                ))
            elif is_spike and in_action:
                peak_mse = max(peak_mse, mse)
                
        return windows

    def judge_is_dynamic(self, windows, avg_mse, total_duration, ssim_drop=0.0, profile_name="ppt_slide", edge_flux_data: Tuple[float, float] = (0.0, 0.0)):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：flux_sum > 0.03 and flux_var < 0.005
        - 条件：not windows
        - 条件：is_knowledge_type
        依据来源（证据链）：
        - 输入参数：avg_mse, total_duration, windows。
        输入参数：
        - windows: 函数入参（类型：未标注）。
        - avg_mse: 函数入参（类型：未标注）。
        - total_duration: 函数入参（类型：未标注）。
        - ssim_drop: 函数入参（类型：未标注）。
        - profile_name: 函数入参（类型：未标注）。
        - edge_flux_data: 函数入参（类型：Tuple[float, float]）。
        输出参数：
        - 结构化字典结果（包含字段：is_dynamic, reason, certainty, action_density）。"""
        profile = self.profiles.get(profile_name, self.profiles["ppt_slide"])
        
        # [Layer 0] 平滑流动态 (Smooth Flow) - 优先判定
        # 第一性原理：流动态往往没有 Spike，必须先于 Spike 逻辑判定
        flux_sum, flux_var = edge_flux_data
        if flux_sum > 0.03 and flux_var < 0.005:
            # 只有当 MSE 极低（非剧烈变动）时，Flux 才有决定权
            if avg_mse < 20.0:
                 return {"is_dynamic": True, "reason": f"smooth_flow_verified (Flux:{flux_sum:.3f})", "certainty": 0.85, "action_density": 1.0, "is_smooth": True}

        # 1. 基础事实计算
        if not windows:
            if avg_mse > 150 and total_duration > 2.0:
                 return {"is_dynamic": True, "reason": "global_high_energy", "certainty": 0.6, "action_density": 1.0}
            return {"is_dynamic": False, "reason": "no_action_detected", "certainty": 0.0, "action_density": 0.0}
            
        # 2. 量化指标计算 (Metrics Calculation)
        action_dur = sum(w.end_t - w.start_t for w in windows)
        action_density = action_dur / total_duration if total_duration > 0 else 0
        peak_count = len(windows)
        
        # 3. 动态类型分类 (Action Type Classification)
        is_knowledge_type = False
        is_transition_candidate = False
        
        # 3.1 物理特征指纹判定
        is_structural_reset = ssim_drop > 0.4
        
        for w in windows:
            dur = w.end_t - w.start_t
            
            # [Type A] 知识生产型 (Knowledge Production)
            # 特征: 持续时长 >= 1.5s (书写过程) 且 结构未完全重置 (非翻页)
            # 或者: 极高确定性 (Certainty > 0.9) 且非边缘短动作
            if not is_structural_reset and (dur >= 1.5 or (w.certainty > 0.85 and dur > 0.8)):
                is_knowledge_type = True
                
            # [Type B] 容器切换候选 (Transition Candidate)
            # 特征: 短促 (<1.5s), 突变强, 或伴随 SSIM 暴跌
            elif dur < 1.5 or is_structural_reset:
                is_transition_candidate = True
                
        # 4. 分层决策 (Hierarchical Decision Tree)
        
        # [Layer 1] 知识生产型 -> 核心验证
        if is_knowledge_type:
            # 🚀 V6.9.8 Refinement (P007 Case): 防止较长的平移转场被误认为书写
            # 真正的知识生产（书写）在长片段中通常具有一定的“离散性”或“密度阈值”
            # 如果动作密度极低（< 10%）且总时长较长（> 10s），且只有一个动作窗，
            # 那么这极可能是一个缓慢的平移或淡入。
            if total_duration > 10.0 and action_density < 0.10 and peak_count == 1:
                return {"is_dynamic": False, "reason": "long_slow_background_transition_filtered", "certainty": 0.3, "action_density": action_density}
            
            # 🚀 V6.9.8 Refinement (P009 Case): 判定动态是否具有“高价值/高效率”
            # 如果虽然有动态，但极其零星（如 60s 视频只有 2s 动作），且非 Math 领域，则降级
            is_math = profile_name == "math_formula" # 假设后续有 profile 传递
            if not is_math and total_duration > 30.0 and action_density < 0.05:
                return {"is_dynamic": False, "reason": "inefficient_dynamic_demoted", "certainty": 0.4, "action_density": action_density}

            return {"is_dynamic": True, "reason": "knowledge_production_verified", "certainty": 0.95, "action_density": action_density}
            
        if is_transition_candidate:
            # 🚀 V6.9.5: Short Clip Auto-Threshold
            # P011 Case: 5.7s total, 0.9s action -> 16%. Should be Static.
            # Dynamic Threshold = 1.0 / Duration + 0.05
            dynamic_density_threshold = min(0.3, max(0.1, 1.0 / max(total_duration, 1.0) + 0.05))
            
            if action_density < dynamic_density_threshold and peak_count <= 2:
                return {"is_dynamic": False, "reason": f"transient_transition_filtered_v2 (T:{dynamic_density_threshold:.2f})", "certainty": 0.2, "action_density": action_density}
                
            if action_density >= dynamic_density_threshold or peak_count > 2:
                 return {"is_dynamic": True, "reason": "significant_transition_sequence", "certainty": 0.65, "action_density": action_density}
                 
        return {"is_dynamic": False, "reason": "low_energy_fluctuation", "certainty": 0.1, "action_density": action_density}

    def calculate_ssim_feature(self, frames: List[np.ndarray]) -> float:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 2
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        输出参数：
        - 数值型计算结果。"""
        if len(frames) < 2: return 0.0
        
        # 简化版: 使用 Canny 边缘图的差异作为 Structural Proxy
        # 真实 SSIM 计算较慢，此处用结构变动率代替
        f_start = cv2.cvtColor(frames[0], cv2.COLOR_BGR2GRAY)
        f_end = cv2.cvtColor(frames[-1], cv2.COLOR_BGR2GRAY)
        
        edges_start = cv2.Canny(f_start, 50, 150)
        edges_end = cv2.Canny(f_end, 50, 150)
        
        diff = np.mean(np.abs(edges_start.astype(float) - edges_end.astype(float)))
        return diff / 255.0  # Normalize 0-1

    def calculate_edge_flux(self, frames: List[np.ndarray]) -> Tuple[float, float]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(frames) < 5
        - 条件：not flux_list
        - 条件：prev_edge is not None
        依据来源（证据链）：
        - 输入参数：frames。
        输入参数：
        - frames: 数据列表/集合（类型：List[np.ndarray]）。
        输出参数：
        - 多值结果元组（各元素含义见实现）。"""
        if len(frames) < 5: return 0.0, 1.0
        
        flux_list = []
        # 降采样：每隔 3 帧取一帧 (采样率提高以计算方差)
        sample_frames = frames[::3]
        
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2,2)) # 细腻一点
        
        prev_edge = None
        for frame in sample_frames:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            
            # 1. 强抗噪：高斯模糊 (平滑块效应)
            blurred = cv2.GaussianBlur(gray, (3, 3), 1.0)
            
            # 2. Canny 边缘提取
            edge = cv2.Canny(blurred, 50, 150)
            
            # 3. 形态学抗噪：膨胀 (融合微小噪点)
            edge = cv2.dilate(edge, kernel, iterations=1)
            
            if prev_edge is not None:
                # 4. 差分计算
                diff = cv2.absdiff(edge, prev_edge)
                
                # 5. 边缘复原：腐蚀 (避免膨胀夸大位移)
                diff = cv2.erode(diff, kernel, iterations=1)
                
                # 6. 单帧能量归一化
                flux = np.sum(diff) / (diff.shape[0] * diff.shape[1] * 255.0)
                flux_list.append(flux)
                
            prev_edge = edge
            
        if not flux_list: return 0.0, 0.0
        
        flux_sum = sum(flux_list) * 3.0 # 补偿采样率
        flux_var = np.var(flux_list) if len(flux_list) > 1 else 0.0
        
        return flux_sum, flux_var
