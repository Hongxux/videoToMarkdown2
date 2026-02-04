"""
模块说明：阶段流程节点 phase5_capture 的实现。
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
from typing import Dict, Any, List, Optional

from ..state import PipelineState
from ..tools.opencv_capture import FrameCapture, SemanticPeakDetector
from ..tools.debug_visualizer import DebugVisualizer
from ..llm.client import create_vision_client
from ..monitoring.logger import get_logger


async def step12_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：peak_params
    - 条件：peak_params and frames
    - 条件：(idx + 1) % 5 == 0
    依据来源（证据链）：
    - 配置字段：peak_metrics_history。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step12_capture", state.get("output_dir", "output/logs"))
    logger.start()
    
    instructions = state.get("screenshot_instructions", [])
    video_path = state.get("video_path", "")
    output_dir = state.get("output_dir", "output")
    
    logger.log_input({"instruction_count": len(instructions), "video": video_path})
    
    try:
        # 截帧属于 I/O 和 CPU 密集型操作，且由于 OpenCV 的 VideoCapture 竞争视频文件句柄
        # 过高的并发会导致磁盘寻道冲突（Thrashing）和解码器溢出。
        # 建议保持在 5-10 之间，以获得最佳吞吐量。
        semaphore = asyncio.Semaphore(8)
        
        async def process_instruction(idx, instruction):
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            决策逻辑：
            - 条件：peak_params
            - 条件：peak_params and frames
            - 条件：(idx + 1) % 5 == 0
            依据来源（证据链）：
            - 输入参数：idx, instruction。
            - 配置字段：peak_metrics_history。
            输入参数：
            - idx: 函数入参（类型：未标注）。
            - instruction: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            async with semaphore:
                try:
                    # 每个任务使用独立的临时子目录，避免并发冲突
                    ins_id = instruction.get("instruction_id", "default")
                    with FrameCapture(video_path, f"{output_dir}/temp_frames_{ins_id}") as capture:
                        # 峰值检测预处理
                        peak_params = instruction.get("opencv_params", {}).get("peak_detect_params")
                        if peak_params:
                            detector = SemanticPeakDetector(capture._cap) # Utilize the underlying cap
                            search_start, search_end = peak_params["search_range"]
                            # 使用 detector 寻找最佳时间点，替换 primary_times
                            peak_time, metrics_history = detector.detect_peak(search_start, search_end, peak_params.get("step_sec", 0.5))
                            instruction["opencv_params"]["primary_times"] = [peak_time]
                            instruction["peak_metrics_history"] = metrics_history # Store validation history for later
                            logger.info(f"Refined capture time for {ins_id} to peak at {peak_time:.2f}s")
                            
                            # Log and Visualize Peak Detection
                            frames_for_viz = [] # Populate this later if we keep temp frames
                            # For now just log the event
                            
                        frames = capture.capture_multiple(instruction)
                        
                        # Post-capture visualization for Peak Detection
                        if peak_params and frames:
                             # Try to match captured frames to timestamps for visualization
                             # Simplified: just use the captured frame path
                             # Since detect_peak scans many frames but we only save one, 
                             # we can't visualize all thumbnails unless we change detect_peak to return paths.
                             # But we can at least draw the graph.
                             DebugVisualizer.draw_peak_strip(
                                 f"{output_dir}/debug_peak_{ins_id}.jpg",
                                 metrics_history,
                                 [(f["timestamp"], f["frame_path"]) for f in frames],
                                 peak_time
                             )
                        
                        instruction_frames = []
                        for frame in frames:
                            instruction_frames.append({
                                "frame_id": frame.frame_id,
                                "instruction_id": instruction["instruction_id"],
                                "fault_id": instruction.get("fault_id"),
                                "viz_id": instruction.get("viz_id"),
                                "segment_id": instruction.get("segment_id", ""),
                                "timestamp": frame.timestamp,
                                "frame_path": frame.frame_path,
                                "is_valid": frame.is_valid,
                                "invalid_reason": frame.invalid_reason,
                                "brightness": frame.brightness,
                                "sharpness": frame.sharpness,
                                "metadata": {
                                    **(frame.metadata or {}),
                                    "peak_metrics_history": instruction.get("peak_metrics_history") if instruction.get("peak_metrics_history") else None
                                }
                            })
                        
                        if (idx + 1) % 5 == 0:
                            logger.log_progress(idx + 1, len(instructions))
                            
                        return instruction_frames
                except Exception as e:
                    logger.log_warning(f"Instruction {instruction['instruction_id']} failed: {e}")
                    return []

        logger.info(f"并发执行 {len(instructions)} 个截帧指令...")
        tasks = [process_instruction(i, ins) for i, ins in enumerate(instructions)]
        results = await asyncio.gather(*tasks)
        
        # 聚合结果
        all_frames = []
        for res in results:
            all_frames.extend(res)
        
        output = {
            "captured_frames": all_frames,
            "current_step": "step12_capture",
            "current_step_status": "completed"
        }
        
        logger.log_output({"frame_count": len(all_frames)})
        timing = logger.end(success=True)
        output["step_timings"] = {"step12_capture": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"captured_frames": [], "errors": [{"step": "step12", "error": str(e)}]}


async def deduplicate_frames(frames: List[Dict], threshold: int = 8) -> tuple[List[Dict], int]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过OpenCV 图像处理、NumPy 数值计算、asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not frames
    - 条件：phash is None
    - 条件：not is_duplicate
    依据来源（证据链）：
    - 输入参数：frames, threshold。
    输入参数：
    - frames: 数据列表/集合（类型：List[Dict]）。
    - threshold: 阈值（类型：int）。
    输出参数：
    - List[Dict], int 列表（与输入或处理结果一一对应）。"""
    import cv2
    import numpy as np
    
    def compute_phash(image_path: str, hash_size: int = 8) -> Optional[np.ndarray]:
        """
        执行逻辑：
        1) 准备输入数据。
        2) 执行计算并返回结果。
        实现方式：通过OpenCV 图像处理、NumPy 数值计算实现。
        核心价值：提供量化结果，为上游决策提供依据。
        决策逻辑：
        - 条件：img is None
        依据来源（证据链）：
        输入参数：
        - image_path: 文件路径（类型：str）。
        - hash_size: 函数入参（类型：int）。
        输出参数：
        - flatten 对象或调用结果。"""
        try:
            img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
            if img is None:
                return None
            
            # 缩放到 hash_size+1 x hash_size
            resized = cv2.resize(img, (hash_size + 1, hash_size))
            
            # 计算DCT
            dct = cv2.dct(np.float32(resized))
            dct_low = dct[:hash_size, :hash_size]
            
            # 计算中值
            median = np.median(dct_low)
            
            # 生成哈希
            return (dct_low > median).flatten()
        except Exception:
            return None
    
    def hamming_distance(hash1: np.ndarray, hash2: np.ndarray) -> int:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过NumPy 数值计算实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - hash1: 函数入参（类型：np.ndarray）。
        - hash2: 函数入参（类型：np.ndarray）。
        输出参数：
        - 数值型计算结果。"""
        return int(np.sum(hash1 != hash2))
    
    if not frames:
        return [], 0
    
    # 并行计算所有帧的哈希
    tasks = []
    for frame in frames:
        tasks.append(asyncio.to_thread(compute_phash, frame.get("frame_path", "")))
    
    hashes = await asyncio.gather(*tasks)
    
    unique_frames = []
    seen_hashes = []
    duplicate_count = 0
    
    for frame, phash in zip(frames, hashes):
        if phash is None:
            # 无法计算哈希，保留帧
            unique_frames.append(frame)
            continue
        
        # 检查是否与已有帧相似
        is_duplicate = False
        for seen_hash in seen_hashes:
            if hamming_distance(phash, seen_hash) < threshold:
                is_duplicate = True
                duplicate_count += 1
                break
        
        if not is_duplicate:
            unique_frames.append(frame)
            seen_hashes.append(phash)
            
    return unique_frames, duplicate_count


async def step13_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not frame.get('is_valid', True)
    - 条件：brightness < thresholds['min_brightness']
    - 条件：is_valid
    依据来源（证据链）：
    - 配置字段：is_valid, min_brightness, min_sharpness。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    logger = get_logger("step13_validate_frame", state.get("output_dir", "output/logs"))
    logger.start()
    
    captured_frames = state.get("captured_frames", [])
    logger.log_input({"frame_count": len(captured_frames)})
    
    # 降低阈值以适应深色背景的教学视频
    # 亮度: 正常帧通常在3-255范围内，低于3是真正的黑屏
    # 清晰度: 通常100以上就可接受
    thresholds = {
        "min_brightness": 3,    # 从30降到3
        "min_sharpness": 50     # 从100降到50
    }
    
    try:
        valid_frames = []
        invalid_frames = []
        
        # 并行处理初始校验（使用asyncio.gather分摊逻辑判断）
        async def process_frame(frame):
            # 如果在step12已经标记为无效，直接跳过
            """
            执行逻辑：
            1) 组织处理流程与依赖调用。
            2) 汇总中间结果并输出。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：编排流程，保证步骤顺序与可追踪性。
            决策逻辑：
            - 条件：not frame.get('is_valid', True)
            - 条件：brightness < thresholds['min_brightness']
            - 条件：sharpness < thresholds['min_sharpness']
            依据来源（证据链）：
            - 输入参数：frame。
            - 配置字段：is_valid, min_brightness, min_sharpness。
            输入参数：
            - frame: 函数入参（类型：未标注）。
            输出参数：
            - 函数计算/封装后的结果对象。"""
            if not frame.get("is_valid", True):
                return frame, False, frame.get("invalid_reason", "Already invalid")
            
            # 使用step12已计算的brightness和sharpness进行判断
            brightness = frame.get("brightness", 0)
            sharpness = frame.get("sharpness", 0)
            
            is_valid = True
            reason = None
            
            if brightness < thresholds["min_brightness"]:
                is_valid = False
                reason = f"Black frame: brightness={brightness:.2f} < {thresholds['min_brightness']}"
            elif sharpness < thresholds["min_sharpness"]:
                is_valid = False
                reason = f"Blurry frame: sharpness={sharpness:.2f} < {thresholds['min_sharpness']}"
            
            return frame, is_valid, reason

        results = await asyncio.gather(*[process_frame(f) for f in captured_frames])
        
        valid_frames = []
        invalid_frames = []
        
        for frame, is_valid, reason in results:
            if is_valid:
                valid_frames.append(frame)
            else:
                frame["is_valid"] = False
                frame["invalid_reason"] = reason
                invalid_frames.append(frame)
        
        # 帧去重：移除相似帧（现在是异步的，必须 await）
        deduplicated_frames, dup_count = await deduplicate_frames(valid_frames, threshold=8)
        
        output = {
            "valid_frames": deduplicated_frames,
            "current_step": "step13_validate_frame",
            "current_step_status": "completed"
        }
        
        logger.log_output({
            "valid_count": len(deduplicated_frames),
            "invalid_count": len(invalid_frames),
            "deduplicated_count": dup_count
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step13_validate_frame": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"valid_frames": [], "errors": [{"step": "step13", "error": str(e)}]}


def calculate_completeness(key_elements: List[str], extracted_elements: List[str]) -> float:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not key_elements
    - 条件：key_lower in extracted.lower() or extracted.lower() in key_lower
    依据来源（证据链）：
    - 输入参数：key_elements。
    输入参数：
    - key_elements: 函数入参（类型：List[str]）。
    - extracted_elements: 函数入参（类型：List[str]）。
    输出参数：
    - 数值型计算结果。"""
    if not key_elements:
        return 1.0
    
    matched = 0
    for key in key_elements:
        # 模糊匹配：检查key是否在任意extracted中部分出现
        key_lower = key.lower()
        for extracted in extracted_elements:
            if key_lower in extracted.lower() or extracted.lower() in key_lower:
                matched += 1
                break
    
    return matched / len(key_elements)


async def step14_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：unqualified_frames
    - 条件：verification_tier == 'L1'
    - 条件：not questions
    依据来源（证据链）：
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    分级标准：
    - A级：完全清晰，核心问题全部回答正确
    - B级：基本符合，核心问题≥80%正确
    - C级：勉强可用，核心问题≥60%正确
    - C+级：key_elements完整度≥min_completeness（完整度达标采用）
    - 不合格：核心问题<60%正确且完整度不达标"""
    logger = get_logger("step14_vision_qa", state.get("output_dir", "output/logs"))
    logger.start()
    
    valid_frames = state.get("valid_frames", [])
    instructions = state.get("screenshot_instructions", [])
    semantic_faults = state.get("semantic_faults", [])
    strategy_matches = state.get("strategy_matches", [])
    
    # 构建映射
    instruction_map = {i["instruction_id"]: i for i in instructions}
    fault_map = {f["fault_id"]: f for f in semantic_faults}
    
    # 构建 segment_id -> strategy_match 映射（用于获取 key_elements 和 min_completeness）
    strategy_by_segment = {s.get("segment_id", ""): s for s in strategy_matches}
    
    logger.log_input({"frame_count": len(valid_frames)})
    
    try:
        vision_client = create_vision_client()
        
        # 并发限制：最多同时处理5个Vision请求
        async def validate_single_frame(frame):
            """
            执行逻辑：
            1) 整理待校验数据。
            2) 按规则逐项校验并返回结果。
            实现方式：通过内部函数组合与条件判断实现。
            核心价值：提前发现数据/状态问题，降低运行风险。
            决策逻辑：
            - 条件：verification_tier == 'L1'
            - 条件：not questions
            - 条件：grade == '不合格'
            依据来源（证据链）：
            输入参数：
            - frame: 函数入参（类型：未标注）。
            输出参数：
            - 结构化字典结果（包含字段：grade, answers, completeness, is_qualified）。"""
            instruction = instruction_map.get(frame.get("instruction_id", ""), {})
            fault = fault_map.get(frame.get("fault_id", ""), {})
            segment_id = fault.get("segment_id", "") or frame.get("segment_id", "")
            strategy = strategy_by_segment.get(segment_id, {})
            
            key_elements = strategy.get("key_elements", [])
            min_completeness = strategy.get("min_completeness", 0.7)
            questions = instruction.get("validation_questions", [])
            fault_type_name = fault.get("fault_type_name", "")
            verification_tier = instruction.get("verification_tier", "L2")
            
            # L1 Quick Check Logic
            if verification_tier == "L1":
                 # Simulate L1 check: if key_elements are present in OCR text (simulated here via simple presence check if OCR data was available)
                 # Since we don't have real OCR integrated yet, we fallback to Vision or basic checks.
                 # For now, let's treat L1 as "if we have key_elements, assume qualified if frame is sharp enough"
                 # BUT, to be safe and consistent with the plan, let's just log it and proceed to Vision for now, 
                 # or implement a lightweight check if possible.
                 pass

            if not questions:
                # 无校验问题，直接标记为合格
                return {
                    **frame,
                    "grade": "A",
                    "answers": [],
                    "completeness": 1.0,
                    "is_qualified": True
                }
            
            try:
                result = await vision_client.validate_frame(
                    frame["frame_path"],
                    questions,
                    fault_type_name
                )
                
                grade = result.get("grade", "C")
                answers = result.get("answers", [])
                extracted_content = result.get("extracted_content", {})
                
                # 计算完整度
                # extracted_content 是 { "Q1": "文本1", "Q2": "文本2" }
                extracted_elements = list(extracted_content.values())
                completeness = calculate_completeness(key_elements, extracted_elements)
                
                # C+级判定（核心修复）：
                # 1. 如果是"不合格"（核心问题未满足），绝不提升为C+
                # 2. 如果是"C"（核心问题满足，但次要问题一个没中），但 completeness（关键字匹配）达标，则提升为"C+"
                if grade == "不合格":
                    is_qualified = False
                elif grade == "C" and completeness >= min_completeness:
                    grade = "C+"
                    is_qualified = True
                else:
                    is_qualified = grade in ["A", "B", "C", "C+"]
                
                # Update verification_tier metadata
                verification_tier = instruction.get("verification_tier", "L2")
                
                return {
                    **frame,
                    "grade": grade,
                    "answers": answers,
                    "extracted_content": extracted_content,
                    "completeness": completeness,
                    "is_qualified": is_qualified,
                    "verification_tier": verification_tier
                }
                
            except Exception as e:
                logger.log_warning(f"Vision validation failed for {frame['frame_id']}: {e}")
                return {
                    **frame,
                    "grade": "C",
                    "completeness": 0.7,
                    "is_qualified": True,
                    "error": str(e)
                }
        
        # 并发执行所有帧的Vision校验
        logger.info(f"并发校验 {len(valid_frames)} 个帧的Vision内容...")
        results = await asyncio.gather(*[validate_single_frame(f) for f in valid_frames])
        
        # 分类结果
        validated_frames = []
        qualified_frames = []
        unqualified_frames = []
        
        for frame_result in results:
            validated_frames.append(frame_result)
            
            # Observability: Generate Overlay for low quality or rejected frames
            is_qualified = frame_result.get("is_qualified", False)
            grade = frame_result.get("grade", "C")
            
            if not is_qualified or grade == "C" or grade == "C+":
                frame_id = frame_result.get("frame_id", "unknown")
                DebugVisualizer.draw_verification_overlay(
                    frame_result["frame_path"],
                    f"{output_dir}/debug_verify_{frame_id}.jpg",
                    frame_result
                )

            if is_qualified:
                qualified_frames.append(frame_result)
            else:
                unqualified_frames.append(frame_result)
        
        await vision_client.close()
        
        output = {
            "validated_frames": validated_frames,
            "qualified_frames": qualified_frames,
            "current_step": "step14_vision_qa",
            "current_step_status": "completed",
            "token_usage": {"step14_vision_qa": 0}  # Vision API不计tokens
        }
        
        # 统计各等级数量
        grade_counts = {}
        for f in validated_frames:
            g = f.get("grade", "?")
            grade_counts[g] = grade_counts.get(g, 0) + 1
        
        logger.log_output({
            "validated_count": len(validated_frames),
            "qualified_count": len(qualified_frames),
            "unqualified_count": len(unqualified_frames),
            "grade_distribution": grade_counts
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step14_vision_qa": timing["duration_ms"]}
        
        # 传递不合格帧给 step15
        if unqualified_frames:
            output["_unqualified_frames"] = unqualified_frames
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"qualified_frames": [], "errors": [{"step": "step14", "error": str(e)}]}



async def step15_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：total_retry_count == 0
    - 条件：needs_retry
    - 条件：tasks
    依据来源（证据链）：
    - 配置字段：success。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    核心动作：
    1. 对不合格帧，基于历史截图和指令，生成更精确的重试指令
    2. 对C+级低完整度帧，在时间上下文中采样，选择完整度最高的帧"""
    logger = get_logger("step15_retry", state.get("output_dir", "output/logs"))
    logger.start()
    
    # 获取不合格帧（从 step14 传递）
    unqualified_frames = state.get("_unqualified_frames", [])
    qualified_frames = state.get("qualified_frames", [])
    video_path = state.get("video_path", "")
    output_dir = state.get("output_dir", "output")
    instructions = state.get("screenshot_instructions", [])
    
    # 新增：识别低等级帧进行重试（用户建议：C级也要重试）
    low_quality_frames = []
    high_quality_frames = []
    
    for frame in qualified_frames:
        grade = frame.get("grade", "C")
        completeness = frame.get("completeness", 1.0)
        
        # 调整阈值：C级一律重试，C+级且完整度<0.8也重试
        # A级和B级不重试
        needs_retry = (
            grade == "C" or  # C级一律重试
            (grade == "C+" and completeness < 0.8)  # C+级低完整度重试
        )
        
        if needs_retry:
            low_quality_frames.append(frame)
        else:
            high_quality_frames.append(frame)
    
    total_retry_count = len(unqualified_frames) + len(low_quality_frames)
    
    logger.log_input({
        "unqualified_count": len(unqualified_frames),
        "low_quality_count": len(low_quality_frames),
        "total_retry_count": total_retry_count
    })
    
    if total_retry_count == 0:
        logger.info("No frames need retry, skipping")
        output = {
            "retry_results": [],
            "permanently_failed": [],
            "current_step": "step15_retry",
            "current_step_status": "completed"
        }
        timing = logger.end(success=True)
        output["step_timings"] = {"step15_retry": timing["duration_ms"]}
        return output
    
    try:
        instruction_map = {i["instruction_id"]: i for i in instructions}
        max_retries = 3
        retry_results = []
        permanently_failed = []
        new_qualified = []
        
        vision_client = create_vision_client()
        
        # 准备并行任务
        tasks = []
        
        # 建议 Vision API 并发控制在 5-10
        semaphore = asyncio.Semaphore(10)

        # 1. 准备不合格帧任务
        for frame in unqualified_frames:
            tasks.append(_retry_unqualified_frame(frame, video_path, output_dir, max_retries, logger))
        
        # 2. 准备低质量帧任务
        for frame in low_quality_frames:
            tasks.append(_retry_low_quality_frame(frame, video_path, output_dir, instruction_map, vision_client, semaphore, logger))
            
            if tasks:
                logger.info(f"同时重试 {len(tasks)} 个帧...")
                results = await asyncio.gather(*tasks)
                
                # 收集并发结果
                for i, r in enumerate(results):
                    retry_results.append(r["retry_info"])
                    if r["success"]:
                        new_qualified.append(r["new_frame"])
                    else:
                        # 对于不合格帧（第一部分任务），如果彻底失败进 permanently_failed
                        if i < len(unqualified_frames):
                            permanently_failed.append(r["failure_info"])
                        else:
                            # 对于低质量帧（第二部分任务），如果重试没改善，保留原稿
                            # 注意：low_quality_frames 的处理逻辑在 _retry_low_quality_frame 内部已经处理了 best_effort
                            # 如果 success 为 False 且是低质量帧，说明采样全失败
                            logger.log_warning(f"Low quality frame retry failed, check if frame exists")
            else:
                logger.info("无需要重试的任务")
        
        await vision_client.close()
        
        # 合并所有合格帧
        all_qualified = high_quality_frames + new_qualified
        
        output = {
            "qualified_frames": all_qualified,
            "retry_results": retry_results,
            "permanently_failed": permanently_failed,
            "current_step": "step15_retry",
            "current_step_status": "completed"
        }
        
        logger.log_output({
            "retry_count": len(retry_results),
            "new_qualified": len(new_qualified),
            "permanently_failed": len(permanently_failed),
            "improved_quality": sum(1 for r in retry_results if r.get("retry_type") == "low_quality" and r.get("final_status") == "success")
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step15_retry": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {
            "retry_results": [],
            "permanently_failed": [],
            "errors": [{"step": "step15", "error": str(e)}]
        }


async def _retry_unqualified_frame(frame: Dict, video_path: str, output_dir: str, max_retries: int, logger) -> Dict:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：valid_results
    - 条件：not success
    - 条件：success
    依据来源（证据链）：
    输入参数：
    - frame: 函数入参（类型：Dict）。
    - video_path: 文件路径（类型：str）。
    - output_dir: 目录路径（类型：str）。
    - max_retries: 函数入参（类型：int）。
    - logger: 函数入参（类型：未标注）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    success = False
    new_frame = None
    
    # 使用独立的临时目录
    temp_dir = f"{output_dir}/temp_retry_{frame['frame_id']}"
    
    # 构造多个偏移量并发尝试
    offsets = [0.5 * i for i in range(1, max_retries + 1)]
    
    async def try_offset(i, offset, capture):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - i: 函数入参（类型：未标注）。
        - offset: 函数入参（类型：未标注）。
        - capture: 函数入参（类型：未标注）。
        输出参数：
        - 结构化字典结果（包含字段：round, capture_time, frame_path, is_valid, sharpness, frame_id）。"""
        new_time = frame["timestamp"] + offset
        new_frame_id = f"{frame['frame_id']}_r{i}"
        
        try:
            # OpenCV操作建议保持串行或通过锁控制，但此处不同采样点通常ok
            result = capture.capture_best_frame(
                new_time,
                new_frame_id,
                enhance_params={"sharpen": True},
                search_window=0.3,
                step=0.04
            )
            return {
                "round": i,
                "capture_time": new_time,
                "frame_path": result.frame_path,
                "is_valid": result.is_valid,
                "sharpness": result.sharpness,
                "frame_id": new_frame_id
            }
        except Exception as e:
            logger.log_warning(f"Retry offset {offset} failed: {e}")
            return None

    logger.debug(f"Retrying unqualified frame {frame['frame_id']} with {len(offsets)} concurrent offsets...")
    with FrameCapture(video_path, temp_dir) as capture:
        results = await asyncio.gather(*[try_offset(i+1, off, capture) for i, off in enumerate(offsets)])
    valid_results = [r for r in results if r and r["is_valid"] and r["sharpness"] > 100]
    
    # 整理历史记录
    retry_history = [r for r in results if r]
    
    if valid_results:
        # 选择第一个成功的
        best = valid_results[0]
        new_frame = {
            **frame,
            "frame_id": best["frame_id"],
            "frame_path": best["frame_path"],
            "timestamp": best["capture_time"],
            "grade": "C",
            "retry_count": best["round"]
        }
        success = True
    
    return {
        "success": success,
        "new_frame": new_frame,
        "retry_info": {
            "original_frame_id": frame["frame_id"],
            "instruction_id": frame["instruction_id"],
            "retry_type": "unqualified",
            "retry_count": len(retry_history),
            "final_status": "success" if success else "failed",
            "retry_history": retry_history
        },
        "failure_info": {
            "frame_id": frame["frame_id"],
            "fault_id": frame["fault_id"],
            "reason": "No valid frames found in all retry offsets",
            "best_frame_path": retry_history[-1]["frame_path"] if retry_history else None
        } if not success else None
    }


async def _retry_low_quality_frame(frame: Dict, video_path: str, output_dir: str, instruction_map: Dict, vision_client, semaphore, logger) -> Dict:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过asyncio 异步调度实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：not candidates
    - 条件：best_candidate['completeness'] - original_completeness < improvement_threshold
    - 条件：improved
    依据来源（证据链）：
    - 配置字段：completeness, core_questions_satisfied。
    输入参数：
    - frame: 函数入参（类型：Dict）。
    - video_path: 文件路径（类型：str）。
    - output_dir: 目录路径（类型：str）。
    - instruction_map: 函数入参（类型：Dict）。
    - vision_client: 客户端实例（类型：未标注）。
    - semaphore: 函数入参（类型：未标注）。
    - logger: 函数入参（类型：未标注）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    策略（用户优化）：
    1. 第一轮：上下浮动2s，每0.5s采样（共9个采样点）
    2. 第二轮：如果最佳候选仍低质量，在其时间点±0.25s精细化采样
    3. 选择completeness最高或核心问题满足最多的帧"""
    original_time = frame["timestamp"]
    original_completeness = frame.get("completeness", 0.0)
    instruction_id = frame.get("instruction_id", "")
    instruction = instruction_map.get(instruction_id, {})
    questions = instruction.get("validation_questions", [])
    min_completeness = 0.7  # 低质量阈值
    
    # 使用独立的临时目录
    temp_dir = f"{output_dir}/temp_lowq_{frame['frame_id']}"
    
    # 第一轮采样：±2s，每0.5s采样（9个点）
    phase1_offsets = [-2.0, -1.5, -1.0, -0.5, 0, 0.5, 1.0, 1.5, 2.0]
    candidates = []
    
    logger.debug(f"Retrying low-quality frame {frame['frame_id']} (completeness={original_completeness:.2f})")
    
    async def process_sample(idx, offset, capture, phase_name="p1"):
        """
        执行逻辑：
        1) 组织处理流程与依赖调用。
        2) 汇总中间结果并输出。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：编排流程，保证步骤顺序与可追踪性。
        决策逻辑：
        - 条件：sample_time < 0
        - 条件：offset == 0 and phase_name == 'p1'
        - 条件：not result.is_valid
        依据来源（证据链）：
        - 输入参数：offset, phase_name。
        输入参数：
        - idx: 函数入参（类型：未标注）。
        - offset: 函数入参（类型：未标注）。
        - capture: 函数入参（类型：未标注）。
        - phase_name: 函数入参（类型：未标注）。
        输出参数：
        - 结构化字典结果（包含字段：frame_id, frame_path, timestamp, completeness, answers, is_original）。"""
        sample_time = original_time + offset
        if sample_time < 0:
            return None
        
        if offset == 0 and phase_name == "p1":
            return {
                "frame_id": frame["frame_id"],
                "frame_path": frame["frame_path"],
                "timestamp": original_time,
                "completeness": original_completeness,
                "answers": frame.get("answers", []),
                "is_original": True
            }
        
        new_frame_id = f"{frame['frame_id']}_{phase_name}_{idx}"
        try:
            # 截帧（OpenCV操作建议保持同步或控制并发）
            result = capture.capture_best_frame(
                sample_time,
                new_frame_id,
                enhance_params={"sharpen": False},
                search_window=0.3 if phase_name == "p1" else 0.2,
                step=0.04
            )
            
            if not result.is_valid:
                return None
            
            # 并行Vision校验
            async with semaphore:
                vision_result = await vision_client.validate_frame(
                    result.frame_path,
                    questions,
                    frame.get("fault_type_name", "")
                )
            
            completeness = vision_result.get("completeness", 0.0)
            logger.debug(f"    {phase_name} Sample {idx}: time={sample_time:.2f}s, completeness={completeness:.2f}")
            
            return {
                "frame_id": new_frame_id,
                "frame_path": result.frame_path,
                "timestamp": sample_time,
                "completeness": completeness,
                "grade": vision_result.get("grade", "C"),
                "answers": vision_result.get("answers", []),
                "is_original": False
            }
        except Exception as e:
            logger.log_warning(f"Failed in {phase_name} at {sample_time}s: {e}")
            return None

    # 执行第一轮并发采样
    logger.debug(f"  Phase 1: Concurrent sampling at ±2s with 0.5s intervals...")
    with FrameCapture(video_path, temp_dir) as capture:
        p1_results = await asyncio.gather(*[process_sample(i, offset, capture, "p1") for i, offset in enumerate(phase1_offsets)])
        candidates = [r for r in p1_results if r]
        
        if not candidates:
            return {
                "success": False,
                "new_frame": None,
                "retry_info": {
                    "original_frame_id": frame["frame_id"],
                    "instruction_id": instruction_id,
                    "retry_type": "low_quality",
                    "retry_count": 0,
                    "final_status": "failed",
                    "reason": "No valid candidates in phase 1"
                }
            }
        
        # 选择第一轮最佳候选
        by_completeness = sorted(candidates, key=lambda x: x["completeness"], reverse=True)
        best_phase1 = by_completeness[0]
        
        # 第二轮精细化：如果最佳候选仍低于阈值，在其时间点±0.25s并发搜索
        if best_phase1["completeness"] < min_completeness:
            logger.debug(f"  Phase 2: Best from phase 1 still low ({best_phase1['completeness']:.2f}), concurrent refine at ±0.25s...")
            phase2_offsets = [-0.25, 0.25]
            base_time = best_phase1["timestamp"]
            
            async def process_refine(idx, offset, capture):
                """
                执行逻辑：
                1) 组织处理流程与依赖调用。
                2) 汇总中间结果并输出。
                实现方式：通过内部函数组合与条件判断实现。
                核心价值：编排流程，保证步骤顺序与可追踪性。
                决策逻辑：
                - 条件：sample_time < 0
                - 条件：not result.is_valid
                依据来源（证据链）：
                输入参数：
                - idx: 函数入参（类型：未标注）。
                - offset: 函数入参（类型：未标注）。
                - capture: 函数入参（类型：未标注）。
                输出参数：
                - 结构化字典结果（包含字段：frame_id, frame_path, timestamp, completeness, grade, answers, is_original, phase）。"""
                sample_time = base_time + offset
                if sample_time < 0:
                    return None
                
                new_frame_id = f"{frame['frame_id']}_p2_{idx}"
                try:
                    result = capture.capture_best_frame(
                        sample_time,
                        new_frame_id,
                        enhance_params={"sharpen": False},
                        search_window=0.2,
                        step=0.04
                    )
                    
                    if not result.is_valid:
                        return None
                    
                    async with semaphore:
                        vision_result = await vision_client.validate_frame(
                            result.frame_path,
                            questions,
                            frame.get("fault_type_name", "")
                        )
                    
                    completeness = vision_result.get("completeness", 0.0)
                    logger.debug(f"    p2 Refine {idx}: time={sample_time:.2f}s, completeness={completeness:.2f}")
                    
                    return {
                        "frame_id": new_frame_id,
                        "frame_path": result.frame_path,
                        "timestamp": sample_time,
                        "completeness": completeness,
                        "grade": vision_result.get("grade", "C"),
                        "answers": vision_result.get("answers", []),
                        "is_original": False,
                        "phase": 2
                    }
                except Exception as e:
                    logger.log_warning(f"Failed in p2 refine at {sample_time}s: {e}")
                    return None

            p2_results = await asyncio.gather(*[process_refine(i, offset, capture) for i, offset in enumerate(phase2_offsets)])
            candidates.extend([r for r in p2_results if r])
    
    if not candidates:
        return {
            "success": False,
            "new_frame": None,
            "retry_info": {
                "original_frame_id": frame["frame_id"],
                "instruction_id": instruction_id,
                "retry_type": "low_quality",
                "retry_count": 0,
                "final_status": "failed",
                "reason": "No valid candidates"
            }
        }
    
    # 智能选择最佳帧
    # 策略1: 按completeness排序
    by_completeness = sorted(candidates, key=lambda x: x["completeness"], reverse=True)
    best_candidate = by_completeness[0]
    
    # 策略2: 如果completeness没有显著提升，考虑核心问题满足数
    # 统计每个候选满足的核心问题数量
    for c in candidates:
        answers = c.get("answers", [])
        core_satisfied = sum(1 for a in answers if a.get("is_core") and a.get("answer") == "是")
        c["core_questions_satisfied"] = core_satisfied
    
    original_core_satisfied = sum(
        1 for a in frame.get("answers", []) 
        if a.get("is_core") and a.get("answer") == "是"
    )
    
    # 如果completeness差不多，优先选择满足更多核心问题的
    improvement_threshold = 0.05  # 完整度提升阈值
    if best_candidate["completeness"] - original_completeness < improvement_threshold:
        # completeness没有显著提升，按核心问题满足数选择
        by_core = sorted(candidates, key=lambda x: x.get("core_questions_satisfied", 0), reverse=True)
        if by_core[0].get("core_questions_satisfied", 0) > original_core_satisfied:
            best_candidate = by_core[0]
            logger.debug(f"  Selected by core questions: {by_core[0].get('core_questions_satisfied', 0)} > {original_core_satisfied}")
    
    improved = (
        best_candidate["completeness"] > original_completeness or
        best_candidate.get("core_questions_satisfied", 0) > original_core_satisfied
    )
    
    if improved:
        # 使用更好的帧
        new_frame = {
            **frame,
            "frame_id": best_candidate["frame_id"],
            "frame_path": best_candidate["frame_path"],
            "timestamp": best_candidate["timestamp"],
            "completeness": best_candidate["completeness"],
            "grade": best_candidate.get("grade", "C"),
            "quality_improved": True,
            "original_completeness": original_completeness,
            "core_questions_satisfied": best_candidate.get("core_questions_satisfied", 0)
        }
        
        logger.info(f"✓ Improved {frame['frame_id']}: completeness {original_completeness:.2f} → {best_candidate['completeness']:.2f}, core_qs {original_core_satisfied} → {best_candidate.get('core_questions_satisfied', 0)}")
        
        return {
            "success": True,
            "new_frame": new_frame,
            "retry_info": {
                "original_frame_id": frame["frame_id"],
                "instruction_id": instruction_id,
                "retry_type": "low_quality",
                "retry_count": len(candidates),
                "final_status": "success",
                "original_completeness": original_completeness,
                "new_completeness": best_candidate["completeness"],
                "improvement": best_candidate["completeness"] - original_completeness,
                "core_questions_improved": best_candidate.get("core_questions_satisfied", 0) - original_core_satisfied,
                "candidates_tested": len(candidates)
            }
        }
    else:
        # 没有改进，但仍使用最佳候选（可能与原帧持平）
        # 用户建议：多次重试失败后，选择最高完整度的帧
        logger.info(f"  No improvement for {frame['frame_id']}, keeping best candidate (completeness={best_candidate['completeness']:.2f})")
        
        # 即使没有改进，也返回最佳候选（至少不会更差）
        best_frame = {
            **frame,
            "frame_id": best_candidate["frame_id"],
            "frame_path": best_candidate["frame_path"],
            "timestamp": best_candidate["timestamp"],
            "completeness": best_candidate["completeness"],
            "grade": best_candidate.get("grade", frame.get("grade", "C")),
            "quality_improved": False,
            "original_completeness": original_completeness
        }
        
        return {
            "success": True,  # 改为返回成功，使用最佳候选
            "new_frame": best_frame,
            "retry_info": {
                "original_frame_id": frame["frame_id"],
                "instruction_id": instruction_id,
                "retry_type": "low_quality",
                "retry_count": len(candidates),
                "final_status": "best_effort",
                "original_completeness": original_completeness,
                "best_completeness": best_candidate["completeness"],
                "reason": "Used best available frame"
            }
        }


# ============================================================================
# Step 15b: 截图后处理（裁剪）
# ============================================================================

# 固定裁剪规则（硬编码）
CROP_RULES = {
    "层级/结构类": {"margin_percent": 10, "strategy": "edge"},
    "流程/流转类": {"margin_percent": 10, "strategy": "edge"},
    "实操/界面类": {"margin_percent": 0, "strategy": "preserve"},
    "对比/差异类": {"margin_percent": 5, "strategy": "center"},
    "复杂逻辑关系类": {"margin_percent": 0, "strategy": "none"}
}


async def step15b_node(state: PipelineState) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：enable_fixed_crop and scene_type and (scene_type in CROP_RULES)
    - 条件：rule['strategy'] != 'none'
    - 条件：processed_path
    依据来源（证据链）：
    - 配置字段：strategy。
    - 阈值常量：CROP_RULES。
    输入参数：
    - state: 函数入参（类型：PipelineState）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。
    补充说明：
    配置项（用户要求默认启用）：
    - enable_ai_crop: 是否启用AI生成裁剪描述 (default: False)
    - enable_fixed_crop: 是否启用固定裁剪规则 (default: True)"""
    logger = get_logger("step15b_postprocess", state.get("output_dir", "output/logs"))
    logger.start()
    
    qualified_frames = state.get("qualified_frames", [])
    visualization_candidates = state.get("visualization_candidates", [])
    output_dir = state.get("output_dir", "output")
    
    # 配置项 - 用户要求默认启用裁剪
    enable_ai_crop = False
    enable_fixed_crop = True
    
    logger.log_input({
        "frame_count": len(qualified_frames),
        "viz_candidate_count": len(visualization_candidates),
        "enable_fixed_crop": enable_fixed_crop
    })
    
    try:
        from pathlib import Path
        
        # 构建 segment_id -> scene_type 映射
        scene_type_map = {v["segment_id"]: v.get("scene_type", "") for v in visualization_candidates}
        
        # 获取 semantic_faults 来关联 fault_id -> segment_id
        semantic_faults = state.get("semantic_faults", [])
        fault_to_segment = {f["fault_id"]: f["segment_id"] for f in semantic_faults}
        
        processed_frames = []
        crop_count = 0
        
        for frame in qualified_frames:
            fault_id = frame.get("fault_id", "")
            segment_id = fault_to_segment.get(fault_id, frame.get("segment_id", ""))
            scene_type = scene_type_map.get(segment_id, "")
            
            original_path = frame.get("frame_path", "")
            
            # 判断是否需要裁剪
            if enable_fixed_crop and scene_type and scene_type in CROP_RULES:
                rule = CROP_RULES[scene_type]
                
                if rule["strategy"] != "none":
                    # 执行裁剪
                    processed_path = _apply_crop(original_path, rule, output_dir)
                    if processed_path:
                        crop_count += 1
                        processed_frames.append({
                            "frame_id": frame["frame_id"],
                            "original_path": original_path,
                            "processed_path": processed_path,
                            "segment_id": segment_id,
                            "scene_type": scene_type,
                            "crop_applied": True
                        })
                        continue
            
            # 不裁剪，直接透传
            processed_frames.append({
                "frame_id": frame["frame_id"],
                "original_path": original_path,
                "processed_path": original_path,  # 使用原始路径
                "segment_id": segment_id,
                "scene_type": scene_type,
                "crop_applied": False
            })
        
        output = {
            "processed_frames": processed_frames,
            "current_step": "step15b_postprocess",
            "current_step_status": "completed"
        }
        
        logger.log_output({
            "processed_count": len(processed_frames),
            "cropped_count": crop_count
        })
        timing = logger.end(success=True)
        output["step_timings"] = {"step15b_postprocess": timing["duration_ms"]}
        
        return output
        
    except Exception as e:
        logger.log_error(e)
        logger.end(success=False)
        return {"processed_frames": [], "errors": [{"step": "step15b", "error": str(e)}]}


def _apply_crop(image_path: str, rule: Dict, output_dir: str) -> Optional[str]:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：strategy == 'edge'
    - 条件：strategy == 'center'
    依据来源（证据链）：
    输入参数：
    - image_path: 文件路径（类型：str）。
    - rule: 函数入参（类型：Dict）。
    - output_dir: 目录路径（类型：str）。
    输出参数：
    - str 对象或调用结果。"""
    try:
        from PIL import Image
        from pathlib import Path
        
        img = Image.open(image_path)
        width, height = img.size
        
        margin_pct = rule.get("margin_percent", 10)
        strategy = rule.get("strategy", "edge")
        
        if strategy == "edge":
            # 裁剪边缘空白
            margin_x = int(width * margin_pct / 100)
            margin_y = int(height * margin_pct / 100)
            cropped = img.crop((margin_x, margin_y, width - margin_x, height - margin_y))
        elif strategy == "center":
            # 中心对齐裁剪
            margin_x = int(width * margin_pct / 100)
            margin_y = int(height * margin_pct / 100)
            cropped = img.crop((margin_x, margin_y, width - margin_x, height - margin_y))
        else:
            # preserve - 不裁剪
            return None
        
        # 保存处理后的图片
        out_dir = Path(output_dir) / "processed_frames"
        out_dir.mkdir(parents=True, exist_ok=True)
        
        orig_name = Path(image_path).stem
        out_path = out_dir / f"{orig_name}_cropped.png"
        cropped.save(str(out_path))
        
        return str(out_path)
        
    except Exception:
        return None
