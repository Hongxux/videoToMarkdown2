"""
模块说明：阶段1流水线 graph 的实现。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。
补充说明：
支持：
- SQLite 持久化检查点（断点续跑）
- 可配置的步骤中间产物输出"""

import asyncio
import json
from typing import Dict, Any, Optional, List
from pathlib import Path

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

# 尝试导入 SqliteSaver（需要 pip install langgraph-checkpoint-sqlite）
try:
    from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
    SQLITE_AVAILABLE = True
except ImportError:
    SQLITE_AVAILABLE = False
    AsyncSqliteSaver = None

from .state import PipelineState, create_initial_state
from .checkpoint import SQLiteCheckpointer, STEP_INDEX_MAP, generate_thread_id
from .nodes import (
    step1_node, 
    step2_node, step3_node, step4_node, step5_node, step6_node,
    step7_node, step7b_node, step7c_node, step8a_node, step8b_node,
    step9_node, step10_node, step11_node,
    step12_node, step13_node, step14_node, step15_node, step15b_node,
    step16_node, step17_node, step18_node, step19_node,
    step20_node, step21_node, step22_node, step22b_node,
    step23_node, step24_node
)
from .monitoring.logger import setup_logging
from .monitoring.tracer import PipelineTracer
from .monitoring.metrics import MetricsCollector


# ============================================================================
# 步骤中间产物输出配置
# ============================================================================

class StepOutputConfig:
    """
    类说明：封装 StepOutputConfig 的职责与行为。
    执行逻辑：
    1) 维护类内状态与依赖。
    2) 通过方法组合对外提供能力。
    实现方式：通过成员变量与方法调用实现。
    核心价值：集中状态与方法，降低分散实现的复杂度。
    输入：
    - 构造函数与业务方法的入参。
    输出：
    - 方法返回结果或内部状态更新。"""
    
    # 默认输出的步骤（只保留关键节点：step2和step6）
    DEFAULT_ENABLED_STEPS = {
        "step2_correction",
        "step6_merge_cross",
    }
    
    def __init__(
        self, 
        output_dir: str = "output/intermediates",
        enabled_steps: Optional[List[str]] = None,
        enable_all: bool = False,
        disable_all: bool = False
    ):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：disable_all
        - 条件：enable_all
        - 条件：enabled_steps is not None
        依据来源（证据链）：
        - 输入参数：disable_all, enable_all, enabled_steps。
        输入参数：
        - output_dir: 目录路径（类型：str）。
        - enabled_steps: 开关/状态（类型：Optional[List[str]]）。
        - enable_all: 开关/状态（类型：bool）。
        - disable_all: 函数入参（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        if disable_all:
            self.enabled_steps = set()
        elif enable_all:
            self.enabled_steps = set(STEP_INDEX_MAP.keys())
        elif enabled_steps is not None:
            self.enabled_steps = set(enabled_steps)
        else:
            self.enabled_steps = self.DEFAULT_ENABLED_STEPS.copy()
    
    def should_output(self, step_name: str) -> bool:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - step_name: 函数入参（类型：str）。
        输出参数：
        - 布尔判断结果。"""
        return step_name in self.enabled_steps
    
    def save_step_output(self, step_name: str, state: Dict[str, Any]):
        """
        执行逻辑：
        1) 组织输出结构与格式。
        2) 写入目标路径并处理异常。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：统一输出格式，降低落盘与格式错误。
        决策逻辑：
        - 条件：not self.should_output(step_name)
        依据来源（证据链）：
        - 输入参数：step_name。
        - 对象内部状态：self.should_output。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - state: 函数入参（类型：Dict[str, Any]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if not self.should_output(step_name):
            return
        
        # 提取该步骤相关的输出
        step_output = self._extract_step_output(step_name, state)
        
        output_file = self.output_dir / f"{step_name}_output.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(step_output, f, ensure_ascii=False, indent=2, default=str)
    
    def _extract_step_output(self, step_name: str, state: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：field in state
        - 条件：isinstance(value, list) and len(value) > 10
        - 条件：value
        依据来源（证据链）：
        - 输入参数：state。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - state: 函数入参（类型：Dict[str, Any]）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        # 步骤 输入 -> 输出 字段映射
        step_io_map = {
            "step1_validate": {
                "input": ["video_path", "subtitle_path"],
                "output": ["is_valid", "domain", "main_topic", "video_title"]
            },
            "step2_correction": {
                "input": ["domain", "subtitle_path"],
                "output": ["corrected_subtitles", "correction_summary"]
            },
            "step3_merge": {
                "input": ["corrected_subtitles"],
                "output": ["merged_sentences"]
            },
            "step4_clean_local": {
                "input": ["merged_sentences"],
                "output": ["cleaned_sentences"]
            },
            "step5_clean_cross": {
                "input": ["cleaned_sentences", "main_topic"],
                "output": ["non_redundant_sentences"]
            },
            "step6_merge_cross": {
                "input": ["non_redundant_sentences"],
                "output": ["pure_text_script"]
            },
            "step7_segment": {
                "input": ["pure_text_script", "main_topic"],
                "output": ["knowledge_segments"]
            },
            "step7c_kp_merge": {
                "input": ["knowledge_segments"],
                "output": ["knowledge_points"]
            },
            "step7b_viz_scene": {
                "input": ["knowledge_segments"],
                "output": ["visualization_candidates"]
            },
            "step8a_fault_detect": {
                "input": ["knowledge_segments"],
                "output": ["fault_candidates"]
            },
            "step8b_fault_locate": {
                "input": ["fault_candidates", "knowledge_segments"],
                "output": ["semantic_faults"]
            },
            "step9_strategy": {
                "input": ["semantic_faults"],
                "output": ["strategy_matches"]
            },
            "step10_timing": {
                "input": ["strategy_matches", "semantic_faults"],
                "output": ["capture_times"]
            },
            "step11_instruction": {
                "input": ["capture_times", "strategy_matches", "semantic_faults"],
                "output": ["screenshot_instructions"]
            },
            "step12_capture": {
                "input": ["screenshot_instructions", "video_path"],
                "output": ["captured_frames"]
            },
            "step13_validate_frame": {
                "input": ["captured_frames"],
                "output": ["valid_frames"]
            },
            "step14_vision_qa": {
                "input": ["valid_frames", "screenshot_instructions", "semantic_faults"],
                "output": ["validated_frames", "qualified_frames"]
            },
            "step15_retry": {
                "input": ["_unqualified_frames", "qualified_frames", "video_path"],
                "output": ["qualified_frames", "retry_results", "permanently_failed"]
            },
            "step15b_postprocess": {
                "input": ["qualified_frames", "visualization_candidates"],
                "output": ["processed_frames"]
            },
            "step16_viz_need": {
                "input": ["knowledge_segments", "semantic_faults", "qualified_frames", "visualization_candidates"],
                "output": ["visualization_needed"]
            },
            "step17_viz_form": {
                "input": ["visualization_needed", "semantic_faults"],
                "output": ["visualization_forms"]
            },
            "step18_core_content": {
                "input": ["knowledge_segments", "visualization_forms", "semantic_faults"],
                "output": ["core_content_judgment"]
            },
            "step19_auxiliary": {
                "input": ["knowledge_segments", "visualization_forms", "core_content_judgment", "semantic_faults"],
                "output": ["auxiliary_information"]
            },
            "step20_integrate": {
                "input": ["knowledge_segments", "core_content_judgment", "qualified_frames", "auxiliary_information", "visualization_forms"],
                "output": ["integrated_materials"]
            },
            "step21_reconstruct": {
                "input": ["integrated_materials", "semantic_faults"],
                "output": ["reconstructed_materials"]
            },
            "step22_markdown": {
                "input": ["reconstructed_materials", "video_path", "video_title", "domain", "main_topic"],
                "output": ["output_markdown_path"]
            },
            "step23_video_name": {
                "input": ["core_content_judgment", "knowledge_segments", "auxiliary_information", "video_path"],
                "output": ["named_video_clips"]
            },
            "step24_screenshot_name": {
                "input": ["qualified_frames", "strategy_matches", "knowledge_segments"],
                "output": ["named_screenshots"]
            }
        }
        
        io_spec = step_io_map.get(step_name, {"input": [], "output": []})
        
        result = {
            "step": step_name,
            "input": {},
            "output": {}
        }
        
        # 提取输入
        for field in io_spec["input"]:
            if field in state:
                value = state[field]
                # 对于大列表只记录摘要
                if isinstance(value, list) and len(value) > 10:
                    result["input"][field] = {
                        "_count": len(value),
                        "_sample": value[:3] if value else []
                    }
                else:
                    result["input"][field] = value
        
        # 提取输出
        for field in io_spec["output"]:
            if field in state:
                result["output"][field] = state[field]
        
        # 添加元信息
        result["_meta"] = {
            "token_usage": state.get("token_usage", {}).get(step_name, 0),
            "timing_ms": state.get("step_timings", {}).get(step_name, 0)
        }
        
        return result


# ============================================================================
# 创建带检查点的节点包装器
# ============================================================================

def create_checkpointed_node(
    node_func,
    step_name: str,
    checkpointer: Optional[SQLiteCheckpointer] = None,
    output_config: Optional[StepOutputConfig] = None
):
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：state.get('_resume_mode') and step_index <= last_index
    - 条件：checkpointer
    - 条件：output_config
    依据来源（证据链）：
    - 输入参数：checkpointer, output_config。
    - 配置字段：_resume_mode。
    输入参数：
    - node_func: 函数入参（类型：未标注）。
    - step_name: 函数入参（类型：str）。
    - checkpointer: 函数入参（类型：Optional[SQLiteCheckpointer]）。
    - output_config: 配置对象/字典（类型：Optional[StepOutputConfig]）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    
    async def wrapper(state: PipelineState) -> Dict[str, Any]:
        # 如果是断点续跑，且当前步骤已经完成，则跳过
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：state.get('_resume_mode') and step_index <= last_index
        - 条件：checkpointer
        - 条件：output_config
        依据来源（证据链）：
        - 输入参数：state。
        - 配置字段：_resume_mode。
        输入参数：
        - state: 函数入参（类型：PipelineState）。
        输出参数：
        - 结构化结果字典（包含关键字段信息）。"""
        step_index = STEP_INDEX_MAP.get(step_name, 0)
        print(f"DEBUG: step={step_name} index={step_index} keys={'step7c_kp_merge' in STEP_INDEX_MAP}")
        last_index = state.get("_last_completed_index", -1)
        
        if state.get("_resume_mode") and step_index <= last_index:
            # 只有当状态中确实缺少该步骤的输出时才报错，否则静默跳过
            return {}

        # 执行原始节点
        result = await node_func(state)
        
        # 合并结果到状态
        merged_state = {**state, **result}
        
        # 保存检查点
        if checkpointer:
            thread_id = state.get("_thread_id", "default")
            step_index = STEP_INDEX_MAP.get(step_name, 0)
            checkpointer.save_checkpoint(thread_id, step_name, step_index, merged_state)
        
        # 保存中间产物
        if output_config:
            output_config.save_step_output(step_name, merged_state)
        
        return result
    
    return wrapper


# 步骤编号映射 (用于 max_step 功能)
STEP_NAME_TO_NUMBER = {
    "step1_validate": 1,
    "step2_correction": 2,
    "step3_merge": 3,
    "step4_clean_local": 4,
    "step5_clean_cross": 5,
    "step6_merge_cross": 6,
    "step7_segment": 7,
    "step7b_viz_scene": 7,  # 7系列
    "step7c_kp_merge": 7,
    "step8a_fault_detect": 8,
    "step8b_fault_locate": 8,
    "step9_strategy": 9,
    "step10_timing": 10,
    "step11_instruction": 11,
    "step12_capture": 12,
    "step13_validate_frame": 13,
    "step14_vision_qa": 14,
    "step15_retry": 15,
    "step15b_postprocess": 15,
    "step16_viz_need": 16,
    "step17_viz_form": 17,
    "step18_core_content": 18,
    "step19_auxiliary": 19,
    "step20_integrate": 20,
    "step21_reconstruct": 21,
    "step22_markdown": 22,
    "step22b_viz_summary": 22,
    "step23_video_name": 23,
    "step24_screenshot_name": 24,
}


def create_pipeline_graph(
    checkpointer: Optional[Any] = None,
    sqlite_checkpointer: Optional[SQLiteCheckpointer] = None,
    output_config: Optional[StepOutputConfig] = None,
    max_step: int = 24  # 🔑 新增: 最大执行到第几步，默认全部
) -> StateGraph:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：max_step < 24
    - 条件：terminal_step
    - 条件：checkpointer
    依据来源（证据链）：
    - 输入参数：checkpointer, max_step, output_config, sqlite_checkpointer。
    - 配置字段：is_valid。
    输入参数：
    - checkpointer: 函数入参（类型：Optional[Any]）。
    - sqlite_checkpointer: 函数入参（类型：Optional[SQLiteCheckpointer]）。
    - output_config: 配置对象/字典（类型：Optional[StepOutputConfig]）。
    - max_step: 函数入参（类型：int）。
    输出参数：
    - compile 对象或调用结果。"""
    graph = StateGraph(PipelineState)
    
    # 创建节点（可选包装）
    def add_node(name: str, func):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：sqlite_checkpointer or output_config
        依据来源（证据链）：
        输入参数：
        - name: 函数入参（类型：str）。
        - func: 函数入参（类型：未标注）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if sqlite_checkpointer or output_config:
            wrapped = create_checkpointed_node(func, name, sqlite_checkpointer, output_config)
            graph.add_node(name, wrapped)
        else:
            graph.add_node(name, func)
    
    # ========== Phase 1: 前期准备 ==========
    add_node("step1_validate", step1_node)
    
    # ========== Phase 2: 文字稿预处理 ==========
    add_node("step2_correction", step2_node)
    add_node("step3_merge", step3_node)
    add_node("step4_clean_local", step4_node)
    add_node("step5_clean_cross", step5_node)
    add_node("step6_merge_cross", step6_node)
    
    # ========== Phase 3: 分片与断层识别 ==========
    add_node("step7_segment", step7_node)
    add_node("step7b_viz_scene", step7b_node)  # 与step8a并行
    add_node("step7c_kp_merge", step7c_node)  # 新增: 知识点合并
    add_node("step8a_fault_detect", step8a_node)
    add_node("step8b_fault_locate", step8b_node)
    
    # ========== Phase 4: 截图指令生成 ==========
    add_node("step9_strategy", step9_node)
    add_node("step10_timing", step10_node)
    add_node("step11_instruction", step11_node)
    
    # ========== Phase 5: 截帧执行与质控 ==========
    add_node("step12_capture", step12_node)
    add_node("step13_validate_frame", step13_node)
    add_node("step14_vision_qa", step14_node)
    add_node("step15_retry", step15_node)
    add_node("step15b_postprocess", step15b_node)  # 新增: 截图后处理
    
    # ========== Phase 6: 可视化判定 ==========
    add_node("step16_viz_need", step16_node)
    add_node("step17_viz_form", step17_node)
    add_node("step18_core_content", step18_node)
    add_node("step19_auxiliary", step19_node)
    
    # ========== Phase 7: 语义重构与输出 ==========
    add_node("step20_integrate", step20_node)
    add_node("step21_reconstruct", step21_node)
    add_node("step22_markdown", step22_node)
    add_node("step22b_viz_summary", step22b_node)
    
    # ========== Phase 8: 归档 ==========
    add_node("step23_video_name", step23_node)
    add_node("step24_screenshot_name", step24_node)
    
    # ========== 定义边 ==========
    
    def should_continue_after_step1(state: PipelineState) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：not state.get('is_valid', False)
        依据来源（证据链）：
        - 输入参数：state。
        - 配置字段：is_valid。
        输入参数：
        - state: 函数入参（类型：PipelineState）。
        输出参数：
        - 字符串结果。"""
        if not state.get("is_valid", False):
            return "end"
        return "step2_correction"
    
    graph.add_conditional_edges(
        "step1_validate",
        should_continue_after_step1,
        {"step2_correction": "step2_correction", "end": END}
    )
    
    # 🔑 确定终止步骤 (根据 max_step)
    terminal_step = None
    if max_step < 24:
        # 找到对应的终止节点
        step_order = [
            "step1_validate", "step2_correction", "step3_merge",
            "step4_clean_local", "step5_clean_cross", "step6_merge_cross",
            "step7_segment", "step7c_kp_merge", "step7b_viz_scene",
            "step8a_fault_detect", "step8b_fault_locate", "step9_strategy",
            "step10_timing", "step11_instruction", "step12_capture",
            "step13_validate_frame", "step14_vision_qa", "step15_retry",
            "step15b_postprocess", "step16_viz_need", "step17_viz_form",
            "step18_core_content", "step19_auxiliary", "step20_integrate",
            "step21_reconstruct", "step22_markdown", "step22b_viz_summary",
            "step23_video_name", "step24_screenshot_name"
        ]
        for step_name in step_order:
            step_num = STEP_NAME_TO_NUMBER.get(step_name, 99)
            if step_num == max_step:
                terminal_step = step_name
        
        # 特殊处理: step6 是常用终止点
        if max_step == 6:
            terminal_step = "step6_merge_cross"
    
    # 顺序边
    edges = [
        ("step2_correction", "step3_merge"),
        ("step3_merge", "step4_clean_local"),
        ("step4_clean_local", "step5_clean_cross"),
        ("step5_clean_cross", "step6_merge_cross"),
        ("step6_merge_cross", "step7_segment"),
        # step7 后执行 step7c（知识点合并）
        ("step7_segment", "step7c_kp_merge"),
        # step7c 后并行执行 step7b 和 step8a
        ("step7c_kp_merge", "step7b_viz_scene"),
        ("step7c_kp_merge", "step8a_fault_detect"),
        # step7b 和 step8a 都完成后进入 step8b
        ("step7b_viz_scene", "step8b_fault_locate"),
        ("step8a_fault_detect", "step8b_fault_locate"),
        ("step8b_fault_locate", "step9_strategy"),
        ("step9_strategy", "step10_timing"),
        ("step10_timing", "step11_instruction"),
        ("step11_instruction", "step12_capture"),
        ("step12_capture", "step13_validate_frame"),
        ("step13_validate_frame", "step14_vision_qa"),
        ("step14_vision_qa", "step15_retry"),
        ("step15_retry", "step15b_postprocess"),  # step15 -> step15b
        ("step15b_postprocess", "step16_viz_need"),  # step15b -> step16
        ("step16_viz_need", "step17_viz_form"),
        ("step17_viz_form", "step18_core_content"),
        ("step18_core_content", "step19_auxiliary"),
        ("step19_auxiliary", "step20_integrate"),
        ("step20_integrate", "step21_reconstruct"),
        ("step21_reconstruct", "step22_markdown"),
        ("step22_markdown", "step22b_viz_summary"),
        ("step22b_viz_summary", "step23_video_name"),
        ("step23_video_name", "step24_screenshot_name"),
        ("step24_screenshot_name", END)
    ]
    
    # 🔑 如果有终止步骤，修改边指向 END
    if terminal_step:
        edges = [
            (src, END if src == terminal_step else dst)
            for src, dst in edges
            if STEP_NAME_TO_NUMBER.get(src, 0) <= max_step
        ]
    
    for src, dst in edges:
        graph.add_edge(src, dst)
    
    graph.set_entry_point("step1_validate")
    
    if checkpointer:
        return graph.compile(checkpointer=checkpointer)
    return graph.compile()


# ============================================================================
# 运行管道
# ============================================================================

async def run_pipeline(
    video_path: str,
    subtitle_path: str,
    output_dir: str = "output",
    enable_checkpoints: bool = False,  # 🔑 默认关闭 (减少输出)
    enable_sqlite: bool = False,        # 🔑 默认关闭 (减少输出)
    enable_logging: bool = False,       # 🔑 新增: 默认关闭日志文件
    enable_traces: bool = False,        # 🔑 新增: 默认关闭 traces
    enable_metrics: bool = False,       # 🔑 新增: 默认关闭 metrics
    resume: bool = False,
    output_steps: Optional[List[str]] = None,
    output_all_steps: bool = False,
    thread_id: Optional[str] = None,
    max_step: int = 24  # 🔑 新增: 最大执行到第几步，默认全部
) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 组织处理流程与依赖调用。
    2) 汇总中间结果并输出。
    实现方式：通过文件系统读写实现。
    核心价值：编排流程，保证步骤顺序与可追踪性。
    决策逻辑：
    - 条件：enable_logging
    - 条件：thread_id is None
    - 条件：enable_sqlite
    依据来源（证据链）：
    - 输入参数：enable_checkpoints, enable_logging, enable_metrics, enable_sqlite, enable_traces, thread_id。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - subtitle_path: 文件路径（类型：str）。
    - output_dir: 目录路径（类型：str）。
    - enable_checkpoints: 开关/状态（类型：bool）。
    - enable_sqlite: 开关/状态（类型：bool）。
    - enable_logging: 开关/状态（类型：bool）。
    - enable_traces: 开关/状态（类型：bool）。
    - enable_metrics: 开关/状态（类型：bool）。
    - resume: 函数入参（类型：bool）。
    - output_steps: 函数入参（类型：Optional[List[str]]）。
    - output_all_steps: 函数入参（类型：bool）。
    - thread_id: 标识符（类型：Optional[str]）。
    - max_step: 函数入参（类型：int）。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    # 🔑 条件性创建日志
    if enable_logging:
        main_logger = setup_logging(f"{output_dir}/logs")
    else:
        main_logger = logging.getLogger("stage1_pipeline")
    
    main_logger.info(f"Starting Stage1 Pipeline (max_step={max_step})")
    main_logger.info(f"Video: {video_path}")
    main_logger.info(f"Subtitle: {subtitle_path}")
    
    # 生成或使用 thread_id
    if thread_id is None:
        thread_id = generate_thread_id(video_path, subtitle_path)
    main_logger.info(f"Thread ID: {thread_id}")
    
    # 🔑 条件性创建 tracer 和 metrics
    tracer = PipelineTracer(Path(output_dir) / "traces") if enable_traces else None
    metrics = MetricsCollector(Path(output_dir) / "metrics") if enable_metrics else None
    
    # 配置中间产物输出
    output_config = StepOutputConfig(
        output_dir=f"{output_dir}/intermediates",
        enabled_steps=output_steps,
        enable_all=output_all_steps
    )
    main_logger.info(f"Intermediate outputs: {len(output_config.enabled_steps)} steps enabled")
    
    # TODO: AsyncSqliteSaver 有兼容性问题，暂时使用 MemorySaver
    # 保留元数据记录
    sqlite_checkpointer = None
    if enable_sqlite:
        db_path = Path(output_dir) / "checkpoints.db"
        sqlite_checkpointer = SQLiteCheckpointer(str(db_path))
        sqlite_checkpointer.start_run(thread_id, video_path, subtitle_path, output_dir)
        main_logger.info(f"Metadata tracking enabled: {db_path}")
    
    # 使用 MemorySaver
    checkpointer = MemorySaver() if enable_checkpoints else None
    if checkpointer:
        main_logger.info("Memory checkpoints enabled")
    else:
        main_logger.info("Checkpoints disabled")
    
    # 🔑 传递 max_step 参数
    graph = create_pipeline_graph(
        checkpointer=checkpointer,
        sqlite_checkpointer=sqlite_checkpointer,
        output_config=output_config,
        max_step=max_step
    )
    
    return await _execute_pipeline(
        graph, video_path, subtitle_path, output_dir,
        thread_id, resume, tracer, metrics, sqlite_checkpointer, main_logger
    )


async def _execute_pipeline(
    graph, video_path, subtitle_path, output_dir,
    thread_id, resume, tracer, metrics, sqlite_checkpointer, main_logger
):
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：resume and sqlite_checkpointer
    - 条件：last_checkpoint
    - 条件：sqlite_checkpointer
    依据来源（证据链）：
    - 输入参数：resume, sqlite_checkpointer。
    输入参数：
    - graph: 函数入参（类型：未标注）。
    - video_path: 文件路径（类型：未标注）。
    - subtitle_path: 文件路径（类型：未标注）。
    - output_dir: 目录路径（类型：未标注）。
    - thread_id: 标识符（类型：未标注）。
    - resume: 函数入参（类型：未标注）。
    - tracer: 函数入参（类型：未标注）。
    - metrics: 函数入参（类型：未标注）。
    - sqlite_checkpointer: 函数入参（类型：未标注）。
    - main_logger: 函数入参（类型：未标注）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    # 准备初始状态
    initial_state = create_initial_state(video_path, subtitle_path, output_dir)
    initial_state["_thread_id"] = thread_id
    
    config = {"configurable": {"thread_id": thread_id}}
    
    # resume: 从 SQLite 加载最后一次成功的状态
    if resume and sqlite_checkpointer:
        print(f"DEBUG: Attempting to resume for thread_id: {thread_id}")
        last_checkpoint = sqlite_checkpointer.load_checkpoint(thread_id)
        if last_checkpoint:
            # 基础信息仍使用新的（防止路径变化）
            last_checkpoint["video_path"] = video_path
            last_checkpoint["subtitle_path"] = subtitle_path
            last_checkpoint["output_dir"] = output_dir
            last_checkpoint["_thread_id"] = thread_id
            
            # 标记续跑模式和进度
            last_checkpoint["_resume_mode"] = True
            last_step = sqlite_checkpointer.get_last_completed_step(thread_id)
            last_checkpoint["_last_completed_index"] = STEP_INDEX_MAP.get(last_step, 0)
            
            initial_state = last_checkpoint
            print(f"DEBUG: Resume mode: loaded state from step '{last_step}' (index {last_checkpoint['_last_completed_index']})")
            main_logger.info(f"Resume mode: loaded state from step '{last_step}' (index {last_checkpoint['_last_completed_index']})")
        else:
            print(f"DEBUG: No checkpoint found for thread {thread_id}")
            main_logger.warning(f"No checkpoint found for thread {thread_id}, starting from scratch.")
    
    try:
        tracer.checkpoint("pipeline_start", {"video": video_path, "thread_id": thread_id, "resume": resume})
        
        final_state = await graph.ainvoke(initial_state, config)
        
        # 更新运行状态
        if sqlite_checkpointer:
            sqlite_checkpointer.update_run_status(thread_id, "completed", "step24_screenshot_name", 28)
        
        tracer.checkpoint("pipeline_end", {"status": "success"})
        tracer.save()
        metrics.save()
        metrics.print_summary()
        
        main_logger.info("Pipeline completed successfully!")
        main_logger.info(f"Output: {final_state.get('output_markdown_path', 'N/A')}")
        
        return final_state
        
    except Exception as e:
        main_logger.error(f"Pipeline failed: {str(e)}")
        
        if sqlite_checkpointer:
            sqlite_checkpointer.update_run_status(
                thread_id, "failed", 
                "unknown",
                0
            )
        
        tracer.checkpoint("pipeline_error", {"error": str(e)})
        tracer.save()
        raise


def run_pipeline_sync(
    video_path: str,
    subtitle_path: str,
    output_dir: str = "output",
    **kwargs
) -> Dict[str, Any]:
    """
    执行逻辑：
    1) 组织处理流程与依赖调用。
    2) 汇总中间结果并输出。
    实现方式：通过asyncio 异步调度实现。
    核心价值：编排流程，保证步骤顺序与可追踪性。
    输入参数：
    - video_path: 文件路径（类型：str）。
    - subtitle_path: 文件路径（类型：str）。
    - output_dir: 目录路径（类型：str）。
    - **kwargs: 可变参数，含义由调用方决定。
    输出参数：
    - 结构化结果字典（包含关键字段信息）。"""
    return asyncio.run(run_pipeline(
        video_path=video_path,
        subtitle_path=subtitle_path,
        output_dir=output_dir,
        **kwargs
    ))


def get_graph_mermaid() -> str:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    输入参数：
    - 无。
    输出参数：
    - 字符串结果。"""
    return """graph TD
    subgraph Phase1["Phase 1: 前期准备"]
        S1[Step 1: 原材料确认]
    end
    
    subgraph Phase2["Phase 2: 文字稿预处理"]
        S2[Step 2: 智能纠错]
        S3[Step 3: 语义合并]
        S4[Step 4: 局部冗余删除]
        S5[Step 5: 跨句冗余删除]
        S6[Step 6: 跨句冗余合并]
    end
    
    subgraph Phase3["Phase 3: 分片与断层"]
        S7[Step 7: 知识点分片]
        S8a[Step 8a: 断层粗定位]
        S8b[Step 8b: 断层精确定位]
    end
    
    subgraph Phase4["Phase 4: 截图指令"]
        S9[Step 9: 策略匹配]
        S10[Step 10: 时间计算]
        S11[Step 11: 指令生成]
    end
    
    subgraph Phase5["Phase 5: 截帧质控"]
        S12[Step 12: 截帧执行]
        S13[Step 13: 帧质量校验]
        S14[Step 14: Vision问答]
        S15[Step 15: 智能重试]
    end
    
    subgraph Phase6["Phase 6: 可视化判定"]
        S16[Step 16: 必要性判定]
        S17[Step 17: 形式选择]
        S18[Step 18: 核心内容]
        S19[Step 19: 辅助信息]
    end
    
    subgraph Phase7["Phase 7: 语义重构"]
        S20[Step 20: 素材整合]
        S21[Step 21: 语义重构]
        S22[Step 22: Markdown生成]
    end
    
    subgraph Phase8["Phase 8: 归档"]
        S23[Step 23: 视频命名]
        S24[Step 24: 截图命名]
    end
    
    S1 -->|valid| S2
    S1 -->|invalid| END[结束]
    S2 --> S3 --> S4 --> S5 --> S6
    S6 --> S7 --> S8a --> S8b
    S8b --> S9 --> S10 --> S11
    S11 --> S12 --> S13 --> S14 --> S15
    S15 --> S16 --> S17 --> S18 --> S19
    S19 --> S20 --> S21 --> S22
    S22 --> S23 --> S24 --> END
"""
