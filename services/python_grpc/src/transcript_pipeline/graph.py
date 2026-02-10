"""
模块说明：Stage1 Pipeline 图编排与运行入口（精简版，仅保留 step1~step6）。
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from .checkpoint import STEP_INDEX_MAP, SQLiteCheckpointer, generate_thread_id
from .monitoring.logger import setup_logging
from .monitoring.metrics import MetricsCollector
from .monitoring.tracer import PipelineTracer
from .nodes import step1_node, step2_node, step3_node, step4_node, step5_node, step6_node
from .state import PipelineState, create_initial_state


class StepOutputConfig:
    """步骤中间产物输出配置。"""

    REQUIRED_ENABLED_STEPS = {
        "step2_correction",
        "step6_merge_cross",
    }

    DEFAULT_ENABLED_STEPS = {
        "step2_correction",
        "step6_merge_cross",
    }

    FULL_PERSISTENCE_STEPS = {
        "step2_correction",
        "step6_merge_cross",
    }

    def __init__(
        self,
        output_dir: str = "output/intermediates",
        enabled_steps: Optional[List[str]] = None,
        enable_all: bool = False,
        disable_all: bool = False,
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        if disable_all:
            resolved_steps = set()
        elif enable_all:
            resolved_steps = set(STEP_INDEX_MAP.keys())
        elif enabled_steps is not None:
            resolved_steps = set(enabled_steps)
        else:
            resolved_steps = self.DEFAULT_ENABLED_STEPS.copy()

        self.enabled_steps = resolved_steps | self.REQUIRED_ENABLED_STEPS

    def should_output(self, step_name: str) -> bool:
        """方法说明：StepOutputConfig.should_output 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        return step_name in self.enabled_steps

    def save_step_output(self, step_name: str, state: Dict[str, Any]):
        """方法说明：StepOutputConfig.save_step_output 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if not self.should_output(step_name):
            return

        step_output = self._extract_step_output(step_name, state)
        output_file = self.output_dir / f"{step_name}_output.json"
        with open(output_file, "w", encoding="utf-8") as output_stream:
            json.dump(step_output, output_stream, ensure_ascii=False, indent=2, default=str)

    def _extract_step_output(self, step_name: str, state: Dict[str, Any]) -> Dict[str, Any]:
        """方法说明：StepOutputConfig._extract_step_output 工具方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        step_io_map = {
            "step1_validate": {
                "input": ["video_path", "subtitle_path"],
                "output": ["is_valid", "domain", "main_topic", "video_title"],
            },
            "step2_correction": {
                "input": ["domain", "subtitle_path"],
                "output": ["corrected_subtitles", "correction_summary"],
            },
            "step3_merge": {
                "input": ["corrected_subtitles"],
                "output": ["merged_sentences"],
            },
            "step4_clean_local": {
                "input": ["merged_sentences"],
                "output": ["cleaned_sentences"],
            },
            "step5_clean_cross": {
                "input": ["cleaned_sentences", "main_topic"],
                "output": ["non_redundant_sentences"],
            },
            "step6_merge_cross": {
                "input": ["non_redundant_sentences"],
                "output": ["pure_text_script"],
            },
        }

        io_spec = step_io_map.get(step_name, {"input": [], "output": []})
        result = {"step": step_name, "input": {}, "output": {}}

        for field in io_spec["input"]:
            if field in state:
                value = state[field]
                if isinstance(value, list) and len(value) > 10:
                    result["input"][field] = f"<{len(value)} items>"
                else:
                    result["input"][field] = value

        for field in io_spec["output"]:
            if field in state:
                value = state[field]
                should_compact_list = (
                    isinstance(value, list)
                    and len(value) > 10
                    and step_name not in self.FULL_PERSISTENCE_STEPS
                )
                if should_compact_list:
                    result["output"][field] = {
                        "count": len(value),
                        "sample": value[:2],
                    }
                else:
                    result["output"][field] = value

        if "step_timings" in state and step_name in state.get("step_timings", {}):
            result["duration_ms"] = state["step_timings"][step_name]
        if "token_usage" in state and step_name in state.get("token_usage", {}):
            result["tokens"] = state["token_usage"][step_name]

        return result


def create_checkpointed_node(
    node_func,
    step_name: str,
    checkpointer: Optional[SQLiteCheckpointer] = None,
    output_config: Optional[StepOutputConfig] = None,
):
    """方法说明：create_checkpointed_node 核心方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    async def wrapper(state: PipelineState) -> Dict[str, Any]:
        step_index = STEP_INDEX_MAP.get(step_name, 0)
        last_index = state.get("_last_completed_index", -1)

        if state.get("_resume_mode") and step_index <= last_index:
            return {}

        result = await node_func(state)
        merged_state = {**state, **result}

        if checkpointer:
            thread_id = state.get("_thread_id", "default")
            checkpointer.save_checkpoint(thread_id, step_name, step_index, merged_state)

        if output_config:
            output_config.save_step_output(step_name, merged_state)

        return result

    return wrapper


STEP_NAME_TO_NUMBER = {
    "step1_validate": 1,
    "step2_correction": 2,
    "step3_merge": 3,
    "step4_clean_local": 4,
    "step5_clean_cross": 5,
    "step6_merge_cross": 6,
}


def create_pipeline_graph(
    checkpointer: Optional[Any] = None,
    sqlite_checkpointer: Optional[SQLiteCheckpointer] = None,
    output_config: Optional[StepOutputConfig] = None,
    max_step: int = 6,
) -> StateGraph:
    """方法说明：create_pipeline_graph 核心方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    graph = StateGraph(PipelineState)

    def add_node(name: str, func):
        if sqlite_checkpointer or output_config:
            wrapped = create_checkpointed_node(func, name, sqlite_checkpointer, output_config)
            graph.add_node(name, wrapped)
        else:
            graph.add_node(name, func)

    add_node("step1_validate", step1_node)
    add_node("step2_correction", step2_node)
    add_node("step3_merge", step3_node)
    add_node("step4_clean_local", step4_node)
    add_node("step5_clean_cross", step5_node)
    add_node("step6_merge_cross", step6_node)

    def should_continue_after_step1(state: PipelineState) -> str:
        if not state.get("is_valid", False):
            return "end"
        return "step2_correction"

    graph.add_conditional_edges(
        "step1_validate",
        should_continue_after_step1,
        {"step2_correction": "step2_correction", "end": END},
    )

    terminal_step = None
    max_step = min(max_step, 6)
    if max_step < 6:
        step_order = [
            "step1_validate",
            "step2_correction",
            "step3_merge",
            "step4_clean_local",
            "step5_clean_cross",
            "step6_merge_cross",
        ]
        for step_name in step_order:
            if STEP_NAME_TO_NUMBER.get(step_name, 99) == max_step:
                terminal_step = step_name

    edges = [
        ("step2_correction", "step3_merge"),
        ("step3_merge", "step4_clean_local"),
        ("step4_clean_local", "step5_clean_cross"),
        ("step5_clean_cross", "step6_merge_cross"),
        ("step6_merge_cross", END),
    ]

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


async def run_pipeline(
    video_path: str,
    subtitle_path: str,
    output_dir: str = "output",
    enable_checkpoints: bool = False,
    enable_sqlite: bool = False,
    enable_logging: bool = False,
    enable_traces: bool = False,
    enable_metrics: bool = False,
    resume: bool = False,
    output_steps: Optional[List[str]] = None,
    output_all_steps: bool = False,
    thread_id: Optional[str] = None,
    max_step: int = 6,
) -> Dict[str, Any]:
    """方法说明：run_pipeline 核心方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    if enable_logging:
        main_logger = setup_logging(f"{output_dir}/logs")
    else:
        main_logger = logging.getLogger("stage1_pipeline")

    max_step = min(max_step, 6)
    main_logger.info(f"Starting Stage1 Pipeline (max_step={max_step})")
    main_logger.info(f"Video: {video_path}")
    main_logger.info(f"Subtitle: {subtitle_path}")

    if thread_id is None:
        thread_id = generate_thread_id(video_path, subtitle_path)
    main_logger.info(f"Thread ID: {thread_id}")

    tracer = PipelineTracer(Path(output_dir) / "traces") if enable_traces else None
    metrics = MetricsCollector(Path(output_dir) / "metrics") if enable_metrics else None

    output_config = StepOutputConfig(
        output_dir=f"{output_dir}/intermediates",
        enabled_steps=output_steps,
        enable_all=output_all_steps,
    )
    main_logger.info(f"Intermediate outputs: {len(output_config.enabled_steps)} steps enabled")

    sqlite_checkpointer = None
    if enable_sqlite:
        db_path = Path(output_dir) / "checkpoints.db"
        sqlite_checkpointer = SQLiteCheckpointer(str(db_path))
        sqlite_checkpointer.start_run(thread_id, video_path, subtitle_path, output_dir)
        main_logger.info(f"Metadata tracking enabled: {db_path}")

    checkpointer = MemorySaver() if enable_checkpoints else None
    if checkpointer:
        main_logger.info("Memory checkpoints enabled")
    else:
        main_logger.info("Checkpoints disabled")

    graph = create_pipeline_graph(
        checkpointer=checkpointer,
        sqlite_checkpointer=sqlite_checkpointer,
        output_config=output_config,
        max_step=max_step,
    )

    return await _execute_pipeline(
        graph,
        video_path,
        subtitle_path,
        output_dir,
        thread_id,
        resume,
        tracer,
        metrics,
        sqlite_checkpointer,
        main_logger,
    )


async def _execute_pipeline(
    graph,
    video_path,
    subtitle_path,
    output_dir,
    thread_id,
    resume,
    tracer,
    metrics,
    sqlite_checkpointer,
    main_logger,
):
    """方法说明：_execute_pipeline 工具方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    initial_state = create_initial_state(video_path, subtitle_path, output_dir)
    initial_state["_thread_id"] = thread_id

    config = {"configurable": {"thread_id": thread_id}}

    if resume and sqlite_checkpointer:
        last_checkpoint = sqlite_checkpointer.load_checkpoint(thread_id)
        if last_checkpoint:
            last_checkpoint["video_path"] = video_path
            last_checkpoint["subtitle_path"] = subtitle_path
            last_checkpoint["output_dir"] = output_dir
            last_checkpoint["_thread_id"] = thread_id

            last_checkpoint["_resume_mode"] = True
            last_step = sqlite_checkpointer.get_last_completed_step(thread_id)
            last_checkpoint["_last_completed_index"] = STEP_INDEX_MAP.get(last_step, 0)

            initial_state = last_checkpoint
            main_logger.info(
                f"Resume mode: loaded state from step '{last_step}' "
                f"(index {last_checkpoint['_last_completed_index']})"
            )
        else:
            main_logger.warning(f"No checkpoint found for thread {thread_id}, starting from scratch.")

    try:
        if tracer:
            tracer.checkpoint("pipeline_start", {"video": video_path, "thread_id": thread_id, "resume": resume})

        final_state = await graph.ainvoke(initial_state, config)

        if sqlite_checkpointer:
            sqlite_checkpointer.update_run_status(thread_id, "completed", "step6_merge_cross", 6)

        if tracer:
            tracer.checkpoint("pipeline_end", {"status": "success"})
            tracer.save()
        if metrics:
            metrics.save()
            metrics.print_summary()

        main_logger.info("Pipeline completed successfully!")
        main_logger.info(f"Output: {final_state.get('pure_text_script', 'N/A')}")
        return final_state

    except Exception as error:
        main_logger.error(f"Pipeline failed: {str(error)}")

        if sqlite_checkpointer:
            sqlite_checkpointer.update_run_status(thread_id, "failed", "unknown", 0)

        if tracer:
            tracer.checkpoint("pipeline_error", {"error": str(error)})
            tracer.save()
        raise


def run_pipeline_sync(
    video_path: str,
    subtitle_path: str,
    output_dir: str = "output",
    **kwargs,
) -> Dict[str, Any]:
    """方法说明：run_pipeline_sync 核心方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    return asyncio.run(
        run_pipeline(
            video_path=video_path,
            subtitle_path=subtitle_path,
            output_dir=output_dir,
            **kwargs,
        )
    )


def get_graph_mermaid() -> str:
    """方法说明：get_graph_mermaid 核心方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    return """graph TD
    S1[Step 1: validate] -->|valid| S2[Step 2: correction]
    S1 -->|invalid| END[END]
    S2 --> S3[Step 3: merge]
    S3 --> S4[Step 4: clean_local]
    S4 --> S5[Step 5: clean_cross]
    S5 --> S6[Step 6: merge_cross]
    S6 --> END
"""
