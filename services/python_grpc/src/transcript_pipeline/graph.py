"""
模块说明：Stage1 Pipeline 图编排与运行入口（精简版，仅保留 step1~step6）。
"""

import asyncio
import inspect
import json
import logging
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from langgraph.graph import StateGraph

from .checkpoint import STEP_INDEX_MAP, SQLiteCheckpointer, generate_thread_id
from .monitoring.logger import setup_logging
from .monitoring.metrics import MetricsCollector
from .monitoring.tracer import PipelineTracer
from .nodes import step1_node, step2_node, step3_node, step3_5_node, step4_node, step5_6_node
from .state import PipelineState, create_initial_state
from .streaming_executor import (
    StreamingStage1Graph,
    should_use_streaming_stage1_executor,
)

_LANGGRAPH_END_SYMBOL = None
_LANGGRAPH_STATE_GRAPH_CLASS = None
_PYDANTIC_MODEL_SCHEMA_PATCHED = False


def _patch_pydantic_model_schema_for_generic_origin() -> bool:
    global _PYDANTIC_MODEL_SCHEMA_PATCHED
    if _PYDANTIC_MODEL_SCHEMA_PATCHED:
        return True
    try:
        from pydantic_core import core_schema
    except Exception:
        return False

    try:
        signature = inspect.signature(core_schema.model_schema)
    except Exception:
        return False

    if "generic_origin" in signature.parameters:
        _PYDANTIC_MODEL_SCHEMA_PATCHED = True
        return True

    original_model_schema = core_schema.model_schema

    def _compat_model_schema(
        cls: Any,
        schema: Any,
        *,
        generic_origin: Any = None,
        **kwargs: Any,
    ) -> Any:
        _ = generic_origin
        return original_model_schema(cls, schema, **kwargs)

    core_schema.model_schema = _compat_model_schema
    _PYDANTIC_MODEL_SCHEMA_PATCHED = True
    logging.getLogger("stage1_pipeline").warning(
        "Applied pydantic_core.model_schema compatibility shim: generic_origin is ignored."
    )
    return True


def _is_generic_origin_mismatch(import_error: Exception) -> bool:
    return "generic_origin" in str(import_error)


def _load_langgraph_symbols() -> tuple[Any, Any]:
    global _LANGGRAPH_END_SYMBOL
    global _LANGGRAPH_STATE_GRAPH_CLASS
    if _LANGGRAPH_END_SYMBOL is not None and _LANGGRAPH_STATE_GRAPH_CLASS is not None:
        return _LANGGRAPH_END_SYMBOL, _LANGGRAPH_STATE_GRAPH_CLASS

    try:
        from langgraph.graph import END as end_symbol
        from langgraph.graph import StateGraph as state_graph_class
    except TypeError as import_error:
        if not _is_generic_origin_mismatch(import_error):
            raise
        if not _patch_pydantic_model_schema_for_generic_origin():
            raise RuntimeError(
                "Failed to patch pydantic compatibility for langgraph import."
            ) from import_error
        from langgraph.graph import END as end_symbol
        from langgraph.graph import StateGraph as state_graph_class

    _LANGGRAPH_END_SYMBOL = end_symbol
    _LANGGRAPH_STATE_GRAPH_CLASS = state_graph_class
    return end_symbol, state_graph_class


def _load_memory_saver_class() -> Any:
    try:
        from langgraph.checkpoint.memory import MemorySaver
    except TypeError as import_error:
        if not _is_generic_origin_mismatch(import_error):
            raise
        if not _patch_pydantic_model_schema_for_generic_origin():
            raise RuntimeError(
                "Failed to patch pydantic compatibility for MemorySaver import."
            ) from import_error
        from langgraph.checkpoint.memory import MemorySaver
    return MemorySaver


class StepOutputConfig:
    """步骤中间产物输出配置。"""

    STEP_NAME_ALIASES = {
        "step5_clean_cross": "step5_6_dedup_merge",
        "step6_merge_cross": "step5_6_dedup_merge",
    }

    OUTPUT_FILE_STEP_ALIASES = {
        "step5_6_dedup_merge": "step6_merge_cross",
    }

    REQUIRED_ENABLED_STEPS = {
        "step2_correction",
        "step5_6_dedup_merge",
    }

    DEFAULT_ENABLED_STEPS = {
        "step2_correction",
        "step5_6_dedup_merge",
    }

    FULL_PERSISTENCE_STEPS = {
        "step2_correction",
        "step3_merge",
        "step3_5_translate",
        "step4_clean_local",
        "step5_6_dedup_merge",
    }

    @classmethod
    def _canonical_step_name(cls, step_name: str) -> str:
        normalized = str(step_name or "").strip()
        return cls.STEP_NAME_ALIASES.get(normalized, normalized)

    def __init__(
        self,
        output_dir: str = "output/intermediates",
        enabled_steps: Optional[List[str]] = None,
        enable_all: bool = False,
        disable_all: bool = False,
        async_write: bool = False,
        write_scope_key: str = "",
    ):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.async_write = bool(async_write)
        self.write_scope_key = str(write_scope_key or "").strip()

        if disable_all:
            resolved_steps = set()
        elif enable_all:
            resolved_steps = {self._canonical_step_name(name) for name in STEP_INDEX_MAP.keys()}
        elif enabled_steps is not None:
            resolved_steps = {self._canonical_step_name(name) for name in enabled_steps}
        else:
            resolved_steps = self.DEFAULT_ENABLED_STEPS.copy()

        self.enabled_steps = resolved_steps | self.REQUIRED_ENABLED_STEPS

    def should_output(self, step_name: str) -> bool:
        """方法说明：StepOutputConfig.should_output 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        canonical_name = self._canonical_step_name(step_name)
        return canonical_name in self.enabled_steps

    def save_step_output(self, step_name: str, state: Dict[str, Any]):
        """方法说明：StepOutputConfig.save_step_output 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        canonical_name = self._canonical_step_name(step_name)
        if not self.should_output(canonical_name):
            return

        step_output = self._extract_step_output(canonical_name, state)
        output_step_name = self.OUTPUT_FILE_STEP_ALIASES.get(canonical_name, canonical_name)
        output_file = self.output_dir / f"{output_step_name}_output.json"

        if self.async_write:
            from services.python_grpc.src.common.utils.async_disk_writer import enqueue_json_write

            enqueue_json_write(
                str(output_file),
                step_output,
                ensure_ascii=False,
                indent=2,
                scope_key=self.write_scope_key,
            )
            return

        with open(output_file, "w", encoding="utf-8") as output_stream:
            json.dump(step_output, output_stream, ensure_ascii=False, indent=2, default=str)

    @staticmethod
    def _sanitize_output_field(step_name: str, field: str, value: Any) -> Any:
        """在写盘前清洗步骤输出，避免暴露不需要的字段。"""
        if (
            step_name == "step2_correction"
            and field == "corrected_subtitles"
            and isinstance(value, list)
        ):
            sanitized_items = []
            for item in value:
                if isinstance(item, dict):
                    sanitized_items.append({k: v for k, v in item.items() if k != "corrections"})
                else:
                    sanitized_items.append(item)
            return sanitized_items
        return value

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
            "step3_5_translate": {
                "input": ["merged_sentences"],
                "output": ["translated_sentences"],
            },
            "step4_clean_local": {
                "input": ["translated_sentences", "merged_sentences"],
                "output": ["cleaned_sentences"],
            },
            "step5_6_dedup_merge": {
                "input": ["cleaned_sentences", "main_topic"],
                "output": ["non_redundant_sentences", "pure_text_script"],
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
                value = self._sanitize_output_field(step_name, field, state[field])
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
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    max_step: int = 6,
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

        if progress_callback:
            try:
                completed = max(0, min(int(step_index), int(max_step)))
                pending = max(0, int(max_step) - completed)
                progress_callback(
                    {
                        "event": "step_completed",
                        "stage": "stage1",
                        "step_name": step_name,
                        "checkpoint": step_name,
                        "completed": completed,
                        "pending": pending,
                        "status": "running",
                        "timestamp_ms": int(time.time() * 1000),
                    }
                )
            except Exception as callback_error:
                logging.getLogger("stage1_pipeline").warning(
                    "Stage1 progress callback failed at %s: %s",
                    step_name,
                    callback_error,
                )

        return result

    return wrapper


STEP_NAME_TO_NUMBER = {
    "step1_validate": 1,
    "step2_correction": 2,
    "step3_merge": 3,
    "step3_5_translate": 4,
    "step4_clean_local": 4,
    "step5_clean_cross": 5,
    "step6_merge_cross": 6,
    "step5_6_dedup_merge": 6,
}


def create_pipeline_graph(
    checkpointer: Optional[Any] = None,
    sqlite_checkpointer: Optional[SQLiteCheckpointer] = None,
    output_config: Optional[StepOutputConfig] = None,
    max_step: int = 6,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> "StateGraph":
    """方法说明：create_pipeline_graph 核心方法。
    执行步骤：
    1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
    2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
    3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
    end_symbol, state_graph_class = _load_langgraph_symbols()
    graph = state_graph_class(PipelineState)

    def add_node(name: str, func):
        if sqlite_checkpointer or output_config:
            wrapped = create_checkpointed_node(
                func,
                name,
                sqlite_checkpointer,
                output_config,
                progress_callback=progress_callback,
                max_step=max_step,
            )
            graph.add_node(name, wrapped)
        else:
            graph.add_node(name, func)

    add_node("step1_validate", step1_node)
    add_node("step2_correction", step2_node)
    add_node("step3_merge", step3_node)
    add_node("step3_5_translate", step3_5_node)
    add_node("step4_clean_local", step4_node)
    add_node("step5_6_dedup_merge", step5_6_node)

    def should_continue_after_step1(state: PipelineState) -> str:
        if not state.get("is_valid", False):
            return "end"
        return "step2_correction"

    graph.add_conditional_edges(
        "step1_validate",
        should_continue_after_step1,
        {"step2_correction": "step2_correction", "end": end_symbol},
    )

    terminal_step = None
    max_step = min(max_step, 6)
    if max_step < 6:
        step_order = [
            "step1_validate",
            "step2_correction",
            "step3_merge",
            "step3_5_translate",
            "step4_clean_local",
            "step5_6_dedup_merge",
        ]
        for step_name in step_order:
            if STEP_NAME_TO_NUMBER.get(step_name, 99) == max_step:
                terminal_step = step_name

    edges = [
        ("step2_correction", "step3_merge"),
        ("step3_merge", "step3_5_translate"),
        ("step3_5_translate", "step4_clean_local"),
        ("step4_clean_local", "step5_6_dedup_merge"),
        ("step5_6_dedup_merge", end_symbol),
    ]

    if terminal_step:
        edges = [
            (src, end_symbol if src == terminal_step else dst)
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
    resume_state: Optional[Dict[str, Any]] = None,
    resume_from_step: Optional[str] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
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
        async_write=str(os.getenv("TRANSCRIPT_ASYNC_PERSIST_WRITES", "1")).strip().lower()
        not in {"0", "false", "no", "off"},
        write_scope_key=str(Path(output_dir).resolve()),
    )
    main_logger.info(f"Intermediate outputs: {len(output_config.enabled_steps)} steps enabled")

    sqlite_checkpointer = None
    if enable_sqlite:
        db_path = Path(output_dir) / "checkpoints.db"
        sqlite_checkpointer = SQLiteCheckpointer(str(db_path))
        sqlite_checkpointer.start_run(thread_id, video_path, subtitle_path, output_dir)
        main_logger.info(f"Metadata tracking enabled: {db_path}")

    checkpointer = None
    if enable_checkpoints:
        try:
            # 仅在启用内存检查点时加载该依赖，避免无关场景被第三方版本冲突阻塞启动。
            memory_saver_class = _load_memory_saver_class()
            checkpointer = memory_saver_class()
        except Exception as import_error:
            raise RuntimeError(
                "Failed to initialize in-memory checkpointer. "
                "Please verify langgraph/langchain/pydantic dependency compatibility."
            ) from import_error
    if checkpointer:
        main_logger.info("Memory checkpoints enabled")
    else:
        main_logger.info("Checkpoints disabled")

    use_streaming_executor, streaming_reason = should_use_streaming_stage1_executor(
        max_step=max_step,
        resume=resume,
        resume_state=resume_state,
        resume_from_step=resume_from_step,
        enable_checkpoints=enable_checkpoints,
    )
    if use_streaming_executor:
        main_logger.info("Stage1 streaming executor enabled")
        graph = StreamingStage1Graph(
            sqlite_checkpointer=sqlite_checkpointer,
            output_config=output_config,
            progress_callback=progress_callback,
            max_step=max_step,
        )
    else:
        main_logger.info("Stage1 streaming executor disabled: %s", streaming_reason)
        graph = create_pipeline_graph(
            checkpointer=checkpointer,
            sqlite_checkpointer=sqlite_checkpointer,
            output_config=output_config,
            max_step=max_step,
            progress_callback=progress_callback,
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
        resume_state,
        resume_from_step,
        progress_callback,
        max_step,
    )


def _raise_if_final_state_failed(final_state: Dict[str, Any]) -> None:
    """统一校验流水线最终状态，避免“失败状态被当作成功返回”."""
    status = str(final_state.get("current_step_status") or "").strip().lower()
    is_valid = final_state.get("is_valid")
    errors = final_state.get("errors")

    if status in {"failed", "error"}:
        step_name = str(final_state.get("current_step") or "unknown")
        raise RuntimeError(
            f"Pipeline ended with failed step: step={step_name}, status={status}, errors={errors!r}"
        )

    if is_valid is False:
        step_name = str(final_state.get("current_step") or "unknown")
        raise RuntimeError(
            f"Pipeline ended with invalid state: step={step_name}, is_valid={is_valid}, errors={errors!r}"
        )

    if isinstance(errors, list) and errors:
        step_name = str(final_state.get("current_step") or "unknown")
        raise RuntimeError(
            f"Pipeline ended with non-empty errors: step={step_name}, errors={errors!r}"
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
    resume_state: Optional[Dict[str, Any]] = None,
    resume_from_step: Optional[str] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    max_step: int = 6,
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

    if resume_state and isinstance(resume_state, dict):
        initial_state.update(resume_state)
        main_logger.info(
            "Resume state injected: fields=%s",
            sorted(resume_state.keys()),
        )

    normalized_resume_from_step = str(resume_from_step or "").strip()
    if normalized_resume_from_step:
        resume_index = STEP_INDEX_MAP.get(normalized_resume_from_step, 0)
        if resume_index > 0:
            initial_state["_resume_mode"] = True
            initial_state["_last_completed_index"] = max(
                int(initial_state.get("_last_completed_index", -1)),
                resume_index,
            )
            main_logger.info(
                "Resume from step override: step=%s index=%s",
                normalized_resume_from_step,
                initial_state["_last_completed_index"],
            )
        else:
            main_logger.warning(
                "Ignore unknown resume_from_step=%s",
                normalized_resume_from_step,
            )

    try:
        if progress_callback:
            try:
                progress_callback(
                    {
                        "event": "pipeline_start",
                        "stage": "stage1",
                        "checkpoint": "pipeline_start",
                        "step_name": "pipeline_start",
                        "completed": 0,
                        "pending": max(0, int(max_step)),
                        "status": "running",
                        "timestamp_ms": int(time.time() * 1000),
                    }
                )
            except Exception as callback_error:
                main_logger.warning("Stage1 progress callback start event failed: %s", callback_error)

        if tracer:
            tracer.checkpoint("pipeline_start", {"video": video_path, "thread_id": thread_id, "resume": resume})

        final_state = await graph.ainvoke(initial_state, config)
        _raise_if_final_state_failed(final_state)

        if sqlite_checkpointer:
            last_step = str(final_state.get("current_step") or "step5_6_dedup_merge")
            completed_index = STEP_INDEX_MAP.get(last_step, STEP_INDEX_MAP.get("step5_6_dedup_merge", 6))
            sqlite_checkpointer.update_run_status(thread_id, "completed", last_step, completed_index)

        if tracer:
            tracer.checkpoint("pipeline_end", {"status": "success"})
            tracer.save()
        if metrics:
            metrics.save()
            metrics.print_summary()

        if progress_callback:
            try:
                progress_callback(
                    {
                        "event": "pipeline_end",
                        "stage": "stage1",
                        "checkpoint": str(final_state.get("current_step") or "step5_6_dedup_merge"),
                        "step_name": str(final_state.get("current_step") or "step5_6_dedup_merge"),
                        "completed": max(0, int(max_step)),
                        "pending": 0,
                        "status": "completed",
                        "timestamp_ms": int(time.time() * 1000),
                    }
                )
            except Exception as callback_error:
                main_logger.warning("Stage1 progress callback end event failed: %s", callback_error)

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
        if progress_callback:
            try:
                progress_callback(
                    {
                        "event": "pipeline_error",
                        "stage": "stage1",
                        "checkpoint": str(initial_state.get("current_step") or "unknown"),
                        "step_name": str(initial_state.get("current_step") or "unknown"),
                        "completed": 0,
                        "pending": max(0, int(max_step)),
                        "status": "failed",
                        "error": str(error),
                        "timestamp_ms": int(time.time() * 1000),
                    }
                )
            except Exception as callback_error:
                main_logger.warning("Stage1 progress callback error event failed: %s", callback_error)
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
    S3 --> S35[Step 3.5: translate]
    S35 --> S4[Step 4: clean_local]
    S4 --> S56[Step 5+6: dedup_merge]
    S56 --> END
"""
