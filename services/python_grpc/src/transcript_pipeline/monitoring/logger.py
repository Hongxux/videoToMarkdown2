"""
模块说明：阶段监控 logger 的实现。
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
- 按步骤分离日志文件
- 彩色控制台输出
- JSON 结构化日志
- LLM调用详细记录"""

import logging
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from functools import lru_cache

from services.python_grpc.src.common.logging import (
    AutoDegradeFilter,
    ColorConsoleFormatter,
    ensure_degrade_level,
    is_degrade_message,
)

try:
    from rich.logging import RichHandler
    from rich.console import Console
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


# ============================================================================
# 自定义日志格式化器
# ============================================================================

class JSONFormatter(logging.Formatter):
    """类说明：JSONFormatter 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def format(self, record: logging.LogRecord) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：hasattr(record, 'data')
        - 条件：record.exc_info
        依据来源（证据链）：
        - 输入参数：record。
        输入参数：
        - record: 函数入参（类型：logging.LogRecord）。
        输出参数：
        - 字符串结果。"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "step": getattr(record, "step_name", "unknown"),
            "message": record.getMessage(),
        }
        
        # 添加额外字段
        if hasattr(record, "data"):
            log_entry["data"] = record.data
            
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
            
        return json.dumps(log_entry, ensure_ascii=False, default=str)


class DetailedFormatter(logging.Formatter):
    """类说明：DetailedFormatter 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def format(self, record: logging.LogRecord) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - record: 函数入参（类型：logging.LogRecord）。
        输出参数：
        - 字符串结果。"""
        step = getattr(record, "step_name", "MAIN")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"[{timestamp}] [{record.levelname:8}] [{step:20}] {record.getMessage()}"


# ============================================================================
# 步骤日志器
# ============================================================================

class StepLogger:
    """类说明：StepLogger 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    # 🔑 全局开关：控制是否创建文件日志
    ENABLE_FILE_OUTPUT = False
    
    def __init__(
        self, 
        step_name: str, 
        output_dir: Optional[Path] = None,
        log_level: int = logging.DEBUG,
        enable_file_output: bool = None  # 🔑 允许每个实例覆盖全局设置
    ):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：self.enable_file_output
        - 条件：output_dir
        - 条件：enable_file_output is not None
        依据来源（证据链）：
        - 输入参数：enable_file_output, output_dir。
        - 对象内部状态：self.enable_file_output。
        输入参数：
        - step_name: 函数入参（类型：str）。
        - output_dir: 目录路径（类型：Optional[Path]）。
        - log_level: 函数入参（类型：int）。
        - enable_file_output: 开关/状态（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.step_name = step_name
        self.output_dir = Path(output_dir) if output_dir else Path("output/logs")
        # 🔑 使用实例参数或全局开关
        self.enable_file_output = enable_file_output if enable_file_output is not None else StepLogger.ENABLE_FILE_OUTPUT
        
        if self.enable_file_output:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = self._setup_logger(log_level)
        self._start_time: Optional[datetime] = None
        self._llm_call_count = 0
        self._total_tokens = 0
        
    def _setup_logger(self, log_level: int) -> logging.Logger:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self.enable_file_output
        - 条件：RICH_AVAILABLE
        依据来源（证据链）：
        - 阈值常量：RICH_AVAILABLE。
        - 对象内部状态：self.enable_file_output。
        输入参数：
        - log_level: 函数入参（类型：int）。
        输出参数：
        - 函数计算/封装后的结果对象。"""
        ensure_degrade_level()
        logger = logging.getLogger(f"stage1.{self.step_name}")
        logger.setLevel(log_level)
        logger.handlers.clear()
        logger.propagate = False  # 防止传播到父logger导致重复输出
        
        # 🔑 仅当启用文件输出时创建文件处理器
        if self.enable_file_output:
            # 文件处理器 - JSON格式
            json_file = self.output_dir / f"{self.step_name}.jsonl"
            json_handler = logging.FileHandler(json_file, encoding="utf-8")
            json_handler.setFormatter(JSONFormatter())
            json_handler.setLevel(logging.DEBUG)
            logger.addHandler(json_handler)
            
            # 文件处理器 - 详细格式
            detail_file = self.output_dir / f"{self.step_name}.log"
            detail_handler = logging.FileHandler(detail_file, encoding="utf-8")
            detail_handler.setFormatter(DetailedFormatter())
            detail_handler.setLevel(logging.DEBUG)
            logger.addHandler(detail_handler)
        
        # 控制台处理器：统一使用彩色格式，确保 INFO/WARNING/ERROR/DEGRADE 可区分
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ColorConsoleFormatter())
        console_handler.setLevel(logging.INFO)
        console_handler.addFilter(AutoDegradeFilter())
        logger.addHandler(console_handler)

        return logger
    
    def start(self):
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
        self._start_time = datetime.now()
        self.logger.info(f"▶ Step [{self.step_name}] started")
        
    def end(self, success: bool = True):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：self._start_time
        - 条件：success
        依据来源（证据链）：
        - 输入参数：success。
        - 对象内部状态：self._start_time。
        输入参数：
        - success: 函数入参（类型：bool）。
        输出参数：
        - 结构化字典结果（包含字段：duration_ms, llm_calls, total_tokens）。"""
        duration = 0
        if self._start_time:
            duration = (datetime.now() - self._start_time).total_seconds() * 1000
            
        status = "✓ completed" if success else "✗ failed"
        self.logger.info(
            f"◀ Step [{self.step_name}] {status} "
            f"[{duration:.0f}ms, {self._llm_call_count} LLM calls, {self._total_tokens} tokens]"
        )
        
        return {
            "duration_ms": duration,
            "llm_calls": self._llm_call_count,
            "total_tokens": self._total_tokens
        }
        
    def log_input(self, data: Dict[str, Any], summary_only: bool = False):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：summary_only
        依据来源（证据链）：
        - 输入参数：summary_only。
        输入参数：
        - data: 数据列表/集合（类型：Dict[str, Any]）。
        - summary_only: 函数入参（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if summary_only:
            # 只记录摘要信息
            summary = self._create_summary(data)
            self.logger.info(f"[INPUT] {summary}")
        else:
            self.logger.debug(
                "[INPUT]",
                extra={"step_name": self.step_name, "data": data}
            )
            # 控制台显示摘要
            summary = self._create_summary(data)
            self.logger.info(f"[INPUT] {summary}")
    
    def log_output(self, data: Dict[str, Any], summary_only: bool = False):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：summary_only
        依据来源（证据链）：
        - 输入参数：summary_only。
        输入参数：
        - data: 数据列表/集合（类型：Dict[str, Any]）。
        - summary_only: 函数入参（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if summary_only:
            summary = self._create_summary(data)
            self.logger.info(f"[OUTPUT] {summary}")
        else:
            self.logger.debug(
                "[OUTPUT]",
                extra={"step_name": self.step_name, "data": data}
            )
            summary = self._create_summary(data)
            self.logger.info(f"[OUTPUT] {summary}")
            
    def log_llm_call(
        self, 
        prompt: str, 
        response: str, 
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        model: str = "unknown",
        latency_ms: float = 0
    ):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：len(prompt) > 500
        - 条件：len(response) > 500
        依据来源（证据链）：
        - 输入参数：prompt, response。
        输入参数：
        - prompt: 文本内容（类型：str）。
        - response: 函数入参（类型：str）。
        - prompt_tokens: 函数入参（类型：int）。
        - completion_tokens: 函数入参（类型：int）。
        - model: 模型/推理配置（类型：str）。
        - latency_ms: 函数入参（类型：float）。
        输出参数：
        - 结构化字典结果（包含字段：model, prompt_tokens, completion_tokens, total_tokens, latency_ms）。"""
        self._llm_call_count += 1
        total_tokens = prompt_tokens + completion_tokens
        self._total_tokens += total_tokens

        
        return {
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "latency_ms": latency_ms
        }
        
    def log_tool_call(self, tool_name: str, params: Dict[str, Any], result: Any):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - tool_name: 函数入参（类型：str）。
        - params: 函数入参（类型：Dict[str, Any]）。
        - result: 函数入参（类型：Any）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.logger.debug(
            f"[TOOL] {tool_name}",
            extra={
                "step_name": self.step_name,
                "data": {"params": params, "result": str(result)[:200]}
            }
        )
        self.logger.info(f"[TOOL] {tool_name} executed")
        
    def log_progress(self, current: int, total: int, message: str = ""):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：total > 0
        依据来源（证据链）：
        - 输入参数：total。
        输入参数：
        - current: 函数入参（类型：int）。
        - total: 函数入参（类型：int）。
        - message: 文本内容（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        progress = (current / total * 100) if total > 0 else 0
        self.logger.info(f"[PROGRESS] {current}/{total} ({progress:.1f}%) {message}")

    def log_batch_summary(
        self,
        *,
        total_batches: int,
        success_batches: int,
        failed_batches: int,
        total_tokens: int,
        total_latency_ms: float,
        failure_reasons: Optional[Dict[str, int]] = None,
        unit_label: str = "batch",
    ):
        """记录批处理汇总：总耗时、总token、成功/失败数、失败原因去重统计。"""
        self.logger.info(
            f"[BATCH_SUMMARY] unit={unit_label} total={total_batches} "
            f"success={success_batches} failed={failed_batches} "
            f"tokens={total_tokens} latency_ms={total_latency_ms:.0f}",
            extra={"step_name": self.step_name}
        )

        if failure_reasons:
            sorted_items = sorted(failure_reasons.items(), key=lambda item: item[1], reverse=True)
            reason_text = "; ".join([f"{reason} x{count}" for reason, count in sorted_items])
            self.logger.warning(
                f"[BATCH_FAILURE_REASONS] {reason_text}",
                extra={"step_name": self.step_name},
            )

    def log_substep(self, substep: str, message: str):
        """记录步骤内细粒度子阶段日志。"""
        self.logger.info(
            f"[SUBSTEP] {substep} | {message}",
            extra={"step_name": self.step_name, "substep": substep},
        )

    def log_degrade(self, message: str, context: Optional[Dict] = None):
        """显式记录降级处理日志。"""
        ensure_degrade_level()
        self.logger.log(
            35,
            message,
            extra={"step_name": self.step_name, "data": context},
        )
        
    def log_warning(self, message: str, context: Optional[Dict] = None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - message: 文本内容（类型：str）。
        - context: 函数入参（类型：Optional[Dict]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        if is_degrade_message(message):
            self.log_degrade(message, context=context)
            return

        self.logger.warning(message, extra={"step_name": self.step_name, "data": context})
        
    def log_error(self, error: Exception, context: Optional[Dict] = None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - error: 函数入参（类型：Exception）。
        - context: 函数入参（类型：Optional[Dict]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.logger.error(
            f"[ERROR] {type(error).__name__}: {str(error)}",
            extra={
                "step_name": self.step_name, 
                "data": {
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                    "context": context
                }
            },
            exc_info=True
        )
        
    def debug(self, message: str, **kwargs):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - message: 文本内容（类型：str）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.logger.debug(message, extra={"step_name": self.step_name, "data": kwargs})
        
    def info(self, message: str, **kwargs):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - message: 文本内容（类型：str）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.logger.info(message, extra={"step_name": self.step_name, "data": kwargs})
        
    def _create_summary(self, data: Dict[str, Any]) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部函数组合与条件判断实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：isinstance(value, list)
        - 条件：isinstance(value, dict)
        - 条件：isinstance(value, str) and len(value) > 50
        依据来源（证据链）：
        输入参数：
        - data: 数据列表/集合（类型：Dict[str, Any]）。
        输出参数：
        - 字符串结果。"""
        summary_parts = []
        for key, value in data.items():
            if isinstance(value, list):
                summary_parts.append(f"{key}=[{len(value)} items]")
            elif isinstance(value, dict):
                summary_parts.append(f"{key}={{...}}")
            elif isinstance(value, str) and len(value) > 50:
                summary_parts.append(f"{key}=\"{value[:50]}...\"")
            else:
                summary_parts.append(f"{key}={value}")
        return ", ".join(summary_parts)


# ============================================================================
# 全局日志配置
# ============================================================================

@lru_cache(maxsize=1)
def setup_logging(
    output_dir: str = "output/logs",
    log_level: int = logging.DEBUG
) -> logging.Logger:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过文件系统读写实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：RICH_AVAILABLE
    依据来源（证据链）：
    - 阈值常量：RICH_AVAILABLE。
    输入参数：
    - output_dir: 目录路径（类型：str）。
    - log_level: 函数入参（类型：int）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    ensure_degrade_level()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # 主日志器
    main_logger = logging.getLogger("stage1")
    main_logger.setLevel(log_level)
    main_logger.handlers.clear()
    
    # 主日志文件
    main_file = output_path / "pipeline.log"
    file_handler = logging.FileHandler(main_file, encoding="utf-8")
    file_handler.setFormatter(DetailedFormatter())
    main_logger.addHandler(file_handler)
    
    # 控制台
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(ColorConsoleFormatter())
    console.setLevel(logging.INFO)
    console.addFilter(AutoDegradeFilter())
    main_logger.addHandler(console)
    
    return main_logger


def get_logger(step_name: str, output_dir: str = "output/logs") -> StepLogger:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过文件系统读写实现。
    核心价值：提供一致读取接口，降低调用耦合。
    输入参数：
    - step_name: 函数入参（类型：str）。
    - output_dir: 目录路径（类型：str）。
    输出参数：
    - StepLogger 对象或调用结果。"""
    return StepLogger(step_name, Path(output_dir))
