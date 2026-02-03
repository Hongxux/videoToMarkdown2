"""
分层日志系统
支持：
- 按步骤分离日志文件
- 彩色控制台输出
- JSON 结构化日志
- LLM调用详细记录
"""

import logging
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from functools import lru_cache

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
    """JSON格式日志"""
    
    def format(self, record: logging.LogRecord) -> str:
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
    """详细格式日志（用于调试）"""
    
    def format(self, record: logging.LogRecord) -> str:
        step = getattr(record, "step_name", "MAIN")
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        return f"[{timestamp}] [{record.levelname:8}] [{step:20}] {record.getMessage()}"


# ============================================================================
# 步骤日志器
# ============================================================================

class StepLogger:
    """
    为每个管道步骤提供独立的日志器
    
    特性：
    - 自动记录输入/输出
    - LLM调用追踪
    - 错误上下文记录
    - 性能计时
    """
    
    # 🔑 全局开关：控制是否创建文件日志
    ENABLE_FILE_OUTPUT = False
    
    def __init__(
        self, 
        step_name: str, 
        output_dir: Optional[Path] = None,
        log_level: int = logging.DEBUG,
        enable_file_output: bool = None  # 🔑 允许每个实例覆盖全局设置
    ):
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
        """设置日志器"""
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
        
        # 控制台处理器
        if RICH_AVAILABLE:
            console_handler = RichHandler(
                console=Console(force_terminal=True),
                show_time=True,
                show_path=False,
                markup=True
            )
        else:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(DetailedFormatter())
        console_handler.setLevel(logging.INFO)
        logger.addHandler(console_handler)
        
        return logger
    
    def start(self):
        """标记步骤开始"""
        self._start_time = datetime.now()
        self.logger.info(f"▶ Step [{self.step_name}] started")
        
    def end(self, success: bool = True):
        """标记步骤结束"""
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
        """记录步骤输入"""
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
        """记录步骤输出"""
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
        """记录 LLM 调用详情"""
        self._llm_call_count += 1
        total_tokens = prompt_tokens + completion_tokens
        self._total_tokens += total_tokens
        
        self.logger.debug(
            f"[LLM #{self._llm_call_count}]",
            extra={
                "step_name": self.step_name,
                "data": {
                    "model": model,
                    "prompt_preview": prompt[:500] + "..." if len(prompt) > 500 else prompt,
                    "response_preview": response[:500] + "..." if len(response) > 500 else response,
                    "prompt_tokens": prompt_tokens,
                    "completion_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                    "latency_ms": latency_ms
                }
            }
        )
        
        self.logger.info(
            f"[LLM] {model} | {prompt_tokens}+{completion_tokens}={total_tokens} tokens | {latency_ms:.0f}ms"
        )
        
        return {
            "model": model,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
            "latency_ms": latency_ms
        }
        
    def log_tool_call(self, tool_name: str, params: Dict[str, Any], result: Any):
        """记录工具调用"""
        self.logger.debug(
            f"[TOOL] {tool_name}",
            extra={
                "step_name": self.step_name,
                "data": {"params": params, "result": str(result)[:200]}
            }
        )
        self.logger.info(f"[TOOL] {tool_name} executed")
        
    def log_progress(self, current: int, total: int, message: str = ""):
        """记录进度"""
        progress = (current / total * 100) if total > 0 else 0
        self.logger.info(f"[PROGRESS] {current}/{total} ({progress:.1f}%) {message}")
        
    def log_warning(self, message: str, context: Optional[Dict] = None):
        """记录警告"""
        self.logger.warning(
            message,
            extra={"step_name": self.step_name, "data": context}
        )
        
    def log_error(self, error: Exception, context: Optional[Dict] = None):
        """记录错误"""
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
        """调试日志"""
        self.logger.debug(message, extra={"step_name": self.step_name, "data": kwargs})
        
    def info(self, message: str, **kwargs):
        """信息日志"""
        self.logger.info(message, extra={"step_name": self.step_name, "data": kwargs})
        
    def _create_summary(self, data: Dict[str, Any]) -> str:
        """创建数据摘要"""
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
    """设置全局日志"""
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
    if RICH_AVAILABLE:
        console = RichHandler(show_time=True, show_path=False)
    else:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(DetailedFormatter())
    console.setLevel(logging.INFO)
    main_logger.addHandler(console)
    
    return main_logger


def get_logger(step_name: str, output_dir: str = "output/logs") -> StepLogger:
    """获取步骤日志器"""
    return StepLogger(step_name, Path(output_dir))
