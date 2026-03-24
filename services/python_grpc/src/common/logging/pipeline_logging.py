"""统一日志配置：彩色控制台、降级级别、结构化字段。"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Optional

_NOISY_HTTP_LOGGERS = (
    "httpx",
    "httpcore",
    "openai",
    "openai._base_client",
)

_ENABLE_HTTP_INFO_ENV = "PIPELINE_ENABLE_HTTP_INFO_LOGS"
_HTTP_LOG_LEVEL_ENV = "PIPELINE_HTTP_LOG_LEVEL"

DEGRADE_LEVEL = 35
_DEGRADE_NAME = "DEGRADE"

_DEGRADE_KEYWORDS = (
    "fallback",
    "degrade",
    "downgrade",
    "降级",
    "兜底",
)


def ensure_degrade_level() -> None:
    """注册 DEGRADE 日志级别并为 Logger 注入 degrade 方法。"""
    if logging.getLevelName(DEGRADE_LEVEL) != _DEGRADE_NAME:
        logging.addLevelName(DEGRADE_LEVEL, _DEGRADE_NAME)

    if not hasattr(logging.Logger, "degrade"):
        def _degrade(self: logging.Logger, message: str, *args, **kwargs):
            if self.isEnabledFor(DEGRADE_LEVEL):
                self._log(DEGRADE_LEVEL, message, args, **kwargs)

        setattr(logging.Logger, "degrade", _degrade)


def is_degrade_message(message: str) -> bool:
    """判断消息是否表达了降级处理语义。"""
    lower_message = str(message).lower()
    return any(keyword in lower_message for keyword in _DEGRADE_KEYWORDS)


class AutoDegradeFilter(logging.Filter):
    """将 warning 级别里的降级语义消息提升为 DEGRADE。"""

    def filter(self, record: logging.LogRecord) -> bool:
        """方法说明：AutoDegradeFilter.filter 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        if record.levelno == logging.WARNING and is_degrade_message(record.getMessage()):
            record.levelno = DEGRADE_LEVEL
            record.levelname = _DEGRADE_NAME
        return True


class ColorConsoleFormatter(logging.Formatter):
    """控制台彩色格式器，按级别上色并打印 step。"""

    COLORS = {
        "DEBUG": "\033[36m",
        "INFO": "\033[32m",
        "WARNING": "\033[33m",
        _DEGRADE_NAME: "\033[95m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[35m",
    }
    RESET = "\033[0m"

    def __init__(self, use_color: Optional[bool] = None):
        super().__init__()
        if use_color is None:
            no_color = os.getenv("NO_COLOR", "").strip().lower() in {"1", "true", "yes", "on"}
            use_color = (not no_color) and bool(getattr(sys.stdout, "isatty", lambda: False)())
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        """方法说明：ColorConsoleFormatter.format 核心方法。
        执行步骤：
        1) 步骤1：接收并校验输入参数，确保当前调用上下文有效。
        2) 步骤2：按方法职责执行核心处理逻辑，并维护必要的中间状态。
        3) 步骤3：返回处理结果或更新状态，供后续流程继续使用。"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = record.levelname
        step = getattr(record, "step_name", "MAIN")
        substep = getattr(record, "substep", "")
        progress = getattr(record, "progress", None)

        substep_segment = f"[{substep}]" if substep else ""
        progress_segment = f"[{progress}]" if progress else ""

        line = (
            f"[{timestamp}] [{level:8}] [{step:20}]"
            f"{substep_segment}{progress_segment} {record.getMessage()}"
        )
        if not self.use_color:
            return line

        color = self.COLORS.get(level, self.RESET)
        return f"{color}{line}{self.RESET}"


def configure_pipeline_logging(
    level: int = logging.INFO,
    *,
    fmt: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force: bool = True,
    stream: Optional[object] = None,
    suppress_http_info_logs: bool = True,
) -> None:
    """配置根日志并将控制台格式替换为彩色格式。"""
    ensure_degrade_level()

    logging.basicConfig(
        level=level,
        format=fmt,
        force=force,
        stream=stream,
    )

    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setFormatter(ColorConsoleFormatter())
        handler.addFilter(AutoDegradeFilter())

    if suppress_http_info_logs:
        enable_http_info = os.getenv(_ENABLE_HTTP_INFO_ENV, "").strip().lower() in {"1", "true", "yes", "on"}
        if not enable_http_info:
            http_log_level_name = os.getenv(_HTTP_LOG_LEVEL_ENV, "WARNING").strip().upper() or "WARNING"
            http_log_level = getattr(logging, http_log_level_name, logging.WARNING)
            if not isinstance(http_log_level, int):
                http_log_level = logging.WARNING
            for logger_name in _NOISY_HTTP_LOGGERS:
                logging.getLogger(logger_name).setLevel(http_log_level)


def get_pipeline_logger(name: Optional[str] = None) -> logging.Logger:
    """获取统一配置下的 logger。"""
    ensure_degrade_level()
    return logging.getLogger(name)


def log_degrade(logger: logging.Logger, message: str, **extra) -> None:
    """输出降级日志。"""
    ensure_degrade_level()
    logger.log(DEGRADE_LEVEL, message, extra=extra if extra else None)
