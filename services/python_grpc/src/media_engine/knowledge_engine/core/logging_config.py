"""
模块说明：视频转Markdown流程中的 logging_config 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

import logging
import json
import os
import sys
from datetime import datetime
from typing import Optional, Dict, Any

from services.python_grpc.src.common.logging import (
    DEGRADE_LEVEL,
    ensure_degrade_level,
    is_degrade_message,
)


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
        - 条件：hasattr(record, 'extra_data')
        - 条件：record.exc_info
        依据来源（证据链）：
        - 输入参数：record。
        输入参数：
        - record: 函数入参（类型：logging.LogRecord）。
        输出参数：
        - 字符串结果。"""
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        
        # 添加额外数据
        if hasattr(record, 'extra_data'):
            log_data["data"] = record.extra_data
        
        # 添加异常信息
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        return json.dumps(log_data, ensure_ascii=False)


class ColoredFormatter(logging.Formatter):
    """类说明：ColoredFormatter 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'DEGRADE': '\033[95m',   # Purple
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
    }
    RESET = '\033[0m'
    
    def format(self, record: logging.LogRecord) -> str:
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：stage
        - 条件：progress is not None
        依据来源（证据链）：
        输入参数：
        - record: 函数入参（类型：logging.LogRecord）。
        输出参数：
        - 字符串结果。"""
        color = self.COLORS.get(record.levelname, self.RESET)
        
        # 时间格式
        time_str = datetime.now().strftime("%H:%M:%S")
        
        # 阶段标签 (如果有)
        stage = getattr(record, 'stage', None)
        stage_str = f"[{stage}] " if stage else ""
        
        # 进度 (如果有)
        progress = getattr(record, 'progress', None)
        progress_str = f"{progress*100:.0f}% " if progress is not None else ""
        
        return f"{color}[{time_str}] {record.levelname:8}{self.RESET} {stage_str}{progress_str}{record.getMessage()}"


class PipelineLogger:
    """类说明：PipelineLogger 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, 
                 name: str = "pipeline",
                 level: int = logging.INFO,
                 log_file: Optional[str] = None,
                 json_output: bool = False):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新、JSON 解析/序列化、文件系统读写实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        决策逻辑：
        - 条件：log_file
        - 条件：json_output
        依据来源（证据链）：
        - 输入参数：json_output, log_file。
        输入参数：
        - name: 函数入参（类型：str）。
        - level: 函数入参（类型：int）。
        - log_file: 函数入参（类型：Optional[str]）。
        - json_output: 函数入参（类型：bool）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        ensure_degrade_level()
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers.clear()
        
        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ColoredFormatter())
        self.logger.addHandler(console_handler)
        
        # 文件处理器 (可选)
        if log_file:
            os.makedirs(os.path.dirname(log_file) or '.', exist_ok=True)
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            if json_output:
                file_handler.setFormatter(JSONFormatter())
            else:
                file_handler.setFormatter(logging.Formatter(
                    '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
                ))
            self.logger.addHandler(file_handler)
    
    def _log(self, level: int, message: str, 
             stage: str = None, progress: float = None, 
             data: Dict[str, Any] = None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：stage
        - 条件：progress is not None
        - 条件：data
        依据来源（证据链）：
        - 输入参数：data, progress, stage。
        输入参数：
        - level: 函数入参（类型：int）。
        - message: 文本内容（类型：str）。
        - stage: 函数入参（类型：str）。
        - progress: 函数入参（类型：float）。
        - data: 数据列表/集合（类型：Dict[str, Any]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        extra = {}
        if stage:
            extra['stage'] = stage
        if progress is not None:
            extra['progress'] = progress
        if data:
            extra['extra_data'] = data
        
        self.logger.log(level, message, extra=extra)
    
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
        self._log(logging.DEBUG, message, **kwargs)
    
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
        self._log(logging.INFO, message, **kwargs)
    
    def warning(self, message: str, **kwargs):
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
        if is_degrade_message(message):
            self.degrade(message, **kwargs)
            return
        self._log(logging.WARNING, message, **kwargs)
    
    def degrade(self, message: str, **kwargs):
        """
        执行逻辑：
        1) 输出降级处理日志。
        2) 用于区分普通 warning 与兜底/降级路径。
        实现方式：通过自定义 DEGRADE 日志级别输出。
        核心价值：提升线上排障效率，避免降级信息被普通告警淹没。
        输入参数：
        - message: 文本内容（类型：str）。
        - **kwargs: 可变参数，含义由调用方决定。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self._log(DEGRADE_LEVEL, message, **kwargs)

    def error(self, message: str, **kwargs):
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
        self._log(logging.ERROR, message, **kwargs)
    
    def critical(self, message: str, **kwargs):
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
        self._log(logging.CRITICAL, message, **kwargs)
    
    def stage_start(self, stage: str, message: str = None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - stage: 函数入参（类型：str）。
        - message: 文本内容（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        msg = message or f"开始 {stage}"
        self.info(msg, stage=stage, progress=0.0)
    
    def stage_progress(self, stage: str, progress: float, message: str):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - stage: 函数入参（类型：str）。
        - progress: 函数入参（类型：float）。
        - message: 文本内容（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.info(message, stage=stage, progress=progress)
    
    def stage_complete(self, stage: str, message: str = None, data: Dict = None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - stage: 函数入参（类型：str）。
        - message: 文本内容（类型：str）。
        - data: 数据列表/集合（类型：Dict）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        msg = message or f"{stage} 完成"
        self.info(msg, stage=stage, progress=1.0, data=data)
    
    def stage_error(self, stage: str, error: Exception, message: str = None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        输入参数：
        - stage: 函数入参（类型：str）。
        - error: 函数入参（类型：Exception）。
        - message: 文本内容（类型：str）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        msg = message or f"{stage} 失败: {error}"
        self.error(msg, stage=stage, data={"error": str(error)})


# 全局默认日志器
_default_logger: Optional[PipelineLogger] = None


def get_logger(name: str = "pipeline") -> PipelineLogger:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：_default_logger is None
    依据来源（证据链）：
    输入参数：
    - name: 函数入参（类型：str）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _default_logger
    if _default_logger is None:
        _default_logger = PipelineLogger(name)
    return _default_logger


def setup_logging(level: int = logging.INFO, 
                  log_file: str = None,
                  json_output: bool = False) -> PipelineLogger:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过JSON 解析/序列化实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    输入参数：
    - level: 函数入参（类型：int）。
    - log_file: 函数入参（类型：str）。
    - json_output: 函数入参（类型：bool）。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    global _default_logger
    _default_logger = PipelineLogger(
        name="pipeline",
        level=level,
        log_file=log_file,
        json_output=json_output
    )
    return _default_logger
