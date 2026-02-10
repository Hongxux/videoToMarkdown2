"""
模块说明：视频转Markdown流程中的 processing 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""
from typing import Callable, Optional, Any, Union
from dataclasses import dataclass


@dataclass
class ProgressEvent:
    """类说明：ProgressEvent 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    stage: str
    progress: float
    message: str
    data: Optional[Any] = None
    status: Optional[str] = None  # 可选状态字段


# Alias for backward compatibility
ProgressUpdate = ProgressEvent


class BaseProcessor:
    """类说明：BaseProcessor 负责封装本模块相关能力。
    执行步骤：
    1) 步骤1：接收调用请求并组织上下文数据。
    2) 步骤2：协调类内方法完成业务处理。
    3) 步骤3：输出处理结果并提供可复用能力。"""
    
    def __init__(self, on_progress: Optional[Callable[[ProgressEvent], None]] = None):
        """
        执行逻辑：
        1) 解析配置或依赖，准备运行环境。
        2) 初始化对象状态、缓存与依赖客户端。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：在初始化阶段固化依赖，保证运行稳定性。
        输入参数：
        - on_progress: 函数入参（类型：Optional[Callable[[ProgressEvent], None]]）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。"""
        self.on_progress = on_progress
    
    def emit_progress(self, stage: str, arg2: Union[float, str], arg3: Union[float, str, None] = None, 
                      arg4: str = None, data: Any = None):
        """
        执行逻辑：
        1) 准备必要上下文与参数。
        2) 执行核心处理并返回结果。
        实现方式：通过内部方法调用/状态更新实现。
        核心价值：封装逻辑单元，提升复用与可维护性。
        决策逻辑：
        - 条件：isinstance(arg2, float) or (isinstance(arg2, (int, float)) and arg3 is None or isinstance(arg3, str))
        - 条件：self.on_progress
        - 条件：isinstance(arg2, (int, float))
        依据来源（证据链）：
        - 输入参数：arg2, arg3, arg4。
        - 对象内部状态：self.on_progress。
        输入参数：
        - stage: 函数入参（类型：str）。
        - arg2: 函数入参（类型：Union[float, str]）。
        - arg3: 函数入参（类型：Union[float, str, None]）。
        - arg4: 函数入参（类型：str）。
        - data: 数据列表/集合（类型：Any）。
        输出参数：
        - 无（仅产生副作用，如日志/写盘/状态更新）。
        补充说明：
        发送进度事件 - 支持两种调用格式：
        格式1 (3参数): emit_progress(stage, progress, message, data=None)
        格式2 (4参数): emit_progress(stage, status, progress, message, data=None)
        stage: 处理阶段 (如 "download", "transcribe", "analyze")
        arg2: 进度值(float) 或 状态字符串(str)
        arg3: 消息(str) 或 进度值(float)
        arg4: 消息(str，仅4参数格式使用)
        data: 可选的额外数据"""
        # 检测调用格式
        if isinstance(arg2, float) or (isinstance(arg2, (int, float)) and arg3 is None or isinstance(arg3, str)):
            # 格式1: emit_progress(stage, progress, message, data)
            if isinstance(arg2, (int, float)):
                progress = float(arg2)
                message = arg3 if arg3 else ""
                status = None
                if arg4 is not None:
                    data = arg4  # arg4 在格式1中可能被传入作为 data
            else:
                # arg2 是 string，可能是 status
                status = arg2
                progress = float(arg3) if arg3 else 0.0
                message = arg4 if arg4 else ""
        else:
            # 格式2: emit_progress(stage, status, progress, message)
            status = arg2
            progress = float(arg3) if arg3 else 0.0
            message = arg4 if arg4 else ""
        
        if self.on_progress:
            event = ProgressEvent(
                stage=stage,
                progress=progress,
                message=message,
                data=data,
                status=status
            )
            self.on_progress(event)
