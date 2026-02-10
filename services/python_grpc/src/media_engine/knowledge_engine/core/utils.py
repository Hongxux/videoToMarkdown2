"""
模块说明：视频转Markdown流程中的 utils 模块。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

from typing import Any, Optional

from services.python_grpc.src.common.utils.path import safe_filename


def get_config_value(config: dict, *keys, default: Any = None) -> Any:
    """
    执行逻辑：
    1) 读取内部状态或外部资源。
    2) 返回读取结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：提供一致读取接口，降低调用耦合。
    决策逻辑：
    - 条件：isinstance(value, dict)
    - 条件：value is None
    依据来源（证据链）：
    输入参数：
    - config: 配置对象/字典（类型：dict）。
    - default: 函数入参（类型：Any）。
    - *keys: 可变参数，含义由调用方决定。
    输出参数：
    - 函数计算/封装后的结果对象。"""
    value = config
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return default
        if value is None:
            return default
    return value


# 别名，保持向后兼容
get_cfg = get_config_value


def format_duration(seconds: float) -> str:
    """
    执行逻辑：
    1) 准备必要上下文与参数。
    2) 执行核心处理并返回结果。
    实现方式：通过内部函数组合与条件判断实现。
    核心价值：封装逻辑单元，提升复用与可维护性。
    决策逻辑：
    - 条件：hours > 0
    - 条件：minutes > 0
    依据来源（证据链）：
    输入参数：
    - seconds: 函数入参（类型：float）。
    输出参数：
    - 字符串结果。"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    
    if hours > 0:
        return f"{hours}h {minutes}m {secs}s"
    elif minutes > 0:
        return f"{minutes}m {secs}s"
    else:
        return f"{secs}s"


