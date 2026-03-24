"""
模块说明：包初始化与公共导出。
执行逻辑：
1) 聚合本模块的类/函数，对外提供核心能力。
2) 通过内部调用与外部依赖完成具体处理。
实现方式：通过模块内函数组合与外部依赖调用实现。
核心价值：统一模块职责边界，降低跨文件耦合成本。
输入：
- 调用方传入的参数与数据路径。
输出：
- 各函数/类返回的结构化结果或副作用。"""

# Stage1 Pipeline - LangGraph Implementation
# 视频文字稿处理流程

from .graph import create_pipeline_graph, run_pipeline
from .state import PipelineState

__version__ = "0.1.0"
__all__ = ["create_pipeline_graph", "run_pipeline", "PipelineState"]
