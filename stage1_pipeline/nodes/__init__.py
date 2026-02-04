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

# Nodes package
from .phase1_preparation import step1_node
from .phase2_preprocessing import (
    step2_node, step3_node, step4_node, step5_node, step6_node
)
from .phase3_segmentation import step7_node, step7b_node, step7c_node, step8a_node, step8b_node
from .phase4_screenshot_gen import step9_node, step10_node, step11_node
from .phase5_capture import step12_node, step13_node, step14_node, step15_node, step15b_node
from .phase6_visualization import step16_node, step17_node, step18_node, step19_node
from .phase7_output import step20_node, step21_node, step22_node, step22b_node
from .phase8_archive import step23_node, step24_node

__all__ = [
    # Phase 1
    "step1_node",
    # Phase 2
    "step2_node", "step3_node", "step4_node", "step5_node", "step6_node",
    # Phase 3
    "step7_node", "step7b_node", "step7c_node", "step8a_node", "step8b_node",
    # Phase 4
    "step9_node", "step10_node", "step11_node",
    # Phase 5
    "step12_node", "step13_node", "step14_node", "step15_node", "step15b_node",
    # Phase 6
    "step16_node", "step17_node", "step18_node", "step19_node",
    # Phase 7
    "step20_node", "step21_node", "step22_node", "step22b_node",
    # Phase 8
    "step23_node", "step24_node"
]
