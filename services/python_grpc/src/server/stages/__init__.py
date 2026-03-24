"""gRPC 服务按阶段拆分的组合层。"""

from .phase2a_stage import Phase2AMaterialStageMixin
from .validation_vl_stage import ValidationAndVLStageMixin

__all__ = [
    "Phase2AMaterialStageMixin",
    "ValidationAndVLStageMixin",
]
