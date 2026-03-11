"""包初始化。"""

from services.python_grpc.src.content_pipeline.phase2b.video_category_service import (
    classify_phase2b_output,
)

__all__ = ["classify_phase2b_output"]
