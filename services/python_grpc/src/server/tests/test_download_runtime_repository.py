from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[5]))

from services.python_grpc.src.server.download_runtime_repository import (
    build_download_runtime_repository,
    get_download_repository_views,
    update_download_repository_views,
)


class _FlowResultStub:
    success = True
    video_path = "D:/demo/video.mp4"
    file_size_bytes = 123456
    duration_sec = 98.7
    resolved_url = "https://example.com/video"
    source_platform = "bilibili"
    canonical_id = "BV123"
    link_resolver = "share_link"
    video_title = "Demo Title"
    content_type = "video"


def test_download_runtime_repository_exposes_views_and_aliases(tmp_path):
    output_dir = str(tmp_path / "task")
    payload = build_download_runtime_repository(
        output_dir=output_dir,
        task_id="task-download",
        raw_video_input="https://example.com/watch?v=1",
    )

    update_download_repository_views(
        payload,
        flow_result=_FlowResultStub(),
        reused=False,
    )

    views = get_download_repository_views(payload)
    assert payload["ready"] is True
    assert payload["status"] == "READY"
    assert views["video_path"] == "D:/demo/video.mp4"
    assert payload["video_path"] == "D:/demo/video.mp4"
    assert views["video_title"] == "Demo Title"
    assert payload["content_type"] == "video"
