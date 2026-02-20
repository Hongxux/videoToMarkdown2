import json
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


try:
    from services.python_grpc.src.server import grpc_service_impl as impl
except Exception as exc:  # pragma: no cover
    pytest.skip(f"grpc_service_impl import unavailable: {exc}", allow_module_level=True)


def test_resolve_assemble_document_title_prefers_custom_request_title(tmp_path):
    resolved = impl._resolve_assemble_document_title(
        request_title="  自定义课程标题  ",
        output_dir=str(tmp_path),
        video_path=str(tmp_path / "video.mp4"),
    )

    assert resolved == "自定义课程标题"


def test_resolve_assemble_document_title_uses_video_meta_when_request_title_is_placeholder(tmp_path):
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "video_meta.json").write_text(
        json.dumps({"title": "  机器学习入门  "}, ensure_ascii=False),
        encoding="utf-8",
    )

    resolved = impl._resolve_assemble_document_title(
        request_title="视频内容",
        output_dir=str(output_dir),
        video_path=str(output_dir / "video.mp4"),
    )

    assert resolved == "机器学习入门"


def test_resolve_assemble_document_title_falls_back_to_video_path_title(tmp_path):
    video_path = tmp_path / "001-Linear_Regression_20240101.mp4"
    video_path.write_bytes(b"video")

    resolved = impl._resolve_assemble_document_title(
        request_title="",
        output_dir=str(tmp_path / "missing"),
        video_path=str(video_path),
    )

    assert resolved == "Linear Regression"


def test_resolve_assemble_document_title_returns_default_when_only_generic_name(tmp_path):
    video_path = tmp_path / "video.mp4"
    video_path.write_bytes(b"video")

    resolved = impl._resolve_assemble_document_title(
        request_title="",
        output_dir=str(tmp_path / "missing"),
        video_path=str(video_path),
    )

    assert resolved == "视频内容"
