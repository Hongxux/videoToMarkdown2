import asyncio
import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


from services.python_grpc.src.content_pipeline.phase2b.video_category_service import (  # noqa: E402
    classify_phase2b_output,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_summary_write_preserves_existing_manual_binding(tmp_path, monkeypatch):
    task_dir = tmp_path / "var" / "storage" / "storage" / "task-3"
    task_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        task_dir / "video_meta.json",
        {
            "title": "Manual Binding Case",
        },
    )
    _write_json(
        task_dir / "result.json",
        {
            "title": "Manual Binding Case",
            "knowledge_groups": [
                {
                    "group_name": "Prefix Table",
                    "units": [
                        {
                            "body_text": "This lesson focuses on string matching and prefix table construction.",
                        }
                    ],
                }
            ],
        },
    )
    summary_path = tmp_path / "var" / "storage" / "category_classification_results.json"
    _write_json(
        summary_path,
        {
            "updated_at": "2026-03-11T00:00:00+00:00",
            "total_videos": 1,
            "category_counts": {
                "dev/old-auto": 1,
            },
            "results": [
                {
                    "video_id": "task-3",
                    "task_path": "storage/task-3",
                    "category_path": "dev/old-auto",
                }
            ],
            "collectionBindings": {
                "storage/task-3": "custom/manual-path",
            },
        },
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "dev/new-auto",
                    "is_new": True,
                    "reasoning": "candidate",
                }
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "dev/new-auto",
                    "is_new": True,
                    "reasoning": "verified",
                }
            ),
            {},
            None,
        ),
    ]

    async def _fake_deepseek_complete_text(**kwargs):
        return responses.pop(0)

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2b.video_category_service.llm_gateway.deepseek_complete_text",
        _fake_deepseek_complete_text,
    )

    result = asyncio.run(
        classify_phase2b_output(
            output_dir=str(task_dir),
            title="Manual Binding Case",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["results"][0]["task_path"] == "storage/task-3"
    assert summary_payload["collectionBindings"]["storage/task-3"] == "custom/manual-path"
