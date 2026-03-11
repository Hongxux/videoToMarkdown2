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


def _build_task_dir(tmp_path: Path, task_id: str = "task-1") -> Path:
    task_dir = tmp_path / "var" / "storage" / "storage" / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    return task_dir


def test_classify_phase2b_output_writes_task_and_summary_artifacts(tmp_path, monkeypatch):
    task_dir = _build_task_dir(tmp_path)
    _write_json(
        task_dir / "video_meta.json",
        {
            "title": "帮你把KMP算法学个通透！（理论篇）",
            "source_url": "https://example.com/video",
        },
    )
    _write_json(
        task_dir / "result.json",
        {
            "title": "帮你把KMP算法学个通透！（理论篇）",
            "knowledge_groups": [
                {
                    "group_name": "KMP算法原理与前置表",
                    "units": [
                        {
                            "body_text": "这节课从 KMP 算法原理和前缀表开始讲解。",
                        }
                    ],
                }
            ],
        },
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "编程开发/Java后端",
                    "is_new": True,
                    "reasoning": "错误候选。",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "编程开发/算法与数据结构",
                    "is_new": True,
                    "reasoning": "视频内容聚焦 KMP 算法原理，应归类到算法与数据结构。",
                },
                ensure_ascii=False,
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
            title="帮你把KMP算法学个通透！（理论篇）",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "编程开发/算法与数据结构"
    assert result["is_new"] is True

    library_path = tmp_path / "var" / "storage" / "storage" / "category_paths.txt"
    assert library_path.read_text(encoding="utf-8").strip() == "编程开发/算法与数据结构"

    summary_path = tmp_path / "var" / "storage" / "category_classification_results.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["total_videos"] == 1
    assert summary_payload["category_counts"] == {"编程开发/算法与数据结构": 1}
    assert summary_payload["results"][0]["category_path"] == "编程开发/算法与数据结构"

    task_payload = json.loads((task_dir / "category_classification.json").read_text(encoding="utf-8"))
    assert task_payload["category_path"] == "编程开发/算法与数据结构"
    assert "Java后端" in task_payload["raw_response"]
    assert "算法与数据结构" in task_payload["verified_raw_response"]

    video_meta = json.loads((task_dir / "video_meta.json").read_text(encoding="utf-8"))
    assert video_meta["category_path"] == "编程开发/算法与数据结构"
    assert video_meta["category_domain"] == "编程开发"
    assert video_meta["category_subdomain"] == "算法与数据结构"


def test_classify_phase2b_output_corrects_is_new_against_existing_library(tmp_path, monkeypatch):
    task_dir = _build_task_dir(tmp_path, task_id="task-2")
    _write_json(
        task_dir / "video_meta.json",
        {
            "title": "提示词工程入门",
        },
    )
    _write_json(
        task_dir / "result.json",
        {
            "title": "提示词工程入门",
            "knowledge_groups": [
                {
                    "group_name": "提示词工程核心原则",
                    "units": [
                        {
                            "body_text": "本节讲解提示词工程的基本原则和应用方法。",
                        }
                    ],
                }
            ],
        },
    )
    library_path = tmp_path / "var" / "storage" / "storage" / "category_paths.txt"
    library_path.parent.mkdir(parents=True, exist_ok=True)
    library_path.write_text("人工智能/提示工程\n", encoding="utf-8")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "人工智能/提示工程",
                    "is_new": True,
                    "reasoning": "候选结果错误地把 is_new 标成 true。",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "人工智能/提示工程",
                    "is_new": False,
                    "reasoning": "分类库中已存在该路径，应复用现有分类。",
                },
                ensure_ascii=False,
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
            title="提示词工程入门",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "人工智能/提示工程"
    assert result["is_new"] is False

    task_payload = json.loads((task_dir / "category_classification.json").read_text(encoding="utf-8"))
    assert task_payload["is_new"] is False
    assert library_path.read_text(encoding="utf-8").strip() == "人工智能/提示工程"
