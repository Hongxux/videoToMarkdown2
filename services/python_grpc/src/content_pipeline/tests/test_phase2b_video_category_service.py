import asyncio
import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


from services.python_grpc.src.common.utils.runtime_recovery_store import RuntimeRecoveryStore  # noqa: E402
from services.python_grpc.src.content_pipeline.phase2b.video_category_service import (  # noqa: E402
    classify_phase2b_output,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_task_dir(tmp_path: Path, task_id: str) -> Path:
    task_dir = tmp_path / "var" / "storage" / "storage" / task_id
    task_dir.mkdir(parents=True, exist_ok=True)
    return task_dir


def _seed_task(
    tmp_path: Path,
    *,
    task_id: str,
    title: str,
    group_name: str,
    body_text: str,
    knowledge_groups: list[dict] | None = None,
    category_path: str | None = None,
) -> Path:
    task_dir = _build_task_dir(tmp_path, task_id)
    _write_json(
        task_dir / "video_meta.json",
        {
            "title": title,
        },
    )
    _write_json(
        task_dir / "result.json",
        {
            "title": title,
            "knowledge_groups": knowledge_groups
            or [
                {
                    "group_name": group_name,
                    "units": [
                        {
                            "body_text": body_text,
                        }
                    ],
                }
            ],
        },
    )
    if category_path:
        _write_json(
            task_dir / "category_classification.json",
            {
                "video_id": task_id,
                "task_path": f"storage/{task_id}",
                "video_title": title,
                "category_path": category_path,
                "target_level": len(category_path.split("/")),
                "is_new": False,
                "reasoning": "seed",
                "generated_at": "2026-03-12T00:00:00+00:00",
                "usage": {},
                "input_snapshot": {
                    "first_unit_text": body_text,
                    "group_names": [group_name],
                },
                "raw_response": "",
                "verified_raw_response": "",
            },
        )
    return task_dir


def _seed_phase2b_result_artifact(
    task_dir: Path,
    *,
    title: str,
    group_name: str,
    body_text: str,
) -> None:
    store = RuntimeRecoveryStore(
        output_dir=str(task_dir),
        task_id=task_dir.name,
        storage_key=task_dir.name,
    )
    store.commit_projection_payload(
        stage="phase2b",
        projection_name="result_document",
        payload={
            "title": title,
            "knowledge_groups": [
                {
                    "group_name": group_name,
                    "units": [
                        {
                            "body_text": body_text,
                        }
                    ],
                }
            ],
        },
    )


def test_classify_phase2b_output_writes_task_and_summary_artifacts(tmp_path, monkeypatch):
    task_dir = _seed_task(
        tmp_path,
        task_id="task-1",
        title="KMP basics",
        group_name="KMP overview",
        body_text="This lesson explains KMP prefix table and string matching.",
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "engineering/algorithms",
                    "is_new": True,
                    "reasoning": "The content focuses on string matching algorithms.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/algorithms",
                    "is_new": True,
                    "reasoning": "The content focuses on string matching algorithms.",
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
            title="KMP basics",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "engineering/algorithms"
    assert result["target_level"] == 2
    assert result["leaf_task_count"] == 1

    library_path = tmp_path / "var" / "storage" / "storage" / "category_paths.txt"
    assert library_path.read_text(encoding="utf-8").strip() == "engineering/algorithms"

    summary_path = tmp_path / "var" / "storage" / "category_classification_results.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["total_videos"] == 1
    assert summary_payload["category_counts"] == {"engineering/algorithms": 1}

    task_payload = json.loads((task_dir / "category_classification.json").read_text(encoding="utf-8"))
    assert task_payload["category_path"] == "engineering/algorithms"
    assert "KMP overview" in task_payload["input_snapshot"]["content_evidence_text"]
    assert task_payload["input_snapshot"]["group_evidence"][0]["group_name"] == "KMP overview"

    video_meta = json.loads((task_dir / "video_meta.json").read_text(encoding="utf-8"))
    assert video_meta["category_path"] == "engineering/algorithms"
    assert video_meta["category_depth"] == 2


def test_classify_phase2b_output_restores_from_sqlite_result_artifact(tmp_path, monkeypatch):
    task_dir = _build_task_dir(tmp_path, "task-sqlite")
    _write_json(
        task_dir / "video_meta.json",
        {
            "title": "SQLite restore case",
        },
    )
    _seed_phase2b_result_artifact(
        task_dir,
        title="SQLite restore case",
        group_name="Dynamic Programming",
        body_text="This lesson explains dynamic programming state design.",
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "engineering/algorithms/dynamic-programming",
                    "is_new": True,
                    "reasoning": "The content focuses on dynamic programming.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/algorithms/dynamic-programming",
                    "is_new": True,
                    "reasoning": "The content focuses on dynamic programming.",
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
            title="SQLite restore case",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "engineering/algorithms/dynamic-programming"


def test_classify_phase2b_output_accepts_direct_multi_level_category_path(tmp_path, monkeypatch):
    task_dir = _seed_task(
        tmp_path,
        task_id="task-direct-leaf",
        title="Java concurrency deep dive",
        group_name="Java concurrency",
        body_text="This lesson focuses on Java concurrency, thread pools, locks, and memory visibility.",
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "engineering/java/concurrency",
                    "is_new": True,
                    "reasoning": "The content directly belongs to a Java concurrency leaf category.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/java/concurrency",
                    "is_new": True,
                    "reasoning": "The content directly belongs to a Java concurrency leaf category.",
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
            title="Java concurrency deep dive",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "engineering/java/concurrency"
    assert result["target_level"] == 3
    assert result["leaf_task_count"] == 1

    library_path = tmp_path / "var" / "storage" / "storage" / "category_paths.txt"
    assert library_path.read_text(encoding="utf-8").strip() == "engineering/java/concurrency"

    summary_path = tmp_path / "var" / "storage" / "category_classification_results.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["category_counts"] == {"engineering/java/concurrency": 1}

    task_payload = json.loads((task_dir / "category_classification.json").read_text(encoding="utf-8"))
    assert task_payload["category_path"] == "engineering/java/concurrency"
    assert task_payload["target_level"] == 3

    video_meta = json.loads((task_dir / "video_meta.json").read_text(encoding="utf-8"))
    assert video_meta["category_path"] == "engineering/java/concurrency"
    assert video_meta["category_depth"] == 3
    assert video_meta["category_leaf"] == "concurrency"


def test_classify_phase2b_output_renders_real_multigroup_evidence_into_prompt(tmp_path, monkeypatch):
    task_dir = _seed_task(
        tmp_path,
        task_id="task-evidence",
        title="Java concurrency deep dive",
        group_name="线程池设计",
        body_text="讲解核心线程数与阻塞队列。",
        knowledge_groups=[
            {
                "group_name": "线程池设计",
                "units": [
                    {
                        "body_text": "讲解核心线程数、阻塞队列、拒绝策略和线程复用。",
                    }
                ],
            },
            {
                "group_name": "并发控制",
                "units": [
                    {
                        "body_text": "讲解 CAS、自旋、可见性和锁竞争的取舍。",
                    }
                ],
            },
        ],
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    captured_prompts: list[str] = []
    responses = [
        (
            json.dumps(
                {
                    "category_path": "engineering/java/concurrency",
                    "is_new": True,
                    "reasoning": "The content focuses on Java concurrency and thread pool design.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/java/concurrency",
                    "is_new": True,
                    "reasoning": "The content focuses on Java concurrency and thread pool design.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
    ]

    async def _fake_deepseek_complete_text(**kwargs):
        captured_prompts.append(str(kwargs.get("prompt") or ""))
        return responses.pop(0)

    monkeypatch.setattr(
        "services.python_grpc.src.content_pipeline.phase2b.video_category_service.llm_gateway.deepseek_complete_text",
        _fake_deepseek_complete_text,
    )

    result = asyncio.run(
        classify_phase2b_output(
            output_dir=str(task_dir),
            title="Java concurrency deep dive",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "engineering/java/concurrency"
    assert captured_prompts
    first_prompt = captured_prompts[0]
    assert "Java concurrency deep dive" in first_prompt
    assert "线程池设计" in first_prompt
    assert "并发控制" in first_prompt
    assert "核心线程数" in first_prompt
    assert "CAS" in first_prompt
    assert "{video_title}" not in first_prompt
    assert "{group_evidence}" not in first_prompt

    task_payload = json.loads((task_dir / "category_classification.json").read_text(encoding="utf-8"))
    assert "并发控制" in task_payload["input_snapshot"]["content_evidence_text"]
    assert len(task_payload["input_snapshot"]["group_evidence"]) == 2


def test_classify_phase2b_output_routes_into_existing_child_category(tmp_path, monkeypatch):
    _seed_task(
        tmp_path,
        task_id="task-old",
        title="Sliding window existing",
        group_name="Sliding window practice",
        body_text="Existing task about sliding window technique.",
        category_path="engineering/algorithms/sliding-window",
    )
    task_dir = _seed_task(
        tmp_path,
        task_id="task-new",
        title="Sliding window new",
        group_name="Sliding window template",
        body_text="New task still focuses on sliding window template and pointers.",
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "engineering/algorithms",
                    "is_new": False,
                    "reasoning": "The topic is algorithmic.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/algorithms",
                    "is_new": False,
                    "reasoning": "The topic is algorithmic.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/algorithms/sliding-window",
                    "is_new": False,
                    "reasoning": "The topic matches the existing sliding-window child.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/algorithms/sliding-window",
                    "is_new": False,
                    "reasoning": "The topic matches the existing sliding-window child.",
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
            title="Sliding window new",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "engineering/algorithms/sliding-window"
    assert result["target_level"] == 3
    assert result["leaf_task_count"] == 2


def test_classify_phase2b_output_rebalances_whole_overloaded_leaf(tmp_path, monkeypatch):
    _seed_task(
        tmp_path,
        task_id="task-old",
        title="Spring Boot intro",
        group_name="Spring Boot basics",
        body_text="Existing task explains Spring Boot starters and auto configuration.",
        category_path="engineering/java",
    )
    task_dir = _seed_task(
        tmp_path,
        task_id="task-new",
        title="JVM tuning intro",
        group_name="JVM tuning basics",
        body_text="New task explains JVM memory layout and garbage collection tuning.",
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setenv("MODULE2_CATEGORY_CLASSIFIER_LEAF_TASK_LIMIT", "1")
    monkeypatch.setenv("MODULE2_CATEGORY_CLASSIFIER_MAX_TARGET_LEVEL", "3")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "engineering/java",
                    "is_new": False,
                    "reasoning": "The topic belongs to Java engineering.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/java",
                    "is_new": False,
                    "reasoning": "The topic belongs to Java engineering.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/java/spring-boot",
                    "is_new": True,
                    "reasoning": "The old task is specifically about Spring Boot.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/java/spring-boot",
                    "is_new": True,
                    "reasoning": "The old task is specifically about Spring Boot.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/java/jvm",
                    "is_new": True,
                    "reasoning": "The new task is specifically about JVM tuning.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/java/jvm",
                    "is_new": True,
                    "reasoning": "The new task is specifically about JVM tuning.",
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
            title="JVM tuning intro",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "engineering/java/jvm"
    assert result["target_level"] == 3
    assert result["leaf_task_count"] == 1

    old_payload = json.loads(
        (tmp_path / "var" / "storage" / "storage" / "task-old" / "category_classification.json").read_text(
            encoding="utf-8"
        )
    )
    assert old_payload["category_path"] == "engineering/java/spring-boot"
    assert old_payload["target_level"] == 3

    summary_path = tmp_path / "var" / "storage" / "category_classification_results.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["category_counts"] == {
        "engineering/java/jvm": 1,
        "engineering/java/spring-boot": 1,
    }


def test_classify_phase2b_output_rebalance_keeps_failed_task_and_continues_others(tmp_path, monkeypatch):
    _seed_task(
        tmp_path,
        task_id="task-old",
        title="TCP basics",
        group_name="TCP protocol",
        body_text="Existing task explains TCP handshake and retransmission.",
        category_path="engineering/network-protocol",
    )
    task_dir = _seed_task(
        tmp_path,
        task_id="task-new",
        title="HTTP caching intro",
        group_name="HTTP caching",
        body_text="New task explains HTTP cache control, ETag, and conditional requests.",
    )
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")
    monkeypatch.setenv("MODULE2_CATEGORY_CLASSIFIER_LEAF_TASK_LIMIT", "1")
    monkeypatch.setenv("MODULE2_CATEGORY_CLASSIFIER_MAX_TARGET_LEVEL", "3")

    responses = [
        (
            json.dumps(
                {
                    "category_path": "engineering/network-protocol",
                    "is_new": False,
                    "reasoning": "The topic belongs to network protocols.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/network-protocol",
                    "is_new": False,
                    "reasoning": "The topic belongs to network protocols.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/java/spring",
                    "is_new": True,
                    "reasoning": "invalid branch for this parent",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/network-protocol/http",
                    "is_new": True,
                    "reasoning": "The new task specifically focuses on HTTP caching.",
                },
                ensure_ascii=False,
            ),
            {},
            None,
        ),
        (
            json.dumps(
                {
                    "category_path": "engineering/network-protocol/http",
                    "is_new": True,
                    "reasoning": "The new task specifically focuses on HTTP caching.",
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
            title="HTTP caching intro",
            result_json_path=str(task_dir / "result.json"),
        )
    )

    assert result is not None
    assert result["category_path"] == "engineering/network-protocol/http"

    old_payload = json.loads(
        (tmp_path / "var" / "storage" / "storage" / "task-old" / "category_classification.json").read_text(
            encoding="utf-8"
        )
    )
    assert old_payload["category_path"] == "engineering/network-protocol"

    new_payload = json.loads((task_dir / "category_classification.json").read_text(encoding="utf-8"))
    assert new_payload["category_path"] == "engineering/network-protocol/http"

    summary_path = tmp_path / "var" / "storage" / "category_classification_results.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary_payload["category_counts"] == {
        "engineering/network-protocol": 1,
        "engineering/network-protocol/http": 1,
    }
