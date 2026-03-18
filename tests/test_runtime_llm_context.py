import asyncio
import json
import sqlite3
import shutil
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils.runtime_llm_context import (
    activate_runtime_llm_context,
    build_runtime_llm_request_payload,
)
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.transcript_pipeline.llm.client import LLMConfig, LLMResponse
from services.python_grpc.src.transcript_pipeline.llm.deepseek import DeepSeekClient


def _make_repo_tmp_dir(test_name: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    base = repo_root / "var" / "tmp_runtime_llm_context_tests"
    base.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in test_name)
    unique_suffix = f"{time.time_ns() % 1_000_000:06d}"
    path = base / f"{safe_name[:24]}_{unique_suffix}"
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _make_client() -> DeepSeekClient:
    config = LLMConfig(
        api_key="test-key",
        base_url="https://api.example.com",
        model="deepseek-chat",
        temperature=0.1,
    )
    return DeepSeekClient(config)


def _make_response(content: str) -> LLMResponse:
    return LLMResponse(
        content=content,
        prompt_tokens=3,
        completion_tokens=5,
        total_tokens=8,
        model="deepseek-chat",
        latency_ms=12.5,
        raw_response={"id": "resp_001"},
    )


def test_stage1_complete_json_restores_from_runtime_commit(monkeypatch):
    tmp_path = _make_repo_tmp_dir("stage1_complete_json_restore")
    client = _make_client()
    calls = {"count": 0}

    async def fake_complete(prompt, system_prompt=None, **kwargs):
        calls["count"] += 1
        return _make_response(json.dumps({"ok": True}, ensure_ascii=False))

    monkeypatch.setattr(client, "complete", fake_complete)

    async def run_once():
        with activate_runtime_llm_context(
            stage="stage1",
            output_dir=str(tmp_path),
            task_id="task_stage1",
            storage_key="storage_stage1",
        ):
            return await client.complete_json("fix subtitles")

    parsed, response = asyncio.run(run_once())
    assert parsed == {"ok": True}
    assert response.total_tokens == 8
    assert calls["count"] == 1

    async def should_not_be_called(prompt, system_prompt=None, **kwargs):
        raise AssertionError("complete should not be called when runtime commit exists")

    monkeypatch.setattr(client, "complete", should_not_be_called)
    restored_parsed, restored_response = asyncio.run(run_once())
    assert restored_parsed == {"ok": True}
    assert restored_response.total_tokens == 0
    assert calls["count"] == 1


def test_stage1_complete_json_sqlite_backend_skips_attempt_files(monkeypatch):
    tmp_path = _make_repo_tmp_dir("stage1_complete_json_sqlite")
    client = _make_client()
    calls = {"count": 0}

    async def fake_complete(prompt, system_prompt=None, **kwargs):
        calls["count"] += 1
        return _make_response(json.dumps({"ok": True}, ensure_ascii=False))

    monkeypatch.setattr(client, "complete", fake_complete)

    async def run_once():
        with activate_runtime_llm_context(
            stage="stage1",
            output_dir=str(tmp_path),
            task_id="task_stage1_sqlite",
            storage_key="storage_stage1_sqlite",
            storage_backend="sqlite",
        ) as context:
            parsed, response = await client.complete_json("fix subtitles")
            rows = context.store.list_sqlite_llm_records(stage="stage1", status="SUCCESS", limit=10)
            assert rows
            return parsed, response

    parsed, response = asyncio.run(run_once())
    assert parsed == {"ok": True}
    assert response.total_tokens == 8
    assert calls["count"] == 1
    assert not any((tmp_path / "intermediates" / "rt" / "stage" / "stage1").rglob("request.json"))
    assert not any((tmp_path / "intermediates" / "rt" / "stage" / "stage1").rglob("manifest.json"))
    assert not any((tmp_path / "intermediates" / "rt" / "stage" / "stage1").rglob("commit.json"))
    assert not any((tmp_path / "intermediates" / "rt" / "stage" / "stage1").rglob("part_*.json"))

    async def should_not_be_called(prompt, system_prompt=None, **kwargs):
        raise AssertionError("complete should not be called when sqlite runtime commit exists")

    monkeypatch.setattr(client, "complete", should_not_be_called)
    restored_parsed, restored_response = asyncio.run(run_once())
    assert restored_parsed == {"ok": True}
    assert restored_response.total_tokens == 0
    assert calls["count"] == 1


def test_runtime_llm_context_stage_prefetch_uses_cache():
    tmp_path = _make_repo_tmp_dir("runtime_llm_context_prefetch")
    request_payload = build_runtime_llm_request_payload(
        model="deepseek-chat",
        prompt="prefetch runtime context",
        system_prompt="",
        kwargs={"temperature": 0.1, "max_tokens": 4096},
    )

    with activate_runtime_llm_context(
        stage="stage1",
        output_dir=str(tmp_path),
        task_id="task_stage1_prefetch",
        storage_key="storage_stage1_prefetch",
    ) as context:
        context.persist_success(
            provider="deepseek",
            request_name="complete_text",
            request_payload=request_payload,
            response_text="stage prefetch cache payload",
            response_metadata={"model": "deepseek-chat"},
        )

    with activate_runtime_llm_context(
        stage="stage1",
        output_dir=str(tmp_path),
        task_id="task_stage1_prefetch",
        storage_key="storage_stage1_prefetch",
    ) as context:
        def _should_not_load(**kwargs):
            raise AssertionError("store.load_committed_llm_response should not be called after stage prefetch")

        context.store.load_committed_llm_response = _should_not_load
        restored = context.load_committed_call(
            provider="deepseek",
            request_name="complete_text",
            request_payload=request_payload,
        )

    assert restored is not None
    assert restored["response_text"] == "stage prefetch cache payload"


def test_phase2a_runtime_llm_context_persists_explicit_request_scope_ids():
    tmp_path = _make_repo_tmp_dir("phase2a_explicit_request_scope_ids")
    request_payload = build_runtime_llm_request_payload(
        model="deepseek-chat",
        prompt="phase2a request without bracketed scope markers",
        system_prompt="",
        kwargs={"temperature": 0.1},
        extra_payload={"request_scope_ids": ["SU900", "SU901"]},
    )

    with activate_runtime_llm_context(
        stage="phase2a",
        output_dir=str(tmp_path),
        task_id="task_phase2a_scope_ids",
        storage_key="storage_phase2a_scope_ids",
    ) as context:
        context.persist_success(
            provider="deepseek",
            request_name="complete_text",
            request_payload=request_payload,
            response_text="phase2a explicit scope ids payload",
            response_metadata={"model": "deepseek-chat"},
        )
        rows = context.store.list_sqlite_llm_records(stage="phase2a", status="SUCCESS", limit=10)

    assert rows
    assert json.loads(rows[0]["request_scope_ids_json"]) == ["SU900", "SU901"]

    with sqlite3.connect(str(tmp_path / "intermediates" / "rt" / "runtime_state.db")) as connection:
        row = connection.execute(
            """
            SELECT request_scope_ids_json
            FROM llm_records
            WHERE stage = ?
            ORDER BY updated_at_ms DESC, attempt DESC
            LIMIT 1
            """,
            ("phase2a",),
        ).fetchone()

    assert row is not None
    assert json.loads(str(row[0] or "[]")) == ["SU900", "SU901"]


def test_runtime_llm_context_build_scope_descriptor_preplans_llm_scope():
    tmp_path = _make_repo_tmp_dir("runtime_llm_context_preplan_scope")
    request_payload = build_runtime_llm_request_payload(
        model="deepseek-chat",
        prompt="scope planning payload",
        system_prompt="",
        kwargs={"temperature": 0.1},
        extra_payload={"request_scope_ids": ["SU010"]},
    )

    with activate_runtime_llm_context(
        stage="phase2a",
        output_dir=str(tmp_path),
        task_id="task_phase2a_preplan",
        storage_key="storage_phase2a_preplan",
    ) as context:
        descriptor = context.build_scope_descriptor(
            provider="deepseek",
            request_name="complete_text",
            request_payload=request_payload,
            runtime_identity={
                "step_name": "phase2a_vl_analysis",
                "unit_id": "SU010",
                "wave_id": "wave_0007",
                "substage_name": "vl_analysis",
            },
        )
        scope_node = context.store.load_scope_node(str(descriptor["scope_ref"]))

    assert scope_node is not None
    assert scope_node["status"] == "PLANNED"
    assert scope_node["chunk_id"] == descriptor["chunk_id"]
    assert int(scope_node.get("attempt_count", 0) or 0) == 0
    assert scope_node["plan_context"]["request_scope_ids"] == ["SU010"]
    assert scope_node["plan_context"]["wave_id"] == "wave_0007"
    assert scope_node["plan_context"]["substage_name"] == "vl_analysis"


def test_runtime_llm_context_planning_does_not_regress_success_scope():
    tmp_path = _make_repo_tmp_dir("runtime_llm_context_keep_success")
    request_payload = build_runtime_llm_request_payload(
        model="deepseek-chat",
        prompt="keep success scope",
        system_prompt="",
        kwargs={"temperature": 0.1},
    )

    with activate_runtime_llm_context(
        stage="phase2a",
        output_dir=str(tmp_path),
        task_id="task_phase2a_keep_success",
        storage_key="storage_phase2a_keep_success",
    ) as context:
        runtime_identity = {"unit_id": "SU011", "step_name": "phase2a_vl_analysis"}
        context.persist_success(
            provider="deepseek",
            request_name="complete_text",
            request_payload=request_payload,
            response_text="success payload",
            response_metadata={"model": "deepseek-chat"},
            runtime_identity=runtime_identity,
        )
        descriptor = context.build_scope_descriptor(
            provider="deepseek",
            request_name="complete_text",
            request_payload=request_payload,
            runtime_identity=runtime_identity,
        )
        scope_node = context.store.load_scope_node(str(descriptor["scope_ref"]))

    assert scope_node is not None
    assert scope_node["status"] == "SUCCESS"


@pytest.mark.parametrize("gateway_name", ["vision_validate_image", "vision_validate_image_sync"])
def test_phase2a_vision_gateway_restores_from_runtime_commit(monkeypatch, gateway_name):
    tmp_path = _make_repo_tmp_dir(f"phase2a_vision_{gateway_name}")
    monkeypatch.setattr(llm_gateway, "_VISION_HEDGE_ENABLED", False)

    class DummyVisionClient:
        def __init__(self):
            self.config = SimpleNamespace(model="vision-unit-test")
            self.calls = 0

        async def validate_image(self, image_path, prompt="", system_prompt=None, skip_duplicate_check=False):
            self.calls += 1
            return {
                "should_include": True,
                "reason": "ok",
                "image_path": image_path,
            }

    client = DummyVisionClient()

    def run_call():
        with activate_runtime_llm_context(
            stage="phase2a",
            output_dir=str(tmp_path),
            task_id="task_phase2a",
            storage_key="storage_phase2a",
        ):
            if gateway_name.endswith("_sync"):
                return llm_gateway.vision_validate_image_sync(
                    image_path="frame_001.png",
                    prompt="describe image",
                    client=client,
                )
            return asyncio.run(
                llm_gateway.vision_validate_image(
                    image_path="frame_001.png",
                    prompt="describe image",
                    client=client,
                )
            )

    first_result = run_call()
    assert first_result["should_include"] is True
    assert client.calls == 1

    async def should_not_be_called(*args, **kwargs):
        raise AssertionError("vision client should not be called when runtime commit exists")

    client.validate_image = should_not_be_called
    restored_result = run_call()
    assert restored_result["reason"] == "ok"
