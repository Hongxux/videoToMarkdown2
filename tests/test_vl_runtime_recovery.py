import asyncio
import gc
import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.python_grpc.src.common.utils.runtime_llm_context import activate_runtime_llm_context
from services.python_grpc.src.content_pipeline.infra.llm import llm_gateway
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_material_generator import VLMaterialGenerator
from services.python_grpc.src.content_pipeline.phase2a.materials.vl_video_analyzer import VLVideoAnalyzer


def test_phase2a_vl_analyzer_restores_committed_llm_call(tmp_path, monkeypatch):
    clip_path = tmp_path / "clip.mp4"
    clip_path.write_bytes(b"video")
    analyzer = VLVideoAnalyzer({"api": {"model": "qwen-vl-max-latest", "max_retries": 0}})
    state = {"calls": 0, "message_version": 0}

    async def fake_build_messages(video_path, extra_prompt=None, override_prompt=None, analysis_mode="default", **kwargs):
        _ = kwargs
        state["message_version"] += 1
        return [
            {"role": "system", "content": "system prompt"},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"mode={analysis_mode}"},
                    {
                        "type": "video_url",
                        "video_url": {"url": f"https://tmp.example.com/upload/{state['message_version']}"},
                    },
                ],
            },
        ]

    async def fake_acquire_client_state():
        return SimpleNamespace(openai_client=object())

    async def fake_release_client_state(client_state):
        return None

    monkeypatch.setattr(analyzer, "_build_messages", fake_build_messages)
    monkeypatch.setattr(analyzer, "_resolve_video_duration_sec", lambda video_path: 9.0)
    monkeypatch.setattr(analyzer, "_resolve_vl_request_timeout_sec", lambda video_path: 30.0)
    monkeypatch.setattr(analyzer, "_resolve_vl_hedge_delay_ms", lambda video_path: 0)
    monkeypatch.setattr(analyzer, "_acquire_client_state", fake_acquire_client_state)
    monkeypatch.setattr(analyzer, "_release_client_state", fake_release_client_state)

    response_text = json.dumps(
        [
            {
                "id": 1,
                "knowledge_type": "process",
                "clip_start_sec": 0.0,
                "clip_end_sec": 4.0,
                "suggested_screenshot_timestamps": [1.5],
            }
        ],
        ensure_ascii=False,
    )

    async def fake_vl_chat_completion(**kwargs):
        state["calls"] += 1
        return llm_gateway.VLChatResult(
            content=response_text,
            finish_reason="stop",
            usage={"prompt_tokens": 3, "completion_tokens": 5, "total_tokens": 8},
            model="qwen-vl-max-latest",
            cache_hit=False,
        )

    monkeypatch.setattr(llm_gateway, "vl_chat_completion", fake_vl_chat_completion)

    async def run_once():
        with activate_runtime_llm_context(
            stage="phase2a",
            output_dir=str(tmp_path),
            task_id="task_phase2a_vl",
            storage_key="storage_phase2a_vl",
        ):
            return await analyzer.analyze_clip(
                clip_path=str(clip_path),
                semantic_unit_start_sec=12.0,
                semantic_unit_id="SU777",
                extra_prompt="focus on teaching steps",
                analysis_mode="default",
            )

    first = asyncio.run(run_once())
    assert first.success is True
    assert state["calls"] == 1

    async def should_not_be_called(**kwargs):
        raise AssertionError("vl_chat_completion should not be called when runtime commit exists")

    monkeypatch.setattr(llm_gateway, "vl_chat_completion", should_not_be_called)
    second = asyncio.run(run_once())

    assert second.success is True
    assert state["calls"] == 1
    assert second.raw_llm_interactions
    restored_interactions = [
        item
        for item in second.raw_llm_interactions
        if bool((item.get("response", {}) if isinstance(item, dict) else {}).get("runtime_restored", False))
    ]
    assert restored_interactions
    assert restored_interactions[0]["response"]["cache_hit"] is True

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
    assert json.loads(str(row[0] or "[]")) == ["SU777"]
    del first
    del second
    del analyzer
    gc.collect()


def test_phase2a_screenshot_chunk_restore_skips_dirty_scope():
    tmp_path = Path(tempfile.mkdtemp(prefix="vl_screenshot_chunk_", dir=str(Path("var").resolve())))
    generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    generator._prepare_runtime_store_for_output_dir(str(tmp_path))
    assert generator._runtime_store is not None

    committed_chunk = {
        "union_start": 11.0,
        "union_end": 13.0,
        "windows": [
            {
                "unit_id": "SU001",
                "expanded_start": 11.0,
                "expanded_end": 13.0,
                "req": {
                    "screenshot_id": "SU001_ss_001",
                    "semantic_unit_id": "SU001",
                    "label": "stable",
                    "timestamp_sec": 12.0,
                },
            }
        ],
    }

    generator._commit_screenshot_chunk_runtime(
        video_path=str(tmp_path / "video.mp4"),
        mode="streaming",
        chunk_index=0,
        chunk=committed_chunk,
    )

    restored_chunk = {
        "union_start": 11.0,
        "union_end": 13.0,
        "windows": [
            {
                "unit_id": "SU001",
                "expanded_start": 11.0,
                "expanded_end": 13.0,
                "req": {
                    "screenshot_id": "SU001_ss_001",
                    "semantic_unit_id": "SU001",
                    "label": "stable",
                    "timestamp_sec": 12.0,
                },
            }
        ],
    }

    assert generator._restore_screenshot_chunk_if_committed(
        video_path=str(tmp_path / "video.mp4"),
        mode="streaming",
        chunk_index=0,
        chunk=restored_chunk,
    ) is True

    chunk_id = generator._runtime_store.build_chunk_id(chunk_index=0, prefix="ss")
    dependency_fingerprints = generator._build_screenshot_chunk_dependency_fingerprints(
        video_path=str(tmp_path / "video.mp4"),
        mode="streaming",
        chunk_id=chunk_id,
        chunk=restored_chunk,
    )
    dependency_scope_ref = next(iter(dependency_fingerprints.keys()))
    generator._runtime_store.mark_scope_dirty(
        dependency_scope_ref,
        reason="stage1_llm_call_recomputed",
        include_descendants=True,
    )

    blocked_chunk = {
        "union_start": 11.0,
        "union_end": 13.0,
        "windows": [
            {
                "unit_id": "SU001",
                "expanded_start": 11.0,
                "expanded_end": 13.0,
                "req": {
                    "screenshot_id": "SU001_ss_001",
                    "semantic_unit_id": "SU001",
                    "label": "stable",
                    "timestamp_sec": 12.0,
                },
            }
        ],
    }

    assert generator._restore_screenshot_chunk_if_committed(
        video_path=str(tmp_path / "video.mp4"),
        mode="streaming",
        chunk_index=0,
        chunk=blocked_chunk,
    ) is False
    del generator
    gc.collect()


def test_phase2a_unit_material_projection_restore_reuses_committed_payload():
    tmp_path = Path(tempfile.mkdtemp(prefix="vl_unit_projection_", dir=str(Path("var").resolve())))
    generator = VLMaterialGenerator({"enabled": True, "screenshot_optimization": {}})
    generator._prepare_runtime_store_for_output_dir(str(tmp_path))
    assert generator._runtime_store is not None

    semantic_unit = {
        "unit_id": "SU009",
        "start_sec": 3.0,
        "end_sec": 9.0,
        "knowledge_type": "concrete",
        "_vl_route_override": "concrete",
        "_vl_route_reason": "vl_should_type_concrete",
        "_vl_no_needed_video": False,
    }
    analysis_result = SimpleNamespace(
        clip_requests=[
            {
                "clip_id": "SU009_CLIP_1",
                "semantic_unit_id": "SU009",
                "start_sec": 3.2,
                "end_sec": 6.6,
            }
        ],
        screenshot_requests=[
            {
                "screenshot_id": "SU009_SS_1",
                "semantic_unit_id": "SU009",
                "timestamp_sec": 4.8,
                "label": "focus",
            }
        ],
        raw_response_json=[{"segment_id": 1, "main_content": "content"}],
    )

    generator._commit_unit_material_projection(
        unit_id="SU009",
        wave_id="wave_0001",
        analysis_mode="concrete",
        clip_path=str(tmp_path / "clip.mp4"),
        semantic_unit=semantic_unit,
        analysis_result=analysis_result,
    )

    restored_semantic_unit = {
        "unit_id": "SU009",
        "start_sec": 3.0,
        "end_sec": 9.0,
        "knowledge_type": "process",
    }
    restored_result = SimpleNamespace(
        clip_requests=[],
        screenshot_requests=[],
        raw_response_json=[{"segment_id": 1, "main_content": "content"}],
    )

    restored_payload = generator._restore_unit_material_projection_if_committed(
        unit_id="SU009",
        wave_id="wave_0001",
        analysis_mode="concrete",
        clip_path=str(tmp_path / "clip.mp4"),
        semantic_unit=restored_semantic_unit,
        analysis_result=restored_result,
    )

    assert isinstance(restored_payload, dict)
    assert restored_semantic_unit["knowledge_type"] == "concrete"
    assert restored_semantic_unit["_vl_route_override"] == "concrete"
    assert restored_result.clip_requests[0]["clip_id"] == "SU009_CLIP_1"
    assert restored_result.screenshot_requests[0]["screenshot_id"] == "SU009_SS_1"
    del generator
    gc.collect()
