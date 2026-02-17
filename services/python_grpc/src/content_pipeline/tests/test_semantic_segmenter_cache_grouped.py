import json
import os
import shutil
from pathlib import Path

from services.python_grpc.src.content_pipeline.phase2a.segmentation.semantic_unit_segmenter import (
    SegmentationResult,
    SemanticUnit,
    SemanticUnitSegmenter,
)


def _build_unit() -> SemanticUnit:
    return SemanticUnit(
        unit_id="SU001",
        knowledge_type="process",
        knowledge_topic="环境配置",
        full_text="第一步安装依赖，第二步校验版本。",
        source_paragraph_ids=["P001", "P002"],
        source_sentence_ids=["S001", "S002"],
        start_sec=1.0,
        end_sec=12.0,
        confidence=0.92,
        mult_steps=True,
        action_segments=[],
        stable_islands=[],
        group_id=3,
        group_name="CloudBot 环境配置",
        group_reason="围绕同一核心论点",
    )


def test_segmenter_cache_save_uses_grouped_payload():
    segmenter = SemanticUnitSegmenter(llm_client=object())
    workdir = Path("var/tmp_segmenter_cache_grouped_case1")
    cache_path = workdir / "semantic_cache.json"
    if workdir.exists():
        shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir, exist_ok=True)
    result = SegmentationResult(
        semantic_units=[_build_unit()],
        total_paragraphs_input=2,
        total_units_output=1,
        llm_token_usage=123,
        processing_time_ms=45.6,
    )
    try:
        segmenter._save_to_cache(result, str(cache_path))

        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        assert isinstance(payload.get("knowledge_groups"), list)
        assert payload["knowledge_groups"][0]["group_name"] == "CloudBot 环境配置"
        assert payload["knowledge_groups"][0]["reason"] == "围绕同一核心论点"
        assert "semantic_units" not in payload
        assert "group_id" not in payload["knowledge_groups"][0]["units"][0]
        assert "group_name" not in payload["knowledge_groups"][0]["units"][0]
        assert "group_reason" not in payload["knowledge_groups"][0]["units"][0]
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_segmenter_cache_load_grouped_payload_inherits_group_meta():
    segmenter = SemanticUnitSegmenter(llm_client=object())
    workdir = Path("var/tmp_segmenter_cache_grouped_case2")
    cache_path = workdir / "semantic_cache.json"
    if workdir.exists():
        shutil.rmtree(workdir, ignore_errors=True)
    os.makedirs(workdir, exist_ok=True)
    try:
        cache_path.write_text(
            json.dumps(
                {
                    "schema_version": "phase2a.grouped.v1",
                    "knowledge_groups": [
                        {
                            "group_id": 7,
                            "group_name": "核心概念讲解",
                            "reason": "同一论点下的解释与示例",
                            "units": [
                                {
                                    "unit_id": "SU777",
                                    "knowledge_type": "abstract",
                                    "knowledge_topic": "核心概念",
                                    "full_text": "先解释概念定义。",
                                    "source_paragraph_ids": ["P010"],
                                    "source_sentence_ids": ["S010"],
                                    "start_sec": 20.0,
                                    "end_sec": 33.0,
                                    "confidence": 0.8,
                                    "mult_steps": False,
                                    "action_segments": [],
                                    "stable_islands": [],
                                }
                            ],
                        }
                    ],
                    "total_paragraphs_input": 1,
                    "total_units_output": 1,
                    "llm_token_usage": 66,
                    "processing_time_ms": 7.5,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        loaded = segmenter._load_from_cache(str(cache_path))
        assert loaded.total_units_output == 1
        assert loaded.llm_token_usage == 66
        assert len(loaded.semantic_units) == 1
        assert loaded.semantic_units[0].unit_id == "SU777"
        assert loaded.semantic_units[0].group_id == 7
        assert loaded.semantic_units[0].group_name == "核心概念讲解"
        assert loaded.semantic_units[0].group_reason == "同一论点下的解释与示例"
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
