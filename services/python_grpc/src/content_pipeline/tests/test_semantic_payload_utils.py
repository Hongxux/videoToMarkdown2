from services.python_grpc.src.content_pipeline.shared.semantic_payload import (
    build_grouped_semantic_units_payload,
    normalize_semantic_units_payload,
)


def test_build_grouped_payload_strips_unit_level_group_fields():
    payload = [
        {
            "unit_id": "SU001",
            "start_sec": 0.0,
            "end_sec": 10.0,
            "group_id": 1,
            "group_name": "核心话题A",
            "group_reason": "同一核心论点聚合",
            "knowledge_type": "abstract",
        },
        {
            "unit_id": "SU002",
            "start_sec": 10.0,
            "end_sec": 20.0,
            "group_id": 1,
            "group_name": "核心话题A",
            "group_reason": "同一核心论点聚合",
            "knowledge_type": "process",
        },
    ]

    grouped = build_grouped_semantic_units_payload(payload)
    assert len(grouped["knowledge_groups"]) == 1
    group = grouped["knowledge_groups"][0]
    assert group["group_name"] == "核心话题A"
    assert group["reason"] == "同一核心论点聚合"
    assert "group_name" not in group["units"][0]
    assert "group_reason" not in group["units"][0]
    assert "group_id" not in group["units"][0]


def test_normalize_payload_flattens_grouped_and_inherits_group_meta():
    payload = {
        "knowledge_groups": [
            {
                "group_id": 3,
                "group_name": "核心话题B",
                "reason": "同一核心论点聚合",
                "units": [
                    {
                        "unit_id": "SU100",
                        "start_sec": 1.0,
                        "end_sec": 2.0,
                        "knowledge_type": "concrete",
                    }
                ],
            }
        ]
    }

    flattened = normalize_semantic_units_payload(payload)
    assert len(flattened) == 1
    assert flattened[0]["group_id"] == 3
    assert flattened[0]["group_name"] == "核心话题B"
    assert flattened[0]["group_reason"] == "同一核心论点聚合"
