import os
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


from services.python_grpc.src.server import grpc_service_impl as impl


def test_phase2a_candidates_include_main_and_intermediates(tmp_path):
    output_dir = str(tmp_path / "task")
    candidates = impl._phase2a_semantic_units_candidates(output_dir)
    assert len(candidates) == 2
    assert candidates[0].endswith(os.path.join("task", "semantic_units_phase2a.json"))
    assert candidates[1].endswith(
        os.path.join("task", "intermediates", "semantic_units_phase2a.json")
    )


def test_resolve_reuse_candidate_prefers_intermediates_when_main_missing(tmp_path):
    output_dir = tmp_path / "task"
    (output_dir / "intermediates").mkdir(parents=True, exist_ok=True)

    main_path = output_dir / "semantic_units_phase2a.json"
    inter_path = output_dir / "intermediates" / "semantic_units_phase2a.json"

    inter_path.write_text("[]", encoding="utf-8")

    expected_fp = "fp-demo"
    impl._write_resource_meta(
        str(inter_path),
        group="phase2a",
        input_fingerprint=expected_fp,
        dependencies={},
        priority=True,
    )

    selected, reason = impl._resolve_reuse_candidate(
        [str(main_path), str(inter_path)],
        group="phase2a",
        expected_input_fingerprint=expected_fp,
        reuse_enabled=True,
    )
    assert selected == str(inter_path)
    assert reason == "ok"

