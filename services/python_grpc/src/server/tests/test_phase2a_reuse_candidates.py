import os
import sys
import json
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[5]))


from services.python_grpc.src.server import grpc_service_impl as impl
from services.python_grpc.src.common.utils.runtime_recovery_store import RuntimeRecoveryStore


def test_phase2a_candidates_include_canonical_and_legacy_paths(tmp_path):
    output_dir = str(tmp_path / "task")
    candidates = impl._phase2a_semantic_units_candidates(output_dir)
    assert len(candidates) == 3
    assert candidates[0].endswith(
        os.path.join("task", "intermediates", "stages", "phase2a", "outputs", "semantic_units.json")
    )
    assert candidates[1].endswith(os.path.join("task", "semantic_units_phase2a.json"))
    assert candidates[2].endswith(
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


def test_write_resource_meta_skips_missing_resource_without_creating_sidecar(tmp_path):
    missing_path = (
        tmp_path
        / "task"
        / "intermediates"
        / "stages"
        / "phase2a"
        / "outputs"
        / "semantic_units.json"
    )

    impl._write_resource_meta(
        str(missing_path),
        group="phase2a",
        input_fingerprint="fp-phase2a",
        dependencies={},
        priority=True,
    )

    assert not missing_path.exists()
    assert not Path(f"{missing_path}.meta.json").exists()


def test_persist_phase2a_semantic_units_payload_syncs_canonical_and_legacy_paths(tmp_path):
    output_dir = tmp_path / "task"
    payload = {"knowledge_groups": [{"group_name": "demo", "units": [{"unit_id": "U001"}]}]}

    persisted_paths = impl._persist_phase2a_semantic_units_payload(str(output_dir), payload)

    assert len(persisted_paths) == 3
    for path in persisted_paths:
        assert Path(path).exists()
        assert json.loads(Path(path).read_text(encoding="utf-8")) == payload


def test_plan_runtime_artifact_reuse_bootstraps_missing_scope_from_existing_file(tmp_path):
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-phase2a-bootstrap")

    input_scope_ref = impl._upsert_runtime_input_scope(
        store,
        stage="phase2a",
        input_name="segmentation_main",
        input_fingerprint="fp-phase2a",
    )

    allow_reuse, reason = impl._plan_runtime_artifact_reuse(
        store,
        stage="phase2a",
        artifact_name="semantic_units",
        expected_input_fingerprint="fp-phase2a",
        current_dependency_fingerprints={input_scope_ref: "fp-phase2a"},
        candidate_path=str(output_dir / "semantic_units.json"),
        bootstrap_on_missing_scope=True,
    )

    artifact_scope_ref = impl._build_runtime_artifact_scope_ref(
        store,
        stage="phase2a",
        artifact_name="semantic_units",
    )
    artifact_node = store.load_scope_node(artifact_scope_ref)

    assert allow_reuse is True
    assert reason == "scope_bootstrapped_from_existing_artifact"
    assert artifact_node is not None
    assert artifact_node["input_fingerprint"] == "fp-phase2a"


def test_plan_runtime_artifact_reuse_blocks_when_upstream_scope_dirty(tmp_path):
    output_dir = tmp_path / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-phase2a-dirty")

    input_scope_ref = impl._upsert_runtime_input_scope(
        store,
        stage="phase2a",
        input_name="segmentation_main",
        input_fingerprint="fp-phase2a",
    )
    impl._upsert_runtime_artifact_scope(
        store,
        stage="phase2a",
        artifact_name="semantic_units",
        input_fingerprint="fp-phase2a",
        local_path=str(output_dir / "semantic_units.json"),
        dependency_fingerprints={input_scope_ref: "fp-phase2a"},
    )
    store.mark_scope_dirty(input_scope_ref, reason="stage1_recomputed", include_descendants=True)

    allow_reuse, reason = impl._plan_runtime_artifact_reuse(
        store,
        stage="phase2a",
        artifact_name="semantic_units",
        expected_input_fingerprint="fp-phase2a",
        current_dependency_fingerprints={input_scope_ref: "fp-phase2a"},
        candidate_path=str(output_dir / "semantic_units.json"),
        bootstrap_on_missing_scope=True,
    )

    assert allow_reuse is False
    assert reason == "scope_marked_dirty"


def test_build_stage1_resume_plan_resumes_after_last_reusable_step(tmp_path):
    output_dir = tmp_path / "task"
    intermediates_dir = output_dir / "intermediates"
    local_storage_dir = output_dir / "local_storage"
    intermediates_dir.mkdir(parents=True, exist_ok=True)
    local_storage_dir.mkdir(parents=True, exist_ok=True)

    stage1_fp = "fp-stage1"
    store = RuntimeRecoveryStore(output_dir=str(output_dir), task_id="task-stage1-plan")

    step2_path = intermediates_dir / "step2_correction_output.json"
    step3_path = intermediates_dir / "step3_merge_output.json"
    step35_path = intermediates_dir / "step3_5_translate_output.json"
    step4_path = intermediates_dir / "step4_clean_local_output.json"
    step6_path = intermediates_dir / "step6_merge_cross_output.json"
    sentence_timestamps_path = local_storage_dir / "sentence_timestamps.json"

    step2_path.write_text(json.dumps({"output": {"corrected_subtitles": [{"subtitle_id": "SUB001"}]}}), encoding="utf-8")
    step3_path.write_text(json.dumps({"output": {"merged_sentences": [{"id": "M001"}]}}), encoding="utf-8")
    step35_path.write_text(json.dumps({"output": {"translated_sentences": [{"id": "T001"}]}}), encoding="utf-8")
    step4_path.write_text(json.dumps({"output": {"cleaned_sentences": [{"id": "C001"}]}}), encoding="utf-8")
    step6_path.write_text(json.dumps({"output": {"pure_text_script": [{"id": "P001"}]}}), encoding="utf-8")
    sentence_timestamps_path.write_text("[]", encoding="utf-8")

    impl._write_resource_meta(
        str(step2_path),
        group="stage1_text",
        input_fingerprint=stage1_fp,
        dependencies={},
    )
    impl._write_resource_meta(
        str(step3_path),
        group="stage1_text",
        input_fingerprint=stage1_fp,
        dependencies={"step2": impl._file_signature(str(step2_path))},
    )
    impl._write_resource_meta(
        str(step35_path),
        group="stage1_text",
        input_fingerprint=stage1_fp,
        dependencies={"step3": impl._file_signature(str(step3_path))},
    )
    impl._write_resource_meta(
        str(step4_path),
        group="stage1_text",
        input_fingerprint=stage1_fp,
        dependencies={
            "step3": impl._file_signature(str(step3_path)),
            "step3_5": impl._file_signature(str(step35_path)),
        },
    )
    impl._write_resource_meta(
        str(step6_path),
        group="stage1_text",
        input_fingerprint=stage1_fp,
        dependencies={"step4": impl._file_signature(str(step4_path))},
    )
    impl._write_resource_meta(
        str(sentence_timestamps_path),
        group="stage1_text",
        input_fingerprint=stage1_fp,
        dependencies={
            "step2": impl._file_signature(str(step2_path)),
            "step6": impl._file_signature(str(step6_path)),
        },
    )

    input_scope_ref = impl._upsert_runtime_input_scope(
        store,
        stage="stage1",
        input_name="main",
        input_fingerprint=stage1_fp,
    )
    stage1_artifact_scope_refs = {
        artifact_name: impl._build_runtime_artifact_scope_ref(
            store,
            stage="stage1",
            artifact_name=artifact_name,
        )
        for artifact_name in (
            "step2_correction",
            "step3_merge",
            "step3_5_translate",
            "step4_clean_local",
            "step5_6_dedup_merge",
            "outputs",
        )
    }

    impl._upsert_runtime_artifact_scope(
        store,
        stage="stage1",
        artifact_name="step2_correction",
        input_fingerprint=stage1_fp,
        local_path=str(step2_path),
        dependency_fingerprints={input_scope_ref: stage1_fp},
    )
    impl._upsert_runtime_artifact_scope(
        store,
        stage="stage1",
        artifact_name="step3_merge",
        input_fingerprint=stage1_fp,
        local_path=str(step3_path),
        dependency_fingerprints={stage1_artifact_scope_refs["step2_correction"]: stage1_fp},
    )
    impl._upsert_runtime_artifact_scope(
        store,
        stage="stage1",
        artifact_name="step3_5_translate",
        input_fingerprint=stage1_fp,
        local_path=str(step35_path),
        dependency_fingerprints={stage1_artifact_scope_refs["step3_merge"]: stage1_fp},
    )
    impl._upsert_runtime_artifact_scope(
        store,
        stage="stage1",
        artifact_name="step4_clean_local",
        input_fingerprint=stage1_fp,
        local_path=str(step4_path),
        dependency_fingerprints={
            stage1_artifact_scope_refs["step3_merge"]: stage1_fp,
            stage1_artifact_scope_refs["step3_5_translate"]: stage1_fp,
        },
    )
    impl._upsert_runtime_artifact_scope(
        store,
        stage="stage1",
        artifact_name="step5_6_dedup_merge",
        input_fingerprint=stage1_fp,
        local_path=str(step6_path),
        dependency_fingerprints={stage1_artifact_scope_refs["step4_clean_local"]: stage1_fp},
    )
    impl._upsert_runtime_artifact_scope(
        store,
        stage="stage1",
        artifact_name="outputs",
        input_fingerprint=stage1_fp,
        local_path=str(step6_path),
        dependency_fingerprints={stage1_artifact_scope_refs["step5_6_dedup_merge"]: stage1_fp},
    )
    store.mark_scope_dirty(
        stage1_artifact_scope_refs["step5_6_dedup_merge"],
        reason="step5_6_recomputed",
        include_descendants=True,
    )

    resume_plan = impl._build_stage1_resume_plan(
        reuse_enabled=True,
        stage1_store=store,
        expected_input_fingerprint=stage1_fp,
        stage1_input_scope_ref=input_scope_ref,
        stage1_artifact_scope_refs=stage1_artifact_scope_refs,
        step2_path=str(step2_path),
        step3_path=str(step3_path),
        step35_path=str(step35_path),
        step4_path=str(step4_path),
        step6_path=str(step6_path),
        local_sentence_ts=str(sentence_timestamps_path),
        need_sentence_timestamps=False,
    )

    assert resume_plan["mode"] == "partial_resume"
    assert resume_plan["resume_from_step"] == "step4_clean_local"
    assert resume_plan["resume_entry_step"] == "step5_6_dedup_merge"
    assert resume_plan["retry_entry_point"] == "step5_6_dedup_merge"
    assert "sentence_timestamps" in resume_plan["reused_artifact_names"]
    assert "outputs" in resume_plan["invalidated_artifact_names"]
    assert resume_plan["failed_scope_id"] == "outputs"
    assert resume_plan["dirty_scope_count"] >= 1
