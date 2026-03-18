import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.benchmarks.runtime_recovery_explain_query_plan import collect_restore_query_plans


def test_runtime_recovery_restore_queries_use_indexes():
    plans = collect_restore_query_plans()

    assert any("idx_llm_restore" in detail for detail in plans["load_latest_committed_llm"])
    assert any("idx_chunk_restore" in detail for detail in plans["load_latest_committed_chunk"])
    assert any("USING INDEX sqlite_autoindex_llm_records_1" in detail for detail in plans["load_latest_llm_attempt"])


def test_runtime_recovery_stage_entry_queries_use_indexes():
    plans = collect_restore_query_plans()

    assert any("idx_scope_hint_plan_stage_unit" in detail for detail in plans["list_scope_hints_pending_llm"])
    assert any("sqlite_autoindex_scope_hint_latest_1" in detail for detail in plans["list_scope_hints_pending_llm"])
    assert any("idx_llm_restore" in detail for detail in plans["batch_load_committed_llm"])
    assert any("idx_chunk_restore" in detail for detail in plans["batch_load_committed_chunk"])
