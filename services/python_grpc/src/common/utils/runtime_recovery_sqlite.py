"""
模块说明：为 RuntimeRecoveryStore 提供 SQLite 元信息索引与内容镜像能力。
执行逻辑：
1) 统一维护 llm/chunk 的元信息表与内容表。
2) 在不推翻文件真源协议的前提下，提供更快的精确检索、批量检索与批量回读。
实现方式：通过 SQLite WAL + 小事务 upsert + 可选压缩实现。
核心价值：把“检索层”和“内容镜像层”收敛到同一个数据库里，降低目录扫描与小文件索引的成本。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
import zlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from services.python_grpc.src.common.utils.hash_policy import stable_json_dumps as _policy_stable_json_dumps

logger = logging.getLogger(__name__)

_DEFAULT_BUSY_TIMEOUT_MS = 15000
_DEFAULT_PAGE_SIZE = 32768
_DEFAULT_COMPRESSION_MIN_BYTES = 8192
_DEFAULT_COMPRESSION_LEVEL = 6
_DEFAULT_ENABLE_LLM_FIELD_RESTORE = False
_REQUEST_SCOPE_ID_PATTERN = re.compile(r"^\[(?P<scope_id>[^\]]+)\]\s", re.MULTILINE)


def _read_env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, str(default)) or "").strip()
    try:
        return int(raw)
    except Exception:
        return int(default)


def _read_env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _stable_json_text(payload: Any) -> str:
    return _policy_stable_json_dumps(payload)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(bytes(payload or b"")).hexdigest()


def _build_preview_text(value: str, limit: int = 240) -> str:
    normalized = str(value or "")
    if len(normalized) <= limit:
        return normalized
    return normalized[:limit] + "...<truncated>"


def resolve_runtime_recovery_db_path() -> Path:
    configured = str(os.getenv("TASK_RUNTIME_SQLITE_DB_PATH", "") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    repo_root = Path(__file__).resolve().parents[5]
    return (repo_root / "var" / "state" / "runtime_recovery_index.db").resolve()


class IllegalStateException(RuntimeError):
    """用于标记 SQLite 镜像层内部的状态不一致。"""


class RuntimeRecoverySqliteIndex:
    """封装 llm/chunk 元信息与内容镜像的 SQLite 访问。"""

    _schema_lock = threading.Lock()
    _initialized_paths: set[str] = set()
    _shared_instances_lock = threading.Lock()
    _shared_instances: Dict[str, "RuntimeRecoverySqliteIndex"] = {}

    @classmethod
    def shared(
        cls,
        *,
        db_path: Optional[str] = None,
    ) -> "RuntimeRecoverySqliteIndex":
        resolved_path = Path(db_path).expanduser().resolve() if db_path else resolve_runtime_recovery_db_path()
        cache_key = str(resolved_path)
        with cls._shared_instances_lock:
            instance = cls._shared_instances.get(cache_key)
            if instance is None:
                instance = cls(db_path=str(resolved_path))
                cls._shared_instances[cache_key] = instance
            return instance

    @classmethod
    def release_shared(
        cls,
        *,
        db_path: Optional[str] = None,
    ) -> bool:
        resolved_path = Path(db_path).expanduser().resolve() if db_path else resolve_runtime_recovery_db_path()
        cache_key = str(resolved_path)
        with cls._shared_instances_lock:
            instance = cls._shared_instances.pop(cache_key, None)
        if instance is None:
            return False
        instance.close()
        return True

    @classmethod
    def release_all_shared(cls) -> int:
        with cls._shared_instances_lock:
            instances = list(cls._shared_instances.values())
            cls._shared_instances.clear()
        released = 0
        for instance in instances:
            try:
                instance.close()
                released += 1
            except Exception:
                logger.debug("Ignore runtime sqlite shared close failure: path=%s", getattr(instance, "db_path", ""))
        return released

    def __init__(
        self,
        *,
        db_path: Optional[str] = None,
        page_size: Optional[int] = None,
        busy_timeout_ms: Optional[int] = None,
        compress_large_payloads: Optional[bool] = None,
        compression_min_bytes: Optional[int] = None,
        compression_level: Optional[int] = None,
        enable_llm_field_restore: Optional[bool] = None,
    ) -> None:
        resolved_path = Path(db_path).expanduser().resolve() if db_path else resolve_runtime_recovery_db_path()
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = resolved_path
        self.page_size = max(4096, int(page_size or _read_env_int("TASK_RUNTIME_SQLITE_PAGE_SIZE", _DEFAULT_PAGE_SIZE)))
        self.busy_timeout_ms = max(
            1000,
            int(busy_timeout_ms or _read_env_int("TASK_RUNTIME_SQLITE_BUSY_TIMEOUT_MS", _DEFAULT_BUSY_TIMEOUT_MS)),
        )
        self.compress_large_payloads = _read_env_bool(
            "TASK_RUNTIME_SQLITE_COMPRESS_LARGE_PAYLOADS",
            True if compress_large_payloads is None else bool(compress_large_payloads),
        )
        self.compression_min_bytes = max(
            1024,
            int(
                compression_min_bytes
                or _read_env_int("TASK_RUNTIME_SQLITE_COMPRESSION_MIN_BYTES", _DEFAULT_COMPRESSION_MIN_BYTES)
            ),
        )
        self.compression_level = min(
            9,
            max(
                1,
                int(
                    compression_level
                    or _read_env_int("TASK_RUNTIME_SQLITE_COMPRESSION_LEVEL", _DEFAULT_COMPRESSION_LEVEL)
                ),
            ),
        )
        self.enable_llm_field_restore = _read_env_bool(
            "TASK_RUNTIME_SQLITE_ENABLE_LLM_FIELD_RESTORE",
            _DEFAULT_ENABLE_LLM_FIELD_RESTORE if enable_llm_field_restore is None else bool(enable_llm_field_restore),
        )
        self._process_pid = int(os.getpid())
        self._thread_local = threading.local()
        self._write_lock = threading.Lock()
        self._read_connections_lock = threading.Lock()
        self._read_connections: Dict[int, sqlite3.Connection] = {}
        self._write_connection: Optional[sqlite3.Connection] = None
        self._ensure_schema()

    def _connect(self, *, check_same_thread: bool = True) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.db_path),
            timeout=self.busy_timeout_ms / 1000.0,
            check_same_thread=check_same_thread,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA temp_store=MEMORY")
        return connection

    def _reset_process_local_state(self) -> None:
        self._close_all_read_connections()
        self._thread_local = threading.local()
        self._invalidate_write_connection()
        self._process_pid = int(os.getpid())

    def _ensure_process_local_state(self) -> None:
        current_pid = int(os.getpid())
        if current_pid == self._process_pid:
            return
        self._reset_process_local_state()

    def _get_write_connection(self) -> sqlite3.Connection:
        self._ensure_process_local_state()
        connection = self._write_connection
        if connection is None:
            connection = self._connect(check_same_thread=False)
            self._write_connection = connection
        return connection

    def _invalidate_write_connection(self) -> None:
        if self._write_connection is not None:
            try:
                self._write_connection.close()
            except Exception:
                pass
            self._write_connection = None

    def _register_read_connection(self, connection: sqlite3.Connection) -> None:
        thread_id = threading.get_ident()
        with self._read_connections_lock:
            previous = self._read_connections.get(thread_id)
            self._read_connections[thread_id] = connection
        if previous is not None and previous is not connection:
            try:
                previous.close()
            except Exception:
                pass

    def _close_all_read_connections(self) -> int:
        with self._read_connections_lock:
            connections = list(self._read_connections.values())
            self._read_connections.clear()
        closed = 0
        for connection in connections:
            try:
                connection.close()
            except Exception:
                pass
            else:
                closed += 1
        return closed

    def close(self) -> int:
        self._ensure_process_local_state()
        closed_read_connections = self._close_all_read_connections()
        self._thread_local = threading.local()
        self._invalidate_write_connection()
        return closed_read_connections

    @staticmethod
    def _table_has_column(connection: sqlite3.Connection, table_name: str, column_name: str) -> bool:
        rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        for row in rows:
            if str(row[1] or "") == str(column_name or ""):
                return True
        return False

    def _ensure_table_column(
        self,
        connection: sqlite3.Connection,
        *,
        table_name: str,
        column_name: str,
        column_ddl: str,
    ) -> None:
        if self._table_has_column(connection, table_name, column_name):
            return
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_ddl}")

    @staticmethod
    def _is_missing_table_error(error: BaseException) -> bool:
        return "no such table" in str(error or "").lower()

    def _repair_schema_after_missing_table(self) -> None:
        normalized_path = str(self.db_path)
        self._reset_process_local_state()
        with self._schema_lock:
            self._initialized_paths.discard(normalized_path)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        normalized_path = str(self.db_path)
        if normalized_path in self._initialized_paths:
            return
        with self._schema_lock:
            if normalized_path in self._initialized_paths:
                return
            is_new_db = not self.db_path.exists() or self.db_path.stat().st_size == 0
            connection = sqlite3.connect(str(self.db_path), timeout=self.busy_timeout_ms / 1000.0)
            try:
                connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
                if is_new_db:
                    connection.execute(f"PRAGMA page_size={self.page_size}")
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA temp_store=MEMORY")
                current_page_size = int(connection.execute("PRAGMA page_size").fetchone()[0] or 0)
                if current_page_size != self.page_size:
                    logger.info(
                        "Runtime recovery SQLite page_size=%s (requested=%s, path=%s)",
                        current_page_size,
                        self.page_size,
                        self.db_path,
                    )
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS task_meta (
                        singleton_key INTEGER PRIMARY KEY CHECK (singleton_key = 1),
                        normalized_video_key TEXT NOT NULL DEFAULT '',
                        schema_version TEXT NOT NULL DEFAULT '',
                        updated_at_ms INTEGER NOT NULL DEFAULT 0
                    );

                    CREATE TABLE IF NOT EXISTS stage_snapshots (
                        stage TEXT NOT NULL,
                        stage_owner TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL,
                        checkpoint TEXT NOT NULL DEFAULT '',
                        completed INTEGER NOT NULL DEFAULT 0,
                        pending INTEGER NOT NULL DEFAULT 0,
                        updated_at_ms INTEGER NOT NULL DEFAULT 0,
                        stage_state_path TEXT NOT NULL DEFAULT '',
                        retry_mode TEXT NOT NULL DEFAULT '',
                        retry_entry_point TEXT NOT NULL DEFAULT '',
                        required_action TEXT NOT NULL DEFAULT '',
                        retry_strategy TEXT NOT NULL DEFAULT '',
                        operator_action TEXT NOT NULL DEFAULT '',
                        action_hint TEXT NOT NULL DEFAULT '',
                        error_class TEXT NOT NULL DEFAULT '',
                        error_code TEXT NOT NULL DEFAULT '',
                        error_message TEXT NOT NULL DEFAULT '',
                        subtitle_path TEXT NOT NULL DEFAULT '',
                        domain TEXT NOT NULL DEFAULT '',
                        main_topic TEXT NOT NULL DEFAULT '',
                        PRIMARY KEY (stage)
                    );

                    CREATE TABLE IF NOT EXISTS scope_nodes (
                        scope_ref TEXT NOT NULL,
                        normalized_video_key TEXT NOT NULL DEFAULT '',
                        stage TEXT NOT NULL DEFAULT '',
                        scope_type TEXT NOT NULL DEFAULT '',
                        scope_id TEXT NOT NULL DEFAULT '',
                        scope_variant TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL DEFAULT '',
                        input_fingerprint TEXT NOT NULL DEFAULT '',
                        local_path TEXT NOT NULL DEFAULT '',
                        chunk_id TEXT NOT NULL DEFAULT '',
                        unit_id TEXT NOT NULL DEFAULT '',
                        stage_step TEXT NOT NULL DEFAULT '',
                        retry_mode TEXT NOT NULL DEFAULT '',
                        retry_entry_point TEXT NOT NULL DEFAULT '',
                        required_action TEXT NOT NULL DEFAULT '',
                        error_class TEXT NOT NULL DEFAULT '',
                        error_code TEXT NOT NULL DEFAULT '',
                        error_message TEXT NOT NULL DEFAULT '',
                        plan_context_json TEXT NOT NULL DEFAULT '{}',
                        resource_snapshot_json TEXT NOT NULL DEFAULT '{}',
                        attempt_count INTEGER NOT NULL DEFAULT 0,
                        result_hash TEXT NOT NULL DEFAULT '',
                        dirty_reason TEXT NOT NULL DEFAULT '',
                        dirty_at_ms INTEGER NOT NULL DEFAULT 0,
                        updated_at_ms INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (scope_ref)
                    );

                    CREATE TABLE IF NOT EXISTS scope_edges (
                        scope_ref TEXT NOT NULL,
                        depends_on_scope_ref TEXT NOT NULL,
                        dependency_fingerprint TEXT NOT NULL DEFAULT '',
                        updated_at_ms INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (scope_ref, depends_on_scope_ref),
                        FOREIGN KEY(scope_ref)
                            REFERENCES scope_nodes(scope_ref)
                            ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS llm_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        normalized_video_key TEXT NOT NULL DEFAULT '',
                        stage TEXT NOT NULL,
                        chunk_id TEXT NOT NULL,
                        llm_call_id TEXT NOT NULL,
                        input_fingerprint TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        unit_id TEXT NOT NULL DEFAULT '',
                        stage_step TEXT NOT NULL DEFAULT '',
                        response_hash TEXT NOT NULL DEFAULT '',
                        request_scope_ids_json TEXT NOT NULL DEFAULT '[]',
                        error_class TEXT NOT NULL DEFAULT '',
                        error_code TEXT NOT NULL DEFAULT '',
                        error_message TEXT NOT NULL DEFAULT '',
                        updated_at_ms INTEGER NOT NULL DEFAULT 0,
                        committed_at_ms INTEGER NOT NULL DEFAULT 0,
                        UNIQUE(stage, chunk_id, llm_call_id, attempt)
                    );

                    CREATE TABLE IF NOT EXISTS llm_record_content (
                        llm_record_id INTEGER PRIMARY KEY,
                        response_codec TEXT NOT NULL DEFAULT '',
                        response_payload BLOB,
                        FOREIGN KEY(llm_record_id) REFERENCES llm_records(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS chunk_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        normalized_video_key TEXT NOT NULL DEFAULT '',
                        stage TEXT NOT NULL,
                        chunk_id TEXT NOT NULL,
                        input_fingerprint TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        result_hash TEXT NOT NULL DEFAULT '',
                        error_class TEXT NOT NULL DEFAULT '',
                        error_code TEXT NOT NULL DEFAULT '',
                        error_message TEXT NOT NULL DEFAULT '',
                        updated_at_ms INTEGER NOT NULL DEFAULT 0,
                        committed_at_ms INTEGER NOT NULL DEFAULT 0,
                        UNIQUE(stage, chunk_id, attempt)
                    );

                    CREATE TABLE IF NOT EXISTS chunk_record_content (
                        chunk_record_id INTEGER PRIMARY KEY,
                        result_codec TEXT NOT NULL DEFAULT '',
                        result_payload BLOB,
                        FOREIGN KEY(chunk_record_id) REFERENCES chunk_records(id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS scope_hint_plan (
                        normalized_video_key TEXT NOT NULL DEFAULT '',
                        stage TEXT NOT NULL,
                        scope_type TEXT NOT NULL,
                        scope_id TEXT NOT NULL,
                        scope_ref TEXT NOT NULL,
                        scope_variant TEXT NOT NULL DEFAULT '',
                        chunk_id TEXT NOT NULL DEFAULT '',
                        llm_call_id TEXT NOT NULL DEFAULT '',
                        unit_id TEXT NOT NULL DEFAULT '',
                        stage_step TEXT NOT NULL DEFAULT '',
                        input_fingerprint TEXT NOT NULL DEFAULT '',
                        dependency_fingerprints_json TEXT NOT NULL DEFAULT '{}',
                        depends_on_json TEXT NOT NULL DEFAULT '[]',
                        plan_status TEXT NOT NULL,
                        dirty_reason TEXT NOT NULL DEFAULT '',
                        retry_mode TEXT NOT NULL DEFAULT '',
                        retry_entry_point TEXT NOT NULL DEFAULT '',
                        required_action TEXT NOT NULL DEFAULT '',
                        local_path TEXT NOT NULL DEFAULT '',
                        updated_at_ms INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (scope_ref)
                    );

                    CREATE TABLE IF NOT EXISTS scope_hint_latest (
                        normalized_video_key TEXT NOT NULL DEFAULT '',
                        stage TEXT NOT NULL,
                        scope_type TEXT NOT NULL,
                        scope_id TEXT NOT NULL,
                        scope_ref TEXT NOT NULL,
                        scope_variant TEXT NOT NULL DEFAULT '',
                        chunk_id TEXT NOT NULL DEFAULT '',
                        llm_call_id TEXT NOT NULL DEFAULT '',
                        unit_id TEXT NOT NULL DEFAULT '',
                        stage_step TEXT NOT NULL DEFAULT '',
                        latest_status TEXT NOT NULL,
                        durable_status TEXT NOT NULL DEFAULT '',
                        input_fingerprint TEXT NOT NULL DEFAULT '',
                        latest_attempt INTEGER NOT NULL DEFAULT 0,
                        can_restore INTEGER NOT NULL DEFAULT 0,
                        dirty_reason TEXT NOT NULL DEFAULT '',
                        retry_mode TEXT NOT NULL DEFAULT '',
                        retry_entry_point TEXT NOT NULL DEFAULT '',
                        required_action TEXT NOT NULL DEFAULT '',
                        error_class TEXT NOT NULL DEFAULT '',
                        error_code TEXT NOT NULL DEFAULT '',
                        error_message TEXT NOT NULL DEFAULT '',
                        local_path TEXT NOT NULL DEFAULT '',
                        source_kind TEXT NOT NULL DEFAULT '',
                        updated_at_ms INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (scope_ref)
                    );

                    """
                )
                self._ensure_table_column(
                    connection,
                    table_name="llm_records",
                    column_name="unit_id",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="llm_records",
                    column_name="stage_step",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="llm_records",
                    column_name="request_scope_ids_json",
                    column_ddl="TEXT NOT NULL DEFAULT '[]'",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_hint_plan",
                    column_name="unit_id",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_hint_plan",
                    column_name="stage_step",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_hint_latest",
                    column_name="unit_id",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_hint_latest",
                    column_name="stage_step",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="chunk_id",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="unit_id",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="stage_step",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="retry_mode",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="retry_entry_point",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="required_action",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="error_class",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="error_code",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="error_message",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="plan_context_json",
                    column_ddl="TEXT NOT NULL DEFAULT '{}'",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="resource_snapshot_json",
                    column_ddl="TEXT NOT NULL DEFAULT '{}'",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="attempt_count",
                    column_ddl="INTEGER NOT NULL DEFAULT 0",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="result_hash",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="dirty_reason",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="scope_nodes",
                    column_name="dirty_at_ms",
                    column_ddl="INTEGER NOT NULL DEFAULT 0",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="retry_mode",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="retry_entry_point",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="required_action",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="retry_strategy",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="operator_action",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="action_hint",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="error_class",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="error_code",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="error_message",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="subtitle_path",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="domain",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                self._ensure_table_column(
                    connection,
                    table_name="stage_snapshots",
                    column_name="main_topic",
                    column_ddl="TEXT NOT NULL DEFAULT ''",
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_task_meta_updated
                    ON task_meta(updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_stage_snapshots_updated
                    ON stage_snapshots(updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_stage_snapshots_status
                    ON stage_snapshots(status, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_nodes_stage_status
                    ON scope_nodes(stage, scope_type, status, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_edges_dependency
                    ON scope_edges(depends_on_scope_ref, updated_at_ms DESC)
                    """
                )
                connection.execute("DROP TABLE IF EXISTS task_artifacts")
                connection.execute("DROP TABLE IF EXISTS stage_journal_events")
                connection.execute("DROP TABLE IF EXISTS stage_outputs_manifests")
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_llm_restore
                    ON llm_records(stage, chunk_id, llm_call_id, input_fingerprint, status, committed_at_ms DESC, attempt DESC)
                    """
                )
                connection.execute(
                    """
                    DROP INDEX IF EXISTS idx_llm_task_stage_status
                    """
                )
                connection.execute(
                    """
                    DROP INDEX IF EXISTS idx_llm_storage_lookup
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_llm_stage_status
                    ON llm_records(stage, status, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_chunk_restore
                    ON chunk_records(stage, chunk_id, input_fingerprint, status, committed_at_ms DESC, attempt DESC)
                    """
                )
                connection.execute(
                    """
                    DROP INDEX IF EXISTS idx_chunk_task_stage_status
                    """
                )
                connection.execute(
                    """
                    DROP INDEX IF EXISTS idx_chunk_storage_lookup
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_chunk_stage_status
                    ON chunk_records(stage, status, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_hint_plan_stage_status
                    ON scope_hint_plan(stage, scope_type, plan_status, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_hint_latest_stage_status
                    ON scope_hint_latest(stage, scope_type, latest_status, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_hint_latest_retry
                    ON scope_hint_latest(retry_mode, latest_status, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_hint_plan_stage_unit
                    ON scope_hint_plan(stage, scope_type, unit_id, stage_step, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_hint_latest_stage_unit
                    ON scope_hint_latest(stage, scope_type, unit_id, stage_step, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_nodes_stage
                    ON scope_nodes(stage, scope_type, updated_at_ms DESC)
                    """
                )
                connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_scope_nodes_status
                    ON scope_nodes(status, updated_at_ms DESC)
                    """
                )
                connection.commit()
            finally:
                connection.close()
            self._initialized_paths.add(normalized_path)

    def _run_write(self, callback):
        last_error: Optional[Exception] = None
        with self._write_lock:
            for attempt_index in range(4):
                connection = self._get_write_connection()
                try:
                    connection.execute("BEGIN IMMEDIATE")
                    result = callback(connection)
                    connection.commit()
                    return result
                except sqlite3.ProgrammingError as error:
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    self._invalidate_write_connection()
                    last_error = error
                    continue
                except sqlite3.OperationalError as error:
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    lowered = str(error).lower()
                    if self._is_missing_table_error(error):
                        self._repair_schema_after_missing_table()
                        last_error = error
                        continue
                    if "closed" in lowered:
                        self._invalidate_write_connection()
                        last_error = error
                        continue
                    if "locked" not in lowered and "busy" not in lowered:
                        raise
                    last_error = error
                    time.sleep(0.05 * (attempt_index + 1))
                except Exception:
                    try:
                        connection.rollback()
                    except Exception:
                        pass
                    raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("sqlite write failed without explicit error")

    def _run_read(self, callback):
        self._ensure_process_local_state()
        connection = getattr(self._thread_local, "read_connection", None)
        if connection is None:
            connection = self._connect()
            self._thread_local.read_connection = connection
            self._register_read_connection(connection)
        try:
            return callback(connection)
        except sqlite3.ProgrammingError:
            try:
                connection.close()
            except Exception:
                pass
            connection = self._connect()
            self._thread_local.read_connection = connection
            self._register_read_connection(connection)
            return callback(connection)
        except sqlite3.OperationalError as error:
            if not self._is_missing_table_error(error):
                raise
            try:
                connection.close()
            except Exception:
                pass
            self._thread_local.read_connection = None
            self._repair_schema_after_missing_table()
            connection = self._connect()
            self._thread_local.read_connection = connection
            self._register_read_connection(connection)
            return callback(connection)
        finally:
            pass

    def _encode_json_blob(self, payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if payload is None:
            return {"codec": "", "blob": None, "preview": "", "raw_bytes": 0, "payload_hash": ""}
        raw_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)
        encoded = self._encode_text_blob(raw_text, base_codec="json-utf8")
        encoded["json_value"] = payload
        return encoded

    def _encode_text_blob(self, text: str, *, base_codec: str = "text-utf8") -> Dict[str, Any]:
        raw_bytes = str(text or "").encode("utf-8")
        codec = base_codec
        payload_bytes = raw_bytes
        if self.compress_large_payloads and len(raw_bytes) >= self.compression_min_bytes:
            compressed = zlib.compress(raw_bytes, self.compression_level)
            if len(compressed) < len(raw_bytes) * 0.9:
                codec = f"{base_codec}+zlib"
                payload_bytes = compressed
        return {
            "codec": codec,
            "blob": sqlite3.Binary(payload_bytes),
            "preview": _build_preview_text(str(text or "")),
            "raw_bytes": len(raw_bytes),
            "payload_hash": _sha256_bytes(raw_bytes),
            "text_value": str(text or ""),
        }

    @staticmethod
    def _decode_text_blob(codec: str, payload: Optional[bytes]) -> str:
        if not codec or payload in (None, b""):
            return ""
        blob = bytes(payload or b"")
        normalized_codec = str(codec or "").strip()
        if normalized_codec.endswith("+zlib"):
            blob = zlib.decompress(blob)
            normalized_codec = normalized_codec[:-5]
        if normalized_codec not in {"text-utf8", "json-utf8"}:
            raise ValueError(f"unsupported sqlite payload codec: {codec}")
        return blob.decode("utf-8")

    @classmethod
    def _decode_json_blob(cls, codec: str, payload: Optional[bytes]) -> Optional[Dict[str, Any]]:
        raw_text = cls._decode_text_blob(codec, payload)
        if not raw_text:
            return None
        decoded = json.loads(raw_text)
        return decoded if isinstance(decoded, dict) else None

    @staticmethod
    def _encode_small_json_text(payload: Any, *, empty_default: str = "") -> str:
        if payload is None:
            return empty_default
        if isinstance(payload, dict) and not payload:
            return empty_default
        if isinstance(payload, list) and not payload:
            return empty_default
        return _stable_json_text(payload)

    @staticmethod
    def _decode_json_text_payload(raw_text: str, *, expected_type: Optional[type] = None, default: Any = None) -> Any:
        normalized = str(raw_text or "").strip()
        if not normalized:
            return default
        decoded = json.loads(normalized)
        if expected_type is not None and not isinstance(decoded, expected_type):
            return default
        return decoded

    @classmethod
    def _split_llm_response_metadata(cls, response_metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        return {}

    @classmethod
    def _build_llm_response_metadata_from_row(cls, row: sqlite3.Row) -> Dict[str, Any]:
        return {}

    @classmethod
    def _build_minimal_llm_manifest_payload(cls, row: sqlite3.Row, response_metadata: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "schema_version": "runtime_llm_manifest_v1",
            "stage": str(row["stage"] or ""),
            "chunk_id": str(row["chunk_id"] or ""),
            "llm_call_id": str(row["llm_call_id"] or ""),
            "attempt": int(row["attempt"] or 0),
            "status": str(row["status"] or ""),
            "input_fingerprint": str(row["input_fingerprint"] or ""),
            "unit_id": str(row["unit_id"] or ""),
            "stage_step": str(row["stage_step"] or ""),
            "step_name": str(row["stage_step"] or ""),
            "request_scope_ids": cls._decode_json_text_payload(
                str(row["request_scope_ids_json"] or ""),
                expected_type=list,
                default=[],
            )
            or [],
            "updated_at_ms": int(row["updated_at_ms"] or 0),
            "response_metadata": response_metadata,
        }

    @classmethod
    def _build_minimal_llm_commit_payload(cls, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "schema_version": "runtime_llm_commit_v1",
            "stage": str(row["stage"] or ""),
            "chunk_id": str(row["chunk_id"] or ""),
            "llm_call_id": str(row["llm_call_id"] or ""),
            "attempt": int(row["attempt"] or 0),
            "status": str(row["status"] or ""),
            "input_fingerprint": str(row["input_fingerprint"] or ""),
            "response_hash": str(row["response_hash"] or ""),
            "committed_at_ms": int(row["committed_at_ms"] or 0),
        }

    @classmethod
    def _build_minimal_chunk_state_payload(cls, row: sqlite3.Row) -> Dict[str, Any]:
        payload = {
            "schema_version": "runtime_chunk_state_v1",
            "stage": str(row["stage"] or ""),
            "chunk_id": str(row["chunk_id"] or ""),
            "attempt": int(row["attempt"] or 0),
            "status": str(row["status"] or ""),
            "input_fingerprint": str(row["input_fingerprint"] or ""),
            "result_hash": str(row["result_hash"] or ""),
            "updated_at_ms": int(row["updated_at_ms"] or 0),
        }
        error_class = str(row["error_class"] or "")
        error_code = str(row["error_code"] or "")
        error_message = str(row["error_message"] or "")
        if error_class:
            payload["error_class"] = error_class
        if error_code:
            payload["error_code"] = error_code
        if error_message:
            payload["error_message"] = error_message
        return payload

    @classmethod
    def _build_minimal_chunk_commit_payload(cls, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "schema_version": "runtime_chunk_commit_v1",
            "stage": str(row["stage"] or ""),
            "chunk_id": str(row["chunk_id"] or ""),
            "attempt": int(row["attempt"] or 0),
            "status": str(row["status"] or ""),
            "input_fingerprint": str(row["input_fingerprint"] or ""),
            "result_hash": str(row["result_hash"] or ""),
            "committed_at_ms": int(row["committed_at_ms"] or 0),
        }

    def _restore_llm_from_row(self, row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        if row is None or row["id"] in (None, ""):
            return None
        response_text = self._decode_text_blob(str(row["response_codec"] or ""), row["response_payload"])
        if _sha256_text(response_text) != str(row["response_hash"] or ""):
            return None
        request_scope_ids = self._decode_json_text_payload(
            str(row["request_scope_ids_json"] or ""),
            expected_type=list,
            default=[],
        )
        if not isinstance(request_scope_ids, list):
            request_scope_ids = []
        try:
            response_metadata = self._build_llm_response_metadata_from_row(row)
        except Exception:
            logger.exception("Runtime recovery SQLite llm field restore failed")
            return None
        return {
            "response_text": response_text,
            "response_metadata": response_metadata,
            "request_scope_ids": list(request_scope_ids),
            "attempt": int(row["attempt"] or 0),
            "commit_payload": self._build_minimal_llm_commit_payload(row),
            "manifest_payload": self._build_minimal_llm_manifest_payload(row, response_metadata),
            "response_hash": str(row["response_hash"] or ""),
            "source": "sqlite",
        }

    def _restore_chunk_from_row(self, row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
        if row is None or row["id"] in (None, ""):
            return None
        result_payload = self._decode_json_blob(str(row["result_codec"] or ""), row["result_payload"])
        if not isinstance(result_payload, dict):
            return None
        if _sha256_text(_stable_json_text(result_payload)) != str(row["result_hash"] or ""):
            return None
        return {
            "commit_payload": self._build_minimal_chunk_commit_payload(row),
            "chunk_state": self._build_minimal_chunk_state_payload(row),
            "result_payload": result_payload,
            "source": "sqlite",
        }

    @staticmethod
    def _batched(values: Sequence[Any], batch_size: int) -> List[List[Any]]:
        normalized = list(values or [])
        if batch_size <= 0:
            return [normalized]
        return [normalized[index:index + batch_size] for index in range(0, len(normalized), batch_size)]

    def _upsert_llm_row(self, connection: sqlite3.Connection, values: Dict[str, Any]) -> int:
        columns = [
            "normalized_video_key",
            "stage",
            "chunk_id",
            "llm_call_id",
            "input_fingerprint",
            "attempt",
            "status",
            "unit_id",
            "stage_step",
            "response_hash",
            "request_scope_ids_json",
            "error_class",
            "error_code",
            "error_message",
            "updated_at_ms",
            "committed_at_ms",
        ]
        placeholders = ", ".join("?" for _ in columns)
        update_clause = ", ".join(f"{column}=excluded.{column}" for column in columns[5:])
        connection.execute(
            f"""
            INSERT INTO llm_records ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(stage, chunk_id, llm_call_id, attempt)
            DO UPDATE SET {update_clause}
            """,
            tuple(values.get(column) for column in columns),
        )
        row = connection.execute(
            """
            SELECT id
            FROM llm_records
            WHERE stage = ? AND chunk_id = ? AND llm_call_id = ? AND attempt = ?
            """,
            (
                values.get("stage"),
                values.get("chunk_id"),
                values.get("llm_call_id"),
                values.get("attempt"),
            ),
        ).fetchone()
        if row is None:
            raise IllegalStateException("llm sqlite upsert lost row id")
        return int(row["id"])

    def _upsert_chunk_row(self, connection: sqlite3.Connection, values: Dict[str, Any]) -> int:
        columns = [
            "normalized_video_key",
            "stage",
            "chunk_id",
            "input_fingerprint",
            "attempt",
            "status",
            "result_hash",
            "error_class",
            "error_code",
            "error_message",
            "updated_at_ms",
            "committed_at_ms",
        ]
        placeholders = ", ".join("?" for _ in columns)
        update_clause = ", ".join(f"{column}=excluded.{column}" for column in columns[5:])
        connection.execute(
            f"""
            INSERT INTO chunk_records ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT(stage, chunk_id, attempt)
            DO UPDATE SET {update_clause}
            """,
            tuple(values.get(column) for column in columns),
        )
        row = connection.execute(
            """
            SELECT id
            FROM chunk_records
            WHERE stage = ? AND chunk_id = ? AND attempt = ?
            """,
            (
                values.get("stage"),
                values.get("chunk_id"),
                values.get("attempt"),
            ),
        ).fetchone()
        if row is None:
            raise IllegalStateException("chunk sqlite upsert lost row id")
        return int(row["id"])

    @staticmethod
    def _plan_status_from_scope_status(status: str, dirty_reason: str, explicit_retry_mode: str = "") -> str:
        normalized_status = RuntimeRecoverySqliteIndex._canonicalize_status(status)
        normalized_reason = str(dirty_reason or "").strip().lower()
        normalized_retry_mode = str(explicit_retry_mode or "").strip().lower()
        if normalized_status == "DIRTY":
            if "fallback" in normalized_reason:
                return "FALLBACK_RETRY_PENDING"
            if "manual" in normalized_reason or "repair" in normalized_reason:
                return "MANUAL_RETRY_PENDING"
            return "RECOMPUTE_PENDING"
        if normalized_status == "SUCCESS":
            return "SATISFIED"
        if normalized_status == "RUNNING":
            return "IN_FLIGHT"
        if normalized_status == "ERROR":
            if normalized_retry_mode == "manual":
                return "MANUAL_REPAIR_REQUIRED"
            return "AUTO_RETRY_PENDING"
        if normalized_status in {"MANUAL_NEEDED", "FAILED"}:
            return "MANUAL_REPAIR_REQUIRED"
        if normalized_status == "STALE":
            return "INVALIDATED"
        return "PLANNED"

    @staticmethod
    def _retry_mode_from_scope_state(status: str, dirty_reason: str, explicit_retry_mode: str) -> str:
        normalized_explicit = str(explicit_retry_mode or "").strip().lower()
        if normalized_explicit:
            return normalized_explicit
        normalized_status = RuntimeRecoverySqliteIndex._canonicalize_status(status)
        normalized_reason = str(dirty_reason or "").strip().lower()
        if normalized_status == "ERROR":
            return "auto"
        if normalized_status in {"MANUAL_NEEDED", "FAILED"}:
            return "manual"
        if normalized_status == "DIRTY" and "fallback" in normalized_reason:
            return "fallback"
        if normalized_status == "DIRTY" and normalized_reason:
            return "manual"
        return ""

    @staticmethod
    def _can_restore_from_status(status: str) -> int:
        return 1 if RuntimeRecoverySqliteIndex._canonicalize_status(status) == "SUCCESS" else 0

    @staticmethod
    def _canonicalize_status(status: Any) -> str:
        normalized = str(status or "").strip().upper()
        if normalized in {"PLANNED", "PLANNING", "RETRYING", "RETRING"}:
            return "PLANNED"
        if normalized in {"RUNNING", "RUNING", "LOCAL_WRITING", "EXECUTING"}:
            return "RUNNING"
        if normalized in {"SUCCESS", "LOCAL_COMMITTED", "COMPLETED"}:
            return "SUCCESS"
        if normalized in {"MANUAL_NEEDED", "MANUAL_RETRY_REQUIRED", "MANUL_NEEDED"}:
            return "MANUAL_NEEDED"
        if normalized in {"ERROR", "AUTO_RETRY_WAIT"}:
            return "ERROR"
        if normalized in {"FAILED", "FATAL", "FAIL"}:
            return "FAILED"
        return normalized

    @staticmethod
    def _normalize_json_string(payload: Any, default_text: str) -> str:
        if payload in (None, "", [], {}):
            return default_text
        try:
            return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        except Exception:
            return default_text

    @staticmethod
    def _normalize_dependency_fingerprint_map(payload: Any) -> Dict[str, str]:
        normalized: Dict[str, str] = {}
        if not isinstance(payload, dict):
            return normalized
        for raw_scope_ref, raw_fingerprint in payload.items():
            scope_ref = str(raw_scope_ref or "").strip()
            fingerprint = str(raw_fingerprint or "").strip()
            if not scope_ref or not fingerprint:
                continue
            normalized[scope_ref] = fingerprint
        return dict(sorted(normalized.items()))

    @staticmethod
    def _coerce_scope_id_list(payload: Any) -> List[str]:
        normalized: List[str] = []
        seen: set[str] = set()
        if isinstance(payload, (list, tuple, set)):
            values = list(payload)
        elif payload in (None, ""):
            values = []
        else:
            values = [payload]
        for raw_item in values:
            scope_id = str(raw_item or "").strip()
            if not scope_id or scope_id in seen:
                continue
            seen.add(scope_id)
            normalized.append(scope_id)
        return normalized

    @classmethod
    def _extract_request_scope_ids(cls, request_payload: Optional[Dict[str, Any]]) -> List[str]:
        payload = dict(request_payload or {})
        structured_candidates: List[Any] = [
            payload.get("request_scope_ids"),
            payload.get("scope_ids"),
        ]
        for nested_key in ("metadata", "runtime_identity", "extra_payload", "kwargs"):
            nested_payload = payload.get(nested_key)
            if not isinstance(nested_payload, dict):
                continue
            structured_candidates.extend(
                [
                    nested_payload.get("request_scope_ids"),
                    nested_payload.get("scope_ids"),
                ]
            )
        for candidate in structured_candidates:
            normalized_candidate = cls._coerce_scope_id_list(candidate)
            if normalized_candidate:
                return normalized_candidate
        prompt = str((request_payload or {}).get("prompt", "") or "")
        extracted: List[str] = []
        seen: set[str] = set()
        for match in _REQUEST_SCOPE_ID_PATTERN.finditer(prompt):
            scope_id = str(match.group("scope_id") or "").strip()
            if not scope_id or scope_id in seen:
                continue
            seen.add(scope_id)
            extracted.append(scope_id)
        return extracted

    @staticmethod
    def _hash_json_payload(payload: Optional[Dict[str, Any]]) -> str:
        if not isinstance(payload, dict) or not payload:
            return ""
        return _sha256_text(_stable_json_text(payload))

    @staticmethod
    def _normalize_scope_ref_list(payload: Any) -> List[str]:
        normalized: List[str] = []
        seen: set[str] = set()
        for raw_item in list(payload or []):
            scope_ref = str(raw_item or "").strip()
            if not scope_ref or scope_ref in seen:
                continue
            seen.add(scope_ref)
            normalized.append(scope_ref)
        return normalized

    @staticmethod
    def _first_non_blank_text(payload: Any, *field_names: str) -> str:
        if not isinstance(payload, dict):
            return ""
        for field_name in field_names:
            value = str(payload.get(field_name, "") or "").strip()
            if value:
                return value
        return ""

    @staticmethod
    def _build_stage_snapshot_scalar_fields(payload: Optional[Dict[str, Any]]) -> Dict[str, str]:
        normalized_payload = dict(payload or {})
        return {
            "retry_mode": RuntimeRecoverySqliteIndex._first_non_blank_text(normalized_payload, "retry_mode", "retryMode"),
            "retry_entry_point": RuntimeRecoverySqliteIndex._first_non_blank_text(
                normalized_payload,
                "retry_entry_point",
                "retryEntryPoint",
            ),
            "required_action": RuntimeRecoverySqliteIndex._first_non_blank_text(
                normalized_payload,
                "required_action",
                "requiredAction",
            ),
            "retry_strategy": RuntimeRecoverySqliteIndex._first_non_blank_text(
                normalized_payload,
                "retry_strategy",
                "retryStrategy",
            ),
            "operator_action": RuntimeRecoverySqliteIndex._first_non_blank_text(
                normalized_payload,
                "operator_action",
                "operatorAction",
            ),
            "action_hint": RuntimeRecoverySqliteIndex._first_non_blank_text(
                normalized_payload,
                "action_hint",
                "actionHint",
            ),
            "error_class": RuntimeRecoverySqliteIndex._first_non_blank_text(normalized_payload, "error_class", "errorClass"),
            "error_code": RuntimeRecoverySqliteIndex._first_non_blank_text(normalized_payload, "error_code", "errorCode"),
            "error_message": RuntimeRecoverySqliteIndex._first_non_blank_text(
                normalized_payload,
                "error_message",
                "errorMessage",
            ),
        }

    def _decode_stage_snapshot_row(self, row: sqlite3.Row, *, output_dir: str) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "stage": str(row["stage"] or ""),
            "stage_owner": str(row["stage_owner"] or ""),
            "status": str(row["status"] or ""),
            "checkpoint": str(row["checkpoint"] or ""),
            "completed": int(row["completed"] or 0),
            "pending": int(row["pending"] or 0),
            "updated_at_ms": int(row["updated_at_ms"] or 0),
            "output_dir": str(output_dir or ""),
            "stage_state_path": str(row["stage_state_path"] or ""),
            "local_stage_state_path": str(row["stage_state_path"] or ""),
            "subtitle_path": str(row["subtitle_path"] or ""),
            "domain": str(row["domain"] or ""),
            "main_topic": str(row["main_topic"] or ""),
        }
        for field_name in (
            "retry_mode",
            "retry_entry_point",
            "required_action",
            "retry_strategy",
            "operator_action",
            "action_hint",
            "error_class",
            "error_code",
            "error_message",
        ):
            field_value = str(row[field_name] or "").strip()
            if field_value:
                payload[field_name] = field_value
        return payload

    def _load_scope_edges(
        self,
        connection: sqlite3.Connection,
        *,
        scope_ref: Optional[str] = None,
    ) -> Dict[str, Dict[str, str]]:
        query = """
            SELECT scope_ref, depends_on_scope_ref, dependency_fingerprint
            FROM scope_edges
            WHERE 1 = 1
        """
        params: List[Any] = []
        normalized_scope_ref = str(scope_ref or "").strip()
        if normalized_scope_ref:
            query += " AND scope_ref = ?"
            params.append(normalized_scope_ref)
        query += " ORDER BY scope_ref ASC, depends_on_scope_ref ASC"
        edge_map: Dict[str, Dict[str, str]] = {}
        for row in connection.execute(query, tuple(params)).fetchall():
            current_scope_ref = str(row["scope_ref"] or "").strip()
            dependency_scope_ref = str(row["depends_on_scope_ref"] or "").strip()
            if not current_scope_ref or not dependency_scope_ref:
                continue
            edge_map.setdefault(current_scope_ref, {})[dependency_scope_ref] = str(row["dependency_fingerprint"] or "")
        return edge_map

    def _decode_scope_node_row(
        self,
        row: sqlite3.Row,
        dependency_edges: Optional[Dict[str, str]] = None,
        *,
        output_dir: str,
    ) -> Dict[str, Any]:
        normalized_dependencies = self._normalize_dependency_fingerprint_map(dependency_edges or {})
        row_keys = set(row.keys())
        payload: Dict[str, Any] = {
            "output_dir": str(output_dir or ""),
            "normalized_video_key": str(row["normalized_video_key"] or ""),
            "stage": str(row["stage"] or ""),
            "scope_type": str(row["scope_type"] or ""),
            "scope_id": str(row["scope_id"] or ""),
            "scope_ref": str(row["scope_ref"] or ""),
            "scope_variant": str(row["scope_variant"] or ""),
            "status": str(row["status"] or ""),
            "input_fingerprint": str(row["input_fingerprint"] or ""),
            "local_path": str(row["local_path"] or ""),
            "updated_at_ms": int(row["updated_at_ms"] or 0),
            "dependency_fingerprints": normalized_dependencies,
            "depends_on": self._normalize_scope_ref_list(list(normalized_dependencies.keys())),
        }
        for field_name in (
            "chunk_id",
            "unit_id",
            "stage_step",
            "retry_mode",
            "retry_entry_point",
            "required_action",
            "error_class",
            "error_code",
            "error_message",
        ):
            if field_name not in row_keys:
                continue
            field_value = str(row[field_name] or "").strip()
            if field_value:
                payload[field_name] = field_value
        dirty_reason = str(row["dirty_reason"] or "").strip() if "dirty_reason" in row_keys else ""
        if dirty_reason:
            payload["dirty_reason"] = dirty_reason
        dirty_at_ms = int(row["dirty_at_ms"] or 0) if "dirty_at_ms" in row_keys else 0
        if dirty_at_ms > 0:
            payload["dirty_at_ms"] = dirty_at_ms
        if "plan_context_json" in row_keys:
            plan_context = self._decode_json_text_payload(
                str(row["plan_context_json"] or ""),
                expected_type=dict,
                default={},
            )
            if isinstance(plan_context, dict) and plan_context:
                payload["plan_context"] = plan_context
        if "resource_snapshot_json" in row_keys:
            resource_snapshot = self._decode_json_text_payload(
                str(row["resource_snapshot_json"] or ""),
                expected_type=dict,
                default={},
            )
            if isinstance(resource_snapshot, dict) and resource_snapshot:
                payload["resource_snapshot"] = resource_snapshot
        attempt_count = int(row["attempt_count"] or 0) if "attempt_count" in row_keys else 0
        if attempt_count > 0:
            payload["attempt_count"] = attempt_count
        result_hash = str(row["result_hash"] or "").strip() if "result_hash" in row_keys else ""
        if result_hash:
            payload["result_hash"] = result_hash
        return payload

    def load_scope_node(self, *, output_dir: str, scope_ref: str) -> Optional[Dict[str, Any]]:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_output_dir or not normalized_scope_ref:
            return None

        def _read(connection: sqlite3.Connection) -> Optional[Dict[str, Any]]:
            row = connection.execute(
                """
                SELECT
                    normalized_video_key,
                    stage,
                    scope_type,
                    scope_id,
                    scope_ref,
                    scope_variant,
                    status,
                    input_fingerprint,
                    local_path,
                    chunk_id,
                    unit_id,
                    stage_step,
                    retry_mode,
                    retry_entry_point,
                    required_action,
                    error_class,
                    error_code,
                    error_message,
                    plan_context_json,
                    resource_snapshot_json,
                    attempt_count,
                    result_hash,
                    dirty_reason,
                    dirty_at_ms,
                    updated_at_ms
                FROM scope_nodes
                WHERE scope_ref = ?
                LIMIT 1
                """,
                (normalized_scope_ref,),
            ).fetchone()
            if row is None:
                return None
            return self._decode_scope_node_row(
                row,
                self._load_scope_edges(
                    connection,
                    scope_ref=normalized_scope_ref,
                ).get(normalized_scope_ref, {}),
                output_dir=normalized_output_dir,
            )

        return self._run_read(_read)

    def list_scope_nodes(
        self,
        *,
        output_dir: str,
        stage: str = "",
        scope_type: str = "",
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        normalized_output_dir = str(output_dir or "").strip()
        if not normalized_output_dir:
            return []
        predicates = ["1 = 1"]
        params: List[Any] = []
        normalized_stage = str(stage or "").strip()
        if normalized_stage:
            predicates.append("stage = ?")
            params.append(normalized_stage)
        normalized_scope_type = str(scope_type or "").strip()
        if normalized_scope_type:
            predicates.append("scope_type = ?")
            params.append(normalized_scope_type)
        params.append(max(1, int(limit or 1)))

        def _read(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
            rows = connection.execute(
                f"""
                SELECT
                    normalized_video_key,
                    stage,
                    scope_type,
                    scope_id,
                    scope_ref,
                    scope_variant,
                    status,
                    input_fingerprint,
                    local_path,
                    chunk_id,
                    unit_id,
                    stage_step,
                    retry_mode,
                    retry_entry_point,
                    required_action,
                    error_class,
                    error_code,
                    error_message,
                    plan_context_json,
                    resource_snapshot_json,
                    attempt_count,
                    result_hash,
                    dirty_reason,
                    dirty_at_ms,
                    updated_at_ms
                FROM scope_nodes
                WHERE {" AND ".join(predicates)}
                ORDER BY updated_at_ms DESC, scope_ref ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()
            dependency_edges = self._load_scope_edges(connection)
            return [
                self._decode_scope_node_row(
                    row,
                    dependency_edges.get(str(row["scope_ref"] or ""), {}),
                    output_dir=normalized_output_dir,
                )
                for row in rows
            ]

        return self._run_read(_read)

    def upsert_scope_node(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        scope_ref: str,
        stage: str,
        scope_type: str,
        scope_id: str,
        scope_variant: str,
        status: str,
        input_fingerprint: str = "",
        local_path: str = "",
        dirty_reason: str = "",
        dirty_at_ms: int = 0,
        updated_at_ms: int = 0,
        dependency_fingerprints: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_output_dir or not normalized_scope_ref:
            return
        safe_updated_at_ms = int(updated_at_ms or time.time() * 1000)
        normalized_dependencies = self._normalize_dependency_fingerprint_map(dependency_fingerprints)
        normalized_depends_on = self._normalize_scope_ref_list(
            depends_on if depends_on is not None else list(normalized_dependencies.keys())
        )
        node_payload = dict(payload or {})
        normalized_dirty_reason = str(dirty_reason or node_payload.get("dirty_reason", "") or "").strip()
        safe_dirty_at_ms = int(dirty_at_ms or node_payload.get("dirty_at_ms", 0) or 0)
        normalized_status = self._canonicalize_status(status)
        normalized_chunk_id = str(node_payload.get("chunk_id", "") or "").strip()
        normalized_unit_id = str(node_payload.get("unit_id", "") or "").strip()
        normalized_stage_step = str(node_payload.get("stage_step", "") or "").strip()
        normalized_retry_mode = self._first_non_blank_text(node_payload, "retry_mode", "retryMode")
        normalized_retry_entry_point = self._first_non_blank_text(
            node_payload,
            "retry_entry_point",
            "retryEntryPoint",
        )
        normalized_required_action = self._first_non_blank_text(
            node_payload,
            "required_action",
            "requiredAction",
        )
        normalized_plan_context_json = self._normalize_json_string(
            node_payload.get("plan_context"),
            "{}",
        )
        normalized_resource_snapshot_json = self._normalize_json_string(
            node_payload.get("resource_snapshot"),
            "{}",
        )
        normalized_error_class = self._first_non_blank_text(node_payload, "error_class", "errorClass")
        normalized_error_code = self._first_non_blank_text(node_payload, "error_code", "errorCode")
        normalized_error_message = self._first_non_blank_text(node_payload, "error_message", "errorMessage")
        normalized_attempt_count = max(0, int(node_payload.get("attempt_count", 0) or 0))
        normalized_result_hash = self._first_non_blank_text(node_payload, "result_hash", "resultHash")
        if normalized_status != "DIRTY":
            normalized_dirty_reason = ""
            safe_dirty_at_ms = 0
        elif safe_dirty_at_ms <= 0:
            safe_dirty_at_ms = safe_updated_at_ms

        def _write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO scope_nodes (
                    scope_ref, normalized_video_key, stage, scope_type, scope_id,
                    scope_variant, status, input_fingerprint, local_path, chunk_id, unit_id, stage_step,
                    retry_mode, retry_entry_point, required_action, error_class, error_code, error_message,
                    plan_context_json, resource_snapshot_json, attempt_count, result_hash,
                    dirty_reason, dirty_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope_ref) DO UPDATE SET
                    normalized_video_key = excluded.normalized_video_key,
                    stage = excluded.stage,
                    scope_type = excluded.scope_type,
                    scope_id = excluded.scope_id,
                    scope_variant = excluded.scope_variant,
                    status = excluded.status,
                    input_fingerprint = excluded.input_fingerprint,
                    local_path = excluded.local_path,
                    chunk_id = excluded.chunk_id,
                    unit_id = excluded.unit_id,
                    stage_step = excluded.stage_step,
                    retry_mode = excluded.retry_mode,
                    retry_entry_point = excluded.retry_entry_point,
                    required_action = excluded.required_action,
                    error_class = excluded.error_class,
                    error_code = excluded.error_code,
                    error_message = excluded.error_message,
                    plan_context_json = excluded.plan_context_json,
                    resource_snapshot_json = excluded.resource_snapshot_json,
                    attempt_count = excluded.attempt_count,
                    result_hash = excluded.result_hash,
                    dirty_reason = excluded.dirty_reason,
                    dirty_at_ms = excluded.dirty_at_ms,
                    updated_at_ms = excluded.updated_at_ms
                """,
                (
                    normalized_scope_ref,
                    str(normalized_video_key or ""),
                    str(stage or ""),
                    str(scope_type or ""),
                    str(scope_id or ""),
                    str(scope_variant or ""),
                    str(status or ""),
                    str(input_fingerprint or ""),
                    str(local_path or ""),
                    normalized_chunk_id,
                    normalized_unit_id,
                    normalized_stage_step,
                    normalized_retry_mode,
                    normalized_retry_entry_point,
                    normalized_required_action,
                    normalized_error_class,
                    normalized_error_code,
                    normalized_error_message,
                    normalized_plan_context_json,
                    normalized_resource_snapshot_json,
                    normalized_attempt_count,
                    normalized_result_hash,
                    normalized_dirty_reason,
                    safe_dirty_at_ms,
                    safe_updated_at_ms,
                ),
            )
            connection.execute(
                """
                DELETE FROM scope_edges
                WHERE scope_ref = ?
                """,
                (normalized_scope_ref,),
            )
            if not normalized_dependencies:
                return
            connection.executemany(
                """
                INSERT INTO scope_edges (
                    scope_ref, depends_on_scope_ref, dependency_fingerprint, updated_at_ms
                ) VALUES (?, ?, ?, ?)
                """,
                [
                    (
                        normalized_scope_ref,
                        dependency_scope_ref,
                        str(normalized_dependencies.get(dependency_scope_ref, "") or ""),
                        safe_updated_at_ms,
                    )
                    for dependency_scope_ref in normalized_depends_on
                ],
            )

        self._run_write(_write)

    def append_stage_journal_event(
        self,
        *,
        output_dir: str,
        task_id: str,
        stage: str,
        event: str,
        checkpoint: str = "",
        status: str = "",
        completed: int = 0,
        pending: int = 0,
        updated_at_ms: int = 0,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return dict(payload or {})

    def list_stage_journal_events(
        self,
        *,
        output_dir: str,
        stage: str,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        return []

    def upsert_stage_outputs_manifest(
        self,
        *,
        output_dir: str,
        task_id: str,
        stage: str,
        updated_at_ms: int = 0,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return dict(payload or {})

    def load_stage_outputs_manifest(self, *, output_dir: str, stage: str) -> Optional[Dict[str, Any]]:
        return None

    def upsert_scope_hint(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        stage: str,
        scope_type: str,
        scope_id: str,
        scope_ref: str,
        scope_variant: str = "",
        chunk_id: str = "",
        llm_call_id: str = "",
        unit_id: str = "",
        stage_step: str = "",
        status: str,
        input_fingerprint: str = "",
        dependency_fingerprints: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
        dirty_reason: str = "",
        retry_mode: str = "",
        retry_entry_point: str = "",
        required_action: str = "",
        error_class: str = "",
        error_code: str = "",
        error_message: str = "",
        latest_attempt: int = 0,
        local_path: str = "",
        source_kind: str = "",
        updated_at_ms: int = 0,
    ) -> None:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_output_dir or not normalized_scope_ref:
            return
        normalized_status = str(status or "").strip().upper()
        normalized_dirty_reason = str(dirty_reason or "").strip()
        normalized_retry_mode = self._retry_mode_from_scope_state(
            normalized_status,
            normalized_dirty_reason,
            retry_mode,
        )
        normalized_retry_entry_point = str(retry_entry_point or "").strip()
        if not normalized_retry_entry_point and normalized_retry_mode == "fallback":
            normalized_retry_entry_point = "fallback_repair:from_scope_hint"
        safe_updated_at_ms = int(updated_at_ms or time.time() * 1000)
        plan_values = {
            "normalized_video_key": str(normalized_video_key or ""),
            "stage": str(stage or ""),
            "scope_type": str(scope_type or ""),
            "scope_id": str(scope_id or ""),
            "scope_ref": normalized_scope_ref,
            "scope_variant": str(scope_variant or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "unit_id": str(unit_id or ""),
            "stage_step": str(stage_step or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "dependency_fingerprints_json": self._normalize_json_string(dependency_fingerprints, "{}"),
            "depends_on_json": self._normalize_json_string(depends_on or [], "[]"),
            "plan_status": self._plan_status_from_scope_status(
                normalized_status,
                normalized_dirty_reason,
                retry_mode,
            ),
            "dirty_reason": normalized_dirty_reason,
            "retry_mode": normalized_retry_mode,
            "retry_entry_point": normalized_retry_entry_point,
            "required_action": str(required_action or ""),
            "local_path": str(local_path or ""),
            "updated_at_ms": safe_updated_at_ms,
        }
        latest_values = {
            "normalized_video_key": str(normalized_video_key or ""),
            "stage": str(stage or ""),
            "scope_type": str(scope_type or ""),
            "scope_id": str(scope_id or ""),
            "scope_ref": normalized_scope_ref,
            "scope_variant": str(scope_variant or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "unit_id": str(unit_id or ""),
            "stage_step": str(stage_step or ""),
            "latest_status": normalized_status,
            "durable_status": normalized_status if normalized_status == "SUCCESS" else "",
            "input_fingerprint": str(input_fingerprint or ""),
            "latest_attempt": int(latest_attempt or 0),
            "can_restore": self._can_restore_from_status(normalized_status),
            "dirty_reason": normalized_dirty_reason,
            "retry_mode": normalized_retry_mode,
            "retry_entry_point": normalized_retry_entry_point,
            "required_action": str(required_action or ""),
            "error_class": str(error_class or ""),
            "error_code": str(error_code or ""),
            "error_message": str(error_message or ""),
            "local_path": str(local_path or ""),
            "source_kind": str(source_kind or ""),
            "updated_at_ms": safe_updated_at_ms,
        }

        def _write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO scope_hint_plan (
                    normalized_video_key, stage, scope_type, scope_id, scope_ref,
                    scope_variant, chunk_id, llm_call_id, unit_id, stage_step, input_fingerprint, dependency_fingerprints_json,
                    depends_on_json, plan_status, dirty_reason, retry_mode, retry_entry_point, required_action,
                    local_path, updated_at_ms
                ) VALUES (
                    :normalized_video_key, :stage, :scope_type, :scope_id, :scope_ref,
                    :scope_variant, :chunk_id, :llm_call_id, :unit_id, :stage_step, :input_fingerprint, :dependency_fingerprints_json,
                    :depends_on_json, :plan_status, :dirty_reason, :retry_mode, :retry_entry_point, :required_action,
                    :local_path, :updated_at_ms
                )
                ON CONFLICT(scope_ref) DO UPDATE SET
                    normalized_video_key = excluded.normalized_video_key,
                    stage = excluded.stage,
                    scope_type = excluded.scope_type,
                    scope_id = excluded.scope_id,
                    scope_variant = excluded.scope_variant,
                    chunk_id = excluded.chunk_id,
                    llm_call_id = excluded.llm_call_id,
                    unit_id = excluded.unit_id,
                    stage_step = excluded.stage_step,
                    input_fingerprint = excluded.input_fingerprint,
                    dependency_fingerprints_json = excluded.dependency_fingerprints_json,
                    depends_on_json = excluded.depends_on_json,
                    plan_status = excluded.plan_status,
                    dirty_reason = excluded.dirty_reason,
                    retry_mode = excluded.retry_mode,
                    retry_entry_point = excluded.retry_entry_point,
                    required_action = excluded.required_action,
                    local_path = excluded.local_path,
                    updated_at_ms = excluded.updated_at_ms
                """,
                plan_values,
            )
            connection.execute(
                """
                INSERT INTO scope_hint_latest (
                    normalized_video_key, stage, scope_type, scope_id, scope_ref,
                    scope_variant, chunk_id, llm_call_id, unit_id, stage_step, latest_status, durable_status, input_fingerprint,
                    latest_attempt, can_restore, dirty_reason, retry_mode, retry_entry_point, required_action,
                    error_class, error_code, error_message, local_path, source_kind, updated_at_ms
                ) VALUES (
                    :normalized_video_key, :stage, :scope_type, :scope_id, :scope_ref,
                    :scope_variant, :chunk_id, :llm_call_id, :unit_id, :stage_step, :latest_status, :durable_status, :input_fingerprint,
                    :latest_attempt, :can_restore, :dirty_reason, :retry_mode, :retry_entry_point, :required_action,
                    :error_class, :error_code, :error_message, :local_path, :source_kind, :updated_at_ms
                )
                ON CONFLICT(scope_ref) DO UPDATE SET
                    normalized_video_key = excluded.normalized_video_key,
                    stage = excluded.stage,
                    scope_type = excluded.scope_type,
                    scope_id = excluded.scope_id,
                    scope_variant = excluded.scope_variant,
                    chunk_id = excluded.chunk_id,
                    llm_call_id = excluded.llm_call_id,
                    unit_id = excluded.unit_id,
                    stage_step = excluded.stage_step,
                    latest_status = excluded.latest_status,
                    durable_status = excluded.durable_status,
                    input_fingerprint = excluded.input_fingerprint,
                    latest_attempt = excluded.latest_attempt,
                    can_restore = excluded.can_restore,
                    dirty_reason = excluded.dirty_reason,
                    retry_mode = excluded.retry_mode,
                    retry_entry_point = excluded.retry_entry_point,
                    required_action = excluded.required_action,
                    error_class = excluded.error_class,
                    error_code = excluded.error_code,
                    error_message = excluded.error_message,
                    local_path = excluded.local_path,
                    source_kind = excluded.source_kind,
                    updated_at_ms = excluded.updated_at_ms
                """,
                latest_values,
            )

        self._run_write(_write)

    def refresh_task_meta(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_output_dir = str(output_dir or "").strip()
        if not normalized_output_dir:
            return
        meta_payload = dict(payload or {})
        safe_updated_at_ms = int(meta_payload.get("updated_at_ms", time.time() * 1000) or time.time() * 1000)
        meta_payload.setdefault("output_dir", normalized_output_dir)
        meta_payload.setdefault("normalized_video_key", str(normalized_video_key or ""))
        meta_payload.setdefault("updated_at_ms", safe_updated_at_ms)

        def _write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO task_meta (
                    singleton_key, normalized_video_key, schema_version, updated_at_ms
                ) VALUES (1, ?, ?, ?)
                ON CONFLICT(singleton_key) DO UPDATE SET
                    normalized_video_key = excluded.normalized_video_key,
                    schema_version = excluded.schema_version,
                    updated_at_ms = excluded.updated_at_ms
                """,
                (
                    str(normalized_video_key or ""),
                    str(meta_payload.get("schema_version", "") or ""),
                    safe_updated_at_ms,
                ),
            )
            connection.execute(
                """
                UPDATE llm_records
                SET normalized_video_key = ?
                """,
                (str(normalized_video_key or ""),),
            )
            connection.execute(
                """
                UPDATE chunk_records
                SET normalized_video_key = ?
                """,
                (str(normalized_video_key or ""),),
            )

        self._run_write(_write)

    def load_task_meta(self, *, output_dir: str) -> Optional[Dict[str, Any]]:
        normalized_output_dir = str(output_dir or "").strip()
        if not normalized_output_dir:
            return None

        def _read(connection: sqlite3.Connection) -> Optional[Dict[str, Any]]:
            row = connection.execute(
                """
                SELECT normalized_video_key, schema_version, updated_at_ms
                FROM task_meta
                WHERE singleton_key = 1
                LIMIT 1
                """,
            ).fetchone()
            if row is None:
                return None
            return {
                "schema_version": str(row["schema_version"] or "runtime_task_meta_v1"),
                "output_dir": normalized_output_dir,
                "normalized_video_key": str(row["normalized_video_key"] or ""),
                "updated_at_ms": int(row["updated_at_ms"] or 0),
            }

        return self._run_read(_read)

    def upsert_stage_snapshot(
        self,
        *,
        output_dir: str,
        task_id: str,
        stage: str,
        stage_owner: str,
        status: str,
        checkpoint: str,
        completed: int,
        pending: int,
        updated_at_ms: int,
        stage_state_path: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_stage = str(stage or "").strip()
        if not normalized_output_dir or not normalized_stage:
            return
        snapshot_payload = dict(payload or {})
        safe_updated_at_ms = int(updated_at_ms or snapshot_payload.get("updated_at_ms", time.time() * 1000) or time.time() * 1000)
        extra_fields = self._build_stage_snapshot_scalar_fields(snapshot_payload)
        def _write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO stage_snapshots (
                    stage, stage_owner, status, checkpoint, completed, pending, updated_at_ms,
                    stage_state_path, retry_mode, retry_entry_point, required_action, retry_strategy, subtitle_path,
                    domain, main_topic, operator_action, action_hint, error_class, error_code, error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(stage) DO UPDATE SET
                    stage_owner = excluded.stage_owner,
                    status = excluded.status,
                    checkpoint = excluded.checkpoint,
                    completed = excluded.completed,
                    pending = excluded.pending,
                    updated_at_ms = excluded.updated_at_ms,
                    stage_state_path = excluded.stage_state_path,
                    retry_mode = excluded.retry_mode,
                    retry_entry_point = excluded.retry_entry_point,
                    required_action = excluded.required_action,
                    retry_strategy = excluded.retry_strategy,
                    subtitle_path = excluded.subtitle_path,
                    domain = excluded.domain,
                    main_topic = excluded.main_topic,
                    operator_action = excluded.operator_action,
                    action_hint = excluded.action_hint,
                    error_class = excluded.error_class,
                    error_code = excluded.error_code,
                    error_message = excluded.error_message
                """,
                (
                    normalized_stage,
                    str(stage_owner or ""),
                    str(status or ""),
                    str(checkpoint or ""),
                    int(completed or 0),
                    int(pending or 0),
                    safe_updated_at_ms,
                    str(stage_state_path or ""),
                    extra_fields["retry_mode"],
                    extra_fields["retry_entry_point"],
                    extra_fields["required_action"],
                    extra_fields["retry_strategy"],
                    self._first_non_blank_text(snapshot_payload, "subtitle_path"),
                    self._first_non_blank_text(snapshot_payload, "domain"),
                    self._first_non_blank_text(snapshot_payload, "main_topic"),
                    extra_fields["operator_action"],
                    extra_fields["action_hint"],
                    extra_fields["error_class"],
                    extra_fields["error_code"],
                    extra_fields["error_message"],
                ),
            )

        self._run_write(_write)

    def load_stage_snapshot(self, *, output_dir: str, stage: str) -> Optional[Dict[str, Any]]:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_stage = str(stage or "").strip()
        if not normalized_output_dir or not normalized_stage:
            return None

        def _read(connection: sqlite3.Connection) -> Optional[Dict[str, Any]]:
            row = connection.execute(
                """
                SELECT
                    stage,
                    stage_owner,
                    status,
                    checkpoint,
                    completed,
                    pending,
                    updated_at_ms,
                    stage_state_path,
                    retry_mode,
                    retry_entry_point,
                    required_action,
                    retry_strategy,
                    subtitle_path,
                    domain,
                    main_topic,
                    operator_action,
                    action_hint,
                    error_class,
                    error_code,
                    error_message
                FROM stage_snapshots
                WHERE stage = ?
                LIMIT 1
                """,
                (normalized_stage,),
            ).fetchone()
            if row is None:
                return None
            return self._decode_stage_snapshot_row(row, output_dir=normalized_output_dir)

        return self._run_read(_read)

    def list_stage_snapshots(
        self,
        *,
        output_dir: str,
        limit: int = 32,
    ) -> List[Dict[str, Any]]:
        normalized_output_dir = str(output_dir or "").strip()
        if not normalized_output_dir:
            return []

        def _read(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
            rows = connection.execute(
                """
                SELECT
                    stage,
                    stage_owner,
                    status,
                    checkpoint,
                    completed,
                    pending,
                    updated_at_ms,
                    stage_state_path,
                    retry_mode,
                    retry_entry_point,
                    required_action,
                    retry_strategy,
                    subtitle_path,
                    domain,
                    main_topic,
                    operator_action,
                    action_hint,
                    error_class,
                    error_code,
                    error_message
                FROM stage_snapshots
                ORDER BY updated_at_ms DESC, stage ASC
                LIMIT ?
                """,
                (max(1, int(limit or 1)),),
            ).fetchall()
            return [self._decode_stage_snapshot_row(row, output_dir=normalized_output_dir) for row in rows]

        return self._run_read(_read)

    def _legacy_unused_upsert_scope_node(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        scope_ref: str,
        stage: str,
        scope_type: str,
        scope_id: str,
        scope_variant: str,
        status: str,
        input_fingerprint: str = "",
        local_path: str = "",
        dirty_reason: str = "",
        dirty_at_ms: int = 0,
        updated_at_ms: int = 0,
        dependency_fingerprints: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_output_dir or not normalized_scope_ref:
            return
        safe_updated_at_ms = int(updated_at_ms or time.time() * 1000)
        normalized_dependencies = self._normalize_dependency_fingerprint_map(dependency_fingerprints)
        normalized_depends_on = self._normalize_scope_ref_list(
            depends_on if depends_on is not None else list(normalized_dependencies.keys())
        )
        normalized_dirty_reason = str(dirty_reason or "")
        safe_dirty_at_ms = int(dirty_at_ms or 0)
        node_payload = dict(payload or {})
        node_payload.setdefault("scope_ref", normalized_scope_ref)
        node_payload.setdefault("task_id", str(task_id or ""))
        node_payload.setdefault("storage_key", str(storage_key or ""))
        node_payload.setdefault("normalized_video_key", str(normalized_video_key or ""))
        node_payload.setdefault("stage", str(stage or ""))
        node_payload.setdefault("scope_type", str(scope_type or ""))
        node_payload.setdefault("scope_id", str(scope_id or ""))
        node_payload.setdefault("scope_variant", str(scope_variant or ""))
        node_payload.setdefault("status", str(status or ""))
        node_payload.setdefault("input_fingerprint", str(input_fingerprint or ""))
        node_payload.setdefault("local_path", str(local_path or ""))
        node_payload["dependency_fingerprints"] = normalized_dependencies
        node_payload["depends_on"] = normalized_depends_on
        if not normalized_dirty_reason:
            normalized_dirty_reason = str(node_payload.get("dirty_reason", "") or "")
        if safe_dirty_at_ms <= 0:
            safe_dirty_at_ms = int(node_payload.get("dirty_at_ms", 0) or 0)
        node_payload["dirty_reason"] = normalized_dirty_reason
        if safe_dirty_at_ms > 0:
            node_payload["dirty_at_ms"] = safe_dirty_at_ms
        elif str(status or "").strip().upper() != "DIRTY":
            node_payload.pop("dirty_at_ms", None)
        node_payload["updated_at_ms"] = safe_updated_at_ms
        node_payload_json = self._normalize_json_string(node_payload, "{}")
        dependency_fingerprints_json = self._normalize_json_string(normalized_dependencies, "{}")
        depends_on_json = self._normalize_json_string(normalized_depends_on, "[]")

        def _write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO scope_nodes (
                    output_dir, scope_ref, task_id, storage_key, normalized_video_key, stage, scope_type, scope_id,
                    scope_variant, status, input_fingerprint, local_path, dirty_reason, dirty_at_ms, updated_at_ms,
                    dependency_fingerprints_json, depends_on_json, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(output_dir, scope_ref) DO UPDATE SET
                    task_id = excluded.task_id,
                    storage_key = excluded.storage_key,
                    normalized_video_key = excluded.normalized_video_key,
                    stage = excluded.stage,
                    scope_type = excluded.scope_type,
                    scope_id = excluded.scope_id,
                    scope_variant = excluded.scope_variant,
                    status = excluded.status,
                    input_fingerprint = excluded.input_fingerprint,
                    local_path = excluded.local_path,
                    dirty_reason = excluded.dirty_reason,
                    dirty_at_ms = excluded.dirty_at_ms,
                    updated_at_ms = excluded.updated_at_ms,
                    dependency_fingerprints_json = excluded.dependency_fingerprints_json,
                    depends_on_json = excluded.depends_on_json,
                    payload_json = excluded.payload_json
                """,
                (
                    normalized_output_dir,
                    normalized_scope_ref,
                    str(task_id or ""),
                    str(storage_key or ""),
                    str(normalized_video_key or ""),
                    str(stage or ""),
                    str(scope_type or ""),
                    str(scope_id or ""),
                    str(scope_variant or ""),
                    str(status or ""),
                    str(input_fingerprint or ""),
                    str(local_path or ""),
                    str(dirty_reason or ""),
                    int(dirty_at_ms or 0),
                    safe_updated_at_ms,
                    dependency_fingerprints_json,
                    depends_on_json,
                    node_payload_json,
                ),
            )
            connection.execute(
                """
                DELETE FROM scope_edges
                WHERE output_dir = ? AND scope_ref = ?
                """,
                (normalized_output_dir, normalized_scope_ref),
            )
            if not normalized_dependencies:
                return
            connection.executemany(
                """
                INSERT INTO scope_edges (
                    output_dir, scope_ref, depends_on_scope_ref, dependency_fingerprint, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        normalized_output_dir,
                        normalized_scope_ref,
                        dependency_scope_ref,
                        dependency_fingerprint,
                        safe_updated_at_ms,
                    )
                    for dependency_scope_ref, dependency_fingerprint in normalized_dependencies.items()
                ],
            )

        self._run_write(_write)

    def _legacy_unused_load_scope_node(self, *, output_dir: str, scope_ref: str) -> Optional[Dict[str, Any]]:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_scope_ref = str(scope_ref or "").strip()
        if not normalized_output_dir or not normalized_scope_ref:
            return None

        def _read(connection: sqlite3.Connection) -> Optional[Dict[str, Any]]:
            row = connection.execute(
                """
                SELECT
                    output_dir,
                    scope_ref,
                    task_id,
                    storage_key,
                    normalized_video_key,
                    stage,
                    scope_type,
                    scope_id,
                    scope_variant,
                    status,
                    input_fingerprint,
                    local_path,
                    dirty_reason,
                    dirty_at_ms,
                    updated_at_ms,
                    dependency_fingerprints_json,
                    depends_on_json,
                    payload_json
                FROM scope_nodes
                WHERE output_dir = ? AND scope_ref = ?
                LIMIT 1
                """,
                (normalized_output_dir, normalized_scope_ref),
            ).fetchone()
            if row is None:
                return None
            payload = self._decode_json_text_payload(
                str(row["payload_json"] or ""),
                expected_type=dict,
                default={},
            )
            if not isinstance(payload, dict):
                payload = {}
            payload.setdefault("output_dir", str(row["output_dir"] or ""))
            payload.setdefault("scope_ref", str(row["scope_ref"] or ""))
            payload.setdefault("task_id", str(row["task_id"] or ""))
            payload.setdefault("storage_key", str(row["storage_key"] or ""))
            payload.setdefault("normalized_video_key", str(row["normalized_video_key"] or ""))
            payload.setdefault("stage", str(row["stage"] or ""))
            payload.setdefault("scope_type", str(row["scope_type"] or ""))
            payload.setdefault("scope_id", str(row["scope_id"] or ""))
            payload.setdefault("scope_variant", str(row["scope_variant"] or ""))
            payload.setdefault("status", str(row["status"] or ""))
            payload.setdefault("input_fingerprint", str(row["input_fingerprint"] or ""))
            payload.setdefault("local_path", str(row["local_path"] or ""))
            payload["dependency_fingerprints"] = self._decode_json_text_payload(
                str(row["dependency_fingerprints_json"] or ""),
                expected_type=dict,
                default={},
            ) or {}
            payload["depends_on"] = self._decode_json_text_payload(
                str(row["depends_on_json"] or ""),
                expected_type=list,
                default=[],
            ) or []
            payload["dirty_reason"] = str(row["dirty_reason"] or "")
            if int(row["dirty_at_ms"] or 0) > 0:
                payload["dirty_at_ms"] = int(row["dirty_at_ms"] or 0)
            payload["updated_at_ms"] = int(row["updated_at_ms"] or 0)
            return payload

        return self._run_read(_read)

    def _legacy_unused_list_scope_nodes(
        self,
        *,
        output_dir: str,
        stage: str = "",
        scope_type: str = "",
        limit: int = 2000,
    ) -> List[Dict[str, Any]]:
        normalized_output_dir = str(output_dir or "").strip()
        if not normalized_output_dir:
            return []
        predicates = ["output_dir = ?"]
        params: List[Any] = [normalized_output_dir]
        if str(stage or "").strip():
            predicates.append("stage = ?")
            params.append(str(stage or "").strip())
        if str(scope_type or "").strip():
            predicates.append("scope_type = ?")
            params.append(str(scope_type or "").strip())
        params.append(max(1, int(limit or 1)))
        query = f"""
            SELECT
                output_dir,
                scope_ref,
                task_id,
                storage_key,
                normalized_video_key,
                stage,
                scope_type,
                scope_id,
                scope_variant,
                status,
                input_fingerprint,
                local_path,
                dirty_reason,
                dirty_at_ms,
                updated_at_ms,
                dependency_fingerprints_json,
                depends_on_json,
                payload_json
            FROM scope_nodes
            WHERE {" AND ".join(predicates)}
            ORDER BY stage ASC, scope_type ASC, scope_id ASC, updated_at_ms ASC
            LIMIT ?
        """

        def _read(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
            collected: List[Dict[str, Any]] = []
            for row in connection.execute(query, tuple(params)).fetchall():
                payload = self._decode_json_text_payload(
                    str(row["payload_json"] or ""),
                    expected_type=dict,
                    default={},
                )
                if not isinstance(payload, dict):
                    payload = {}
                payload.setdefault("output_dir", str(row["output_dir"] or ""))
                payload.setdefault("scope_ref", str(row["scope_ref"] or ""))
                payload.setdefault("task_id", str(row["task_id"] or ""))
                payload.setdefault("storage_key", str(row["storage_key"] or ""))
                payload.setdefault("normalized_video_key", str(row["normalized_video_key"] or ""))
                payload.setdefault("stage", str(row["stage"] or ""))
                payload.setdefault("scope_type", str(row["scope_type"] or ""))
                payload.setdefault("scope_id", str(row["scope_id"] or ""))
                payload.setdefault("scope_variant", str(row["scope_variant"] or ""))
                payload.setdefault("status", str(row["status"] or ""))
                payload.setdefault("input_fingerprint", str(row["input_fingerprint"] or ""))
                payload.setdefault("local_path", str(row["local_path"] or ""))
                payload["dependency_fingerprints"] = self._decode_json_text_payload(
                    str(row["dependency_fingerprints_json"] or ""),
                    expected_type=dict,
                    default={},
                ) or {}
                payload["depends_on"] = self._decode_json_text_payload(
                    str(row["depends_on_json"] or ""),
                    expected_type=list,
                    default=[],
                ) or []
                payload["dirty_reason"] = str(row["dirty_reason"] or "")
                if int(row["dirty_at_ms"] or 0) > 0:
                    payload["dirty_at_ms"] = int(row["dirty_at_ms"] or 0)
                payload["updated_at_ms"] = int(row["updated_at_ms"] or 0)
                collected.append(payload)
            return collected

        return self._run_read(_read)

    def _legacy_unused_append_stage_journal_event(
        self,
        *,
        output_dir: str,
        task_id: str,
        stage: str,
        event: str,
        checkpoint: str,
        status: str,
        completed: int = 0,
        pending: int = 0,
        updated_at_ms: int,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_stage = str(stage or "").strip()
        if not normalized_output_dir or not normalized_stage:
            return
        journal_payload = dict(payload or {})
        journal_payload.setdefault("task_id", str(task_id or ""))
        journal_payload.setdefault("stage", normalized_stage)
        journal_payload.setdefault("event", str(event or ""))
        journal_payload.setdefault("checkpoint", str(checkpoint or ""))
        journal_payload.setdefault("status", str(status or ""))
        journal_payload.setdefault("completed", int(completed or 0))
        journal_payload.setdefault("pending", int(pending or 0))
        journal_payload.setdefault("updated_at_ms", int(updated_at_ms or time.time() * 1000))
        journal_payload_json = self._normalize_json_string(journal_payload, "{}")

        def _write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO stage_journal_events (
                    output_dir, task_id, stage, event, status, checkpoint, updated_at_ms, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_output_dir,
                    str(task_id or ""),
                    normalized_stage,
                    str(event or ""),
                    str(status or ""),
                    str(checkpoint or ""),
                    int(updated_at_ms or time.time() * 1000),
                    journal_payload_json,
                ),
            )

        self._run_write(_write)

    def _legacy_unused_upsert_stage_outputs_manifest(
        self,
        *,
        output_dir: str,
        task_id: str,
        stage: str,
        updated_at_ms: int,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        normalized_output_dir = str(output_dir or "").strip()
        normalized_stage = str(stage or "").strip()
        if not normalized_output_dir or not normalized_stage:
            return
        manifest_payload = dict(payload or {})
        manifest_payload.setdefault("task_id", str(task_id or ""))
        manifest_payload.setdefault("stage", normalized_stage)
        manifest_payload.setdefault("updated_at_ms", int(updated_at_ms or time.time() * 1000))
        manifest_payload_json = self._normalize_json_string(manifest_payload, "{}")

        def _write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO stage_outputs_manifests (
                    output_dir, stage, task_id, updated_at_ms, payload_json
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(output_dir, stage) DO UPDATE SET
                    task_id = excluded.task_id,
                    updated_at_ms = excluded.updated_at_ms,
                    payload_json = excluded.payload_json
                """,
                (
                    normalized_output_dir,
                    normalized_stage,
                    str(task_id or ""),
                    int(updated_at_ms or time.time() * 1000),
                    manifest_payload_json,
                ),
            )

        self._run_write(_write)

    def record_llm_attempt_started(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
        attempt: int,
        request_payload: Dict[str, Any],
        manifest_payload: Dict[str, Any],
        attempt_dir: str,
        manifest_path: str,
    ) -> None:
        request_scope_ids_json = self._normalize_json_string(self._extract_request_scope_ids(request_payload), "[]")
        row_values = {
            "normalized_video_key": str(normalized_video_key or ""),
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "attempt": int(attempt or 0),
            "status": str((manifest_payload or {}).get("status", "") or ""),
            "unit_id": str((manifest_payload or {}).get("unit_id", "") or ""),
            "stage_step": str((manifest_payload or {}).get("stage_step", "") or (manifest_payload or {}).get("step_name", "") or ""),
            "response_hash": "",
            "request_scope_ids_json": request_scope_ids_json,
            "error_class": "",
            "error_code": "",
            "error_message": "",
            "updated_at_ms": int((manifest_payload or {}).get("updated_at_ms", 0) or 0),
            "committed_at_ms": 0,
        }

        def _write(connection: sqlite3.Connection) -> None:
            llm_record_id = self._upsert_llm_row(connection, row_values)
            connection.execute("DELETE FROM llm_record_content WHERE llm_record_id = ?", (llm_record_id,))

        self._run_write(_write)

    def record_llm_attempt_committed(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
        attempt: int,
        request_payload: Optional[Dict[str, Any]],
        manifest_payload: Dict[str, Any],
        commit_payload: Dict[str, Any],
        response_text: str,
        attempt_dir: str,
        manifest_path: str,
        commit_path: str,
    ) -> None:
        response_encoded = self._encode_text_blob(str(response_text or ""))
        request_scope_ids_json = self._normalize_json_string(self._extract_request_scope_ids(request_payload), "[]")
        row_values = {
            "normalized_video_key": str(normalized_video_key or ""),
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "attempt": int(attempt or 0),
            "status": str((commit_payload or {}).get("status", "") or (manifest_payload or {}).get("status", "") or ""),
            "unit_id": str((manifest_payload or {}).get("unit_id", "") or ""),
            "stage_step": str((manifest_payload or {}).get("stage_step", "") or (manifest_payload or {}).get("step_name", "") or ""),
            "response_hash": str((commit_payload or {}).get("response_hash", "") or response_encoded.get("payload_hash", "") or ""),
            "request_scope_ids_json": request_scope_ids_json,
            "error_class": "",
            "error_code": "",
            "error_message": "",
            "updated_at_ms": int((manifest_payload or {}).get("updated_at_ms", 0) or 0),
            "committed_at_ms": int((commit_payload or {}).get("committed_at_ms", 0) or 0),
        }

        def _write(connection: sqlite3.Connection) -> None:
            llm_record_id = self._upsert_llm_row(connection, row_values)
            connection.execute(
                """
                INSERT INTO llm_record_content (
                    llm_record_id,
                    response_codec,
                    response_payload
                ) VALUES (?, ?, ?)
                ON CONFLICT(llm_record_id)
                DO UPDATE SET
                    response_codec = excluded.response_codec,
                    response_payload = excluded.response_payload
                """,
                (
                    llm_record_id,
                    str(response_encoded.get("codec", "") or ""),
                    response_encoded.get("blob"),
                ),
            )

        self._run_write(_write)

    def record_llm_attempt_failed(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
        attempt: int,
        request_payload: Optional[Dict[str, Any]],
        manifest_payload: Dict[str, Any],
        error_payload: Dict[str, Any],
        attempt_dir: str,
        manifest_path: str,
    ) -> None:
        request_scope_ids_json = self._normalize_json_string(self._extract_request_scope_ids(request_payload), "[]")
        row_values = {
            "normalized_video_key": str(normalized_video_key or ""),
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "llm_call_id": str(llm_call_id or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "attempt": int(attempt or 0),
            "status": str((manifest_payload or {}).get("status", "") or ""),
            "unit_id": str((manifest_payload or {}).get("unit_id", "") or ""),
            "stage_step": str((manifest_payload or {}).get("stage_step", "") or (manifest_payload or {}).get("step_name", "") or ""),
            "response_hash": "",
            "request_scope_ids_json": request_scope_ids_json,
            "error_class": str((error_payload or {}).get("error_class", "") or ""),
            "error_code": str((error_payload or {}).get("error_code", "") or ""),
            "error_message": str((error_payload or {}).get("error_message", "") or ""),
            "updated_at_ms": int((manifest_payload or {}).get("updated_at_ms", 0) or 0),
            "committed_at_ms": 0,
        }

        def _write(connection: sqlite3.Connection) -> None:
            llm_record_id = self._upsert_llm_row(connection, row_values)
            connection.execute("DELETE FROM llm_record_content WHERE llm_record_id = ?", (llm_record_id,))

        self._run_write(_write)

    def record_chunk_committed(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
        attempt: int,
        result_payload: Dict[str, Any],
        chunk_state_payload: Dict[str, Any],
        commit_payload: Dict[str, Any],
        chunk_dir: str,
        chunk_state_path: str,
        commit_path: str,
    ) -> None:
        result_encoded = self._encode_json_blob(result_payload)
        row_values = {
            "normalized_video_key": str(normalized_video_key or ""),
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "attempt": int(attempt or 0),
            "status": str((commit_payload or {}).get("status", "") or (chunk_state_payload or {}).get("status", "") or ""),
            "result_hash": str((commit_payload or {}).get("result_hash", "") or (chunk_state_payload or {}).get("result_hash", "") or ""),
            "error_class": "",
            "error_code": "",
            "error_message": "",
            "updated_at_ms": int((chunk_state_payload or {}).get("updated_at_ms", 0) or 0),
            "committed_at_ms": int((commit_payload or {}).get("committed_at_ms", 0) or 0),
        }

        def _write(connection: sqlite3.Connection) -> None:
            chunk_record_id = self._upsert_chunk_row(connection, row_values)
            connection.execute(
                """
                INSERT INTO chunk_record_content (
                    chunk_record_id,
                    result_codec,
                    result_payload
                ) VALUES (?, ?, ?)
                ON CONFLICT(chunk_record_id)
                DO UPDATE SET
                    result_codec = excluded.result_codec,
                    result_payload = excluded.result_payload
                """,
                (
                    chunk_record_id,
                    str(result_encoded.get("codec", "") or ""),
                    result_encoded.get("blob"),
                ),
            )

        self._run_write(_write)

    def record_chunk_state(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
        attempt: int,
        chunk_state_payload: Dict[str, Any],
        chunk_dir: str,
        chunk_state_path: str,
    ) -> None:
        row_values = {
            "normalized_video_key": str(normalized_video_key or ""),
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "attempt": int(attempt or 0),
            "status": str((chunk_state_payload or {}).get("status", "") or ""),
            "result_hash": str((chunk_state_payload or {}).get("result_hash", "") or ""),
            "error_class": str((chunk_state_payload or {}).get("error_class", "") or ""),
            "error_code": str((chunk_state_payload or {}).get("error_code", "") or ""),
            "error_message": str((chunk_state_payload or {}).get("error_message", "") or ""),
            "updated_at_ms": int((chunk_state_payload or {}).get("updated_at_ms", 0) or 0),
            "committed_at_ms": 0,
        }

        def _write(connection: sqlite3.Connection) -> None:
            chunk_record_id = self._upsert_chunk_row(connection, row_values)
            connection.execute("DELETE FROM chunk_record_content WHERE chunk_record_id = ?", (chunk_record_id,))

        self._run_write(_write)

    def record_chunk_failed(
        self,
        *,
        output_dir: str,
        task_id: str,
        storage_key: str,
        normalized_video_key: str,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
        attempt: int,
        chunk_state_payload: Dict[str, Any],
        error_payload: Dict[str, Any],
        chunk_dir: str,
        chunk_state_path: str,
    ) -> None:
        row_values = {
            "normalized_video_key": str(normalized_video_key or ""),
            "stage": str(stage or ""),
            "chunk_id": str(chunk_id or ""),
            "input_fingerprint": str(input_fingerprint or ""),
            "attempt": int(attempt or 0),
            "status": str((chunk_state_payload or {}).get("status", "") or ""),
            "result_hash": "",
            "error_class": str((error_payload or {}).get("error_class", "") or ""),
            "error_code": str((error_payload or {}).get("error_code", "") or ""),
            "error_message": str((error_payload or {}).get("error_message", "") or ""),
            "updated_at_ms": int((chunk_state_payload or {}).get("updated_at_ms", 0) or 0),
            "committed_at_ms": 0,
        }

        def _write(connection: sqlite3.Connection) -> None:
            chunk_record_id = self._upsert_chunk_row(connection, row_values)
            connection.execute("DELETE FROM chunk_record_content WHERE chunk_record_id = ?", (chunk_record_id,))

        self._run_write(_write)

    def load_latest_committed_llm(
        self,
        *,
        output_dir: str,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
        input_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        query = """
            SELECT
                m.*,
                c.response_codec,
                c.response_payload
            FROM llm_records m
            LEFT JOIN llm_record_content c ON c.llm_record_id = m.id
            WHERE m.stage = ?
              AND m.chunk_id = ?
              AND m.llm_call_id = ?
              AND m.input_fingerprint = ?
              AND m.status IN ('SUCCESS', 'LOCAL_COMMITTED', 'COMPLETED')
            ORDER BY m.committed_at_ms DESC, m.attempt DESC
            LIMIT 4
        """

        def _read(connection: sqlite3.Connection) -> Optional[Dict[str, Any]]:
            rows = connection.execute(
                query,
                (
                    str(stage or ""),
                    str(chunk_id or ""),
                    str(llm_call_id or ""),
                    str(input_fingerprint or ""),
                ),
            ).fetchall()
            for row in rows:
                restored = self._restore_llm_from_row(row)
                if restored is not None:
                    return restored
            return None

        return self._run_read(_read)

    def load_latest_llm_attempt(
        self,
        *,
        output_dir: str,
        stage: str,
        chunk_id: str,
        llm_call_id: str,
    ) -> Optional[Dict[str, Any]]:
        query = """
            SELECT
                attempt,
                status,
                updated_at_ms
            FROM llm_records
            WHERE stage = ?
              AND chunk_id = ?
              AND llm_call_id = ?
            ORDER BY attempt DESC, updated_at_ms DESC
            LIMIT 1
        """

        def _read(connection: sqlite3.Connection) -> Optional[Dict[str, Any]]:
            row = connection.execute(
                query,
                (
                    str(stage or ""),
                    str(chunk_id or ""),
                    str(llm_call_id or ""),
                ),
            ).fetchone()
            if row is None:
                return None
            return {
                "attempt": int(row["attempt"] or 0),
                "status": self._canonicalize_status(row["status"]),
                "updated_at_ms": int(row["updated_at_ms"] or 0),
            }

        return self._run_read(_read)

    def load_latest_committed_chunk(
        self,
        *,
        output_dir: str,
        stage: str,
        chunk_id: str,
        input_fingerprint: str,
    ) -> Optional[Dict[str, Any]]:
        query = """
            SELECT
                m.*,
                c.result_codec,
                c.result_payload
            FROM chunk_records m
            LEFT JOIN chunk_record_content c ON c.chunk_record_id = m.id
            WHERE m.stage = ?
              AND m.chunk_id = ?
              AND m.input_fingerprint = ?
              AND m.status IN ('SUCCESS', 'LOCAL_COMMITTED', 'COMPLETED')
            ORDER BY m.committed_at_ms DESC, m.attempt DESC
            LIMIT 4
        """

        def _read(connection: sqlite3.Connection) -> Optional[Dict[str, Any]]:
            rows = connection.execute(
                query,
                (
                    str(stage or ""),
                    str(chunk_id or ""),
                    str(input_fingerprint or ""),
                ),
            ).fetchall()
            for row in rows:
                restored = self._restore_chunk_from_row(row)
                if restored is not None:
                    return restored
            return None

        return self._run_read(_read)

    def load_latest_committed_chunk_by_chunk_id(
        self,
        *,
        output_dir: str,
        stage: str,
        chunk_id: str,
    ) -> Optional[Dict[str, Any]]:
        query = """
            SELECT
                m.*,
                c.result_codec,
                c.result_payload
            FROM chunk_records m
            LEFT JOIN chunk_record_content c ON c.chunk_record_id = m.id
            WHERE m.stage = ?
              AND m.chunk_id = ?
              AND m.status IN ('SUCCESS', 'LOCAL_COMMITTED', 'COMPLETED')
            ORDER BY m.committed_at_ms DESC, m.attempt DESC
            LIMIT 4
        """

        def _read(connection: sqlite3.Connection) -> Optional[Dict[str, Any]]:
            rows = connection.execute(
                query,
                (
                    str(stage or ""),
                    str(chunk_id or ""),
                ),
            ).fetchall()
            for row in rows:
                restored = self._restore_chunk_from_row(row)
                if restored is not None:
                    return restored
            return None

        return self._run_read(_read)

    def list_llm_records(
        self,
        *,
        output_dir: Optional[str] = None,
        task_id: Optional[str] = None,
        stage: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        predicates: List[str] = ["1 = 1"]
        params: List[Any] = []
        if str(stage or "").strip():
            predicates.append("stage = ?")
            params.append(str(stage or "").strip())
        normalized_status = self._canonicalize_status(status)
        if normalized_status == "SUCCESS":
            predicates.append("status IN ('SUCCESS','LOCAL_COMMITTED','COMPLETED')")
        elif normalized_status == "RUNNING":
            predicates.append("status IN ('RUNNING','LOCAL_WRITING','EXECUTING')")
        elif normalized_status:
            predicates.append("status = ?")
            params.append(normalized_status)
        params.append(max(1, int(limit or 1)))
        query = f"""
            SELECT
                stage,
                chunk_id,
                llm_call_id,
                input_fingerprint,
                attempt,
                status,
                unit_id,
                stage_step,
                request_scope_ids_json,
                response_hash,
                updated_at_ms,
                committed_at_ms
            FROM llm_records
            WHERE {" AND ".join(predicates)}
            ORDER BY updated_at_ms DESC, attempt DESC
            LIMIT ?
        """
        def _read(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
            payloads: List[Dict[str, Any]] = []
            for row in connection.execute(query, tuple(params)).fetchall():
                item = dict(row)
                item["status"] = self._canonicalize_status(item.get("status", ""))
                payloads.append(item)
            return payloads

        return self._run_read(_read)

    def list_chunk_records(
        self,
        *,
        output_dir: Optional[str] = None,
        task_id: Optional[str] = None,
        stage: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        predicates: List[str] = ["1 = 1"]
        params: List[Any] = []
        if str(stage or "").strip():
            predicates.append("stage = ?")
            params.append(str(stage or "").strip())
        normalized_status = self._canonicalize_status(status)
        if normalized_status == "SUCCESS":
            predicates.append("status IN ('SUCCESS','LOCAL_COMMITTED','COMPLETED')")
        elif normalized_status == "RUNNING":
            predicates.append("status IN ('RUNNING','LOCAL_WRITING','EXECUTING')")
        elif normalized_status:
            predicates.append("status = ?")
            params.append(normalized_status)
        params.append(max(1, int(limit or 1)))
        query = f"""
            SELECT
                stage,
                chunk_id,
                input_fingerprint,
                attempt,
                status,
                result_hash,
                updated_at_ms,
                committed_at_ms
            FROM chunk_records
            WHERE {" AND ".join(predicates)}
            ORDER BY updated_at_ms DESC, attempt DESC
            LIMIT ?
        """
        def _read(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
            payloads: List[Dict[str, Any]] = []
            for row in connection.execute(query, tuple(params)).fetchall():
                item = dict(row)
                item["status"] = self._canonicalize_status(item.get("status", ""))
                payloads.append(item)
            return payloads

        return self._run_read(_read)

    def list_scope_hints(
        self,
        *,
        output_dir: Optional[str] = None,
        task_id: Optional[str] = None,
        stage: Optional[str] = None,
        scope_type: Optional[str] = None,
        latest_status: Optional[str] = None,
        retry_mode: Optional[str] = None,
        pending_only: bool = False,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        predicates: List[str] = ["1 = 1"]
        params: List[Any] = []
        if str(stage or "").strip():
            predicates.append("p.stage = ?")
            params.append(str(stage or "").strip())
        if str(scope_type or "").strip():
            predicates.append("p.scope_type = ?")
            params.append(str(scope_type or "").strip())
        if str(latest_status or "").strip():
            predicates.append("l.latest_status = ?")
            params.append(str(latest_status or "").strip().upper())
        if str(retry_mode or "").strip():
            predicates.append("COALESCE(l.retry_mode, p.retry_mode) = ?")
            params.append(str(retry_mode or "").strip().lower())
        if pending_only:
            predicates.append(
                "(plan_status IN ('PLANNED','IN_FLIGHT','RECOMPUTE_PENDING','AUTO_RETRY_PENDING','MANUAL_RETRY_PENDING','FALLBACK_RETRY_PENDING','MANUAL_REPAIR_REQUIRED') "
                "OR latest_status IN ('ERROR','MANUAL_NEEDED','FAILED','DIRTY','RUNNING'))"
            )
        params.append(max(1, int(limit or 1)))
        query = f"""
            SELECT
                p.normalized_video_key,
                p.stage,
                p.scope_type,
                p.scope_id,
                p.scope_ref,
                p.scope_variant,
                p.chunk_id,
                p.llm_call_id,
                p.unit_id,
                p.stage_step,
                p.input_fingerprint,
                p.plan_status,
                p.dirty_reason,
                p.retry_mode,
                p.retry_entry_point,
                p.required_action,
                p.local_path,
                p.updated_at_ms,
                l.latest_status,
                l.durable_status,
                l.latest_attempt,
                l.can_restore,
                l.error_class,
                l.error_code,
                l.error_message,
                l.source_kind
            FROM scope_hint_plan p
            LEFT JOIN scope_hint_latest l
              ON l.scope_ref = p.scope_ref
            WHERE {" AND ".join(predicates)}
            ORDER BY p.updated_at_ms DESC, p.scope_type ASC, p.scope_id ASC
            LIMIT ?
        """
        return self._run_read(lambda connection: [dict(row) for row in connection.execute(query, tuple(params)).fetchall()])

    def _read_batch_with_connection(self, callback):
        connection = self._connect()
        try:
            return callback(connection)
        finally:
            connection.close()

    def batch_load_committed_llm(
        self,
        *,
        output_dir: str,
        requests: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        normalized_requests = [dict(item) for item in list(requests or []) if isinstance(item, dict)]
        if not normalized_requests:
            return []

        def _read(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
            results: List[Dict[str, Any]] = [{"request": request, "restored": None} for request in normalized_requests]
            batch_size = max(1, min(120, 900 // 6))
            for request_batch in self._batched(list(enumerate(normalized_requests)), batch_size):
                value_sql: List[str] = []
                params: List[Any] = []
                for request_index, request in request_batch:
                    value_sql.append("(?, ?, ?, ?, ?)")
                    params.extend(
                        [
                            int(request_index),
                            str(request.get("stage", "") or ""),
                            str(request.get("chunk_id", "") or ""),
                            str(request.get("llm_call_id", "") or ""),
                            str(request.get("input_fingerprint", "") or ""),
                        ]
                    )
                query = f"""
                    WITH requested(request_index, stage, chunk_id, llm_call_id, input_fingerprint) AS (
                        VALUES {", ".join(value_sql)}
                    )
                    SELECT
                        requested.request_index,
                        m.*,
                        c.response_codec,
                        c.response_payload
                    FROM requested
                    LEFT JOIN llm_records m
                      ON m.stage = requested.stage
                     AND m.chunk_id = requested.chunk_id
                     AND m.llm_call_id = requested.llm_call_id
                     AND m.input_fingerprint = requested.input_fingerprint
                     AND m.status IN ('SUCCESS', 'LOCAL_COMMITTED', 'COMPLETED')
                    LEFT JOIN llm_record_content c
                      ON c.llm_record_id = m.id
                    ORDER BY requested.request_index ASC, m.committed_at_ms DESC, m.attempt DESC
                """
                grouped_rows: Dict[int, List[sqlite3.Row]] = {}
                for row in connection.execute(query, tuple(params)).fetchall():
                    grouped_rows.setdefault(int(row["request_index"] or 0), []).append(row)
                for request_index, _ in request_batch:
                    for row in grouped_rows.get(int(request_index), []):
                        restored = self._restore_llm_from_row(row)
                        if restored is None:
                            continue
                        results[int(request_index)]["restored"] = restored
                        break
            return results

        return self._read_batch_with_connection(_read)

    def batch_load_committed_chunk(
        self,
        *,
        output_dir: str,
        requests: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        normalized_requests = [dict(item) for item in list(requests or []) if isinstance(item, dict)]
        if not normalized_requests:
            return []

        def _read(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
            results: List[Dict[str, Any]] = [{"request": request, "restored": None} for request in normalized_requests]
            batch_size = max(1, min(150, 900 // 5))
            for request_batch in self._batched(list(enumerate(normalized_requests)), batch_size):
                value_sql: List[str] = []
                params: List[Any] = []
                for request_index, request in request_batch:
                    value_sql.append("(?, ?, ?, ?)")
                    params.extend(
                        [
                            int(request_index),
                            str(request.get("stage", "") or ""),
                            str(request.get("chunk_id", "") or ""),
                            str(request.get("input_fingerprint", "") or ""),
                        ]
                    )
                query = f"""
                    WITH requested(request_index, stage, chunk_id, input_fingerprint) AS (
                        VALUES {", ".join(value_sql)}
                    )
                    SELECT
                        requested.request_index,
                        m.*,
                        c.result_codec,
                        c.result_payload
                    FROM requested
                    LEFT JOIN chunk_records m
                      ON m.stage = requested.stage
                     AND m.chunk_id = requested.chunk_id
                     AND m.input_fingerprint = requested.input_fingerprint
                     AND m.status IN ('SUCCESS', 'LOCAL_COMMITTED', 'COMPLETED')
                    LEFT JOIN chunk_record_content c
                      ON c.chunk_record_id = m.id
                    ORDER BY requested.request_index ASC, m.committed_at_ms DESC, m.attempt DESC
                """
                grouped_rows: Dict[int, List[sqlite3.Row]] = {}
                for row in connection.execute(query, tuple(params)).fetchall():
                    grouped_rows.setdefault(int(row["request_index"] or 0), []).append(row)
                for request_index, _ in request_batch:
                    for row in grouped_rows.get(int(request_index), []):
                        restored = self._restore_chunk_from_row(row)
                        if restored is None:
                            continue
                        results[int(request_index)]["restored"] = restored
                        break
            return results

        return self._read_batch_with_connection(_read)
