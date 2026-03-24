from __future__ import annotations

import copy
import os
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Tuple


@dataclass
class RuntimeStageRepositoryEntry:
    stage: str
    output_dir: str
    repository_id: str
    schema_version: str
    payload: Dict[str, Any] = field(default_factory=dict)

    def clone_payload(self) -> Dict[str, Any]:
        return copy.deepcopy(self.payload)


class RuntimeStageRepositoryRegistry:
    """按阶段与任务目录缓存运行态仓库。

    约束：
    1) 仓库只负责热路径读写与跨阶段同进程透传，不承担延迟刷盘职责。
    2) 一切 planning / 状态迁移 / 单元完成 / 单元失败 仍必须立即写入 SQLite。
    3) 只有仓库初始化或冷恢复时，才允许从 SQLite 批量装载已成功单元重建工作集。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: Dict[Tuple[str, str], RuntimeStageRepositoryEntry] = {}
        self._entries_by_repository_id: Dict[str, RuntimeStageRepositoryEntry] = {}

    @staticmethod
    def _normalize_stage(stage: str) -> str:
        return str(stage or "").strip().lower()

    @staticmethod
    def _normalize_output_dir(output_dir: str) -> str:
        normalized = str(output_dir or "").strip()
        if not normalized:
            return ""
        return os.path.abspath(normalized)

    def put(
        self,
        *,
        stage: str,
        output_dir: str,
        payload: Dict[str, Any],
        repository_id: str = "",
        schema_version: str = "runtime_stage_repository_v1",
    ) -> RuntimeStageRepositoryEntry:
        normalized_stage = self._normalize_stage(stage)
        normalized_output_dir = self._normalize_output_dir(output_dir)
        if not normalized_stage or not normalized_output_dir:
            raise ValueError("stage and output_dir must not be empty")
        normalized_repository_id = str(repository_id or "").strip() or f"{normalized_stage}_{uuid.uuid4().hex}"
        entry = RuntimeStageRepositoryEntry(
            stage=normalized_stage,
            output_dir=normalized_output_dir,
            repository_id=normalized_repository_id,
            schema_version=str(schema_version or "runtime_stage_repository_v1").strip() or "runtime_stage_repository_v1",
            payload=copy.deepcopy(dict(payload or {})),
        )
        entry_key = (normalized_stage, normalized_output_dir)
        with self._lock:
            previous = self._entries.get(entry_key)
            if previous is not None:
                self._entries_by_repository_id.pop(previous.repository_id, None)
            self._entries[entry_key] = entry
            self._entries_by_repository_id[normalized_repository_id] = entry
        return entry

    def get(self, *, stage: str, output_dir: str) -> Optional[RuntimeStageRepositoryEntry]:
        normalized_stage = self._normalize_stage(stage)
        normalized_output_dir = self._normalize_output_dir(output_dir)
        if not normalized_stage or not normalized_output_dir:
            return None
        with self._lock:
            return self._entries.get((normalized_stage, normalized_output_dir))

    def get_by_repository_id(self, repository_id: str) -> Optional[RuntimeStageRepositoryEntry]:
        normalized_repository_id = str(repository_id or "").strip()
        if not normalized_repository_id:
            return None
        with self._lock:
            return self._entries_by_repository_id.get(normalized_repository_id)

    def clear(self, *, stage: str, output_dir: str) -> Optional[RuntimeStageRepositoryEntry]:
        normalized_stage = self._normalize_stage(stage)
        normalized_output_dir = self._normalize_output_dir(output_dir)
        if not normalized_stage or not normalized_output_dir:
            return None
        entry_key = (normalized_stage, normalized_output_dir)
        with self._lock:
            removed = self._entries.pop(entry_key, None)
            if removed is not None:
                self._entries_by_repository_id.pop(removed.repository_id, None)
            return removed

    def mutate(
        self,
        *,
        stage: str,
        output_dir: str,
        mutator: Callable[[Dict[str, Any]], Optional[Dict[str, Any]]],
        repository_id: str = "",
        schema_version: str = "runtime_stage_repository_v1",
    ) -> RuntimeStageRepositoryEntry:
        normalized_stage = self._normalize_stage(stage)
        normalized_output_dir = self._normalize_output_dir(output_dir)
        if not normalized_stage or not normalized_output_dir:
            raise ValueError("stage and output_dir must not be empty")
        if not callable(mutator):
            raise ValueError("mutator must be callable")

        normalized_repository_id = str(repository_id or "").strip()
        normalized_schema_version = str(schema_version or "runtime_stage_repository_v1").strip()
        if not normalized_schema_version:
            normalized_schema_version = "runtime_stage_repository_v1"

        entry_key = (normalized_stage, normalized_output_dir)
        with self._lock:
            entry = self._entries.get(entry_key)
            if entry is None:
                if not normalized_repository_id:
                    normalized_repository_id = f"{normalized_stage}_{uuid.uuid4().hex}"
                entry = RuntimeStageRepositoryEntry(
                    stage=normalized_stage,
                    output_dir=normalized_output_dir,
                    repository_id=normalized_repository_id,
                    schema_version=normalized_schema_version,
                    payload={},
                )
                self._entries[entry_key] = entry
                self._entries_by_repository_id[entry.repository_id] = entry
            else:
                if normalized_repository_id and normalized_repository_id != entry.repository_id:
                    self._entries_by_repository_id.pop(entry.repository_id, None)
                    entry.repository_id = normalized_repository_id
                    self._entries_by_repository_id[entry.repository_id] = entry
                if normalized_schema_version:
                    entry.schema_version = normalized_schema_version
                if not isinstance(entry.payload, dict):
                    entry.payload = {}

            updated_payload = mutator(entry.payload)
            if isinstance(updated_payload, dict) and updated_payload is not entry.payload:
                entry.payload = updated_payload

            return RuntimeStageRepositoryEntry(
                stage=entry.stage,
                output_dir=entry.output_dir,
                repository_id=entry.repository_id,
                schema_version=entry.schema_version,
                payload=copy.deepcopy(entry.payload),
            )
