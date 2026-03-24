"""
VL 令牌桶限流器：双维度控制 RPM 与 TPM。
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from typing import Tuple


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


@dataclass
class TokenBucket:
    capacity: float
    refill_per_second: float
    tokens: float
    last_refill_time: float

    @classmethod
    def create(cls, capacity: int, refill_per_second: float) -> "TokenBucket":
        cap = max(1.0, float(capacity))
        refill = max(0.0, float(refill_per_second))
        return cls(
            capacity=cap,
            refill_per_second=refill,
            tokens=cap,
            last_refill_time=time.monotonic(),
        )

    def refill(self, now: float) -> None:
        if self.refill_per_second <= 0:
            return
        elapsed = max(0.0, now - self.last_refill_time)
        if elapsed <= 0:
            return
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_second)
        self.last_refill_time = now

    def missing_tokens(self, required: float) -> float:
        return max(0.0, float(required) - self.tokens)

    def consume(self, required: float) -> None:
        self.tokens = max(0.0, self.tokens - float(required))

    def wait_seconds(self, required: float) -> float:
        if self.refill_per_second <= 0:
            return 0.0
        missing = self.missing_tokens(required)
        if missing <= 0:
            return 0.0
        return missing / self.refill_per_second


class VLRateLimiter:
    """
    双桶限流：
    - RPM 桶：每次请求消耗 1
    - TPM 桶：每次请求消耗 estimated_tokens
    """

    def __init__(self, rpm_limit: int | None = None, tpm_limit: int | None = None):
        resolved_rpm = int(rpm_limit if rpm_limit is not None else _env_int("VL_RATE_LIMIT_RPM", 1200))
        resolved_tpm = int(tpm_limit if tpm_limit is not None else _env_int("VL_RATE_LIMIT_TPM", 1_000_000))
        self._enabled = resolved_rpm > 0 and resolved_tpm > 0
        self._rpm = TokenBucket.create(max(1, resolved_rpm), max(1, resolved_rpm) / 60.0)
        self._tpm = TokenBucket.create(max(1, resolved_tpm), max(1, resolved_tpm) / 60.0)
        self._lock = asyncio.Lock()

    async def acquire(self, estimated_tokens: int) -> Tuple[float, float]:
        """
        返回值：(rpm_wait_sec, tpm_wait_sec)，便于调用方记录日志。
        """
        if not self._enabled:
            return 0.0, 0.0

        need_tpm = max(1.0, float(estimated_tokens))
        total_rpm_wait = 0.0
        total_tpm_wait = 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                self._rpm.refill(now)
                self._tpm.refill(now)
                rpm_wait = self._rpm.wait_seconds(1.0)
                tpm_wait = self._tpm.wait_seconds(need_tpm)
                wait_sec = max(rpm_wait, tpm_wait)
                if wait_sec <= 1e-6:
                    self._rpm.consume(1.0)
                    self._tpm.consume(need_tpm)
                    return total_rpm_wait, total_tpm_wait
                total_rpm_wait += max(0.0, rpm_wait)
                total_tpm_wait += max(0.0, tpm_wait)
            await asyncio.sleep(wait_sec)
