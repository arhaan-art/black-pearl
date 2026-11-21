#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Retry with backoff, per-record penalize, processor yield, dead-letter queue.

Three distinct mechanisms (not one loop): retry-with-budget, per-record
penalize (delayed ZSET-style), and processor yield after consecutive
errors. Exhausted or non-retryable failures land in the DLQ with replay.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, TypeVar

from .envelope import IngestEnvelope

logger = logging.getLogger("ingest.retry")
T = TypeVar("T")


@dataclass
class RetryPolicy:
    max_retries: int = 3
    base_delay_seconds: float = 1.0
    backoff_factor: float = 2.0
    penalty_seconds: float = 30.0
    yield_after_errors: int = 5
    yield_seconds: float = 10.0

    def delay_for(self, attempt: int) -> float:
        return self.base_delay_seconds * (self.backoff_factor ** max(attempt - 1, 0))


@dataclass
class DLQEntry:
    envelope: Dict
    failure_stage: str
    error_class: str  # retryable | non-retryable
    error: str
    attempts: int
    first_failure_epoch: float = field(default_factory=time.time)
    last_failure_epoch: float = field(default_factory=time.time)

    def to_dict(self) -> Dict:
        return {
            "envelope": self.envelope, "failure_stage": self.failure_stage,
            "error_class": self.error_class, "error": self.error,
            "attempts": self.attempts,
            "first_failure_epoch": self.first_failure_epoch,
            "last_failure_epoch": self.last_failure_epoch,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "DLQEntry":
        return cls(envelope=data["envelope"], failure_stage=data.get("failure_stage", ""),
                   error_class=data.get("error_class", "retryable"),
                   error=data.get("error", ""), attempts=int(data.get("attempts", 0)),
                   first_failure_epoch=float(data.get("first_failure_epoch", time.time())),
                   last_failure_epoch=float(data.get("last_failure_epoch", time.time())))


class DeadLetterQueue:
    """Redis list ``ingest:dlq`` when a client is given, else in-memory."""

    def __init__(self, redis_client=None) -> None:
        self._redis = redis_client
        self._mem: Dict[str, DLQEntry] = {}

    def push(self, env: IngestEnvelope, stage: str, error: str,
             error_class: str, attempts: int) -> DLQEntry:
        entry = DLQEntry(envelope=env.to_dict(), failure_stage=stage,
                         error_class=error_class, error=str(error), attempts=attempts)
        if self._redis is not None:
            try:
                self._redis.hset(f"ingest:dlq:{env.envelope_uuid}", mapping={
                    "envelope": json.dumps(entry.envelope), "failure_stage": stage,
                    "error_class": error_class, "error": str(error),
                    "attempts": str(attempts)})
                self._redis.rpush("ingest:dlq", env.envelope_uuid)
                return entry
            except Exception as exc:
                logger.warning("redis DLQ failed, memory fallback: %s", exc)
        self._mem[env.envelope_uuid] = entry
        logger.warning("DLQ push uuid=%s stage=%s class=%s attempts=%s",
                       env.envelope_uuid, stage, error_class, attempts)
        return entry

    def list_entries(self) -> List[DLQEntry]:
        if self._redis is not None:
            try:
                ids = self._redis.lrange("ingest:dlq", 0, -1) or []
                out = []
                for raw_id in ids:
                    eid = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
                    row = self._redis.hgetall(f"ingest:dlq:{eid}") or {}
                    row = {k.decode() if isinstance(k, bytes) else k:
                           v.decode() if isinstance(v, bytes) else v for k, v in row.items()}
                    if row:
                        out.append(DLQEntry(
                            envelope=json.loads(row.get("envelope", "{}")),
                            failure_stage=row.get("failure_stage", ""),
                            error_class=row.get("error_class", "retryable"),
                            error=row.get("error", ""),
                            attempts=int(row.get("attempts", 0))))
                return out
            except Exception as exc:
                logger.warning("redis DLQ list failed: %s", exc)
        return list(self._mem.values())

    def pop(self, envelope_uuid: str) -> Optional[DLQEntry]:
        if self._redis is not None:
            try:
                self._redis.lrem("ingest:dlq", 0, envelope_uuid)
                self._redis.delete(f"ingest:dlq:{envelope_uuid}")
            except Exception:
                pass
        return self._mem.pop(envelope_uuid, None)

    def depth(self) -> int:
        if self._redis is not None:
            try:
                return int(self._redis.llen("ingest:dlq"))
            except Exception:
                pass
        return len(self._mem)


class RetryExecutor:
    def __init__(self, policy: Optional[RetryPolicy] = None,
                 dlq: Optional[DeadLetterQueue] = None,
                 sleeper: Callable[[float], None] = time.sleep) -> None:
        self.policy = policy or RetryPolicy()
        self.dlq = dlq or DeadLetterQueue()
        self._sleeper = sleeper
        self._consecutive_errors = 0
        self._penalized: Dict[str, float] = {}  # envelope_uuid -> retry_at epoch

    def penalize(self, env: IngestEnvelope) -> float:
        retry_at = time.time() + self.policy.penalty_seconds
        self._penalized[env.envelope_uuid] = retry_at
        return retry_at

    def is_penalized(self, env: IngestEnvelope) -> bool:
        return self._penalized.get(env.envelope_uuid, 0) > time.time()

    def clear_penalty(self, env: IngestEnvelope) -> None:
        self._penalized.pop(env.envelope_uuid, None)

    def penalized(self) -> Dict[str, float]:
        return dict(self._penalized)

    def run(self, fn: Callable[[], T], env: IngestEnvelope, stage: str,
            non_retryable: bool = False) -> Optional[T]:
        """Run ``fn`` with budget; on exhaustion push to DLQ and return None."""
        attempts = 0
        last_error: Optional[Exception] = None
        while attempts <= self.policy.max_retries:
            try:
                result = fn()
                self._consecutive_errors = 0
                self.clear_penalty(env)
                return result
            except Exception as exc:
                last_error = exc
                attempts += 1
                self._consecutive_errors += 1
                logger.warning("stage=%s uuid=%s attempt=%s/%s error=%s",
                               stage, env.envelope_uuid, attempts,
                               self.policy.max_retries, exc)
                if non_retryable or attempts > self.policy.max_retries:
                    break
                if self._consecutive_errors >= self.policy.yield_after_errors:
                    logger.warning("yielding %ss after %s consecutive errors",
                                   self.policy.yield_seconds, self._consecutive_errors)
                    self._sleeper(self.policy.yield_seconds)
                    self._consecutive_errors = 0
                else:
                    self._sleeper(self.policy.delay_for(attempts))
        error_class = "non-retryable" if non_retryable else "retryable"
        self.dlq.push(env, stage=stage, error=str(last_error),
                      error_class=error_class, attempts=attempts)
        env.add_provenance("DROP", processor_id=stage,
                           detail=f"dlq:{error_class}:{last_error}")
        return None
