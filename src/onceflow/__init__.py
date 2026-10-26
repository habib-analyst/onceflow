"""onceflow — dead-letter forensics and safe replay for event pipelines.

Intake -> idempotent execution -> DLQ -> forensic classification ->
selective replay with the original idempotency key preserved.
"""

from .classifier import classify
from .errors import (
    CrashAfterEffectError,
    DependencyDownError,
    OnceflowError,
    OrderingViolationError,
    PayloadValidationError,
    ScopeDeniedError,
    TransientProcessingError,
)
from .executor import ReconcilingExecutor, Runtime, StandardExecutor
from .intake import ingest, serve
from .ledger import AuditLog, CompletionStore, EffectLedger
from .models import (
    Classification,
    Event,
    FailureClass,
    FailureRecord,
    ReplayAction,
)
from .queue import FileQueue, InMemoryQueue
from .replay import classified_replay, naive_replay, no_replay, process_stream

__version__ = "0.1.0"

__all__ = [
    "AuditLog",
    "Classification",
    "CompletionStore",
    "CrashAfterEffectError",
    "DependencyDownError",
    "EffectLedger",
    "Event",
    "FailureClass",
    "FailureRecord",
    "FileQueue",
    "InMemoryQueue",
    "OnceflowError",
    "OrderingViolationError",
    "PayloadValidationError",
    "ReconcilingExecutor",
    "ReplayAction",
    "Runtime",
    "ScopeDeniedError",
    "StandardExecutor",
    "TransientProcessingError",
    "classified_replay",
    "classify",
    "ingest",
    "naive_replay",
    "no_replay",
    "process_stream",
    "serve",
]
