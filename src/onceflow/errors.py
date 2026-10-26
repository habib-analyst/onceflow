"""Typed failure modes for the onceflow pipeline.

Every failure an executor can raise is one of these types. The DLQ forensic
classifier keys off the exception type plus the failure record's context,
so handlers must raise the precise type rather than a bare Exception.
"""


class OnceflowError(Exception):
    """Base class for all pipeline failures."""


class PayloadValidationError(OnceflowError):
    """The payload itself is malformed and will never validate (poison)."""


class ScopeDeniedError(OnceflowError):
    """The event references a scope the consumer is not authorised for."""


class OrderingViolationError(OnceflowError):
    """A predecessor event for the same aggregate has not been applied yet."""


class DependencyDownError(OnceflowError):
    """A required downstream dependency is currently unavailable."""


class TransientProcessingError(OnceflowError):
    """A transient failure (timeout, throttle, temporary glitch)."""


class CrashAfterEffectError(OnceflowError):
    """Simulated crash: the effect was applied, then the worker died
    before the completion record was written. Outcome is unknown to the
    broker, which is exactly the case that makes naive replay dangerous."""
