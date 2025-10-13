"""Domain specific error types for the ETL workflow demo."""

from temporalio.exceptions import ApplicationError


class RetryableSourceError(ApplicationError):
    """Raised when the source experiences a transient failure."""

    def __init__(self, message: str) -> None:
        super().__init__(message, non_retryable=False)


class NonRetryableSourceError(ApplicationError):
    """Raised when the source indicates a permanent failure."""

    def __init__(self, message: str) -> None:
        super().__init__(message, non_retryable=True)


class SinkWriteError(ApplicationError):
    """Raised when the sink write fails unexpectedly."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message, non_retryable=not retryable)
