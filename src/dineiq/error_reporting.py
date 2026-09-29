"""User-safe operation errors with full diagnostics retained in a local log."""

from __future__ import annotations

import logging
from pathlib import Path


def operation_error_message(action: str, error: BaseException, log_path: Path) -> str:
    """Log diagnostic details and return an actionable message safe for the UI."""
    log_path = Path(log_path)
    logger = logging.getLogger("dineiq.operation_errors")
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        resolved = log_path.resolve()
        if not any(
            isinstance(handler, logging.FileHandler)
            and Path(handler.baseFilename).resolve() == resolved
            for handler in logger.handlers
        ):
            handler = logging.FileHandler(resolved, encoding="utf-8")
            handler.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)s %(message)s"
            ))
            logger.addHandler(handler)
        logger.error(
            "%s failed (%s): %s",
            action,
            type(error).__name__,
            error,
            exc_info=(type(error), error, error.__traceback__),
        )
        for handler in logger.handlers:
            handler.flush()
    except OSError:
        logging.getLogger("dineiq.operation_errors.fallback").exception(
            "%s failed; could not write diagnostics to %s", action, log_path
        )

    kind = type(error).__name__
    if isinstance(error, FileNotFoundError):
        reason = "a required input or generated output was not found; check the configured dataset and build outputs"
    elif isinstance(error, PermissionError):
        reason = "the application cannot access a required file or database; check file permissions"
    elif isinstance(error, TimeoutError):
        reason = "the operation timed out; check that Spark and required services are running"
    elif isinstance(error, ConnectionError):
        reason = "a required service could not be reached; check the service and network connection"
    elif isinstance(error, ValueError):
        reason = "the supplied value or configuration is invalid; review the inputs and configuration"
    else:
        reason = "an unexpected processing, model, Spark, or database error occurred"
    return (
        f"{action} could not complete: {reason}. "
        f"Diagnostic details ({kind}) were recorded in {log_path}."
    )
