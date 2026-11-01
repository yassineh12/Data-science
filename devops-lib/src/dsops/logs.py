"""Structured JSON logging with context that follows the request across threads and tasks.

::

    configure_logging(level="INFO", service="ingest")
    log = get_logger(__name__)

    with log_context(job_id="2026-11-01", partition=7):
        log.info("loaded rows", extra={"rows": 1200})
    # {"ts": "...", "level": "INFO", "logger": "...", "msg": "loaded rows",
    #  "service": "ingest", "job_id": "2026-11-01", "partition": 7, "rows": 1200}

Context lives in a :mod:`contextvars` variable, so it is isolated per asyncio task
and per thread (use :func:`copy_context_to` when handing work to a thread pool).
"""

from __future__ import annotations

import contextlib
import contextvars
import datetime as _dt
import json
import logging
import sys
import uuid
from typing import IO, Any, Callable, Iterator

_CONTEXT: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("dsops_log_context", default={})

# Attributes every LogRecord has; anything else in record.__dict__ came from ``extra=``.
_RESERVED = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime", "taskName"}


def get_context() -> dict[str, Any]:
    return dict(_CONTEXT.get())


@contextlib.contextmanager
def log_context(**fields: Any) -> Iterator[dict[str, Any]]:
    """Attach ``fields`` to every log record emitted inside the block."""
    token = _CONTEXT.set({**_CONTEXT.get(), **fields})
    try:
        yield _CONTEXT.get()
    finally:
        _CONTEXT.reset(token)


def bind(**fields: Any) -> None:
    """Add fields to the current context without a ``with`` block (e.g. at worker start-up)."""
    _CONTEXT.set({**_CONTEXT.get(), **fields})


def clear_context() -> None:
    _CONTEXT.set({})


def new_correlation_id() -> str:
    return uuid.uuid4().hex


def copy_context_to(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap ``func`` so it runs with the caller's log context (for executors/threads)."""
    ctx = contextvars.copy_context()

    def runner(*args: Any, **kwargs: Any) -> Any:
        return ctx.copy().run(func, *args, **kwargs)

    return runner


class JsonFormatter(logging.Formatter):
    def __init__(self, *, static_fields: dict[str, Any] | None = None):
        super().__init__()
        self.static_fields = static_fields or {}

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": _dt.datetime.fromtimestamp(record.created, tz=_dt.timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(self.static_fields)
        payload.update(_CONTEXT.get())
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_type"] = record.exc_info[0].__name__ if record.exc_info[0] else None
            payload["exc"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


class ContextTextFormatter(logging.Formatter):
    """Human-readable output for local development, with context appended as key=value."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)-8s %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        ctx = {**_CONTEXT.get(), **{k: v for k, v in record.__dict__.items()
                                    if k not in _RESERVED and not k.startswith("_")}}
        if ctx:
            base += " " + " ".join(f"{k}={v}" for k, v in ctx.items())
        return base


def configure_logging(
    level: str | int = "INFO",
    *,
    json_output: bool = True,
    service: str | None = None,
    stream: IO[str] | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> logging.Handler:
    """Install one handler on the root logger (replacing earlier dsops handlers)."""
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, "_dsops", False):
            root.removeHandler(h)

    handler = logging.StreamHandler(stream or sys.stderr)
    handler._dsops = True  # type: ignore[attr-defined]
    static = dict(extra_fields or {})
    if service:
        static["service"] = service
    handler.setFormatter(JsonFormatter(static_fields=static) if json_output else ContextTextFormatter())
    root.addHandler(handler)
    root.setLevel(level if isinstance(level, int) else level.upper())
    return handler


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
