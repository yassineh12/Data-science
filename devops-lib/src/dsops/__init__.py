"""dsops — a DevOps toolkit for distributed systems and data engineering.

The core is dependency-free. Optional extras unlock YAML config (``dsops[yaml]``)
and Redis-backed coordination (``dsops[redis]``).

Modules
-------
resilience   retry/backoff, circuit breaker, token-bucket rate limiter, bulkhead
config       layered, typed configuration (defaults -> files -> env -> overrides)
logs         structured JSON logging with context propagation
metrics      counters, gauges, histograms with Prometheus exposition
health       liveness/readiness checks and a tiny ops HTTP server
distributed  consistent hashing, distributed locks, idempotency
pipeline     DAG runner with retries/checkpoints, batching, data-quality checks
"""

from .config import ConfigError, Secret, load_config
from .distributed import (
    ConsistentHashRing,
    FileLock,
    InMemoryIdempotencyStore,
    LockNotAcquired,
    SQLiteIdempotencyStore,
    idempotent,
)
from .health import HealthRegistry, serve_ops
from .logs import configure_logging, get_logger, log_context
from .metrics import Counter, Gauge, Histogram, Registry, default_registry
from .pipeline import DAG, Field, Schema, chunked, parallel_map, validate_records
from .resilience import (
    Bulkhead,
    BulkheadFullError,
    CircuitBreaker,
    CircuitOpenError,
    RetryError,
    TokenBucket,
    retry,
)

__version__ = "0.1.0"

__all__ = [
    "Bulkhead",
    "BulkheadFullError",
    "CircuitBreaker",
    "CircuitOpenError",
    "ConfigError",
    "ConsistentHashRing",
    "Counter",
    "DAG",
    "Field",
    "FileLock",
    "Gauge",
    "HealthRegistry",
    "Histogram",
    "InMemoryIdempotencyStore",
    "LockNotAcquired",
    "Registry",
    "RetryError",
    "SQLiteIdempotencyStore",
    "Schema",
    "Secret",
    "TokenBucket",
    "chunked",
    "configure_logging",
    "default_registry",
    "get_logger",
    "idempotent",
    "load_config",
    "log_context",
    "parallel_map",
    "retry",
    "serve_ops",
    "validate_records",
    "__version__",
]
