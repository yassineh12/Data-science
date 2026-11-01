"""In-process metrics with Prometheus text exposition (no client library needed).

::

    rows = Counter("etl_rows_total", "Rows processed", labelnames=("table",))
    lag = Gauge("consumer_lag", "Messages behind head")
    latency = Histogram("load_seconds", "Load latency")

    rows.labels(table="orders").inc(500)
    with latency.time():
        load()

    print(default_registry.render())   # serve this on /metrics
"""

from __future__ import annotations

import bisect
import contextlib
import functools
import math
import threading
import time
from typing import Any, Callable, Iterator, Sequence, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, math.inf)


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _fmt_labels(names: Sequence[str], values: Sequence[str], extra: dict[str, str] | None = None) -> str:
    pairs = list(zip(names, values)) + list((extra or {}).items())
    if not pairs:
        return ""
    return "{" + ",".join(f'{k}="{_escape(str(v))}"' for k, v in pairs) + "}"


def _fmt_num(v: float) -> str:
    if v == math.inf:
        return "+Inf"
    if v == -math.inf:
        return "-Inf"
    if v == int(v) and abs(v) < 1e15:
        return str(int(v))
    return repr(float(v))


class Registry:
    def __init__(self) -> None:
        self._metrics: dict[str, _Metric] = {}
        self._lock = threading.Lock()

    def register(self, metric: "_Metric") -> None:
        with self._lock:
            if metric.name in self._metrics:
                raise ValueError(f"metric {metric.name!r} already registered")
            self._metrics[metric.name] = metric

    def unregister(self, name: str) -> None:
        with self._lock:
            self._metrics.pop(name, None)

    def get(self, name: str) -> "_Metric | None":
        return self._metrics.get(name)

    def collect(self) -> list["_Metric"]:
        with self._lock:
            return list(self._metrics.values())

    def render(self) -> str:
        """Prometheus text exposition format 0.0.4."""
        lines: list[str] = []
        for metric in sorted(self.collect(), key=lambda m: m.name):
            lines.append(f"# HELP {metric.name} {metric.documentation}")
            lines.append(f"# TYPE {metric.name} {metric.kind}")
            lines.extend(metric.samples())
        return "\n".join(lines) + "\n"

    def snapshot(self) -> dict[str, Any]:
        """JSON-friendly view of every metric, handy for tests and debug endpoints."""
        return {m.name: m.snapshot() for m in self.collect()}


default_registry = Registry()


class _Metric:
    kind = "untyped"

    def __init__(
        self,
        name: str,
        documentation: str = "",
        *,
        labelnames: Sequence[str] = (),
        registry: Registry | None = default_registry,
    ):
        self.name = name
        self.documentation = documentation or name
        self.labelnames = tuple(labelnames)
        self._children: dict[tuple[str, ...], Any] = {}
        self._lock = threading.Lock()
        if registry is not None:
            registry.register(self)

    def labels(self, *values: str, **kw: str) -> Any:
        if values and kw:
            raise TypeError("use positional or keyword label values, not both")
        if kw:
            if set(kw) != set(self.labelnames):
                raise ValueError(f"expected labels {self.labelnames}, got {tuple(kw)}")
            values = tuple(str(kw[n]) for n in self.labelnames)
        else:
            values = tuple(str(v) for v in values)
        if len(values) != len(self.labelnames):
            raise ValueError(f"expected {len(self.labelnames)} label values, got {len(values)}")
        with self._lock:
            child = self._children.get(values)
            if child is None:
                child = self._children[values] = self._new_child()
            return child

    def _default(self) -> Any:
        if self.labelnames:
            raise ValueError(f"metric {self.name!r} has labels; call .labels(...) first")
        return self.labels()

    def _new_child(self) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError

    def _items(self) -> list[tuple[tuple[str, ...], Any]]:
        with self._lock:
            return sorted(self._children.items())

    def samples(self) -> list[str]:
        return [f"{self.name}{_fmt_labels(self.labelnames, k)} {_fmt_num(c.get())}" for k, c in self._items()]

    def snapshot(self) -> Any:
        if not self.labelnames:
            items = self._items()
            return items[0][1].get() if items else 0
        return {",".join(k): c.get() for k, c in self._items()}


class _Value:
    def __init__(self) -> None:
        self._v = 0.0
        self._lock = threading.Lock()

    def get(self) -> float:
        return self._v


class _CounterChild(_Value):
    def inc(self, amount: float = 1.0) -> None:
        if amount < 0:
            raise ValueError("counters can only increase")
        with self._lock:
            self._v += amount


class Counter(_Metric):
    """Monotonically increasing value (requests, rows, errors)."""

    kind = "counter"

    def _new_child(self) -> _CounterChild:
        return _CounterChild()

    def inc(self, amount: float = 1.0) -> None:
        self._default().inc(amount)

    def count_exceptions(self, func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception:
                self.inc()
                raise

        return wrapper  # type: ignore[return-value]


class _GaugeChild(_Value):
    def set(self, value: float) -> None:
        with self._lock:
            self._v = float(value)

    def inc(self, amount: float = 1.0) -> None:
        with self._lock:
            self._v += amount

    def dec(self, amount: float = 1.0) -> None:
        self.inc(-amount)

    @contextlib.contextmanager
    def track_inprogress(self) -> Iterator[None]:
        self.inc()
        try:
            yield
        finally:
            self.dec()


class Gauge(_Metric):
    """Value that goes up and down (queue depth, lag, in-flight requests)."""

    kind = "gauge"

    def _new_child(self) -> _GaugeChild:
        return _GaugeChild()

    def set(self, value: float) -> None:
        self._default().set(value)

    def inc(self, amount: float = 1.0) -> None:
        self._default().inc(amount)

    def dec(self, amount: float = 1.0) -> None:
        self._default().dec(amount)

    def track_inprogress(self) -> contextlib.AbstractContextManager[None]:
        return self._default().track_inprogress()


class _HistogramChild:
    def __init__(self, buckets: Sequence[float]):
        self.buckets = tuple(buckets)
        self.counts = [0] * len(self.buckets)
        self.sum = 0.0
        self.count = 0
        self._lock = threading.Lock()

    def observe(self, value: float) -> None:
        idx = bisect.bisect_left(self.buckets, value)
        with self._lock:
            if idx < len(self.counts):
                self.counts[idx] += 1
            self.sum += value
            self.count += 1

    @contextlib.contextmanager
    def time(self, clock: Callable[[], float] = time.perf_counter) -> Iterator[None]:
        start = clock()
        try:
            yield
        finally:
            self.observe(clock() - start)

    def cumulative(self) -> list[tuple[float, int]]:
        out, running = [], 0
        with self._lock:
            for b, c in zip(self.buckets, self.counts):
                running += c
                out.append((b, running))
        return out

    def get(self) -> dict[str, Any]:
        return {"count": self.count, "sum": self.sum,
                "buckets": {_fmt_num(b): c for b, c in self.cumulative()}}


class Histogram(_Metric):
    """Distribution of observations (latency, batch size) in cumulative buckets."""

    kind = "histogram"

    def __init__(self, name: str, documentation: str = "", *, buckets: Sequence[float] = DEFAULT_BUCKETS, **kw: Any):
        b = sorted(set(float(x) for x in buckets))
        if not b or b[-1] != math.inf:
            b.append(math.inf)
        self.buckets = tuple(b)
        super().__init__(name, documentation, **kw)

    def _new_child(self) -> _HistogramChild:
        return _HistogramChild(self.buckets)

    def observe(self, value: float) -> None:
        self._default().observe(value)

    def time(self) -> contextlib.AbstractContextManager[None]:
        return self._default().time()

    def timed(self, func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with self.time():
                return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    def samples(self) -> list[str]:
        lines = []
        for key, child in self._items():
            for bound, cum in child.cumulative():
                lines.append(f"{self.name}_bucket{_fmt_labels(self.labelnames, key, {'le': _fmt_num(bound)})} {cum}")
            lines.append(f"{self.name}_sum{_fmt_labels(self.labelnames, key)} {_fmt_num(child.sum)}")
            lines.append(f"{self.name}_count{_fmt_labels(self.labelnames, key)} {child.count}")
        return lines
