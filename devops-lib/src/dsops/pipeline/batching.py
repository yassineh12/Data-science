"""Helpers for moving data in batches and fanning work out across workers."""

from __future__ import annotations

import itertools
from collections import defaultdict
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Hashable, Iterable, Iterator, TypeVar

T = TypeVar("T")
R = TypeVar("R")


def chunked(iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield lists of at most ``size`` items. Works on unbounded streams (lazy)."""
    if size < 1:
        raise ValueError("size must be >= 1")
    it = iter(iterable)
    while batch := list(itertools.islice(it, size)):
        yield batch


@dataclass
class MapResult:
    results: list[Any] = field(default_factory=list)
    errors: list[tuple[int, Any, BaseException]] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def parallel_map(
    fn: Callable[[T], R],
    items: Iterable[T],
    *,
    workers: int = 8,
    mode: str = "thread",
    ordered: bool = True,
    return_exceptions: bool = False,
    executor: Executor | None = None,
) -> list[R] | MapResult:
    """Apply ``fn`` to every item concurrently.

    ``mode="thread"`` suits I/O (APIs, databases, object storage); ``mode="process"``
    suits CPU-bound transforms (``fn`` must then be picklable / module-level).

    With ``return_exceptions=False`` the first error is raised. With ``True`` a
    :class:`MapResult` is returned holding successes (``None`` in failed slots when
    ordered) and ``(index, item, exception)`` for every failure.
    """
    if mode not in ("thread", "process"):
        raise ValueError("mode must be 'thread' or 'process'")
    items = list(items)
    own = executor is None
    pool: Executor = executor or (ThreadPoolExecutor if mode == "thread" else ProcessPoolExecutor)(max_workers=workers)
    try:
        futures = {pool.submit(fn, item): i for i, item in enumerate(items)}
        slots: list[Any] = [None] * len(items)
        completion: list[Any] = []
        errors: list[tuple[int, Any, BaseException]] = []
        for fut in as_completed(futures):
            i = futures[fut]
            try:
                value = fut.result()
            except Exception as exc:  # noqa: BLE001
                if not return_exceptions:
                    for f in futures:
                        f.cancel()
                    raise
                errors.append((i, items[i], exc))
                continue
            slots[i] = value
            completion.append(value)
    finally:
        if own:
            pool.shutdown(wait=True, cancel_futures=True)

    values = slots if ordered else completion
    if return_exceptions:
        return MapResult(values, sorted(errors, key=lambda e: e[0]))
    return values


def partition_by(records: Iterable[T], key: Callable[[T], Hashable]) -> dict[Hashable, list[T]]:
    """Group records by ``key`` — e.g. by date before writing ``dt=YYYY-MM-DD`` partitions."""
    out: dict[Hashable, list[T]] = defaultdict(list)
    for r in records:
        out[key(r)].append(r)
    return dict(out)


def dedupe(records: Iterable[T], key: Callable[[T], Hashable], *, keep: str = "last") -> list[T]:
    """Drop duplicate records by ``key``, keeping the first or last occurrence (order preserved)."""
    if keep not in ("first", "last"):
        raise ValueError("keep must be 'first' or 'last'")
    seen: dict[Hashable, int] = {}
    out: list[T] = []
    for r in records:
        k = key(r)
        if k in seen:
            if keep == "last":
                out[seen[k]] = r
            continue
        seen[k] = len(out)
        out.append(r)
    return out
