"""Coordination primitives for distributed workers.

ConsistentHashRing   route keys to nodes so adding/removing a node moves ~1/N of keys
FileLock             cross-process lock on one host or a shared filesystem (POSIX)
RedisLock            lease-based lock across hosts (needs ``dsops[redis]``)
idempotent           make at-least-once consumers safe to re-run, backed by a store
"""

from __future__ import annotations

import bisect
import functools
import hashlib
import json
import os
import pickle
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


# --------------------------------------------------------------------------- #
# Consistent hashing
# --------------------------------------------------------------------------- #


def _hash(key: str) -> int:
    return int.from_bytes(hashlib.blake2b(key.encode(), digest_size=8).digest(), "big")


class ConsistentHashRing:
    """Hash ring with virtual nodes and optional per-node weights.

    ``get_node(key)`` is stable across processes and machines (no reliance on
    Python's randomised ``hash()``), so every worker agrees on ownership.
    """

    def __init__(self, nodes: Iterable[str] | dict[str, int] = (), *, replicas: int = 160):
        if replicas < 1:
            raise ValueError("replicas must be >= 1")
        self.replicas = replicas
        self._ring: list[int] = []
        self._owners: dict[int, str] = {}
        self._weights: dict[str, int] = {}
        items = nodes.items() if isinstance(nodes, dict) else ((n, 1) for n in nodes)
        for node, weight in items:
            self.add_node(node, weight)

    @property
    def nodes(self) -> list[str]:
        return sorted(self._weights)

    def __len__(self) -> int:
        return len(self._weights)

    def __contains__(self, node: object) -> bool:
        return node in self._weights

    def add_node(self, node: str, weight: int = 1) -> None:
        if node in self._weights:
            raise ValueError(f"node {node!r} already in ring")
        if weight < 1:
            raise ValueError("weight must be >= 1")
        self._weights[node] = weight
        for i in range(self.replicas * weight):
            h = _hash(f"{node}#{i}")
            if h in self._owners:  # astronomically rare collision: first owner wins
                continue
            self._owners[h] = node
            bisect.insort(self._ring, h)

    def remove_node(self, node: str) -> None:
        if node not in self._weights:
            raise KeyError(node)
        del self._weights[node]
        self._ring = [h for h in self._ring if self._owners[h] != node]
        self._owners = {h: n for h, n in self._owners.items() if n != node}

    def get_node(self, key: str) -> str:
        if not self._ring:
            raise LookupError("hash ring is empty")
        idx = bisect.bisect(self._ring, _hash(key)) % len(self._ring)
        return self._owners[self._ring[idx]]

    def get_nodes(self, key: str, n: int) -> list[str]:
        """Up to ``n`` distinct nodes for ``key`` in ring order — use for replica placement."""
        if not self._ring:
            raise LookupError("hash ring is empty")
        n = min(n, len(self._weights))
        out: list[str] = []
        start = bisect.bisect(self._ring, _hash(key))
        for offset in range(len(self._ring)):
            node = self._owners[self._ring[(start + offset) % len(self._ring)]]
            if node not in out:
                out.append(node)
                if len(out) == n:
                    break
        return out

    def distribution(self, keys: Iterable[str]) -> dict[str, int]:
        counts = dict.fromkeys(self._weights, 0)
        for k in keys:
            counts[self.get_node(k)] += 1
        return counts


# --------------------------------------------------------------------------- #
# Locks
# --------------------------------------------------------------------------- #


class LockNotAcquired(TimeoutError):
    pass


class Lock(Protocol):
    def acquire(self, timeout: float | None = None) -> bool: ...
    def release(self) -> None: ...
    def __enter__(self) -> "Lock": ...
    def __exit__(self, *exc: Any) -> None: ...


class _LockBase:
    def __enter__(self) -> Any:
        if not self.acquire(timeout=getattr(self, "timeout", None)):  # type: ignore[attr-defined]
            raise LockNotAcquired(f"could not acquire {self!r}")
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()  # type: ignore[attr-defined]


class FileLock(_LockBase):
    """Exclusive ``fcntl.flock`` on a lock file. Released automatically if the process dies."""

    def __init__(self, path: str | os.PathLike[str], *, timeout: float | None = None, poll_interval: float = 0.05):
        import fcntl  # noqa: F401 - fail early on non-POSIX platforms

        self.path = Path(path)
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._fd: int | None = None
        self._thread_lock = threading.Lock()

    def acquire(self, timeout: float | None = None) -> bool:
        import fcntl

        timeout = self.timeout if timeout is None else timeout
        deadline = None if timeout is None else time.monotonic() + timeout
        if not self._thread_lock.acquire(timeout=-1 if timeout is None else max(0.0, timeout)):
            return False
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if deadline is not None and time.monotonic() >= deadline:
                    os.close(fd)
                    self._thread_lock.release()
                    return False
                time.sleep(self.poll_interval)
        os.ftruncate(fd, 0)
        os.write(fd, json.dumps({"pid": os.getpid(), "acquired_at": time.time()}).encode())
        self._fd = fd
        return True

    def release(self) -> None:
        import fcntl

        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None
        self._thread_lock.release()

    @property
    def locked(self) -> bool:
        return self._fd is not None

    def __repr__(self) -> str:
        return f"FileLock({str(self.path)!r})"


class RedisLock(_LockBase):
    """Lease lock: ``SET key token NX PX ttl``; released only by the holder (token check in Lua).

    The lease expires after ``ttl`` seconds even if the holder crashes. Long jobs
    should call :meth:`extend` periodically. This is a single-instance Redis lock —
    adequate for avoiding duplicate work, not for strict mutual exclusion under
    network partitions (use fencing tokens or a consensus store for that).
    """

    _RELEASE = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
    _EXTEND = ("if redis.call('get', KEYS[1]) == ARGV[1] then "
               "return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end")

    def __init__(self, client: Any, name: str, *, ttl: float = 30.0, timeout: float | None = None,
                 poll_interval: float = 0.1):
        self.client = client
        self.key = f"dsops:lock:{name}"
        self.ttl = ttl
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.token: str | None = None

    def acquire(self, timeout: float | None = None) -> bool:
        timeout = self.timeout if timeout is None else timeout
        deadline = None if timeout is None else time.monotonic() + timeout
        token = uuid.uuid4().hex
        while True:
            if self.client.set(self.key, token, nx=True, px=int(self.ttl * 1000)):
                self.token = token
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(self.poll_interval)

    def extend(self, ttl: float | None = None) -> bool:
        if self.token is None:
            return False
        return bool(self.client.eval(self._EXTEND, 1, self.key, self.token, int((ttl or self.ttl) * 1000)))

    def release(self) -> None:
        if self.token is not None:
            self.client.eval(self._RELEASE, 1, self.key, self.token)
            self.token = None

    def __repr__(self) -> str:
        return f"RedisLock({self.key!r})"


# --------------------------------------------------------------------------- #
# Idempotency
# --------------------------------------------------------------------------- #


_MISSING = object()


class IdempotencyStore(Protocol):
    def get(self, key: str) -> Any: ...
    def put(self, key: str, value: Any, ttl: float | None = None) -> None: ...


class InMemoryIdempotencyStore:
    def __init__(self, *, clock: Callable[[], float] = time.time):
        self._data: dict[str, tuple[Any, float | None]] = {}
        self._lock = threading.Lock()
        self._clock = clock

    def get(self, key: str) -> Any:
        with self._lock:
            item = self._data.get(key)
            if item is None:
                return _MISSING
            value, expires = item
            if expires is not None and self._clock() >= expires:
                del self._data[key]
                return _MISSING
            return value

    def put(self, key: str, value: Any, ttl: float | None = None) -> None:
        with self._lock:
            self._data[key] = (value, None if ttl is None else self._clock() + ttl)


class SQLiteIdempotencyStore:
    """Durable store that survives restarts and is shared by processes on one host."""

    def __init__(self, path: str | os.PathLike[str], *, clock: Callable[[], float] = time.time):
        self.path = str(path)
        self._clock = clock
        self._local = threading.local()
        with self._conn() as c:
            c.execute("CREATE TABLE IF NOT EXISTS idempotency "
                      "(key TEXT PRIMARY KEY, value BLOB NOT NULL, expires_at REAL)")

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    def get(self, key: str) -> Any:
        row = self._conn().execute("SELECT value, expires_at FROM idempotency WHERE key=?", (key,)).fetchone()
        if row is None:
            return _MISSING
        if row[1] is not None and self._clock() >= row[1]:
            self._conn().execute("DELETE FROM idempotency WHERE key=?", (key,))
            return _MISSING
        return pickle.loads(row[0])

    def put(self, key: str, value: Any, ttl: float | None = None) -> None:
        expires = None if ttl is None else self._clock() + ttl
        self._conn().execute("INSERT OR REPLACE INTO idempotency(key, value, expires_at) VALUES (?,?,?)",
                             (key, pickle.dumps(value), expires))

    def purge_expired(self) -> int:
        cur = self._conn().execute("DELETE FROM idempotency WHERE expires_at IS NOT NULL AND expires_at <= ?",
                                   (self._clock(),))
        return cur.rowcount


def idempotent(
    store: IdempotencyStore,
    *,
    key: Callable[..., str],
    ttl: float | None = None,
    lock_factory: Callable[[str], Any] | None = None,
) -> Callable[[F], F]:
    """Return the stored result instead of re-running ``func`` for a key already processed.

    ``key`` derives the idempotency key from the call arguments (e.g. a message ID).
    Pass ``lock_factory`` (key -> lock) to stop two workers processing the same key at once.
    Failures are not stored, so a failed call can be retried.
    """

    def deco(func: F) -> F:
        local_locks: dict[str, threading.Lock] = {}
        guard = threading.Lock()

        def _lock_for(k: str) -> Any:
            if lock_factory is not None:
                return lock_factory(k)
            with guard:
                return local_locks.setdefault(k, threading.Lock())

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            k = f"{func.__module__}.{func.__qualname__}:{key(*args, **kwargs)}"
            cached = store.get(k)
            if cached is not _MISSING:
                return cached
            with _lock_for(k):
                cached = store.get(k)  # someone may have finished while we waited
                if cached is not _MISSING:
                    return cached
                result = func(*args, **kwargs)
                store.put(k, result, ttl)
                return result

        return wrapper  # type: ignore[return-value]

    return deco
