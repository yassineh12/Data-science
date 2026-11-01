import multiprocessing as mp
import threading
import time
from pathlib import Path

import pytest

from dsops.distributed import (
    ConsistentHashRing,
    FileLock,
    InMemoryIdempotencyStore,
    LockNotAcquired,
    RedisLock,
    SQLiteIdempotencyStore,
    idempotent,
)

KEYS = [f"key-{i}" for i in range(20_000)]


# ------------------------------------------------------------------- ring


def test_ring_is_deterministic_and_balanced():
    a = ConsistentHashRing(["n1", "n2", "n3", "n4"])
    b = ConsistentHashRing(["n4", "n3", "n2", "n1"])
    assert all(a.get_node(k) == b.get_node(k) for k in KEYS[:1000])
    dist = a.distribution(KEYS)
    expected = len(KEYS) / 4
    assert all(abs(c - expected) / expected < 0.2 for c in dist.values()), dist


def test_adding_a_node_moves_about_one_over_n_keys():
    ring = ConsistentHashRing(["n1", "n2", "n3", "n4"])
    before = {k: ring.get_node(k) for k in KEYS}
    ring.add_node("n5")
    moved = [k for k in KEYS if ring.get_node(k) != before[k]]
    assert 0.12 < len(moved) / len(KEYS) < 0.28           # ~1/5
    assert all(ring.get_node(k) == "n5" for k in moved)    # keys only move to the new node


def test_remove_node_and_replicas():
    ring = ConsistentHashRing(["a", "b", "c"])
    nodes = ring.get_nodes("user:42", 2)
    assert len(nodes) == 2 and len(set(nodes)) == 2
    assert ring.get_nodes("x", 10) and len(ring.get_nodes("x", 10)) == 3
    ring.remove_node("a")
    assert "a" not in ring and all(ring.get_node(k) != "a" for k in KEYS[:500])
    with pytest.raises(KeyError):
        ring.remove_node("a")


def test_weights():
    ring = ConsistentHashRing({"big": 3, "small": 1})
    dist = ring.distribution(KEYS)
    assert 2.0 < dist["big"] / dist["small"] < 4.5


def test_empty_ring():
    with pytest.raises(LookupError):
        ConsistentHashRing().get_node("x")


# ------------------------------------------------------------------ locks


def _hold_lock(path: str, ready, release):
    with FileLock(path):
        ready.set()
        release.wait(5)


def test_file_lock_excludes_other_processes(tmp_path: Path):
    path = str(tmp_path / "job.lock")
    ctx = mp.get_context("fork")
    ready, release = ctx.Event(), ctx.Event()
    p = ctx.Process(target=_hold_lock, args=(path, ready, release))
    p.start()
    try:
        assert ready.wait(5)
        assert FileLock(path).acquire(timeout=0.1) is False
        with pytest.raises(LockNotAcquired):
            with FileLock(path, timeout=0.1):
                pass
    finally:
        release.set()
        p.join(5)
    with FileLock(path, timeout=1) as lock:
        assert lock.locked


def test_file_lock_between_threads(tmp_path: Path):
    lock = FileLock(tmp_path / "t.lock")
    counter, out = [0], []

    def work():
        for _ in range(50):
            with lock:
                v = counter[0]
                time.sleep(0)
                counter[0] = v + 1

    threads = [threading.Thread(target=work) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert counter[0] == 200


class FakeRedis:
    """Just enough of redis-py for RedisLock: SET NX PX and the two Lua scripts."""

    def __init__(self):
        self.data = {}

    def set(self, key, value, nx=False, px=None):
        if nx and key in self.data:
            return None
        self.data[key] = value
        return True

    def eval(self, script, numkeys, key, token, *args):
        if self.data.get(key) != token:
            return 0
        if "del" in script:
            del self.data[key]
        return 1


def test_redis_lock_only_holder_can_release():
    r = FakeRedis()
    a = RedisLock(r, "etl", ttl=10)
    b = RedisLock(r, "etl", ttl=10, poll_interval=0.01)
    assert a.acquire(timeout=0)
    assert b.acquire(timeout=0.03) is False
    b.release()  # not the holder: no-op
    assert "dsops:lock:etl" in r.data
    assert a.extend()
    a.release()
    assert b.acquire(timeout=0)


# ------------------------------------------------------------ idempotency


@pytest.mark.parametrize("make_store", [
    lambda tmp: InMemoryIdempotencyStore(),
    lambda tmp: SQLiteIdempotencyStore(tmp / "idem.db"),
])
def test_idempotent_runs_once_per_key(tmp_path, make_store):
    store, calls = make_store(tmp_path), []

    @idempotent(store, key=lambda msg: msg["id"])
    def handle(msg):
        calls.append(msg["id"])
        return {"processed": msg["id"]}

    assert handle({"id": "m1"}) == {"processed": "m1"}
    assert handle({"id": "m1"}) == {"processed": "m1"}   # redelivery
    handle({"id": "m2"})
    assert calls == ["m1", "m2"]


def test_idempotent_concurrent_duplicates_run_once():
    store, calls = InMemoryIdempotencyStore(), []

    @idempotent(store, key=lambda k: k)
    def slow(k):
        calls.append(k)
        time.sleep(0.05)
        return k

    threads = [threading.Thread(target=slow, args=("same",)) for _ in range(5)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert calls == ["same"]


def test_idempotent_failures_are_not_cached():
    store, attempts = InMemoryIdempotencyStore(), []

    @idempotent(store, key=lambda: "k")
    def f():
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError
        return "ok"

    with pytest.raises(RuntimeError):
        f()
    assert f() == "ok"


def test_ttl_expiry(tmp_path):
    now = [1000.0]
    for store in (InMemoryIdempotencyStore(clock=lambda: now[0]),
                  SQLiteIdempotencyStore(tmp_path / "ttl.db", clock=lambda: now[0])):
        store.put("k", 1, ttl=10)
        assert store.get("k") == 1
        now[0] += 11
        assert store.get("k") != 1
        now[0] = 1000.0


def test_sqlite_store_survives_restart(tmp_path):
    SQLiteIdempotencyStore(tmp_path / "d.db").put("k", {"a": 1})
    assert SQLiteIdempotencyStore(tmp_path / "d.db").get("k") == {"a": 1}
