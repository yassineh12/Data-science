"""Health checks and a minimal ops HTTP server for containers and Kubernetes probes.

::

    health = HealthRegistry()
    health.register("postgres", lambda: db.execute("select 1"), critical=True)
    health.register("cache", ping_redis, critical=False)   # failure => degraded, not down

    server = serve_ops(port=9100, health=health)   # /healthz /readyz /metrics in a daemon thread
    ...
    server.shutdown()
"""

from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from .metrics import Registry, default_registry

OK, DEGRADED, FAIL = "ok", "degraded", "fail"


@dataclass
class CheckResult:
    name: str
    status: str
    critical: bool
    duration_ms: float
    detail: str | None = None


@dataclass
class _Check:
    fn: Callable[[], Any]
    critical: bool
    timeout: float


class HealthRegistry:
    """A check passes if it returns anything other than ``False`` without raising in time."""

    def __init__(self, *, default_timeout: float = 2.0):
        self._checks: dict[str, _Check] = {}
        self._ready = threading.Event()
        self._ready.set()
        self.default_timeout = default_timeout

    def register(self, name: str, fn: Callable[[], Any], *, critical: bool = True,
                 timeout: float | None = None) -> None:
        self._checks[name] = _Check(fn, critical, timeout or self.default_timeout)

    def check(self, name: str, *, critical: bool = True, timeout: float | None = None) -> Callable:
        """Decorator form of :meth:`register`."""

        def deco(fn: Callable[[], Any]) -> Callable[[], Any]:
            self.register(name, fn, critical=critical, timeout=timeout)
            return fn

        return deco

    def unregister(self, name: str) -> None:
        self._checks.pop(name, None)

    def set_ready(self, ready: bool) -> None:
        """Flip readiness, e.g. ``False`` while draining before shutdown."""
        (self._ready.set if ready else self._ready.clear)()

    @property
    def ready(self) -> bool:
        return self._ready.is_set()

    def run(self) -> dict[str, Any]:
        results: list[CheckResult] = []
        if self._checks:
            with ThreadPoolExecutor(max_workers=min(8, len(self._checks))) as pool:
                futures = {name: (pool.submit(_timed, c.fn), c) for name, c in self._checks.items()}
                for name, (fut, chk) in futures.items():
                    try:
                        ok, ms, detail = fut.result(timeout=chk.timeout)
                    except FutureTimeout:
                        ok, ms, detail = False, chk.timeout * 1000, f"timed out after {chk.timeout}s"
                    status = OK if ok else (FAIL if chk.critical else DEGRADED)
                    results.append(CheckResult(name, status, chk.critical, round(ms, 2), detail))

        if any(r.status == FAIL for r in results):
            overall = FAIL
        elif any(r.status == DEGRADED for r in results):
            overall = DEGRADED
        else:
            overall = OK
        return {"status": overall, "checks": [asdict(r) for r in results]}


def _timed(fn: Callable[[], Any]) -> tuple[bool, float, str | None]:
    start = time.perf_counter()
    try:
        result = fn()
        ok, detail = result is not False, None if result is not False else "check returned False"
    except Exception as exc:  # noqa: BLE001 - a failing check must not crash the probe
        ok, detail = False, f"{type(exc).__name__}: {exc}"
    return ok, (time.perf_counter() - start) * 1000, detail


def _make_handler(health: HealthRegistry, registry: Registry) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: str, ctype: str) -> None:
            data = body.encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802 - http.server API
            path = self.path.split("?", 1)[0]
            if path in ("/healthz", "/livez"):
                # Liveness: the process answers. Dependencies belong in readiness.
                self._send(200, json.dumps({"status": OK}), "application/json")
            elif path == "/readyz":
                report = health.run()
                if not health.ready:
                    report["status"] = FAIL
                    report["detail"] = "not ready (draining or starting)"
                code = 503 if report["status"] == FAIL else 200
                self._send(code, json.dumps(report), "application/json")
            elif path == "/metrics":
                self._send(200, registry.render(), "text/plain; version=0.0.4; charset=utf-8")
            else:
                self._send(404, json.dumps({"error": "not found"}), "application/json")

        def log_message(self, *args: Any) -> None:  # keep probes out of the logs
            pass

    return Handler


def serve_ops(
    port: int = 9100,
    *,
    host: str = "0.0.0.0",
    health: HealthRegistry | None = None,
    registry: Registry | None = None,
) -> ThreadingHTTPServer:
    """Start ``/healthz``, ``/readyz`` and ``/metrics`` on a daemon thread. Port 0 picks a free port."""
    server = ThreadingHTTPServer((host, port), _make_handler(health or HealthRegistry(), registry or default_registry))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="dsops-ops-server", daemon=True).start()
    return server
