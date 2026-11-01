import io
import json
import logging
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import pytest

from dsops.health import HealthRegistry, serve_ops
from dsops.logs import configure_logging, copy_context_to, get_logger, log_context
from dsops.metrics import Counter, Gauge, Histogram, Registry


# ------------------------------------------------------------------- logs


@pytest.fixture
def json_log():
    buf = io.StringIO()
    handler = configure_logging("DEBUG", service="test-svc", stream=buf)
    yield buf
    logging.getLogger().removeHandler(handler)


def _lines(buf):
    return [json.loads(line) for line in buf.getvalue().splitlines()]


def test_json_logging_includes_context_and_extra(json_log):
    log = get_logger("t")
    with log_context(job="nightly", partition=3):
        log.info("loaded %d rows", 10, extra={"table": "orders"})
    log.info("outside")
    first, second = _lines(json_log)
    assert first["msg"] == "loaded 10 rows"
    assert first["service"] == "test-svc"
    assert first["job"] == "nightly" and first["partition"] == 3 and first["table"] == "orders"
    assert "job" not in second


def test_exceptions_are_serialised(json_log):
    try:
        1 / 0
    except ZeroDivisionError:
        get_logger("t").exception("failed")
    (rec,) = _lines(json_log)
    assert rec["exc_type"] == "ZeroDivisionError" and "Traceback" in rec["exc"]


def test_context_crosses_into_thread_pool(json_log):
    log = get_logger("t")
    with log_context(request_id="abc"):
        with ThreadPoolExecutor(1) as pool:
            pool.submit(copy_context_to(lambda: log.info("in worker"))).result()
    (rec,) = _lines(json_log)
    assert rec["request_id"] == "abc"


# ---------------------------------------------------------------- metrics


def test_counter_gauge_histogram_render():
    reg = Registry()
    c = Counter("rows_total", "Rows", labelnames=("table",), registry=reg)
    g = Gauge("lag", "Lag", registry=reg)
    h = Histogram("latency_seconds", "Latency", buckets=(0.1, 1), registry=reg)

    c.labels(table="orders").inc(3)
    c.labels("users").inc()
    g.set(7)
    g.dec(2)
    for v in (0.05, 0.5, 5):
        h.observe(v)

    text = reg.render()
    assert '# TYPE rows_total counter' in text
    assert 'rows_total{table="orders"} 3' in text
    assert 'lag 5' in text
    assert 'latency_seconds_bucket{le="0.1"} 1' in text
    assert 'latency_seconds_bucket{le="1"} 2' in text
    assert 'latency_seconds_bucket{le="+Inf"} 3' in text
    assert 'latency_seconds_count 3' in text
    assert reg.snapshot()["lag"] == 5


def test_counter_rejects_negative_and_wrong_labels():
    reg = Registry()
    c = Counter("x_total", labelnames=("a",), registry=reg)
    with pytest.raises(ValueError):
        c.inc()  # needs labels
    with pytest.raises(ValueError):
        c.labels(b="1")
    with pytest.raises(ValueError):
        c.labels("1").inc(-1)


def test_duplicate_registration_rejected():
    reg = Registry()
    Counter("dup", registry=reg)
    with pytest.raises(ValueError):
        Counter("dup", registry=reg)


def test_counter_thread_safety():
    reg = Registry()
    c = Counter("n", registry=reg)

    def work():
        for _ in range(10_000):
            c.inc()

    threads = [threading.Thread(target=work) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert reg.snapshot()["n"] == 80_000


def test_histogram_timer_and_label_escaping():
    reg = Registry()
    h = Histogram("op_seconds", labelnames=("op",), registry=reg)
    with h.labels(op='say "hi"\n').time():
        pass
    assert 'op="say \\"hi\\"\\n"' in reg.render()


# ----------------------------------------------------------------- health


def test_health_registry_statuses():
    hr = HealthRegistry(default_timeout=0.5)
    hr.register("db", lambda: True)
    hr.register("cache", lambda: (_ for _ in ()).throw(ConnectionError("down")), critical=False)
    report = hr.run()
    assert report["status"] == "degraded"
    by_name = {c["name"]: c for c in report["checks"]}
    assert by_name["cache"]["detail"].startswith("ConnectionError")

    hr.register("queue", lambda: False)
    assert hr.run()["status"] == "fail"


def test_health_timeout():
    hr = HealthRegistry()
    hr.register("slow", lambda: time.sleep(1), timeout=0.05)
    report = hr.run()
    assert report["status"] == "fail" and "timed out" in report["checks"][0]["detail"]


def test_ops_server_endpoints():
    reg = Registry()
    Counter("hits_total", registry=reg).inc(2)
    hr = HealthRegistry()
    hr.register("ok", lambda: True)
    server = serve_ops(0, host="127.0.0.1", health=hr, registry=reg)
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        assert json.load(urllib.request.urlopen(f"{base}/healthz"))["status"] == "ok"
        assert json.load(urllib.request.urlopen(f"{base}/readyz"))["status"] == "ok"
        assert "hits_total 2" in urllib.request.urlopen(f"{base}/metrics").read().decode()

        hr.set_ready(False)  # draining
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(f"{base}/readyz")
        assert ei.value.code == 503
    finally:
        server.shutdown()
