"""End-to-end example: a resilient, observable, resumable ETL pipeline.

Run it:

    dsops dag run examples/etl_pipeline.py:dag --param rows=500 --run-id demo --checkpoint-dir .ckpt
    dsops dag graph examples/etl_pipeline.py:dag
    python examples/etl_pipeline.py          # same thing, plus a metrics dump

What it shows
-------------
* config      typed settings from defaults + env (ETL_SOURCE__FAILURE_RATE=0.5 ...)
* resilience  retry with jittered backoff and a circuit breaker around a flaky "API"
* pipeline    DAG with parallel branches, checkpoints, data-quality gate
* distributed consistent hashing to assign rows to workers
* connectors  a partitioned dataset written with mode="overwrite", so re-runs are idempotent
              (set ETL_WAREHOUSE=s3://bucket/prefix to write to S3 instead of a local dir)
* metrics     task/row counters exposed in Prometheus format
"""

from __future__ import annotations

import json
import random
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

from dsops import (
    DAG,
    CircuitBreaker,
    ConsistentHashRing,
    Counter,
    Field,
    Schema,
    load_config,
    parallel_map,
    retry,
    validate_records,
    write_partitioned,
)
from dsops.metrics import Registry
from dsops.pipeline import in_range, matches, one_of


@dataclass
class Source:
    failure_rate: float = 0.2  # how often the fake API errors
    page_size: int = 100


@dataclass
class Settings:
    source: Source = field(default_factory=Source)
    workers: list[str] = field(default_factory=lambda: ["worker-a", "worker-b", "worker-c"])
    max_error_rate: float = 0.05
    warehouse: str = str(Path(tempfile.gettempdir()) / "dsops-demo-warehouse")  # or s3://bucket/prefix


settings = load_config(Settings, env_prefix="ETL_")
registry = Registry()
rows_loaded = Counter("etl_rows_loaded_total", "Rows written to the warehouse", labelnames=("worker",),
                      registry=registry)
api_breaker = CircuitBreaker("orders-api", failure_threshold=10, recovery_timeout=5)
rng = random.Random(7)


@retry(max_attempts=6, base_delay=0.01, retry_on=(ConnectionError,))
@api_breaker
def fetch_page(page: int, size: int) -> list[dict]:
    """Stand-in for an HTTP call that sometimes fails."""
    if rng.random() < settings.source.failure_rate:
        raise ConnectionError(f"page {page}: upstream 503")
    out = []
    for i in range(page * size, (page + 1) * size):
        out.append({
            "order_id": i,
            "email": f"user{i}@example.com" if i % 37 else "broken-email",  # a few bad rows
            "amount": round(rng.uniform(1, 500), 2),
            "status": rng.choice(["new", "paid", "refunded"]),
        })
    return out


ORDERS = Schema([
    Field("order_id", int, nullable=False, unique=True),
    Field("email", str, checks=[matches(r"^[^@\s]+@[^@\s]+$")]),
    Field("amount", float, checks=[in_range(0, 10_000)]),
    Field("status", str, checks=[one_of({"new", "paid", "refunded"})]),
])

dag = DAG("orders_etl")


@dag.task(retries=1)
def extract(ctx) -> list[dict]:
    total = int(ctx.params.get("rows", 300))
    size = settings.source.page_size
    pages = range((total + size - 1) // size)
    batches = parallel_map(lambda p: fetch_page(p, size), pages, workers=4)
    rows = [r for batch in batches for r in batch][:total]
    ctx.log.info("extracted rows", extra={"rows": len(rows)})
    return rows


@dag.task(depends_on=[extract])
def validate(extract, ctx) -> list[dict]:
    report = validate_records(extract, ORDERS, min_rows=1)
    ctx.log.info("quality report", extra=report.to_dict())
    report.raise_if(max_error_rate=settings.max_error_rate)  # gate: stop bad data reaching the warehouse
    return report.valid


@dag.task(depends_on=[validate])
def revenue_by_status(validate) -> dict[str, float]:
    totals: dict[str, float] = {}
    for r in validate:
        if r["status"] != "refunded":
            totals[r["status"]] = round(totals.get(r["status"], 0) + r["amount"], 2)
    return totals


@dag.task(depends_on=[validate])
def load(validate, ctx) -> dict[str, int]:
    """Write one dataset partitioned by run and by owning worker (chosen with a hash ring).

    mode="overwrite" replaces only this run's partitions, so re-running the same
    run_id rewrites them instead of duplicating rows.
    """
    ring = ConsistentHashRing(settings.workers)
    result = write_partitioned(
        validate,
        settings.warehouse,
        partition_by={"run": lambda r: ctx.run_id, "worker": lambda r: ring.get_node(str(r["order_id"]))},
        mode="overwrite",
        compression="gzip",
        max_rows_per_file=250,
        write_id=ctx.run_id,
    )
    written: dict[str, int] = {}
    for f in result.files:
        written[f.partition["worker"]] = written.get(f.partition["worker"], 0) + f.rows
    for worker, n in written.items():
        rows_loaded.labels(worker).inc(n)
    return dict(sorted(written.items()))


@dag.task(depends_on=[revenue_by_status, load])
def publish(revenue_by_status, load) -> dict:
    return {"revenue": revenue_by_status, "rows_per_worker": load}


if __name__ == "__main__":
    from dsops import configure_logging

    configure_logging("INFO", json_output=False)
    result = dag.run(params={"rows": 600}, run_id="example", max_workers=4, metrics=registry)
    print(result.summary())
    if result.ok:
        print(json.dumps(result["publish"], indent=2))
    print(registry.render())
