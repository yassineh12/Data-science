# dsops

A DevOps toolkit for **distributed systems** and **data engineering** in Python.

The core is stdlib-only, so it drops into any project — a notebook, a Lambda, a
Kubernetes worker or an Airflow task — without dragging in a dependency tree.
Optional extras add YAML config (`dsops[yaml]`) and Redis locks (`dsops[redis]`).

| Module | What it gives you |
| --- | --- |
| `dsops.resilience` | `retry` (sync + async, exponential backoff, full jitter), `CircuitBreaker`, `TokenBucket` rate limiter, `Bulkhead` |
| `dsops.connectors` | Hive-style partitioned datasets on disk (`dt=2026-11-01/…`): JSONL or CSV, optional gzip, staged writes with atomic commit, append / overwrite / error / ignore modes, partition-pruned reads |
| `dsops.config` | Typed dataclass config, layered: defaults → TOML/JSON/YAML files → env vars → overrides; `Secret` values that never get logged |
| `dsops.logs` | JSON logs with context (`job_id`, `run_id`, …) that follows work across threads and asyncio tasks |
| `dsops.metrics` | `Counter`, `Gauge`, `Histogram` with labels; Prometheus text output, no client library |
| `dsops.health` | Health checks (critical vs degraded, timeouts) and an ops server for `/livez`, `/readyz`, `/metrics` |
| `dsops.distributed` | `ConsistentHashRing`, `FileLock`, `RedisLock` (lease + token), `@idempotent` with in-memory or SQLite stores |
| `dsops.pipeline` | `DAG` runner (parallel, retries, checkpoint/resume, Mermaid graphs), `chunked`, `parallel_map`, `partition_by`, `dedupe`, schema and data-quality gates |
| `dsops` CLI | `dsops dag run/graph/list`, `dsops ring`, `dsops serve` |

## Install

From this repository:

```bash
pip install -e "devops-lib[dev]"          # local development
pip install "git+https://github.com/yassineh12/Data-science.git#subdirectory=devops-lib"   # in another project
```

Requires Python 3.11+.

## Quick tour

### A resumable pipeline with a data-quality gate

```python
from dsops import DAG, Field, Schema, validate_records
from dsops.pipeline import in_range

dag = DAG("daily_orders")

@dag.task(retries=3, retry_delay=2)
def extract(ctx):
    return fetch_orders(ctx.params["date"])          # retried on failure

@dag.task(depends_on=[extract])
def validate(extract):
    schema = Schema([Field("order_id", int, nullable=False, unique=True),
                     Field("amount", float, checks=[in_range(0, 1e6)])])
    report = validate_records(extract, schema, min_rows=1)
    report.raise_if(max_error_rate=0.01)             # stop bad data here
    return report.valid

@dag.task(depends_on=[validate])
def load(validate):
    warehouse.write("orders", validate)

result = dag.run(params={"date": "2026-11-01"}, run_id="2026-11-01",
                 checkpoint_dir=".checkpoints", max_workers=4)
print(result.summary())
result.raise_for_failures()
```

Tasks receive upstream results as keyword arguments named after the upstream
task, and get a `TaskContext` if they ask for `ctx`. Independent branches run in
parallel. Re-running with the same `run_id` and `checkpoint_dir` skips tasks that
already succeeded, so a crash at step 7 resumes at step 7. When a task fails, its
downstream tasks are marked `upstream_failed` and unrelated branches carry on
(use `fail_fast=True` to stop everything instead).

From the shell:

```bash
dsops dag run  pipelines/orders.py:dag --param date='"2026-11-01"' --run-id 2026-11-01 --checkpoint-dir .ckpt
dsops dag run  my_pkg.pipelines:dag --only load            # one task plus everything it needs
dsops dag graph pipelines/orders.py:dag                     # paste into any Mermaid renderer
```

### Partitioned datasets on disk

```python
from dsops import PartitionedWriter, read_partitioned

with PartitionedWriter("lake/orders", partition_by=["dt", "country"],
                       compression="gzip", mode="overwrite") as w:
    for batch in extract_batches():
        w.write(batch)            # buffered per partition, rolled every 100k rows

print(w.result.rows, w.result.partitions)
# lake/orders/dt=2026-11-01/country=FR/part-<write_id>-00000.jsonl.gz
# lake/orders/_manifests/<write_id>.json   <- files, rows and bytes for this write

for row in read_partitioned("lake/orders", filters={"dt": "2026-11-01", "country": ["FR", "US"]}):
    ...                           # only matching directories are opened
```

Files are written to `_staging/` and moved into place only on commit, so readers
never see a half-written partition, and an exception inside the `with` block
leaves the dataset as it was. `mode="overwrite"` replaces only the partitions
this write touches, which makes daily re-runs idempotent. Partition keys can be
computed: `partition_by={"dt": lambda r: r["event_time"][:10]}`.

Limits: each partition is committed atomically, but a write spanning several
partitions is not one transaction. If a crash hits mid-commit, replaced data
sits in `_trash/<write_id>/` for recovery. CSV reads every value back as a string.

### Calling things that fail

```python
from dsops import retry, CircuitBreaker, TokenBucket

breaker = CircuitBreaker("payments-api", failure_threshold=5, recovery_timeout=30)
limiter = TokenBucket(rate=20, capacity=40)          # 20 req/s, bursts of 40

@retry(max_attempts=5, base_delay=0.2, retry_on=(ConnectionError, TimeoutError))
@breaker
@limiter.limit
def charge(order):
    return http.post("/charges", json=order)
```

Order matters: the breaker sits inside `retry`, so each attempt counts as one
failure, and once the circuit opens `retry` stops at once (because
`CircuitOpenError` is not in `retry_on`).

### Configuration

```python
from dataclasses import dataclass, field
from dsops import load_config, Secret

@dataclass
class DB:
    host: str = "localhost"
    port: int = 5432
    password: Secret = field(default_factory=lambda: Secret(""))

@dataclass
class Settings:
    db: DB = field(default_factory=DB)
    workers: int = 4
    kafka_brokers: list[str] = field(default_factory=list)

settings = load_config(Settings, files=["config.toml", "config.prod.toml"],
                       env_prefix="APP_", optional_files=True)
# APP_DB__HOST=db.internal  APP_KAFKA_BROKERS=b1:9092,b2:9092  APP_DB__PASSWORD=...
```

Unknown keys and bad types raise `ConfigError` with the full path
(`db.port: expected int, got 'abc'`) at start-up, not three hours into a job.

### Coordination between workers

```python
from dsops import ConsistentHashRing, FileLock, SQLiteIdempotencyStore, idempotent
from dsops.distributed import RedisLock

ring = ConsistentHashRing(["worker-0", "worker-1", "worker-2"])
if ring.get_node(f"partition:{p}") == MY_POD_NAME:
    process(p)                           # each partition has exactly one owner

with FileLock("/var/run/nightly.lock", timeout=0):   # one run per host
    run_nightly()

with RedisLock(redis.Redis(), "nightly", ttl=60):    # one run across hosts
    run_nightly()

store = SQLiteIdempotencyStore("/data/processed.db")

@idempotent(store, key=lambda msg: msg["id"], ttl=7 * 86400)
def handle(msg):                          # safe under at-least-once delivery
    ...
```

### Observability

```python
from dsops import configure_logging, get_logger, log_context, Counter, Histogram, HealthRegistry, serve_ops

configure_logging("INFO", service="ingest")          # JSON lines on stderr
log = get_logger(__name__)

rows = Counter("ingest_rows_total", "Rows ingested", labelnames=("source",))
latency = Histogram("ingest_batch_seconds", "Batch latency")

health = HealthRegistry()
health.register("postgres", lambda: db.execute("select 1"))
health.register("cache", redis_ping, critical=False)  # failure = degraded, not down
serve_ops(9100, health=health)                        # /livez /readyz /metrics

with log_context(batch_id=batch.id), latency.time():
    rows.labels(source="s3").inc(len(batch))
    log.info("batch done", extra={"rows": len(batch)})
```

Pass `metrics=registry` to `DAG.run` to record `dsops_task_runs_total` and
`dsops_task_duration_seconds` for every task.

## Deploying

`templates/` holds starting points you can copy into a project:

- `Dockerfile` — multi-stage, non-root, health-checked
- `docker-compose.yml` + `prometheus.yml` — app, Redis and Prometheus locally
- `k8s/deployment.yaml` — worker with liveness/readiness probes and scrape annotations
- `k8s/cronjob.yaml` — scheduled batch run that resumes from checkpoints on retry

CI for this library lives in `.github/workflows/dsops.yml` at the repo root
(tests on Python 3.11–3.13, runs the example pipeline, builds the wheel).

## Example

`examples/etl_pipeline.py` wires everything together: config from env, a flaky
API behind retry + circuit breaker, parallel extraction, a quality gate,
hash-ring sharded idempotent loads, and Prometheus metrics.

```bash
python examples/etl_pipeline.py
ETL_SOURCE__FAILURE_RATE=0.5 dsops dag run examples/etl_pipeline.py:dag --param rows=1000
```

## Limits, stated plainly

- The DAG runner uses threads. CPU-heavy work belongs in `parallel_map(..., mode="process")`
  inside a task, or in Spark/Dask. Task timeouts are not enforced (Python threads cannot be killed safely).
- Checkpoints are pickles: fine for lists, dicts and DataFrames; write large outputs to storage and
  return the path instead.
- `RedisLock` is a single-instance lease lock. It prevents duplicate work. It does not give strict
  mutual exclusion through network partitions — use fencing tokens or a consensus store for that.
- `FileLock` is POSIX-only and reliable on local disks; NFS behaviour depends on the mount.

## Development

```bash
cd devops-lib
pip install -e ".[dev]"
pytest -q
```
