"""A small, dependency-free DAG runner for data pipelines.

Tasks are plain functions. A task receives the results of its upstream tasks as
keyword arguments named after them, and may ask for a ``ctx`` argument to get a
:class:`TaskContext` (run id, params, attempt number, logger).

::

    dag = DAG("daily_orders")

    @dag.task(retries=3)
    def extract(ctx):
        return fetch_orders(ctx.params["date"])

    @dag.task(depends_on=["extract"])
    def clean(extract):
        return [r for r in extract if r["amount"] > 0]

    @dag.task(depends_on=["clean"])
    def load(clean):
        warehouse.write("orders", clean)

    result = dag.run(params={"date": "2026-11-01"}, max_workers=4,
                     run_id="2026-11-01", checkpoint_dir=".checkpoints")
    result.raise_for_failures()

Independent tasks run in parallel on a thread pool. With ``checkpoint_dir`` and a
stable ``run_id``, a re-run skips tasks that already succeeded — so a pipeline
that died at step 7 of 9 resumes at step 7.
"""

from __future__ import annotations

import inspect
import logging
import os
import pickle
import tempfile
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from ..errors import DsopsError
from ..logs import copy_context_to, log_context
from ..metrics import Counter, Histogram, Registry
from ..resilience import RetryPolicy

log = logging.getLogger(__name__)

SUCCESS = "success"
FAILED = "failed"
CACHED = "cached"
UPSTREAM_FAILED = "upstream_failed"
CANCELLED = "cancelled"


class DAGError(DsopsError):
    pass


class CycleError(DAGError):
    pass


class PipelineFailed(DAGError):
    def __init__(self, result: "RunResult"):
        failed = ", ".join(f"{n} ({r.error!r})" for n, r in result.tasks.items() if r.status == FAILED)
        super().__init__(f"DAG {result.dag!r} run {result.run_id!r} failed: {failed}")
        self.result = result


@dataclass
class Task:
    name: str
    fn: Callable[..., Any]
    depends_on: tuple[str, ...] = ()
    retries: int = 0
    retry_delay: float = 1.0
    retry_on: tuple[type[BaseException], ...] = (Exception,)
    checkpoint: bool = True
    tags: tuple[str, ...] = ()

    def accepted_args(self) -> tuple[set[str], bool]:
        params = inspect.signature(self.fn).parameters.values()
        names = {p.name for p in params if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)}
        var_kw = any(p.kind == p.VAR_KEYWORD for p in params)
        return names, var_kw


@dataclass
class TaskContext:
    dag: str
    task: str
    run_id: str
    attempt: int
    params: dict[str, Any]
    log: logging.Logger


@dataclass
class TaskResult:
    name: str
    status: str
    attempts: int = 0
    duration: float = 0.0
    error: BaseException | None = None
    value: Any = field(default=None, repr=False)


@dataclass
class RunResult:
    dag: str
    run_id: str
    tasks: dict[str, TaskResult]
    duration: float

    @property
    def ok(self) -> bool:
        return all(r.status in (SUCCESS, CACHED) for r in self.tasks.values())

    def __getitem__(self, task: str) -> Any:
        """Value returned by ``task``."""
        return self.tasks[task].value

    def failed(self) -> list[str]:
        return [n for n, r in self.tasks.items() if r.status == FAILED]

    def raise_for_failures(self) -> "RunResult":
        if not self.ok:
            raise PipelineFailed(self)
        return self

    def summary(self) -> str:
        width = max((len(n) for n in self.tasks), default=4)
        lines = [f"DAG {self.dag} run {self.run_id}: {'OK' if self.ok else 'FAILED'} in {self.duration:.2f}s"]
        for n, r in self.tasks.items():
            err = f"  {type(r.error).__name__}: {r.error}" if r.error else ""
            lines.append(f"  {n:<{width}}  {r.status:<15} attempts={r.attempts} {r.duration:7.3f}s{err}")
        return "\n".join(lines)


class DAG:
    def __init__(self, name: str, *, default_retries: int = 0, default_retry_delay: float = 1.0):
        self.name = name
        self.default_retries = default_retries
        self.default_retry_delay = default_retry_delay
        self.tasks: dict[str, Task] = {}

    # ------------------------------------------------------------------ build

    def add_task(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        depends_on: Iterable[str | Callable[..., Any]] = (),
        retries: int | None = None,
        retry_delay: float | None = None,
        retry_on: tuple[type[BaseException], ...] = (Exception,),
        checkpoint: bool = True,
        tags: Iterable[str] = (),
    ) -> Task:
        task_name = name or fn.__name__
        if task_name in self.tasks:
            raise DAGError(f"task {task_name!r} already defined in DAG {self.name!r}")
        deps = tuple(d if isinstance(d, str) else getattr(d, "__dsops_task__", d.__name__) for d in depends_on)
        task = Task(
            name=task_name, fn=fn, depends_on=deps,
            retries=self.default_retries if retries is None else retries,
            retry_delay=self.default_retry_delay if retry_delay is None else retry_delay,
            retry_on=retry_on, checkpoint=checkpoint, tags=tuple(tags),
        )
        self.tasks[task_name] = task
        return task

    def task(self, _fn: Callable[..., Any] | None = None, **kwargs: Any) -> Any:
        """Decorator form of :meth:`add_task`. ``depends_on`` may list names or decorated functions."""

        def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
            t = self.add_task(fn, **kwargs)
            fn.__dsops_task__ = t.name  # type: ignore[attr-defined]
            return fn

        return deco(_fn) if _fn is not None else deco

    # --------------------------------------------------------------- inspect

    def validate(self) -> list[str]:
        """Check references and cycles; return tasks in a valid execution order."""
        for t in self.tasks.values():
            missing = [d for d in t.depends_on if d not in self.tasks]
            if missing:
                raise DAGError(f"task {t.name!r} depends on unknown task(s) {missing}")
        indeg = {n: len(t.depends_on) for n, t in self.tasks.items()}
        children = self._children()
        ready = [n for n, d in indeg.items() if d == 0]
        order: list[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for c in children[n]:
                indeg[c] -= 1
                if indeg[c] == 0:
                    ready.append(c)
        if len(order) != len(self.tasks):
            stuck = sorted(n for n, d in indeg.items() if d > 0)
            raise CycleError(f"cycle detected among tasks {stuck}")
        return order

    def _children(self) -> dict[str, list[str]]:
        children: dict[str, list[str]] = {n: [] for n in self.tasks}
        for t in self.tasks.values():
            for d in t.depends_on:
                children[d].append(t.name)
        return children

    def downstream(self, name: str) -> set[str]:
        children, seen, stack = self._children(), set(), [name]
        while stack:
            for c in children[stack.pop()]:
                if c not in seen:
                    seen.add(c)
                    stack.append(c)
        return seen

    def upstream(self, name: str) -> set[str]:
        seen, stack = set(), [name]
        while stack:
            for d in self.tasks[stack.pop()].depends_on:
                if d not in seen:
                    seen.add(d)
                    stack.append(d)
        return seen

    def subset(self, targets: Iterable[str], *, include_upstream: bool = True) -> "DAG":
        """A new DAG containing ``targets`` (and, by default, everything they need)."""
        keep = set(targets)
        unknown = keep - set(self.tasks)
        if unknown:
            raise DAGError(f"unknown task(s) {sorted(unknown)}")
        if include_upstream:
            for t in list(keep):
                keep |= self.upstream(t)
        sub = DAG(self.name, default_retries=self.default_retries, default_retry_delay=self.default_retry_delay)
        for n in self.validate():
            if n in keep:
                t = self.tasks[n]
                sub.tasks[n] = Task(**{**t.__dict__, "depends_on": tuple(d for d in t.depends_on if d in keep)})
        return sub

    def to_mermaid(self) -> str:
        lines = ["flowchart LR"]
        for n in self.validate():
            t = self.tasks[n]
            lines.append(f"    {n}[{n}]")
            for d in t.depends_on:
                lines.append(f"    {d} --> {n}")
        return "\n".join(lines)

    # ------------------------------------------------------------------- run

    def run(
        self,
        *,
        params: dict[str, Any] | None = None,
        run_id: str | None = None,
        max_workers: int = 4,
        checkpoint_dir: str | os.PathLike[str] | None = None,
        fail_fast: bool = False,
        metrics: Registry | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> RunResult:
        order = self.validate()
        run_id = run_id or uuid.uuid4().hex[:12]
        params = dict(params or {})
        ckpt = _Checkpoints(Path(checkpoint_dir) / self.name / run_id) if checkpoint_dir else None
        instruments = _instruments(metrics) if metrics is not None else None

        results: dict[str, TaskResult] = {}
        remaining = {n: set(self.tasks[n].depends_on) for n in order}
        children = self._children()
        started = time.perf_counter()

        def finish(res: TaskResult) -> None:
            results[res.name] = res
            remaining.pop(res.name, None)
            if instruments:
                instruments[0].labels(self.name, res.name, res.status).inc()
                if res.status in (SUCCESS, FAILED):
                    instruments[1].labels(self.name, res.name).observe(res.duration)
            if res.status in (FAILED, UPSTREAM_FAILED, CANCELLED):
                for d in self.downstream(res.name):
                    if d not in results:
                        results[d] = TaskResult(d, UPSTREAM_FAILED)
                        remaining.pop(d, None)
                        if instruments:
                            instruments[0].labels(self.name, d, UPSTREAM_FAILED).inc()
            else:
                for c in children[res.name]:
                    if c in remaining:
                        remaining[c].discard(res.name)

        with log_context(dag=self.name, run_id=run_id):
            log.info("starting DAG run with %d task(s)", len(order))
            with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"dag-{self.name}") as pool:
                inflight: dict[Future[TaskResult], str] = {}
                stop = False
                while remaining or inflight:
                    progressed = False
                    if not stop:
                        running = set(inflight.values())
                        for name in [n for n, deps in remaining.items() if not deps and n not in running]:
                            progressed = True
                            task = self.tasks[name]
                            if ckpt and task.checkpoint and ckpt.has(name):
                                log.info("task %s restored from checkpoint", name)
                                finish(TaskResult(name, CACHED, value=ckpt.load(name)))
                                continue
                            upstream = {d: results[d].value for d in task.depends_on}
                            runner = copy_context_to(self._execute)
                            fut = pool.submit(runner, task, upstream, params, run_id, ckpt, sleep)
                            inflight[fut] = name
                    if not inflight:
                        if progressed and remaining and not stop:
                            continue  # tasks restored from checkpoints unblocked more work
                        break
                    done, _ = wait(inflight, return_when=FIRST_COMPLETED)
                    for fut in done:
                        inflight.pop(fut)
                        res = fut.result()
                        finish(res)
                        if res.status == FAILED and fail_fast:
                            stop = True
                for name in list(remaining):
                    finish(TaskResult(name, CANCELLED))

            ordered = {n: results[n] for n in order}
            result = RunResult(self.name, run_id, ordered, time.perf_counter() - started)
            (log.info if result.ok else log.error)("DAG run finished: %s", "ok" if result.ok else "failed")
            return result

    def _execute(
        self,
        task: Task,
        upstream: dict[str, Any],
        params: dict[str, Any],
        run_id: str,
        ckpt: "_Checkpoints | None",
        sleep: Callable[[float], None],
    ) -> TaskResult:
        accepted, var_kw = task.accepted_args()
        kwargs = {k: v for k, v in upstream.items() if var_kw or k in accepted}
        policy = RetryPolicy(max_attempts=task.retries + 1, base_delay=task.retry_delay,
                             retry_on=task.retry_on, jitter="none")
        task_log = logging.getLogger(f"{__name__}.{self.name}.{task.name}")
        start = time.perf_counter()
        with log_context(task=task.name):
            for attempt in range(1, policy.max_attempts + 1):
                if "ctx" in accepted:
                    kwargs["ctx"] = TaskContext(self.name, task.name, run_id, attempt, params, task_log)
                try:
                    value = task.fn(**kwargs)
                except Exception as exc:  # noqa: BLE001 - recorded in the result
                    if attempt < policy.max_attempts and policy.should_retry(exc):
                        delay = policy.backoff(attempt)
                        log.warning("task %s attempt %d failed: %r; retrying in %.2fs", task.name, attempt, exc, delay)
                        sleep(delay)
                        continue
                    log.exception("task %s failed after %d attempt(s)", task.name, attempt)
                    return TaskResult(task.name, FAILED, attempt, time.perf_counter() - start, exc)
                if ckpt and task.checkpoint:
                    ckpt.save(task.name, value)
                duration = time.perf_counter() - start
                log.info("task %s succeeded in %.3fs", task.name, duration)
                return TaskResult(task.name, SUCCESS, attempt, duration, value=value)
        raise AssertionError("unreachable")


def _instruments(reg: Registry) -> tuple[Counter, Histogram]:
    runs = reg.get("dsops_task_runs_total") or Counter(
        "dsops_task_runs_total", "DAG task runs by final status",
        labelnames=("dag", "task", "status"), registry=reg)
    dur = reg.get("dsops_task_duration_seconds") or Histogram(
        "dsops_task_duration_seconds", "DAG task wall time including retries",
        labelnames=("dag", "task"), registry=reg)
    return runs, dur  # type: ignore[return-value]


class _Checkpoints:
    """Pickled task outputs, written atomically so a crash never leaves a half-written file."""

    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True)

    def _path(self, name: str) -> Path:
        return self.root / f"{name}.pkl"

    def has(self, name: str) -> bool:
        return self._path(name).exists()

    def load(self, name: str) -> Any:
        with self._path(name).open("rb") as fh:
            return pickle.load(fh)

    def save(self, name: str, value: Any) -> None:
        fd, tmp = tempfile.mkstemp(dir=self.root, prefix=f".{name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as fh:
                pickle.dump(value, fh)
            os.replace(tmp, self._path(name))
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise
