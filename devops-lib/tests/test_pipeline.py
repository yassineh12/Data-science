import threading
import time
from pathlib import Path

import pytest

from dsops.metrics import Registry
from dsops.pipeline import (
    CACHED,
    CANCELLED,
    DAG,
    FAILED,
    SUCCESS,
    UPSTREAM_FAILED,
    CycleError,
    DAGError,
    DataQualityError,
    Field,
    PipelineFailed,
    Schema,
    chunked,
    dedupe,
    in_range,
    matches,
    one_of,
    parallel_map,
    partition_by,
    validate_records,
)


def make_etl(calls):
    dag = DAG("etl")

    @dag.task
    def extract(ctx):
        calls.append("extract")
        return list(range(ctx.params.get("n", 5)))

    @dag.task(depends_on=[extract])
    def double(extract):
        calls.append("double")
        return [x * 2 for x in extract]

    @dag.task(depends_on=["extract"])
    def count(extract):
        calls.append("count")
        return len(extract)

    @dag.task(depends_on=["double", "count"])
    def report(double, count):
        calls.append("report")
        return {"sum": sum(double), "n": count}

    return dag


# -------------------------------------------------------------------- DAG


def test_dag_passes_upstream_results_and_params():
    calls = []
    res = make_etl(calls).run(params={"n": 4})
    assert res.ok
    assert res["report"] == {"sum": 12, "n": 4}
    assert calls[0] == "extract" and calls[-1] == "report"


def test_independent_tasks_run_in_parallel():
    dag = DAG("par")
    barrier = threading.Barrier(3, timeout=2)

    for name in ("a", "b", "c"):
        dag.add_task(lambda: barrier.wait(), name=name)  # deadlocks unless all 3 run at once

    assert dag.run(max_workers=3).ok


def test_task_retries_then_succeeds():
    dag, attempts, sleeps = DAG("r"), [], []

    @dag.task(retries=2, retry_delay=0.5)
    def flaky(ctx):
        attempts.append(ctx.attempt)
        if ctx.attempt < 3:
            raise ConnectionError
        return "ok"

    res = dag.run(sleep=sleeps.append)
    assert res.ok and res.tasks["flaky"].attempts == 3
    assert attempts == [1, 2, 3] and sleeps == [0.5, 1.0]


def test_failure_marks_downstream_and_keeps_independent_branches():
    dag = DAG("f")
    dag.add_task(lambda: 1 / 0, name="bad")
    dag.add_task(lambda bad: bad, name="after_bad", depends_on=["bad"])
    dag.add_task(lambda after_bad: 1, name="grandchild", depends_on=["after_bad"])
    dag.add_task(lambda: "fine", name="independent")

    res = dag.run()
    assert not res.ok and res.failed() == ["bad"]
    assert res.tasks["after_bad"].status == UPSTREAM_FAILED
    assert res.tasks["grandchild"].status == UPSTREAM_FAILED
    assert res.tasks["independent"].status == SUCCESS
    assert isinstance(res.tasks["bad"].error, ZeroDivisionError)
    with pytest.raises(PipelineFailed, match="bad"):
        res.raise_for_failures()


def test_fail_fast_cancels_pending_work():
    dag = DAG("ff")
    dag.add_task(lambda: 1 / 0, name="bad")
    dag.add_task(lambda: time.sleep(0.1), name="slow")
    dag.add_task(lambda slow: 1, name="after_slow", depends_on=["slow"])
    res = dag.run(max_workers=2, fail_fast=True)
    assert res.tasks["bad"].status == FAILED
    assert res.tasks["after_slow"].status == CANCELLED


def test_checkpoints_resume_after_failure(tmp_path: Path):
    calls, should_fail = [], [True]
    dag = DAG("resume")

    @dag.task
    def expensive():
        calls.append("expensive")
        return [1, 2, 3]

    @dag.task(depends_on=["expensive"])
    def load(expensive):
        calls.append("load")
        if should_fail[0]:
            raise IOError("warehouse down")
        return sum(expensive)

    first = dag.run(run_id="2026-11-01", checkpoint_dir=tmp_path)
    assert not first.ok
    should_fail[0] = False
    second = dag.run(run_id="2026-11-01", checkpoint_dir=tmp_path)
    assert second.ok and second["load"] == 6
    assert second.tasks["expensive"].status == CACHED
    assert calls == ["expensive", "load", "load"]  # expensive ran once


def test_cycle_and_unknown_dependency_detection():
    dag = DAG("cyc")
    dag.add_task(lambda: 1, name="a", depends_on=["b"])
    dag.add_task(lambda: 1, name="b", depends_on=["a"])
    with pytest.raises(CycleError):
        dag.validate()

    dag2 = DAG("missing")
    dag2.add_task(lambda: 1, name="a", depends_on=["ghost"])
    with pytest.raises(DAGError, match="ghost"):
        dag2.run()

    with pytest.raises(DAGError, match="already defined"):
        dag2.add_task(lambda: 1, name="a")


def test_subset_mermaid_and_metrics():
    calls = []
    dag = make_etl(calls)
    sub = dag.subset(["count"])
    assert set(sub.tasks) == {"extract", "count"}
    assert sub.run().ok and calls == ["extract", "count"]

    mermaid = dag.to_mermaid()
    assert mermaid.startswith("flowchart LR") and "extract --> double" in mermaid

    reg = Registry()
    dag.run(metrics=reg)
    dag.run(metrics=reg)  # instruments reused, not re-registered
    text = reg.render()
    assert 'dsops_task_runs_total{dag="etl",task="report",status="success"} 2' in text
    assert "dsops_task_duration_seconds_bucket" in text


# --------------------------------------------------------------- batching


def test_chunked_lazy_and_exact():
    assert list(chunked(range(7), 3)) == [[0, 1, 2], [3, 4, 5], [6]]
    import itertools
    assert next(chunked(itertools.count(), 2)) == [0, 1]
    with pytest.raises(ValueError):
        list(chunked([1], 0))


def test_parallel_map_ordered_and_errors():
    assert parallel_map(lambda x: x * x, range(10), workers=4) == [x * x for x in range(10)]

    def maybe(x):
        if x == 3:
            raise ValueError(x)
        return x

    with pytest.raises(ValueError):
        parallel_map(maybe, range(5))
    res = parallel_map(maybe, range(5), return_exceptions=True)
    assert res.results == [0, 1, 2, None, 4]
    assert [(i, item) for i, item, _ in res.errors] == [(3, 3)]


def _square(x):
    return x * x


def test_parallel_map_processes():
    assert parallel_map(_square, [1, 2, 3], mode="process", workers=2) == [1, 4, 9]


def test_partition_and_dedupe():
    rows = [{"id": 1, "d": "a", "v": 1}, {"id": 2, "d": "b", "v": 1}, {"id": 1, "d": "a", "v": 2}]
    assert {k: len(v) for k, v in partition_by(rows, lambda r: r["d"]).items()} == {"a": 2, "b": 1}
    assert dedupe(rows, lambda r: r["id"]) == [rows[2], rows[1]]
    assert dedupe(rows, lambda r: r["id"], keep="first") == [rows[0], rows[1]]


# ---------------------------------------------------------------- quality


SCHEMA = Schema([
    Field("id", int, nullable=False, unique=True),
    Field("email", str, checks=[matches(r"^[^@]+@[^@]+$")]),
    Field("amount", float, checks=[in_range(0, 100)]),
    Field("status", str, required=False, checks=[one_of({"new", "paid"})]),
])


def test_validate_records_splits_valid_and_invalid():
    rows = [
        {"id": 1, "email": "a@x.io", "amount": 10, "status": "new"},
        {"id": 2, "email": "nope", "amount": 5.5},
        {"id": 1, "email": "b@x.io", "amount": 1},            # duplicate id
        {"id": None, "email": "c@x.io", "amount": 1},         # null id
        {"id": 5, "email": "d@x.io", "amount": 500},          # out of range
        {"id": True, "email": "e@x.io", "amount": 1},         # bool is not int
        {"id": 7, "email": "f@x.io", "amount": 1, "status": "lost"},
        {"id": 8, "amount": 1},                               # missing email
    ]
    report = validate_records(rows, SCHEMA)
    assert [r["id"] for r in report.valid] == [1]
    errs = {e.index: e.errors for e in report.invalid}
    assert "does not match" in errs[1]["email"]
    assert errs[2]["id"] == "duplicate of row 0"
    assert errs[3]["id"] == "null not allowed"
    assert "> 100" in errs[4]["amount"]
    assert "expected int" in errs[5]["id"]
    assert "not in" in errs[6]["status"]
    assert errs[7]["email"] == "missing"
    assert report.error_rate == pytest.approx(7 / 8)
    with pytest.raises(DataQualityError):
        report.raise_if(max_error_rate=0.5)


def test_dataset_level_gates_and_coercion():
    schema = Schema([Field("n", int, coerce=True), Field("x", str, nullable=True)], allow_extra=False)
    report = validate_records([{"n": "3", "x": None}, {"n": "4", "x": None, "extra": 1}], schema,
                              min_rows=5, max_null_rate={"x": 0.5})
    assert report.valid == [{"n": 3, "x": None}]
    assert report.invalid[0].errors == {"extra": "unexpected field"}
    assert any("min_rows" in e for e in report.dataset_errors)
    assert any("null rate" in e for e in report.dataset_errors)
    with pytest.raises(DataQualityError):
        report.raise_if(max_error_rate=1.0)


def test_accepts_dataframe_like_objects():
    class FakeDF:
        def to_dict(self, orient):
            assert orient == "records"
            return [{"id": 1, "email": "a@b.c", "amount": 1.0}]

    assert validate_records(FakeDF(), SCHEMA).passed
