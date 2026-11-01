import datetime as dt
import json
from pathlib import Path

import pytest

from dsops.connectors import (
    DatasetError,
    PartitionedWriter,
    list_partitions,
    parse_partition_path,
    partition_path,
    read_partitioned,
    write_partitioned,
)

ROWS = [
    {"id": 1, "dt": "2026-11-01", "country": "FR", "amount": 10.5},
    {"id": 2, "dt": "2026-11-01", "country": "US", "amount": 3},
    {"id": 3, "dt": "2026-11-02", "country": "FR", "amount": 7},
    {"id": 4, "dt": "2026-11-02", "country": "FR", "amount": 1},
]


def files_under(root: Path) -> list[str]:
    return sorted(str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()
                  and not p.relative_to(root).parts[0].startswith("_"))


def test_hive_layout_and_round_trip(tmp_path):
    res = write_partitioned(ROWS, tmp_path, partition_by=["dt", "country"])
    assert res.rows == 4
    assert res.partitions == ["dt=2026-11-01/country=FR", "dt=2026-11-01/country=US", "dt=2026-11-02/country=FR"]
    assert all(f.startswith("dt=") and f.endswith(".jsonl") for f in files_under(tmp_path))
    assert sorted(r["id"] for r in read_partitioned(tmp_path)) == [1, 2, 3, 4]
    # JSON types survive the trip
    assert next(r for r in read_partitioned(tmp_path) if r["id"] == 1)["amount"] == 10.5


def test_filters_prune_partitions(tmp_path):
    write_partitioned(ROWS, tmp_path, partition_by=["dt", "country"])
    assert {r["id"] for r in read_partitioned(tmp_path, filters={"dt": "2026-11-02"})} == {3, 4}
    assert {r["id"] for r in read_partitioned(tmp_path, filters={"country": ["US"]})} == {2}
    assert {r["id"] for r in read_partitioned(tmp_path, filters=lambda p: p.get("country") != "FR")} == {2}
    assert list_partitions(tmp_path, filters={"dt": "2026-11-01"}) == [
        {"dt": "2026-11-01", "country": "FR"}, {"dt": "2026-11-01", "country": "US"}]


def test_partition_columns_added_back_when_dropped_from_records(tmp_path):
    rows = [{"id": 1, "event_time": "2026-11-01T10:00:00"}]
    write_partitioned(rows, tmp_path, partition_by={"dt": lambda r: r["event_time"][:10]})
    (rec,) = read_partitioned(tmp_path)
    assert rec == {"id": 1, "event_time": "2026-11-01T10:00:00", "dt": "2026-11-01"}
    (rec,) = read_partitioned(tmp_path, add_partition_columns=False)
    assert "dt" not in rec


def test_append_adds_files(tmp_path):
    write_partitioned(ROWS[:2], tmp_path, partition_by=["dt"])
    write_partitioned(ROWS[:2], tmp_path, partition_by=["dt"])
    assert len(list(read_partitioned(tmp_path))) == 4
    assert len(files_under(tmp_path)) == 2


def test_overwrite_replaces_only_touched_partitions(tmp_path):
    write_partitioned(ROWS, tmp_path, partition_by=["dt"])
    write_partitioned([{"id": 99, "dt": "2026-11-02"}], tmp_path, partition_by=["dt"], mode="overwrite")
    by_dt = {}
    for r in read_partitioned(tmp_path):
        by_dt.setdefault(r["dt"], set()).add(r["id"])
    assert by_dt == {"2026-11-01": {1, 2}, "2026-11-02": {99}}
    assert not (tmp_path / "_trash").exists()


def test_overwrite_unpartitioned(tmp_path):
    write_partitioned(ROWS, tmp_path)
    write_partitioned([{"id": 5}], tmp_path, mode="overwrite")
    assert [r["id"] for r in read_partitioned(tmp_path)] == [5]


def test_error_and_ignore_modes(tmp_path):
    write_partitioned(ROWS[:1], tmp_path, partition_by=["dt"])
    with pytest.raises(DatasetError, match="already exist"):
        write_partitioned(ROWS, tmp_path, partition_by=["dt"], mode="error")
    assert len(list(read_partitioned(tmp_path))) == 1  # nothing changed

    res = write_partitioned(ROWS, tmp_path, partition_by=["dt"], mode="ignore")
    assert res.skipped == ["dt=2026-11-01"]
    assert sorted(r["id"] for r in read_partitioned(tmp_path)) == [1, 3, 4]


def test_exception_in_block_leaves_dataset_untouched(tmp_path):
    write_partitioned(ROWS[:1], tmp_path, partition_by=["dt"])
    before = files_under(tmp_path)
    with pytest.raises(RuntimeError):
        with PartitionedWriter(tmp_path, partition_by=["dt"], max_rows_per_file=1) as w:
            w.write(ROWS)  # files get flushed to staging...
            raise RuntimeError("upstream died")
    assert files_under(tmp_path) == before
    assert not (tmp_path / "_staging").exists()


def test_readers_never_see_staged_files(tmp_path):
    with PartitionedWriter(tmp_path, partition_by=["dt"], max_rows_per_file=1) as w:
        w.write(ROWS)
        assert (tmp_path / "_staging").exists()
        assert list(read_partitioned(tmp_path)) == []
    assert len(list(read_partitioned(tmp_path))) == 4


def test_file_rolling_and_manifest(tmp_path):
    res = write_partitioned(ROWS, tmp_path, partition_by=["dt"], max_rows_per_file=1, write_id="w1")
    assert len(res.files) == 4 and all(f.rows == 1 and f.bytes > 0 for f in res.files)
    manifest = json.loads((tmp_path / "_manifests" / "w1.json").read_text())
    assert manifest["rows"] == 4 and manifest["mode"] == "append" and len(manifest["files"]) == 4


@pytest.mark.parametrize("fmt,compression", [("csv", None), ("csv", "gzip"), ("jsonl", "gzip")])
def test_formats(tmp_path, fmt, compression):
    rows = [{"id": 1, "dt": "d1", "note": 'comma, "quote"'}, {"id": 2, "dt": "d1", "extra": "x"}]
    res = write_partitioned(rows, tmp_path, partition_by=["dt"], format=fmt, compression=compression)
    assert res.files[0].path.endswith(f".{fmt}" + (".gz" if compression else ""))
    out = sorted(read_partitioned(tmp_path), key=lambda r: str(r["id"]))
    assert out[0]["note"] == 'comma, "quote"'
    if fmt == "csv":
        assert out[0]["id"] == "1" and out[1]["extra"] == "x"  # CSV is untyped
    else:
        assert out[0]["id"] == 1


def test_awkward_partition_values_are_encoded(tmp_path):
    rows = [{"k": "a/b=c"}, {"k": None}, {"k": ""}, {"k": "_hidden"}, {"k": dt.date(2026, 11, 1)}]
    write_partitioned(rows, tmp_path, partition_by=["k"])
    dirs = sorted(p.name for p in tmp_path.iterdir() if not p.name.startswith("_"))
    assert "k=a%2Fb%3Dc" in dirs and "k=__HIVE_DEFAULT_PARTITION__" in dirs
    values = sorted(str(p["k"]) for p in list_partitions(tmp_path))
    assert values == ["2026-11-01", "None", "_hidden", "a/b=c"]


def test_partition_path_helpers():
    assert partition_path({"dt": "2026-11-01", "c": "FR"}) == "dt=2026-11-01/c=FR"
    assert parse_partition_path("dt=2026-11-01/c=FR/part-1.jsonl") == {"dt": "2026-11-01", "c": "FR"}


def test_validation_errors(tmp_path):
    with pytest.raises(ValueError):
        PartitionedWriter(tmp_path, format="parquet")
    with pytest.raises(ValueError):
        PartitionedWriter(tmp_path, partition_by=["bad key"])
    with pytest.raises(DatasetError, match="partition"):
        write_partitioned([{"id": 1}], tmp_path, partition_by=["dt"])
    w = PartitionedWriter(tmp_path)
    w.commit()
    with pytest.raises(DatasetError):
        w.write([{"a": 1}])
    with pytest.raises(DatasetError, match="does not exist"):
        list(read_partitioned(tmp_path / "missing"))


def test_works_as_a_dag_task(tmp_path):
    from dsops import DAG

    dag = DAG("lake")

    @dag.task
    def extract():
        return ROWS

    @dag.task(depends_on=[extract])
    def load(extract, ctx):
        res = write_partitioned(extract, tmp_path, partition_by=["dt"], mode="overwrite", write_id=ctx.run_id)
        return res.partitions  # return paths, not data

    out = dag.run(run_id="r1").raise_for_failures()
    assert out["load"] == ["dt=2026-11-01", "dt=2026-11-02"]
