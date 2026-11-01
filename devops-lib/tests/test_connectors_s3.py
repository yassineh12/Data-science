"""The partitioned dataset API on S3, against moto's in-memory S3."""

import contextlib

import pytest

boto3 = pytest.importorskip("boto3")
moto = pytest.importorskip("moto")

from dsops.connectors import (  # noqa: E402
    DatasetError,
    LocalStorage,
    PartitionedWriter,
    S3Storage,
    list_partitions,
    open_storage,
    read_partitioned,
    write_partitioned,
)

ROWS = [
    {"id": 1, "dt": "2026-11-01", "country": "FR"},
    {"id": 2, "dt": "2026-11-01", "country": "US"},
    {"id": 3, "dt": "2026-11-02", "country": "FR"},
]


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with moto.mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="lake")
        yield client


@pytest.fixture
def store(s3):
    return S3Storage("lake", "warehouse/orders", client=s3)


def keys(s3, prefix=""):
    resp = s3.list_objects_v2(Bucket="lake", Prefix=prefix)
    return sorted(o["Key"] for o in resp.get("Contents", []))


def test_round_trip_and_layout(s3, store):
    res = write_partitioned(ROWS, store, partition_by=["dt", "country"], compression="gzip", write_id="w1")
    assert res.root == "s3://lake/warehouse/orders" and res.rows == 3
    assert keys(s3) == [
        "warehouse/orders/_manifests/w1.json",
        "warehouse/orders/dt=2026-11-01/country=FR/part-w1-00000.jsonl.gz",
        "warehouse/orders/dt=2026-11-01/country=US/part-w1-00000.jsonl.gz",
        "warehouse/orders/dt=2026-11-02/country=FR/part-w1-00000.jsonl.gz",
    ]  # no _staging left behind
    assert sorted(r["id"] for r in read_partitioned(store)) == [1, 2, 3]
    assert [r["id"] for r in read_partitioned(store, filters={"dt": "2026-11-02"})] == [3]
    assert list_partitions(store, filters={"country": "US"}) == [{"dt": "2026-11-01", "country": "US"}]


def test_filters_avoid_listing_pruned_prefixes(s3, store):
    write_partitioned(ROWS, store, partition_by=["dt", "country"])
    listed = []
    original = store.list_dir

    def spy(prefix):
        listed.append(prefix)
        return original(prefix)

    store.list_dir = spy
    list(read_partitioned(store, filters={"dt": "2026-11-02"}))
    assert not any(p.startswith("dt=2026-11-01") for p in listed)


def test_overwrite_replaces_only_touched_partitions(s3, store):
    write_partitioned(ROWS, store, partition_by=["dt"], write_id="w1")
    write_partitioned([{"id": 9, "dt": "2026-11-02"}], store, partition_by=["dt"], mode="overwrite", write_id="w2")
    assert sorted(r["id"] for r in read_partitioned(store)) == [1, 2, 9]
    assert not keys(s3, "warehouse/orders/dt=2026-11-02/part-w1")  # old file deleted
    assert not keys(s3, "warehouse/orders/_trash")


def test_overwrite_with_same_write_id_keeps_new_files(s3, store):
    for _ in range(2):  # a retried run reuses its run id as write_id
        write_partitioned(ROWS, store, partition_by=["dt"], mode="overwrite", write_id="run-1")
    assert sorted(r["id"] for r in read_partitioned(store)) == [1, 2, 3]


def test_failed_write_leaves_bucket_untouched(s3, store):
    write_partitioned(ROWS[:1], store, partition_by=["dt"], write_id="w1")
    before = keys(s3)
    with pytest.raises(RuntimeError):
        with PartitionedWriter(store, partition_by=["dt"], max_rows_per_file=1) as w:
            w.write(ROWS)  # staged objects uploaded...
            assert keys(s3, "warehouse/orders/_staging")
            assert sorted(r["id"] for r in read_partitioned(store)) == [1]  # ...but invisible
            raise RuntimeError("upstream died")
    assert keys(s3) == before


def test_error_and_ignore_modes(s3, store):
    write_partitioned(ROWS[:1], store, partition_by=["dt"])
    with pytest.raises(DatasetError):
        write_partitioned(ROWS, store, partition_by=["dt"], mode="error")
    res = write_partitioned(ROWS, store, partition_by=["dt"], mode="ignore")
    assert res.skipped == ["dt=2026-11-01"]


def test_custom_lock_is_used(s3, store):
    events = []

    @contextlib.contextmanager
    def lock():
        events.append("acquire")
        yield
        events.append("release")

    write_partitioned(ROWS, store, partition_by=["dt"], lock=lock())
    assert events == ["acquire", "release"]


def test_open_storage_and_url_roots(s3, monkeypatch, tmp_path):
    monkeypatch.setattr(boto3, "client", lambda *a, **k: s3)  # so the URL form uses the mocked client
    write_partitioned(ROWS, "s3://lake/via-url", partition_by=["dt"])
    assert len(list(read_partitioned("s3://lake/via-url"))) == 3
    assert isinstance(open_storage(tmp_path), LocalStorage)
    with pytest.raises(ValueError):
        open_storage("s3://")
    with pytest.raises(DatasetError, match="does not exist"):
        list(read_partitioned("s3://lake/nothing-here"))


def test_delete_prefix_batches_over_1000_keys(s3):
    st = S3Storage("lake", "big", client=s3)
    for i in range(1_005):
        st.write_bytes(f"_staging/x/{i}.jsonl", b"{}\n")
    st.delete_prefix("_staging")
    assert keys(s3, "big/") == []
    with pytest.raises(ValueError):
        st.delete_prefix("")
