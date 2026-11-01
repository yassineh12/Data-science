"""SQL connector tests.

SQLite runs always. Postgres runs when DSOPS_TEST_POSTGRES_DSN is set, e.g.
``DSOPS_TEST_POSTGRES_DSN=postgresql://postgres@127.0.0.1:5432/postgres pytest``.
"""

import os
import threading
import uuid

import pytest

from dsops.connectors.sql import POSTGRES, SQLITE, Database, SQLError, quote_ident

PG_DSN = os.environ.get("DSOPS_TEST_POSTGRES_DSN")


@pytest.fixture(params=["sqlite", "postgres"])
def db(request, tmp_path):
    if request.param == "sqlite":
        database = Database.sqlite(str(tmp_path / "test.db"))
    else:
        if not PG_DSN:
            pytest.skip("set DSOPS_TEST_POSTGRES_DSN to run Postgres tests")
        pytest.importorskip("psycopg")
        database = Database.postgres(PG_DSN)
    yield database
    database.close()


@pytest.fixture
def table(db):
    name = f"t_{uuid.uuid4().hex[:10]}"
    db.execute(f"CREATE TABLE {name} (id INTEGER PRIMARY KEY, dt TEXT, name TEXT, amount REAL)")
    yield name
    db.execute(f"DROP TABLE IF EXISTS {name}")


def rows(n, dt="2026-11-01", start=0):
    return [{"id": i, "dt": dt, "name": f"n{i}", "amount": float(i)} for i in range(start, start + n)]


def count(db, table, where=""):
    return db.scalar(f"SELECT COUNT(*) FROM {table} {where}")


# ------------------------------------------------------------------ writes


def test_append_in_batches(db, table):
    stats = db.write_rows(table, iter(rows(2_500)), batch_size=1_000)
    assert (stats.rows, stats.batches) == (2_500, 3)
    assert count(db, table) == 2_500


def test_write_is_all_or_nothing(db, table):
    bad = rows(10) + [{"id": 3, "dt": "x", "name": "dup", "amount": 0}]  # primary key violation
    with pytest.raises(Exception):
        db.write_rows(table, bad, batch_size=4)
    assert count(db, table) == 0
    db.write_rows(table, rows(2))  # connection still usable after the rollback
    assert count(db, table) == 2


def test_upsert_merges_by_key(db, table):
    db.write_rows(table, rows(3))
    changed = [{"id": 1, "dt": "2026-11-01", "name": "updated", "amount": 99.0},
               {"id": 7, "dt": "2026-11-01", "name": "new", "amount": 7.0}]
    db.write_rows(table, changed, mode="upsert", key=["id"])
    got = {r["id"]: r["name"] for r in db.query(f"SELECT id, name FROM {table} ORDER BY id")}
    assert got == {0: "n0", 1: "updated", 2: "n2", 7: "new"}


def test_overwrite_where_is_an_idempotent_partition_reload(db, table):
    db.write_rows(table, rows(5, dt="2026-10-31"))
    db.write_rows(table, rows(5, dt="2026-11-01", start=100))
    for _ in range(2):  # re-running the day must not duplicate it
        stats = db.write_rows(table, rows(3, dt="2026-11-01", start=100), mode="overwrite",
                              where={"dt": "2026-11-01"})
    assert stats.deleted == 3 and stats.rows == 3
    p = db.dialect.placeholder
    assert db.scalar(f"SELECT COUNT(*) FROM {table} WHERE dt = {p}", ["2026-11-01"]) == 3
    assert db.scalar(f"SELECT COUNT(*) FROM {table} WHERE dt = {p}", ["2026-10-31"]) == 5


def test_overwrite_whole_table_and_null_condition(db, table):
    db.write_rows(table, rows(4) + [{"id": 50, "dt": None, "name": "x", "amount": 0}])
    assert db.write_rows(table, [], mode="overwrite", where={"dt": None}).deleted == 1
    assert db.write_rows(table, rows(1), mode="overwrite", where={}).deleted == 4
    assert count(db, table) == 1


def test_write_validation(db, table):
    with pytest.raises(ValueError, match="needs key"):
        db.write_rows(table, rows(1), mode="upsert")
    with pytest.raises(ValueError, match="needs where"):
        db.write_rows(table, rows(1), mode="overwrite")
    with pytest.raises(ValueError, match="only applies"):
        db.write_rows(table, rows(1), where={"dt": "x"})
    with pytest.raises(SQLError, match="not in"):
        db.write_rows(table, [{"id": 1}, {"id": 2, "surprise": 1}])
    assert count(db, table) == 0
    assert db.write_rows(table, []).rows == 0


def test_missing_columns_become_null(db, table):
    db.write_rows(table, [{"id": 1, "name": "a"}, {"id": 2}], columns=["id", "name"])
    assert db.query(f"SELECT id, name FROM {table} ORDER BY id") == [{"id": 1, "name": "a"}, {"id": 2, "name": None}]


# ------------------------------------------------------------------- reads


def test_stream_and_read_since(db, table):
    db.write_rows(table, rows(250))
    assert sum(1 for _ in db.stream(f"SELECT * FROM {table}", batch_size=40)) == 250

    first = list(db.read_since(table, "id", None, columns=["id"], batch_size=100))
    assert [r["id"] for r in first[:3]] == [0, 1, 2] and len(first) == 250
    watermark = first[-1]["id"]
    db.write_rows(table, rows(5, start=250))
    assert [r["id"] for r in db.read_since(table, "id", watermark)] == [250, 251, 252, 253, 254]


def test_stream_does_not_disturb_writes(db, table):
    db.write_rows(table, rows(100))
    seen = 0
    for _ in db.stream(f"SELECT id FROM {table}", batch_size=10):
        seen += 1
        if seen == 50:
            db.write_rows(table, rows(1, start=1_000))  # write mid-stream on the main connection
    assert seen == 100 and count(db, table) == 101


def test_nested_transactions_commit_once(db, table):
    with pytest.raises(RuntimeError):
        with db.transaction():
            db.write_rows(table, rows(2))  # joins the outer transaction
            raise RuntimeError("abort everything")
    assert count(db, table) == 0


def test_threads_get_their_own_connections(db, table):
    errors = []

    def work(start):
        try:
            db.write_rows(table, rows(50, start=start))
        except Exception as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=work, args=(i * 1_000,)) for i in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert not errors and count(db, table) == 200


def test_ping_and_reconnect(db):
    assert db.ping()
    db.connection().close()  # simulate a dropped connection
    assert db.ping()


# ----------------------------------------------------------- pure helpers


def test_quoting_and_sql_generation():
    assert quote_ident("analytics.orders") == '"analytics"."orders"'
    assert quote_ident('we"ird') == '"we""ird"'
    with pytest.raises(ValueError):
        quote_ident("")
    sql = POSTGRES.upsert_sql("s.t", ["id", "v"], ["id"])
    assert sql == ('INSERT INTO "s"."t" ("id", "v") VALUES (%s, %s) '
                   'ON CONFLICT ("id") DO UPDATE SET "v" = excluded."v"')
    assert SQLITE.upsert_sql("t", ["id"], ["id"]).endswith('ON CONFLICT ("id") DO NOTHING')
    assert SQLITE.where({"a": 1, "b": None}) == ('"a" = ? AND "b" IS NULL', [1])
    with pytest.raises(ValueError):
        POSTGRES.upsert_sql("t", ["v"], ["id"])


def test_in_memory_sqlite_is_shared_across_connections():
    with Database.sqlite() as mem:
        mem.execute("CREATE TABLE x (id INTEGER)")
        mem.write_rows("x", [{"id": 1}, {"id": 2}])
        assert [r["id"] for r in mem.stream("SELECT id FROM x ORDER BY id")] == [1, 2]


def test_connect_is_retried():
    attempts = []
    import sqlite3

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise ConnectionError("db starting up")
        return sqlite3.connect(":memory:")

    db = Database(flaky, dialect="sqlite", connect_retries=3, retry_delay=0)
    assert db.ping() and len(attempts) == 3
