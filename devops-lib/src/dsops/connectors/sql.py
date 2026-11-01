"""SQL databases through DB-API 2.0: Postgres (via ``psycopg``) and SQLite.

::

    db = Database.postgres("postgresql://etl@db:5432/warehouse")

    # idempotent daily reload: delete the day, insert the day, one transaction
    db.write_rows("orders", rows, mode="replace", replace_where={"dt": "2026-11-01"})

    # merge by primary key
    db.write_rows("customers", rows, mode="upsert", key=["customer_id"])

    # stream a large table without loading it into memory (server-side cursor on Postgres)
    for row in db.stream("select * from events where dt = %s", ["2026-11-01"]):
        ...

    # incremental extraction from a watermark
    for row in db.read_since("events", "updated_at", last_seen):
        last_seen = row["updated_at"]

    health.register("postgres", db.ping)

Install the driver with ``pip install 'dsops[postgres]'``. SQLite needs nothing,
which makes it handy for tests and local runs; the API is the same apart from
the parameter placeholder (``%s`` for Postgres, ``?`` for SQLite) in raw SQL.
"""

from __future__ import annotations

import contextlib
import itertools
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

from ..resilience import RetryPolicy, retry

Row = dict[str, Any]
MODES = ("append", "upsert", "replace")


class SQLError(Exception):
    pass


# --------------------------------------------------------------------------- #
# Dialects
# --------------------------------------------------------------------------- #


def quote_ident(name: str) -> str:
    """``analytics.orders`` -> ``"analytics"."orders"``; embedded quotes are doubled."""
    if not name or "\x00" in name:
        raise SQLError(f"invalid identifier {name!r}")
    return ".".join('"' + part.replace('"', '""') + '"' for part in name.split("."))


def _quote_column(name: str) -> str:
    if not name or "\x00" in name:
        raise SQLError(f"invalid column name {name!r}")
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class Dialect:
    name: str
    placeholder: str

    def insert_sql(self, table: str, columns: Sequence[str]) -> str:
        cols = ", ".join(_quote_column(c) for c in columns)
        marks = ", ".join([self.placeholder] * len(columns))
        return f"INSERT INTO {quote_ident(table)} ({cols}) VALUES ({marks})"

    def upsert_sql(self, table: str, columns: Sequence[str], key: Sequence[str]) -> str:
        missing = [k for k in key if k not in columns]
        if missing:
            raise SQLError(f"upsert key column(s) {missing} not in written columns")
        conflict = ", ".join(_quote_column(k) for k in key)
        updates = [c for c in columns if c not in key]
        if not updates:
            return f"{self.insert_sql(table, columns)} ON CONFLICT ({conflict}) DO NOTHING"
        sets = ", ".join(f"{_quote_column(c)} = excluded.{_quote_column(c)}" for c in updates)
        return f"{self.insert_sql(table, columns)} ON CONFLICT ({conflict}) DO UPDATE SET {sets}"

    def where(self, conditions: Mapping[str, Any]) -> tuple[str, list[Any]]:
        parts, params = [], []
        for col, value in conditions.items():
            if value is None:
                parts.append(f"{_quote_column(col)} IS NULL")
            else:
                parts.append(f"{_quote_column(col)} = {self.placeholder}")
                params.append(value)
        return " AND ".join(parts), params


POSTGRES = Dialect("postgres", "%s")
SQLITE = Dialect("sqlite", "?")
DIALECTS = {"postgres": POSTGRES, "postgresql": POSTGRES, "sqlite": SQLITE}


# --------------------------------------------------------------------------- #
# Database
# --------------------------------------------------------------------------- #


@dataclass
class WriteStats:
    table: str
    mode: str
    rows: int = 0
    batches: int = 0
    deleted: int = 0


class Database:
    """A connection factory plus the operations pipelines need.

    Each thread gets its own connection, opened lazily (with retries) and reopened
    if the server dropped it. :meth:`stream` uses a dedicated connection so a long
    read never blocks or breaks writes made meanwhile.
    """

    def __init__(
        self,
        connect: Callable[[], Any],
        *,
        dialect: str | Dialect = "postgres",
        connect_retries: int = 3,
        retry_delay: float = 0.5,
    ):
        self._connect_raw = connect
        self.dialect = DIALECTS[dialect] if isinstance(dialect, str) else dialect
        self._connect = retry(policy=RetryPolicy(max_attempts=connect_retries + 1, base_delay=retry_delay))(connect)
        self._local = threading.local()
        self._all: list[Any] = []
        self._guard = threading.Lock()

    @classmethod
    def postgres(cls, dsn: str, **kwargs: Any) -> "Database":
        try:
            import psycopg  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - depends on env
            raise SQLError("Postgres support needs psycopg: pip install 'dsops[postgres]'") from exc
        return cls(lambda: psycopg.connect(dsn), dialect="postgres", **kwargs)

    @classmethod
    def sqlite(cls, path: str = ":memory:", **kwargs: Any) -> "Database":
        """SQLite file, or a private in-memory database shared by this object's connections."""
        if path == ":memory:":
            uri = f"file:dsops_{uuid.uuid4().hex}?mode=memory&cache=shared"
            db = cls(lambda: sqlite3.connect(uri, uri=True, check_same_thread=False), dialect="sqlite", **kwargs)
            keeper = db._connect_raw()  # the in-memory database lives as long as one connection does
            db._all.append(keeper)
            return db

        def connect() -> sqlite3.Connection:
            conn = sqlite3.connect(path, check_same_thread=False, timeout=30)
            conn.execute("PRAGMA journal_mode=WAL")  # readers and a writer can work at the same time
            return conn

        return cls(connect, dialect="sqlite", **kwargs)

    # ------------------------------------------------------------ connections

    def connection(self) -> Any:
        conn = getattr(self._local, "conn", None)
        if conn is not None and not _is_closed(conn):
            return conn
        conn = self._connect()
        self._local.conn = conn
        self._local.depth = 0
        with self._guard:
            self._all.append(conn)
        return conn

    def close(self) -> None:
        with self._guard:
            conns, self._all = self._all, []
        for c in conns:
            with contextlib.suppress(Exception):
                c.close()
        self._local = threading.local()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @contextlib.contextmanager
    def transaction(self) -> Iterator[Any]:
        """Yield a cursor; commit on success, roll back on error. Nested blocks join the outer one."""
        conn = self.connection()
        depth = getattr(self._local, "depth", 0)
        cur = conn.cursor()
        self._local.depth = depth + 1
        try:
            yield cur
            if depth == 0:
                conn.commit()
        except BaseException:
            if depth == 0:
                with contextlib.suppress(Exception):
                    conn.rollback()
            raise
        finally:
            self._local.depth = depth
            with contextlib.suppress(Exception):
                cur.close()

    # ---------------------------------------------------------------- reads

    def execute(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> int:
        with self.transaction() as cur:
            cur.execute(sql, params or ())
            return cur.rowcount

    def query(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> list[Row]:
        with self.transaction() as cur:
            cur.execute(sql, params or ())
            return _rows(cur, cur.fetchall())

    def scalar(self, sql: str, params: Sequence[Any] | Mapping[str, Any] | None = None) -> Any:
        with self.transaction() as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            return None if row is None else row[0]

    def stream(
        self,
        sql: str,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        batch_size: int = 10_000,
    ) -> Iterator[Row]:
        """Yield rows lazily. On Postgres this is a server-side cursor, so memory stays flat."""
        conn = self._connect()
        try:
            if self.dialect is POSTGRES and _supports_named_cursor(conn):
                cur = conn.cursor(name=f"dsops_{uuid.uuid4().hex[:12]}")
                cur.itersize = batch_size
            else:
                cur = conn.cursor()
            try:
                cur.execute(sql, params or ())
                while batch := cur.fetchmany(batch_size):
                    yield from _rows(cur, batch)
            finally:
                with contextlib.suppress(Exception):
                    cur.close()
            conn.rollback()  # read-only: nothing to keep
        finally:
            with contextlib.suppress(Exception):
                conn.close()

    def read_since(
        self,
        table: str,
        column: str,
        watermark: Any,
        *,
        columns: Sequence[str] | None = None,
        batch_size: int = 10_000,
    ) -> Iterator[Row]:
        """Rows where ``column > watermark`` (all rows if ``watermark`` is None), ordered by ``column``.

        Store the last ``row[column]`` you processed and pass it next run. Use a
        column that only grows (``updated_at``, an identity id) and is indexed.
        """
        cols = "*" if not columns else ", ".join(_quote_column(c) for c in columns)
        sql = f"SELECT {cols} FROM {quote_ident(table)}"
        params: list[Any] = []
        if watermark is not None:
            sql += f" WHERE {_quote_column(column)} > {self.dialect.placeholder}"
            params.append(watermark)
        sql += f" ORDER BY {_quote_column(column)}"
        return self.stream(sql, params, batch_size=batch_size)

    def ping(self) -> bool:
        """Health check: ``SELECT 1`` on this thread's connection."""
        return self.scalar("SELECT 1") == 1

    # --------------------------------------------------------------- writes

    def write_rows(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        *,
        mode: str = "append",
        key: Sequence[str] | None = None,
        replace_where: Mapping[str, Any] | None = None,
        columns: Sequence[str] | None = None,
        batch_size: int = 1_000,
    ) -> WriteStats:
        """Insert ``rows`` in batches inside one transaction — all of them land, or none do.

        append   plain INSERT
        upsert   INSERT ... ON CONFLICT (key) DO UPDATE (``key`` must be a unique/primary key)
        replace  DELETE rows matching ``replace_where`` then INSERT. Pass ``replace_where={}``
                 to replace the whole table. Readers see the old data until commit.
        """
        if mode not in MODES:
            raise SQLError(f"mode must be one of {MODES}")
        if mode == "upsert" and not key:
            raise SQLError("mode='upsert' needs key=[...]")
        if mode == "replace" and replace_where is None:
            raise SQLError("mode='replace' needs replace_where={...} (use {} to replace the whole table)")
        if batch_size < 1:
            raise SQLError("batch_size must be >= 1")

        it = iter(rows)
        first = next(it, None)
        stats = WriteStats(table, mode)
        if columns is None:
            columns = list(first.keys()) if first is not None else []
        columns = list(columns)
        allowed = set(columns)

        if mode == "upsert":
            sql = self.dialect.upsert_sql(table, columns, key or ())
        else:
            sql = self.dialect.insert_sql(table, columns) if columns else ""

        def as_tuple(i: int, r: Mapping[str, Any]) -> tuple[Any, ...]:
            extra = set(r) - allowed
            if extra:
                raise SQLError(f"row {i} has column(s) {sorted(extra)} not in {columns}")
            return tuple(r.get(c) for c in columns)

        with self.transaction() as cur:
            if mode == "replace":
                clause, params = self.dialect.where(replace_where or {})
                delete = f"DELETE FROM {quote_ident(table)}" + (f" WHERE {clause}" if clause else "")
                cur.execute(delete, params)
                stats.deleted = max(cur.rowcount, 0)
            if first is None:
                return stats
            numbered = enumerate(itertools.chain([first], it))
            while batch := [as_tuple(i, r) for i, r in itertools.islice(numbered, batch_size)]:
                cur.executemany(sql, batch)
                stats.rows += len(batch)
                stats.batches += 1
        return stats


def _rows(cur: Any, raw: Sequence[Sequence[Any]]) -> list[Row]:
    names = [d[0] for d in cur.description or ()]
    return [dict(zip(names, r)) for r in raw]


def _is_closed(conn: Any) -> bool:
    if hasattr(conn, "closed"):  # psycopg, psycopg2
        return bool(conn.closed) or bool(getattr(conn, "broken", False))
    try:  # sqlite3 and drivers without a flag: a closed connection refuses new cursors
        conn.cursor().close()
    except Exception:
        return True
    return False


def _supports_named_cursor(conn: Any) -> bool:
    try:
        import inspect

        return "name" in inspect.signature(conn.cursor).parameters
    except (TypeError, ValueError):
        return False
