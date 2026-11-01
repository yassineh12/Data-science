"""Hive-style partitioned datasets on a local directory or S3.

Layout::

    root/                                   (a path, or s3://bucket/prefix)
      dt=2026-11-01/country=FR/part-<write_id>-00000.jsonl.gz
      dt=2026-11-01/country=US/part-<write_id>-00000.jsonl.gz
      _manifests/<write_id>.json             # files, rows and bytes of each committed write

Writes are staged under ``root/_staging/<write_id>/`` and moved into place only
when the writer commits, so readers never see half-written files. If the block
raises, the staged files are deleted and the dataset is untouched.

::

    with PartitionedWriter("s3://lake/orders", partition_by=["dt"], mode="overwrite") as w:
        for batch in batches:
            w.write(batch)
    print(w.result.rows, w.result.partitions)

    rows = list(read_partitioned("s3://lake/orders", filters={"dt": "2026-11-01"}))

Formats: ``jsonl`` (types preserved as JSON allows) and ``csv`` (everything
becomes a string on read). Add ``compression="gzip"`` for either. See
:mod:`dsops.connectors.storage` for how local and S3 commits differ.
"""

from __future__ import annotations

import csv
import datetime as _dt
import gzip
import io
import json
import os
import re
import uuid
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field
from typing import IO, Any, BinaryIO, Callable, Iterable, Iterator, Mapping, Sequence
from urllib.parse import quote, unquote

from ..errors import DsopsError
from .storage import Storage, join, open_storage

NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"
FORMATS = ("jsonl", "csv")
MODES = ("append", "overwrite", "error", "ignore")
_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

PartitionSpec = Sequence[str] | Mapping[str, Callable[[Mapping[str, Any]], Any]]
Root = str | os.PathLike[str] | Storage


class DatasetError(DsopsError):
    pass


# --------------------------------------------------------------------------- #
# Partition paths
# --------------------------------------------------------------------------- #


def encode_value(value: Any) -> str:
    if value is None or value == "":
        return NULL_PARTITION
    if isinstance(value, (_dt.date, _dt.datetime)):
        value = value.isoformat()
    return quote(str(value), safe="")


def decode_value(text: str) -> str | None:
    return None if text == NULL_PARTITION else unquote(text)


def partition_path(values: Mapping[str, Any]) -> str:
    """``{"dt": "2026-11-01", "country": "FR"}`` -> ``"dt=2026-11-01/country=FR"``."""
    return "/".join(f"{k}={encode_value(v)}" for k, v in values.items())


def parse_partition_path(rel: str | os.PathLike[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for part in os.fspath(rel).replace(os.sep, "/").split("/"):
        key, sep, val = part.partition("=")
        if not sep:
            break
        out[key] = decode_value(val)
    return out


def _parent(key: str) -> str:
    return key.rsplit("/", 1)[0] if "/" in key else ""


# --------------------------------------------------------------------------- #
# Writer
# --------------------------------------------------------------------------- #


@dataclass
class FileInfo:
    path: str  # relative to the dataset root
    partition: dict[str, Any]
    rows: int
    bytes: int


@dataclass
class DatasetWriteResult:
    write_id: str
    root: str
    mode: str
    files: list[FileInfo] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)  # partitions left alone by mode="ignore"

    @property
    def rows(self) -> int:
        return sum(f.rows for f in self.files)

    @property
    def partitions(self) -> list[str]:
        return sorted({_parent(f.path) for f in self.files})


class PartitionedWriter:
    """Buffer records per partition, stage files, then commit partition by partition.

    ``mode`` decides what happens to a partition that already has data:

    append     add new files next to the existing ones (default)
    overwrite  replace only the partitions this write touches ("dynamic partition overwrite")
    error      raise :class:`DatasetError` before anything is changed
    ignore     leave the existing partition alone and drop the new rows for it

    ``lock``: ``True`` uses the storage's own lock (a file lock locally, none on S3),
    ``False`` disables locking, or pass any context manager (e.g. a ``RedisLock``).
    """

    def __init__(
        self,
        root: Root,
        *,
        partition_by: PartitionSpec = (),
        format: str = "jsonl",
        compression: str | None = None,
        mode: str = "append",
        max_rows_per_file: int = 100_000,
        write_id: str | None = None,
        lock: bool | Any = True,
    ):
        if format not in FORMATS:
            raise ValueError(f"format must be one of {FORMATS}")
        if compression not in (None, "gzip"):
            raise ValueError("compression must be None or 'gzip'")
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        if max_rows_per_file < 1:
            raise ValueError("max_rows_per_file must be >= 1")

        self.storage = open_storage(root)
        if isinstance(partition_by, Mapping):
            self._partition_fns = dict(partition_by)
        else:
            self._partition_fns = {k: (lambda r, k=k: r[k]) for k in partition_by}
        for key in self._partition_fns:
            if not _KEY_RE.match(key):
                raise ValueError(f"invalid partition key {key!r}: use letters, digits and underscores")

        self.format = format
        self.compression = compression
        self.mode = mode
        self.max_rows_per_file = max_rows_per_file
        self.write_id = write_id or _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
        self._lock = lock
        self._staging = join("_staging", self.write_id)
        self._buffers: dict[str, tuple[dict[str, Any], list[Mapping[str, Any]]]] = {}
        self._counters: dict[str, int] = {}
        self._files: list[FileInfo] = []
        self._state = "open"
        self.result: DatasetWriteResult | None = None

    @property
    def extension(self) -> str:
        return f".{self.format}" + (".gz" if self.compression == "gzip" else "")

    # -------------------------------------------------------------- writing

    def write(self, records: Iterable[Mapping[str, Any]]) -> int:
        if self._state != "open":
            raise DatasetError(f"writer is {self._state}")
        n = 0
        for rec in records:
            try:
                values = {k: fn(rec) for k, fn in self._partition_fns.items()}
            except (KeyError, TypeError, ValueError) as exc:
                raise DatasetError(f"cannot compute partition for record {n}: {exc!r}") from exc
            rel = partition_path(values)
            buf = self._buffers.get(rel)
            if buf is None:
                buf = self._buffers[rel] = (values, [])
            buf[1].append(rec)
            if len(buf[1]) >= self.max_rows_per_file:
                self._flush(rel)
            n += 1
        return n

    def _flush(self, rel: str) -> None:
        values, rows = self._buffers.pop(rel, ({}, []))
        if not rows:
            return
        idx = self._counters.get(rel, 0)
        self._counters[rel] = idx + 1
        key = join(rel, f"part-{self.write_id}-{idx:05d}{self.extension}")
        data = _encode(rows, self.format, self.compression)
        self.storage.write_bytes(join(self._staging, key), data)
        self._files.append(FileInfo(key, values, len(rows), len(data)))

    # ------------------------------------------------------------ committing

    def _guard(self) -> Any:
        if self._lock is True:
            return self.storage.lock()
        if self._lock is False or self._lock is None:
            return nullcontext()
        return self._lock

    def commit(self) -> DatasetWriteResult:
        if self._state != "open":
            raise DatasetError(f"writer is {self._state}")
        for rel in list(self._buffers):
            self._flush(rel)

        st = self.storage
        result = DatasetWriteResult(self.write_id, st.url, self.mode)
        by_partition: dict[str, list[FileInfo]] = {}
        for f in self._files:
            by_partition.setdefault(_parent(f.path), []).append(f)
        trash = join("_trash", self.write_id)

        try:
            with self._guard():
                existing = {rel: _data_files(st, rel) for rel in by_partition}
                existing = {rel: old for rel, old in existing.items() if old}
                if self.mode == "error" and existing:
                    raise DatasetError(f"partition(s) already exist: {sorted(existing)}")

                for rel, files in sorted(by_partition.items()):
                    old = existing.get(rel, [])
                    if old and self.mode == "ignore":
                        result.skipped.append(rel)
                        continue
                    if old and self.mode == "overwrite" and st.swap_prefix(
                            join(self._staging, rel), rel, join(trash, rel)):
                        result.files.extend(files)  # whole partition swapped in one rename
                        continue
                    new_keys = {f.path for f in files}
                    for f in files:
                        st.move(join(self._staging, f.path), f.path)
                    if self.mode == "overwrite":
                        for key in old:
                            if key not in new_keys:  # same write_id re-run: keep what we just wrote
                                st.delete(key)
                    result.files.extend(files)
                st.delete_prefix(trash)
                st.write_bytes(join("_manifests", f"{self.write_id}.json"), self._manifest(result))
        except BaseException:
            self._discard_staging()
            self._state = "aborted"
            raise

        self._discard_staging()
        self._state = "committed"
        self.result = result
        return result

    def abort(self) -> None:
        if self._state == "open":
            self._buffers.clear()
            self._discard_staging()
            self._state = "aborted"

    def _discard_staging(self) -> None:
        self.storage.delete_prefix(self._staging)

    def _manifest(self, result: DatasetWriteResult) -> bytes:
        body = {
            "write_id": result.write_id,
            "committed_at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "mode": result.mode,
            "format": self.format,
            "compression": self.compression,
            "rows": result.rows,
            "files": [asdict(f) for f in result.files],
            "skipped_partitions": result.skipped,
        }
        return json.dumps(body, indent=2, default=str).encode()

    def __enter__(self) -> "PartitionedWriter":
        return self

    def __exit__(self, exc_type: Any, *exc: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.abort()


def write_partitioned(records: Iterable[Mapping[str, Any]], root: Root, **kwargs: Any) -> DatasetWriteResult:
    """One-shot helper: write ``records`` and commit."""
    with PartitionedWriter(root, **kwargs) as w:
        w.write(records)
    assert w.result is not None
    return w.result


# --------------------------------------------------------------------------- #
# Reader
# --------------------------------------------------------------------------- #

Filter = Mapping[str, Any] | Callable[[dict[str, str | None]], bool]


def list_partitions(root: Root, filters: Filter | None = None) -> list[dict[str, str | None]]:
    """Every partition that holds committed data, optionally pruned by ``filters``."""
    st = open_storage(root)
    found = []
    for rel, files in _walk_partitions(st, "", filters):
        if any(_is_data_name(n) for n in files):
            found.append(parse_partition_path(rel))
    return sorted(found, key=lambda d: [str(v) for v in d.values()])


def read_partitioned(
    root: Root,
    *,
    filters: Filter | None = None,
    add_partition_columns: bool = True,
) -> Iterator[dict[str, Any]]:
    """Stream records from committed files, skipping partitions that fail ``filters``.

    ``filters`` is either ``{"dt": "2026-11-01", "country": ["FR", "US"]}`` (values
    compared as strings; a list means "any of") or a callable taking the partition
    dict. Non-matching directories are never listed or opened, which matters on S3.
    Partition values are added to records that do not already have them.
    """
    st = open_storage(root)
    if not st.exists():
        raise DatasetError(f"dataset {st.url} does not exist")
    for rel, files in _walk_partitions(st, "", filters):
        values = parse_partition_path(rel)
        for name in files:
            if not _is_data_name(name):
                continue
            with st.open_read(join(rel, name)) as fh:
                for rec in _decode(fh, name):
                    if add_partition_columns:
                        for k, v in values.items():
                            rec.setdefault(k, v)
                    yield rec


def _walk_partitions(st: Storage, rel: str, filters: Filter | None) -> Iterator[tuple[str, list[str]]]:
    """Yield ``(partition, file names)`` for ``rel`` and every visible ``key=value`` dir below it."""
    if not _matches(parse_partition_path(rel), filters):
        return  # prune: nothing below a non-matching partition can match
    dirs, files = st.list_dir(rel)
    yield rel, files
    for name in dirs:
        if "=" in name and not name.startswith(("_", ".")):
            yield from _walk_partitions(st, join(rel, name), filters)


def _matches(values: dict[str, str | None], filters: Filter | None) -> bool:
    if filters is None:
        return True
    if callable(filters):
        return bool(filters(values))
    for key, want in filters.items():
        if key not in values:
            continue  # deeper level not reached yet, or not a partition key: don't prune
        options = want if isinstance(want, (list, tuple, set, frozenset)) else [want]
        if values[key] not in {None if o is None else str(o) for o in options}:
            return False
    return True


# --------------------------------------------------------------------------- #
# File formats
# --------------------------------------------------------------------------- #


def _format_of(name: str) -> tuple[str, bool] | None:
    gz = name.endswith(".gz")
    if gz:
        name = name[:-3]
    for fmt in FORMATS:
        if name.endswith(f".{fmt}"):
            return fmt, gz
    return None


def _is_data_name(name: str) -> bool:
    return not name.startswith(("_", ".")) and _format_of(name) is not None


def _data_files(st: Storage, rel: str) -> list[str]:
    return [join(rel, n) for n in st.list_dir(rel)[1] if _is_data_name(n)]


def _encode(rows: list[Mapping[str, Any]], fmt: str, compression: str | None) -> bytes:
    buf = io.StringIO(newline="")
    _serialise(buf, rows, fmt)
    data = buf.getvalue().encode("utf-8")
    return gzip.compress(data, mtime=0) if compression == "gzip" else data


def _serialise(fh: IO[str], rows: list[Mapping[str, Any]], fmt: str) -> None:
    if fmt == "jsonl":
        for r in rows:
            fh.write(json.dumps(r, default=_json_default, ensure_ascii=False))
            fh.write("\n")
        return
    fieldnames: dict[str, None] = {}
    for r in rows:
        fieldnames.update(dict.fromkeys(r))
    writer = csv.DictWriter(fh, fieldnames=list(fieldnames))
    writer.writeheader()
    writer.writerows(rows)


def _decode(raw: BinaryIO, name: str) -> Iterator[dict[str, Any]]:
    fmt, gz = _format_of(name)  # type: ignore[misc]
    binary: BinaryIO = gzip.GzipFile(fileobj=raw) if gz else raw  # type: ignore[assignment]
    text = io.TextIOWrapper(binary, encoding="utf-8", newline="")
    if fmt == "jsonl":
        for line in text:
            if line.strip():
                yield json.loads(line)
    else:
        yield from csv.DictReader(text)


def _json_default(o: Any) -> Any:
    if isinstance(o, (_dt.date, _dt.datetime)):
        return o.isoformat()
    if isinstance(o, (set, frozenset)):
        return sorted(o)
    return str(o)
