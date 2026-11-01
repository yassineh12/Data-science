"""Hive-style partitioned datasets on a local or mounted filesystem.

Layout::

    root/
      dt=2026-11-01/country=FR/part-<write_id>-00000.jsonl.gz
      dt=2026-11-01/country=US/part-<write_id>-00000.jsonl.gz
      _manifests/20261101T020000Z-<write_id>.json     # what each write produced

Writes are staged under ``root/_staging/<write_id>/`` and only moved into place
when the writer commits, so readers never see half-written files. If the block
raises, the staged files are deleted and the dataset is untouched.

::

    with PartitionedWriter("lake/orders", partition_by=["dt"], mode="overwrite") as w:
        for batch in batches:
            w.write(batch)
    print(w.result.rows, w.result.partitions)

    rows = list(read_partitioned("lake/orders", filters={"dt": "2026-11-01"}))

Formats: ``jsonl`` (types preserved as JSON allows) and ``csv`` (everything
becomes a string on read). Add ``compression="gzip"`` for either.
"""

from __future__ import annotations

import csv
import datetime as _dt
import gzip
import io
import json
import os
import re
import shutil
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import IO, Any, Callable, Iterable, Iterator, Mapping, Sequence
from urllib.parse import quote, unquote

from ..distributed import FileLock

NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"
FORMATS = ("jsonl", "csv")
MODES = ("append", "overwrite", "error", "ignore")
_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

PartitionSpec = Sequence[str] | Mapping[str, Callable[[Mapping[str, Any]], Any]]


class DatasetError(Exception):
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


def parse_partition_path(rel: str | Path) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for part in Path(rel).parts:
        key, sep, val = part.partition("=")
        if not sep:
            break
        out[key] = decode_value(val)
    return out


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
class WriteResult:
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
        return sorted({str(Path(f.path).parent) for f in self.files})


class PartitionedWriter:
    """Buffer records per partition, write files to a staging area, commit atomically per partition.

    ``mode`` decides what happens to a partition that already has data:

    append     add new files next to the existing ones (default)
    overwrite  replace only the partitions this write touches ("dynamic partition overwrite")
    error      raise :class:`DatasetError` before anything is changed
    ignore     leave the existing partition alone and drop the new rows for it
    """

    def __init__(
        self,
        root: str | os.PathLike[str],
        *,
        partition_by: PartitionSpec = (),
        format: str = "jsonl",
        compression: str | None = None,
        mode: str = "append",
        max_rows_per_file: int = 100_000,
        write_id: str | None = None,
        lock: bool = True,
    ):
        if format not in FORMATS:
            raise ValueError(f"format must be one of {FORMATS}")
        if compression not in (None, "gzip"):
            raise ValueError("compression must be None or 'gzip'")
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        if max_rows_per_file < 1:
            raise ValueError("max_rows_per_file must be >= 1")

        self.root = Path(root)
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
        self._staging = self.root / "_staging" / self.write_id
        self._buffers: dict[str, tuple[dict[str, Any], list[Mapping[str, Any]]]] = {}
        self._counters: dict[str, int] = {}
        self._files: list[FileInfo] = []
        self._state = "open"
        self.result: WriteResult | None = None

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
        name = f"part-{self.write_id}-{idx:05d}{self.extension}"
        target_dir = self._staging / rel if rel else self._staging
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / name
        with _open_write(path, self.compression) as fh:
            _serialise(fh, rows, self.format)
        file_rel = f"{rel}/{name}" if rel else name
        self._files.append(FileInfo(file_rel, values, len(rows), path.stat().st_size))

    # ------------------------------------------------------------ committing

    def commit(self) -> WriteResult:
        if self._state != "open":
            raise DatasetError(f"writer is {self._state}")
        for rel in list(self._buffers):
            self._flush(rel)

        result = WriteResult(self.write_id, str(self.root), self.mode)
        by_partition: dict[str, list[FileInfo]] = {}
        for f in self._files:
            by_partition.setdefault(str(Path(f.path).parent) if "/" in f.path else "", []).append(f)

        self.root.mkdir(parents=True, exist_ok=True)
        guard = FileLock(self.root / "_lock") if self._lock else None
        if guard:
            guard.acquire()
        try:
            existing = {rel for rel in by_partition if _has_data(self.root / rel if rel else self.root)}
            if self.mode == "error" and existing:
                raise DatasetError(f"partition(s) already exist: {sorted(existing)}")

            trash = self.root / "_trash" / self.write_id
            for rel, files in sorted(by_partition.items()):
                dest = self.root / rel if rel else self.root
                if rel in existing and self.mode == "ignore":
                    result.skipped.append(rel)
                    continue
                if rel in existing and self.mode == "overwrite":
                    if rel:
                        (trash / rel).parent.mkdir(parents=True, exist_ok=True)
                        os.replace(dest, trash / rel)
                    else:  # unpartitioned dataset: swap out the data files only
                        trash.mkdir(parents=True, exist_ok=True)
                        for old in _data_files(dest):
                            os.replace(old, trash / old.name)
                dest.mkdir(parents=True, exist_ok=True)
                for f in files:
                    os.replace(self._staging / f.path, self.root / f.path)
                result.files.extend(files)
            shutil.rmtree(trash, ignore_errors=True)
            self._write_manifest(result)
        except BaseException:
            self._cleanup()
            self._state = "aborted"
            raise
        finally:
            if guard:
                guard.release()

        self._cleanup()
        self._state = "committed"
        self.result = result
        return result

    def abort(self) -> None:
        if self._state == "open":
            self._buffers.clear()
            self._cleanup()
            self._state = "aborted"

    def _cleanup(self) -> None:
        shutil.rmtree(self._staging, ignore_errors=True)
        for parent in (self._staging.parent, self.root / "_trash"):
            try:
                parent.rmdir()  # only removes it if empty
            except OSError:
                pass

    def _write_manifest(self, result: WriteResult) -> None:
        mdir = self.root / "_manifests"
        mdir.mkdir(exist_ok=True)
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
        tmp = mdir / f".{result.write_id}.json.tmp"
        tmp.write_text(json.dumps(body, indent=2, default=str))
        os.replace(tmp, mdir / f"{result.write_id}.json")

    def __enter__(self) -> "PartitionedWriter":
        return self

    def __exit__(self, exc_type: Any, *exc: Any) -> None:
        if exc_type is None:
            self.commit()
        else:
            self.abort()


def write_partitioned(records: Iterable[Mapping[str, Any]], root: str | os.PathLike[str], **kwargs: Any) -> WriteResult:
    """One-shot helper: write ``records`` and commit."""
    with PartitionedWriter(root, **kwargs) as w:
        w.write(records)
    assert w.result is not None
    return w.result


# --------------------------------------------------------------------------- #
# Reader
# --------------------------------------------------------------------------- #

Filter = Mapping[str, Any] | Callable[[dict[str, str | None]], bool]


def list_partitions(root: str | os.PathLike[str], filters: Filter | None = None) -> list[dict[str, str | None]]:
    """Every partition that holds committed data, optionally pruned by ``filters``."""
    root = Path(root)
    found = []
    for directory in _walk_partitions(root):
        if any(_is_data_file(p) for p in directory.iterdir()):
            values = parse_partition_path(directory.relative_to(root))
            if _matches(values, filters):
                found.append(values)
    return sorted(found, key=lambda d: [str(v) for v in d.values()])


def read_partitioned(
    root: str | os.PathLike[str],
    *,
    filters: Filter | None = None,
    add_partition_columns: bool = True,
) -> Iterator[dict[str, Any]]:
    """Stream records from committed files, skipping partitions that fail ``filters``.

    ``filters`` is either ``{"dt": "2026-11-01", "country": ["FR", "US"]}`` (values
    compared as strings; a list means "any of") or a callable taking the partition
    dict. Partition values are added to records that do not already have them.
    """
    root = Path(root)
    if not root.exists():
        raise DatasetError(f"dataset {root} does not exist")
    for directory in _walk_partitions(root):
        values = parse_partition_path(directory.relative_to(root))
        if not _matches(values, filters):
            continue
        for path in sorted(p for p in directory.iterdir() if _is_data_file(p)):
            for rec in _deserialise(path):
                if add_partition_columns:
                    for k, v in values.items():
                        rec.setdefault(k, v)
                yield rec


def _walk_partitions(root: Path) -> Iterator[Path]:
    """Yield ``root`` and every visible ``key=value`` directory below it, depth first, sorted."""
    yield root
    try:
        children = sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(("_", ".")))
    except FileNotFoundError:
        return
    for child in children:
        if "=" in child.name:
            yield from _walk_partitions(child)


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


def _is_data_file(p: Path) -> bool:
    return p.is_file() and not p.name.startswith(("_", ".")) and _format_of(p) is not None


def _data_files(directory: Path) -> list[Path]:
    return [p for p in directory.iterdir() if _is_data_file(p)]


def _has_data(directory: Path) -> bool:
    return directory.is_dir() and any(_is_data_file(p) for p in directory.iterdir())


def _format_of(p: Path) -> tuple[str, bool] | None:
    name = p.name
    gz = name.endswith(".gz")
    if gz:
        name = name[:-3]
    for fmt in FORMATS:
        if name.endswith(f".{fmt}"):
            return fmt, gz
    return None


def _open_write(path: Path, compression: str | None) -> IO[str]:
    if compression == "gzip":
        return io.TextIOWrapper(gzip.open(path, "wb"), encoding="utf-8", newline="")
    return path.open("w", encoding="utf-8", newline="")


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


def _deserialise(path: Path) -> Iterator[dict[str, Any]]:
    fmt, gz = _format_of(path)  # type: ignore[misc]
    opener = (lambda: io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8", newline="")) if gz \
        else (lambda: path.open("r", encoding="utf-8", newline=""))
    with opener() as fh:
        if fmt == "jsonl":
            for line in fh:
                if line.strip():
                    yield json.loads(line)
        else:
            yield from csv.DictReader(fh)


def _json_default(o: Any) -> Any:
    if isinstance(o, (_dt.date, _dt.datetime)):
        return o.isoformat()
    if isinstance(o, (set, frozenset)):
        return sorted(o)
    return str(o)
