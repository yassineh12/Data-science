"""Where dataset files live: a local directory or an S3 bucket, behind one small interface.

Keys are ``/``-separated paths relative to the dataset root; ``""`` is the root.
:func:`open_storage` turns ``"lake/orders"``, a ``Path`` or ``"s3://bucket/lake/orders"``
into the right backend, so everything built on top accepts any of them.

The two backends differ in one guarantee, and the writer relies on it:

LocalStorage  ``swap_prefix`` renames a whole partition directory into place, so
              readers see the old partition or the new one, never a mix.
S3Storage     S3 has no rename. Each object appears atomically, but replacing a
              partition is "copy new files in, then delete old ones", so a reader
              listing during that window can see both. S3 also has no lock; pass
              ``lock=`` (e.g. a :class:`~dsops.distributed.RedisLock`) to the writer
              if several jobs may overwrite the same partition at once.
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
from pathlib import Path
from typing import Any, BinaryIO, ContextManager, Protocol, runtime_checkable

from ..distributed import FileLock


@runtime_checkable
class Storage(Protocol):
    url: str

    def exists(self) -> bool: ...
    def list_dir(self, prefix: str) -> tuple[list[str], list[str]]: ...
    def list_files(self, prefix: str) -> list[str]: ...
    def write_bytes(self, key: str, data: bytes) -> None: ...
    def open_read(self, key: str) -> BinaryIO: ...
    def move(self, src: str, dst: str) -> None: ...
    def delete(self, key: str) -> None: ...
    def delete_prefix(self, prefix: str) -> None: ...
    def swap_prefix(self, src: str, dst: str, trash: str) -> bool: ...
    def lock(self) -> ContextManager[Any]: ...


def join(*parts: str) -> str:
    return "/".join(p.strip("/") for p in parts if p and p.strip("/"))


def open_storage(root: "str | os.PathLike[str] | Storage") -> "Storage":
    if isinstance(root, Storage):
        return root
    text = os.fspath(root)
    if text.startswith("s3://"):
        bucket, _, prefix = text[5:].partition("/")
        if not bucket:
            raise ValueError(f"no bucket in {text!r}")
        return S3Storage(bucket, prefix)
    return LocalStorage(text)


# --------------------------------------------------------------------------- #
# Local filesystem
# --------------------------------------------------------------------------- #


class LocalStorage:
    def __init__(self, root: str | os.PathLike[str]):
        self.root = Path(root)
        self.url = str(self.root)

    def _path(self, key: str) -> Path:
        return self.root / key if key else self.root

    def exists(self) -> bool:
        return self.root.exists()

    def list_dir(self, prefix: str) -> tuple[list[str], list[str]]:
        path = self._path(prefix)
        if not path.is_dir():
            return [], []
        dirs, files = [], []
        for p in sorted(path.iterdir()):
            (dirs if p.is_dir() else files).append(p.name)
        return dirs, files

    def list_files(self, prefix: str) -> list[str]:
        path = self._path(prefix)
        if not path.exists():
            return []
        return sorted(p.relative_to(self.root).as_posix() for p in path.rglob("*") if p.is_file())

    def write_bytes(self, key: str, data: bytes) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def open_read(self, key: str) -> BinaryIO:
        return self._path(key).open("rb")

    def move(self, src: str, dst: str) -> None:
        target = self._path(dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(self._path(src), target)

    def delete(self, key: str) -> None:
        self._path(key).unlink(missing_ok=True)
        self._prune(self._path(key).parent)

    def delete_prefix(self, prefix: str) -> None:
        if not prefix:
            raise ValueError("refusing to delete the whole dataset")
        shutil.rmtree(self._path(prefix), ignore_errors=True)
        self._prune(self._path(prefix).parent)

    def swap_prefix(self, src: str, dst: str, trash: str) -> bool:
        """Atomically replace directory ``dst`` with ``src``; the old one goes to ``trash``."""
        if not dst:
            return False  # cannot rename the dataset root itself
        dst_path = self._path(dst)
        if dst_path.exists():
            self.move(dst, trash)
        self.move(src, dst)
        return True

    def lock(self) -> ContextManager[Any]:
        return FileLock(self.root / "_lock")

    def _prune(self, path: Path) -> None:
        """Remove empty directories between ``path`` and the root (bookkeeping dirs only)."""
        while path != self.root and self.root in path.parents:
            try:
                path.rmdir()
            except OSError:
                return
            path = path.parent

    def __repr__(self) -> str:
        return f"LocalStorage({self.url!r})"


# --------------------------------------------------------------------------- #
# S3
# --------------------------------------------------------------------------- #


class S3Storage:
    """Objects under ``s3://bucket/prefix``. Pass ``client=`` to reuse a configured boto3 client."""

    def __init__(self, bucket: str, prefix: str = "", *, client: Any = None):
        if client is None:
            try:
                import boto3  # type: ignore[import-not-found]
            except ImportError as exc:  # pragma: no cover - depends on env
                raise ImportError("S3 support needs boto3: pip install 'dsops[s3]'") from exc
            client = boto3.client("s3")
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.url = f"s3://{bucket}/{self.prefix}" if self.prefix else f"s3://{bucket}"

    def _key(self, key: str) -> str:
        return join(self.prefix, key)

    def _rel(self, full: str) -> str:
        return full[len(self.prefix) + 1:] if self.prefix else full

    def _pages(self, **kwargs: Any) -> Any:
        return self.client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, **kwargs)

    def exists(self) -> bool:
        base = self._key("")
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=base + "/" if base else "", MaxKeys=1)
        return resp.get("KeyCount", 0) > 0

    def list_dir(self, prefix: str) -> tuple[list[str], list[str]]:
        base = self._key(prefix)
        base = base + "/" if base else ""
        dirs, files = [], []
        for page in self._pages(Prefix=base, Delimiter="/"):
            dirs += [cp["Prefix"][len(base):].rstrip("/") for cp in page.get("CommonPrefixes", [])]
            files += [o["Key"][len(base):] for o in page.get("Contents", []) if not o["Key"].endswith("/")]
        return sorted(dirs), sorted(files)

    def list_files(self, prefix: str) -> list[str]:
        base = self._key(prefix)
        base = base + "/" if base else ""
        return sorted(self._rel(o["Key"]) for page in self._pages(Prefix=base) for o in page.get("Contents", []))

    def write_bytes(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def open_read(self, key: str) -> BinaryIO:
        body = self.client.get_object(Bucket=self.bucket, Key=self._key(key))["Body"]
        try:
            return io.BytesIO(body.read())  # data files are bounded by max_rows_per_file
        finally:
            body.close()

    def move(self, src: str, dst: str) -> None:
        self.client.copy_object(Bucket=self.bucket, Key=self._key(dst),
                                CopySource={"Bucket": self.bucket, "Key": self._key(src)})
        self.client.delete_object(Bucket=self.bucket, Key=self._key(src))

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(key))

    def delete_prefix(self, prefix: str) -> None:
        if not prefix:
            raise ValueError("refusing to delete the whole dataset")
        keys = [self._key(k) for k in self.list_files(prefix)]
        for i in range(0, len(keys), 1000):  # DeleteObjects takes at most 1000 keys
            self.client.delete_objects(Bucket=self.bucket,
                                       Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]], "Quiet": True})

    def swap_prefix(self, src: str, dst: str, trash: str) -> bool:
        return False  # no rename on S3; the writer falls back to copy-then-delete

    def lock(self) -> ContextManager[Any]:
        return contextlib.nullcontext()

    def __repr__(self) -> str:
        return f"S3Storage({self.url!r})"
