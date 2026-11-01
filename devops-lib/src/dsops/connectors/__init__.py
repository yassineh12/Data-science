"""Connectors: getting data in and out of storage.

``dataset``  Hive-style partitioned datasets (JSONL/CSV, optional gzip) on a local
             directory or S3, with staged writes and per-partition commits.
``storage``  The local and S3 backends behind ``dataset``; S3 needs ``pip install 'dsops[s3]'``.
``sql``      Postgres and SQLite tables: transactional batch writes (append / upsert /
             overwrite), streaming reads, incremental extraction. Postgres needs
             ``pip install 'dsops[postgres]'``.

Both writers share vocabulary: ``mode="append"`` adds, ``mode="overwrite"`` replaces
only the slice being written (dataset partitions / rows matching ``where=``), and
each returns a result object with ``.rows`` and ``.mode``.
"""

from .dataset import (
    DatasetError,
    DatasetWriteResult,
    FileInfo,
    PartitionedWriter,
    list_partitions,
    parse_partition_path,
    partition_path,
    read_partitioned,
    write_partitioned,
)
from .sql import Database, SQLError, TableWriteResult, quote_ident
from .storage import LocalStorage, S3Storage, Storage, open_storage

__all__ = [
    "Database",
    "DatasetError",
    "DatasetWriteResult",
    "FileInfo",
    "LocalStorage",
    "PartitionedWriter",
    "S3Storage",
    "SQLError",
    "Storage",
    "TableWriteResult",
    "list_partitions",
    "open_storage",
    "parse_partition_path",
    "partition_path",
    "quote_ident",
    "read_partitioned",
    "write_partitioned",
]
