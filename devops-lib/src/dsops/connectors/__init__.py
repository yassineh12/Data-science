"""Connectors: getting data in and out of storage.

``local``  Hive-style partitioned datasets (JSONL/CSV, optional gzip) with atomic commits.
``sql``    Postgres and SQLite: transactional batch writes (append/upsert/replace), streaming
           reads, incremental extraction. Postgres needs ``pip install 'dsops[postgres]'``.
"""

from .local import (
    DatasetError,
    FileInfo,
    PartitionedWriter,
    WriteResult,
    list_partitions,
    partition_path,
    parse_partition_path,
    read_partitioned,
    write_partitioned,
)
from .sql import Database, SQLError, WriteStats, quote_ident

__all__ = [
    "Database",
    "DatasetError",
    "FileInfo",
    "PartitionedWriter",
    "SQLError",
    "WriteResult",
    "WriteStats",
    "list_partitions",
    "parse_partition_path",
    "partition_path",
    "quote_ident",
    "read_partitioned",
    "write_partitioned",
]
