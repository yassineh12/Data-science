"""Connectors: getting data in and out of storage.

``local``  Hive-style partitioned datasets (JSONL/CSV, optional gzip) with atomic commits.
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

__all__ = [
    "DatasetError",
    "FileInfo",
    "PartitionedWriter",
    "WriteResult",
    "list_partitions",
    "parse_partition_path",
    "partition_path",
    "read_partitioned",
    "write_partitioned",
]
