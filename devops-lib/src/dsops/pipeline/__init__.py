"""Data-engineering building blocks: DAG runner, batching, data-quality checks."""

from .batching import MapResult, chunked, dedupe, parallel_map, partition_by
from .dag import (
    CACHED,
    CANCELLED,
    DAG,
    FAILED,
    SUCCESS,
    UPSTREAM_FAILED,
    CycleError,
    DAGError,
    PipelineFailed,
    RunResult,
    TaskContext,
    TaskResult,
)
from .quality import (
    DataQualityError,
    Field,
    QualityReport,
    Schema,
    in_range,
    length,
    matches,
    one_of,
    predicate,
    validate_records,
)

__all__ = [
    "CACHED", "CANCELLED", "DAG", "FAILED", "SUCCESS", "UPSTREAM_FAILED",
    "CycleError", "DAGError", "DataQualityError", "Field", "MapResult", "PipelineFailed",
    "QualityReport", "RunResult", "Schema", "TaskContext", "TaskResult",
    "chunked", "dedupe", "in_range", "length", "matches", "one_of",
    "parallel_map", "partition_by", "predicate", "validate_records",
]
