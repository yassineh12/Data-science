"""Every operational error shares one base; built-in bases are kept where callers rely on them."""

import pytest

import dsops
from dsops.connectors import DatasetError, SQLError
from dsops.pipeline import CycleError, DAGError, DataQualityError, PipelineFailed


@pytest.mark.parametrize("cls", [
    dsops.RetryError, dsops.CircuitOpenError, dsops.BulkheadFullError, dsops.ConfigError,
    dsops.LockNotAcquired, DAGError, CycleError, PipelineFailed, DataQualityError, DatasetError, SQLError,
])
def test_all_errors_derive_from_dsops_error(cls):
    assert issubclass(cls, dsops.DsopsError)


def test_builtin_bases_preserved():
    assert issubclass(dsops.ConfigError, ValueError)
    assert issubclass(dsops.LockNotAcquired, TimeoutError)


def test_bad_arguments_raise_value_error():
    with pytest.raises(ValueError):
        dsops.PartitionedWriter(".", mode="nope")
    with pytest.raises(ValueError):
        dsops.Database.sqlite().write_rows("t", [], mode="nope")
    with pytest.raises(ValueError):
        dsops.TokenBucket(rate=0)
