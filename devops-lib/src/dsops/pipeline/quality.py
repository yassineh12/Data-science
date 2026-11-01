"""Record-level schema validation and dataset-level data-quality gates.

Works on any iterable of dicts. A pandas DataFrame is accepted too (it is read
via ``to_dict("records")``), but pandas is never imported here.

::

    schema = Schema([
        Field("order_id", int, nullable=False, unique=True),
        Field("email", str, checks=[matches(r"^[^@]+@[^@]+$")]),
        Field("amount", float, checks=[in_range(0, 1_000_000)]),
        Field("status", str, checks=[one_of({"new", "paid", "refunded"})]),
    ])
    report = validate_records(rows, schema, min_rows=1)
    report.raise_if(max_error_rate=0.01)   # fail the pipeline if >1% of rows are bad
    load(report.valid)                     # quarantine report.invalid elsewhere
"""

from __future__ import annotations

import re
from collections import Counter as _Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

Check = Callable[[Any], "str | None"]  # returns an error message, or None if the value is fine


class DataQualityError(Exception):
    def __init__(self, message: str, report: "QualityReport"):
        super().__init__(message)
        self.report = report


# ------------------------------------------------------------------- checks


def in_range(lo: float | None = None, hi: float | None = None) -> Check:
    def check(v: Any) -> str | None:
        if lo is not None and v < lo:
            return f"{v!r} < {lo}"
        if hi is not None and v > hi:
            return f"{v!r} > {hi}"
        return None

    return check


def one_of(allowed: Iterable[Any]) -> Check:
    allowed = frozenset(allowed)

    def check(v: Any) -> str | None:
        return None if v in allowed else f"{v!r} not in {sorted(map(str, allowed))}"

    return check


def matches(pattern: str) -> Check:
    rx = re.compile(pattern)

    def check(v: Any) -> str | None:
        return None if isinstance(v, str) and rx.search(v) else f"{v!r} does not match /{pattern}/"

    return check


def length(min_len: int = 0, max_len: int | None = None) -> Check:
    def check(v: Any) -> str | None:
        n = len(v)
        if n < min_len or (max_len is not None and n > max_len):
            return f"length {n} outside [{min_len}, {max_len if max_len is not None else '∞'}]"
        return None

    return check


def predicate(fn: Callable[[Any], bool], message: str) -> Check:
    return lambda v: None if fn(v) else message


# ------------------------------------------------------------------- schema


@dataclass
class Field:
    name: str
    type: type | tuple[type, ...] | None = None
    required: bool = True
    nullable: bool = True
    unique: bool = False
    checks: list[Check] = field(default_factory=list)
    coerce: bool = False

    def _type_ok(self, v: Any) -> bool:
        if self.type is None:
            return True
        types = self.type if isinstance(self.type, tuple) else (self.type,)
        if isinstance(v, bool) and bool not in types:
            return False  # bool is an int subclass; don't let True pass as an int
        if float in types and isinstance(v, int) and not isinstance(v, bool):
            return True
        return isinstance(v, types)


@dataclass
class Schema:
    fields: list[Field]
    allow_extra: bool = True

    def __post_init__(self) -> None:
        names = [f.name for f in self.fields]
        dupes = [n for n, c in _Counter(names).items() if c > 1]
        if dupes:
            raise ValueError(f"duplicate field(s) {dupes}")


@dataclass
class RowError:
    index: int
    record: Mapping[str, Any]
    errors: dict[str, str]


@dataclass
class QualityReport:
    total: int
    valid: list[dict[str, Any]]
    invalid: list[RowError]
    field_errors: dict[str, int]
    dataset_errors: list[str]

    @property
    def error_rate(self) -> float:
        return len(self.invalid) / self.total if self.total else 0.0

    @property
    def passed(self) -> bool:
        return not self.invalid and not self.dataset_errors

    def raise_if(self, *, max_error_rate: float = 0.0) -> "QualityReport":
        if self.dataset_errors:
            raise DataQualityError("; ".join(self.dataset_errors), self)
        if self.error_rate > max_error_rate:
            top = ", ".join(f"{k}={v}" for k, v in sorted(self.field_errors.items(), key=lambda kv: -kv[1])[:5])
            raise DataQualityError(
                f"{len(self.invalid)}/{self.total} rows invalid ({self.error_rate:.2%} > {max_error_rate:.2%}); "
                f"top fields: {top}", self)
        return self

    def summary(self) -> dict[str, Any]:
        return {"total": self.total, "valid": len(self.valid), "invalid": len(self.invalid),
                "error_rate": round(self.error_rate, 6), "field_errors": dict(self.field_errors),
                "dataset_errors": list(self.dataset_errors)}


def validate_records(
    records: Iterable[Mapping[str, Any]] | Any,
    schema: Schema,
    *,
    min_rows: int | None = None,
    max_rows: int | None = None,
    max_null_rate: Mapping[str, float] | None = None,
) -> QualityReport:
    if hasattr(records, "to_dict") and not isinstance(records, Mapping):
        records = records.to_dict("records")

    valid: list[dict[str, Any]] = []
    invalid: list[RowError] = []
    field_errors: _Counter[str] = _Counter()
    nulls: _Counter[str] = _Counter()
    seen: dict[str, dict[Any, int]] = {f.name: {} for f in schema.fields if f.unique}
    known = {f.name for f in schema.fields}
    total = 0

    for idx, rec in enumerate(records):
        total += 1
        row = dict(rec)
        errs: dict[str, str] = {}

        if not schema.allow_extra:
            for extra in sorted(set(row) - known):
                errs[extra] = "unexpected field"

        for f in schema.fields:
            if f.name not in row:
                if f.required:
                    errs[f.name] = "missing"
                continue
            v = row[f.name]
            if v is None:
                nulls[f.name] += 1
                if not f.nullable:
                    errs[f.name] = "null not allowed"
                continue
            if f.coerce and f.type is not None and not f._type_ok(v):
                target = f.type[0] if isinstance(f.type, tuple) else f.type
                try:
                    v = row[f.name] = target(v)
                except (TypeError, ValueError):
                    pass
            if not f._type_ok(v):
                errs[f.name] = f"expected {getattr(f.type, '__name__', f.type)}, got {type(v).__name__}"
                continue
            for check in f.checks:
                try:
                    msg = check(v)
                except Exception as exc:  # noqa: BLE001 - a broken check flags the row, not the run
                    msg = f"check raised {type(exc).__name__}: {exc}"
                if msg:
                    errs[f.name] = msg
                    break
            if f.unique and f.name not in errs:
                first = seen[f.name].setdefault(v, idx)
                if first != idx:
                    errs[f.name] = f"duplicate of row {first}"

        if errs:
            invalid.append(RowError(idx, rec, errs))
            field_errors.update(errs.keys())
        else:
            valid.append(row)

    dataset_errors: list[str] = []
    if min_rows is not None and total < min_rows:
        dataset_errors.append(f"row count {total} < min_rows {min_rows}")
    if max_rows is not None and total > max_rows:
        dataset_errors.append(f"row count {total} > max_rows {max_rows}")
    for name, limit in (max_null_rate or {}).items():
        rate = nulls[name] / total if total else 0.0
        if rate > limit:
            dataset_errors.append(f"null rate of {name!r} {rate:.2%} > {limit:.2%}")

    return QualityReport(total, valid, invalid, dict(field_errors), dataset_errors)
