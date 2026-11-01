"""Layered, typed configuration — the twelve-factor way.

Precedence (lowest to highest)::

    dataclass defaults  <  config files (in order)  <  environment  <  explicit overrides

Nested dataclasses map to nested sections. Environment variables use a prefix and
a double underscore for nesting: with ``env_prefix="APP_"``, ``APP_DB__HOST``
sets ``config.db.host``.

Example::

    @dataclass
    class DB:
        host: str = "localhost"
        port: int = 5432
        password: Secret = Secret("")

    @dataclass
    class Settings:
        db: DB = field(default_factory=DB)
        workers: int = 4
        debug: bool = False

    settings = load_config(Settings, files=["config.toml"], env_prefix="APP_")
"""

from __future__ import annotations

import dataclasses
import json
import os
import types
import typing
from pathlib import Path
from typing import Any, Mapping, Sequence, TypeVar, Union

from .errors import DsopsError

T = TypeVar("T")

_TRUE = {"1", "true", "yes", "on", "y", "t"}
_FALSE = {"0", "false", "no", "off", "n", "f", ""}


class ConfigError(DsopsError, ValueError):
    """Raised for unreadable files, missing required values or values of the wrong type."""


class Secret:
    """A string that never shows up in logs, reprs or tracebacks. Call ``.get()`` to read it."""

    __slots__ = ("_value",)

    def __init__(self, value: str):
        self._value = value

    def get(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "Secret('********')" if self._value else "Secret('')"

    __str__ = __repr__

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Secret) and other._value == self._value

    def __hash__(self) -> int:
        return hash(self._value)

    def __bool__(self) -> bool:
        return bool(self._value)


# --------------------------------------------------------------------------- #
# Sources
# --------------------------------------------------------------------------- #


def read_file(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Read a JSON, TOML or YAML file into a dict."""
    p = Path(path)
    try:
        text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config file {p}: {exc}") from exc

    suffix = p.suffix.lower()
    try:
        if suffix == ".json":
            data = json.loads(text)
        elif suffix == ".toml":
            import tomllib  # py3.11+

            data = tomllib.loads(text)
        elif suffix in (".yaml", ".yml"):
            try:
                import yaml  # type: ignore[import-untyped]
            except ImportError as exc:  # pragma: no cover - depends on env
                raise ImportError("YAML config needs PyYAML: pip install 'dsops[yaml]'") from exc
            data = yaml.safe_load(text) or {}
        else:
            raise ConfigError(f"unsupported config format {suffix!r} for {p}")
    except (ConfigError, ImportError):
        raise
    except Exception as exc:
        raise ConfigError(f"cannot parse config file {p}: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError(f"top level of {p} must be a mapping, got {type(data).__name__}")
    return data


def env_to_dict(prefix: str, environ: Mapping[str, str] | None = None) -> dict[str, Any]:
    """``APP_DB__HOST=x`` -> ``{"db": {"host": "x"}}`` (keys lower-cased)."""
    environ = os.environ if environ is None else environ
    out: dict[str, Any] = {}
    plen = len(prefix)
    for key, value in environ.items():
        if not key.upper().startswith(prefix.upper()) or len(key) == plen:
            continue
        parts = [part.lower() for part in key[plen:].split("__") if part]
        node = out
        for part in parts[:-1]:
            nxt = node.setdefault(part, {})
            if not isinstance(nxt, dict):
                nxt = node[part] = {}
            node = nxt
        node[parts[-1]] = value
    return out


def deep_merge(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(out.get(key), Mapping):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = value
    return out


# --------------------------------------------------------------------------- #
# Coercion into dataclasses
# --------------------------------------------------------------------------- #


def _coerce(value: Any, tp: Any, path: str) -> Any:
    origin = typing.get_origin(tp)
    args = typing.get_args(tp)

    if tp is Any:
        return value

    if origin in (Union, types.UnionType):
        if value is None and type(None) in args:
            return None
        errors = []
        for arg in (a for a in args if a is not type(None)):
            try:
                return _coerce(value, arg, path)
            except ConfigError as exc:
                errors.append(str(exc))
        raise ConfigError(f"{path}: {value!r} matches none of {tp}: {'; '.join(errors)}")

    if dataclasses.is_dataclass(tp) and isinstance(tp, type):
        if isinstance(value, tp):
            return value
        if not isinstance(value, Mapping):
            raise ConfigError(f"{path}: expected a section (mapping), got {value!r}")
        return _build(tp, value, path)

    if tp is Secret:
        return value if isinstance(value, Secret) else Secret(str(value))

    if tp is bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        if isinstance(value, str) and value.strip().lower() in _TRUE | _FALSE:
            return value.strip().lower() in _TRUE
        raise ConfigError(f"{path}: expected bool, got {value!r}")

    if tp in (int, float):
        if isinstance(value, bool):
            raise ConfigError(f"{path}: expected {tp.__name__}, got bool")
        try:
            result = tp(value)
        except (TypeError, ValueError) as exc:
            raise ConfigError(f"{path}: expected {tp.__name__}, got {value!r}") from exc
        if tp is int and isinstance(value, float) and value != result:
            raise ConfigError(f"{path}: expected int, got non-integral {value!r}")
        return result

    if tp is str:
        if isinstance(value, (Mapping, list, tuple)):
            raise ConfigError(f"{path}: expected str, got {type(value).__name__}")
        return str(value)

    if tp is Path:
        return Path(value)

    if origin in (list, tuple, set, frozenset) or tp in (list, tuple, set, frozenset):
        container = origin or tp
        if isinstance(value, str):
            value = [v.strip() for v in value.split(",") if v.strip()]
        if not isinstance(value, (list, tuple, set, frozenset)):
            raise ConfigError(f"{path}: expected a list, got {value!r}")
        if container is tuple and args and not (len(args) == 2 and args[1] is Ellipsis):
            if len(args) != len(value):
                raise ConfigError(f"{path}: expected {len(args)} items, got {len(value)}")
            return tuple(_coerce(v, a, f"{path}[{i}]") for i, (v, a) in enumerate(zip(args, value)))
        item_tp = args[0] if args else Any
        return container(_coerce(v, item_tp, f"{path}[{i}]") for i, v in enumerate(value))

    if origin is dict or tp is dict:
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ConfigError(f"{path}: expected a mapping or JSON object string") from exc
        if not isinstance(value, Mapping):
            raise ConfigError(f"{path}: expected a mapping, got {value!r}")
        kt, vt = args if args else (Any, Any)
        return {_coerce(k, kt, path): _coerce(v, vt, f"{path}.{k}") for k, v in value.items()}

    if isinstance(tp, type):
        if isinstance(value, tp):
            return value
        try:
            return tp(value)
        except Exception as exc:
            raise ConfigError(f"{path}: cannot convert {value!r} to {tp.__name__}") from exc

    return value


def _build(cls: type[T], data: Mapping[str, Any], prefix: str = "") -> T:
    hints = typing.get_type_hints(cls)
    known = {f.name for f in dataclasses.fields(cls)}  # type: ignore[arg-type]
    unknown = set(data) - known
    if unknown:
        where = prefix or cls.__name__
        raise ConfigError(f"{where}: unknown key(s) {sorted(unknown)}")

    kwargs: dict[str, Any] = {}
    for f in dataclasses.fields(cls):  # type: ignore[arg-type]
        path = f"{prefix}.{f.name}" if prefix else f.name
        if f.name in data:
            kwargs[f.name] = _coerce(data[f.name], hints[f.name], path)
        elif f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
            tp = hints[f.name]
            if dataclasses.is_dataclass(tp):
                kwargs[f.name] = _build(tp, {}, path)  # type: ignore[arg-type]
            else:
                raise ConfigError(f"{path}: required value is missing")
    return cls(**kwargs)


def load_config(
    cls: type[T],
    *,
    files: Sequence[str | os.PathLike[str]] = (),
    env_prefix: str | None = None,
    environ: Mapping[str, str] | None = None,
    overrides: Mapping[str, Any] | None = None,
    optional_files: bool = False,
) -> T:
    """Build an instance of the dataclass ``cls`` from every configured source."""
    if not (dataclasses.is_dataclass(cls) and isinstance(cls, type)):
        raise TypeError("load_config expects a dataclass type")

    merged: dict[str, Any] = {}
    for path in files:
        if optional_files and not Path(path).exists():
            continue
        merged = deep_merge(merged, read_file(path))
    if env_prefix:
        merged = deep_merge(merged, env_to_dict(env_prefix, environ))
    if overrides:
        merged = deep_merge(merged, overrides)
    return _build(cls, merged)


def to_dict(obj: Any, *, reveal_secrets: bool = False) -> Any:
    """Dataclass -> plain dict, with secrets masked unless ``reveal_secrets``."""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {f.name: to_dict(getattr(obj, f.name), reveal_secrets=reveal_secrets)
                for f in dataclasses.fields(obj)}
    if isinstance(obj, Secret):
        return obj.get() if reveal_secrets else repr(obj)
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_dict(v, reveal_secrets=reveal_secrets) for v in obj]
    if isinstance(obj, dict):
        return {k: to_dict(v, reveal_secrets=reveal_secrets) for k, v in obj.items()}
    if isinstance(obj, Path):
        return str(obj)
    return obj
