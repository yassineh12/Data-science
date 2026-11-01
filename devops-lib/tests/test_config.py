import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pytest

from dsops.config import ConfigError, Secret, env_to_dict, load_config, to_dict


@dataclass
class DB:
    host: str = "localhost"
    port: int = 5432
    password: Secret = field(default_factory=lambda: Secret(""))


@dataclass
class Settings:
    db: DB = field(default_factory=DB)
    workers: int = 4
    debug: bool = False
    tags: list[str] = field(default_factory=list)
    timeout: Optional[float] = None


def test_defaults():
    s = load_config(Settings)
    assert s.db.host == "localhost" and s.workers == 4 and s.timeout is None


def test_precedence_files_env_overrides(tmp_path: Path):
    toml = tmp_path / "base.toml"
    toml.write_text('workers = 8\n[db]\nhost = "db.internal"\nport = 6543\n')
    js = tmp_path / "prod.json"
    js.write_text(json.dumps({"db": {"port": 7000}, "debug": True}))
    env = {"APP_DB__HOST": "env-host", "APP_TAGS": "a, b,c", "APP_DB__PASSWORD": "hunter2", "OTHER": "x"}

    s = load_config(Settings, files=[toml, js], env_prefix="APP_", environ=env, overrides={"workers": 16})

    assert s.db.host == "env-host"        # env beats files
    assert s.db.port == 7000              # later file beats earlier
    assert s.debug is True
    assert s.workers == 16                # overrides beat everything
    assert s.tags == ["a", "b", "c"]
    assert s.db.password.get() == "hunter2"


def test_yaml(tmp_path: Path):
    pytest.importorskip("yaml")
    y = tmp_path / "c.yaml"
    y.write_text("workers: 2\ndb:\n  host: yhost\n")
    s = load_config(Settings, files=[y])
    assert s.workers == 2 and s.db.host == "yhost"


@pytest.mark.parametrize("raw,expected", [("true", True), ("0", False), ("YES", True), ("off", False)])
def test_bool_coercion(raw, expected):
    assert load_config(Settings, env_prefix="X_", environ={"X_DEBUG": raw}).debug is expected


def test_type_errors_are_reported_with_path():
    with pytest.raises(ConfigError, match="db.port"):
        load_config(Settings, overrides={"db": {"port": "not-a-number"}})
    with pytest.raises(ConfigError, match="debug"):
        load_config(Settings, overrides={"debug": "maybe"})


def test_unknown_keys_rejected():
    with pytest.raises(ConfigError, match="unknown"):
        load_config(Settings, overrides={"wrokers": 3})


def test_required_field_missing():
    @dataclass
    class Need:
        api_url: str

    with pytest.raises(ConfigError, match="api_url"):
        load_config(Need)


def test_secret_masked_everywhere():
    s = load_config(Settings, overrides={"db": {"password": "hunter2"}})
    assert "hunter2" not in repr(s)
    assert to_dict(s)["db"]["password"] == "Secret('********')"
    assert to_dict(s, reveal_secrets=True)["db"]["password"] == "hunter2"


def test_missing_and_optional_files(tmp_path: Path):
    with pytest.raises(ConfigError, match="cannot read"):
        load_config(Settings, files=[tmp_path / "nope.toml"])
    assert load_config(Settings, files=[tmp_path / "nope.toml"], optional_files=True).workers == 4


def test_env_to_dict_nesting():
    assert env_to_dict("APP_", {"APP_A__B__C": "1", "APP_X": "2"}) == {"a": {"b": {"c": "1"}}, "x": "2"}
