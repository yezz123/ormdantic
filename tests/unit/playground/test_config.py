from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from ormdantic.playground.config import (
    ConfigError,
    EnvironmentConfig,
    PlaygroundConfig,
    ProjectConfig,
    load_config,
    parse_config_source,
    resolve_database_url,
    write_config,
    write_config_source,
)

VALID_CONFIG = """\
[project]
target = "example.database:db"
migrations_dir = "schema/migrations"
format = "toml"
watch = ["app/**/*.py", "schema/migrations/**/*.toml"]
database_poll_seconds = 2.5
debounce_milliseconds = 125

[environments.development]
url_env = "DATABASE_URL"
env_file = ".env"
safety = "confirm"

[environments.production]
url_env = "PRODUCTION_DATABASE_URL"
env_file = ".env.production"
safety = "confirm"
production = true
"""


def test_discover_config_walks_to_nearest_parent(tmp_path: Path) -> None:
    config = importlib.import_module("ormdantic.playground.config")
    project = tmp_path / "project"
    nested = project / "src" / "package"
    nested.mkdir(parents=True)
    expected = project / "ormdantic.toml"
    expected.write_text("[project]\n")

    assert config.discover_config(nested) == expected


def test_load_config_selects_development_and_resolves_project_paths(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ormdantic.toml"
    path.write_text(VALID_CONFIG)

    effective = load_config(path)

    assert effective.root == tmp_path
    assert effective.project.target == "example.database:db"
    assert effective.project.migrations_dir == tmp_path / "schema/migrations"
    assert effective.project.format == "toml"
    assert effective.environment.name == "development"
    assert effective.environment.env_file == tmp_path / ".env"


def test_cli_values_override_project_and_selected_environment(tmp_path: Path) -> None:
    path = tmp_path / "ormdantic.toml"
    path.write_text(VALID_CONFIG)

    effective = load_config(
        path,
        environment="production",
        target="override.database:db",
        migrations_dir=Path("other/migrations"),
    )

    assert effective.project.target == "override.database:db"
    assert effective.project.migrations_dir == tmp_path / "other/migrations"
    assert effective.environment.name == "production"
    assert effective.environment.safety == "typed"
    assert effective.environment.production is True


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (VALID_CONFIG.replace('format = "toml"', 'format = "yaml"'), "project.format"),
        (
            VALID_CONFIG.replace(
                "debounce_milliseconds = 125", "debounce_milliseconds = -1"
            ),
            "project.debounce_milliseconds",
        ),
        (
            VALID_CONFIG.replace('safety = "confirm"', 'safety = "unsafe"', 1),
            "environments.development.safety",
        ),
        (VALID_CONFIG.replace('format = "toml"', "unknown = true"), "project.unknown"),
    ],
)
def test_load_config_rejects_invalid_values_with_exact_key(
    tmp_path: Path,
    payload: str,
    message: str,
) -> None:
    path = tmp_path / "ormdantic.toml"
    path.write_text(payload)

    with pytest.raises(ConfigError, match=message):
        load_config(path)


def test_load_config_rejects_an_unknown_environment(tmp_path: Path) -> None:
    path = tmp_path / "ormdantic.toml"
    path.write_text(VALID_CONFIG)

    with pytest.raises(ConfigError, match="environments.staging"):
        load_config(path, environment="staging")


def test_resolve_database_url_prefers_environment_without_leaking_value(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "ormdantic.toml"
    path.write_text(VALID_CONFIG)
    effective = load_config(path)
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:secret@localhost/app")

    resolved = resolve_database_url(effective.environment)

    assert resolved.value == "postgresql://user:secret@localhost/app"
    assert resolved.label == "DATABASE_URL"
    assert "secret" not in repr(resolved)
    assert "secret" not in str(resolved)


def test_resolve_database_url_falls_back_to_dotenv(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "ormdantic.toml"
    path.write_text(VALID_CONFIG)
    (tmp_path / ".env").write_text(
        "DATABASE_URL=sqlite:///playground.sqlite3 # local database\n"
    )
    monkeypatch.delenv("DATABASE_URL", raising=False)

    resolved = resolve_database_url(load_config(path).environment)

    assert resolved.value == "sqlite:///playground.sqlite3"
    assert resolved.label == f"{tmp_path / '.env'}:DATABASE_URL"


def test_write_config_round_trips_and_refuses_overwrite(tmp_path: Path) -> None:
    config = PlaygroundConfig(
        project=ProjectConfig(
            target="example.database:db",
            migrations_dir=Path("migrations"),
            watch=("app/**/*.py", "migrations/**/*.toml"),
            database_poll_seconds=3.0,
            debounce_milliseconds=200,
        ),
        environments={
            "development": EnvironmentConfig(
                name="development",
                url_env="DATABASE_URL",
                env_file=Path(".env"),
            ),
            "production": EnvironmentConfig(
                name="production",
                url_env="PRODUCTION_DATABASE_URL",
                env_file=None,
                safety="typed",
                production=True,
            ),
        },
    )
    path = tmp_path / "config" / "ormdantic.toml"

    write_config(path, config)
    source = path.read_text()
    effective = load_config(path)

    assert "[project]" in source
    assert "[environments.development]" in source
    assert "postgresql://" not in source
    assert effective.project.target == "example.database:db"
    assert effective.environment.url_env == "DATABASE_URL"
    with pytest.raises(FileExistsError):
        write_config(path, config)


def test_database_url_resolution_error_names_safe_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MISSING_DATABASE_URL", raising=False)
    monkeypatch.setenv(
        "UNRELATED_SECRET_URL",
        "postgresql://private:credential@internal.example/app",
    )
    environment = EnvironmentConfig(
        name="development",
        url_env="MISSING_DATABASE_URL",
        env_file=tmp_path / ".missing",
    )

    with pytest.raises(ConfigError) as exc_info:
        resolve_database_url(environment)

    message = str(exc_info.value)
    assert "MISSING_DATABASE_URL" in message
    assert str(tmp_path / ".missing") in message
    assert "private:credential" not in message


def test_write_config_source_validates_before_atomic_overwrite(
    tmp_path: Path,
) -> None:
    path = tmp_path / "ormdantic.toml"
    path.write_text(VALID_CONFIG)

    parsed = parse_config_source(VALID_CONFIG)
    assert parsed.project.target == "example.database:db"

    with pytest.raises(ConfigError, match="invalid TOML"):
        write_config_source(path, "[project\n")
    assert path.read_text() == VALID_CONFIG

    updated = VALID_CONFIG.replace(
        "database_poll_seconds = 2.5", "database_poll_seconds = 3.5"
    )
    write_config_source(path, updated)

    assert path.read_text() == updated
    assert load_config(path).project.database_poll_seconds == 3.5
