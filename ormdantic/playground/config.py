"""Project configuration for the Ormdantic playground."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Literal, cast

from ormdantic._migrations.documents import toml_key, toml_loads, toml_value

CONFIG_NAME = "ormdantic.toml"
DEFAULT_WATCH = (
    "app/**/*.py",
    "migrations/**/*.toml",
    "migrations/**/*.json",
)
PROJECT_KEYS = {
    "target",
    "migrations_dir",
    "format",
    "watch",
    "database_poll_seconds",
    "debounce_milliseconds",
}
ENVIRONMENT_KEYS = {"url_env", "env_file", "safety", "production"}


class ConfigError(ValueError):
    """Raised when playground configuration is invalid or incomplete."""


@dataclass(frozen=True)
class ProjectConfig:
    """Project-wide playground settings."""

    target: str | None = None
    migrations_dir: Path = Path("migrations")
    format: Literal["toml", "json"] = "toml"
    watch: tuple[str, ...] = DEFAULT_WATCH
    database_poll_seconds: float = 5.0
    debounce_milliseconds: int = 300


@dataclass(frozen=True)
class EnvironmentConfig:
    """Connection lookup and safety settings for a named environment."""

    name: str
    url_env: str = "DATABASE_URL"
    env_file: Path | None = Path(".env")
    safety: Literal["confirm", "typed"] = "confirm"
    production: bool = False


@dataclass(frozen=True)
class PlaygroundConfig:
    """Serializable playground configuration."""

    project: ProjectConfig
    environments: Mapping[str, EnvironmentConfig]


@dataclass(frozen=True)
class EffectiveConfig:
    """A selected environment with all filesystem paths resolved."""

    path: Path
    root: Path
    project: ProjectConfig
    environment: EnvironmentConfig


@dataclass(frozen=True, repr=False)
class DatabaseUrlSource:
    """A secret database URL and its non-secret source label."""

    value: str
    label: str

    def __repr__(self) -> str:
        return f"DatabaseUrlSource(value='<redacted>', label={self.label!r})"

    def __str__(self) -> str:
        return f"<redacted> (source: {self.label})"


def discover_config(start: Path) -> Path | None:
    """Find the nearest playground configuration at or above ``start``."""
    current = start.resolve()
    if current.is_file():
        current = current.parent
    for directory in (current, *current.parents):
        candidate = directory / CONFIG_NAME
        if candidate.is_file():
            return candidate
    return None


def load_config(
    path: Path,
    *,
    environment: str | None = None,
    target: str | None = None,
    migrations_dir: Path | None = None,
) -> EffectiveConfig:
    """Load, validate, select, and resolve a playground configuration."""
    config_path = path.resolve()
    config = parse_config(config_path)
    environment_name = environment or "development"
    selected = config.environments.get(environment_name)
    if selected is None:
        raise ConfigError(
            f"environments.{environment_name}: named environment is not configured"
        )

    root = config_path.parent
    project = config.project
    effective_target = _optional_nonempty_string(
        target if target is not None else project.target,
        "project.target",
    )
    if effective_target is None:
        raise ConfigError("project.target: import target is required")
    migration_path = migrations_dir or project.migrations_dir
    resolved_project = replace(
        project,
        target=effective_target,
        migrations_dir=_resolve_path(root, migration_path),
    )
    resolved_environment = replace(
        selected,
        env_file=(
            _resolve_path(root, selected.env_file)
            if selected.env_file is not None
            else None
        ),
        safety="typed" if selected.production else selected.safety,
    )
    return EffectiveConfig(
        path=config_path,
        root=root,
        project=resolved_project,
        environment=resolved_environment,
    )


def parse_config(path: Path) -> PlaygroundConfig:
    """Parse a configuration while retaining its relative paths."""
    try:
        source = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise
    return parse_config_source(source, source_name=str(path))


def parse_config_source(
    source: str,
    *,
    source_name: str = "ormdantic.toml",
) -> PlaygroundConfig:
    """Parse and validate an in-memory playground TOML document."""
    try:
        payload = toml_loads(source)
    except Exception as exc:
        raise ConfigError(f"{source_name}: invalid TOML: {exc}") from exc
    _reject_unknown(payload, {"project", "environments"}, "")
    project_payload = _table(payload.get("project"), "project")
    environments_payload = _table(payload.get("environments"), "environments")
    if not environments_payload:
        raise ConfigError("environments: configure at least one named environment")
    project = _parse_project(project_payload)
    environments = {
        str(name): _parse_environment(
            str(name),
            _table(value, f"environments.{name}"),
        )
        for name, value in environments_payload.items()
    }
    return PlaygroundConfig(project=project, environments=environments)


def write_config_source(path: Path, source: str) -> None:
    """Validate then atomically replace an existing playground TOML file."""
    destination = path.resolve()
    parse_config_source(source, source_name=str(destination))
    _atomic_write(destination, source)


def write_config(
    path: Path,
    config: PlaygroundConfig,
    *,
    overwrite: bool = False,
) -> None:
    """Atomically write a canonical playground TOML file."""
    destination = path.resolve()
    if destination.exists() and not overwrite:
        raise FileExistsError(destination)
    source = _config_toml(config)
    _atomic_write(destination, source)


def _atomic_write(destination: Path, source: str) -> None:
    """Write a complete text file with an fsync and same-directory replace."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(source)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(destination)


def resolve_database_url(environment: EnvironmentConfig) -> DatabaseUrlSource:
    """Resolve a URL from the process environment and then its dotenv file."""
    env_value = _clean_value(os.environ.get(environment.url_env))
    if env_value is not None:
        return DatabaseUrlSource(env_value, environment.url_env)
    dotenv_values = _read_env_file(environment.env_file)
    dotenv_value = _clean_value(dotenv_values.get(environment.url_env))
    if dotenv_value is not None:
        return DatabaseUrlSource(
            dotenv_value,
            f"{environment.env_file}:{environment.url_env}",
        )
    raise ConfigError(
        f"environments.{environment.name}.url_env: export {environment.url_env} "
        f"or define it in {environment.env_file or '.env'}"
    )


def _parse_project(payload: Mapping[str, Any]) -> ProjectConfig:
    _reject_unknown(payload, PROJECT_KEYS, "project")
    format_value = _string(payload.get("format", "toml"), "project.format")
    if format_value not in {"toml", "json"}:
        raise ConfigError("project.format: expected 'toml' or 'json'")
    migrations_dir = Path(
        _string(payload.get("migrations_dir", "migrations"), "project.migrations_dir")
    )
    watch_value = payload.get("watch", DEFAULT_WATCH)
    if not isinstance(watch_value, list | tuple) or not all(
        isinstance(item, str) and item.strip() for item in watch_value
    ):
        raise ConfigError("project.watch: expected an array of non-empty strings")
    poll = _number(
        payload.get("database_poll_seconds", 5.0),
        "project.database_poll_seconds",
    )
    if poll <= 0:
        raise ConfigError("project.database_poll_seconds: expected a positive number")
    debounce = _integer(
        payload.get("debounce_milliseconds", 300),
        "project.debounce_milliseconds",
    )
    if debounce < 0:
        raise ConfigError("project.debounce_milliseconds: expected zero or greater")
    return ProjectConfig(
        target=_optional_nonempty_string(payload.get("target"), "project.target"),
        migrations_dir=migrations_dir,
        format=cast(Literal["toml", "json"], format_value),
        watch=tuple(str(item) for item in watch_value),
        database_poll_seconds=float(poll),
        debounce_milliseconds=debounce,
    )


def _parse_environment(
    name: str,
    payload: Mapping[str, Any],
) -> EnvironmentConfig:
    if not name.strip():
        raise ConfigError("environments: environment names cannot be empty")
    _reject_unknown(payload, ENVIRONMENT_KEYS, f"environments.{name}")
    url_env = _string(
        payload.get("url_env", "DATABASE_URL"),
        f"environments.{name}.url_env",
    )
    env_file_value = payload.get("env_file", ".env")
    env_file = (
        None
        if env_file_value is None
        else Path(_string(env_file_value, f"environments.{name}.env_file"))
    )
    safety = _string(
        payload.get("safety", "confirm"),
        f"environments.{name}.safety",
    )
    if safety not in {"confirm", "typed"}:
        raise ConfigError(f"environments.{name}.safety: expected 'confirm' or 'typed'")
    production = _boolean(
        payload.get("production", False),
        f"environments.{name}.production",
    )
    return EnvironmentConfig(
        name=name,
        url_env=url_env,
        env_file=env_file,
        safety=cast(Literal["confirm", "typed"], safety),
        production=production,
    )


def _config_toml(config: PlaygroundConfig) -> str:
    project = config.project
    lines = [
        "[project]",
        *_toml_assignments(
            {
                "target": project.target,
                "migrations_dir": str(project.migrations_dir),
                "format": project.format,
                "watch": list(project.watch),
                "database_poll_seconds": project.database_poll_seconds,
                "debounce_milliseconds": project.debounce_milliseconds,
            }
        ),
    ]
    for name, environment in config.environments.items():
        lines.extend(
            [
                "",
                f"[environments.{toml_key(name)}]",
                *_toml_assignments(
                    {
                        "url_env": environment.url_env,
                        "env_file": (
                            str(environment.env_file)
                            if environment.env_file is not None
                            else None
                        ),
                        "safety": environment.safety,
                        "production": environment.production,
                    }
                ),
            ]
        )
    return "\n".join(lines) + "\n"


def _toml_assignments(payload: Mapping[str, Any]) -> list[str]:
    return [
        f"{toml_key(key)} = {toml_value(value)}"
        for key, value in payload.items()
        if value is not None
    ]


def _reject_unknown(
    payload: Mapping[str, Any],
    allowed: set[str],
    prefix: str,
) -> None:
    unknown = sorted(set(payload) - allowed)
    if unknown:
        key = f"{prefix}.{unknown[0]}" if prefix else unknown[0]
        raise ConfigError(f"{key}: unknown configuration key")


def _table(value: Any, key: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigError(f"{key}: expected a TOML table")
    return cast(Mapping[str, Any], value)


def _string(value: Any, key: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key}: expected a non-empty string")
    return value.strip()


def _optional_nonempty_string(value: Any, key: str) -> str | None:
    if value is None:
        return None
    return _string(value, key)


def _number(value: Any, key: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigError(f"{key}: expected a number")
    return float(value)


def _integer(value: Any, key: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{key}: expected an integer")
    return value


def _boolean(value: Any, key: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{key}: expected a boolean")
    return value


def _resolve_path(root: Path, value: Path) -> Path:
    return value.resolve() if value.is_absolute() else (root / value).resolve()


def _clean_value(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _read_env_file(path: Path | None) -> dict[str, str]:
    if path is None or not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, separator, value = line.partition("=")
        if not separator or not key.strip():
            continue
        values[key.strip()] = _parse_env_value(value)
    return values


def _parse_env_value(value: str) -> str:
    cleaned = value.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"'", '"'}:
        return cleaned[1:-1]
    if " #" in cleaned:
        cleaned = cleaned.split(" #", 1)[0].rstrip()
    return cleaned
