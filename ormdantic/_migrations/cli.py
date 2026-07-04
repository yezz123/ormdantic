"""Typer commands for migration workflows."""

from __future__ import annotations

import asyncio
import importlib
import os
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Callable, NoReturn, TypeVar

import typer

from ormdantic import Ormdantic
from ormdantic.errors import OrmdanticError
from ormdantic.migrations import (
    MIGRATION_TABLE,
    MigrationArtifact,
    MigrationPlan,
    SchemaSnapshot,
    create_migration_artifact,
    squash_migrations,
)

migrations_app = typer.Typer(
    help="Create, review, apply, and squash migration artifacts.",
    no_args_is_help=True,
)

T = TypeVar("T")
DEFAULT_URL_ENV = "DATABASE_URL"
DEFAULT_ENV_FILE = Path(".env")


@dataclass(frozen=True)
class ResolvedDatabaseUrl:
    """A database URL plus the place it was read from."""

    value: str
    source: str


class MigrationCliError(ValueError):
    """Raised for user-facing migration CLI input errors."""


def _database_url_argument() -> Any:
    return typer.Argument(
        help=(
            "Database URL. Optional when --url, DATABASE_URL, or a .env file provides it."
        )
    )


def _database_url_option() -> Any:
    return typer.Option(
        "--url",
        help="Database URL. Overrides positional URL, environment, and .env.",
    )


def _url_env_option() -> Any:
    return typer.Option(
        "--url-env",
        help="Environment variable to read when --url is not passed.",
    )


def _env_file_option() -> Any:
    return typer.Option(
        "--env-file",
        help="Read the database URL from this .env file when the environment is unset.",
    )


@migrations_app.command("snapshot")
def snapshot_command(
    target: Annotated[
        str,
        typer.Argument(help="Import path like package.module:db."),
    ],
    out: Annotated[
        Path,
        typer.Option("--out", "-o", help="Output snapshot file."),
    ],
    format: Annotated[
        str | None,
        typer.Option("--format", "-f", help="Output format: json or toml."),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option("--interactive", "-i", help="Prompt before overwriting files."),
    ] = False,
) -> None:
    """Export a schema snapshot from a module-level database object."""
    database = _load_database(target)
    _confirm_overwrite(out, interactive)
    database.migrations.snapshot().write(out, format=format)
    typer.echo(str(out))


@migrations_app.command("init")
def init_command(
    url: Annotated[str | None, _database_url_argument()] = None,
    url_option: Annotated[str | None, _database_url_option()] = None,
    url_env: Annotated[str, _url_env_option()] = DEFAULT_URL_ENV,
    env_file: Annotated[Path | None, _env_file_option()] = DEFAULT_ENV_FILE,
) -> None:
    """Initialize migration history metadata on a database."""
    resolved = _run_cli_action(
        lambda: _resolve_database_url(
            url,
            url_option,
            url_env=url_env,
            env_file=env_file,
        )
    )

    async def init_history() -> None:
        database = Ormdantic(resolved.value)
        await database.migrations.ensure_revision_table()

    _run_cli(init_history)
    typer.echo("Initialized Ormdantic migration history.")
    typer.echo(f"History table: {MIGRATION_TABLE}")
    _echo_connection(resolved)


@migrations_app.command("create")
def create_command(
    revision: Annotated[str, typer.Argument(help="Migration revision identifier.")],
    from_snapshot: Annotated[
        Path,
        typer.Option("--from", help="Source schema snapshot."),
    ],
    to_snapshot: Annotated[
        Path,
        typer.Option("--to", help="Target schema snapshot."),
    ],
    dialect: Annotated[str, typer.Option("--dialect", help="SQL dialect or URL.")],
    out: Annotated[
        Path,
        typer.Option("--out", "-o", help="Output migration artifact."),
    ],
    format: Annotated[
        str | None,
        typer.Option("--format", "-f", help="Output format: json or toml."),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option("--interactive", "-i", help="Prompt before overwriting files."),
    ] = False,
    message: Annotated[
        str | None,
        typer.Option("--message", "-m", help="Migration description."),
    ] = None,
    depends_on: Annotated[
        list[str] | None,
        typer.Option("--depends-on", help="Dependency revision."),
    ] = None,
    branch_label: Annotated[
        list[str] | None,
        typer.Option("--branch-label", help="Optional branch labels."),
    ] = None,
) -> None:
    """Create a migration artifact from two snapshots."""
    artifact = create_migration_artifact(
        revision,
        SchemaSnapshot.read(from_snapshot),
        SchemaSnapshot.read(to_snapshot),
        dialect=dialect,
        description=message,
        depends_on=depends_on,
        branch_labels=branch_label,
    )
    _echo_warnings(artifact)
    _confirm_overwrite(out, interactive)
    artifact.write(out, format=format)
    typer.echo(str(out))


@migrations_app.command("preview")
def preview_command(
    artifact: Annotated[Path, typer.Argument(help="Migration artifact file.")],
    rollback: Annotated[
        bool,
        typer.Option("--rollback", help="Preview rollback SQL."),
    ] = False,
) -> None:
    """Print SQL from a migration artifact."""
    migration = MigrationArtifact.read(artifact)
    statements = migration.rollback_operations if rollback else migration.operations
    typer.echo(f"# revision: {migration.revision}")
    if migration.dialect:
        typer.echo(f"# dialect: {migration.dialect}")
    typer.echo(
        "# safety: "
        f"unsafe={migration.to_plan().has_unsafe_operations} "
        f"destructive={migration.to_plan().has_destructive_operations} "
        f"requires_rebuild={migration.safety.get('requires_rebuild', False)}"
    )
    for warning in migration.warnings:
        typer.secho(f"# warning: {warning.message}", fg=typer.colors.YELLOW)
    for statement in statements:
        typer.echo(statement.sql)


@migrations_app.command("autogenerate")
def autogenerate_command(
    target: Annotated[
        str,
        typer.Argument(help="Import path like package.module:db."),
    ],
    revision: Annotated[str, typer.Argument(help="Migration revision identifier.")],
    out: Annotated[
        Path,
        typer.Option("--out", "-o", help="Output migration artifact."),
    ],
    message: Annotated[
        str | None,
        typer.Option("--message", "-m", help="Migration description."),
    ] = None,
    include_table: Annotated[
        list[str] | None,
        typer.Option("--include-table", help="Glob for included table names."),
    ] = None,
    exclude_table: Annotated[
        list[str] | None,
        typer.Option("--exclude-table", help="Glob for excluded table names."),
    ] = None,
    schema: Annotated[
        str | None,
        typer.Option("--schema", help="Optional schema/namespace."),
    ] = None,
    format: Annotated[
        str | None,
        typer.Option("--format", "-f", help="Output format: json or toml."),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option("--interactive", "-i", help="Prompt before overwriting files."),
    ] = False,
) -> None:
    """Generate migration artifact from live DB schema to current models."""
    database = _load_database(target)
    artifact = database.migrations.autogenerate(
        revision,
        description=message,
        include_tables=include_table,
        exclude_tables=exclude_table,
        schema=schema,
    )
    if artifact is None:
        typer.echo("no-op")
        return
    _echo_warnings(artifact)
    _confirm_overwrite(out, interactive)
    artifact.write(out, format=format)
    typer.echo(str(out))


@migrations_app.command("apply")
def apply_command(
    targets: Annotated[
        list[str] | None,
        typer.Argument(
            help=("Migration artifact file, or legacy DATABASE_URL plus artifact file.")
        ),
    ] = None,
    url_option: Annotated[str | None, _database_url_option()] = None,
    url_env: Annotated[str, _url_env_option()] = DEFAULT_URL_ENV,
    env_file: Annotated[Path | None, _env_file_option()] = DEFAULT_ENV_FILE,
    allow_destructive: Annotated[
        bool,
        typer.Option("--allow-destructive", help="Allow destructive migration SQL."),
    ] = False,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Prompt before applying destructive migration SQL.",
        ),
    ] = False,
) -> None:
    """Apply one migration artifact."""
    resolved, artifact = _run_cli_action(
        lambda: _resolve_database_url_and_path(
            targets or [],
            path_label="artifact",
            url_option=url_option,
            url_env=url_env,
            env_file=env_file,
        )
    )
    migration = _run_cli_action(lambda: _read_artifact_for_cli(artifact))
    allow = _confirm_destructive(migration, allow_destructive, interactive)

    async def apply_one() -> bool:
        database = Ormdantic(resolved.value)
        return await database.migrations.apply_artifact(
            migration,
            allow_destructive=allow,
        )

    applied = _run_cli(apply_one)
    if applied:
        typer.echo(f"Applied migration: {migration.revision}")
    else:
        typer.echo(f"Skipped migration: {migration.revision} is already applied.")
    _echo_connection(resolved)


@migrations_app.command("apply-dir")
def apply_dir_command(
    targets: Annotated[
        list[str] | None,
        typer.Argument(
            help=(
                "Directory containing migration artifacts, or legacy DATABASE_URL plus directory."
            )
        ),
    ] = None,
    url_option: Annotated[str | None, _database_url_option()] = None,
    url_env: Annotated[str, _url_env_option()] = DEFAULT_URL_ENV,
    env_file: Annotated[Path | None, _env_file_option()] = DEFAULT_ENV_FILE,
    pattern: Annotated[
        str | None,
        typer.Option(
            "--pattern",
            help="Glob pattern. Defaults to applying *.json and *.toml.",
        ),
    ] = None,
    allow_destructive: Annotated[
        bool,
        typer.Option("--allow-destructive", help="Allow destructive migration SQL."),
    ] = False,
) -> None:
    """Apply migration artifacts from a directory in filename order."""
    resolved, directory = _run_cli_action(
        lambda: _resolve_database_url_and_path(
            targets or [],
            path_label="directory",
            url_option=url_option,
            url_env=url_env,
            env_file=env_file,
        )
    )

    async def apply_many() -> list[str]:
        database = Ormdantic(resolved.value)
        return await database.migrations.apply_directory(
            directory,
            pattern=pattern,
            allow_destructive=allow_destructive,
        )

    applied = _run_cli(apply_many)
    if applied:
        noun = "migration" if len(applied) == 1 else "migrations"
        typer.echo(f"Applied {len(applied)} {noun} from {directory}.")
        for revision in applied:
            typer.echo(f"- {revision}")
    else:
        typer.echo(f"No pending migrations in {directory}.")
    _echo_connection(resolved)


@migrations_app.command("status")
def status_command(
    url: Annotated[str | None, _database_url_argument()] = None,
    url_option: Annotated[str | None, _database_url_option()] = None,
    url_env: Annotated[str, _url_env_option()] = DEFAULT_URL_ENV,
    env_file: Annotated[Path | None, _env_file_option()] = DEFAULT_ENV_FILE,
) -> None:
    """Print migration dirty state and current revision."""
    resolved = _run_cli_action(
        lambda: _resolve_database_url(
            url,
            url_option,
            url_env=url_env,
            env_file=env_file,
        )
    )

    async def fetch_status() -> dict[str, Any]:
        database = Ormdantic(resolved.value)
        return await database.migrations.status()

    status = _run_cli(fetch_status)
    typer.echo("Migration status")
    typer.echo(f"Dirty: {'yes' if status['dirty'] else 'no'}")
    typer.echo(f"Current revision: {status['current'] or 'none'}")
    typer.echo(f"Applied revisions: {len(status['applied'])}")
    _echo_connection(resolved)


@migrations_app.command("history")
def history_command(
    url: Annotated[str | None, _database_url_argument()] = None,
    url_option: Annotated[str | None, _database_url_option()] = None,
    url_env: Annotated[str, _url_env_option()] = DEFAULT_URL_ENV,
    env_file: Annotated[Path | None, _env_file_option()] = DEFAULT_ENV_FILE,
) -> None:
    """Print migration revision history."""
    resolved = _run_cli_action(
        lambda: _resolve_database_url(
            url,
            url_option,
            url_env=url_env,
            env_file=env_file,
        )
    )

    async def fetch_history() -> list[Any]:
        database = Ormdantic(resolved.value)
        return await database.migrations.history()

    history = _run_cli(fetch_history)
    typer.echo("Migration history")
    _echo_connection(resolved)
    if not history:
        typer.echo("No migration history rows found.")
        return
    typer.echo("Revision\tStatus\tDirty\tChecksum\tApplied at")
    for entry in history:
        typer.echo(
            f"{entry.revision}\t{entry.status}\tdirty={entry.dirty}\t"
            f"checksum={entry.checksum or '-'}\tat={entry.applied_at or '-'}"
        )


@migrations_app.command("current")
def current_command(
    url: Annotated[str | None, _database_url_argument()] = None,
    url_option: Annotated[str | None, _database_url_option()] = None,
    url_env: Annotated[str, _url_env_option()] = DEFAULT_URL_ENV,
    env_file: Annotated[Path | None, _env_file_option()] = DEFAULT_ENV_FILE,
) -> None:
    """Print current applied revision."""
    resolved = _run_cli_action(
        lambda: _resolve_database_url(
            url,
            url_option,
            url_env=url_env,
            env_file=env_file,
        )
    )

    async def fetch_current() -> Any:
        database = Ormdantic(resolved.value)
        return await database.migrations.current()

    current = _run_cli(fetch_current)
    typer.echo(f"Current revision: {current.revision if current else 'none'}")
    _echo_connection(resolved)


@migrations_app.command("rollback")
def rollback_command(
    targets: Annotated[
        list[str] | None,
        typer.Argument(
            help=(
                "Migration artifact file, legacy DATABASE_URL plus artifact, or legacy DATABASE_URL when using --revision."
            )
        ),
    ] = None,
    url_option: Annotated[str | None, _database_url_option()] = None,
    url_env: Annotated[str, _url_env_option()] = DEFAULT_URL_ENV,
    env_file: Annotated[Path | None, _env_file_option()] = DEFAULT_ENV_FILE,
    revision: Annotated[
        str | None,
        typer.Option("--revision", help="Revision to rollback from directory."),
    ] = None,
    directory: Annotated[
        Path | None,
        typer.Option("--dir", help="Directory to resolve --revision artifacts."),
    ] = None,
    allow_destructive: Annotated[
        bool,
        typer.Option("--allow-destructive", help="Allow destructive rollback SQL."),
    ] = False,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Prompt before destructive rollback SQL is executed.",
        ),
    ] = False,
) -> None:
    """Roll back one applied migration artifact."""
    resolved, artifact = _run_cli_action(
        lambda: _resolve_rollback_targets(
            targets or [],
            revision=revision,
            directory=directory,
            url_option=url_option,
            url_env=url_env,
            env_file=env_file,
        )
    )
    target_artifact = artifact
    if target_artifact is None:
        if revision is None or directory is None:
            raise typer.BadParameter(
                "Provide either an artifact path or --revision with --dir."
            )
        target_artifact = _run_cli_action(
            lambda: _artifact_for_revision(directory, revision)
        )
    migration = _run_cli_action(lambda: _read_artifact_for_cli(target_artifact))
    allow = _confirm_rollback_destructive(migration, allow_destructive, interactive)

    async def rollback_one() -> bool:
        database = Ormdantic(resolved.value)
        return await database.migrations.rollback_artifact(
            migration,
            allow_destructive=allow,
        )

    rolled_back = _run_cli(rollback_one)
    if rolled_back:
        typer.echo(f"Rolled back migration: {migration.revision}")
    else:
        typer.echo(f"Skipped rollback: {migration.revision} is not applied.")
    _echo_connection(resolved)


@migrations_app.command("repair")
def repair_command(
    url: Annotated[str | None, _database_url_argument()] = None,
    url_option: Annotated[str | None, _database_url_option()] = None,
    url_env: Annotated[str, _url_env_option()] = DEFAULT_URL_ENV,
    env_file: Annotated[Path | None, _env_file_option()] = DEFAULT_ENV_FILE,
    revision: Annotated[
        str | None,
        typer.Option("--revision", help="Revision to repair (defaults to all)."),
    ] = None,
    status: Annotated[
        str | None,
        typer.Option(
            "--status", help="Force status value (applied/failed/rolled_back)."
        ),
    ] = None,
    clear_dirty: Annotated[
        bool,
        typer.Option("--clear-dirty/--keep-dirty", help="Clear dirty marker."),
    ] = True,
    checksum: Annotated[
        str | None,
        typer.Option("--checksum", help="Override checksum for repaired revisions."),
    ] = None,
) -> None:
    """Repair dirty migration metadata entries."""
    resolved = _run_cli_action(
        lambda: _resolve_database_url(
            url,
            url_option,
            url_env=url_env,
            env_file=env_file,
        )
    )

    async def repair_rows() -> int:
        database = Ormdantic(resolved.value)
        return await database.migrations.repair(
            revision=revision,
            status=status,
            clear_dirty=clear_dirty,
            checksum=checksum,
        )

    repaired = _run_cli(repair_rows)
    typer.echo(f"Repaired migration rows: {repaired}")
    _echo_connection(resolved)


@migrations_app.command("check")
def check_command(
    directory: Annotated[
        Path,
        typer.Argument(help="Directory containing migration artifacts."),
    ],
    pattern: Annotated[
        str | None,
        typer.Option("--pattern", help="Optional glob pattern."),
    ] = None,
) -> None:
    """Validate migration artifact checksums and parseability."""
    files = (
        sorted(directory.glob(pattern))
        if pattern is not None
        else sorted({*directory.glob("*.json"), *directory.glob("*.toml")})
    )
    for path in files:
        artifact = MigrationArtifact.read(path)
        artifact.validate_checksum()
        _ = artifact.to_plan()
    typer.echo(f"ok ({len(files)} files)")


@migrations_app.command("squash")
def squash_command(
    revision: Annotated[str, typer.Argument(help="New squashed revision identifier.")],
    artifacts: Annotated[
        list[Path],
        typer.Argument(help="Contiguous migration artifact files."),
    ],
    out: Annotated[
        Path,
        typer.Option("--out", "-o", help="Output migration artifact."),
    ],
    dialect: Annotated[
        str | None,
        typer.Option("--dialect", help="SQL dialect or URL."),
    ] = None,
    format: Annotated[
        str | None,
        typer.Option("--format", "-f", help="Output format: json or toml."),
    ] = None,
    interactive: Annotated[
        bool,
        typer.Option("--interactive", "-i", help="Prompt before overwriting files."),
    ] = False,
) -> None:
    """Squash contiguous migration artifacts into one net migration."""
    artifact = squash_migrations(
        revision,
        [MigrationArtifact.read(path) for path in artifacts],
        dialect=dialect,
    )
    _echo_warnings(artifact)
    _confirm_overwrite(out, interactive)
    artifact.write(out, format=format)
    typer.echo(str(out))


def _run_cli_action(action: Callable[[], T]) -> T:
    try:
        return action()
    except (MigrationCliError, OrmdanticError, FileNotFoundError, OSError) as exc:
        _exit_cli_error(_format_cli_error(exc))
    except ValueError as exc:
        _exit_cli_error(str(exc))


def _run_cli(coroutine_factory: Callable[[], Any]) -> Any:
    return _run_cli_action(lambda: asyncio.run(coroutine_factory()))


def _exit_cli_error(message: str) -> NoReturn:
    typer.secho(f"Error: {message}", fg=typer.colors.RED)
    raise typer.Exit(1)


def _format_cli_error(error: BaseException) -> str:
    if isinstance(error, MigrationCliError | OrmdanticError):
        return str(error)
    if isinstance(error, FileNotFoundError):
        missing = error.filename or str(error)
        return f"File not found: {missing}"
    if isinstance(error, OSError):
        return str(error)
    return str(error)


def _resolve_database_url(
    positional_url: str | None,
    option_url: str | None,
    *,
    url_env: str,
    env_file: Path | None,
) -> ResolvedDatabaseUrl:
    option_value = _clean_database_url(option_url)
    if option_value is not None:
        return ResolvedDatabaseUrl(option_value, "--url")

    positional_value = _clean_database_url(positional_url)
    if positional_value is not None:
        return ResolvedDatabaseUrl(positional_value, "argument")

    env_value = _clean_database_url(os.environ.get(url_env))
    if env_value is not None:
        return ResolvedDatabaseUrl(env_value, url_env)

    env_path = env_file if env_file is not None else None
    dotenv_values = _read_env_file(env_path)
    dotenv_value = _clean_database_url(dotenv_values.get(url_env))
    if dotenv_value is not None:
        return ResolvedDatabaseUrl(dotenv_value, f"{env_path}:{url_env}")

    raise MigrationCliError(
        "Database URL is required. Pass --url postgresql://user:pass@host/db, "
        f"export {url_env}=postgresql://user:pass@host/db, "
        f"or add {url_env}=postgresql://user:pass@host/db to "
        f"{env_path or '.env'}."
    )


def _resolve_database_url_and_path(
    targets: Sequence[str],
    *,
    path_label: str,
    url_option: str | None,
    url_env: str,
    env_file: Path | None,
) -> tuple[ResolvedDatabaseUrl, Path]:
    if not targets:
        raise MigrationCliError(f"{path_label.capitalize()} path is required.")
    if len(targets) == 1:
        resolved = _resolve_database_url(
            None,
            url_option,
            url_env=url_env,
            env_file=env_file,
        )
        return resolved, Path(targets[0])
    if len(targets) == 2:
        if _clean_database_url(url_option) is not None:
            raise MigrationCliError(
                f"Pass the {path_label} path once; --url already provides the database URL."
            )
        resolved = _resolve_database_url(
            targets[0],
            None,
            url_env=url_env,
            env_file=env_file,
        )
        return resolved, Path(targets[1])
    raise MigrationCliError(
        f"Expected {path_label} or DATABASE_URL {path_label}; got {len(targets)} arguments."
    )


def _resolve_rollback_targets(
    targets: Sequence[str],
    *,
    revision: str | None,
    directory: Path | None,
    url_option: str | None,
    url_env: str,
    env_file: Path | None,
) -> tuple[ResolvedDatabaseUrl, Path | None]:
    using_revision = revision is not None or directory is not None
    if using_revision:
        if len(targets) > 1:
            raise MigrationCliError(
                "Rollback with --revision accepts at most one legacy DATABASE_URL argument."
            )
        resolved = _resolve_database_url(
            targets[0] if targets else None,
            url_option,
            url_env=url_env,
            env_file=env_file,
        )
        return resolved, None

    if not targets:
        raise MigrationCliError(
            "Migration artifact path is required unless --revision and --dir are provided."
        )
    return _resolve_database_url_and_path(
        targets,
        path_label="artifact",
        url_option=url_option,
        url_env=url_env,
        env_file=env_file,
    )


def _clean_database_url(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _read_env_file(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, separator, value = line.partition("=")
        if not separator:
            continue
        key = key.strip()
        if not key:
            continue
        values[key] = _parse_env_value(value)
    return values


def _parse_env_value(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    if " #" in value:
        value = value.split(" #", 1)[0].rstrip()
    return value


def _read_artifact_for_cli(path: Path) -> MigrationArtifact:
    try:
        return MigrationArtifact.read(path)
    except FileNotFoundError as exc:
        raise MigrationCliError(f"Migration artifact not found: {path}") from exc
    except ValueError as exc:
        raise MigrationCliError(str(exc)) from exc


def _echo_connection(resolved: ResolvedDatabaseUrl) -> None:
    typer.echo(
        f"Connection: {_redact_database_url(resolved.value)} "
        f"(source: {resolved.source})"
    )


def _redact_database_url(url: str) -> str:
    if "@" not in url:
        return url
    prefix, suffix = url.rsplit("@", 1)
    if ":" not in prefix:
        return url
    user_prefix, _password = prefix.rsplit(":", 1)
    return f"{user_prefix}:<redacted>@{suffix}"


def _load_database(target: str) -> Any:
    database = _load_object(target)
    if not hasattr(database, "migrations"):
        raise TypeError(f"{target} does not resolve to an Ormdantic database")
    return database


def _load_object(target: str) -> Any:
    module_name, separator, object_path = target.partition(":")
    if not separator or not module_name or not object_path:
        raise ValueError("target must use module:object syntax")
    module = importlib.import_module(module_name)
    value: Any = module
    for part in object_path.split("."):
        value = getattr(value, part)
    return value


def _echo_warnings(artifact: MigrationArtifact) -> None:
    for warning in artifact.warnings:
        typer.secho(f"warning: {warning.message}", fg=typer.colors.YELLOW)


def _confirm_overwrite(path: Path, interactive: bool) -> None:
    if interactive and path.exists():
        if not typer.confirm(f"Overwrite {path}?", default=False):
            raise typer.Abort()


def _confirm_destructive(
    artifact: MigrationArtifact,
    allow_destructive: bool,
    interactive: bool,
) -> bool:
    if allow_destructive or not artifact.to_plan().has_destructive_operations:
        return allow_destructive
    if interactive:
        _echo_warnings(artifact)
        return typer.confirm(
            "This migration contains destructive SQL. Apply it?",
            default=False,
        )
    return allow_destructive


def _confirm_rollback_destructive(
    artifact: MigrationArtifact,
    allow_destructive: bool,
    interactive: bool,
) -> bool:
    rollback_plan = MigrationPlan(operations=list(artifact.rollback_operations))
    if allow_destructive or not rollback_plan.has_destructive_operations:
        return allow_destructive
    if interactive:
        return typer.confirm(
            "This rollback contains destructive SQL. Continue?",
            default=False,
        )
    return allow_destructive


def _artifact_for_revision(directory: Path, revision: str) -> Path:
    files = sorted({*directory.glob("*.json"), *directory.glob("*.toml")})
    for path in files:
        if path.stem.startswith(revision):
            return path
        artifact = MigrationArtifact.read(path)
        if artifact.revision == revision:
            return path
    raise FileNotFoundError(
        f"could not find migration artifact for revision '{revision}' in {directory}"
    )
