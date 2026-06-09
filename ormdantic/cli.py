"""Typer command line helpers for Ormdantic."""

from __future__ import annotations

import asyncio
import importlib
from pathlib import Path
from typing import Annotated, Any, Sequence

import typer

from ormdantic import Ormdantic
from ormdantic.migrations import (
    MigrationArtifact,
    MigrationPlan,
    SchemaSnapshot,
    create_migration_artifact,
    squash_migrations,
)

app = typer.Typer(help="Ormdantic command line tools.", no_args_is_help=True)
migrations_app = typer.Typer(
    help="Create, review, apply, and squash migration artifacts.",
    no_args_is_help=True,
)
app.add_typer(migrations_app, name="migrations")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Ormdantic command line interface."""
    try:
        app(
            args=list(argv) if argv is not None else None,
            prog_name="ormdantic",
            standalone_mode=False,
        )
    except typer.Abort:
        return 1
    except typer.Exit as exc:
        return int(exc.exit_code)
    return 0


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
    url: Annotated[str, typer.Argument(help="Database URL.")],
) -> None:
    """Initialize migration history metadata on a database."""

    async def init_history() -> None:
        database = Ormdantic(url)
        await database.migrations.ensure_revision_table()

    asyncio.run(init_history())
    typer.echo("initialized")


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
    url: Annotated[str, typer.Argument(help="Database URL.")],
    artifact: Annotated[Path, typer.Argument(help="Migration artifact file.")],
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
    migration = MigrationArtifact.read(artifact)
    allow = _confirm_destructive(migration, allow_destructive, interactive)

    async def apply_one() -> bool:
        database = Ormdantic(url)
        return await database.migrations.apply_artifact(
            migration,
            allow_destructive=allow,
        )

    applied = asyncio.run(apply_one())
    typer.echo("applied" if applied else "skipped")


@migrations_app.command("apply-dir")
def apply_dir_command(
    url: Annotated[str, typer.Argument(help="Database URL.")],
    directory: Annotated[
        Path,
        typer.Argument(help="Directory containing migration artifacts."),
    ],
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

    async def apply_many() -> list[str]:
        database = Ormdantic(url)
        return await database.migrations.apply_directory(
            directory,
            pattern=pattern,
            allow_destructive=allow_destructive,
        )

    for revision in asyncio.run(apply_many()):
        typer.echo(revision)


@migrations_app.command("status")
def status_command(
    url: Annotated[str, typer.Argument(help="Database URL.")],
) -> None:
    """Print migration dirty state and current revision."""

    async def fetch_status() -> dict[str, Any]:
        database = Ormdantic(url)
        return await database.migrations.status()

    status = asyncio.run(fetch_status())
    typer.echo(f"dirty: {status['dirty']}")
    typer.echo(f"current: {status['current'] or 'none'}")
    typer.echo(f"applied: {len(status['applied'])}")


@migrations_app.command("history")
def history_command(
    url: Annotated[str, typer.Argument(help="Database URL.")],
) -> None:
    """Print migration revision history."""

    async def fetch_history() -> list[Any]:
        database = Ormdantic(url)
        return await database.migrations.history()

    for entry in asyncio.run(fetch_history()):
        typer.echo(
            f"{entry.revision}\t{entry.status}\tdirty={entry.dirty}\t"
            f"checksum={entry.checksum or '-'}\tat={entry.applied_at or '-'}"
        )


@migrations_app.command("current")
def current_command(
    url: Annotated[str, typer.Argument(help="Database URL.")],
) -> None:
    """Print current applied revision."""

    async def fetch_current() -> Any:
        database = Ormdantic(url)
        return await database.migrations.current()

    current = asyncio.run(fetch_current())
    typer.echo(current.revision if current else "")


@migrations_app.command("rollback")
def rollback_command(
    url: Annotated[str, typer.Argument(help="Database URL.")],
    artifact: Annotated[
        Path | None,
        typer.Argument(
            help="Migration artifact file (optional when using --revision)."
        ),
    ] = None,
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
    target_artifact = artifact
    if target_artifact is None:
        if revision is None or directory is None:
            raise typer.BadParameter(
                "Provide either an artifact path or --revision with --dir."
            )
        target_artifact = _artifact_for_revision(directory, revision)
    migration = MigrationArtifact.read(target_artifact)
    allow = _confirm_rollback_destructive(migration, allow_destructive, interactive)

    async def rollback_one() -> bool:
        database = Ormdantic(url)
        return await database.migrations.rollback_artifact(
            migration,
            allow_destructive=allow,
        )

    rolled_back = asyncio.run(rollback_one())
    typer.echo("rolled-back" if rolled_back else "skipped")


@migrations_app.command("repair")
def repair_command(
    url: Annotated[str, typer.Argument(help="Database URL.")],
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

    async def repair_rows() -> int:
        database = Ormdantic(url)
        return await database.migrations.repair(
            revision=revision,
            status=status,
            clear_dirty=clear_dirty,
            checksum=checksum,
        )

    repaired = asyncio.run(repair_rows())
    typer.echo(f"repaired: {repaired}")


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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
