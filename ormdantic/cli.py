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
) -> None:
    """Create a migration artifact from two snapshots."""
    artifact = create_migration_artifact(
        revision,
        SchemaSnapshot.read(from_snapshot),
        SchemaSnapshot.read(to_snapshot),
        dialect=dialect,
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
    for statement in statements:
        typer.echo(statement.sql)


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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
