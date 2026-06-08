"""Command line helpers for Ormdantic."""

from __future__ import annotations

import argparse
import asyncio
import importlib
from typing import Any, Sequence

from ormdantic import Ormdantic
from ormdantic.migrations import (
    MigrationArtifact,
    SchemaSnapshot,
    create_migration_artifact,
    squash_migrations,
)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Ormdantic command line interface."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ormdantic")
    subcommands = parser.add_subparsers(dest="command", required=True)

    migrations = subcommands.add_parser("migrations", help="Manage migrations")
    migration_commands = migrations.add_subparsers(
        dest="migration_command", required=True
    )

    snapshot = migration_commands.add_parser(
        "snapshot",
        help="Export a schema snapshot from a module:database object",
    )
    snapshot.add_argument("target", help="Import path like package.module:db")
    snapshot.add_argument("--out", required=True, help="Output snapshot JSON file")
    snapshot.set_defaults(handler=_snapshot_command)

    create = migration_commands.add_parser(
        "create",
        help="Create a migration artifact from two snapshots",
    )
    create.add_argument("revision", help="Migration revision identifier")
    create.add_argument("--from", dest="from_snapshot", required=True)
    create.add_argument("--to", dest="to_snapshot", required=True)
    create.add_argument("--dialect", required=True)
    create.add_argument("--out", required=True, help="Output migration JSON file")
    create.set_defaults(handler=_create_command)

    preview = migration_commands.add_parser(
        "preview",
        help="Print SQL from a migration artifact",
    )
    preview.add_argument("artifact", help="Migration artifact JSON file")
    preview.add_argument("--rollback", action="store_true", help="Preview rollback SQL")
    preview.set_defaults(handler=_preview_command)

    apply = migration_commands.add_parser(
        "apply",
        help="Apply one migration artifact",
    )
    apply.add_argument("url", help="Database URL")
    apply.add_argument("artifact", help="Migration artifact JSON file")
    apply.add_argument("--allow-destructive", action="store_true")
    apply.set_defaults(handler=_apply_command)

    apply_dir = migration_commands.add_parser(
        "apply-dir",
        help="Apply migration artifacts from a directory in filename order",
    )
    apply_dir.add_argument("url", help="Database URL")
    apply_dir.add_argument(
        "directory", help="Directory containing migration JSON files"
    )
    apply_dir.add_argument("--pattern", default="*.json")
    apply_dir.add_argument("--allow-destructive", action="store_true")
    apply_dir.set_defaults(handler=_apply_dir_command)

    squash = migration_commands.add_parser(
        "squash",
        help="Squash contiguous migration artifacts into one artifact",
    )
    squash.add_argument("revision", help="New squashed revision identifier")
    squash.add_argument("artifacts", nargs="+", help="Migration artifact JSON files")
    squash.add_argument("--dialect")
    squash.add_argument("--out", required=True, help="Output migration JSON file")
    squash.set_defaults(handler=_squash_command)

    return parser


def _snapshot_command(args: argparse.Namespace) -> int:
    database = _load_object(args.target)
    if not hasattr(database, "migrations"):
        raise TypeError(f"{args.target} does not resolve to an Ormdantic database")
    snapshot = database.migrations.snapshot()
    snapshot.write(args.out)
    print(args.out)
    return 0


def _create_command(args: argparse.Namespace) -> int:
    artifact = create_migration_artifact(
        args.revision,
        SchemaSnapshot.read(args.from_snapshot),
        SchemaSnapshot.read(args.to_snapshot),
        dialect=args.dialect,
    )
    artifact.write(args.out)
    print(args.out)
    return 0


def _preview_command(args: argparse.Namespace) -> int:
    artifact = MigrationArtifact.read(args.artifact)
    statements = artifact.rollback_operations if args.rollback else artifact.operations
    for statement in statements:
        print(statement.sql)
    return 0


def _apply_command(args: argparse.Namespace) -> int:
    async def apply_one() -> bool:
        database = Ormdantic(args.url)
        return await database.migrations.apply_file(
            args.artifact,
            allow_destructive=args.allow_destructive,
        )

    applied = asyncio.run(apply_one())
    print("applied" if applied else "skipped")
    return 0


def _apply_dir_command(args: argparse.Namespace) -> int:
    async def apply_many() -> list[str]:
        database = Ormdantic(args.url)
        return await database.migrations.apply_directory(
            args.directory,
            pattern=args.pattern,
            allow_destructive=args.allow_destructive,
        )

    for revision in asyncio.run(apply_many()):
        print(revision)
    return 0


def _squash_command(args: argparse.Namespace) -> int:
    artifacts = [MigrationArtifact.read(path) for path in args.artifacts]
    artifact = squash_migrations(
        args.revision,
        artifacts,
        dialect=args.dialect,
    )
    artifact.write(args.out)
    print(args.out)
    return 0


def _load_object(target: str) -> Any:
    module_name, separator, object_path = target.partition(":")
    if not separator or not module_name or not object_path:
        raise ValueError("target must use module:object syntax")
    module = importlib.import_module(module_name)
    value: Any = module
    for part in object_path.split("."):
        value = getattr(value, part)
    return value


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
