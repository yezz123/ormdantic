"""Typer command line entry point for Ormdantic."""

from __future__ import annotations

from collections.abc import Sequence

import typer

from ormdantic._migrations.cli import (
    MigrationCliError as MigrationCliError,
)
from ormdantic._migrations.cli import (
    _artifact_for_revision as _artifact_for_revision,
)
from ormdantic._migrations.cli import (
    _confirm_destructive as _confirm_destructive,
)
from ormdantic._migrations.cli import (
    _confirm_overwrite as _confirm_overwrite,
)
from ormdantic._migrations.cli import (
    _confirm_rollback_destructive as _confirm_rollback_destructive,
)
from ormdantic._migrations.cli import (
    _echo_warnings as _echo_warnings,
)
from ormdantic._migrations.cli import (
    _load_database as _load_database,
)
from ormdantic._migrations.cli import (
    _load_object as _load_object,
)
from ormdantic._migrations.cli import (
    apply_command as apply_command,
)
from ormdantic._migrations.cli import (
    apply_dir_command as apply_dir_command,
)
from ormdantic._migrations.cli import (
    autogenerate_command as autogenerate_command,
)
from ormdantic._migrations.cli import (
    check_command as check_command,
)
from ormdantic._migrations.cli import (
    create_command as create_command,
)
from ormdantic._migrations.cli import (
    current_command as current_command,
)
from ormdantic._migrations.cli import (
    history_command as history_command,
)
from ormdantic._migrations.cli import (
    init_command as init_command,
)
from ormdantic._migrations.cli import (
    migrations_app,
)
from ormdantic._migrations.cli import (
    preview_command as preview_command,
)
from ormdantic._migrations.cli import (
    repair_command as repair_command,
)
from ormdantic._migrations.cli import (
    rollback_command as rollback_command,
)
from ormdantic._migrations.cli import (
    snapshot_command as snapshot_command,
)
from ormdantic._migrations.cli import (
    squash_command as squash_command,
)
from ormdantic._migrations.cli import (
    status_command as status_command,
)
from ormdantic.errors import OrmdanticError

app = typer.Typer(help="Ormdantic command line tools.", no_args_is_help=True)
app.add_typer(migrations_app, name="migrations")


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Ormdantic command line interface."""
    try:
        result = app(
            args=list(argv) if argv is not None else None,
            prog_name="ormdantic",
            standalone_mode=False,
        )
    except typer.Abort:
        return 1
    except typer.Exit as exc:
        return int(exc.exit_code)
    except (
        MigrationCliError,
        OrmdanticError,
        FileNotFoundError,
        OSError,
        ValueError,
    ) as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED)
        return 1
    if isinstance(result, int):
        return result
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
