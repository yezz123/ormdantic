"""Lazy launcher for the optional Textual application."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any, cast

from ormdantic.errors import OrmdanticError


class PlaygroundDependencyError(OrmdanticError):
    """Raised when the optional playground dependencies are unavailable."""


def run_playground(
    *,
    config_path: Path | None,
    environment: str | None,
    target: str | None,
    migrations_dir: Path | None,
) -> None:
    """Load and run the optional Textual application."""
    try:
        module = import_module("ormdantic.playground.app")
    except ImportError as exc:
        if exc.name and exc.name.split(".", 1)[0] == "textual":
            raise PlaygroundDependencyError(
                "Install the playground with: pip install 'ormdantic[playground]'"
            ) from exc
        raise

    playground_app = cast(Any, module).PlaygroundApp.from_cli(
        config_path=config_path,
        environment=environment,
        target=target,
        migrations_dir=migrations_dir,
    )
    playground_app.run()
