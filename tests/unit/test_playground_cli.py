from __future__ import annotations

from pathlib import Path

import pytest
from click.utils import strip_ansi
from typer.testing import CliRunner

from ormdantic.cli import app
from ormdantic.playground import launcher

runner = CliRunner()


def test_root_help_lists_playground() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "playground" in result.stdout


def test_playground_help_describes_configuration_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FORCE_COLOR", "1")
    result = runner.invoke(app, ["playground", "--help"])
    output = strip_ansi(result.stdout)

    assert result.exit_code == 0
    assert "--config" in output
    assert "--environment" in output
    assert "--target" in output
    assert "--migrations-dir" in output


def test_playground_command_forwards_cli_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def fake_run_playground(**options: object) -> None:
        calls.append(options)

    monkeypatch.setattr(launcher, "run_playground", fake_run_playground)

    result = runner.invoke(
        app,
        [
            "playground",
            "--config",
            "config/ormdantic.toml",
            "--environment",
            "staging",
            "--target",
            "example.database:db",
            "--migrations-dir",
            "schema/migrations",
        ],
    )

    assert result.exit_code == 0
    assert calls == [
        {
            "config_path": Path("config/ormdantic.toml"),
            "environment": "staging",
            "target": "example.database:db",
            "migrations_dir": Path("schema/migrations"),
        }
    ]


def test_launcher_explains_how_to_install_missing_textual(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def import_without_textual(name: str) -> None:
        assert name == "ormdantic.playground.app"
        raise ModuleNotFoundError("No module named 'textual'", name="textual")

    monkeypatch.setattr(launcher, "import_module", import_without_textual)

    with pytest.raises(
        launcher.PlaygroundDependencyError,
        match=r"pip install 'ormdantic\[playground\]'",
    ):
        launcher.run_playground(
            config_path=None,
            environment=None,
            target=None,
            migrations_dir=None,
        )
