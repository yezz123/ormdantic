from __future__ import annotations

import pytest
from typer.testing import CliRunner

from ormdantic import cli as root_cli
from ormdantic._migrations import cli as migration_cli

runner = CliRunner()


def test_root_cli_re_exports_migration_cli_group_and_handlers() -> None:
    assert root_cli.migrations_app is migration_cli.migrations_app
    assert root_cli.create_command is migration_cli.create_command
    assert root_cli.apply_command is migration_cli.apply_command
    assert root_cli.rollback_command is migration_cli.rollback_command
    assert root_cli._artifact_for_revision is migration_cli._artifact_for_revision


def test_load_object_validates_module_colon_object_syntax() -> None:
    assert migration_cli._load_object("ormdantic.cli:main") is root_cli.main

    with pytest.raises(ValueError, match="module:object"):
        migration_cli._load_object("ormdantic.cli")


def test_resolve_database_url_prefers_explicit_then_environment_then_dotenv(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "DATABASE_URL=postgresql://dotenv:secret@localhost:5432/app",
                "OTHER=value",
            ]
        )
    )

    monkeypatch.delenv("DATABASE_URL", raising=False)
    resolved = migration_cli._resolve_database_url(
        None,
        None,
        url_env="DATABASE_URL",
        env_file=env_file,
    )
    assert resolved.value == "postgresql://dotenv:secret@localhost:5432/app"
    assert resolved.source == f"{env_file}:DATABASE_URL"

    monkeypatch.setenv("DATABASE_URL", "postgresql://exported:secret@localhost/db")
    resolved = migration_cli._resolve_database_url(
        None,
        None,
        url_env="DATABASE_URL",
        env_file=env_file,
    )
    assert resolved.value == "postgresql://exported:secret@localhost/db"
    assert resolved.source == "DATABASE_URL"

    resolved = migration_cli._resolve_database_url(
        "postgresql://positional:secret@localhost/db",
        None,
        url_env="DATABASE_URL",
        env_file=env_file,
    )
    assert resolved.value == "postgresql://positional:secret@localhost/db"
    assert resolved.source == "argument"

    resolved = migration_cli._resolve_database_url(
        "postgresql://positional:secret@localhost/db",
        "postgresql://option:secret@localhost/db",
        url_env="DATABASE_URL",
        env_file=env_file,
    )
    assert resolved.value == "postgresql://option:secret@localhost/db"
    assert resolved.source == "--url"


def test_resolve_database_url_reports_how_to_connect_when_missing(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    with pytest.raises(migration_cli.MigrationCliError) as exc_info:
        migration_cli._resolve_database_url(
            None,
            None,
            url_env="DATABASE_URL",
            env_file=tmp_path / ".env",
        )

    message = str(exc_info.value)
    assert "Database URL is required" in message
    assert "--url" in message
    assert "export DATABASE_URL" in message
    assert ".env" in message


def test_parse_database_path_args_supports_env_style_and_legacy_url_style(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "migrations"
    directory.mkdir()
    monkeypatch.setenv("DATABASE_URL", "postgresql://env:secret@localhost/db")

    resolved, path = migration_cli._resolve_database_url_and_path(
        ["migrations"],
        path_label="directory",
        url_option=None,
        url_env="DATABASE_URL",
        env_file=None,
    )
    assert resolved.value == "postgresql://env:secret@localhost/db"
    assert path == migration_cli.Path("migrations")

    resolved, path = migration_cli._resolve_database_url_and_path(
        ["postgresql://legacy:secret@localhost/db", "migrations"],
        path_label="directory",
        url_option=None,
        url_env="DATABASE_URL",
        env_file=None,
    )
    assert resolved.value == "postgresql://legacy:secret@localhost/db"
    assert resolved.source == "argument"
    assert path == migration_cli.Path("migrations")


def test_current_command_reads_database_url_from_env_and_prints_explanation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeCurrent:
        revision = "001_initial"

    class FakeMigrations:
        async def current(self) -> FakeCurrent:
            return FakeCurrent()

    class FakeOrmdantic:
        def __init__(self, url: str) -> None:
            self.url = url
            self.migrations = FakeMigrations()

    monkeypatch.setattr(migration_cli, "Ormdantic", FakeOrmdantic)

    result = runner.invoke(
        migration_cli.migrations_app,
        ["current"],
        env={"DATABASE_URL": "postgresql://postgres:postgres@localhost:5432/postgres"},
    )

    assert result.exit_code == 0
    assert "Current revision: 001_initial" in result.stdout
    assert "DATABASE_URL" in result.stdout
    assert "postgres:postgres" not in result.stdout


def test_init_command_prints_history_table_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class FakeMigrations:
        async def ensure_revision_table(self) -> None:
            calls.append("ensure")

    class FakeOrmdantic:
        def __init__(self, url: str) -> None:
            calls.append(url)
            self.migrations = FakeMigrations()

    monkeypatch.setattr(migration_cli, "Ormdantic", FakeOrmdantic)

    result = runner.invoke(
        migration_cli.migrations_app,
        ["init", "--url", "postgresql://postgres:postgres@localhost:5432/postgres"],
    )

    assert result.exit_code == 0
    assert calls == [
        "postgresql://postgres:postgres@localhost:5432/postgres",
        "ensure",
    ]
    assert "Initialized Ormdantic migration history." in result.stdout
    assert "ormdantic_migrations" in result.stdout
    assert "postgres:postgres" not in result.stdout


def test_apply_dir_uses_env_url_and_summarizes_applied_revisions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeMigrations:
        async def apply_directory(
            self,
            directory: migration_cli.Path,
            *,
            pattern: str | None = None,
            allow_destructive: bool = False,
        ) -> list[str]:
            assert directory == migration_cli.Path("migrations")
            assert pattern is None
            assert allow_destructive is False
            return ["001_initial", "002_tasks"]

    class FakeOrmdantic:
        def __init__(self, url: str) -> None:
            assert url == "postgresql://postgres:postgres@localhost:5432/postgres"
            self.migrations = FakeMigrations()

    monkeypatch.setattr(migration_cli, "Ormdantic", FakeOrmdantic)

    result = runner.invoke(
        migration_cli.migrations_app,
        ["apply-dir", "migrations"],
        env={"DATABASE_URL": "postgresql://postgres:postgres@localhost:5432/postgres"},
    )

    assert result.exit_code == 0
    assert "Applied 2 migrations from migrations." in result.stdout
    assert "- 001_initial" in result.stdout
    assert "- 002_tasks" in result.stdout


def test_apply_dir_reports_missing_database_url_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    result = runner.invoke(
        migration_cli.migrations_app,
        ["apply-dir", "migrations", "--env-file", "missing.env"],
    )

    assert result.exit_code == 1
    assert "Database URL is required" in result.stdout
    assert "export DATABASE_URL" in result.stdout
    assert "Traceback" not in result.stdout


def test_root_main_returns_nested_migration_error_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    exit_code = root_cli.main(
        ["migrations", "apply-dir", "migrations", "--env-file", "missing.env"]
    )

    assert exit_code == 1
