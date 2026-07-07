from __future__ import annotations

from types import SimpleNamespace

import pytest
import typer
from typer.testing import CliRunner

from ormdantic import cli as root_cli
from ormdantic import migrations
from ormdantic._migrations import cli as migration_cli
from ormdantic.errors import SchemaError

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


def test_snapshot_command_loads_database_writes_snapshot_and_echoes_path(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[migration_cli.Path, str | None]] = []
    out = tmp_path / "snapshot.json"

    class FakeSnapshot:
        def write(self, path: migration_cli.Path, *, format: str | None = None) -> None:
            calls.append((path, format))
            path.write_text("{}")

    class FakeMigrations:
        def snapshot(self) -> FakeSnapshot:
            return FakeSnapshot()

    fake_database = SimpleNamespace(migrations=FakeMigrations())
    monkeypatch.setattr(migration_cli, "_load_database", lambda target: fake_database)

    result = runner.invoke(
        migration_cli.migrations_app,
        ["snapshot", "module:db", "--out", str(out), "--format", "json"],
    )

    assert result.exit_code == 0
    assert calls == [(out, "json")]
    assert str(out) in result.stdout


def test_autogenerate_command_prints_noop_without_writing(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeMigrations:
        def autogenerate(self, *args: object, **kwargs: object) -> None:
            return None

    monkeypatch.setattr(
        migration_cli,
        "_load_database",
        lambda target: SimpleNamespace(migrations=FakeMigrations()),
    )

    out = tmp_path / "noop.json"
    migration_cli.autogenerate_command(
        "module:db",
        "001_noop",
        out,
        message=None,
        include_table=None,
        exclude_table=None,
        schema=None,
        format=None,
        interactive=False,
    )

    assert "no-op" in capsys.readouterr().out
    assert not out.exists()


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


def test_apply_and_apply_dir_commands_report_skipped_and_empty_results(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    resolved = migration_cli.ResolvedDatabaseUrl("sqlite:///db.sqlite3", "argument")
    migration = SimpleNamespace(
        revision="001_initial",
        warnings=[],
        to_plan=lambda: SimpleNamespace(has_destructive_operations=False),
    )

    class FakeMigrations:
        async def apply_artifact(
            self, artifact: object, *, allow_destructive: bool = False
        ) -> bool:
            assert artifact is migration
            assert allow_destructive is False
            return False

        async def apply_directory(
            self,
            directory: migration_cli.Path,
            *,
            pattern: str | None = None,
            allow_destructive: bool = False,
        ) -> list[str]:
            assert directory == migration_cli.Path("migrations")
            assert pattern == "*.json"
            assert allow_destructive is True
            return []

    class FakeOrmdantic:
        def __init__(self, url: str) -> None:
            assert url == resolved.value
            self.migrations = FakeMigrations()

    monkeypatch.setattr(migration_cli, "Ormdantic", FakeOrmdantic)
    monkeypatch.setattr(
        migration_cli,
        "_resolve_database_url_and_path",
        lambda *args, **kwargs: (resolved, migration_cli.Path("001.json")),
    )
    monkeypatch.setattr(
        migration_cli,
        "_read_artifact_for_cli",
        lambda path: migration,
    )

    migration_cli.apply_command(
        targets=[],
        url_option=None,
        url_env="DATABASE_URL",
        env_file=None,
        allow_destructive=False,
        interactive=False,
    )
    assert (
        "Skipped migration: 001_initial is already applied." in capsys.readouterr().out
    )

    monkeypatch.setattr(
        migration_cli,
        "_resolve_database_url_and_path",
        lambda *args, **kwargs: (resolved, migration_cli.Path("migrations")),
    )
    migration_cli.apply_dir_command(
        targets=[],
        url_option=None,
        url_env="DATABASE_URL",
        env_file=None,
        pattern="*.json",
        allow_destructive=True,
    )
    assert "No pending migrations in migrations." in capsys.readouterr().out


def test_preview_command_prints_warnings_and_rollback_sql(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakePlan:
        has_unsafe_operations = True
        has_destructive_operations = True

    fake_artifact = SimpleNamespace(
        revision="002_down",
        dialect="postgresql",
        operations=[SimpleNamespace(sql="SELECT 1")],
        rollback_operations=[SimpleNamespace(sql="DROP TABLE flavor")],
        warnings=[SimpleNamespace(message="drops flavor")],
        safety={"requires_rebuild": True},
        to_plan=lambda: FakePlan(),
    )
    monkeypatch.setattr(
        migration_cli.MigrationArtifact,
        "read",
        lambda artifact: fake_artifact,
    )

    migration_cli.preview_command(migration_cli.Path("002_down.json"), rollback=True)

    output = capsys.readouterr().out
    assert "# revision: 002_down" in output
    assert "# dialect: postgresql" in output
    assert "# warning: drops flavor" in output
    assert "DROP TABLE flavor" in output
    assert "SELECT 1" not in output


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


def test_history_and_rollback_commands_cover_empty_and_skipped_outputs(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    resolved = migration_cli.ResolvedDatabaseUrl("sqlite:///db.sqlite3", "argument")
    migration = SimpleNamespace(
        revision="001_initial",
        rollback_operations=[],
        warnings=[],
    )

    class FakeMigrations:
        async def history(self) -> list[object]:
            return []

        async def rollback_artifact(
            self, artifact: object, *, allow_destructive: bool = False
        ) -> bool:
            assert artifact is migration
            assert allow_destructive is False
            return False

    class FakeOrmdantic:
        def __init__(self, url: str) -> None:
            assert url == resolved.value
            self.migrations = FakeMigrations()

    monkeypatch.setattr(migration_cli, "Ormdantic", FakeOrmdantic)
    monkeypatch.setattr(
        migration_cli,
        "_resolve_database_url",
        lambda *args, **kwargs: resolved,
    )
    migration_cli.history_command(
        url=None,
        url_option=None,
        url_env="DATABASE_URL",
        env_file=None,
    )
    assert "No migration history rows found." in capsys.readouterr().out

    monkeypatch.setattr(
        migration_cli,
        "_resolve_rollback_targets",
        lambda *args, **kwargs: (resolved, None),
    )
    monkeypatch.setattr(
        migration_cli,
        "_artifact_for_revision",
        lambda directory, revision: tmp_path / "001.json",
    )
    monkeypatch.setattr(
        migration_cli,
        "_read_artifact_for_cli",
        lambda path: migration,
    )
    migration_cli.rollback_command(
        targets=[],
        url_option=None,
        url_env="DATABASE_URL",
        env_file=None,
        revision="001_initial",
        directory=tmp_path,
        allow_destructive=False,
        interactive=False,
    )
    assert "Skipped rollback: 001_initial is not applied." in capsys.readouterr().out


def test_root_main_returns_nested_migration_error_exit_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    exit_code = root_cli.main(
        ["migrations", "apply-dir", "migrations", "--env-file", "missing.env"]
    )

    assert exit_code == 1


def test_env_file_parser_handles_exports_comments_quotes_and_invalid_lines(
    tmp_path,
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "",
                "# comment",
                "export DATABASE_URL='postgresql://user:pass@localhost/db'",
                "PLAIN=value # trailing comment",
                'QUOTED="literal # not a comment"',
                "NO_SEPARATOR",
                "=missing_key",
            ]
        )
    )

    assert migration_cli._read_env_file(None) == {}
    assert migration_cli._read_env_file(tmp_path / "missing.env") == {}
    values = migration_cli._read_env_file(env_file)
    assert values == {
        "DATABASE_URL": "postgresql://user:pass@localhost/db",
        "PLAIN": "value",
        "QUOTED": "literal # not a comment",
    }


def test_database_url_and_path_resolution_reports_ambiguous_inputs(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite:///env.sqlite3")

    with pytest.raises(
        migration_cli.MigrationCliError, match="Artifact path is required"
    ):
        migration_cli._resolve_database_url_and_path(
            [],
            path_label="artifact",
            url_option=None,
            url_env="DATABASE_URL",
            env_file=None,
        )

    with pytest.raises(migration_cli.MigrationCliError, match="--url already provides"):
        migration_cli._resolve_database_url_and_path(
            ["sqlite:///legacy.sqlite3", "001.json"],
            path_label="artifact",
            url_option="sqlite:///option.sqlite3",
            url_env="DATABASE_URL",
            env_file=None,
        )

    with pytest.raises(migration_cli.MigrationCliError, match="got 3 arguments"):
        migration_cli._resolve_database_url_and_path(
            ["one", "two", "three"],
            path_label="directory",
            url_option=None,
            url_env="DATABASE_URL",
            env_file=tmp_path / ".env",
        )


def test_rollback_target_resolution_covers_revision_and_artifact_modes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "sqlite:///env.sqlite3")

    resolved, artifact = migration_cli._resolve_rollback_targets(
        ["sqlite:///legacy.sqlite3"],
        revision="001",
        directory=migration_cli.Path("migrations"),
        url_option=None,
        url_env="DATABASE_URL",
        env_file=None,
    )
    assert resolved.value == "sqlite:///legacy.sqlite3"
    assert artifact is None

    with pytest.raises(migration_cli.MigrationCliError, match="at most one"):
        migration_cli._resolve_rollback_targets(
            ["sqlite:///one.sqlite3", "sqlite:///two.sqlite3"],
            revision="001",
            directory=migration_cli.Path("migrations"),
            url_option=None,
            url_env="DATABASE_URL",
            env_file=None,
        )

    with pytest.raises(
        migration_cli.MigrationCliError, match="artifact path is required"
    ):
        migration_cli._resolve_rollback_targets(
            [],
            revision=None,
            directory=None,
            url_option=None,
            url_env="DATABASE_URL",
            env_file=None,
        )


def test_rollback_command_requires_revision_directory_when_no_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        migration_cli,
        "_resolve_rollback_targets",
        lambda *args, **kwargs: (
            migration_cli.ResolvedDatabaseUrl("sqlite:///db.sqlite3", "argument"),
            None,
        ),
    )

    with pytest.raises(typer.BadParameter, match="--revision with --dir"):
        migration_cli.rollback_command(
            targets=[],
            url_option=None,
            url_env="DATABASE_URL",
            env_file=None,
            revision=None,
            directory=None,
        )


def test_cli_formatting_redaction_loading_and_action_error_branches(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert migration_cli._format_cli_error(FileNotFoundError("missing")) == (
        "File not found: missing"
    )
    assert migration_cli._format_cli_error(OSError("bad disk")) == "bad disk"
    assert migration_cli._format_cli_error(SchemaError("bad schema")) == "bad schema"
    assert migration_cli._format_cli_error(RuntimeError("boom")) == "boom"
    assert (
        migration_cli._redact_database_url("sqlite:///db.sqlite3")
        == "sqlite:///db.sqlite3"
    )
    assert (
        migration_cli._redact_database_url("user@localhost/db") == "user@localhost/db"
    )
    assert (
        migration_cli._redact_database_url("postgresql://user@localhost/db")
        == "postgresql:<redacted>@localhost/db"
    )
    assert (
        migration_cli._redact_database_url("postgresql://user:secret@localhost/db")
        == "postgresql://user:<redacted>@localhost/db"
    )

    monkeypatch.setattr(migration_cli, "_load_object", lambda _: object())
    with pytest.raises(TypeError, match="does not resolve"):
        migration_cli._load_database("module:db")

    for action in [
        lambda: (_ for _ in ()).throw(migration_cli.MigrationCliError("bad input")),
        lambda: (_ for _ in ()).throw(ValueError("plain bad input")),
    ]:
        with pytest.raises(typer.Exit):
            migration_cli._run_cli_action(action)
    assert "Error: bad input" in capsys.readouterr().out


def test_confirm_destructive_and_overwrite_helpers(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakePlan:
        has_destructive_operations = True

    class FakeArtifact:
        warnings: list[object] = []

        def to_plan(self) -> FakePlan:
            return FakePlan()

    artifact = FakeArtifact()
    assert migration_cli._confirm_destructive(artifact, True, False) is True
    assert migration_cli._confirm_destructive(artifact, False, False) is False

    monkeypatch.setattr(migration_cli.typer, "confirm", lambda *_args, **_kwargs: True)
    assert migration_cli._confirm_destructive(artifact, False, True) is True

    rollback_artifact = SimpleNamespace(
        rollback_operations=[migrations.MigrationOperation("DROP TABLE flavor")]
    )
    assert (
        migration_cli._confirm_rollback_destructive(rollback_artifact, True, False)
        is True
    )
    assert (
        migration_cli._confirm_rollback_destructive(rollback_artifact, False, True)
        is True
    )

    existing = tmp_path / "snapshot.json"
    existing.write_text("{}")
    migration_cli._confirm_overwrite(existing, interactive=True)
    monkeypatch.setattr(migration_cli.typer, "confirm", lambda *_args, **_kwargs: False)
    with pytest.raises(typer.Abort):
        migration_cli._confirm_overwrite(existing, interactive=True)


def test_read_artifact_and_find_revision_error_branches(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing = tmp_path / "missing.json"
    with pytest.raises(migration_cli.MigrationCliError, match="not found"):
        migration_cli._read_artifact_for_cli(missing)

    def raise_value_error(_path: migration_cli.Path) -> object:
        raise ValueError("bad artifact")

    monkeypatch.setattr(migration_cli.MigrationArtifact, "read", raise_value_error)
    with pytest.raises(migration_cli.MigrationCliError, match="bad artifact"):
        migration_cli._read_artifact_for_cli(tmp_path / "bad.json")

    class FakeArtifact:
        def __init__(self, revision: str) -> None:
            self.revision = revision

    files = [tmp_path / "alpha.json", tmp_path / "other.toml"]
    for path in files:
        path.write_text("{}")
    assert migration_cli._artifact_for_revision(tmp_path, "alpha") == files[0]

    def read_revision(path: migration_cli.Path) -> FakeArtifact:
        return FakeArtifact("target" if path.name == "other.toml" else "other")

    monkeypatch.setattr(migration_cli.MigrationArtifact, "read", read_revision)
    assert migration_cli._artifact_for_revision(tmp_path, "target") == files[1]

    with pytest.raises(FileNotFoundError):
        migration_cli._artifact_for_revision(tmp_path, "missing")


def test_squash_command_writes_artifact_and_echoes_warnings(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = tmp_path / "001.json"
    source.write_text("{}")
    out = tmp_path / "squashed.json"
    writes: list[tuple[migration_cli.Path, str | None]] = []

    class FakeSquashed:
        warnings = [SimpleNamespace(message="squash warning")]

        def write(self, path: migration_cli.Path, *, format: str | None = None) -> None:
            writes.append((path, format))
            path.write_text("{}")

    monkeypatch.setattr(migration_cli.MigrationArtifact, "read", lambda path: object())
    monkeypatch.setattr(
        migration_cli,
        "squash_migrations",
        lambda revision, artifacts, dialect=None: FakeSquashed(),
    )

    migration_cli.squash_command(
        "010_squashed",
        [source],
        out,
        dialect="sqlite",
        format="json",
        interactive=False,
    )

    output = capsys.readouterr().out
    assert "warning: squash warning" in output
    assert str(out) in output
    assert writes == [(out, "json")]
