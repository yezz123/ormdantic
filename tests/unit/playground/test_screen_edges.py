from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from textual.widgets import DataTable, Input, Select, Static, TextArea

from ormdantic.migrations import (
    ColumnSnapshot,
    EnumTypeSnapshot,
    IndexSnapshot,
    MigrationArtifact,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
    TableCheckSnapshot,
    TableSnapshot,
    ViewSnapshot,
)
from ormdantic.playground.app import PlaygroundApp
from ormdantic.playground.config import (
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.controller import PlaygroundController
from ormdantic.playground.diagnostics import Diagnostic, Severity
from ormdantic.playground.screens.drift import DriftView
from ormdantic.playground.screens.editor import EditorView
from ormdantic.playground.screens.history import HistoryView
from ormdantic.playground.screens.migrations import MigrationsView
from ormdantic.playground.screens.settings import SettingsView
from ormdantic.playground.screens.setup import SetupScreen
from ormdantic.playground.widgets.diagnostic_list import DiagnosticList
from ormdantic.playground.widgets.migration_list import MigrationList
from ormdantic.playground.widgets.navigation import NavigationRail
from ormdantic.playground.widgets.operation_list import OperationList
from ormdantic.playground.widgets.schema_tree import SchemaTree


def configured(tmp_path: Path) -> tuple[EffectiveConfig, PlaygroundController, Path]:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    path = tmp_path / "ormdantic.toml"
    path.write_text(
        """\
[project]
target = "app:db"
migrations_dir = "migrations"

[environments.development]
url_env = "DATABASE_URL"
"""
    )
    effective = EffectiveConfig(
        path=path,
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations),
        environment=EnvironmentConfig(name="development", env_file=None),
    )
    artifact = MigrationArtifact.from_plan(
        "001_users",
        MigrationPlan(
            operations=[MigrationOperation(sql="CREATE TABLE users (id INTEGER)")],
            rollback_operations=[MigrationOperation(sql="DROP TABLE users")],
        ),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )
    artifact_path = migrations / "001_users.toml"
    artifact.write(artifact_path)
    controller = PlaygroundController(effective)
    controller.select_artifact(artifact_path)
    return effective, controller, artifact_path


async def test_setup_validation_write_failure_and_cancel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = PlaygroundApp(config=None, setup_path=tmp_path / "ormdantic.toml")

    async with app.run_test() as pilot:
        screen = app.screen
        assert isinstance(screen, SetupScreen)
        screen.save()
        assert "model target" in screen.query_one("#setup-error", Static).render().plain

        screen.query_one("#setup-target", Input).value = "app:db"
        screen.query_one("#setup-migrations", Input).value = ""
        screen.save()
        assert "migrations" in screen.query_one("#setup-error", Static).render().plain

        screen.query_one("#setup-migrations", Input).value = "migrations"
        screen.query_one("#setup-url-env", Input).value = ""
        screen.save()
        assert (
            "environment variable"
            in screen.query_one("#setup-error", Static).render().plain
        )

        screen.query_one("#setup-url-env", Input).value = "DATABASE_URL"
        monkeypatch.setattr(
            "ormdantic.playground.screens.setup.write_config",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("read only")),
        )
        screen.save()
        assert "read only" in screen.query_one("#setup-error", Static).render().plain
        screen.cancel()
        await pilot.pause()


async def test_settings_fallbacks_and_event_guards(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effective, controller, _ = configured(tmp_path)
    app = PlaygroundApp(config=effective, controller=controller, auto_watch=False)

    async with app.run_test() as pilot:
        view = app.query_one("#view-settings", SettingsView)
        effective.path.write_text("[invalid")
        assert view._environment_options() == [("development", "development")]

        view._loading = True
        view.environment_changed(SimpleNamespace(value="staging"))
        view._loading = False
        view.environment_changed(SimpleNamespace(value=Select.NULL))
        view.environment_changed(SimpleNamespace(value="development"))

        monkeypatch.setattr(
            app,
            "switch_environment",
            lambda _name: (_ for _ in ()).throw(ValueError("unknown environment")),
        )
        view.environment_changed(SimpleNamespace(value="staging"))
        assert (
            "unknown environment"
            in view.query_one("#settings-error", Static).render().plain
        )

        view.config = None
        view.save_source()
        assert "not available" in view._source()
        assert view._environment_options() == [("development", "development")]
        view.update_state(controller.state)
        view.toggle_watcher()
        await pilot.pause()
        assert controller.state.watcher_paused is True


async def test_editor_failure_guards_and_secondary_views(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effective, controller, _ = configured(tmp_path)
    app = PlaygroundApp(config=effective, controller=controller, auto_watch=False)
    notices: list[tuple[str, str]] = []

    async with app.run_test() as pilot:
        monkeypatch.setattr(
            app,
            "notify",
            lambda message, *, severity="information", **_kwargs: notices.append(
                (message, severity)
            ),
        )
        editor = app.query_one("#view-editor", EditorView)
        editor._schedule_draft()
        editor._schedule_draft()

        monkeypatch.setattr(
            controller,
            "save_active",
            lambda: (_ for _ in ()).throw(ValueError("save failed")),
        )
        editor.save()
        assert notices[-1] == ("save failed", "error")

        monkeypatch.setattr(
            controller,
            "convert_active_to_toml",
            lambda: (_ for _ in ()).throw(ValueError("convert failed")),
        )
        editor.convert()
        assert notices[-1] == ("convert failed", "error")

        monkeypatch.setattr(
            controller,
            "write_active_draft",
            lambda: (_ for _ in ()).throw(OSError("disk full")),
        )
        editor._save_draft()
        assert notices[-1] == (
            "Could not save recovery draft: disk full",
            "warning",
        )

        app.controller = None
        editor.save()
        editor.convert()
        editor._load_selected_sql()
        editor._save_draft()

        migrations = app.query_one("#view-migrations", MigrationsView)
        migrations.open_editor()
        migrations.squash_pending()
        history = app.query_one("#view-history", HistoryView)
        history.repair_history()
        drift = app.query_one("#view-drift", DriftView)
        drift.generate()
        await pilot.pause()


async def test_rich_schema_and_diagnostics_widgets(tmp_path: Path) -> None:
    effective, controller, _ = configured(tmp_path)
    app = PlaygroundApp(config=effective, controller=controller, auto_watch=False)
    snapshot = SchemaSnapshot(
        tables=[
            TableSnapshot(
                model_key="app.User",
                name="users",
                primary_key="id",
                columns=[
                    ColumnSnapshot("id", "integer", False, True),
                    ColumnSnapshot(
                        "team_id",
                        "integer",
                        True,
                        False,
                        foreign_table="teams",
                    ),
                ],
                indexes=[IndexSnapshot("ix_users_team", ["team_id"])],
                check_constraints=[TableCheckSnapshot("ck_team", "team_id > 0")],
            )
        ],
        views=[ViewSnapshot("active_users", "SELECT * FROM users", schema="main")],
        enum_types=[EnumTypeSnapshot("status", ["open", "done"])],
    )

    async with app.run_test() as pilot:
        tree = app.query_one("#model-schema", SchemaTree)
        tree.update_snapshot(snapshot)
        labels = [node.label.plain for node in tree.root.children]
        assert any("users" in label for label in labels)
        assert any("active_users" in label for label in labels)
        assert any("status" in label for label in labels)

        diagnostics = app.query_one(DiagnosticList)
        diagnostics.update_diagnostics(
            (
                Diagnostic.create(
                    Severity.ERROR,
                    "test.error",
                    "Could not inspect schema",
                    hint="Check the target",
                ),
                Diagnostic.create(Severity.WARNING, "test.warning", "Drift exists"),
                Diagnostic.create(Severity.INFO, "test.info", "Watching files"),
            )
        )
        rendered = diagnostics.render().plain
        assert "Check the target" in rendered
        assert "Watching files" in rendered

        operations = app.query_one("#operation-list", OperationList)
        operations.update_document(None)
        await pilot.pause()


async def test_remaining_screen_success_and_selection_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effective, controller, artifact_path = configured(tmp_path)
    effective.path.write_text(
        effective.path.read_text()
        + '\n[environments.staging]\nurl_env = "DATABASE_URL"\n'
    )
    app = PlaygroundApp(config=effective, controller=controller, auto_watch=False)
    notices: list[str] = []

    async with app.run_test() as pilot:
        monkeypatch.setattr(
            app,
            "notify",
            lambda message, **_kwargs: notices.append(message),
        )
        editor = app.query_one("#view-editor", EditorView)
        editor.save()
        assert any("Saved" in message for message in notices)

        app.controller = None
        editor.source_changed(
            SimpleNamespace(text_area=app.query_one("#artifact-source", TextArea))
        )
        app.controller = controller
        controller.workspace = controller.workspace.__class__(
            documents=controller.workspace.documents,
            selected_path=None,
        )
        editor._selected_operation = (False, 0)
        sql = app.query_one("#operation-sql", TextArea)
        sql.disabled = False
        editor.sql_changed(SimpleNamespace(text_area=sql))

        controller.select_artifact(artifact_path)
        editor._selected_operation = (False, 99)
        editor._load_selected_sql()
        assert "no longer exists" in sql.text

        document = controller.active_document
        assert document is not None
        changed_document = document.__class__(
            path=document.path,
            format=document.format,
            source=document.source + "\n# synchronized",
            artifact=document.artifact,
        )
        monkeypatch.setattr(
            controller,
            "edit_active_sql",
            lambda *_args, **_kwargs: changed_document,
        )
        editor._selected_operation = (False, 0)
        editor._loading = False
        sql.disabled = False
        sql.load_text("SELECT 2")
        editor.sql_changed(SimpleNamespace(text_area=sql))
        assert "synchronized" in app.query_one("#artifact-source", TextArea).text

        migrations = app.query_one("#view-migrations", MigrationsView)
        table = app.query_one("#migration-table", DataTable)
        original_select = controller.select_artifact
        calls = 0

        def select_with_relative_fallback(path: Path) -> None:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise ValueError("use direct path")
            original_select(path)

        monkeypatch.setattr(
            controller, "select_artifact", select_with_relative_fallback
        )
        migrations._loading = False
        migrations.select_migration(
            SimpleNamespace(
                cursor_row=table.cursor_row,
                row_key=SimpleNamespace(value=str(artifact_path)),
            )
        )
        assert calls == 2

        repaired: list[str] = []
        monkeypatch.setattr(app, "open_repair_dialog", repaired.append)
        history = app.query_one("#view-history", HistoryView)
        history._selected_revision = "001_users"
        history.repair_history()
        assert repaired == ["001_users"]

        schema = app.query_one("#view-schema")
        schema.show_selection(
            SimpleNamespace(node=SimpleNamespace(data={"kind": "table"}))
        )
        assert "table" in app.query_one("#schema-selection", Static).render().plain

        settings = app.query_one("#view-settings", SettingsView)
        settings.environment_changed(SimpleNamespace(value="staging"))
        await pilot.pause()
        assert app.config is not None
        assert app.config.environment.name == "staging"

        empty_artifact = MigrationArtifact.from_plan(
            "003_empty",
            MigrationPlan(),
            SchemaSnapshot.empty(),
            SchemaSnapshot.empty(),
            dialect="sqlite",
        )
        document = controller.workspace.documents[0]
        app.query_one("#operation-list", OperationList).update_document(
            document.__class__(
                path=document.path,
                format=document.format,
                source=document.source,
                artifact=empty_artifact,
            )
        )
        app.query_one("#migration-table", MigrationList).update_artifacts(
            controller.state.migrations.artifacts,
            tmp_path / "not-selected.toml",
        )

    MigrationList().update_artifacts(())
    NavigationRail().navigate(SimpleNamespace(button=SimpleNamespace(id=None)))
    EditorView(controller.state).update_state(controller.state)
    SettingsView(controller.state, effective).update_config(effective)
