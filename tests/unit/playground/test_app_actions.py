from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from textual.widgets import ContentSwitcher, Static

from ormdantic.migrations import (
    MigrationArtifact,
    MigrationHistoryEntry,
    MigrationOperation,
    MigrationPlan,
    SchemaDiff,
    SchemaSnapshot,
)
from ormdantic.playground.app import PlaygroundApp
from ormdantic.playground.config import (
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.controller import (
    ControllerActionOutcome,
    PlaygroundController,
)
from ormdantic.playground.diagnostics import Diagnostic, Severity
from ormdantic.playground.safety import SafetyDecision
from ormdantic.playground.screens.workflows import RepairDialog, SquashDialog
from ormdantic.playground.state import PlaygroundState, SchemaState
from ormdantic.playground.watcher import WatchEvent, WatchReason
from ormdantic.playground.widgets.action_dialog import ActionDialog


def configured(tmp_path: Path) -> tuple[EffectiveConfig, PlaygroundController, Path]:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    env_file = tmp_path / ".env"
    env_file.write_text("TEST_DATABASE_URL=sqlite:///todo.db\n")
    config_path = tmp_path / "ormdantic.toml"
    config_path.write_text(
        """\
[project]
target = "app:db"
migrations_dir = "migrations"

[environments.development]
url_env = "TEST_DATABASE_URL"
env_file = ".env"

[environments.staging]
url_env = "TEST_DATABASE_URL"
env_file = ".env"
safety = "typed"
"""
    )
    effective = EffectiveConfig(
        path=config_path,
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations),
        environment=EnvironmentConfig(
            name="development",
            url_env="TEST_DATABASE_URL",
            env_file=env_file,
        ),
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
    path = migrations / "001_users.toml"
    artifact.write(path)
    controller = PlaygroundController(effective)
    return effective, controller, path


def test_from_cli_discovers_and_resolves_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    missing = tmp_path / "missing.toml"
    assert (
        PlaygroundApp.from_cli(
            config_path=missing,
            environment=None,
            target=None,
            migrations_dir=None,
        ).config
        is None
    )

    effective, _, _ = configured(tmp_path)
    app = PlaygroundApp.from_cli(
        config_path=None,
        environment="staging",
        target="new.module:db",
        migrations_dir=tmp_path / "other-migrations",
    )

    assert app.config is not None
    assert app.config.path == effective.path
    assert app.config.environment.name == "staging"
    assert app.config.project.target == "new.module:db"


async def test_actions_callbacks_and_guard_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effective, controller, path = configured(tmp_path)
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
        app.action_save()
        assert notices[-1][1] == "warning"

        controller.select_artifact(path)

        def fail_save() -> object:
            raise ValueError("cannot save")

        monkeypatch.setattr(controller, "save_active", fail_save)
        app.action_save()
        assert notices[-1] == ("cannot save", "error")

        refreshed: list[int | None] = []

        async def refresh(*, generation: int | None = None):
            refreshed.append(generation)
            return controller.state

        monkeypatch.setattr(controller, "refresh", refresh)
        app.action_refresh()
        await pilot.pause()
        assert refreshed == [None]

        app.action_pause_watcher()
        assert controller.state.watcher_paused is True
        app._show_section("does-not-exist")
        assert app.query_one(ContentSwitcher).current == "view-overview"

        app.controller = None
        app.action_refresh()
        app.action_pause_watcher()
        app.open_generate_dialog()
        app.open_repair_dialog("missing")
        app.open_squash_dialog(())
        app.config = None
        with pytest.raises(ValueError, match="setup"):
            app.switch_environment("staging")
        with pytest.raises(ValueError, match="setup"):
            app.save_config_source("")
        with pytest.raises(ValueError, match="not configured"):
            app._workflow_preflight(
                controller.build_action_request("apply", database_name="todo"),
                revision_state_valid=True,
            )
        assert app._database_name() == "database"

        app.controller = controller
        app.config = effective
        controller.state = replace(controller.state, schema=SchemaState(stale=True))
        app.open_generate_dialog()
        assert notices[-1][1] == "warning"

        app._action_complete(None)
        failure = ControllerActionOutcome(
            SafetyDecision(False, None, ("blocked",)),
            executed=False,
            error=Diagnostic.create(Severity.ERROR, "test", "operation failed"),
        )
        app._action_complete(failure)
        assert notices[-1] == ("operation failed", "error")
        app._action_complete(
            ControllerActionOutcome(SafetyDecision(True, None), executed=True)
        )
        assert "completed" in notices[-1][0]
        app._action_complete(
            ControllerActionOutcome(SafetyDecision(True, None), executed=False)
        )
        app._generate_complete(None)
        app._generate_complete(path)
        assert app.query_one(ContentSwitcher).current == "view-editor"


async def test_repair_squash_preflight_and_watch_reload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effective, controller, path = configured(tmp_path)
    snapshot = SchemaSnapshot.empty()
    controller.state = replace(
        controller.state,
        dialect="sqlite",
        schema=SchemaState(
            model_snapshot=snapshot,
            live_snapshot=snapshot,
            diff=SchemaDiff(),
            forward_sql=("SELECT 1",),
        ),
        migrations=replace(
            controller.state.migrations,
            history=(
                MigrationHistoryEntry(
                    revision="001_users",
                    status="failed",
                    dirty=True,
                ),
            ),
            dirty=True,
        ),
    )
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
        app.open_repair_dialog("001_users")
        await pilot.pause()
        assert isinstance(app.screen, RepairDialog)
        app.pop_screen()

        controller.reload_workspace()
        controller.select_artifact(path)
        duplicate = effective.project.migrations_dir / "002_more.toml"
        MigrationArtifact.from_plan(
            "002_more",
            MigrationPlan(
                operations=[MigrationOperation(sql="ALTER TABLE users ADD name TEXT")],
                rollback_operations=[
                    MigrationOperation(sql="ALTER TABLE users DROP COLUMN name")
                ],
            ),
            snapshot,
            snapshot,
            dialect="sqlite",
        ).write(duplicate)
        controller.reload_workspace()
        app.open_squash_dialog((path, duplicate))
        await pilot.pause()
        assert isinstance(app.screen, SquashDialog)
        app.pop_screen()

        monkeypatch.setattr(
            controller,
            "build_squash_request",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad squash")),
        )
        app.open_squash_dialog((path, duplicate))
        assert notices[-1] == ("bad squash", "error")

        reloaded: list[bool] = []
        refreshed: list[int | None] = []
        monkeypatch.setattr(
            controller, "reload_workspace", lambda: reloaded.append(True)
        )

        async def refresh(*, generation: int | None = None):
            refreshed.append(generation)
            return controller.state

        monkeypatch.setattr(controller, "refresh", refresh)
        await app._watch_event(WatchEvent(9, (WatchReason.FILES,), (path,)))
        assert reloaded == [True]
        assert refreshed == [9]

        controller.edit_active_source("revision = [")
        await app._watch_event(WatchEvent(10, (WatchReason.FILES,), (path,)))
        assert reloaded == [True]
        assert refreshed == [9, 10]


async def test_environment_switch_and_database_name(tmp_path: Path) -> None:
    effective, controller, _ = configured(tmp_path)
    app = PlaygroundApp(config=effective, controller=controller, auto_watch=False)

    async with app.run_test() as pilot:
        app.switch_environment("staging")
        await pilot.pause()
        assert app.config is not None
        assert app.config.environment.name == "staging"
        assert app.controller is not controller
        assert app._database_name() == "todo.db"


async def test_action_dialog_opening_preflight_and_error_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effective, controller, path = configured(tmp_path)
    controller.select_artifact(path)
    snapshot = SchemaSnapshot.empty()
    controller.state = replace(
        controller.state,
        dialect="sqlite",
        schema=SchemaState(
            model_snapshot=snapshot,
            live_snapshot=snapshot,
            diff=SchemaDiff(),
            forward_sql=("SELECT 1",),
        ),
    )
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
        context = app._preflight_context("apply")
        assert context.connected is True
        assert context.target_imported is True
        assert context.revision_state_valid is True
        assert context.operations_supported is True

        original_workspace = controller.workspace
        document = controller.active_document
        assert document is not None

        def invalid_checksum() -> None:
            raise ValueError("changed")

        broken = SimpleNamespace(
            validate_checksum=invalid_checksum,
            dialect="sqlite",
            operations=(MigrationOperation(sql="SELECT 1"),),
            rollback_operations=(),
            checksum="invalid",
        )
        controller.workspace = replace(
            controller.workspace,
            documents=(replace(document, artifact=broken),),
        )
        assert app._preflight_context("apply").checksum_valid is False
        assert app._preflight_context("rollback").operations_supported is False
        controller.workspace = original_workspace
        controller.workspace = replace(
            controller.workspace,
            documents=(replace(document, artifact=None),),
        )
        empty_context = app._preflight_context("apply")
        assert empty_context.artifact_valid is False
        assert empty_context.sql_present is False
        controller.workspace = original_workspace

        app.action_apply_migration()
        await pilot.pause()
        assert isinstance(app.screen, ActionDialog)
        app.pop_screen()

        app.action_rollback_migration()
        await pilot.pause()
        assert isinstance(app.screen, ActionDialog)
        app.pop_screen()

        monkeypatch.setattr(
            controller,
            "build_action_request",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad action")),
        )
        app._open_action_dialog("apply")
        assert notices[-1] == ("bad action", "error")

        controller.workspace = replace(controller.workspace, selected_path=None)
        app._open_action_dialog("apply")
        assert notices[-1][1] == "warning"
        with pytest.raises(ValueError, match="select a migration"):
            app._preflight_context("apply")
        app.open_repair_dialog("missing")
        assert notices[-1][1] == "error"

        effective.environment.env_file.write_text(
            "TEST_DATABASE_URL=postgresql://database.internal\n"
        )
        assert app._database_name() == "database.internal"

        class Worker:
            def __init__(self) -> None:
                self.cancelled = False

            def cancel(self) -> None:
                self.cancelled = True

        class Watcher:
            def __init__(self) -> None:
                self.stopped = False

            async def stop(self) -> None:
                self.stopped = True

        worker = Worker()
        watcher = Watcher()
        app._watcher_worker = worker
        app._watcher = watcher
        app._restart_watcher()
        await pilot.pause()
        assert worker.cancelled is True
        assert watcher.stopped is True

        await app.screen.mount(Static(classes="state-aware"))
        app._state_changed(controller.state)

        app.controller = None
        await app._watch_event(WatchEvent(11, (WatchReason.DATABASE,)))
        app._state_changed(PlaygroundState(environment="ignored"))


async def test_clean_quit_and_setup_cancel_exit(tmp_path: Path) -> None:
    effective, controller, _ = configured(tmp_path)
    app = PlaygroundApp(config=effective, controller=controller, auto_watch=False)

    async with app.run_test() as pilot:
        await app.action_quit()
        await pilot.pause()

    callback_app = PlaygroundApp(
        config=effective,
        controller=controller,
        auto_watch=False,
    )
    async with callback_app.run_test() as pilot:
        callback_app._quit_complete(True)
        await pilot.pause()

    setup_app = PlaygroundApp(config=None, auto_watch=False)
    async with setup_app.run_test() as pilot:
        setup_app._setup_complete(None)
        await pilot.pause()
