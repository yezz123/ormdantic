from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from textual.widgets import Button, Input, Static, TextArea

from ormdantic.migrations import (
    MigrationArtifact,
    MigrationChange,
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
from ormdantic.playground.screens.workflows import (
    GenerateDialog,
    RepairDialog,
    SquashDialog,
)
from ormdantic.playground.state import MigrationState, SchemaState


def configured_controller(tmp_path: Path) -> PlaygroundController:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    env_file = tmp_path / ".env"
    env_file.write_text("PLAYGROUND_TEST_URL=sqlite:///app.db\n")
    config = EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations),
        environment=EnvironmentConfig(
            name="development",
            url_env="PLAYGROUND_TEST_URL",
            env_file=env_file,
        ),
    )
    return PlaygroundController(config)


def healthy_schema() -> SchemaState:
    snapshot = SchemaSnapshot.empty()
    return SchemaState(
        model_snapshot=snapshot,
        live_snapshot=snapshot,
        diff=SchemaDiff(
            changes=[
                MigrationChange(
                    action="add",
                    object_type="table",
                    table="users",
                    name=None,
                    message="Create users",
                )
            ]
        ),
        forward_sql=("CREATE TABLE users (id INTEGER)",),
        stale=False,
    )


async def test_generate_dialog_requires_valid_revision_and_shows_all_sql(
    tmp_path: Path,
) -> None:
    controller = configured_controller(tmp_path)
    controller.state = replace(controller.state, schema=healthy_schema())
    app = PlaygroundApp(config=controller.config, controller=controller)

    async with app.run_test(size=(110, 36)) as pilot:
        await pilot.press("g")
        await pilot.pause()
        assert isinstance(app.screen, GenerateDialog)
        assert (
            "CREATE TABLE users" in app.screen.query_one("#generate-sql", TextArea).text
        )
        revision = app.screen.query_one("#generate-revision", Input)
        execute = app.screen.query_one("#generate-execute", Button)
        revision.value = "../invalid"
        await pilot.pause()
        assert execute.disabled is True
        revision.value = "002_add_users"
        await pilot.pause()
        assert execute.disabled is False
        app.screen.cancel()
        await pilot.pause()
        assert not isinstance(app.screen, GenerateDialog)


async def test_repair_dialog_always_requires_exact_typed_phrase(
    tmp_path: Path,
) -> None:
    controller = configured_controller(tmp_path)
    controller.state = replace(
        controller.state,
        schema=healthy_schema(),
        migrations=MigrationState(
            history=(
                MigrationHistoryEntry(
                    revision="001_failed",
                    status="failed",
                    dirty=True,
                ),
            ),
            dirty=True,
        ),
        dialect="sqlite",
    )
    app = PlaygroundApp(config=controller.config, controller=controller)

    async with app.run_test(size=(110, 36)) as pilot:
        app.open_repair_dialog("001_failed")
        await pilot.pause()
        assert isinstance(app.screen, RepairDialog)
        phrase = app.screen.query_one("#repair-confirmation", Input)
        execute = app.screen.query_one("#repair-execute", Button)
        phrase.value = "app.db 001_failed "
        await pilot.pause()
        assert execute.disabled is True
        phrase.value = "app.db 001_failed"
        await pilot.pause()
        assert execute.disabled is False
        await pilot.click("#repair-cancel")
        await pilot.pause()
        assert not isinstance(app.screen, RepairDialog)


async def test_squash_dialog_previews_every_pending_file_and_sql(
    tmp_path: Path,
) -> None:
    controller = configured_controller(tmp_path)
    paths: list[Path] = []
    for revision, sql in (("001_users", "SELECT 1"), ("002_accounts", "SELECT 2")):
        artifact = MigrationArtifact.from_plan(
            revision,
            MigrationPlan(operations=[MigrationOperation(sql=sql)]),
            SchemaSnapshot.empty(),
            SchemaSnapshot.empty(),
            dialect="sqlite",
        )
        path = controller.config.project.migrations_dir / f"{revision}.toml"
        artifact.write(path)
        paths.append(path)
    controller.reload_workspace()
    controller.state = replace(
        controller.state,
        schema=healthy_schema(),
        dialect="sqlite",
    )
    app = PlaygroundApp(config=controller.config, controller=controller)

    async with app.run_test(size=(110, 40)) as pilot:
        app.open_squash_dialog(tuple(paths))
        await pilot.pause()
        assert isinstance(app.screen, SquashDialog)
        sql = app.screen.query_one("#squash-sql", TextArea).text
        assert "SELECT 1" in sql
        assert "SELECT 2" in sql
        execute = app.screen.query_one("#squash-execute", Button)
        phrase = app.screen.query_one("#squash-confirmation", Input)
        phrase.value = "app.db squashed_001_users_002_accounts"
        await pilot.pause()
        assert execute.disabled is False
        await pilot.click("#squash-cancel")
        await pilot.pause()
        assert not isinstance(app.screen, SquashDialog)


async def test_generate_execution_error_then_success(
    tmp_path: Path,
    monkeypatch,
) -> None:
    controller = configured_controller(tmp_path)
    controller.state = replace(controller.state, schema=healthy_schema())
    app = PlaygroundApp(config=controller.config, controller=controller)
    dialog = GenerateDialog(controller)

    async def fail(*_args, **_kwargs):
        raise ValueError("generation failed")

    generated = tmp_path / "migrations" / "002_users.toml"

    async def succeed(*_args, **_kwargs):
        return generated

    async with app.run_test() as pilot:
        await app.push_screen(dialog)
        dialog.query_one("#generate-revision", Input).value = "002_users"
        dialog.query_one("#generate-description", Input).value = "  "
        monkeypatch.setattr(controller, "generate_migration", fail)
        await dialog.execute()
        assert (
            "generation failed"
            in dialog.query_one("#generate-error", Static).render().plain
        )

        monkeypatch.setattr(controller, "generate_migration", succeed)
        await dialog.execute()
        await pilot.pause()
        assert not isinstance(app.screen, GenerateDialog)


async def test_repair_and_squash_execution_errors_are_visible(
    tmp_path: Path,
    monkeypatch,
) -> None:
    controller = configured_controller(tmp_path)
    controller.state = replace(
        controller.state,
        schema=healthy_schema(),
        migrations=MigrationState(
            history=(
                MigrationHistoryEntry(
                    revision="001_failed",
                    status="failed",
                    dirty=True,
                ),
            ),
            dirty=True,
        ),
        dialect="sqlite",
    )
    app = PlaygroundApp(config=controller.config, controller=controller)

    async def failed(*_args, **_kwargs):
        return ControllerActionOutcome(
            SafetyDecision(True, None),
            executed=False,
            error=Diagnostic.create(Severity.ERROR, "test", "workflow failed"),
        )

    async def succeeded(*_args, **_kwargs):
        return ControllerActionOutcome(SafetyDecision(True, None), executed=True)

    async with app.run_test() as pilot:
        app.open_repair_dialog("001_failed")
        await pilot.pause()
        repair = app.screen
        assert isinstance(repair, RepairDialog)
        monkeypatch.setattr(controller, "execute_repair", failed)
        await repair.execute()
        assert (
            "workflow failed"
            in repair.query_one("#repair-preflight", Static).render().plain
        )
        monkeypatch.setattr(controller, "execute_repair", succeeded)
        await repair.execute()
        await pilot.pause()
        assert not isinstance(app.screen, RepairDialog)

        paths: list[Path] = []
        for revision in ("002_one", "003_two"):
            artifact = MigrationArtifact.from_plan(
                revision,
                MigrationPlan(operations=[MigrationOperation(sql="SELECT 1")]),
                SchemaSnapshot.empty(),
                SchemaSnapshot.empty(),
                dialect="sqlite",
            )
            path = controller.config.project.migrations_dir / f"{revision}.toml"
            artifact.write(path)
            paths.append(path)
        controller.reload_workspace()
        app.open_squash_dialog(tuple(paths))
        await pilot.pause()
        squash = app.screen
        assert isinstance(squash, SquashDialog)
        squash.query_one("#squash-revision", Input).value = "../invalid"
        squash._refresh_decision()
        assert (
            "revision name is invalid"
            in squash.query_one("#squash-preflight", Static).render().plain
        )
        monkeypatch.setattr(controller, "execute_squash", failed)
        await squash.execute()
        assert (
            "workflow failed"
            in squash.query_one("#squash-preflight", Static).render().plain
        )
        monkeypatch.setattr(controller, "execute_squash", succeeded)
        await squash.execute()
        await pilot.pause()
        assert not isinstance(app.screen, SquashDialog)
