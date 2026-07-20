from __future__ import annotations

from pathlib import Path

from textual.widgets import Button, Checkbox, Input, Static, TextArea

from ormdantic.migrations import (
    MigrationArtifact,
    MigrationOperation,
    MigrationPlan,
    SchemaSnapshot,
)
from ormdantic.playground.app import PlaygroundApp
from ormdantic.playground.config import (
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.controller import PlaygroundController
from ormdantic.playground.safety import PreflightContext
from ormdantic.playground.widgets.action_dialog import ActionDialog


def config(tmp_path: Path, *, typed: bool = False) -> EffectiveConfig:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations),
        environment=EnvironmentConfig(
            name="staging" if typed else "development",
            safety="typed" if typed else "confirm",
        ),
    )


def controller(tmp_path: Path, *, typed: bool = False) -> PlaygroundController:
    effective = config(tmp_path, typed=typed)
    migration = MigrationArtifact.from_plan(
        "002_remove_legacy",
        MigrationPlan(
            operations=[
                MigrationOperation(
                    sql="DROP TABLE legacy",
                    kind="drop_table",
                    destructive=True,
                )
            ],
            rollback_operations=[
                MigrationOperation(sql="CREATE TABLE legacy (id INTEGER)")
            ],
        ),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )
    path = effective.project.migrations_dir / "002_remove_legacy.toml"
    migration.write(path)
    value = PlaygroundController(effective)
    value.select_artifact(path)
    return value


def context(checksum: str, generation: int) -> PreflightContext:
    return PreflightContext(
        connected=True,
        target_imported=True,
        dialect="sqlite",
        artifact_dialect="sqlite",
        history_readable=True,
        history_dirty=False,
        artifact_valid=True,
        checksum_valid=True,
        dependencies_valid=True,
        revision_state_valid=True,
        rollback_available=True,
        snapshot_current=True,
        operations_supported=True,
        operation_running=False,
        editor_valid=True,
        editor_dirty=False,
        sql_present=True,
        destructive_reviewed=True,
        artifact_checksum=checksum,
        generation=generation,
    )


async def test_typed_action_dialog_shows_full_context_and_exact_phrase(
    tmp_path: Path,
) -> None:
    value = controller(tmp_path, typed=True)
    app = PlaygroundApp(config=value.config, controller=value)
    request = value.build_action_request("apply", database_name="staging_db")
    dialog = ActionDialog(
        controller=value,
        request=request,
        context=context(request.artifact_checksum or "", value.state.generation),
    )

    async with app.run_test(size=(120, 38)) as pilot:
        await app.push_screen(dialog)
        await pilot.pause()

        summary = app.screen.query_one("#action-summary", Static).render().plain
        sql = app.screen.query_one("#action-sql", TextArea).text
        phrase = app.screen.query_one("#action-confirmation", Input)
        execute = app.screen.query_one("#action-execute", Button)
        assert "staging" in summary
        assert "staging_db" in summary
        assert "002_remove_legacy" in summary
        assert "DESTRUCTIVE" in summary
        assert "DROP TABLE legacy" in sql
        assert execute.disabled is True

        phrase.value = "wrong"
        await pilot.pause()
        assert execute.disabled is True
        phrase.value = "staging_db 002_remove_legacy"
        await pilot.pause()
        assert execute.disabled is False


async def test_destructive_confirm_policy_requires_review_checkbox(
    tmp_path: Path,
) -> None:
    value = controller(tmp_path)
    app = PlaygroundApp(config=value.config, controller=value)
    request = value.build_action_request("apply", database_name="app_db")
    preflight = context(request.artifact_checksum or "", value.state.generation)
    preflight = PreflightContext(
        **{**preflight.__dict__, "destructive_reviewed": False}
    )

    async with app.run_test(size=(120, 38)) as pilot:
        await app.push_screen(
            ActionDialog(controller=value, request=request, context=preflight)
        )
        await pilot.pause()

        execute = app.screen.query_one("#action-execute", Button)
        review = app.screen.query_one("#action-review", Checkbox)
        assert execute.disabled is True
        review.value = True
        await pilot.pause()
        assert execute.disabled is False


async def test_preflight_failures_remain_visible_and_block_execution(
    tmp_path: Path,
) -> None:
    value = controller(tmp_path)
    app = PlaygroundApp(config=value.config, controller=value)
    request = value.build_action_request("apply", database_name="app_db")
    blocked = context(request.artifact_checksum or "", value.state.generation)
    blocked = PreflightContext(**{**blocked.__dict__, "history_dirty": True})

    async with app.run_test(size=(120, 38)) as pilot:
        await app.push_screen(
            ActionDialog(controller=value, request=request, context=blocked)
        )
        await pilot.pause()

        preflight = app.screen.query_one("#action-preflight", Static).render().plain
        assert "history is dirty" in preflight
        assert app.screen.query_one("#action-execute", Button).disabled is True
