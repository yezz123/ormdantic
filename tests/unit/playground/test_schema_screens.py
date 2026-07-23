from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from textual.widgets import DataTable, Static, TextArea

from ormdantic.migrations import (
    ColumnSnapshot,
    MigrationChange,
    SchemaDiff,
    SchemaSnapshot,
    TableSnapshot,
)
from ormdantic.playground.app import PlaygroundApp
from ormdantic.playground.config import (
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.controller import PlaygroundController
from ormdantic.playground.state import PlaygroundState, RefreshStatus, SchemaState
from ormdantic.playground.widgets.schema_tree import SchemaTree


def config(tmp_path: Path) -> EffectiveConfig:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations),
        environment=EnvironmentConfig(name="development"),
    )


def snapshot(*, with_email: bool = False) -> SchemaSnapshot:
    columns = [
        ColumnSnapshot("id", "integer", False, True),
        ColumnSnapshot("name", "string", False, False),
    ]
    if with_email:
        columns.append(ColumnSnapshot("email", "string", True, False))
    return SchemaSnapshot(
        tables=[
            TableSnapshot(
                model_key="tests.User",
                name="users",
                primary_key="id",
                columns=columns,
            )
        ]
    )


def state() -> PlaygroundState:
    change = MigrationChange(
        action="add",
        object_type="column",
        table="users",
        name="email",
        message="Add nullable email column",
        unsafe=False,
        destructive=False,
    )
    return PlaygroundState(
        environment="development",
        connection_label="DATABASE_URL",
        dialect="sqlite",
        generation=3,
        status=RefreshStatus.HEALTHY,
        schema=SchemaState(
            model_snapshot=snapshot(with_email=True),
            live_snapshot=snapshot(),
            diff=SchemaDiff(changes=[change]),
            forward_sql=("ALTER TABLE users ADD COLUMN email TEXT",),
            rollback_sql=("ALTER TABLE users DROP COLUMN email",),
        ),
    )


def app_with_state(tmp_path: Path, value: PlaygroundState) -> PlaygroundApp:
    effective = config(tmp_path)
    controller = PlaygroundController(effective)
    controller.state = value
    return PlaygroundApp(config=effective, controller=controller)


async def test_overview_cards_summarize_current_health(tmp_path: Path) -> None:
    app = app_with_state(tmp_path, state())

    async with app.run_test(size=(120, 35)) as pilot:
        await pilot.pause()

        assert "HEALTHY" in app.query_one("#overview-health", Static).render().plain
        assert "1" in app.query_one("#overview-drift", Static).render().plain
        assert (
            "DATABASE_URL"
            in app.query_one("#overview-connection", Static).render().plain
        )


async def test_schema_screen_renders_model_and_live_trees(tmp_path: Path) -> None:
    app = app_with_state(tmp_path, state())

    async with app.run_test(size=(120, 35)) as pilot:
        await pilot.click("#nav-schema")
        await pilot.pause()

        model = app.query_one("#model-schema", SchemaTree)
        live = app.query_one("#live-schema", SchemaTree)
        model_labels = [child.label.plain for child in model.root.children]
        live_labels = [child.label.plain for child in live.root.children]
        assert any("users" in label for label in model_labels)
        assert any("users" in label for label in live_labels)
        assert "1 table" in app.query_one("#model-schema-count", Static).render().plain


async def test_drift_screen_renders_change_and_sql_preview(tmp_path: Path) -> None:
    app = app_with_state(tmp_path, state())

    async with app.run_test(size=(120, 35)) as pilot:
        await pilot.click("#nav-drift")
        await pilot.pause()

        table = app.query_one("#drift-table", DataTable)
        preview = app.query_one("#drift-sql", TextArea)
        assert table.row_count == 1
        assert "ADD COLUMN email" in preview.text
        assert "SAFE" in app.query_one("#drift-summary", Static).render().plain


async def test_schema_views_keep_available_side_when_other_source_fails(
    tmp_path: Path,
) -> None:
    partial = replace(
        state(),
        status=RefreshStatus.PARTIAL,
        schema=SchemaState(model_snapshot=snapshot(), live_snapshot=None, stale=True),
    )
    app = app_with_state(tmp_path, partial)

    async with app.run_test(size=(120, 35)) as pilot:
        await pilot.click("#nav-schema")
        await pilot.pause()

        assert app.query_one("#model-schema", SchemaTree).root.children
        assert not app.query_one("#live-schema", SchemaTree).root.children
        assert (
            "unavailable" in app.query_one("#live-schema-count", Static).render().plain
        )


async def test_published_state_updates_visible_views(tmp_path: Path) -> None:
    app = app_with_state(tmp_path, PlaygroundState(environment="development"))

    async with app.run_test(size=(120, 35)) as pilot:
        await pilot.pause()
        app._state_changed(state())
        await pilot.pause()

        assert "HEALTHY" in app.query_one("#overview-health", Static).render().plain
        assert app.query_one("#drift-table", DataTable).row_count == 1
