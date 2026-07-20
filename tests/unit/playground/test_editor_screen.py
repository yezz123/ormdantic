from __future__ import annotations

from pathlib import Path

from textual.widgets import Button, DataTable, ListView, Static, TextArea

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
from ormdantic.playground.workspace import draft_path


def config(tmp_path: Path) -> EffectiveConfig:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations),
        environment=EnvironmentConfig(name="development"),
    )


def artifact(revision: str, *, destructive: bool = False) -> MigrationArtifact:
    return MigrationArtifact.from_plan(
        revision,
        MigrationPlan(
            operations=[
                MigrationOperation(
                    sql=f"CREATE TABLE {revision} (id INTEGER)",
                    kind="create_table",
                    destructive=destructive,
                )
            ],
            rollback_operations=[
                MigrationOperation(
                    sql=f"DROP TABLE {revision}",
                    kind="drop_table",
                    destructive=True,
                )
            ],
        ),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )


def app_with_migrations(tmp_path: Path) -> PlaygroundApp:
    effective = config(tmp_path)
    artifact("001_users").write(effective.project.migrations_dir / "001_users.toml")
    artifact("002_accounts", destructive=True).write(
        effective.project.migrations_dir / "002_accounts.json"
    )
    controller = PlaygroundController(effective)
    return PlaygroundApp(config=effective, controller=controller)


async def test_migrations_screen_lists_mixed_formats_and_risk(tmp_path: Path) -> None:
    app = app_with_migrations(tmp_path)

    async with app.run_test(size=(130, 38)) as pilot:
        await pilot.click("#nav-migrations")
        await pilot.pause()

        table = app.query_one("#migration-table", DataTable)
        assert table.row_count == 2
        rendered = "\n".join(
            " ".join(str(cell) for cell in table.get_row_at(index))
            for index in range(table.row_count)
        )
        assert "001_users" in rendered
        assert "002_accounts" in rendered
        assert "toml" in rendered
        assert "json" in rendered


async def test_migration_row_highlight_does_not_republish_state_forever(
    tmp_path: Path,
) -> None:
    app = app_with_migrations(tmp_path)
    counts = {
        "rows": 0,
        "source": 0,
        "sql": 0,
        "operations": 0,
        "edit_source": 0,
        "edit_sql": 0,
    }
    controller = app.controller
    assert controller is not None
    original_source_edit = controller.edit_active_source
    original_sql_edit = controller.edit_active_sql

    def track_source_edit(source: str):
        counts["edit_source"] += 1
        return original_source_edit(source)

    def track_sql_edit(index: int, sql: str, *, rollback: bool = False):
        counts["edit_sql"] += 1
        return original_sql_edit(index, sql, rollback=rollback)

    controller.edit_active_source = track_source_edit  # type: ignore[method-assign]
    controller.edit_active_sql = track_sql_edit  # type: ignore[method-assign]

    def count_highlights(message: object) -> None:
        if isinstance(message, DataTable.RowHighlighted):
            counts["rows"] += 1
        elif isinstance(message, TextArea.Changed):
            key = "source" if message.text_area.id == "artifact-source" else "sql"
            counts[key] += 1
        elif isinstance(message, ListView.Highlighted):
            counts["operations"] += 1

    async with app.run_test(message_hook=count_highlights) as pilot:
        await pilot.pause(0.1)

    assert counts["edit_source"] == 0, counts
    assert counts["edit_sql"] == 0, counts


async def test_toml_editor_loads_source_and_operation_sql(tmp_path: Path) -> None:
    app = app_with_migrations(tmp_path)
    path = app.controller.workspace.documents[0].path  # type: ignore[union-attr]
    app.controller.select_artifact(path)  # type: ignore[union-attr]

    async with app.run_test(size=(130, 38)) as pilot:
        await pilot.click("#nav-editor")
        await pilot.pause()

        source = app.query_one("#artifact-source", TextArea)
        sql = app.query_one("#operation-sql", TextArea)
        operations = app.query_one("#operation-list", ListView)
        assert source.language == "toml"
        assert source.read_only is False
        assert 'revision = "001_users"' in source.text
        assert "CREATE TABLE 001_users" in sql.text
        assert len(operations.children) == 2


async def test_sql_editor_synchronizes_back_to_toml_and_controller(
    tmp_path: Path,
) -> None:
    app = app_with_migrations(tmp_path)
    path = app.controller.workspace.documents[0].path  # type: ignore[union-attr]
    app.controller.select_artifact(path)  # type: ignore[union-attr]

    async with app.run_test(size=(130, 38)) as pilot:
        await pilot.click("#nav-editor")
        sql = app.query_one("#operation-sql", TextArea)
        sql.text = "CREATE TABLE people (id INTEGER)"
        await pilot.pause()

        active = app.controller.active_document  # type: ignore[union-attr]
        assert active is not None
        assert active.artifact is not None
        assert active.artifact.operations[0].sql == "CREATE TABLE people (id INTEGER)"
        assert "CREATE TABLE people" in app.query_one("#artifact-source", TextArea).text
        assert "UNSAVED" in app.query_one("#editor-status", Static).render().plain


async def test_invalid_toml_disables_sql_editing_and_save(tmp_path: Path) -> None:
    app = app_with_migrations(tmp_path)
    path = app.controller.workspace.documents[0].path  # type: ignore[union-attr]
    app.controller.select_artifact(path)  # type: ignore[union-attr]

    async with app.run_test(size=(130, 38)) as pilot:
        await pilot.click("#nav-editor")
        source = app.query_one("#artifact-source", TextArea)
        source.text = "revision = ["
        await pilot.pause()

        assert app.query_one("#operation-sql", TextArea).disabled is True
        assert app.query_one("#editor-save", Button).disabled is True
        assert "Fix" in app.query_one("#editor-status", Static).render().plain


async def test_json_is_read_only_until_explicit_convert_to_toml(
    tmp_path: Path,
) -> None:
    app = app_with_migrations(tmp_path)
    path = app.controller.workspace.documents[1].path  # type: ignore[union-attr]
    app.controller.select_artifact(path)  # type: ignore[union-attr]

    async with app.run_test(size=(130, 38)) as pilot:
        await pilot.click("#nav-editor")
        await pilot.pause()

        source = app.query_one("#artifact-source", TextArea)
        assert app.controller.active_document is not None  # type: ignore[union-attr]
        assert app.controller.active_document.path == path  # type: ignore[union-attr]
        assert source.language == "json"
        assert source.read_only is True
        assert app.query_one("#editor-convert", Button).display is True

        await pilot.click("#editor-convert")
        await pilot.pause()

        converted = path.with_suffix(".toml")
        assert path.is_file()
        assert converted.is_file()
        assert app.controller.active_document is not None  # type: ignore[union-attr]
        assert app.controller.active_document.path == converted  # type: ignore[union-attr]
        assert app.query_one("#artifact-source", TextArea).read_only is False


async def test_dirty_editor_autosaves_recovery_draft(tmp_path: Path) -> None:
    app = app_with_migrations(tmp_path)
    path = app.controller.workspace.documents[0].path  # type: ignore[union-attr]
    app.controller.select_artifact(path)  # type: ignore[union-attr]

    async with app.run_test(size=(130, 38)) as pilot:
        await pilot.click("#nav-editor")
        source = app.query_one("#artifact-source", TextArea)
        source.text = "revision = ["
        await pilot.pause(0.4)

        active = app.controller.active_document  # type: ignore[union-attr]
        assert active is not None
        assert draft_path(tmp_path, active).read_text() == "revision = ["
