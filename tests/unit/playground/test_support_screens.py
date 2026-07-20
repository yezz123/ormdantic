from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from textual.widgets import Button, DataTable, Select, Static, TextArea

from ormdantic.migrations import (
    MigrationArtifact,
    MigrationHistoryEntry,
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
from ormdantic.playground.screens.confirmations import ConfirmQuitScreen
from ormdantic.playground.state import MigrationState, OperationState

CONFIG_SOURCE = """\
[project]
target = "app:db"
migrations_dir = "migrations"

[environments.development]
url_env = "DATABASE_URL"
safety = "confirm"

[environments.staging]
url_env = "STAGING_DATABASE_URL"
safety = "typed"
"""


def app(tmp_path: Path) -> PlaygroundApp:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    path = tmp_path / "ormdantic.toml"
    path.write_text(CONFIG_SOURCE)
    effective = EffectiveConfig(
        path=path,
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations),
        environment=EnvironmentConfig(name="development"),
    )
    controller = PlaygroundController(effective)
    controller.state = replace(
        controller.state,
        migrations=MigrationState(
            history=(
                MigrationHistoryEntry(
                    revision="001_initial",
                    status="applied",
                    execution_time_ms=18,
                    applied_at="2026-07-19T12:00:00+00:00",
                ),
            ),
            current_revision="001_initial",
        ),
        operation=OperationState(
            name="apply",
            target="001_initial",
            message="Applied 001_initial",
        ),
    )
    return PlaygroundApp(config=effective, controller=controller)


async def test_history_screen_renders_durable_rows_and_recent_log(
    tmp_path: Path,
) -> None:
    playground = app(tmp_path)

    async with playground.run_test(size=(120, 35)) as pilot:
        await pilot.click("#nav-history")
        await pilot.pause()

        assert playground.query_one("#history-table", DataTable).row_count == 1
        assert (
            "Applied 001_initial"
            in playground.query_one("#operation-log", Static).render().plain
        )


async def test_settings_show_environments_and_editable_toml(tmp_path: Path) -> None:
    playground = app(tmp_path)

    async with playground.run_test(size=(120, 35)) as pilot:
        await pilot.click("#nav-settings")
        await pilot.pause()

        selector = playground.query_one("#settings-environment", Select)
        editor = playground.query_one("#settings-source", TextArea)
        assert selector.value == "development"
        assert "[environments.staging]" in editor.text
        assert editor.language == "toml"
        assert editor.read_only is False


async def test_settings_only_replace_config_after_valid_toml(tmp_path: Path) -> None:
    playground = app(tmp_path)
    config_path = playground.config.path  # type: ignore[union-attr]

    async with playground.run_test(size=(120, 35)) as pilot:
        await pilot.click("#nav-settings")
        editor = playground.query_one("#settings-source", TextArea)
        original = config_path.read_text()

        editor.load_text("[project\n")
        await pilot.pause()
        await pilot.click("#settings-save")
        await pilot.pause()
        assert config_path.read_text() == original
        assert (
            "invalid TOML"
            in playground.query_one("#settings-error", Static).render().plain
        )

        updated = original.replace('target = "app:db"', 'target = "app:new_db"')
        editor.load_text(updated)
        await pilot.pause()
        assert editor.text == updated
        playground.query_one("#settings-save", Button).press()
        await pilot.pause()
        assert "Saved" in playground.query_one("#settings-error", Static).render().plain
        assert config_path.read_text() == updated
        assert playground.config is not None
        assert playground.config.project.target == "app:new_db"


async def test_help_documents_bindings_and_confirmation_rules(tmp_path: Path) -> None:
    playground = app(tmp_path)

    async with playground.run_test() as pilot:
        await pilot.press("?")
        await pilot.pause()

        help_text = playground.query_one("#help-content", Static).render().plain
        assert "ctrl+s" in help_text
        assert "typed confirmation" in help_text
        assert "background" in help_text


async def test_quit_with_dirty_editor_requires_confirmation(tmp_path: Path) -> None:
    playground = app(tmp_path)
    assert playground.controller is not None
    migration = MigrationArtifact.from_plan(
        "001_dirty",
        MigrationPlan(operations=[MigrationOperation(sql="SELECT 1")]),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect="sqlite",
    )
    path = playground.config.project.migrations_dir / "001_dirty.toml"  # type: ignore[union-attr]
    migration.write(path)
    playground.controller.reload_workspace()
    playground.controller.select_artifact(path)
    playground.controller.edit_active_source("revision = [")

    async with playground.run_test() as pilot:
        await pilot.press("q")
        await pilot.pause()

        assert isinstance(playground.screen, ConfirmQuitScreen)
        await pilot.click("#quit-cancel")
        await pilot.pause()
        assert not isinstance(playground.screen, ConfirmQuitScreen)
