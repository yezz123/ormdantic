from __future__ import annotations

import asyncio
from collections.abc import Callable
from pathlib import Path

from textual.pilot import Pilot
from textual.widgets import Button, Checkbox, Input, Static, TextArea

from ormdantic.playground.app import PlaygroundApp
from ormdantic.playground.config import load_config
from ormdantic.playground.screens.workflows import GenerateDialog
from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.widgets.action_dialog import ActionDialog

MODEL_WITH_USERS = """\
from pydantic import BaseModel
from ormdantic import Ormdantic

db = Ormdantic("sqlite:///:memory:")

@db.table("users", pk="id")
class User(BaseModel):
    id: int
    name: str
"""

MODEL_WITHOUT_USERS = """\
from ormdantic import Ormdantic

db = Ormdantic("sqlite:///:memory:")
"""


async def wait_until(
    pilot: Pilot[None],
    predicate: Callable[[], bool],
    *,
    attempts: int = 400,
) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.05)
        await pilot.pause()
    controller = getattr(pilot.app, "controller", None)
    if controller is None:
        raise AssertionError("playground condition did not become true: no controller")
    state = controller.state
    diagnostics = tuple(
        f"{diagnostic.code}: {diagnostic.message}" for diagnostic in state.diagnostics
    )
    raise AssertionError(
        "playground condition did not become true: "
        f"status={state.status.value}, generation={state.generation}, "
        f"operation={state.operation.message!r}, diagnostics={diagnostics!r}"
    )


def project(tmp_path: Path, monkeypatch) -> tuple[PlaygroundApp, Path]:
    package = tmp_path / "project"
    package.mkdir()
    (package / "__init__.py").write_text("")
    model_path = package / "models.py"
    model_path.write_text(MODEL_WITH_USERS)
    database_path = tmp_path / "playground.sqlite3"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database_path}")
    config_path = tmp_path / "ormdantic.toml"
    config_path.write_text(
        """\
[project]
target = "project.models:db"
migrations_dir = "migrations"
watch = ["project/**/*.py", "migrations/**/*.toml"]
database_poll_seconds = 0.2
debounce_milliseconds = 40

[environments.development]
url_env = "DATABASE_URL"
safety = "typed"
"""
    )
    return PlaygroundApp(config=load_config(config_path)), model_path


async def type_action_phrase(
    app: PlaygroundApp,
    pilot: Pilot[None],
    phrase: str,
    *,
    destructive: bool = False,
) -> None:
    assert isinstance(app.screen, ActionDialog)
    if destructive:
        app.screen.query_one("#action-review", Checkbox).value = True
    app.screen.query_one("#action-confirmation", Input).value = phrase
    await pilot.pause()
    execute = app.screen.query_one("#action-execute", Button)
    if execute.disabled:
        reasons = app.screen.query_one("#action-preflight", Static).render().plain
        raise AssertionError(f"action remained disabled: {reasons}")
    await pilot.click("#action-execute")
    for _ in range(200):
        await asyncio.sleep(0.05)
        await pilot.pause()
        if not isinstance(app.screen, ActionDialog):
            return
        preflight = app.screen.query_one("#action-preflight", Static).render().plain
        if "failed" in preflight.casefold() or "error" in preflight.casefold():
            raise AssertionError(f"action execution failed: {preflight}")
    preflight = app.screen.query_one("#action-preflight", Static).render().plain
    raise AssertionError(f"action dialog did not close: {preflight}")


async def generate_revision(
    app: PlaygroundApp,
    pilot: Pilot[None],
    revision: str,
) -> Path:
    await pilot.press("g")
    await pilot.pause()
    assert isinstance(app.screen, GenerateDialog)
    app.screen.query_one("#generate-revision", Input).value = revision
    await pilot.pause()
    await pilot.click("#generate-execute")
    await wait_until(
        pilot,
        lambda: (
            app.controller is not None
            and app.controller.state.migrations.selected_path is not None
        ),
    )
    assert app.controller is not None
    selected = app.controller.state.migrations.selected_path
    assert selected is not None
    return selected


async def test_sqlite_tui_generate_edit_apply_watch_drop_and_rollback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    app, model_path = project(tmp_path, monkeypatch)

    async with app.run_test(size=(140, 45)) as pilot:
        await wait_until(
            pilot,
            lambda: (
                app.controller is not None
                and app.controller.state.schema.ready_for_generation
            ),
        )

        first = await generate_revision(app, pilot, "001_users")
        assert first.suffix == ".toml"
        sql_editor = app.query_one("#operation-sql", TextArea)
        sql_editor.load_text(sql_editor.text.rstrip() + "\n")
        await pilot.pause()
        await pilot.press("ctrl+s")
        await pilot.pause()

        await pilot.press("a")
        await pilot.pause()
        await type_action_phrase(
            app,
            pilot,
            "playground.sqlite3 001_users",
        )
        await wait_until(
            pilot,
            lambda: (
                app.controller is not None
                and app.controller.state.operation.message == "Applied 001_users"
            ),
        )
        assert app.controller is not None
        assert app.controller.state.schema.diff is not None
        assert not app.controller.state.schema.diff.changes

        previous_generation = app.controller.state.generation
        model_path.write_text(MODEL_WITHOUT_USERS)
        await wait_until(
            pilot,
            lambda: (
                app.controller is not None
                and app.controller.state.generation > previous_generation
                and app.controller.state.schema.ready_for_generation
            ),
        )

        second = await generate_revision(app, pilot, "002_drop_users")
        assert second.is_file()
        await pilot.press("a")
        await pilot.pause()
        await type_action_phrase(
            app,
            pilot,
            "playground.sqlite3 002_drop_users",
            destructive=True,
        )
        await wait_until(
            pilot,
            lambda: (
                app.controller is not None
                and app.controller.state.operation.message == "Applied 002_drop_users"
            ),
        )
        artifact_status = {
            item.revision: item.status
            for item in app.controller.state.migrations.artifacts
        }
        assert artifact_status.get("002_drop_users") == "applied", (
            artifact_status,
            app.controller.state.migrations.history,
        )

        await pilot.press("b")
        await pilot.pause()
        await type_action_phrase(
            app,
            pilot,
            "playground.sqlite3 002_drop_users",
            destructive=True,
        )
        await wait_until(
            pilot,
            lambda: (
                app.controller is not None
                and app.controller.state.operation.message
                == "Rolled back 002_drop_users"
            ),
        )
        state: PlaygroundState = app.controller.state
        assert any(
            entry.revision == "002_drop_users" and entry.status == "rolled_back"
            for entry in state.migrations.history
        )
