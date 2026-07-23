from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path

import pytest
from textual.widgets import ContentSwitcher, Input

from ormdantic.playground.app import PlaygroundApp
from ormdantic.playground.config import (
    EffectiveConfig,
    EnvironmentConfig,
    ProjectConfig,
)
from ormdantic.playground.controller import PlaygroundController
from ormdantic.playground.screens.setup import SetupScreen
from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.watcher import WatchEvent, WatchReason


def config(tmp_path: Path) -> EffectiveConfig:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    return EffectiveConfig(
        path=tmp_path / "ormdantic.toml",
        root=tmp_path,
        project=ProjectConfig(target="app:db", migrations_dir=migrations),
        environment=EnvironmentConfig(name="development", env_file=tmp_path / ".env"),
    )


async def test_missing_config_opens_first_run_setup(tmp_path: Path) -> None:
    app = PlaygroundApp(
        config=None,
        setup_path=tmp_path / "ormdantic.toml",
        auto_watch=False,
    )

    async with app.run_test() as pilot:
        await pilot.pause()

        assert isinstance(app.screen, SetupScreen)
        assert app.screen.query_one("#setup-target", Input).value == ""


async def test_setup_writes_config_and_enters_overview(tmp_path: Path) -> None:
    path = tmp_path / "ormdantic.toml"
    app = PlaygroundApp(config=None, setup_path=path, auto_watch=False)

    async with app.run_test() as pilot:
        await pilot.pause()
        screen = app.screen
        screen.query_one("#setup-target", Input).value = "app.database:db"
        screen.query_one("#setup-migrations", Input).value = "migrations"
        screen.query_one("#setup-url-env", Input).value = "DATABASE_URL"
        await pilot.click("#setup-save")
        await pilot.pause()

        assert path.is_file()
        assert not isinstance(app.screen, SetupScreen)
        assert app.query_one(ContentSwitcher).current == "view-overview"
        assert app.controller is not None


async def test_valid_config_opens_overview_with_global_status(tmp_path: Path) -> None:
    app = PlaygroundApp(config=config(tmp_path), auto_watch=False)

    async with app.run_test(size=(120, 35)) as pilot:
        await pilot.pause()

        switcher = app.query_one(ContentSwitcher)
        assert switcher.current == "view-overview"
        status = app.query_one("#global-status").render().plain
        assert "development" in status
        assert "idle" in status


async def test_navigation_switches_every_primary_section(tmp_path: Path) -> None:
    app = PlaygroundApp(config=config(tmp_path), auto_watch=False)

    async with app.run_test(size=(120, 35)) as pilot:
        for section in (
            "schema",
            "drift",
            "migrations",
            "editor",
            "history",
            "settings",
        ):
            await pilot.click(f"#nav-{section}")
            await pilot.pause()
            assert app.query_one(ContentSwitcher).current == f"view-{section}"


async def test_help_binding_opens_help_section(tmp_path: Path) -> None:
    app = PlaygroundApp(config=config(tmp_path), auto_watch=False)

    async with app.run_test() as pilot:
        await pilot.press("?")
        await pilot.pause()

        assert app.query_one(ContentSwitcher).current == "view-help"


async def test_narrow_terminal_uses_compact_navigation_layout(tmp_path: Path) -> None:
    app = PlaygroundApp(config=config(tmp_path), auto_watch=False)

    async with app.run_test(size=(80, 24)) as pilot:
        await pilot.pause()

        assert app.query_one("#app-body").has_class("compact")
        assert app.query_one("#navigation").size.width <= 80


async def test_watcher_lifecycle_refreshes_and_pause_binding_is_real(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    effective = config(tmp_path)
    controller = PlaygroundController(effective)
    generations: list[int | None] = []

    async def refresh(*, generation: int | None = None) -> PlaygroundState:
        generations.append(generation)
        return controller.state

    monkeypatch.setattr(controller, "refresh", refresh)

    class RecordingWatcher:
        def __init__(self, **options: object) -> None:
            self.options = options
            self.paused = False
            self.stopped = False
            self._stop = asyncio.Event()

        async def run(
            self,
            emit: Callable[[WatchEvent], Awaitable[None]],
        ) -> None:
            await emit(WatchEvent(4, (WatchReason.INITIAL,)))
            await self._stop.wait()

        def pause(self) -> None:
            self.paused = True

        def resume(self) -> None:
            self.paused = False

        async def stop(self) -> None:
            self.stopped = True
            self._stop.set()

    created: list[RecordingWatcher] = []

    def watcher_factory(**options: object) -> RecordingWatcher:
        watcher = RecordingWatcher(**options)
        created.append(watcher)
        return watcher

    app = PlaygroundApp(
        config=effective,
        controller=controller,
        auto_watch=True,
        watcher_factory=watcher_factory,
    )

    async with app.run_test() as pilot:
        await pilot.pause()
        assert generations == [4]
        assert created[0].options["patterns"] == effective.project.watch

        await pilot.press("p")
        assert created[0].paused is True
        assert controller.state.watcher_paused is True
        await pilot.press("p")
        assert created[0].paused is False
        assert controller.state.watcher_paused is False

    assert created[0].stopped is True
