"""Environment, watcher, and embedded project configuration settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, Label, Select, Static, TextArea

from ormdantic.playground.config import EffectiveConfig, parse_config
from ormdantic.playground.state import PlaygroundState

if TYPE_CHECKING:
    from ormdantic.playground.app import PlaygroundApp


class SettingsView(Vertical):
    """Review environment policy and edit the project TOML in place."""

    if TYPE_CHECKING:

        @property
        def app(self) -> PlaygroundApp: ...

    def __init__(self, state: PlaygroundState, config: EffectiveConfig | None) -> None:
        super().__init__(id="view-settings", classes="section-view state-aware")
        self.state = state
        self.config = config
        self._loading = True

    def compose(self) -> ComposeResult:
        yield Static("PROJECT CONTROL", classes="eyebrow")
        yield Label("Settings", classes="section-title")
        with Horizontal(classes="settings-toolbar"):
            yield Select(
                self._environment_options(),
                value=self.state.environment,
                allow_blank=False,
                id="settings-environment",
            )
            yield Button("Pause watcher", id="settings-watcher")
            yield Button("Save TOML", id="settings-save", variant="primary")
        yield Static(id="settings-summary", classes="section-description")
        yield TextArea.code_editor(
            self._source(),
            language="toml",
            id="settings-source",
        )
        yield Static("", id="settings-error")

    def on_mount(self) -> None:
        self._loading = False
        self.update_state(self.state)

    def update_state(self, state: PlaygroundState) -> None:
        self.state = state
        if not self.is_mounted:
            return
        self.query_one("#settings-summary", Static).update(
            f"target {self.config.project.target if self.config else '—'} · "
            f"migrations {self.config.project.migrations_dir if self.config else '—'} · "
            f"poll {self.config.project.database_poll_seconds if self.config else '—'}s · "
            f"safety {self.config.environment.safety if self.config else '—'}"
        )
        self.query_one("#settings-watcher", Button).label = (
            "Resume watcher" if state.watcher_paused else "Pause watcher"
        )

    def update_config(self, config: EffectiveConfig) -> None:
        """Replace effective settings after setup, save, or environment change."""
        self.config = config
        if not self.is_mounted:
            return
        selector = self.query_one("#settings-environment", Select)
        selector.set_options(self._environment_options())
        selector.value = config.environment.name
        self.query_one("#settings-source", TextArea).load_text(self._source())
        self.update_state(self.state)

    @on(Select.Changed, "#settings-environment")
    def environment_changed(self, event: Select.Changed) -> None:
        if self._loading or event.value is Select.NULL:
            return
        if str(event.value) == self.state.environment:
            return
        try:
            self.app.switch_environment(str(event.value))
        except Exception as exc:
            self.query_one("#settings-error", Static).update(str(exc))

    @on(Button.Pressed, "#settings-watcher")
    def toggle_watcher(self) -> None:
        self.app.action_pause_watcher()

    @on(Button.Pressed, "#settings-save")
    def save_source(self) -> None:
        if self.config is None:
            return
        try:
            self.app.save_config_source(
                self.query_one("#settings-source", TextArea).text
            )
        except Exception as exc:
            self.query_one("#settings-error", Static).update(str(exc))
            return
        self.query_one("#settings-error", Static).update("Saved and reloaded")

    def _environment_options(self) -> list[tuple[str, str]]:
        if self.config is None:
            return [(self.state.environment, self.state.environment)]
        try:
            names = parse_config(self.config.path).environments
        except Exception:
            names = {self.state.environment: self.config.environment}
        return [(name, name) for name in names]

    def _source(self) -> str:
        if self.config is None or not self.config.path.is_file():
            return "# ormdantic.toml is not available"
        return self.config.path.read_text(encoding="utf-8")
