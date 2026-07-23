"""First-run project configuration screen."""

from __future__ import annotations

from pathlib import Path

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, Static

from ormdantic.playground.config import (
    EffectiveConfig,
    EnvironmentConfig,
    PlaygroundConfig,
    ProjectConfig,
    load_config,
    write_config,
)


class SetupScreen(ModalScreen[EffectiveConfig | None]):
    """Collect safe first-run settings without asking for credentials."""

    def __init__(self, path: Path) -> None:
        super().__init__()
        self.path = path.resolve()

    def compose(self) -> ComposeResult:
        with Container(id="setup-dialog"):
            yield Static("ORMDANTIC PLAYGROUND", classes="eyebrow")
            yield Label("Configure this project", id="setup-title")
            yield Static(
                "Only environment-variable names are stored. Database passwords "
                "never enter this file.",
                classes="muted",
            )
            with Vertical(classes="setup-fields"):
                with Horizontal(classes="setup-row"):
                    with Vertical(classes="field-group"):
                        yield Label("Model target", classes="field-label")
                        yield Input(
                            placeholder="app.database:db",
                            id="setup-target",
                        )
                    with Vertical(classes="field-group"):
                        yield Label("Migrations directory", classes="field-label")
                        yield Input(value="migrations", id="setup-migrations")
                with Horizontal(classes="setup-row"):
                    with Vertical(classes="field-group"):
                        yield Label(
                            "Database URL environment variable",
                            classes="field-label",
                        )
                        yield Input(value="DATABASE_URL", id="setup-url-env")
                    with Vertical(classes="field-group"):
                        yield Label("Optional .env file", classes="field-label")
                        yield Input(value=".env", id="setup-env-file")
            yield Static("", id="setup-error")
            with Horizontal(classes="dialog-actions"):
                yield Button("Exit", id="setup-cancel", variant="default")
                yield Button("Create playground", id="setup-save", variant="primary")

    def on_mount(self) -> None:
        self.query_one("#setup-target", Input).focus()

    @on(Button.Pressed, "#setup-save")
    def save(self) -> None:
        target = self.query_one("#setup-target", Input).value.strip()
        migrations = self.query_one("#setup-migrations", Input).value.strip()
        url_env = self.query_one("#setup-url-env", Input).value.strip()
        env_file_value = self.query_one("#setup-env-file", Input).value.strip()
        if not target or ":" not in target:
            self._show_error("Use a model target such as app.database:db.")
            return
        if not migrations:
            self._show_error("Choose a migrations directory.")
            return
        if not url_env:
            self._show_error("Choose the environment variable that contains the URL.")
            return
        config = PlaygroundConfig(
            project=ProjectConfig(
                target=target,
                migrations_dir=Path(migrations),
                format="toml",
                watch=(
                    "app/**/*.py",
                    f"{migrations}/**/*.toml",
                    f"{migrations}/**/*.json",
                ),
            ),
            environments={
                "development": EnvironmentConfig(
                    name="development",
                    url_env=url_env,
                    env_file=Path(env_file_value) if env_file_value else None,
                )
            },
        )
        try:
            write_config(self.path, config)
            effective = load_config(self.path)
        except Exception as exc:
            self._show_error(str(exc))
            return
        self.dismiss(effective)

    @on(Button.Pressed, "#setup-cancel")
    def cancel(self) -> None:
        self.dismiss(None)

    def _show_error(self, message: str) -> None:
        self.query_one("#setup-error", Static).update(message)
