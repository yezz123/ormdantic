"""Primary playground navigation."""

from __future__ import annotations

from textual import on
from textual.app import ComposeResult
from textual.widget import Widget
from textual.widgets import Button, Static

from ormdantic.playground.messages import Navigate

SECTIONS = (
    ("overview", "Overview", "1"),
    ("schema", "Schema", "2"),
    ("drift", "Drift", "3"),
    ("migrations", "Migrations", "4"),
    ("editor", "Editor", "5"),
    ("history", "History & logs", "6"),
    ("settings", "Settings", "7"),
)


class NavigationRail(Widget):
    """Keyboard-friendly primary section navigation."""

    def compose(self) -> ComposeResult:
        yield Static("WORKSPACE", classes="nav-label")
        for section, label, number in SECTIONS:
            yield Button(
                f"{number}  {label}",
                id=f"nav-{section}",
                classes="nav-button",
                flat=True,
            )

    @on(Button.Pressed, ".nav-button")
    def navigate(self, event: Button.Pressed) -> None:
        button_id = event.button.id
        if button_id is not None:
            self.post_message(Navigate(button_id.removeprefix("nav-")))
