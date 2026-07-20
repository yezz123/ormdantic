"""Small local confirmation screens."""

from __future__ import annotations

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static


class ConfirmQuitScreen(ModalScreen[bool]):
    """Protect dirty editor buffers from accidental quit."""

    def compose(self) -> ComposeResult:
        with Container(id="quit-dialog"):
            yield Static("UNSAVED WORK", classes="eyebrow")
            yield Label("Quit without saving?", id="quit-title")
            yield Static(
                "A recovery draft may exist, but the migration file has not been saved.",
                classes="muted",
            )
            with Horizontal(classes="dialog-actions"):
                yield Button("Keep editing", id="quit-cancel", variant="primary")
                yield Button("Quit", id="quit-confirm", variant="error")

    @on(Button.Pressed, "#quit-cancel")
    def cancel(self) -> None:
        self.dismiss(False)

    @on(Button.Pressed, "#quit-confirm")
    def confirm(self) -> None:
        self.dismiss(True)
