"""Persistent render of environment and refresh health."""

from __future__ import annotations

from rich.text import Text
from textual.widgets import Static

from ormdantic.playground.state import PlaygroundState


class StatusBar(Static):
    """Compact non-secret global state summary."""

    def __init__(self, state: PlaygroundState, *, id: str | None = None) -> None:
        super().__init__(id=id)
        self.state = state

    def update_state(self, state: PlaygroundState) -> None:
        """Render a newly published controller state."""
        self.state = state
        self.refresh()

    def render(self) -> Text:
        state = self.state
        connection = state.connection_label or "offline"
        dialect = state.dialect or "dialect —"
        watcher = "paused" if state.watcher_paused else "watching"
        dirty = (
            " • unsaved"
            if any(item.status == "dirty" for item in state.migrations.artifacts)
            else ""
        )
        return Text(
            f"{state.environment}  •  {connection}  •  {dialect}  •  "
            f"{state.status.value}  •  {watcher}{dirty}"
        )
