"""Bounded, secret-safe operation log rendering."""

from __future__ import annotations

from textual.widgets import Static

from ormdantic.playground.state import PlaygroundState


class LogView(Static):
    """Render recent operation and diagnostic messages."""

    def update_state(self, state: PlaygroundState) -> None:
        lines: list[str] = []
        if state.operation.message:
            lines.append(f"operation · {state.operation.message}")
        lines.extend(
            f"{diagnostic.severity.value} · {diagnostic.message}"
            for diagnostic in state.diagnostics[-20:]
        )
        self.update("\n".join(lines) if lines else "No operation log entries")
