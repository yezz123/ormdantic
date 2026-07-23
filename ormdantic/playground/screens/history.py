"""Durable migration history and recent operation logs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, DataTable, Label, Static

from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.widgets.log_view import LogView

if TYPE_CHECKING:
    from ormdantic.playground.app import PlaygroundApp


class HistoryView(Vertical):
    """Show migration status rows and secret-safe recent logs."""

    if TYPE_CHECKING:

        @property
        def app(self) -> PlaygroundApp: ...

    def __init__(self, state: PlaygroundState) -> None:
        super().__init__(id="view-history", classes="section-view state-aware")
        self.state = state
        self._selected_revision: str | None = None

    def compose(self) -> ComposeResult:
        yield Static("DURABLE DATABASE STATE", classes="eyebrow")
        with Horizontal(classes="title-row"):
            yield Label("History & logs", classes="section-title")
            yield Button(
                "Repair dirty row",
                id="history-repair",
                variant="error",
                disabled=True,
            )
        yield Static(
            "Applied, failed, rolled-back, and dirty revisions from the database.",
            classes="section-description",
        )
        with Horizontal(classes="history-grid"):
            yield DataTable(
                cursor_type="row",
                zebra_stripes=True,
                id="history-table",
            )
            with Vertical(classes="log-pane"):
                yield Static("RECENT OPERATION LOG", classes="panel-title")
                yield LogView(id="operation-log")

    def on_mount(self) -> None:
        self.query_one("#history-table", DataTable).add_columns(
            "Status",
            "Revision",
            "Applied",
            "Duration",
            "Dirty",
        )
        self.update_state(self.state)

    def update_state(self, state: PlaygroundState) -> None:
        self.state = state
        if not self.is_mounted:
            return
        table = self.query_one("#history-table", DataTable)
        table.clear()
        for entry in state.migrations.history:
            table.add_row(
                entry.status,
                entry.revision,
                entry.applied_at or "—",
                f"{entry.execution_time_ms} ms" if entry.execution_time_ms else "—",
                "yes" if entry.dirty else "no",
                key=entry.revision,
            )
        self.query_one("#operation-log", LogView).update_state(state)
        dirty = {entry.revision for entry in state.migrations.history if entry.dirty}
        self.query_one("#history-repair", Button).disabled = (
            self._selected_revision not in dirty
        )

    @on(DataTable.RowHighlighted, "#history-table")
    def select_history(self, event: DataTable.RowHighlighted) -> None:
        self._selected_revision = str(event.row_key.value)
        dirty = any(
            entry.revision == self._selected_revision and entry.dirty
            for entry in self.state.migrations.history
        )
        self.query_one("#history-repair", Button).disabled = not dirty

    @on(Button.Pressed, "#history-repair")
    def repair_history(self) -> None:
        if self._selected_revision is not None:
            self.app.open_repair_dialog(self._selected_revision)
