"""Structured schema drift and SQL review."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, DataTable, Label, Static

from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.widgets.diagnostic_list import DiagnosticList
from ormdantic.playground.widgets.sql_preview import SqlPreview

if TYPE_CHECKING:
    from ormdantic.playground.app import PlaygroundApp


class DriftView(Vertical):
    """Safety-labelled schema changes with generated SQL."""

    if TYPE_CHECKING:

        @property
        def app(self) -> PlaygroundApp: ...

    def __init__(self, state: PlaygroundState) -> None:
        super().__init__(id="view-drift", classes="section-view state-aware")
        self.state = state

    def compose(self) -> ComposeResult:
        yield Static("MODEL ↔ DATABASE", classes="eyebrow")
        with Horizontal(classes="title-row"):
            yield Label("Schema drift", classes="section-title")
            yield Button(
                "Generate migration",
                id="drift-generate",
                variant="primary",
            )
        yield Static(id="drift-summary", classes="section-description")
        with Horizontal(classes="drift-grid"):
            yield DataTable(
                cursor_type="row",
                zebra_stripes=True,
                id="drift-table",
            )
            with Vertical(classes="drift-preview"):
                yield Static("FORWARD SQL", classes="panel-title")
                yield SqlPreview(id="drift-sql")
                yield DiagnosticList(id="drift-diagnostics")

    def on_mount(self) -> None:
        table = self.query_one("#drift-table", DataTable)
        table.add_columns("Safety", "Change", "Object", "Summary")
        self.update_state(self.state)

    def update_state(self, state: PlaygroundState) -> None:
        self.state = state
        if not self.is_mounted:
            return
        table = self.query_one("#drift-table", DataTable)
        table.clear()
        changes = state.schema.diff.changes if state.schema.diff else []
        for index, change in enumerate(changes):
            safety = (
                "DESTRUCTIVE"
                if change.destructive
                else "CAUTION"
                if change.unsafe
                else "SAFE"
            )
            qualified = f"{change.table}.{change.name}" if change.name else change.table
            table.add_row(
                safety,
                change.action,
                f"{change.object_type} {qualified}",
                change.message,
                key=str(index),
            )
        destructive = sum(change.destructive for change in changes)
        unsafe = sum(change.unsafe and not change.destructive for change in changes)
        classification = (
            "DESTRUCTIVE" if destructive else "CAUTION" if unsafe else "SAFE"
        )
        self.query_one("#drift-summary", Static).update(
            f"{len(changes)} change{'s' if len(changes) != 1 else ''} · "
            f"{classification} · generation {state.generation}"
        )
        self.query_one("#drift-sql", SqlPreview).set_statements(
            state.schema.forward_sql
        )
        self.query_one("#drift-diagnostics", DiagnosticList).update_diagnostics(
            state.schema.diagnostics
        )
        self.query_one("#drift-generate", Button).disabled = (
            state.schema.stale
            or state.schema.model_snapshot is None
            or state.schema.live_snapshot is None
            or not state.schema.forward_sql
        )

    @on(Button.Pressed, "#drift-generate")
    def generate(self) -> None:
        self.app.open_generate_dialog()
