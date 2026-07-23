"""Project health overview."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Label, Static

from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.widgets.diagnostic_list import DiagnosticList


class OverviewView(Vertical):
    """At-a-glance connection, drift, and migration health."""

    def __init__(self, state: PlaygroundState) -> None:
        super().__init__(id="view-overview", classes="section-view state-aware")
        self.state = state

    def compose(self) -> ComposeResult:
        yield Static("PROJECT PULSE", classes="eyebrow")
        yield Label("Overview", classes="section-title")
        yield Static(
            "Everything that changed, what is safe, and what needs your attention.",
            classes="section-description",
        )
        with Horizontal(classes="metric-grid"):
            yield Static(id="overview-health", classes="metric-card")
            yield Static(id="overview-connection", classes="metric-card")
            yield Static(id="overview-drift", classes="metric-card")
            yield Static(id="overview-migrations", classes="metric-card")
        yield Static("RECENT DIAGNOSTICS", classes="panel-title")
        yield DiagnosticList(id="overview-diagnostics", classes="diagnostic-panel")

    def on_mount(self) -> None:
        self.update_state(self.state)

    def update_state(self, state: PlaygroundState) -> None:
        self.state = state
        if not self.is_mounted:
            return
        model_tables = (
            len(state.schema.model_snapshot.tables)
            if state.schema.model_snapshot
            else 0
        )
        live_tables = (
            len(state.schema.live_snapshot.tables) if state.schema.live_snapshot else 0
        )
        drift = len(state.schema.diff.changes) if state.schema.diff else 0
        unsafe = len(state.schema.diff.unsafe_changes) if state.schema.diff else 0
        pending = sum(
            artifact.status == "pending" for artifact in state.migrations.artifacts
        )
        stale = " · STALE" if state.schema.stale else ""
        self.query_one("#overview-health", Static).update(
            f"SCHEMA HEALTH\n{state.status.value.upper()}{stale}\n"
            f"{model_tables} model / {live_tables} live tables"
        )
        self.query_one("#overview-connection", Static).update(
            "CONNECTION\n"
            f"{state.connection_label or 'Not connected'}\n"
            f"{state.dialect or 'No dialect detected'}"
        )
        safety = f"{unsafe} caution" if unsafe else "safe to review"
        self.query_one("#overview-drift", Static).update(
            f"SCHEMA DRIFT\n{drift} change{'s' if drift != 1 else ''}\n{safety}"
        )
        self.query_one("#overview-migrations", Static).update(
            f"MIGRATIONS\n{pending} pending\n"
            f"head {state.migrations.current_revision or '—'}"
        )
        self.query_one("#overview-diagnostics", DiagnosticList).update_diagnostics(
            state.diagnostics or state.schema.diagnostics
        )
