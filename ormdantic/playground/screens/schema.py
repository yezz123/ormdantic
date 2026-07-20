"""Side-by-side model and live schema explorer."""

from __future__ import annotations

import json
from typing import Any

from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Label, Static, Tree

from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.widgets.schema_tree import SchemaTree


class SchemaView(Vertical):
    """Compare normalized model and live database objects."""

    def __init__(self, state: PlaygroundState) -> None:
        super().__init__(id="view-schema", classes="section-view state-aware")
        self.state = state

    def compose(self) -> ComposeResult:
        yield Static("NORMALIZED METADATA", classes="eyebrow")
        yield Label("Schema explorer", classes="section-title")
        yield Static(
            "Expand a table to compare columns, keys, indexes, and constraints.",
            classes="section-description",
        )
        with Horizontal(classes="schema-grid"):
            with Vertical(classes="schema-pane"):
                yield Static("MODELS", classes="panel-title")
                yield Static(id="model-schema-count", classes="pane-meta")
                yield SchemaTree("Registered models", id="model-schema")
            with Vertical(classes="schema-pane"):
                yield Static("LIVE DATABASE", classes="panel-title")
                yield Static(id="live-schema-count", classes="pane-meta")
                yield SchemaTree("Live database", id="live-schema")
            with Vertical(classes="schema-details"):
                yield Static("SELECTION", classes="panel-title")
                yield Static(
                    "Select a schema object to inspect normalized metadata.",
                    id="schema-selection",
                )

    def on_mount(self) -> None:
        self.update_state(self.state)

    def update_state(self, state: PlaygroundState) -> None:
        self.state = state
        if not self.is_mounted:
            return
        model = state.schema.model_snapshot
        live = state.schema.live_snapshot
        self.query_one("#model-schema", SchemaTree).update_snapshot(model)
        self.query_one("#live-schema", SchemaTree).update_snapshot(live)
        self.query_one("#model-schema-count", Static).update(_count(model))
        self.query_one("#live-schema-count", Static).update(_count(live))

    @on(Tree.NodeHighlighted)
    def show_selection(self, event: Tree.NodeHighlighted[Any]) -> None:
        if event.node.data is None:
            return
        self.query_one("#schema-selection", Static).update(
            json.dumps(event.node.data, indent=2, sort_keys=True, default=str)
        )


def _count(snapshot: Any | None) -> str:
    if snapshot is None:
        return "unavailable · previous side remains visible"
    tables = len(snapshot.tables)
    views = len(snapshot.views)
    return f"{tables} table{'s' if tables != 1 else ''} · {views} view{'s' if views != 1 else ''}"
