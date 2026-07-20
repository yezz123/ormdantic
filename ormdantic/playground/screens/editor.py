"""Full TOML artifact and per-operation SQL editor."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.timer import Timer
from textual.widgets import Button, Label, ListView, Static, TextArea

from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.widgets.artifact_editor import ArtifactEditor
from ormdantic.playground.widgets.operation_list import OperationItem, OperationList

if TYPE_CHECKING:
    from ormdantic.playground.app import PlaygroundApp


class EditorView(Vertical):
    """Synchronize complete migration source with selected operation SQL."""

    DRAFT_DELAY = 0.25

    if TYPE_CHECKING:

        @property
        def app(self) -> PlaygroundApp: ...

    def __init__(self, state: PlaygroundState) -> None:
        super().__init__(id="view-editor", classes="section-view state-aware")
        self.state = state
        self._loading = False
        self._selected_operation: tuple[bool, int] | None = None
        self._draft_timer: Timer | None = None

    def compose(self) -> ComposeResult:
        yield Static("SOURCE OF TRUTH", classes="eyebrow")
        with Horizontal(classes="title-row"):
            yield Label("Migration editor", classes="section-title")
            yield Static("Select a migration", id="editor-status")
            yield Button("Convert to TOML", id="editor-convert", variant="warning")
            yield Button("Save", id="editor-save", variant="primary")
        with Horizontal(classes="editor-grid"):
            with Vertical(classes="source-pane"):
                yield Static("ARTIFACT SOURCE", classes="panel-title")
                yield ArtifactEditor(id="artifact-source")
            with Vertical(classes="operation-pane"):
                yield Static("OPERATIONS", classes="panel-title")
                yield OperationList(id="operation-list")
                yield Static("SELECTED SQL", classes="panel-title")
                yield TextArea.code_editor(
                    "-- Select an operation",
                    language="sql",
                    id="operation-sql",
                )

    def on_mount(self) -> None:
        self._sync_document()

    def update_state(self, state: PlaygroundState) -> None:
        self.state = state
        if (
            self.is_mounted
            and self.query_one("#operation-list", OperationList).is_attached
            and not self._loading
        ):
            self._sync_document()

    @on(TextArea.Changed, "#artifact-source")
    def source_changed(self, event: TextArea.Changed) -> None:
        if self._loading or self.app.controller is None:
            return
        document = self.app.controller.active_document
        if document is None or document.format == "json":
            return
        if event.text_area.text == document.source:
            return
        updated = self.app.controller.edit_active_source(event.text_area.text)
        self._update_validity(updated)
        self._schedule_draft()

    @on(TextArea.Changed, "#operation-sql")
    def sql_changed(self, event: TextArea.Changed) -> None:
        if (
            self._loading
            or self._selected_operation is None
            or self.app.controller is None
            or event.text_area.disabled
        ):
            return
        rollback, index = self._selected_operation
        document = self.app.controller.active_document
        if document is None or document.artifact is None:
            return
        operations = (
            document.artifact.rollback_operations
            if rollback
            else document.artifact.operations
        )
        if index >= len(operations) or event.text_area.text == operations[index].sql:
            return
        updated = self.app.controller.edit_active_sql(
            index,
            event.text_area.text,
            rollback=rollback,
        )
        self._loading = True
        source = self.query_one("#artifact-source", ArtifactEditor)
        if source.text != updated.source:
            source.load_text(updated.source)
        self._loading = False
        self._update_validity(updated)
        self._schedule_draft()

    @on(ListView.Highlighted, "#operation-list")
    def operation_selected(self, event: ListView.Highlighted) -> None:
        if self._loading or not isinstance(event.item, OperationItem):
            return
        self._selected_operation = (event.item.rollback, event.item.operation_index)
        self._load_selected_sql()

    @on(Button.Pressed, "#editor-save")
    def save(self) -> None:
        if self.app.controller is None:
            return
        try:
            saved = self.app.controller.save_active()
            self.app.controller.discard_active_draft()
        except Exception as exc:
            self.app.notify(str(exc), severity="error")
            return
        self._sync_document()
        self.app.notify(f"Saved {saved.path.name}")

    @on(Button.Pressed, "#editor-convert")
    def convert(self) -> None:
        if self.app.controller is None:
            return
        try:
            converted = self.app.controller.convert_active_to_toml()
        except Exception as exc:
            self.app.notify(str(exc), severity="error")
            return
        self._sync_document()
        self.app.notify(f"Created {converted.path.name}")

    def _sync_document(self) -> None:
        if self.app.controller is None:
            return
        document = self.app.controller.active_document
        source = self.query_one("#artifact-source", ArtifactEditor)
        operations = self.query_one("#operation-list", OperationList)
        convert = self.query_one("#editor-convert", Button)
        self._loading = True
        if document is None:
            source.language = "toml"
            source.read_only = True
            source.load_text("# Select a migration from the Migrations screen")
            operations.update_document(None)
            self.query_one("#operation-sql", TextArea).load_text(
                "-- Select an operation"
            )
            self._selected_operation = None
            self.query_one("#editor-status", Static).update("Select a migration")
            self.query_one("#editor-save", Button).disabled = True
            convert.display = False
            self._loading = False
            return
        source.language = document.format
        source.read_only = document.format == "json"
        if source.text != document.source:
            source.load_text(document.source)
        operations.update_document(document)
        convert.display = document.format == "json"
        self._selected_operation = (
            (False, 0) if document.artifact and document.artifact.operations else None
        )
        self._load_selected_sql()
        self._loading = False
        self._update_validity(document)

    def _load_selected_sql(self) -> None:
        if self.app.controller is None:
            return
        document = self.app.controller.active_document
        editor = self.query_one("#operation-sql", TextArea)
        if (
            document is None
            or document.artifact is None
            or self._selected_operation is None
        ):
            editor.load_text("-- Fix artifact validation before editing SQL")
            editor.disabled = True
            return
        rollback, index = self._selected_operation
        operations = (
            document.artifact.rollback_operations
            if rollback
            else document.artifact.operations
        )
        if index >= len(operations):
            editor.load_text("-- Operation no longer exists")
            editor.disabled = True
            return
        self._loading = True
        editor.disabled = False
        editor.load_text(operations[index].sql)
        self._loading = False

    def _update_validity(self, document: object) -> None:
        active = self.app.controller.active_document if self.app.controller else None
        status = self.query_one("#editor-status", Static)
        save = self.query_one("#editor-save", Button)
        sql = self.query_one("#operation-sql", TextArea)
        if active is None or active.artifact is None:
            message = (
                active.diagnostics[0].message
                if active and active.diagnostics
                else "Invalid artifact"
            )
            status.update(f"Fix validation · {message}")
            save.disabled = True
            sql.disabled = True
            return
        status.update(
            f"{'UNSAVED' if active.dirty else 'SAVED'} · "
            f"{active.path.name} · checksum valid"
        )
        save.disabled = active.format == "json"
        sql.disabled = False

    def _schedule_draft(self) -> None:
        if self._draft_timer is not None:
            self._draft_timer.stop()
        self._draft_timer = self.set_timer(self.DRAFT_DELAY, self._save_draft)

    def _save_draft(self) -> None:
        if self.app.controller is None or self.app.controller.active_document is None:
            return
        try:
            self.app.controller.write_active_draft()
        except OSError as exc:
            self.app.notify(f"Could not save recovery draft: {exc}", severity="warning")
