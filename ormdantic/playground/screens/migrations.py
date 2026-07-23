"""Migration revision browser and artifact details."""

from __future__ import annotations

from typing import TYPE_CHECKING

from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Button, DataTable, Label, Static

from ormdantic.playground.state import PlaygroundState
from ormdantic.playground.widgets.migration_list import MigrationList
from ormdantic.playground.widgets.sql_preview import SqlPreview

if TYPE_CHECKING:
    from ormdantic.playground.app import PlaygroundApp


class MigrationsView(Vertical):
    """Browse artifact health, dependencies, risk, and SQL."""

    if TYPE_CHECKING:

        @property
        def app(self) -> PlaygroundApp: ...

    def __init__(self, state: PlaygroundState) -> None:
        super().__init__(id="view-migrations", classes="section-view state-aware")
        self.state = state
        self._loading = False

    def compose(self) -> ComposeResult:
        yield Static("REVISION WORKSPACE", classes="eyebrow")
        with Horizontal(classes="title-row"):
            yield Label("Migrations", classes="section-title")
            yield Button("Squash pending", id="migration-squash", variant="warning")
            yield Button("Open editor", id="migration-open-editor", variant="primary")
        yield Static(
            "TOML is canonical. JSON stays executable and converts explicitly.",
            classes="section-description",
        )
        with Horizontal(classes="migration-grid"):
            yield MigrationList(id="migration-table")
            with Vertical(classes="migration-detail"):
                yield Static("SELECT A REVISION", id="migration-detail")
                yield Static("FORWARD SQL", classes="panel-title")
                yield SqlPreview(id="migration-sql")

    def on_mount(self) -> None:
        self.update_state(self.state)

    def update_state(self, state: PlaygroundState) -> None:
        self.state = state
        if not self.is_mounted:
            return
        self._loading = True
        self.query_one("#migration-table", MigrationList).update_artifacts(
            state.migrations.artifacts,
            state.migrations.selected_path,
        )
        self.call_after_refresh(self._finish_table_update)
        self._show_active()
        pending = tuple(
            item
            for item in state.migrations.artifacts
            if item.status == "pending" and item.valid
        )
        self.query_one("#migration-squash", Button).disabled = len(pending) < 2

    @on(DataTable.RowHighlighted, "#migration-table")
    def select_migration(self, event: DataTable.RowHighlighted) -> None:
        if self._loading or self.app.controller is None:
            return
        table = self.query_one("#migration-table", MigrationList)
        if event.cursor_row != table.cursor_row:
            return
        path = str(event.row_key.value)
        try:
            self.app.controller.select_artifact(self.app.controller.config.root / path)
        except ValueError:
            from pathlib import Path

            self.app.controller.select_artifact(Path(path))
        self._show_active()

    @on(Button.Pressed, "#migration-open-editor")
    def open_editor(self) -> None:
        self.app._show_section("editor")

    @on(Button.Pressed, "#migration-squash")
    def squash_pending(self) -> None:
        paths = tuple(
            item.path
            for item in self.state.migrations.artifacts
            if item.status == "pending" and item.valid
        )
        self.app.open_squash_dialog(paths)

    def _show_active(self) -> None:
        if not self.is_mounted or self.app.controller is None:
            return
        document = self.app.controller.active_document
        detail = self.query_one("#migration-detail", Static)
        preview = self.query_one("#migration-sql", SqlPreview)
        if document is None:
            detail.update("Select a revision to inspect its metadata and SQL.")
            preview.set_statements(())
            return
        artifact = document.artifact
        if artifact is None:
            detail.update(
                f"{document.path.name}\nINVALID\n"
                + "\n".join(item.message for item in document.diagnostics)
            )
            preview.set_statements(())
            return
        plan = artifact.to_plan()
        detail.update(
            f"{artifact.revision}\n{document.format.upper()} · "
            f"{len(artifact.operations)} up / {len(artifact.rollback_operations)} down\n"
            f"depends on {', '.join(artifact.depends_on) or '—'}\n"
            f"checksum {artifact.checksum or '—'}\n"
            f"safety {'DESTRUCTIVE' if plan.has_destructive_operations else 'SAFE'}"
        )
        preview.set_statements(
            tuple(operation.sql for operation in artifact.operations)
        )

    def _finish_table_update(self) -> None:
        self._loading = False
