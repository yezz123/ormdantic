"""Migration revision browser widget."""

from __future__ import annotations

from pathlib import Path

from textual.widgets import DataTable

from ormdantic.playground.state import ArtifactSummary


class MigrationList(DataTable[str]):
    """Compact revision/status/risk table."""

    def on_mount(self) -> None:
        self.cursor_type = "row"
        self.zebra_stripes = True
        self.add_columns("Status", "Revision", "Format", "Risk")

    def update_artifacts(
        self,
        artifacts: tuple[ArtifactSummary, ...],
        selected_path: Path | None = None,
    ) -> None:
        """Replace rows from state summaries."""
        if not self.is_mounted:
            return
        self.clear()
        for artifact in artifacts:
            risk = (
                "destructive"
                if artifact.destructive
                else "caution"
                if artifact.unsafe
                else "safe"
            )
            self.add_row(
                artifact.status,
                artifact.revision or artifact.path.name,
                artifact.format,
                risk,
                key=str(artifact.path),
            )
        if selected_path is not None:
            for index, artifact in enumerate(artifacts):
                if artifact.path == selected_path:
                    self.move_cursor(row=index, column=0, animate=False)
                    break
