"""Compact actionable diagnostics widget."""

from __future__ import annotations

from textual.widgets import Static

from ormdantic.playground.diagnostics import Diagnostic


class DiagnosticList(Static):
    """Render diagnostics with severity and recovery hints."""

    def update_diagnostics(self, diagnostics: tuple[Diagnostic, ...]) -> None:
        if not diagnostics:
            self.update("No diagnostics")
            return
        lines = []
        for diagnostic in diagnostics:
            marker = {"error": "×", "warning": "!", "info": "·"}[
                diagnostic.severity.value
            ]
            line = f"{marker} {diagnostic.message}"
            if diagnostic.hint:
                line += f"\n  → {diagnostic.hint}"
            lines.append(line)
        self.update("\n".join(lines))
