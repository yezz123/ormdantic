"""Read-only SQL preview widget."""

from __future__ import annotations

from textual.widgets import TextArea


class SqlPreview(TextArea):
    """Syntax-aware SQL preview that never accepts edits."""

    def __init__(self, *, id: str | None = None) -> None:
        super().__init__(
            "-- No SQL generated",
            language="sql",
            read_only=True,
            show_line_numbers=True,
            soft_wrap=False,
            id=id,
        )

    def set_statements(self, statements: tuple[str, ...]) -> None:
        self.text = (
            "\n\n".join(statement.rstrip(";") + ";" for statement in statements)
            if statements
            else "-- No SQL generated"
        )
