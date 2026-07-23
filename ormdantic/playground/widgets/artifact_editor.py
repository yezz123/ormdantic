"""Syntax-aware migration source editor."""

from __future__ import annotations

from textual.widgets import TextArea


class ArtifactEditor(TextArea):
    """Code editor tuned for canonical TOML and legacy JSON artifacts."""

    def __init__(self, *, id: str | None = None) -> None:
        super().__init__(
            "",
            language="toml",
            theme="monokai",
            soft_wrap=False,
            tab_behavior="indent",
            show_line_numbers=True,
            id=id,
        )
