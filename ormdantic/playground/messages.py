"""Textual messages emitted by playground widgets."""

from __future__ import annotations

from textual.message import Message


class Navigate(Message):
    """Request that the application show a primary section."""

    def __init__(self, section: str) -> None:
        super().__init__()
        self.section = section
