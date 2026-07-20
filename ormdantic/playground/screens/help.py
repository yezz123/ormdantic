"""In-application keyboard, safety, and recovery help."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.widgets import Label, Static

HELP_TEXT = """\
KEYBOARD
r refresh   p pause watcher   ctrl+s save   g generate
a apply     b rollback        ? help         q quit

SAFETY
Every mutation opens a current preflight with the environment, database,
revision, safety class, warnings, and complete SQL. A typed confirmation is
required by typed environments. Production destructive phrases also include
the environment and destructive-operation count. Confirmations are never cached.

EDITOR & RECOVERY
TOML is canonical. JSON remains readable and executable until Convert to TOML
creates a separate file. Dirty buffers autosave under .ormdantic/drafts.

CANCELLATION
Cancelling a UI worker discards its result. A native database call already in
progress may continue finishing in the background; the playground never claims
that driver-side work was cancelled.
"""


class HelpView(Vertical):
    """Visible reference for keyboard and destructive-action safeguards."""

    def __init__(self) -> None:
        super().__init__(id="view-help", classes="section-view")

    def compose(self) -> ComposeResult:
        yield Static("REFERENCE", classes="eyebrow")
        yield Label("Keyboard & safety help", classes="section-title")
        yield Static(HELP_TEXT, id="help-content")
