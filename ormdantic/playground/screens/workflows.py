"""Generate, repair, and squash workflow dialogs."""

from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Button, Input, Label, Static, TextArea

from ormdantic.playground.controller import (
    ControllerActionOutcome,
    PlaygroundController,
)
from ormdantic.playground.safety import (
    ActionRequest,
    PreflightContext,
    SafetyDecision,
    evaluate_action,
)

_REVISION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class GenerateDialog(ModalScreen[Path | None]):
    """Name and create a TOML artifact from the current fresh drift."""

    def __init__(self, controller: PlaygroundController) -> None:
        super().__init__()
        self.controller = controller

    def compose(self) -> ComposeResult:
        state = self.controller.state
        with Container(classes="workflow-dialog", id="generate-dialog"):
            yield Static("GENERATE FROM CURRENT DRIFT", classes="eyebrow")
            yield Label("Create migration artifact", classes="workflow-title")
            yield Static(
                f"Environment  {state.environment}\n"
                f"Generation   {state.generation}\n"
                f"Changes      {len(state.schema.diff.changes) if state.schema.diff else 0}\n"
                "Format       TOML",
                classes="workflow-summary",
            )
            yield Input(placeholder="002_add_accounts", id="generate-revision")
            yield Input(placeholder="Optional description", id="generate-description")
            yield Static("GENERATED SQL", classes="panel-title")
            yield TextArea.code_editor(
                "\n\n".join(state.schema.forward_sql),
                language="sql",
                read_only=True,
                id="generate-sql",
            )
            yield Static("", id="generate-error")
            with Horizontal(classes="dialog-actions"):
                yield Button("Cancel", id="generate-cancel")
                yield Button(
                    "Create TOML",
                    id="generate-execute",
                    variant="primary",
                    disabled=True,
                )

    @on(Input.Changed, "#generate-revision")
    def validate_revision(self) -> None:
        revision = self.query_one("#generate-revision", Input).value
        state = self.controller.state
        valid = bool(_REVISION.fullmatch(revision))
        ready = (
            valid
            and not state.schema.stale
            and state.schema.model_snapshot is not None
            and state.schema.live_snapshot is not None
            and bool(state.schema.forward_sql)
        )
        self.query_one("#generate-execute", Button).disabled = not ready
        self.query_one("#generate-error", Static).update(
            ""
            if ready or not revision
            else "Use letters, numbers, dots, dashes, and underscores only."
        )

    @on(Button.Pressed, "#generate-cancel")
    def cancel(self) -> None:
        self.dismiss(None)

    @on(Button.Pressed, "#generate-execute")
    async def execute(self) -> None:
        revision = self.query_one("#generate-revision", Input).value
        description = self.query_one("#generate-description", Input).value.strip()
        button = self.query_one("#generate-execute", Button)
        button.disabled = True
        try:
            path = await self.controller.generate_migration(
                revision,
                description or None,
            )
        except Exception as exc:
            self.query_one("#generate-error", Static).update(str(exc))
            button.disabled = False
            return
        self.dismiss(path)


class RepairDialog(ModalScreen[ControllerActionOutcome | None]):
    """Clear one dirty history row without hiding the retained status."""

    def __init__(
        self,
        *,
        controller: PlaygroundController,
        request: ActionRequest,
        context: PreflightContext,
        status: str,
    ) -> None:
        super().__init__()
        self.controller = controller
        self.request = request
        self.context = context
        self.status = status
        self.decision = evaluate_action(
            request,
            controller.config.environment,
            context,
            confirmed=True,
        )

    def compose(self) -> ComposeResult:
        with Container(classes="workflow-dialog", id="repair-dialog"):
            yield Static("DIRTY HISTORY REPAIR", classes="eyebrow")
            yield Label("Repair migration metadata", classes="workflow-title")
            yield Static(
                f"Environment  {self.request.environment}\n"
                f"Database     {self.request.database_name}\n"
                f"Revision     {self.request.target}\n"
                f"Keep status  {self.status}\n"
                "Change        clear dirty flag",
                classes="workflow-summary",
            )
            yield Static(
                "This edits migration history only. It does not execute schema SQL.",
                classes="muted",
            )
            yield Static("", classes="preflight-results", id="repair-preflight")
            yield Static("", classes="muted", id="repair-phrase-label")
            yield Input(id="repair-confirmation", placeholder="Type the exact phrase")
            with Horizontal(classes="dialog-actions"):
                yield Button("Cancel", id="repair-cancel")
                yield Button(
                    "Repair history",
                    id="repair-execute",
                    variant="error",
                    disabled=True,
                )

    def on_mount(self) -> None:
        self._refresh_decision()

    @on(Input.Changed, "#repair-confirmation")
    def confirmation_changed(self) -> None:
        self._refresh_decision()

    @on(Button.Pressed, "#repair-cancel")
    def cancel(self) -> None:
        self.dismiss(None)

    @on(Button.Pressed, "#repair-execute")
    async def execute(self) -> None:
        outcome = await self.controller.execute_repair(
            self.request,
            self.context,
            status=self.status,
            confirmed=True,
            confirmation=self.query_one("#repair-confirmation", Input).value,
        )
        if outcome.error is not None:
            self.query_one("#repair-preflight", Static).update(outcome.error.message)
            return
        self.dismiss(outcome)

    def _refresh_decision(self) -> None:
        confirmation = self.query_one("#repair-confirmation", Input).value
        self.decision = evaluate_action(
            self.request,
            self.controller.config.environment,
            self.context,
            confirmed=True,
            confirmation=confirmation,
        )
        self.query_one("#repair-phrase-label", Static).update(
            f"Type exactly: {self.decision.phrase}"
        )
        self.query_one("#repair-preflight", Static).update(
            "✓ History repair checks pass"
            if not self.decision.reasons
            else "\n".join(f"× {reason}" for reason in self.decision.reasons)
        )
        self.query_one("#repair-execute", Button).disabled = not self.decision.allowed


class SquashDialog(ModalScreen[ControllerActionOutcome | None]):
    """Review exact pending inputs before creating a squashed artifact."""

    def __init__(
        self,
        *,
        controller: PlaygroundController,
        paths: tuple[Path, ...],
        database_name: str,
        context: PreflightContext,
        default_revision: str,
    ) -> None:
        super().__init__()
        self.controller = controller
        self.paths = paths
        self.database_name = database_name
        self.context = context
        self.default_revision = default_revision
        self.request = controller.build_squash_request(
            paths,
            default_revision,
            database_name=database_name,
        )
        self.decision = SafetyDecision(False, None)

    def compose(self) -> ComposeResult:
        with Container(classes="workflow-dialog", id="squash-dialog"):
            yield Static("LOCAL HISTORY CONSOLIDATION", classes="eyebrow")
            yield Label("Squash pending migrations", classes="workflow-title")
            yield Static(
                f"Environment  {self.request.environment}\n"
                f"Database     {self.request.database_name}\n"
                f"Inputs       {len(self.paths)} pending files\n"
                + "\n".join(f"  • {path.name}" for path in self.paths),
                classes="workflow-summary",
            )
            yield Input(value=self.default_revision, id="squash-revision")
            yield Static("COMBINED INPUT SQL", classes="panel-title")
            yield TextArea.code_editor(
                "\n\n".join(self.request.sql),
                language="sql",
                read_only=True,
                id="squash-sql",
            )
            yield Static("", classes="preflight-results", id="squash-preflight")
            yield Static("", classes="muted", id="squash-phrase-label")
            yield Input(id="squash-confirmation", placeholder="Type the exact phrase")
            with Horizontal(classes="dialog-actions"):
                yield Button("Cancel", id="squash-cancel")
                yield Button(
                    "Create squash",
                    id="squash-execute",
                    variant="warning",
                    disabled=True,
                )

    def on_mount(self) -> None:
        self._refresh_decision()

    @on(Input.Changed, "#squash-revision")
    @on(Input.Changed, "#squash-confirmation")
    def input_changed(self) -> None:
        self._refresh_decision()

    @on(Button.Pressed, "#squash-cancel")
    def cancel(self) -> None:
        self.dismiss(None)

    @on(Button.Pressed, "#squash-execute")
    async def execute(self) -> None:
        outcome = await self.controller.execute_squash(
            self.request,
            self.context,
            self.paths,
            confirmed=True,
            confirmation=self.query_one("#squash-confirmation", Input).value,
        )
        if outcome.error is not None:
            self.query_one("#squash-preflight", Static).update(outcome.error.message)
            return
        self.dismiss(outcome)

    def _refresh_decision(self) -> None:
        revision = self.query_one("#squash-revision", Input).value
        confirmation = self.query_one("#squash-confirmation", Input).value
        valid_revision = bool(_REVISION.fullmatch(revision))
        if valid_revision:
            self.request = replace(self.request, target=revision)
        self.decision = evaluate_action(
            self.request,
            self.controller.config.environment,
            self.context,
            confirmed=True,
            confirmation=confirmation,
        )
        reasons = list(self.decision.reasons)
        if not valid_revision:
            reasons.insert(0, "revision name is invalid")
        self.query_one("#squash-phrase-label", Static).update(
            f"Type exactly: {self.decision.phrase or ''}"
        )
        self.query_one("#squash-preflight", Static).update(
            "✓ Inputs and history checks pass"
            if not reasons
            else "\n".join(f"× {reason}" for reason in reasons)
        )
        self.query_one("#squash-execute", Button).disabled = bool(reasons)
