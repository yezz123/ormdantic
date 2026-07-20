"""Guarded migration action review dialog."""

from __future__ import annotations

from dataclasses import replace

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Horizontal
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Input, Label, Static, TextArea

from ormdantic.playground.controller import (
    ControllerActionOutcome,
    PlaygroundController,
)
from ormdantic.playground.safety import (
    ActionRequest,
    PreflightContext,
    Risk,
    evaluate_action,
)


class ActionDialog(ModalScreen[ControllerActionOutcome | None]):
    """Review complete SQL and current preflight before any database mutation."""

    def __init__(
        self,
        *,
        controller: PlaygroundController,
        request: ActionRequest,
        context: PreflightContext,
    ) -> None:
        super().__init__()
        self.controller = controller
        self.request = request
        self.context = context
        self.decision = evaluate_action(
            request,
            controller.config.environment,
            context,
            confirmed=True,
        )

    def compose(self) -> ComposeResult:
        with Container(id="action-dialog"):
            yield Static("MIGRATION PREFLIGHT", classes="eyebrow")
            yield Label(
                f"Review {self.request.action}",
                id="action-title",
            )
            yield Static(self._summary(), id="action-summary")
            yield Static("SQL TO EXECUTE", classes="panel-title")
            yield TextArea.code_editor(
                "\n\n".join(self.request.sql),
                language="sql",
                read_only=True,
                id="action-sql",
            )
            yield Static("PREFLIGHT", classes="panel-title")
            yield Static("", id="action-preflight")
            yield Checkbox(
                "I reviewed every destructive operation shown above",
                value=self.context.destructive_reviewed,
                id="action-review",
            )
            yield Static("", id="action-phrase-label", classes="muted")
            yield Input(
                placeholder="Type the exact phrase",
                id="action-confirmation",
            )
            with Horizontal(classes="dialog-actions"):
                yield Button("Cancel", id="action-cancel")
                yield Button(
                    self.request.action.title(),
                    id="action-execute",
                    variant="error"
                    if self.request.risk is Risk.DESTRUCTIVE
                    else "primary",
                )

    def on_mount(self) -> None:
        destructive = self.request.risk is Risk.DESTRUCTIVE
        self.query_one("#action-review", Checkbox).display = destructive
        self._refresh_decision()

    @on(Input.Changed, "#action-confirmation")
    @on(Checkbox.Changed, "#action-review")
    def confirmation_changed(self) -> None:
        self._refresh_decision()

    @on(Button.Pressed, "#action-cancel")
    def cancel(self) -> None:
        self.dismiss(None)

    @on(Button.Pressed, "#action-execute")
    async def execute(self) -> None:
        if not self.decision.allowed:
            return
        outcome = await self.controller.execute_action(
            self.request,
            self._current_context(),
            confirmed=True,
            confirmation=self.query_one("#action-confirmation", Input).value,
        )
        if outcome.error is not None:
            self.query_one("#action-preflight", Static).update(outcome.error.message)
            return
        self.dismiss(outcome)

    def _refresh_decision(self) -> None:
        context = self._current_context()
        confirmation = self.query_one("#action-confirmation", Input).value
        self.decision = evaluate_action(
            self.request,
            self.controller.config.environment,
            context,
            confirmed=True,
            confirmation=confirmation,
        )
        phrase_input = self.query_one("#action-confirmation", Input)
        phrase_label = self.query_one("#action-phrase-label", Static)
        phrase_input.display = self.decision.phrase is not None
        phrase_label.display = self.decision.phrase is not None
        phrase_label.update(
            f"Type exactly: {self.decision.phrase}" if self.decision.phrase else ""
        )
        reasons = self.decision.reasons
        self.query_one("#action-preflight", Static).update(
            "✓ All current checks pass"
            if not reasons
            else "\n".join(f"× {reason}" for reason in reasons)
        )
        self.query_one("#action-execute", Button).disabled = not self.decision.allowed

    def _current_context(self) -> PreflightContext:
        reviewed = self.context.destructive_reviewed
        if self.is_mounted:
            reviewed = self.query_one("#action-review", Checkbox).value
        return replace(self.context, destructive_reviewed=reviewed)

    def _summary(self) -> str:
        return (
            f"Environment  {self.request.environment}\n"
            f"Database     {self.request.database_name}\n"
            f"Revision     {self.request.target}\n"
            f"Risk         {self.request.risk.value.upper()}\n"
            f"Operations   {len(self.request.sql)} total · "
            f"{len(self.request.destructive_sql)} destructive"
        )
