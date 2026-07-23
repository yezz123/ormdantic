"""State and intent coordinator for the playground application."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, cast

from ormdantic.migrations import MigrationArtifact, MigrationPlan
from ormdantic.playground.config import EffectiveConfig
from ormdantic.playground.diagnostics import Diagnostic, Severity, redact_text
from ormdantic.playground.operations import MigrationOperations
from ormdantic.playground.safety import (
    ActionRequest,
    PreflightContext,
    Risk,
    SafetyDecision,
    classify_plan,
    evaluate_action,
)
from ormdantic.playground.services import RefreshService
from ormdantic.playground.state import (
    ArtifactSummary,
    MigrationState,
    OperationState,
    PlaygroundState,
    RefreshStatus,
    accept_refresh,
)
from ormdantic.playground.workspace import (
    ArtifactDocument,
    MigrationWorkspace,
    convert_to_toml,
    discard_draft,
    load_workspace,
    recover_draft,
    replace_operation_sql,
    save_document,
    select_document,
    update_source,
    write_draft,
)


@dataclass(frozen=True)
class ControllerActionOutcome:
    """Safety and execution result returned to a view."""

    decision: SafetyDecision
    executed: bool
    result: object | None = None
    error: Diagnostic | None = None


class PlaygroundController:
    """Own application state and make services available through safe intents."""

    def __init__(
        self,
        config: EffectiveConfig,
        *,
        refresh_service: Any | None = None,
        operations: Any | None = None,
    ) -> None:
        self.config = config
        self.refresh_service = refresh_service or RefreshService()
        self.operations = operations or MigrationOperations(config)
        self.workspace = load_workspace(config.project.migrations_dir)
        self.state = PlaygroundState(
            environment=config.environment.name,
            migrations=_migration_state(self.workspace),
        )
        self._refresh_lock = asyncio.Lock()
        self._subscribers: list[Callable[[PlaygroundState], None]] = []

    @property
    def active_document(self) -> ArtifactDocument | None:
        """Return the selected migration document, if any."""
        selected = self.workspace.selected_path
        if selected is None:
            return None
        return next(
            (
                document
                for document in self.workspace.documents
                if document.path == selected
            ),
            None,
        )

    def subscribe(
        self,
        callback: Callable[[PlaygroundState], None],
    ) -> Callable[[], None]:
        """Publish future state snapshots and return an unsubscribe callback."""
        self._subscribers.append(callback)

        def unsubscribe() -> None:
            try:
                self._subscribers.remove(callback)
            except ValueError:
                return

        return unsubscribe

    async def refresh(self, *, generation: int | None = None) -> PlaygroundState:
        """Run and publish a generation-safe schema refresh."""
        async with self._refresh_lock:
            next_generation = max(self.state.generation + 1, generation or 0)
            self.state = replace(
                self.state,
                generation=next_generation,
                status=RefreshStatus.RUNNING,
            )
            self._publish()
            result = await self.refresh_service.refresh(
                self.config,
                generation=next_generation,
                previous=self.state.schema,
            )
            updated = accept_refresh(self.state, result)
            updated = replace(
                updated,
                migrations=_migration_state(self.workspace, updated.migrations),
            )
            self.state = updated
            self._publish()
            return self.state

    def reload_workspace(self) -> MigrationWorkspace:
        """Reload migration files while preserving a still-valid selection."""
        selected = self.workspace.selected_path
        workspace = load_workspace(self.config.project.migrations_dir)
        if selected is not None and any(
            document.path == selected for document in workspace.documents
        ):
            workspace = select_document(workspace, selected)
        self.workspace = workspace
        self._sync_workspace_state()
        return workspace

    def select_artifact(self, path: Path) -> None:
        """Select a migration for review and editing."""
        if self.workspace.selected_path == path.resolve():
            return
        self.workspace = select_document(self.workspace, path)
        self._sync_workspace_state()

    def edit_active_source(self, source: str) -> ArtifactDocument:
        """Apply a TOML source edit to the selected document."""
        active = self._require_active_document()
        updated = update_source(active, source)
        self._replace_document(updated)
        return updated

    def edit_active_sql(
        self,
        index: int,
        sql: str,
        *,
        rollback: bool = False,
    ) -> ArtifactDocument:
        """Apply a selected operation SQL edit."""
        active = self._require_active_document()
        updated = replace_operation_sql(
            active,
            index=index,
            sql=sql,
            rollback=rollback,
        )
        self._replace_document(updated)
        return updated

    def save_active(
        self,
        *,
        destination: Path | None = None,
        overwrite: bool = False,
    ) -> ArtifactDocument:
        """Atomically persist the selected valid editor document."""
        saved = save_document(
            self._require_active_document(),
            destination=destination,
            overwrite=overwrite,
        )
        self._replace_document(saved)
        if destination is not None:
            self.workspace = replace(self.workspace, selected_path=saved.path)
        self._sync_workspace_state()
        return saved

    def convert_active_to_toml(
        self,
        destination: Path | None = None,
        *,
        overwrite: bool = False,
    ) -> ArtifactDocument:
        """Convert selected legacy JSON to a separate TOML artifact."""
        active = self._require_active_document()
        target = destination or active.path.with_suffix(".toml")
        converted = convert_to_toml(active, target, overwrite=overwrite)
        self.reload_workspace()
        self.workspace = select_document(self.workspace, converted.path)
        self._sync_workspace_state()
        return self._require_active_document()

    def write_active_draft(self) -> Path:
        """Persist selected editor source in project-local recovery storage."""
        return write_draft(self.config.root, self._require_active_document())

    def recover_active_draft(self) -> ArtifactDocument:
        """Restore selected editor source from its recovery draft."""
        recovered = recover_draft(
            self.config.root,
            self._require_active_document(),
        )
        self._replace_document(recovered)
        return recovered

    def discard_active_draft(self) -> None:
        """Discard selected recovery source after UI confirmation."""
        discard_draft(self.config.root, self._require_active_document())

    def build_action_request(
        self,
        action: str,
        *,
        database_name: str,
    ) -> ActionRequest:
        """Bind an action review to current artifact content and generation."""
        document = self._require_active_document()
        artifact = document.artifact
        if artifact is None:
            raise ValueError("selected migration artifact is invalid")
        if action == "apply":
            operations = artifact.operations
            plan = MigrationPlan(operations=list(operations))
        elif action == "rollback":
            operations = artifact.rollback_operations
            plan = MigrationPlan(
                operations=list(operations),
                diff=artifact.diff,
                warnings=artifact.warnings,
                safety=artifact.safety,
            )
        else:
            raise ValueError(f"unsupported artifact action: {action}")
        risk = classify_plan(plan)
        return ActionRequest(
            action=action,
            environment=self.config.environment.name,
            database_name=database_name,
            target=artifact.revision,
            risk=risk,
            sql=tuple(operation.sql for operation in operations),
            destructive_sql=tuple(
                operation.sql for operation in operations if operation.destructive
            ),
            artifact_checksum=artifact.checksum,
            reviewed_generation=self.state.generation,
        )

    async def generate_migration(
        self,
        revision: str,
        description: str | None = None,
    ) -> Path | None:
        """Generate a canonical TOML artifact from the current fresh drift."""
        before = self.state.schema.live_snapshot
        after = self.state.schema.model_snapshot
        if before is None or after is None or self.state.schema.stale:
            raise ValueError("refresh model and live schemas before generating")
        if not self.state.schema.forward_sql:
            raise ValueError("there is no schema drift to generate")
        self.state = replace(
            self.state,
            operation=OperationState(
                name="generate",
                target=revision,
                running=True,
                message=f"Generating {revision}",
            ),
        )
        self._publish()
        try:
            path = await self.operations.generate(
                revision,
                description,
                before=before,
                after=after,
                depends_on=(
                    (self.state.migrations.current_revision,)
                    if self.state.migrations.current_revision
                    else ()
                ),
            )
        except Exception as exc:
            self._publish_operation_failure("generate", revision, exc)
            raise
        if path is not None:
            self.reload_workspace()
            self.select_artifact(path)
        self.state = replace(
            self.state,
            operation=OperationState(
                name="generate",
                target=revision,
                message=(
                    f"Generated {revision}"
                    if path is not None
                    else "No migration generated"
                ),
            ),
        )
        self._publish()
        return path

    def build_repair_request(
        self,
        revision: str,
        *,
        database_name: str,
    ) -> ActionRequest:
        """Bind a dirty-history repair to the selected row and generation."""
        entry = next(
            (
                item
                for item in self.state.migrations.history
                if item.revision == revision
            ),
            None,
        )
        if entry is None:
            raise ValueError(f"history revision is not available: {revision}")
        if not entry.dirty:
            raise ValueError(f"history revision is not dirty: {revision}")
        return ActionRequest(
            action="repair",
            environment=self.config.environment.name,
            database_name=database_name,
            target=revision,
            risk=Risk.HISTORY_REWRITE,
            sql=(f"clear dirty flag; preserve status {entry.status}",),
            reviewed_generation=self.state.generation,
        )

    async def execute_repair(
        self,
        request: ActionRequest,
        context: PreflightContext,
        *,
        status: str,
        confirmed: bool,
        confirmation: str | None,
    ) -> ControllerActionOutcome:
        """Repair one dirty history row after an exact typed review."""
        decision = evaluate_action(
            request,
            self.config.environment,
            context,
            confirmed=confirmed,
            confirmation=confirmation,
        )
        if not decision.allowed:
            return ControllerActionOutcome(decision=decision, executed=False)
        self.state = replace(
            self.state,
            operation=OperationState(
                name="repair",
                target=request.target,
                running=True,
                message=f"Repairing {request.target}",
            ),
        )
        self._publish()
        try:
            result = await self.operations.repair(
                request.target,
                status,
                request,
                decision,
            )
        except Exception as exc:
            diagnostic = self._publish_operation_failure(
                "repair",
                request.target,
                exc,
            )
            return ControllerActionOutcome(
                decision=decision,
                executed=True,
                error=diagnostic,
            )
        await self.refresh()
        self.state = replace(
            self.state,
            operation=OperationState(
                name="repair",
                target=request.target,
                message=f"Repaired {request.target}",
            ),
        )
        self._publish()
        return ControllerActionOutcome(decision, True, result)

    def build_squash_request(
        self,
        paths: tuple[Path, ...],
        revision: str,
        *,
        database_name: str,
    ) -> ActionRequest:
        """Bind a local squash preview to exact pending artifact checksums."""
        documents = self._squash_documents(paths)
        artifacts = cast(
            tuple[MigrationArtifact, ...],
            tuple(document.artifact for document in documents),
        )
        sql = tuple(
            operation.sql for artifact in artifacts for operation in artifact.operations
        )
        return ActionRequest(
            action="squash",
            environment=self.config.environment.name,
            database_name=database_name,
            target=revision,
            risk=Risk.HISTORY_REWRITE,
            sql=sql,
            destructive_sql=tuple(
                operation.sql
                for artifact in artifacts
                for operation in artifact.operations
                if operation.destructive
            ),
            artifact_checksum=_workspace_checksum(documents),
            reviewed_generation=self.state.generation,
        )

    async def execute_squash(
        self,
        request: ActionRequest,
        context: PreflightContext,
        paths: tuple[Path, ...],
        *,
        confirmed: bool,
        confirmation: str | None,
    ) -> ControllerActionOutcome:
        """Write a squashed artifact only if the reviewed inputs are unchanged."""
        decision = evaluate_action(
            request,
            self.config.environment,
            context,
            confirmed=confirmed,
            confirmation=confirmation,
        )
        if not decision.allowed:
            return ControllerActionOutcome(decision=decision, executed=False)
        documents = self._squash_documents(paths)
        if request.artifact_checksum != _workspace_checksum(documents):
            decision = replace(
                decision,
                allowed=False,
                reasons=(*decision.reasons, "squash inputs changed after review"),
            )
            return ControllerActionOutcome(decision=decision, executed=False)
        self.state = replace(
            self.state,
            operation=OperationState(
                name="squash",
                target=request.target,
                running=True,
                message=f"Squashing into {request.target}",
            ),
        )
        self._publish()
        try:
            path = await self.operations.squash(
                paths,
                request.target,
                request,
                decision,
            )
        except Exception as exc:
            diagnostic = self._publish_operation_failure(
                "squash",
                request.target,
                exc,
            )
            return ControllerActionOutcome(
                decision=decision,
                executed=True,
                error=diagnostic,
            )
        self.reload_workspace()
        self.select_artifact(path)
        self.state = replace(
            self.state,
            operation=OperationState(
                name="squash",
                target=request.target,
                message=f"Created squash {request.target}",
            ),
        )
        self._publish()
        return ControllerActionOutcome(decision, True, path)

    async def execute_action(
        self,
        request: ActionRequest,
        context: PreflightContext,
        *,
        confirmed: bool = False,
        confirmation: str | None = None,
    ) -> ControllerActionOutcome:
        """Evaluate, execute, refresh, and only then report completion."""
        decision = evaluate_action(
            request,
            self.config.environment,
            context,
            confirmed=confirmed,
            confirmation=confirmation,
        )
        if not decision.allowed:
            return ControllerActionOutcome(decision=decision, executed=False)
        document = self._require_active_document()
        self.state = replace(
            self.state,
            operation=OperationState(
                name=request.action,
                target=request.target,
                running=True,
                message=f"Running {request.action} for {request.target}",
            ),
        )
        self._publish()
        try:
            if request.action == "apply":
                result = await self.operations.apply(document, request, decision)
            elif request.action == "rollback":
                result = await self.operations.rollback(document, request, decision)
            else:
                raise ValueError(f"unsupported controller action: {request.action}")
        except Exception as exc:
            diagnostic = self._publish_operation_failure(
                request.action,
                request.target,
                exc,
            )
            return ControllerActionOutcome(
                decision=decision,
                executed=True,
                error=diagnostic,
            )

        self.reload_workspace()
        await self.refresh()
        verb = "Applied" if request.action == "apply" else "Rolled back"
        self.state = replace(
            self.state,
            operation=OperationState(
                name=request.action,
                target=request.target,
                running=False,
                message=f"{verb} {request.target}",
            ),
        )
        self._publish()
        return ControllerActionOutcome(
            decision=decision,
            executed=True,
            result=result,
        )

    def _replace_document(self, updated: ArtifactDocument) -> None:
        documents = tuple(
            updated if document.path == self.workspace.selected_path else document
            for document in self.workspace.documents
        )
        if updated.path not in {document.path for document in documents}:
            documents = (*documents, updated)
        self.workspace = replace(
            self.workspace,
            documents=documents,
            selected_path=updated.path,
        )
        self._sync_workspace_state()

    def _squash_documents(
        self,
        paths: tuple[Path, ...],
    ) -> tuple[ArtifactDocument, ...]:
        if len(paths) < 2:
            raise ValueError("select at least two pending migrations to squash")
        resolved = tuple(path.resolve() for path in paths)
        by_path = {document.path: document for document in self.workspace.documents}
        try:
            documents = tuple(by_path[path] for path in resolved)
        except KeyError as exc:
            raise ValueError(
                f"migration is not in the workspace: {exc.args[0]}"
            ) from exc
        status = {item.path: item.status for item in self.state.migrations.artifacts}
        if any(status.get(document.path) == "applied" for document in documents):
            raise ValueError("applied migrations cannot be squashed")
        if any(document.artifact is None or document.dirty for document in documents):
            raise ValueError("save and validate every squash input first")
        return documents

    def _publish_operation_failure(
        self,
        action: str,
        target: str,
        error: Exception,
    ) -> Diagnostic:
        diagnostic = Diagnostic.create(
            Severity.ERROR,
            f"operation.{action}_failed",
            str(error),
            hint="Review the operation log and database state before retrying.",
        )
        self.state = replace(
            self.state,
            operation=OperationState(
                name=action,
                target=target,
                message=redact_text(str(error)),
            ),
            diagnostics=(*self.state.diagnostics, diagnostic),
        )
        self._publish()
        return diagnostic

    def _require_active_document(self) -> ArtifactDocument:
        document = self.active_document
        if document is None:
            raise ValueError("select a migration artifact first")
        return document

    def _sync_workspace_state(self) -> None:
        self.state = replace(
            self.state,
            migrations=_migration_state(self.workspace, self.state.migrations),
        )
        self._publish()

    def _publish(self) -> None:
        for callback in tuple(self._subscribers):
            callback(self.state)


def _migration_state(
    workspace: MigrationWorkspace,
    current: MigrationState | None = None,
) -> MigrationState:
    current = current or MigrationState()
    history_status = {entry.revision: entry.status for entry in current.history}
    summaries: list[ArtifactSummary] = []
    for document in workspace.documents:
        artifact = document.artifact
        revision = artifact.revision if artifact is not None else None
        status = (
            "invalid"
            if artifact is None
            else history_status.get(artifact.revision, "pending")
        )
        plan = artifact.to_plan() if artifact is not None else None
        summaries.append(
            ArtifactSummary(
                path=document.path,
                revision=revision,
                status=status,
                format=document.format,
                destructive=(
                    plan.has_destructive_operations if plan is not None else False
                ),
                unsafe=(plan.has_unsafe_operations if plan is not None else False),
                valid=artifact is not None
                and all(
                    item.severity is not Severity.ERROR for item in document.diagnostics
                ),
            )
        )
    return replace(
        current,
        artifacts=tuple(summaries),
        selected_path=workspace.selected_path,
    )


def _workspace_checksum(documents: tuple[ArtifactDocument, ...]) -> str:
    identity = "\n".join(
        f"{document.path}:{document.artifact.checksum}"
        for document in documents
        if document.artifact is not None
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()
