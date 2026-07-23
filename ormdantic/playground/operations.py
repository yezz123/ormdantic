"""Authorized migration operations shared by the playground UI."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path
from typing import Any

from ormdantic import Ormdantic
from ormdantic.migrations import (
    MigrationArtifact,
    MigrationHistoryEntry,
    MigrationPlan,
    SchemaSnapshot,
    create_migration_artifact,
)
from ormdantic.playground.config import (
    DatabaseUrlSource,
    EffectiveConfig,
    resolve_database_url,
)
from ormdantic.playground.safety import (
    ActionRequest,
    Risk,
    SafetyDecision,
    classify_plan,
)
from ormdantic.playground.workspace import ArtifactDocument, save_document

_REVISION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_RISK_ORDER = {
    Risk.READ_ONLY: 0,
    Risk.WRITE: 1,
    Risk.HISTORY_REWRITE: 2,
    Risk.DESTRUCTIVE: 3,
}


class MigrationOperations:
    """Run migration APIs in worker threads after exact safety authorization."""

    def __init__(
        self,
        config: EffectiveConfig,
        *,
        url_resolver: Callable[..., DatabaseUrlSource] = resolve_database_url,
        database_factory: Callable[[str], Any] = Ormdantic,
        thread_runner: Callable[..., Any] = asyncio.to_thread,
        artifact_generator: Callable[..., MigrationArtifact | None] | None = None,
    ) -> None:
        self.config = config
        self._url_resolver = url_resolver
        self._database_factory = database_factory
        self._thread_runner = thread_runner
        self._artifact_generator = artifact_generator or _generate_artifact

    async def generate(
        self,
        revision: str,
        description: str | None,
        *,
        before: SchemaSnapshot,
        after: SchemaSnapshot,
        depends_on: Sequence[str] = (),
    ) -> Path | None:
        """Generate and atomically save a canonical TOML artifact."""
        _validate_revision(revision)
        resolved = self._url_resolver(self.config.environment)
        generate = partial(
            self._artifact_generator,
            revision,
            before,
            after,
            dialect=resolved.value,
            description=description,
            depends_on=tuple(depends_on),
        )
        artifact = await self._thread_runner(generate)
        if artifact is None:
            return None
        destination = self.config.project.migrations_dir / f"{revision}.toml"
        document = ArtifactDocument(
            path=destination,
            format="toml",
            source=artifact.to_toml(),
            artifact=artifact,
            last_valid_artifact=artifact,
            dirty=True,
        )
        return save_document(document).path

    async def apply(
        self,
        document: ArtifactDocument,
        request: ActionRequest,
        decision: SafetyDecision,
    ) -> bool:
        """Apply the exact artifact covered by an allowed safety decision."""
        artifact = _authorized_artifact(document, request, decision, "apply")
        risk = classify_plan(artifact.to_plan())
        _validate_risk(request.risk, risk)
        resolved = self._url_resolver(self.config.environment)
        return await self._thread_runner(
            _apply_sync,
            self._database_factory,
            resolved.value,
            artifact,
            risk is Risk.DESTRUCTIVE,
        )

    async def rollback(
        self,
        document: ArtifactDocument,
        request: ActionRequest,
        decision: SafetyDecision,
    ) -> bool:
        """Roll back the exact artifact covered by an allowed decision."""
        artifact = _authorized_artifact(document, request, decision, "rollback")
        rollback_plan = MigrationPlan(
            operations=list(artifact.rollback_operations),
            diff=artifact.diff,
            warnings=artifact.warnings,
            safety=artifact.safety,
        )
        risk = classify_plan(rollback_plan)
        _validate_risk(request.risk, risk)
        resolved = self._url_resolver(self.config.environment)
        return await self._thread_runner(
            _rollback_sync,
            self._database_factory,
            resolved.value,
            artifact,
            risk is Risk.DESTRUCTIVE,
        )

    async def repair(
        self,
        revision: str,
        status: str,
        request: ActionRequest,
        decision: SafetyDecision,
    ) -> int:
        """Repair selected history metadata after authorization."""
        _authorized_request(request, decision, "repair", revision)
        resolved = self._url_resolver(self.config.environment)
        return await self._thread_runner(
            _repair_sync,
            self._database_factory,
            resolved.value,
            revision,
            status,
        )

    async def squash(
        self,
        paths: Sequence[Path],
        revision: str,
        request: ActionRequest,
        decision: SafetyDecision,
    ) -> Path:
        """Squash a reviewed contiguous selection into a new TOML artifact."""
        _validate_revision(revision)
        _authorized_request(request, decision, "squash", revision)
        artifacts = tuple(MigrationArtifact.read(path) for path in paths)
        resolved = self._url_resolver(self.config.environment)
        squashed = await self._thread_runner(
            _squash_sync,
            self._database_factory,
            resolved.value,
            revision,
            artifacts,
        )
        destination = self.config.project.migrations_dir / f"{revision}.toml"
        document = ArtifactDocument(
            path=destination,
            format="toml",
            source=squashed.to_toml(),
            artifact=squashed,
            last_valid_artifact=squashed,
            dirty=True,
        )
        return save_document(document).path

    async def history(self) -> tuple[MigrationHistoryEntry, ...]:
        """Read durable history in the blocking worker boundary."""
        resolved = self._url_resolver(self.config.environment)
        return await self._thread_runner(
            _history_sync,
            self._database_factory,
            resolved.value,
        )


def _authorized_artifact(
    document: ArtifactDocument,
    request: ActionRequest,
    decision: SafetyDecision,
    action: str,
) -> MigrationArtifact:
    artifact = document.artifact
    if artifact is None:
        raise PermissionError("invalid migration artifact is not authorized")
    _authorized_request(request, decision, action, artifact.revision)
    if request.artifact_checksum != artifact.checksum:
        raise PermissionError("artifact changed after the safety review")
    return artifact


def _authorized_request(
    request: ActionRequest,
    decision: SafetyDecision,
    action: str,
    target: str,
) -> None:
    if not decision.allowed:
        raise PermissionError("migration action is not authorized")
    if request.action != action or request.target != target:
        raise PermissionError("safety decision does not match the requested action")


def _validate_risk(reviewed: Risk, actual: Risk) -> None:
    if _RISK_ORDER[reviewed] < _RISK_ORDER[actual]:
        raise PermissionError(
            f"reviewed risk {reviewed.value!r} is lower than actual {actual.value!r}"
        )


def _validate_revision(revision: str) -> None:
    if not _REVISION.fullmatch(revision):
        raise ValueError(
            "revision must start with a letter or number and contain only "
            "letters, numbers, dots, dashes, or underscores"
        )


def _generate_artifact(
    revision: str,
    before: SchemaSnapshot,
    after: SchemaSnapshot,
    *,
    dialect: str,
    description: str | None,
    depends_on: Sequence[str],
) -> MigrationArtifact | None:
    artifact = create_migration_artifact(
        revision,
        before,
        after,
        dialect=dialect,
        description=description,
        depends_on=depends_on,
    )
    return artifact if artifact.operations else None


def _apply_sync(
    database_factory: Callable[[str], Any],
    url: str,
    artifact: MigrationArtifact,
    allow_destructive: bool,
) -> bool:
    database = database_factory(url)
    return bool(
        asyncio.run(
            database.migrations.apply_artifact(
                artifact,
                allow_destructive=allow_destructive,
            )
        )
    )


def _rollback_sync(
    database_factory: Callable[[str], Any],
    url: str,
    artifact: MigrationArtifact,
    allow_destructive: bool,
) -> bool:
    database = database_factory(url)
    return bool(
        asyncio.run(
            database.migrations.rollback_artifact(
                artifact,
                allow_destructive=allow_destructive,
            )
        )
    )


def _repair_sync(
    database_factory: Callable[[str], Any],
    url: str,
    revision: str,
    status: str,
) -> int:
    database = database_factory(url)
    return int(
        asyncio.run(
            database.migrations.repair(
                revision=revision,
                status=status,
                clear_dirty=True,
            )
        )
    )


def _squash_sync(
    database_factory: Callable[[str], Any],
    url: str,
    revision: str,
    artifacts: Sequence[MigrationArtifact],
) -> MigrationArtifact:
    database = database_factory(url)
    return database.migrations.squash(revision, artifacts, dialect=url)


def _history_sync(
    database_factory: Callable[[str], Any],
    url: str,
) -> tuple[MigrationHistoryEntry, ...]:
    database = database_factory(url)
    return tuple(asyncio.run(database.migrations.history()))
