"""Migration artifact serialization and checksum helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from os import PathLike
from pathlib import Path
from typing import Any

from ormdantic._migrations.documents import toml_dumps, toml_loads
from ormdantic._migrations.models import (
    MIGRATION_ARTIFACT_VERSION,
    MigrationChange,
    MigrationOperation,
    MigrationPlan,
    MigrationWarning,
    SchemaDiff,
    SchemaSnapshot,
    optional_str,
)
from ormdantic._migrations.sql import document_format

UTC = timezone.utc


@dataclass(frozen=True)
class MigrationArtifact:
    """A serializable migration file with snapshots, SQL, and safety metadata."""

    revision: str
    from_snapshot: SchemaSnapshot
    to_snapshot: SchemaSnapshot
    operations: list[MigrationOperation] = field(default_factory=list)
    rollback_operations: list[MigrationOperation] = field(default_factory=list)
    diff: SchemaDiff = field(default_factory=SchemaDiff)
    warnings: list[MigrationWarning] = field(default_factory=list)
    description: str | None = None
    created_at: str = field(
        default_factory=lambda: datetime.now(UTC).replace(microsecond=0).isoformat()
    )
    dialect: str | None = None
    checksum: str | None = None
    depends_on: list[str] = field(default_factory=list)
    branch_labels: list[str] = field(default_factory=list)
    safety: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    artifact_version: int = MIGRATION_ARTIFACT_VERSION
    version: int = MIGRATION_ARTIFACT_VERSION

    @classmethod
    def from_plan(
        cls,
        revision: str,
        plan: MigrationPlan,
        from_snapshot: SchemaSnapshot,
        to_snapshot: SchemaSnapshot,
        *,
        dialect: str | None = None,
        description: str | None = None,
        depends_on: Sequence[str] | None = None,
        branch_labels: Sequence[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
        created_at: str | None = None,
    ) -> MigrationArtifact:
        artifact = cls(
            revision=revision,
            from_snapshot=from_snapshot,
            to_snapshot=to_snapshot,
            operations=[
                operation_from_dict(operation_to_dict(operation))
                for operation in plan.operations
            ],
            rollback_operations=[
                operation_from_dict(operation_to_dict(operation))
                for operation in plan.rollback_operations
            ],
            diff=diff_from_dict(diff_to_dict(plan.diff)),
            warnings=[
                warning_from_dict(warning_to_dict(warning)) for warning in plan.warnings
            ],
            description=description,
            created_at=created_at
            or datetime.now(UTC).replace(microsecond=0).isoformat(),
            dialect=dialect,
            depends_on=[str(item) for item in depends_on or ()],
            branch_labels=[str(item) for item in branch_labels or ()],
            safety=dict(plan.safety),
            metadata=dict(metadata or {}),
            artifact_version=MIGRATION_ARTIFACT_VERSION,
            version=MIGRATION_ARTIFACT_VERSION,
        )
        return artifact.with_checksum()

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> MigrationArtifact:
        artifact_version = int(
            payload.get("artifact_version", payload.get("version", 1))
        )
        artifact = cls(
            revision=str(payload["revision"]),
            from_snapshot=SchemaSnapshot.from_dict(payload["from_snapshot"]),
            to_snapshot=SchemaSnapshot.from_dict(payload["to_snapshot"]),
            operations=[
                operation_from_dict(operation)
                for operation in payload.get("up", payload.get("operations", []))
            ],
            rollback_operations=[
                operation_from_dict(operation)
                for operation in payload.get(
                    "down", payload.get("rollback_operations", [])
                )
            ],
            diff=diff_from_dict(payload.get("diff", {})),
            warnings=[
                warning_from_dict(warning) for warning in payload.get("warnings", [])
            ],
            description=optional_str(payload.get("description")),
            created_at=str(
                payload.get(
                    "created_at",
                    datetime.now(UTC).replace(microsecond=0).isoformat(),
                )
            ),
            dialect=optional_str(payload.get("dialect")),
            checksum=optional_str(payload.get("checksum")),
            depends_on=[str(item) for item in payload.get("depends_on", [])],
            branch_labels=[str(item) for item in payload.get("branch_labels", [])],
            safety=dict(payload.get("safety", {})),
            metadata=dict(payload.get("metadata", {})),
            artifact_version=artifact_version,
            version=int(payload.get("version", artifact_version)),
        )
        if artifact.checksum:
            artifact.validate_checksum()
        return artifact

    @classmethod
    def from_json(cls, payload: str | bytes | bytearray) -> MigrationArtifact:
        return cls.from_dict(json.loads(payload))

    @classmethod
    def from_toml(cls, payload: str | bytes | bytearray) -> MigrationArtifact:
        return cls.from_dict(toml_loads(payload))

    @classmethod
    def read(
        cls, path: str | PathLike[str], *, format: str | None = None
    ) -> MigrationArtifact:
        input_path = Path(path)
        document = input_path.read_text()
        if document_format(str(input_path), format) == "toml":
            return cls.from_toml(document)
        return cls.from_json(document)

    def to_plan(self) -> MigrationPlan:
        return MigrationPlan(
            operations=[
                operation_from_dict(operation_to_dict(operation))
                for operation in self.operations
            ],
            rollback_operations=[
                operation_from_dict(operation_to_dict(operation))
                for operation in self.rollback_operations
            ],
            diff=diff_from_dict(diff_to_dict(self.diff)),
            warnings=[
                warning_from_dict(warning_to_dict(warning)) for warning in self.warnings
            ],
            safety=dict(self.safety),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "artifact_version": self.artifact_version,
            "revision": self.revision,
            "description": self.description,
            "created_at": self.created_at,
            "dialect": self.dialect,
            "checksum": self.checksum,
            "depends_on": list(self.depends_on),
            "branch_labels": list(self.branch_labels),
            "from_snapshot": self.from_snapshot.to_dict(),
            "to_snapshot": self.to_snapshot.to_dict(),
            "up": [operation_to_dict(operation) for operation in self.operations],
            "down": [
                operation_to_dict(operation) for operation in self.rollback_operations
            ],
            "diff": diff_to_dict(self.diff),
            "warnings": [warning_to_dict(warning) for warning in self.warnings],
            "safety": dict(self.safety),
            "metadata": dict(self.metadata),
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    def to_toml(self) -> str:
        return toml_dumps(self.to_dict())

    def write(self, path: str | PathLike[str], *, format: str | None = None) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        if document_format(str(output), format) == "toml":
            output.write_text(self.to_toml())
        else:
            output.write_text(self.to_json())

    def with_checksum(self) -> MigrationArtifact:
        payload = dict(self.to_dict())
        payload.pop("checksum", None)
        checksum = artifact_checksum(payload)
        return MigrationArtifact(
            revision=self.revision,
            from_snapshot=self.from_snapshot,
            to_snapshot=self.to_snapshot,
            operations=self.operations,
            rollback_operations=self.rollback_operations,
            diff=self.diff,
            warnings=self.warnings,
            description=self.description,
            created_at=self.created_at,
            dialect=self.dialect,
            checksum=checksum,
            depends_on=self.depends_on,
            branch_labels=self.branch_labels,
            safety=self.safety,
            metadata=self.metadata,
            artifact_version=self.artifact_version,
            version=self.version,
        )

    def validate_checksum(self) -> None:
        if not self.checksum:
            return
        payload = dict(self.to_dict())
        payload.pop("checksum", None)
        expected = artifact_checksum(payload)
        if expected != self.checksum:
            raise ValueError(
                f"migration artifact checksum mismatch for revision {self.revision}: "
                f"expected {self.checksum}, calculated {expected}"
            )


def change_to_dict(change: MigrationChange) -> dict[str, Any]:
    return {
        "action": change.action,
        "object_type": change.object_type,
        "table": change.table,
        "name": change.name,
        "message": change.message,
        "unsafe": change.unsafe,
        "destructive": change.destructive,
        "details": change.details,
    }


def change_from_dict(payload: Mapping[str, Any]) -> MigrationChange:
    return MigrationChange(
        action=str(payload["action"]),
        object_type=str(payload["object_type"]),
        table=str(payload["table"]),
        name=str(payload["name"]),
        message=str(payload["message"]),
        unsafe=bool(payload.get("unsafe", False)),
        destructive=bool(payload.get("destructive", False)),
        details=normalize_change_details(dict(payload.get("details", {}))),
    )


def normalize_change_details(details: dict[str, Any]) -> dict[str, Any]:
    if {"name", "kind", "nullable", "primary_key"} <= set(details):
        details.setdefault("foreign_table", None)
        details.setdefault("foreign_column", None)
        details.setdefault("max_length", None)
    for key in ("from", "to"):
        value = details.get(key)
        if isinstance(value, Mapping):
            details[key] = normalize_change_details(dict(value))
    return details


def warning_to_dict(warning: MigrationWarning) -> dict[str, Any]:
    return {
        "code": warning.code,
        "message": warning.message,
        "table": warning.table,
        "name": warning.name,
    }


def warning_from_dict(payload: Mapping[str, Any]) -> MigrationWarning:
    return MigrationWarning(
        code=str(payload["code"]),
        message=str(payload["message"]),
        table=optional_str(payload.get("table")),
        name=optional_str(payload.get("name")),
    )


def diff_to_dict(diff: SchemaDiff) -> dict[str, Any]:
    return {
        "changes": [change_to_dict(change) for change in diff.changes],
        "warnings": [warning_to_dict(warning) for warning in diff.warnings],
    }


def diff_from_dict(payload: Mapping[str, Any]) -> SchemaDiff:
    return SchemaDiff(
        changes=[change_from_dict(change) for change in payload.get("changes", [])],
        warnings=[
            warning_from_dict(warning) for warning in payload.get("warnings", [])
        ],
    )


def operation_to_dict(operation: MigrationOperation) -> dict[str, Any]:
    return {
        "sql": operation.sql,
        "values": list(operation.values),
        "description": operation.description,
        "unsafe": operation.unsafe,
        "destructive": operation.destructive,
        "kind": operation.kind,
        "table": operation.table,
        "object_name": operation.object_name,
        "reversible": operation.reversible,
        "requires_lock": operation.requires_lock,
        "requires_rebuild": operation.requires_rebuild,
        "generated_rollback": operation.generated_rollback,
        "metadata": dict(operation.metadata),
    }


def operation_from_dict(payload: Mapping[str, Any]) -> MigrationOperation:
    return MigrationOperation(
        sql=str(payload["sql"]),
        values=tuple(payload.get("values", ())),
        description=optional_str(payload.get("description")),
        unsafe=bool(payload.get("unsafe", False)),
        destructive=bool(payload.get("destructive", False)),
        kind=str(payload.get("kind", "statement")),
        table=optional_str(payload.get("table")),
        object_name=optional_str(payload.get("object_name")),
        reversible=bool(payload.get("reversible", True)),
        requires_lock=bool(payload.get("requires_lock", True)),
        requires_rebuild=bool(payload.get("requires_rebuild", False)),
        generated_rollback=bool(payload.get("generated_rollback", False)),
        metadata=dict(payload.get("metadata", {})),
    )


def coerce_artifact(
    artifact: MigrationArtifact | Mapping[str, Any] | str | PathLike[str],
) -> MigrationArtifact:
    if isinstance(artifact, MigrationArtifact):
        return artifact
    if isinstance(artifact, Mapping):
        return MigrationArtifact.from_dict(artifact)
    return MigrationArtifact.read(artifact)


def validate_contiguous_artifacts(artifacts: Sequence[MigrationArtifact]) -> None:
    for previous, current in zip(artifacts, artifacts[1:], strict=False):
        if previous.to_snapshot.to_dict() != current.from_snapshot.to_dict():
            raise ValueError(
                "migration artifacts are not contiguous: "
                f"{previous.revision} does not feed {current.revision}"
            )


def migration_files(path: str | PathLike[str], pattern: str | None) -> list[Path]:
    directory = Path(path)
    if pattern is None:
        return sorted({*directory.glob("*.json"), *directory.glob("*.toml")})
    return sorted(directory.glob(pattern))


def artifact_checksum(payload: Mapping[str, Any]) -> str:
    canonical = canonicalize_checksum_payload(payload)
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def plan_checksum(revision: str, plan: MigrationPlan) -> str:
    payload = {
        "revision": revision,
        "operations": [operation_to_dict(operation) for operation in plan.operations],
        "rollback_operations": [
            operation_to_dict(operation) for operation in plan.rollback_operations
        ],
        "diff": diff_to_dict(plan.diff),
        "warnings": [warning_to_dict(warning) for warning in plan.warnings],
        "safety": dict(plan.safety),
    }
    return artifact_checksum(payload)


def canonicalize_checksum_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if item is None:
                continue
            normalized[str(key)] = canonicalize_checksum_payload(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [canonicalize_checksum_payload(item) for item in value]
    return value


# Private aliases preserve the migration facade's historical helper names.
_artifact_checksum = artifact_checksum
_canonicalize_checksum_payload = canonicalize_checksum_payload
_change_from_dict = change_from_dict
_change_to_dict = change_to_dict
_coerce_artifact = coerce_artifact
_diff_from_dict = diff_from_dict
_diff_to_dict = diff_to_dict
_migration_files = migration_files
_normalize_change_details = normalize_change_details
_operation_from_dict = operation_from_dict
_operation_to_dict = operation_to_dict
_plan_checksum = plan_checksum
_validate_contiguous_artifacts = validate_contiguous_artifacts
_warning_from_dict = warning_from_dict
_warning_to_dict = warning_to_dict
