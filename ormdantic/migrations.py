"""Migration planning and execution facade."""

from __future__ import annotations

import importlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from fnmatch import fnmatch
from os import PathLike
from pathlib import Path
from typing import Any

from ormdantic import __version__

UTC = timezone.utc

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None

RuntimeCheck = tuple[str, str, str]
RuntimeColumn = tuple[
    str,
    str,
    bool,
    bool,
    str | None,
    str | None,
    int | None,
    bool,
    list[RuntimeCheck],
]
RuntimeIndex = tuple[str, list[str], bool]
RuntimeRelationship = tuple[str, str, str, str | None]
RuntimeTableSpec = tuple[
    str,
    str,
    str,
    list[RuntimeColumn],
    list[RuntimeIndex],
    list[list[str]],
    list[RuntimeRelationship],
]

MIGRATION_TABLE = "ormdantic_migrations"
MIGRATION_LOCK_NAME = "ormdantic:migration:lock"
MIGRATION_STATUS_APPLIED = "applied"
MIGRATION_STATUS_FAILED = "failed"
MIGRATION_STATUS_ROLLED_BACK = "rolled_back"
MIGRATION_ARTIFACT_VERSION = 2


@dataclass(frozen=True)
class ColumnSnapshot:
    """Serializable table column metadata used by migration snapshots."""

    name: str
    kind: str
    nullable: bool
    primary_key: bool
    foreign_table: str | None = None
    foreign_column: str | None = None
    max_length: int | None = None
    unique: bool = False
    checks: list[RuntimeCheck] = field(default_factory=list)

    @classmethod
    def from_runtime(cls, column: Sequence[Any]) -> "ColumnSnapshot":
        return cls(
            name=str(column[0]),
            kind=str(column[1]),
            nullable=bool(column[2]),
            primary_key=bool(column[3]),
            foreign_table=_optional_str(column[4]),
            foreign_column=_optional_str(column[5]),
            max_length=_optional_int(column[6]),
            unique=bool(column[7]),
            checks=[_runtime_check(check) for check in column[8]],
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ColumnSnapshot":
        return cls(
            name=str(payload["name"]),
            kind=str(payload["kind"]),
            nullable=bool(payload["nullable"]),
            primary_key=bool(payload["primary_key"]),
            foreign_table=_optional_str(payload.get("foreign_table")),
            foreign_column=_optional_str(payload.get("foreign_column")),
            max_length=_optional_int(payload.get("max_length")),
            unique=bool(payload.get("unique", False)),
            checks=[_runtime_check(check) for check in payload.get("checks", [])],
        )

    def to_runtime(self) -> RuntimeColumn:
        return (
            self.name,
            self.kind,
            self.nullable,
            self.primary_key,
            self.foreign_table,
            self.foreign_column,
            self.max_length,
            self.unique,
            list(self.checks),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "kind": self.kind,
            "nullable": self.nullable,
            "primary_key": self.primary_key,
            "foreign_table": self.foreign_table,
            "foreign_column": self.foreign_column,
            "max_length": self.max_length,
            "unique": self.unique,
            "checks": [list(check) for check in self.checks],
        }


@dataclass(frozen=True)
class IndexSnapshot:
    """Serializable index metadata used by migration snapshots."""

    name: str
    columns: list[str]
    unique: bool = False

    @classmethod
    def from_runtime(cls, index: Sequence[Any]) -> "IndexSnapshot":
        return cls(str(index[0]), [str(column) for column in index[1]], bool(index[2]))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "IndexSnapshot":
        return cls(
            str(payload["name"]),
            [str(column) for column in payload["columns"]],
            bool(payload.get("unique", False)),
        )

    def to_runtime(self) -> RuntimeIndex:
        return (self.name, list(self.columns), self.unique)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "columns": list(self.columns),
            "unique": self.unique,
        }


@dataclass(frozen=True)
class RelationshipSnapshot:
    """Serializable relationship metadata used by migration snapshots."""

    field: str
    foreign_table: str
    foreign_column: str
    back_reference: str | None = None

    @classmethod
    def from_runtime(cls, relationship: Sequence[Any]) -> "RelationshipSnapshot":
        return cls(
            str(relationship[0]),
            str(relationship[1]),
            str(relationship[2]),
            _optional_str(relationship[3]),
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "RelationshipSnapshot":
        return cls(
            str(payload["field"]),
            str(payload["foreign_table"]),
            str(payload["foreign_column"]),
            _optional_str(payload.get("back_reference")),
        )

    def to_runtime(self) -> RuntimeRelationship:
        return (
            self.field,
            self.foreign_table,
            self.foreign_column,
            self.back_reference,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "foreign_table": self.foreign_table,
            "foreign_column": self.foreign_column,
            "back_reference": self.back_reference,
        }


@dataclass(frozen=True)
class TableSnapshot:
    """Serializable table metadata used by migration snapshots."""

    model_key: str
    name: str
    primary_key: str
    columns: list[ColumnSnapshot] = field(default_factory=list)
    indexes: list[IndexSnapshot] = field(default_factory=list)
    unique_constraints: list[list[str]] = field(default_factory=list)
    relationships: list[RelationshipSnapshot] = field(default_factory=list)

    @classmethod
    def from_runtime(cls, table: Sequence[Any]) -> "TableSnapshot":
        return cls(
            model_key=str(table[0]),
            name=str(table[1]),
            primary_key=str(table[2]),
            columns=[ColumnSnapshot.from_runtime(column) for column in table[3]],
            indexes=[IndexSnapshot.from_runtime(index) for index in table[4]],
            unique_constraints=[
                [str(column) for column in columns] for columns in table[5]
            ],
            relationships=[
                RelationshipSnapshot.from_runtime(relationship)
                for relationship in table[6]
            ],
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TableSnapshot":
        return cls(
            model_key=str(payload["model_key"]),
            name=str(payload["name"]),
            primary_key=str(payload["primary_key"]),
            columns=[
                ColumnSnapshot.from_dict(column)
                for column in payload.get("columns", [])
            ],
            indexes=[
                IndexSnapshot.from_dict(index) for index in payload.get("indexes", [])
            ],
            unique_constraints=[
                [str(column) for column in columns]
                for columns in payload.get("unique_constraints", [])
            ],
            relationships=[
                RelationshipSnapshot.from_dict(relationship)
                for relationship in payload.get("relationships", [])
            ],
        )

    def to_runtime(self) -> RuntimeTableSpec:
        return (
            self.model_key,
            self.name,
            self.primary_key,
            [column.to_runtime() for column in self.columns],
            [index.to_runtime() for index in self.indexes],
            [list(columns) for columns in self.unique_constraints],
            [relationship.to_runtime() for relationship in self.relationships],
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_key": self.model_key,
            "name": self.name,
            "primary_key": self.primary_key,
            "columns": [column.to_dict() for column in self.columns],
            "indexes": [index.to_dict() for index in self.indexes],
            "unique_constraints": [
                list(columns) for columns in self.unique_constraints
            ],
            "relationships": [
                relationship.to_dict() for relationship in self.relationships
            ],
        }


@dataclass(frozen=True)
class SchemaSnapshot:
    """Serializable database schema snapshot."""

    tables: list[TableSnapshot] = field(default_factory=list)
    version: int = 1

    @classmethod
    def empty(cls) -> "SchemaSnapshot":
        return cls()

    @classmethod
    def from_runtime(cls, tables: Sequence[Sequence[Any]]) -> "SchemaSnapshot":
        return cls([TableSnapshot.from_runtime(table) for table in tables])

    @classmethod
    def from_database(cls, database: Any) -> "SchemaSnapshot":
        _prepare_database_relationships(database)
        return cls.from_runtime(database._runtime_table_specs())

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SchemaSnapshot":
        return cls(
            tables=[
                TableSnapshot.from_dict(table) for table in payload.get("tables", [])
            ],
            version=int(payload.get("version", 1)),
        )

    @classmethod
    def from_json(cls, payload: str | bytes | bytearray) -> "SchemaSnapshot":
        return cls.from_dict(json.loads(payload))

    @classmethod
    def from_toml(cls, payload: str | bytes | bytearray) -> "SchemaSnapshot":
        return cls.from_dict(_toml_loads(payload))

    @classmethod
    def read(
        cls, path: str | PathLike[str], *, format: str | None = None
    ) -> "SchemaSnapshot":
        document = Path(path).read_text()
        if _document_format(path, format) == "toml":
            return cls.from_toml(document)
        return cls.from_json(document)

    def to_runtime(self) -> list[RuntimeTableSpec]:
        return [table.to_runtime() for table in self.tables]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "tables": [table.to_dict() for table in self.tables],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    def to_toml(self) -> str:
        return _toml_dumps(self.to_dict())

    def write(self, path: str | PathLike[str], *, format: str | None = None) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        if _document_format(path, format) == "toml":
            output.write_text(self.to_toml())
        else:
            output.write_text(self.to_json())


@dataclass(frozen=True)
class MigrationChange:
    """A human-readable schema diff item."""

    action: str
    object_type: str
    table: str
    name: str
    message: str
    unsafe: bool = False
    destructive: bool = False
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MigrationWarning:
    """A migration safety warning."""

    code: str
    message: str
    table: str | None = None
    name: str | None = None


@dataclass(frozen=True)
class MigrationHistoryEntry:
    """One row from the durable migration history table."""

    revision: str
    description: str | None = None
    checksum: str | None = None
    applied_at: str | None = None
    execution_time_ms: int | None = None
    status: str = MIGRATION_STATUS_APPLIED
    dirty: bool = False
    artifact_version: int | None = None
    ormdantic_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SchemaDiff:
    """Structured schema diff output."""

    changes: list[MigrationChange] = field(default_factory=list)
    warnings: list[MigrationWarning] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.changes

    @property
    def destructive_changes(self) -> list[MigrationChange]:
        return [change for change in self.changes if change.destructive]

    @property
    def unsafe_changes(self) -> list[MigrationChange]:
        return [change for change in self.changes if change.unsafe]

    @property
    def has_unsafe_operations(self) -> bool:
        return bool(self.unsafe_changes)

    @property
    def has_destructive_operations(self) -> bool:
        return bool(self.destructive_changes)

    def summary(self) -> list[str]:
        return [change.message for change in self.changes]


@dataclass
class MigrationOperation:
    """A SQL migration operation."""

    sql: str
    values: tuple[Any, ...] = ()
    description: str | None = None
    unsafe: bool = False
    destructive: bool = False
    kind: str = "statement"
    table: str | None = None
    object_name: str | None = None
    reversible: bool = True
    requires_lock: bool = True
    requires_rebuild: bool = False
    generated_rollback: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MigrationPlan:
    """A generated migration plan."""

    operations: list[MigrationOperation] = field(default_factory=list)
    rollback_operations: list[MigrationOperation] = field(default_factory=list)
    diff: SchemaDiff = field(default_factory=SchemaDiff)
    warnings: list[MigrationWarning] = field(default_factory=list)
    safety: dict[str, Any] = field(default_factory=dict)

    def is_empty(self) -> bool:
        return not self.operations

    @property
    def rollback_available(self) -> bool:
        return bool(self.rollback_operations)

    @property
    def has_unsafe_operations(self) -> bool:
        return bool(
            self.diff.unsafe_changes or any(op.unsafe for op in self.operations)
        )

    @property
    def has_destructive_operations(self) -> bool:
        return bool(
            self.diff.destructive_changes
            or any(op.destructive for op in self.operations)
            or (
                self.diff.is_empty()
                and any(_operation_looks_destructive(op.sql) for op in self.operations)
            )
        )

    def dry_run(self) -> list[str]:
        return [operation.sql for operation in self.operations]

    def rollback_sql(self) -> list[str]:
        return [operation.sql for operation in self.rollback_operations]


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
    ) -> "MigrationArtifact":
        artifact = cls(
            revision=revision,
            from_snapshot=from_snapshot,
            to_snapshot=to_snapshot,
            operations=[
                _operation_from_dict(_operation_to_dict(operation))
                for operation in plan.operations
            ],
            rollback_operations=[
                _operation_from_dict(_operation_to_dict(operation))
                for operation in plan.rollback_operations
            ],
            diff=_diff_from_dict(_diff_to_dict(plan.diff)),
            warnings=[
                _warning_from_dict(_warning_to_dict(warning))
                for warning in plan.warnings
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
    def from_dict(cls, payload: Mapping[str, Any]) -> "MigrationArtifact":
        artifact_version = int(
            payload.get("artifact_version", payload.get("version", 1))
        )
        artifact = cls(
            revision=str(payload["revision"]),
            from_snapshot=SchemaSnapshot.from_dict(payload["from_snapshot"]),
            to_snapshot=SchemaSnapshot.from_dict(payload["to_snapshot"]),
            operations=[
                _operation_from_dict(operation)
                for operation in payload.get("up", payload.get("operations", []))
            ],
            rollback_operations=[
                _operation_from_dict(operation)
                for operation in payload.get(
                    "down", payload.get("rollback_operations", [])
                )
            ],
            diff=_diff_from_dict(payload.get("diff", {})),
            warnings=[
                _warning_from_dict(warning) for warning in payload.get("warnings", [])
            ],
            description=_optional_str(payload.get("description")),
            created_at=str(
                payload.get(
                    "created_at",
                    datetime.now(UTC).replace(microsecond=0).isoformat(),
                )
            ),
            dialect=_optional_str(payload.get("dialect")),
            checksum=_optional_str(payload.get("checksum")),
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
    def from_json(cls, payload: str | bytes | bytearray) -> "MigrationArtifact":
        return cls.from_dict(json.loads(payload))

    @classmethod
    def from_toml(cls, payload: str | bytes | bytearray) -> "MigrationArtifact":
        return cls.from_dict(_toml_loads(payload))

    @classmethod
    def read(
        cls, path: str | PathLike[str], *, format: str | None = None
    ) -> "MigrationArtifact":
        document = Path(path).read_text()
        if _document_format(path, format) == "toml":
            return cls.from_toml(document)
        return cls.from_json(document)

    def to_plan(self) -> MigrationPlan:
        return MigrationPlan(
            operations=[
                _operation_from_dict(_operation_to_dict(operation))
                for operation in self.operations
            ],
            rollback_operations=[
                _operation_from_dict(_operation_to_dict(operation))
                for operation in self.rollback_operations
            ],
            diff=_diff_from_dict(_diff_to_dict(self.diff)),
            warnings=[
                _warning_from_dict(_warning_to_dict(warning))
                for warning in self.warnings
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
            "up": [_operation_to_dict(operation) for operation in self.operations],
            "down": [
                _operation_to_dict(operation) for operation in self.rollback_operations
            ],
            "diff": _diff_to_dict(self.diff),
            "warnings": [_warning_to_dict(warning) for warning in self.warnings],
            "safety": dict(self.safety),
            "metadata": dict(self.metadata),
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    def to_toml(self) -> str:
        return _toml_dumps(self.to_dict())

    def write(self, path: str | PathLike[str], *, format: str | None = None) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        if _document_format(path, format) == "toml":
            output.write_text(self.to_toml())
        else:
            output.write_text(self.to_json())

    def with_checksum(self) -> "MigrationArtifact":
        payload = dict(self.to_dict())
        payload.pop("checksum", None)
        checksum = _artifact_checksum(payload)
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
        expected = _artifact_checksum(payload)
        if expected != self.checksum:
            raise ValueError(
                f"migration artifact checksum mismatch for revision {self.revision}: "
                f"expected {self.checksum}, calculated {expected}"
            )


class MigrationManager:
    """Generate, review, apply, and roll back SQL migration plans."""

    def __init__(self, database: Any) -> None:
        self._database = database

    def _resolve_snapshots(
        self,
        from_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        to_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
    ) -> tuple[SchemaSnapshot, SchemaSnapshot]:
        before = (
            _coerce_snapshot(from_snapshot)
            if from_snapshot is not None
            else SchemaSnapshot.empty()
        )
        after = (
            _coerce_snapshot(to_snapshot)
            if to_snapshot is not None
            else self.snapshot()
        )
        return before, after

    @property
    def _connection_url(self) -> str:
        return str(self._database._connection)

    def _dialect(self) -> str:
        return _dialect_name(self._connection_url)

    def _native_connection(self) -> Any:
        rust = _require_migration_symbol("PyNativeConnection")
        return rust.PyNativeConnection(self._connection_url)

    def snapshot(self) -> SchemaSnapshot:
        """Return a serializable snapshot for the currently registered models."""
        return SchemaSnapshot.from_database(self._database)

    def live_snapshot(
        self,
        *,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        schema: str | None = None,
    ) -> SchemaSnapshot:
        """Return a live schema snapshot reflected from the current database."""
        return _reflect_schema_snapshot(
            self._connection_url,
            dialect=self._dialect(),
            include_tables=include_tables,
            exclude_tables=exclude_tables,
            schema=schema,
        )

    def autogenerate(
        self,
        revision: str,
        *,
        dialect: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        schema: str | None = None,
        description: str | None = None,
        depends_on: Sequence[str] | None = None,
        branch_labels: Sequence[str] | None = None,
        path: str | PathLike[str] | None = None,
        skip_noop: bool = True,
    ) -> MigrationArtifact | None:
        """Generate a migration artifact by diffing live schema against models."""
        before = self.live_snapshot(
            include_tables=include_tables,
            exclude_tables=exclude_tables,
            schema=schema,
        )
        after = self.snapshot()
        active_dialect = dialect or self._connection_url
        plan = _build_plan(active_dialect, before, after)
        if plan.is_empty() and skip_noop:
            return None
        artifact = MigrationArtifact.from_plan(
            revision,
            plan,
            before,
            after,
            dialect=active_dialect,
            description=description,
            depends_on=depends_on,
            branch_labels=branch_labels,
            metadata={"autogenerated": True, "schema": schema},
        )
        if path is not None:
            artifact.write(path)
        return artifact

    def diff(
        self,
        from_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        to_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
    ) -> SchemaDiff:
        """Return structured schema changes between two snapshots."""
        before, after = self._resolve_snapshots(from_snapshot, to_snapshot)
        return diff_snapshots(before, after)

    def generate_plan(
        self,
        from_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        to_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        *,
        dialect: str | None = None,
    ) -> MigrationPlan:
        """Generate a migration plan and rollback SQL between two snapshots."""
        before, after = self._resolve_snapshots(from_snapshot, to_snapshot)
        return _build_plan(dialect or self._database._connection, before, after)

    def create_migration(
        self,
        revision: str,
        from_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        to_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        *,
        dialect: str | None = None,
        description: str | None = None,
        depends_on: Sequence[str] | None = None,
        branch_labels: Sequence[str] | None = None,
        path: str | PathLike[str] | None = None,
    ) -> MigrationArtifact:
        """Generate a serializable migration artifact."""
        before, after = self._resolve_snapshots(from_snapshot, to_snapshot)
        artifact = create_migration_artifact(
            revision,
            before,
            after,
            dialect=dialect or self._database._connection,
            description=description,
            depends_on=depends_on,
            branch_labels=branch_labels,
        )
        if path is not None:
            artifact.write(path)
        return artifact

    def dry_run(
        self,
        from_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        to_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        *,
        dialect: str | None = None,
    ) -> list[str]:
        """Return generated SQL without applying it."""
        return self.generate_plan(
            from_snapshot,
            to_snapshot,
            dialect=dialect,
        ).dry_run()

    async def ensure_revision_table(self) -> None:
        """Create/upgrade the migration history table when missing."""
        connection = self._native_connection()
        _ensure_migration_history_table(connection, self._dialect())

    async def applied_revisions(self) -> list[str]:
        """Return applied migration revisions ordered by apply time."""
        return [
            entry.revision
            for entry in await self.history()
            if entry.status == MIGRATION_STATUS_APPLIED and not entry.dirty
        ]

    async def history(self) -> list[MigrationHistoryEntry]:
        """Return migration history rows."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        return _history_entries(connection, dialect)

    async def current(self) -> MigrationHistoryEntry | None:
        """Return the latest successfully applied revision."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        return _current_entry(connection, dialect)

    async def is_dirty(self) -> bool:
        """Return whether migration history is currently marked dirty."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        return _is_dirty(connection, dialect)

    async def status(self) -> dict[str, Any]:
        """Return current migration status metadata."""
        current = await self.current()
        return {
            "dirty": await self.is_dirty(),
            "current": current.revision if current else None,
            "current_entry": current,
            "applied": await self.applied_revisions(),
        }

    async def repair(
        self,
        *,
        revision: str | None = None,
        status: str | None = None,
        clear_dirty: bool = True,
        checksum: str | None = None,
    ) -> int:
        """Repair migration metadata after a failed/manual migration."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        repaired = _repair_history(
            connection,
            dialect,
            revision=revision,
            status=status,
            clear_dirty=clear_dirty,
            checksum=checksum,
        )
        _commit_migration_history_if_needed(connection, dialect)
        return repaired

    async def apply_artifact(
        self,
        artifact: MigrationArtifact | Mapping[str, Any] | str | PathLike[str],
        *,
        allow_destructive: bool = False,
    ) -> bool:
        """Apply a migration artifact and record its revision."""
        migration = _coerce_artifact(artifact)
        migration.validate_checksum()
        return await self.apply(
            migration.revision,
            migration.to_plan(),
            allow_destructive=allow_destructive,
            checksum=migration.checksum,
            description=migration.description,
            artifact_version=migration.artifact_version,
            metadata=migration.metadata,
        )

    async def apply_file(
        self,
        path: str | PathLike[str],
        *,
        allow_destructive: bool = False,
    ) -> bool:
        """Apply a migration artifact from disk."""
        return await self.apply_artifact(
            MigrationArtifact.read(path),
            allow_destructive=allow_destructive,
        )

    async def apply_directory(
        self,
        path: str | PathLike[str],
        *,
        pattern: str | None = None,
        allow_destructive: bool = False,
    ) -> list[str]:
        """Apply migration artifacts in filename order."""
        if await self.is_dirty():
            raise ValueError(
                "migration history is dirty; run `ormdantic migrations repair` before apply-dir"
            )
        applied = []
        applied_set = set(await self.applied_revisions())
        for artifact_path in _migration_files(path, pattern):
            artifact = MigrationArtifact.read(artifact_path)
            missing_dependencies = [
                revision
                for revision in artifact.depends_on
                if revision not in applied_set
            ]
            if missing_dependencies:
                raise ValueError(
                    f"migration {artifact.revision} has missing dependencies: "
                    + ", ".join(missing_dependencies)
                )
            if await self.apply_artifact(
                artifact,
                allow_destructive=allow_destructive,
            ):
                applied.append(artifact.revision)
                applied_set.add(artifact.revision)
        return applied

    async def apply(
        self,
        revision: str,
        plan: MigrationPlan,
        *,
        allow_destructive: bool = False,
        checksum: str | None = None,
        description: str | None = None,
        artifact_version: int = MIGRATION_ARTIFACT_VERSION,
        metadata: Mapping[str, Any] | None = None,
    ) -> bool:
        """Apply a migration plan and record its revision.

        Returns ``False`` when the revision is already recorded.
        """
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        if _is_dirty(connection, dialect):
            raise ValueError(
                "database is marked dirty from a failed migration; run repair before applying"
            )
        existing = _history_entry(connection, dialect, revision)
        expected_checksum = checksum or _plan_checksum(revision, plan)
        if (
            existing is not None
            and existing.status == MIGRATION_STATUS_APPLIED
            and not existing.dirty
        ):
            if existing.checksum and existing.checksum != expected_checksum:
                raise ValueError(
                    f"revision {revision} already applied with checksum "
                    f"{existing.checksum}, requested {expected_checksum}"
                )
            return False
        if plan.has_destructive_operations and not allow_destructive:
            raise ValueError(
                "migration contains destructive operations; pass "
                "allow_destructive=True to apply it"
            )
        _raise_if_unsupported_sqlite_plan(dialect, plan.operations)
        _run_migration_operations(
            connection=connection,
            dialect=dialect,
            revision=revision,
            operations=plan.operations,
            status=MIGRATION_STATUS_APPLIED,
            description=description,
            checksum=expected_checksum,
            artifact_version=artifact_version,
            metadata=dict(metadata or {}),
        )
        return True

    async def rollback(
        self,
        revision: str,
        plan: MigrationPlan,
        *,
        allow_destructive: bool = False,
        checksum: str | None = None,
        description: str | None = None,
        artifact_version: int = MIGRATION_ARTIFACT_VERSION,
        metadata: Mapping[str, Any] | None = None,
    ) -> bool:
        """Run rollback SQL and remove a migration revision."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        existing = _history_entry(connection, dialect, revision)
        if existing is None or existing.status != MIGRATION_STATUS_APPLIED:
            return False
        if not plan.rollback_available:
            raise ValueError(
                "rollback SQL is unavailable for this migration; "
                "provide explicit down SQL or generated rollback operations"
            )
        rollback_plan = MigrationPlan(
            operations=list(plan.rollback_operations),
            diff=plan.diff,
            warnings=plan.warnings,
            safety=plan.safety,
        )
        if rollback_plan.has_destructive_operations and not allow_destructive:
            raise ValueError(
                "rollback contains destructive operations; pass "
                "allow_destructive=True to apply it"
            )
        _raise_if_unsupported_sqlite_plan(dialect, rollback_plan.operations)
        _run_migration_operations(
            connection=connection,
            dialect=dialect,
            revision=revision,
            operations=rollback_plan.operations,
            status=MIGRATION_STATUS_ROLLED_BACK,
            description=description or existing.description,
            checksum=checksum
            or existing.checksum
            or _plan_checksum(revision, rollback_plan),
            artifact_version=artifact_version,
            metadata=dict(metadata or {}),
        )
        return True

    async def rollback_artifact(
        self,
        artifact: MigrationArtifact | Mapping[str, Any] | str | PathLike[str],
        *,
        allow_destructive: bool = False,
    ) -> bool:
        """Roll back a migration artifact when rollback SQL is available."""
        migration = _coerce_artifact(artifact)
        return await self.rollback(
            migration.revision,
            migration.to_plan(),
            allow_destructive=allow_destructive,
            checksum=migration.checksum,
            description=migration.description,
            artifact_version=migration.artifact_version,
            metadata=migration.metadata,
        )

    async def rollback_file(
        self, path: str | PathLike[str], *, allow_destructive: bool = False
    ) -> bool:
        """Roll back a migration artifact from disk."""
        return await self.rollback_artifact(
            MigrationArtifact.read(path),
            allow_destructive=allow_destructive,
        )

    def squash(
        self,
        revision: str,
        artifacts: Sequence[
            MigrationArtifact | Mapping[str, Any] | str | PathLike[str]
        ],
        *,
        dialect: str | None = None,
        path: str | PathLike[str] | None = None,
    ) -> MigrationArtifact:
        """Squash contiguous migration artifacts into one net migration."""
        artifact = squash_migrations(
            revision,
            artifacts,
            dialect=dialect or self._database._connection,
        ).with_checksum()
        if path is not None:
            artifact.write(path)
        return artifact


def diff_snapshots(
    from_snapshot: SchemaSnapshot, to_snapshot: SchemaSnapshot
) -> SchemaDiff:
    """Return structured changes between schema snapshots."""
    changes: list[MigrationChange] = []
    from_tables = {table.name: table for table in from_snapshot.tables}
    to_tables = {table.name: table for table in to_snapshot.tables}

    for table in to_snapshot.tables:
        if table.name not in from_tables:
            changes.append(
                MigrationChange(
                    "add",
                    "table",
                    table.name,
                    table.name,
                    f"Added table {table.name}",
                )
            )

    for table in from_snapshot.tables:
        if table.name not in to_tables:
            changes.append(
                MigrationChange(
                    "remove",
                    "table",
                    table.name,
                    table.name,
                    f"Removed table {table.name}",
                    unsafe=True,
                    destructive=True,
                )
            )

    for table_name, before in from_tables.items():
        after = to_tables.get(table_name)
        if after is None:
            continue
        _diff_columns(changes, before, after)
        _diff_indexes(changes, before, after)
        _diff_constraints(changes, before, after)

    warnings = [_warning_for_change(change) for change in changes if change.unsafe]
    return SchemaDiff(changes, warnings)


def create_migration_artifact(
    revision: str,
    from_snapshot: SchemaSnapshot | Mapping[str, Any],
    to_snapshot: SchemaSnapshot | Mapping[str, Any],
    *,
    dialect: str,
    description: str | None = None,
    depends_on: Sequence[str] | None = None,
    branch_labels: Sequence[str] | None = None,
) -> MigrationArtifact:
    """Generate a migration artifact from two snapshots."""
    before = _coerce_snapshot(from_snapshot)
    after = _coerce_snapshot(to_snapshot)
    plan = _build_plan(dialect, before, after)
    return MigrationArtifact.from_plan(
        revision,
        plan,
        before,
        after,
        dialect=dialect,
        description=description,
        depends_on=depends_on,
        branch_labels=branch_labels,
    )


def read_migration(path: str | PathLike[str]) -> MigrationArtifact:
    """Read a migration artifact from disk."""
    return MigrationArtifact.read(path)


def squash_migrations(
    revision: str,
    artifacts: Sequence[MigrationArtifact | Mapping[str, Any] | str | PathLike[str]],
    *,
    dialect: str | None = None,
) -> MigrationArtifact:
    """Squash contiguous migration artifacts into one net migration."""
    migrations = [_coerce_artifact(artifact) for artifact in artifacts]
    if not migrations:
        raise ValueError("at least one migration artifact is required")
    _validate_contiguous_artifacts(migrations)
    migration_dialect = dialect or migrations[0].dialect
    if migration_dialect is None:
        raise ValueError("dialect is required when migration artifacts omit dialect")
    squashed = create_migration_artifact(
        revision,
        migrations[0].from_snapshot,
        migrations[-1].to_snapshot,
        dialect=migration_dialect,
    )
    covered_revisions = {migration.revision for migration in migrations}
    depends_on = [
        dependency
        for migration in migrations
        for dependency in migration.depends_on
        if dependency not in covered_revisions
    ]
    branch_labels = sorted(
        {label for migration in migrations for label in migration.branch_labels}
    )
    description = (
        "; ".join(filter(None, [migration.description for migration in migrations]))
        or f"squash {migrations[0].revision}..{migrations[-1].revision}"
    )
    return MigrationArtifact(
        revision=squashed.revision,
        from_snapshot=squashed.from_snapshot,
        to_snapshot=squashed.to_snapshot,
        operations=squashed.operations,
        rollback_operations=squashed.rollback_operations,
        diff=squashed.diff,
        warnings=squashed.warnings,
        description=description,
        created_at=squashed.created_at,
        dialect=squashed.dialect,
        checksum=squashed.checksum,
        depends_on=depends_on,
        branch_labels=branch_labels,
        safety=squashed.safety,
        metadata={
            "squashed_revisions": [migration.revision for migration in migrations],
        },
        artifact_version=MIGRATION_ARTIFACT_VERSION,
        version=MIGRATION_ARTIFACT_VERSION,
    ).with_checksum()


def _build_plan(
    dialect: str, from_snapshot: SchemaSnapshot, to_snapshot: SchemaSnapshot
) -> MigrationPlan:
    schema_diff = diff_snapshots(from_snapshot, to_snapshot)
    dialect_name = _dialect_name(dialect)
    compiled = _compile_schema_diff(
        dialect,
        from_snapshot,
        to_snapshot,
    )
    rollback_compiled = _compile_schema_diff(
        dialect,
        to_snapshot,
        from_snapshot,
    )
    warnings = list(schema_diff.warnings)
    operations = [_operation_from_compiled(item, dialect_name) for item in compiled]
    rollback_operations = [
        _operation_from_compiled(item, dialect_name, generated_rollback=True)
        for item in rollback_compiled
    ]
    requires_rebuild = any(op.requires_rebuild for op in operations)
    rollback_requires_rebuild = any(op.requires_rebuild for op in rollback_operations)
    if dialect_name == "sqlite":
        operations = _rewrite_sqlite_rebuild_operations(
            operations,
            from_snapshot,
            to_snapshot,
            destructive=schema_diff.has_destructive_operations,
            unsafe=schema_diff.has_unsafe_operations,
        )
        rollback_operations = _rewrite_sqlite_rebuild_operations(
            rollback_operations,
            to_snapshot,
            from_snapshot,
            destructive=schema_diff.has_destructive_operations,
            unsafe=schema_diff.has_unsafe_operations,
        )
    if schema_diff.has_destructive_operations:
        for operation in operations:
            operation.unsafe = True
            operation.destructive = True
    elif schema_diff.has_unsafe_operations:
        for operation in operations:
            operation.unsafe = True
    _raise_if_unsupported_sqlite_plan(dialect_name, operations)
    safety = {
        "dialect": dialect_name,
        "unsafe": schema_diff.has_unsafe_operations,
        "destructive": schema_diff.has_destructive_operations,
        "requires_rebuild": requires_rebuild,
        "rollback_requires_rebuild": rollback_requires_rebuild,
        "rollback_available": bool(rollback_operations),
    }
    return MigrationPlan(operations, rollback_operations, schema_diff, warnings, safety)


def _diff_columns(
    changes: list[MigrationChange], before: TableSnapshot, after: TableSnapshot
) -> None:
    before_columns = {column.name: column for column in before.columns}
    after_columns = {column.name: column for column in after.columns}

    for column in after.columns:
        if column.name not in before_columns:
            unsafe = not column.nullable and not column.primary_key
            changes.append(
                MigrationChange(
                    "add",
                    "column",
                    after.name,
                    column.name,
                    f"Added column {after.name}.{column.name}",
                    unsafe=unsafe,
                    details=column.to_dict(),
                )
            )

    for column in before.columns:
        if column.name not in after_columns:
            changes.append(
                MigrationChange(
                    "remove",
                    "column",
                    before.name,
                    column.name,
                    f"Removed column {before.name}.{column.name}",
                    unsafe=True,
                    destructive=True,
                    details=column.to_dict(),
                )
            )

    for column_name, old_column in before_columns.items():
        new_column = after_columns.get(column_name)
        if new_column is None or old_column == new_column:
            continue
        changed = _changed_column_fields(old_column, new_column)
        destructive = _is_destructive_column_change(changed)
        changes.append(
            MigrationChange(
                "change",
                "column",
                after.name,
                column_name,
                f"Changed column {after.name}.{column_name}: {', '.join(changed)}",
                unsafe=True,
                destructive=destructive,
                details={
                    "fields": changed,
                    "from": old_column.to_dict(),
                    "to": new_column.to_dict(),
                },
            )
        )


def _diff_indexes(
    changes: list[MigrationChange], before: TableSnapshot, after: TableSnapshot
) -> None:
    before_indexes = {index.name: index for index in before.indexes}
    after_indexes = {index.name: index for index in after.indexes}

    for index in after.indexes:
        if index.name not in before_indexes:
            changes.append(
                MigrationChange(
                    "add",
                    "index",
                    after.name,
                    index.name,
                    f"Added index {index.name} on {after.name}",
                    details=index.to_dict(),
                )
            )

    for index in before.indexes:
        if index.name not in after_indexes:
            changes.append(
                MigrationChange(
                    "remove",
                    "index",
                    before.name,
                    index.name,
                    f"Removed index {index.name} from {before.name}",
                    unsafe=index.unique,
                    details=index.to_dict(),
                )
            )

    for index_name, old_index in before_indexes.items():
        new_index = after_indexes.get(index_name)
        if new_index is None or old_index == new_index:
            continue
        changes.append(
            MigrationChange(
                "change",
                "index",
                after.name,
                index_name,
                f"Changed index {index_name} on {after.name}",
                unsafe=old_index.unique or new_index.unique,
                details={
                    "from": old_index.to_dict(),
                    "to": new_index.to_dict(),
                },
            )
        )


def _diff_constraints(
    changes: list[MigrationChange], before: TableSnapshot, after: TableSnapshot
) -> None:
    before_constraints = _constraints(before)
    after_constraints = _constraints(after)

    for name, constraint in after_constraints.items():
        if name not in before_constraints:
            changes.append(
                MigrationChange(
                    "add",
                    "constraint",
                    after.name,
                    name,
                    f"Added {constraint['kind']} constraint {name} on {after.name}",
                    details=constraint,
                )
            )

    for name, constraint in before_constraints.items():
        if name not in after_constraints:
            changes.append(
                MigrationChange(
                    "remove",
                    "constraint",
                    before.name,
                    name,
                    f"Removed {constraint['kind']} constraint {name} from {before.name}",
                    unsafe=True,
                    details=constraint,
                )
            )

    for name, old_constraint in before_constraints.items():
        new_constraint = after_constraints.get(name)
        if new_constraint is None or old_constraint == new_constraint:
            continue
        changes.append(
            MigrationChange(
                "change",
                "constraint",
                after.name,
                name,
                f"Changed {old_constraint['kind']} constraint {name} on {after.name}",
                unsafe=True,
                details={
                    "from": old_constraint,
                    "to": new_constraint,
                },
            )
        )


def _constraints(table: TableSnapshot) -> dict[str, dict[str, Any]]:
    constraints: dict[str, dict[str, Any]] = {}
    unique_constraints = list(table.unique_constraints)
    unique_constraints.extend(
        [column.name] for column in table.columns if column.unique
    )
    for idx, columns in enumerate(unique_constraints):
        name = f"{table.name}_unique_{idx}"
        constraints[name] = {
            "kind": "unique",
            "columns": list(columns),
        }
    for column in table.columns:
        for check in column.checks:
            suffix = _check_suffix(check)
            name = f"{table.name}_{column.name}_{suffix}_check"
            constraints[name] = {
                "kind": "check",
                "column": column.name,
                "check": list(check),
                "expression": _check_expression(column.name, check),
            }
        if column.foreign_table and column.foreign_column:
            name = f"{table.name}_{column.name}_foreign_key"
            constraints[name] = {
                "kind": "foreign_key",
                "columns": [column.name],
                "foreign_table": column.foreign_table,
                "foreign_columns": [column.foreign_column],
            }
    return constraints


def _changed_column_fields(
    old_column: ColumnSnapshot, new_column: ColumnSnapshot
) -> list[str]:
    fields = []
    for field_name in (
        "kind",
        "nullable",
        "primary_key",
        "foreign_table",
        "foreign_column",
        "max_length",
        "unique",
        "checks",
    ):
        if getattr(old_column, field_name) != getattr(new_column, field_name):
            fields.append(field_name)
    return fields


def _is_destructive_column_change(fields: Sequence[str]) -> bool:
    destructive_fields = {
        "kind",
        "nullable",
        "primary_key",
        "foreign_table",
        "foreign_column",
    }
    return any(field in destructive_fields for field in fields)


def _warning_for_change(change: MigrationChange) -> MigrationWarning:
    if change.destructive:
        code = f"destructive_{change.object_type}_{change.action}"
        message = f"{change.message}; this may delete or rewrite existing data"
    else:
        code = f"unsafe_{change.object_type}_{change.action}"
        message = f"{change.message}; review before applying"
    return MigrationWarning(code, message, change.table, change.name)


def _change_to_dict(change: MigrationChange) -> dict[str, Any]:
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


def _change_from_dict(payload: Mapping[str, Any]) -> MigrationChange:
    return MigrationChange(
        action=str(payload["action"]),
        object_type=str(payload["object_type"]),
        table=str(payload["table"]),
        name=str(payload["name"]),
        message=str(payload["message"]),
        unsafe=bool(payload.get("unsafe", False)),
        destructive=bool(payload.get("destructive", False)),
        details=_normalize_change_details(dict(payload.get("details", {}))),
    )


def _normalize_change_details(details: dict[str, Any]) -> dict[str, Any]:
    if {"name", "kind", "nullable", "primary_key"} <= set(details):
        details.setdefault("foreign_table", None)
        details.setdefault("foreign_column", None)
        details.setdefault("max_length", None)
    for key in ("from", "to"):
        value = details.get(key)
        if isinstance(value, Mapping):
            details[key] = _normalize_change_details(dict(value))
    return details


def _warning_to_dict(warning: MigrationWarning) -> dict[str, Any]:
    return {
        "code": warning.code,
        "message": warning.message,
        "table": warning.table,
        "name": warning.name,
    }


def _warning_from_dict(payload: Mapping[str, Any]) -> MigrationWarning:
    return MigrationWarning(
        code=str(payload["code"]),
        message=str(payload["message"]),
        table=_optional_str(payload.get("table")),
        name=_optional_str(payload.get("name")),
    )


def _diff_to_dict(diff: SchemaDiff) -> dict[str, Any]:
    return {
        "changes": [_change_to_dict(change) for change in diff.changes],
        "warnings": [_warning_to_dict(warning) for warning in diff.warnings],
    }


def _diff_from_dict(payload: Mapping[str, Any]) -> SchemaDiff:
    return SchemaDiff(
        changes=[_change_from_dict(change) for change in payload.get("changes", [])],
        warnings=[
            _warning_from_dict(warning) for warning in payload.get("warnings", [])
        ],
    )


def _operation_to_dict(operation: MigrationOperation) -> dict[str, Any]:
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


def _operation_from_dict(payload: Mapping[str, Any]) -> MigrationOperation:
    return MigrationOperation(
        sql=str(payload["sql"]),
        values=tuple(payload.get("values", ())),
        description=_optional_str(payload.get("description")),
        unsafe=bool(payload.get("unsafe", False)),
        destructive=bool(payload.get("destructive", False)),
        kind=str(payload.get("kind", "statement")),
        table=_optional_str(payload.get("table")),
        object_name=_optional_str(payload.get("object_name")),
        reversible=bool(payload.get("reversible", True)),
        requires_lock=bool(payload.get("requires_lock", True)),
        requires_rebuild=bool(payload.get("requires_rebuild", False)),
        generated_rollback=bool(payload.get("generated_rollback", False)),
        metadata=dict(payload.get("metadata", {})),
    )


def _coerce_artifact(
    artifact: MigrationArtifact | Mapping[str, Any] | str | PathLike[str],
) -> MigrationArtifact:
    if isinstance(artifact, MigrationArtifact):
        return artifact
    if isinstance(artifact, Mapping):
        return MigrationArtifact.from_dict(artifact)
    return MigrationArtifact.read(artifact)


def _validate_contiguous_artifacts(artifacts: Sequence[MigrationArtifact]) -> None:
    for previous, current in zip(artifacts, artifacts[1:], strict=False):
        if previous.to_snapshot.to_dict() != current.from_snapshot.to_dict():
            raise ValueError(
                "migration artifacts are not contiguous: "
                f"{previous.revision} does not feed {current.revision}"
            )


def _migration_files(path: str | PathLike[str], pattern: str | None) -> list[Path]:
    directory = Path(path)
    if pattern is None:
        return sorted({*directory.glob("*.json"), *directory.glob("*.toml")})
    return sorted(directory.glob(pattern))


def _artifact_checksum(payload: Mapping[str, Any]) -> str:
    canonical = _canonicalize_checksum_payload(payload)
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    import hashlib

    return hashlib.sha256(encoded).hexdigest()


def _plan_checksum(revision: str, plan: MigrationPlan) -> str:
    payload = {
        "revision": revision,
        "operations": [_operation_to_dict(operation) for operation in plan.operations],
        "rollback_operations": [
            _operation_to_dict(operation) for operation in plan.rollback_operations
        ],
        "diff": _diff_to_dict(plan.diff),
        "warnings": [_warning_to_dict(warning) for warning in plan.warnings],
        "safety": dict(plan.safety),
    }
    return _artifact_checksum(payload)


def _canonicalize_checksum_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if item is None:
                continue
            normalized[str(key)] = _canonicalize_checksum_payload(item)
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonicalize_checksum_payload(item) for item in value]
    return value


def _dialect_name(value: str) -> str:
    normalized = value.strip().lower()
    if "://" in normalized:
        normalized = normalized.split("://", 1)[0]
    if "+" in normalized:
        normalized = normalized.split("+", 1)[0]
    aliases = {
        "postgres": "postgresql",
        "postgresql": "postgresql",
        "psql": "postgresql",
        "sqlite": "sqlite",
        "mysql": "mysql",
        "mariadb": "mariadb",
        "mssql": "mssql",
        "sqlserver": "mssql",
        "oracle": "oracle",
    }
    return aliases.get(normalized, normalized)


def _operation_from_compiled(
    item: Mapping[str, Any],
    dialect: str,
    *,
    generated_rollback: bool = False,
) -> MigrationOperation:
    sql = str(item["sql"])
    metadata = _classify_sql_operation(sql)
    return MigrationOperation(
        sql=sql,
        values=tuple(item.get("params", ())),
        kind=str(metadata["kind"]),
        table=_optional_str(metadata.get("table")),
        object_name=_optional_str(metadata.get("object_name")),
        reversible=bool(metadata.get("reversible", True)),
        requires_lock=True,
        requires_rebuild=bool(metadata.get("requires_rebuild", False)),
        generated_rollback=generated_rollback,
        metadata={
            "dialect": dialect,
            "classification": metadata,
        },
    )


def _rewrite_sqlite_rebuild_operations(
    operations: list[MigrationOperation],
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
    *,
    destructive: bool,
    unsafe: bool,
) -> list[MigrationOperation]:
    affected_tables = [
        table
        for table in dict.fromkeys(
            operation.table for operation in operations if operation.requires_rebuild
        )
        if table
    ]
    if not affected_tables:
        return operations

    rewritten: list[MigrationOperation] = []
    emitted_rebuilds: set[str] = set()
    for operation in operations:
        if operation.table in affected_tables:
            if operation.table not in emitted_rebuilds:
                rewritten.extend(
                    _sqlite_rebuild_table_operations(
                        operation.table,
                        from_snapshot,
                        to_snapshot,
                        destructive=destructive,
                        unsafe=unsafe,
                    )
                )
                emitted_rebuilds.add(operation.table)
            continue
        rewritten.append(operation)
    return rewritten


def _sqlite_rebuild_table_operations(
    table_name: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
    *,
    destructive: bool,
    unsafe: bool,
) -> list[MigrationOperation]:
    before = {table.name: table for table in from_snapshot.tables}.get(table_name)
    after = {table.name: table for table in to_snapshot.tables}.get(table_name)
    if before is None or after is None:
        raise ValueError(f"cannot rebuild unknown sqlite table '{table_name}'")

    temp_name = _sqlite_rebuild_table_name(table_name)
    temp_table = _table_snapshot_with_name(after, temp_name, indexes=[])
    temp_create = _compile_table_create_sql("sqlite", temp_table)[0]
    final_index_sql = _compile_table_create_sql("sqlite", after)[1:]
    common_columns = [
        column.name
        for column in after.columns
        if any(column.name == old_column.name for old_column in before.columns)
    ]
    operations = [
        MigrationOperation(
            f"DROP TABLE IF EXISTS {_quote_ident('sqlite', temp_name)}",
            description=f"drop stale rebuild table for {table_name}",
            unsafe=True,
            destructive=False,
            kind="sqlite_rebuild_table",
            table=table_name,
            object_name=temp_name,
            metadata={"sqlite_rebuild": True, "phase": "drop_temp"},
        ),
        MigrationOperation(
            temp_create,
            description=f"create rebuild table for {table_name}",
            unsafe=unsafe,
            destructive=False,
            kind="sqlite_rebuild_table",
            table=table_name,
            object_name=temp_name,
            metadata={"sqlite_rebuild": True, "phase": "create_temp"},
        ),
    ]
    if common_columns:
        selected = ", ".join(
            _quote_ident("sqlite", column) for column in common_columns
        )
        operations.append(
            MigrationOperation(
                f"INSERT INTO {_quote_ident('sqlite', temp_name)} ({selected}) "
                f"SELECT {selected} FROM {_quote_ident('sqlite', table_name)}",
                description=f"copy rows for sqlite rebuild of {table_name}",
                unsafe=True,
                destructive=False,
                kind="sqlite_rebuild_table",
                table=table_name,
                object_name=temp_name,
                metadata={
                    "sqlite_rebuild": True,
                    "phase": "copy_rows",
                    "columns": list(common_columns),
                },
            )
        )
    operations.extend(
        [
            MigrationOperation(
                f"DROP TABLE {_quote_ident('sqlite', table_name)}",
                description=f"drop old table for sqlite rebuild of {table_name}",
                unsafe=True,
                destructive=destructive,
                kind="sqlite_rebuild_table",
                table=table_name,
                object_name=table_name,
                metadata={"sqlite_rebuild": True, "phase": "drop_old"},
            ),
            MigrationOperation(
                f"ALTER TABLE {_quote_ident('sqlite', temp_name)} "
                f"RENAME TO {_quote_ident('sqlite', table_name)}",
                description=f"rename rebuilt table {table_name}",
                unsafe=unsafe,
                destructive=False,
                kind="sqlite_rebuild_table",
                table=table_name,
                object_name=temp_name,
                metadata={"sqlite_rebuild": True, "phase": "rename"},
            ),
        ]
    )
    operations.extend(
        MigrationOperation(
            sql,
            description=f"recreate index for sqlite rebuild of {table_name}",
            unsafe=unsafe,
            destructive=False,
            kind="sqlite_rebuild_table",
            table=table_name,
            metadata={"sqlite_rebuild": True, "phase": "create_index"},
        )
        for sql in final_index_sql
    )
    return operations


def _sqlite_rebuild_table_name(table_name: str) -> str:
    safe = "".join(
        char if char.isalnum() or char == "_" else "_" for char in table_name
    )
    return f"__ormdantic_rebuild_{safe}"


def _table_snapshot_with_name(
    table: TableSnapshot,
    name: str,
    *,
    indexes: Sequence[IndexSnapshot] | None = None,
) -> TableSnapshot:
    return TableSnapshot(
        model_key=table.model_key,
        name=name,
        primary_key=table.primary_key,
        columns=list(table.columns),
        indexes=list(table.indexes if indexes is None else indexes),
        unique_constraints=[list(columns) for columns in table.unique_constraints],
        relationships=list(table.relationships),
    )


def _compile_table_create_sql(dialect: str, table: TableSnapshot) -> list[str]:
    compiled = _compile_schema_diff(
        dialect,
        SchemaSnapshot.empty(),
        SchemaSnapshot(tables=[table]),
    )
    return [str(item["sql"]) for item in compiled]


def _classify_sql_operation(sql: str) -> dict[str, Any]:
    normalized = " ".join(sql.strip().split())
    upper = normalized.upper()
    kind = "statement"
    table: str | None = None
    object_name: str | None = None
    requires_rebuild = False
    reversible = True
    destructive = False
    unsafe = False

    if upper.startswith("CREATE TABLE"):
        kind = "create_table"
        table = _sql_identifier_after(normalized, "CREATE TABLE")
    elif upper.startswith("DROP TABLE"):
        kind = "drop_table"
        table = _sql_identifier_after(normalized, "DROP TABLE")
        destructive = True
        unsafe = True
    elif upper.startswith("ALTER TABLE"):
        kind = "alter_table"
        table = _sql_identifier_after(normalized, "ALTER TABLE")
        unsafe = True
        if " DROP COLUMN " in upper or " DROP CONSTRAINT " in upper:
            destructive = True
        if any(
            clause in upper
            for clause in (
                " ADD CONSTRAINT ",
                " DROP CONSTRAINT ",
                " ALTER COLUMN ",
                " DROP COLUMN ",
            )
        ):
            requires_rebuild = True
    elif upper.startswith("CREATE INDEX") or upper.startswith("CREATE UNIQUE INDEX"):
        kind = "create_index"
        table = _sql_identifier_after_keyword(normalized, " ON ")
        unsafe = "UNIQUE" in upper
    elif upper.startswith("DROP INDEX"):
        kind = "drop_index"
        unsafe = True
    elif upper.startswith("INSERT INTO"):
        kind = "insert"
        table = _sql_identifier_after(normalized, "INSERT INTO")
        reversible = False
        unsafe = True
    elif upper.startswith("UPDATE"):
        kind = "update"
        table = _sql_identifier_after(normalized, "UPDATE")
        reversible = False
        unsafe = True
    elif upper.startswith("DELETE FROM"):
        kind = "delete"
        table = _sql_identifier_after(normalized, "DELETE FROM")
        reversible = False
        destructive = True
        unsafe = True

    return {
        "kind": kind,
        "table": table,
        "object_name": object_name,
        "requires_rebuild": requires_rebuild,
        "reversible": reversible,
        "destructive": destructive,
        "unsafe": unsafe,
    }


def _sql_identifier_after(statement: str, prefix: str) -> str | None:
    remainder = statement[len(prefix) :].strip()
    if remainder.startswith("IF EXISTS"):
        remainder = remainder[len("IF EXISTS") :].strip()
    if remainder.startswith("IF NOT EXISTS"):
        remainder = remainder[len("IF NOT EXISTS") :].strip()
    if not remainder:
        return None
    token = remainder.split(" ", 1)[0].strip()
    return token.strip('`"[]')


def _sql_identifier_after_keyword(statement: str, keyword: str) -> str | None:
    marker = statement.upper().find(keyword)
    if marker < 0:
        return None
    remainder = statement[marker + len(keyword) :].strip()
    if not remainder:
        return None
    token = remainder.split(" ", 1)[0].strip()
    return token.strip('`"[]')


def _raise_if_unsupported_sqlite_plan(
    dialect: str, operations: Sequence[MigrationOperation]
) -> None:
    if _dialect_name(dialect) != "sqlite":
        return
    blocked = [operation for operation in operations if operation.requires_rebuild]
    if not blocked:
        return
    snippets = ", ".join(operation.sql for operation in blocked[:3])
    if len(blocked) > 3:
        snippets += ", ..."
    raise ValueError(
        "sqlite migration includes unresolved operations that require a table rebuild: "
        f"{snippets}"
    )


def _operation_payload(plan: MigrationPlan) -> list[tuple[str, tuple[Any, ...]]]:
    return [(operation.sql, operation.values) for operation in plan.operations]


def _quote_ident(dialect: str, identifier: str) -> str:
    name = identifier.replace("\x00", "")
    dialect_name = _dialect_name(dialect)
    if dialect_name in {"mysql", "mariadb"}:
        return f"`{name.replace('`', '``')}`"
    if dialect_name == "mssql":
        return f"[{name.replace(']', ']]')}]"
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, Mapping | list | tuple):
        value = json.dumps(value, sort_keys=True)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _query_rows(
    connection: Any, sql: str, params: Sequence[Any] | None = None
) -> list[list[Any]]:
    result = connection.execute(sql, list(params or ()))
    if not isinstance(result, Mapping):
        return []
    rows = result.get("rows", [])
    if not isinstance(rows, Sequence):
        return []
    return [list(row) for row in rows if isinstance(row, Sequence)]


def _query_rows_url(rust_module: Any, url: str, sql: str) -> list[list[Any]]:
    result = rust_module.execute_native(url, sql, [])
    if not isinstance(result, Mapping):
        return []
    rows = result.get("rows", [])
    if not isinstance(rows, Sequence):
        return []
    return [list(row) for row in rows if isinstance(row, Sequence)]


def _query_scalar(
    connection: Any, sql: str, params: Sequence[Any] | None = None
) -> Any:
    rows = _query_rows(connection, sql, params)
    if not rows:
        return None
    if not rows[0]:
        return None
    return rows[0][0]


def _db_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "t", "true", "yes", "y"}


def _migration_table_column_defs(dialect: str) -> list[tuple[str, str, str | None]]:
    dialect_name = _dialect_name(dialect)
    text_type = "TEXT"
    short_text_type = text_type
    revision_type = f"{text_type} PRIMARY KEY"
    integer_type = "INTEGER"
    metadata_type = text_type
    if dialect_name in {"mysql", "mariadb"}:
        short_text_type = "VARCHAR(255)"
        revision_type = "VARCHAR(255) PRIMARY KEY"
        metadata_type = "TEXT"
    elif dialect_name == "mssql":
        text_type = "NVARCHAR(2048)"
        short_text_type = "NVARCHAR(255)"
        revision_type = "NVARCHAR(255) PRIMARY KEY"
        integer_type = "BIGINT"
        metadata_type = "NVARCHAR(MAX)"
    elif dialect_name == "oracle":
        text_type = "VARCHAR2(2048)"
        short_text_type = "VARCHAR2(255)"
        revision_type = "VARCHAR2(255) PRIMARY KEY"
        integer_type = "NUMBER(19)"
        metadata_type = "VARCHAR2(4000)"
    return [
        ("revision", revision_type, None),
        ("description", text_type, None),
        ("checksum", short_text_type, None),
        ("applied_at", short_text_type, None),
        ("execution_time_ms", integer_type, None),
        ("status", short_text_type, _sql_literal(MIGRATION_STATUS_APPLIED)),
        ("dirty", integer_type, "0"),
        ("artifact_version", integer_type, None),
        ("ormdantic_version", short_text_type, None),
        ("metadata", metadata_type, None),
    ]


def _ensure_migration_history_table(connection: Any, dialect: str) -> None:
    table = _quote_ident(dialect, MIGRATION_TABLE)
    columns = _migration_table_column_defs(dialect)
    create_columns = ", ".join(
        f"{_quote_ident(dialect, name)} {column_type}"
        + (f" DEFAULT {default}" if default is not None else "")
        for name, column_type, default in columns
    )
    existed = _migration_history_table_exists(connection, dialect)
    if not existed:
        try:
            connection.execute(f"CREATE TABLE {table} ({create_columns})", [])
        except Exception as exc:
            if not _is_duplicate_table_error(exc):
                raise
            existed = True
    if existed:
        for name, column_type, default in columns[1:]:
            if _migration_history_column_exists(connection, dialect, name):
                continue
            statement = _add_migration_history_column_sql(
                dialect,
                table,
                name,
                column_type,
                default,
            )
            try:
                connection.execute(statement, [])
            except Exception as exc:
                if not _is_duplicate_column_error(exc):
                    raise
    connection.execute(
        f"UPDATE {table} SET {_quote_ident(dialect, 'status')} = "
        f"{_sql_literal(MIGRATION_STATUS_APPLIED)} WHERE {_quote_ident(dialect, 'status')} IS NULL",
        [],
    )
    connection.execute(
        f"UPDATE {table} SET {_quote_ident(dialect, 'dirty')} = 0 "
        f"WHERE {_quote_ident(dialect, 'dirty')} IS NULL",
        [],
    )
    connection.execute(
        f"UPDATE {table} SET {_quote_ident(dialect, 'artifact_version')} = "
        f"{MIGRATION_ARTIFACT_VERSION} WHERE {_quote_ident(dialect, 'artifact_version')} IS NULL",
        [],
    )
    _commit_migration_history_if_needed(connection, dialect)


def _migration_history_table_exists(connection: Any, dialect: str) -> bool:
    dialect_name = _dialect_name(dialect)
    table_name = _sql_literal(MIGRATION_TABLE)
    if dialect_name == "sqlite":
        sql = (
            "SELECT COUNT(*) FROM sqlite_master "
            f"WHERE type = 'table' AND name = {table_name}"
        )
    elif dialect_name == "postgresql":
        sql = (
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = current_schema() AND table_name = {table_name}"
        )
    elif dialect_name in {"mysql", "mariadb"}:
        sql = (
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = DATABASE() AND table_name = {table_name}"
        )
    elif dialect_name == "mssql":
        sql = f"SELECT COUNT(*) FROM sys.tables WHERE name = {table_name}"
    elif dialect_name == "oracle":
        sql = f"SELECT COUNT(*) FROM user_tables WHERE table_name = {table_name}"
    else:
        return False
    value = _query_scalar(connection, sql)
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return _db_truthy(value)


def _add_migration_history_column_sql(
    dialect: str,
    table: str,
    name: str,
    column_type: str,
    default: str | None,
) -> str:
    column_def = f"{_quote_ident(dialect, name)} {column_type}"
    if default is not None:
        column_def += f" DEFAULT {default}"
    dialect_name = _dialect_name(dialect)
    if dialect_name == "mssql":
        return f"ALTER TABLE {table} ADD {column_def}"
    if dialect_name == "oracle":
        return f"ALTER TABLE {table} ADD ({column_def})"
    return f"ALTER TABLE {table} ADD COLUMN {column_def}"


def _migration_history_column_exists(
    connection: Any,
    dialect: str,
    column: str,
) -> bool:
    dialect_name = _dialect_name(dialect)
    table_name = _sql_literal(MIGRATION_TABLE)
    column_name = _sql_literal(column)
    if dialect_name == "sqlite":
        sql = (
            "SELECT COUNT(*) FROM "
            f"pragma_table_info({table_name}) WHERE name = {column_name}"
        )
    elif dialect_name == "postgresql":
        sql = (
            "SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema = current_schema() AND table_name = {table_name} "
            f"AND column_name = {column_name}"
        )
    elif dialect_name in {"mysql", "mariadb"}:
        sql = (
            "SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema = DATABASE() AND table_name = {table_name} "
            f"AND column_name = {column_name}"
        )
    elif dialect_name == "mssql":
        sql = (
            "SELECT COUNT(*) FROM sys.columns "
            f"WHERE object_id = OBJECT_ID(N{table_name}) AND name = {column_name}"
        )
    elif dialect_name == "oracle":
        sql = (
            "SELECT COUNT(*) FROM user_tab_columns "
            f"WHERE table_name = {table_name} AND column_name = {column_name}"
        )
    else:
        return False
    value = _query_scalar(connection, sql)
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return _db_truthy(value)


def _is_duplicate_table_error(error: Exception) -> bool:
    message = str(error).lower()
    return (
        ("already exists" in message and ("table" in message or "object" in message))
        or "already an object named" in message
        or "ora-00955" in message
        or "name is already used by an existing object" in message
    )


def _is_duplicate_column_error(error: Exception) -> bool:
    message = str(error).lower()
    return (
        ("duplicate" in message and "column" in message)
        or ("already exists" in message and "column" in message)
        or "ora-01430" in message
        or ("column" in message and "must be unique" in message)
        or ("column" in message and "specified more than once" in message)
    )


def _history_entries(connection: Any, dialect: str) -> list[MigrationHistoryEntry]:
    table = _quote_ident(dialect, MIGRATION_TABLE)
    columns = [
        "revision",
        "description",
        "checksum",
        "applied_at",
        "execution_time_ms",
        "status",
        "dirty",
        "artifact_version",
        "ormdantic_version",
        "metadata",
    ]
    selected = ", ".join(_quote_ident(dialect, name) for name in columns)
    rows = _query_rows(
        connection,
        f"SELECT {selected} FROM {table} ORDER BY {_quote_ident(dialect, 'applied_at')}, "
        f"{_quote_ident(dialect, 'revision')}",
    )
    history: list[MigrationHistoryEntry] = []
    for row in rows:
        metadata: dict[str, Any] = {}
        if len(row) >= 10 and row[9]:
            try:
                metadata = dict(json.loads(str(row[9])))
            except Exception:
                metadata = {"raw": str(row[9])}
        history.append(
            MigrationHistoryEntry(
                revision=str(row[0]),
                description=_optional_str(row[1] if len(row) > 1 else None),
                checksum=_optional_str(row[2] if len(row) > 2 else None),
                applied_at=_optional_str(row[3] if len(row) > 3 else None),
                execution_time_ms=_optional_int(row[4] if len(row) > 4 else None),
                status=str(
                    row[5] if len(row) > 5 and row[5] else MIGRATION_STATUS_APPLIED
                ),
                dirty=_db_truthy(row[6] if len(row) > 6 else None),
                artifact_version=_optional_int(row[7] if len(row) > 7 else None),
                ormdantic_version=_optional_str(row[8] if len(row) > 8 else None),
                metadata=metadata,
            )
        )
    return history


def _history_entry(
    connection: Any, dialect: str, revision: str
) -> MigrationHistoryEntry | None:
    for entry in _history_entries(connection, dialect):
        if entry.revision == revision:
            return entry
    return None


def _current_entry(connection: Any, dialect: str) -> MigrationHistoryEntry | None:
    for entry in reversed(_history_entries(connection, dialect)):
        if entry.status == MIGRATION_STATUS_APPLIED and not entry.dirty:
            return entry
    return None


def _is_dirty(connection: Any, dialect: str) -> bool:
    return any(entry.dirty for entry in _history_entries(connection, dialect))


def _repair_history(
    connection: Any,
    dialect: str,
    *,
    revision: str | None,
    status: str | None,
    clear_dirty: bool,
    checksum: str | None,
) -> int:
    entries = _history_entries(connection, dialect)
    updated = 0
    for entry in entries:
        if revision is not None and entry.revision != revision:
            continue
        payload = MigrationHistoryEntry(
            revision=entry.revision,
            description=entry.description,
            checksum=checksum if checksum is not None else entry.checksum,
            applied_at=entry.applied_at,
            execution_time_ms=entry.execution_time_ms,
            status=status or entry.status,
            dirty=False if clear_dirty else entry.dirty,
            artifact_version=entry.artifact_version,
            ormdantic_version=entry.ormdantic_version,
            metadata=entry.metadata,
        )
        _write_history_entry(connection, dialect, payload)
        updated += 1
    return updated


def _write_history_entry(
    connection: Any, dialect: str, entry: MigrationHistoryEntry
) -> None:
    table = _quote_ident(dialect, MIGRATION_TABLE)
    revision_column = _quote_ident(dialect, "revision")
    connection.execute(
        f"DELETE FROM {table} WHERE {revision_column} = {_sql_literal(entry.revision)}",
        [],
    )
    columns = [
        "revision",
        "description",
        "checksum",
        "applied_at",
        "execution_time_ms",
        "status",
        "dirty",
        "artifact_version",
        "ormdantic_version",
        "metadata",
    ]
    values = [
        entry.revision,
        entry.description,
        entry.checksum,
        entry.applied_at,
        entry.execution_time_ms,
        entry.status,
        entry.dirty,
        entry.artifact_version,
        entry.ormdantic_version,
        entry.metadata or None,
    ]
    rendered_values = ", ".join(_sql_literal(value) for value in values)
    rendered_columns = ", ".join(_quote_ident(dialect, column) for column in columns)
    connection.execute(
        f"INSERT INTO {table} ({rendered_columns}) VALUES ({rendered_values})",
        [],
    )


def _dialect_supports_transactional_ddl(dialect: str) -> bool:
    return _dialect_name(dialect) in {"sqlite", "postgresql"}


def _acquire_migration_lock(connection: Any, dialect: str) -> str | None:
    dialect_name = _dialect_name(dialect)
    if dialect_name == "postgresql":
        acquired = _query_scalar(
            connection,
            "SELECT pg_try_advisory_lock(hashtext('ormdantic_migration_lock'))",
        )
        if not _db_truthy(acquired):
            raise ValueError(
                "failed to acquire postgres advisory migration lock; another migration may be running"
            )
        return "SELECT pg_advisory_unlock(hashtext('ormdantic_migration_lock'))"
    if dialect_name in {"mysql", "mariadb"}:
        acquired = _query_scalar(
            connection,
            f"SELECT GET_LOCK({_sql_literal(MIGRATION_LOCK_NAME)}, 30)",
        )
        if not _db_truthy(acquired):
            raise ValueError(
                "failed to acquire mysql migration lock; another migration may be running"
            )
        return f"SELECT RELEASE_LOCK({_sql_literal(MIGRATION_LOCK_NAME)})"
    if dialect_name == "mssql":
        connection.execute(
            "EXEC sp_getapplock @Resource = 'ormdantic_migration_lock', "
            "@LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 30000",
            [],
        )
        return (
            "EXEC sp_releaseapplock @Resource = 'ormdantic_migration_lock', "
            "@LockOwner = 'Session'"
        )
    return None


def _run_migration_operations(
    *,
    connection: Any,
    dialect: str,
    revision: str,
    operations: Sequence[MigrationOperation],
    status: str,
    description: str | None,
    checksum: str | None,
    artifact_version: int,
    metadata: Mapping[str, Any],
) -> None:
    _ensure_migration_history_table(connection, dialect)
    transaction_open = False
    release_lock_sql = _acquire_migration_lock(connection, dialect)
    start = time.perf_counter()
    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    try:
        if _dialect_supports_transactional_ddl(dialect):
            if _dialect_name(dialect) == "sqlite":
                connection.execute("BEGIN IMMEDIATE", [])
            else:
                connection.begin()
            transaction_open = True
        pending = MigrationHistoryEntry(
            revision=revision,
            description=description,
            checksum=checksum,
            applied_at=now,
            execution_time_ms=None,
            status=status,
            dirty=True,
            artifact_version=artifact_version,
            ormdantic_version=__version__,
            metadata={
                "phase": "running",
                "operation_count": len(operations),
                **dict(metadata),
            },
        )
        _write_history_entry(connection, dialect, pending)
        for operation in operations:
            connection.execute(operation.sql, list(operation.values))
        if transaction_open:
            connection.commit()
            transaction_open = False
        elapsed = int((time.perf_counter() - start) * 1000)
        _write_history_entry(
            connection,
            dialect,
            MigrationHistoryEntry(
                revision=revision,
                description=description,
                checksum=checksum,
                applied_at=now,
                execution_time_ms=elapsed,
                status=status,
                dirty=False,
                artifact_version=artifact_version,
                ormdantic_version=__version__,
                metadata={
                    "phase": "completed",
                    "operation_count": len(operations),
                    **dict(metadata),
                },
            ),
        )
        _commit_migration_history_if_needed(connection, dialect)
    except Exception:
        if transaction_open:
            try:
                connection.rollback()
            except Exception:
                pass
        elapsed = int((time.perf_counter() - start) * 1000)
        _write_history_entry(
            connection,
            dialect,
            MigrationHistoryEntry(
                revision=revision,
                description=description,
                checksum=checksum,
                applied_at=now,
                execution_time_ms=elapsed,
                status=MIGRATION_STATUS_FAILED,
                dirty=True,
                artifact_version=artifact_version,
                ormdantic_version=__version__,
                metadata={
                    "phase": "failed",
                    "operation_count": len(operations),
                    **dict(metadata),
                },
            ),
        )
        try:
            _commit_migration_history_if_needed(connection, dialect)
        except Exception:
            pass
        raise
    finally:
        if release_lock_sql:
            try:
                connection.execute(release_lock_sql, [])
            except Exception:
                pass


def _commit_migration_history_if_needed(connection: Any, dialect: str) -> None:
    if _dialect_name(dialect) == "oracle" and hasattr(connection, "commit"):
        connection.commit()


def _reflect_schema_snapshot(
    url: str,
    *,
    dialect: str,
    include_tables: Sequence[str] | None,
    exclude_tables: Sequence[str] | None,
    schema: str | None,
) -> SchemaSnapshot:
    dialect_name = _dialect_name(dialect)
    if dialect_name != "sqlite":
        return _reflect_server_snapshot(
            url,
            dialect=dialect_name,
            include_tables=include_tables,
            exclude_tables=exclude_tables,
            schema=schema,
        )
    return _reflect_sqlite_snapshot(
        url,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
        schema=schema,
    )


def _reflect_server_snapshot(
    url: str,
    *,
    dialect: str,
    include_tables: Sequence[str] | None,
    exclude_tables: Sequence[str] | None,
    schema: str | None,
) -> SchemaSnapshot:
    rust = _require_migration_symbol("execute_native")
    tables = [
        table
        for table in _reflect_server_tables(rust, url, dialect, schema)
        if table != MIGRATION_TABLE
        and _table_matches_filters(table, include_tables, exclude_tables)
    ]
    column_rows = _reflect_server_columns(rust, url, dialect, schema, tables)
    primary_keys = _reflect_server_primary_keys(rust, url, dialect, schema, tables)
    unique_constraints = _reflect_server_unique_constraints(
        rust, url, dialect, schema, tables
    )
    foreign_keys = _reflect_server_foreign_keys(rust, url, dialect, schema, tables)
    indexes = _reflect_server_indexes(rust, url, dialect, schema, tables)
    snapshots: list[TableSnapshot] = []
    for table_name in tables:
        table_columns = column_rows.get(table_name, [])
        pk_columns = primary_keys.get(table_name, [])
        unique_columns = {
            columns[0]
            for columns in unique_constraints.get(table_name, [])
            if len(columns) == 1
        }
        table_unique_constraints = [
            columns
            for columns in unique_constraints.get(table_name, [])
            if len(columns) > 1
        ]
        table_foreign_keys = foreign_keys.get(table_name, {})
        columns = [
            ColumnSnapshot(
                name=column["name"],
                kind=column["kind"],
                nullable=column["nullable"] and column["name"] not in set(pk_columns),
                primary_key=column["name"] in set(pk_columns),
                foreign_table=table_foreign_keys.get(column["name"], (None, None))[0],
                foreign_column=table_foreign_keys.get(column["name"], (None, None))[1],
                max_length=column["max_length"],
                unique=column["name"] in unique_columns,
            )
            for column in table_columns
        ]
        primary_key = (
            pk_columns[0] if pk_columns else (columns[0].name if columns else "id")
        )
        snapshots.append(
            TableSnapshot(
                model_key=table_name,
                name=table_name,
                primary_key=primary_key,
                columns=columns,
                indexes=indexes.get(table_name, []),
                unique_constraints=table_unique_constraints,
                relationships=[],
            )
        )
    return SchemaSnapshot(tables=snapshots, version=MIGRATION_ARTIFACT_VERSION)


def _reflect_server_tables(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
) -> list[str]:
    schema_filter = _schema_filter(dialect, schema)
    if dialect == "postgresql":
        sql = (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = {schema_filter} AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
    elif dialect in {"mysql", "mariadb"}:
        sql = (
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = {schema_filter} AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        )
    elif dialect == "mssql":
        sql = (
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = {schema_filter} AND TABLE_TYPE = 'BASE TABLE' "
            "ORDER BY TABLE_NAME"
        )
    elif dialect == "oracle":
        table_view = _oracle_table_view(schema)
        owner_filter = _oracle_owner_filter(schema, table_alias="")
        sql = f"SELECT table_name FROM {table_view} {owner_filter} ORDER BY table_name"
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    return [str(row[0]) for row in _query_rows_url(rust, url, sql)]


def _reflect_server_columns(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    if not table_names:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    if dialect in {"postgresql", "mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "table_name")
        sql = (
            "SELECT table_name, column_name, data_type, is_nullable, "
            "character_maximum_length, numeric_precision, numeric_scale, ordinal_position "
            "FROM information_schema.columns "
            f"WHERE table_schema = {schema_filter} {table_filter} "
            "ORDER BY table_name, ordinal_position"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "TABLE_NAME")
        sql = (
            "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, "
            "CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, ORDINAL_POSITION "
            "FROM INFORMATION_SCHEMA.COLUMNS "
            f"WHERE TABLE_SCHEMA = {schema_filter} {table_filter} "
            "ORDER BY TABLE_NAME, ORDINAL_POSITION"
        )
    elif dialect == "oracle":
        view = _oracle_tab_columns_view(schema)
        owner_filter = _oracle_owner_filter(schema, table_alias="")
        table_filter = _table_name_filter(table_names, "table_name")
        where = f"{owner_filter} " if owner_filter else ""
        if where:
            where += f"AND hidden_column = 'NO' {table_filter}"
        else:
            where = f"WHERE hidden_column = 'NO' {table_filter}"
        sql = (
            "SELECT table_name, column_name, data_type, nullable, char_length, "
            f"data_precision, data_scale, column_id FROM {view} {where} "
            "ORDER BY table_name, column_id"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    columns: dict[str, list[dict[str, Any]]] = {}
    for row in _query_rows_url(rust, url, sql):
        table_name = str(row[0])
        data_type = str(row[2])
        columns.setdefault(table_name, []).append(
            {
                "name": str(row[1]),
                "kind": _normalize_reflected_type(
                    dialect,
                    data_type,
                    precision=_optional_int(row[5] if len(row) > 5 else None),
                    scale=_optional_int(row[6] if len(row) > 6 else None),
                ),
                "nullable": _nullable_from_reflection(row[3]),
                "max_length": _reflected_max_length(row[4] if len(row) > 4 else None),
            }
        )
    return columns


def _reflect_server_primary_keys(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, list[str]]:
    return _reflect_key_columns(rust, url, dialect, schema, "PRIMARY KEY", table_names)


def _reflect_server_unique_constraints(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, list[list[str]]]:
    grouped = _reflect_named_key_columns(
        rust, url, dialect, schema, "UNIQUE", table_names
    )
    return {
        table: [columns for _, columns in constraints]
        for table, constraints in grouped.items()
    }


def _reflect_key_columns(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    constraint_type: str,
    table_names: Sequence[str],
) -> dict[str, list[str]]:
    grouped = _reflect_named_key_columns(
        rust, url, dialect, schema, constraint_type, table_names
    )
    return {
        table: constraints[0][1] if constraints else []
        for table, constraints in grouped.items()
    }


def _reflect_named_key_columns(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    constraint_type: str,
    table_names: Sequence[str],
) -> dict[str, list[tuple[str, list[str]]]]:
    if not table_names:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    if dialect in {"postgresql", "mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "tc.table_name")
        sql = (
            "SELECT kcu.table_name, kcu.constraint_name, kcu.column_name, "
            "kcu.ordinal_position "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "ON tc.constraint_schema = kcu.constraint_schema "
            "AND tc.constraint_name = kcu.constraint_name "
            "AND tc.table_name = kcu.table_name "
            f"WHERE tc.table_schema = {schema_filter} "
            f"AND tc.constraint_type = {_sql_literal(constraint_type)} {table_filter} "
            "ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "tc.TABLE_NAME")
        sql = (
            "SELECT kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.COLUMN_NAME, "
            "kcu.ORDINAL_POSITION "
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
            "ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA "
            "AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
            "AND tc.TABLE_NAME = kcu.TABLE_NAME "
            f"WHERE tc.TABLE_SCHEMA = {schema_filter} "
            f"AND tc.CONSTRAINT_TYPE = {_sql_literal(constraint_type)} {table_filter} "
            "ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION"
        )
    elif dialect == "oracle":
        constraints = _oracle_constraints_view(schema)
        columns = _oracle_cons_columns_view(schema)
        owner_join = ""
        owner_filter = ""
        if schema:
            owner_join = "AND c.owner = cc.owner "
            owner_filter = f"AND c.owner = {_sql_literal(schema.upper())} "
        oracle_type = "P" if constraint_type == "PRIMARY KEY" else "U"
        table_filter = _table_name_filter(table_names, "cc.table_name")
        sql = (
            "SELECT cc.table_name, cc.constraint_name, cc.column_name, cc.position "
            f"FROM {constraints} c JOIN {columns} cc "
            "ON c.constraint_name = cc.constraint_name "
            f"{owner_join}AND c.table_name = cc.table_name "
            f"WHERE c.constraint_type = {_sql_literal(oracle_type)} {owner_filter}"
            f"{table_filter} "
            "ORDER BY cc.table_name, cc.constraint_name, cc.position"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    grouped: dict[str, dict[str, list[tuple[int, str]]]] = {}
    for row in _query_rows_url(rust, url, sql):
        table_name = str(row[0])
        constraint_name = str(row[1])
        ordinal = _optional_int(row[3]) or 0
        grouped.setdefault(table_name, {}).setdefault(constraint_name, []).append(
            (ordinal, str(row[2]))
        )
    return {
        table: [
            (
                name,
                [column for _, column in sorted(columns, key=lambda item: item[0])],
            )
            for name, columns in constraints.items()
        ]
        for table, constraints in grouped.items()
    }


def _reflect_server_foreign_keys(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, dict[str, tuple[str, str]]]:
    if not table_names:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "tc.table_name")
        sql = (
            "SELECT kcu.table_name, kcu.column_name, ccu.table_name, ccu.column_name, "
            "kcu.ordinal_position "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "ON tc.constraint_schema = kcu.constraint_schema "
            "AND tc.constraint_name = kcu.constraint_name "
            "AND tc.table_name = kcu.table_name "
            "JOIN information_schema.constraint_column_usage ccu "
            "ON tc.constraint_schema = ccu.constraint_schema "
            "AND tc.constraint_name = ccu.constraint_name "
            f"WHERE tc.table_schema = {schema_filter} "
            f"AND tc.constraint_type = 'FOREIGN KEY' {table_filter} "
            "ORDER BY kcu.table_name, tc.constraint_name, kcu.ordinal_position"
        )
    elif dialect in {"mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "table_name")
        sql = (
            "SELECT table_name, column_name, referenced_table_name, "
            "referenced_column_name, ordinal_position "
            "FROM information_schema.key_column_usage "
            f"WHERE table_schema = {schema_filter} "
            f"AND referenced_table_name IS NOT NULL {table_filter} "
            "ORDER BY table_name, constraint_name, ordinal_position"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "parent_table.name")
        sql = (
            "SELECT parent_table.name, parent_column.name, "
            "referenced_table.name, referenced_column.name, fkc.constraint_column_id "
            "FROM sys.foreign_key_columns fkc "
            "JOIN sys.tables parent_table ON fkc.parent_object_id = parent_table.object_id "
            "JOIN sys.schemas parent_schema ON parent_table.schema_id = parent_schema.schema_id "
            "JOIN sys.columns parent_column ON fkc.parent_object_id = parent_column.object_id "
            "AND fkc.parent_column_id = parent_column.column_id "
            "JOIN sys.tables referenced_table ON fkc.referenced_object_id = referenced_table.object_id "
            "JOIN sys.columns referenced_column ON fkc.referenced_object_id = referenced_column.object_id "
            "AND fkc.referenced_column_id = referenced_column.column_id "
            f"WHERE parent_schema.name = {schema_filter} {table_filter} "
            "ORDER BY parent_table.name, fkc.constraint_object_id, fkc.constraint_column_id"
        )
    elif dialect == "oracle":
        constraints = _oracle_constraints_view(schema)
        columns = _oracle_cons_columns_view(schema)
        owner_join = ""
        owner_filter = ""
        if schema:
            owner_join = "AND c.owner = cc.owner AND rc.owner = rcc.owner "
            owner_filter = f"AND c.owner = {_sql_literal(schema.upper())} "
        table_filter = _table_name_filter(table_names, "cc.table_name")
        sql = (
            "SELECT cc.table_name, cc.column_name, rcc.table_name, rcc.column_name, cc.position "
            f"FROM {constraints} c "
            f"JOIN {columns} cc ON c.constraint_name = cc.constraint_name "
            f"{owner_join}AND c.table_name = cc.table_name "
            f"JOIN {constraints} rc ON c.r_constraint_name = rc.constraint_name "
            f"JOIN {columns} rcc ON rc.constraint_name = rcc.constraint_name "
            "AND rc.table_name = rcc.table_name AND cc.position = rcc.position "
            "WHERE c.constraint_type = 'R' "
            f"{owner_filter}{table_filter} "
            "ORDER BY cc.table_name, c.constraint_name, cc.position"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    foreign_keys: dict[str, dict[str, tuple[str, str]]] = {}
    for row in _query_rows_url(rust, url, sql):
        foreign_keys.setdefault(str(row[0]), {})[str(row[1])] = (
            str(row[2]),
            str(row[3]),
        )
    return foreign_keys


def _reflect_server_indexes(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, list[IndexSnapshot]]:
    if not table_names:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "t.relname")
        sql = (
            "SELECT t.relname, i.relname, ix.indisunique, a.attname, x.ordinality "
            "FROM pg_class t "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "JOIN pg_index ix ON t.oid = ix.indrelid "
            "JOIN pg_class i ON i.oid = ix.indexrelid "
            "JOIN unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ordinality) ON true "
            "JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum "
            f"WHERE n.nspname = {schema_filter} {table_filter} AND NOT ix.indisprimary "
            "AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.oid "
            "AND c.contype IN ('p', 'u')) "
            "ORDER BY t.relname, i.relname, x.ordinality"
        )
    elif dialect in {"mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "table_name")
        sql = (
            "SELECT table_name, index_name, CASE non_unique WHEN 0 THEN 1 ELSE 0 END, "
            "column_name, seq_in_index "
            "FROM information_schema.statistics "
            f"WHERE table_schema = {schema_filter} {table_filter} AND index_name <> 'PRIMARY' "
            "ORDER BY table_name, index_name, seq_in_index"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "t.name")
        sql = (
            "SELECT t.name, i.name, CONVERT(int, i.is_unique), c.name, ic.key_ordinal "
            "FROM sys.indexes i "
            "JOIN sys.tables t ON i.object_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
            "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
            f"WHERE s.name = {schema_filter} {table_filter} AND i.is_primary_key = 0 "
            "AND i.is_unique_constraint = 0 AND i.name IS NOT NULL "
            "AND ic.is_included_column = 0 "
            "ORDER BY t.name, i.name, ic.key_ordinal"
        )
    elif dialect == "oracle":
        indexes = _oracle_indexes_view(schema)
        columns = _oracle_ind_columns_view(schema)
        constraints = _oracle_constraints_view(schema)
        owner_join = ""
        owner_filter = ""
        owner_not_exists = ""
        if schema:
            owner_join = "AND i.owner = ic.index_owner "
            owner_filter = f"AND i.owner = {_sql_literal(schema.upper())} "
            owner_not_exists = "AND c.owner = i.owner "
        table_filter = _table_name_filter(table_names, "i.table_name")
        sql = (
            "SELECT i.table_name, i.index_name, "
            "CASE i.uniqueness WHEN 'UNIQUE' THEN 1 ELSE 0 END, "
            "ic.column_name, ic.column_position "
            f"FROM {indexes} i JOIN {columns} ic "
            "ON i.index_name = ic.index_name "
            f"{owner_join}WHERE 1 = 1 {owner_filter}"
            f"{table_filter} AND NOT EXISTS (SELECT 1 "
            f"FROM {constraints} c WHERE c.index_name = i.index_name "
            f"{owner_not_exists}AND c.constraint_type IN ('P', 'U')) "
            "ORDER BY i.table_name, i.index_name, ic.column_position"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for row in _query_rows_url(rust, url, sql):
        table_name = str(row[0])
        index_name = str(row[1])
        index = grouped.setdefault(table_name, {}).setdefault(
            index_name,
            {"columns": [], "unique": _db_truthy(row[2])},
        )
        index["columns"].append((_optional_int(row[4]) or 0, str(row[3])))
    return {
        table: [
            IndexSnapshot(
                name=name,
                columns=[
                    column
                    for _, column in sorted(index["columns"], key=lambda item: item[0])
                ],
                unique=bool(index["unique"]),
            )
            for name, index in indexes.items()
        ]
        for table, indexes in grouped.items()
    }


def _schema_filter(dialect: str, schema: str | None) -> str:
    if schema:
        return _sql_literal(schema.upper() if dialect == "oracle" else schema)
    if dialect == "postgresql":
        return "current_schema()"
    if dialect in {"mysql", "mariadb"}:
        return "DATABASE()"
    if dialect == "mssql":
        return "SCHEMA_NAME()"
    if dialect == "oracle":
        return ""
    raise ValueError(f"schema filter is not available for dialect '{dialect}'")


def _table_name_filter(table_names: Sequence[str], column: str) -> str:
    if not table_names:
        return "AND 1 = 0"
    names = ", ".join(_sql_literal(table) for table in table_names)
    return f"AND {column} IN ({names})"


def _oracle_table_view(schema: str | None) -> str:
    return "all_tables" if schema else "user_tables"


def _oracle_tab_columns_view(schema: str | None) -> str:
    return "all_tab_cols" if schema else "user_tab_cols"


def _oracle_constraints_view(schema: str | None) -> str:
    return "all_constraints" if schema else "user_constraints"


def _oracle_cons_columns_view(schema: str | None) -> str:
    return "all_cons_columns" if schema else "user_cons_columns"


def _oracle_indexes_view(schema: str | None) -> str:
    return "all_indexes" if schema else "user_indexes"


def _oracle_ind_columns_view(schema: str | None) -> str:
    return "all_ind_columns" if schema else "user_ind_columns"


def _oracle_owner_filter(schema: str | None, *, table_alias: str) -> str:
    if not schema:
        return ""
    prefix = f"{table_alias}." if table_alias else ""
    return f"WHERE {prefix}owner = {_sql_literal(schema.upper())}"


def _nullable_from_reflection(value: Any) -> bool:
    return str(value).strip().upper() in {"YES", "Y", "TRUE", "1"}


def _reflected_max_length(value: Any) -> int | None:
    length = _optional_int(value)
    if length is None or length < 0:
        return None
    return length


def _normalize_reflected_type(
    dialect: str,
    value: Any,
    *,
    precision: int | None = None,
    scale: int | None = None,
) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return "str"
    if "UUID" in text or text == "UNIQUEIDENTIFIER":
        return "uuid"
    if "JSON" in text:
        return "json"
    if any(token in text for token in ("BLOB", "BINARY", "BYTEA", "RAW", "IMAGE")):
        return "bytes"
    if "BOOL" in text or text == "BIT":
        return "bool"
    if any(
        token in text
        for token in ("DOUBLE", "FLOAT", "REAL", "BINARY_FLOAT", "BINARY_DOUBLE")
    ):
        return "float"
    if any(token in text for token in ("INT", "SERIAL", "BIGSERIAL", "SMALLSERIAL")):
        return "int"
    if text in {"DATE"}:
        return "date"
    if any(token in text for token in ("TIMESTAMP", "DATETIME", "TIME WITH")):
        return "datetime"
    if any(token in text for token in ("DECIMAL", "NUMERIC", "NUMBER", "MONEY")):
        if dialect == "oracle" and scale in {None, 0}:
            return "int"
        return "decimal"
    if any(token in text for token in ("CHAR", "CLOB", "TEXT", "STRING", "ENUM")):
        return "str"
    return text.lower()


def _reflect_sqlite_snapshot(
    url: str,
    *,
    include_tables: Sequence[str] | None,
    exclude_tables: Sequence[str] | None,
    schema: str | None,
) -> SchemaSnapshot:
    if schema is not None and schema not in {"main", ""}:
        raise ValueError("sqlite reflection only supports the main schema")
    rust = _require_migration_symbol("PyDatabase")
    runtime = rust.PyDatabase(url, [])
    tables = []
    for table_name in sorted(str(name) for name in runtime.table_names()):
        if table_name.startswith("sqlite_"):
            continue
        if table_name == MIGRATION_TABLE:
            continue
        if not _table_matches_filters(table_name, include_tables, exclude_tables):
            continue
        columns_info = list(runtime.columns(table_name))
        foreign_keys = list(runtime.foreign_keys(table_name))
        foreign_map = {
            str(item["from"]): (str(item["table"]), str(item["to"]))
            for item in foreign_keys
            if item.get("from") and item.get("table") and item.get("to")
        }
        index_rows = list(runtime.indexes(table_name))
        indexes: list[IndexSnapshot] = []
        unique_constraints: list[list[str]] = []
        for index_row in index_rows:
            if not index_row.get("name"):
                continue
            index_name = str(index_row["name"])
            if index_name.startswith("sqlite_autoindex_"):
                continue
            unique = _db_truthy(index_row.get("unique"))
            indexes.append(IndexSnapshot(name=index_name, columns=[], unique=unique))
        columns: list[ColumnSnapshot] = []
        primary_key = "id"
        for item in columns_info:
            column_name = str(item["name"])
            column_type = _normalize_sqlite_type(item.get("type"))
            pk = _db_truthy(item.get("primary_key"))
            if pk and primary_key == "id":
                primary_key = column_name
            foreign_table, foreign_column = foreign_map.get(column_name, (None, None))
            columns.append(
                ColumnSnapshot(
                    name=column_name,
                    kind=column_type,
                    nullable=_db_truthy(item.get("nullable")) and not pk,
                    primary_key=pk,
                    foreign_table=foreign_table,
                    foreign_column=foreign_column,
                    unique=False,
                )
            )
        if (
            columns
            and primary_key == "id"
            and not any(column.primary_key for column in columns)
        ):
            primary_key = columns[0].name
        tables.append(
            TableSnapshot(
                model_key=table_name,
                name=table_name,
                primary_key=primary_key,
                columns=columns,
                indexes=indexes,
                unique_constraints=unique_constraints,
                relationships=[],
            )
        )
    return SchemaSnapshot(tables=tables, version=MIGRATION_ARTIFACT_VERSION)


def _normalize_sqlite_type(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return "str"
    if "INT" in text:
        return "int"
    if any(token in text for token in ("CHAR", "CLOB", "TEXT")):
        return "str"
    if "BLOB" in text:
        return "bytes"
    if any(token in text for token in ("REAL", "FLOA", "DOUB")):
        return "float"
    if "BOOL" in text:
        return "bool"
    return text.lower()


def _table_matches_filters(
    table_name: str,
    include_tables: Sequence[str] | None,
    exclude_tables: Sequence[str] | None,
) -> bool:
    if include_tables:
        if not any(fnmatch(table_name, pattern) for pattern in include_tables):
            return False
    if exclude_tables and any(
        fnmatch(table_name, pattern) for pattern in exclude_tables
    ):
        return False
    return True


def _operation_looks_destructive(sql: str) -> bool:
    normalized = " ".join(sql.strip().upper().split())
    if normalized.startswith(("DROP TABLE ", "TRUNCATE TABLE ", "DELETE FROM ")):
        return True
    return normalized.startswith("ALTER TABLE ") and " DROP " in normalized


def _document_format(path: str | PathLike[str], format: str | None = None) -> str:
    if format is not None:
        normalized = format.lower().lstrip(".")
    else:
        normalized = Path(path).suffix.lower().lstrip(".") or "json"
    if normalized not in {"json", "toml"}:
        raise ValueError(f"unsupported migration document format '{normalized}'")
    return normalized


def _toml_loads(payload: str | bytes | bytearray) -> dict[str, Any]:
    text = payload.decode() if isinstance(payload, (bytes, bytearray)) else payload
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python < 3.11
        try:
            import tomli as tomllib  # type: ignore[no-redef]
        except ModuleNotFoundError as exc:  # pragma: no cover
            raise RuntimeError(
                "Reading TOML migration files on Python < 3.11 requires tomli"
            ) from exc
    return tomllib.loads(text)


def _toml_dumps(payload: Mapping[str, Any]) -> str:
    lines = [
        f"{_toml_key(key)} = {_toml_value(value)}"
        for key, value in payload.items()
        if value is not None
    ]
    return "\n".join(lines) + "\n"


def _toml_key(key: str) -> str:
    if key.replace("_", "").replace("-", "").isalnum():
        return key
    return json.dumps(key)


def _toml_value(value: Any) -> str:
    if value is None:
        raise ValueError("TOML does not support null values")
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, Mapping):
        parts = [
            f"{_toml_key(str(key))} = {_toml_value(item)}"
            for key, item in value.items()
            if item is not None
        ]
        return "{ " + ", ".join(parts) + " }"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    raise TypeError(f"unsupported TOML value {value!r}")


def _compile_schema_diff(
    dialect: str, from_snapshot: SchemaSnapshot, to_snapshot: SchemaSnapshot
) -> list[dict[str, Any]]:
    rust = _require_migration_symbol("compile_schema_diff")
    return list(
        rust.compile_schema_diff(
            dialect,
            from_snapshot.to_runtime(),
            to_snapshot.to_runtime(),
        )
    )


def _coerce_snapshot(snapshot: SchemaSnapshot | Mapping[str, Any]) -> SchemaSnapshot:
    if isinstance(snapshot, SchemaSnapshot):
        return snapshot
    return SchemaSnapshot.from_dict(snapshot)


def _prepare_database_relationships(database: Any) -> None:
    for table in database._table_map.name_to_data.values():
        table.relationships = database.get(table)


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _runtime_check(check: Sequence[Any]) -> RuntimeCheck:
    return (str(check[0]), str(check[1]), str(check[2]))


def _check_expression(field: str, check: RuntimeCheck) -> str:
    kind, operator, value = check
    if kind == "comparison":
        return f"{field} {operator} {value}"
    if kind == "length":
        return f"LENGTH({field}) {operator} {value}"
    raise ValueError(f"unsupported check constraint kind '{kind}'")


def _check_suffix(check: RuntimeCheck) -> str:
    kind, operator, _ = check
    suffixes = {
        ("comparison", ">="): "ge",
        ("comparison", ">"): "gt",
        ("comparison", "<="): "le",
        ("comparison", "<"): "lt",
        ("length", ">="): "min_length",
        ("length", "<="): "max_length",
    }
    try:
        return suffixes[(kind, operator)]
    except KeyError as exc:
        raise ValueError(
            f"unsupported check constraint operator '{operator}' for kind '{kind}'"
        ) from exc


def _require_migration_symbol(symbol: str) -> Any:
    if _ormdantic is None or not hasattr(_ormdantic, symbol):
        raise RuntimeError(
            "Ormdantic requires the Rust extension for migration planning. "
            "Install the package with maturin or reinstall the wheel."
        )
    return _ormdantic
