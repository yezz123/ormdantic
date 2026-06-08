"""Migration planning and execution facade."""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path
from typing import Any, Mapping, Sequence

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
    def read(cls, path: str | PathLike[str]) -> "SchemaSnapshot":
        return cls.from_json(Path(path).read_text())

    def to_runtime(self) -> list[RuntimeTableSpec]:
        return [table.to_runtime() for table in self.tables]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "tables": [table.to_dict() for table in self.tables],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    def write(self, path: str | PathLike[str]) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
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


@dataclass
class MigrationPlan:
    """A generated migration plan."""

    operations: list[MigrationOperation] = field(default_factory=list)
    rollback_operations: list[MigrationOperation] = field(default_factory=list)
    diff: SchemaDiff = field(default_factory=SchemaDiff)
    warnings: list[MigrationWarning] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.operations

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
    dialect: str | None = None
    version: int = 1

    @classmethod
    def from_plan(
        cls,
        revision: str,
        plan: MigrationPlan,
        from_snapshot: SchemaSnapshot,
        to_snapshot: SchemaSnapshot,
        *,
        dialect: str | None = None,
    ) -> "MigrationArtifact":
        return cls(
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
            dialect=dialect,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "MigrationArtifact":
        return cls(
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
            dialect=_optional_str(payload.get("dialect")),
            version=int(payload.get("version", 1)),
        )

    @classmethod
    def from_json(cls, payload: str | bytes | bytearray) -> "MigrationArtifact":
        return cls.from_dict(json.loads(payload))

    @classmethod
    def read(cls, path: str | PathLike[str]) -> "MigrationArtifact":
        return cls.from_json(Path(path).read_text())

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
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "revision": self.revision,
            "dialect": self.dialect,
            "from_snapshot": self.from_snapshot.to_dict(),
            "to_snapshot": self.to_snapshot.to_dict(),
            "up": [_operation_to_dict(operation) for operation in self.operations],
            "down": [
                _operation_to_dict(operation) for operation in self.rollback_operations
            ],
            "diff": _diff_to_dict(self.diff),
            "warnings": [_warning_to_dict(warning) for warning in self.warnings],
        }

    def to_json(self, *, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)

    def write(self, path: str | PathLike[str]) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(self.to_json())


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

    def snapshot(self) -> SchemaSnapshot:
        """Return a serializable snapshot for the currently registered models."""
        return SchemaSnapshot.from_database(self._database)

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
        path: str | PathLike[str] | None = None,
    ) -> MigrationArtifact:
        """Generate a serializable migration artifact."""
        before, after = self._resolve_snapshots(from_snapshot, to_snapshot)
        artifact = create_migration_artifact(
            revision,
            before,
            after,
            dialect=dialect or self._database._connection,
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
        """Create the migration revision table when missing."""
        self._database._ensure_runtime().ensure_revision_table()

    async def applied_revisions(self) -> list[str]:
        """Return applied migration revisions."""
        return list(self._database._ensure_runtime().applied_revisions())

    async def apply_artifact(
        self,
        artifact: MigrationArtifact | Mapping[str, Any] | str | PathLike[str],
        *,
        allow_destructive: bool = False,
    ) -> bool:
        """Apply a migration artifact and record its revision."""
        migration = _coerce_artifact(artifact)
        return await self.apply(
            migration.revision,
            migration.to_plan(),
            allow_destructive=allow_destructive,
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
        pattern: str = "*.json",
        allow_destructive: bool = False,
    ) -> list[str]:
        """Apply migration artifacts in filename order."""
        applied = []
        for artifact_path in _migration_files(path, pattern):
            artifact = MigrationArtifact.read(artifact_path)
            if await self.apply_artifact(
                artifact,
                allow_destructive=allow_destructive,
            ):
                applied.append(artifact.revision)
        return applied

    async def apply(
        self,
        revision: str,
        plan: MigrationPlan,
        *,
        allow_destructive: bool = False,
    ) -> bool:
        """Apply a migration plan and record its revision.

        Returns ``False`` when the revision is already recorded.
        """
        if revision in await self.applied_revisions():
            return False
        if plan.has_destructive_operations and not allow_destructive:
            raise ValueError(
                "migration contains destructive operations; pass "
                "allow_destructive=True to apply it"
            )
        self._database._ensure_runtime().apply_migration(
            revision, _operation_payload(plan)
        )
        return True

    async def rollback(self, revision: str, plan: MigrationPlan) -> bool:
        """Run rollback SQL and remove a migration revision."""
        if revision not in await self.applied_revisions():
            return False
        operations = plan.rollback_operations or plan.operations
        self._database._ensure_runtime().rollback_migration(
            revision, _operation_payload(MigrationPlan(list(operations)))
        )
        return True

    async def rollback_artifact(
        self,
        artifact: MigrationArtifact | Mapping[str, Any] | str | PathLike[str],
    ) -> bool:
        """Roll back a migration artifact when rollback SQL is available."""
        migration = _coerce_artifact(artifact)
        return await self.rollback(migration.revision, migration.to_plan())

    async def rollback_file(self, path: str | PathLike[str]) -> bool:
        """Roll back a migration artifact from disk."""
        return await self.rollback_artifact(MigrationArtifact.read(path))

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
        )
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
    return create_migration_artifact(
        revision,
        migrations[0].from_snapshot,
        migrations[-1].to_snapshot,
        dialect=migration_dialect,
    )


def _build_plan(
    dialect: str, from_snapshot: SchemaSnapshot, to_snapshot: SchemaSnapshot
) -> MigrationPlan:
    schema_diff = diff_snapshots(from_snapshot, to_snapshot)
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
    operations = [
        MigrationOperation(
            sql=str(item["sql"]),
            values=tuple(item.get("params", ())),
        )
        for item in compiled
    ]
    rollback_operations = [
        MigrationOperation(
            sql=str(item["sql"]),
            values=tuple(item.get("params", ())),
        )
        for item in rollback_compiled
    ]
    if schema_diff.has_destructive_operations:
        for operation in operations:
            operation.unsafe = True
            operation.destructive = True
    elif schema_diff.has_unsafe_operations:
        for operation in operations:
            operation.unsafe = True
    return MigrationPlan(operations, rollback_operations, schema_diff, warnings)


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
        details=dict(payload.get("details", {})),
    )


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
    }


def _operation_from_dict(payload: Mapping[str, Any]) -> MigrationOperation:
    return MigrationOperation(
        sql=str(payload["sql"]),
        values=tuple(payload.get("values", ())),
        description=_optional_str(payload.get("description")),
        unsafe=bool(payload.get("unsafe", False)),
        destructive=bool(payload.get("destructive", False)),
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


def _migration_files(path: str | PathLike[str], pattern: str) -> list[Path]:
    return sorted(Path(path).glob(pattern))


def _operation_payload(plan: MigrationPlan) -> list[tuple[str, tuple[Any, ...]]]:
    return [(operation.sql, operation.values) for operation in plan.operations]


def _operation_looks_destructive(sql: str) -> bool:
    normalized = " ".join(sql.strip().upper().split())
    if normalized.startswith(("DROP TABLE ", "TRUNCATE TABLE ", "DELETE FROM ")):
        return True
    return normalized.startswith("ALTER TABLE ") and " DROP " in normalized


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
