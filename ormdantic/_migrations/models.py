"""Serializable migration model objects."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from os import PathLike
from pathlib import Path
from typing import Any

from ormdantic._migrations.documents import toml_dumps, toml_loads
from ormdantic._migrations.sql import document_format, operation_looks_destructive

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
            foreign_table=optional_str(column[4]),
            foreign_column=optional_str(column[5]),
            max_length=optional_int(column[6]),
            unique=bool(column[7]),
            checks=[runtime_check(check) for check in column[8]],
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ColumnSnapshot":
        return cls(
            name=str(payload["name"]),
            kind=str(payload["kind"]),
            nullable=bool(payload["nullable"]),
            primary_key=bool(payload["primary_key"]),
            foreign_table=optional_str(payload.get("foreign_table")),
            foreign_column=optional_str(payload.get("foreign_column")),
            max_length=optional_int(payload.get("max_length")),
            unique=bool(payload.get("unique", False)),
            checks=[runtime_check(check) for check in payload.get("checks", [])],
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
            optional_str(relationship[3]),
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "RelationshipSnapshot":
        return cls(
            str(payload["field"]),
            str(payload["foreign_table"]),
            str(payload["foreign_column"]),
            optional_str(payload.get("back_reference")),
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
        prepare_database_relationships(database)
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
        return cls.from_dict(toml_loads(payload))

    @classmethod
    def read(
        cls, path: str | PathLike[str], *, format: str | None = None
    ) -> "SchemaSnapshot":
        document = Path(path).read_text()
        if document_format(path, format) == "toml":
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
        return toml_dumps(self.to_dict())

    def write(self, path: str | PathLike[str], *, format: str | None = None) -> None:
        output = Path(path)
        output.parent.mkdir(parents=True, exist_ok=True)
        if document_format(path, format) == "toml":
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
                and any(operation_looks_destructive(op.sql) for op in self.operations)
            )
        )

    def dry_run(self) -> list[str]:
        return [operation.sql for operation in self.operations]

    def rollback_sql(self) -> list[str]:
        return [operation.sql for operation in self.rollback_operations]


def optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def runtime_check(check: Sequence[Any]) -> RuntimeCheck:
    return (str(check[0]), str(check[1]), str(check[2]))


def prepare_database_relationships(database: Any) -> None:
    for table in database._table_map.name_to_data.values():
        table.relationships = database.get(table)
