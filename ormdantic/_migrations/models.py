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
RuntimeConstraintTiming = tuple[bool | None, bool]
RuntimeSqliteColumnConflict = tuple[str | None, str | None, str | None]
RuntimeColumnTail = tuple[
    RuntimeConstraintTiming | None,
    str | None,
    RuntimeSqliteColumnConflict | None,
]
RuntimeIdentityOptions = tuple[
    bool,
    int | None,
    int | None,
    int | None,
    int | None,
    bool,
    int | None,
    bool,
    bool,
    bool,
    bool,
]
RuntimeColumnOptions = tuple[
    str | None,
    str | None,
    bool,
    bool,
    str | None,
    int | None,
    int | None,
    RuntimeIdentityOptions | None,
    str | None,
    str | None,
    str | None,
    RuntimeColumnTail | None,
]
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
    RuntimeColumnOptions,
]
RuntimeIndex = tuple[
    str,
    list[str],
    bool,
    str | None,
    list[str],
    str | None,
    list[str],
    list[tuple[str, str]],
]
RuntimeTableCheck = tuple[str, str, bool, bool]
RuntimeUniqueConstraint = tuple[
    str,
    list[str],
    bool | None,
    bool,
    bool,
    str | None,
    str | None,
    bool | None,
    str | None,
    str | None,
]
RuntimeExclusionElement = tuple[str, str]
RuntimeExclusionConstraint = tuple[
    str,
    list[RuntimeExclusionElement],
    list[RuntimeExclusionElement],
    str,
    str | None,
    bool | None,
    bool,
    dict[str, str],
]
RuntimeForeignKeyConstraint = tuple[
    str,
    list[str],
    str,
    list[str],
    str | None,
    str | None,
    bool | None,
    bool,
    bool,
    str | None,
]
RuntimeEnumType = tuple[str, list[str], str | None, str | None]
RuntimeNamespace = tuple[str, str | None]
RuntimeSequence = tuple[
    str,
    str | None,
    int | None,
    int | None,
    int | None,
    int | None,
    bool,
    int | None,
    str | None,
    str | None,
    bool,
    bool,
    bool,
]
RuntimeView = tuple[str, str | None, str, bool, str | None]
RuntimeRelationship = tuple[str, str, str, str | None]
RuntimeTableOptions = tuple[
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    str | None,
    list[str],
    list[tuple[str, str]],
    str | None,
    str | None,
    str | None,
    str | None,
    bool,
    bool,
    bool,
    str | None,
    bool,
    str | None,
    int | None,
    bool | None,
    bool | None,
    bool | None,
    bool | None,
    bool | None,
    int | None,
    int | None,
    int | None,
    int | None,
    str | None,
    str | None,
    str | None,
    str | None,
    list[str],
    str | None,
    int | None,
    str | None,
    int | None,
    int | None,
]
RuntimeTableSpec = tuple[
    str,
    str,
    str,
    list[RuntimeColumn],
    list[RuntimeIndex],
    list[list[str]],
    list[RuntimeUniqueConstraint],
    list[RuntimeTableCheck],
    list[RuntimeForeignKeyConstraint],
    list[RuntimeExclusionConstraint],
    RuntimeTableOptions,
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
    comment: str | None = None
    foreign_table: str | None = None
    foreign_column: str | None = None
    max_length: int | None = None
    unique: bool = False
    checks: list[RuntimeCheck] = field(default_factory=list)
    server_default: str | None = None
    computed: str | None = None
    computed_persisted: bool = False
    autoincrement: bool = False
    identity: bool = False
    identity_always: bool = False
    identity_start: int | None = None
    identity_increment: int | None = None
    identity_min_value: int | None = None
    identity_max_value: int | None = None
    identity_no_min_value: bool = False
    identity_no_max_value: bool = False
    identity_cycle: bool = False
    identity_cache: int | None = None
    identity_order: bool = False
    identity_on_null: bool = False
    collation: str | None = None
    numeric_precision: int | None = None
    numeric_scale: int | None = None
    foreign_key_name: str | None = None
    on_delete: str | None = None
    on_update: str | None = None
    deferrable: bool | None = None
    initially_deferred: bool = False
    sqlite_on_conflict_primary_key: str | None = None
    sqlite_on_conflict_not_null: str | None = None
    sqlite_on_conflict_unique: str | None = None

    @property
    def has_identity(self) -> bool:
        return (
            self.identity
            or self.identity_always
            or self.identity_start is not None
            or self.identity_increment is not None
            or self.identity_min_value is not None
            or self.identity_max_value is not None
            or self.identity_no_min_value
            or self.identity_no_max_value
            or self.identity_cycle
            or self.identity_cache is not None
            or self.identity_order
            or self.identity_on_null
        )

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
            **runtime_column_options(column),
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
            comment=optional_str(payload.get("comment")),
            checks=[runtime_check(check) for check in payload.get("checks", [])],
            server_default=optional_str(payload.get("server_default")),
            computed=optional_str(payload.get("computed")),
            computed_persisted=bool(payload.get("computed_persisted", False)),
            autoincrement=bool(payload.get("autoincrement", False)),
            identity=bool(payload.get("identity", False)),
            identity_always=bool(payload.get("identity_always", False)),
            identity_start=optional_int(payload.get("identity_start")),
            identity_increment=optional_int(payload.get("identity_increment")),
            identity_min_value=optional_int(payload.get("identity_min_value")),
            identity_max_value=optional_int(payload.get("identity_max_value")),
            identity_no_min_value=bool(payload.get("identity_no_min_value", False)),
            identity_no_max_value=bool(payload.get("identity_no_max_value", False)),
            identity_cycle=bool(payload.get("identity_cycle", False)),
            identity_cache=optional_int(payload.get("identity_cache")),
            identity_order=bool(payload.get("identity_order", False)),
            identity_on_null=bool(payload.get("identity_on_null", False)),
            collation=optional_str(payload.get("collation")),
            numeric_precision=optional_int(payload.get("numeric_precision")),
            numeric_scale=optional_int(payload.get("numeric_scale")),
            foreign_key_name=optional_str(payload.get("foreign_key_name")),
            on_delete=optional_str(payload.get("on_delete")),
            on_update=optional_str(payload.get("on_update")),
            deferrable=optional_bool(payload.get("deferrable")),
            initially_deferred=bool(payload.get("initially_deferred", False)),
            sqlite_on_conflict_primary_key=optional_str(
                payload.get("sqlite_on_conflict_primary_key")
            ),
            sqlite_on_conflict_not_null=optional_str(
                payload.get("sqlite_on_conflict_not_null")
            ),
            sqlite_on_conflict_unique=optional_str(
                payload.get("sqlite_on_conflict_unique")
            ),
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
            (
                self.server_default,
                self.computed,
                self.computed_persisted,
                self.autoincrement,
                self.collation,
                self.numeric_precision,
                self.numeric_scale,
                (
                    (
                        self.identity_always,
                        self.identity_start,
                        self.identity_increment,
                        self.identity_min_value,
                        self.identity_max_value,
                        self.identity_cycle,
                        self.identity_cache,
                        self.identity_order,
                        self.identity_on_null,
                        self.identity_no_min_value,
                        self.identity_no_max_value,
                    )
                    if self.has_identity
                    else None
                ),
                self.foreign_key_name,
                self.on_delete,
                self.on_update,
                (
                    (
                        (self.deferrable, self.initially_deferred)
                        if self.deferrable is not None or self.initially_deferred
                        else None
                    ),
                    self.comment,
                    (
                        (
                            self.sqlite_on_conflict_primary_key,
                            self.sqlite_on_conflict_not_null,
                            self.sqlite_on_conflict_unique,
                        )
                        if (
                            self.sqlite_on_conflict_primary_key is not None
                            or self.sqlite_on_conflict_not_null is not None
                            or self.sqlite_on_conflict_unique is not None
                        )
                        else None
                    ),
                ),
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
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
        if self.comment is not None:
            payload["comment"] = self.comment
        if self.server_default is not None:
            payload["server_default"] = self.server_default
        if self.computed is not None:
            payload["computed"] = self.computed
        if self.computed_persisted:
            payload["computed_persisted"] = self.computed_persisted
        if self.autoincrement:
            payload["autoincrement"] = self.autoincrement
        if self.has_identity:
            payload["identity"] = True
        if self.identity_always:
            payload["identity_always"] = self.identity_always
        if self.identity_start is not None:
            payload["identity_start"] = self.identity_start
        if self.identity_increment is not None:
            payload["identity_increment"] = self.identity_increment
        if self.identity_min_value is not None:
            payload["identity_min_value"] = self.identity_min_value
        if self.identity_max_value is not None:
            payload["identity_max_value"] = self.identity_max_value
        if self.identity_no_min_value:
            payload["identity_no_min_value"] = self.identity_no_min_value
        if self.identity_no_max_value:
            payload["identity_no_max_value"] = self.identity_no_max_value
        if self.identity_cycle:
            payload["identity_cycle"] = self.identity_cycle
        if self.identity_cache is not None:
            payload["identity_cache"] = self.identity_cache
        if self.identity_order:
            payload["identity_order"] = self.identity_order
        if self.identity_on_null:
            payload["identity_on_null"] = self.identity_on_null
        if self.collation is not None:
            payload["collation"] = self.collation
        if self.numeric_precision is not None:
            payload["numeric_precision"] = self.numeric_precision
        if self.numeric_scale is not None:
            payload["numeric_scale"] = self.numeric_scale
        if self.foreign_key_name is not None:
            payload["foreign_key_name"] = self.foreign_key_name
        if self.on_delete is not None:
            payload["on_delete"] = self.on_delete
        if self.on_update is not None:
            payload["on_update"] = self.on_update
        if self.deferrable is not None:
            payload["deferrable"] = self.deferrable
        if self.initially_deferred:
            payload["initially_deferred"] = self.initially_deferred
        if self.sqlite_on_conflict_primary_key is not None:
            payload["sqlite_on_conflict_primary_key"] = (
                self.sqlite_on_conflict_primary_key
            )
        if self.sqlite_on_conflict_not_null is not None:
            payload["sqlite_on_conflict_not_null"] = self.sqlite_on_conflict_not_null
        if self.sqlite_on_conflict_unique is not None:
            payload["sqlite_on_conflict_unique"] = self.sqlite_on_conflict_unique
        return payload


@dataclass(frozen=True)
class IndexSnapshot:
    """Serializable index metadata used by migration snapshots."""

    name: str
    columns: list[str]
    unique: bool = False
    where: str | None = None
    include_columns: list[str] = field(default_factory=list)
    method: str | None = None
    expressions: list[str] = field(default_factory=list)
    postgres_with: list[tuple[str, str]] = field(default_factory=list)
    comment: str | None = None
    postgres_tablespace: str | None = None
    mssql_filegroup: str | None = None
    mssql_clustered: bool = False
    oracle_tablespace: str | None = None
    mysql_prefix: str | None = None
    mysql_length: dict[str, int] = field(default_factory=dict)
    mysql_using: str | None = None
    postgres_ops: dict[str, str] = field(default_factory=dict)
    mysql_visible: bool | None = None
    oracle_bitmap: bool = False
    oracle_compress: int | bool | None = None
    postgres_nulls_not_distinct: bool = False

    @classmethod
    def from_runtime(cls, index: Sequence[Any]) -> "IndexSnapshot":
        if len(index) > 13 and isinstance(index[13], Mapping):
            mysql_prefix = None
            mysql_lengths = mysql_index_lengths(index[13])
            mysql_using = optional_str(index[14]) if len(index) > 14 else None
        else:
            mysql_prefix = optional_str(index[13]) if len(index) > 13 else None
            mysql_lengths = mysql_index_lengths(index[14]) if len(index) > 14 else {}
            mysql_using = optional_str(index[15]) if len(index) > 15 else None
        return cls(
            str(index[0]),
            [str(column) for column in index[1]],
            bool(index[2]),
            str(index[3]) if len(index) > 3 and index[3] is not None else None,
            [str(column) for column in index[4]] if len(index) > 4 else [],
            str(index[5]) if len(index) > 5 and index[5] is not None else None,
            [str(expression) for expression in index[6]] if len(index) > 6 else [],
            postgres_storage_parameters(index[7]) if len(index) > 7 else [],
            optional_str(index[8]) if len(index) > 8 else None,
            optional_str(index[9]) if len(index) > 9 else None,
            optional_str(index[10]) if len(index) > 10 else None,
            bool(index[11]) if len(index) > 11 else False,
            optional_str(index[12]) if len(index) > 12 else None,
            mysql_prefix,
            mysql_lengths,
            mysql_using,
            postgres_index_ops(index[16]) if len(index) > 16 else {},
            optional_bool(index[17]) if len(index) > 17 else None,
            bool(index[18]) if len(index) > 18 else False,
            oracle_index_compress(index[19]) if len(index) > 19 else None,
            bool(index[20]) if len(index) > 20 else False,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "IndexSnapshot":
        return cls(
            str(payload["name"]),
            [str(column) for column in payload["columns"]],
            bool(payload.get("unique", False)),
            (
                str(payload["where"])
                if payload.get("where") is not None
                else (
                    str(payload["where_expr"])
                    if payload.get("where_expr") is not None
                    else None
                )
            ),
            [str(column) for column in payload.get("include_columns", [])],
            str(payload["method"]) if payload.get("method") is not None else None,
            [str(expression) for expression in payload.get("expressions", [])],
            postgres_storage_parameters(payload.get("postgres_with", [])),
            optional_str(payload.get("comment")),
            optional_str(payload.get("postgres_tablespace")),
            optional_str(payload.get("mssql_filegroup")),
            bool(payload.get("mssql_clustered", False)),
            optional_str(payload.get("oracle_tablespace")),
            optional_str(payload.get("mysql_prefix")),
            mysql_index_lengths(payload.get("mysql_length", {})),
            optional_str(payload.get("mysql_using")),
            postgres_index_ops(payload.get("postgres_ops", {})),
            optional_bool(payload.get("mysql_visible")),
            bool(payload.get("oracle_bitmap", False)),
            oracle_index_compress(payload.get("oracle_compress")),
            bool(payload.get("postgres_nulls_not_distinct", False)),
        )

    def to_runtime(self) -> RuntimeIndex:
        return (
            self.name,
            list(self.columns),
            self.unique,
            self.where,
            list(self.include_columns),
            self.method,
            list(self.expressions),
            list(self.postgres_with),
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "columns": list(self.columns),
            "unique": self.unique,
        }
        if self.where is not None:
            payload["where"] = self.where
        if self.include_columns:
            payload["include_columns"] = list(self.include_columns)
        if self.method is not None:
            payload["method"] = self.method
        if self.expressions:
            payload["expressions"] = list(self.expressions)
        if self.postgres_with:
            payload["postgres_with"] = [
                [name, value] for name, value in self.postgres_with
            ]
        if self.comment is not None:
            payload["comment"] = self.comment
        if self.postgres_tablespace is not None:
            payload["postgres_tablespace"] = self.postgres_tablespace
        if self.mssql_filegroup is not None:
            payload["mssql_filegroup"] = self.mssql_filegroup
        if self.mssql_clustered:
            payload["mssql_clustered"] = self.mssql_clustered
        if self.oracle_tablespace is not None:
            payload["oracle_tablespace"] = self.oracle_tablespace
        if self.oracle_bitmap:
            payload["oracle_bitmap"] = self.oracle_bitmap
        if self.oracle_compress is not None:
            payload["oracle_compress"] = self.oracle_compress
        if self.mysql_prefix is not None:
            payload["mysql_prefix"] = self.mysql_prefix
        if self.mysql_length:
            payload["mysql_length"] = dict(self.mysql_length)
        if self.mysql_using is not None:
            payload["mysql_using"] = self.mysql_using
        if self.postgres_ops:
            payload["postgres_ops"] = dict(self.postgres_ops)
        if self.mysql_visible is not None:
            payload["mysql_visible"] = self.mysql_visible
        if self.postgres_nulls_not_distinct:
            payload["postgres_nulls_not_distinct"] = self.postgres_nulls_not_distinct
        return payload


@dataclass(frozen=True)
class TableCheckSnapshot:
    """Serializable table-level CHECK constraint metadata."""

    name: str
    expression: str
    validated: bool = True
    no_inherit: bool = False
    comment: str | None = None

    @classmethod
    def from_runtime(cls, check: Sequence[Any]) -> "TableCheckSnapshot":
        return cls(
            str(check[0]),
            str(check[1]),
            bool(check[2]) if len(check) > 2 else True,
            bool(check[3]) if len(check) > 3 else False,
            optional_str(check[4]) if len(check) > 4 else None,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TableCheckSnapshot":
        return cls(
            str(payload["name"]),
            str(payload["expression"]),
            bool(payload.get("validated", True)),
            bool(payload.get("no_inherit", False)),
            optional_str(payload.get("comment")),
        )

    def to_runtime(self) -> RuntimeTableCheck:
        return (self.name, self.expression, self.validated, self.no_inherit)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "expression": self.expression,
        }
        if not self.validated:
            payload["validated"] = False
        if self.no_inherit:
            payload["no_inherit"] = True
        if self.comment is not None:
            payload["comment"] = self.comment
        return payload


@dataclass(frozen=True)
class UniqueConstraintSnapshot:
    """Serializable named UNIQUE constraint metadata."""

    name: str
    columns: list[str]
    deferrable: bool | None = None
    initially_deferred: bool = False
    nulls_not_distinct: bool = False
    sqlite_on_conflict: str | None = None
    mssql_filegroup: str | None = None
    mssql_clustered: bool | None = None
    comment: str | None = None
    postgres_include: list[str] = field(default_factory=list)
    oracle_tablespace: str | None = None
    oracle_compress: int | bool | None = None

    @classmethod
    def from_runtime(cls, constraint: Sequence[Any]) -> "UniqueConstraintSnapshot":
        mssql_filegroup: str | None = None
        mssql_clustered: bool | None = None
        oracle_tablespace: str | None = None
        oracle_compress: int | bool | None = None
        comment: str | None = None
        postgres_include: list[str] = []
        if len(constraint) > 10:
            mssql_filegroup = (
                optional_str(constraint[6]) if len(constraint) > 6 else None
            )
            mssql_clustered = (
                optional_bool(constraint[7]) if len(constraint) > 7 else None
            )
            if len(constraint) > 8:
                comment = optional_str(constraint[8])
            if len(constraint) > 9 and constraint[9] is not None:
                postgres_include = [str(column) for column in constraint[9]]
            oracle_tablespace = optional_str(constraint[10])
            if len(constraint) > 11:
                oracle_compress = oracle_index_compress(constraint[11])
        elif len(constraint) == 10 and is_non_string_sequence(constraint[9]):
            mssql_filegroup = (
                optional_str(constraint[6]) if len(constraint) > 6 else None
            )
            mssql_clustered = (
                optional_bool(constraint[7]) if len(constraint) > 7 else None
            )
            comment = optional_str(constraint[8])
            postgres_include = [str(column) for column in constraint[9]]
        elif len(constraint) == 10:
            mssql_filegroup = (
                optional_str(constraint[6]) if len(constraint) > 6 else None
            )
            mssql_clustered = (
                optional_bool(constraint[7]) if len(constraint) > 7 else None
            )
            oracle_tablespace = optional_str(constraint[8])
            oracle_compress = oracle_index_compress(constraint[9])
        elif len(constraint) == 9:
            mssql_filegroup = (
                optional_str(constraint[6]) if len(constraint) > 6 else None
            )
            mssql_clustered = (
                optional_bool(constraint[7]) if len(constraint) > 7 else None
            )
            oracle_tablespace = optional_str(constraint[8])
        elif len(constraint) > 7 and is_non_string_sequence(constraint[7]):
            comment = optional_str(constraint[6]) if len(constraint) > 6 else None
            postgres_include = [str(column) for column in constraint[7]]
        else:
            if len(constraint) > 6:
                if isinstance(constraint[6], bool):
                    mssql_clustered = optional_bool(constraint[6])
                elif constraint[6] is not None:
                    if len(constraint) > 7:
                        mssql_filegroup = optional_str(constraint[6])
                    else:
                        comment = optional_str(constraint[6])
            if len(constraint) > 7:
                if isinstance(constraint[7], bool):
                    mssql_clustered = optional_bool(constraint[7])
                elif constraint[7] is not None:
                    comment = optional_str(constraint[7])
            if len(constraint) > 8 and constraint[8] is not None:
                if is_non_string_sequence(constraint[8]):
                    postgres_include = [str(column) for column in constraint[8]]
                else:
                    comment = optional_str(constraint[8])
            if len(constraint) > 9 and constraint[9] is not None:
                postgres_include = [str(column) for column in constraint[9]]
        return cls(
            str(constraint[0]),
            [str(column) for column in constraint[1]],
            optional_bool(constraint[2]) if len(constraint) > 2 else None,
            bool(constraint[3]) if len(constraint) > 3 else False,
            bool(constraint[4]) if len(constraint) > 4 else False,
            optional_str(constraint[5]) if len(constraint) > 5 else None,
            mssql_filegroup,
            mssql_clustered,
            comment,
            postgres_include,
            oracle_tablespace,
            oracle_compress,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "UniqueConstraintSnapshot":
        return cls(
            str(payload["name"]),
            [str(column) for column in payload["columns"]],
            optional_bool(payload.get("deferrable")),
            bool(payload.get("initially_deferred", False)),
            bool(payload.get("nulls_not_distinct", False)),
            optional_str(payload.get("sqlite_on_conflict")),
            optional_str(payload.get("mssql_filegroup")),
            optional_bool(payload.get("mssql_clustered")),
            optional_str(payload.get("comment")),
            [str(column) for column in payload.get("postgres_include") or []],
            optional_str(payload.get("oracle_tablespace")),
            oracle_index_compress(payload.get("oracle_compress")),
        )

    def to_runtime(self) -> RuntimeUniqueConstraint:
        return (
            self.name,
            list(self.columns),
            self.deferrable,
            self.initially_deferred,
            self.nulls_not_distinct,
            self.sqlite_on_conflict,
            self.mssql_filegroup,
            self.mssql_clustered,
            self.oracle_tablespace,
            oracle_index_compress_runtime(self.oracle_compress),
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "columns": list(self.columns),
        }
        if self.deferrable is not None:
            payload["deferrable"] = self.deferrable
        if self.initially_deferred:
            payload["initially_deferred"] = self.initially_deferred
        if self.nulls_not_distinct:
            payload["nulls_not_distinct"] = self.nulls_not_distinct
        if self.sqlite_on_conflict is not None:
            payload["sqlite_on_conflict"] = self.sqlite_on_conflict
        if self.mssql_filegroup is not None:
            payload["mssql_filegroup"] = self.mssql_filegroup
        if self.mssql_clustered is not None:
            payload["mssql_clustered"] = self.mssql_clustered
        if self.oracle_tablespace is not None:
            payload["oracle_tablespace"] = self.oracle_tablespace
        if self.oracle_compress is not None:
            payload["oracle_compress"] = self.oracle_compress
        if self.comment is not None:
            payload["comment"] = self.comment
        if self.postgres_include:
            payload["postgres_include"] = list(self.postgres_include)
        return payload


@dataclass(frozen=True)
class ForeignKeyConstraintSnapshot:
    """Serializable table-level FOREIGN KEY constraint metadata."""

    name: str
    columns: list[str]
    foreign_table: str
    foreign_columns: list[str]
    on_delete: str | None = None
    on_update: str | None = None
    deferrable: bool | None = None
    initially_deferred: bool = False
    validated: bool = True
    match: str | None = None
    comment: str | None = None

    @classmethod
    def from_runtime(
        cls,
        constraint: Sequence[Any],
    ) -> "ForeignKeyConstraintSnapshot":
        return cls(
            str(constraint[0]),
            [str(column) for column in constraint[1]],
            str(constraint[2]),
            [str(column) for column in constraint[3]],
            optional_str(constraint[4]) if len(constraint) > 4 else None,
            optional_str(constraint[5]) if len(constraint) > 5 else None,
            optional_bool(constraint[6]) if len(constraint) > 6 else None,
            bool(constraint[7]) if len(constraint) > 7 else False,
            bool(constraint[8]) if len(constraint) > 8 else True,
            optional_str(constraint[9]) if len(constraint) > 9 else None,
            optional_str(constraint[10]) if len(constraint) > 10 else None,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ForeignKeyConstraintSnapshot":
        return cls(
            str(payload["name"]),
            [str(column) for column in payload["columns"]],
            str(payload["foreign_table"]),
            [str(column) for column in payload["foreign_columns"]],
            optional_str(payload.get("on_delete")),
            optional_str(payload.get("on_update")),
            optional_bool(payload.get("deferrable")),
            bool(payload.get("initially_deferred", False)),
            bool(payload.get("validated", True)),
            optional_str(payload.get("match")),
            optional_str(payload.get("comment")),
        )

    def to_runtime(self) -> RuntimeForeignKeyConstraint:
        return (
            self.name,
            list(self.columns),
            self.foreign_table,
            list(self.foreign_columns),
            self.on_delete,
            self.on_update,
            self.deferrable,
            self.initially_deferred,
            self.validated,
            self.match,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "columns": list(self.columns),
            "foreign_table": self.foreign_table,
            "foreign_columns": list(self.foreign_columns),
        }
        if self.on_delete is not None:
            payload["on_delete"] = self.on_delete
        if self.on_update is not None:
            payload["on_update"] = self.on_update
        if self.deferrable is not None:
            payload["deferrable"] = self.deferrable
        if self.initially_deferred:
            payload["initially_deferred"] = self.initially_deferred
        if not self.validated:
            payload["validated"] = False
        if self.match is not None:
            payload["match"] = self.match
        if self.comment is not None:
            payload["comment"] = self.comment
        return payload


@dataclass(frozen=True)
class ExclusionConstraintSnapshot:
    """Serializable PostgreSQL EXCLUDE constraint metadata."""

    name: str
    columns: list[RuntimeExclusionElement] = field(default_factory=list)
    expressions: list[RuntimeExclusionElement] = field(default_factory=list)
    using: str = "gist"
    where: str | None = None
    deferrable: bool | None = None
    initially_deferred: bool = False
    ops: dict[str, str] = field(default_factory=dict)
    comment: str | None = None

    @classmethod
    def from_runtime(
        cls,
        constraint: Sequence[Any],
    ) -> "ExclusionConstraintSnapshot":
        ops: dict[str, str] = {}
        comment: str | None = None
        if len(constraint) > 7:
            if isinstance(constraint[7], Mapping):
                ops = postgres_index_ops(constraint[7])
                comment = optional_str(constraint[8]) if len(constraint) > 8 else None
            else:
                comment = optional_str(constraint[7])
        return cls(
            str(constraint[0]),
            exclusion_elements(constraint[1]),
            exclusion_elements(constraint[2]),
            str(constraint[3]) if len(constraint) > 3 else "gist",
            optional_str(constraint[4]) if len(constraint) > 4 else None,
            optional_bool(constraint[5]) if len(constraint) > 5 else None,
            bool(constraint[6]) if len(constraint) > 6 else False,
            ops,
            comment,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ExclusionConstraintSnapshot":
        return cls(
            str(payload["name"]),
            exclusion_elements(payload.get("columns", [])),
            exclusion_elements(payload.get("expressions", [])),
            str(payload.get("using", "gist")),
            optional_str(payload.get("where")),
            optional_bool(payload.get("deferrable")),
            bool(payload.get("initially_deferred", False)),
            postgres_index_ops(payload.get("ops", {})),
            optional_str(payload.get("comment")),
        )

    def to_runtime(self) -> RuntimeExclusionConstraint:
        return (
            self.name,
            list(self.columns),
            list(self.expressions),
            self.using,
            self.where,
            self.deferrable,
            self.initially_deferred,
            dict(self.ops),
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "columns": [list(element) for element in self.columns],
            "expressions": [list(element) for element in self.expressions],
            "using": self.using,
        }
        if self.where is not None:
            payload["where"] = self.where
        if self.deferrable is not None:
            payload["deferrable"] = self.deferrable
        if self.initially_deferred:
            payload["initially_deferred"] = self.initially_deferred
        if self.ops:
            payload["ops"] = dict(self.ops)
        if self.comment is not None:
            payload["comment"] = self.comment
        return payload


@dataclass(frozen=True)
class EnumTypeSnapshot:
    """Serializable native enum type metadata used by migration snapshots."""

    name: str
    values: list[str]
    schema: str | None = None
    comment: str | None = None

    @classmethod
    def from_runtime(cls, enum_type: Sequence[Any]) -> "EnumTypeSnapshot":
        schema = optional_str(enum_type[2]) if len(enum_type) > 2 else None
        comment = optional_str(enum_type[3]) if len(enum_type) > 3 else None
        return cls(
            str(enum_type[0]),
            [str(value) for value in enum_type[1]],
            schema,
            comment,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "EnumTypeSnapshot":
        return cls(
            str(payload["name"]),
            [str(value) for value in payload["values"]],
            optional_str(payload.get("schema")),
            optional_str(payload.get("comment")),
        )

    def to_runtime(self) -> RuntimeEnumType:
        return (self.name, list(self.values), self.schema, self.comment)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "values": list(self.values),
        }
        if self.schema is not None:
            payload["schema"] = self.schema
        if self.comment is not None:
            payload["comment"] = self.comment
        return payload


@dataclass(frozen=True)
class NamespaceSnapshot:
    """Serializable database namespace/schema metadata used by migration snapshots."""

    name: str
    comment: str | None = None

    @classmethod
    def from_runtime(cls, namespace: Sequence[Any]) -> "NamespaceSnapshot":
        return cls(
            str(namespace[0]),
            optional_str(namespace[1]) if len(namespace) > 1 else None,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "NamespaceSnapshot":
        return cls(str(payload["name"]), optional_str(payload.get("comment")))

    def to_runtime(self) -> RuntimeNamespace:
        return (self.name, self.comment)

    def to_dict(self) -> dict[str, Any]:
        payload = {"name": self.name}
        if self.comment is not None:
            payload["comment"] = self.comment
        return payload


@dataclass(frozen=True)
class SequenceSnapshot:
    """Serializable database sequence metadata used by migration snapshots."""

    name: str
    schema: str | None = None
    start: int | None = None
    increment: int | None = None
    min_value: int | None = None
    max_value: int | None = None
    cycle: bool = False
    cache: int | None = None
    comment: str | None = None
    data_type: str | None = None
    order: bool = False
    no_min_value: bool = False
    no_max_value: bool = False

    @classmethod
    def from_runtime(cls, sequence: Sequence[Any]) -> "SequenceSnapshot":
        return cls(
            str(sequence[0]),
            optional_str(sequence[1]) if len(sequence) > 1 else None,
            optional_int(sequence[2]) if len(sequence) > 2 else None,
            optional_int(sequence[3]) if len(sequence) > 3 else None,
            optional_int(sequence[4]) if len(sequence) > 4 else None,
            optional_int(sequence[5]) if len(sequence) > 5 else None,
            bool(sequence[6]) if len(sequence) > 6 else False,
            optional_int(sequence[7]) if len(sequence) > 7 else None,
            optional_str(sequence[8]) if len(sequence) > 8 else None,
            optional_str(sequence[9]) if len(sequence) > 9 else None,
            bool(sequence[10]) if len(sequence) > 10 else False,
            bool(sequence[11]) if len(sequence) > 11 else False,
            bool(sequence[12]) if len(sequence) > 12 else False,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SequenceSnapshot":
        return cls(
            str(payload["name"]),
            optional_str(payload.get("schema")),
            optional_int(payload.get("start")),
            optional_int(payload.get("increment")),
            optional_int(payload.get("min_value")),
            optional_int(payload.get("max_value")),
            bool(payload.get("cycle", False)),
            optional_int(payload.get("cache")),
            optional_str(payload.get("comment")),
            optional_str(payload.get("data_type")),
            bool(payload.get("order", False)),
            bool(payload.get("no_min_value", False)),
            bool(payload.get("no_max_value", False)),
        )

    def to_runtime(self) -> RuntimeSequence:
        return (
            self.name,
            self.schema,
            self.start,
            self.increment,
            self.min_value,
            self.max_value,
            self.cycle,
            self.cache,
            self.comment,
            self.data_type,
            self.order,
            self.no_min_value,
            self.no_max_value,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": self.name}
        if self.schema is not None:
            payload["schema"] = self.schema
        if self.start is not None:
            payload["start"] = self.start
        if self.increment is not None:
            payload["increment"] = self.increment
        if self.min_value is not None:
            payload["min_value"] = self.min_value
        if self.max_value is not None:
            payload["max_value"] = self.max_value
        if self.cycle:
            payload["cycle"] = self.cycle
        if self.cache is not None:
            payload["cache"] = self.cache
        if self.comment is not None:
            payload["comment"] = self.comment
        if self.data_type is not None:
            payload["data_type"] = self.data_type
        if self.order:
            payload["order"] = self.order
        if self.no_min_value:
            payload["no_min_value"] = self.no_min_value
        if self.no_max_value:
            payload["no_max_value"] = self.no_max_value
        return payload


@dataclass(frozen=True)
class ViewSnapshot:
    """Serializable database view metadata used by migration snapshots."""

    name: str
    definition: str
    schema: str | None = None
    materialized: bool = False
    comment: str | None = None

    @classmethod
    def from_runtime(cls, view: Sequence[Any]) -> "ViewSnapshot":
        return cls(
            str(view[0]),
            normalized_view_definition(str(view[2])),
            optional_str(view[1]) if len(view) > 1 else None,
            bool(view[3]) if len(view) > 3 else False,
            optional_str(view[4]) if len(view) > 4 else None,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "ViewSnapshot":
        return cls(
            str(payload["name"]),
            normalized_view_definition(str(payload["definition"])),
            optional_str(payload.get("schema")),
            bool(payload.get("materialized", False)),
            optional_str(payload.get("comment")),
        )

    def to_runtime(self) -> RuntimeView:
        return (
            self.name,
            self.schema,
            self.definition,
            self.materialized,
            self.comment,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "definition": self.definition,
        }
        if self.schema is not None:
            payload["schema"] = self.schema
        if self.materialized:
            payload["materialized"] = self.materialized
        if self.comment is not None:
            payload["comment"] = self.comment
        return payload


def normalized_view_definition(definition: str) -> str:
    """Normalize stored view definitions enough to avoid trivial churn."""
    normalized = definition.strip()
    if normalized.endswith(";"):
        normalized = normalized[:-1].strip()
    return normalized


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
    schema: str | None = None
    columns: list[ColumnSnapshot] = field(default_factory=list)
    indexes: list[IndexSnapshot] = field(default_factory=list)
    unique_constraints: list[list[str]] = field(default_factory=list)
    named_unique_constraints: list[UniqueConstraintSnapshot] = field(
        default_factory=list
    )
    check_constraints: list[TableCheckSnapshot] = field(default_factory=list)
    foreign_key_constraints: list[ForeignKeyConstraintSnapshot] = field(
        default_factory=list
    )
    exclusion_constraints: list[ExclusionConstraintSnapshot] = field(
        default_factory=list
    )
    relationships: list[RelationshipSnapshot] = field(default_factory=list)
    comment: str | None = None
    tablespace: str | None = None
    mysql_engine: str | None = None
    mysql_charset: str | None = None
    mysql_collation: str | None = None
    mysql_row_format: str | None = None
    mysql_key_block_size: int | None = None
    mysql_pack_keys: bool | None = None
    mysql_checksum: bool | None = None
    mysql_delay_key_write: bool | None = None
    mysql_stats_persistent: bool | None = None
    mysql_stats_auto_recalc: bool | None = None
    mysql_stats_sample_pages: int | None = None
    mysql_avg_row_length: int | None = None
    mysql_max_rows: int | None = None
    mysql_min_rows: int | None = None
    mysql_insert_method: str | None = None
    mysql_data_directory: str | None = None
    mysql_index_directory: str | None = None
    mysql_connection: str | None = None
    mysql_union: list[str] = field(default_factory=list)
    mysql_partition_by: str | None = None
    mysql_partitions: int | None = None
    mysql_subpartition_by: str | None = None
    mysql_subpartitions: int | None = None
    mysql_auto_increment: int | None = None
    postgres_inherits: list[str] = field(default_factory=list)
    postgres_with: list[tuple[str, str]] = field(default_factory=list)
    postgres_using: str | None = None
    postgres_partition_by: str | None = None
    postgres_partition_of: str | None = None
    postgres_partition_for: str | None = None
    postgres_unlogged: bool = False
    sqlite_strict: bool = False
    sqlite_without_rowid: bool = False
    oracle_compress: int | bool | None = None

    @classmethod
    def from_runtime(cls, table: Sequence[Any]) -> "TableSnapshot":
        sqlite_strict = False
        sqlite_without_rowid = False
        schema = None
        if len(table) > 11:
            named_unique_constraints = table[6]
            check_constraints = table[7]
            foreign_key_constraints = table[8]
            exclusion_constraints = table[9]
            if is_runtime_table_options(table[10]):
                comment = optional_str(table[10][0])
                tablespace = optional_str(table[10][1])
                mysql_engine = (
                    optional_str(table[10][2]) if len(table[10]) > 2 else None
                )
                mysql_charset = (
                    optional_str(table[10][3]) if len(table[10]) > 3 else None
                )
                mysql_collation = (
                    optional_str(table[10][4]) if len(table[10]) > 4 else None
                )
                new_mysql_options = len(table[10]) > 11
                mysql_row_format = (
                    optional_str(table[10][5]) if new_mysql_options else None
                )
                postgres_offset = 1 if new_mysql_options else 0
                postgres_inherits = (
                    [str(parent) for parent in table[10][5 + postgres_offset]]
                    if len(table[10]) > 5 + postgres_offset
                    else []
                )
                postgres_with = (
                    postgres_storage_parameters(table[10][6 + postgres_offset])
                    if len(table[10]) > 6 + postgres_offset
                    else []
                )
                postgres_using = (
                    optional_str(table[10][7 + postgres_offset])
                    if len(table[10]) > 7 + postgres_offset
                    else None
                )
                postgres_partition_by = (
                    optional_str(table[10][8 + postgres_offset])
                    if len(table[10]) > 8 + postgres_offset
                    else None
                )
                postgres_partition_of = (
                    optional_str(table[10][9 + postgres_offset])
                    if len(table[10]) > 9 + postgres_offset
                    else None
                )
                postgres_partition_for = (
                    optional_str(table[10][10 + postgres_offset])
                    if len(table[10]) > 10 + postgres_offset
                    else None
                )
                postgres_unlogged = (
                    bool(table[10][11 + postgres_offset])
                    if len(table[10]) > 11 + postgres_offset
                    else False
                )
                sqlite_strict = (
                    bool(table[10][12 + postgres_offset])
                    if len(table[10]) > 12 + postgres_offset
                    else False
                )
                sqlite_without_rowid = (
                    bool(table[10][13 + postgres_offset])
                    if len(table[10]) > 13 + postgres_offset
                    else False
                )
                schema = (
                    optional_str(table[10][14 + postgres_offset])
                    if len(table[10]) > 14 + postgres_offset
                    else None
                )
                oracle_compress = (
                    oracle_table_compress(table[10][16 + postgres_offset])
                    if len(table[10]) > 16 + postgres_offset
                    else None
                )
                mysql_key_block_size = (
                    optional_int(table[10][17 + postgres_offset])
                    if len(table[10]) > 17 + postgres_offset
                    else None
                )
                mysql_pack_keys = (
                    optional_bool(table[10][18 + postgres_offset])
                    if len(table[10]) > 18 + postgres_offset
                    else None
                )
                mysql_checksum = (
                    optional_bool(table[10][19 + postgres_offset])
                    if len(table[10]) > 19 + postgres_offset
                    else None
                )
                mysql_delay_key_write = (
                    optional_bool(table[10][20 + postgres_offset])
                    if len(table[10]) > 20 + postgres_offset
                    else None
                )
                mysql_stats_persistent = (
                    optional_bool(table[10][21 + postgres_offset])
                    if len(table[10]) > 21 + postgres_offset
                    else None
                )
                mysql_stats_auto_recalc = (
                    optional_bool(table[10][22 + postgres_offset])
                    if len(table[10]) > 22 + postgres_offset
                    else None
                )
                mysql_stats_sample_pages = (
                    optional_int(table[10][23 + postgres_offset])
                    if len(table[10]) > 23 + postgres_offset
                    else None
                )
                mysql_avg_row_length = (
                    optional_int(table[10][24 + postgres_offset])
                    if len(table[10]) > 24 + postgres_offset
                    else None
                )
                mysql_max_rows = (
                    optional_int(table[10][25 + postgres_offset])
                    if len(table[10]) > 25 + postgres_offset
                    else None
                )
                mysql_min_rows = (
                    optional_int(table[10][26 + postgres_offset])
                    if len(table[10]) > 26 + postgres_offset
                    else None
                )
                mysql_insert_method = (
                    optional_str(table[10][27 + postgres_offset])
                    if len(table[10]) > 27 + postgres_offset
                    else None
                )
                mysql_data_directory = (
                    optional_str(table[10][28 + postgres_offset])
                    if len(table[10]) > 28 + postgres_offset
                    else None
                )
                mysql_index_directory = (
                    optional_str(table[10][29 + postgres_offset])
                    if len(table[10]) > 29 + postgres_offset
                    else None
                )
                mysql_connection = (
                    optional_str(table[10][30 + postgres_offset])
                    if len(table[10]) > 30 + postgres_offset
                    else None
                )
                mysql_union = (
                    string_list(table[10][31 + postgres_offset])
                    if len(table[10]) > 31 + postgres_offset
                    else []
                )
                mysql_partition_by = (
                    optional_str(table[10][32 + postgres_offset])
                    if len(table[10]) > 32 + postgres_offset
                    else None
                )
                mysql_partitions = (
                    optional_int(table[10][33 + postgres_offset])
                    if len(table[10]) > 33 + postgres_offset
                    else None
                )
                mysql_subpartition_by = (
                    optional_str(table[10][34 + postgres_offset])
                    if len(table[10]) > 34 + postgres_offset
                    else None
                )
                mysql_subpartitions = (
                    optional_int(table[10][35 + postgres_offset])
                    if len(table[10]) > 35 + postgres_offset
                    else None
                )
                mysql_auto_increment = (
                    optional_int(table[10][36 + postgres_offset])
                    if len(table[10]) > 36 + postgres_offset
                    else None
                )
            else:
                comment = optional_str(table[10])
                tablespace = None
                mysql_engine = None
                mysql_charset = None
                mysql_collation = None
                mysql_row_format = None
                mysql_key_block_size = None
                mysql_pack_keys = None
                mysql_checksum = None
                mysql_delay_key_write = None
                mysql_stats_persistent = None
                mysql_stats_auto_recalc = None
                mysql_stats_sample_pages = None
                mysql_avg_row_length = None
                mysql_max_rows = None
                mysql_min_rows = None
                mysql_insert_method = None
                mysql_data_directory = None
                mysql_index_directory = None
                mysql_connection = None
                mysql_union = []
                mysql_partition_by = None
                mysql_partitions = None
                mysql_subpartition_by = None
                mysql_subpartitions = None
                mysql_auto_increment = None
                postgres_inherits = []
                postgres_with = []
                postgres_using = None
                postgres_partition_by = None
                postgres_partition_of = None
                postgres_partition_for = None
                postgres_unlogged = False
                schema = None
                oracle_compress = None
            relationships = table[11]
        elif len(table) > 10:
            named_unique_constraints = table[6]
            check_constraints = table[7]
            foreign_key_constraints = table[8]
            exclusion_constraints = table[9]
            comment = None
            tablespace = None
            mysql_engine = None
            mysql_charset = None
            mysql_collation = None
            mysql_row_format = None
            mysql_key_block_size = None
            mysql_pack_keys = None
            mysql_checksum = None
            mysql_delay_key_write = None
            mysql_stats_persistent = None
            mysql_stats_auto_recalc = None
            mysql_stats_sample_pages = None
            mysql_avg_row_length = None
            mysql_max_rows = None
            mysql_min_rows = None
            mysql_insert_method = None
            mysql_data_directory = None
            mysql_index_directory = None
            mysql_connection = None
            mysql_union = []
            mysql_partition_by = None
            mysql_partitions = None
            mysql_subpartition_by = None
            mysql_subpartitions = None
            mysql_auto_increment = None
            postgres_inherits = []
            postgres_with = []
            postgres_using = None
            postgres_partition_by = None
            postgres_partition_of = None
            postgres_partition_for = None
            postgres_unlogged = False
            schema = None
            oracle_compress = None
            relationships = table[10]
        elif len(table) > 9:
            named_unique_constraints = table[6]
            check_constraints = table[7]
            foreign_key_constraints = table[8]
            exclusion_constraints = []
            comment = None
            tablespace = None
            mysql_engine = None
            mysql_charset = None
            mysql_collation = None
            mysql_row_format = None
            mysql_key_block_size = None
            mysql_pack_keys = None
            mysql_checksum = None
            mysql_delay_key_write = None
            mysql_stats_persistent = None
            mysql_stats_auto_recalc = None
            mysql_stats_sample_pages = None
            mysql_avg_row_length = None
            mysql_max_rows = None
            mysql_min_rows = None
            mysql_insert_method = None
            mysql_data_directory = None
            mysql_index_directory = None
            mysql_connection = None
            mysql_union = []
            mysql_partition_by = None
            mysql_partitions = None
            mysql_subpartition_by = None
            mysql_subpartitions = None
            mysql_auto_increment = None
            postgres_inherits = []
            postgres_with = []
            postgres_using = None
            postgres_partition_by = None
            postgres_partition_of = None
            postgres_partition_for = None
            postgres_unlogged = False
            schema = None
            oracle_compress = None
            relationships = table[9]
        elif len(table) > 8:
            named_unique_constraints = table[6]
            check_constraints = table[7]
            foreign_key_constraints = []
            exclusion_constraints = []
            comment = None
            tablespace = None
            mysql_engine = None
            mysql_charset = None
            mysql_collation = None
            mysql_row_format = None
            mysql_key_block_size = None
            mysql_pack_keys = None
            mysql_checksum = None
            mysql_delay_key_write = None
            mysql_stats_persistent = None
            mysql_stats_auto_recalc = None
            mysql_stats_sample_pages = None
            mysql_avg_row_length = None
            mysql_max_rows = None
            mysql_min_rows = None
            mysql_insert_method = None
            mysql_data_directory = None
            mysql_index_directory = None
            mysql_connection = None
            mysql_union = []
            mysql_partition_by = None
            mysql_partitions = None
            mysql_subpartition_by = None
            mysql_subpartitions = None
            mysql_auto_increment = None
            postgres_inherits = []
            postgres_with = []
            postgres_using = None
            postgres_partition_by = None
            postgres_partition_of = None
            postgres_partition_for = None
            postgres_unlogged = False
            schema = None
            oracle_compress = None
            relationships = table[8]
        elif len(table) > 7:
            named_unique_constraints = []
            check_constraints = table[6]
            foreign_key_constraints = []
            exclusion_constraints = []
            comment = None
            tablespace = None
            mysql_engine = None
            mysql_charset = None
            mysql_collation = None
            mysql_row_format = None
            mysql_key_block_size = None
            mysql_pack_keys = None
            mysql_checksum = None
            mysql_delay_key_write = None
            mysql_stats_persistent = None
            mysql_stats_auto_recalc = None
            mysql_stats_sample_pages = None
            mysql_avg_row_length = None
            mysql_max_rows = None
            mysql_min_rows = None
            mysql_insert_method = None
            mysql_data_directory = None
            mysql_index_directory = None
            mysql_connection = None
            mysql_union = []
            mysql_partition_by = None
            mysql_partitions = None
            mysql_subpartition_by = None
            mysql_subpartitions = None
            mysql_auto_increment = None
            postgres_inherits = []
            postgres_with = []
            postgres_using = None
            postgres_partition_by = None
            postgres_partition_of = None
            postgres_partition_for = None
            postgres_unlogged = False
            schema = None
            oracle_compress = None
            relationships = table[7]
        else:
            named_unique_constraints = []
            check_constraints = []
            foreign_key_constraints = []
            exclusion_constraints = []
            comment = None
            tablespace = None
            mysql_engine = None
            mysql_charset = None
            mysql_collation = None
            mysql_row_format = None
            mysql_key_block_size = None
            mysql_pack_keys = None
            mysql_checksum = None
            mysql_delay_key_write = None
            mysql_stats_persistent = None
            mysql_stats_auto_recalc = None
            mysql_stats_sample_pages = None
            mysql_avg_row_length = None
            mysql_max_rows = None
            mysql_min_rows = None
            mysql_insert_method = None
            mysql_data_directory = None
            mysql_index_directory = None
            mysql_connection = None
            mysql_union = []
            mysql_partition_by = None
            mysql_partitions = None
            mysql_subpartition_by = None
            mysql_subpartitions = None
            mysql_auto_increment = None
            postgres_inherits = []
            postgres_with = []
            postgres_using = None
            postgres_partition_by = None
            postgres_partition_of = None
            postgres_partition_for = None
            postgres_unlogged = False
            schema = None
            oracle_compress = None
            relationships = table[6]
        return cls(
            model_key=str(table[0]),
            name=str(table[1]),
            primary_key=str(table[2]),
            schema=schema,
            comment=comment,
            tablespace=tablespace,
            mysql_engine=mysql_engine,
            mysql_charset=mysql_charset,
            mysql_collation=mysql_collation,
            mysql_row_format=mysql_row_format,
            mysql_key_block_size=mysql_key_block_size,
            mysql_pack_keys=mysql_pack_keys,
            mysql_checksum=mysql_checksum,
            mysql_delay_key_write=mysql_delay_key_write,
            mysql_stats_persistent=mysql_stats_persistent,
            mysql_stats_auto_recalc=mysql_stats_auto_recalc,
            mysql_stats_sample_pages=mysql_stats_sample_pages,
            mysql_avg_row_length=mysql_avg_row_length,
            mysql_max_rows=mysql_max_rows,
            mysql_min_rows=mysql_min_rows,
            mysql_insert_method=mysql_insert_method,
            mysql_data_directory=mysql_data_directory,
            mysql_index_directory=mysql_index_directory,
            mysql_connection=mysql_connection,
            mysql_union=mysql_union,
            mysql_partition_by=mysql_partition_by,
            mysql_partitions=mysql_partitions,
            mysql_subpartition_by=mysql_subpartition_by,
            mysql_subpartitions=mysql_subpartitions,
            mysql_auto_increment=mysql_auto_increment,
            postgres_inherits=postgres_inherits,
            postgres_with=postgres_with,
            postgres_using=postgres_using,
            postgres_partition_by=postgres_partition_by,
            postgres_partition_of=postgres_partition_of,
            postgres_partition_for=postgres_partition_for,
            postgres_unlogged=postgres_unlogged,
            sqlite_strict=sqlite_strict,
            sqlite_without_rowid=sqlite_without_rowid,
            oracle_compress=oracle_compress,
            columns=[ColumnSnapshot.from_runtime(column) for column in table[3]],
            indexes=[IndexSnapshot.from_runtime(index) for index in table[4]],
            unique_constraints=[
                [str(column) for column in columns] for columns in table[5]
            ],
            named_unique_constraints=[
                UniqueConstraintSnapshot.from_runtime(constraint)
                for constraint in named_unique_constraints
            ],
            check_constraints=[
                TableCheckSnapshot.from_runtime(check) for check in check_constraints
            ],
            foreign_key_constraints=[
                ForeignKeyConstraintSnapshot.from_runtime(constraint)
                for constraint in foreign_key_constraints
            ],
            exclusion_constraints=[
                ExclusionConstraintSnapshot.from_runtime(constraint)
                for constraint in exclusion_constraints
            ],
            relationships=[
                RelationshipSnapshot.from_runtime(relationship)
                for relationship in relationships
            ],
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "TableSnapshot":
        return cls(
            model_key=str(payload["model_key"]),
            name=str(payload["name"]),
            primary_key=str(payload["primary_key"]),
            schema=optional_str(payload.get("schema")),
            comment=optional_str(payload.get("comment")),
            tablespace=optional_str(payload.get("tablespace")),
            mysql_engine=optional_str(payload.get("mysql_engine")),
            mysql_charset=optional_str(payload.get("mysql_charset")),
            mysql_collation=optional_str(payload.get("mysql_collation")),
            mysql_row_format=optional_str(payload.get("mysql_row_format")),
            mysql_key_block_size=optional_int(payload.get("mysql_key_block_size")),
            mysql_pack_keys=optional_bool(payload.get("mysql_pack_keys")),
            mysql_checksum=optional_bool(payload.get("mysql_checksum")),
            mysql_delay_key_write=optional_bool(payload.get("mysql_delay_key_write")),
            mysql_stats_persistent=optional_bool(payload.get("mysql_stats_persistent")),
            mysql_stats_auto_recalc=optional_bool(
                payload.get("mysql_stats_auto_recalc")
            ),
            mysql_stats_sample_pages=optional_int(
                payload.get("mysql_stats_sample_pages")
            ),
            mysql_avg_row_length=optional_int(payload.get("mysql_avg_row_length")),
            mysql_max_rows=optional_int(payload.get("mysql_max_rows")),
            mysql_min_rows=optional_int(payload.get("mysql_min_rows")),
            mysql_insert_method=optional_str(payload.get("mysql_insert_method")),
            mysql_data_directory=optional_str(payload.get("mysql_data_directory")),
            mysql_index_directory=optional_str(payload.get("mysql_index_directory")),
            mysql_connection=optional_str(payload.get("mysql_connection")),
            mysql_union=string_list(payload.get("mysql_union", [])),
            mysql_partition_by=optional_str(payload.get("mysql_partition_by")),
            mysql_partitions=optional_int(payload.get("mysql_partitions")),
            mysql_subpartition_by=optional_str(payload.get("mysql_subpartition_by")),
            mysql_subpartitions=optional_int(payload.get("mysql_subpartitions")),
            mysql_auto_increment=optional_int(payload.get("mysql_auto_increment")),
            postgres_inherits=[
                str(parent) for parent in payload.get("postgres_inherits", [])
            ],
            postgres_with=postgres_storage_parameters(payload.get("postgres_with", [])),
            postgres_using=optional_str(payload.get("postgres_using")),
            postgres_partition_by=optional_str(payload.get("postgres_partition_by")),
            postgres_partition_of=optional_str(payload.get("postgres_partition_of")),
            postgres_partition_for=optional_str(payload.get("postgres_partition_for")),
            postgres_unlogged=bool(payload.get("postgres_unlogged", False)),
            sqlite_strict=bool(payload.get("sqlite_strict", False)),
            sqlite_without_rowid=bool(payload.get("sqlite_without_rowid", False)),
            oracle_compress=oracle_table_compress(payload.get("oracle_compress")),
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
            named_unique_constraints=[
                UniqueConstraintSnapshot.from_dict(constraint)
                for constraint in payload.get("named_unique_constraints", [])
            ],
            check_constraints=[
                TableCheckSnapshot.from_dict(check)
                for check in payload.get("check_constraints", [])
            ],
            foreign_key_constraints=[
                ForeignKeyConstraintSnapshot.from_dict(constraint)
                for constraint in payload.get("foreign_key_constraints", [])
            ],
            exclusion_constraints=[
                ExclusionConstraintSnapshot.from_dict(constraint)
                for constraint in payload.get("exclusion_constraints", [])
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
            [constraint.to_runtime() for constraint in self.named_unique_constraints],
            [check.to_runtime() for check in self.check_constraints],
            [constraint.to_runtime() for constraint in self.foreign_key_constraints],
            [constraint.to_runtime() for constraint in self.exclusion_constraints],
            (
                self.comment,
                self.tablespace,
                self.mysql_engine,
                self.mysql_charset,
                self.mysql_collation,
                self.mysql_row_format,
                list(self.postgres_inherits),
                list(self.postgres_with),
                self.postgres_using,
                self.postgres_partition_by,
                self.postgres_partition_of,
                self.postgres_partition_for,
                self.postgres_unlogged,
                self.sqlite_strict,
                self.sqlite_without_rowid,
                self.schema,
                any(index.mssql_clustered for index in self.indexes)
                or any(
                    constraint.mssql_clustered is True
                    for constraint in self.named_unique_constraints
                ),
                oracle_table_compress_runtime(self.oracle_compress),
                self.mysql_key_block_size,
                self.mysql_pack_keys,
                self.mysql_checksum,
                self.mysql_delay_key_write,
                self.mysql_stats_persistent,
                self.mysql_stats_auto_recalc,
                self.mysql_stats_sample_pages,
                self.mysql_avg_row_length,
                self.mysql_max_rows,
                self.mysql_min_rows,
                self.mysql_insert_method,
                self.mysql_data_directory,
                self.mysql_index_directory,
                self.mysql_connection,
                list(self.mysql_union),
                self.mysql_partition_by,
                self.mysql_partitions,
                self.mysql_subpartition_by,
                self.mysql_subpartitions,
                self.mysql_auto_increment,
            ),
            [relationship.to_runtime() for relationship in self.relationships],
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "model_key": self.model_key,
            "name": self.name,
            "primary_key": self.primary_key,
            "columns": [column.to_dict() for column in self.columns],
            "indexes": [index.to_dict() for index in self.indexes],
            "unique_constraints": [
                list(columns) for columns in self.unique_constraints
            ],
            "named_unique_constraints": [
                constraint.to_dict() for constraint in self.named_unique_constraints
            ],
            "check_constraints": [check.to_dict() for check in self.check_constraints],
            "foreign_key_constraints": [
                constraint.to_dict() for constraint in self.foreign_key_constraints
            ],
            "exclusion_constraints": [
                constraint.to_dict() for constraint in self.exclusion_constraints
            ],
            "relationships": [
                relationship.to_dict() for relationship in self.relationships
            ],
        }
        if self.schema is not None:
            payload["schema"] = self.schema
        if self.comment is not None:
            payload["comment"] = self.comment
        if self.tablespace is not None:
            payload["tablespace"] = self.tablespace
        if self.mysql_engine is not None:
            payload["mysql_engine"] = self.mysql_engine
        if self.mysql_charset is not None:
            payload["mysql_charset"] = self.mysql_charset
        if self.mysql_collation is not None:
            payload["mysql_collation"] = self.mysql_collation
        if self.mysql_row_format is not None:
            payload["mysql_row_format"] = self.mysql_row_format
        if self.mysql_key_block_size is not None:
            payload["mysql_key_block_size"] = self.mysql_key_block_size
        if self.mysql_pack_keys is not None:
            payload["mysql_pack_keys"] = self.mysql_pack_keys
        if self.mysql_checksum is not None:
            payload["mysql_checksum"] = self.mysql_checksum
        if self.mysql_delay_key_write is not None:
            payload["mysql_delay_key_write"] = self.mysql_delay_key_write
        if self.mysql_stats_persistent is not None:
            payload["mysql_stats_persistent"] = self.mysql_stats_persistent
        if self.mysql_stats_auto_recalc is not None:
            payload["mysql_stats_auto_recalc"] = self.mysql_stats_auto_recalc
        if self.mysql_stats_sample_pages is not None:
            payload["mysql_stats_sample_pages"] = self.mysql_stats_sample_pages
        if self.mysql_avg_row_length is not None:
            payload["mysql_avg_row_length"] = self.mysql_avg_row_length
        if self.mysql_max_rows is not None:
            payload["mysql_max_rows"] = self.mysql_max_rows
        if self.mysql_min_rows is not None:
            payload["mysql_min_rows"] = self.mysql_min_rows
        if self.mysql_insert_method is not None:
            payload["mysql_insert_method"] = self.mysql_insert_method
        if self.mysql_data_directory is not None:
            payload["mysql_data_directory"] = self.mysql_data_directory
        if self.mysql_index_directory is not None:
            payload["mysql_index_directory"] = self.mysql_index_directory
        if self.mysql_connection is not None:
            payload["mysql_connection"] = self.mysql_connection
        if self.mysql_union:
            payload["mysql_union"] = list(self.mysql_union)
        if self.mysql_partition_by is not None:
            payload["mysql_partition_by"] = self.mysql_partition_by
        if self.mysql_partitions is not None:
            payload["mysql_partitions"] = self.mysql_partitions
        if self.mysql_subpartition_by is not None:
            payload["mysql_subpartition_by"] = self.mysql_subpartition_by
        if self.mysql_subpartitions is not None:
            payload["mysql_subpartitions"] = self.mysql_subpartitions
        if self.mysql_auto_increment is not None:
            payload["mysql_auto_increment"] = self.mysql_auto_increment
        if self.postgres_inherits:
            payload["postgres_inherits"] = list(self.postgres_inherits)
        if self.postgres_with:
            payload["postgres_with"] = [
                [name, value] for name, value in self.postgres_with
            ]
        if self.postgres_using is not None:
            payload["postgres_using"] = self.postgres_using
        if self.postgres_partition_by is not None:
            payload["postgres_partition_by"] = self.postgres_partition_by
        if self.postgres_partition_of is not None:
            payload["postgres_partition_of"] = self.postgres_partition_of
        if self.postgres_partition_for is not None:
            payload["postgres_partition_for"] = self.postgres_partition_for
        if self.postgres_unlogged:
            payload["postgres_unlogged"] = self.postgres_unlogged
        if self.sqlite_strict:
            payload["sqlite_strict"] = self.sqlite_strict
        if self.sqlite_without_rowid:
            payload["sqlite_without_rowid"] = self.sqlite_without_rowid
        if self.oracle_compress is not None:
            payload["oracle_compress"] = self.oracle_compress
        return payload


@dataclass(frozen=True)
class SchemaSnapshot:
    """Serializable database schema snapshot."""

    tables: list[TableSnapshot] = field(default_factory=list)
    namespaces: list[NamespaceSnapshot] = field(default_factory=list)
    enum_types: list[EnumTypeSnapshot] = field(default_factory=list)
    sequences: list[SequenceSnapshot] = field(default_factory=list)
    views: list[ViewSnapshot] = field(default_factory=list)
    version: int = 1

    @classmethod
    def empty(cls) -> "SchemaSnapshot":
        return cls()

    @classmethod
    def from_runtime(cls, tables: Sequence[Sequence[Any]]) -> "SchemaSnapshot":
        return cls([TableSnapshot.from_runtime(table) for table in tables])

    @classmethod
    def from_database(
        cls,
        database: Any,
        *,
        native_enum_types: bool = False,
        enum_schema: str | None = None,
    ) -> "SchemaSnapshot":
        prepare_database_relationships(database)
        enum_types: list[EnumTypeSnapshot] = []
        if native_enum_types:
            from ormdantic.schema import enum_type_descriptors

            enum_types = [
                EnumTypeSnapshot.from_runtime(enum_type)
                for enum_type in enum_type_descriptors(
                    database._table_map,
                    schema=enum_schema,
                )
            ]
        namespaces = [
            NamespaceSnapshot.from_runtime(namespace)
            for namespace in database._runtime_namespace_specs()
        ]
        sequences = [
            SequenceSnapshot.from_runtime(sequence)
            for sequence in database._runtime_sequence_specs()
        ]
        views = [
            ViewSnapshot.from_runtime(view) for view in database._runtime_view_specs()
        ]
        return cls(
            tables=[
                TableSnapshot.from_runtime(table)
                for table in database._runtime_table_specs(
                    native_enum_types=native_enum_types,
                    enum_schema=enum_schema,
                )
            ],
            namespaces=namespaces,
            enum_types=enum_types,
            sequences=sequences,
            views=views,
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "SchemaSnapshot":
        return cls(
            tables=[
                TableSnapshot.from_dict(table) for table in payload.get("tables", [])
            ],
            namespaces=[
                NamespaceSnapshot.from_dict(namespace)
                for namespace in payload.get("namespaces", [])
            ],
            enum_types=[
                EnumTypeSnapshot.from_dict(enum_type)
                for enum_type in payload.get("enum_types", [])
            ],
            sequences=[
                SequenceSnapshot.from_dict(sequence)
                for sequence in payload.get("sequences", [])
            ],
            views=[ViewSnapshot.from_dict(view) for view in payload.get("views", [])],
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
        input_path = Path(path)
        document = input_path.read_text()
        if document_format(str(input_path), format) == "toml":
            return cls.from_toml(document)
        return cls.from_json(document)

    def to_runtime(self) -> list[RuntimeTableSpec]:
        return [table.to_runtime() for table in self.tables]

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "version": self.version,
            "tables": [table.to_dict() for table in self.tables],
        }
        if self.namespaces:
            payload["namespaces"] = [
                namespace.to_dict() for namespace in self.namespaces
            ]
        if self.enum_types:
            payload["enum_types"] = [
                enum_type.to_dict() for enum_type in self.enum_types
            ]
        if self.sequences:
            payload["sequences"] = [sequence.to_dict() for sequence in self.sequences]
        if self.views:
            payload["views"] = [view.to_dict() for view in self.views]
        return payload

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


def string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not is_non_string_sequence(value):
        return [str(value)]
    return [str(item) for item in value]


def is_runtime_table_options(value: Any) -> bool:
    """Return whether a runtime table field is the bundled table options tuple."""
    return (
        not isinstance(value, str) and isinstance(value, Sequence) and len(value) >= 2
    )


def is_non_string_sequence(value: Any) -> bool:
    """Return whether value is a runtime sequence payload, not scalar text."""
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def optional_bool(value: Any) -> bool | None:
    return None if value is None else bool(value)


def oracle_index_compress(value: Any) -> int | bool | None:
    if value is None or value is False:
        return None
    if value is True:
        return True
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "false", "none", "no", "disabled"}:
            return None
        if normalized in {"true", "compress", "enabled"}:
            return True
        try:
            value = int(normalized)
        except ValueError as exc:
            raise ValueError(
                "Oracle index compression must be true or a positive integer"
            ) from exc
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("Oracle index compression must be true or a positive integer")
    if value <= 0:
        raise ValueError("Oracle index compression must be positive")
    return value


def oracle_table_compress(value: Any) -> int | bool | None:
    try:
        return oracle_index_compress(value)
    except ValueError as exc:
        message = str(exc).replace("index compression", "table compression")
        raise ValueError(message) from exc


def oracle_index_compress_runtime(value: Any) -> str | None:
    compress = oracle_index_compress(value)
    if compress is None:
        return None
    if compress is True:
        return "true"
    return str(compress)


def oracle_table_compress_runtime(value: Any) -> str | None:
    compress = oracle_table_compress(value)
    if compress is None:
        return None
    if compress is True:
        return "true"
    return str(compress)


def postgres_storage_parameters(value: Any) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        return [
            (str(name), str(parameter)) for name, parameter in sorted(value.items())
        ]
    return [(str(item[0]), str(item[1])) for item in value]


def mysql_index_lengths(value: Any) -> dict[str, int]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return {str(column): int(length) for column, length in value.items()}
    return {str(item[0]): int(item[1]) for item in value}


def postgres_index_ops(value: Any) -> dict[str, str]:
    if not value:
        return {}
    if isinstance(value, Mapping):
        return {str(item): str(opclass) for item, opclass in value.items()}
    return {str(item[0]): str(item[1]) for item in value}


def runtime_column_options(column: Sequence[Any]) -> dict[str, Any]:
    if len(column) <= 9:
        return {}
    options = column[9]
    if isinstance(options, Sequence) and not isinstance(
        options, (str, bytes, bytearray)
    ):
        return {
            "server_default": optional_str(options[0]) if len(options) > 0 else None,
            "computed": optional_str(options[1]) if len(options) > 1 else None,
            "computed_persisted": bool(options[2]) if len(options) > 2 else False,
            "autoincrement": bool(options[3]) if len(options) > 3 else False,
            "collation": optional_str(options[4]) if len(options) > 4 else None,
            "numeric_precision": optional_int(options[5]) if len(options) > 5 else None,
            "numeric_scale": optional_int(options[6]) if len(options) > 6 else None,
            **(runtime_identity_options(options[7]) if len(options) > 7 else {}),
            "foreign_key_name": optional_str(options[8]) if len(options) > 8 else None,
            "on_delete": optional_str(options[9]) if len(options) > 9 else None,
            "on_update": optional_str(options[10]) if len(options) > 10 else None,
            **(runtime_column_tail(options[11]) if len(options) > 11 else {}),
        }
    return {}


def runtime_identity_options(options: Any) -> dict[str, Any]:
    if not isinstance(options, Sequence) or isinstance(
        options, (str, bytes, bytearray)
    ):
        return {}
    return {
        "identity": True,
        "identity_always": bool(options[0]) if len(options) > 0 else False,
        "identity_start": optional_int(options[1]) if len(options) > 1 else None,
        "identity_increment": optional_int(options[2]) if len(options) > 2 else None,
        "identity_min_value": optional_int(options[3]) if len(options) > 3 else None,
        "identity_max_value": optional_int(options[4]) if len(options) > 4 else None,
        "identity_cycle": bool(options[5]) if len(options) > 5 else False,
        "identity_cache": optional_int(options[6]) if len(options) > 6 else None,
        "identity_order": bool(options[7]) if len(options) > 7 else False,
        "identity_on_null": bool(options[8]) if len(options) > 8 else False,
        "identity_no_min_value": bool(options[9]) if len(options) > 9 else False,
        "identity_no_max_value": bool(options[10]) if len(options) > 10 else False,
    }


def runtime_constraint_timing(options: Any) -> dict[str, Any]:
    if not isinstance(options, Sequence) or isinstance(
        options, (str, bytes, bytearray)
    ):
        return {}
    return {
        "deferrable": optional_bool(options[0]) if len(options) > 0 else None,
        "initially_deferred": bool(options[1]) if len(options) > 1 else False,
    }


def runtime_column_tail(options: Any) -> dict[str, Any]:
    if not isinstance(options, Sequence) or isinstance(
        options, (str, bytes, bytearray)
    ):
        return {}
    if len(options) > 1 and (options[1] is None or isinstance(options[1], str)):
        return {
            **runtime_constraint_timing(options[0]),
            "comment": optional_str(options[1]),
            **(runtime_sqlite_column_conflict(options[2]) if len(options) > 2 else {}),
        }
    return runtime_constraint_timing(options)


def runtime_sqlite_column_conflict(options: Any) -> dict[str, Any]:
    if not isinstance(options, Sequence) or isinstance(
        options, (str, bytes, bytearray)
    ):
        return {}
    return {
        "sqlite_on_conflict_primary_key": (
            optional_str(options[0]) if len(options) > 0 else None
        ),
        "sqlite_on_conflict_not_null": (
            optional_str(options[1]) if len(options) > 1 else None
        ),
        "sqlite_on_conflict_unique": (
            optional_str(options[2]) if len(options) > 2 else None
        ),
    }


def runtime_check(check: Sequence[Any]) -> RuntimeCheck:
    return (str(check[0]), str(check[1]), str(check[2]))


def exclusion_elements(elements: Any) -> list[RuntimeExclusionElement]:
    if not isinstance(elements, Sequence) or isinstance(
        elements, (str, bytes, bytearray)
    ):
        return []
    return [(str(element[0]), str(element[1])) for element in elements]


def prepare_database_relationships(database: Any) -> None:
    for table in database._table_map.name_to_data.values():
        table.relationships = database.get(table)
