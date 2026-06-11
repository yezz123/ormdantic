"""Schema descriptor helpers for the Rust runtime."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from decimal import Decimal
from enum import Enum
from typing import Any, get_origin

from pydantic import BaseModel

from ormdantic._introspect import (
    FieldMetadata,
    is_dict_annotation,
    is_list_annotation,
    model_field,
    model_fields,
)
from ormdantic.errors import TypeConversionError
from ormdantic.models import (
    Map,
    TableCheck,
    TableColumn,
    TableExclusion,
    TableForeignKey,
    TableIndex,
    TableUnique,
)
from ormdantic.naming import snake_case

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None

RuntimeIndexDescriptor = tuple[
    str,
    list[str],
    bool,
    str | None,
    list[str],
    str | None,
    list[str],
    list[tuple[str, str]],
    str | None,
    str | None,
    str | None,
    bool,
    str | None,
    str | None,
    dict[str, int],
    str | None,
    dict[str, str],
    bool | None,
    bool,
    int | bool | None,
    bool,
]
RuntimeTableCheckDescriptor = tuple[str, str, bool, bool, str | None]
RuntimeUniqueConstraintDescriptor = tuple[
    str,
    list[str],
    bool | None,
    bool,
    bool,
    str | None,
    str | None,
    bool | None,
    str | None,
    list[str],
    str | None,
    int | bool | None,
]
RuntimeForeignKeyConstraintDescriptor = tuple[
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
    str | None,
]
RuntimeExclusionElementDescriptor = tuple[str, str]
RuntimeExclusionConstraintDescriptor = tuple[
    str,
    list[RuntimeExclusionElementDescriptor],
    list[RuntimeExclusionElementDescriptor],
    str,
    str | None,
    bool | None,
    bool,
    dict[str, str],
    str | None,
]
RuntimeEnumTypeDescriptor = tuple[str, list[str], str | None, str | None]
RuntimeConstraintTimingDescriptor = tuple[bool | None, bool]
RuntimeSqliteColumnConflictDescriptor = tuple[str | None, str | None, str | None]
RuntimeColumnTailDescriptor = tuple[
    RuntimeConstraintTimingDescriptor | None,
    str | None,
    RuntimeSqliteColumnConflictDescriptor | None,
]
RuntimeTableOptionsDescriptor = tuple[
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
RuntimeColumnDescriptor = tuple[
    str,
    str,
    bool,
    bool,
    str | None,
    str | None,
    int | None,
    bool,
    list[tuple[str, str, str]],
    tuple[
        str | None,
        str | None,
        bool,
        bool,
        str | None,
        int | None,
        int | None,
        tuple[
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
        | None,
        str | None,
        str | None,
        str | None,
        RuntimeColumnTailDescriptor | None,
    ],
]


def validate_table_map(table_map: Map) -> int | None:
    """Validate current Python table metadata through Rust when available."""
    if _ormdantic is None or not hasattr(_ormdantic, "validate_schema_tables"):
        return None
    tables = []
    for table in table_map.name_to_data.values():
        columns = [
            column_descriptor(table_map, table, field_name, field)
            for field_name, field in model_fields(table.model).items()
            if field_name not in table.back_references
        ]
        relationships = []
        for field_name, relationship in table.relationships.items():
            related = table_map.name_to_data[relationship.foreign_table]
            relationships.append(
                (
                    field_name,
                    relationship.foreign_table,
                    related.pk,
                    relationship.back_references,
                )
            )
        tables.append(
            (
                table.model.__name__,
                table.tablename,
                table.pk,
                columns,
                rust_index_descriptors(table),
                table.unique_constraints,
                rust_unique_constraint_descriptors(table),
                rust_table_check_descriptors(table),
                rust_foreign_key_constraint_descriptors(table),
                rust_exclusion_constraint_descriptors(table),
                (
                    table.comment,
                    table.tablespace,
                    table.mysql_engine,
                    table.mysql_charset,
                    table.mysql_collation,
                    table.mysql_row_format,
                    list(table.postgres_inherits),
                    list(table.postgres_with),
                    table.postgres_using,
                    table.postgres_partition_by,
                    table.postgres_partition_of,
                    table.postgres_partition_for,
                    table.postgres_unlogged,
                    table.sqlite_strict,
                    table.sqlite_without_rowid,
                    table.schema_name,
                    _table_uses_mssql_clustered_index(table),
                    oracle_index_compress_runtime(table.oracle_compress),
                    table.mysql_key_block_size,
                    table.mysql_pack_keys,
                    table.mysql_checksum,
                    table.mysql_delay_key_write,
                    table.mysql_stats_persistent,
                    table.mysql_stats_auto_recalc,
                    table.mysql_stats_sample_pages,
                    table.mysql_avg_row_length,
                    table.mysql_max_rows,
                    table.mysql_min_rows,
                    table.mysql_insert_method,
                    table.mysql_data_directory,
                    table.mysql_index_directory,
                    table.mysql_connection,
                    list(table.mysql_union),
                    table.mysql_partition_by,
                    table.mysql_partitions,
                    table.mysql_subpartition_by,
                    table.mysql_subpartitions,
                    table.mysql_auto_increment,
                ),
                relationships,
            )
        )
    return int(_ormdantic.validate_schema_tables(tables))


def compile_create_table_sql(table_map: Map, tablename: str, dialect: str) -> list[str]:
    """Compile create-table DDL statements for a registered table."""
    rust = _require_schema_symbol("compile_create_table_sql")
    table = table_map.name_to_data[tablename]
    native_enum_types = dialect.lower().startswith(("postgres", "postgresql"))
    columns = [
        column_descriptor(
            table_map,
            table,
            field_name,
            field,
            native_enum_types=native_enum_types,
        )
        for field_name, field in model_fields(table.model).items()
        if field_name not in table.back_references
    ]

    def compile_with_tablespace(tablespace: str | None) -> list[str]:
        return list(
            rust.compile_create_table_sql(
                dialect,
                table.tablename,
                columns,
                rust_index_descriptors(table),
                table.unique_constraints,
                rust_unique_constraint_descriptors(table),
                rust_table_check_descriptors(table),
                rust_foreign_key_constraint_descriptors(table),
                rust_exclusion_constraint_descriptors(table),
                table.comment,
                tablespace,
                table.mysql_engine,
                table.mysql_charset,
                table.mysql_collation,
                table.mysql_row_format,
                list(table.postgres_inherits),
                list(table.postgres_with),
                table.postgres_using,
                table.postgres_partition_by,
                table.postgres_partition_of,
                table.postgres_partition_for,
                table.postgres_unlogged,
                table.sqlite_strict,
                table.sqlite_without_rowid,
                table.schema_name,
                _table_uses_mssql_clustered_index(table),
                oracle_index_compress_runtime(table.oracle_compress),
                table.mysql_key_block_size,
                table.mysql_pack_keys,
                table.mysql_checksum,
                table.mysql_delay_key_write,
                table.mysql_stats_persistent,
                table.mysql_stats_auto_recalc,
                table.mysql_stats_sample_pages,
                table.mysql_avg_row_length,
                table.mysql_max_rows,
                table.mysql_min_rows,
                table.mysql_insert_method,
                table.mysql_data_directory,
                table.mysql_index_directory,
                table.mysql_connection,
                list(table.mysql_union),
                table.mysql_partition_by,
                table.mysql_partitions,
                table.mysql_subpartition_by,
                table.mysql_subpartitions,
                table.mysql_auto_increment,
            )
        )

    try:
        statements = compile_with_tablespace(table.tablespace)
    except ValueError as exc:
        statements = _compile_mssql_filegroup_create_fallback(
            dialect,
            table,
            exc,
            compile_with_tablespace,
        )
    statements = _compile_mssql_clustered_index_sql(dialect, table, statements)
    statements = _compile_mssql_index_filegroup_sql(dialect, table, statements)
    statements = _compile_oracle_index_options_sql(dialect, table, statements)
    statements = _compile_postgres_index_ops_sql(dialect, table, statements)
    statements = _compile_postgres_index_nulls_not_distinct_sql(
        dialect,
        table,
        statements,
    )
    statements = _compile_mysql_index_prefix_sql(dialect, table, statements)
    statements = _compile_mysql_index_using_sql(dialect, table, statements)
    statements = _compile_mysql_index_length_sql(dialect, table, statements)
    statements = _compile_mysql_index_visibility_sql(dialect, table, statements)
    statements = _compile_postgres_unique_include_sql(dialect, table, statements)
    statements = _compile_inline_index_comment_sql(dialect, table, statements)
    statements.extend(_compile_constraint_comment_sql(dialect, table))
    statements.extend(_compile_index_tablespace_sql(dialect, table))
    statements.extend(_compile_index_comment_sql(dialect, table))
    return statements


def _table_uses_mssql_clustered_index(table: Any) -> bool:
    return any(index.mssql_clustered for index in table.indexes) or any(
        constraint.mssql_clustered is True
        for constraint in table.named_unique_constraints
    )


def _compile_mssql_filegroup_create_fallback(
    dialect: str,
    table: Any,
    error: ValueError,
    compile_with_tablespace: Callable[[str | None], list[str]],
) -> list[str]:
    from ormdantic._migrations.sql import dialect_name, quote_ident

    if (
        dialect_name(dialect) != "mssql"
        or table.tablespace is None
        or "table tablespace" not in str(error)
    ):
        raise error
    statements = compile_with_tablespace(None)
    if not statements:
        return statements
    statements[0] = f"{statements[0]} ON {quote_ident(dialect, table.tablespace)}"
    return statements


def _compile_inline_index_comment_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.comment is not None for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_inline_index_comment_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(index.name, list(index.columns), comment=index.comment)
            for index in table.indexes
        ],
    )
    return _annotate_inline_index_comment_sql(dialect, statements, [snapshot_table])


def _compile_mssql_index_filegroup_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.mssql_filegroup is not None for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_mssql_index_filegroup_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                mssql_filegroup=index.mssql_filegroup,
            )
            for index in table.indexes
        ],
    )
    return _annotate_mssql_index_filegroup_sql(dialect, statements, [snapshot_table])


def _compile_mssql_clustered_index_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.mssql_clustered for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_mssql_clustered_index_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                mssql_clustered=index.mssql_clustered,
            )
            for index in table.indexes
        ],
    )
    return _annotate_mssql_clustered_index_sql(dialect, statements, [snapshot_table])


def _compile_oracle_index_tablespace_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    return _compile_oracle_index_options_sql(dialect, table, statements)


def _compile_oracle_index_options_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(
        index.oracle_tablespace is not None
        or index.oracle_bitmap
        or index.oracle_compress is not None
        for index in table.indexes
    ):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_oracle_index_options_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                oracle_tablespace=index.oracle_tablespace,
                oracle_bitmap=index.oracle_bitmap,
                oracle_compress=index.oracle_compress,
            )
            for index in table.indexes
        ],
    )
    return _annotate_oracle_index_options_sql(dialect, statements, [snapshot_table])


def _compile_postgres_index_ops_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.postgres_ops for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_postgres_index_ops_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                expressions=list(index.expressions),
                postgres_ops=dict(index.postgres_ops),
            )
            for index in table.indexes
        ],
    )
    return _annotate_postgres_index_ops_sql(dialect, statements, [snapshot_table])


def _compile_postgres_index_nulls_not_distinct_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.postgres_nulls_not_distinct for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import (
        _annotate_postgres_index_nulls_not_distinct_sql,
    )

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                unique=index.unique,
                include_columns=list(index.include_columns),
                postgres_nulls_not_distinct=index.postgres_nulls_not_distinct,
            )
            for index in table.indexes
        ],
    )
    return _annotate_postgres_index_nulls_not_distinct_sql(
        dialect,
        statements,
        [snapshot_table],
    )


def _compile_mysql_index_length_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.mysql_length for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_mysql_index_length_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                mysql_length=dict(index.mysql_length),
            )
            for index in table.indexes
        ],
    )
    return _annotate_mysql_index_length_sql(dialect, statements, [snapshot_table])


def _compile_mysql_index_prefix_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.mysql_prefix is not None for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_mysql_index_prefix_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                mysql_prefix=index.mysql_prefix,
            )
            for index in table.indexes
        ],
    )
    return _annotate_mysql_index_prefix_sql(dialect, statements, [snapshot_table])


def _compile_mysql_index_using_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.mysql_using is not None for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_mysql_index_using_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                mysql_using=index.mysql_using,
            )
            for index in table.indexes
        ],
    )
    return _annotate_mysql_index_using_sql(dialect, statements, [snapshot_table])


def _compile_mysql_index_visibility_sql(
    dialect: str, table: Any, statements: list[str]
) -> list[str]:
    if not any(index.mysql_visible is not None for index in table.indexes):
        return statements
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _annotate_mysql_index_visibility_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        indexes=[
            IndexSnapshot(
                index.name,
                list(index.columns),
                mysql_visible=index.mysql_visible,
            )
            for index in table.indexes
        ],
    )
    return _annotate_mysql_index_visibility_sql(dialect, statements, [snapshot_table])


def _compile_constraint_comment_sql(dialect: str, table: Any) -> list[str]:
    constraints = (
        list(table.named_unique_constraints)
        + list(table.check_constraints)
        + list(table.foreign_key_constraints)
        + list(table.exclusion_constraints)
    )
    if not any(constraint.comment is not None for constraint in constraints):
        return []
    from ormdantic._migrations.models import TableSnapshot
    from ormdantic._migrations.planning import _set_constraint_comment_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
    )
    statements: list[str] = []
    for constraint in constraints:
        if constraint.comment is None:
            continue
        comment_sql = _set_constraint_comment_sql(
            dialect,
            snapshot_table,
            constraint.name,
            constraint.comment,
            for_create=True,
        )
        if comment_sql is not None:
            statements.append(comment_sql)
    return statements


def _compile_postgres_unique_include_sql(
    dialect: str,
    table: Any,
    statements: list[str],
) -> list[str]:
    if not any(
        constraint.postgres_include for constraint in table.named_unique_constraints
    ):
        return statements
    from ormdantic._migrations.models import TableSnapshot, UniqueConstraintSnapshot
    from ormdantic._migrations.planning import _annotate_postgres_unique_include_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
        named_unique_constraints=[
            UniqueConstraintSnapshot(
                constraint.name,
                list(constraint.columns),
                constraint.deferrable,
                constraint.initially_deferred,
                constraint.nulls_not_distinct,
                constraint.sqlite_on_conflict,
                constraint.mssql_filegroup,
                constraint.mssql_clustered,
                constraint.comment,
                list(constraint.postgres_include),
                constraint.oracle_tablespace,
                constraint.oracle_compress,
            )
            for constraint in table.named_unique_constraints
        ],
    )
    return _annotate_postgres_unique_include_sql(dialect, statements, [snapshot_table])


def _compile_index_tablespace_sql(dialect: str, table: Any) -> list[str]:
    if not any(index.postgres_tablespace is not None for index in table.indexes):
        return []
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _set_index_tablespace_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
    )
    statements: list[str] = []
    for index in table.indexes:
        if index.postgres_tablespace is None:
            continue
        tablespace_sql = _set_index_tablespace_sql(
            dialect,
            snapshot_table,
            IndexSnapshot(
                index.name,
                list(index.columns),
                postgres_tablespace=index.postgres_tablespace,
            ),
            for_create=True,
        )
        if tablespace_sql is not None:
            statements.append(tablespace_sql)
    return statements


def _compile_index_comment_sql(dialect: str, table: Any) -> list[str]:
    if not any(index.comment is not None for index in table.indexes):
        return []
    if dialect.lower() in {"mysql", "mariadb"}:
        return []
    from ormdantic._migrations.models import IndexSnapshot, TableSnapshot
    from ormdantic._migrations.planning import _set_index_comment_sql

    snapshot_table = TableSnapshot(
        model_key=str(table.model.__name__),
        name=table.tablename,
        primary_key=table.pk,
        schema=table.schema_name,
    )
    statements: list[str] = []
    for index in table.indexes:
        if index.comment is None:
            continue
        comment_sql = _set_index_comment_sql(
            dialect,
            snapshot_table,
            IndexSnapshot(index.name, list(index.columns), comment=index.comment),
            for_create=True,
        )
        if comment_sql is not None:
            statements.append(comment_sql)
    return statements


def compile_drop_table_sql(tablename: str, dialect: str) -> str:
    """Compile a drop-table DDL statement for a table name."""
    rust = _require_schema_symbol("compile_drop_table_sql")
    return str(rust.compile_drop_table_sql(dialect, tablename))


def column_descriptor(
    table_map: Map,
    table: Any,
    field_name: str,
    field: FieldMetadata,
    *,
    native_enum_types: bool = False,
    enum_schema: str | None = None,
) -> RuntimeColumnDescriptor:
    """Return a compact Rust schema descriptor for one model field."""
    relationship = table.relationships.get(field_name)
    related_table = (
        table_map.name_to_data[relationship.foreign_table] if relationship else None
    )
    foreign_table = _qualified_table_name(related_table) if related_table else None
    foreign_column = related_table.pk if related_table else None
    related_pk_field = (
        model_field(related_table.model, related_table.pk) if related_table else None
    )
    max_length: int | None = field.max_length
    if max_length is None and related_pk_field is not None:
        max_length = related_pk_field.max_length
    options: TableColumn = table.column_options.get(field_name, TableColumn())
    if foreign_table is None and options.has_foreign_key_options:
        raise ValueError(
            f"foreign key options for table '{table.tablename}' field "
            f"'{field_name}' require a relationship field"
        )
    validate_enum_type_options(table.tablename, field_name, field, options)
    kind_field = related_pk_field or field
    numeric_precision, numeric_scale = numeric_shape_options(kind_field, options)
    return (
        field_name,
        field_kind(
            kind_field,
            native_enum_types=native_enum_types,
            options=options,
            enum_schema=enum_schema,
        ),
        bool(field_name != table.pk and not field.required),
        bool(field_name == table.pk),
        foreign_table,
        foreign_column,
        max_length,
        bool(field_name in table.unique),
        check_constraints(field_name, field),
        (
            options.server_default,
            options.computed,
            options.computed_persisted,
            options.autoincrement,
            options.collation,
            numeric_precision,
            numeric_scale,
            (
                (
                    options.identity_always,
                    options.identity_start,
                    options.identity_increment,
                    options.identity_min_value,
                    options.identity_max_value,
                    options.identity_cycle,
                    options.identity_cache,
                    options.identity_order,
                    options.identity_on_null,
                    options.identity_no_min_value,
                    options.identity_no_max_value,
                )
                if options.has_identity
                else None
            ),
            options.foreign_key_name,
            options.on_delete,
            options.on_update,
            (
                (
                    (options.deferrable, options.initially_deferred)
                    if options.deferrable is not None or options.initially_deferred
                    else None
                ),
                options.comment,
                sqlite_column_conflict_descriptor(options),
            ),
        ),
    )


def numeric_shape_options(
    field: FieldMetadata,
    options: TableColumn,
) -> tuple[int | None, int | None]:
    """Return explicit or Pydantic-inferred numeric precision and scale."""
    if options.numeric_precision is not None or options.numeric_scale is not None:
        return options.numeric_precision, options.numeric_scale
    if field.max_digits is None or field.decimal_places is None:
        return None, None
    return int(field.max_digits), int(field.decimal_places)


def sqlite_column_conflict_descriptor(
    options: TableColumn,
) -> RuntimeSqliteColumnConflictDescriptor | None:
    """Return SQLite column conflict policies for the runtime descriptor."""
    if (
        options.sqlite_on_conflict_primary_key is None
        and options.sqlite_on_conflict_not_null is None
        and options.sqlite_on_conflict_unique is None
    ):
        return None
    return (
        options.sqlite_on_conflict_primary_key,
        options.sqlite_on_conflict_not_null,
        options.sqlite_on_conflict_unique,
    )


def enum_type_descriptors(
    table_map: Map,
    *,
    schema: str | None = None,
) -> list[RuntimeEnumTypeDescriptor]:
    """Return native enum type descriptors inferred from string-valued Enum fields."""
    enum_types: dict[tuple[str | None, str], RuntimeEnumTypeDescriptor] = {}
    for table in table_map.name_to_data.values():
        for field_name, field in model_fields(table.model).items():
            if field_name in table.back_references:
                continue
            enum_type = native_enum_annotation(field)
            if enum_type is None:
                continue
            options = table.column_options.get(field_name, TableColumn())
            descriptor = enum_type_descriptor(
                enum_type,
                options=options,
                schema=schema,
            )
            key = enum_type_key(descriptor)
            existing = enum_types.get(key)
            if existing is not None:
                existing_name, existing_values, existing_schema, existing_comment = (
                    existing
                )
                name, values, schema, comment = descriptor
                if existing_values != values:
                    raise ValueError(
                        f"duplicate native enum type name "
                        f"'{enum_type_qualified_name(descriptor)}' "
                        "with different values"
                    )
                if (
                    existing_comment is not None
                    and comment is not None
                    and existing_comment != comment
                ):
                    raise ValueError(
                        f"duplicate native enum type name "
                        f"'{enum_type_qualified_name(descriptor)}' "
                        "with different comments"
                    )
                enum_types[key] = (
                    existing_name,
                    existing_values,
                    existing_schema,
                    existing_comment if existing_comment is not None else comment,
                )
                continue
            enum_types[key] = descriptor
    return list(enum_types.values())


def enum_type_descriptor(
    enum_type: type[Enum],
    *,
    options: TableColumn | None = None,
    schema: str | None = None,
) -> RuntimeEnumTypeDescriptor:
    """Return a native enum type descriptor for a Python Enum class."""
    return (
        enum_type_name(enum_type, options=options),
        [str(member.value) for member in enum_type],
        enum_type_schema(options, fallback=schema),
        options.enum_type_comment if options is not None else None,
    )


def enum_type_name(enum_type: type[Enum], *, options: TableColumn | None = None) -> str:
    """Return the default database enum type name for a Python Enum class."""
    if options is not None and options.enum_type_name is not None:
        return options.enum_type_name
    return snake_case(enum_type.__name__)


def enum_type_schema(
    options: TableColumn | None,
    *,
    fallback: str | None = None,
) -> str | None:
    """Return the configured enum schema, falling back to the snapshot schema."""
    if options is not None and options.enum_schema is not None:
        return options.enum_schema
    return fallback


def enum_type_key(descriptor: RuntimeEnumTypeDescriptor) -> tuple[str | None, str]:
    """Return a stable key for a native enum type descriptor."""
    return descriptor[2], descriptor[0]


def enum_type_qualified_name(descriptor: RuntimeEnumTypeDescriptor) -> str:
    """Return a display name for a native enum type descriptor."""
    name, _values, schema, _comment = descriptor
    if schema is None:
        return name
    return f"{schema}.{name}"


def index_descriptors(
    table: Any,
) -> list[RuntimeIndexDescriptor]:
    """Return compact index descriptors for snapshots."""
    indexes: list[RuntimeIndexDescriptor] = []
    seen: set[str] = set()

    def append_index(index: RuntimeIndexDescriptor) -> None:
        name = index[0]
        if name in seen:
            raise ValueError(
                f"duplicate index name '{name}' on table '{table.tablename}'"
            )
        seen.add(name)
        indexes.append(index)

    for column in table.indexed:
        append_index(
            (
                f"{table.tablename}_{column}_idx",
                [column],
                False,
                None,
                [],
                None,
                [],
                [],
                None,
                None,
                None,
                False,
                None,
                None,
                {},
                None,
                {},
                None,
                False,
                None,
                False,
            )
        )
    for column in table.unique:
        append_index(
            (
                f"{table.tablename}_{column}_unique_idx",
                [column],
                True,
                None,
                [],
                None,
                [],
                [],
                None,
                None,
                None,
                False,
                None,
                None,
                {},
                None,
                {},
                None,
                False,
                None,
                False,
            )
        )
    for index in table.indexes:
        append_index(table_index_descriptor(index))
    return indexes


def rust_index_descriptors(table: Any) -> list[tuple[Any, ...]]:
    """Return index descriptors without Python-only metadata."""
    return [index[:8] for index in index_descriptors(table)]


def table_index_descriptor(index: TableIndex) -> RuntimeIndexDescriptor:
    """Return a snapshot descriptor for an explicit table index."""
    return (
        index.name,
        list(index.columns),
        index.unique,
        index.where,
        list(index.include_columns),
        index.method,
        list(index.expressions),
        list(index.postgres_with),
        index.comment,
        index.postgres_tablespace,
        index.mssql_filegroup,
        index.mssql_clustered,
        index.oracle_tablespace,
        index.mysql_prefix,
        dict(index.mysql_length),
        index.mysql_using,
        dict(index.postgres_ops),
        index.mysql_visible,
        index.oracle_bitmap,
        index.oracle_compress,
        index.postgres_nulls_not_distinct,
    )


def _qualified_table_name(table: Any) -> str:
    schema = getattr(table, "schema_name", None)
    name = table.tablename
    if schema:
        return f"{schema}.{name}"
    return name


def table_check_descriptors(table: Any) -> list[RuntimeTableCheckDescriptor]:
    """Return check-constraint descriptors for snapshots."""
    return [table_check_descriptor(check) for check in table.check_constraints]


def rust_table_check_descriptors(table: Any) -> list[tuple[Any, ...]]:
    """Return check-constraint descriptors without Python-only metadata."""
    return [check[:4] for check in table_check_descriptors(table)]


def table_check_descriptor(check: TableCheck) -> RuntimeTableCheckDescriptor:
    """Return a snapshot descriptor for an explicit table check."""
    return (
        check.name,
        check.expression,
        check.validated,
        check.no_inherit,
        check.comment,
    )


def foreign_key_constraint_descriptors(
    table: Any,
) -> list[RuntimeForeignKeyConstraintDescriptor]:
    """Return descriptors for table-level FOREIGN KEY constraints."""
    constraints: list[RuntimeForeignKeyConstraintDescriptor] = []
    seen = set()
    for field_name in table.relationships:
        options = table.column_options.get(field_name, TableColumn())
        seen.add(
            options.foreign_key_name or f"{table.tablename}_{field_name}_foreign_key"
        )
    for constraint in table.foreign_key_constraints:
        descriptor = foreign_key_constraint_descriptor(constraint)
        if descriptor[0] in seen:
            raise ValueError(
                f"duplicate foreign key constraint name '{descriptor[0]}' "
                f"on table '{table.tablename}'"
            )
        seen.add(descriptor[0])
        constraints.append(descriptor)
    return constraints


def rust_foreign_key_constraint_descriptors(table: Any) -> list[tuple[Any, ...]]:
    """Return foreign-key descriptors without Python-only metadata."""
    return [constraint[:10] for constraint in foreign_key_constraint_descriptors(table)]


def foreign_key_constraint_descriptor(
    constraint: TableForeignKey,
) -> RuntimeForeignKeyConstraintDescriptor:
    """Return a snapshot descriptor for an explicit foreign key constraint."""
    return (
        constraint.name,
        list(constraint.columns),
        constraint.foreign_table,
        list(constraint.foreign_columns),
        constraint.on_delete,
        constraint.on_update,
        constraint.deferrable,
        constraint.initially_deferred,
        constraint.validated,
        constraint.match,
        constraint.comment,
    )


def exclusion_constraint_descriptors(
    table: Any,
) -> list[RuntimeExclusionConstraintDescriptor]:
    """Return descriptors for PostgreSQL EXCLUDE constraints."""
    constraints: list[RuntimeExclusionConstraintDescriptor] = []
    seen = {
        constraint.name
        for constraint in (
            list(table.named_unique_constraints)
            + list(table.check_constraints)
            + list(table.foreign_key_constraints)
        )
    }
    for field_name in table.relationships:
        options = table.column_options.get(field_name, TableColumn())
        seen.add(
            options.foreign_key_name or f"{table.tablename}_{field_name}_foreign_key"
        )
    for constraint in table.exclusion_constraints:
        descriptor = exclusion_constraint_descriptor(constraint)
        if descriptor[0] in seen:
            raise ValueError(
                f"duplicate exclusion constraint name '{descriptor[0]}' "
                f"on table '{table.tablename}'"
            )
        seen.add(descriptor[0])
        constraints.append(descriptor)
    return constraints


def rust_exclusion_constraint_descriptors(table: Any) -> list[tuple[Any, ...]]:
    """Return exclusion-constraint descriptors without Python-only metadata."""
    return [constraint[:8] for constraint in exclusion_constraint_descriptors(table)]


def exclusion_constraint_descriptor(
    constraint: TableExclusion,
) -> RuntimeExclusionConstraintDescriptor:
    """Return a snapshot descriptor for an explicit exclusion constraint."""
    return (
        constraint.name,
        list(constraint.columns),
        list(constraint.expressions),
        constraint.using,
        constraint.where,
        constraint.deferrable,
        constraint.initially_deferred,
        dict(constraint.ops),
        constraint.comment,
    )


def unique_constraint_descriptors(
    table: Any,
) -> list[RuntimeUniqueConstraintDescriptor]:
    """Return descriptors for named table UNIQUE constraints."""
    constraints: list[RuntimeUniqueConstraintDescriptor] = []
    auto_unique_count = len(table.unique_constraints) + len(table.unique)
    seen = {f"{table.tablename}_unique_{idx}" for idx in range(auto_unique_count)}
    for constraint in table.named_unique_constraints:
        descriptor = unique_constraint_descriptor(constraint)
        if descriptor[0] in seen:
            raise ValueError(
                f"duplicate unique constraint name '{descriptor[0]}' "
                f"on table '{table.tablename}'"
            )
        seen.add(descriptor[0])
        constraints.append(descriptor)
    return constraints


def rust_unique_constraint_descriptors(table: Any) -> list[tuple[Any, ...]]:
    """Return unique-constraint descriptors without Python-only metadata."""
    return [
        (
            constraint[0],
            constraint[1],
            constraint[2],
            constraint[3],
            constraint[4],
            constraint[5],
            constraint[6],
            constraint[7],
            constraint[10],
            oracle_index_compress_runtime(constraint[11]),
        )
        for constraint in unique_constraint_descriptors(table)
    ]


def oracle_index_compress_runtime(value: int | bool | None) -> str | None:
    """Return a compact Oracle index compression token for the Rust bridge."""
    if value is None or value is False:
        return None
    if value is True:
        return "true"
    return str(value)


def unique_constraint_descriptor(
    constraint: TableUnique,
) -> RuntimeUniqueConstraintDescriptor:
    """Return a snapshot descriptor for an explicit unique constraint."""
    return (
        constraint.name,
        list(constraint.columns),
        constraint.deferrable,
        constraint.initially_deferred,
        constraint.nulls_not_distinct,
        constraint.sqlite_on_conflict,
        constraint.mssql_filegroup,
        constraint.mssql_clustered,
        constraint.comment,
        list(constraint.postgres_include),
        constraint.oracle_tablespace,
        constraint.oracle_compress,
    )


def field_kind(
    field: FieldMetadata,
    *,
    native_enum_types: bool = False,
    options: TableColumn | None = None,
    enum_schema: str | None = None,
) -> str:
    """Map a Pydantic field to a Rust schema field kind."""
    annotation = field.annotation
    if get_origin(annotation) is Callable or annotation is Callable:
        raise TypeConversionError(annotation)
    if is_dict_annotation(annotation):
        return "dict"
    if is_list_annotation(annotation):
        return "list"
    if annotation.__class__.__name__ == "UnionType" or getattr(
        annotation, "__origin__", None
    ):
        for arg in field.args:
            if arg is type(None):
                continue
            if is_dict_annotation(arg):
                return "dict"
            if is_list_annotation(arg):
                return "list"
            if isinstance(arg, type) and issubclass(arg, BaseModel):
                return "uuid"
            if isinstance(arg, type):
                annotation = arg
                break
    if getattr(annotation, "__name__", "") == "UUID":
        return "uuid"
    if annotation is str:
        return "str"
    if annotation is int:
        return "int"
    if annotation is float:
        return "float"
    if annotation is bool:
        return "bool"
    if annotation is Decimal:
        return "decimal"
    if annotation is bytes:
        return "bytes"
    if getattr(annotation, "__name__", "") == "date":
        return "date"
    if getattr(annotation, "__name__", "") == "datetime":
        return "datetime"
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return enum_field_kind(
            annotation,
            native_enum_types=native_enum_types,
            options=options,
            schema=enum_schema,
        )
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return "model_json"
    return "json"


def enum_field_kind(
    enum_type: type[Enum],
    *,
    native_enum_types: bool = False,
    options: TableColumn | None = None,
    schema: str | None = None,
) -> str:
    """Return the runtime field kind for an Enum annotation."""
    if native_enum_types and native_enum_type_supported(enum_type):
        name = enum_type_name(enum_type, options=options)
        schema = enum_type_schema(options, fallback=schema)
        if schema is not None:
            return f"enum:{schema}.{name}"
        return f"enum:{name}"
    return "enum"


def validate_enum_type_options(
    table_name: str,
    field_name: str,
    field: FieldMetadata,
    options: TableColumn,
) -> None:
    """Validate that native enum options are attached to supported Enum fields."""
    if not options.has_enum_type_options or native_enum_annotation(field) is not None:
        return
    raise ValueError(
        f"native enum options for table '{table_name}' field '{field_name}' "
        "require a string-valued Enum field"
    )


def check_constraints(
    field_name: str, field: FieldMetadata
) -> list[tuple[str, str, str]]:
    """Return structured DDL check constraints for a Pydantic field."""
    checks = []
    if enum_type := enum_annotation(field):
        checks.append(("enum", "in", enum_values_sql(enum_type)))
    if field.ge is not None:
        checks.append(("comparison", ">=", str(field.ge)))
    if field.gt is not None:
        checks.append(("comparison", ">", str(field.gt)))
    if field.le is not None:
        checks.append(("comparison", "<=", str(field.le)))
    if field.lt is not None:
        checks.append(("comparison", "<", str(field.lt)))
    if field.min_length is not None:
        checks.append(("length", ">=", str(field.min_length)))
    if field.max_length is not None:
        checks.append(("length", "<=", str(field.max_length)))
    if field.pattern is not None:
        pattern = getattr(field.pattern, "pattern", field.pattern)
        checks.append(("pattern", "matches", sql_literal(pattern)))
    if field.multiple_of is not None:
        checks.append(("multiple_of", "=", sql_literal(field.multiple_of)))
    return checks


def enum_annotation(field: FieldMetadata) -> type[Enum] | None:
    """Return the Enum annotation for a field when present."""
    candidates = (field.annotation, *field.args)
    for annotation in candidates:
        if isinstance(annotation, type) and issubclass(annotation, Enum):
            return annotation
    return None


def native_enum_annotation(field: FieldMetadata) -> type[Enum] | None:
    """Return a string-valued Enum annotation suitable for native enum objects."""
    enum_type = enum_annotation(field)
    if enum_type is None or not native_enum_type_supported(enum_type):
        return None
    return enum_type


def native_enum_type_supported(enum_type: type[Enum]) -> bool:
    """Return whether an Enum can be represented as a native database enum type."""
    return all(isinstance(member.value, str) for member in enum_type)


def enum_values_sql(enum_type: type[Enum]) -> str:
    """Return SQL literals for an enum CHECK IN list."""
    return ", ".join(sql_literal(member.value) for member in enum_type)


def sql_literal(value: Any) -> str:
    """Return a compact SQL literal for generated CHECK expressions."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _require_schema_symbol(symbol: str) -> Any:
    if _ormdantic is None or not hasattr(_ormdantic, symbol):
        raise RuntimeError(
            "Ormdantic requires the Rust extension for schema compilation. "
            "Install the package with maturin or reinstall the wheel."
        )
    return _ormdantic
