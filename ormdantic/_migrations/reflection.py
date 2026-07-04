"""Live schema reflection helpers for migration autogeneration."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from typing import Any

from ormdantic._migrations.history import MIGRATION_TABLE
from ormdantic._migrations.models import (
    MIGRATION_ARTIFACT_VERSION,
    ColumnSnapshot,
    EnumTypeSnapshot,
    ExclusionConstraintSnapshot,
    ForeignKeyConstraintSnapshot,
    IndexSnapshot,
    NamespaceSnapshot,
    RuntimeCheck,
    SchemaSnapshot,
    SequenceSnapshot,
    TableCheckSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
    ViewSnapshot,
    normalized_view_definition,
)
from ormdantic._migrations.models import (
    optional_bool as _optional_bool,
)
from ormdantic._migrations.models import (
    optional_int as _optional_int,
)
from ormdantic._migrations.models import (
    optional_str as _optional_str,
)
from ormdantic._migrations.models import (
    oracle_index_compress as _oracle_index_compress,
)
from ormdantic._migrations.sql import (
    db_truthy as _db_truthy,
)
from ormdantic._migrations.sql import (
    dialect_name as _dialect_name,
)
from ormdantic._migrations.sql import (
    query_rows_url as _query_rows_url,
)
from ormdantic._migrations.sql import (
    sql_literal as _sql_literal,
)
from ormdantic._migrations.sql import (
    table_matches_filters as _table_matches_filters,
)
from ormdantic._native import import_native_extension

ForeignKeyReflection = dict[str, str | bool | None]
KeyConstraintReflection = tuple[
    str,
    list[str],
    bool | None,
    bool,
    bool,
    str | None,
    list[str],
    str | None,
    bool | None,
    str | None,
    int | bool | None,
]
MysqlTableOptionValue = str | int | bool | list[str] | None
MysqlTableOptions = Mapping[str, MysqlTableOptionValue]


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
    table_comments = _reflect_server_table_comments(rust, url, dialect, schema, tables)
    column_comments = _reflect_server_column_comments(
        rust, url, dialect, schema, tables
    )
    table_tablespaces = _reflect_server_table_tablespaces(
        rust, url, dialect, schema, tables
    )
    oracle_table_compressions = _reflect_server_oracle_table_compressions(
        rust, url, dialect, schema, tables
    )
    postgres_inherits = _reflect_server_postgres_inherits(
        rust, url, dialect, schema, tables
    )
    postgres_with = _reflect_server_postgres_with(rust, url, dialect, schema, tables)
    postgres_using = _reflect_server_postgres_using(rust, url, dialect, schema, tables)
    postgres_unlogged = _reflect_server_postgres_unlogged(
        rust, url, dialect, schema, tables
    )
    postgres_partition_by = _reflect_server_postgres_partition_by(
        rust, url, dialect, schema, tables
    )
    postgres_partitions = _reflect_server_postgres_partitions(
        rust, url, dialect, schema, tables
    )
    mysql_defaults = _reflect_server_mysql_defaults(rust, url, dialect)
    mysql_table_options = _reflect_server_mysql_table_options(
        rust,
        url,
        dialect,
        schema,
        tables,
        defaults=mysql_defaults,
    )
    column_rows = _reflect_server_columns(rust, url, dialect, schema, tables)
    _normalize_mysql_default_column_collations(
        dialect,
        column_rows,
        mysql_table_options,
        mysql_defaults,
    )
    _normalize_mssql_default_column_collations(
        dialect,
        column_rows,
        _reflect_server_mssql_database_collation(rust, url, dialect),
    )
    _normalize_oracle_default_column_metadata(dialect, column_rows)
    primary_keys = _reflect_server_primary_keys(rust, url, dialect, schema, tables)
    unique_constraints = _reflect_server_unique_constraints(
        rust, url, dialect, schema, tables
    )
    reflected_foreign_key_constraints = _reflect_server_foreign_key_constraints(
        rust, url, dialect, schema, tables, include_single_column=True
    )
    foreign_keys, foreign_key_constraints = _split_reflected_foreign_key_constraints(
        reflected_foreign_key_constraints
    )
    exclusion_constraints = _reflect_server_exclusion_constraints(
        rust, url, dialect, schema, tables
    )
    indexes = _reflect_server_indexes(rust, url, dialect, schema, tables)
    check_constraints = _reflect_server_check_constraints(
        rust, url, dialect, schema, tables
    )
    scoped_tables = tables if include_tables or exclude_tables else None
    sequences = _reflect_server_sequences(rust, url, dialect, schema, scoped_tables)
    views = _reflect_server_views(
        rust,
        url,
        dialect,
        schema,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
    )
    enum_types = _reflect_server_enum_types(rust, url, dialect, schema, tables)
    extra_schemas: list[str] = []
    for enum_type in enum_types:
        if enum_type.schema is not None and enum_type.schema != schema:
            extra_schemas.append(enum_type.schema)
    for sequence in sequences:
        if sequence.schema is not None and sequence.schema != schema:
            extra_schemas.append(sequence.schema)
    for view in views:
        if view.schema is not None and view.schema != schema:
            extra_schemas.append(view.schema)
    namespaces = _reflect_server_namespaces(
        rust,
        url,
        dialect,
        schema,
        extra_schemas=extra_schemas,
    )
    snapshots: list[TableSnapshot] = []
    for table_name in tables:
        table_columns = column_rows.get(table_name, [])
        pk_columns = primary_keys.get(table_name, [])
        reflected_unique_constraints = unique_constraints.get(table_name, [])
        unique_columns, table_unique_constraints = _split_reflected_unique_constraints(
            reflected_unique_constraints
        )
        table_foreign_keys = foreign_keys.get(table_name, {})
        columns = [
            reflected_column(
                column,
                pk_columns,
                unique_columns,
                table_foreign_keys,
                column_comments.get((table_name, column["name"])),
            )
            for column in table_columns
        ]
        columns, table_check_constraints = _normalize_generated_column_checks(
            dialect,
            table_name,
            columns,
            check_constraints.get(table_name, []),
        )
        primary_key = (
            pk_columns[0] if pk_columns else (columns[0].name if columns else "id")
        )
        table_mysql_options = mysql_table_options.get(table_name, {})
        snapshots.append(
            TableSnapshot(
                model_key=table_name,
                name=table_name,
                primary_key=primary_key,
                schema=schema,
                comment=table_comments.get(table_name),
                tablespace=table_tablespaces.get(table_name),
                mysql_engine=_mysql_option_str(table_mysql_options, "engine"),
                mysql_charset=_mysql_option_str(table_mysql_options, "charset"),
                mysql_collation=_mysql_option_str(table_mysql_options, "collation"),
                mysql_row_format=_mysql_option_str(table_mysql_options, "row_format"),
                mysql_key_block_size=_mysql_option_int(
                    table_mysql_options,
                    "key_block_size",
                ),
                mysql_pack_keys=_mysql_option_bool(table_mysql_options, "pack_keys"),
                mysql_checksum=_mysql_option_bool(table_mysql_options, "checksum"),
                mysql_delay_key_write=_mysql_option_bool(
                    table_mysql_options,
                    "delay_key_write",
                ),
                mysql_stats_persistent=_mysql_option_bool(
                    table_mysql_options,
                    "stats_persistent",
                ),
                mysql_stats_auto_recalc=_mysql_option_bool(
                    table_mysql_options,
                    "stats_auto_recalc",
                ),
                mysql_stats_sample_pages=_mysql_option_int(
                    table_mysql_options,
                    "stats_sample_pages",
                ),
                mysql_avg_row_length=_mysql_option_int(
                    table_mysql_options,
                    "avg_row_length",
                ),
                mysql_max_rows=_mysql_option_int(table_mysql_options, "max_rows"),
                mysql_min_rows=_mysql_option_int(table_mysql_options, "min_rows"),
                mysql_insert_method=_mysql_option_str(
                    table_mysql_options,
                    "insert_method",
                ),
                mysql_data_directory=_mysql_option_str(
                    table_mysql_options,
                    "data_directory",
                ),
                mysql_index_directory=_mysql_option_str(
                    table_mysql_options,
                    "index_directory",
                ),
                mysql_connection=_mysql_option_str(table_mysql_options, "connection"),
                mysql_union=_mysql_option_str_list(table_mysql_options, "union"),
                mysql_partition_by=_mysql_option_str(
                    table_mysql_options,
                    "partition_by",
                ),
                mysql_partitions=_mysql_option_int(table_mysql_options, "partitions"),
                mysql_subpartition_by=_mysql_option_str(
                    table_mysql_options,
                    "subpartition_by",
                ),
                mysql_subpartitions=_mysql_option_int(
                    table_mysql_options,
                    "subpartitions",
                ),
                mysql_auto_increment=_mysql_option_int(
                    table_mysql_options,
                    "auto_increment",
                ),
                oracle_compress=oracle_table_compressions.get(table_name),
                postgres_inherits=postgres_inherits.get(table_name, []),
                postgres_with=postgres_with.get(table_name, []),
                postgres_using=postgres_using.get(table_name),
                postgres_unlogged=postgres_unlogged.get(table_name, False),
                postgres_partition_by=postgres_partition_by.get(table_name),
                postgres_partition_of=postgres_partitions.get(table_name, (None, None))[
                    0
                ],
                postgres_partition_for=postgres_partitions.get(
                    table_name, (None, None)
                )[1],
                columns=columns,
                indexes=indexes.get(table_name, []),
                named_unique_constraints=table_unique_constraints,
                check_constraints=table_check_constraints,
                foreign_key_constraints=foreign_key_constraints.get(table_name, []),
                exclusion_constraints=exclusion_constraints.get(table_name, []),
                relationships=[],
            )
        )
    return SchemaSnapshot(
        tables=snapshots,
        namespaces=namespaces,
        enum_types=enum_types,
        sequences=sequences,
        views=views,
        version=MIGRATION_ARTIFACT_VERSION,
    )


def reflected_column(
    column: dict[str, Any],
    pk_columns: Sequence[str],
    unique_columns: set[str],
    table_foreign_keys: dict[str, ForeignKeyReflection],
    comment: str | None = None,
) -> ColumnSnapshot:
    foreign_key = table_foreign_keys.get(column["name"], {})
    return ColumnSnapshot(
        name=column["name"],
        kind=column["kind"],
        nullable=column["nullable"] and column["name"] not in set(pk_columns),
        primary_key=column["name"] in set(pk_columns),
        comment=comment,
        foreign_table=_optional_str(foreign_key.get("foreign_table")),
        foreign_column=_optional_str(foreign_key.get("foreign_column")),
        foreign_key_name=_optional_str(foreign_key.get("name")),
        on_delete=_optional_str(foreign_key.get("on_delete")),
        on_update=_optional_str(foreign_key.get("on_update")),
        deferrable=_optional_bool(foreign_key.get("deferrable")),
        initially_deferred=bool(foreign_key.get("initially_deferred", False)),
        max_length=column["max_length"],
        unique=column["name"] in unique_columns,
        server_default=column.get("server_default"),
        computed=_optional_str(column.get("computed")),
        computed_persisted=bool(column.get("computed_persisted", False)),
        autoincrement=bool(column.get("autoincrement", False)),
        identity=bool(column.get("identity", False)),
        identity_always=bool(column.get("identity_always", False)),
        identity_start=_optional_int(column.get("identity_start")),
        identity_increment=_optional_int(column.get("identity_increment")),
        collation=_optional_str(column.get("collation")),
        numeric_precision=_optional_int(column.get("numeric_precision")),
        numeric_scale=_optional_int(column.get("numeric_scale")),
    )


def _split_reflected_unique_constraints(
    constraints: Sequence[UniqueConstraintSnapshot],
) -> tuple[set[str], list[UniqueConstraintSnapshot]]:
    unique_columns: set[str] = set()
    table_constraints: list[UniqueConstraintSnapshot] = []
    for constraint in constraints:
        if _unique_constraint_requires_table_snapshot(constraint):
            table_constraints.append(constraint)
            continue
        unique_columns.add(constraint.columns[0])
    return unique_columns, table_constraints


def _unique_constraint_requires_table_snapshot(
    constraint: UniqueConstraintSnapshot,
) -> bool:
    if len(constraint.columns) != 1:
        return True
    return (
        constraint.deferrable is not None
        or constraint.initially_deferred
        or constraint.nulls_not_distinct
        or constraint.sqlite_on_conflict is not None
        or constraint.comment is not None
        or bool(constraint.postgres_include)
    )


def sqlite_reflected_foreign_key(item: dict[str, Any]) -> ForeignKeyReflection:
    return {
        "foreign_table": str(item["table"]),
        "foreign_column": str(item["to"]),
        "name": _optional_str(item.get("name")),
        "on_delete": _normalize_reflected_foreign_key_action(
            "sqlite",
            item.get("on_delete"),
        ),
        "on_update": _normalize_reflected_foreign_key_action(
            "sqlite",
            item.get("on_update"),
        ),
        "deferrable": None,
        "initially_deferred": False,
    }


_GENERATED_CHECK_SUFFIXES: dict[str, tuple[str, str]] = {
    "enum_values": ("enum", "in"),
    "ge": ("comparison", ">="),
    "gt": ("comparison", ">"),
    "le": ("comparison", "<="),
    "lt": ("comparison", "<"),
    "min_length": ("length", ">="),
    "max_length": ("length", "<="),
    "pattern": ("pattern", "matches"),
    "multiple_of": ("multiple_of", "="),
}
_GENERATED_CHECK_ORDER = {
    check: index for index, check in enumerate(_GENERATED_CHECK_SUFFIXES.values())
}


def _normalize_generated_column_checks(
    dialect: str,
    table_name: str,
    columns: Sequence[ColumnSnapshot],
    checks: Sequence[TableCheckSnapshot],
) -> tuple[list[ColumnSnapshot], list[TableCheckSnapshot]]:
    column_checks = {column.name: list(column.checks) for column in columns}
    remaining_checks: list[TableCheckSnapshot] = []
    for check in checks:
        generated_check: tuple[str, RuntimeCheck] | None = None
        for column in columns:
            generated_check = _generated_column_check(
                dialect,
                table_name,
                column.name,
                check,
            )
            if generated_check is not None:
                break
        if generated_check is None:
            remaining_checks.append(check)
            continue
        column_name, runtime_check = generated_check
        if runtime_check not in column_checks[column_name]:
            column_checks[column_name].append(runtime_check)

    reflected_columns = [
        (
            replace(column, checks=_sort_generated_checks(column_checks[column.name]))
            if _sort_generated_checks(column_checks[column.name]) != column.checks
            else column
        )
        for column in columns
    ]
    return reflected_columns, remaining_checks


def _sort_generated_checks(checks: Sequence[RuntimeCheck]) -> list[RuntimeCheck]:
    return sorted(checks, key=_generated_check_sort_key)


def _generated_check_sort_key(check: RuntimeCheck) -> tuple[int, str, str, str]:
    kind, operator, value = check
    return (
        _GENERATED_CHECK_ORDER.get((kind, operator), len(_GENERATED_CHECK_ORDER)),
        kind,
        operator,
        value,
    )


def _generated_column_check(
    dialect: str,
    table_name: str,
    column_name: str,
    check: TableCheckSnapshot,
) -> tuple[str, RuntimeCheck] | None:
    for suffix, (kind, operator) in _GENERATED_CHECK_SUFFIXES.items():
        if check.name != f"{table_name}_{column_name}_{suffix}_check":
            continue
        value = _generated_column_check_value(
            dialect,
            column_name,
            kind,
            operator,
            check.expression,
        )
        if value is None:
            return None
        return column_name, (kind, operator, value)
    return None


def _generated_column_check_value(
    dialect: str,
    column_name: str,
    kind: str,
    operator: str,
    expression: str,
) -> str | None:
    expression = _strip_outer_parentheses(expression.strip())
    column = _reflected_column_pattern(dialect, column_name)
    if kind == "comparison":
        value = _generated_check_match(
            dialect,
            kind,
            expression,
            rf"^{column}\s*{re.escape(operator)}\s*(?P<value>.+?)$",
        )
        if value is not None:
            return value
        if dialect == "sqlite":
            value = _match_value(
                expression,
                rf"^ormdantic_decimal_cmp\s*\(\s*{column}\s*,\s*"
                rf"(?P<value>.+?)\s*\)\s*{re.escape(operator)}\s*0$",
            )
            if value is not None:
                return _sqlite_decimal_check_literal(value)
        return None
    if kind == "length":
        length_function = (
            "LEN"
            if dialect == "mssql"
            else r"(?:LENGTH|CHAR_LENGTH|CHARACTER_LENGTH|OCTET_LENGTH)"
        )
        return _generated_check_match(
            dialect,
            kind,
            expression,
            rf"^{length_function}\s*\(\s*{column}\s*\)\s*{re.escape(operator)}\s*"
            rf"(?P<value>.+?)$",
        )
    if kind == "enum" and operator == "in":
        value = _generated_check_match(
            dialect,
            kind,
            expression,
            rf"^{column}\s+IN\s*\(\s*(?P<value>.+?)\s*\)$",
        )
        if value is not None:
            return value
        if dialect == "postgresql":
            return _generated_check_match(
                dialect,
                kind,
                expression,
                rf"^{column}\s*=\s*ANY\s*\(\s*ARRAY\s*\[(?P<value>.+?)\]\s*\)$",
            )
        return None
    if kind == "pattern" and operator == "matches":
        if dialect == "sqlite":
            return _generated_check_match(
                dialect,
                kind,
                expression,
                rf"^ormdantic_regex_match\s*\(\s*{column}\s*,\s*"
                rf"(?P<value>.+?)\s*\)\s*=\s*1$",
            )
        if dialect == "postgresql":
            return _generated_check_match(
                dialect,
                kind,
                expression,
                rf"^{column}\s*~\s*(?P<value>.+?)$",
            )
        if dialect in {"mysql", "mariadb"}:
            return _generated_check_match(
                dialect,
                kind,
                expression,
                rf"^{column}\s+REGEXP\s+(?P<value>.+?)$",
            )
        if dialect == "oracle":
            return _generated_check_match(
                dialect,
                kind,
                expression,
                rf"^REGEXP_LIKE\s*\(\s*{column}\s*,\s*(?P<value>.+?)\s*\)$",
            )
        return None
    if kind == "multiple_of" and operator == "=":
        if dialect == "sqlite":
            return _generated_check_match(
                dialect,
                kind,
                expression,
                rf"^ormdantic_decimal_multiple_of\s*\(\s*{column}\s*,\s*"
                rf"(?P<value>.+?)\s*\)\s*=\s*1$",
            )
        if dialect == "mssql":
            return _generated_check_match(
                dialect,
                kind,
                expression,
                rf"^{column}\s*%\s*(?P<value>.+?)\s*=\s*0$",
            )
        return _generated_check_match(
            dialect,
            kind,
            expression,
            rf"^MOD\s*\(\s*{column}\s*,\s*(?P<value>.+?)\s*\)\s*=\s*0$",
        )
    return None


def _reflected_column_pattern(dialect: str, column_name: str) -> str:
    column = re.escape(column_name)
    quoted_patterns = [column]
    if dialect in {"mysql", "mariadb"}:
        quoted_patterns.append(f"`{column}`")
    elif dialect == "mssql":
        quoted_patterns.append(rf"\[{column}\]")
    elif dialect in {"oracle", "sqlite"}:
        quoted_patterns.append(f'"{column}"')
    if dialect != "postgresql":
        return rf"(?:{'|'.join(quoted_patterns)})"
    postgres_cast_types = (
        "text|character varying|varchar|numeric|decimal|integer|bigint|smallint|"
        "double precision|real"
    )
    return (
        rf"(?:{column}|\(\s*{column}\s*\))" rf"(?:\s*::\s*(?:{postgres_cast_types}))?"
    )


def _generated_check_match(
    dialect: str,
    kind: str,
    expression: str,
    pattern: str,
) -> str | None:
    value = _match_value(expression, pattern)
    if value is None:
        return None
    return _normalize_generated_check_value(dialect, kind, value)


def _match_value(expression: str, pattern: str) -> str | None:
    match = re.match(pattern, expression, flags=re.IGNORECASE | re.DOTALL)
    if match is None:
        return None
    return match.group("value").strip()


def _normalize_generated_check_value(dialect: str, kind: str, value: str) -> str:
    del kind
    value = _strip_outer_parentheses(value.strip())
    if dialect == "postgresql":
        value = _strip_postgres_literal_casts(value)
    return value


def _strip_postgres_literal_casts(value: str) -> str:
    cast_types = (
        "text|character varying|varchar|numeric|decimal|integer|bigint|smallint|"
        "double precision|real"
    )
    literal = r"'(?:''|[^'])*'|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?"
    value = re.sub(
        rf"\(\s*({literal})\s*\)\s*::\s*(?:{cast_types})",
        r"\1",
        value,
        flags=re.IGNORECASE,
    )
    return re.sub(
        rf"({literal})\s*::\s*(?:{cast_types})",
        r"\1",
        value,
        flags=re.IGNORECASE,
    )


def _sqlite_decimal_check_literal(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == "'" and value[-1] == "'":
        return value[1:-1].replace("''", "'")
    return value


def _normalize_reflected_foreign_key_action(
    dialect: str,
    value: Any,
) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower().replace(" ", "_")
    if not normalized or normalized == "no_action":
        return None
    if normalized == "restrict" and dialect in {"mysql", "mariadb"}:
        return None
    if normalized in {"cascade", "restrict", "set_null", "set_default"}:
        return normalized
    return None


def _normalize_reflected_foreign_key_match(value: Any) -> str | None:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    if normalized == "full":
        return normalized
    return None


def _normalize_reflected_deferrable(value: Any) -> bool | None:
    if value is None:
        return None
    normalized = str(value).strip().lower().replace(" ", "_")
    if normalized in {"yes", "true", "1", "deferrable"}:
        return True
    if normalized in {"no", "false", "0", "not_deferrable"}:
        return False
    return None


def _normalize_reflected_initially_deferred(value: Any) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower().replace(" ", "_")
    return normalized in {"yes", "true", "1", "deferred", "initially_deferred"}


def _normalize_reflected_validated(value: Any) -> bool:
    if value is None:
        return True
    normalized = str(value).strip().lower().replace(" ", "_")
    return normalized in {"yes", "true", "1", "validated"}


def _postgres_storage_parameters_text(value: Any) -> list[tuple[str, str]]:
    parameters: list[tuple[str, str]] = []
    for item in str(value or "").split(","):
        name, separator, parameter = item.partition("=")
        name = name.strip()
        parameter = parameter.strip()
        if name and separator and parameter:
            parameters.append((name, parameter))
    return parameters


def _comma_separated_identifiers(value: Any) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


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
        materialized_views = _oracle_materialized_views_view(schema)
        owner_filter = _oracle_owner_filter(schema, table_alias="t")
        materialized_owner_match = " AND m.owner = t.owner" if schema else ""
        materialized_filter_prefix = "AND" if owner_filter else "WHERE"
        sql = (
            f"SELECT t.table_name FROM {table_view} t {owner_filter} "
            f"{materialized_filter_prefix} NOT EXISTS ("
            f"SELECT 1 FROM {materialized_views} m "
            f"WHERE m.mview_name = t.table_name{materialized_owner_match}) "
            "ORDER BY t.table_name"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    return [str(row[0]) for row in _query_rows_url(rust, url, sql)]


def _reflect_server_table_comments(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, str]:
    if not table_names:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "c.relname")
        sql = (
            "SELECT c.relname, obj_description(c.oid, 'pg_class') "
            "FROM pg_class c "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            f"WHERE n.nspname = {schema_filter} "
            f"AND c.relkind IN ('r', 'p') {table_filter} "
            "ORDER BY c.relname"
        )
    elif dialect in {"mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "table_name")
        sql = (
            "SELECT table_name, NULLIF(table_comment, '') "
            "FROM information_schema.tables "
            f"WHERE table_schema = {schema_filter} "
            f"AND table_type = 'BASE TABLE' {table_filter} "
            "ORDER BY table_name"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "t.name")
        sql = (
            "SELECT t.name, CAST(ep.value AS NVARCHAR(MAX)) "
            "FROM sys.tables t "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "JOIN sys.extended_properties ep ON ep.major_id = t.object_id "
            "AND ep.minor_id = 0 AND ep.class = 1 "
            "AND ep.name = N'MS_Description' "
            f"WHERE s.name = {schema_filter} {table_filter} "
            "ORDER BY t.name"
        )
    elif dialect == "oracle":
        comments_view = _oracle_table_comments_view(schema)
        table_filter = _table_name_filter(table_names, "table_name")
        owner_filter = ""
        if schema:
            owner_filter = f"AND owner = {_sql_literal(schema.upper())} "
        sql = (
            "SELECT table_name, comments "
            f"FROM {comments_view} WHERE table_type = 'TABLE' "
            f"{owner_filter}{table_filter} "
            "ORDER BY table_name"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    comments: dict[str, str] = {}
    for row in _query_rows_url(rust, url, sql):
        comment = _optional_str(row[1] if len(row) > 1 else None)
        if comment:
            comments[str(row[0])] = comment
    return comments


def _reflect_server_column_comments(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[tuple[str, str], str]:
    if not table_names:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "c.relname")
        sql = (
            "SELECT c.relname, a.attname, d.description "
            "FROM pg_class c "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "JOIN pg_attribute a ON a.attrelid = c.oid "
            "JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum "
            f"WHERE n.nspname = {schema_filter} "
            f"AND c.relkind IN ('r', 'p') AND a.attnum > 0 "
            f"AND NOT a.attisdropped {table_filter} "
            "ORDER BY c.relname, a.attnum"
        )
    elif dialect in {"mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "table_name")
        sql = (
            "SELECT table_name, column_name, NULLIF(column_comment, '') "
            "FROM information_schema.columns "
            f"WHERE table_schema = {schema_filter} {table_filter} "
            "ORDER BY table_name, ordinal_position"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "t.name")
        sql = (
            "SELECT t.name, c.name, CAST(ep.value AS NVARCHAR(MAX)) "
            "FROM sys.tables t "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "JOIN sys.columns c ON c.object_id = t.object_id "
            "JOIN sys.extended_properties ep ON ep.major_id = t.object_id "
            "AND ep.minor_id = c.column_id AND ep.class = 1 "
            "AND ep.name = N'MS_Description' "
            f"WHERE s.name = {schema_filter} {table_filter} "
            "ORDER BY t.name, c.column_id"
        )
    elif dialect == "oracle":
        comments_view = _oracle_column_comments_view(schema)
        table_filter = _table_name_filter(table_names, "table_name")
        owner_filter = ""
        if schema:
            owner_filter = f"AND owner = {_sql_literal(schema.upper())} "
        sql = (
            "SELECT table_name, column_name, comments "
            f"FROM {comments_view} WHERE comments IS NOT NULL "
            f"{owner_filter}{table_filter} "
            "ORDER BY table_name, column_name"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    comments: dict[tuple[str, str], str] = {}
    for row in _query_rows_url(rust, url, sql):
        comment = _optional_str(row[2] if len(row) > 2 else None)
        if comment:
            comments[(str(row[0]), str(row[1]))] = comment
    return comments


def _reflect_server_table_tablespaces(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, str]:
    if not table_names or dialect not in {
        "postgresql",
        "mysql",
        "mariadb",
        "mssql",
        "oracle",
    }:
        return {}
    if dialect == "oracle":
        tables_view = _oracle_table_view(schema)
        owner_filter = _oracle_owner_filter(schema, table_alias="t") or "WHERE 1 = 1"
        table_filter = _table_name_filter(table_names, "t.table_name")
        default_tablespace = _reflect_server_oracle_default_tablespace(
            rust,
            url,
            dialect,
            schema,
        )
        sql = (
            "SELECT t.table_name, t.tablespace_name "
            f"FROM {tables_view} t {owner_filter} "
            f"{table_filter} AND t.tablespace_name IS NOT NULL "
            "ORDER BY t.table_name"
        )
        oracle_tablespaces: dict[str, str] = {}
        for row in _query_rows_url(rust, url, sql):
            tablespace = _oracle_defaulted_tablespace(
                _optional_str(row[1] if len(row) > 1 else None),
                default_tablespace,
            )
            if tablespace:
                oracle_tablespaces[str(row[0])] = tablespace
        return oracle_tablespaces
    if dialect == "mysql":
        schema_filter = _schema_filter(dialect, schema)
        table_filter = _table_name_filter(table_names, "t.table_name")
        sql = (
            "SELECT t.table_name, s.name "
            "FROM information_schema.tables t "
            "JOIN information_schema.innodb_tables it "
            "ON it.name = CONCAT(t.table_schema, '/', t.table_name) "
            "JOIN information_schema.innodb_tablespaces s ON s.space = it.space "
            f"WHERE t.table_schema = {schema_filter} "
            "AND t.table_type = 'BASE TABLE' "
            f"{table_filter} "
            "AND (s.name = 'innodb_system' OR it.space_type IN ('General', 'System')) "
            "AND s.name <> CONCAT(t.table_schema, '/', t.table_name) "
            "ORDER BY t.table_name"
        )
        mysql_tablespaces: dict[str, str] = {}
        for row in _query_rows_url(rust, url, sql):
            tablespace = _optional_str(row[1] if len(row) > 1 else None)
            if tablespace:
                mysql_tablespaces[str(row[0])] = tablespace
        return mysql_tablespaces
    if dialect == "mariadb":
        schema_filter = _schema_filter(dialect, schema)
        table_filter = _table_name_filter(table_names, "t.table_name")
        sql = (
            "SELECT t.table_name, s.name "
            "FROM information_schema.tables t "
            "JOIN information_schema.innodb_sys_tables it "
            "ON it.name = CONCAT(t.table_schema, '/', t.table_name) "
            "JOIN information_schema.innodb_sys_tablespaces s ON s.space = it.space "
            f"WHERE t.table_schema = {schema_filter} "
            "AND t.table_type = 'BASE TABLE' "
            f"{table_filter} "
            "AND s.name <> CONCAT(t.table_schema, '/', t.table_name) "
            "ORDER BY t.table_name"
        )
        mariadb_tablespaces: dict[str, str] = {}
        for row in _query_rows_url(rust, url, sql):
            tablespace = _optional_str(row[1] if len(row) > 1 else None)
            if tablespace:
                mariadb_tablespaces[str(row[0])] = tablespace
        return mariadb_tablespaces
    if dialect == "mssql":
        schema_filter = _schema_filter(dialect, schema)
        table_filter = _table_name_filter(table_names, "t.name")
        sql = (
            "SELECT t.name, fg.name "
            "FROM sys.tables t "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "JOIN sys.indexes i ON i.object_id = t.object_id "
            "AND i.index_id IN (0, 1) "
            "JOIN sys.filegroups fg ON i.data_space_id = fg.data_space_id "
            f"WHERE s.name = {schema_filter} "
            f"AND t.is_ms_shipped = 0 {table_filter} "
            "AND fg.is_default = 0 "
            "ORDER BY t.name"
        )
        mssql_filegroups: dict[str, str] = {}
        for row in _query_rows_url(rust, url, sql):
            filegroup = _optional_str(row[1] if len(row) > 1 else None)
            if filegroup:
                mssql_filegroups[str(row[0])] = filegroup
        return mssql_filegroups
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "c.relname")
    sql = (
        "SELECT c.relname, ts.spcname "
        "FROM pg_class c "
        "JOIN pg_namespace n ON c.relnamespace = n.oid "
        "JOIN pg_tablespace ts ON c.reltablespace = ts.oid "
        f"WHERE n.nspname = {schema_filter} "
        f"AND c.relkind IN ('r', 'p') {table_filter} "
        "ORDER BY c.relname"
    )
    tablespaces: dict[str, str] = {}
    for row in _query_rows_url(rust, url, sql):
        tablespace = _optional_str(row[1] if len(row) > 1 else None)
        if tablespace:
            tablespaces[str(row[0])] = tablespace
    return tablespaces


def _reflect_server_oracle_table_compressions(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, int | bool]:
    if not table_names or dialect != "oracle":
        return {}
    tables_view = _oracle_table_view(schema)
    owner_filter = _oracle_owner_filter(schema, table_alias="t") or "WHERE 1 = 1"
    table_filter = _table_name_filter(table_names, "t.table_name")
    sql = (
        "SELECT t.table_name, t.compression, t.compress_for "
        f"FROM {tables_view} t {owner_filter} "
        f"{table_filter} AND t.compression = 'ENABLED' "
        "ORDER BY t.table_name"
    )
    compressions: dict[str, int | bool] = {}
    for row in _query_rows_url(rust, url, sql):
        compression = (_optional_str(row[1] if len(row) > 1 else None) or "").upper()
        if compression != "ENABLED":
            continue
        compress_for = _optional_str(row[2] if len(row) > 2 else None)
        if compress_for and compress_for.strip().isdigit():
            level = int(compress_for.strip())
            if level > 0:
                compressions[str(row[0])] = level
                continue
        compressions[str(row[0])] = True
    return compressions


def _reflect_server_postgres_inherits(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, list[str]]:
    if not table_names or dialect != "postgresql":
        return {}
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "child.relname")
    sql = (
        "SELECT child.relname, parent.relname "
        "FROM pg_inherits i "
        "JOIN pg_class child ON i.inhrelid = child.oid "
        "JOIN pg_namespace child_ns ON child.relnamespace = child_ns.oid "
        "JOIN pg_class parent ON i.inhparent = parent.oid "
        f"WHERE child_ns.nspname = {schema_filter} {table_filter} "
        "AND NOT child.relispartition "
        "ORDER BY child.relname, i.inhseqno"
    )
    inherits: dict[str, list[str]] = {}
    for row in _query_rows_url(rust, url, sql):
        inherits.setdefault(str(row[0]), []).append(str(row[1]))
    return inherits


def _reflect_server_postgres_with(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, list[tuple[str, str]]]:
    if not table_names or dialect != "postgresql":
        return {}
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "c.relname")
    sql = (
        "SELECT c.relname, opts.option_name, opts.option_value "
        "FROM pg_class c "
        "JOIN pg_namespace n ON c.relnamespace = n.oid "
        "JOIN LATERAL pg_options_to_table(c.reloptions) "
        "AS opts(option_name, option_value) ON true "
        f"WHERE n.nspname = {schema_filter} "
        f"AND c.relkind IN ('r', 'p') {table_filter} "
        "ORDER BY c.relname, opts.option_name"
    )
    options: dict[str, list[tuple[str, str]]] = {}
    for row in _query_rows_url(rust, url, sql):
        options.setdefault(str(row[0]), []).append((str(row[1]), str(row[2])))
    return options


def _reflect_server_postgres_using(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, str]:
    if not table_names or dialect != "postgresql":
        return {}
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "c.relname")
    sql = (
        "SELECT c.relname, am.amname "
        "FROM pg_class c "
        "JOIN pg_namespace n ON c.relnamespace = n.oid "
        "JOIN pg_am am ON c.relam = am.oid "
        f"WHERE n.nspname = {schema_filter} "
        f"AND c.relkind IN ('r', 'p') {table_filter} "
        "AND am.amname <> current_setting('default_table_access_method', true) "
        "ORDER BY c.relname"
    )
    return {
        str(row[0]): str(row[1])
        for row in _query_rows_url(rust, url, sql)
        if _optional_str(row[1] if len(row) > 1 else None)
    }


def _reflect_server_postgres_unlogged(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, bool]:
    if not table_names or dialect != "postgresql":
        return {}
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "c.relname")
    sql = (
        "SELECT c.relname, c.relpersistence = 'u' "
        "FROM pg_class c "
        "JOIN pg_namespace n ON c.relnamespace = n.oid "
        f"WHERE n.nspname = {schema_filter} "
        f"AND c.relkind IN ('r', 'p') {table_filter} "
        "AND c.relpersistence = 'u' "
        "ORDER BY c.relname"
    )
    return {str(row[0]): bool(row[1]) for row in _query_rows_url(rust, url, sql)}


def _reflect_server_postgres_partition_by(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, str]:
    if not table_names or dialect != "postgresql":
        return {}
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "c.relname")
    sql = (
        "SELECT c.relname, pg_get_partkeydef(c.oid) "
        "FROM pg_class c "
        "JOIN pg_namespace n ON c.relnamespace = n.oid "
        "JOIN pg_partitioned_table p ON p.partrelid = c.oid "
        f"WHERE n.nspname = {schema_filter} "
        f"AND c.relkind = 'p' {table_filter} "
        "ORDER BY c.relname"
    )
    return {
        str(row[0]): str(row[1])
        for row in _query_rows_url(rust, url, sql)
        if _optional_str(row[1] if len(row) > 1 else None)
    }


def _reflect_server_postgres_partitions(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, tuple[str, str]]:
    if not table_names or dialect != "postgresql":
        return {}
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "child.relname")
    sql = (
        "SELECT child.relname, parent.relname, "
        "pg_get_expr(child.relpartbound, child.oid) "
        "FROM pg_class child "
        "JOIN pg_namespace child_ns ON child.relnamespace = child_ns.oid "
        "JOIN pg_inherits i ON i.inhrelid = child.oid "
        "JOIN pg_class parent ON i.inhparent = parent.oid "
        f"WHERE child_ns.nspname = {schema_filter} "
        f"AND child.relispartition {table_filter} "
        "ORDER BY child.relname"
    )
    partitions: dict[str, tuple[str, str]] = {}
    for row in _query_rows_url(rust, url, sql):
        bound = _optional_str(row[2] if len(row) > 2 else None)
        if bound:
            partitions[str(row[0])] = (str(row[1]), bound)
    return partitions


def _reflect_server_mysql_table_options(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
    *,
    defaults: Mapping[str, str] | None = None,
) -> dict[str, dict[str, MysqlTableOptionValue]]:
    if not table_names or dialect not in {"mysql", "mariadb"}:
        return {}
    if defaults is None:
        defaults = _reflect_server_mysql_defaults(rust, url, dialect)
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "t.table_name")
    sql = (
        "SELECT t.table_name, NULLIF(t.engine, ''), c.character_set_name, "
        "NULLIF(t.table_collation, ''), NULLIF(t.row_format, ''), "
        "NULLIF(t.create_options, ''), t.auto_increment "
        "FROM information_schema.tables t "
        "LEFT JOIN information_schema.collation_character_set_applicability c "
        "ON c.collation_name = t.table_collation "
        f"WHERE t.table_schema = {schema_filter} "
        "AND t.table_type = 'BASE TABLE' "
        f"{table_filter} ORDER BY t.table_name"
    )
    options: dict[str, dict[str, MysqlTableOptionValue]] = {}
    for row in _query_rows_url(rust, url, sql):
        create_options = row[5] if len(row) > 5 else None
        row_format = _optional_str(row[4] if len(row) > 4 else None)
        if _mysql_create_option_token(create_options, "row_format") is None:
            row_format = _mysql_defaulted_option(
                row_format,
                defaults.get("innodb_default_row_format"),
            )
        options[str(row[0])] = {
            "engine": _mysql_defaulted_option(
                _optional_str(row[1] if len(row) > 1 else None),
                defaults.get("default_storage_engine"),
            ),
            "charset": _mysql_defaulted_option(
                _optional_str(row[2] if len(row) > 2 else None),
                defaults.get("character_set_database"),
            ),
            "collation": _mysql_defaulted_option(
                _optional_str(row[3] if len(row) > 3 else None),
                defaults.get("collation_database"),
            ),
            "row_format": row_format,
            "key_block_size": _mysql_create_option_int(
                create_options,
                "key_block_size",
            ),
            "pack_keys": _mysql_create_option_bool(create_options, "pack_keys"),
            "checksum": _mysql_create_option_bool(create_options, "checksum"),
            "delay_key_write": _mysql_create_option_bool(
                create_options,
                "delay_key_write",
            ),
            "stats_persistent": _mysql_create_option_bool(
                create_options,
                "stats_persistent",
            ),
            "stats_auto_recalc": _mysql_create_option_bool(
                create_options,
                "stats_auto_recalc",
            ),
            "stats_sample_pages": _mysql_create_option_int(
                create_options,
                "stats_sample_pages",
            ),
            "avg_row_length": _mysql_create_option_int(
                create_options,
                "avg_row_length",
            ),
            "max_rows": _mysql_create_option_int(create_options, "max_rows"),
            "min_rows": _mysql_create_option_int(create_options, "min_rows"),
            "insert_method": _mysql_create_option_token(
                create_options,
                "insert_method",
            ),
            "data_directory": _mysql_create_option_string(
                create_options,
                "data directory",
            ),
            "index_directory": _mysql_create_option_string(
                create_options,
                "index directory",
            ),
            "connection": _mysql_create_option_string(
                create_options,
                "connection",
            ),
            "union": _mysql_create_option_union(create_options),
            "auto_increment": _optional_int(row[6] if len(row) > 6 else None),
        }
    for table_name, partition_options in _reflect_server_mysql_partition_options(
        rust,
        url,
        dialect,
        schema,
        table_names,
    ).items():
        options.setdefault(table_name, {}).update(partition_options)
    return options


def _mysql_option_str(options: MysqlTableOptions, key: str) -> str | None:
    value = options.get(key)
    return value if isinstance(value, str) else None


def _mysql_option_int(options: MysqlTableOptions, key: str) -> int | None:
    value = options.get(key)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _mysql_option_bool(options: MysqlTableOptions, key: str) -> bool | None:
    value = options.get(key)
    return value if isinstance(value, bool) else None


def _mysql_option_str_list(options: MysqlTableOptions, key: str) -> list[str]:
    value = options.get(key)
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _reflect_server_mysql_defaults(
    rust: Any,
    url: str,
    dialect: str,
) -> dict[str, str]:
    if dialect not in {"mysql", "mariadb"}:
        return {}
    rows = _query_rows_url(
        rust,
        url,
        (
            "SELECT @@default_storage_engine, @@character_set_database, "
            "@@collation_database, @@innodb_default_row_format"
        ),
    )
    if not rows:
        return {}
    row = rows[0]
    keys = (
        "default_storage_engine",
        "character_set_database",
        "collation_database",
        "innodb_default_row_format",
    )
    return {
        key: str(value)
        for key, value in zip(keys, row, strict=False)
        if value is not None
    }


def _mysql_defaulted_option(value: str | None, default: str | None) -> str | None:
    if value is None:
        return None
    if default is not None and value.casefold() == default.casefold():
        return None
    return value


def _normalize_mysql_default_column_collations(
    dialect: str,
    columns: dict[str, list[dict[str, Any]]],
    table_options: Mapping[str, Mapping[str, Any]],
    defaults: Mapping[str, str],
) -> None:
    if dialect not in {"mysql", "mariadb"}:
        return
    default_collation = defaults.get("collation_database")
    for table_name, table_columns in columns.items():
        table_collation = _optional_str(
            table_options.get(table_name, {}).get("collation")
        )
        inherited_collation = table_collation or default_collation
        for column in table_columns:
            if (
                inherited_collation is not None
                and _mysql_defaulted_option(
                    _optional_str(column.get("collation")),
                    inherited_collation,
                )
                is None
            ):
                column.pop("collation", None)


def _reflect_server_mssql_database_collation(
    rust: Any,
    url: str,
    dialect: str,
) -> str | None:
    if dialect != "mssql":
        return None
    rows = _query_rows_url(
        rust,
        url,
        "SELECT CONVERT(NVARCHAR(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))",
    )
    if not rows:
        return None
    return _optional_str(rows[0][0] if rows[0] else None)


def _reflect_server_oracle_default_tablespace(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
) -> str | None:
    if dialect != "oracle":
        return None
    if schema:
        sql = (
            "SELECT default_tablespace FROM all_users "
            f"WHERE username = {_sql_literal(schema.upper())}"
        )
    else:
        sql = "SELECT default_tablespace FROM user_users"
    rows = _query_rows_url(rust, url, sql)
    if not rows:
        return None
    return _optional_str(rows[0][0] if rows[0] else None)


def _oracle_defaulted_tablespace(
    tablespace: str | None,
    default_tablespace: str | None,
) -> str | None:
    if tablespace is None:
        return None
    if (
        default_tablespace is not None
        and tablespace.casefold() == default_tablespace.casefold()
    ):
        return None
    return tablespace


def _normalize_mssql_default_column_collations(
    dialect: str,
    columns: dict[str, list[dict[str, Any]]],
    database_collation: str | None,
) -> None:
    if dialect != "mssql" or database_collation is None:
        return
    for table_columns in columns.values():
        for column in table_columns:
            if (
                _mysql_defaulted_option(
                    _optional_str(column.get("collation")),
                    database_collation,
                )
                is None
            ):
                column.pop("collation", None)


def _normalize_oracle_default_column_metadata(
    dialect: str,
    columns: dict[str, list[dict[str, Any]]],
) -> None:
    if dialect != "oracle":
        return
    for table_columns in columns.values():
        for column in table_columns:
            if _optional_str(column.get("collation")) == "USING_NLS_COMP":
                column.pop("collation", None)


def _reflected_server_default(dialect: str, value: Any) -> str | None:
    default = _optional_str(value)
    if default is None:
        return None
    default = default.strip()
    if dialect == "mssql":
        return _strip_outer_parentheses(default)
    return default


def _reflect_server_mysql_partition_options(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, dict[str, str | int | None]]:
    if not table_names or dialect not in {"mysql", "mariadb"}:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "p.table_name")
    sql = (
        "SELECT p.table_name, NULLIF(p.partition_method, ''), "
        "NULLIF(p.partition_expression, ''), p.partition_name, "
        "NULLIF(p.subpartition_method, ''), "
        "NULLIF(p.subpartition_expression, ''), p.subpartition_name "
        "FROM information_schema.partitions p "
        f"WHERE p.table_schema = {schema_filter} "
        "AND p.partition_name IS NOT NULL "
        f"{table_filter} "
        "ORDER BY p.table_name, p.partition_ordinal_position, "
        "p.subpartition_ordinal_position"
    )
    grouped: dict[str, dict[str, Any]] = {}
    for row in _query_rows_url(rust, url, sql):
        table_name = str(row[0])
        table = grouped.setdefault(
            table_name,
            {
                "partition_method": None,
                "partition_expression": None,
                "partition_names": set(),
                "subpartition_method": None,
                "subpartition_expression": None,
                "subpartition_names": {},
            },
        )
        table["partition_method"] = table["partition_method"] or _optional_str(
            row[1] if len(row) > 1 else None
        )
        table["partition_expression"] = table["partition_expression"] or _optional_str(
            row[2] if len(row) > 2 else None
        )
        partition_name = _optional_str(row[3] if len(row) > 3 else None)
        if partition_name is not None:
            table["partition_names"].add(partition_name)
        table["subpartition_method"] = table["subpartition_method"] or _optional_str(
            row[4] if len(row) > 4 else None
        )
        table["subpartition_expression"] = table[
            "subpartition_expression"
        ] or _optional_str(row[5] if len(row) > 5 else None)
        subpartition_name = _optional_str(row[6] if len(row) > 6 else None)
        if partition_name is not None and subpartition_name is not None:
            table["subpartition_names"].setdefault(partition_name, set()).add(
                subpartition_name
            )
    reflected: dict[str, dict[str, str | int | None]] = {}
    for table_name, table in grouped.items():
        subpartition_counts = [
            len(subpartitions)
            for subpartitions in table["subpartition_names"].values()
            if subpartitions
        ]
        reflected[table_name] = {
            "partition_by": _mysql_partition_clause(
                table["partition_method"],
                table["partition_expression"],
            ),
            "partitions": len(table["partition_names"]) or None,
            "subpartition_by": _mysql_partition_clause(
                table["subpartition_method"],
                table["subpartition_expression"],
            ),
            "subpartitions": max(subpartition_counts) if subpartition_counts else None,
        }
    return reflected


def _mysql_partition_clause(method: Any, expression: Any) -> str | None:
    method_text = _optional_str(method)
    expression_text = _optional_str(expression)
    if method_text is None or expression_text is None:
        return None
    return f"{' '.join(method_text.upper().split())} ({expression_text.strip()})"


def _mysql_create_option_int(create_options: Any, option: str) -> int | None:
    text = _optional_str(create_options)
    if text is None:
        return None
    match = re.search(rf"(?i)(?:^|\s){re.escape(option)}=(\d+)(?:\s|$)", text)
    if match is None:
        return None
    return int(match.group(1))


def _mysql_create_option_bool(create_options: Any, option: str) -> bool | None:
    text = _optional_str(create_options)
    if text is None:
        return None
    match = re.search(
        rf"(?i)(?:^|\s){re.escape(option)}=(0|1|on|off|true|false|yes|no)(?:\s|$)",
        text,
    )
    if match is None:
        return None
    return match.group(1).lower() in {"1", "on", "true", "yes"}


def _mysql_create_option_token(create_options: Any, option: str) -> str | None:
    text = _optional_str(create_options)
    if text is None:
        return None
    match = re.search(
        rf"(?i)(?:^|\s){re.escape(option)}=([A-Za-z0-9_]+)(?:\s|$)",
        text,
    )
    if match is None:
        return None
    return match.group(1)


def _mysql_create_option_string(create_options: Any, option: str) -> str | None:
    text = _optional_str(create_options)
    if text is None:
        return None
    option_pattern = r"\s+".join(re.escape(part) for part in option.split())
    match = re.search(
        rf"(?i)(?:^|\s){option_pattern}=(?:'((?:''|[^'])*)'|(\S+))(?:\s|$)",
        text,
    )
    if match is None:
        return None
    quoted, unquoted = match.groups()
    if quoted is not None:
        return quoted.replace("''", "'")
    return unquoted


def _mysql_create_option_union(create_options: Any) -> list[str]:
    text = _optional_str(create_options)
    if text is None:
        return []
    match = re.search(r"(?i)(?:^|\s)union=\(([^)]*)\)(?:\s|$)", text)
    if match is None:
        return []
    tables: list[str] = []
    for item in match.group(1).split(","):
        table = _mysql_unquote_identifier(item.strip())
        if table:
            tables.append(table)
    return tables


def _mysql_unquote_identifier(identifier: str) -> str:
    parts = []
    for part in identifier.split("."):
        part = part.strip()
        if len(part) >= 2 and part[0] == "`" and part[-1] == "`":
            part = part[1:-1].replace("``", "`")
        parts.append(part)
    return ".".join(parts)


def _reflect_server_enum_types(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str] | None = None,
) -> list[EnumTypeSnapshot]:
    if dialect != "postgresql":
        return []
    schema_filter = _schema_filter(dialect, schema)
    scope_filter = f"n.nspname = {schema_filter}"
    referenced_filter = ""
    if table_names:
        table_filter = _table_name_filter(table_names, "rel.relname")
        referenced_filter = (
            " OR EXISTS ("
            "SELECT 1 FROM pg_attribute a "
            "JOIN pg_class rel ON rel.oid = a.attrelid "
            "JOIN pg_namespace reln ON reln.oid = rel.relnamespace "
            "WHERE a.atttypid = t.oid "
            "AND a.attnum > 0 AND NOT a.attisdropped "
            f"AND reln.nspname = {schema_filter} {table_filter}"
            ")"
        )
    sql = (
        "SELECT n.nspname, t.typname, e.enumlabel, e.enumsortorder, "
        f"{scope_filter}, obj_description(t.oid, 'pg_type') AS comment "
        "FROM pg_type t "
        "JOIN pg_enum e ON t.oid = e.enumtypid "
        "JOIN pg_namespace n ON n.oid = t.typnamespace "
        f"WHERE ({scope_filter}{referenced_filter}) "
        "ORDER BY n.nspname, t.typname, e.enumsortorder"
    )
    grouped: dict[tuple[str | None, str], list[str]] = {}
    comments: dict[tuple[str | None, str], str] = {}
    for row in _query_rows_url(rust, url, sql):
        if len(row) > 4:
            enum_schema = _optional_str(row[0])
            enum_name = str(row[1])
            enum_value = str(row[2])
            in_reflected_schema = _db_truthy(row[4])
            snapshot_schema = schema if in_reflected_schema else enum_schema
            comment = _optional_str(row[5]) if len(row) > 5 else None
        else:
            enum_name = str(row[0])
            enum_value = str(row[1])
            snapshot_schema = schema
            comment = None
        key = (snapshot_schema, enum_name)
        grouped.setdefault(key, []).append(enum_value)
        if comment is not None and key not in comments:
            comments[key] = comment
    return [
        EnumTypeSnapshot(
            name,
            values,
            schema=enum_schema,
            comment=comments.get((enum_schema, name)),
        )
        for (enum_schema, name), values in grouped.items()
    ]


def _reflect_server_namespaces(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    *,
    extra_schemas: Sequence[str | None] | None = None,
) -> list[NamespaceSnapshot]:
    if dialect not in {"postgresql", "mysql", "mariadb", "mssql"}:
        return []
    extra_schemas = extra_schemas or []
    if schema is None and not any(extra_schemas):
        return []
    schema_filter = _schema_filter(dialect, schema)
    namespace_filter = _namespace_filter(
        dialect,
        schema_filter,
        extra_schemas,
        include_scope=schema is not None,
    )
    if dialect == "postgresql":
        sql = (
            "SELECT nspname, obj_description(oid, 'pg_namespace') AS comment "
            "FROM pg_namespace "
            f"WHERE {namespace_filter('nspname')} "
            "AND nspname <> 'information_schema' "
            "AND nspname NOT LIKE 'pg\\_%' ESCAPE '\\' "
            "ORDER BY nspname"
        )
    elif dialect in {"mysql", "mariadb"}:
        sql = (
            "SELECT SCHEMA_NAME "
            "FROM information_schema.SCHEMATA "
            f"WHERE {namespace_filter('SCHEMA_NAME')} "
            "ORDER BY SCHEMA_NAME"
        )
    elif dialect == "mssql":
        sql = (
            "SELECT s.name, CAST(ep.value AS NVARCHAR(MAX)) AS comment "
            "FROM sys.schemas s "
            "LEFT JOIN sys.extended_properties ep "
            "ON ep.class = 3 AND ep.major_id = s.schema_id "
            "AND ep.minor_id = 0 AND ep.name = N'MS_Description' "
            f"WHERE {namespace_filter('s.name')} ORDER BY s.name"
        )
    else:
        return []
    return [
        NamespaceSnapshot(str(row[0]), _optional_str(row[1]) if len(row) > 1 else None)
        for row in _query_rows_url(rust, url, sql)
    ]


def _reflect_server_sequences(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str] | None = None,
) -> list[SequenceSnapshot]:
    if dialect not in {"postgresql", "mariadb", "mssql", "oracle"}:
        return []
    schema_filter = _schema_filter(dialect, schema)
    if dialect == "postgresql":
        scope_filter = f"ps.schemaname = {schema_filter}"
        if table_names is not None:
            if not table_names:
                return []
            table_filter = _table_name_filter(table_names, "rel.relname")
            sequence_filter = (
                "EXISTS ("
                "SELECT 1 FROM pg_class seq "
                "JOIN pg_namespace seqn ON seqn.oid = seq.relnamespace "
                "JOIN pg_depend dep ON dep.refclassid = 'pg_class'::regclass "
                "AND dep.refobjid = seq.oid "
                "JOIN pg_attrdef def ON def.oid = dep.objid "
                "JOIN pg_class rel ON rel.oid = def.adrelid "
                "JOIN pg_namespace reln ON reln.oid = rel.relnamespace "
                "WHERE seq.relkind = 'S' "
                "AND seq.relname = ps.sequencename "
                "AND seqn.nspname = ps.schemaname "
                "AND dep.classid = 'pg_attrdef'::regclass "
                f"AND reln.nspname = {schema_filter} {table_filter}"
                ")"
            )
        else:
            sequence_filter = scope_filter
        sql = (
            "SELECT ps.schemaname, ps.sequencename, ps.start_value, "
            "ps.increment_by, ps.min_value, ps.max_value, ps.cycle, "
            f"ps.cache_size, {scope_filter}, "
            "(SELECT obj_description(seq_comment.oid, 'pg_class') "
            "FROM pg_class seq_comment "
            "JOIN pg_namespace seq_comment_ns "
            "ON seq_comment_ns.oid = seq_comment.relnamespace "
            "WHERE seq_comment.relkind = 'S' "
            "AND seq_comment.relname = ps.sequencename "
            "AND seq_comment_ns.nspname = ps.schemaname) "
            "AS comment, ps.data_type::text "
            "FROM pg_sequences "
            "ps "
            f"WHERE ({sequence_filter}) "
            "ORDER BY ps.schemaname, ps.sequencename"
        )
    elif dialect == "mariadb":
        sequence_columns = _mariadb_sequence_catalog_columns(rust, url)
        cache_column = "CACHE_SIZE" if "CACHE_SIZE" in sequence_columns else "NULL"
        data_type_column = "DATA_TYPE" if "DATA_TYPE" in sequence_columns else "NULL"
        sql = (
            "SELECT SEQUENCE_NAME, START_VALUE, INCREMENT, MINIMUM_VALUE, "
            f"MAXIMUM_VALUE, CYCLE_OPTION, {cache_column}, {data_type_column} "
            "FROM information_schema.SEQUENCES "
            f"WHERE SEQUENCE_SCHEMA = {schema_filter} "
            "ORDER BY SEQUENCE_NAME"
        )
    elif dialect == "mssql":
        sql = (
            "SELECT seq.name, CONVERT(nvarchar(80), seq.start_value), "
            "CONVERT(nvarchar(80), seq.increment), "
            "CONVERT(nvarchar(80), seq.minimum_value), "
            "CONVERT(nvarchar(80), seq.maximum_value), seq.is_cycling, "
            "seq.cache_size, CONVERT(nvarchar(4000), ep.value), "
            "TYPE_NAME(seq.user_type_id) "
            "FROM sys.sequences seq "
            "JOIN sys.schemas schema_ref ON seq.schema_id = schema_ref.schema_id "
            "LEFT JOIN sys.extended_properties ep ON ep.class = 1 "
            "AND ep.major_id = seq.object_id AND ep.minor_id = 0 "
            "AND ep.name = N'MS_Description' "
            f"WHERE schema_ref.name = {schema_filter} "
            "ORDER BY seq.name"
        )
    elif dialect == "oracle":
        sequences = _oracle_sequences_view(schema)
        owner_filter = _oracle_owner_filter(schema, table_alias="")
        sql = (
            "SELECT sequence_name, TO_CHAR(increment_by), "
            "TO_CHAR(min_value), TO_CHAR(max_value), cycle_flag, "
            f"TO_CHAR(cache_size), order_flag FROM {sequences} {owner_filter} "
            "ORDER BY sequence_name"
        )
    else:
        return []
    snapshots = [
        _reflected_sequence_snapshot(dialect, schema, row)
        for row in _query_rows_url(rust, url, sql)
    ]
    if dialect == "oracle":
        snapshots = [
            sequence
            for sequence in snapshots
            if _is_reflected_oracle_user_sequence(sequence.name)
        ]
    return snapshots


def _mariadb_sequence_catalog_columns(rust: Any, url: str) -> set[str]:
    rows = _query_rows_url(
        rust,
        url,
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = 'information_schema' "
        "AND TABLE_NAME = 'SEQUENCES'",
    )
    return {str(row[0]).upper() for row in rows if row}


def _is_reflected_oracle_user_sequence(name: str) -> bool:
    normalized = name.upper()
    return not normalized.startswith(("MVIEW$_", "ISEQ$$_"))


def _reflected_sequence_snapshot(
    dialect: str,
    schema: str | None,
    row: Sequence[Any],
) -> SequenceSnapshot:
    if dialect == "postgresql" and len(row) > 7:
        sequence_schema = _optional_str(row[0])
        sequence_name = str(row[1])
        option_offset = 2
        in_reflected_schema = _db_truthy(row[8] if len(row) > 8 else None)
        snapshot_schema = schema if in_reflected_schema else sequence_schema
        comment = _optional_str(row[9]) if len(row) > 9 else None
        data_type = _reflected_sequence_data_type(row[10] if len(row) > 10 else None)
        order = False
    elif dialect == "mssql":
        sequence_name = str(row[0])
        option_offset = 1
        snapshot_schema = schema
        comment = _optional_non_empty_str(row[7]) if len(row) > 7 else None
        data_type = _reflected_sequence_data_type(row[8] if len(row) > 8 else None)
        order = False
    else:
        sequence_name = str(row[0])
        if dialect == "oracle":
            return SequenceSnapshot(
                name=sequence_name,
                schema=schema,
                increment=_optional_int(row[1] if len(row) > 1 else None),
                min_value=_optional_int(row[2] if len(row) > 2 else None),
                max_value=_optional_int(row[3] if len(row) > 3 else None),
                cycle=_db_truthy(row[4] if len(row) > 4 else None),
                cache=_optional_int(row[5] if len(row) > 5 else None),
                order=_db_truthy(row[6] if len(row) > 6 else None),
            )
        option_offset = 1
        snapshot_schema = schema
        comment = None
        data_type = (
            _reflected_sequence_data_type(row[7] if len(row) > 7 else None)
            if dialect == "mariadb"
            else None
        )
        order = (
            _db_truthy(row[7] if len(row) > 7 else None)
            if dialect == "oracle"
            else False
        )
    return SequenceSnapshot(
        name=sequence_name,
        schema=snapshot_schema,
        start=_optional_int(row[option_offset] if len(row) > option_offset else None),
        increment=_optional_int(
            row[option_offset + 1] if len(row) > option_offset + 1 else None
        ),
        min_value=_optional_int(
            row[option_offset + 2] if len(row) > option_offset + 2 else None
        ),
        max_value=_optional_int(
            row[option_offset + 3] if len(row) > option_offset + 3 else None
        ),
        cycle=_db_truthy(
            row[option_offset + 4] if len(row) > option_offset + 4 else None
        ),
        cache=_optional_int(
            row[option_offset + 5] if len(row) > option_offset + 5 else None
        ),
        comment=comment,
        data_type=data_type,
        order=order,
    )


def _reflected_sequence_data_type(value: Any) -> str | None:
    data_type = _optional_str(value)
    if data_type is None:
        return None
    normalized = " ".join(data_type.strip().lower().split())
    return normalized or None


def _reflect_server_views(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    include_tables: Sequence[str] | None = None,
    exclude_tables: Sequence[str] | None = None,
) -> list[ViewSnapshot]:
    if dialect not in {"postgresql", "mysql", "mariadb", "mssql", "oracle"}:
        return []
    schema_filter = _schema_filter(dialect, schema)
    materialized_rows: list[list[Any]] = []
    if dialect == "postgresql":
        sql = (
            "SELECT v.table_name, v.view_definition, "
            "obj_description(c.oid, 'pg_class') AS comment "
            "FROM information_schema.views v "
            "JOIN pg_namespace n ON n.nspname = v.table_schema "
            "JOIN pg_class c ON c.relname = v.table_name AND c.relnamespace = n.oid "
            f"WHERE v.table_schema = {schema_filter} "
            "ORDER BY v.table_name"
        )
        materialized_sql = (
            "SELECT m.matviewname, m.definition, "
            "obj_description(c.oid, 'pg_class') AS comment "
            "FROM pg_matviews m "
            "JOIN pg_namespace n ON n.nspname = m.schemaname "
            "JOIN pg_class c ON c.relname = m.matviewname AND c.relnamespace = n.oid "
            f"WHERE m.schemaname = {schema_filter} "
            "ORDER BY m.matviewname"
        )
        materialized_rows = _query_rows_url(rust, url, materialized_sql)
    elif dialect in {"mysql", "mariadb"}:
        sql = (
            "SELECT TABLE_NAME, VIEW_DEFINITION "
            "FROM information_schema.VIEWS "
            f"WHERE TABLE_SCHEMA = {schema_filter} "
            "ORDER BY TABLE_NAME"
        )
    elif dialect == "mssql":
        sql = (
            "SELECT v.TABLE_NAME, v.VIEW_DEFINITION, "
            "CAST(ep.value AS NVARCHAR(MAX)) AS comment "
            "FROM INFORMATION_SCHEMA.VIEWS v "
            "JOIN sys.schemas s ON s.name = v.TABLE_SCHEMA "
            "JOIN sys.views sv ON sv.name = v.TABLE_NAME AND sv.schema_id = s.schema_id "
            "LEFT JOIN sys.extended_properties ep "
            "ON ep.class = 1 AND ep.major_id = sv.object_id "
            "AND ep.minor_id = 0 AND ep.name = N'MS_Description' "
            f"WHERE v.TABLE_SCHEMA = {schema_filter} "
            "ORDER BY v.TABLE_NAME"
        )
    elif dialect == "oracle":
        views = _oracle_views_view(schema)
        comments = _oracle_table_comments_view(schema)
        owner_filter = _oracle_owner_filter(schema, table_alias="v")
        owner_join = "AND c.owner = v.owner " if schema else ""
        sql = (
            "SELECT v.view_name, v.text_vc, c.comments "
            f"FROM {views} v "
            f"LEFT JOIN {comments} c "
            f"ON c.table_name = v.view_name {owner_join}"
            f"{owner_filter} ORDER BY v.view_name"
        )
        materialized_views = _oracle_materialized_views_view(schema)
        materialized_comments = _oracle_materialized_view_comments_view(schema)
        materialized_owner_filter = _oracle_owner_filter(schema, table_alias="m")
        materialized_owner_join = "AND c.owner = m.owner " if schema else ""
        materialized_rows = _query_rows_url(
            rust,
            url,
            (
                "SELECT m.mview_name, "
                f"{_oracle_materialized_view_definition_sql(schema)}, c.comments "
                f"FROM {materialized_views} m "
                f"LEFT JOIN {materialized_comments} c "
                f"ON c.mview_name = m.mview_name {materialized_owner_join}"
                f"{materialized_owner_filter} ORDER BY m.mview_name"
            ),
        )
    else:
        return []
    rows = _query_rows_url(rust, url, sql)
    snapshots = [
        ViewSnapshot(
            str(row[0]),
            normalized_view_definition(str(row[1])),
            schema=schema,
            comment=(
                _optional_view_comment(dialect, False, row[2]) if len(row) > 2 else None
            ),
        )
        for row in rows
        if len(row) > 1 and row[1] is not None
    ]
    snapshots.extend(
        ViewSnapshot(
            str(row[0]),
            normalized_view_definition(
                _reflected_oracle_materialized_view_definition(str(row[1]))
                if dialect == "oracle"
                else str(row[1])
            ),
            schema=schema,
            materialized=True,
            comment=(
                _optional_view_comment(dialect, True, row[2]) if len(row) > 2 else None
            ),
        )
        for row in materialized_rows
        if len(row) > 1 and row[1] is not None
    )
    return [
        snapshot
        for snapshot in snapshots
        if _table_matches_filters(snapshot.name, include_tables, exclude_tables)
    ]


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
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "c.table_name")
        sql = (
            "SELECT c.table_name, c.column_name, c.data_type, c.is_nullable, "
            "c.character_maximum_length, c.numeric_precision, c.numeric_scale, "
            "c.ordinal_position, c.column_default, "
            "CASE WHEN c.data_type = 'USER-DEFINED' AND EXISTS ("
            "SELECT 1 FROM pg_type pt "
            "JOIN pg_enum pe ON pe.enumtypid = pt.oid "
            "JOIN pg_namespace pn ON pn.oid = pt.typnamespace "
            "WHERE pt.typname = c.udt_name AND pn.nspname = c.udt_schema"
            ") THEN c.udt_name ELSE NULL END, c.collation_name, "
            "c.is_identity, c.identity_generation, "
            "c.identity_start, c.identity_increment, "
            "c.is_generated, c.generation_expression, "
            "CASE WHEN c.data_type = 'USER-DEFINED' AND EXISTS ("
            "SELECT 1 FROM pg_type pt "
            "JOIN pg_enum pe ON pe.enumtypid = pt.oid "
            "JOIN pg_namespace pn ON pn.oid = pt.typnamespace "
            "WHERE pt.typname = c.udt_name AND pn.nspname = c.udt_schema"
            ") THEN c.udt_schema ELSE NULL END, c.table_schema, "
            "c.identity_minimum, c.identity_maximum, c.identity_cycle, "
            "(SELECT ps.cache_size FROM pg_sequences ps "
            "JOIN pg_class seq ON seq.relname = ps.sequencename "
            "JOIN pg_namespace seq_ns ON seq_ns.oid = seq.relnamespace "
            "AND seq_ns.nspname = ps.schemaname "
            "JOIN pg_depend dep ON dep.objid = seq.oid "
            "AND dep.deptype IN ('a', 'i') "
            "JOIN pg_class tbl ON tbl.oid = dep.refobjid "
            "JOIN pg_namespace tbl_ns ON tbl_ns.oid = tbl.relnamespace "
            "JOIN pg_attribute attr ON attr.attrelid = tbl.oid "
            "AND attr.attnum = dep.refobjsubid "
            "WHERE tbl_ns.nspname = c.table_schema "
            "AND tbl.relname = c.table_name "
            "AND attr.attname = c.column_name LIMIT 1) "
            "FROM information_schema.columns c "
            f"WHERE c.table_schema = {schema_filter} {table_filter} "
            "ORDER BY c.table_name, c.ordinal_position"
        )
    elif dialect in {"mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "table_name")
        sql = (
            "SELECT table_name, column_name, data_type, is_nullable, "
            "character_maximum_length, numeric_precision, numeric_scale, "
            "ordinal_position, column_default, collation_name, extra, "
            "generation_expression "
            "FROM information_schema.columns "
            f"WHERE table_schema = {schema_filter} {table_filter} "
            "ORDER BY table_name, ordinal_position"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "c.TABLE_NAME")
        object_name = "QUOTENAME(c.TABLE_SCHEMA) + '.' + QUOTENAME(c.TABLE_NAME)"
        identity_check = (
            f"COLUMNPROPERTY(OBJECT_ID({object_name}), c.COLUMN_NAME, 'IsIdentity')"
        )
        sql = (
            "SELECT c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, "
            "c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, "
            "c.ORDINAL_POSITION, c.COLUMN_DEFAULT, c.COLLATION_NAME, "
            f"{identity_check}, "
            f"CASE WHEN {identity_check} = 1 THEN IDENT_SEED({object_name}) ELSE NULL END, "
            f"CASE WHEN {identity_check} = 1 THEN IDENT_INCR({object_name}) ELSE NULL END, "
            "cc.definition, cc.is_persisted "
            "FROM INFORMATION_SCHEMA.COLUMNS c "
            "LEFT JOIN sys.schemas s ON s.name = c.TABLE_SCHEMA "
            "LEFT JOIN sys.tables t ON t.name = c.TABLE_NAME "
            "AND t.schema_id = s.schema_id "
            "LEFT JOIN sys.columns sc ON sc.object_id = t.object_id "
            "AND sc.name = c.COLUMN_NAME "
            "LEFT JOIN sys.computed_columns cc ON cc.object_id = sc.object_id "
            "AND cc.column_id = sc.column_id "
            f"WHERE c.TABLE_SCHEMA = {schema_filter} {table_filter} "
            "ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION"
        )
    elif dialect == "oracle":
        view = _oracle_tab_columns_view(schema)
        identity_view = _oracle_identity_columns_view(schema)
        identity_owner_filter = "AND i.owner = c.owner " if schema else ""
        owner_filter = _oracle_owner_filter(schema, table_alias="c")
        table_filter = _table_name_filter(table_names, "c.table_name")
        where = f"{owner_filter} " if owner_filter else "WHERE 1 = 1 "
        where += f"AND c.hidden_column = 'NO' {table_filter}"
        sql = (
            "SELECT c.table_name, c.column_name, c.data_type, c.nullable, c.char_length, "
            "c.data_precision, c.data_scale, c.column_id, c.data_default_vc, c.collation, "
            "(SELECT i.generation_type FROM "
            f"{identity_view} i WHERE i.table_name = c.table_name "
            f"AND i.column_name = c.column_name {identity_owner_filter}), "
            "c.virtual_column, "
            "(SELECT i.identity_options FROM "
            f"{identity_view} i WHERE i.table_name = c.table_name "
            f"AND i.column_name = c.column_name {identity_owner_filter}) "
            f"FROM {view} c {where} "
            "ORDER BY c.table_name, c.column_id"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    columns: dict[str, list[dict[str, Any]]] = {}
    for row in _query_rows_url(rust, url, sql):
        table_name = str(row[0])
        data_type = str(row[2])
        enum_type_name = _reflected_enum_type_name(dialect, schema, row)
        numeric_precision = _optional_int(row[5] if len(row) > 5 else None)
        numeric_scale = _optional_int(row[6] if len(row) > 6 else None)
        collation = _reflected_column_collation(dialect, row)
        kind = _normalize_reflected_type(
            dialect,
            data_type,
            precision=numeric_precision,
            scale=numeric_scale,
            enum_type_name=enum_type_name,
        )
        column = {
            "name": str(row[1]),
            "kind": kind,
            "nullable": _nullable_from_reflection(row[3]),
            "max_length": _reflected_max_length(
                row[4] if len(row) > 4 else None,
                data_type,
            ),
            "server_default": _reflected_server_default(
                dialect,
                row[8] if len(row) > 8 else None,
            ),
        }
        column.update(_reflected_column_identity_options(dialect, row))
        computed_options = _reflected_column_computed_options(dialect, row)
        if computed_options:
            column["server_default"] = None
            column.update(computed_options)
        if collation is not None:
            column["collation"] = collation
        if kind == "decimal" and numeric_precision is not None:
            column["numeric_precision"] = numeric_precision
        if kind == "decimal" and numeric_scale is not None:
            column["numeric_scale"] = numeric_scale
        columns.setdefault(table_name, []).append(column)
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
) -> dict[str, list[UniqueConstraintSnapshot]]:
    grouped = _reflect_named_key_columns(
        rust, url, dialect, schema, "UNIQUE", table_names
    )
    return {
        table: [
            UniqueConstraintSnapshot(
                name,
                columns,
                deferrable,
                initially_deferred,
                nulls_not_distinct,
                mssql_filegroup=mssql_filegroup,
                mssql_clustered=mssql_clustered,
                oracle_tablespace=oracle_tablespace,
                oracle_compress=oracle_compress,
                comment=comment,
                postgres_include=postgres_include,
            )
            for (
                name,
                columns,
                deferrable,
                initially_deferred,
                nulls_not_distinct,
                comment,
                postgres_include,
                mssql_filegroup,
                mssql_clustered,
                oracle_tablespace,
                oracle_compress,
            ) in constraints
        ]
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
) -> dict[str, list[KeyConstraintReflection]]:
    if not table_names:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    oracle_default_tablespace = _reflect_server_oracle_default_tablespace(
        rust,
        url,
        dialect,
        schema,
    )
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "tc.table_name")
        nulls_expr = (
            "COALESCE((to_jsonb(idx)->>'indnullsnotdistinct')::boolean, false)"
            if constraint_type == "UNIQUE"
            else "false"
        )
        include_expr = (
            "(SELECT string_agg(att.attname, ',' ORDER BY key_pos.ordinality) "
            "FROM unnest(idx.indkey) WITH ORDINALITY AS key_pos(attnum, ordinality) "
            "JOIN pg_attribute att ON att.attrelid = rel.oid "
            "AND att.attnum = key_pos.attnum "
            "WHERE key_pos.ordinality > idx.indnkeyatts)"
            if constraint_type == "UNIQUE"
            else "NULL"
        )
        sql = (
            "SELECT kcu.table_name, kcu.constraint_name, kcu.column_name, "
            "kcu.ordinal_position, tc.is_deferrable, tc.initially_deferred, "
            f"{nulls_expr}, obj_description(c.oid, 'pg_constraint') AS comment, "
            f"{include_expr} AS include_columns, NULL AS mssql_filegroup, "
            "NULL AS mssql_clustered, NULL AS oracle_tablespace, "
            "NULL AS oracle_compress, NULL AS oracle_prefix_length "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "ON tc.constraint_schema = kcu.constraint_schema "
            "AND tc.constraint_name = kcu.constraint_name "
            "AND tc.table_name = kcu.table_name "
            "LEFT JOIN pg_namespace n ON n.nspname = tc.constraint_schema "
            "LEFT JOIN pg_class rel ON rel.relname = tc.table_name "
            "AND rel.relnamespace = n.oid "
            "LEFT JOIN pg_constraint c ON c.connamespace = n.oid "
            "AND c.conname = tc.constraint_name "
            "AND c.conrelid = rel.oid "
            "LEFT JOIN pg_index idx ON idx.indexrelid = c.conindid "
            f"WHERE tc.table_schema = {schema_filter} "
            f"AND tc.constraint_type = {_sql_literal(constraint_type)} {table_filter} "
            "ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position"
        )
    elif dialect in {"mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "tc.table_name")
        sql = (
            "SELECT kcu.table_name, kcu.constraint_name, kcu.column_name, "
            "kcu.ordinal_position, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
            "NULL, NULL, NULL "
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
            "kcu.ORDINAL_POSITION, NULL, NULL, NULL, "
            "CONVERT(nvarchar(max), ep.value), NULL, fg.name AS mssql_filegroup, "
            "CASE WHEN index_ref.type_desc = 'CLUSTERED' THEN 1 "
            "WHEN index_ref.type_desc = 'NONCLUSTERED' THEN 0 "
            "ELSE NULL END AS mssql_clustered, NULL AS oracle_tablespace, "
            "NULL AS oracle_compress, NULL AS oracle_prefix_length "
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu "
            "ON tc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA "
            "AND tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME "
            "AND tc.TABLE_NAME = kcu.TABLE_NAME "
            "JOIN sys.schemas schema_ref ON schema_ref.name = tc.TABLE_SCHEMA "
            "JOIN sys.tables table_ref ON table_ref.name = tc.TABLE_NAME "
            "AND table_ref.schema_id = schema_ref.schema_id "
            "LEFT JOIN sys.key_constraints key_ref "
            "ON key_ref.parent_object_id = table_ref.object_id "
            "AND key_ref.name = tc.CONSTRAINT_NAME "
            "LEFT JOIN sys.indexes index_ref "
            "ON index_ref.object_id = table_ref.object_id "
            "AND index_ref.index_id = key_ref.unique_index_id "
            "LEFT JOIN sys.filegroups fg ON index_ref.data_space_id = fg.data_space_id "
            "AND fg.is_default = 0 "
            "LEFT JOIN sys.extended_properties ep ON ep.class = 1 "
            "AND ep.major_id = key_ref.object_id AND ep.minor_id = 0 "
            "AND ep.name = N'MS_Description' "
            f"WHERE tc.TABLE_SCHEMA = {schema_filter} "
            f"AND tc.CONSTRAINT_TYPE = {_sql_literal(constraint_type)} {table_filter} "
            "ORDER BY kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION"
        )
    elif dialect == "oracle":
        constraints = _oracle_constraints_view(schema)
        columns = _oracle_cons_columns_view(schema)
        indexes = _oracle_indexes_view(schema)
        owner_join = ""
        index_owner_join = ""
        owner_filter = ""
        if schema:
            owner_join = "AND c.owner = cc.owner "
            index_owner_join = "AND i.owner = c.index_owner "
            owner_filter = f"AND c.owner = {_sql_literal(schema.upper())} "
        oracle_type = "P" if constraint_type == "PRIMARY KEY" else "U"
        table_filter = _table_name_filter(table_names, "cc.table_name")
        sql = (
            "SELECT cc.table_name, cc.constraint_name, cc.column_name, cc.position, "
            "c.deferrable, c.deferred, CAST(NULL AS NUMBER), "
            "CAST(NULL AS VARCHAR2(1)), CAST(NULL AS VARCHAR2(1)), "
            "CAST(NULL AS VARCHAR2(1)), CAST(NULL AS NUMBER), "
            "i.tablespace_name AS oracle_tablespace, "
            "i.compression AS oracle_compress, i.prefix_length AS oracle_prefix_length "
            f"FROM {constraints} c JOIN {columns} cc "
            "ON c.constraint_name = cc.constraint_name "
            f"{owner_join}AND c.table_name = cc.table_name "
            f"LEFT JOIN {indexes} i ON i.index_name = c.index_name "
            f"{index_owner_join}"
            f"WHERE c.constraint_type = {_sql_literal(oracle_type)} {owner_filter}"
            f"{table_filter} "
            "ORDER BY cc.table_name, cc.constraint_name, cc.position"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for row in _query_rows_url(rust, url, sql):
        table_name = str(row[0])
        constraint_name = str(row[1])
        ordinal = _optional_int(row[3]) or 0
        deferrable = _normalize_reflected_deferrable(row[4] if len(row) > 4 else None)
        initially_deferred = _normalize_reflected_initially_deferred(
            row[5] if len(row) > 5 else None
        )
        if deferrable is False and not initially_deferred:
            deferrable = None
        constraint = grouped.setdefault(table_name, {}).setdefault(
            constraint_name,
            {
                "columns": [],
                "deferrable": deferrable,
                "initially_deferred": initially_deferred,
                "nulls_not_distinct": _optional_bool(row[6] if len(row) > 6 else None)
                or False,
                "comment": _optional_str(row[7] if len(row) > 7 else None),
                "postgres_include": _comma_separated_identifiers(
                    row[8] if len(row) > 8 else None
                ),
                "mssql_filegroup": _optional_str(row[9] if len(row) > 9 else None),
                "mssql_clustered": _optional_bool(row[10] if len(row) > 10 else None),
                "oracle_tablespace": _oracle_defaulted_tablespace(
                    _optional_str(row[11] if len(row) > 11 else None),
                    oracle_default_tablespace,
                ),
                "oracle_compress": _reflected_oracle_compress(
                    row[12] if len(row) > 12 else None,
                    row[13] if len(row) > 13 else None,
                ),
            },
        )
        constraint["columns"].append((ordinal, str(row[2])))
    return {
        table: [
            (
                name,
                [
                    column
                    for _, column in sorted(
                        constraint["columns"], key=lambda item: item[0]
                    )
                ],
                constraint["deferrable"],
                constraint["initially_deferred"],
                constraint["nulls_not_distinct"],
                constraint["comment"],
                constraint["postgres_include"],
                constraint["mssql_filegroup"],
                constraint["mssql_clustered"],
                constraint["oracle_tablespace"],
                constraint["oracle_compress"],
            )
            for name, constraint in constraints.items()
        ]
        for table, constraints in grouped.items()
    }


def _reflected_oracle_compress(
    compression: Any,
    prefix_length: Any,
) -> int | bool | None:
    if compression is None:
        return None
    normalized = str(compression).strip().upper()
    if normalized in {"", "DISABLED", "NO", "NONE"}:
        return None
    prefix = _optional_int(prefix_length)
    return prefix if prefix is not None and prefix > 0 else _oracle_index_compress(True)


def _reflect_server_foreign_keys(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, dict[str, ForeignKeyReflection]]:
    constraints = _reflect_server_foreign_key_constraints(
        rust,
        url,
        dialect,
        schema,
        table_names,
        include_single_column=True,
    )
    foreign_keys, _table_constraints = _split_reflected_foreign_key_constraints(
        constraints
    )
    return foreign_keys


def _split_reflected_foreign_key_constraints(
    constraints_by_table: Mapping[str, Sequence[ForeignKeyConstraintSnapshot]],
) -> tuple[
    dict[str, dict[str, ForeignKeyReflection]],
    dict[str, list[ForeignKeyConstraintSnapshot]],
]:
    column_foreign_keys: dict[str, dict[str, ForeignKeyReflection]] = {}
    table_constraints: dict[str, list[ForeignKeyConstraintSnapshot]] = {}
    for table_name, constraints in constraints_by_table.items():
        for constraint in constraints:
            if _foreign_key_constraint_requires_table_snapshot(constraint):
                table_constraints.setdefault(table_name, []).append(constraint)
                continue
            column_foreign_keys.setdefault(table_name, {})[constraint.columns[0]] = {
                "foreign_table": constraint.foreign_table,
                "foreign_column": constraint.foreign_columns[0],
                "name": constraint.name,
                "on_delete": constraint.on_delete,
                "on_update": constraint.on_update,
                "deferrable": constraint.deferrable,
                "initially_deferred": constraint.initially_deferred,
            }
    return column_foreign_keys, table_constraints


def _foreign_key_constraint_requires_table_snapshot(
    constraint: ForeignKeyConstraintSnapshot,
) -> bool:
    if len(constraint.columns) != 1 or len(constraint.foreign_columns) != 1:
        return True
    if not constraint.validated:
        return True
    return constraint.match not in {None, "simple"}


def _reflect_server_foreign_key_constraints(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
    *,
    include_single_column: bool = False,
) -> dict[str, list[ForeignKeyConstraintSnapshot]]:
    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for row in _reflect_server_foreign_key_rows(
        rust, url, dialect, schema, table_names
    ):
        table_name = str(row[0])
        constraint_name = _optional_str(row[4])
        if constraint_name is None:
            constraint_name = f"{table_name}_{row[1]}_foreign_key"
        ordinal = _optional_int(row[7]) or 0
        foreign_table = _reflected_foreign_table_name(
            dialect,
            str(row[2]),
            _optional_str(row[12] if len(row) > 12 else None),
            _optional_str(row[14] if len(row) > 14 else None) or schema,
        )
        deferrable = _normalize_reflected_deferrable(row[8] if len(row) > 8 else None)
        initially_deferred = _normalize_reflected_initially_deferred(
            row[9] if len(row) > 9 else None
        )
        if deferrable is False and not initially_deferred:
            deferrable = None
        constraint = grouped.setdefault(table_name, {}).setdefault(
            constraint_name,
            {
                "columns": [],
                "foreign_table": foreign_table,
                "foreign_columns": [],
                "on_delete": _normalize_reflected_foreign_key_action(dialect, row[5]),
                "on_update": _normalize_reflected_foreign_key_action(dialect, row[6]),
                "deferrable": deferrable,
                "initially_deferred": initially_deferred,
                "validated": _normalize_reflected_validated(
                    row[10] if len(row) > 10 else None
                ),
                "match": _normalize_reflected_foreign_key_match(
                    row[11] if len(row) > 11 else None
                ),
                "comment": _optional_str(row[13] if len(row) > 13 else None),
            },
        )
        constraint["columns"].append((ordinal, str(row[1])))
        constraint["foreign_columns"].append((ordinal, str(row[3])))
    return {
        table: [
            ForeignKeyConstraintSnapshot(
                name,
                [
                    column
                    for _, column in sorted(
                        constraint["columns"], key=lambda item: item[0]
                    )
                ],
                constraint["foreign_table"],
                [
                    column
                    for _, column in sorted(
                        constraint["foreign_columns"], key=lambda item: item[0]
                    )
                ],
                constraint["on_delete"],
                constraint["on_update"],
                constraint["deferrable"],
                constraint["initially_deferred"],
                constraint["validated"],
                constraint["match"],
                constraint["comment"],
            )
            for name, constraint in constraints.items()
            if include_single_column or len(constraint["columns"]) > 1
        ]
        for table, constraints in grouped.items()
    }


def _reflected_foreign_table_name(
    dialect: str,
    table_name: str,
    foreign_schema: str | None,
    local_schema: str | None,
) -> str:
    if foreign_schema is None:
        return table_name
    if _same_reflected_schema(dialect, foreign_schema, local_schema):
        return table_name
    return f"{foreign_schema}.{table_name}"


def _same_reflected_schema(
    dialect: str,
    foreign_schema: str,
    local_schema: str | None,
) -> bool:
    if local_schema is None:
        return False
    if dialect == "oracle":
        return foreign_schema.upper() == local_schema.upper()
    if dialect in {"mysql", "mariadb", "mssql"}:
        return foreign_schema.lower() == local_schema.lower()
    return foreign_schema == local_schema


def _reflect_server_foreign_key_rows(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> list[list[Any]]:
    if not table_names:
        return []
    schema_filter = _schema_filter(dialect, schema)
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "tc.table_name")
        sql = (
            "SELECT kcu.table_name, kcu.column_name, rkcu.table_name, rkcu.column_name, "
            "tc.constraint_name, rc.delete_rule, rc.update_rule, kcu.ordinal_position, "
            "tc.is_deferrable, tc.initially_deferred, c.convalidated, rc.match_option, "
            "rkcu.table_schema, obj_description(c.oid, 'pg_constraint') AS comment, "
            "kcu.table_schema "
            "FROM information_schema.table_constraints tc "
            "JOIN pg_constraint c ON c.conname = tc.constraint_name "
            "AND c.connamespace = ("
            "SELECT oid FROM pg_namespace WHERE nspname = tc.constraint_schema"
            ") "
            "JOIN information_schema.key_column_usage kcu "
            "ON tc.constraint_schema = kcu.constraint_schema "
            "AND tc.constraint_name = kcu.constraint_name "
            "AND tc.table_name = kcu.table_name "
            "JOIN information_schema.referential_constraints rc "
            "ON tc.constraint_schema = rc.constraint_schema "
            "AND tc.constraint_name = rc.constraint_name "
            "JOIN information_schema.key_column_usage rkcu "
            "ON rkcu.constraint_schema = rc.unique_constraint_schema "
            "AND rkcu.constraint_name = rc.unique_constraint_name "
            "AND rkcu.ordinal_position = kcu.position_in_unique_constraint "
            f"WHERE tc.table_schema = {schema_filter} "
            f"AND tc.constraint_type = 'FOREIGN KEY' {table_filter} "
            "ORDER BY kcu.table_name, tc.constraint_name, kcu.ordinal_position"
        )
    elif dialect in {"mysql", "mariadb"}:
        table_filter = _table_name_filter(table_names, "kcu.table_name")
        sql = (
            "SELECT kcu.table_name, kcu.column_name, kcu.referenced_table_name, "
            "kcu.referenced_column_name, kcu.constraint_name, rc.delete_rule, "
            "rc.update_rule, kcu.ordinal_position, NULL, NULL, NULL, NULL, "
            "kcu.referenced_table_schema, NULL, kcu.table_schema "
            "FROM information_schema.key_column_usage kcu "
            "JOIN information_schema.referential_constraints rc "
            "ON kcu.constraint_schema = rc.constraint_schema "
            "AND kcu.constraint_name = rc.constraint_name "
            "AND kcu.table_name = rc.table_name "
            f"WHERE kcu.table_schema = {schema_filter} "
            f"AND kcu.referenced_table_name IS NOT NULL {table_filter} "
            "ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "parent_table.name")
        sql = (
            "SELECT parent_table.name, parent_column.name, "
            "referenced_table.name, referenced_column.name, fk.name, "
            "fk.delete_referential_action_desc, fk.update_referential_action_desc, "
            "fkc.constraint_column_id, NULL, NULL, "
            "CASE WHEN fk.is_not_trusted = 1 THEN 0 ELSE 1 END, "
            "NULL, referenced_schema.name, "
            "CONVERT(nvarchar(max), ep.value), parent_schema.name "
            "FROM sys.foreign_key_columns fkc "
            "JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id "
            "JOIN sys.tables parent_table ON fkc.parent_object_id = parent_table.object_id "
            "JOIN sys.schemas parent_schema ON parent_table.schema_id = parent_schema.schema_id "
            "JOIN sys.columns parent_column ON fkc.parent_object_id = parent_column.object_id "
            "AND fkc.parent_column_id = parent_column.column_id "
            "JOIN sys.tables referenced_table ON fkc.referenced_object_id = referenced_table.object_id "
            "JOIN sys.schemas referenced_schema ON referenced_table.schema_id = referenced_schema.schema_id "
            "JOIN sys.columns referenced_column ON fkc.referenced_object_id = referenced_column.object_id "
            "AND fkc.referenced_column_id = referenced_column.column_id "
            "LEFT JOIN sys.extended_properties ep ON ep.class = 1 "
            "AND ep.major_id = fk.object_id AND ep.minor_id = 0 "
            "AND ep.name = N'MS_Description' "
            f"WHERE parent_schema.name = {schema_filter} {table_filter} "
            "ORDER BY parent_table.name, fkc.constraint_object_id, fkc.constraint_column_id"
        )
    elif dialect == "oracle":
        constraints = _oracle_constraints_view(schema)
        columns = _oracle_cons_columns_view(schema)
        local_owner_join = ""
        referenced_owner_join = ""
        owner_filter = ""
        referenced_owner_column = "CAST(NULL AS VARCHAR2(1))"
        local_owner_column = "CAST(NULL AS VARCHAR2(1))"
        if schema:
            local_owner_join = "AND c.owner = cc.owner "
            referenced_owner_join = "AND rc.owner = rcc.owner "
            owner_filter = f"AND c.owner = {_sql_literal(schema.upper())} "
            referenced_owner_column = "rc.owner"
            local_owner_column = "c.owner"
        table_filter = _table_name_filter(table_names, "cc.table_name")
        sql = (
            "SELECT cc.table_name, cc.column_name, rcc.table_name, rcc.column_name, "
            "c.constraint_name, c.delete_rule, CAST(NULL AS VARCHAR2(1)), "
            f"cc.position, c.deferrable, c.deferred, c.validated, "
            f"CAST(NULL AS VARCHAR2(1)), {referenced_owner_column}, "
            f"CAST(NULL AS VARCHAR2(1)), {local_owner_column} "
            f"FROM {constraints} c "
            f"JOIN {columns} cc ON c.constraint_name = cc.constraint_name "
            f"{local_owner_join}AND c.table_name = cc.table_name "
            f"JOIN {constraints} rc ON c.r_constraint_name = rc.constraint_name "
            f"JOIN {columns} rcc ON rc.constraint_name = rcc.constraint_name "
            f"{referenced_owner_join}AND rc.table_name = rcc.table_name "
            "AND cc.position = rcc.position "
            "WHERE c.constraint_type = 'R' "
            f"{owner_filter}{table_filter} "
            "ORDER BY cc.table_name, c.constraint_name, cc.position"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    return _query_rows_url(rust, url, sql)


def _reflect_server_check_constraints(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, list[TableCheckSnapshot]]:
    if not table_names:
        return {}
    schema_filter = _schema_filter(dialect, schema)
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "t.relname")
        sql = (
            "SELECT t.relname, c.conname, pg_get_expr(c.conbin, c.conrelid), "
            "c.convalidated, c.connoinherit, "
            "obj_description(c.oid, 'pg_constraint') AS comment "
            "FROM pg_constraint c "
            "JOIN pg_class t ON c.conrelid = t.oid "
            "JOIN pg_namespace n ON t.relnamespace = n.oid "
            f"WHERE n.nspname = {schema_filter} "
            f"AND c.contype = 'c' {table_filter} "
            "ORDER BY t.relname, c.conname"
        )
    elif dialect == "mysql":
        table_filter = _table_name_filter(table_names, "tc.table_name")
        sql = (
            "SELECT tc.table_name, tc.constraint_name, cc.check_clause, "
            "tc.enforced, NULL, NULL "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.check_constraints cc "
            "ON tc.constraint_schema = cc.constraint_schema "
            "AND tc.constraint_name = cc.constraint_name "
            f"WHERE tc.table_schema = {schema_filter} "
            f"AND tc.constraint_type = 'CHECK' {table_filter} "
            "ORDER BY tc.table_name, tc.constraint_name"
        )
    elif dialect == "mariadb":
        table_filter = _table_name_filter(table_names, "tc.table_name")
        sql = (
            "SELECT tc.table_name, tc.constraint_name, cc.check_clause, NULL, NULL, NULL "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.check_constraints cc "
            "ON tc.constraint_schema = cc.constraint_schema "
            "AND tc.constraint_name = cc.constraint_name "
            f"WHERE tc.table_schema = {schema_filter} "
            f"AND tc.constraint_type = 'CHECK' {table_filter} "
            "ORDER BY tc.table_name, tc.constraint_name"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "tc.TABLE_NAME")
        sql = (
            "SELECT tc.TABLE_NAME, tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE, "
            "CASE WHEN check_ref.is_not_trusted = 1 THEN 0 ELSE 1 END, NULL, "
            "CONVERT(nvarchar(max), ep.value) "
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
            "JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS cc "
            "ON tc.CONSTRAINT_SCHEMA = cc.CONSTRAINT_SCHEMA "
            "AND tc.CONSTRAINT_NAME = cc.CONSTRAINT_NAME "
            "JOIN sys.schemas schema_ref ON schema_ref.name = tc.TABLE_SCHEMA "
            "JOIN sys.tables table_ref ON table_ref.name = tc.TABLE_NAME "
            "AND table_ref.schema_id = schema_ref.schema_id "
            "LEFT JOIN sys.check_constraints check_ref "
            "ON check_ref.parent_object_id = table_ref.object_id "
            "AND check_ref.name = tc.CONSTRAINT_NAME "
            "LEFT JOIN sys.extended_properties ep ON ep.class = 1 "
            "AND ep.major_id = check_ref.object_id AND ep.minor_id = 0 "
            "AND ep.name = N'MS_Description' "
            f"WHERE tc.TABLE_SCHEMA = {schema_filter} "
            f"AND tc.CONSTRAINT_TYPE = 'CHECK' {table_filter} "
            "ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME"
        )
    elif dialect == "oracle":
        constraints = _oracle_constraints_view(schema)
        owner_filter = ""
        if schema:
            owner_filter = f"AND owner = {_sql_literal(schema.upper())} "
        table_filter = _table_name_filter(table_names, "table_name")
        sql = (
            "SELECT table_name, constraint_name, search_condition_vc, validated, "
            "CAST(NULL AS VARCHAR2(1)), CAST(NULL AS VARCHAR2(1)) "
            f"FROM {constraints} WHERE constraint_type = 'C' "
            f"{owner_filter}AND generated = 'USER NAME' {table_filter} "
            "ORDER BY table_name, constraint_name"
        )
    else:
        raise ValueError(f"live autogenerate does not support dialect '{dialect}'")
    checks: dict[str, list[TableCheckSnapshot]] = {}
    for row in _query_rows_url(rust, url, sql):
        if row[2] is None:
            continue
        checks.setdefault(str(row[0]), []).append(
            TableCheckSnapshot(
                str(row[1]),
                str(row[2]),
                _normalize_reflected_validated(row[3] if len(row) > 3 else None),
                _db_truthy(row[4] if len(row) > 4 else None),
                _optional_str(row[5] if len(row) > 5 else None),
            )
        )
    return checks


def _reflect_server_exclusion_constraints(
    rust: Any,
    url: str,
    dialect: str,
    schema: str | None,
    table_names: Sequence[str],
) -> dict[str, list[ExclusionConstraintSnapshot]]:
    if not table_names or dialect != "postgresql":
        return {}
    schema_filter = _schema_filter(dialect, schema)
    table_filter = _table_name_filter(table_names, "t.relname")
    sql = (
        "SELECT t.relname, c.conname, pg_get_constraintdef(c.oid, true), "
        "c.condeferrable, c.condeferred, "
        "obj_description(c.oid, 'pg_constraint') AS comment "
        "FROM pg_constraint c "
        "JOIN pg_class t ON c.conrelid = t.oid "
        "JOIN pg_namespace n ON t.relnamespace = n.oid "
        f"WHERE n.nspname = {schema_filter} "
        f"AND c.contype = 'x' {table_filter} "
        "ORDER BY t.relname, c.conname"
    )
    constraints: dict[str, list[ExclusionConstraintSnapshot]] = {}
    for row in _query_rows_url(rust, url, sql):
        definition = _optional_str(row[2])
        if definition is None:
            continue
        constraints.setdefault(str(row[0]), []).append(
            replace(
                _parse_postgres_exclusion_constraint(
                    str(row[1]),
                    definition,
                    deferrable=_normalize_reflected_deferrable(
                        row[3] if len(row) > 3 else None
                    ),
                    initially_deferred=_normalize_reflected_initially_deferred(
                        row[4] if len(row) > 4 else None
                    ),
                ),
                comment=_optional_str(row[5] if len(row) > 5 else None),
            )
        )
    return constraints


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
    oracle_default_tablespace = _reflect_server_oracle_default_tablespace(
        rust,
        url,
        dialect,
        schema,
    )
    if dialect == "postgresql":
        table_filter = _table_name_filter(table_names, "t.relname")
        sql = (
            "SELECT t.relname, i.relname, ix.indisunique, a.attname, x.ordinality, "
            "pg_get_expr(ix.indpred, ix.indrelid), am.amname, "
            "pg_get_indexdef(i.oid, x.ordinality::int, true), "
            "(x.ordinality <= ix.indnkeyatts), "
            "(SELECT string_agg(opt.option_name || '=' || opt.option_value, ',' "
            "ORDER BY opt.option_name) FROM pg_options_to_table(i.reloptions) opt), "
            "obj_description(i.oid, 'pg_class') AS comment, "
            "ts.spcname AS tablespace, "
            "CASE WHEN opc.opcdefault THEN NULL "
            "WHEN opn.nspname IS NOT NULL AND opn.nspname <> 'pg_catalog' "
            "THEN opn.nspname || '.' || opc.opcname ELSE opc.opcname END AS opclass, "
            "COALESCE((to_jsonb(ix)->>'indnullsnotdistinct')::boolean, false) "
            "AS nulls_not_distinct "
            "FROM pg_class t "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            "JOIN pg_index ix ON t.oid = ix.indrelid "
            "JOIN pg_class i ON i.oid = ix.indexrelid "
            "JOIN pg_am am ON am.oid = i.relam "
            "JOIN unnest(ix.indkey) WITH ORDINALITY AS x(attnum, ordinality) ON true "
            "LEFT JOIN unnest(ix.indclass) WITH ORDINALITY AS oc(opclass_oid, opclass_ordinality) "
            "ON oc.opclass_ordinality = x.ordinality "
            "LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum "
            "LEFT JOIN pg_opclass opc ON opc.oid = oc.opclass_oid "
            "LEFT JOIN pg_namespace opn ON opn.oid = opc.opcnamespace "
            "LEFT JOIN pg_tablespace ts ON ts.oid = i.reltablespace "
            f"WHERE n.nspname = {schema_filter} {table_filter} AND NOT ix.indisprimary "
            "AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.oid "
            "AND c.contype IN ('p', 'u')) "
            "ORDER BY t.relname, i.relname, x.ordinality"
        )
    elif dialect == "mysql":
        table_filter = _table_name_filter(table_names, "s.table_name")
        sql = (
            "SELECT s.table_name, s.index_name, "
            "CASE s.non_unique WHEN 0 THEN 1 ELSE 0 END, "
            "s.column_name, s.seq_in_index, s.index_comment, s.sub_part, "
            "s.index_type, s.is_visible "
            "FROM information_schema.statistics s "
            f"WHERE s.table_schema = {schema_filter} {table_filter} "
            "AND s.index_name <> 'PRIMARY' "
            "AND NOT EXISTS ("
            "SELECT 1 FROM information_schema.table_constraints tc "
            "WHERE tc.table_schema = s.table_schema "
            "AND tc.table_name = s.table_name "
            "AND tc.constraint_name = s.index_name "
            "AND tc.constraint_type = 'UNIQUE') "
            "ORDER BY s.table_name, s.index_name, s.seq_in_index"
        )
    elif dialect == "mariadb":
        table_filter = _table_name_filter(table_names, "s.table_name")
        sql = (
            "SELECT s.table_name, s.index_name, "
            "CASE s.non_unique WHEN 0 THEN 1 ELSE 0 END, "
            "s.column_name, s.seq_in_index, s.index_comment, s.sub_part, "
            "s.index_type "
            "FROM information_schema.statistics s "
            f"WHERE s.table_schema = {schema_filter} {table_filter} "
            "AND s.index_name <> 'PRIMARY' "
            "AND NOT EXISTS ("
            "SELECT 1 FROM information_schema.table_constraints tc "
            "WHERE tc.table_schema = s.table_schema "
            "AND tc.table_name = s.table_name "
            "AND tc.constraint_name = s.index_name "
            "AND tc.constraint_type = 'UNIQUE') "
            "ORDER BY s.table_name, s.index_name, s.seq_in_index"
        )
    elif dialect == "mssql":
        table_filter = _table_name_filter(table_names, "t.name")
        sql = (
            "SELECT t.name, i.name, CONVERT(int, i.is_unique), c.name, "
            "CASE WHEN ic.is_included_column = 1 THEN ic.index_column_id ELSE ic.key_ordinal END, "
            "CONVERT(int, ic.is_included_column), i.filter_definition, "
            "CONVERT(nvarchar(max), ep.value), fg.name, "
            "CONVERT(int, CASE WHEN i.type_desc = 'CLUSTERED' THEN 1 ELSE 0 END) "
            "FROM sys.indexes i "
            "JOIN sys.tables t ON i.object_id = t.object_id "
            "JOIN sys.schemas s ON t.schema_id = s.schema_id "
            "JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
            "JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
            "LEFT JOIN sys.extended_properties ep ON ep.class = 7 "
            "AND ep.major_id = i.object_id AND ep.minor_id = i.index_id "
            "AND ep.name = N'MS_Description' "
            "LEFT JOIN sys.filegroups fg ON i.data_space_id = fg.data_space_id "
            "AND fg.is_default = 0 "
            f"WHERE s.name = {schema_filter} {table_filter} AND i.is_primary_key = 0 "
            "AND i.is_unique_constraint = 0 AND i.name IS NOT NULL "
            "ORDER BY t.name, i.name, ic.is_included_column, ic.key_ordinal, ic.index_column_id"
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
            "ic.column_name, ic.column_position, i.tablespace_name, "
            "i.index_type, i.compression, i.prefix_length "
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
            {
                "columns": [],
                "expressions": [],
                "include_columns": [],
                "unique": _db_truthy(row[2]),
                "where": None,
                "method": None,
                "postgres_with": [],
                "comment": None,
                "postgres_tablespace": None,
                "postgres_ops": {},
                "postgres_nulls_not_distinct": False,
                "mssql_filegroup": None,
                "mssql_clustered": False,
                "oracle_tablespace": None,
                "oracle_bitmap": False,
                "oracle_compress": None,
                "mysql_prefix": None,
                "mysql_length": {},
                "mysql_using": None,
                "mysql_visible": None,
            },
        )
        ordinal = _optional_int(row[4]) or 0
        if dialect == "postgresql":
            if row[5] is not None:
                index["where"] = str(row[5])
            if row[6] is not None and str(row[6]).lower() != "btree":
                index["method"] = str(row[6])
            if len(row) > 9 and row[9] is not None:
                index["postgres_with"] = _postgres_storage_parameters_text(row[9])
            if len(row) > 10 and row[10] is not None and index["comment"] is None:
                index["comment"] = _optional_str(row[10])
            if len(row) > 11 and row[11] is not None:
                index["postgres_tablespace"] = _optional_str(row[11])
            item = str(row[7]) if row[7] is not None else str(row[3])
            if len(row) > 12 and row[12] is not None:
                opclass_key = str(row[3]) if row[3] is not None else item
                index["postgres_ops"][opclass_key] = str(row[12])
            if len(row) > 13 and _db_truthy(row[13]):
                index["postgres_nulls_not_distinct"] = True
            if _db_truthy(row[8]):
                if row[3] is None:
                    index["expressions"].append((ordinal, item))
                else:
                    index["columns"].append((ordinal, str(row[3])))
            else:
                index["include_columns"].append((ordinal, item))
            continue
        if dialect == "mssql":
            if row[6] is not None:
                index["where"] = str(row[6])
            if len(row) > 7:
                comment = _optional_non_empty_str(row[7])
                if comment is not None and index["comment"] is None:
                    index["comment"] = comment
            if len(row) > 8 and row[8] is not None:
                index["mssql_filegroup"] = _optional_str(row[8])
            if len(row) > 9 and _db_truthy(row[9]):
                index["mssql_clustered"] = True
            if _db_truthy(row[5]):
                index["include_columns"].append((ordinal, str(row[3])))
            else:
                index["columns"].append((ordinal, str(row[3])))
            continue
        if dialect in {"mysql", "mariadb"} and len(row) > 5:
            comment = _optional_non_empty_str(row[5])
            if comment is not None and index["comment"] is None:
                index["comment"] = comment
            if len(row) > 6 and row[6] is not None:
                index["mysql_length"][str(row[3])] = int(row[6])
            if len(row) > 7 and row[7] is not None:
                index_type = str(row[7])
                index_type_key = index_type.lower()
                if index_type_key == "fulltext":
                    index["mysql_prefix"] = "FULLTEXT"
                elif index_type_key in {"spatial", "rtree"}:
                    index["mysql_prefix"] = "SPATIAL"
                elif index_type_key != "btree":
                    index["mysql_using"] = index_type
            if dialect == "mysql" and len(row) > 8 and row[8] is not None:
                if not _db_truthy(row[8]):
                    index["mysql_visible"] = False
        if dialect == "oracle":
            if len(row) > 5 and row[5] is not None:
                index["oracle_tablespace"] = _oracle_defaulted_tablespace(
                    _optional_str(row[5]),
                    oracle_default_tablespace,
                )
            if len(row) > 6 and row[6] is not None:
                index_type = str(row[6]).upper()
                if "BITMAP" in index_type:
                    index["oracle_bitmap"] = True
            if len(row) > 7 and row[7] is not None:
                compression = str(row[7]).strip().upper()
                if compression not in {"", "DISABLED", "NO", "NONE"}:
                    prefix_length = _optional_int(row[8]) if len(row) > 8 else None
                    index["oracle_compress"] = (
                        prefix_length
                        if prefix_length is not None and prefix_length > 0
                        else True
                    )
        index["columns"].append((ordinal, str(row[3])))
    return {
        table: [
            IndexSnapshot(
                name=name,
                columns=[
                    column
                    for _, column in sorted(index["columns"], key=lambda item: item[0])
                ],
                expressions=[
                    expression
                    for _, expression in sorted(
                        index["expressions"], key=lambda item: item[0]
                    )
                ],
                unique=bool(index["unique"]),
                where=index["where"],
                include_columns=[
                    column
                    for _, column in sorted(
                        index["include_columns"], key=lambda item: item[0]
                    )
                ],
                method=index["method"],
                postgres_with=index["postgres_with"],
                postgres_ops=dict(index["postgres_ops"]),
                postgres_nulls_not_distinct=bool(index["postgres_nulls_not_distinct"]),
                comment=index["comment"],
                postgres_tablespace=index["postgres_tablespace"],
                mssql_filegroup=index["mssql_filegroup"],
                mssql_clustered=bool(index["mssql_clustered"]),
                oracle_tablespace=index["oracle_tablespace"],
                oracle_bitmap=bool(index["oracle_bitmap"]),
                oracle_compress=index["oracle_compress"],
                mysql_prefix=index["mysql_prefix"],
                mysql_length=dict(index["mysql_length"]),
                mysql_using=index["mysql_using"],
                mysql_visible=index["mysql_visible"],
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


def _optional_non_empty_str(value: Any) -> str | None:
    text = _optional_str(value)
    if text is None:
        return None
    text = text.strip()
    return text or None


def _namespace_filter(
    dialect: str,
    schema_filter: str,
    extra_schemas: Sequence[str | None] | None,
    *,
    include_scope: bool = True,
) -> Callable[[str], str]:
    schemas = sorted({schema for schema in extra_schemas or [] if schema})
    if not schemas:
        return lambda column: f"{column} = {schema_filter}"
    names = ", ".join(
        _sql_literal(schema.upper() if dialect == "oracle" else schema)
        for schema in schemas
    )
    if not include_scope:
        return lambda column: f"{column} IN ({names})"
    return lambda column: f"({column} = {schema_filter} OR {column} IN ({names}))"


def _table_name_filter(table_names: Sequence[str], column: str) -> str:
    if not table_names:
        return "AND 1 = 0"
    names = ", ".join(_sql_literal(table) for table in table_names)
    return f"AND {column} IN ({names})"


def _parse_postgres_exclusion_constraint(
    name: str,
    definition: str,
    *,
    deferrable: bool | None,
    initially_deferred: bool,
) -> ExclusionConstraintSnapshot:
    prefix = "EXCLUDE USING "
    text = definition.strip()
    if not text.upper().startswith(prefix):
        return ExclusionConstraintSnapshot(
            name,
            expressions=[(text, "")],
            deferrable=deferrable,
            initially_deferred=initially_deferred,
        )
    remainder = text[len(prefix) :].strip()
    method, _, after_method = remainder.partition(" ")
    elements_text, tail = _take_parenthesized(after_method.strip())
    columns: list[tuple[str, str]] = []
    expressions: list[tuple[str, str]] = []
    ops: dict[str, str] = {}
    for element in _split_top_level(elements_text, ","):
        value, operator = _split_exclusion_element(element)
        value, opclass = _split_exclusion_value_opclass(value)
        column = _reflected_exclusion_column(value)
        if column is None:
            expressions.append((value, operator))
            if opclass is not None:
                ops[value] = opclass
        else:
            columns.append((column, operator))
            if opclass is not None:
                ops[column] = opclass
    where = _parse_exclusion_where(tail)
    return ExclusionConstraintSnapshot(
        name,
        columns=columns,
        expressions=expressions,
        using=method or "gist",
        where=where,
        deferrable=deferrable,
        initially_deferred=initially_deferred,
        ops=ops,
    )


def _take_parenthesized(text: str) -> tuple[str, str]:
    if not text.startswith("("):
        return text, ""
    depth = 0
    in_quote = False
    idx = 0
    while idx < len(text):
        char = text[idx]
        if char == '"':
            in_quote = not in_quote
        elif not in_quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return text[1:idx], text[idx + 1 :].strip()
        idx += 1
    return text[1:], ""


def _split_top_level(text: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    in_quote = False
    idx = 0
    while idx < len(text):
        char = text[idx]
        if char == '"':
            in_quote = not in_quote
        elif not in_quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0 and text.startswith(delimiter, idx):
                parts.append(text[start:idx].strip())
                idx += len(delimiter)
                start = idx
                continue
        idx += 1
    parts.append(text[start:].strip())
    return [part for part in parts if part]


def _split_exclusion_element(element: str) -> tuple[str, str]:
    marker = " WITH "
    upper = element.upper()
    depth = 0
    in_quote = False
    idx = 0
    while idx < len(element):
        char = element[idx]
        if char == '"':
            in_quote = not in_quote
        elif not in_quote:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0 and upper.startswith(marker, idx):
                return element[:idx].strip(), element[idx + len(marker) :].strip()
        idx += 1
    return element.strip(), ""


def _split_exclusion_value_opclass(value: str) -> tuple[str, str | None]:
    text = value.strip()
    if not text:
        return "", None
    if text.startswith('"'):
        end = _quoted_identifier_end(text)
        if end is not None:
            return _maybe_exclusion_opclass(text[: end + 1], text[end + 1 :])
    if text.startswith("("):
        expression, tail = _take_parenthesized(text)
        if tail:
            return _maybe_exclusion_opclass(expression.strip(), tail)
    function_match = re.match(r"[A-Za-z_][A-Za-z0-9_$]*\(", text)
    if function_match is not None:
        paren_start = text.find("(")
        expression, tail = _take_parenthesized(text[paren_start:])
        if tail:
            base = f"{text[:paren_start]}({expression})"
            return _maybe_exclusion_opclass(base, tail)
    identifier_match = re.match(r"[A-Za-z_][A-Za-z0-9_$]*", text)
    if identifier_match is not None:
        return _maybe_exclusion_opclass(
            identifier_match.group(0),
            text[identifier_match.end() :],
        )
    return text, None


def _maybe_exclusion_opclass(base: str, tail: str) -> tuple[str, str | None]:
    opclass = tail.strip()
    if opclass and _looks_like_postgres_opclass(opclass):
        return base.strip(), opclass
    return f"{base}{tail}".strip(), None


def _quoted_identifier_end(text: str) -> int | None:
    idx = 1
    while idx < len(text):
        if text[idx] != '"':
            idx += 1
            continue
        if idx + 1 < len(text) and text[idx + 1] == '"':
            idx += 2
            continue
        return idx
    return None


def _looks_like_postgres_opclass(value: str) -> bool:
    parts = value.split(".")
    if not 1 <= len(parts) <= 2:
        return False
    for part in parts:
        if re.fullmatch(r'"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_$]*', part) is None:
            return False
    return True


def _reflected_exclusion_column(value: str) -> str | None:
    if not value:
        return None
    if value.startswith('"') and value.endswith('"') and len(value) >= 2:
        return value[1:-1].replace('""', '"')
    if value.replace("_", "").isalnum() and not value[0].isdigit():
        return value
    return None


def _parse_exclusion_where(tail: str) -> str | None:
    tail = tail.strip()
    if not tail.upper().startswith("WHERE"):
        return None
    predicate, _rest = _take_parenthesized(tail[5:].strip())
    return _strip_outer_parentheses(predicate.strip())


def _strip_outer_parentheses(text: str) -> str:
    while text.startswith("(") and text.endswith(")"):
        inner, tail = _take_parenthesized(text)
        if tail:
            break
        text = inner.strip()
    return text


def _oracle_table_view(schema: str | None) -> str:
    return "all_tables" if schema else "user_tables"


def _oracle_table_comments_view(schema: str | None) -> str:
    return "all_tab_comments" if schema else "user_tab_comments"


def _oracle_column_comments_view(schema: str | None) -> str:
    return "all_col_comments" if schema else "user_col_comments"


def _oracle_tab_columns_view(schema: str | None) -> str:
    return "all_tab_cols" if schema else "user_tab_cols"


def _oracle_identity_columns_view(schema: str | None) -> str:
    return "all_tab_identity_cols" if schema else "user_tab_identity_cols"


def _oracle_constraints_view(schema: str | None) -> str:
    return "all_constraints" if schema else "user_constraints"


def _oracle_cons_columns_view(schema: str | None) -> str:
    return "all_cons_columns" if schema else "user_cons_columns"


def _oracle_indexes_view(schema: str | None) -> str:
    return "all_indexes" if schema else "user_indexes"


def _oracle_ind_columns_view(schema: str | None) -> str:
    return "all_ind_columns" if schema else "user_ind_columns"


def _oracle_sequences_view(schema: str | None) -> str:
    return "all_sequences" if schema else "user_sequences"


def _oracle_views_view(schema: str | None) -> str:
    return "all_views" if schema else "user_views"


def _oracle_materialized_views_view(schema: str | None) -> str:
    return "all_mviews" if schema else "user_mviews"


def _oracle_materialized_view_comments_view(schema: str | None) -> str:
    return "all_mview_comments" if schema else "user_mview_comments"


def _oracle_materialized_view_definition_sql(schema: str | None) -> str:
    owner_arg = ", m.owner" if schema else ""
    return (
        "DBMS_LOB.SUBSTR("
        f"DBMS_METADATA.GET_DDL('MATERIALIZED_VIEW', m.mview_name{owner_arg}), "
        "32000, 1)"
    )


def _reflected_oracle_materialized_view_definition(value: str) -> str:
    match = re.search(r"(?is)\bas\s+(select\b.+)\Z", value.strip())
    if match is None:
        return value
    return match.group(1).strip()


def _optional_view_comment(dialect: str, materialized: bool, value: Any) -> str | None:
    comment = _optional_str(value)
    if comment is None or comment == "":
        return None
    if (
        dialect == "oracle"
        and materialized
        and _is_oracle_materialized_view_default_comment(comment)
    ):
        return None
    return comment


def _is_oracle_materialized_view_default_comment(comment: str) -> bool:
    return (
        re.fullmatch(
            r"snapshot table for snapshot\s+\S+", comment.strip(), re.IGNORECASE
        )
        is not None
    )


def _oracle_owner_filter(schema: str | None, *, table_alias: str) -> str:
    if not schema:
        return ""
    prefix = f"{table_alias}." if table_alias else ""
    return f"WHERE {prefix}owner = {_sql_literal(schema.upper())}"


def _nullable_from_reflection(value: Any) -> bool:
    return str(value).strip().upper() in {"YES", "Y", "TRUE", "1"}


def _reflected_max_length(value: Any, data_type: Any = None) -> int | None:
    if _unbounded_text_data_type(data_type):
        return None
    length = _optional_int(value)
    if length is None or length <= 0:
        return None
    return length


def _unbounded_text_data_type(value: Any) -> bool:
    text = str(value or "").strip().upper()
    if not text:
        return False
    return text in {
        "BLOB",
        "BYTEA",
        "CLOB",
        "IMAGE",
        "JSON",
        "JSONB",
        "LONG",
        "LONGTEXT",
        "MEDIUMTEXT",
        "NCLOB",
        "NTEXT",
        "TEXT",
        "TINYTEXT",
        "XML",
    }


def _reflected_column_collation(dialect: str, row: Sequence[Any]) -> str | None:
    if dialect == "postgresql":
        return _optional_str(row[10] if len(row) > 10 else None)
    return _optional_str(row[9] if len(row) > 9 else None)


def _reflected_enum_type_name(
    dialect: str,
    schema: str | None,
    row: Sequence[Any],
) -> str | None:
    if dialect != "postgresql":
        return None
    enum_name = _optional_str(row[9] if len(row) > 9 else None)
    if enum_name is None:
        return None
    enum_schema = _optional_str(row[17] if len(row) > 17 else None)
    if enum_schema is None:
        return enum_name
    table_schema = _optional_str(row[18] if len(row) > 18 else None) or schema
    if enum_schema != table_schema:
        return f"{enum_schema}.{enum_name}"
    return enum_name


def _reflected_column_identity_options(
    dialect: str,
    row: Sequence[Any],
) -> dict[str, bool | int]:
    if dialect == "postgresql":
        if not _db_truthy(row[11] if len(row) > 11 else None):
            return {}
        generation = str(row[12] if len(row) > 12 and row[12] is not None else "")
        options: dict[str, bool | int] = {
            "identity": True,
            "identity_always": generation.strip().lower() == "always",
        }
        start = _optional_int(row[13] if len(row) > 13 else None)
        increment = _optional_int(row[14] if len(row) > 14 else None)
        if start is not None:
            options["identity_start"] = start
        if increment is not None:
            options["identity_increment"] = increment
        min_value = _optional_int(row[19] if len(row) > 19 else None)
        max_value = _optional_int(row[20] if len(row) > 20 else None)
        cycle = row[21] if len(row) > 21 else None
        cache = _optional_int(row[22] if len(row) > 22 else None)
        if min_value is not None:
            options["identity_min_value"] = min_value
        if max_value is not None:
            options["identity_max_value"] = max_value
        if _db_truthy(cycle):
            options["identity_cycle"] = True
        if cache is not None:
            options["identity_cache"] = cache
        return options
    if dialect in {"mysql", "mariadb"}:
        extra = str(row[10] if len(row) > 10 and row[10] is not None else "")
        if "auto_increment" in extra.lower():
            return {"autoincrement": True}
        return {}
    if dialect == "mssql":
        if not _db_truthy(row[10] if len(row) > 10 else None):
            return {}
        mssql_options: dict[str, bool | int] = {"identity": True}
        start = _optional_int(row[11] if len(row) > 11 else None)
        increment = _optional_int(row[12] if len(row) > 12 else None)
        if start is not None:
            mssql_options["identity_start"] = start
        if increment is not None:
            mssql_options["identity_increment"] = increment
        return mssql_options
    if dialect == "oracle":
        oracle_generation = _optional_str(row[10] if len(row) > 10 else None)
        if oracle_generation is None:
            return {}
        oracle_options: dict[str, bool | int] = {
            "identity": True,
            "identity_always": oracle_generation.strip().lower() == "always",
        }
        if "on null" in oracle_generation.strip().lower():
            oracle_options["identity_on_null"] = True
        oracle_options.update(
            _reflected_oracle_identity_sequence_options(
                row[12] if len(row) > 12 else None
            )
        )
        return oracle_options
    return {}


def _reflected_oracle_identity_sequence_options(
    value: Any,
) -> dict[str, bool | int]:
    text = _optional_str(value)
    if text is None:
        return {}
    normalized = text.upper().replace(":", " ").replace(",", " ")
    options: dict[str, bool | int] = {}
    start = _reflected_identity_option_int(
        normalized,
        (r"\bSTART(?:\s+WITH)?\s+(-?\d+)",),
    )
    increment = _reflected_identity_option_int(
        normalized,
        (r"\bINCREMENT(?:\s+BY)?\s+(-?\d+)",),
    )
    min_value = _reflected_identity_option_int(
        normalized,
        (r"\bMINVALUE\s+(-?\d+)", r"\bMIN_VALUE\s+(-?\d+)"),
    )
    max_value = _reflected_identity_option_int(
        normalized,
        (r"\bMAXVALUE\s+(-?\d+)", r"\bMAX_VALUE\s+(-?\d+)"),
    )
    no_min_value = _reflected_identity_no_bound_option(normalized, "MIN")
    no_max_value = _reflected_identity_no_bound_option(normalized, "MAX")
    cache = _reflected_identity_option_int(
        normalized,
        (r"\bCACHE\s+(-?\d+)", r"\bCACHE_SIZE\s+(-?\d+)"),
    )
    if start is not None:
        options["identity_start"] = start
    if increment is not None:
        options["identity_increment"] = increment
    if no_min_value:
        options["identity_no_min_value"] = True
    elif min_value is not None:
        options["identity_min_value"] = min_value
    if no_max_value:
        options["identity_no_max_value"] = True
    elif max_value is not None:
        options["identity_max_value"] = max_value
    if _reflected_identity_option_flag(normalized, "CYCLE"):
        options["identity_cycle"] = True
    if cache is not None and cache > 0:
        options["identity_cache"] = cache
    if _reflected_identity_option_flag(normalized, "ORDER"):
        options["identity_order"] = True
    return options


def _reflected_identity_no_bound_option(text: str, bound: str) -> bool:
    return bool(
        re.search(rf"\bNO\s*{bound}VALUE\b", text)
        or re.search(rf"\bNO_{bound}_?VALUE\b", text)
    )


def _reflected_identity_option_int(
    text: str,
    patterns: Sequence[str],
) -> int | None:
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return int(match.group(1))
    return None


def _reflected_identity_option_flag(text: str, option: str) -> bool:
    if re.search(rf"\b{option}_FLAG\s+Y\b", text):
        return True
    if re.search(rf"\bNO{option}\b", text) or re.search(
        rf"\b{option}_FLAG\s+N\b",
        text,
    ):
        return False
    return bool(re.search(rf"\b{option}\b", text))


def _reflected_column_computed_options(
    dialect: str,
    row: Sequence[Any],
) -> dict[str, str | bool]:
    if dialect == "postgresql":
        generated = str(row[15] if len(row) > 15 and row[15] is not None else "")
        expression = _optional_non_empty_str(row[16] if len(row) > 16 else None)
        if generated.strip().lower() == "always" and expression is not None:
            return {"computed": expression, "computed_persisted": True}
        return {}
    if dialect in {"mysql", "mariadb"}:
        expression = _optional_non_empty_str(row[11] if len(row) > 11 else None)
        if expression is None:
            return {}
        extra = str(row[10] if len(row) > 10 and row[10] is not None else "")
        return {
            "computed": expression,
            "computed_persisted": "stored" in extra.lower(),
        }
    if dialect == "mssql":
        expression = _optional_non_empty_str(row[13] if len(row) > 13 else None)
        if expression is None:
            return {}
        return {
            "computed": _strip_outer_parentheses(expression.strip()),
            "computed_persisted": _db_truthy(row[14] if len(row) > 14 else None),
        }
    if dialect == "oracle":
        if not _db_truthy(row[11] if len(row) > 11 else None):
            return {}
        expression = _optional_non_empty_str(row[8] if len(row) > 8 else None)
        if expression is None:
            return {}
        return {"computed": expression}
    return {}


def _normalize_reflected_type(
    dialect: str,
    value: Any,
    *,
    precision: int | None = None,
    scale: int | None = None,
    enum_type_name: str | None = None,
) -> str:
    if dialect == "postgresql" and enum_type_name:
        return f"enum:{enum_type_name}"
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
        table_sql = _reflect_sqlite_table_sql(rust, url, table_name)
        sqlite_strict, sqlite_without_rowid = _sqlite_table_options(table_sql)
        check_constraints = _sqlite_named_check_constraints(table_sql)
        column_collations = _sqlite_column_collations(table_sql)
        column_conflicts = _sqlite_column_conflicts(table_sql)
        column_generated = _sqlite_column_generated(table_sql)
        autoincrement_columns = _sqlite_autoincrement_columns(table_sql)
        named_unique_by_columns = {
            tuple(constraint.columns): constraint
            for constraint in _sqlite_named_unique_constraints(table_sql)
        }
        named_foreign_key_constraints = _sqlite_named_foreign_key_constraints(table_sql)
        single_column_foreign_keys = {
            constraint.columns[0]: constraint
            for constraint in named_foreign_key_constraints
            if not _foreign_key_constraint_requires_table_snapshot(constraint)
        }
        table_foreign_key_columns = {
            column
            for constraint in named_foreign_key_constraints
            if _foreign_key_constraint_requires_table_snapshot(constraint)
            for column in constraint.columns
        }
        columns_info = list(runtime.columns(table_name))
        foreign_keys = list(runtime.foreign_keys(table_name))
        foreign_map: dict[str, ForeignKeyReflection] = {}
        for item in foreign_keys:
            if not item.get("from") or not item.get("table") or not item.get("to"):
                continue
            local_column = str(item["from"])
            if local_column in table_foreign_key_columns:
                continue
            foreign_key = sqlite_reflected_foreign_key(item)
            named_foreign_key = single_column_foreign_keys.get(local_column)
            if named_foreign_key is not None:
                if foreign_key.get("name") is None:
                    foreign_key["name"] = named_foreign_key.name
                if foreign_key.get("on_delete") is None:
                    foreign_key["on_delete"] = named_foreign_key.on_delete
                if foreign_key.get("on_update") is None:
                    foreign_key["on_update"] = named_foreign_key.on_update
                foreign_key["deferrable"] = named_foreign_key.deferrable
                foreign_key["initially_deferred"] = named_foreign_key.initially_deferred
            foreign_map[local_column] = foreign_key
        index_rows = list(runtime.indexes(table_name, True))
        index_sql_by_name = _reflect_sqlite_index_sql(rust, url, table_name)
        indexes: list[IndexSnapshot] = []
        unique_constraints: list[list[str]] = []
        named_unique_constraints: list[UniqueConstraintSnapshot] = []
        used_named_unique_constraints: set[tuple[str, ...]] = set()
        unique_columns: set[str] = set()
        for index_row in index_rows:
            if not index_row.get("name"):
                continue
            index_name = str(index_row["name"])
            index_columns = [str(column) for column in index_row.get("columns", [])]
            unique = _db_truthy(index_row.get("unique"))
            origin = _optional_str(index_row.get("origin"))
            if unique and origin == "u" and index_columns:
                index_columns_key = tuple(index_columns)
                named_constraint = named_unique_by_columns.get(index_columns_key)
                if named_constraint is not None:
                    if _sqlite_generated_unique_constraint(
                        table_name,
                        named_constraint,
                    ):
                        column_name = named_constraint.columns[0]
                        unique_columns.add(column_name)
                        if named_constraint.sqlite_on_conflict is not None:
                            column_conflicts.setdefault(column_name, {})[
                                "sqlite_on_conflict_unique"
                            ] = named_constraint.sqlite_on_conflict
                    elif index_columns_key not in used_named_unique_constraints:
                        named_unique_constraints.append(named_constraint)
                        used_named_unique_constraints.add(index_columns_key)
                elif len(index_columns) == 1:
                    unique_columns.add(index_columns[0])
                else:
                    unique_constraints.append(index_columns)
                continue
            if origin == "pk" or index_name.startswith("sqlite_autoindex_"):
                continue
            index_expressions: list[str] = []
            index_where: str | None = None
            if index_name in index_sql_by_name:
                index_columns, index_expressions, index_where = _sqlite_index_metadata(
                    index_sql_by_name[index_name],
                    index_columns,
                )
            indexes.append(
                IndexSnapshot(
                    name=index_name,
                    columns=index_columns,
                    unique=unique,
                    where=index_where,
                    expressions=index_expressions,
                )
            )
        columns: list[ColumnSnapshot] = []
        primary_key = "id"
        for item in columns_info:
            column_name = str(item["name"])
            raw_column_type = item.get("type")
            column_type = _normalize_sqlite_type(raw_column_type)
            numeric_precision, numeric_scale = _sqlite_numeric_precision_scale(
                raw_column_type
            )
            pk = _db_truthy(item.get("primary_key"))
            if pk and primary_key == "id":
                primary_key = column_name
            foreign_key = foreign_map.get(column_name, {})
            computed, computed_persisted = column_generated.get(
                column_name,
                (None, False),
            )
            conflict_options = column_conflicts.get(column_name, {})
            columns.append(
                ColumnSnapshot(
                    name=column_name,
                    kind=column_type,
                    nullable=_db_truthy(item.get("nullable")) and not pk,
                    primary_key=pk,
                    foreign_table=_optional_str(foreign_key.get("foreign_table")),
                    foreign_column=_optional_str(foreign_key.get("foreign_column")),
                    foreign_key_name=_optional_str(foreign_key.get("name")),
                    on_delete=_optional_str(foreign_key.get("on_delete")),
                    on_update=_optional_str(foreign_key.get("on_update")),
                    deferrable=_optional_bool(foreign_key.get("deferrable")),
                    initially_deferred=bool(
                        foreign_key.get("initially_deferred", False)
                    ),
                    unique=column_name in unique_columns,
                    server_default=_optional_str(item.get("default")),
                    computed=computed,
                    computed_persisted=computed_persisted,
                    autoincrement=column_name in autoincrement_columns,
                    collation=column_collations.get(column_name),
                    numeric_precision=numeric_precision,
                    numeric_scale=numeric_scale,
                    sqlite_on_conflict_primary_key=conflict_options.get(
                        "sqlite_on_conflict_primary_key"
                    ),
                    sqlite_on_conflict_not_null=conflict_options.get(
                        "sqlite_on_conflict_not_null"
                    ),
                    sqlite_on_conflict_unique=conflict_options.get(
                        "sqlite_on_conflict_unique"
                    ),
                )
            )
        if (
            columns
            and primary_key == "id"
            and not any(column.primary_key for column in columns)
        ):
            primary_key = columns[0].name
        columns, check_constraints = _normalize_generated_column_checks(
            "sqlite",
            table_name,
            columns,
            check_constraints,
        )
        tables.append(
            TableSnapshot(
                model_key=table_name,
                name=table_name,
                primary_key=primary_key,
                columns=columns,
                indexes=indexes,
                unique_constraints=unique_constraints,
                named_unique_constraints=named_unique_constraints,
                check_constraints=check_constraints,
                foreign_key_constraints=[
                    constraint
                    for constraint in named_foreign_key_constraints
                    if _foreign_key_constraint_requires_table_snapshot(constraint)
                ],
                sqlite_strict=sqlite_strict,
                sqlite_without_rowid=sqlite_without_rowid,
                relationships=[],
            )
        )
    views = _reflect_sqlite_views(rust, url, include_tables, exclude_tables)
    return SchemaSnapshot(
        tables=tables,
        views=views,
        version=MIGRATION_ARTIFACT_VERSION,
    )


def _reflect_sqlite_views(
    rust: Any,
    url: str,
    include_tables: Sequence[str] | None,
    exclude_tables: Sequence[str] | None,
) -> list[ViewSnapshot]:
    sql = "SELECT name, sql FROM sqlite_master WHERE type = 'view' ORDER BY name"
    views: list[ViewSnapshot] = []
    for row in _query_rows_url(rust, url, sql):
        if len(row) < 2 or row[1] is None:
            continue
        view_name = str(row[0])
        if not _table_matches_filters(view_name, include_tables, exclude_tables):
            continue
        views.append(
            ViewSnapshot(
                view_name,
                _sqlite_view_definition(str(row[1])),
            )
        )
    return views


def _reflect_sqlite_table_checks(
    rust: Any,
    url: str,
    table_name: str,
) -> list[TableCheckSnapshot]:
    return _sqlite_named_check_constraints(
        _reflect_sqlite_table_sql(rust, url, table_name)
    )


def _reflect_sqlite_table_sql(
    rust: Any,
    url: str,
    table_name: str,
) -> str:
    sql = (
        "SELECT sql FROM sqlite_master WHERE type = 'table' "
        f"AND name = {_sql_literal(table_name)}"
    )
    rows = _query_rows_url(rust, url, sql)
    if not rows or not rows[0] or rows[0][0] is None:
        return ""
    return str(rows[0][0])


def _reflect_sqlite_index_sql(
    rust: Any,
    url: str,
    table_name: str,
) -> dict[str, str]:
    sql = (
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' "
        f"AND tbl_name = {_sql_literal(table_name)} AND sql IS NOT NULL ORDER BY name"
    )
    index_sql: dict[str, str] = {}
    for row in _query_rows_url(rust, url, sql):
        if len(row) < 2 or row[0] is None or row[1] is None:
            continue
        index_sql[str(row[0])] = str(row[1])
    return index_sql


def _sqlite_view_definition(sql: str) -> str:
    cursor = _find_sql_keyword(sql, "AS", 0)
    if cursor < 0:
        return normalized_view_definition(sql)
    return normalized_view_definition(sql[cursor + len("AS") :])


def _sqlite_table_options(sql: str) -> tuple[bool, bool]:
    if not sql:
        return False, False
    cursor = _find_sql_char(sql, "(", 0)
    if cursor < 0:
        return False, False
    _body, end = _read_parenthesized_sql(sql, cursor)
    if _body is None:
        return False, False
    tail = sql[end:].strip().rstrip(";")
    strict = _find_sql_keyword(tail, "STRICT", 0) >= 0
    without_rowid = False
    position = _find_sql_keyword(tail, "WITHOUT", 0)
    while position >= 0:
        if _sql_keyword_sequence_end(tail, ("WITHOUT", "ROWID"), position) is not None:
            without_rowid = True
            break
        position = _find_sql_keyword(tail, "WITHOUT", position + len("WITHOUT"))
    return strict, without_rowid


def _sqlite_column_collations(sql: str) -> dict[str, str]:
    collations: dict[str, str] = {}
    for column_name, segment, segment_cursor in _sqlite_column_definition_segments(sql):
        match = _find_sql_top_level_keyword_sequence(
            segment,
            ("COLLATE",),
            segment_cursor,
        )
        if match is None:
            continue
        _start, collate_cursor = match
        collate_cursor = _skip_sql_whitespace(segment, collate_cursor)
        collation, _end = _read_sql_identifier(segment, collate_cursor)
        if collation is not None:
            collations[column_name] = collation
    return collations


def _sqlite_column_conflicts(sql: str) -> dict[str, dict[str, str]]:
    conflicts: dict[str, dict[str, str]] = {}
    for column_name, segment, segment_cursor in _sqlite_column_definition_segments(sql):
        options: dict[str, str] = {}
        primary_key_conflict = _sqlite_column_constraint_conflict(
            segment,
            segment_cursor,
            ("PRIMARY", "KEY"),
        )
        if primary_key_conflict is not None:
            options["sqlite_on_conflict_primary_key"] = primary_key_conflict
        not_null_conflict = _sqlite_column_constraint_conflict(
            segment,
            segment_cursor,
            ("NOT", "NULL"),
        )
        if not_null_conflict is not None:
            options["sqlite_on_conflict_not_null"] = not_null_conflict
        unique_conflict = _sqlite_column_constraint_conflict(
            segment,
            segment_cursor,
            ("UNIQUE",),
        )
        if unique_conflict is not None:
            options["sqlite_on_conflict_unique"] = unique_conflict
        if options:
            conflicts[column_name] = options
    return conflicts


def _sqlite_column_constraint_conflict(
    segment: str,
    cursor: int,
    keywords: tuple[str, ...],
) -> str | None:
    match = _find_sql_top_level_keyword_sequence(segment, keywords, cursor)
    if match is None:
        return None
    _start, conflict_cursor = match
    conflict_cursor = _skip_sql_whitespace(segment, conflict_cursor)
    if keywords == ("PRIMARY", "KEY"):
        order_cursor = _sql_keyword_sequence_end(segment, ("ASC",), conflict_cursor)
        if order_cursor is None:
            order_cursor = _sql_keyword_sequence_end(
                segment,
                ("DESC",),
                conflict_cursor,
            )
        if order_cursor is not None:
            conflict_cursor = _skip_sql_whitespace(segment, order_cursor)
    conflict_end = _sql_keyword_sequence_end(
        segment,
        ("ON", "CONFLICT"),
        conflict_cursor,
    )
    if conflict_end is None:
        return None
    conflict_cursor = conflict_end
    conflict_cursor = _skip_sql_whitespace(segment, conflict_cursor)
    policy, _end = _read_sql_identifier(segment, conflict_cursor)
    return _normalize_sqlite_conflict_policy(policy)


def _sqlite_column_generated(sql: str) -> dict[str, tuple[str, bool]]:
    generated: dict[str, tuple[str, bool]] = {}
    for column_name, segment, segment_cursor in _sqlite_column_definition_segments(sql):
        match = _find_sql_top_level_keyword_sequence(
            segment,
            ("GENERATED", "ALWAYS", "AS"),
            segment_cursor,
        )
        if match is None:
            continue
        _start, generated_cursor = match
        generated_cursor = _skip_sql_whitespace(segment, generated_cursor)
        if generated_cursor >= len(segment) or segment[generated_cursor] != "(":
            continue
        expression, generated_cursor = _read_parenthesized_sql(
            segment,
            generated_cursor,
        )
        if expression is None:
            continue
        persisted = (
            _find_sql_top_level_keyword_sequence(
                segment,
                ("STORED",),
                generated_cursor,
            )
            is not None
        )
        generated[column_name] = (expression.strip(), persisted)
    return generated


def _sqlite_autoincrement_columns(sql: str) -> set[str]:
    columns: set[str] = set()
    for column_name, segment, segment_cursor in _sqlite_column_definition_segments(sql):
        if (
            _find_sql_top_level_keyword_sequence(
                segment,
                ("AUTOINCREMENT",),
                segment_cursor,
            )
            is not None
        ):
            columns.add(column_name)
    return columns


def _sqlite_column_definition_segments(sql: str) -> list[tuple[str, str, int]]:
    if not sql:
        return []
    cursor = _find_sql_char(sql, "(", 0)
    if cursor < 0:
        return []
    body, _end = _read_parenthesized_sql(sql, cursor)
    if body is None:
        return []
    columns: list[tuple[str, str, int]] = []
    for segment in _split_sql_top_level_commas(body):
        segment_cursor = _skip_sql_whitespace(segment, 0)
        if _sqlite_table_constraint_segment(segment, segment_cursor):
            continue
        column_name, segment_cursor = _read_sql_identifier(segment, segment_cursor)
        if column_name is not None:
            columns.append((column_name, segment, segment_cursor))
    return columns


def _sqlite_generated_unique_constraint(
    table_name: str,
    constraint: UniqueConstraintSnapshot,
) -> bool:
    return (
        len(constraint.columns) == 1
        and constraint.name.startswith(f"{table_name}_unique_")
        and constraint.name.removeprefix(f"{table_name}_unique_").isdigit()
    )


def _sqlite_index_metadata(
    sql: str,
    reflected_columns: Sequence[str],
) -> tuple[list[str], list[str], str | None]:
    elements, tail = _sqlite_index_elements(sql)
    where = _sqlite_index_where(tail)
    if not elements:
        return list(reflected_columns), [], where

    columns: list[str] = []
    expressions: list[str] = []
    for element in elements:
        column = _sqlite_plain_index_column(element)
        if column is None:
            expressions.append(element)
        else:
            columns.append(column)
    return columns, expressions, where


def _sqlite_index_elements(sql: str) -> tuple[list[str], str]:
    match = _find_sql_top_level_keyword_sequence(sql, ("ON",), 0)
    if match is None:
        return [], ""
    _start, cursor = match
    cursor = _skip_sql_whitespace(sql, cursor)
    _table_name, cursor = _read_sql_identifier(sql, cursor)
    if _table_name is None:
        return [], ""
    cursor = _skip_sql_whitespace(sql, cursor)
    while cursor < len(sql) and sql[cursor] == ".":
        cursor = _skip_sql_whitespace(sql, cursor + 1)
        _table_name, cursor = _read_sql_identifier(sql, cursor)
        if _table_name is None:
            return [], ""
        cursor = _skip_sql_whitespace(sql, cursor)
    if cursor >= len(sql) or sql[cursor] != "(":
        return [], ""
    body, end = _read_parenthesized_sql(sql, cursor)
    if body is None:
        return [], ""
    return _split_sql_top_level_commas(body), sql[end:].strip()


def _sqlite_index_where(tail: str) -> str | None:
    match = _find_sql_top_level_keyword_sequence(tail, ("WHERE",), 0)
    if match is None:
        return None
    _start, cursor = match
    where = tail[cursor:].strip().rstrip(";").strip()
    return where or None


def _sqlite_plain_index_column(element: str) -> str | None:
    cursor = _skip_sql_whitespace(element, 0)
    column, cursor = _read_sql_identifier(element, cursor)
    if column is None:
        return None
    cursor = _skip_sql_whitespace(element, cursor)
    if cursor != len(element):
        return None
    return column


def _sqlite_table_constraint_segment(segment: str, cursor: int) -> bool:
    return any(
        _sql_keyword_at(segment, keyword, cursor)
        for keyword in ("CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN")
    )


def _sqlite_named_check_constraints(sql: str) -> list[TableCheckSnapshot]:
    return [
        TableCheckSnapshot(name, expression.strip())
        for name, expression, _tail in _sqlite_named_constraint_segments(
            sql, ("CHECK",)
        )
    ]


def _sqlite_named_unique_constraints(sql: str) -> list[UniqueConstraintSnapshot]:
    constraints: list[UniqueConstraintSnapshot] = []
    for name, expression, tail in _sqlite_named_constraint_segments(sql, ("UNIQUE",)):
        columns = _sqlite_identifier_list(expression)
        if columns:
            constraints.append(
                UniqueConstraintSnapshot(
                    name,
                    columns,
                    sqlite_on_conflict=_sqlite_conflict_from_tail(tail),
                )
            )
    return constraints


def _sqlite_named_foreign_key_names(sql: str) -> dict[str, str]:
    foreign_keys: dict[str, str] = {}
    for constraint in _sqlite_named_foreign_key_constraints(sql):
        if len(constraint.columns) == 1:
            foreign_keys[constraint.columns[0]] = constraint.name
    return foreign_keys


def _sqlite_named_foreign_key_constraints(
    sql: str,
) -> list[ForeignKeyConstraintSnapshot]:
    constraints: list[ForeignKeyConstraintSnapshot] = []
    for name, expression, tail in _sqlite_named_constraint_segments(
        sql, ("FOREIGN", "KEY")
    ):
        columns = _sqlite_identifier_list(expression)
        if not columns:
            continue
        foreign_table, foreign_columns = _sqlite_foreign_key_reference(tail)
        if foreign_table is None or not foreign_columns:
            continue
        deferrable, initially_deferred = _sqlite_foreign_key_timing(tail)
        constraints.append(
            ForeignKeyConstraintSnapshot(
                name,
                columns,
                foreign_table,
                foreign_columns,
                on_delete=_sqlite_foreign_key_action(tail, ("ON", "DELETE")),
                on_update=_sqlite_foreign_key_action(tail, ("ON", "UPDATE")),
                deferrable=deferrable,
                initially_deferred=initially_deferred,
                match=_sqlite_foreign_key_match(tail),
            )
        )
    return constraints


def _sqlite_foreign_key_reference(tail: str) -> tuple[str | None, list[str]]:
    match = _find_sql_keyword_sequence(tail, ("REFERENCES",), 0)
    if match is None:
        return None, []
    _start, cursor = match
    cursor = _skip_sql_whitespace(tail, cursor)
    foreign_table, cursor = _read_sql_identifier(tail, cursor)
    if foreign_table is None:
        return None, []
    cursor = _skip_sql_whitespace(tail, cursor)
    if cursor >= len(tail) or tail[cursor] != "(":
        return foreign_table, []
    expression, _end = _read_parenthesized_sql(tail, cursor)
    if expression is None:
        return foreign_table, []
    return foreign_table, _sqlite_identifier_list(expression)


def _sqlite_foreign_key_action(
    tail: str,
    keywords: tuple[str, ...],
) -> str | None:
    match = _find_sql_keyword_sequence(tail, keywords, 0)
    if match is None:
        return None
    _start, cursor = match
    cursor = _skip_sql_whitespace(tail, cursor)
    for action in (
        ("SET", "NULL"),
        ("SET", "DEFAULT"),
        ("NO", "ACTION"),
        ("CASCADE",),
        ("RESTRICT",),
    ):
        if _sql_keyword_sequence_end(tail, action, cursor) is not None:
            return _normalize_reflected_foreign_key_action("sqlite", " ".join(action))
    return None


def _sqlite_foreign_key_match(tail: str) -> str | None:
    match = _find_sql_keyword_sequence(tail, ("MATCH",), 0)
    if match is None:
        return None
    _start, cursor = match
    cursor = _skip_sql_whitespace(tail, cursor)
    match_type, _end = _read_sql_identifier(tail, cursor)
    return _normalize_reflected_foreign_key_match(match_type)


def _sqlite_foreign_key_timing(tail: str) -> tuple[bool | None, bool]:
    deferrable: bool | None = None
    if _find_sql_keyword_sequence(tail, ("NOT", "DEFERRABLE"), 0) is not None:
        deferrable = False
    elif _find_sql_keyword(tail, "DEFERRABLE", 0) >= 0:
        deferrable = True
    initially_deferred = (
        _find_sql_keyword_sequence(tail, ("INITIALLY", "DEFERRED"), 0) is not None
    )
    if initially_deferred and deferrable is None:
        deferrable = True
    return deferrable, initially_deferred


def _sqlite_named_constraint_bodies(
    sql: str,
    keywords: tuple[str, ...],
) -> list[tuple[str, str]]:
    return [
        (name, expression)
        for name, expression, _tail in _sqlite_named_constraint_segments(sql, keywords)
    ]


def _sqlite_named_constraint_segments(
    sql: str,
    keywords: tuple[str, ...],
) -> list[tuple[str, str, str]]:
    bodies: list[tuple[str, str, str]] = []
    if not sql:
        return bodies
    position = 0
    while True:
        constraint_start = _find_sql_keyword(sql, "CONSTRAINT", position)
        if constraint_start < 0:
            break
        cursor = _skip_sql_whitespace(sql, constraint_start + len("CONSTRAINT"))
        name, cursor = _read_sql_identifier(sql, cursor)
        if name is None:
            position = cursor + 1
            continue
        cursor = _skip_sql_whitespace(sql, cursor)
        keyword_end = _sql_keyword_sequence_end(sql, keywords, cursor)
        if keyword_end is None:
            position = cursor + 1
            continue
        cursor = _skip_sql_whitespace(sql, keyword_end)
        if cursor >= len(sql) or sql[cursor] != "(":
            position = cursor + 1
            continue
        expression, end = _read_parenthesized_sql(sql, cursor)
        if expression is not None:
            bodies.append((name, expression, _sqlite_constraint_tail(sql, end)))
        position = max(end, cursor + 1)
    return bodies


def _sqlite_constraint_tail(sql: str, position: int) -> str:
    cursor = position
    quote: str | None = None
    bracketed = False
    depth = 0
    while cursor < len(sql):
        char = sql[cursor]
        if bracketed:
            if char == "]":
                bracketed = False
            cursor += 1
            continue
        if quote is not None:
            if char == quote:
                if quote in {"'", '"'} and cursor + 1 < len(sql):
                    if sql[cursor + 1] == quote:
                        cursor += 2
                        continue
                quote = None
            cursor += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            cursor += 1
            continue
        if char == "[":
            bracketed = True
            cursor += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            if depth == 0:
                break
            depth -= 1
        elif char == "," and depth == 0:
            break
        cursor += 1
    return sql[position:cursor].strip()


def _sqlite_conflict_from_tail(tail: str) -> str | None:
    position = _find_sql_keyword(tail, "ON", 0)
    if position < 0:
        return None
    cursor = _sql_keyword_sequence_end(tail, ("ON", "CONFLICT"), position)
    if cursor is None:
        return None
    cursor = _skip_sql_whitespace(tail, cursor)
    policy, _end = _read_sql_identifier(tail, cursor)
    return _normalize_sqlite_conflict_policy(policy)


def _normalize_sqlite_conflict_policy(policy: str | None) -> str | None:
    if policy is None:
        return None
    normalized = policy.upper()
    if normalized in {"ROLLBACK", "ABORT", "FAIL", "IGNORE", "REPLACE"}:
        return normalized
    return None


def _sqlite_identifier_list(expression: str) -> list[str]:
    identifiers: list[str] = []
    position = 0
    while position < len(expression):
        position = _skip_sql_whitespace(expression, position)
        if position < len(expression) and expression[position] == ",":
            position += 1
            continue
        identifier, end = _read_sql_identifier(expression, position)
        if identifier is None:
            break
        identifiers.append(identifier)
        position = _skip_sql_whitespace(expression, end)
        if position >= len(expression):
            break
        if expression[position] == ",":
            position += 1
            continue
        break
    return identifiers


def _split_sql_top_level_commas(sql: str) -> list[str]:
    parts: list[str] = []
    start = 0
    cursor = 0
    quote: str | None = None
    bracketed = False
    depth = 0
    while cursor < len(sql):
        char = sql[cursor]
        if bracketed:
            if char == "]":
                bracketed = False
            cursor += 1
            continue
        if quote is not None:
            if char == quote:
                if quote in {"'", '"'} and cursor + 1 < len(sql):
                    if sql[cursor + 1] == quote:
                        cursor += 2
                        continue
                quote = None
            cursor += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            cursor += 1
            continue
        if char == "[":
            bracketed = True
            cursor += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            if depth > 0:
                depth -= 1
        elif char == "," and depth == 0:
            part = sql[start:cursor].strip()
            if part:
                parts.append(part)
            start = cursor + 1
        cursor += 1
    part = sql[start:].strip()
    if part:
        parts.append(part)
    return parts


def _sql_keyword_sequence_end(
    sql: str,
    keywords: tuple[str, ...],
    position: int,
) -> int | None:
    cursor = position
    for keyword in keywords:
        cursor = _skip_sql_whitespace(sql, cursor)
        if not _sql_keyword_at(sql, keyword, cursor):
            return None
        cursor += len(keyword)
    return cursor


def _find_sql_keyword_sequence(
    sql: str,
    keywords: tuple[str, ...],
    start: int,
) -> tuple[int, int] | None:
    position = _find_sql_keyword(sql, keywords[0], start)
    while position >= 0:
        end = _sql_keyword_sequence_end(sql, keywords, position)
        if end is not None:
            return position, end
        position = _find_sql_keyword(sql, keywords[0], position + len(keywords[0]))
    return None


def _find_sql_top_level_keyword_sequence(
    sql: str,
    keywords: tuple[str, ...],
    start: int,
) -> tuple[int, int] | None:
    position = start
    quote: str | None = None
    bracketed = False
    depth = 0
    while position < len(sql):
        char = sql[position]
        if bracketed:
            if char == "]":
                bracketed = False
            position += 1
            continue
        if quote is not None:
            if char == quote:
                if quote in {"'", '"'} and position + 1 < len(sql):
                    if sql[position + 1] == quote:
                        position += 2
                        continue
                quote = None
            position += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            position += 1
            continue
        if char == "[":
            bracketed = True
            position += 1
            continue
        if char == "(":
            depth += 1
            position += 1
            continue
        if char == ")":
            if depth > 0:
                depth -= 1
            position += 1
            continue
        if depth == 0:
            end = _sql_keyword_sequence_end(sql, keywords, position)
            if end is not None:
                return position, end
        position += 1
    return None


def _skip_sql_whitespace(sql: str, position: int) -> int:
    while position < len(sql) and sql[position].isspace():
        position += 1
    return position


def _find_sql_char(sql: str, target: str, start: int = 0) -> int:
    position = start
    quote: str | None = None
    bracketed = False
    while position < len(sql):
        char = sql[position]
        if bracketed:
            if char == "]":
                bracketed = False
            position += 1
            continue
        if quote is not None:
            if char == quote:
                if quote in {"'", '"'} and position + 1 < len(sql):
                    if sql[position + 1] == quote:
                        position += 2
                        continue
                quote = None
            position += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            position += 1
            continue
        if char == "[":
            bracketed = True
            position += 1
            continue
        if char == target:
            return position
        position += 1
    return -1


def _find_sql_keyword(sql: str, keyword: str, start: int = 0) -> int:
    keyword_lower = keyword.lower()
    keyword_len = len(keyword)
    position = start
    quote: str | None = None
    bracketed = False
    while position <= len(sql) - keyword_len:
        char = sql[position]
        if bracketed:
            if char == "]":
                bracketed = False
            position += 1
            continue
        if quote is not None:
            if char == quote:
                if quote in {"'", '"'} and position + 1 < len(sql):
                    if sql[position + 1] == quote:
                        position += 2
                        continue
                quote = None
            position += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            position += 1
            continue
        if char == "[":
            bracketed = True
            position += 1
            continue
        if sql[
            position : position + keyword_len
        ].lower() == keyword_lower and _sql_keyword_boundary(
            sql, position, keyword_len
        ):
            return position
        position += 1
    return -1


def _sql_keyword_at(sql: str, keyword: str, position: int) -> bool:
    keyword_len = len(keyword)
    if position > len(sql) - keyword_len:
        return False
    return sql[
        position : position + keyword_len
    ].lower() == keyword.lower() and _sql_keyword_boundary(sql, position, keyword_len)


def _sql_keyword_boundary(sql: str, position: int, length: int) -> bool:
    before = sql[position - 1] if position > 0 else ""
    after_index = position + length
    after = sql[after_index] if after_index < len(sql) else ""
    return not _sql_identifier_char(before) and not _sql_identifier_char(after)


def _sql_identifier_char(char: str) -> bool:
    return bool(char) and (char.isalnum() or char == "_")


def _read_sql_identifier(sql: str, position: int) -> tuple[str | None, int]:
    if position >= len(sql):
        return None, position
    char = sql[position]
    if char in {'"', "'", "`"}:
        return _read_quoted_sql_identifier(sql, position, char, char)
    if char == "[":
        return _read_quoted_sql_identifier(sql, position, "[", "]")
    end = position
    while end < len(sql) and not sql[end].isspace() and sql[end] not in ",()":
        end += 1
    if end == position:
        return None, position
    return sql[position:end], end


def _read_quoted_sql_identifier(
    sql: str,
    position: int,
    opening: str,
    closing: str,
) -> tuple[str | None, int]:
    cursor = position + 1
    parts: list[str] = []
    while cursor < len(sql):
        char = sql[cursor]
        if char == closing:
            if closing in {"'", '"'} and cursor + 1 < len(sql):
                if sql[cursor + 1] == closing:
                    parts.append(closing)
                    cursor += 2
                    continue
            return "".join(parts), cursor + 1
        parts.append(char)
        cursor += 1
    return None, position


def _read_parenthesized_sql(sql: str, position: int) -> tuple[str | None, int]:
    depth = 0
    cursor = position
    quote: str | None = None
    bracketed = False
    while cursor < len(sql):
        char = sql[cursor]
        if bracketed:
            if char == "]":
                bracketed = False
            cursor += 1
            continue
        if quote is not None:
            if char == quote:
                if quote in {"'", '"'} and cursor + 1 < len(sql):
                    if sql[cursor + 1] == quote:
                        cursor += 2
                        continue
                quote = None
            cursor += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            cursor += 1
            continue
        if char == "[":
            bracketed = True
            cursor += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return sql[position + 1 : cursor], cursor + 1
        cursor += 1
    return None, position


def _normalize_sqlite_type(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return "str"
    if any(token in text for token in ("DECIMAL", "NUMERIC", "NUMBER")):
        return "decimal"
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


def _sqlite_numeric_precision_scale(value: Any) -> tuple[int | None, int | None]:
    text = str(value or "").strip().upper()
    if not any(token in text for token in ("DECIMAL", "NUMERIC", "NUMBER")):
        return None, None
    start = _find_sql_char(text, "(", 0)
    if start < 0:
        return None, None
    expression, _end = _read_parenthesized_sql(text, start)
    if expression is None:
        return None, None
    parts = [part.strip() for part in expression.split(",")]
    if len(parts) != 2:
        return None, None
    precision = _optional_int(parts[0])
    scale = _optional_int(parts[1])
    if precision is None or scale is None:
        return None, None
    return precision, scale


def _require_migration_symbol(symbol: str) -> Any:
    return import_native_extension(
        context="migration reflection",
        required_symbols=(symbol,),
    )
