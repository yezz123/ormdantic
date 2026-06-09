"""Live schema reflection helpers for migration autogeneration."""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from typing import Any

from ormdantic._migrations.history import MIGRATION_TABLE
from ormdantic._migrations.models import (
    MIGRATION_ARTIFACT_VERSION,
    ColumnSnapshot,
    IndexSnapshot,
    SchemaSnapshot,
    TableSnapshot,
)
from ormdantic._migrations.models import (
    optional_int as _optional_int,
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


def _require_migration_symbol(symbol: str) -> Any:
    try:
        rust = importlib.import_module("ormdantic._ormdantic")
    except ImportError as exc:  # pragma: no cover - exercised when extension is absent
        raise RuntimeError(
            "Ormdantic requires the Rust extension for migration reflection. "
            "Install the package with maturin or reinstall the wheel."
        ) from exc
    if not hasattr(rust, symbol):
        raise RuntimeError(
            "Ormdantic requires the Rust extension for migration reflection. "
            "Install the package with maturin or reinstall the wheel."
        )
    return rust
