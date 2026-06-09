from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from fnmatch import fnmatch
from typing import Any


def dialect_name(value: str) -> str:
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


def quote_ident(dialect: str, identifier: str) -> str:
    name = identifier.replace("\x00", "")
    dialect = dialect_name(dialect)
    if dialect in {"mysql", "mariadb"}:
        return f"`{name.replace('`', '``')}`"
    if dialect == "mssql":
        return f"[{name.replace(']', ']]')}]"
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def sql_literal(value: Any) -> str:
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


def query_rows(
    connection: Any, sql: str, params: Sequence[Any] | None = None
) -> list[list[Any]]:
    result = connection.execute(sql, list(params or ()))
    if not isinstance(result, Mapping):
        return []
    rows = result.get("rows", [])
    if not isinstance(rows, Sequence):
        return []
    return [list(row) for row in rows if isinstance(row, Sequence)]


def query_rows_url(rust_module: Any, url: str, sql: str) -> list[list[Any]]:
    result = rust_module.execute_native(url, sql, [])
    if not isinstance(result, Mapping):
        return []
    rows = result.get("rows", [])
    if not isinstance(rows, Sequence):
        return []
    return [list(row) for row in rows if isinstance(row, Sequence)]


def query_scalar(connection: Any, sql: str, params: Sequence[Any] | None = None) -> Any:
    rows = query_rows(connection, sql, params)
    if not rows:
        return None
    if not rows[0]:
        return None
    return rows[0][0]


def db_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "t", "true", "yes", "y"}


def table_matches_filters(
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


def operation_looks_destructive(sql: str) -> bool:
    normalized = " ".join(sql.strip().upper().split())
    if normalized.startswith(("DROP TABLE ", "TRUNCATE TABLE ", "DELETE FROM ")):
        return True
    return normalized.startswith("ALTER TABLE ") and " DROP " in normalized


def document_format(path: str, format: str | None = None) -> str:
    from pathlib import Path

    if format is not None:
        normalized = format.lower().lstrip(".")
    else:
        normalized = Path(path).suffix.lower().lstrip(".") or "json"
    if normalized not in {"json", "toml"}:
        raise ValueError(f"unsupported migration document format '{normalized}'")
    return normalized
