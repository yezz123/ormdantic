"""Migration history table storage and execution helpers."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from ormdantic import __version__
from ormdantic._migrations.models import (
    MIGRATION_ARTIFACT_VERSION,
    MIGRATION_STATUS_APPLIED,
    MIGRATION_STATUS_FAILED,
    MigrationHistoryEntry,
    MigrationOperation,
    optional_int,
    optional_str,
)
from ormdantic._migrations.sql import (
    db_truthy,
    dialect_name,
    query_rows,
    query_scalar,
    quote_ident,
    sql_literal,
)

UTC = timezone.utc
MIGRATION_TABLE = "ormdantic_migrations"
MIGRATION_LOCK_NAME = "ormdantic:migration:lock"


def migration_table_column_defs(dialect: str) -> list[tuple[str, str, str | None]]:
    dialect = dialect_name(dialect)
    text_type = "TEXT"
    short_text_type = text_type
    revision_type = f"{text_type} PRIMARY KEY"
    integer_type = "INTEGER"
    metadata_type = text_type
    if dialect in {"mysql", "mariadb"}:
        short_text_type = "VARCHAR(255)"
        revision_type = "VARCHAR(255) PRIMARY KEY"
        metadata_type = "TEXT"
    elif dialect == "mssql":
        text_type = "NVARCHAR(2048)"
        short_text_type = "NVARCHAR(255)"
        revision_type = "NVARCHAR(255) PRIMARY KEY"
        integer_type = "BIGINT"
        metadata_type = "NVARCHAR(MAX)"
    elif dialect == "oracle":
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
        ("status", short_text_type, sql_literal(MIGRATION_STATUS_APPLIED)),
        ("dirty", integer_type, "0"),
        ("artifact_version", integer_type, None),
        ("ormdantic_version", short_text_type, None),
        ("metadata", metadata_type, None),
    ]


def ensure_migration_history_table(connection: Any, dialect: str) -> None:
    table = quote_ident(dialect, MIGRATION_TABLE)
    columns = migration_table_column_defs(dialect)
    create_columns = ", ".join(
        f"{quote_ident(dialect, name)} {column_type}"
        + (f" DEFAULT {default}" if default is not None else "")
        for name, column_type, default in columns
    )
    existed = migration_history_table_exists(connection, dialect)
    if not existed:
        try:
            connection.execute(f"CREATE TABLE {table} ({create_columns})", [])
        except Exception as exc:
            if not is_duplicate_table_error(exc):
                raise
            existed = True
    if existed:
        for name, column_type, default in columns[1:]:
            if migration_history_column_exists(connection, dialect, name):
                continue
            statement = add_migration_history_column_sql(
                dialect,
                table,
                name,
                column_type,
                default,
            )
            try:
                connection.execute(statement, [])
            except Exception as exc:
                if not is_duplicate_column_error(exc):
                    raise
    connection.execute(
        f"UPDATE {table} SET {quote_ident(dialect, 'status')} = "
        f"{sql_literal(MIGRATION_STATUS_APPLIED)} WHERE {quote_ident(dialect, 'status')} IS NULL",
        [],
    )
    connection.execute(
        f"UPDATE {table} SET {quote_ident(dialect, 'dirty')} = 0 "
        f"WHERE {quote_ident(dialect, 'dirty')} IS NULL",
        [],
    )
    connection.execute(
        f"UPDATE {table} SET {quote_ident(dialect, 'artifact_version')} = "
        f"{MIGRATION_ARTIFACT_VERSION} WHERE {quote_ident(dialect, 'artifact_version')} IS NULL",
        [],
    )
    commit_migration_history_if_needed(connection, dialect)


def migration_history_table_exists(connection: Any, dialect: str) -> bool:
    dialect = dialect_name(dialect)
    table_name = sql_literal(MIGRATION_TABLE)
    if dialect == "sqlite":
        sql = (
            "SELECT COUNT(*) FROM sqlite_master "
            f"WHERE type = 'table' AND name = {table_name}"
        )
    elif dialect == "postgresql":
        sql = (
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = current_schema() AND table_name = {table_name}"
        )
    elif dialect in {"mysql", "mariadb"}:
        sql = (
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = DATABASE() AND table_name = {table_name}"
        )
    elif dialect == "mssql":
        sql = f"SELECT COUNT(*) FROM sys.tables WHERE name = {table_name}"
    elif dialect == "oracle":
        sql = f"SELECT COUNT(*) FROM user_tables WHERE table_name = {table_name}"
    else:
        return False
    value = query_scalar(connection, sql)
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return db_truthy(value)


def add_migration_history_column_sql(
    dialect: str,
    table: str,
    name: str,
    column_type: str,
    default: str | None,
) -> str:
    column_def = f"{quote_ident(dialect, name)} {column_type}"
    if default is not None:
        column_def += f" DEFAULT {default}"
    dialect = dialect_name(dialect)
    if dialect == "mssql":
        return f"ALTER TABLE {table} ADD {column_def}"
    if dialect == "oracle":
        return f"ALTER TABLE {table} ADD ({column_def})"
    return f"ALTER TABLE {table} ADD COLUMN {column_def}"


def migration_history_column_exists(
    connection: Any,
    dialect: str,
    column: str,
) -> bool:
    dialect = dialect_name(dialect)
    table_name = sql_literal(MIGRATION_TABLE)
    column_name = sql_literal(column)
    if dialect == "sqlite":
        sql = (
            "SELECT COUNT(*) FROM "
            f"pragma_table_info({table_name}) WHERE name = {column_name}"
        )
    elif dialect == "postgresql":
        sql = (
            "SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema = current_schema() AND table_name = {table_name} "
            f"AND column_name = {column_name}"
        )
    elif dialect in {"mysql", "mariadb"}:
        sql = (
            "SELECT COUNT(*) FROM information_schema.columns "
            f"WHERE table_schema = DATABASE() AND table_name = {table_name} "
            f"AND column_name = {column_name}"
        )
    elif dialect == "mssql":
        sql = (
            "SELECT COUNT(*) FROM sys.columns "
            f"WHERE object_id = OBJECT_ID(N{table_name}) AND name = {column_name}"
        )
    elif dialect == "oracle":
        sql = (
            "SELECT COUNT(*) FROM user_tab_columns "
            f"WHERE table_name = {table_name} AND column_name = {column_name}"
        )
    else:
        return False
    value = query_scalar(connection, sql)
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return db_truthy(value)


def is_duplicate_table_error(error: Exception) -> bool:
    message = str(error).lower()
    return (
        ("already exists" in message and ("table" in message or "object" in message))
        or "already an object named" in message
        or "ora-00955" in message
        or "name is already used by an existing object" in message
    )


def is_duplicate_column_error(error: Exception) -> bool:
    message = str(error).lower()
    return (
        ("duplicate" in message and "column" in message)
        or ("already exists" in message and "column" in message)
        or "ora-01430" in message
        or ("column" in message and "must be unique" in message)
        or ("column" in message and "specified more than once" in message)
    )


def history_entries(connection: Any, dialect: str) -> list[MigrationHistoryEntry]:
    table = quote_ident(dialect, MIGRATION_TABLE)
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
    selected = ", ".join(quote_ident(dialect, name) for name in columns)
    rows = query_rows(
        connection,
        f"SELECT {selected} FROM {table} ORDER BY {quote_ident(dialect, 'applied_at')}, "
        f"{quote_ident(dialect, 'revision')}",
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
                description=optional_str(row[1] if len(row) > 1 else None),
                checksum=optional_str(row[2] if len(row) > 2 else None),
                applied_at=optional_str(row[3] if len(row) > 3 else None),
                execution_time_ms=optional_int(row[4] if len(row) > 4 else None),
                status=str(
                    row[5] if len(row) > 5 and row[5] else MIGRATION_STATUS_APPLIED
                ),
                dirty=db_truthy(row[6] if len(row) > 6 else None),
                artifact_version=optional_int(row[7] if len(row) > 7 else None),
                ormdantic_version=optional_str(row[8] if len(row) > 8 else None),
                metadata=metadata,
            )
        )
    return history


def history_entry(
    connection: Any, dialect: str, revision: str
) -> MigrationHistoryEntry | None:
    for entry in history_entries(connection, dialect):
        if entry.revision == revision:
            return entry
    return None


def current_entry(connection: Any, dialect: str) -> MigrationHistoryEntry | None:
    for entry in reversed(history_entries(connection, dialect)):
        if entry.status == MIGRATION_STATUS_APPLIED and not entry.dirty:
            return entry
    return None


def is_dirty(connection: Any, dialect: str) -> bool:
    return any(entry.dirty for entry in history_entries(connection, dialect))


def repair_history(
    connection: Any,
    dialect: str,
    *,
    revision: str | None,
    status: str | None,
    clear_dirty: bool,
    checksum: str | None,
) -> int:
    entries = history_entries(connection, dialect)
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
        write_history_entry(connection, dialect, payload)
        updated += 1
    return updated


def write_history_entry(
    connection: Any, dialect: str, entry: MigrationHistoryEntry
) -> None:
    table = quote_ident(dialect, MIGRATION_TABLE)
    revision_column = quote_ident(dialect, "revision")
    connection.execute(
        f"DELETE FROM {table} WHERE {revision_column} = {sql_literal(entry.revision)}",
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
    rendered_values = ", ".join(sql_literal(value) for value in values)
    rendered_columns = ", ".join(quote_ident(dialect, column) for column in columns)
    connection.execute(
        f"INSERT INTO {table} ({rendered_columns}) VALUES ({rendered_values})",
        [],
    )


def dialect_supports_transactional_ddl(dialect: str) -> bool:
    return dialect_name(dialect) in {"sqlite", "postgresql"}


def acquire_migration_lock(connection: Any, dialect: str) -> str | None:
    dialect = dialect_name(dialect)
    if dialect == "postgresql":
        acquired = query_scalar(
            connection,
            "SELECT pg_try_advisory_lock(hashtext('ormdantic_migration_lock'))",
        )
        if not db_truthy(acquired):
            raise ValueError(
                "failed to acquire postgres advisory migration lock; another migration may be running"
            )
        return "SELECT pg_advisory_unlock(hashtext('ormdantic_migration_lock'))"
    if dialect in {"mysql", "mariadb"}:
        acquired = query_scalar(
            connection,
            f"SELECT GET_LOCK({sql_literal(MIGRATION_LOCK_NAME)}, 30)",
        )
        if not db_truthy(acquired):
            raise ValueError(
                "failed to acquire mysql migration lock; another migration may be running"
            )
        return f"SELECT RELEASE_LOCK({sql_literal(MIGRATION_LOCK_NAME)})"
    if dialect == "mssql":
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


def run_migration_operations(
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
    ensure_migration_history_table(connection, dialect)
    transaction_open = False
    release_lock_sql = acquire_migration_lock(connection, dialect)
    start = time.perf_counter()
    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    try:
        if dialect_supports_transactional_ddl(dialect):
            if dialect_name(dialect) == "sqlite":
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
        write_history_entry(connection, dialect, pending)
        for operation in operations:
            connection.execute(operation.sql, list(operation.values))
        if transaction_open:
            connection.commit()
            transaction_open = False
        elapsed = int((time.perf_counter() - start) * 1000)
        write_history_entry(
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
        commit_migration_history_if_needed(connection, dialect)
    except Exception:
        if transaction_open:
            try:
                connection.rollback()
            except Exception:
                pass
        elapsed = int((time.perf_counter() - start) * 1000)
        write_history_entry(
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
            commit_migration_history_if_needed(connection, dialect)
        except Exception:
            pass
        raise
    finally:
        if release_lock_sql:
            try:
                connection.execute(release_lock_sql, [])
            except Exception:
                pass


def commit_migration_history_if_needed(connection: Any, dialect: str) -> None:
    if dialect_name(dialect) == "oracle" and hasattr(connection, "commit"):
        connection.commit()


_acquire_migration_lock = acquire_migration_lock
_add_migration_history_column_sql = add_migration_history_column_sql
_commit_migration_history_if_needed = commit_migration_history_if_needed
_current_entry = current_entry
_dialect_supports_transactional_ddl = dialect_supports_transactional_ddl
_ensure_migration_history_table = ensure_migration_history_table
_history_entries = history_entries
_history_entry = history_entry
_is_dirty = is_dirty
_is_duplicate_column_error = is_duplicate_column_error
_is_duplicate_table_error = is_duplicate_table_error
_migration_history_column_exists = migration_history_column_exists
_migration_history_table_exists = migration_history_table_exists
_migration_table_column_defs = migration_table_column_defs
_repair_history = repair_history
_run_migration_operations = run_migration_operations
_write_history_entry = write_history_entry
