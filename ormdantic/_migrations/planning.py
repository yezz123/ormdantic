"""Schema diffing and migration plan generation helpers."""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
from os import PathLike
from typing import Any

from ormdantic._migrations.artifacts import (
    MigrationArtifact,
    _coerce_artifact,
    _validate_contiguous_artifacts,
)
from ormdantic._migrations.models import (
    MIGRATION_ARTIFACT_VERSION,
    ColumnSnapshot,
    IndexSnapshot,
    MigrationChange,
    MigrationOperation,
    MigrationPlan,
    MigrationWarning,
    RuntimeCheck,
    SchemaDiff,
    SchemaSnapshot,
    TableSnapshot,
    optional_str,
)
from ormdantic._migrations.sql import dialect_name, quote_ident


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
    normalized_dialect = dialect_name(dialect)
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
        _operation_from_compiled(item, normalized_dialect) for item in compiled
    ]
    rollback_operations = [
        _operation_from_compiled(item, normalized_dialect, generated_rollback=True)
        for item in rollback_compiled
    ]
    requires_rebuild = any(op.requires_rebuild for op in operations)
    rollback_requires_rebuild = any(op.requires_rebuild for op in rollback_operations)
    if normalized_dialect == "sqlite":
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
    _raise_if_unsupported_sqlite_plan(normalized_dialect, operations)
    safety = {
        "dialect": normalized_dialect,
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
        table=optional_str(metadata.get("table")),
        object_name=optional_str(metadata.get("object_name")),
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
            f"DROP TABLE IF EXISTS {quote_ident('sqlite', temp_name)}",
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
        selected = ", ".join(quote_ident("sqlite", column) for column in common_columns)
        operations.append(
            MigrationOperation(
                f"INSERT INTO {quote_ident('sqlite', temp_name)} ({selected}) "
                f"SELECT {selected} FROM {quote_ident('sqlite', table_name)}",
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
                f"DROP TABLE {quote_ident('sqlite', table_name)}",
                description=f"drop old table for sqlite rebuild of {table_name}",
                unsafe=True,
                destructive=destructive,
                kind="sqlite_rebuild_table",
                table=table_name,
                object_name=table_name,
                metadata={"sqlite_rebuild": True, "phase": "drop_old"},
            ),
            MigrationOperation(
                f"ALTER TABLE {quote_ident('sqlite', temp_name)} "
                f"RENAME TO {quote_ident('sqlite', table_name)}",
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
    if dialect_name(dialect) != "sqlite":
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
    try:
        rust = importlib.import_module("ormdantic._ormdantic")
    except ImportError as exc:  # pragma: no cover - exercised when extension is absent
        raise RuntimeError(
            "Ormdantic requires the Rust extension for migration planning. "
            "Install the package with maturin or reinstall the wheel."
        ) from exc
    if not hasattr(rust, symbol):
        raise RuntimeError(
            "Ormdantic requires the Rust extension for migration planning. "
            "Install the package with maturin or reinstall the wheel."
        )
    return rust
