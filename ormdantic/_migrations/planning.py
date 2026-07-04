"""Schema diffing and migration plan generation helpers."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import replace
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
    EnumTypeSnapshot,
    ExclusionConstraintSnapshot,
    ForeignKeyConstraintSnapshot,
    IndexSnapshot,
    MigrationChange,
    MigrationOperation,
    MigrationPlan,
    MigrationWarning,
    NamespaceSnapshot,
    RuntimeCheck,
    SchemaDiff,
    SchemaSnapshot,
    SequenceSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
    ViewSnapshot,
    optional_str,
)
from ormdantic._migrations.sql import dialect_name, quote_ident, sql_literal
from ormdantic._native import import_native_extension


def diff_snapshots(
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
    *,
    dialect: str | None = None,
) -> SchemaDiff:
    """Return structured changes between schema snapshots."""
    normalized_dialect = dialect_name(dialect) if dialect is not None else None
    changes: list[MigrationChange] = []
    from_tables = {_table_key(table): table for table in from_snapshot.tables}
    to_tables = {_table_key(table): table for table in to_snapshot.tables}

    _diff_namespaces(changes, from_snapshot, to_snapshot)
    _diff_enum_types(changes, from_snapshot, to_snapshot)
    _diff_sequences(changes, from_snapshot, to_snapshot, dialect=normalized_dialect)
    _diff_views(changes, from_snapshot, to_snapshot, dialect=normalized_dialect)

    for table in to_snapshot.tables:
        table_label = _table_label(table)
        if _table_key(table) not in from_tables:
            changes.append(
                MigrationChange(
                    "add",
                    "table",
                    table_label,
                    table_label,
                    f"Added table {table_label}",
                )
            )

    for table in from_snapshot.tables:
        table_label = _table_label(table)
        if _table_key(table) not in to_tables:
            changes.append(
                MigrationChange(
                    "remove",
                    "table",
                    table_label,
                    table_label,
                    f"Removed table {table_label}",
                    unsafe=True,
                    destructive=True,
                )
            )

    for table_key, before in from_tables.items():
        after = to_tables.get(table_key)
        if after is None:
            continue
        _diff_columns(changes, before, after, dialect=normalized_dialect)
        _diff_indexes(changes, before, after)
        _diff_constraints(changes, before, after)
        _diff_table_metadata(changes, before, after, dialect=normalized_dialect)

    warnings = [_warning_for_change(change) for change in changes if change.unsafe]
    return SchemaDiff(changes, warnings)


def _table_key(table: TableSnapshot) -> tuple[str | None, str]:
    return (table.schema, table.name)


def _table_label(table: TableSnapshot) -> str:
    if table.schema is None:
        return table.name
    return f"{table.schema}.{table.name}"


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
    normalized_dialect = dialect_name(dialect)
    schema_diff = diff_snapshots(
        from_snapshot,
        to_snapshot,
        dialect=normalized_dialect,
    )
    if schema_diff.changes:
        compiled = _compile_schema_diff(
            dialect,
            from_snapshot,
            to_snapshot,
        )
        rollback_to_snapshot = from_snapshot
        if normalized_dialect in {"mysql", "mariadb"}:
            rollback_to_snapshot = _snapshot_without_unmanaged_mysql_auto_increment(
                from_snapshot,
                to_snapshot,
            )
        rollback_compiled = _compile_schema_diff(
            dialect,
            to_snapshot,
            rollback_to_snapshot,
        )
    else:
        compiled = []
        rollback_compiled = []
    namespace_before, namespace_after = _compile_namespace_diff(
        normalized_dialect,
        from_snapshot,
        to_snapshot,
    )
    enum_before, enum_after = _compile_enum_type_diff(
        normalized_dialect,
        from_snapshot,
        to_snapshot,
    )
    sequence_before, sequence_after = _compile_sequence_diff(
        normalized_dialect,
        from_snapshot,
        to_snapshot,
    )
    constraint_comment_after = _compile_constraint_comment_diff(
        normalized_dialect,
        from_snapshot,
        to_snapshot,
    )
    unique_metadata_after = _compile_unique_constraint_metadata_diff(
        normalized_dialect,
        from_snapshot,
        to_snapshot,
    )
    index_metadata_after = _compile_index_metadata_diff(
        normalized_dialect,
        from_snapshot,
        to_snapshot,
    )
    view_before, view_after = _compile_view_diff(
        normalized_dialect,
        from_snapshot,
        to_snapshot,
    )
    rollback_enum_before, rollback_enum_after = _compile_enum_type_diff(
        normalized_dialect,
        to_snapshot,
        from_snapshot,
    )
    rollback_sequence_before, rollback_sequence_after = _compile_sequence_diff(
        normalized_dialect,
        to_snapshot,
        from_snapshot,
    )
    rollback_constraint_comment_after = _compile_constraint_comment_diff(
        normalized_dialect,
        to_snapshot,
        from_snapshot,
    )
    rollback_unique_metadata_after = _compile_unique_constraint_metadata_diff(
        normalized_dialect,
        to_snapshot,
        from_snapshot,
    )
    rollback_index_metadata_after = _compile_index_metadata_diff(
        normalized_dialect,
        to_snapshot,
        from_snapshot,
    )
    rollback_view_before, rollback_view_after = _compile_view_diff(
        normalized_dialect,
        to_snapshot,
        from_snapshot,
    )
    rollback_namespace_before, rollback_namespace_after = _compile_namespace_diff(
        normalized_dialect,
        to_snapshot,
        from_snapshot,
    )
    warnings = list(schema_diff.warnings)
    operations = [
        _operation_from_compiled(item, normalized_dialect)
        for item in [
            *namespace_before,
            *sequence_before,
            *enum_before,
            *view_before,
            *compiled,
            *unique_metadata_after,
            *constraint_comment_after,
            *index_metadata_after,
            *enum_after,
            *sequence_after,
            *view_after,
            *namespace_after,
        ]
    ]
    rollback_operations = [
        _operation_from_compiled(item, normalized_dialect, generated_rollback=True)
        for item in [
            *rollback_namespace_before,
            *rollback_sequence_before,
            *rollback_enum_before,
            *rollback_view_before,
            *rollback_compiled,
            *rollback_unique_metadata_after,
            *rollback_constraint_comment_after,
            *rollback_index_metadata_after,
            *rollback_enum_after,
            *rollback_sequence_after,
            *rollback_view_after,
            *rollback_namespace_after,
        ]
    ]
    if normalized_dialect == "sqlite":
        _mark_sqlite_table_option_rebuilds(operations, schema_diff)
        _mark_sqlite_table_option_rebuilds(rollback_operations, schema_diff)
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
    unsafe = schema_diff.has_unsafe_operations or any(
        operation.unsafe for operation in operations
    )
    destructive = schema_diff.has_destructive_operations or any(
        operation.destructive for operation in operations
    )
    safety = {
        "dialect": normalized_dialect,
        "unsafe": unsafe,
        "destructive": destructive,
        "requires_rebuild": requires_rebuild,
        "rollback_requires_rebuild": rollback_requires_rebuild,
        "rollback_available": bool(rollback_operations),
    }
    return MigrationPlan(operations, rollback_operations, schema_diff, warnings, safety)


def _diff_namespaces(
    changes: list[MigrationChange],
    before: SchemaSnapshot,
    after: SchemaSnapshot,
) -> None:
    before_namespaces = {
        _namespace_key(namespace): namespace for namespace in before.namespaces
    }
    after_namespaces = {
        _namespace_key(namespace): namespace for namespace in after.namespaces
    }

    for key, namespace in after_namespaces.items():
        if key not in before_namespaces:
            changes.append(
                MigrationChange(
                    "add",
                    "namespace",
                    "",
                    namespace.name,
                    f"Added namespace {namespace.name}",
                    details=namespace.to_dict(),
                )
            )

    for key, namespace in before_namespaces.items():
        if key not in after_namespaces:
            changes.append(
                MigrationChange(
                    "remove",
                    "namespace",
                    "",
                    namespace.name,
                    f"Removed namespace {namespace.name}",
                    unsafe=True,
                    destructive=True,
                    details=namespace.to_dict(),
                )
            )

    for key, before_namespace in before_namespaces.items():
        after_namespace = after_namespaces.get(key)
        if (
            after_namespace is None
            or before_namespace.comment == after_namespace.comment
        ):
            continue
        changes.append(
            MigrationChange(
                "change",
                "namespace",
                "",
                after_namespace.name,
                f"Changed namespace {after_namespace.name}: comment",
                details={
                    "fields": ["comment"],
                    "from": {"comment": before_namespace.comment},
                    "to": {"comment": after_namespace.comment},
                },
            )
        )


def _diff_enum_types(
    changes: list[MigrationChange],
    before: SchemaSnapshot,
    after: SchemaSnapshot,
) -> None:
    before_enum_types = {
        _enum_type_key(enum_type): enum_type for enum_type in before.enum_types
    }
    after_enum_types = {
        _enum_type_key(enum_type): enum_type for enum_type in after.enum_types
    }

    for key, enum_type in after_enum_types.items():
        if key not in before_enum_types:
            changes.append(
                MigrationChange(
                    "add",
                    "enum_type",
                    "",
                    _enum_type_qualified_name(enum_type),
                    f"Added enum type {_enum_type_qualified_name(enum_type)}",
                    details=enum_type.to_dict(),
                )
            )

    for key, enum_type in before_enum_types.items():
        if key not in after_enum_types:
            changes.append(
                MigrationChange(
                    "remove",
                    "enum_type",
                    "",
                    _enum_type_qualified_name(enum_type),
                    f"Removed enum type {_enum_type_qualified_name(enum_type)}",
                    unsafe=True,
                    destructive=True,
                    details=enum_type.to_dict(),
                )
            )

    for key, old_enum_type in before_enum_types.items():
        new_enum_type = after_enum_types.get(key)
        if new_enum_type is None or old_enum_type == new_enum_type:
            continue
        if not _enum_type_requires_recreate(old_enum_type, new_enum_type):
            changes.append(
                MigrationChange(
                    "change",
                    "enum_type",
                    "",
                    _enum_type_qualified_name(new_enum_type),
                    f"Changed enum type {_enum_type_qualified_name(new_enum_type)}: comment",
                    details={
                        "fields": ["comment"],
                        "from": {"comment": old_enum_type.comment},
                        "to": {"comment": new_enum_type.comment},
                    },
                )
            )
            continue
        changes.append(
            MigrationChange(
                "change",
                "enum_type",
                "",
                _enum_type_qualified_name(new_enum_type),
                f"Changed enum type {_enum_type_qualified_name(new_enum_type)}",
                unsafe=True,
                destructive=True,
                details={
                    "from": old_enum_type.to_dict(),
                    "to": new_enum_type.to_dict(),
                },
            )
        )


def _diff_sequences(
    changes: list[MigrationChange],
    before: SchemaSnapshot,
    after: SchemaSnapshot,
    *,
    dialect: str | None = None,
) -> None:
    before_sequences = {
        _sequence_key(sequence): sequence for sequence in before.sequences
    }
    after_sequences = {
        _sequence_key(sequence): sequence for sequence in after.sequences
    }

    for key, sequence in after_sequences.items():
        if key not in before_sequences:
            changes.append(
                MigrationChange(
                    "add",
                    "sequence",
                    "",
                    _sequence_qualified_name(sequence),
                    f"Added sequence {_sequence_qualified_name(sequence)}",
                    details=sequence.to_dict(),
                )
            )

    for key, sequence in before_sequences.items():
        if key not in after_sequences:
            changes.append(
                MigrationChange(
                    "remove",
                    "sequence",
                    "",
                    _sequence_qualified_name(sequence),
                    f"Removed sequence {_sequence_qualified_name(sequence)}",
                    unsafe=True,
                    destructive=True,
                    details=sequence.to_dict(),
                )
            )

    for key, old_sequence in before_sequences.items():
        new_sequence = after_sequences.get(key)
        if new_sequence is None or old_sequence == new_sequence:
            continue
        if not _sequence_requires_recreate(
            old_sequence,
            new_sequence,
            dialect=dialect,
        ):
            if old_sequence.comment == new_sequence.comment:
                continue
            changes.append(
                MigrationChange(
                    "change",
                    "sequence",
                    "",
                    _sequence_qualified_name(new_sequence),
                    f"Changed sequence {_sequence_qualified_name(new_sequence)}: comment",
                    details={
                        "fields": ["comment"],
                        "from": {"comment": old_sequence.comment},
                        "to": {"comment": new_sequence.comment},
                    },
                )
            )
            continue
        changes.append(
            MigrationChange(
                "change",
                "sequence",
                "",
                _sequence_qualified_name(new_sequence),
                f"Changed sequence {_sequence_qualified_name(new_sequence)}",
                unsafe=True,
                details={
                    "from": old_sequence.to_dict(),
                    "to": new_sequence.to_dict(),
                },
            )
        )


def _diff_views(
    changes: list[MigrationChange],
    before: SchemaSnapshot,
    after: SchemaSnapshot,
    *,
    dialect: str | None = None,
) -> None:
    before_views = {_view_key(view): view for view in before.views}
    after_views = {_view_key(view): view for view in after.views}

    for key, view in after_views.items():
        if key not in before_views:
            changes.append(
                MigrationChange(
                    "add",
                    "view",
                    "",
                    _view_qualified_name(view),
                    f"Added view {_view_qualified_name(view)}",
                    details=view.to_dict(),
                )
            )

    for key, view in before_views.items():
        if key not in after_views:
            changes.append(
                MigrationChange(
                    "remove",
                    "view",
                    "",
                    _view_qualified_name(view),
                    f"Removed view {_view_qualified_name(view)}",
                    unsafe=True,
                    destructive=True,
                    details=view.to_dict(),
                )
            )

    for key, old_view in before_views.items():
        new_view = after_views.get(key)
        if new_view is None or old_view == new_view:
            continue
        if _view_requires_recreate(old_view, new_view, dialect=dialect):
            changes.append(
                MigrationChange(
                    "change",
                    "view",
                    "",
                    _view_qualified_name(new_view),
                    f"Changed view {_view_qualified_name(new_view)}",
                    unsafe=True,
                    destructive=True,
                    details={
                        "from": old_view.to_dict(),
                        "to": new_view.to_dict(),
                    },
                )
            )
            continue
        if old_view.comment != new_view.comment:
            changes.append(
                MigrationChange(
                    "change",
                    "view",
                    "",
                    _view_qualified_name(new_view),
                    f"Changed view {_view_qualified_name(new_view)}: comment",
                    details={
                        "fields": ["comment"],
                        "from": {"comment": old_view.comment},
                        "to": {"comment": new_view.comment},
                    },
                )
            )


def _diff_columns(
    changes: list[MigrationChange],
    before: TableSnapshot,
    after: TableSnapshot,
    *,
    dialect: str | None = None,
) -> None:
    before_columns = {column.name: column for column in before.columns}
    after_columns = {column.name: column for column in after.columns}
    before_label = _table_label(before)
    after_label = _table_label(after)

    for column in after.columns:
        if column.name not in before_columns:
            unsafe = not column.nullable and not column.primary_key
            changes.append(
                MigrationChange(
                    "add",
                    "column",
                    after_label,
                    column.name,
                    f"Added column {after_label}.{column.name}",
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
                    before_label,
                    column.name,
                    f"Removed column {before_label}.{column.name}",
                    unsafe=True,
                    destructive=True,
                    details=column.to_dict(),
                )
            )

    for column_name, old_column in before_columns.items():
        new_column = after_columns.get(column_name)
        if new_column is None or old_column == new_column:
            continue
        changed = _changed_column_fields(
            old_column,
            new_column,
            dialect=dialect,
        )
        if not changed:
            continue
        unsafe = any(field != "comment" for field in changed)
        destructive = _is_destructive_column_change(changed)
        changes.append(
            MigrationChange(
                "change",
                "column",
                after_label,
                column_name,
                f"Changed column {after_label}.{column_name}: {', '.join(changed)}",
                unsafe=unsafe,
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
    before_label = _table_label(before)
    after_label = _table_label(after)

    for index in after.indexes:
        if index.name not in before_indexes:
            changes.append(
                MigrationChange(
                    "add",
                    "index",
                    after_label,
                    index.name,
                    f"Added index {index.name} on {after_label}",
                    details=index.to_dict(),
                )
            )

    for index in before.indexes:
        if index.name not in after_indexes:
            changes.append(
                MigrationChange(
                    "remove",
                    "index",
                    before_label,
                    index.name,
                    f"Removed index {index.name} from {before_label}",
                    unsafe=index.unique,
                    details=index.to_dict(),
                )
            )

    for index_name, old_index in before_indexes.items():
        new_index = after_indexes.get(index_name)
        if new_index is None or old_index == new_index:
            continue
        if _indexes_equivalent(old_index, new_index):
            fields: list[str] = []
            from_details: dict[str, Any] = {}
            to_details: dict[str, Any] = {}
            if old_index.comment != new_index.comment:
                fields.append("comment")
                from_details["comment"] = old_index.comment
                to_details["comment"] = new_index.comment
            if old_index.postgres_tablespace != new_index.postgres_tablespace:
                fields.append("postgres_tablespace")
                from_details["postgres_tablespace"] = old_index.postgres_tablespace
                to_details["postgres_tablespace"] = new_index.postgres_tablespace
            if old_index.mssql_filegroup != new_index.mssql_filegroup:
                fields.append("mssql_filegroup")
                from_details["mssql_filegroup"] = old_index.mssql_filegroup
                to_details["mssql_filegroup"] = new_index.mssql_filegroup
            if old_index.mssql_clustered != new_index.mssql_clustered:
                fields.append("mssql_clustered")
                from_details["mssql_clustered"] = old_index.mssql_clustered
                to_details["mssql_clustered"] = new_index.mssql_clustered
            if old_index.oracle_tablespace != new_index.oracle_tablespace:
                fields.append("oracle_tablespace")
                from_details["oracle_tablespace"] = old_index.oracle_tablespace
                to_details["oracle_tablespace"] = new_index.oracle_tablespace
            if old_index.oracle_bitmap != new_index.oracle_bitmap:
                fields.append("oracle_bitmap")
                from_details["oracle_bitmap"] = old_index.oracle_bitmap
                to_details["oracle_bitmap"] = new_index.oracle_bitmap
            if old_index.oracle_compress != new_index.oracle_compress:
                fields.append("oracle_compress")
                from_details["oracle_compress"] = old_index.oracle_compress
                to_details["oracle_compress"] = new_index.oracle_compress
            if old_index.postgres_ops != new_index.postgres_ops:
                fields.append("postgres_ops")
                from_details["postgres_ops"] = dict(old_index.postgres_ops)
                to_details["postgres_ops"] = dict(new_index.postgres_ops)
            if (
                old_index.postgres_nulls_not_distinct
                != new_index.postgres_nulls_not_distinct
            ):
                fields.append("postgres_nulls_not_distinct")
                from_details["postgres_nulls_not_distinct"] = (
                    old_index.postgres_nulls_not_distinct
                )
                to_details["postgres_nulls_not_distinct"] = (
                    new_index.postgres_nulls_not_distinct
                )
            if old_index.mysql_prefix != new_index.mysql_prefix:
                fields.append("mysql_prefix")
                from_details["mysql_prefix"] = old_index.mysql_prefix
                to_details["mysql_prefix"] = new_index.mysql_prefix
            if old_index.mysql_length != new_index.mysql_length:
                fields.append("mysql_length")
                from_details["mysql_length"] = dict(old_index.mysql_length)
                to_details["mysql_length"] = dict(new_index.mysql_length)
            if old_index.mysql_using != new_index.mysql_using:
                fields.append("mysql_using")
                from_details["mysql_using"] = old_index.mysql_using
                to_details["mysql_using"] = new_index.mysql_using
            if _normalized_default_mysql_visible(
                old_index.mysql_visible
            ) != _normalized_default_mysql_visible(new_index.mysql_visible):
                fields.append("mysql_visible")
                from_details["mysql_visible"] = old_index.mysql_visible
                to_details["mysql_visible"] = new_index.mysql_visible
            if fields:
                changes.append(
                    MigrationChange(
                        "change",
                        "index",
                        after_label,
                        index_name,
                        f"Changed index {index_name} on {after_label}: "
                        f"{', '.join(fields)}",
                        details={
                            "fields": fields,
                            "from": from_details,
                            "to": to_details,
                        },
                    )
                )
            continue
        changes.append(
            MigrationChange(
                "change",
                "index",
                after_label,
                index_name,
                f"Changed index {index_name} on {after_label}",
                unsafe=old_index.unique or new_index.unique,
                details={
                    "from": old_index.to_dict(),
                    "to": new_index.to_dict(),
                },
            )
        )


def _indexes_equivalent(before: IndexSnapshot, after: IndexSnapshot) -> bool:
    return (
        before.name == after.name
        and before.columns == after.columns
        and before.unique == after.unique
        and before.where == after.where
        and before.include_columns == after.include_columns
        and _normalized_default_index_method(before.method)
        == _normalized_default_index_method(after.method)
        and before.expressions == after.expressions
        and before.postgres_with == after.postgres_with
    )


def _normalized_default_index_method(method: str | None) -> str | None:
    if method is None:
        return None
    normalized = method.lower()
    if normalized == "btree":
        return None
    return normalized


def _normalized_default_mysql_visible(visible: bool | None) -> bool | None:
    if visible is True:
        return None
    return visible


def _diff_constraints(
    changes: list[MigrationChange], before: TableSnapshot, after: TableSnapshot
) -> None:
    before_constraints = _constraints(before)
    after_constraints = _constraints(after)
    before_label = _table_label(before)
    after_label = _table_label(after)

    for name, constraint in after_constraints.items():
        if name not in before_constraints:
            changes.append(
                MigrationChange(
                    "add",
                    "constraint",
                    after_label,
                    name,
                    f"Added {constraint['kind']} constraint {name} on {after_label}",
                    details=constraint,
                )
            )

    for name, constraint in before_constraints.items():
        if name not in after_constraints:
            changes.append(
                MigrationChange(
                    "remove",
                    "constraint",
                    before_label,
                    name,
                    f"Removed {constraint['kind']} constraint {name} from {before_label}",
                    unsafe=True,
                    details=constraint,
                )
            )

    for name, old_constraint in before_constraints.items():
        new_constraint = after_constraints.get(name)
        if new_constraint is None or old_constraint == new_constraint:
            continue
        if _constraints_equivalent(old_constraint, new_constraint):
            if old_constraint.get("comment") != new_constraint.get("comment"):
                changes.append(
                    MigrationChange(
                        "change",
                        "constraint",
                        after_label,
                        name,
                        f"Changed {old_constraint['kind']} constraint {name} "
                        f"on {after_label}: comment",
                        details={
                            "fields": ["comment"],
                            "from": {"comment": old_constraint.get("comment")},
                            "to": {"comment": new_constraint.get("comment")},
                        },
                    )
                )
            continue
        changes.append(
            MigrationChange(
                "change",
                "constraint",
                after_label,
                name,
                f"Changed {old_constraint['kind']} constraint {name} on {after_label}",
                unsafe=True,
                details={
                    "from": old_constraint,
                    "to": new_constraint,
                },
            )
        )


def _constraints_equivalent(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> bool:
    before_payload = dict(before)
    after_payload = dict(after)
    before_payload.pop("comment", None)
    after_payload.pop("comment", None)
    return before_payload == after_payload


def _diff_table_metadata(
    changes: list[MigrationChange],
    before: TableSnapshot,
    after: TableSnapshot,
    *,
    dialect: str | None = None,
) -> None:
    fields: list[str] = []
    before_details: dict[str, Any] = {}
    after_details: dict[str, Any] = {}
    if before.comment != after.comment:
        fields.append("comment")
        before_details["comment"] = before.comment
        after_details["comment"] = after.comment
    if before.tablespace != after.tablespace:
        fields.append("tablespace")
        before_details["tablespace"] = before.tablespace
        after_details["tablespace"] = after.tablespace
    if before.mysql_engine != after.mysql_engine:
        fields.append("mysql_engine")
        before_details["mysql_engine"] = before.mysql_engine
        after_details["mysql_engine"] = after.mysql_engine
    if before.mysql_charset != after.mysql_charset:
        fields.append("mysql_charset")
        before_details["mysql_charset"] = before.mysql_charset
        after_details["mysql_charset"] = after.mysql_charset
    if before.mysql_collation != after.mysql_collation:
        fields.append("mysql_collation")
        before_details["mysql_collation"] = before.mysql_collation
        after_details["mysql_collation"] = after.mysql_collation
    if before.mysql_row_format != after.mysql_row_format:
        fields.append("mysql_row_format")
        before_details["mysql_row_format"] = before.mysql_row_format
        after_details["mysql_row_format"] = after.mysql_row_format
    if before.mysql_key_block_size != after.mysql_key_block_size:
        fields.append("mysql_key_block_size")
        before_details["mysql_key_block_size"] = before.mysql_key_block_size
        after_details["mysql_key_block_size"] = after.mysql_key_block_size
    if before.mysql_pack_keys != after.mysql_pack_keys:
        fields.append("mysql_pack_keys")
        before_details["mysql_pack_keys"] = before.mysql_pack_keys
        after_details["mysql_pack_keys"] = after.mysql_pack_keys
    if before.mysql_checksum != after.mysql_checksum:
        fields.append("mysql_checksum")
        before_details["mysql_checksum"] = before.mysql_checksum
        after_details["mysql_checksum"] = after.mysql_checksum
    if before.mysql_delay_key_write != after.mysql_delay_key_write:
        fields.append("mysql_delay_key_write")
        before_details["mysql_delay_key_write"] = before.mysql_delay_key_write
        after_details["mysql_delay_key_write"] = after.mysql_delay_key_write
    if before.mysql_stats_persistent != after.mysql_stats_persistent:
        fields.append("mysql_stats_persistent")
        before_details["mysql_stats_persistent"] = before.mysql_stats_persistent
        after_details["mysql_stats_persistent"] = after.mysql_stats_persistent
    if before.mysql_stats_auto_recalc != after.mysql_stats_auto_recalc:
        fields.append("mysql_stats_auto_recalc")
        before_details["mysql_stats_auto_recalc"] = before.mysql_stats_auto_recalc
        after_details["mysql_stats_auto_recalc"] = after.mysql_stats_auto_recalc
    if before.mysql_stats_sample_pages != after.mysql_stats_sample_pages:
        fields.append("mysql_stats_sample_pages")
        before_details["mysql_stats_sample_pages"] = before.mysql_stats_sample_pages
        after_details["mysql_stats_sample_pages"] = after.mysql_stats_sample_pages
    if before.mysql_avg_row_length != after.mysql_avg_row_length:
        fields.append("mysql_avg_row_length")
        before_details["mysql_avg_row_length"] = before.mysql_avg_row_length
        after_details["mysql_avg_row_length"] = after.mysql_avg_row_length
    if before.mysql_max_rows != after.mysql_max_rows:
        fields.append("mysql_max_rows")
        before_details["mysql_max_rows"] = before.mysql_max_rows
        after_details["mysql_max_rows"] = after.mysql_max_rows
    if before.mysql_min_rows != after.mysql_min_rows:
        fields.append("mysql_min_rows")
        before_details["mysql_min_rows"] = before.mysql_min_rows
        after_details["mysql_min_rows"] = after.mysql_min_rows
    if before.mysql_insert_method != after.mysql_insert_method:
        fields.append("mysql_insert_method")
        before_details["mysql_insert_method"] = before.mysql_insert_method
        after_details["mysql_insert_method"] = after.mysql_insert_method
    if before.mysql_data_directory != after.mysql_data_directory:
        fields.append("mysql_data_directory")
        before_details["mysql_data_directory"] = before.mysql_data_directory
        after_details["mysql_data_directory"] = after.mysql_data_directory
    if before.mysql_index_directory != after.mysql_index_directory:
        fields.append("mysql_index_directory")
        before_details["mysql_index_directory"] = before.mysql_index_directory
        after_details["mysql_index_directory"] = after.mysql_index_directory
    if before.mysql_connection != after.mysql_connection:
        fields.append("mysql_connection")
        before_details["mysql_connection"] = before.mysql_connection
        after_details["mysql_connection"] = after.mysql_connection
    if before.mysql_union != after.mysql_union:
        fields.append("mysql_union")
        before_details["mysql_union"] = list(before.mysql_union)
        after_details["mysql_union"] = list(after.mysql_union)
    if before.mysql_partition_by != after.mysql_partition_by:
        fields.append("mysql_partition_by")
        before_details["mysql_partition_by"] = before.mysql_partition_by
        after_details["mysql_partition_by"] = after.mysql_partition_by
    if before.mysql_partitions != after.mysql_partitions:
        fields.append("mysql_partitions")
        before_details["mysql_partitions"] = before.mysql_partitions
        after_details["mysql_partitions"] = after.mysql_partitions
    if before.mysql_subpartition_by != after.mysql_subpartition_by:
        fields.append("mysql_subpartition_by")
        before_details["mysql_subpartition_by"] = before.mysql_subpartition_by
        after_details["mysql_subpartition_by"] = after.mysql_subpartition_by
    if before.mysql_subpartitions != after.mysql_subpartitions:
        fields.append("mysql_subpartitions")
        before_details["mysql_subpartitions"] = before.mysql_subpartitions
        after_details["mysql_subpartitions"] = after.mysql_subpartitions
    if _mysql_auto_increment_changed(before, after, dialect=dialect):
        fields.append("mysql_auto_increment")
        before_details["mysql_auto_increment"] = before.mysql_auto_increment
        after_details["mysql_auto_increment"] = after.mysql_auto_increment
    if before.postgres_inherits != after.postgres_inherits:
        fields.append("postgres_inherits")
        before_details["postgres_inherits"] = list(before.postgres_inherits)
        after_details["postgres_inherits"] = list(after.postgres_inherits)
    if before.postgres_with != after.postgres_with:
        fields.append("postgres_with")
        before_details["postgres_with"] = [
            list(parameter) for parameter in before.postgres_with
        ]
        after_details["postgres_with"] = [
            list(parameter) for parameter in after.postgres_with
        ]
    if before.postgres_using != after.postgres_using:
        fields.append("postgres_using")
        before_details["postgres_using"] = before.postgres_using
        after_details["postgres_using"] = after.postgres_using
    if before.postgres_unlogged != after.postgres_unlogged:
        fields.append("postgres_unlogged")
        before_details["postgres_unlogged"] = before.postgres_unlogged
        after_details["postgres_unlogged"] = after.postgres_unlogged
    if before.sqlite_strict != after.sqlite_strict:
        fields.append("sqlite_strict")
        before_details["sqlite_strict"] = before.sqlite_strict
        after_details["sqlite_strict"] = after.sqlite_strict
    if before.sqlite_without_rowid != after.sqlite_without_rowid:
        fields.append("sqlite_without_rowid")
        before_details["sqlite_without_rowid"] = before.sqlite_without_rowid
        after_details["sqlite_without_rowid"] = after.sqlite_without_rowid
    if before.oracle_compress != after.oracle_compress:
        fields.append("oracle_compress")
        before_details["oracle_compress"] = before.oracle_compress
        after_details["oracle_compress"] = after.oracle_compress
    sqlite_table_options_changed = (
        before.sqlite_strict != after.sqlite_strict
        or before.sqlite_without_rowid != after.sqlite_without_rowid
    )
    destructive = (
        before.postgres_partition_by != after.postgres_partition_by
        or sqlite_table_options_changed
        or before.oracle_compress != after.oracle_compress
    )
    unsafe = destructive or (
        before.postgres_partition_of != after.postgres_partition_of
        or before.postgres_partition_for != after.postgres_partition_for
    )
    if unsafe:
        for field_name in (
            "postgres_partition_by",
            "postgres_partition_of",
            "postgres_partition_for",
        ):
            if getattr(before, field_name) != getattr(after, field_name):
                fields.append(field_name)
                before_details[field_name] = getattr(before, field_name)
                after_details[field_name] = getattr(after, field_name)
    if not fields:
        return
    after_label = _table_label(after)
    changes.append(
        MigrationChange(
            "change",
            "table",
            after_label,
            after_label,
            f"Changed table {after_label}: {', '.join(fields)}",
            unsafe=unsafe,
            destructive=destructive,
            details={
                "fields": fields,
                "from": before_details,
                "to": after_details,
            },
        )
    )


def _mysql_auto_increment_changed(
    before: TableSnapshot,
    after: TableSnapshot,
    *,
    dialect: str | None,
) -> bool:
    if dialect in {"mysql", "mariadb"} and after.mysql_auto_increment is None:
        return False
    return before.mysql_auto_increment != after.mysql_auto_increment


def _constraints(table: TableSnapshot) -> dict[str, dict[str, Any]]:
    constraints: dict[str, dict[str, Any]] = {}
    for constraint in table.named_unique_constraints:
        constraints[constraint.name] = {
            "kind": "unique",
            "columns": list(constraint.columns),
            "postgres_include": list(constraint.postgres_include),
            "deferrable": constraint.deferrable,
            "initially_deferred": constraint.initially_deferred,
            "nulls_not_distinct": constraint.nulls_not_distinct,
            "sqlite_on_conflict": constraint.sqlite_on_conflict,
            "mssql_filegroup": constraint.mssql_filegroup,
            "mssql_clustered": _mssql_unique_clustered_constraint_value(constraint),
            "oracle_tablespace": constraint.oracle_tablespace,
            "oracle_compress": constraint.oracle_compress,
            "comment": constraint.comment,
        }
    unique_constraints = list(table.unique_constraints)
    unique_constraints.extend(
        [column.name] for column in table.columns if column.unique
    )
    for idx, columns in enumerate(unique_constraints):
        name = f"{table.name}_unique_{idx}"
        constraints[name] = {
            "kind": "unique",
            "columns": list(columns),
            "sqlite_on_conflict": next(
                (
                    column.sqlite_on_conflict_unique
                    for column in table.columns
                    if [column.name] == list(columns)
                ),
                None,
            ),
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
            name = column.foreign_key_name or f"{table.name}_{column.name}_foreign_key"
            constraints[name] = {
                "kind": "foreign_key",
                "columns": [column.name],
                "foreign_table": column.foreign_table,
                "foreign_columns": [column.foreign_column],
                "on_delete": column.on_delete,
                "on_update": column.on_update,
                "deferrable": column.deferrable,
                "initially_deferred": column.initially_deferred,
            }
    for table_check in table.check_constraints:
        constraints[table_check.name] = {
            "kind": "check",
            "expression": _normalized_table_check_expression(table_check.expression),
            "validated": table_check.validated,
            "no_inherit": table_check.no_inherit,
            "comment": table_check.comment,
        }
    for foreign_key in table.foreign_key_constraints:
        constraints[foreign_key.name] = {
            "kind": "foreign_key",
            "columns": list(foreign_key.columns),
            "foreign_table": foreign_key.foreign_table,
            "foreign_columns": list(foreign_key.foreign_columns),
            "on_delete": foreign_key.on_delete,
            "on_update": foreign_key.on_update,
            "deferrable": foreign_key.deferrable,
            "initially_deferred": foreign_key.initially_deferred,
            "validated": foreign_key.validated,
            "match": foreign_key.match,
            "comment": foreign_key.comment,
        }
    for exclusion in table.exclusion_constraints:
        constraints[exclusion.name] = {
            "kind": "exclusion",
            "columns": [list(element) for element in exclusion.columns],
            "expressions": [list(element) for element in exclusion.expressions],
            "ops": dict(exclusion.ops),
            "using": exclusion.using,
            "where": exclusion.where,
            "deferrable": exclusion.deferrable,
            "initially_deferred": exclusion.initially_deferred,
            "comment": exclusion.comment,
        }
    return constraints


def _mssql_unique_clustered_value(value: bool | None) -> bool | None:
    return True if value is True else None


def _mssql_unique_clustered_constraint_value(
    constraint: UniqueConstraintSnapshot,
) -> bool | None:
    if constraint.mssql_clustered is False and constraint.mssql_filegroup is not None:
        return False
    return _mssql_unique_clustered_value(constraint.mssql_clustered)


def _changed_column_fields(
    old_column: ColumnSnapshot,
    new_column: ColumnSnapshot,
    *,
    dialect: str | None = None,
) -> list[str]:
    fields = []
    for field_name in (
        "kind",
        "nullable",
        "primary_key",
        "comment",
        "foreign_table",
        "foreign_column",
        "max_length",
        "unique",
        "checks",
        "server_default",
        "computed",
        "computed_persisted",
        "autoincrement",
        "identity",
        "identity_always",
        "identity_start",
        "identity_increment",
        "identity_cycle",
        "identity_cache",
        "identity_order",
        "identity_on_null",
        "collation",
        "numeric_precision",
        "numeric_scale",
        "foreign_key_name",
        "on_delete",
        "on_update",
        "deferrable",
        "initially_deferred",
        "sqlite_on_conflict_primary_key",
        "sqlite_on_conflict_not_null",
        "sqlite_on_conflict_unique",
    ):
        if getattr(old_column, field_name) != getattr(new_column, field_name):
            fields.append(field_name)
    for value_field, no_value_field in (
        ("identity_min_value", "identity_no_min_value"),
        ("identity_max_value", "identity_no_max_value"),
    ):
        if _identity_bound_changed(
            old_column,
            new_column,
            value_field,
            no_value_field,
            dialect=dialect,
        ):
            fields.append(value_field)
    return fields


def _identity_bound_changed(
    old_column: ColumnSnapshot,
    new_column: ColumnSnapshot,
    value_field: str,
    no_value_field: str,
    *,
    dialect: str | None = None,
) -> bool:
    old_value = getattr(old_column, value_field)
    new_value = getattr(new_column, value_field)
    if old_value != new_value:
        if (
            _identity_bound_matches_default(
                old_column,
                value_field,
                old_value,
                dialect=dialect,
            )
            and new_value is None
        ):
            return False
        if (
            _identity_bound_matches_default(
                new_column,
                value_field,
                new_value,
                dialect=dialect,
            )
            and old_value is None
        ):
            return False
        return True
    if old_value is not None or new_value is not None:
        return getattr(old_column, no_value_field) != getattr(
            new_column,
            no_value_field,
        )
    return False


def _identity_bound_matches_default(
    column: ColumnSnapshot,
    value_field: str,
    value: Any,
    *,
    dialect: str | None,
) -> bool:
    bound = "min" if value_field == "identity_min_value" else "max"
    default = _default_identity_bound(
        dialect,
        column,
        bound=bound,
    )
    return default is not None and value == default


def _default_identity_bound(
    dialect: str | None,
    column: ColumnSnapshot,
    *,
    bound: str,
) -> int | None:
    if dialect is None:
        return None
    increment = (
        column.identity_increment if column.identity_increment is not None else 1
    )
    if dialect == "postgresql":
        type_min, type_max = _postgres_integer_type_range(column.kind)
        if increment < 0:
            return type_min if bound == "min" else -1
        return 1 if bound == "min" else type_max
    if dialect == "oracle":
        if increment < 0:
            return -(10**27 - 1) if bound == "min" else -1
        return 1 if bound == "min" else 10**28 - 1
    return None


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
    metadata.update(
        {
            key: item[key]
            for key in (
                "kind",
                "table",
                "object_name",
                "unsafe",
                "destructive",
                "reversible",
                "requires_rebuild",
            )
            if key in item
        }
    )
    return MigrationOperation(
        sql=sql,
        values=tuple(item.get("params", ())),
        kind=str(metadata["kind"]),
        table=optional_str(metadata.get("table")),
        object_name=optional_str(metadata.get("object_name")),
        unsafe=bool(metadata.get("unsafe", False)) and not generated_rollback,
        destructive=bool(metadata.get("destructive", False)) and not generated_rollback,
        reversible=bool(metadata.get("reversible", True)),
        requires_lock=True,
        requires_rebuild=bool(metadata.get("requires_rebuild", False)),
        generated_rollback=generated_rollback,
        metadata={
            "dialect": dialect,
            "classification": metadata,
        },
    )


def _mark_sqlite_table_option_rebuilds(
    operations: Sequence[MigrationOperation],
    schema_diff: SchemaDiff,
) -> None:
    rebuild_tables: set[str] = set()
    option_fields = {"sqlite_strict", "sqlite_without_rowid"}
    for change in schema_diff.changes:
        if change.action != "change" or change.object_type != "table":
            continue
        fields = {str(field) for field in change.details.get("fields", [])}
        if fields & option_fields:
            rebuild_tables.add(change.table)
    if not rebuild_tables:
        return
    for operation in operations:
        if operation.table not in rebuild_tables:
            continue
        if operation.kind not in {"drop_table", "create_table"}:
            continue
        operation.requires_rebuild = True
        operation.metadata["sqlite_table_options_rebuild"] = True


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
        schema=table.schema,
        columns=list(table.columns),
        indexes=list(table.indexes if indexes is None else indexes),
        unique_constraints=[list(columns) for columns in table.unique_constraints],
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
        check_constraints=list(table.check_constraints),
        foreign_key_constraints=[
            ForeignKeyConstraintSnapshot(
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
            )
            for constraint in table.foreign_key_constraints
        ],
        exclusion_constraints=[
            ExclusionConstraintSnapshot(
                constraint.name,
                list(constraint.columns),
                list(constraint.expressions),
                constraint.using,
                constraint.where,
                constraint.deferrable,
                constraint.initially_deferred,
                dict(constraint.ops),
            )
            for constraint in table.exclusion_constraints
        ],
        relationships=list(table.relationships),
        comment=table.comment,
        tablespace=table.tablespace,
        mysql_engine=table.mysql_engine,
        mysql_charset=table.mysql_charset,
        mysql_collation=table.mysql_collation,
        mysql_row_format=table.mysql_row_format,
        mysql_key_block_size=table.mysql_key_block_size,
        mysql_pack_keys=table.mysql_pack_keys,
        mysql_checksum=table.mysql_checksum,
        mysql_delay_key_write=table.mysql_delay_key_write,
        mysql_stats_persistent=table.mysql_stats_persistent,
        mysql_stats_auto_recalc=table.mysql_stats_auto_recalc,
        mysql_stats_sample_pages=table.mysql_stats_sample_pages,
        mysql_avg_row_length=table.mysql_avg_row_length,
        mysql_max_rows=table.mysql_max_rows,
        mysql_min_rows=table.mysql_min_rows,
        mysql_insert_method=table.mysql_insert_method,
        mysql_data_directory=table.mysql_data_directory,
        mysql_index_directory=table.mysql_index_directory,
        mysql_connection=table.mysql_connection,
        mysql_union=list(table.mysql_union),
        mysql_partition_by=table.mysql_partition_by,
        mysql_partitions=table.mysql_partitions,
        mysql_subpartition_by=table.mysql_subpartition_by,
        mysql_subpartitions=table.mysql_subpartitions,
        mysql_auto_increment=table.mysql_auto_increment,
        postgres_inherits=list(table.postgres_inherits),
        postgres_with=list(table.postgres_with),
        postgres_using=table.postgres_using,
        postgres_partition_by=table.postgres_partition_by,
        postgres_partition_of=table.postgres_partition_of,
        postgres_partition_for=table.postgres_partition_for,
        postgres_unlogged=table.postgres_unlogged,
        sqlite_strict=table.sqlite_strict,
        sqlite_without_rowid=table.sqlite_without_rowid,
        oracle_compress=table.oracle_compress,
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
    elif upper.startswith("CREATE SCHEMA"):
        kind = "create_schema"
        object_name = _sql_identifier_after(normalized, "CREATE SCHEMA")
    elif upper.startswith("IF SCHEMA_ID") and "CREATE SCHEMA" in upper:
        kind = "create_schema"
    elif upper.startswith("CREATE MATERIALIZED VIEW"):
        kind = "create_materialized_view"
        object_name = _sql_identifier_after(normalized, "CREATE MATERIALIZED VIEW")
    elif upper.startswith("CREATE VIEW"):
        kind = "create_view"
        object_name = _sql_identifier_after(normalized, "CREATE VIEW")
    elif upper.startswith("CREATE SEQUENCE"):
        kind = "create_sequence"
        object_name = _sql_identifier_after(normalized, "CREATE SEQUENCE")
    elif upper.startswith("CREATE TYPE"):
        kind = "create_enum_type"
        object_name = _sql_identifier_after(normalized, "CREATE TYPE")
    elif upper.startswith("DROP TABLE"):
        kind = "drop_table"
        table = _sql_identifier_after(normalized, "DROP TABLE")
        destructive = True
        unsafe = True
    elif upper.startswith("DROP SCHEMA"):
        kind = "drop_schema"
        object_name = _sql_identifier_after(normalized, "DROP SCHEMA")
        destructive = True
        unsafe = True
    elif upper.startswith("DROP MATERIALIZED VIEW"):
        kind = "drop_materialized_view"
        object_name = _sql_identifier_after(normalized, "DROP MATERIALIZED VIEW")
        destructive = True
        unsafe = True
    elif upper.startswith("DROP VIEW"):
        kind = "drop_view"
        object_name = _sql_identifier_after(normalized, "DROP VIEW")
        destructive = True
        unsafe = True
    elif upper.startswith("DROP SEQUENCE"):
        kind = "drop_sequence"
        object_name = _sql_identifier_after(normalized, "DROP SEQUENCE")
        destructive = True
        unsafe = True
    elif upper.startswith("DROP TYPE"):
        kind = "drop_enum_type"
        object_name = _sql_identifier_after(normalized, "DROP TYPE")
        destructive = True
        unsafe = True
    elif upper.startswith("COMMENT ON COLUMN"):
        kind = "comment_column"
        object_name = _sql_identifier_after(normalized, "COMMENT ON COLUMN")
        table = _table_from_column_identifier(object_name)
    elif upper.startswith("COMMENT ON TABLE"):
        kind = "comment_table"
        table = _sql_identifier_after(normalized, "COMMENT ON TABLE")
    elif upper.startswith("COMMENT ON TYPE"):
        kind = "comment_enum_type"
        object_name = _sql_identifier_after(normalized, "COMMENT ON TYPE")
    elif upper.startswith("COMMENT ON INDEX"):
        kind = "comment_index"
        object_name = _sql_identifier_after(normalized, "COMMENT ON INDEX")
    elif upper.startswith("COMMENT ON CONSTRAINT"):
        kind = "comment_constraint"
        object_name = _sql_identifier_after(normalized, "COMMENT ON CONSTRAINT")
    elif upper.startswith("COMMENT ON MATERIALIZED VIEW"):
        kind = "comment_view"
        object_name = _sql_identifier_after(normalized, "COMMENT ON MATERIALIZED VIEW")
    elif upper.startswith("COMMENT ON VIEW"):
        kind = "comment_view"
        object_name = _sql_identifier_after(normalized, "COMMENT ON VIEW")
    elif upper.startswith("COMMENT ON SEQUENCE"):
        kind = "comment_sequence"
        object_name = _sql_identifier_after(normalized, "COMMENT ON SEQUENCE")
    elif upper.startswith("COMMENT ON SCHEMA"):
        kind = "comment_schema"
        object_name = _sql_identifier_after(normalized, "COMMENT ON SCHEMA")
    elif (
        (
            "SYS.SP_UPDATEEXTENDEDPROPERTY" in upper
            or "SYS.SP_ADDEXTENDEDPROPERTY" in upper
            or "SYS.SP_DROPEXTENDEDPROPERTY" in upper
        )
        and "@LEVEL0TYPE = N'SCHEMA'" in upper
        and "@LEVEL1TYPE" not in upper
    ):
        kind = "comment_schema"
    elif (
        "SYS.SP_UPDATEEXTENDEDPROPERTY" in upper
        or "SYS.SP_ADDEXTENDEDPROPERTY" in upper
        or "SYS.SP_DROPEXTENDEDPROPERTY" in upper
    ) and "@LEVEL1TYPE = N'VIEW'" in upper:
        kind = "comment_view"
    elif (
        "SYS.SP_UPDATEEXTENDEDPROPERTY" in upper
        or "SYS.SP_ADDEXTENDEDPROPERTY" in upper
        or "SYS.SP_DROPEXTENDEDPROPERTY" in upper
    ) and "@LEVEL1TYPE = N'SEQUENCE'" in upper:
        kind = "comment_sequence"
    elif (
        "SYS.SP_UPDATEEXTENDEDPROPERTY" in upper
        or "SYS.SP_ADDEXTENDEDPROPERTY" in upper
        or "SYS.SP_DROPEXTENDEDPROPERTY" in upper
    ) and "@LEVEL2TYPE = N'INDEX'" in upper:
        kind = "comment_index"
    elif (
        "SYS.SP_UPDATEEXTENDEDPROPERTY" in upper
        or "SYS.SP_ADDEXTENDEDPROPERTY" in upper
        or "SYS.SP_DROPEXTENDEDPROPERTY" in upper
    ) and "@LEVEL2TYPE = N'CONSTRAINT'" in upper:
        kind = "comment_constraint"
    elif upper.startswith("ALTER TABLE"):
        table = _sql_identifier_after(normalized, "ALTER TABLE")
        if " MODIFY COLUMN " in upper and " COMMENT" in upper:
            kind = "comment_column"
        elif " COMMENT" in upper:
            kind = "comment_table"
        elif (
            " SET TABLESPACE " in upper
            or " MOVE TABLESPACE " in upper
            or " TABLESPACE " in upper
            or upper.endswith(" MOVE")
            or " REBUILD ON " in upper
            or " ENGINE " in upper
            or " CHARACTER SET " in upper
            or " COLLATE " in upper
            or " ROW_FORMAT " in upper
            or " KEY_BLOCK_SIZE " in upper
            or " PACK_KEYS " in upper
            or " CHECKSUM " in upper
            or " DELAY_KEY_WRITE " in upper
            or " STATS_PERSISTENT " in upper
            or " STATS_AUTO_RECALC " in upper
            or " STATS_SAMPLE_PAGES " in upper
            or " AVG_ROW_LENGTH " in upper
            or " MAX_ROWS " in upper
            or " MIN_ROWS " in upper
            or " INSERT_METHOD " in upper
            or " DATA DIRECTORY " in upper
            or " INDEX DIRECTORY " in upper
            or " CONNECTION " in upper
            or " UNION " in upper
            or " INHERIT " in upper
            or " NO INHERIT " in upper
            or " SET (" in upper
            or " RESET (" in upper
            or " SET ACCESS METHOD " in upper
            or " SET LOGGED" in upper
            or " SET UNLOGGED" in upper
            or " ATTACH PARTITION " in upper
            or " DETACH PARTITION " in upper
        ):
            kind = "table_partition" if " PARTITION " in upper else "table_storage"
            unsafe = kind == "table_partition"
        else:
            kind = "alter_table"
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
    elif upper.startswith("ALTER INDEX"):
        object_name = _sql_identifier_after(normalized, "ALTER INDEX")
        if " SET TABLESPACE " in upper:
            kind = "index_storage"
        else:
            kind = "alter_index"
            unsafe = True
    elif _is_create_index_statement(upper):
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
    return _normalize_sql_identifier_token(token)


def _table_from_column_identifier(identifier: str | None) -> str | None:
    if identifier is None:
        return None
    parts = [part.strip('`"[]') for part in identifier.split(".")]
    if len(parts) >= 2:
        return parts[-2]
    return parts[0] if parts else None


def _sql_identifier_after_keyword(statement: str, keyword: str) -> str | None:
    marker = statement.upper().find(keyword)
    if marker < 0:
        return None
    remainder = statement[marker + len(keyword) :].strip()
    if not remainder:
        return None
    token = remainder.split(" ", 1)[0].strip()
    return _normalize_sql_identifier_token(token)


def _normalize_sql_identifier_token(token: str) -> str:
    parts = [part.strip('`"[]') for part in token.split(".")]
    return ".".join(parts)


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
    normalized_dialect = dialect_name(dialect)
    _raise_if_unsupported_mssql_table_filegroup_changes(
        normalized_dialect,
        from_snapshot,
        to_snapshot,
    )
    metadata_snapshot = to_snapshot
    rust_from_snapshot = from_snapshot
    rust_to_snapshot = to_snapshot
    if normalized_dialect == "mariadb":
        rust_to_snapshot = _snapshot_without_mariadb_table_tablespace_changes(
            from_snapshot,
            to_snapshot,
        )
    if normalized_dialect in {"mysql", "mariadb"}:
        rust_from_snapshot = _snapshot_without_unmanaged_mysql_auto_increment(
            rust_from_snapshot,
            rust_to_snapshot,
        )
    rust_from_snapshot = _snapshot_without_python_metadata(rust_from_snapshot)
    rust_to_snapshot = _snapshot_without_python_metadata(rust_to_snapshot)
    rust_from_snapshot, rust_to_snapshot = (
        _snapshots_with_normalized_matching_table_checks(
            rust_from_snapshot,
            rust_to_snapshot,
        )
    )
    compiled = list(
        rust.compile_schema_diff(
            dialect,
            rust_from_snapshot.to_runtime(),
            rust_to_snapshot.to_runtime(),
        )
    )
    compiled = _rewrite_generated_column_check_operations(
        normalized_dialect,
        compiled,
        metadata_snapshot.tables,
    )
    operations = _annotate_postgres_unique_include_operations(
        normalized_dialect,
        compiled,
        metadata_snapshot.tables,
    )
    operations = _annotate_postgres_index_ops_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_postgres_index_nulls_not_distinct_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_mysql_index_prefix_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_mysql_index_using_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_mysql_index_length_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_mysql_index_visibility_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_inline_index_comment_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_mssql_clustered_index_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_mssql_index_filegroup_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations = _annotate_oracle_index_options_operations(
        normalized_dialect,
        operations,
        metadata_snapshot.tables,
    )
    operations.extend(
        _compile_mariadb_table_tablespace_diff(
            normalized_dialect,
            from_snapshot,
            to_snapshot,
        )
    )
    return operations


def _rewrite_generated_column_check_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    replacements: dict[str, str] = {}
    normalized_dialect = dialect_name(dialect)
    for table in tables:
        for column in table.columns:
            for check in column.checks:
                portable = _check_expression(column.name, check)
                rendered = _check_expression(
                    column.name,
                    check,
                    dialect=normalized_dialect,
                )
                if portable != rendered:
                    replacements[portable] = rendered
    if not replacements:
        return [dict(operation) for operation in operations]
    rewritten: list[dict[str, Any]] = []
    for operation in operations:
        payload = dict(operation)
        sql = str(payload["sql"])
        for portable, rendered in replacements.items():
            sql = sql.replace(portable, rendered)
        payload["sql"] = sql
        rewritten.append(payload)
    return rewritten


def _snapshot_without_mariadb_table_tablespace_changes(
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> SchemaSnapshot:
    from_tables = {_table_key(table): table for table in from_snapshot.tables}
    tables: list[TableSnapshot] = []
    for table in to_snapshot.tables:
        before = from_tables.get(_table_key(table))
        if before is not None and before.tablespace != table.tablespace:
            tables.append(replace(table, tablespace=before.tablespace))
        else:
            tables.append(table)
    return replace(to_snapshot, tables=tables)


def _snapshot_without_unmanaged_mysql_auto_increment(
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> SchemaSnapshot:
    to_tables = {_table_key(table): table for table in to_snapshot.tables}
    tables: list[TableSnapshot] = []
    for table in from_snapshot.tables:
        target = to_tables.get(_table_key(table))
        if (
            target is not None
            and target.mysql_auto_increment is None
            and table.mysql_auto_increment is not None
        ):
            tables.append(replace(table, mysql_auto_increment=None))
        else:
            tables.append(table)
    return replace(from_snapshot, tables=tables)


def _compile_mariadb_table_tablespace_diff(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> list[dict[str, Any]]:
    if dialect_name(dialect) != "mariadb":
        return []
    from_tables = {_table_key(table): table for table in from_snapshot.tables}
    statements: list[dict[str, Any]] = []
    for table in to_snapshot.tables:
        before = from_tables.get(_table_key(table))
        if before is None or before.tablespace == table.tablespace:
            continue
        statements.append(
            _compiled_sql(_set_mysql_table_tablespace_sql(dialect, table))
        )
    return statements


def _raise_if_unsupported_mssql_table_filegroup_changes(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> None:
    if dialect_name(dialect) != "mssql":
        return
    from_tables = {_table_key(table): table for table in from_snapshot.tables}
    changed: list[str] = []
    for table in to_snapshot.tables:
        before = from_tables.get(_table_key(table))
        if before is None or before.tablespace == table.tablespace:
            continue
        changed.append(_table_label(table))
    if not changed:
        return
    snippets = ", ".join(changed[:3])
    if len(changed) > 3:
        snippets += ", ..."
    raise ValueError(
        "SQL Server table filegroup changes require a manual table move or rebuild; "
        f"changed tables: {snippets}"
    )


def _annotate_postgres_unique_include_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_postgres_unique_include_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_postgres_unique_include_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    includes = _postgres_unique_include_by_key(tables)
    if not includes:
        return list(statements)
    if dialect_name(dialect) != "postgresql":
        raise ValueError(
            "PostgreSQL unique constraint INCLUDE columns only support PostgreSQL"
        )
    return [
        _postgres_unique_include_statement_sql(statement, includes)
        for statement in statements
    ]


def _postgres_unique_include_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], list[str]]:
    includes: dict[tuple[str, str], list[str]] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for constraint in table.named_unique_constraints:
            if not constraint.postgres_include:
                continue
            for table_name in table_names:
                includes[(table_name, constraint.name)] = list(
                    constraint.postgres_include
                )
    return includes


def _postgres_unique_include_statement_sql(
    statement: str,
    includes: Mapping[tuple[str, str], Sequence[str]],
) -> str:
    normalized = " ".join(statement.strip().split())
    upper = normalized.upper()
    if upper.startswith("CREATE TABLE"):
        return _postgres_unique_include_create_table_sql(statement, includes)
    if upper.startswith("ALTER TABLE") and " ADD CONSTRAINT " in upper:
        return _postgres_unique_include_alter_table_sql(statement, includes)
    return statement


def _postgres_unique_include_create_table_sql(
    statement: str,
    includes: Mapping[tuple[str, str], Sequence[str]],
) -> str:
    normalized = " ".join(statement.strip().split())
    table_name = _sql_identifier_after(normalized, "CREATE TABLE")
    if table_name is None:
        return statement
    start = _find_sql_char(statement, "(", 0)
    if start is None:
        return statement
    end = _find_matching_sql_paren(statement, start)
    if end is None:
        return statement
    changed = False
    items: list[str] = []
    for item in _split_top_level_sql_list(statement[start + 1 : end]):
        constraint_name = _unique_constraint_segment_name(item)
        include_columns = (
            includes.get((table_name, constraint_name))
            if constraint_name is not None
            else None
        )
        if include_columns:
            item = _postgres_unique_include_constraint_sql(item, include_columns)
            changed = True
        items.append(item)
    if not changed:
        return statement
    return f"{statement[: start + 1]}{', '.join(items)}{statement[end:]}"


def _postgres_unique_include_alter_table_sql(
    statement: str,
    includes: Mapping[tuple[str, str], Sequence[str]],
) -> str:
    normalized = " ".join(statement.strip().split())
    table_name = _sql_identifier_after(normalized, "ALTER TABLE")
    constraint_name = _sql_identifier_after_keyword(normalized, " ADD CONSTRAINT ")
    if table_name is None or constraint_name is None:
        return statement
    include_columns = includes.get((table_name, constraint_name))
    if not include_columns:
        return statement
    return _postgres_unique_include_constraint_sql(statement, include_columns)


def _unique_constraint_segment_name(segment: str) -> str | None:
    normalized = " ".join(segment.strip().split())
    upper = normalized.upper()
    if not upper.startswith("CONSTRAINT ") or " UNIQUE" not in upper:
        return None
    return _sql_identifier_after(normalized, "CONSTRAINT")


def _postgres_unique_include_constraint_sql(
    statement: str,
    include_columns: Sequence[str],
) -> str:
    upper = statement.upper()
    unique_at = upper.find(" UNIQUE")
    if unique_at < 0:
        raise ValueError(
            "cannot apply PostgreSQL unique constraint INCLUDE columns to SQL: "
            f"{statement}"
        )
    start = _find_sql_char(statement, "(", unique_at)
    if start is None:
        raise ValueError(
            "cannot apply PostgreSQL unique constraint INCLUDE columns to SQL: "
            f"{statement}"
        )
    end = _find_matching_sql_paren(statement, start)
    if end is None:
        raise ValueError(
            "cannot apply PostgreSQL unique constraint INCLUDE columns to SQL: "
            f"{statement}"
        )
    include_sql = ", ".join(
        quote_ident("postgresql", column) for column in include_columns
    )
    return f"{statement[: end + 1]} INCLUDE ({include_sql}){statement[end + 1 :]}"


def _annotate_postgres_index_ops_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_postgres_index_ops_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_postgres_index_ops_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    ops_by_key = _postgres_index_ops_by_key(tables)
    if not ops_by_key:
        return list(statements)
    if dialect_name(dialect) != "postgresql":
        raise ValueError("PostgreSQL index operator classes only support PostgreSQL")
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        ops = ops_by_key.get(key) if key is not None else None
        if ops is None:
            annotated.append(statement)
        else:
            annotated.append(_postgres_index_ops_sql(statement, ops))
    return annotated


def _postgres_index_ops_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], dict[str, str]]:
    ops_by_key: dict[tuple[str, str], dict[str, str]] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if not index.postgres_ops:
                continue
            for table_name in table_names:
                ops_by_key[(table_name, index.name)] = dict(index.postgres_ops)
    return ops_by_key


def _postgres_index_ops_sql(statement: str, ops: Mapping[str, str]) -> str:
    column_bounds = _index_column_list_bounds(statement)
    if column_bounds is None:
        raise ValueError(
            "cannot apply PostgreSQL index operator classes to CREATE INDEX SQL: "
            f"{statement}"
        )
    start, end = column_bounds
    columns_sql = statement[start + 1 : end]
    seen: set[str] = set()
    items = [
        _postgres_index_ops_item_sql(item, ops, seen)
        for item in _split_top_level_sql_list(columns_sql)
    ]
    missing = sorted(set(ops) - seen)
    if missing:
        unknown = ", ".join(missing)
        key = _create_index_statement_key(statement)
        index_name = key[1] if key is not None else "<unknown>"
        raise ValueError(
            f"PostgreSQL index operator classes for index '{index_name}' "
            f"reference columns or expressions not present in CREATE INDEX SQL: {unknown}"
        )
    return f"{statement[: start + 1]}{', '.join(items)}{statement[end:]}"


def _postgres_index_ops_item_sql(
    item: str,
    ops: Mapping[str, str],
    seen: set[str],
) -> str:
    column_bounds = _leading_index_column_identifier_bounds(item)
    if column_bounds is not None:
        _start, end = column_bounds
        item_key = _normalize_sql_identifier_token(item[column_bounds[0] : end])
        opclass = ops.get(item_key)
        if opclass is not None:
            seen.add(item_key)
            return f"{item[:end]} {opclass}{item[end:]}"
    expression_key = item.strip()
    opclass = ops.get(expression_key)
    if opclass is None:
        return item
    seen.add(expression_key)
    return f"{item} {opclass}"


def _leading_index_column_identifier_bounds(value: str) -> tuple[int, int] | None:
    bounds = _leading_sql_identifier_bounds(value)
    if bounds is None:
        return None
    _start, end = bounds
    tail = value[end:].lstrip()
    if tail.startswith("("):
        return None
    return bounds


def _annotate_postgres_index_nulls_not_distinct_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_postgres_index_nulls_not_distinct_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_postgres_index_nulls_not_distinct_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    indexes = _postgres_nulls_not_distinct_indexes_by_key(tables)
    if not indexes:
        return list(statements)
    if dialect_name(dialect) != "postgresql":
        raise ValueError("PostgreSQL index NULLS NOT DISTINCT only supports PostgreSQL")
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        if key is None or key not in indexes:
            annotated.append(statement)
        else:
            annotated.append(_postgres_index_nulls_not_distinct_sql(statement))
    return annotated


def _postgres_nulls_not_distinct_indexes_by_key(
    tables: Sequence[TableSnapshot],
) -> set[tuple[str, str]]:
    indexes: set[tuple[str, str]] = set()
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if not index.postgres_nulls_not_distinct:
                continue
            if not index.unique:
                raise ValueError(
                    "PostgreSQL index NULLS NOT DISTINCT requires unique indexes"
                )
            for table_name in table_names:
                indexes.add((table_name, index.name))
    return indexes


def _postgres_index_nulls_not_distinct_sql(statement: str) -> str:
    upper = statement.upper()
    if not (
        upper.startswith("CREATE UNIQUE INDEX")
        or upper.startswith("CREATE UNIQUE INDEX IF NOT EXISTS")
    ):
        raise ValueError(
            "cannot apply PostgreSQL index NULLS NOT DISTINCT to non-unique "
            f"CREATE INDEX SQL: {statement}"
        )
    column_bounds = _index_column_list_bounds(statement)
    if column_bounds is None:
        raise ValueError(
            "cannot apply PostgreSQL index NULLS NOT DISTINCT to CREATE INDEX SQL: "
            f"{statement}"
        )
    _start, end = column_bounds
    insert_at = end + 1
    tail = statement[insert_at:]
    leading = len(tail) - len(tail.lstrip())
    include_at = insert_at + leading
    if statement[include_at:].upper().startswith("INCLUDE"):
        include_start = _find_sql_char(statement, "(", include_at)
        if include_start is None:
            raise ValueError(
                "cannot apply PostgreSQL index NULLS NOT DISTINCT after INCLUDE "
                f"clause in SQL: {statement}"
            )
        include_end = _find_matching_sql_paren(statement, include_start)
        if include_end is None:
            raise ValueError(
                "cannot apply PostgreSQL index NULLS NOT DISTINCT after INCLUDE "
                f"clause in SQL: {statement}"
            )
        insert_at = include_end + 1
    return f"{statement[:insert_at]} NULLS NOT DISTINCT{statement[insert_at:]}"


def _annotate_mysql_index_prefix_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_mysql_index_prefix_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_mysql_index_prefix_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    prefixes = _mysql_index_prefixes_by_key(tables)
    if not prefixes:
        return list(statements)
    normalized_dialect = dialect_name(dialect)
    if normalized_dialect not in _MYSQL_INDEX_OPTION_DIALECTS:
        raise ValueError("MySQL/MariaDB index prefixes only support MySQL and MariaDB")
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        prefix = prefixes.get(key) if key is not None else None
        if prefix is None:
            annotated.append(statement)
        else:
            annotated.append(_mysql_index_prefix_sql(statement, prefix))
    return annotated


def _mysql_index_prefixes_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], str]:
    prefixes: dict[tuple[str, str], str] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if index.mysql_prefix is None:
                continue
            for table_name in table_names:
                prefixes[(table_name, index.name)] = index.mysql_prefix
    return prefixes


def _mysql_index_prefix_sql(statement: str, mysql_prefix: str) -> str:
    key = _create_index_statement_key(statement)
    if key is None:
        raise ValueError(
            f"cannot apply MySQL/MariaDB index prefix to CREATE INDEX SQL: {statement}"
        )
    normalized = " ".join(statement.strip().split())
    prefix = _create_index_statement_prefix(normalized.upper())
    if prefix is None:
        raise ValueError(
            f"cannot apply MySQL/MariaDB index prefix to CREATE INDEX SQL: {statement}"
        )
    if prefix != "CREATE INDEX":
        raise ValueError(
            "MySQL/MariaDB index prefixes can only be applied to non-unique "
            f"regular CREATE INDEX SQL: {statement}"
        )
    return f"CREATE {mysql_prefix} INDEX{normalized[len(prefix) :]}"


def _annotate_mysql_index_using_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_mysql_index_using_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_mysql_index_using_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    methods = _mysql_index_using_by_key(tables)
    if not methods:
        return list(statements)
    normalized_dialect = dialect_name(dialect)
    if normalized_dialect not in _MYSQL_INDEX_OPTION_DIALECTS:
        raise ValueError(
            "MySQL/MariaDB index USING methods only support MySQL and MariaDB"
        )
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        method = methods.get(key) if key is not None else None
        if method is None:
            annotated.append(statement)
        else:
            annotated.append(_mysql_index_using_sql(statement, method))
    return annotated


def _mysql_index_using_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], str]:
    methods: dict[tuple[str, str], str] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if index.mysql_using is None:
                continue
            for table_name in table_names:
                methods[(table_name, index.name)] = index.mysql_using
    return methods


def _mysql_index_using_sql(statement: str, method: str) -> str:
    key = _create_index_statement_key(statement)
    if key is None:
        raise ValueError(
            "cannot apply MySQL/MariaDB index USING method to CREATE INDEX SQL: "
            f"{statement}"
        )
    normalized = " ".join(statement.strip().split())
    prefix = _create_index_statement_prefix(normalized.upper())
    if prefix is None:
        raise ValueError(
            "cannot apply MySQL/MariaDB index USING method to CREATE INDEX SQL: "
            f"{statement}"
        )
    if prefix in {"CREATE FULLTEXT INDEX", "CREATE SPATIAL INDEX"}:
        raise ValueError(
            "MySQL/MariaDB index prefixes cannot be combined with USING methods"
        )
    index_name = _sql_identifier_after(normalized, prefix)
    if index_name is None:
        raise ValueError(
            "cannot apply MySQL/MariaDB index USING method to CREATE INDEX SQL: "
            f"{statement}"
        )
    after_index_name = normalized[len(prefix) :].lstrip()
    index_token = after_index_name.split(" ", 1)[0]
    insert_at = normalized.find(index_token) + len(index_token)
    return f"{normalized[:insert_at]} USING {method}{normalized[insert_at:]}"


def _annotate_mysql_index_length_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_mysql_index_length_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_mysql_index_length_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    lengths = _mysql_index_lengths_by_key(tables)
    if not lengths:
        return list(statements)
    normalized_dialect = dialect_name(dialect)
    if normalized_dialect not in _MYSQL_INDEX_OPTION_DIALECTS:
        raise ValueError(
            "MySQL/MariaDB index prefix lengths only support MySQL and MariaDB"
        )
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        index_lengths = lengths.get(key) if key is not None else None
        if index_lengths is None:
            annotated.append(statement)
        else:
            annotated.append(_mysql_index_length_sql(statement, index_lengths))
    return annotated


def _mysql_index_lengths_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], dict[str, int]]:
    lengths: dict[tuple[str, str], dict[str, int]] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if not index.mysql_length:
                continue
            for table_name in table_names:
                lengths[(table_name, index.name)] = dict(index.mysql_length)
    return lengths


def _mysql_index_length_sql(statement: str, lengths: Mapping[str, int]) -> str:
    column_bounds = _index_column_list_bounds(statement)
    if column_bounds is None:
        raise ValueError(
            "cannot apply MySQL/MariaDB index prefix lengths to CREATE INDEX SQL: "
            f"{statement}"
        )
    start, end = column_bounds
    columns_sql = statement[start + 1 : end]
    seen: set[str] = set()
    items = [
        _mysql_index_column_length_item_sql(item, lengths, seen)
        for item in _split_top_level_sql_list(columns_sql)
    ]
    missing = sorted(set(lengths) - seen)
    if missing:
        unknown = ", ".join(missing)
        key = _create_index_statement_key(statement)
        index_name = key[1] if key is not None else "<unknown>"
        raise ValueError(
            f"MySQL/MariaDB index prefix lengths for index '{index_name}' "
            f"reference columns not present in CREATE INDEX SQL: {unknown}"
        )
    return f"{statement[: start + 1]}{', '.join(items)}{statement[end:]}"


def _annotate_mysql_index_visibility_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_mysql_index_visibility_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_mysql_index_visibility_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    visibility = _mysql_index_visibility_by_key(tables)
    if not visibility:
        return list(statements)
    if dialect_name(dialect) != "mysql":
        raise ValueError("MySQL index visibility only supports MySQL")
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        visible = visibility.get(key) if key is not None else None
        if visible is None:
            annotated.append(statement)
        else:
            annotated.append(_mysql_index_visibility_sql(statement, visible))
    return annotated


def _mysql_index_visibility_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], bool]:
    visibility: dict[tuple[str, str], bool] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if index.mysql_visible is None:
                continue
            for table_name in table_names:
                visibility[(table_name, index.name)] = index.mysql_visible
    return visibility


def _mysql_index_visibility_sql(statement: str, visible: bool) -> str:
    if _create_index_statement_key(statement) is None:
        raise ValueError(
            f"cannot apply MySQL index visibility to CREATE INDEX SQL: {statement}"
        )
    return f"{statement} {'VISIBLE' if visible else 'INVISIBLE'}"


def _index_column_list_bounds(statement: str) -> tuple[int, int] | None:
    on_marker = statement.upper().find(" ON ")
    if on_marker < 0:
        return None
    start = _find_sql_char(statement, "(", on_marker + 4)
    if start is None:
        return None
    end = _find_matching_sql_paren(statement, start)
    if end is None:
        return None
    return start, end


def _find_sql_char(statement: str, target: str, start: int) -> int | None:
    quote: str | None = None
    index = start
    while index < len(statement):
        char = statement[index]
        if quote is not None:
            if char == quote:
                if quote in {"'", "]"} and index + 1 < len(statement):
                    escaped = "'" if quote == "'" else "]"
                    if statement[index + 1] == escaped:
                        index += 2
                        continue
                quote = None
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue
        if char == "[":
            quote = "]"
            index += 1
            continue
        if char == target:
            return index
        index += 1
    return None


def _find_matching_sql_paren(statement: str, start: int) -> int | None:
    quote: str | None = None
    depth = 0
    index = start
    while index < len(statement):
        char = statement[index]
        if quote is not None:
            if char == quote:
                if quote in {"'", "]"} and index + 1 < len(statement):
                    escaped = "'" if quote == "'" else "]"
                    if statement[index + 1] == escaped:
                        index += 2
                        continue
                quote = None
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue
        if char == "[":
            quote = "]"
            index += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return None


def _split_top_level_sql_list(value: str) -> list[str]:
    items: list[str] = []
    quote: str | None = None
    depth = 0
    start = 0
    index = 0
    while index < len(value):
        char = value[index]
        if quote is not None:
            if char == quote:
                if quote in {"'", "]"} and index + 1 < len(value):
                    escaped = "'" if quote == "'" else "]"
                    if value[index + 1] == escaped:
                        index += 2
                        continue
                quote = None
            index += 1
            continue
        if char in {"'", '"', "`"}:
            quote = char
            index += 1
            continue
        if char == "[":
            quote = "]"
            index += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            items.append(value[start:index].strip())
            start = index + 1
        index += 1
    tail = value[start:].strip()
    if tail:
        items.append(tail)
    return items


def _mysql_index_column_length_item_sql(
    item: str,
    lengths: Mapping[str, int],
    seen: set[str],
) -> str:
    bounds = _leading_sql_identifier_bounds(item)
    if bounds is None:
        return item
    _start, end = bounds
    column_name = _normalize_sql_identifier_token(item[bounds[0] : end])
    length = lengths.get(column_name)
    if length is None:
        return item
    seen.add(column_name)
    return f"{item[:end]}({length}){item[end:]}"


def _leading_sql_identifier_bounds(value: str) -> tuple[int, int] | None:
    start = len(value) - len(value.lstrip())
    if start >= len(value):
        return None
    first = value[start]
    if first in {'"', "`"}:
        end = _quoted_identifier_end(value, start, first)
        return (start, end) if end is not None else None
    if first == "[":
        end = _quoted_identifier_end(value, start, "]")
        return (start, end) if end is not None else None
    if not _is_sql_identifier_start(first):
        return None
    end = start + 1
    while end < len(value) and _is_sql_identifier_char(value[end]):
        end += 1
    return start, end


def _quoted_identifier_end(value: str, start: int, quote: str) -> int | None:
    index = start + 1
    while index < len(value):
        if value[index] == quote:
            if index + 1 < len(value) and value[index + 1] == quote:
                index += 2
                continue
            return index + 1
        index += 1
    return None


def _is_sql_identifier_start(value: str) -> bool:
    return value.isalpha() or value in {"_", "$"}


def _is_sql_identifier_char(value: str) -> bool:
    return value.isalnum() or value in {"_", "$"}


def _annotate_inline_index_comment_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    if dialect not in _INLINE_INDEX_COMMENT_DIALECTS:
        return [dict(operation) for operation in operations]
    statements = [str(operation["sql"]) for operation in operations]
    annotated = _annotate_inline_index_comment_sql(dialect, statements, tables)
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, annotated, strict=True)
    ]


def _annotate_inline_index_comment_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    if dialect_name(dialect) not in _INLINE_INDEX_COMMENT_DIALECTS:
        return list(statements)
    comments = _inline_index_comments_by_key(tables)
    if not comments:
        return list(statements)
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        comment = comments.get(key) if key is not None else None
        if comment is None:
            annotated.append(statement)
        else:
            annotated.append(
                _append_inline_index_comment_sql(dialect, statement, comment)
            )
    return annotated


def _inline_index_comments_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], str]:
    comments: dict[tuple[str, str], str] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if index.comment is None:
                continue
            for table_name in table_names:
                comments[(table_name, index.name)] = index.comment
    return comments


def _create_index_statement_key(statement: str) -> tuple[str, str] | None:
    normalized = " ".join(statement.strip().split())
    upper = normalized.upper()
    prefix = _create_index_statement_prefix(upper)
    if prefix is None:
        return None
    index_name = _sql_identifier_after(normalized, prefix)
    table_name = _sql_identifier_after_keyword(normalized, " ON ")
    if table_name is None or index_name is None:
        return None
    return (table_name, index_name)


def _is_create_index_statement(upper: str) -> bool:
    return _create_index_statement_prefix(upper) is not None


def _create_index_statement_prefix(upper: str) -> str | None:
    for prefix in (
        "CREATE UNIQUE CLUSTERED INDEX",
        "CREATE UNIQUE NONCLUSTERED INDEX",
        "CREATE CLUSTERED INDEX",
        "CREATE NONCLUSTERED INDEX",
        "CREATE FULLTEXT INDEX",
        "CREATE SPATIAL INDEX",
        "CREATE UNIQUE INDEX",
        "CREATE INDEX",
    ):
        if upper.startswith(prefix):
            return prefix
    return None


def _append_inline_index_comment_sql(dialect: str, statement: str, comment: str) -> str:
    normalized_dialect = dialect_name(dialect)
    if normalized_dialect not in _INLINE_INDEX_COMMENT_DIALECTS:
        raise ValueError("inline index comments only support MySQL and MariaDB")
    return f"{statement} COMMENT {sql_literal(comment)}"


def _annotate_mssql_clustered_index_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_mssql_clustered_index_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_mssql_clustered_index_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    clustered = _mssql_clustered_indexes_by_key(tables)
    if not clustered:
        return list(statements)
    normalized_dialect = dialect_name(dialect)
    if normalized_dialect != "mssql":
        raise ValueError("SQL Server clustered indexes only support SQL Server")
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        if key is None or key not in clustered:
            annotated.append(statement)
        else:
            annotated.append(_mssql_clustered_index_sql(statement))
    return annotated


def _mssql_clustered_indexes_by_key(
    tables: Sequence[TableSnapshot],
) -> set[tuple[str, str]]:
    clustered: set[tuple[str, str]] = set()
    for table in tables:
        table_clustered = [index for index in table.indexes if index.mssql_clustered]
        if len(table_clustered) > 1:
            names = ", ".join(index.name for index in table_clustered)
            raise ValueError(
                f"table '{_table_label(table)}' has multiple SQL Server clustered "
                f"indexes: {names}"
            )
        table_names = {_table_label(table), table.name}
        for index in table_clustered:
            for table_name in table_names:
                clustered.add((table_name, index.name))
    return clustered


def _mssql_clustered_index_sql(statement: str) -> str:
    replacements = (
        ("CREATE UNIQUE INDEX", "CREATE UNIQUE CLUSTERED INDEX"),
        ("CREATE INDEX", "CREATE CLUSTERED INDEX"),
    )
    upper = statement.upper()
    for prefix, replacement in replacements:
        if upper.startswith(prefix):
            return f"{replacement}{statement[len(prefix) :]}"
    return statement


def _annotate_mssql_index_filegroup_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_mssql_index_filegroup_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_mssql_index_filegroup_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    filegroups = _mssql_index_filegroups_by_key(tables)
    if not filegroups:
        return list(statements)
    normalized_dialect = dialect_name(dialect)
    if normalized_dialect != "mssql":
        raise ValueError("SQL Server index filegroups only support SQL Server")
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        filegroup = filegroups.get(key) if key is not None else None
        if filegroup is None:
            annotated.append(statement)
        else:
            annotated.append(
                f"{statement} ON {quote_ident(normalized_dialect, filegroup)}"
            )
    return annotated


def _mssql_index_filegroups_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], str]:
    filegroups: dict[tuple[str, str], str] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if index.mssql_filegroup is None:
                continue
            for table_name in table_names:
                filegroups[(table_name, index.name)] = index.mssql_filegroup
    return filegroups


def _annotate_oracle_index_options_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    statements = _annotate_oracle_index_options_sql(
        dialect,
        [str(operation["sql"]) for operation in operations],
        tables,
    )
    return [
        {**dict(operation), "sql": sql}
        for operation, sql in zip(operations, statements, strict=True)
    ]


def _annotate_oracle_index_tablespace_operations(
    dialect: str,
    operations: Sequence[Mapping[str, Any]],
    tables: Sequence[TableSnapshot],
) -> list[dict[str, Any]]:
    return _annotate_oracle_index_options_operations(dialect, operations, tables)


def _annotate_oracle_index_options_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    options_by_key = _oracle_index_options_by_key(tables)
    if not options_by_key:
        return list(statements)
    normalized_dialect = dialect_name(dialect)
    if normalized_dialect != "oracle":
        raise ValueError(
            "Oracle index tablespaces, bitmap indexes, and compression only support Oracle"
        )
    annotated: list[str] = []
    for statement in statements:
        key = _create_index_statement_key(statement)
        options = options_by_key.get(key) if key is not None else None
        if options is None:
            annotated.append(statement)
        else:
            annotated.append(
                _append_oracle_index_options(normalized_dialect, statement, options)
            )
    return annotated


def _annotate_oracle_index_tablespace_sql(
    dialect: str,
    statements: Sequence[str],
    tables: Sequence[TableSnapshot],
) -> list[str]:
    return _annotate_oracle_index_options_sql(dialect, statements, tables)


def _oracle_index_options_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], dict[str, Any]]:
    options: dict[tuple[str, str], dict[str, Any]] = {}
    for table in tables:
        table_names = {_table_label(table), table.name}
        for index in table.indexes:
            if (
                index.oracle_tablespace is None
                and not index.oracle_bitmap
                and index.oracle_compress is None
            ):
                continue
            for table_name in table_names:
                options[(table_name, index.name)] = {
                    "bitmap": index.oracle_bitmap,
                    "compress": index.oracle_compress,
                    "tablespace": index.oracle_tablespace,
                }
    return options


def _oracle_index_tablespaces_by_key(
    tables: Sequence[TableSnapshot],
) -> dict[tuple[str, str], str]:
    return {
        key: options["tablespace"]
        for key, options in _oracle_index_options_by_key(tables).items()
        if options["tablespace"] is not None
    }


def _append_oracle_index_options(
    dialect: str,
    statement: str,
    options: Mapping[str, Any],
) -> str:
    sql = statement
    if options.get("bitmap"):
        upper = sql.upper()
        if upper.startswith("CREATE UNIQUE INDEX"):
            raise ValueError("Oracle bitmap indexes cannot be unique")
        if upper.startswith("CREATE INDEX"):
            sql = f"CREATE BITMAP INDEX{sql[len('CREATE INDEX') :]}"
    compress = options.get("compress")
    if compress is not None:
        sql = f"{sql} COMPRESS"
        if isinstance(compress, int) and not isinstance(compress, bool):
            sql = f"{sql} {compress}"
    tablespace = options.get("tablespace")
    if tablespace is not None:
        sql = f"{sql} TABLESPACE {quote_ident(dialect, str(tablespace))}"
    return sql


def _snapshot_without_python_metadata(snapshot: SchemaSnapshot) -> SchemaSnapshot:
    return replace(
        snapshot,
        tables=[
            replace(
                table,
                indexes=[
                    (
                        replace(
                            index,
                            comment=None,
                            postgres_tablespace=None,
                            postgres_ops={},
                            postgres_nulls_not_distinct=False,
                            mssql_filegroup=None,
                            oracle_tablespace=None,
                            oracle_bitmap=False,
                            oracle_compress=None,
                            mysql_prefix=None,
                            mysql_length={},
                            mysql_using=None,
                            mysql_visible=None,
                        )
                        if index.comment is not None
                        or index.postgres_tablespace is not None
                        or index.postgres_ops
                        or index.postgres_nulls_not_distinct
                        or index.mssql_filegroup is not None
                        or index.oracle_tablespace is not None
                        or index.oracle_bitmap
                        or index.oracle_compress is not None
                        or index.mysql_prefix is not None
                        or index.mysql_length
                        or index.mysql_using is not None
                        or index.mysql_visible is not None
                        else index
                    )
                    for index in table.indexes
                ],
                named_unique_constraints=[
                    _unique_constraint_without_python_metadata(constraint)
                    for constraint in table.named_unique_constraints
                ],
                check_constraints=[
                    (
                        replace(constraint, comment=None)
                        if constraint.comment is not None
                        else constraint
                    )
                    for constraint in table.check_constraints
                ],
                foreign_key_constraints=[
                    (
                        replace(constraint, comment=None)
                        if constraint.comment is not None
                        else constraint
                    )
                    for constraint in table.foreign_key_constraints
                ],
                exclusion_constraints=[
                    (
                        replace(constraint, comment=None)
                        if constraint.comment is not None
                        else constraint
                    )
                    for constraint in table.exclusion_constraints
                ],
            )
            for table in snapshot.tables
        ],
    )


def _unique_constraint_without_python_metadata(
    constraint: UniqueConstraintSnapshot,
) -> UniqueConstraintSnapshot:
    normalized_mssql_clustered = _mssql_unique_clustered_constraint_value(constraint)
    if (
        constraint.comment is None
        and not constraint.postgres_include
        and constraint.mssql_clustered == normalized_mssql_clustered
    ):
        return constraint
    return replace(
        constraint,
        comment=None,
        postgres_include=[],
        mssql_clustered=normalized_mssql_clustered,
    )


def _snapshots_with_normalized_matching_table_checks(
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> tuple[SchemaSnapshot, SchemaSnapshot]:
    from_tables = {_table_key(table): table for table in from_snapshot.tables}
    to_tables = {_table_key(table): table for table in to_snapshot.tables}
    from_replacements: dict[tuple[str | None, str], dict[str, str]] = {}
    to_replacements: dict[tuple[str | None, str], dict[str, str]] = {}

    for key, from_table in from_tables.items():
        to_table = to_tables.get(key)
        if to_table is None:
            continue
        from_checks = {check.name: check for check in from_table.check_constraints}
        to_checks = {check.name: check for check in to_table.check_constraints}
        for name, from_check in from_checks.items():
            to_check = to_checks.get(name)
            if to_check is None:
                continue
            normalized = _normalized_table_check_expression(from_check.expression)
            if normalized != _normalized_table_check_expression(to_check.expression):
                continue
            from_replacements.setdefault(key, {})[name] = normalized
            to_replacements.setdefault(key, {})[name] = normalized

    if not from_replacements and not to_replacements:
        return from_snapshot, to_snapshot
    return (
        _snapshot_with_table_check_expressions(from_snapshot, from_replacements),
        _snapshot_with_table_check_expressions(to_snapshot, to_replacements),
    )


def _snapshot_with_table_check_expressions(
    snapshot: SchemaSnapshot,
    replacements: Mapping[tuple[str | None, str], Mapping[str, str]],
) -> SchemaSnapshot:
    if not replacements:
        return snapshot
    tables: list[TableSnapshot] = []
    for table in snapshot.tables:
        table_replacements = replacements.get(_table_key(table))
        if not table_replacements:
            tables.append(table)
            continue
        tables.append(
            replace(
                table,
                check_constraints=[
                    (
                        replace(check, expression=table_replacements[check.name])
                        if check.name in table_replacements
                        else check
                    )
                    for check in table.check_constraints
                ],
            )
        )
    return replace(snapshot, tables=tables)


def _normalized_table_check_expression(expression: str) -> str:
    normalized = " ".join(expression.strip().split())
    normalized = _strip_wrapping_parentheses(normalized)
    if "'" in normalized or '"' in normalized:
        return normalized
    normalized = re.sub(
        r"\(\s*([A-Za-z_][A-Za-z0-9_$#]*)\s*\)::"
        r"[A-Za-z_][A-Za-z0-9_$#]*(?:\(\d+\))?",
        r"\1",
        normalized,
    )
    normalized = re.sub(r"\[([A-Za-z_][A-Za-z0-9_$#]*)\]", r"\1", normalized)
    normalized = re.sub(r"\(([-+]?\d+(?:\.\d+)?)\)", r"\1", normalized)
    normalized = _strip_wrapping_parentheses(normalized)
    normalized = re.sub(r"\s*(>=|<=|<>|!=|=|>|<)\s*", r" \1 ", normalized)
    normalized = re.sub(r"\s*,\s*", ", ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _strip_wrapping_parentheses(expression: str) -> str:
    normalized = expression.strip()
    while _has_wrapping_parentheses(normalized):
        normalized = normalized[1:-1].strip()
    return normalized


def _has_wrapping_parentheses(expression: str) -> bool:
    if len(expression) < 2 or expression[0] != "(" or expression[-1] != ")":
        return False
    depth = 0
    in_single_quote = False
    in_double_quote = False
    in_bracket_quote = False
    index = 0
    while index < len(expression):
        char = expression[index]
        if in_single_quote:
            if (
                char == "'"
                and index + 1 < len(expression)
                and expression[index + 1] == "'"
            ):
                index += 2
                continue
            if char == "'":
                in_single_quote = False
        elif in_double_quote:
            if char == '"':
                in_double_quote = False
        elif in_bracket_quote:
            if char == "]":
                in_bracket_quote = False
        elif char == "'":
            in_single_quote = True
        elif char == '"':
            in_double_quote = True
        elif char == "[":
            in_bracket_quote = True
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0 and index != len(expression) - 1:
                return False
            if depth < 0:
                return False
        index += 1
    return depth == 0 and not in_single_quote and not in_double_quote


def _compile_namespace_diff(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    before: list[dict[str, Any]] = []
    after: list[dict[str, Any]] = []
    from_namespaces = {
        _namespace_key(namespace): namespace for namespace in from_snapshot.namespaces
    }
    to_namespaces = {
        _namespace_key(namespace): namespace for namespace in to_snapshot.namespaces
    }
    changed = [
        key
        for key, namespace in from_namespaces.items()
        if key in to_namespaces and namespace.comment != to_namespaces[key].comment
    ]
    if not (set(from_namespaces) ^ set(to_namespaces) or changed):
        return before, after
    if dialect_name(dialect) not in _NAMESPACE_DIALECTS:
        raise ValueError(
            "namespace migrations only support PostgreSQL, MySQL, MariaDB, and SQL Server"
        )

    for key, namespace in to_namespaces.items():
        if key not in from_namespaces:
            before.append(_compiled_sql(_create_namespace_sql(dialect, namespace)))
            comment_sql = _set_namespace_comment_sql(
                dialect,
                namespace.name,
                namespace.comment,
                for_create=True,
            )
            if comment_sql is not None:
                before.append(_compiled_sql(comment_sql))

    for key in changed:
        comment_sql = _set_namespace_comment_sql(
            dialect,
            to_namespaces[key].name,
            to_namespaces[key].comment,
        )
        if comment_sql is not None:
            before.append(_compiled_sql(comment_sql))

    for key, namespace in from_namespaces.items():
        if key not in to_namespaces:
            after.append(_compiled_sql(_drop_namespace_sql(dialect, namespace)))
    return before, after


def _compile_enum_type_diff(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    before: list[dict[str, Any]] = []
    after: list[dict[str, Any]] = []
    from_enum_types = {
        _enum_type_key(enum_type): enum_type for enum_type in from_snapshot.enum_types
    }
    to_enum_types = {
        _enum_type_key(enum_type): enum_type for enum_type in to_snapshot.enum_types
    }
    recreated = [
        key
        for key, enum_type in from_enum_types.items()
        if key in to_enum_types
        and _enum_type_requires_recreate(enum_type, to_enum_types[key])
    ]
    comment_changed = [
        key
        for key, enum_type in from_enum_types.items()
        if key in to_enum_types
        and not _enum_type_requires_recreate(enum_type, to_enum_types[key])
        and enum_type.comment != to_enum_types[key].comment
    ]
    if not (set(from_enum_types) ^ set(to_enum_types) or recreated or comment_changed):
        return before, after
    if dialect_name(dialect) != "postgresql":
        raise ValueError("native enum type migrations only support PostgreSQL")

    for key, enum_type in to_enum_types.items():
        if key not in from_enum_types:
            before.append(_compiled_sql(_create_enum_type_sql(dialect, enum_type)))
            comment_sql = _set_enum_type_comment_sql(
                dialect,
                enum_type,
                for_create=True,
            )
            if comment_sql is not None:
                before.append(_compiled_sql(comment_sql))

    for key in recreated:
        after.append(_compiled_sql(_drop_enum_type_sql(dialect, from_enum_types[key])))
        after.append(_compiled_sql(_create_enum_type_sql(dialect, to_enum_types[key])))
        comment_sql = _set_enum_type_comment_sql(
            dialect,
            to_enum_types[key],
            for_create=True,
        )
        if comment_sql is not None:
            after.append(_compiled_sql(comment_sql))

    for key in comment_changed:
        comment_sql = _set_enum_type_comment_sql(dialect, to_enum_types[key])
        if comment_sql is not None:
            before.append(_compiled_sql(comment_sql))

    for key, enum_type in from_enum_types.items():
        if key not in to_enum_types:
            after.append(_compiled_sql(_drop_enum_type_sql(dialect, enum_type)))
    return before, after


def _compile_sequence_diff(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    before: list[dict[str, Any]] = []
    after: list[dict[str, Any]] = []
    from_sequences = {
        _sequence_key(sequence): sequence for sequence in from_snapshot.sequences
    }
    to_sequences = {
        _sequence_key(sequence): sequence for sequence in to_snapshot.sequences
    }
    recreated = [
        key
        for key, sequence in from_sequences.items()
        if key in to_sequences
        and _sequence_requires_recreate(
            sequence,
            to_sequences[key],
            dialect=dialect_name(dialect),
        )
    ]
    comment_changed = [
        key
        for key, sequence in from_sequences.items()
        if key in to_sequences
        and not _sequence_requires_recreate(
            sequence,
            to_sequences[key],
            dialect=dialect_name(dialect),
        )
        and sequence.comment != to_sequences[key].comment
    ]
    if not (set(from_sequences) ^ set(to_sequences) or recreated or comment_changed):
        return before, after
    if dialect_name(dialect) not in _SEQUENCE_DIALECTS:
        raise ValueError(
            "sequence migrations only support PostgreSQL, MariaDB, SQL Server, and Oracle"
        )

    for key, sequence in to_sequences.items():
        if key not in from_sequences:
            before.append(_compiled_sql(_create_sequence_sql(dialect, sequence)))
            comment_sql = _set_sequence_comment_sql(dialect, sequence, for_create=True)
            if comment_sql is not None:
                before.append(_compiled_sql(comment_sql))

    for key in recreated:
        after.append(_compiled_sql(_drop_sequence_sql(dialect, from_sequences[key])))
        after.append(_compiled_sql(_create_sequence_sql(dialect, to_sequences[key])))
        comment_sql = _set_sequence_comment_sql(
            dialect,
            to_sequences[key],
            for_create=True,
        )
        if comment_sql is not None:
            after.append(_compiled_sql(comment_sql))

    for key in comment_changed:
        comment_sql = _set_sequence_comment_sql(dialect, to_sequences[key])
        if comment_sql is not None:
            before.append(_compiled_sql(comment_sql))

    for key, sequence in from_sequences.items():
        if key not in to_sequences:
            after.append(_compiled_sql(_drop_sequence_sql(dialect, sequence)))
    return before, after


def _compile_index_metadata_diff(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> list[dict[str, Any]]:
    statements: list[dict[str, Any]] = []
    from_tables = {_table_key(table): table for table in from_snapshot.tables}
    to_tables = {_table_key(table): table for table in to_snapshot.tables}

    for key, table in to_tables.items():
        from_table = from_tables.get(key)
        from_indexes = (
            {index.name: index for index in from_table.indexes}
            if from_table is not None
            else {}
        )
        for index in table.indexes:
            old_index = from_indexes.get(index.name)
            if (
                old_index is not None
                and _indexes_equivalent(old_index, index)
                and old_index.comment == index.comment
                and old_index.postgres_tablespace == index.postgres_tablespace
                and old_index.mssql_filegroup == index.mssql_filegroup
                and old_index.mssql_clustered == index.mssql_clustered
                and old_index.oracle_tablespace == index.oracle_tablespace
                and old_index.oracle_bitmap == index.oracle_bitmap
                and old_index.oracle_compress == index.oracle_compress
                and old_index.postgres_ops == index.postgres_ops
                and old_index.postgres_nulls_not_distinct
                == index.postgres_nulls_not_distinct
                and old_index.mysql_prefix == index.mysql_prefix
                and old_index.mysql_length == index.mysql_length
                and old_index.mysql_using == index.mysql_using
                and _normalized_default_mysql_visible(old_index.mysql_visible)
                == _normalized_default_mysql_visible(index.mysql_visible)
            ):
                continue
            postgres_ops_changed = (
                old_index is not None
                and _indexes_equivalent(old_index, index)
                and old_index.postgres_ops != index.postgres_ops
            )
            if (
                dialect in _INLINE_INDEX_COMMENT_DIALECTS
                and old_index is not None
                and _indexes_equivalent(old_index, index)
                and (
                    old_index.comment != index.comment
                    or old_index.mysql_prefix != index.mysql_prefix
                    or old_index.mysql_length != index.mysql_length
                    or old_index.mysql_using != index.mysql_using
                    or _normalized_default_mysql_visible(old_index.mysql_visible)
                    != _normalized_default_mysql_visible(index.mysql_visible)
                )
            ):
                if from_table is None:
                    continue
                statements.extend(
                    _compile_index_recreate_sql(
                        dialect, from_table, table, old_index, index
                    )
                )
                continue
            if old_index is not None and not _indexes_equivalent(old_index, index):
                for_create = True
            else:
                for_create = old_index is None
            if (
                index.comment is None
                and index.postgres_tablespace is None
                and not index.postgres_ops
                and not index.postgres_nulls_not_distinct
                and index.mssql_filegroup is None
                and not index.mssql_clustered
                and index.oracle_tablespace is None
                and not index.oracle_bitmap
                and index.oracle_compress is None
                and index.mysql_prefix is None
                and not index.mysql_length
                and index.mysql_using is None
                and index.mysql_visible is None
                and for_create
            ):
                continue
            mssql_index_placement_changed = old_index is not None and (
                old_index.mssql_filegroup != index.mssql_filegroup
                or old_index.mssql_clustered != index.mssql_clustered
            )
            if (
                index.mssql_filegroup is not None
                or index.mssql_clustered
                or mssql_index_placement_changed
            ):
                if dialect_name(dialect) != "mssql":
                    raise ValueError(
                        "SQL Server index filegroups and clustered indexes only support SQL Server"
                    )
                if mssql_index_placement_changed and old_index is not None:
                    if from_table is None:
                        continue
                    statements.extend(
                        _compile_index_recreate_sql(
                            dialect,
                            from_table,
                            table,
                            old_index,
                            index,
                        )
                    )
                    if index.comment is not None or old_index.comment != index.comment:
                        comment_sql = _set_index_comment_sql(
                            dialect,
                            table,
                            index,
                            for_create=True,
                        )
                        if comment_sql is not None:
                            statements.append(_compiled_sql(comment_sql))
                    continue
            oracle_index_options_changed = old_index is not None and (
                old_index.oracle_tablespace != index.oracle_tablespace
                or old_index.oracle_bitmap != index.oracle_bitmap
                or old_index.oracle_compress != index.oracle_compress
            )
            if (
                index.oracle_tablespace is not None
                or index.oracle_bitmap
                or index.oracle_compress is not None
                or oracle_index_options_changed
            ):
                if dialect_name(dialect) != "oracle":
                    raise ValueError(
                        "Oracle index tablespaces, bitmap indexes, and compression "
                        "only support Oracle"
                    )
                if oracle_index_options_changed and old_index is not None:
                    if from_table is None:
                        continue
                    statements.extend(
                        _compile_index_recreate_sql(
                            dialect,
                            from_table,
                            table,
                            old_index,
                            index,
                        )
                    )
                    continue
            if index.postgres_ops or postgres_ops_changed:
                if dialect_name(dialect) != "postgresql":
                    raise ValueError(
                        "PostgreSQL index operator classes only support PostgreSQL"
                    )
                if postgres_ops_changed and old_index is not None:
                    if from_table is None:
                        continue
                    statements.extend(
                        _compile_index_recreate_sql(
                            dialect,
                            from_table,
                            table,
                            old_index,
                            index,
                        )
                    )
                    continue
            postgres_nulls_not_distinct_changed = (
                old_index is not None
                and old_index.postgres_nulls_not_distinct
                != index.postgres_nulls_not_distinct
            )
            if index.postgres_nulls_not_distinct or postgres_nulls_not_distinct_changed:
                if dialect_name(dialect) != "postgresql":
                    raise ValueError(
                        "PostgreSQL index NULLS NOT DISTINCT only supports PostgreSQL"
                    )
                if index.postgres_nulls_not_distinct and not index.unique:
                    raise ValueError(
                        "PostgreSQL index NULLS NOT DISTINCT requires unique indexes"
                    )
                if postgres_nulls_not_distinct_changed and old_index is not None:
                    if from_table is None:
                        continue
                    statements.extend(
                        _compile_index_recreate_sql(
                            dialect,
                            from_table,
                            table,
                            old_index,
                            index,
                        )
                    )
                    continue
            mysql_index_length_changed = (
                old_index is not None and old_index.mysql_length != index.mysql_length
            )
            mysql_index_prefix_changed = (
                old_index is not None and old_index.mysql_prefix != index.mysql_prefix
            )
            mysql_index_using_changed = (
                old_index is not None and old_index.mysql_using != index.mysql_using
            )
            mysql_index_visibility_changed = (
                old_index is not None
                and _normalized_default_mysql_visible(old_index.mysql_visible)
                != _normalized_default_mysql_visible(index.mysql_visible)
            )
            if index.mysql_visible is not None or mysql_index_visibility_changed:
                if dialect_name(dialect) != "mysql":
                    raise ValueError("MySQL index visibility only supports MySQL")
                if mysql_index_visibility_changed and old_index is not None:
                    if from_table is None:
                        continue
                    statements.extend(
                        _compile_index_recreate_sql(
                            dialect,
                            from_table,
                            table,
                            old_index,
                            index,
                        )
                    )
                    continue
            if (
                index.mysql_prefix is not None
                or mysql_index_prefix_changed
                or index.mysql_length
                or mysql_index_length_changed
                or index.mysql_using is not None
                or mysql_index_using_changed
            ):
                if dialect_name(dialect) not in _MYSQL_INDEX_OPTION_DIALECTS:
                    raise ValueError(
                        "MySQL/MariaDB index prefixes, prefix lengths, and USING methods only support MySQL and MariaDB"
                    )
                if (
                    mysql_index_prefix_changed
                    or mysql_index_length_changed
                    or mysql_index_using_changed
                ) and old_index is not None:
                    if from_table is None:
                        continue
                    statements.extend(
                        _compile_index_recreate_sql(
                            dialect,
                            from_table,
                            table,
                            old_index,
                            index,
                        )
                    )
                    continue
            if index.postgres_tablespace is not None or (
                not for_create
                and old_index is not None
                and old_index.postgres_tablespace != index.postgres_tablespace
            ):
                tablespace_sql = _set_index_tablespace_sql(
                    dialect,
                    table,
                    index,
                    for_create=for_create,
                )
                if tablespace_sql is not None:
                    statements.append(_compiled_sql(tablespace_sql))
            if index.comment is not None or (
                not for_create
                and old_index is not None
                and old_index.comment != index.comment
            ):
                if dialect in _INLINE_INDEX_COMMENT_DIALECTS:
                    continue
                comment_sql = _set_index_comment_sql(
                    dialect,
                    table,
                    index,
                    for_create=for_create,
                )
                if comment_sql is not None:
                    statements.append(_compiled_sql(comment_sql))
    return statements


def _compile_index_recreate_sql(
    dialect: str,
    from_table: TableSnapshot,
    to_table: TableSnapshot,
    old_index: IndexSnapshot,
    new_index: IndexSnapshot,
) -> list[dict[str, Any]]:
    drop_table = replace(from_table, indexes=[old_index])
    no_index_from = replace(from_table, indexes=[])
    no_index_to = replace(to_table, indexes=[])
    create_table = replace(to_table, indexes=[new_index])
    operations = [
        *_compile_schema_diff(
            dialect,
            SchemaSnapshot(tables=[drop_table]),
            SchemaSnapshot(tables=[no_index_from]),
        ),
        *_compile_schema_diff(
            dialect,
            SchemaSnapshot(tables=[no_index_to]),
            SchemaSnapshot(tables=[create_table]),
        ),
    ]
    if dialect_name(dialect) == "postgresql":
        operations.extend(
            _compile_index_metadata_diff(
                dialect,
                SchemaSnapshot(tables=[no_index_to]),
                SchemaSnapshot(tables=[create_table]),
            )
        )
    return operations


def _compile_unique_constraint_metadata_diff(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> list[dict[str, Any]]:
    statements: list[dict[str, Any]] = []
    from_tables = {_table_key(table): table for table in from_snapshot.tables}
    to_tables = {_table_key(table): table for table in to_snapshot.tables}

    for key, table in to_tables.items():
        from_table = from_tables.get(key)
        from_constraints = (
            {
                constraint.name: constraint
                for constraint in from_table.named_unique_constraints
            }
            if from_table is not None
            else {}
        )
        for constraint in table.named_unique_constraints:
            old_constraint = from_constraints.get(constraint.name)
            include_changed = (
                old_constraint is not None
                and _unique_constraints_equivalent_without_postgres_include(
                    old_constraint,
                    constraint,
                )
                and old_constraint.postgres_include != constraint.postgres_include
            )
            if constraint.postgres_include or include_changed:
                if dialect_name(dialect) != "postgresql":
                    raise ValueError(
                        "PostgreSQL unique constraint INCLUDE columns only support PostgreSQL"
                    )
                if include_changed and old_constraint is not None:
                    if from_table is None:
                        continue
                    statements.extend(
                        _compile_unique_constraint_recreate_sql(
                            dialect,
                            from_table,
                            table,
                            old_constraint,
                            constraint,
                        )
                    )
    return statements


def _unique_constraints_equivalent_without_postgres_include(
    before: UniqueConstraintSnapshot,
    after: UniqueConstraintSnapshot,
) -> bool:
    return replace(before, postgres_include=[]) == replace(
        after,
        postgres_include=[],
    )


def _compile_unique_constraint_recreate_sql(
    dialect: str,
    from_table: TableSnapshot,
    to_table: TableSnapshot,
    old_constraint: UniqueConstraintSnapshot,
    new_constraint: UniqueConstraintSnapshot,
) -> list[dict[str, Any]]:
    drop_table = replace(from_table, named_unique_constraints=[old_constraint])
    no_unique_from = replace(from_table, named_unique_constraints=[])
    no_unique_to = replace(to_table, named_unique_constraints=[])
    create_table = replace(to_table, named_unique_constraints=[new_constraint])
    return [
        *_compile_schema_diff(
            dialect,
            SchemaSnapshot(tables=[drop_table]),
            SchemaSnapshot(tables=[no_unique_from]),
        ),
        *_compile_schema_diff(
            dialect,
            SchemaSnapshot(tables=[no_unique_to]),
            SchemaSnapshot(tables=[create_table]),
        ),
    ]


def _compile_constraint_comment_diff(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> list[dict[str, Any]]:
    statements: list[dict[str, Any]] = []
    from_tables = {_table_key(table): table for table in from_snapshot.tables}
    to_tables = {_table_key(table): table for table in to_snapshot.tables}

    for key, table in to_tables.items():
        from_table = from_tables.get(key)
        from_constraints = (
            _explicit_constraint_comments(from_table) if from_table is not None else {}
        )
        for name, constraint_metadata in _explicit_constraint_comments(table).items():
            old_constraint = from_constraints.get(name)
            old_comment = old_constraint[1] if old_constraint is not None else None
            new_comment = constraint_metadata[1]
            if (
                old_constraint is not None
                and _explicit_constraint_definition(table, name)
                == _explicit_constraint_definition(from_table, name)
                and old_comment == new_comment
            ):
                continue
            for_create = old_constraint is None or (
                from_table is not None
                and _explicit_constraint_definition(table, name)
                != _explicit_constraint_definition(from_table, name)
            )
            if new_comment is None and for_create:
                continue
            comment_sql = _set_constraint_comment_sql(
                dialect,
                table,
                name,
                new_comment,
                for_create=for_create,
            )
            if comment_sql is not None:
                statements.append(_compiled_sql(comment_sql))
    return statements


def _explicit_constraint_comments(
    table: TableSnapshot,
) -> dict[str, tuple[str, str | None]]:
    return {
        name: (payload["kind"], payload.get("comment"))
        for name, payload in _explicit_constraints(table).items()
    }


def _explicit_constraint_definition(
    table: TableSnapshot | None,
    name: str,
) -> dict[str, Any] | None:
    if table is None:
        return None
    constraint = _explicit_constraints(table).get(name)
    if constraint is None:
        return None
    payload = dict(constraint)
    payload.pop("comment", None)
    return payload


def _explicit_constraints(table: TableSnapshot) -> dict[str, dict[str, Any]]:
    constraints: dict[str, dict[str, Any]] = {}
    for constraint in table.named_unique_constraints:
        constraints[constraint.name] = {
            "kind": "unique",
            "columns": list(constraint.columns),
            "postgres_include": list(constraint.postgres_include),
            "deferrable": constraint.deferrable,
            "initially_deferred": constraint.initially_deferred,
            "nulls_not_distinct": constraint.nulls_not_distinct,
            "sqlite_on_conflict": constraint.sqlite_on_conflict,
            "mssql_filegroup": constraint.mssql_filegroup,
            "mssql_clustered": _mssql_unique_clustered_constraint_value(constraint),
            "oracle_tablespace": constraint.oracle_tablespace,
            "oracle_compress": constraint.oracle_compress,
            "comment": constraint.comment,
        }
    for table_check in table.check_constraints:
        constraints[table_check.name] = {
            "kind": "check",
            "expression": _normalized_table_check_expression(table_check.expression),
            "validated": table_check.validated,
            "no_inherit": table_check.no_inherit,
            "comment": table_check.comment,
        }
    for foreign_key in table.foreign_key_constraints:
        constraints[foreign_key.name] = {
            "kind": "foreign_key",
            "columns": list(foreign_key.columns),
            "foreign_table": foreign_key.foreign_table,
            "foreign_columns": list(foreign_key.foreign_columns),
            "on_delete": foreign_key.on_delete,
            "on_update": foreign_key.on_update,
            "deferrable": foreign_key.deferrable,
            "initially_deferred": foreign_key.initially_deferred,
            "validated": foreign_key.validated,
            "match": foreign_key.match,
            "comment": foreign_key.comment,
        }
    for exclusion in table.exclusion_constraints:
        constraints[exclusion.name] = {
            "kind": "exclusion",
            "columns": [list(element) for element in exclusion.columns],
            "expressions": [list(element) for element in exclusion.expressions],
            "ops": dict(exclusion.ops),
            "using": exclusion.using,
            "where": exclusion.where,
            "deferrable": exclusion.deferrable,
            "initially_deferred": exclusion.initially_deferred,
            "comment": exclusion.comment,
        }
    return constraints


def _compile_view_diff(
    dialect: str,
    from_snapshot: SchemaSnapshot,
    to_snapshot: SchemaSnapshot,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    before: list[dict[str, Any]] = []
    after: list[dict[str, Any]] = []
    from_views = {_view_key(view): view for view in from_snapshot.views}
    to_views = {_view_key(view): view for view in to_snapshot.views}
    recreated = [
        key
        for key, view in from_views.items()
        if key in to_views
        and _view_requires_recreate(view, to_views[key], dialect=dialect)
    ]
    comment_changed = [
        key
        for key, view in from_views.items()
        if key in to_views
        and not _view_requires_recreate(view, to_views[key], dialect=dialect)
        and view.comment != to_views[key].comment
    ]
    if not (set(from_views) ^ set(to_views) or recreated or comment_changed):
        return before, after
    if dialect_name(dialect) not in _VIEW_DIALECTS:
        raise ValueError(
            "view migrations only support SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle"
        )

    for key in recreated:
        before.append(_compiled_sql(_drop_view_sql(dialect, from_views[key])))

    for key, view in from_views.items():
        if key not in to_views:
            before.append(_compiled_sql(_drop_view_sql(dialect, view)))

    for key, view in to_views.items():
        if key not in from_views:
            after.append(_compiled_sql(_create_view_sql(dialect, view)))
            comment_operation = _compiled_view_comment_sql(
                dialect, view, for_create=True
            )
            if comment_operation is not None:
                after.append(comment_operation)

    for key in recreated:
        after.append(_compiled_sql(_create_view_sql(dialect, to_views[key])))
        comment_operation = _compiled_view_comment_sql(
            dialect, to_views[key], for_create=True
        )
        if comment_operation is not None:
            after.append(comment_operation)

    for key in comment_changed:
        comment_operation = _compiled_view_comment_sql(dialect, to_views[key])
        if comment_operation is not None:
            before.append(comment_operation)
    return before, after


def _create_enum_type_sql(dialect: str, enum_type: EnumTypeSnapshot) -> str:
    values = ", ".join(sql_literal(value) for value in enum_type.values)
    return f"CREATE TYPE {_quote_enum_type_name(dialect, enum_type)} AS ENUM ({values})"


def _drop_enum_type_sql(dialect: str, enum_type: EnumTypeSnapshot) -> str:
    return f"DROP TYPE IF EXISTS {_quote_enum_type_name(dialect, enum_type)}"


def _set_enum_type_comment_sql(
    dialect: str,
    enum_type: EnumTypeSnapshot,
    *,
    for_create: bool = False,
) -> str | None:
    if enum_type.comment is None and for_create:
        return None
    if dialect_name(dialect) != "postgresql":
        raise ValueError("native enum type comments only support PostgreSQL")
    literal = (
        sql_literal(enum_type.comment) if enum_type.comment is not None else "NULL"
    )
    return f"COMMENT ON TYPE {_quote_enum_type_name(dialect, enum_type)} IS {literal}"


def _set_index_comment_sql(
    dialect: str,
    table: TableSnapshot,
    index: IndexSnapshot,
    *,
    for_create: bool = False,
) -> str | None:
    if index.comment is None and for_create:
        return None
    normalized = dialect_name(dialect)
    if normalized == "postgresql":
        literal = sql_literal(index.comment) if index.comment is not None else "NULL"
        return (
            f"COMMENT ON INDEX {_quote_index_name(dialect, table, index)} IS {literal}"
        )
    if normalized == "mssql":
        return _mssql_index_comment_sql(table, index)
    raise ValueError("standalone index comments only support PostgreSQL and SQL Server")


def _set_index_tablespace_sql(
    dialect: str,
    table: TableSnapshot,
    index: IndexSnapshot,
    *,
    for_create: bool = False,
) -> str | None:
    if index.postgres_tablespace is None and for_create:
        return None
    if dialect_name(dialect) != "postgresql":
        raise ValueError("index tablespaces only support PostgreSQL")
    tablespace = index.postgres_tablespace or "pg_default"
    return (
        f"ALTER INDEX {_quote_index_name(dialect, table, index)} "
        f"SET TABLESPACE {quote_ident(dialect, tablespace)}"
    )


def _set_mysql_table_tablespace_sql(dialect: str, table: TableSnapshot) -> str:
    normalized = dialect_name(dialect)
    if normalized not in {"mysql", "mariadb"}:
        raise ValueError("table tablespaces only support MySQL and MariaDB")
    tablespace = table.tablespace or "innodb_file_per_table"
    return (
        f"ALTER TABLE {_quote_table_name(dialect, table)} "
        f"TABLESPACE {quote_ident(dialect, tablespace)}"
    )


def _set_constraint_comment_sql(
    dialect: str,
    table: TableSnapshot,
    constraint_name: str,
    comment: str | None,
    *,
    for_create: bool = False,
) -> str | None:
    if comment is None and for_create:
        return None
    normalized = dialect_name(dialect)
    if normalized == "postgresql":
        literal = sql_literal(comment) if comment is not None else "NULL"
        return (
            f"COMMENT ON CONSTRAINT {quote_ident(dialect, constraint_name)} "
            f"ON {_quote_table_name(dialect, table)} IS {literal}"
        )
    if normalized == "mssql":
        return _mssql_constraint_comment_sql(table, constraint_name, comment)
    raise ValueError("constraint comments only support PostgreSQL and SQL Server")


def _mssql_constraint_comment_sql(
    table: TableSnapshot,
    constraint_name: str,
    comment: str | None,
) -> str:
    schema_value = (
        _mssql_unicode_literal(table.schema)
        if table.schema is not None
        else "SCHEMA_NAME()"
    )
    table_literal = _mssql_unicode_literal(table.name)
    constraint_literal = _mssql_unicode_literal(constraint_name)
    exists_predicate = (
        "EXISTS (SELECT 1 FROM sys.extended_properties ep "
        "JOIN sys.objects constraint_ref ON ep.major_id = constraint_ref.object_id "
        "JOIN sys.tables t ON constraint_ref.parent_object_id = t.object_id "
        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE ep.class = 1 AND ep.minor_id = 0 "
        "AND ep.name = N'MS_Description' "
        f"AND s.name = @schema AND t.name = {table_literal} "
        f"AND constraint_ref.name = {constraint_literal})"
    )
    level_args = (
        "@level0type = N'SCHEMA', @level0name = @schema, "
        f"@level1type = N'TABLE', @level1name = {table_literal}, "
        f"@level2type = N'CONSTRAINT', @level2name = {constraint_literal}"
    )
    if comment is None:
        return (
            f"DECLARE @schema sysname = {schema_value}; "
            f"IF {exists_predicate} "
            "EXEC sys.sp_dropextendedproperty @name = N'MS_Description', "
            f"{level_args}"
        )
    comment_literal = _mssql_unicode_literal(comment)
    return (
        f"DECLARE @schema sysname = {schema_value}; "
        f"IF {exists_predicate} "
        "EXEC sys.sp_updateextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}; "
        "ELSE EXEC sys.sp_addextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}"
    )


def _quote_table_name(dialect: str, table: TableSnapshot) -> str:
    if table.schema is None:
        return quote_ident(dialect, table.name)
    return f"{quote_ident(dialect, table.schema)}.{quote_ident(dialect, table.name)}"


def _quote_index_name(dialect: str, table: TableSnapshot, index: IndexSnapshot) -> str:
    if table.schema is None:
        return quote_ident(dialect, index.name)
    return f"{quote_ident(dialect, table.schema)}.{quote_ident(dialect, index.name)}"


def _quote_enum_type_name(dialect: str, enum_type: EnumTypeSnapshot) -> str:
    if enum_type.schema is None:
        return quote_ident(dialect, enum_type.name)
    return (
        f"{quote_ident(dialect, enum_type.schema)}."
        f"{quote_ident(dialect, enum_type.name)}"
    )


def _enum_type_key(enum_type: EnumTypeSnapshot) -> tuple[str | None, str]:
    return (enum_type.schema, enum_type.name)


def _enum_type_qualified_name(enum_type: EnumTypeSnapshot) -> str:
    if enum_type.schema is None:
        return enum_type.name
    return f"{enum_type.schema}.{enum_type.name}"


def _enum_type_requires_recreate(
    before: EnumTypeSnapshot,
    after: EnumTypeSnapshot,
) -> bool:
    return (
        before.name != after.name
        or before.schema != after.schema
        or before.values != after.values
    )


_NAMESPACE_DIALECTS = {"postgresql", "mysql", "mariadb", "mssql"}
_SEQUENCE_DIALECTS = {"postgresql", "mariadb", "mssql", "oracle"}
_VIEW_DIALECTS = {"sqlite", "postgresql", "mysql", "mariadb", "mssql", "oracle"}
_MATERIALIZED_VIEW_DIALECTS = {"postgresql", "oracle"}
_INLINE_INDEX_COMMENT_DIALECTS = {"mysql", "mariadb"}
_MYSQL_INDEX_OPTION_DIALECTS = {"mysql", "mariadb"}


def _create_namespace_sql(dialect: str, namespace: NamespaceSnapshot) -> str:
    normalized = dialect_name(dialect)
    if normalized not in _NAMESPACE_DIALECTS:
        raise ValueError(
            "namespace migrations only support PostgreSQL, MySQL, MariaDB, and SQL Server"
        )
    if normalized == "mssql":
        create_sql = f"CREATE SCHEMA {quote_ident(dialect, namespace.name)}"
        return (
            f"IF SCHEMA_ID({_mssql_unicode_literal(namespace.name)}) IS NULL "
            f"EXEC({_mssql_unicode_literal(create_sql)})"
        )
    return f"CREATE SCHEMA IF NOT EXISTS {quote_ident(dialect, namespace.name)}"


def _drop_namespace_sql(dialect: str, namespace: NamespaceSnapshot) -> str:
    normalized = dialect_name(dialect)
    if normalized not in _NAMESPACE_DIALECTS:
        raise ValueError(
            "namespace migrations only support PostgreSQL, MySQL, MariaDB, and SQL Server"
        )
    return f"DROP SCHEMA IF EXISTS {quote_ident(dialect, namespace.name)}"


def _set_namespace_comment_sql(
    dialect: str,
    name: str,
    comment: str | None,
    *,
    for_create: bool = False,
) -> str | None:
    if comment is None and for_create:
        return None
    normalized = dialect_name(dialect)
    if normalized == "postgresql":
        literal = sql_literal(comment) if comment is not None else "NULL"
        return f"COMMENT ON SCHEMA {quote_ident(dialect, name)} IS {literal}"
    if normalized == "mssql":
        return _mssql_namespace_comment_sql(name, comment)
    raise ValueError("namespace comments only support PostgreSQL and SQL Server")


def _mssql_unicode_literal(value: str) -> str:
    return f"N{sql_literal(value)}"


def _mssql_namespace_comment_sql(name: str, comment: str | None) -> str:
    schema_literal = _mssql_unicode_literal(name)
    exists_predicate = (
        "EXISTS (SELECT 1 FROM sys.extended_properties ep "
        "JOIN sys.schemas s ON ep.major_id = s.schema_id "
        "WHERE ep.class = 3 AND ep.minor_id = 0 "
        "AND ep.name = N'MS_Description' "
        f"AND s.name = {schema_literal})"
    )
    level_args = f"@level0type = N'SCHEMA', @level0name = {schema_literal}"
    if comment is None:
        return (
            f"IF {exists_predicate} "
            "EXEC sys.sp_dropextendedproperty @name = N'MS_Description', "
            f"{level_args}"
        )
    comment_literal = _mssql_unicode_literal(comment)
    return (
        f"IF {exists_predicate} "
        "EXEC sys.sp_updateextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}; "
        "ELSE EXEC sys.sp_addextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}"
    )


def _mssql_index_comment_sql(table: TableSnapshot, index: IndexSnapshot) -> str:
    schema_value = (
        _mssql_unicode_literal(table.schema)
        if table.schema is not None
        else "SCHEMA_NAME()"
    )
    table_literal = _mssql_unicode_literal(table.name)
    index_literal = _mssql_unicode_literal(index.name)
    exists_predicate = (
        "EXISTS (SELECT 1 FROM sys.extended_properties ep "
        "JOIN sys.indexes i ON ep.major_id = i.object_id "
        "AND ep.minor_id = i.index_id "
        "JOIN sys.tables t ON i.object_id = t.object_id "
        "JOIN sys.schemas s ON t.schema_id = s.schema_id "
        "WHERE ep.class = 7 AND ep.name = N'MS_Description' "
        f"AND s.name = @schema AND t.name = {table_literal} "
        f"AND i.name = {index_literal})"
    )
    level_args = (
        "@level0type = N'SCHEMA', @level0name = @schema, "
        f"@level1type = N'TABLE', @level1name = {table_literal}, "
        f"@level2type = N'INDEX', @level2name = {index_literal}"
    )
    if index.comment is None:
        return (
            f"DECLARE @schema sysname = {schema_value}; "
            f"IF {exists_predicate} "
            "EXEC sys.sp_dropextendedproperty @name = N'MS_Description', "
            f"{level_args}"
        )
    comment_literal = _mssql_unicode_literal(index.comment)
    return (
        f"DECLARE @schema sysname = {schema_value}; "
        f"IF {exists_predicate} "
        "EXEC sys.sp_updateextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}; "
        "ELSE EXEC sys.sp_addextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}"
    )


def _namespace_key(namespace: NamespaceSnapshot) -> str:
    return namespace.name


def _create_sequence_sql(dialect: str, sequence: SequenceSnapshot) -> str:
    normalized = dialect_name(dialect)
    if normalized not in _SEQUENCE_DIALECTS:
        raise ValueError(
            "sequence migrations only support PostgreSQL, MariaDB, SQL Server, and Oracle"
        )
    data_type_clause = _sequence_data_type_clause(normalized, sequence)
    options: list[str] = []
    if sequence.start is not None:
        options.append(f"START WITH {sequence.start}")
    if sequence.increment is not None:
        options.append(f"INCREMENT BY {sequence.increment}")
    if sequence.no_min_value and sequence.min_value is not None:
        raise ValueError("no_min_value cannot be combined with min_value")
    if sequence.no_max_value and sequence.max_value is not None:
        raise ValueError("no_max_value cannot be combined with max_value")
    if sequence.no_min_value:
        options.append(_sequence_no_min_value_clause(normalized))
    elif sequence.min_value is not None:
        options.append(f"MINVALUE {sequence.min_value}")
    if sequence.no_max_value:
        options.append(_sequence_no_max_value_clause(normalized))
    elif sequence.max_value is not None:
        options.append(f"MAXVALUE {sequence.max_value}")
    if sequence.cycle:
        options.append("CYCLE")
    if sequence.cache is not None:
        options.append(f"CACHE {sequence.cache}")
    if sequence.order:
        options.append(_sequence_order_clause(normalized))
    suffix = f" {' '.join(options)}" if options else ""
    exists = " IF NOT EXISTS" if normalized in {"postgresql", "mariadb"} else ""
    return (
        f"CREATE SEQUENCE{exists} {_quote_sequence_name(dialect, sequence)}"
        f"{data_type_clause}{suffix}"
    )


_SEQUENCE_DATA_TYPE_PATTERN = re.compile(
    r"[a-z_][a-z0-9_$]*(?:\.[a-z_][a-z0-9_$]*)?"
    r"(?:\([0-9]{1,3}(?:, 0)?\))?"
    r"(?: (?:signed|unsigned))?"
)
_POSTGRES_SEQUENCE_DATA_TYPES = {"smallint", "integer", "bigint"}
_MARIADB_SEQUENCE_DATA_TYPES = {
    "tinyint",
    "smallint",
    "mediumint",
    "int",
    "integer",
    "bigint",
}


def _sequence_data_type_clause(
    normalized_dialect: str,
    sequence: SequenceSnapshot,
) -> str:
    data_type = _normalized_sequence_data_type(sequence)
    if data_type is None:
        return ""
    if normalized_dialect == "oracle":
        raise ValueError(
            "sequence data types only support PostgreSQL, MariaDB, and SQL Server"
        )
    if (
        normalized_dialect == "postgresql"
        and data_type not in _POSTGRES_SEQUENCE_DATA_TYPES
    ):
        raise ValueError(
            "PostgreSQL sequence data_type must be smallint, integer, or bigint"
        )
    if normalized_dialect == "mariadb":
        parts = data_type.split()
        base_type = parts[0]
        if (
            base_type not in _MARIADB_SEQUENCE_DATA_TYPES
            or "(" in base_type
            or len(parts) > 2
        ):
            raise ValueError(
                "MariaDB sequence data_type must be an integer type, optionally signed or unsigned"
            )
    if normalized_dialect == "mssql" and data_type.endswith((" signed", " unsigned")):
        raise ValueError(
            "SQL Server sequence data_type cannot include signed or unsigned"
        )
    return f" AS {data_type}"


def _sequence_order_clause(normalized_dialect: str) -> str:
    if normalized_dialect != "oracle":
        raise ValueError("sequence ordering only supports Oracle")
    return "ORDER"


def _sequence_no_min_value_clause(normalized_dialect: str) -> str:
    if normalized_dialect == "oracle":
        return "NOMINVALUE"
    return "NO MINVALUE"


def _sequence_no_max_value_clause(normalized_dialect: str) -> str:
    if normalized_dialect == "oracle":
        return "NOMAXVALUE"
    return "NO MAXVALUE"


def _normalized_sequence_data_type(sequence: SequenceSnapshot) -> str | None:
    data_type = optional_str(sequence.data_type)
    if data_type is None:
        return None
    normalized = " ".join(data_type.strip().lower().split())
    normalized = re.sub(r"\s*,\s*", ", ", normalized)
    if not normalized:
        raise ValueError(
            f"sequence {_sequence_qualified_name(sequence)} data_type cannot be empty"
        )
    if _SEQUENCE_DATA_TYPE_PATTERN.fullmatch(normalized) is None:
        raise ValueError(
            f"sequence {_sequence_qualified_name(sequence)} data_type must be a safe SQL type"
        )
    return normalized


def _drop_sequence_sql(dialect: str, sequence: SequenceSnapshot) -> str:
    normalized = dialect_name(dialect)
    if normalized not in _SEQUENCE_DIALECTS:
        raise ValueError(
            "sequence migrations only support PostgreSQL, MariaDB, SQL Server, and Oracle"
        )
    exists = "" if normalized == "oracle" else " IF EXISTS"
    return f"DROP SEQUENCE{exists} {_quote_sequence_name(dialect, sequence)}"


def _set_sequence_comment_sql(
    dialect: str,
    sequence: SequenceSnapshot,
    *,
    for_create: bool = False,
) -> str | None:
    if sequence.comment is None and for_create:
        return None
    normalized = dialect_name(dialect)
    if normalized == "postgresql":
        literal = (
            sql_literal(sequence.comment) if sequence.comment is not None else "NULL"
        )
        return f"COMMENT ON SEQUENCE {_quote_sequence_name(dialect, sequence)} IS {literal}"
    if normalized == "mssql":
        return _mssql_sequence_comment_sql(sequence)
    raise ValueError("sequence comments only support PostgreSQL and SQL Server")


def _mssql_sequence_comment_sql(sequence: SequenceSnapshot) -> str:
    schema_value = (
        _mssql_unicode_literal(sequence.schema)
        if sequence.schema is not None
        else "SCHEMA_NAME()"
    )
    sequence_literal = _mssql_unicode_literal(sequence.name)
    exists_predicate = (
        "EXISTS (SELECT 1 FROM sys.extended_properties ep "
        "JOIN sys.sequences seq ON ep.major_id = seq.object_id "
        "JOIN sys.schemas s ON seq.schema_id = s.schema_id "
        "WHERE ep.class = 1 AND ep.minor_id = 0 "
        "AND ep.name = N'MS_Description' "
        f"AND s.name = @schema AND seq.name = {sequence_literal})"
    )
    level_args = (
        "@level0type = N'SCHEMA', @level0name = @schema, "
        f"@level1type = N'SEQUENCE', @level1name = {sequence_literal}"
    )
    if sequence.comment is None:
        return (
            f"DECLARE @schema sysname = {schema_value}; "
            f"IF {exists_predicate} "
            "EXEC sys.sp_dropextendedproperty @name = N'MS_Description', "
            f"{level_args}"
        )
    comment_literal = _mssql_unicode_literal(sequence.comment)
    return (
        f"DECLARE @schema sysname = {schema_value}; "
        f"IF {exists_predicate} "
        "EXEC sys.sp_updateextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}; "
        "ELSE EXEC sys.sp_addextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}"
    )


def _quote_sequence_name(dialect: str, sequence: SequenceSnapshot) -> str:
    if sequence.schema is None:
        return quote_ident(dialect, sequence.name)
    return (
        f"{quote_ident(dialect, sequence.schema)}.{quote_ident(dialect, sequence.name)}"
    )


def _sequence_key(sequence: SequenceSnapshot) -> tuple[str | None, str]:
    return (sequence.schema, sequence.name)


def _sequence_qualified_name(sequence: SequenceSnapshot) -> str:
    if sequence.schema is None:
        return sequence.name
    return f"{sequence.schema}.{sequence.name}"


def _sequence_requires_recreate(
    before: SequenceSnapshot,
    after: SequenceSnapshot,
    *,
    dialect: str | None = None,
) -> bool:
    return (
        before.name != after.name
        or before.schema != after.schema
        or before.start != after.start
        or before.increment != after.increment
        or _sequence_bound_requires_recreate(
            before,
            after,
            "min_value",
            "no_min_value",
            dialect=dialect,
        )
        or _sequence_bound_requires_recreate(
            before,
            after,
            "max_value",
            "no_max_value",
            dialect=dialect,
        )
        or before.cycle != after.cycle
        or before.cache != after.cache
        or before.data_type != after.data_type
        or before.order != after.order
    )


def _sequence_bound_requires_recreate(
    before: SequenceSnapshot,
    after: SequenceSnapshot,
    value_field: str,
    no_value_field: str,
    *,
    dialect: str | None = None,
) -> bool:
    before_value = getattr(before, value_field)
    after_value = getattr(after, value_field)
    if before_value != after_value:
        if (
            _sequence_bound_matches_default(
                before,
                value_field,
                before_value,
                dialect=dialect,
            )
            and after_value is None
        ):
            return False
        if (
            _sequence_bound_matches_default(
                after,
                value_field,
                after_value,
                dialect=dialect,
            )
            and before_value is None
        ):
            return False
        return True
    if before_value is not None or after_value is not None:
        return getattr(before, no_value_field) != getattr(after, no_value_field)
    return False


def _sequence_bound_matches_default(
    sequence: SequenceSnapshot,
    value_field: str,
    value: Any,
    *,
    dialect: str | None,
) -> bool:
    bound = "min" if value_field == "min_value" else "max"
    default = _default_sequence_bound(dialect, sequence, bound=bound)
    return default is not None and value == default


def _default_sequence_bound(
    dialect: str | None,
    sequence: SequenceSnapshot,
    *,
    bound: str,
) -> int | None:
    if dialect is None:
        return None
    increment = sequence.increment if sequence.increment is not None else 1
    if dialect == "postgresql":
        type_min, type_max = _postgres_integer_type_range(sequence.data_type)
        if increment < 0:
            return type_min if bound == "min" else -1
        return 1 if bound == "min" else type_max
    if dialect == "mariadb":
        type_min, type_max = _mariadb_sequence_type_range(sequence.data_type)
        if increment < 0:
            return type_min if bound == "min" else -1
        return 1 if bound == "min" else type_max
    if dialect == "mssql":
        type_min, type_max = _mssql_sequence_type_range(sequence.data_type)
        return type_min if bound == "min" else type_max
    if dialect == "oracle":
        if increment < 0:
            return -(10**27 - 1) if bound == "min" else -1
        return 1 if bound == "min" else 10**28 - 1
    return None


def _postgres_integer_type_range(data_type: str | None) -> tuple[int, int]:
    normalized = _normalized_integer_type_name(data_type) or "bigint"
    return {
        "smallint": (-32768, 32767),
        "integer": (-2147483648, 2147483647),
        "int": (-2147483648, 2147483647),
        "bigint": (-9223372036854775808, 9223372036854775807),
    }.get(normalized, (-9223372036854775808, 9223372036854775807))


def _mariadb_sequence_type_range(data_type: str | None) -> tuple[int, int]:
    normalized = _normalized_integer_type_name(data_type) or "bigint"
    unsigned = _normalized_integer_type_is_unsigned(data_type)
    ranges = {
        "tinyint": ((-128, 127), (0, 255)),
        "smallint": ((-32768, 32767), (0, 65535)),
        "mediumint": ((-8388608, 8388607), (0, 16777215)),
        "int": ((-2147483648, 2147483647), (0, 4294967295)),
        "integer": ((-2147483648, 2147483647), (0, 4294967295)),
        "bigint": (
            (-9223372036854775807, 9223372036854775806),
            (0, 9223372036854775806),
        ),
    }
    signed_range, unsigned_range = ranges.get(
        normalized,
        ((-9223372036854775807, 9223372036854775806), (0, 9223372036854775806)),
    )
    return unsigned_range if unsigned else signed_range


def _mssql_sequence_type_range(data_type: str | None) -> tuple[int, int]:
    normalized = _normalized_integer_type_name(data_type) or "bigint"
    return {
        "tinyint": (0, 255),
        "smallint": (-32768, 32767),
        "int": (-2147483648, 2147483647),
        "integer": (-2147483648, 2147483647),
        "bigint": (-9223372036854775808, 9223372036854775807),
    }.get(normalized, (-9223372036854775808, 9223372036854775807))


def _normalized_integer_type_name(data_type: str | None) -> str | None:
    if data_type is None:
        return None
    normalized = data_type.strip().lower().split()[0]
    return normalized.split("(", 1)[0]


def _normalized_integer_type_is_unsigned(data_type: str | None) -> bool:
    return data_type is not None and " unsigned" in f" {data_type.lower()}"


def _create_view_sql(dialect: str, view: ViewSnapshot) -> str:
    normalized = dialect_name(dialect)
    _ensure_view_dialect_supported(normalized, view)
    materialized = " MATERIALIZED" if view.materialized else ""
    return (
        f"CREATE{materialized} VIEW {_quote_view_name(dialect, view)} "
        f"AS {view.definition}"
    )


def _drop_view_sql(dialect: str, view: ViewSnapshot) -> str:
    normalized = dialect_name(dialect)
    _ensure_view_dialect_supported(normalized, view)
    materialized = " MATERIALIZED" if view.materialized else ""
    exists = "" if normalized == "oracle" else " IF EXISTS"
    return f"DROP{materialized} VIEW{exists} {_quote_view_name(dialect, view)}"


def _set_view_comment_sql(
    dialect: str,
    view: ViewSnapshot,
    *,
    for_create: bool = False,
) -> str | None:
    if view.comment is None and for_create:
        return None
    normalized = dialect_name(dialect)
    if normalized == "postgresql":
        view_kind = "MATERIALIZED VIEW" if view.materialized else "VIEW"
        literal = sql_literal(view.comment) if view.comment is not None else "NULL"
        return f"COMMENT ON {view_kind} {_quote_view_name(dialect, view)} IS {literal}"
    if normalized == "oracle":
        literal = sql_literal(view.comment if view.comment is not None else "")
        if view.materialized:
            return (
                f"COMMENT ON MATERIALIZED VIEW {_quote_view_name(dialect, view)} "
                f"IS {literal}"
            )
        return f"COMMENT ON TABLE {_quote_view_name(dialect, view)} IS {literal}"
    if normalized == "mssql" and not view.materialized:
        return _mssql_view_comment_sql(_view_qualified_name(view), view.comment)
    raise ValueError(
        "view comments only support PostgreSQL, SQL Server regular views, and Oracle"
    )


def _ensure_view_dialect_supported(dialect: str, view: ViewSnapshot) -> None:
    if dialect not in _VIEW_DIALECTS:
        raise ValueError(
            "view migrations only support SQLite, PostgreSQL, MySQL, MariaDB, SQL Server, and Oracle"
        )
    if view.materialized and dialect not in _MATERIALIZED_VIEW_DIALECTS:
        raise ValueError(
            "materialized view migrations only support PostgreSQL and Oracle"
        )


def _quote_view_name(dialect: str, view: ViewSnapshot) -> str:
    if view.schema is None:
        return quote_ident(dialect, view.name)
    return f"{quote_ident(dialect, view.schema)}.{quote_ident(dialect, view.name)}"


def _view_key(view: ViewSnapshot) -> tuple[str | None, str]:
    return (view.schema, view.name)


def _view_qualified_name(view: ViewSnapshot) -> str:
    if view.schema is None:
        return view.name
    return f"{view.schema}.{view.name}"


def _view_requires_recreate(
    before: ViewSnapshot,
    after: ViewSnapshot,
    *,
    dialect: str | None = None,
) -> bool:
    return (
        not _view_definitions_equivalent(before.definition, after.definition, dialect)
        or before.schema != after.schema
        or before.materialized != after.materialized
    )


def _view_definitions_equivalent(
    before: str,
    after: str,
    dialect: str | None,
) -> bool:
    if before == after:
        return True
    normalized_dialect = dialect_name(dialect) if dialect is not None else None
    if normalized_dialect in {"mysql", "mariadb"}:
        before_mysql_signature = _mysql_simple_view_signature(before)
        after_mysql_signature = _mysql_simple_view_signature(after)
        return (
            before_mysql_signature is not None
            and before_mysql_signature == after_mysql_signature
        )
    if normalized_dialect == "postgresql":
        before_postgres_signature = _postgres_simple_view_signature(before)
        after_postgres_signature = _postgres_simple_view_signature(after)
        return (
            before_postgres_signature is not None
            and before_postgres_signature == after_postgres_signature
        )
    if normalized_dialect == "mssql":
        before_mssql_signature = _mssql_simple_view_signature(before)
        after_mssql_signature = _mssql_simple_view_signature(after)
        return (
            before_mssql_signature is not None
            and before_mssql_signature == after_mssql_signature
        )
    return False


def _mysql_simple_view_signature(
    definition: str,
) -> tuple[str, tuple[tuple[str, str | None], ...]] | None:
    normalized = definition.strip().rstrip(";").strip()
    match = re.fullmatch(r"(?is)select\s+(.+?)\s+from\s+(.+)", normalized)
    if match is None:
        return None
    columns_sql, table_sql = match.groups()
    table = _mysql_view_identifier_name(table_sql)
    if table is None:
        return None
    columns: list[tuple[str, str | None]] = []
    for item in _split_top_level_sql_list(columns_sql):
        column = _mysql_simple_view_column_signature(item)
        if column is None:
            return None
        columns.append(column)
    return table, tuple(columns)


def _mysql_simple_view_column_signature(item: str) -> tuple[str, str | None] | None:
    match = re.fullmatch(r"(?is)(.+?)\s+as\s+(.+)", item.strip())
    if match is None:
        expression = item
        alias = None
    else:
        expression, alias = match.groups()
    column = _mysql_view_identifier_name(expression)
    if column is None:
        return None
    alias_name = _mysql_view_identifier_name(alias) if alias is not None else None
    if alias is not None and alias_name is None:
        return None
    if alias_name == column:
        alias_name = None
    return column, alias_name


def _postgres_simple_view_signature(
    definition: str,
) -> tuple[tuple[str, ...], tuple[tuple[str, str | None], ...]] | None:
    normalized = definition.strip().rstrip(";").strip()
    match = re.fullmatch(r"(?is)select\s+(.+?)\s+from\s+(.+)", normalized)
    if match is None:
        return None
    columns_sql, table_sql = match.groups()
    table = _postgres_view_identifier_parts(table_sql)
    if table is None:
        return None
    columns: list[tuple[str, str | None]] = []
    for item in _split_top_level_sql_list(columns_sql):
        column = _postgres_simple_view_column_signature(item)
        if column is None:
            return None
        columns.append(column)
    return tuple(table), tuple(columns)


def _postgres_simple_view_column_signature(
    item: str,
) -> tuple[str, str | None] | None:
    match = re.fullmatch(r"(?is)(.+?)\s+as\s+(.+)", item.strip())
    if match is None:
        expression = item
        alias = None
    else:
        expression, alias = match.groups()
    column = _postgres_view_identifier_name(expression)
    if column is None:
        return None
    alias_name = _postgres_view_identifier_name(alias) if alias is not None else None
    if alias is not None and alias_name is None:
        return None
    if alias_name == column:
        alias_name = None
    return column, alias_name


def _mssql_simple_view_signature(
    definition: str,
) -> tuple[str, tuple[tuple[str, str | None], ...]] | None:
    normalized = _mssql_view_select_definition(definition)
    match = re.fullmatch(r"(?is)select\s+(.+?)\s+from\s+(.+)", normalized)
    if match is None:
        return None
    columns_sql, table_sql = match.groups()
    table = _mssql_view_identifier_name(table_sql)
    if table is None:
        return None
    columns: list[tuple[str, str | None]] = []
    for item in _split_top_level_sql_list(columns_sql):
        column = _mssql_simple_view_column_signature(item)
        if column is None:
            return None
        columns.append(column)
    return table, tuple(columns)


def _mssql_view_select_definition(definition: str) -> str:
    normalized = definition.strip().rstrip(";").strip()
    match = re.fullmatch(
        r"(?is)create\s+(?:or\s+alter\s+)?view\s+.+?\s+as\s+(.+)",
        normalized,
    )
    if match is not None:
        return match.group(1).strip()
    return normalized


def _mssql_simple_view_column_signature(item: str) -> tuple[str, str | None] | None:
    match = re.fullmatch(r"(?is)(.+?)\s+as\s+(.+)", item.strip())
    if match is None:
        expression = item
        alias = None
    else:
        expression, alias = match.groups()
    column = _mssql_view_identifier_name(expression)
    if column is None:
        return None
    alias_name = _mssql_view_identifier_name(alias) if alias is not None else None
    if alias is not None and alias_name is None:
        return None
    if alias_name == column:
        alias_name = None
    return column, alias_name


def _mysql_view_identifier_name(value: str | None) -> str | None:
    if value is None:
        return None
    parts = _mysql_view_identifier_parts(value)
    if parts is None or not parts:
        return None
    return parts[-1].lower()


def _mysql_view_identifier_parts(value: str) -> list[str] | None:
    text = value.strip()
    if not text:
        return None
    parts: list[str] = []
    start = 0
    quote: str | None = None
    index = 0
    while index < len(text):
        char = text[index]
        if quote is not None:
            if char == quote:
                if quote == "`" and index + 1 < len(text) and text[index + 1] == "`":
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char == "`":
            quote = "`"
            index += 1
            continue
        if char == ".":
            parts.append(text[start:index].strip())
            start = index + 1
        elif char.isspace():
            return None
        index += 1
    if quote is not None:
        return None
    parts.append(text[start:].strip())
    normalized_parts: list[str] = []
    for part in parts:
        normalized = _mysql_view_identifier_part(part)
        if normalized is None:
            return None
        normalized_parts.append(normalized)
    return normalized_parts


def _mysql_view_identifier_part(part: str) -> str | None:
    if len(part) >= 2 and part[0] == "`" and part[-1] == "`":
        unquoted = part[1:-1].replace("``", "`").strip()
    else:
        unquoted = part
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", unquoted) is None:
        return None
    return unquoted


def _postgres_view_identifier_name(value: str | None) -> str | None:
    if value is None:
        return None
    parts = _postgres_view_identifier_parts(value)
    if parts is None or not parts:
        return None
    return parts[-1]


def _postgres_view_identifier_parts(value: str) -> list[str] | None:
    text = value.strip()
    if not text:
        return None
    parts: list[str] = []
    start = 0
    quote = False
    index = 0
    while index < len(text):
        char = text[index]
        if quote:
            if char == '"':
                if index + 1 < len(text) and text[index + 1] == '"':
                    index += 2
                    continue
                quote = False
            index += 1
            continue
        if char == '"':
            quote = True
            index += 1
            continue
        if char == ".":
            parts.append(text[start:index].strip())
            start = index + 1
        elif char.isspace():
            return None
        index += 1
    if quote:
        return None
    parts.append(text[start:].strip())
    normalized_parts: list[str] = []
    for part in parts:
        normalized = _postgres_view_identifier_part(part)
        if normalized is None:
            return None
        normalized_parts.append(normalized)
    return normalized_parts


def _postgres_view_identifier_part(part: str) -> str | None:
    if len(part) >= 2 and part[0] == '"' and part[-1] == '"':
        unquoted = part[1:-1].replace('""', '"')
        if not unquoted:
            return None
        return unquoted
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", part) is None:
        return None
    return part.lower()


def _mssql_view_identifier_name(value: str | None) -> str | None:
    if value is None:
        return None
    parts = _mssql_view_identifier_parts(value)
    if parts is None or not parts:
        return None
    return parts[-1].lower()


def _mssql_view_identifier_parts(value: str) -> list[str] | None:
    text = value.strip()
    if not text:
        return None
    parts: list[str] = []
    start = 0
    quote: str | None = None
    index = 0
    while index < len(text):
        char = text[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(text) and text[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char == "[":
            quote = "]"
            index += 1
            continue
        if char == '"':
            quote = '"'
            index += 1
            continue
        if char == ".":
            parts.append(text[start:index].strip())
            start = index + 1
        elif char.isspace():
            return None
        index += 1
    if quote is not None:
        return None
    parts.append(text[start:].strip())
    normalized_parts: list[str] = []
    for part in parts:
        normalized = _mssql_view_identifier_part(part)
        if normalized is None:
            return None
        normalized_parts.append(normalized)
    return normalized_parts


def _mssql_view_identifier_part(part: str) -> str | None:
    if len(part) >= 2 and part[0] == "[" and part[-1] == "]":
        unquoted = part[1:-1].replace("]]", "]")
    elif len(part) >= 2 and part[0] == '"' and part[-1] == '"':
        unquoted = part[1:-1].replace('""', '"')
    else:
        unquoted = part
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", unquoted) is None:
        return None
    return unquoted


def _mssql_view_comment_sql(name: str, comment: str | None) -> str:
    schema, view_name = _split_qualified_name(name)
    schema_value = (
        _mssql_unicode_literal(schema) if schema is not None else "SCHEMA_NAME()"
    )
    view_literal = _mssql_unicode_literal(view_name)
    exists_predicate = (
        "EXISTS (SELECT 1 FROM sys.extended_properties ep "
        "JOIN sys.views v ON ep.major_id = v.object_id "
        "JOIN sys.schemas s ON v.schema_id = s.schema_id "
        "WHERE ep.class = 1 AND ep.minor_id = 0 "
        "AND ep.name = N'MS_Description' "
        f"AND s.name = @schema AND v.name = {view_literal})"
    )
    level_args = (
        "@level0type = N'SCHEMA', @level0name = @schema, "
        f"@level1type = N'VIEW', @level1name = {view_literal}"
    )
    if comment is None:
        return (
            f"DECLARE @schema sysname = {schema_value}; "
            f"IF {exists_predicate} "
            "EXEC sys.sp_dropextendedproperty @name = N'MS_Description', "
            f"{level_args}"
        )
    comment_literal = _mssql_unicode_literal(comment)
    return (
        f"DECLARE @schema sysname = {schema_value}; "
        f"IF {exists_predicate} "
        "EXEC sys.sp_updateextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}; "
        "ELSE EXEC sys.sp_addextendedproperty @name = N'MS_Description', "
        f"@value = {comment_literal}, {level_args}"
    )


def _split_qualified_name(name: str) -> tuple[str | None, str]:
    if "." not in name:
        return None, name
    schema, object_name = name.rsplit(".", 1)
    return schema, object_name


def _compiled_sql(sql: str, **metadata: Any) -> dict[str, Any]:
    compiled: dict[str, Any] = {"sql": sql}
    compiled.update(metadata)
    return compiled


def _compiled_view_comment_sql(
    dialect: str,
    view: ViewSnapshot,
    *,
    for_create: bool = False,
) -> dict[str, Any] | None:
    sql = _set_view_comment_sql(dialect, view, for_create=for_create)
    if sql is None:
        return None
    return _compiled_sql(
        sql,
        kind="comment_view",
        table=None,
        object_name=_view_qualified_name(view),
        unsafe=False,
        destructive=False,
        requires_rebuild=False,
    )


def _coerce_snapshot(snapshot: SchemaSnapshot | Mapping[str, Any]) -> SchemaSnapshot:
    if isinstance(snapshot, SchemaSnapshot):
        return snapshot
    return SchemaSnapshot.from_dict(snapshot)


def _check_expression(
    field: str,
    check: RuntimeCheck,
    *,
    dialect: str | None = None,
) -> str:
    kind, operator, value = check
    rendered_field = _check_field_expression(field, dialect=dialect)
    if kind == "comparison":
        return f"{rendered_field} {operator} {value}"
    if kind == "length":
        length_function = "LEN" if dialect_name(dialect or "") == "mssql" else "LENGTH"
        return f"{length_function}({rendered_field}) {operator} {value}"
    if kind == "enum" and operator == "in":
        return f"{rendered_field} IN ({value})"
    if kind == "pattern" and operator == "matches":
        return f"ormdantic_regex_match({rendered_field}, {value})"
    if kind == "multiple_of" and operator == "=":
        return f"ormdantic_multiple_of({rendered_field}, {value})"
    raise ValueError(f"unsupported check constraint kind '{kind}'")


def _check_field_expression(field: str, *, dialect: str | None = None) -> str:
    return (
        quote_ident("oracle", field)
        if dialect_name(dialect or "") == "oracle"
        else field
    )


def _check_suffix(check: RuntimeCheck) -> str:
    kind, operator, _ = check
    suffixes = {
        ("comparison", ">="): "ge",
        ("comparison", ">"): "gt",
        ("comparison", "<="): "le",
        ("comparison", "<"): "lt",
        ("length", ">="): "min_length",
        ("length", "<="): "max_length",
        ("enum", "in"): "enum_values",
        ("pattern", "matches"): "pattern",
        ("multiple_of", "="): "multiple_of",
    }
    try:
        return suffixes[(kind, operator)]
    except KeyError as exc:
        raise ValueError(
            f"unsupported check constraint operator '{operator}' for kind '{kind}'"
        ) from exc


def _require_migration_symbol(symbol: str) -> Any:
    return import_native_extension(
        context="migration planning",
        required_symbols=(symbol,),
    )
