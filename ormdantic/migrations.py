"""Migration planning and execution facade."""

from __future__ import annotations

import importlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import replace
from os import PathLike
from typing import Any

from ormdantic._migrations.artifacts import (
    MigrationArtifact,
    _coerce_artifact,
    _migration_files,
    _plan_checksum,
)
from ormdantic._migrations.artifacts import (
    _artifact_checksum as _artifact_checksum,
)
from ormdantic._migrations.artifacts import (
    _canonicalize_checksum_payload as _canonicalize_checksum_payload,
)
from ormdantic._migrations.artifacts import (
    _change_from_dict as _change_from_dict,
)
from ormdantic._migrations.artifacts import (
    _change_to_dict as _change_to_dict,
)
from ormdantic._migrations.artifacts import (
    _diff_from_dict as _diff_from_dict,
)
from ormdantic._migrations.artifacts import (
    _diff_to_dict as _diff_to_dict,
)
from ormdantic._migrations.artifacts import (
    _normalize_change_details as _normalize_change_details,
)
from ormdantic._migrations.artifacts import (
    _operation_from_dict as _operation_from_dict,
)
from ormdantic._migrations.artifacts import (
    _operation_to_dict as _operation_to_dict,
)
from ormdantic._migrations.artifacts import (
    _validate_contiguous_artifacts as _validate_contiguous_artifacts,
)
from ormdantic._migrations.artifacts import (
    _warning_from_dict as _warning_from_dict,
)
from ormdantic._migrations.artifacts import (
    _warning_to_dict as _warning_to_dict,
)
from ormdantic._migrations.history import (
    MIGRATION_LOCK_NAME as MIGRATION_LOCK_NAME,
)
from ormdantic._migrations.history import (
    MIGRATION_TABLE as MIGRATION_TABLE,
)
from ormdantic._migrations.history import (
    _acquire_migration_lock as _acquire_migration_lock,
)
from ormdantic._migrations.history import (
    _add_migration_history_column_sql as _add_migration_history_column_sql,
)
from ormdantic._migrations.history import (
    _commit_migration_history_if_needed,
    _current_entry,
    _ensure_migration_history_table,
    _history_entries,
    _history_entry,
    _is_dirty,
    _repair_history,
    _run_migration_operations,
)
from ormdantic._migrations.history import (
    _dialect_supports_transactional_ddl as _dialect_supports_transactional_ddl,
)
from ormdantic._migrations.history import (
    _is_duplicate_column_error as _is_duplicate_column_error,
)
from ormdantic._migrations.history import (
    _is_duplicate_table_error as _is_duplicate_table_error,
)
from ormdantic._migrations.history import (
    _migration_history_column_exists as _migration_history_column_exists,
)
from ormdantic._migrations.history import (
    _migration_history_table_exists as _migration_history_table_exists,
)
from ormdantic._migrations.history import (
    _migration_table_column_defs as _migration_table_column_defs,
)
from ormdantic._migrations.history import (
    _write_history_entry as _write_history_entry,
)
from ormdantic._migrations.models import (
    MIGRATION_ARTIFACT_VERSION,
    MIGRATION_STATUS_APPLIED,
    MIGRATION_STATUS_ROLLED_BACK,
    MigrationHistoryEntry,
    MigrationPlan,
    SchemaSnapshot,
)
from ormdantic._migrations.models import (
    MIGRATION_STATUS_FAILED as MIGRATION_STATUS_FAILED,
)
from ormdantic._migrations.models import (
    ColumnSnapshot as ColumnSnapshot,
)
from ormdantic._migrations.models import (
    EnumTypeSnapshot as EnumTypeSnapshot,
)
from ormdantic._migrations.models import (
    ExclusionConstraintSnapshot as ExclusionConstraintSnapshot,
)
from ormdantic._migrations.models import (
    ForeignKeyConstraintSnapshot as ForeignKeyConstraintSnapshot,
)
from ormdantic._migrations.models import (
    IndexSnapshot as IndexSnapshot,
)
from ormdantic._migrations.models import (
    MigrationChange as MigrationChange,
)
from ormdantic._migrations.models import (
    MigrationOperation as MigrationOperation,
)
from ormdantic._migrations.models import (
    MigrationWarning as MigrationWarning,
)
from ormdantic._migrations.models import (
    NamespaceSnapshot as NamespaceSnapshot,
)
from ormdantic._migrations.models import (
    RuntimeCheck as RuntimeCheck,
)
from ormdantic._migrations.models import (
    SchemaDiff as SchemaDiff,
)
from ormdantic._migrations.models import (
    SequenceSnapshot as SequenceSnapshot,
)
from ormdantic._migrations.models import (
    TableCheckSnapshot as TableCheckSnapshot,
)
from ormdantic._migrations.models import (
    TableSnapshot as TableSnapshot,
)
from ormdantic._migrations.models import (
    UniqueConstraintSnapshot as UniqueConstraintSnapshot,
)
from ormdantic._migrations.models import (
    ViewSnapshot as ViewSnapshot,
)
from ormdantic._migrations.planning import (
    _build_plan,
    _coerce_snapshot,
    create_migration_artifact,
    diff_snapshots,
    squash_migrations,
)
from ormdantic._migrations.planning import (
    _changed_column_fields as _changed_column_fields,
)
from ormdantic._migrations.planning import (
    _check_expression as _check_expression,
)
from ormdantic._migrations.planning import (
    _check_suffix as _check_suffix,
)
from ormdantic._migrations.planning import (
    _classify_sql_operation as _classify_sql_operation,
)
from ormdantic._migrations.planning import (
    _compile_enum_type_diff as _compile_enum_type_diff,
)
from ormdantic._migrations.planning import (
    _compile_namespace_diff as _compile_namespace_diff,
)
from ormdantic._migrations.planning import (
    _compile_schema_diff as _compile_schema_diff,
)
from ormdantic._migrations.planning import (
    _compile_table_create_sql as _compile_table_create_sql,
)
from ormdantic._migrations.planning import (
    _compile_view_diff as _compile_view_diff,
)
from ormdantic._migrations.planning import (
    _constraints as _constraints,
)
from ormdantic._migrations.planning import (
    _diff_columns as _diff_columns,
)
from ormdantic._migrations.planning import (
    _diff_constraints as _diff_constraints,
)
from ormdantic._migrations.planning import (
    _diff_enum_types as _diff_enum_types,
)
from ormdantic._migrations.planning import (
    _diff_indexes as _diff_indexes,
)
from ormdantic._migrations.planning import (
    _diff_namespaces as _diff_namespaces,
)
from ormdantic._migrations.planning import (
    _diff_views as _diff_views,
)
from ormdantic._migrations.planning import (
    _is_destructive_column_change as _is_destructive_column_change,
)
from ormdantic._migrations.planning import (
    _operation_from_compiled as _operation_from_compiled,
)
from ormdantic._migrations.planning import (
    _operation_payload as _operation_payload,
)
from ormdantic._migrations.planning import (
    _raise_if_unsupported_sqlite_plan as _raise_if_unsupported_sqlite_plan,
)
from ormdantic._migrations.planning import (
    _rewrite_sqlite_rebuild_operations as _rewrite_sqlite_rebuild_operations,
)
from ormdantic._migrations.planning import (
    _set_constraint_comment_sql as _set_constraint_comment_sql,
)
from ormdantic._migrations.planning import (
    _set_enum_type_comment_sql as _set_enum_type_comment_sql,
)
from ormdantic._migrations.planning import (
    _set_index_comment_sql as _set_index_comment_sql,
)
from ormdantic._migrations.planning import (
    _set_index_tablespace_sql as _set_index_tablespace_sql,
)
from ormdantic._migrations.planning import (
    _set_namespace_comment_sql as _set_namespace_comment_sql,
)
from ormdantic._migrations.planning import (
    _set_sequence_comment_sql as _set_sequence_comment_sql,
)
from ormdantic._migrations.planning import (
    _set_view_comment_sql as _set_view_comment_sql,
)
from ormdantic._migrations.planning import (
    _sql_identifier_after as _sql_identifier_after,
)
from ormdantic._migrations.planning import (
    _sql_identifier_after_keyword as _sql_identifier_after_keyword,
)
from ormdantic._migrations.planning import (
    _sqlite_rebuild_table_name as _sqlite_rebuild_table_name,
)
from ormdantic._migrations.planning import (
    _sqlite_rebuild_table_operations as _sqlite_rebuild_table_operations,
)
from ormdantic._migrations.planning import (
    _table_snapshot_with_name as _table_snapshot_with_name,
)
from ormdantic._migrations.planning import (
    _warning_for_change as _warning_for_change,
)
from ormdantic._migrations.reflection import (
    _normalize_reflected_type as _normalize_reflected_type,
)
from ormdantic._migrations.reflection import (
    _normalize_reflected_validated as _normalize_reflected_validated,
)
from ormdantic._migrations.reflection import (
    _normalize_sqlite_type as _normalize_sqlite_type,
)
from ormdantic._migrations.reflection import (
    _nullable_from_reflection as _nullable_from_reflection,
)
from ormdantic._migrations.reflection import (
    _oracle_cons_columns_view as _oracle_cons_columns_view,
)
from ormdantic._migrations.reflection import (
    _oracle_constraints_view as _oracle_constraints_view,
)
from ormdantic._migrations.reflection import (
    _oracle_ind_columns_view as _oracle_ind_columns_view,
)
from ormdantic._migrations.reflection import (
    _oracle_indexes_view as _oracle_indexes_view,
)
from ormdantic._migrations.reflection import (
    _oracle_materialized_views_view as _oracle_materialized_views_view,
)
from ormdantic._migrations.reflection import (
    _oracle_owner_filter as _oracle_owner_filter,
)
from ormdantic._migrations.reflection import (
    _oracle_sequences_view as _oracle_sequences_view,
)
from ormdantic._migrations.reflection import (
    _oracle_tab_columns_view as _oracle_tab_columns_view,
)
from ormdantic._migrations.reflection import (
    _oracle_table_view as _oracle_table_view,
)
from ormdantic._migrations.reflection import (
    _oracle_views_view as _oracle_views_view,
)
from ormdantic._migrations.reflection import (
    _reflect_key_columns as _reflect_key_columns,
)
from ormdantic._migrations.reflection import (
    _reflect_named_key_columns as _reflect_named_key_columns,
)
from ormdantic._migrations.reflection import (
    _reflect_schema_snapshot,
)
from ormdantic._migrations.reflection import (
    _reflect_server_check_constraints as _reflect_server_check_constraints,
)
from ormdantic._migrations.reflection import (
    _reflect_server_column_comments as _reflect_server_column_comments,
)
from ormdantic._migrations.reflection import (
    _reflect_server_columns as _reflect_server_columns,
)
from ormdantic._migrations.reflection import (
    _reflect_server_enum_types as _reflect_server_enum_types,
)
from ormdantic._migrations.reflection import (
    _reflect_server_exclusion_constraints as _reflect_server_exclusion_constraints,
)
from ormdantic._migrations.reflection import (
    _reflect_server_foreign_key_constraints as _reflect_server_foreign_key_constraints,
)
from ormdantic._migrations.reflection import (
    _reflect_server_foreign_keys as _reflect_server_foreign_keys,
)
from ormdantic._migrations.reflection import (
    _reflect_server_indexes as _reflect_server_indexes,
)
from ormdantic._migrations.reflection import (
    _reflect_server_mysql_table_options as _reflect_server_mysql_table_options,
)
from ormdantic._migrations.reflection import (
    _reflect_server_namespaces as _reflect_server_namespaces,
)
from ormdantic._migrations.reflection import (
    _reflect_server_oracle_table_compressions as _reflect_server_oracle_table_compressions,
)
from ormdantic._migrations.reflection import (
    _reflect_server_postgres_inherits as _reflect_server_postgres_inherits,
)
from ormdantic._migrations.reflection import (
    _reflect_server_postgres_partition_by as _reflect_server_postgres_partition_by,
)
from ormdantic._migrations.reflection import (
    _reflect_server_postgres_partitions as _reflect_server_postgres_partitions,
)
from ormdantic._migrations.reflection import (
    _reflect_server_postgres_unlogged as _reflect_server_postgres_unlogged,
)
from ormdantic._migrations.reflection import (
    _reflect_server_postgres_using as _reflect_server_postgres_using,
)
from ormdantic._migrations.reflection import (
    _reflect_server_postgres_with as _reflect_server_postgres_with,
)
from ormdantic._migrations.reflection import (
    _reflect_server_primary_keys as _reflect_server_primary_keys,
)
from ormdantic._migrations.reflection import (
    _reflect_server_sequences as _reflect_server_sequences,
)
from ormdantic._migrations.reflection import (
    _reflect_server_snapshot as _reflect_server_snapshot,
)
from ormdantic._migrations.reflection import (
    _reflect_server_table_comments as _reflect_server_table_comments,
)
from ormdantic._migrations.reflection import (
    _reflect_server_table_tablespaces as _reflect_server_table_tablespaces,
)
from ormdantic._migrations.reflection import (
    _reflect_server_tables as _reflect_server_tables,
)
from ormdantic._migrations.reflection import (
    _reflect_server_unique_constraints as _reflect_server_unique_constraints,
)
from ormdantic._migrations.reflection import (
    _reflect_server_views as _reflect_server_views,
)
from ormdantic._migrations.reflection import (
    _reflect_sqlite_snapshot as _reflect_sqlite_snapshot,
)
from ormdantic._migrations.reflection import (
    _reflect_sqlite_views as _reflect_sqlite_views,
)
from ormdantic._migrations.reflection import (
    _reflected_max_length as _reflected_max_length,
)
from ormdantic._migrations.reflection import (
    _schema_filter as _schema_filter,
)
from ormdantic._migrations.reflection import (
    _table_name_filter as _table_name_filter,
)
from ormdantic._migrations.sql import (
    dialect_name as _dialect_name,
)
from ormdantic._migrations.sql import (
    table_matches_filters as _table_matches_filters,
)

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


def _filter_snapshot_for_autogenerate_scope(
    snapshot: SchemaSnapshot,
    *,
    include_tables: Sequence[str] | None,
    exclude_tables: Sequence[str] | None,
    schema: str | None,
) -> SchemaSnapshot:
    if not include_tables and not exclude_tables:
        return snapshot
    tables = [
        table
        for table in snapshot.tables
        if _table_matches_filters(table.name, include_tables, exclude_tables)
    ]
    enum_types = [
        enum_type
        for enum_type in snapshot.enum_types
        if _enum_type_key(enum_type) in _enum_type_keys_for_tables(tables)
    ]
    sequences = [
        sequence
        for sequence in snapshot.sequences
        if _sequence_key(sequence) in _sequence_keys_for_tables(tables)
    ]
    views = [
        view
        for view in snapshot.views
        if _table_matches_filters(view.name, include_tables, exclude_tables)
    ]
    namespace_names = _namespace_names_for_scoped_objects(
        tables,
        enum_types,
        sequences,
        views,
    )
    if schema is not None and (tables or enum_types or sequences or views):
        namespace_names.add(schema)
    return replace(
        snapshot,
        tables=tables,
        namespaces=[
            namespace
            for namespace in snapshot.namespaces
            if namespace.name in namespace_names
        ],
        enum_types=enum_types,
        sequences=sequences,
        views=views,
    )


def _enum_type_keys_for_tables(
    tables: Sequence[TableSnapshot],
) -> set[tuple[str | None, str]]:
    keys: set[tuple[str | None, str]] = set()
    for table in tables:
        for column in table.columns:
            if column.kind.startswith("enum:"):
                keys.add(_enum_column_kind_key(column.kind))
    return keys


def _enum_column_kind_key(kind: str) -> tuple[str | None, str]:
    enum_name = kind.removeprefix("enum:")
    if "." not in enum_name:
        return None, enum_name
    schema, name = enum_name.rsplit(".", 1)
    return schema, name


def _enum_type_key(enum_type: EnumTypeSnapshot) -> tuple[str | None, str]:
    return enum_type.schema, enum_type.name


def _sequence_keys_for_tables(
    tables: Sequence[TableSnapshot],
) -> set[tuple[str | None, str]]:
    keys: set[tuple[str | None, str]] = set()
    for table in tables:
        for column in table.columns:
            if column.server_default is None:
                continue
            key = _sequence_key_from_default(column.server_default)
            if key is not None:
                keys.add(key)
    return keys


def _sequence_key_from_default(default: str) -> tuple[str | None, str] | None:
    match = re.search(r"\bnextval\s*\(\s*'((?:''|[^'])+)'", default, re.IGNORECASE)
    if match is None:
        return None
    name = match.group(1).replace("''", "'")
    if "." not in name:
        return None, _unquote_sequence_part(name)
    schema, sequence = name.rsplit(".", 1)
    return _unquote_sequence_part(schema), _unquote_sequence_part(sequence)


def _unquote_sequence_part(part: str) -> str:
    part = part.strip()
    if len(part) >= 2 and part[0] == '"' and part[-1] == '"':
        return part[1:-1].replace('""', '"')
    return part


def _sequence_key(sequence: SequenceSnapshot) -> tuple[str | None, str]:
    return sequence.schema, sequence.name


def _namespace_names_for_scoped_objects(
    tables: Sequence[TableSnapshot],
    enum_types: Sequence[EnumTypeSnapshot],
    sequences: Sequence[SequenceSnapshot],
    views: Sequence[ViewSnapshot],
) -> set[str]:
    names: set[str] = set()
    for table in tables:
        if table.schema is not None:
            names.add(table.schema)
    for enum_type in enum_types:
        if enum_type.schema is not None:
            names.add(enum_type.schema)
    for sequence in sequences:
        if sequence.schema is not None:
            names.add(sequence.schema)
    for view in views:
        if view.schema is not None:
            names.add(view.schema)
    return names


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

    @property
    def _connection_url(self) -> str:
        return str(self._database._connection)

    def _dialect(self) -> str:
        return _dialect_name(self._connection_url)

    def _native_connection(self) -> Any:
        rust = _require_migration_symbol("PyNativeConnection")
        return rust.PyNativeConnection(self._connection_url)

    def snapshot(self) -> SchemaSnapshot:
        """Return a serializable snapshot for the currently registered models."""
        return SchemaSnapshot.from_database(
            self._database,
            native_enum_types=self._dialect() == "postgresql",
        )

    def live_snapshot(
        self,
        *,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        schema: str | None = None,
    ) -> SchemaSnapshot:
        """Return a live schema snapshot reflected from the current database."""
        return _reflect_schema_snapshot(
            self._connection_url,
            dialect=self._dialect(),
            include_tables=include_tables,
            exclude_tables=exclude_tables,
            schema=schema,
        )

    def autogenerate(
        self,
        revision: str,
        *,
        dialect: str | None = None,
        include_tables: Sequence[str] | None = None,
        exclude_tables: Sequence[str] | None = None,
        schema: str | None = None,
        description: str | None = None,
        depends_on: Sequence[str] | None = None,
        branch_labels: Sequence[str] | None = None,
        path: str | PathLike[str] | None = None,
        skip_noop: bool = True,
    ) -> MigrationArtifact | None:
        """Generate a migration artifact by diffing live schema against models."""
        before = self.live_snapshot(
            include_tables=include_tables,
            exclude_tables=exclude_tables,
            schema=schema,
        )
        after = _filter_snapshot_for_autogenerate_scope(
            self.snapshot(),
            include_tables=include_tables,
            exclude_tables=exclude_tables,
            schema=schema,
        )
        if schema is not None and after.tables:
            after = replace(
                after,
                tables=[
                    replace(table, schema=schema) if table.schema is None else table
                    for table in after.tables
                ],
            )
        if schema is not None and after.enum_types:
            after = replace(
                after,
                enum_types=[
                    (
                        replace(enum_type, schema=schema)
                        if enum_type.schema is None
                        else enum_type
                    )
                    for enum_type in after.enum_types
                ],
            )
        if schema is not None and after.sequences:
            after = replace(
                after,
                sequences=[
                    (
                        replace(sequence, schema=schema)
                        if sequence.schema is None
                        else sequence
                    )
                    for sequence in after.sequences
                ],
            )
        if schema is not None and after.views:
            after = replace(
                after,
                views=[
                    replace(view, schema=schema) if view.schema is None else view
                    for view in after.views
                ],
            )
        if before.enum_types and not after.enum_types:
            # Non-PostgreSQL target snapshots do not infer native enum types.
            # Preserve reflected objects so autogenerate does not plan accidental drops.
            after = replace(after, enum_types=list(before.enum_types))
        if before.namespaces and not after.namespaces:
            # Namespace ownership is explicit. Preserve reflected namespaces unless
            # the model snapshot registers its own namespace target.
            after = replace(after, namespaces=list(before.namespaces))
        if before.sequences and not after.sequences:
            # Preserve reflected sequences unless the model snapshot registers its
            # own sequence target.
            after = replace(after, sequences=list(before.sequences))
        if before.views and not after.views:
            # Preserve reflected views unless the model snapshot registers its own
            # view target.
            after = replace(after, views=list(before.views))
        active_dialect = dialect or self._connection_url
        plan = _build_plan(active_dialect, before, after)
        if plan.is_empty() and skip_noop:
            return None
        artifact = MigrationArtifact.from_plan(
            revision,
            plan,
            before,
            after,
            dialect=active_dialect,
            description=description,
            depends_on=depends_on,
            branch_labels=branch_labels,
            metadata={"autogenerated": True, "schema": schema},
        )
        if path is not None:
            artifact.write(path)
        return artifact

    def diff(
        self,
        from_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
        to_snapshot: SchemaSnapshot | Mapping[str, Any] | None = None,
    ) -> SchemaDiff:
        """Return structured schema changes between two snapshots."""
        before, after = self._resolve_snapshots(from_snapshot, to_snapshot)
        return diff_snapshots(before, after, dialect=self._database._connection)

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
        description: str | None = None,
        depends_on: Sequence[str] | None = None,
        branch_labels: Sequence[str] | None = None,
        path: str | PathLike[str] | None = None,
    ) -> MigrationArtifact:
        """Generate a serializable migration artifact."""
        before, after = self._resolve_snapshots(from_snapshot, to_snapshot)
        artifact = create_migration_artifact(
            revision,
            before,
            after,
            dialect=dialect or self._database._connection,
            description=description,
            depends_on=depends_on,
            branch_labels=branch_labels,
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
        """Create/upgrade the migration history table when missing."""
        connection = self._native_connection()
        _ensure_migration_history_table(connection, self._dialect())

    async def applied_revisions(self) -> list[str]:
        """Return applied migration revisions ordered by apply time."""
        return [
            entry.revision
            for entry in await self.history()
            if entry.status == MIGRATION_STATUS_APPLIED and not entry.dirty
        ]

    async def history(self) -> list[MigrationHistoryEntry]:
        """Return migration history rows."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        return _history_entries(connection, dialect)

    async def current(self) -> MigrationHistoryEntry | None:
        """Return the latest successfully applied revision."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        return _current_entry(connection, dialect)

    async def is_dirty(self) -> bool:
        """Return whether migration history is currently marked dirty."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        return _is_dirty(connection, dialect)

    async def status(self) -> dict[str, Any]:
        """Return current migration status metadata."""
        current = await self.current()
        return {
            "dirty": await self.is_dirty(),
            "current": current.revision if current else None,
            "current_entry": current,
            "applied": await self.applied_revisions(),
        }

    async def repair(
        self,
        *,
        revision: str | None = None,
        status: str | None = None,
        clear_dirty: bool = True,
        checksum: str | None = None,
    ) -> int:
        """Repair migration metadata after a failed/manual migration."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        repaired = _repair_history(
            connection,
            dialect,
            revision=revision,
            status=status,
            clear_dirty=clear_dirty,
            checksum=checksum,
        )
        _commit_migration_history_if_needed(connection, dialect)
        return repaired

    async def apply_artifact(
        self,
        artifact: MigrationArtifact | Mapping[str, Any] | str | PathLike[str],
        *,
        allow_destructive: bool = False,
    ) -> bool:
        """Apply a migration artifact and record its revision."""
        migration = _coerce_artifact(artifact)
        migration.validate_checksum()
        return await self.apply(
            migration.revision,
            migration.to_plan(),
            allow_destructive=allow_destructive,
            checksum=migration.checksum,
            description=migration.description,
            artifact_version=migration.artifact_version,
            metadata=migration.metadata,
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
        pattern: str | None = None,
        allow_destructive: bool = False,
    ) -> list[str]:
        """Apply migration artifacts in filename order."""
        if await self.is_dirty():
            raise ValueError(
                "migration history is dirty; run `ormdantic migrations repair` before apply-dir"
            )
        applied = []
        applied_set = set(await self.applied_revisions())
        for artifact_path in _migration_files(path, pattern):
            artifact = MigrationArtifact.read(artifact_path)
            missing_dependencies = [
                revision
                for revision in artifact.depends_on
                if revision not in applied_set
            ]
            if missing_dependencies:
                raise ValueError(
                    f"migration {artifact.revision} has missing dependencies: "
                    + ", ".join(missing_dependencies)
                )
            if await self.apply_artifact(
                artifact,
                allow_destructive=allow_destructive,
            ):
                applied.append(artifact.revision)
                applied_set.add(artifact.revision)
        return applied

    async def apply(
        self,
        revision: str,
        plan: MigrationPlan,
        *,
        allow_destructive: bool = False,
        checksum: str | None = None,
        description: str | None = None,
        artifact_version: int = MIGRATION_ARTIFACT_VERSION,
        metadata: Mapping[str, Any] | None = None,
    ) -> bool:
        """Apply a migration plan and record its revision.

        Returns ``False`` when the revision is already recorded.
        """
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        if _is_dirty(connection, dialect):
            raise ValueError(
                "database is marked dirty from a failed migration; run repair before applying"
            )
        existing = _history_entry(connection, dialect, revision)
        expected_checksum = checksum or _plan_checksum(revision, plan)
        if (
            existing is not None
            and existing.status == MIGRATION_STATUS_APPLIED
            and not existing.dirty
        ):
            if existing.checksum and existing.checksum != expected_checksum:
                raise ValueError(
                    f"revision {revision} already applied with checksum "
                    f"{existing.checksum}, requested {expected_checksum}"
                )
            return False
        if plan.has_destructive_operations and not allow_destructive:
            raise ValueError(
                "migration contains destructive operations; pass "
                "allow_destructive=True to apply it"
            )
        _raise_if_unsupported_sqlite_plan(dialect, plan.operations)
        _run_migration_operations(
            connection=connection,
            dialect=dialect,
            revision=revision,
            operations=plan.operations,
            status=MIGRATION_STATUS_APPLIED,
            description=description,
            checksum=expected_checksum,
            artifact_version=artifact_version,
            metadata=dict(metadata or {}),
        )
        return True

    async def rollback(
        self,
        revision: str,
        plan: MigrationPlan,
        *,
        allow_destructive: bool = False,
        checksum: str | None = None,
        description: str | None = None,
        artifact_version: int = MIGRATION_ARTIFACT_VERSION,
        metadata: Mapping[str, Any] | None = None,
    ) -> bool:
        """Run rollback SQL and remove a migration revision."""
        connection = self._native_connection()
        dialect = self._dialect()
        _ensure_migration_history_table(connection, dialect)
        existing = _history_entry(connection, dialect, revision)
        if existing is None or existing.status != MIGRATION_STATUS_APPLIED:
            return False
        if not plan.rollback_available:
            raise ValueError(
                "rollback SQL is unavailable for this migration; "
                "provide explicit down SQL or generated rollback operations"
            )
        rollback_plan = MigrationPlan(
            operations=list(plan.rollback_operations),
            diff=plan.diff,
            warnings=plan.warnings,
            safety=plan.safety,
        )
        if rollback_plan.has_destructive_operations and not allow_destructive:
            raise ValueError(
                "rollback contains destructive operations; pass "
                "allow_destructive=True to apply it"
            )
        _raise_if_unsupported_sqlite_plan(dialect, rollback_plan.operations)
        _run_migration_operations(
            connection=connection,
            dialect=dialect,
            revision=revision,
            operations=rollback_plan.operations,
            status=MIGRATION_STATUS_ROLLED_BACK,
            description=description or existing.description,
            checksum=checksum
            or existing.checksum
            or _plan_checksum(revision, rollback_plan),
            artifact_version=artifact_version,
            metadata=dict(metadata or {}),
        )
        return True

    async def rollback_artifact(
        self,
        artifact: MigrationArtifact | Mapping[str, Any] | str | PathLike[str],
        *,
        allow_destructive: bool = False,
    ) -> bool:
        """Roll back a migration artifact when rollback SQL is available."""
        migration = _coerce_artifact(artifact)
        return await self.rollback(
            migration.revision,
            migration.to_plan(),
            allow_destructive=allow_destructive,
            checksum=migration.checksum,
            description=migration.description,
            artifact_version=migration.artifact_version,
            metadata=migration.metadata,
        )

    async def rollback_file(
        self, path: str | PathLike[str], *, allow_destructive: bool = False
    ) -> bool:
        """Roll back a migration artifact from disk."""
        return await self.rollback_artifact(
            MigrationArtifact.read(path),
            allow_destructive=allow_destructive,
        )

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
        ).with_checksum()
        if path is not None:
            artifact.write(path)
        return artifact


def _require_migration_symbol(symbol: str) -> Any:
    if _ormdantic is None or not hasattr(_ormdantic, symbol):
        raise RuntimeError(
            "Ormdantic requires the Rust extension for migration planning. "
            "Install the package with maturin or reinstall the wheel."
        )
    return _ormdantic
