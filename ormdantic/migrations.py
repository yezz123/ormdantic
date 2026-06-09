"""Migration planning and execution facade."""

from __future__ import annotations

import importlib
from collections.abc import Mapping, Sequence
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
    RuntimeCheck as RuntimeCheck,
)
from ormdantic._migrations.models import (
    SchemaDiff as SchemaDiff,
)
from ormdantic._migrations.models import (
    TableSnapshot as TableSnapshot,
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
    _compile_schema_diff as _compile_schema_diff,
)
from ormdantic._migrations.planning import (
    _compile_table_create_sql as _compile_table_create_sql,
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
    _diff_indexes as _diff_indexes,
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
    _oracle_owner_filter as _oracle_owner_filter,
)
from ormdantic._migrations.reflection import (
    _oracle_tab_columns_view as _oracle_tab_columns_view,
)
from ormdantic._migrations.reflection import (
    _oracle_table_view as _oracle_table_view,
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
    _reflect_server_columns as _reflect_server_columns,
)
from ormdantic._migrations.reflection import (
    _reflect_server_foreign_keys as _reflect_server_foreign_keys,
)
from ormdantic._migrations.reflection import (
    _reflect_server_indexes as _reflect_server_indexes,
)
from ormdantic._migrations.reflection import (
    _reflect_server_primary_keys as _reflect_server_primary_keys,
)
from ormdantic._migrations.reflection import (
    _reflect_server_snapshot as _reflect_server_snapshot,
)
from ormdantic._migrations.reflection import (
    _reflect_server_tables as _reflect_server_tables,
)
from ormdantic._migrations.reflection import (
    _reflect_server_unique_constraints as _reflect_server_unique_constraints,
)
from ormdantic._migrations.reflection import (
    _reflect_sqlite_snapshot as _reflect_sqlite_snapshot,
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

try:
    _ormdantic: Any | None = importlib.import_module("ormdantic._ormdantic")
except ImportError:  # pragma: no cover - exercised when extension is not built
    _ormdantic = None


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
        return SchemaSnapshot.from_database(self._database)

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
        after = self.snapshot()
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
        return diff_snapshots(before, after)

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
