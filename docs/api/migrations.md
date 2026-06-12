# Migrations

The migration API provides snapshots, diffs, generated plans, migration artifacts, history operations, rollback, repair, and squash helpers.

::: ormdantic.migrations.MigrationManager

## Snapshot Types

The snapshot and plan models live in `ormdantic._migrations.models`.

::: ormdantic._migrations.models.SchemaSnapshot
::: ormdantic._migrations.models.TableSnapshot
::: ormdantic._migrations.models.ColumnSnapshot
::: ormdantic._migrations.models.IndexSnapshot
::: ormdantic._migrations.models.TableCheckSnapshot
::: ormdantic._migrations.models.UniqueConstraintSnapshot
::: ormdantic._migrations.models.ForeignKeyConstraintSnapshot
::: ormdantic._migrations.models.ExclusionConstraintSnapshot
::: ormdantic._migrations.models.NamespaceSnapshot
::: ormdantic._migrations.models.SequenceSnapshot
::: ormdantic._migrations.models.ViewSnapshot
::: ormdantic._migrations.models.EnumTypeSnapshot
::: ormdantic._migrations.models.SchemaDiff
::: ormdantic._migrations.models.MigrationPlan
::: ormdantic._migrations.models.MigrationOperation
::: ormdantic._migrations.models.MigrationWarning
::: ormdantic._migrations.models.MigrationHistoryEntry

## Artifact Type

::: ormdantic._migrations.artifacts.MigrationArtifact
