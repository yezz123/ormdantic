from __future__ import annotations

import pytest

from ormdantic import migrations
from ormdantic._migrations import planning
from ormdantic._migrations.models import (
    ColumnSnapshot,
    EnumTypeSnapshot,
    ExclusionConstraintSnapshot,
    ForeignKeyConstraintSnapshot,
    IndexSnapshot,
    MigrationOperation,
    MigrationPlan,
    NamespaceSnapshot,
    SchemaSnapshot,
    SequenceSnapshot,
    TableCheckSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
    ViewSnapshot,
)


def table(
    name: str,
    columns: list[ColumnSnapshot],
    *,
    schema: str | None = None,
    indexes: list[IndexSnapshot] | None = None,
    unique_constraints: list[list[str]] | None = None,
    named_unique_constraints: list[UniqueConstraintSnapshot] | None = None,
    check_constraints: list[TableCheckSnapshot] | None = None,
    foreign_key_constraints: list[ForeignKeyConstraintSnapshot] | None = None,
    exclusion_constraints: list[ExclusionConstraintSnapshot] | None = None,
    comment: str | None = None,
    tablespace: str | None = None,
    mysql_engine: str | None = None,
    mysql_charset: str | None = None,
    mysql_collation: str | None = None,
    mysql_row_format: str | None = None,
    mysql_key_block_size: int | None = None,
    mysql_pack_keys: bool | None = None,
    mysql_checksum: bool | None = None,
    mysql_delay_key_write: bool | None = None,
    mysql_stats_persistent: bool | None = None,
    mysql_stats_auto_recalc: bool | None = None,
    mysql_stats_sample_pages: int | None = None,
    mysql_avg_row_length: int | None = None,
    mysql_max_rows: int | None = None,
    mysql_min_rows: int | None = None,
    mysql_insert_method: str | None = None,
    mysql_data_directory: str | None = None,
    mysql_index_directory: str | None = None,
    mysql_connection: str | None = None,
    mysql_union: list[str] | None = None,
    mysql_partition_by: str | None = None,
    mysql_partitions: int | None = None,
    mysql_subpartition_by: str | None = None,
    mysql_subpartitions: int | None = None,
    mysql_auto_increment: int | None = None,
    postgres_inherits: list[str] | None = None,
    postgres_with: list[tuple[str, str]] | None = None,
    postgres_using: str | None = None,
    postgres_unlogged: bool = False,
    postgres_partition_by: str | None = None,
    postgres_partition_of: str | None = None,
    postgres_partition_for: str | None = None,
    sqlite_strict: bool = False,
    sqlite_without_rowid: bool = False,
    oracle_compress: int | bool | None = None,
) -> TableSnapshot:
    return TableSnapshot(
        model_key=name.title(),
        name=name,
        primary_key="id",
        schema=schema,
        columns=columns,
        indexes=indexes or [],
        unique_constraints=unique_constraints or [],
        named_unique_constraints=named_unique_constraints or [],
        check_constraints=check_constraints or [],
        foreign_key_constraints=foreign_key_constraints or [],
        exclusion_constraints=exclusion_constraints or [],
        relationships=[],
        comment=comment,
        tablespace=tablespace,
        mysql_engine=mysql_engine,
        mysql_charset=mysql_charset,
        mysql_collation=mysql_collation,
        mysql_row_format=mysql_row_format,
        mysql_key_block_size=mysql_key_block_size,
        mysql_pack_keys=mysql_pack_keys,
        mysql_checksum=mysql_checksum,
        mysql_delay_key_write=mysql_delay_key_write,
        mysql_stats_persistent=mysql_stats_persistent,
        mysql_stats_auto_recalc=mysql_stats_auto_recalc,
        mysql_stats_sample_pages=mysql_stats_sample_pages,
        mysql_avg_row_length=mysql_avg_row_length,
        mysql_max_rows=mysql_max_rows,
        mysql_min_rows=mysql_min_rows,
        mysql_insert_method=mysql_insert_method,
        mysql_data_directory=mysql_data_directory,
        mysql_index_directory=mysql_index_directory,
        mysql_connection=mysql_connection,
        mysql_union=mysql_union or [],
        mysql_partition_by=mysql_partition_by,
        mysql_partitions=mysql_partitions,
        mysql_subpartition_by=mysql_subpartition_by,
        mysql_subpartitions=mysql_subpartitions,
        mysql_auto_increment=mysql_auto_increment,
        postgres_inherits=postgres_inherits or [],
        postgres_with=postgres_with or [],
        postgres_using=postgres_using,
        postgres_unlogged=postgres_unlogged,
        postgres_partition_by=postgres_partition_by,
        postgres_partition_of=postgres_partition_of,
        postgres_partition_for=postgres_partition_for,
        sqlite_strict=sqlite_strict,
        sqlite_without_rowid=sqlite_without_rowid,
        oracle_compress=oracle_compress,
    )


def column(
    name: str,
    kind: str = "str",
    *,
    nullable: bool = False,
    primary_key: bool = False,
    comment: str | None = None,
    unique: bool = False,
    checks: list[tuple[str, str, str]] | None = None,
    foreign_table: str | None = None,
    foreign_column: str | None = None,
    foreign_key_name: str | None = None,
    on_delete: str | None = None,
    on_update: str | None = None,
    sqlite_on_conflict_primary_key: str | None = None,
    sqlite_on_conflict_not_null: str | None = None,
    sqlite_on_conflict_unique: str | None = None,
) -> ColumnSnapshot:
    return ColumnSnapshot(
        name=name,
        kind=kind,
        nullable=nullable,
        primary_key=primary_key,
        comment=comment,
        unique=unique,
        checks=checks or [],
        foreign_table=foreign_table,
        foreign_column=foreign_column,
        foreign_key_name=foreign_key_name,
        on_delete=on_delete,
        on_update=on_update,
        sqlite_on_conflict_primary_key=sqlite_on_conflict_primary_key,
        sqlite_on_conflict_not_null=sqlite_on_conflict_not_null,
        sqlite_on_conflict_unique=sqlite_on_conflict_unique,
    )


def test_public_migration_facade_re_exports_planning_helpers() -> None:
    assert migrations.diff_snapshots is planning.diff_snapshots
    assert migrations.create_migration_artifact is planning.create_migration_artifact
    assert migrations.squash_migrations is planning.squash_migrations
    assert migrations._build_plan is planning._build_plan
    assert migrations._classify_sql_operation is planning._classify_sql_operation
    assert migrations._compile_enum_type_diff is planning._compile_enum_type_diff
    assert migrations._compile_view_diff is planning._compile_view_diff
    assert (
        migrations._set_constraint_comment_sql is planning._set_constraint_comment_sql
    )
    assert migrations._set_enum_type_comment_sql is planning._set_enum_type_comment_sql
    assert migrations._set_index_comment_sql is planning._set_index_comment_sql
    assert migrations._set_index_tablespace_sql is planning._set_index_tablespace_sql
    assert migrations._set_namespace_comment_sql is planning._set_namespace_comment_sql
    assert migrations._set_sequence_comment_sql is planning._set_sequence_comment_sql
    assert migrations._set_view_comment_sql is planning._set_view_comment_sql
    assert migrations._diff_enum_types is planning._diff_enum_types
    assert migrations._diff_views is planning._diff_views


def test_diff_snapshots_reports_columns_indexes_and_constraints() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name", checks=[("length", ">=", "2")]),
                    column(
                        "supplier_id", foreign_table="supplier", foreign_column="id"
                    ),
                ],
                indexes=[IndexSnapshot("flavor_name_idx", ["name"], unique=False)],
                unique_constraints=[["name"]],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name", nullable=True, checks=[("length", ">=", "3")]),
                    column("rating", "int", nullable=True),
                ],
                indexes=[IndexSnapshot("flavor_rating_idx", ["rating"], unique=True)],
                unique_constraints=[["name", "rating"]],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    summary = diff.summary()

    assert "Changed column flavor.name: nullable, checks" in summary
    assert "Added column flavor.rating" in summary
    assert "Removed column flavor.supplier_id" in summary
    assert "Added index flavor_rating_idx on flavor" in summary
    assert "Removed index flavor_name_idx from flavor" in summary
    assert any(change.object_type == "constraint" for change in diff.changes)
    assert diff.has_unsafe_operations
    assert diff.has_destructive_operations


def test_diff_snapshots_distinguishes_schema_qualified_tables() -> None:
    before = SchemaSnapshot(
        tables=[
            table("flavor", [column("id", primary_key=True)], schema="public"),
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                schema="inventory",
            ),
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table("flavor", [column("id", primary_key=True)], schema="public"),
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name", nullable=True),
                ],
                schema="inventory",
            ),
        ]
    )

    diff = planning.diff_snapshots(before, after)

    assert diff.summary() == ["Changed column inventory.flavor.name: nullable"]
    assert diff.changes[0].table == "inventory.flavor"


def test_diff_snapshots_treats_same_table_name_in_different_schemas_as_distinct() -> (
    None
):
    before = SchemaSnapshot(
        tables=[
            table("flavor", [column("id", primary_key=True)], schema="public"),
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table("flavor", [column("id", primary_key=True)], schema="inventory"),
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == [
        "Added table inventory.flavor",
        "Removed table public.flavor",
    ]
    assert [change.table for change in diff.changes] == [
        "inventory.flavor",
        "public.flavor",
    ]
    assert plan.dry_run() == [
        'CREATE TABLE IF NOT EXISTS "inventory"."flavor" '
        '("id" TEXT PRIMARY KEY NOT NULL)',
        'DROP TABLE IF EXISTS "public"."flavor"',
    ]


def test_build_plan_changes_table_comment() -> None:
    before = SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])])
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                comment="Flavor metadata",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed table flavor: comment" in diff.summary()
    assert plan.dry_run() == ["COMMENT ON TABLE \"flavor\" IS 'Flavor metadata'"]
    assert plan.rollback_sql() == ['COMMENT ON TABLE "flavor" IS NULL']
    assert not plan.operations[0].unsafe


def test_build_plan_changes_column_comment() -> None:
    before = SchemaSnapshot(
        tables=[table("flavor", [column("id", primary_key=True), column("name")])]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name", comment="Flavor display name"),
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)
    mysql_plan = planning._build_plan("mysql", before, after)

    assert "Changed column flavor.name: comment" in diff.summary()
    assert plan.dry_run() == [
        'COMMENT ON COLUMN "flavor"."name" IS \'Flavor display name\''
    ]
    assert plan.rollback_sql() == ['COMMENT ON COLUMN "flavor"."name" IS NULL']
    assert not plan.operations[0].unsafe
    assert mysql_plan.dry_run() == [
        "ALTER TABLE `flavor` MODIFY COLUMN `name` TEXT NOT NULL "
        "COMMENT 'Flavor display name'"
    ]
    assert mysql_plan.rollback_sql() == [
        "ALTER TABLE `flavor` MODIFY COLUMN `name` TEXT NOT NULL COMMENT ''"
    ]
    assert not mysql_plan.operations[0].unsafe


def test_build_plan_changes_postgres_table_tablespace() -> None:
    before = SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])])
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                tablespace="fastspace",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed table flavor: tablespace" in diff.summary()
    assert plan.dry_run() == ['ALTER TABLE "flavor" SET TABLESPACE "fastspace"']
    assert plan.rollback_sql() == ['ALTER TABLE "flavor" SET TABLESPACE "pg_default"']
    assert not plan.operations[0].unsafe


def test_build_plan_changes_oracle_table_tablespace() -> None:
    before = SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])])
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                tablespace="fastspace",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("oracle", before, after)

    assert "Changed table flavor: tablespace" in diff.summary()
    assert plan.dry_run() == ['ALTER TABLE "flavor" MOVE TABLESPACE "fastspace"']
    assert plan.rollback_sql() == ['ALTER TABLE "flavor" MOVE']
    assert plan.operations[0].kind == "table_storage"
    assert plan.rollback_operations[0].kind == "table_storage"
    assert not plan.operations[0].unsafe


def test_build_plan_creates_oracle_table_compression_inline() -> None:
    before = SchemaSnapshot.empty()
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                oracle_compress=6,
            )
        ]
    )

    plan = planning._build_plan("oracle", before, after)

    assert plan.dry_run() == [
        'CREATE TABLE "flavor" ("id" VARCHAR2(255) PRIMARY KEY NOT NULL) COMPRESS FOR 6'
    ]
    with pytest.raises(ValueError, match="Oracle table compression"):
        planning._build_plan("sqlite", before, after)


def test_build_plan_recreates_oracle_table_when_compression_changes() -> None:
    before = SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])])
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                oracle_compress=True,
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("oracle", before, after)

    assert "Changed table flavor: oracle_compress" in diff.summary()
    assert diff.warnings
    assert diff.warnings[0].code == "destructive_table_change"
    assert plan.dry_run() == [
        'DROP TABLE "flavor"',
        'CREATE TABLE "flavor" ("id" VARCHAR2(255) PRIMARY KEY NOT NULL) COMPRESS',
    ]
    assert plan.rollback_sql() == [
        'DROP TABLE "flavor"',
        'CREATE TABLE "flavor" ("id" VARCHAR2(255) PRIMARY KEY NOT NULL)',
    ]
    assert plan.operations[0].destructive
    with pytest.raises(ValueError, match="Oracle table compression"):
        planning._build_plan("sqlite", before, after)


def test_build_plan_changes_mysql_and_mariadb_table_tablespace() -> None:
    before = SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])])
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                tablespace="fastspace",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    mysql_plan = planning._build_plan("mysql", before, after)
    mariadb_plan = planning._build_plan("mariadb", before, after)

    assert "Changed table flavor: tablespace" in diff.summary()
    assert mysql_plan.dry_run() == ["ALTER TABLE `flavor` TABLESPACE `fastspace`"]
    assert mysql_plan.rollback_sql() == [
        "ALTER TABLE `flavor` TABLESPACE `innodb_file_per_table`"
    ]
    assert mysql_plan.operations[0].kind == "table_storage"
    assert mysql_plan.rollback_operations[0].kind == "table_storage"
    assert not mysql_plan.operations[0].unsafe
    assert mariadb_plan.dry_run() == mysql_plan.dry_run()
    assert mariadb_plan.rollback_sql() == mysql_plan.rollback_sql()
    assert mariadb_plan.operations[0].kind == "table_storage"
    assert mariadb_plan.rollback_operations[0].kind == "table_storage"
    assert not mariadb_plan.operations[0].unsafe


def test_build_plan_rejects_mssql_table_filegroup_changes() -> None:
    before = SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])])
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                tablespace="fastspace",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)

    assert "Changed table flavor: tablespace" in diff.summary()
    with pytest.raises(ValueError, match="SQL Server table filegroup changes"):
        planning._build_plan("mssql", before, after)


def test_build_plan_changes_mysql_table_options() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                mysql_engine="InnoDB",
                mysql_charset="utf8mb4",
                mysql_collation="utf8mb4_unicode_ci",
                mysql_row_format="DYNAMIC",
                mysql_key_block_size=8,
                mysql_pack_keys=True,
                mysql_checksum=True,
                mysql_delay_key_write=True,
                mysql_stats_persistent=True,
                mysql_stats_auto_recalc=False,
                mysql_stats_sample_pages=32,
                mysql_avg_row_length=64,
                mysql_max_rows=1000,
                mysql_min_rows=10,
                mysql_insert_method="LAST",
                mysql_data_directory="/var/lib/mysql/data",
                mysql_index_directory="/var/lib/mysql/index",
                mysql_connection="mysql://remote.example/db/flavor",
                mysql_union=["flavor_hot", "flavor_cold"],
                mysql_partition_by="HASH (id)",
                mysql_partitions=4,
                mysql_subpartition_by="KEY (id)",
                mysql_subpartitions=2,
                mysql_auto_increment=101,
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                mysql_engine="MyISAM",
                mysql_charset="latin1",
                mysql_collation="latin1_swedish_ci",
                mysql_row_format="COMPACT",
                mysql_key_block_size=4,
                mysql_pack_keys=False,
                mysql_checksum=False,
                mysql_delay_key_write=False,
                mysql_stats_persistent=False,
                mysql_stats_auto_recalc=True,
                mysql_stats_sample_pages=16,
                mysql_avg_row_length=32,
                mysql_max_rows=500,
                mysql_min_rows=5,
                mysql_insert_method="FIRST",
                mysql_data_directory="/srv/mysql/data",
                mysql_index_directory="/srv/mysql/index",
                mysql_connection="mysql://remote.example/db/flavor_archive",
                mysql_union=["flavor_archive_hot", "flavor_archive_cold"],
                mysql_partition_by="LINEAR HASH (id)",
                mysql_partitions=8,
                mysql_subpartition_by="KEY (code)",
                mysql_subpartitions=4,
                mysql_auto_increment=202,
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mysql", before, after)

    assert (
        "Changed table flavor: mysql_engine, mysql_charset, mysql_collation, "
        "mysql_row_format, mysql_key_block_size, mysql_pack_keys, mysql_checksum, "
        "mysql_delay_key_write, mysql_stats_persistent, mysql_stats_auto_recalc, "
        "mysql_stats_sample_pages, mysql_avg_row_length, mysql_max_rows, "
        "mysql_min_rows, mysql_insert_method, mysql_data_directory, "
        "mysql_index_directory, mysql_connection, mysql_union, "
        "mysql_partition_by, mysql_partitions, mysql_subpartition_by, "
        "mysql_subpartitions, mysql_auto_increment"
    ) in diff.summary()
    assert plan.dry_run() == [
        "ALTER TABLE `flavor` ENGINE = MyISAM "
        "DEFAULT CHARACTER SET = latin1 COLLATE = latin1_swedish_ci "
        "ROW_FORMAT = COMPACT KEY_BLOCK_SIZE = 4 PACK_KEYS = 0 CHECKSUM = 0 "
        "DELAY_KEY_WRITE = 0 STATS_PERSISTENT = 0 STATS_AUTO_RECALC = 1 "
        "STATS_SAMPLE_PAGES = 16 AVG_ROW_LENGTH = 32 MAX_ROWS = 500 "
        "MIN_ROWS = 5 INSERT_METHOD = FIRST DATA DIRECTORY = '/srv/mysql/data' "
        "INDEX DIRECTORY = '/srv/mysql/index' "
        "CONNECTION = 'mysql://remote.example/db/flavor_archive' "
        "UNION = (`flavor_archive_hot`, `flavor_archive_cold`) "
        "PARTITION BY LINEAR HASH (id) PARTITIONS 8 "
        "SUBPARTITION BY KEY (code) SUBPARTITIONS 4 "
        "AUTO_INCREMENT = 202"
    ]
    assert plan.rollback_sql() == [
        "ALTER TABLE `flavor` ENGINE = InnoDB "
        "DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci "
        "ROW_FORMAT = DYNAMIC KEY_BLOCK_SIZE = 8 PACK_KEYS = 1 CHECKSUM = 1 "
        "DELAY_KEY_WRITE = 1 STATS_PERSISTENT = 1 STATS_AUTO_RECALC = 0 "
        "STATS_SAMPLE_PAGES = 32 AVG_ROW_LENGTH = 64 MAX_ROWS = 1000 "
        "MIN_ROWS = 10 INSERT_METHOD = LAST DATA DIRECTORY = '/var/lib/mysql/data' "
        "INDEX DIRECTORY = '/var/lib/mysql/index' "
        "CONNECTION = 'mysql://remote.example/db/flavor' "
        "UNION = (`flavor_hot`, `flavor_cold`) "
        "PARTITION BY HASH (id) PARTITIONS 4 "
        "SUBPARTITION BY KEY (id) SUBPARTITIONS 2 "
        "AUTO_INCREMENT = 101"
    ]
    assert plan.operations[0].kind == "table_partition"
    assert plan.operations[0].unsafe


@pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
def test_build_plan_ignores_unmanaged_reflected_mysql_auto_increment(
    dialect: str,
) -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", "int", primary_key=True)],
                comment="old metadata",
                mysql_auto_increment=42,
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", "int", primary_key=True)],
                comment="new metadata",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after, dialect=dialect)
    plan = planning._build_plan(dialect, before, after)

    assert diff.summary() == ["Changed table flavor: comment"]
    assert plan.dry_run() == ["ALTER TABLE `flavor` COMMENT = 'new metadata'"]
    assert plan.rollback_sql() == ["ALTER TABLE `flavor` COMMENT = 'old metadata'"]


@pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
def test_build_plan_keeps_explicit_mysql_auto_increment_targets(
    dialect: str,
) -> None:
    before = SchemaSnapshot(
        tables=[table("flavor", [column("id", "int", primary_key=True)])]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", "int", primary_key=True)],
                mysql_auto_increment=202,
            )
        ]
    )

    diff = planning.diff_snapshots(before, after, dialect=dialect)
    plan = planning._build_plan(dialect, before, after)

    assert diff.summary() == ["Changed table flavor: mysql_auto_increment"]
    assert plan.dry_run() == ["ALTER TABLE `flavor` AUTO_INCREMENT = 202"]


def test_build_plan_changes_postgres_table_inheritance() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                postgres_inherits=["old_base"],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                postgres_inherits=["base_flavor"],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed table flavor: postgres_inherits" in diff.summary()
    assert plan.dry_run() == [
        'ALTER TABLE "flavor" NO INHERIT "old_base"',
        'ALTER TABLE "flavor" INHERIT "base_flavor"',
    ]
    assert plan.rollback_sql() == [
        'ALTER TABLE "flavor" NO INHERIT "base_flavor"',
        'ALTER TABLE "flavor" INHERIT "old_base"',
    ]
    assert [operation.kind for operation in plan.operations] == [
        "table_storage",
        "table_storage",
    ]
    assert not any(operation.unsafe for operation in plan.operations)


def test_build_plan_changes_postgres_storage_parameters() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                postgres_with=[
                    ("fillfactor", "80"),
                    ("autovacuum_enabled", "true"),
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                postgres_with=[
                    ("fillfactor", "70"),
                    ("toast.autovacuum_enabled", "false"),
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed table flavor: postgres_with" in diff.summary()
    assert plan.dry_run() == [
        'ALTER TABLE "flavor" SET (fillfactor = 70, toast.autovacuum_enabled = false)',
        'ALTER TABLE "flavor" RESET (autovacuum_enabled)',
    ]
    assert plan.rollback_sql() == [
        'ALTER TABLE "flavor" SET (autovacuum_enabled = true, fillfactor = 80)',
        'ALTER TABLE "flavor" RESET (toast.autovacuum_enabled)',
    ]
    assert [operation.kind for operation in plan.operations] == [
        "table_storage",
        "table_storage",
    ]
    assert not any(operation.unsafe for operation in plan.operations)


def test_build_plan_changes_postgres_table_access_method() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                postgres_using="heap",
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                postgres_using="custom_heap",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed table flavor: postgres_using" in diff.summary()
    assert plan.dry_run() == ['ALTER TABLE "flavor" SET ACCESS METHOD "custom_heap"']
    assert plan.rollback_sql() == ['ALTER TABLE "flavor" SET ACCESS METHOD "heap"']
    assert plan.operations[0].kind == "table_storage"
    assert plan.rollback_operations[0].kind == "table_storage"
    assert not plan.operations[0].unsafe


def test_build_plan_changes_postgres_unlogged_persistence() -> None:
    before = SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])])
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True)],
                postgres_unlogged=True,
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed table flavor: postgres_unlogged" in diff.summary()
    assert not diff.has_unsafe_operations
    assert plan.dry_run() == ['ALTER TABLE "flavor" SET UNLOGGED']
    assert plan.rollback_sql() == ['ALTER TABLE "flavor" SET LOGGED']
    assert plan.operations[0].kind == "table_storage"
    assert plan.rollback_operations[0].kind == "table_storage"
    assert not plan.operations[0].unsafe


def test_build_plan_rebuilds_changed_sqlite_table_options() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", "int", primary_key=True),
                    column("name"),
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", "int", primary_key=True),
                    column("name"),
                ],
                sqlite_strict=True,
                sqlite_without_rowid=True,
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("sqlite", before, after)

    assert "Changed table flavor: sqlite_strict, sqlite_without_rowid" in diff.summary()
    assert diff.has_unsafe_operations
    assert diff.has_destructive_operations
    assert plan.safety["requires_rebuild"]
    assert [operation.metadata.get("phase") for operation in plan.operations] == [
        "drop_temp",
        "create_temp",
        "copy_rows",
        "drop_old",
        "rename",
    ]
    assert plan.dry_run() == [
        'DROP TABLE IF EXISTS "__ormdantic_rebuild_flavor"',
        'CREATE TABLE IF NOT EXISTS "__ormdantic_rebuild_flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL, "name" TEXT NOT NULL) '
        "STRICT, WITHOUT ROWID",
        'INSERT INTO "__ormdantic_rebuild_flavor" ("id", "name") '
        'SELECT "id", "name" FROM "flavor"',
        'DROP TABLE "flavor"',
        'ALTER TABLE "__ormdantic_rebuild_flavor" RENAME TO "flavor"',
    ]
    assert all(operation.unsafe for operation in plan.operations)
    assert all(operation.destructive for operation in plan.operations)


def test_diff_snapshots_reports_sqlite_conflict_clause_changes() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", "int", primary_key=True),
                    column("name", unique=True),
                    column("code"),
                ],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column(
                        "id",
                        "int",
                        primary_key=True,
                        sqlite_on_conflict_primary_key="REPLACE",
                    ),
                    column(
                        "name",
                        unique=True,
                        sqlite_on_conflict_not_null="FAIL",
                        sqlite_on_conflict_unique="IGNORE",
                    ),
                    column("code"),
                ],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        sqlite_on_conflict="ABORT",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    constraints = planning._constraints(after.tables[0])
    summary = "\n".join(diff.summary())

    assert "sqlite_on_conflict_primary_key" in summary
    assert "sqlite_on_conflict_not_null" in summary
    assert "sqlite_on_conflict_unique" in summary
    assert "Changed unique constraint flavor_code_unique on flavor" in summary
    assert constraints["flavor_unique_0"]["sqlite_on_conflict"] == "IGNORE"
    assert constraints["flavor_code_unique"]["sqlite_on_conflict"] == "ABORT"


def test_diff_snapshots_reports_namespace_enum_index_and_constraint_metadata_edges() -> (
    None
):
    before_index = IndexSnapshot(
        "flavor_meta_idx",
        ["name"],
        method="btree",
        comment="old comment",
        postgres_tablespace="old_pg_space",
        mssql_filegroup="old_fg",
        mssql_clustered=False,
        oracle_tablespace="old_ora_space",
        mysql_prefix="old_prefix",
        mysql_length={"name": 12},
        mysql_using="BTREE",
        postgres_ops={"name": "text_pattern_ops"},
        mysql_visible=True,
        oracle_bitmap=False,
        oracle_compress=None,
        postgres_nulls_not_distinct=False,
    )
    after_index = IndexSnapshot(
        "flavor_meta_idx",
        ["name"],
        method="BTREE",
        comment="new comment",
        postgres_tablespace="new_pg_space",
        mssql_filegroup="new_fg",
        mssql_clustered=True,
        oracle_tablespace="new_ora_space",
        mysql_prefix="new_prefix",
        mysql_length={"name": 24},
        mysql_using="HASH",
        postgres_ops={"name": "varchar_pattern_ops"},
        mysql_visible=False,
        oracle_bitmap=True,
        oracle_compress=2,
        postgres_nulls_not_distinct=True,
    )
    before = SchemaSnapshot(
        namespaces=[
            NamespaceSnapshot("legacy", "old namespace"),
            NamespaceSnapshot("shared", "old shared"),
        ],
        enum_types=[EnumTypeSnapshot("flavor_state", ["draft"], schema="app")],
        tables=[
            table(
                "flavor",
                [column("id", "int", primary_key=True), column("name")],
                indexes=[before_index],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_unique",
                        ["name"],
                        comment="old unique",
                    )
                ],
            )
        ],
    )
    after = SchemaSnapshot(
        namespaces=[
            NamespaceSnapshot("shared", "new shared"),
            NamespaceSnapshot("analytics", "new namespace"),
        ],
        enum_types=[
            EnumTypeSnapshot(
                "flavor_state",
                ["draft", "active"],
                schema="app",
                comment="state values",
            ),
            EnumTypeSnapshot("flavor_label", ["seasonal"]),
        ],
        tables=[
            table(
                "flavor",
                [column("id", "int", primary_key=True), column("name")],
                indexes=[after_index],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_unique",
                        ["name"],
                        comment="new unique",
                    )
                ],
            )
        ],
    )

    diff = planning.diff_snapshots(before, after, dialect="postgresql")
    summary = "\n".join(diff.summary())

    assert "Added namespace analytics" in summary
    assert "Removed namespace legacy" in summary
    assert "Changed namespace shared: comment" in summary
    assert "Added enum type flavor_label" in summary
    assert "Changed enum type app.flavor_state" in summary
    assert "Changed index flavor_meta_idx on flavor:" in summary
    for field in (
        "comment",
        "postgres_tablespace",
        "mssql_filegroup",
        "mssql_clustered",
        "oracle_tablespace",
        "oracle_bitmap",
        "oracle_compress",
        "postgres_ops",
        "postgres_nulls_not_distinct",
        "mysql_prefix",
        "mysql_length",
        "mysql_using",
        "mysql_visible",
    ):
        assert field in summary
    assert "Changed unique constraint flavor_name_unique on flavor: comment" in summary
    assert planning._normalized_default_index_method(None) is None
    assert planning._normalized_default_index_method("HASH") == "hash"


def test_identity_bounds_and_sqlite_rebuild_markers_cover_edge_branches() -> None:
    integer_column = ColumnSnapshot(
        "id",
        "int",
        False,
        True,
        identity_increment=-5,
        identity_min_value=-(2**31),
        identity_no_min_value=True,
    )
    defaulted_integer = ColumnSnapshot(
        "id",
        "int",
        False,
        True,
        identity_increment=-5,
        identity_no_min_value=True,
    )
    oracle_column = ColumnSnapshot(
        "id",
        "int",
        False,
        True,
        identity_increment=-1,
        identity_max_value=-1,
    )
    custom_bound = ColumnSnapshot(
        "id",
        "int",
        False,
        True,
        identity_increment=1,
        identity_min_value=10,
    )

    assert not planning._identity_bound_changed(
        integer_column,
        defaulted_integer,
        "identity_min_value",
        "identity_no_min_value",
        dialect="postgresql",
    )
    assert not planning._identity_bound_changed(
        defaulted_integer,
        integer_column,
        "identity_min_value",
        "identity_no_min_value",
        dialect="postgresql",
    )
    assert planning._identity_bound_changed(
        integer_column,
        ColumnSnapshot("id", "int", False, True, identity_min_value=5),
        "identity_min_value",
        "identity_no_min_value",
        dialect="postgresql",
    )
    assert planning._identity_bound_changed(
        integer_column,
        ColumnSnapshot("id", "int", False, True, identity_min_value=-(2**31)),
        "identity_min_value",
        "identity_no_min_value",
        dialect="postgresql",
    )
    assert planning._identity_bound_matches_default(
        oracle_column,
        "identity_max_value",
        -1,
        dialect="oracle",
    )
    assert not planning._identity_bound_matches_default(
        custom_bound,
        "identity_min_value",
        10,
        dialect=None,
    )
    assert planning._default_identity_bound("sqlite", custom_bound, bound="min") is None
    assert planning._is_destructive_column_change(["comment", "foreign_column"])

    operations = [
        MigrationOperation(
            'DROP TABLE "other"',
            kind="drop_table",
            table="other",
            metadata={},
        ),
        MigrationOperation(
            'DROP TABLE "flavor"',
            kind="drop_table",
            table="flavor",
            metadata={},
        ),
        MigrationOperation(
            'CREATE TABLE "flavor" ("id" INTEGER)',
            kind="create_table",
            table="flavor",
            metadata={},
        ),
        MigrationOperation(
            'ALTER TABLE "flavor" ADD COLUMN "name" TEXT',
            kind="add_column",
            table="flavor",
            metadata={},
        ),
    ]
    schema_diff = planning.diff_snapshots(
        SchemaSnapshot(
            tables=[
                table("flavor", [column("id", "int", primary_key=True)]),
                table("other", [column("id", "int", primary_key=True)]),
            ]
        ),
        SchemaSnapshot(
            tables=[
                table(
                    "flavor",
                    [column("id", "int", primary_key=True)],
                    sqlite_strict=True,
                ),
                table("other", [column("id", "int", primary_key=True)]),
            ]
        ),
    )

    planning._mark_sqlite_table_option_rebuilds(operations, schema_diff)

    assert operations[0].requires_rebuild is False
    assert operations[1].requires_rebuild is True
    assert operations[2].requires_rebuild is True
    assert operations[3].requires_rebuild is False
    assert operations[1].metadata["sqlite_table_options_rebuild"] is True


def test_build_plan_recreates_postgres_partition_key_changes() -> None:
    before = SchemaSnapshot(
        tables=[table("flavor", [column("id", "int", primary_key=True)])]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", "int", primary_key=True)],
                postgres_partition_by="RANGE (id)",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed table flavor: postgres_partition_by" in diff.summary()
    assert diff.has_destructive_operations
    assert plan.dry_run() == [
        'DROP TABLE IF EXISTS "flavor"',
        'CREATE TABLE IF NOT EXISTS "flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL) PARTITION BY RANGE (id)',
    ]
    assert plan.rollback_sql() == [
        'DROP TABLE IF EXISTS "flavor"',
        'CREATE TABLE IF NOT EXISTS "flavor" ("id" INTEGER PRIMARY KEY NOT NULL)',
    ]
    assert [operation.kind for operation in plan.operations] == [
        "drop_table",
        "create_table",
    ]
    assert all(operation.unsafe for operation in plan.operations)
    assert all(operation.destructive for operation in plan.operations)


def test_build_plan_rebinds_postgres_child_partition_without_recreate() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor_2026",
                [column("id", "int", primary_key=True)],
                postgres_partition_of="flavor",
                postgres_partition_for="FOR VALUES FROM (2026) TO (2027)",
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor_2026",
                [column("id", "int", primary_key=True)],
                postgres_partition_of="flavor",
                postgres_partition_for="FOR VALUES FROM (2026) TO (2028)",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed table flavor_2026: postgres_partition_for" in diff.summary()
    assert diff.has_unsafe_operations
    assert not diff.has_destructive_operations
    assert plan.dry_run() == [
        'ALTER TABLE "flavor" DETACH PARTITION "flavor_2026"',
        'ALTER TABLE "flavor" ATTACH PARTITION "flavor_2026" '
        "FOR VALUES FROM (2026) TO (2028)",
    ]
    assert plan.rollback_sql() == [
        'ALTER TABLE "flavor" DETACH PARTITION "flavor_2026"',
        'ALTER TABLE "flavor" ATTACH PARTITION "flavor_2026" '
        "FOR VALUES FROM (2026) TO (2027)",
    ]
    assert [operation.kind for operation in plan.operations] == [
        "table_partition",
        "table_partition",
    ]
    assert all(operation.unsafe for operation in plan.operations)
    assert not any(operation.destructive for operation in plan.operations)


def test_build_plan_attaches_and_detaches_postgres_child_partitions() -> None:
    regular = SchemaSnapshot(
        tables=[table("flavor_2026", [column("id", "int", primary_key=True)])]
    )
    partitioned = SchemaSnapshot(
        tables=[
            table(
                "flavor_2026",
                [column("id", "int", primary_key=True)],
                postgres_partition_of="flavor",
                postgres_partition_for="FOR VALUES FROM (2026) TO (2027)",
            )
        ]
    )

    attach_diff = planning.diff_snapshots(regular, partitioned)
    attach_plan = planning._build_plan("postgresql", regular, partitioned)
    detach_diff = planning.diff_snapshots(partitioned, regular)
    detach_plan = planning._build_plan("postgresql", partitioned, regular)

    assert (
        "Changed table flavor_2026: postgres_partition_of, postgres_partition_for"
        in attach_diff.summary()
    )
    assert attach_diff.has_unsafe_operations
    assert not attach_diff.has_destructive_operations
    assert attach_plan.dry_run() == [
        'ALTER TABLE "flavor" ATTACH PARTITION "flavor_2026" '
        "FOR VALUES FROM (2026) TO (2027)"
    ]
    assert attach_plan.rollback_sql() == [
        'ALTER TABLE "flavor" DETACH PARTITION "flavor_2026"'
    ]
    assert attach_plan.operations[0].kind == "table_partition"
    assert attach_plan.operations[0].unsafe
    assert not attach_plan.operations[0].destructive

    assert (
        "Changed table flavor_2026: postgres_partition_of, postgres_partition_for"
        in detach_diff.summary()
    )
    assert detach_diff.has_unsafe_operations
    assert not detach_diff.has_destructive_operations
    assert detach_plan.dry_run() == [
        'ALTER TABLE "flavor" DETACH PARTITION "flavor_2026"'
    ]
    assert detach_plan.rollback_sql() == [
        'ALTER TABLE "flavor" ATTACH PARTITION "flavor_2026" '
        "FOR VALUES FROM (2026) TO (2027)"
    ]
    assert detach_plan.operations[0].kind == "table_partition"
    assert detach_plan.operations[0].unsafe
    assert not detach_plan.operations[0].destructive


def test_build_plan_recreates_changed_advanced_index() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
        column("rating", "int"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[IndexSnapshot("flavor_name_idx", ["name"])],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        where="name IS NOT NULL",
                        include_columns=["rating"],
                        method="btree",
                        expressions=["LOWER(name)"],
                        postgres_with=[("fillfactor", "70")],
                        postgres_ops={"name": "text_pattern_ops"},
                        comment="Flavor lookup index",
                        postgres_tablespace="fastspace",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)
    sql = plan.dry_run()

    assert "Changed index flavor_name_idx on flavor" in diff.summary()
    assert 'DROP INDEX IF EXISTS "flavor_name_idx"' in sql
    assert (
        'CREATE INDEX IF NOT EXISTS "flavor_name_idx" ON "flavor" '
        'USING btree ("name" text_pattern_ops, LOWER(name)) INCLUDE ("rating") '
        "WITH (fillfactor = 70) WHERE name IS NOT NULL"
    ) in sql
    assert 'ALTER INDEX "flavor_name_idx" SET TABLESPACE "fastspace"' in sql
    assert "COMMENT ON INDEX \"flavor_name_idx\" IS 'Flavor lookup index'" in sql
    assert plan.rollback_operations


def test_build_plan_recreates_postgres_index_when_operator_classes_change() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                        postgres_ops={"name": "text_pattern_ops"},
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == ["Changed index flavor_name_idx on flavor: postgres_ops"]
    assert plan.dry_run() == [
        'DROP INDEX IF EXISTS "flavor_name_idx"',
        'CREATE INDEX IF NOT EXISTS "flavor_name_idx" '
        'ON "flavor" ("name" text_pattern_ops)',
        "COMMENT ON INDEX \"flavor_name_idx\" IS 'Flavor lookup index'",
    ]
    assert plan.rollback_sql() == [
        'DROP INDEX IF EXISTS "flavor_name_idx"',
        'CREATE INDEX IF NOT EXISTS "flavor_name_idx" ON "flavor" ("name")',
        "COMMENT ON INDEX \"flavor_name_idx\" IS 'Flavor lookup index'",
    ]


def test_build_plan_creates_postgres_unique_index_nulls_not_distinct() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name"), column("rating")],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name"), column("rating")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_unique_idx",
                        ["name"],
                        unique=True,
                        include_columns=["rating"],
                        postgres_nulls_not_distinct=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == ["Added index flavor_name_unique_idx on flavor"]
    assert plan.dry_run() == [
        'CREATE UNIQUE INDEX IF NOT EXISTS "flavor_name_unique_idx" '
        'ON "flavor" ("name") INCLUDE ("rating") NULLS NOT DISTINCT'
    ]


def test_build_plan_recreates_postgres_unique_index_when_nulls_change() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_unique_idx",
                        ["name"],
                        unique=True,
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_unique_idx",
                        ["name"],
                        unique=True,
                        postgres_nulls_not_distinct=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == [
        "Changed index flavor_name_unique_idx on flavor: postgres_nulls_not_distinct"
    ]
    assert plan.dry_run() == [
        'DROP INDEX IF EXISTS "flavor_name_unique_idx"',
        'CREATE UNIQUE INDEX IF NOT EXISTS "flavor_name_unique_idx" '
        'ON "flavor" ("name") NULLS NOT DISTINCT',
    ]
    assert plan.rollback_sql() == [
        'DROP INDEX IF EXISTS "flavor_name_unique_idx"',
        'CREATE UNIQUE INDEX IF NOT EXISTS "flavor_name_unique_idx" '
        'ON "flavor" ("name")',
    ]


def test_build_plan_changes_index_comment_without_recreating_index() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Old lookup index",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == ["Changed index flavor_name_idx on flavor: comment"]
    assert not diff.has_unsafe_operations
    assert plan.dry_run() == [
        "COMMENT ON INDEX \"flavor_name_idx\" IS 'Flavor lookup index'"
    ]
    assert plan.rollback_sql() == [
        "COMMENT ON INDEX \"flavor_name_idx\" IS 'Old lookup index'"
    ]


def test_build_plan_changes_mssql_index_comment_without_recreating_index() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                schema="inventory",
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Old lookup index",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                schema="inventory",
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == [
        "Changed index flavor_name_idx on inventory.flavor: comment"
    ]
    assert not diff.has_unsafe_operations
    assert len(plan.dry_run()) == 1
    assert plan.operations[0].kind == "comment_index"
    assert "sys.sp_updateextendedproperty" in plan.dry_run()[0]
    assert "@level1type = N'TABLE'" in plan.dry_run()[0]
    assert "@level2type = N'INDEX'" in plan.dry_run()[0]
    assert "@level2name = N'flavor_name_idx'" in plan.dry_run()[0]
    assert "@value = N'Flavor lookup index'" in plan.dry_run()[0]
    assert "@value = N'Old lookup index'" in plan.rollback_sql()[0]


def test_build_plan_changes_index_postgres_tablespace_without_recreating_index() -> (
    None
):
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[IndexSnapshot("flavor_name_idx", ["name"])],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        postgres_tablespace="fastspace",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == [
        "Changed index flavor_name_idx on flavor: postgres_tablespace"
    ]
    assert not diff.has_unsafe_operations
    assert plan.dry_run() == [
        'ALTER INDEX "flavor_name_idx" SET TABLESPACE "fastspace"'
    ]
    assert plan.rollback_sql() == [
        'ALTER INDEX "flavor_name_idx" SET TABLESPACE "pg_default"'
    ]
    assert plan.operations[0].kind == "index_storage"


def test_build_plan_creates_mssql_index_filegroups_inline() -> None:
    before = SchemaSnapshot(
        tables=[table("flavor", [column("id", primary_key=True), column("name")])]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mssql_filegroup="indexspace",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == ["Added index flavor_name_idx on flavor"]
    assert plan.dry_run() == [
        "CREATE INDEX [flavor_name_idx] ON [flavor] ([name]) ON [indexspace]"
    ]


def test_build_plan_creates_mssql_clustered_indexes_inline() -> None:
    before = SchemaSnapshot(
        tables=[table("flavor", [column("id", primary_key=True), column("name")])]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mssql_clustered=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == ["Added index flavor_name_idx on flavor"]
    assert plan.dry_run() == [
        "CREATE CLUSTERED INDEX [flavor_name_idx] ON [flavor] ([name])"
    ]


def test_build_plan_creates_mssql_clustered_indexes_with_nonclustered_primary_key() -> (
    None
):
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mssql_clustered=True,
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("mssql", SchemaSnapshot.empty(), after)

    assert "PRIMARY KEY NONCLUSTERED" in plan.dry_run()[0]
    assert (
        "CREATE CLUSTERED INDEX [flavor_name_idx] ON [flavor] ([name])"
        in plan.dry_run()
    )


def test_build_plan_creates_mssql_filtered_include_indexes_inline() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name"),
                    column("rating", "int"),
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name"),
                    column("rating", "int"),
                ],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        include_columns=["rating"],
                        where="[name] IS NOT NULL",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == ["Added index flavor_name_idx on flavor"]
    assert plan.dry_run() == [
        "CREATE INDEX [flavor_name_idx] ON [flavor] ([name]) "
        "INCLUDE ([rating]) WHERE [name] IS NOT NULL"
    ]


def test_build_plan_changes_mssql_index_filegroup_by_recreating_index() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[IndexSnapshot("flavor_name_idx", ["name"])],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                        mssql_filegroup="indexspace",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == [
        "Changed index flavor_name_idx on flavor: comment, mssql_filegroup"
    ]
    assert plan.dry_run()[:2] == [
        "DROP INDEX [flavor_name_idx] ON [flavor]",
        "CREATE INDEX [flavor_name_idx] ON [flavor] ([name]) ON [indexspace]",
    ]
    assert "sys.sp_addextendedproperty" in plan.dry_run()[2]
    assert plan.rollback_sql() == [
        "DROP INDEX [flavor_name_idx] ON [flavor]",
        "CREATE INDEX [flavor_name_idx] ON [flavor] ([name])",
    ]


def test_build_plan_creates_oracle_index_tablespaces_inline() -> None:
    before = SchemaSnapshot(
        tables=[table("flavor", [column("id", primary_key=True), column("name")])]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name", "id"],
                        oracle_tablespace="oraclespace",
                        oracle_bitmap=True,
                        oracle_compress=2,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("oracle", before, after)

    assert diff.summary() == ["Added index flavor_name_idx on flavor"]
    assert plan.dry_run() == [
        'CREATE BITMAP INDEX "flavor_name_idx" ON "flavor" ("name", "id") '
        'COMPRESS 2 TABLESPACE "oraclespace"'
    ]


def test_build_plan_changes_oracle_index_tablespace_by_recreating_index() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[IndexSnapshot("flavor_name_idx", ["name"])],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        oracle_tablespace="oraclespace",
                        oracle_bitmap=True,
                        oracle_compress=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("oracle", before, after)

    assert diff.summary() == [
        "Changed index flavor_name_idx on flavor: "
        "oracle_tablespace, oracle_bitmap, oracle_compress"
    ]
    assert plan.dry_run() == [
        'DROP INDEX "flavor_name_idx"',
        'CREATE BITMAP INDEX "flavor_name_idx" ON "flavor" ("name") '
        'COMPRESS TABLESPACE "oraclespace"',
    ]
    assert plan.rollback_sql() == [
        'DROP INDEX "flavor_name_idx"',
        'CREATE INDEX "flavor_name_idx" ON "flavor" ("name")',
    ]


def test_build_plan_creates_mysql_index_comments_inline() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("mysql", SchemaSnapshot.empty(), after)

    assert (
        "CREATE INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "COMMENT 'Flavor lookup index'"
    ) in plan.dry_run()


def test_build_plan_creates_mysql_index_prefix_lengths_inline() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name"), column("code")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_code_idx",
                        ["name", "code"],
                        comment="Flavor lookup index",
                        mysql_length={"name": 12, "code": 6},
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("mysql", SchemaSnapshot.empty(), after)

    assert (
        "CREATE INDEX `flavor_name_code_idx` "
        "ON `flavor` (`name`(12), `code`(6)) "
        "COMMENT 'Flavor lookup index'"
    ) in plan.dry_run()


def test_build_plan_creates_mysql_index_using_methods_inline() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mysql_using="HASH",
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("mysql", SchemaSnapshot.empty(), after)

    assert (
        "CREATE INDEX `flavor_name_idx` USING HASH ON `flavor` (`name`)"
        in plan.dry_run()
    )


def test_build_plan_creates_mysql_index_visibility_inline() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Hidden lookup index",
                        mysql_visible=False,
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("mysql", SchemaSnapshot.empty(), after)

    assert (
        "CREATE INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "INVISIBLE COMMENT 'Hidden lookup index'"
    ) in plan.dry_run()


def test_build_plan_creates_mysql_index_prefixes_inline() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Search lookup index",
                        mysql_prefix="FULLTEXT",
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("mysql", SchemaSnapshot.empty(), after)

    assert (
        "CREATE FULLTEXT INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "COMMENT 'Search lookup index'"
    ) in plan.dry_run()


def test_build_plan_recreates_mysql_index_when_comment_changes() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Old lookup index",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mysql", before, after)

    assert diff.summary() == ["Changed index flavor_name_idx on flavor: comment"]
    assert not diff.has_unsafe_operations
    assert plan.dry_run() == [
        "DROP INDEX `flavor_name_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "COMMENT 'Flavor lookup index'",
    ]
    assert plan.rollback_sql() == [
        "DROP INDEX `flavor_name_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "COMMENT 'Old lookup index'",
    ]
    assert plan.has_unsafe_operations
    assert plan.safety["unsafe"] is True


def test_build_plan_recreates_mysql_index_when_prefix_lengths_change() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
        column("code"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_code_idx",
                        ["name", "code"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_code_idx",
                        ["name", "code"],
                        comment="Flavor lookup index",
                        mysql_length={"name": 12, "code": 6},
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mysql", before, after)

    assert diff.summary() == [
        "Changed index flavor_name_code_idx on flavor: mysql_length"
    ]
    assert plan.dry_run() == [
        "DROP INDEX `flavor_name_code_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_code_idx` "
        "ON `flavor` (`name`(12), `code`(6)) "
        "COMMENT 'Flavor lookup index'",
    ]
    assert plan.rollback_sql() == [
        "DROP INDEX `flavor_name_code_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_code_idx` ON `flavor` (`name`, `code`) "
        "COMMENT 'Flavor lookup index'",
    ]


def test_build_plan_recreates_mysql_index_when_using_method_changes() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                        mysql_using="HASH",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mysql", before, after)

    assert diff.summary() == ["Changed index flavor_name_idx on flavor: mysql_using"]
    assert plan.dry_run() == [
        "DROP INDEX `flavor_name_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_idx` USING HASH ON `flavor` (`name`) "
        "COMMENT 'Flavor lookup index'",
    ]
    assert plan.rollback_sql() == [
        "DROP INDEX `flavor_name_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "COMMENT 'Flavor lookup index'",
    ]


def test_build_plan_recreates_mysql_index_when_visibility_changes() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                        mysql_visible=False,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mysql", before, after)

    assert diff.summary() == ["Changed index flavor_name_idx on flavor: mysql_visible"]
    assert plan.dry_run() == [
        "DROP INDEX `flavor_name_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "INVISIBLE COMMENT 'Flavor lookup index'",
    ]
    assert plan.rollback_sql() == [
        "DROP INDEX `flavor_name_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "COMMENT 'Flavor lookup index'",
    ]


def test_build_plan_normalizes_mysql_visible_default() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[IndexSnapshot("flavor_name_idx", ["name"])],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mysql_visible=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mysql", before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


def test_build_plan_recreates_mysql_index_when_prefix_changes() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Search lookup index",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Search lookup index",
                        mysql_prefix="FULLTEXT",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mysql", before, after)

    assert diff.summary() == ["Changed index flavor_name_idx on flavor: mysql_prefix"]
    assert plan.dry_run() == [
        "DROP INDEX `flavor_name_idx` ON `flavor`",
        "CREATE FULLTEXT INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "COMMENT 'Search lookup index'",
    ]
    assert plan.rollback_sql() == [
        "DROP INDEX `flavor_name_idx` ON `flavor`",
        "CREATE INDEX `flavor_name_idx` ON `flavor` (`name`) "
        "COMMENT 'Search lookup index'",
    ]


def test_build_plan_rejects_index_comments_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        comment="Flavor lookup index",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="index comments"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_index_tablespaces_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        postgres_tablespace="fastspace",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="index tablespaces"):
        planning._build_plan("mysql", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_postgres_index_operator_classes_on_unsupported_dialects() -> (
    None
):
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        postgres_ops={"name": "text_pattern_ops"},
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="operator classes"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_postgres_index_nulls_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_unique_idx",
                        ["name"],
                        unique=True,
                        postgres_nulls_not_distinct=True,
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="NULLS NOT DISTINCT"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_mysql_index_prefix_lengths_on_unsupported_dialects() -> (
    None
):
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mysql_length={"name": 12},
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="index prefix lengths"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_mysql_index_prefixes_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mysql_prefix="FULLTEXT",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="index prefixes"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_mysql_index_using_methods_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mysql_using="HASH",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="USING methods"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_mysql_index_visibility_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mysql_visible=False,
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="index visibility"):
        planning._build_plan("mariadb", SchemaSnapshot.empty(), after)
    with pytest.raises(ValueError, match="index visibility"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_mysql_index_prefix_with_using_method() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mysql_prefix="FULLTEXT",
                        mysql_using="HASH",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="cannot be combined with USING methods"):
        planning._build_plan("mysql", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_mssql_index_filegroups_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mssql_filegroup="indexspace",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="SQL Server index filegroups"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_mssql_clustered_indexes_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        mssql_clustered=True,
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="SQL Server clustered indexes"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_oracle_index_tablespaces_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
                indexes=[
                    IndexSnapshot(
                        "flavor_name_idx",
                        ["name"],
                        oracle_tablespace="oraclespace",
                        oracle_bitmap=True,
                        oracle_compress=True,
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="Oracle index tablespaces"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_treats_default_postgres_btree_method_as_omitted() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[IndexSnapshot("flavor_name_idx", ["name"])],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                indexes=[IndexSnapshot("flavor_name_idx", ["name"], method="btree")],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.changes == []
    assert plan.is_empty()


def test_build_plan_recreates_changed_table_check_constraint() -> None:
    columns = [
        column("id", primary_key=True),
        column("rating", "int"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_rating_range_check",
                        "rating BETWEEN 0 AND 100",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_rating_range_check",
                        "rating BETWEEN 1 AND 100",
                        validated=False,
                        no_inherit=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)
    sql = plan.dry_run()

    assert "Changed check constraint flavor_rating_range_check on flavor" in (
        diff.summary()
    )
    assert 'ALTER TABLE "flavor" DROP CONSTRAINT "flavor_rating_range_check"' in sql
    assert (
        'ALTER TABLE "flavor" ADD CONSTRAINT '
        '"flavor_rating_range_check" CHECK (rating BETWEEN 1 AND 100) '
        "NO INHERIT NOT VALID"
    ) in sql
    assert plan.rollback_operations


def test_build_plan_treats_catalog_wrapped_table_check_as_equivalent() -> None:
    columns = [
        column("id", primary_key=True),
        column("rating", "int"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_rating_check",
                        "([rating]>=(0))",
                        comment="Rating guard",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_rating_check",
                        "rating >= 0",
                        comment="Rating guard",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


def test_build_plan_creates_mysql_not_enforced_check_constraint() -> None:
    columns = [
        column("id", primary_key=True),
        column("rating", "int"),
    ]
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_rating_check",
                        "rating >= 0",
                        validated=False,
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("mysql", SchemaSnapshot.empty(), after)

    assert plan.dry_run() == [
        "CREATE TABLE IF NOT EXISTS `flavor` "
        "(`id` VARCHAR(255) NOT NULL PRIMARY KEY, `rating` INTEGER NOT NULL, "
        "CONSTRAINT `flavor_rating_check` CHECK (rating >= 0) NOT ENFORCED)"
    ]
    with pytest.raises(ValueError, match="constraint validation toggles"):
        planning._build_plan("mariadb", SchemaSnapshot.empty(), after)


def test_build_plan_recreates_changed_named_unique_constraint() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
        column("rating", "int"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_named_unique",
                        ["name"],
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_named_unique",
                        ["name"],
                        nulls_not_distinct=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)
    sql = plan.dry_run()

    assert "Changed unique constraint flavor_name_named_unique on flavor" in (
        diff.summary()
    )
    assert 'ALTER TABLE "flavor" DROP CONSTRAINT "flavor_name_named_unique"' in sql
    assert (
        'ALTER TABLE "flavor" ADD CONSTRAINT "flavor_name_named_unique" '
        'UNIQUE NULLS NOT DISTINCT ("name")'
    ) in sql
    assert (
        'ALTER TABLE "flavor" ADD CONSTRAINT "flavor_name_named_unique" UNIQUE ("name")'
    ) in plan.rollback_sql()
    assert plan.rollback_operations


def test_build_plan_creates_postgres_unique_include_columns_inline() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name"),
                    column("rating", "int"),
                ],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_unique",
                        ["name"],
                        postgres_include=["rating"],
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("postgresql", SchemaSnapshot.empty(), after)

    assert (
        'CONSTRAINT "flavor_name_unique" UNIQUE ("name") INCLUDE ("rating")'
        in plan.dry_run()[0]
    )


def test_build_plan_recreates_postgres_unique_when_include_columns_change() -> None:
    columns = [
        column("id", primary_key=True),
        column("name"),
        column("rating", "int"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_unique",
                        ["name"],
                        comment="Flavor identity",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_unique",
                        ["name"],
                        comment="Flavor identity",
                        postgres_include=["rating"],
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed unique constraint flavor_name_unique on flavor" in diff.summary()
    assert plan.dry_run() == [
        'ALTER TABLE "flavor" DROP CONSTRAINT "flavor_name_unique"',
        'ALTER TABLE "flavor" ADD CONSTRAINT "flavor_name_unique" '
        'UNIQUE ("name") INCLUDE ("rating")',
        'COMMENT ON CONSTRAINT "flavor_name_unique" ON "flavor" IS \'Flavor identity\'',
    ]
    assert plan.rollback_sql() == [
        'ALTER TABLE "flavor" DROP CONSTRAINT "flavor_name_unique"',
        'ALTER TABLE "flavor" ADD CONSTRAINT "flavor_name_unique" UNIQUE ("name")',
        'COMMENT ON CONSTRAINT "flavor_name_unique" ON "flavor" IS \'Flavor identity\'',
    ]


def test_build_plan_rejects_postgres_unique_include_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name"),
                    column("rating", "int"),
                ],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_name_unique",
                        ["name"],
                        postgres_include=["rating"],
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="unique constraint INCLUDE"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_creates_mssql_unique_constraint_clustering_inline() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code"), column("name")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        mssql_filegroup="constraintspace",
                        mssql_clustered=True,
                    ),
                    UniqueConstraintSnapshot(
                        "flavor_name_unique",
                        ["name"],
                        mssql_filegroup="constraintspace",
                        mssql_clustered=False,
                    ),
                ],
            )
        ]
    )

    plan = planning._build_plan("mssql", SchemaSnapshot.empty(), after)

    assert "PRIMARY KEY NONCLUSTERED" in plan.dry_run()[0]
    assert (
        "CONSTRAINT [flavor_code_unique] UNIQUE CLUSTERED ([code]) "
        "ON [constraintspace]" in plan.dry_run()[0]
    )
    assert (
        "CONSTRAINT [flavor_name_unique] UNIQUE NONCLUSTERED ([name]) "
        "ON [constraintspace]" in plan.dry_run()[0]
    )


def test_build_plan_treats_mssql_nonclustered_unique_as_default() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        mssql_clustered=False,
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot("flavor_code_unique", ["code"])
                ],
            )
        ]
    )

    plan = planning._build_plan("mssql", before, after)

    assert plan.dry_run() == []


def test_build_plan_treats_commented_mssql_nonclustered_unique_as_default() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        mssql_clustered=False,
                        comment="Flavor identity",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        comment="Flavor identity",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


def test_build_plan_recreates_mssql_unique_when_clustering_changes() -> None:
    columns = [
        column("id", primary_key=True),
        column("code"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                named_unique_constraints=[
                    UniqueConstraintSnapshot("flavor_code_unique", ["code"])
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        mssql_filegroup="constraintspace",
                        mssql_clustered=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert "Changed unique constraint flavor_code_unique on flavor" in diff.summary()
    assert plan.dry_run() == [
        "ALTER TABLE [flavor] DROP CONSTRAINT [flavor_code_unique]",
        "ALTER TABLE [flavor] ADD CONSTRAINT [flavor_code_unique] "
        "UNIQUE CLUSTERED ([code]) ON [constraintspace]",
    ]
    assert plan.rollback_sql() == [
        "ALTER TABLE [flavor] DROP CONSTRAINT [flavor_code_unique]",
        "ALTER TABLE [flavor] ADD CONSTRAINT [flavor_code_unique] UNIQUE ([code])",
    ]


def test_build_plan_creates_oracle_unique_constraint_tablespaces_inline() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        oracle_tablespace="constraintspace",
                        oracle_compress=2,
                    )
                ],
            )
        ]
    )

    plan = planning._build_plan("oracle", SchemaSnapshot.empty(), after)

    assert (
        'CONSTRAINT "flavor_code_unique" UNIQUE ("code") '
        'USING INDEX COMPRESS 2 TABLESPACE "constraintspace"'
    ) in plan.dry_run()[0]


def test_build_plan_recreates_oracle_unique_when_tablespace_changes() -> None:
    columns = [
        column("id", primary_key=True),
        column("code"),
    ]
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                named_unique_constraints=[
                    UniqueConstraintSnapshot("flavor_code_unique", ["code"])
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                columns,
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        oracle_tablespace="constraintspace",
                        oracle_compress=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("oracle", before, after)

    assert "Changed unique constraint flavor_code_unique on flavor" in diff.summary()
    assert plan.dry_run() == [
        'ALTER TABLE "flavor" DROP CONSTRAINT "flavor_code_unique"',
        'ALTER TABLE "flavor" ADD CONSTRAINT "flavor_code_unique" '
        'UNIQUE ("code") USING INDEX COMPRESS TABLESPACE "constraintspace"',
    ]
    assert plan.rollback_sql() == [
        'ALTER TABLE "flavor" DROP CONSTRAINT "flavor_code_unique"',
        'ALTER TABLE "flavor" ADD CONSTRAINT "flavor_code_unique" UNIQUE ("code")',
    ]


def test_build_plan_rejects_mssql_unique_clustering_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        mssql_clustered=True,
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="SQL Server unique constraint clustering"):
        planning._build_plan("postgresql", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_mssql_unique_filegroups_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        mssql_filegroup="constraintspace",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="SQL Server unique constraint filegroups"):
        planning._build_plan("postgresql", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_oracle_unique_tablespaces_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        oracle_tablespace="constraintspace",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="Oracle unique constraint tablespaces"):
        planning._build_plan("postgresql", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_oracle_unique_compression_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("code")],
                named_unique_constraints=[
                    UniqueConstraintSnapshot(
                        "flavor_code_unique",
                        ["code"],
                        oracle_compress=True,
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="Oracle unique constraint compression"):
        planning._build_plan("postgresql", SchemaSnapshot.empty(), after)


def test_build_plan_recreates_changed_foreign_key_action() -> None:
    before = SchemaSnapshot(
        tables=[
            table("supplier", [column("id", primary_key=True)]),
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column(
                        "supplier_id",
                        foreign_table="supplier",
                        foreign_column="id",
                        foreign_key_name="flavor_supplier_fk",
                        on_delete="restrict",
                    ),
                ],
            ),
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table("supplier", [column("id", primary_key=True)]),
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column(
                        "supplier_id",
                        foreign_table="supplier",
                        foreign_column="id",
                        foreign_key_name="flavor_supplier_fk",
                        on_delete="set_null",
                        on_update="cascade",
                    ),
                ],
            ),
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)
    sql = plan.dry_run()

    assert "Changed foreign_key constraint flavor_supplier_fk on flavor" in (
        diff.summary()
    )
    assert 'ALTER TABLE "flavor" DROP CONSTRAINT "flavor_supplier_fk"' in sql
    assert (
        'ALTER TABLE "flavor" ADD CONSTRAINT "flavor_supplier_fk" '
        'FOREIGN KEY ("supplier_id") REFERENCES "supplier" ("id") '
        "ON DELETE SET NULL ON UPDATE CASCADE"
    ) in sql


def test_build_plan_creates_composite_foreign_key_constraint() -> None:
    before = SchemaSnapshot(
        tables=[
            table("supplier", [column("id", primary_key=True), column("code")]),
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("supplier_id"),
                    column("supplier_code"),
                ],
            ),
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table("supplier", [column("id", primary_key=True), column("code")]),
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("supplier_id"),
                    column("supplier_code"),
                ],
                foreign_key_constraints=[
                    ForeignKeyConstraintSnapshot(
                        "flavor_supplier_pair_fk",
                        ["supplier_id", "supplier_code"],
                        "supplier",
                        ["id", "code"],
                        on_delete="cascade",
                        deferrable=True,
                        initially_deferred=True,
                        validated=False,
                        match="full",
                        comment="Supplier pair lookup",
                    )
                ],
            ),
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Added foreign_key constraint flavor_supplier_pair_fk on flavor" in (
        diff.summary()
    )
    assert plan.dry_run() == [
        'ALTER TABLE "flavor" ADD CONSTRAINT "flavor_supplier_pair_fk" '
        'FOREIGN KEY ("supplier_id", "supplier_code") '
        'REFERENCES "supplier" ("id", "code") MATCH FULL '
        "ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED NOT VALID",
        'COMMENT ON CONSTRAINT "flavor_supplier_pair_fk" ON "flavor" IS '
        "'Supplier pair lookup'",
    ]
    assert plan.rollback_sql() == [
        'ALTER TABLE "flavor" DROP CONSTRAINT "flavor_supplier_pair_fk"'
    ]


def test_build_plan_changes_constraint_comment_without_recreating_constraint() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("rating", "int")],
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_rating_check",
                        "rating >= 0",
                        comment="Old rating guard",
                    )
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("rating", "int")],
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_rating_check",
                        "rating >= 0",
                        comment="Rating guard",
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == [
        "Changed check constraint flavor_rating_check on flavor: comment"
    ]
    assert not diff.has_unsafe_operations
    assert plan.dry_run() == [
        'COMMENT ON CONSTRAINT "flavor_rating_check" ON "flavor" IS \'Rating guard\''
    ]
    assert plan.rollback_sql() == [
        'COMMENT ON CONSTRAINT "flavor_rating_check" ON "flavor" IS '
        "'Old rating guard'"
    ]
    mssql_plan = planning._build_plan("mssql", before, after)
    mssql_sql = mssql_plan.dry_run()[0]
    mssql_rollback_sql = mssql_plan.rollback_sql()[0]
    assert len(mssql_plan.operations) == 1
    assert mssql_plan.operations[0].kind == "comment_constraint"
    assert "sys.sp_updateextendedproperty" in mssql_sql
    assert "@level1type = N'TABLE'" in mssql_sql
    assert "@level2type = N'CONSTRAINT'" in mssql_sql
    assert "@level2name = N'flavor_rating_check'" in mssql_sql
    assert "@value = N'Rating guard'" in mssql_sql
    assert "@value = N'Old rating guard'" in mssql_rollback_sql


def test_build_plan_rejects_constraint_comments_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("rating", "int")],
                check_constraints=[
                    TableCheckSnapshot(
                        "flavor_rating_check",
                        "rating >= 0",
                        comment="Rating guard",
                    )
                ],
            )
        ]
    )

    with pytest.raises(ValueError, match="constraint comments"):
        planning._build_plan("mysql", SchemaSnapshot.empty(), after)


def test_build_plan_creates_postgres_exclusion_constraint() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "booking",
                [
                    column("id", primary_key=True),
                    column("room_id"),
                    column("during"),
                    column("cancelled", "bool"),
                ],
            ),
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "booking",
                [
                    column("id", primary_key=True),
                    column("room_id"),
                    column("during"),
                    column("cancelled", "bool"),
                ],
                exclusion_constraints=[
                    ExclusionConstraintSnapshot(
                        "booking_room_overlap",
                        columns=[("room_id", "="), ("during", "&&")],
                        ops={"during": "gist_tstzrange_ops"},
                        using="gist",
                        where="cancelled = false",
                        deferrable=True,
                        initially_deferred=True,
                    )
                ],
            ),
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Added exclusion constraint booking_room_overlap on booking" in (
        diff.summary()
    )
    assert plan.dry_run() == [
        'ALTER TABLE "booking" ADD CONSTRAINT "booking_room_overlap" '
        'EXCLUDE USING gist ("room_id" WITH =, '
        '"during" gist_tstzrange_ops WITH &&) '
        "WHERE (cancelled = false) DEFERRABLE INITIALLY DEFERRED"
    ]
    assert plan.rollback_sql() == [
        'ALTER TABLE "booking" DROP CONSTRAINT "booking_room_overlap"'
    ]


def test_build_plan_creates_and_drops_native_enum_types() -> None:
    before = SchemaSnapshot.empty()
    after = SchemaSnapshot(
        enum_types=[
            EnumTypeSnapshot(
                "flavor_kind",
                ["mocha", "latte"],
                schema="public",
                comment="Flavor enum",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Added enum type public.flavor_kind" in diff.summary()
    assert plan.dry_run() == [
        "CREATE TYPE \"public\".\"flavor_kind\" AS ENUM ('mocha', 'latte')",
        'COMMENT ON TYPE "public"."flavor_kind" IS \'Flavor enum\'',
    ]
    assert plan.rollback_sql() == ['DROP TYPE IF EXISTS "public"."flavor_kind"']


def test_build_plan_creates_and_drops_namespaces_before_dependents() -> None:
    before = SchemaSnapshot.empty()
    after = SchemaSnapshot(
        namespaces=[NamespaceSnapshot("inventory", comment="Warehouse schema")],
        sequences=[SequenceSnapshot("flavor_id_seq", schema="inventory")],
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id FROM flavor",
                schema="inventory",
            )
        ],
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Added namespace inventory" in diff.summary()
    assert plan.dry_run()[0] == 'CREATE SCHEMA IF NOT EXISTS "inventory"'
    assert plan.dry_run()[1] == (
        "COMMENT ON SCHEMA \"inventory\" IS 'Warehouse schema'"
    )
    assert plan.dry_run()[2] == (
        'CREATE SEQUENCE IF NOT EXISTS "inventory"."flavor_id_seq"'
    )
    assert plan.dry_run()[-1] == (
        'CREATE VIEW "inventory"."active_flavors" AS SELECT id FROM flavor'
    )
    assert plan.rollback_sql()[0] == 'DROP VIEW IF EXISTS "inventory"."active_flavors"'
    assert plan.rollback_sql()[-1] == 'DROP SCHEMA IF EXISTS "inventory"'


def test_build_plan_changes_namespace_comment() -> None:
    before = SchemaSnapshot(
        namespaces=[NamespaceSnapshot("inventory", comment="Old schema")]
    )
    after = SchemaSnapshot(
        namespaces=[NamespaceSnapshot("inventory", comment="Warehouse schema")]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)
    mssql_plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == ["Changed namespace inventory: comment"]
    assert plan.dry_run() == ["COMMENT ON SCHEMA \"inventory\" IS 'Warehouse schema'"]
    assert plan.rollback_sql() == ["COMMENT ON SCHEMA \"inventory\" IS 'Old schema'"]
    assert mssql_plan.operations[0].kind == "comment_schema"
    assert "sys.sp_updateextendedproperty" in mssql_plan.dry_run()[0]
    assert "@level0type = N'SCHEMA'" in mssql_plan.dry_run()[0]
    assert "@value = N'Warehouse schema'" in mssql_plan.dry_run()[0]


def test_build_plan_rejects_namespace_comments_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        namespaces=[NamespaceSnapshot("inventory", comment="Warehouse schema")]
    )

    with pytest.raises(ValueError, match="namespace comments"):
        planning._build_plan("mysql", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_namespaces_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(namespaces=[NamespaceSnapshot("inventory")])

    with pytest.raises(ValueError, match="namespace migrations"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_creates_and_drops_sequences() -> None:
    before = SchemaSnapshot.empty()
    after = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                schema="public",
                start=10,
                increment=5,
                min_value=1,
                max_value=1000,
                cycle=True,
                cache=20,
                comment="Flavor ids",
                data_type="bigint",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Added sequence public.flavor_id_seq" in diff.summary()
    assert plan.dry_run() == [
        'CREATE SEQUENCE IF NOT EXISTS "public"."flavor_id_seq" '
        "AS bigint START WITH 10 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 CYCLE CACHE 20",
        'COMMENT ON SEQUENCE "public"."flavor_id_seq" IS \'Flavor ids\'',
    ]
    assert plan.rollback_sql() == ['DROP SEQUENCE IF EXISTS "public"."flavor_id_seq"']


def test_build_plan_recreates_changed_sequence() -> None:
    before = SchemaSnapshot(
        sequences=[SequenceSnapshot("flavor_id_seq", start=1, data_type="int")]
    )
    after = SchemaSnapshot(
        sequences=[SequenceSnapshot("flavor_id_seq", start=100, data_type="bigint")]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)

    assert "Changed sequence flavor_id_seq" in diff.summary()
    assert plan.dry_run() == [
        "DROP SEQUENCE IF EXISTS [flavor_id_seq]",
        "CREATE SEQUENCE [flavor_id_seq] AS bigint START WITH 100",
    ]


def test_build_plan_rejects_sequence_data_types_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        sequences=[SequenceSnapshot("flavor_id_seq", data_type="bigint")]
    )

    with pytest.raises(ValueError, match="sequence data types"):
        planning._build_plan("oracle", SchemaSnapshot.empty(), after)


def test_build_plan_creates_oracle_ordered_sequence() -> None:
    before = SchemaSnapshot.empty()
    after = SchemaSnapshot(
        sequences=[SequenceSnapshot("flavor_id_seq", cache=20, order=True)]
    )

    plan = planning._build_plan("oracle", before, after)

    assert plan.dry_run() == ['CREATE SEQUENCE "flavor_id_seq" CACHE 20 ORDER']
    assert plan.rollback_sql() == ['DROP SEQUENCE "flavor_id_seq"']


def test_build_plan_rejects_sequence_order_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(sequences=[SequenceSnapshot("flavor_id_seq", order=True)])

    with pytest.raises(ValueError, match="sequence ordering"):
        planning._build_plan("postgresql", SchemaSnapshot.empty(), after)


def test_build_plan_creates_sequence_no_bound_options() -> None:
    after = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                no_min_value=True,
                no_max_value=True,
            )
        ]
    )

    postgres_plan = planning._build_plan("postgresql", SchemaSnapshot.empty(), after)
    oracle_plan = planning._build_plan("oracle", SchemaSnapshot.empty(), after)
    mssql_plan = planning._build_plan("mssql", SchemaSnapshot.empty(), after)

    assert postgres_plan.dry_run() == [
        'CREATE SEQUENCE IF NOT EXISTS "flavor_id_seq" NO MINVALUE NO MAXVALUE'
    ]
    assert oracle_plan.dry_run() == [
        'CREATE SEQUENCE "flavor_id_seq" NOMINVALUE NOMAXVALUE'
    ]
    assert mssql_plan.dry_run() == [
        "CREATE SEQUENCE [flavor_id_seq] NO MINVALUE NO MAXVALUE"
    ]


def test_build_plan_rejects_sequence_no_bound_conflicts() -> None:
    after = SchemaSnapshot(
        sequences=[SequenceSnapshot("flavor_id_seq", min_value=1, no_min_value=True)]
    )

    with pytest.raises(ValueError, match="no_min_value"):
        planning._build_plan("postgresql", SchemaSnapshot.empty(), after)


def test_build_plan_ignores_existing_sequence_no_bound_authoring_flags() -> None:
    before = SchemaSnapshot(sequences=[SequenceSnapshot("flavor_id_seq")])
    after = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                no_min_value=True,
                no_max_value=True,
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


@pytest.mark.parametrize(
    ("dialect", "before_sequence"),
    [
        (
            "postgresql",
            SequenceSnapshot(
                "flavor_id_seq",
                data_type="bigint",
                min_value=1,
                max_value=9223372036854775807,
            ),
        ),
        (
            "mariadb",
            SequenceSnapshot(
                "flavor_id_seq",
                data_type="bigint",
                min_value=1,
                max_value=9223372036854775806,
            ),
        ),
        (
            "mssql",
            SequenceSnapshot(
                "flavor_id_seq",
                data_type="bigint",
                min_value=-9223372036854775808,
                max_value=9223372036854775807,
            ),
        ),
        (
            "oracle",
            SequenceSnapshot(
                "flavor_id_seq",
                min_value=1,
                max_value=10**28 - 1,
            ),
        ),
    ],
)
def test_build_plan_ignores_reflected_default_sequence_bounds(
    dialect: str,
    before_sequence: SequenceSnapshot,
) -> None:
    before = SchemaSnapshot(sequences=[before_sequence])
    after = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                data_type=before_sequence.data_type,
                no_min_value=True,
                no_max_value=True,
            )
        ]
    )

    diff = planning.diff_snapshots(before, after, dialect=dialect)
    plan = planning._build_plan(dialect, before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


def test_build_plan_keeps_mssql_explicit_sequence_minimum_distinct() -> None:
    before = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                data_type="bigint",
                min_value=1,
                max_value=9223372036854775807,
            )
        ]
    )
    after = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                data_type="bigint",
                no_min_value=True,
                no_max_value=True,
            )
        ]
    )

    diff = planning.diff_snapshots(before, after, dialect="mssql")
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == ["Changed sequence flavor_id_seq"]
    assert plan.dry_run() == [
        "DROP SEQUENCE IF EXISTS [flavor_id_seq]",
        "CREATE SEQUENCE [flavor_id_seq] AS bigint NO MINVALUE NO MAXVALUE",
    ]


@pytest.mark.parametrize(
    ("dialect", "before_column"),
    [
        (
            "postgresql",
            ColumnSnapshot(
                "id",
                "int",
                False,
                True,
                identity=True,
                identity_min_value=1,
                identity_max_value=2147483647,
            ),
        ),
        (
            "oracle",
            ColumnSnapshot(
                "id",
                "int",
                False,
                True,
                identity=True,
                identity_min_value=1,
                identity_max_value=10**28 - 1,
            ),
        ),
    ],
)
def test_build_plan_ignores_reflected_default_identity_bounds(
    dialect: str,
    before_column: ColumnSnapshot,
) -> None:
    before = SchemaSnapshot(tables=[table("flavor", [before_column])])
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    ColumnSnapshot(
                        "id",
                        "int",
                        False,
                        True,
                        identity=True,
                        identity_no_min_value=True,
                        identity_no_max_value=True,
                    )
                ],
            )
        ]
    )

    diff = planning.diff_snapshots(before, after, dialect=dialect)
    plan = planning._build_plan(dialect, before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


def test_build_plan_changes_sequence_comment_without_recreating_sequence() -> None:
    before = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                schema="public",
                comment="Old flavor ids",
            )
        ]
    )
    after = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                schema="public",
                comment="Flavor ids",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == ["Changed sequence public.flavor_id_seq: comment"]
    assert not diff.has_unsafe_operations
    assert plan.dry_run() == [
        'COMMENT ON SEQUENCE "public"."flavor_id_seq" IS \'Flavor ids\''
    ]
    assert plan.rollback_sql() == [
        'COMMENT ON SEQUENCE "public"."flavor_id_seq" IS \'Old flavor ids\''
    ]


def test_build_plan_changes_mssql_sequence_comment_only() -> None:
    before = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                schema="inventory",
                comment="Old flavor ids",
            )
        ]
    )
    after = SchemaSnapshot(
        sequences=[
            SequenceSnapshot(
                "flavor_id_seq",
                schema="inventory",
                comment="Flavor ids",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("mssql", before, after)
    sql = plan.dry_run()[0]
    rollback_sql = plan.rollback_sql()[0]

    assert diff.summary() == ["Changed sequence inventory.flavor_id_seq: comment"]
    assert not diff.has_unsafe_operations
    assert len(plan.operations) == 1
    assert plan.operations[0].kind == "comment_sequence"
    assert "DECLARE @schema sysname = N'inventory'" in sql
    assert "sys.sp_updateextendedproperty" in sql
    assert "@level1type = N'SEQUENCE'" in sql
    assert "@level1name = N'flavor_id_seq'" in sql
    assert "@value = N'Flavor ids'" in sql
    assert "@value = N'Old flavor ids'" in rollback_sql


def test_build_plan_rejects_sequence_comments_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        sequences=[SequenceSnapshot("flavor_id_seq", comment="Flavor ids")]
    )

    with pytest.raises(ValueError, match="sequence comments"):
        planning._build_plan("mariadb", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_sequences_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(sequences=[SequenceSnapshot("flavor_id_seq")])

    with pytest.raises(ValueError, match="sequence migrations"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_build_plan_creates_and_drops_views_after_tables() -> None:
    before = SchemaSnapshot.empty()
    after = SchemaSnapshot(
        tables=[table("flavor", [column("id", primary_key=True), column("name")])],
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id, name FROM flavor WHERE deleted_at IS NULL",
                schema="public",
            )
        ],
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Added view public.active_flavors" in diff.summary()
    assert plan.dry_run()[-1] == (
        'CREATE VIEW "public"."active_flavors" '
        "AS SELECT id, name FROM flavor WHERE deleted_at IS NULL"
    )
    assert plan.rollback_sql()[0] == 'DROP VIEW IF EXISTS "public"."active_flavors"'


def test_build_plan_creates_and_drops_materialized_views_after_tables() -> None:
    before = SchemaSnapshot.empty()
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("supplier_id"),
                ],
            )
        ],
        views=[
            ViewSnapshot(
                "active_flavor_counts",
                "SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id",
                schema="public",
                materialized=True,
            )
        ],
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Added view public.active_flavor_counts" in diff.summary()
    assert plan.dry_run()[-1] == (
        'CREATE MATERIALIZED VIEW "public"."active_flavor_counts" '
        "AS SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id"
    )
    assert (
        plan.rollback_sql()[0]
        == 'DROP MATERIALIZED VIEW IF EXISTS "public"."active_flavor_counts"'
    )


def test_build_plan_recreates_changed_view_around_table_diff() -> None:
    before = SchemaSnapshot(
        tables=[table("flavor", [column("id", primary_key=True), column("name")])],
        views=[ViewSnapshot("active_flavors", "SELECT id, name FROM flavor")],
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name"),
                    column("deleted_at", nullable=True),
                ],
            )
        ],
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id, name FROM flavor WHERE deleted_at IS NULL",
            )
        ],
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("sqlite", before, after)

    assert "Changed view active_flavors" in diff.summary()
    assert plan.dry_run()[0] == 'DROP VIEW IF EXISTS "active_flavors"'
    assert plan.dry_run()[-1] == (
        'CREATE VIEW "active_flavors" '
        "AS SELECT id, name FROM flavor WHERE deleted_at IS NULL"
    )


def test_build_plan_ignores_postgres_reflected_simple_view_rewrite() -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                " SELECT id,\n    name\n   FROM flavor",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("active_flavors", "SELECT id, name FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect="postgresql")
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


def test_build_plan_keeps_changed_postgres_view_definitions() -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                " SELECT id,\n    name\n   FROM flavor",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("active_flavors", "SELECT id FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect="postgresql")
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == ["Changed view active_flavors"]
    assert plan.dry_run() == [
        'DROP VIEW IF EXISTS "active_flavors"',
        'CREATE VIEW "active_flavors" AS SELECT id FROM flavor',
    ]


def test_build_plan_keeps_changed_postgres_complex_view_definitions() -> None:
    before = SchemaSnapshot(
        views=[ViewSnapshot("flavor_stats", "SELECT COUNT(*) AS total FROM flavor")]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("flavor_stats", "SELECT MAX(id) AS total FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect="postgresql")
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == ["Changed view flavor_stats"]
    assert plan.dry_run() == [
        'DROP VIEW IF EXISTS "flavor_stats"',
        'CREATE VIEW "flavor_stats" AS SELECT MAX(id) AS total FROM flavor',
    ]


def test_build_plan_ignores_mssql_reflected_simple_view_rewrite() -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "CREATE VIEW [dbo].[active_flavors] "
                "AS SELECT [id], [name] FROM [dbo].[flavor]",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("active_flavors", "SELECT id, name FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect="mssql")
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


def test_build_plan_keeps_changed_mssql_view_definitions() -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "CREATE VIEW [dbo].[active_flavors] "
                "AS SELECT [id], [name] FROM [dbo].[flavor]",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("active_flavors", "SELECT id FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect="mssql")
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == ["Changed view active_flavors"]
    assert plan.dry_run() == [
        "DROP VIEW IF EXISTS [active_flavors]",
        "CREATE VIEW [active_flavors] AS SELECT id FROM flavor",
    ]


def test_build_plan_keeps_changed_mssql_complex_view_definitions() -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "flavor_stats",
                "CREATE VIEW [dbo].[flavor_stats] "
                "AS SELECT COUNT(*) AS [total] FROM [dbo].[flavor]",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("flavor_stats", "SELECT MAX(id) AS total FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect="mssql")
    plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == ["Changed view flavor_stats"]
    assert plan.dry_run() == [
        "DROP VIEW IF EXISTS [flavor_stats]",
        "CREATE VIEW [flavor_stats] AS SELECT MAX(id) AS total FROM flavor",
    ]


@pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
def test_build_plan_ignores_mysql_reflected_simple_view_rewrite(
    dialect: str,
) -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "select `mysql`.`flavor`.`id` AS `id`,"
                "`mysql`.`flavor`.`name` AS `name` from `mysql`.`flavor`",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("active_flavors", "SELECT id, name FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect=dialect)
    plan = planning._build_plan(dialect, before, after)

    assert diff.summary() == []
    assert plan.dry_run() == []


@pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
def test_build_plan_keeps_changed_mysql_view_definitions(dialect: str) -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "select `mysql`.`flavor`.`id` AS `id`,"
                "`mysql`.`flavor`.`name` AS `name` from `mysql`.`flavor`",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("active_flavors", "SELECT id FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect=dialect)
    plan = planning._build_plan(dialect, before, after)

    assert diff.summary() == ["Changed view active_flavors"]
    assert plan.dry_run() == [
        "DROP VIEW IF EXISTS `active_flavors`",
        "CREATE VIEW `active_flavors` AS SELECT id FROM flavor",
    ]


@pytest.mark.parametrize("dialect", ["mysql", "mariadb"])
def test_build_plan_keeps_changed_mysql_complex_view_definitions(dialect: str) -> None:
    before = SchemaSnapshot(
        views=[ViewSnapshot("flavor_stats", "SELECT COUNT(*) AS total FROM flavor")]
    )
    after = SchemaSnapshot(
        views=[ViewSnapshot("flavor_stats", "SELECT MAX(id) AS total FROM flavor")]
    )

    diff = planning.diff_snapshots(before, after, dialect=dialect)
    plan = planning._build_plan(dialect, before, after)

    assert diff.summary() == ["Changed view flavor_stats"]
    assert plan.dry_run() == [
        "DROP VIEW IF EXISTS `flavor_stats`",
        "CREATE VIEW `flavor_stats` AS SELECT MAX(id) AS total FROM flavor",
    ]


def test_build_plan_changes_view_comment_without_recreating_view() -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id FROM flavor",
                schema="public",
                comment="Old view",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id FROM flavor",
                schema="public",
                comment="Active flavors",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)
    mssql_plan = planning._build_plan("mssql", before, after)

    assert diff.summary() == ["Changed view public.active_flavors: comment"]
    assert not diff.has_unsafe_operations
    assert not diff.has_destructive_operations
    assert plan.dry_run() == [
        'COMMENT ON VIEW "public"."active_flavors" IS \'Active flavors\''
    ]
    assert plan.rollback_sql() == [
        'COMMENT ON VIEW "public"."active_flavors" IS \'Old view\''
    ]
    assert mssql_plan.operations[0].kind == "comment_view"
    assert "sys.sp_updateextendedproperty" in mssql_plan.dry_run()[0]
    assert "@level1type = N'VIEW'" in mssql_plan.dry_run()[0]
    assert "@value = N'Active flavors'" in mssql_plan.dry_run()[0]


def test_build_plan_changes_oracle_materialized_view_comment_only() -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavor_counts",
                "SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id",
                schema="inventory",
                materialized=True,
                comment="Old summary",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavor_counts",
                "SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id",
                schema="inventory",
                materialized=True,
                comment="Active flavor counts",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("oracle", before, after)

    assert diff.summary() == ["Changed view inventory.active_flavor_counts: comment"]
    assert not diff.has_unsafe_operations
    assert plan.dry_run() == [
        'COMMENT ON MATERIALIZED VIEW "inventory"."active_flavor_counts" '
        "IS 'Active flavor counts'"
    ]
    assert plan.rollback_sql() == [
        'COMMENT ON MATERIALIZED VIEW "inventory"."active_flavor_counts" '
        "IS 'Old summary'"
    ]


def test_build_plan_changes_oracle_regular_view_comment_only() -> None:
    before = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id FROM flavor",
                schema="inventory",
                comment="Old view",
            )
        ]
    )
    after = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id FROM flavor",
                schema="inventory",
                comment="Active flavors",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("oracle", before, after)

    assert diff.summary() == ["Changed view inventory.active_flavors: comment"]
    assert not diff.has_unsafe_operations
    assert plan.operations[0].kind == "comment_view"
    assert plan.operations[0].object_name == "inventory.active_flavors"
    assert plan.operations[0].table is None
    assert plan.dry_run() == [
        'COMMENT ON TABLE "inventory"."active_flavors" IS \'Active flavors\''
    ]
    assert plan.rollback_sql() == [
        'COMMENT ON TABLE "inventory"."active_flavors" IS \'Old view\''
    ]


def test_build_plan_creates_view_comment_after_view() -> None:
    after = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id FROM flavor",
                comment="Active flavors",
            )
        ]
    )

    diff = planning.diff_snapshots(SchemaSnapshot.empty(), after)
    plan = planning._build_plan("postgresql", SchemaSnapshot.empty(), after)

    assert diff.summary() == ["Added view active_flavors"]
    assert plan.dry_run() == [
        'CREATE VIEW "active_flavors" AS SELECT id FROM flavor',
        "COMMENT ON VIEW \"active_flavors\" IS 'Active flavors'",
    ]


def test_build_plan_rejects_view_comments_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id FROM flavor",
                comment="Active flavors",
            )
        ]
    )

    with pytest.raises(ValueError, match="view comments"):
        planning._build_plan("mysql", SchemaSnapshot.empty(), after)


def test_build_plan_rejects_materialized_views_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        views=[
            ViewSnapshot(
                "active_flavors",
                "SELECT id FROM flavor",
                materialized=True,
            )
        ]
    )

    with pytest.raises(ValueError, match="materialized view migrations"):
        planning._build_plan("mysql", SchemaSnapshot.empty(), after)


def test_build_plan_recreates_changed_native_enum_type() -> None:
    before = SchemaSnapshot(
        enum_types=[EnumTypeSnapshot("flavor_kind", ["mocha", "latte"])]
    )
    after = SchemaSnapshot(
        enum_types=[EnumTypeSnapshot("flavor_kind", ["mocha", "latte", "cortado"])]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert "Changed enum type flavor_kind" in diff.summary()
    assert plan.has_destructive_operations
    assert plan.dry_run() == [
        'DROP TYPE IF EXISTS "flavor_kind"',
        "CREATE TYPE \"flavor_kind\" AS ENUM ('mocha', 'latte', 'cortado')",
    ]


def test_build_plan_changes_native_enum_type_comment_without_recreating_type() -> None:
    before = SchemaSnapshot(
        enum_types=[
            EnumTypeSnapshot(
                "flavor_kind",
                ["mocha", "latte"],
                schema="public",
                comment="Old flavor enum",
            )
        ]
    )
    after = SchemaSnapshot(
        enum_types=[
            EnumTypeSnapshot(
                "flavor_kind",
                ["mocha", "latte"],
                schema="public",
                comment="Flavor enum",
            )
        ]
    )

    diff = planning.diff_snapshots(before, after)
    plan = planning._build_plan("postgresql", before, after)

    assert diff.summary() == ["Changed enum type public.flavor_kind: comment"]
    assert not diff.has_unsafe_operations
    assert plan.dry_run() == [
        'COMMENT ON TYPE "public"."flavor_kind" IS \'Flavor enum\''
    ]
    assert plan.rollback_sql() == [
        'COMMENT ON TYPE "public"."flavor_kind" IS \'Old flavor enum\''
    ]


def test_build_plan_rejects_native_enum_types_on_unsupported_dialects() -> None:
    after = SchemaSnapshot(
        enum_types=[EnumTypeSnapshot("flavor_kind", ["mocha", "latte"])]
    )

    with pytest.raises(ValueError, match="native enum type migrations"):
        planning._build_plan("sqlite", SchemaSnapshot.empty(), after)


def test_sql_operation_classification_extracts_metadata() -> None:
    assert planning._classify_sql_operation(
        'CREATE TABLE IF NOT EXISTS "flavor" (id TEXT)'
    ) == {
        "kind": "create_table",
        "table": "flavor",
        "object_name": None,
        "requires_rebuild": False,
        "reversible": True,
        "destructive": False,
        "unsafe": False,
    }
    drop = planning._classify_sql_operation('ALTER TABLE "flavor" DROP COLUMN "code"')
    assert drop["kind"] == "alter_table"
    assert drop["table"] == "flavor"
    assert drop["requires_rebuild"]
    assert drop["destructive"]
    postgres_comment = planning._classify_sql_operation(
        "COMMENT ON TABLE \"flavor\" IS 'metadata'"
    )
    assert postgres_comment["kind"] == "comment_table"
    assert postgres_comment["table"] == "flavor"
    mysql_comment = planning._classify_sql_operation(
        "ALTER TABLE `flavor` COMMENT = 'metadata'"
    )
    assert mysql_comment["kind"] == "comment_table"
    assert mysql_comment["table"] == "flavor"
    assert not mysql_comment["unsafe"]
    column_comment = planning._classify_sql_operation(
        'COMMENT ON COLUMN "flavor"."name" IS \'metadata\''
    )
    assert column_comment["kind"] == "comment_column"
    assert column_comment["table"] == "flavor"
    assert column_comment["object_name"] == "flavor.name"
    assert not column_comment["unsafe"]
    schema_comment = planning._classify_sql_operation(
        "COMMENT ON SCHEMA \"inventory\" IS 'metadata'"
    )
    assert schema_comment["kind"] == "comment_schema"
    assert schema_comment["object_name"] == "inventory"
    assert not schema_comment["unsafe"]
    view_comment = planning._classify_sql_operation(
        'COMMENT ON VIEW "inventory"."active_flavors" IS \'metadata\''
    )
    assert view_comment["kind"] == "comment_view"
    assert view_comment["object_name"] == "inventory.active_flavors"
    assert not view_comment["unsafe"]
    index_comment = planning._classify_sql_operation(
        'COMMENT ON INDEX "inventory"."flavor_name_idx" IS \'metadata\''
    )
    assert index_comment["kind"] == "comment_index"
    assert index_comment["object_name"] == "inventory.flavor_name_idx"
    assert not index_comment["unsafe"]
    mssql_index_comment = planning._classify_sql_operation(
        "EXEC sys.sp_updateextendedproperty @name = N'MS_Description', "
        "@value = N'metadata', @level0type = N'SCHEMA', "
        "@level0name = N'inventory', @level1type = N'TABLE', "
        "@level1name = N'flavor', @level2type = N'INDEX', "
        "@level2name = N'flavor_name_idx'"
    )
    assert mssql_index_comment["kind"] == "comment_index"
    assert not mssql_index_comment["unsafe"]
    mssql_constraint_comment = planning._classify_sql_operation(
        "EXEC sys.sp_updateextendedproperty @name = N'MS_Description', "
        "@value = N'metadata', @level0type = N'SCHEMA', "
        "@level0name = N'inventory', @level1type = N'TABLE', "
        "@level1name = N'flavor', @level2type = N'CONSTRAINT', "
        "@level2name = N'flavor_rating_check'"
    )
    assert mssql_constraint_comment["kind"] == "comment_constraint"
    assert not mssql_constraint_comment["unsafe"]
    mssql_sequence_comment = planning._classify_sql_operation(
        "EXEC sys.sp_updateextendedproperty @name = N'MS_Description', "
        "@value = N'metadata', @level0type = N'SCHEMA', "
        "@level0name = N'inventory', @level1type = N'SEQUENCE', "
        "@level1name = N'flavor_id_seq'"
    )
    assert mssql_sequence_comment["kind"] == "comment_sequence"
    assert not mssql_sequence_comment["unsafe"]
    constraint_comment = planning._classify_sql_operation(
        'COMMENT ON CONSTRAINT "flavor_rating_check" '
        'ON "inventory"."flavor" IS \'metadata\''
    )
    assert constraint_comment["kind"] == "comment_constraint"
    assert constraint_comment["object_name"] == "flavor_rating_check"
    assert not constraint_comment["unsafe"]
    index_storage = planning._classify_sql_operation(
        'ALTER INDEX "inventory"."flavor_name_idx" SET TABLESPACE "fastspace"'
    )
    assert index_storage["kind"] == "index_storage"
    assert index_storage["object_name"] == "inventory.flavor_name_idx"
    assert not index_storage["unsafe"]
    mysql_column_comment = planning._classify_sql_operation(
        "ALTER TABLE `flavor` MODIFY COLUMN `name` TEXT NOT NULL COMMENT 'metadata'"
    )
    assert mysql_column_comment["kind"] == "comment_column"
    assert mysql_column_comment["table"] == "flavor"
    assert not mysql_column_comment["unsafe"]
    storage = planning._classify_sql_operation(
        'ALTER TABLE "flavor" SET TABLESPACE "fastspace"'
    )
    assert storage["kind"] == "table_storage"
    assert storage["table"] == "flavor"
    assert not storage["unsafe"]
    oracle_move = planning._classify_sql_operation('ALTER TABLE "flavor" MOVE')
    assert oracle_move["kind"] == "table_storage"
    assert oracle_move["table"] == "flavor"
    assert not oracle_move["unsafe"]
    mysql_storage = planning._classify_sql_operation(
        "ALTER TABLE `flavor` ENGINE = MyISAM DEFAULT CHARACTER SET = latin1 "
        "COLLATE = latin1_swedish_ci ROW_FORMAT = COMPACT"
    )
    assert mysql_storage["kind"] == "table_storage"
    assert mysql_storage["table"] == "flavor"
    assert not mysql_storage["unsafe"]
    postgres_inherit = planning._classify_sql_operation(
        'ALTER TABLE "flavor" INHERIT "base_flavor"'
    )
    assert postgres_inherit["kind"] == "table_storage"
    assert postgres_inherit["table"] == "flavor"
    assert not postgres_inherit["unsafe"]
    postgres_no_inherit = planning._classify_sql_operation(
        'ALTER TABLE "flavor" NO INHERIT "old_base"'
    )
    assert postgres_no_inherit["kind"] == "table_storage"
    assert postgres_no_inherit["table"] == "flavor"
    assert not postgres_no_inherit["unsafe"]
    postgres_storage_set = planning._classify_sql_operation(
        'ALTER TABLE "flavor" SET (fillfactor = 70)'
    )
    assert postgres_storage_set["kind"] == "table_storage"
    assert postgres_storage_set["table"] == "flavor"
    assert not postgres_storage_set["unsafe"]
    postgres_storage_reset = planning._classify_sql_operation(
        'ALTER TABLE "flavor" RESET (fillfactor)'
    )
    assert postgres_storage_reset["kind"] == "table_storage"
    assert postgres_storage_reset["table"] == "flavor"
    assert not postgres_storage_reset["unsafe"]
    postgres_access_method = planning._classify_sql_operation(
        'ALTER TABLE "flavor" SET ACCESS METHOD "custom_heap"'
    )
    assert postgres_access_method["kind"] == "table_storage"
    assert postgres_access_method["table"] == "flavor"
    assert not postgres_access_method["unsafe"]
    postgres_unlogged = planning._classify_sql_operation(
        'ALTER TABLE "flavor" SET UNLOGGED'
    )
    assert postgres_unlogged["kind"] == "table_storage"
    assert postgres_unlogged["table"] == "flavor"
    assert not postgres_unlogged["unsafe"]
    postgres_logged = planning._classify_sql_operation(
        'ALTER TABLE "flavor" SET LOGGED'
    )
    assert postgres_logged["kind"] == "table_storage"
    assert postgres_logged["table"] == "flavor"
    assert not postgres_logged["unsafe"]
    postgres_attach_partition = planning._classify_sql_operation(
        'ALTER TABLE "flavor" ATTACH PARTITION "flavor_2026" '
        "FOR VALUES FROM (2026) TO (2027)"
    )
    assert postgres_attach_partition["kind"] == "table_partition"
    assert postgres_attach_partition["table"] == "flavor"
    assert postgres_attach_partition["unsafe"]
    assert not postgres_attach_partition["destructive"]
    postgres_detach_partition = planning._classify_sql_operation(
        'ALTER TABLE "flavor" DETACH PARTITION "flavor_2026"'
    )
    assert postgres_detach_partition["kind"] == "table_partition"
    assert postgres_detach_partition["table"] == "flavor"
    assert postgres_detach_partition["unsafe"]
    assert not postgres_detach_partition["destructive"]
    assert planning._classify_sql_operation("DELETE FROM flavor")["reversible"] is False
    assert planning._classify_sql_operation(
        "CREATE UNIQUE INDEX flavor_name_idx ON flavor (name)"
    )["unsafe"]
    assert (
        planning._classify_sql_operation("CREATE TYPE flavor AS ENUM ('mocha')")["kind"]
        == "create_enum_type"
    )
    assert planning._classify_sql_operation("DROP TYPE IF EXISTS flavor")["destructive"]
    enum_comment = planning._classify_sql_operation(
        "COMMENT ON TYPE \"flavor_kind\" IS 'Flavor enum'"
    )
    assert enum_comment["kind"] == "comment_enum_type"
    assert enum_comment["object_name"] == "flavor_kind"
    assert not enum_comment["unsafe"]
    assert planning._classify_sql_operation(
        'CREATE SEQUENCE IF NOT EXISTS "flavor_id_seq"'
    ) == {
        "kind": "create_sequence",
        "table": None,
        "object_name": "flavor_id_seq",
        "requires_rebuild": False,
        "reversible": True,
        "destructive": False,
        "unsafe": False,
    }
    drop_sequence = planning._classify_sql_operation(
        'DROP SEQUENCE IF EXISTS "flavor_id_seq"'
    )
    assert drop_sequence["kind"] == "drop_sequence"
    assert drop_sequence["object_name"] == "flavor_id_seq"
    assert drop_sequence["destructive"]
    comment_sequence = planning._classify_sql_operation(
        "COMMENT ON SEQUENCE \"flavor_id_seq\" IS 'Flavor ids'"
    )
    assert comment_sequence["kind"] == "comment_sequence"
    assert comment_sequence["object_name"] == "flavor_id_seq"
    assert not comment_sequence["unsafe"]
    assert planning._classify_sql_operation(
        'CREATE VIEW "active_flavors" AS SELECT id FROM flavor'
    ) == {
        "kind": "create_view",
        "table": None,
        "object_name": "active_flavors",
        "requires_rebuild": False,
        "reversible": True,
        "destructive": False,
        "unsafe": False,
    }
    drop_view = planning._classify_sql_operation(
        'DROP MATERIALIZED VIEW IF EXISTS "active_flavors"'
    )
    assert drop_view["kind"] == "drop_materialized_view"
    assert drop_view["object_name"] == "active_flavors"
    assert drop_view["destructive"]
    insert = planning._classify_sql_operation("INSERT INTO flavor (id) VALUES (1)")
    assert insert["kind"] == "insert"
    assert insert["table"] == "flavor"
    assert not insert["reversible"]
    assert insert["unsafe"]
    update = planning._classify_sql_operation("UPDATE flavor SET name = 'Mocha'")
    assert update["kind"] == "update"
    assert update["table"] == "flavor"
    assert not update["reversible"]
    assert update["unsafe"]


def test_sqlite_rebuild_guard_reports_first_three_operations() -> None:
    operations = [
        MigrationOperation(
            f'ALTER TABLE "flavor_{index}" DROP COLUMN "legacy"',
            requires_rebuild=True,
        )
        for index in range(4)
    ]

    with pytest.raises(ValueError, match=r"flavor_0.*flavor_1.*flavor_2.*, \.\.\."):
        planning._raise_if_unsupported_sqlite_plan("sqlite", operations)

    planning._raise_if_unsupported_sqlite_plan("postgresql", operations)
    planning._raise_if_unsupported_sqlite_plan(
        "sqlite", [MigrationOperation('CREATE TABLE "flavor" (id TEXT)')]
    )


def test_index_sql_annotation_helpers_cover_parser_failures_and_quotes() -> None:
    assert planning._postgres_index_ops_sql(
        'CREATE INDEX flavor_name_idx ON flavor ("name" DESC, lower("code"))',
        {"name": "text_pattern_ops", 'lower("code")': "text_pattern_ops"},
    ) == (
        "CREATE INDEX flavor_name_idx ON flavor "
        '("name" text_pattern_ops DESC, lower("code") text_pattern_ops)'
    )
    with pytest.raises(ValueError, match="operator classes"):
        planning._postgres_index_ops_sql(
            "CREATE INDEX flavor_name_idx ON flavor",
            {"name": "text_pattern_ops"},
        )
    with pytest.raises(ValueError, match="not present"):
        planning._postgres_index_ops_sql(
            "CREATE INDEX flavor_name_idx ON flavor (name)",
            {"missing": "text_pattern_ops"},
        )
    with pytest.raises(ValueError, match="non-unique"):
        planning._postgres_index_nulls_not_distinct_sql(
            "CREATE INDEX flavor_name_idx ON flavor (name)"
        )
    with pytest.raises(ValueError, match="after INCLUDE"):
        planning._postgres_index_nulls_not_distinct_sql(
            "CREATE UNIQUE INDEX flavor_name_idx ON flavor (name) INCLUDE broken"
        )
    with pytest.raises(ValueError, match="prefixes can only"):
        planning._mysql_index_prefix_sql(
            "CREATE UNIQUE INDEX flavor_name_idx ON flavor (name)",
            "FULLTEXT",
        )
    with pytest.raises(ValueError, match="combined with USING"):
        planning._mysql_index_using_sql(
            "CREATE FULLTEXT INDEX flavor_name_idx ON flavor (name)",
            "HASH",
        )
    with pytest.raises(ValueError, match="prefix lengths"):
        planning._mysql_index_length_sql(
            "CREATE INDEX flavor_name_idx ON flavor (name)",
            {"missing": 12},
        )
    with pytest.raises(ValueError, match="index visibility"):
        planning._mysql_index_visibility_sql("not an index", False)

    assert planning._split_top_level_sql_list(
        "LOWER('a,b'), [weird]]name], func(`x,y`, 2)"
    ) == ["LOWER('a,b')", "[weird]]name]", "func(`x,y`, 2)"]
    assert planning._index_column_list_bounds("CREATE INDEX idx ON flavor") is None
    assert planning._index_column_list_bounds("CREATE INDEX idx flavor (name)") is None
    assert planning._find_matching_sql_paren("func('unclosed'", 4) is None
    assert planning._leading_sql_identifier_bounds("  1bad") is None
    assert planning._leading_sql_identifier_bounds("   ") is None
    assert planning._leading_sql_identifier_bounds(' "unterminated') is None


@pytest.mark.parametrize(
    ("dialect", "sequence", "message"),
    [
        (
            "postgresql",
            SequenceSnapshot("flavor_id_seq", data_type="numeric"),
            "smallint, integer, or bigint",
        ),
        (
            "mariadb",
            SequenceSnapshot("flavor_id_seq", data_type="decimal(10, 0)"),
            "integer type",
        ),
        (
            "mariadb",
            SequenceSnapshot("flavor_id_seq", data_type="bigint unsigned extra"),
            "safe SQL type",
        ),
        (
            "mssql",
            SequenceSnapshot("flavor_id_seq", data_type="bigint unsigned"),
            "signed or unsigned",
        ),
        (
            "oracle",
            SequenceSnapshot("flavor_id_seq", data_type="bigint"),
            "sequence data types",
        ),
    ],
)
def test_sequence_type_validation_rejects_dialect_specific_invalid_types(
    dialect: str, sequence: SequenceSnapshot, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        planning._create_sequence_sql(dialect, sequence)


def test_sequence_helpers_cover_dialect_defaults_and_invalid_bounds() -> None:
    assert planning._create_sequence_sql(
        "mariadb",
        SequenceSnapshot(
            "flavor_id_seq",
            schema="inventory",
            data_type="INT UNSIGNED",
            start=10,
            increment=5,
            no_min_value=True,
            no_max_value=True,
            cache=20,
        ),
    ) == (
        "CREATE SEQUENCE IF NOT EXISTS `inventory`.`flavor_id_seq` AS int unsigned "
        "START WITH 10 INCREMENT BY 5 NO MINVALUE NO MAXVALUE CACHE 20"
    )
    assert (
        planning._drop_sequence_sql(
            "oracle", SequenceSnapshot("flavor_id_seq", schema="inventory")
        )
        == 'DROP SEQUENCE "inventory"."flavor_id_seq"'
    )
    with pytest.raises(ValueError, match="data_type cannot be empty"):
        planning._create_sequence_sql(
            "postgresql", SequenceSnapshot("flavor_id_seq", data_type=" ")
        )
    with pytest.raises(ValueError, match="safe SQL type"):
        planning._create_sequence_sql(
            "postgresql", SequenceSnapshot("flavor_id_seq", data_type="int; drop")
        )
    assert (
        planning._sequence_bound_requires_recreate(
            SequenceSnapshot(
                "flavor_id_seq",
                data_type="integer",
                increment=-1,
                min_value=-2147483648,
            ),
            SequenceSnapshot("flavor_id_seq", data_type="integer", increment=-1),
            "min_value",
            "no_min_value",
            dialect="postgresql",
        )
        is False
    )
    assert (
        planning._sequence_bound_requires_recreate(
            SequenceSnapshot(
                "flavor_id_seq", data_type="tinyint unsigned", max_value=255
            ),
            SequenceSnapshot("flavor_id_seq", data_type="tinyint unsigned"),
            "max_value",
            "no_max_value",
            dialect="mariadb",
        )
        is False
    )
    assert (
        planning._sequence_bound_requires_recreate(
            SequenceSnapshot("flavor_id_seq", max_value=10, no_max_value=False),
            SequenceSnapshot("flavor_id_seq", max_value=10, no_max_value=True),
            "max_value",
            "no_max_value",
            dialect=None,
        )
        is True
    )


def test_simple_view_signature_helpers_cover_quoted_identifier_edges() -> None:
    assert planning._mysql_simple_view_signature(
        "SELECT `Flavor`.`Name` AS `Label`, id FROM `Inventory`.`Flavor`"
    ) == ("flavor", (("name", "label"), ("id", None)))
    assert (
        planning._mysql_simple_view_signature("SELECT lower(name) FROM flavor") is None
    )
    assert planning._mysql_view_identifier_parts("`inventory`.`odd_name`") == [
        "inventory",
        "odd_name",
    ]
    assert planning._mysql_view_identifier_parts("`inventory`.`odd``name`") is None
    assert planning._mysql_view_identifier_parts("`unterminated") is None
    assert planning._mysql_view_identifier_parts("inventory . flavor") is None
    assert planning._mysql_view_identifier_parts("9bad") is None

    assert planning._postgres_simple_view_signature(
        'SELECT "Flavor"."Name" AS "Label", id FROM "Inventory"."Flavor"'
    ) == (("Inventory", "Flavor"), (("Name", "Label"), ("id", None)))
    assert (
        planning._postgres_simple_view_signature("SELECT lower(name) FROM flavor")
        is None
    )
    assert planning._postgres_view_identifier_parts('"Inventory"."odd""name"') == [
        "Inventory",
        'odd"name',
    ]
    assert planning._postgres_view_identifier_parts('"unterminated') is None
    assert planning._postgres_view_identifier_parts("inventory . flavor") is None
    assert planning._postgres_view_identifier_parts("9bad") is None

    assert planning._mssql_simple_view_signature(
        "CREATE OR ALTER VIEW [dbo].[active_flavors] AS "
        "SELECT [Flavor].[Name] AS [Label], id FROM [Inventory].[Flavor]"
    ) == ("flavor", (("name", "label"), ("id", None)))
    assert (
        planning._mssql_simple_view_signature("SELECT lower(name) FROM flavor") is None
    )
    assert planning._mssql_view_identifier_parts("[Inventory].[odd_name]") == [
        "Inventory",
        "odd_name",
    ]
    assert planning._mssql_view_identifier_parts("[Inventory].[odd]]name]") is None
    assert planning._mssql_view_identifier_parts('"Inventory"."odd""name"') is None
    assert planning._mssql_view_identifier_parts("[unterminated") is None
    assert planning._mssql_view_identifier_parts("inventory . flavor") is None
    assert planning._mssql_view_identifier_parts("9bad") is None


def test_check_constraint_helpers_render_supported_checks() -> None:
    assert planning._check_expression("name", ("length", ">=", "2")) == (
        "LENGTH(name) >= 2"
    )
    assert (
        planning._check_expression(
            "name",
            ("length", "<=", "10"),
            dialect="oracle",
        )
        == 'LENGTH("name") <= 10'
    )
    assert planning._check_expression("flavor", ("enum", "in", "'mocha'")) == (
        "flavor IN ('mocha')"
    )
    assert planning._check_expression("code", ("pattern", "matches", "'^[A-Z]+$'")) == (
        "ormdantic_regex_match(code, '^[A-Z]+$')"
    )
    assert planning._check_expression("quantity", ("multiple_of", "=", "5")) == (
        "ormdantic_multiple_of(quantity, 5)"
    )
    assert planning._check_suffix(("comparison", "<=", "10")) == "le"
    assert planning._check_suffix(("enum", "in", "'mocha'")) == "enum_values"
    assert planning._check_suffix(("pattern", "matches", "'^[A-Z]+$'")) == "pattern"
    assert planning._check_suffix(("multiple_of", "=", "5")) == "multiple_of"
    with pytest.raises(ValueError, match="unsupported check constraint kind"):
        planning._check_expression("name", ("jsonpath", "@@", "$.a"))
    with pytest.raises(ValueError, match="unsupported check constraint operator"):
        planning._check_suffix(("comparison", "!=", "10"))


def test_build_plan_renders_mssql_length_checks_with_len_function() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name", checks=[("length", "<=", "255")]),
                ],
            )
        ]
    )

    plan = planning._build_plan("mssql", before, after)

    assert any("CHECK (LEN(name) <= 255)" in sql for sql in plan.dry_run())
    assert all("LENGTH(" not in sql for sql in plan.dry_run())


def test_build_plan_quotes_oracle_column_check_identifiers() -> None:
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [column("id", primary_key=True), column("name")],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name", checks=[("length", "<=", "255")]),
                ],
            )
        ]
    )

    plan = planning._build_plan("oracle", before, after)

    assert any('CHECK (LENGTH("name") <= 255)' in sql for sql in plan.dry_run())


def test_snapshot_coercion_accepts_objects_and_mappings() -> None:
    snapshot = SchemaSnapshot(
        tables=[table("flavor", [column("id", primary_key=True)])]
    )

    assert planning._coerce_snapshot(snapshot) is snapshot
    assert planning._coerce_snapshot(snapshot.to_dict()).to_dict() == snapshot.to_dict()


def test_table_snapshot_with_name_preserves_schema_and_table_metadata() -> None:
    original = table(
        "flavor",
        [
            column("id", primary_key=True),
            column("code"),
            column("supplier_id"),
        ],
        schema="inventory",
        indexes=[IndexSnapshot("flavor_code_idx", ["code"], unique=False)],
        unique_constraints=[["code"]],
        named_unique_constraints=[
            UniqueConstraintSnapshot("flavor_code_unique", ["code"])
        ],
        check_constraints=[TableCheckSnapshot("flavor_code_check", "length(code) > 0")],
        foreign_key_constraints=[
            ForeignKeyConstraintSnapshot(
                "flavor_supplier_fk",
                ["supplier_id"],
                "supplier",
                ["id"],
            )
        ],
        exclusion_constraints=[
            ExclusionConstraintSnapshot(
                "flavor_code_exclude",
                columns=[("code", "=")],
            )
        ],
        comment="flavor table",
        tablespace="fastspace",
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
        mysql_collation="utf8mb4_unicode_ci",
        mysql_row_format="DYNAMIC",
        mysql_key_block_size=8,
        mysql_pack_keys=True,
        mysql_checksum=True,
        mysql_delay_key_write=True,
        mysql_stats_persistent=True,
        mysql_stats_auto_recalc=False,
        mysql_stats_sample_pages=32,
        mysql_avg_row_length=64,
        mysql_max_rows=1000,
        mysql_min_rows=10,
        mysql_insert_method="LAST",
        mysql_data_directory="/var/lib/mysql/data",
        mysql_index_directory="/var/lib/mysql/index",
        mysql_connection="mysql://remote.example/db/flavor",
        mysql_union=["flavor_hot", "flavor_cold"],
        mysql_partition_by="HASH (id)",
        mysql_partitions=4,
        mysql_subpartition_by="KEY (id)",
        mysql_subpartitions=2,
        mysql_auto_increment=101,
        postgres_inherits=["base_flavor"],
        postgres_with=[("fillfactor", "70")],
        postgres_using="heap",
        postgres_unlogged=True,
        postgres_partition_by="RANGE (id)",
        sqlite_strict=True,
        sqlite_without_rowid=True,
        oracle_compress=True,
    )

    renamed = planning._table_snapshot_with_name(original, "__tmp_flavor", indexes=[])

    assert renamed.name == "__tmp_flavor"
    assert renamed.schema == "inventory"
    assert renamed.indexes == []
    assert renamed.model_key == original.model_key
    assert renamed.primary_key == original.primary_key
    assert renamed.columns == original.columns
    assert renamed.unique_constraints == original.unique_constraints
    assert renamed.named_unique_constraints == original.named_unique_constraints
    assert renamed.check_constraints == original.check_constraints
    assert renamed.foreign_key_constraints == original.foreign_key_constraints
    assert renamed.exclusion_constraints == original.exclusion_constraints
    assert renamed.comment == original.comment
    assert renamed.tablespace == original.tablespace
    assert renamed.mysql_engine == original.mysql_engine
    assert renamed.mysql_charset == original.mysql_charset
    assert renamed.mysql_collation == original.mysql_collation
    assert renamed.mysql_row_format == original.mysql_row_format
    assert renamed.mysql_key_block_size == original.mysql_key_block_size
    assert renamed.mysql_pack_keys == original.mysql_pack_keys
    assert renamed.mysql_checksum == original.mysql_checksum
    assert renamed.mysql_delay_key_write == original.mysql_delay_key_write
    assert renamed.mysql_stats_persistent == original.mysql_stats_persistent
    assert renamed.mysql_stats_auto_recalc == original.mysql_stats_auto_recalc
    assert renamed.mysql_stats_sample_pages == original.mysql_stats_sample_pages
    assert renamed.mysql_avg_row_length == original.mysql_avg_row_length
    assert renamed.mysql_max_rows == original.mysql_max_rows
    assert renamed.mysql_min_rows == original.mysql_min_rows
    assert renamed.mysql_insert_method == original.mysql_insert_method
    assert renamed.mysql_data_directory == original.mysql_data_directory
    assert renamed.mysql_index_directory == original.mysql_index_directory
    assert renamed.mysql_connection == original.mysql_connection
    assert renamed.mysql_union == original.mysql_union
    assert renamed.mysql_partition_by == original.mysql_partition_by
    assert renamed.mysql_partitions == original.mysql_partitions
    assert renamed.mysql_subpartition_by == original.mysql_subpartition_by
    assert renamed.mysql_subpartitions == original.mysql_subpartitions
    assert renamed.mysql_auto_increment == original.mysql_auto_increment
    assert renamed.postgres_inherits == original.postgres_inherits
    assert renamed.postgres_with == original.postgres_with
    assert renamed.postgres_using == original.postgres_using
    assert renamed.postgres_unlogged == original.postgres_unlogged
    assert renamed.postgres_partition_by == original.postgres_partition_by
    assert renamed.sqlite_strict == original.sqlite_strict
    assert renamed.sqlite_without_rowid == original.sqlite_without_rowid
    assert renamed.oracle_compress == original.oracle_compress


def test_sqlite_rebuild_rewrites_rebuild_operations(monkeypatch) -> None:
    before_table = table(
        "flavor",
        [
            column("id", primary_key=True),
            column("name"),
            column("code"),
        ],
    )
    after_table = table(
        "flavor",
        [
            column("id", primary_key=True),
            column("name"),
        ],
        indexes=[IndexSnapshot("flavor_name_idx", ["name"], unique=False)],
    )

    def fake_compile_schema_diff(
        dialect: str,
        from_snapshot: SchemaSnapshot,
        to_snapshot: SchemaSnapshot,
    ) -> list[dict[str, str]]:
        del dialect, from_snapshot
        target = to_snapshot.tables[0]
        return [
            {"sql": f'CREATE TABLE "{target.name}" ("id" TEXT, "name" TEXT)'},
            {
                "sql": f'CREATE INDEX "{target.name}_name_idx" ON "{target.name}" ("name")'
            },
        ]

    monkeypatch.setattr(
        planning,
        "_compile_schema_diff",
        fake_compile_schema_diff,
    )
    operation = MigrationOperation(
        'ALTER TABLE "flavor" DROP COLUMN "code"',
        table="flavor",
        requires_rebuild=True,
    )

    rewritten = planning._rewrite_sqlite_rebuild_operations(
        [operation],
        SchemaSnapshot(tables=[before_table]),
        SchemaSnapshot(tables=[after_table]),
        destructive=True,
        unsafe=True,
    )

    phases = [item.metadata.get("phase") for item in rewritten]
    assert phases == [
        "drop_temp",
        "create_temp",
        "copy_rows",
        "drop_old",
        "rename",
        "create_index",
    ]
    assert rewritten[2].sql == (
        'INSERT INTO "__ormdantic_rebuild_flavor" ("id", "name") '
        'SELECT "id", "name" FROM "flavor"'
    )
    assert rewritten[3].destructive


def test_sqlite_unresolved_rebuild_operations_raise() -> None:
    with pytest.raises(ValueError, match="require a table rebuild"):
        planning._raise_if_unsupported_sqlite_plan(
            "sqlite",
            [
                MigrationOperation(
                    'ALTER TABLE "flavor" DROP COLUMN "code"',
                    requires_rebuild=True,
                )
            ],
        )


def test_planning_helper_edge_branches_cover_metadata_and_parser_fallbacks() -> None:
    identity_default = ColumnSnapshot(
        "id",
        "int",
        False,
        True,
        identity=True,
        identity_min_value=1,
        identity_max_value=2147483647,
    )
    identity_implicit = ColumnSnapshot("id", "int", False, True, identity=True)
    assert not planning._identity_bound_changed(
        identity_default,
        identity_implicit,
        "identity_min_value",
        "identity_no_min_value",
        dialect="postgresql",
    )
    assert not planning._identity_bound_changed(
        identity_implicit,
        identity_default,
        "identity_max_value",
        "identity_no_max_value",
        dialect="postgresql",
    )
    assert planning._identity_bound_changed(
        ColumnSnapshot("id", "int", False, True, identity_min_value=1),
        ColumnSnapshot(
            "id",
            "int",
            False,
            True,
            identity_min_value=1,
            identity_no_min_value=True,
        ),
        "identity_min_value",
        "identity_no_min_value",
    )
    assert planning._default_identity_bound(None, identity_default, bound="min") is None
    assert (
        planning._default_identity_bound(
            "oracle",
            ColumnSnapshot("id", "int", False, True, identity_increment=-1),
            bound="max",
        )
        == -1
    )

    operation = MigrationOperation('CREATE TABLE "flavor" ("id" TEXT)')
    assert planning._rewrite_sqlite_rebuild_operations(
        [operation],
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        destructive=False,
        unsafe=False,
    ) == [operation]
    with pytest.raises(ValueError, match="cannot rebuild unknown sqlite table"):
        planning._sqlite_rebuild_table_operations(
            "missing",
            SchemaSnapshot.empty(),
            SchemaSnapshot.empty(),
            destructive=True,
            unsafe=True,
        )
    rebuild_ops = planning._sqlite_rebuild_table_operations(
        "flavor",
        SchemaSnapshot(
            tables=[
                table(
                    "flavor",
                    [column("id", primary_key=True), column("legacy")],
                )
            ]
        ),
        SchemaSnapshot(
            tables=[
                table(
                    "flavor",
                    [column("id", primary_key=True), column("name")],
                    indexes=[IndexSnapshot("flavor_name_idx", ["name"])],
                )
            ]
        ),
        destructive=True,
        unsafe=True,
    )
    copy_rows = next(
        operation
        for operation in rebuild_ops
        if operation.metadata.get("phase") == "copy_rows"
    )
    assert copy_rows.sql == (
        'INSERT INTO "__ormdantic_rebuild_flavor" ("id") SELECT "id" FROM "flavor"'
    )

    assert (
        planning._classify_sql_operation(
            "IF SCHEMA_ID(N'app') IS NULL EXEC(N'CREATE SCHEMA app')"
        )["kind"]
        == "create_schema"
    )
    assert (
        planning._classify_sql_operation("CREATE TYPE flavor_kind AS ENUM ('mocha')")[
            "kind"
        ]
        == "create_enum_type"
    )
    assert planning._classify_sql_operation("ALTER INDEX ix REBUILD")["kind"] == (
        "alter_index"
    )
    assert not planning._classify_sql_operation("ALTER INDEX ix SET TABLESPACE fast")[
        "unsafe"
    ]
    assert planning._sql_identifier_after("CREATE TABLE", "CREATE TABLE") is None
    assert planning._sql_identifier_after_keyword("ALTER TABLE flavor", "ADD") is None
    assert planning._sql_identifier_after_keyword("CREATE INDEX ix", " ON ") is None
    assert planning._table_from_column_identifier(None) is None
    assert planning._table_from_column_identifier("id") == "id"
    assert planning._sql_identifier_after_keyword("ADD", "ADD") is None

    with pytest.raises(ValueError, match="at least one migration artifact"):
        planning.squash_migrations("squashed", [])
    no_dialect = migrations.MigrationArtifact.from_plan(
        "001",
        MigrationPlan(),
        SchemaSnapshot.empty(),
        SchemaSnapshot.empty(),
        dialect=None,
    )
    with pytest.raises(ValueError, match="dialect is required"):
        planning.squash_migrations("squashed", [no_dialect])

    assert planning._has_wrapping_parentheses("('it''s ok')")
    assert planning._has_wrapping_parentheses('("identifier")')
    assert planning._has_wrapping_parentheses("([identifier])")
    assert not planning._has_wrapping_parentheses("(value) + 1")
    assert not planning._has_wrapping_parentheses(")value(")

    with pytest.raises(ValueError, match="bitmap indexes cannot be unique"):
        planning._append_oracle_index_options(
            "oracle",
            'CREATE UNIQUE INDEX "flavor_name_idx" ON "flavor" ("name")',
            {"bitmap": True},
        )
    assert (
        planning._append_oracle_index_options(
            "oracle",
            'CREATE INDEX "flavor_name_idx" ON "flavor" ("name")',
            {"bitmap": True, "compress": True, "tablespace": "fastspace"},
        )
        == 'CREATE BITMAP INDEX "flavor_name_idx" ON "flavor" ("name") '
        'COMPRESS TABLESPACE "fastspace"'
    )

    assert planning._create_namespace_sql(
        "mssql",
        NamespaceSnapshot("app"),
    ).startswith("IF SCHEMA_ID(N'app') IS NULL EXEC")
    with pytest.raises(ValueError, match="namespace migrations"):
        planning._drop_namespace_sql("sqlite", NamespaceSnapshot("app"))

    descending = SequenceSnapshot(
        "flavor_seq",
        increment=-1,
        data_type="smallint",
    )
    assert (
        planning._default_sequence_bound("postgresql", descending, bound="min")
        == -32768
    )
    assert planning._default_sequence_bound("postgresql", descending, bound="max") == -1
    assert planning._default_sequence_bound("oracle", descending, bound="max") == -1
    assert planning._default_sequence_bound(None, descending, bound="min") is None

    assert planning._annotate_postgres_unique_include_sql(
        "postgresql",
        ["CREATE TABLE flavor (id TEXT)"],
        [],
    ) == ["CREATE TABLE flavor (id TEXT)"]
    with pytest.raises(ValueError, match="INCLUDE columns"):
        planning._postgres_unique_include_constraint_sql(
            "CONSTRAINT flavor_name_unique CHECK (name <> '')",
            ["code"],
        )
    with pytest.raises(ValueError, match="INCLUDE columns"):
        planning._postgres_unique_include_constraint_sql(
            "CONSTRAINT flavor_name_unique UNIQUE name",
            ["code"],
        )
    with pytest.raises(ValueError, match="INCLUDE columns"):
        planning._postgres_unique_include_constraint_sql(
            "CONSTRAINT flavor_name_unique UNIQUE (name",
            ["code"],
        )
    assert (
        planning._postgres_unique_include_create_table_sql(
            "CREATE TABLE flavor id TEXT",
            {("flavor", "flavor_name_unique"): ["code"]},
        )
        == "CREATE TABLE flavor id TEXT"
    )
    assert (
        planning._postgres_unique_include_create_table_sql(
            "CREATE TABLE flavor (CONSTRAINT flavor_name_unique UNIQUE (name)",
            {("flavor", "flavor_name_unique"): ["code"]},
        )
        == "CREATE TABLE flavor (CONSTRAINT flavor_name_unique UNIQUE (name)"
    )
    assert (
        planning._postgres_unique_include_alter_table_sql(
            "ALTER TABLE flavor ADD CHECK (name <> '')",
            {("flavor", "flavor_name_unique"): ["code"]},
        )
        == "ALTER TABLE flavor ADD CHECK (name <> '')"
    )

    with pytest.raises(ValueError, match="NULLS NOT DISTINCT"):
        planning._annotate_postgres_index_nulls_not_distinct_sql(
            "sqlite",
            ["CREATE UNIQUE INDEX flavor_name_idx ON flavor (name)"],
            [
                table(
                    "flavor",
                    [column("id", primary_key=True), column("name")],
                    indexes=[
                        IndexSnapshot(
                            "flavor_name_idx",
                            ["name"],
                            unique=True,
                            postgres_nulls_not_distinct=True,
                        )
                    ],
                )
            ],
        )
    with pytest.raises(ValueError, match="non-unique"):
        planning._postgres_index_nulls_not_distinct_sql(
            "CREATE INDEX flavor_name_idx ON flavor (name)"
        )
    with pytest.raises(ValueError, match="CREATE INDEX SQL"):
        planning._postgres_index_nulls_not_distinct_sql(
            "CREATE UNIQUE INDEX flavor_name_idx ON flavor"
        )
    with pytest.raises(ValueError, match="after INCLUDE"):
        planning._postgres_index_nulls_not_distinct_sql(
            "CREATE UNIQUE INDEX flavor_name_idx ON flavor (name) INCLUDE code"
        )
    with pytest.raises(ValueError, match="after INCLUDE"):
        planning._postgres_index_nulls_not_distinct_sql(
            "CREATE UNIQUE INDEX flavor_name_idx ON flavor (name) INCLUDE (code"
        )

    with pytest.raises(ValueError, match="index prefix"):
        planning._mysql_index_prefix_sql("CREATE INDEX ix", "FULLTEXT")
    with pytest.raises(ValueError, match="non-unique"):
        planning._mysql_index_prefix_sql(
            "CREATE UNIQUE INDEX ix ON flavor (name)",
            "FULLTEXT",
        )
    assert planning._annotate_mysql_index_using_sql(
        "mysql",
        ["CREATE INDEX ix ON flavor (name)"],
        [],
    ) == ["CREATE INDEX ix ON flavor (name)"]
    with pytest.raises(ValueError, match="USING method"):
        planning._mysql_index_using_sql("CREATE INDEX ix", "HASH")
    with pytest.raises(ValueError, match="cannot be combined"):
        planning._mysql_index_using_sql(
            "CREATE FULLTEXT INDEX ix ON flavor (name)",
            "HASH",
        )
    assert (
        planning._mysql_index_length_sql(
            "CREATE INDEX ix ON flavor (LOWER(name), code)",
            {"code": 12},
        )
        == "CREATE INDEX ix ON flavor (LOWER(name), code(12))"
    )
    with pytest.raises(ValueError, match="prefix lengths"):
        planning._mysql_index_length_sql("CREATE INDEX ix ON flavor", {"name": 8})
    with pytest.raises(ValueError, match="reference columns"):
        planning._mysql_index_length_sql(
            "CREATE INDEX ix ON flavor (name)",
            {"code": 8},
        )
    assert planning._annotate_mysql_index_visibility_sql(
        "mysql",
        ["CREATE INDEX ix ON flavor (name)"],
        [],
    ) == ["CREATE INDEX ix ON flavor (name)"]
    with pytest.raises(ValueError, match="index visibility"):
        planning._mysql_index_visibility_sql("CREATE INDEX ix", False)

    assert planning._leading_sql_identifier_bounds("   ") is None
    assert planning._leading_sql_identifier_bounds(" 9bad") is None
    assert planning._leading_sql_identifier_bounds('"unterminated') is None
    assert planning._leading_sql_identifier_bounds("[unterminated") is None
    assert planning._leading_index_column_identifier_bounds(" 9bad") is None
    assert planning._leading_index_column_identifier_bounds("lower(name)") is None
    assert planning._quoted_identifier_end('"a""b" tail', 0, '"') == 6
    assert planning._find_sql_char("['('] \"(\" (target)", "(", 0) == 10
    assert planning._find_matching_sql_paren("(name, '[)]', [x]) tail", 0) == 17
    assert planning._find_matching_sql_paren("(name", 0) is None
    assert planning._create_index_statement_key("CREATE INDEX ix flavor (name)") is None
    assert planning._create_index_statement_prefix("CREATE UNKNOWN INDEX") is None
    assert planning._mssql_clustered_index_sql("CREATE SPATIAL INDEX ix ON t (g)") == (
        "CREATE SPATIAL INDEX ix ON t (g)"
    )
    with pytest.raises(ValueError, match="multiple SQL Server clustered"):
        planning._mssql_clustered_indexes_by_key(
            [
                table(
                    "flavor",
                    [column("id", primary_key=True), column("name"), column("code")],
                    indexes=[
                        IndexSnapshot(
                            "flavor_name_idx",
                            ["name"],
                            mssql_clustered=True,
                        ),
                        IndexSnapshot(
                            "flavor_code_idx",
                            ["code"],
                            mssql_clustered=True,
                        ),
                    ],
                )
            ]
        )


def test_remaining_planning_helper_edges_cover_schema_objects_and_sql_fallbacks() -> (
    None
):
    before = SchemaSnapshot(
        enum_types=[
            EnumTypeSnapshot("flavor_kind", ["mocha"], schema="inventory"),
        ],
        sequences=[
            SequenceSnapshot("flavor_id_seq", schema="inventory"),
        ],
        views=[
            ViewSnapshot("flavor_view", "SELECT id FROM flavor", schema="inventory"),
        ],
    )
    diff = planning.diff_snapshots(before, SchemaSnapshot.empty(), dialect="postgresql")
    assert any(change.object_type == "enum_type" for change in diff.changes)
    assert any(change.object_type == "sequence" for change in diff.changes)
    assert any(change.object_type == "view" for change in diff.changes)

    assert "identity_min_value" in planning._changed_column_fields(
        ColumnSnapshot("id", "int", False, True, identity_min_value=5),
        ColumnSnapshot("id", "int", False, True, identity_min_value=6),
        dialect="postgresql",
    )
    payload = planning._operation_payload(
        MigrationPlan([MigrationOperation("SELECT ?", values=(1,))])
    )
    assert payload == [("SELECT ?", (1,))]

    changed_tables = [
        table(f"flavor_{index}", [column("id", primary_key=True)], tablespace="old")
        for index in range(4)
    ]
    moved_tables = [
        table(f"flavor_{index}", [column("id", primary_key=True)], tablespace="new")
        for index in range(4)
    ]
    with pytest.raises(ValueError, match=r"flavor_0, flavor_1, flavor_2, \.\.\."):
        planning._raise_if_unsupported_mssql_table_filegroup_changes(
            "mssql",
            SchemaSnapshot(tables=changed_tables),
            SchemaSnapshot(tables=moved_tables),
        )
    assert planning._compile_mariadb_table_tablespace_diff(
        "mariadb",
        SchemaSnapshot(tables=[changed_tables[0]]),
        SchemaSnapshot(tables=[moved_tables[0]]),
    )[0]["sql"].startswith("ALTER TABLE")
    assert (
        planning._snapshot_without_mariadb_table_tablespace_changes(
            SchemaSnapshot(tables=[changed_tables[0]]),
            SchemaSnapshot(tables=[moved_tables[0]]),
        )
        .tables[0]
        .tablespace
        == "old"
    )
    assert (
        planning._snapshot_without_unmanaged_mysql_auto_increment(
            SchemaSnapshot(
                tables=[
                    table(
                        "flavor",
                        [column("id", primary_key=True)],
                        mysql_auto_increment=42,
                    )
                ]
            ),
            SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])]),
        )
        .tables[0]
        .mysql_auto_increment
        is None
    )

    assert (
        planning._postgres_unique_include_statement_sql(
            "SELECT 1",
            {("flavor", "flavor_name_unique"): ["code"]},
        )
        == "SELECT 1"
    )
    assert (
        planning._postgres_unique_include_create_table_sql(
            "CREATE TABLE",
            {("flavor", "flavor_name_unique"): ["code"]},
        )
        == "CREATE TABLE"
    )
    assert (
        planning._postgres_unique_include_create_table_sql(
            "CREATE TABLE flavor (CONSTRAINT other UNIQUE (name))",
            {("flavor", "flavor_name_unique"): ["code"]},
        )
        == "CREATE TABLE flavor (CONSTRAINT other UNIQUE (name))"
    )
    assert (
        planning._postgres_unique_include_alter_table_sql(
            "ALTER TABLE flavor ADD CONSTRAINT other UNIQUE (name)",
            {("flavor", "flavor_name_unique"): ["code"]},
        )
        == "ALTER TABLE flavor ADD CONSTRAINT other UNIQUE (name)"
    )
    assert (
        planning._postgres_index_ops_sql(
            "CREATE INDEX ix ON flavor (lower(name))",
            {"lower(name)": "text_pattern_ops"},
        )
        == "CREATE INDEX ix ON flavor (lower(name) text_pattern_ops)"
    )
    assert planning._leading_index_column_identifier_bounds("lower(name)") is None
    with pytest.raises(ValueError, match="requires unique indexes"):
        planning._postgres_nulls_not_distinct_indexes_by_key(
            [
                table(
                    "flavor",
                    [column("id", primary_key=True), column("name")],
                    indexes=[
                        IndexSnapshot(
                            "flavor_name_idx",
                            ["name"],
                            unique=False,
                            postgres_nulls_not_distinct=True,
                        )
                    ],
                )
            ]
        )

    assert planning._index_column_list_bounds("CREATE INDEX ix ON flavor (name") is None
    assert planning._find_sql_char("'a''b' target", "t", 0) == 7
    assert planning._find_matching_sql_paren("('a''b')", 0) == 7
    assert planning._split_top_level_sql_list("'a'',b', c") == ["'a'',b'", "c"]
    assert planning._mysql_index_column_length_item_sql("1bad", {"bad": 4}, set()) == (
        "1bad"
    )
    with pytest.raises(ValueError, match="inline index comments"):
        planning._append_inline_index_comment_sql("sqlite", "CREATE INDEX ix", "note")

    unsupported_index_table = table(
        "flavor",
        [column("id", primary_key=True), column("name")],
        indexes=[IndexSnapshot("flavor_name_idx", ["name"], mssql_clustered=True)],
    )
    with pytest.raises(ValueError, match="clustered indexes"):
        planning._annotate_mssql_clustered_index_sql(
            "sqlite",
            ["CREATE INDEX flavor_name_idx ON flavor (name)"],
            [unsupported_index_table],
        )
    unsupported_filegroup_table = table(
        "flavor",
        [column("id", primary_key=True), column("name")],
        indexes=[
            IndexSnapshot("flavor_name_idx", ["name"], mssql_filegroup="fastdata")
        ],
    )
    with pytest.raises(ValueError, match="index filegroups"):
        planning._annotate_mssql_index_filegroup_sql(
            "sqlite",
            ["CREATE INDEX flavor_name_idx ON flavor (name)"],
            [unsupported_filegroup_table],
        )
    oracle_table = table(
        "flavor",
        [column("id", primary_key=True), column("name")],
        indexes=[
            IndexSnapshot(
                "flavor_name_idx",
                ["name"],
                oracle_tablespace="fastspace",
                oracle_compress=2,
            )
        ],
    )
    assert planning._annotate_oracle_index_tablespace_operations(
        "oracle",
        [{"sql": 'CREATE INDEX "flavor_name_idx" ON "flavor" ("name")'}],
        [oracle_table],
    )[0]["sql"].endswith('COMPRESS 2 TABLESPACE "fastspace"')
    assert planning._annotate_oracle_index_tablespace_sql(
        "oracle",
        ['CREATE INDEX "flavor_name_idx" ON "flavor" ("name")'],
        [oracle_table],
    )[0].endswith('COMPRESS 2 TABLESPACE "fastspace"')
    assert planning._oracle_index_tablespaces_by_key([oracle_table]) == {
        ("flavor", "flavor_name_idx"): "fastspace"
    }

    metadata_table = table(
        "flavor",
        [column("id", primary_key=True), column("name")],
        indexes=[
            IndexSnapshot(
                "flavor_name_idx",
                ["name"],
                comment="Name lookup",
                postgres_tablespace="fastspace",
                postgres_ops={"name": "text_pattern_ops"},
                postgres_nulls_not_distinct=True,
                mssql_filegroup="fastdata",
                oracle_tablespace="fastspace",
                oracle_bitmap=True,
                oracle_compress=True,
                mysql_prefix="FULLTEXT",
                mysql_length={"name": 12},
                mysql_using="BTREE",
                mysql_visible=False,
            )
        ],
        named_unique_constraints=[
            UniqueConstraintSnapshot(
                "flavor_name_unique",
                ["name"],
                comment="unique name",
                postgres_include=["id"],
            )
        ],
        check_constraints=[
            TableCheckSnapshot("flavor_name_check", "name <> ''", comment="check")
        ],
        foreign_key_constraints=[
            ForeignKeyConstraintSnapshot(
                "flavor_parent_fk",
                ["id"],
                "flavor",
                ["id"],
                comment="parent",
            )
        ],
        exclusion_constraints=[
            ExclusionConstraintSnapshot(
                "flavor_name_excl",
                columns=[("name", "=")],
                comment="exclude",
            )
        ],
    )
    normalized = planning._snapshot_without_python_metadata(
        SchemaSnapshot(tables=[metadata_table])
    ).tables[0]
    assert normalized.indexes[0].comment is None
    assert normalized.indexes[0].postgres_ops == {}
    assert normalized.named_unique_constraints[0].postgres_include == []
    assert normalized.check_constraints[0].comment is None
    assert normalized.foreign_key_constraints[0].comment is None
    assert normalized.exclusion_constraints[0].comment is None

    unmatched_check = table(
        "flavor",
        [column("id", primary_key=True)],
        check_constraints=[TableCheckSnapshot("missing_in_target", "id > 0")],
    )
    planning._snapshots_with_normalized_matching_table_checks(
        SchemaSnapshot(tables=[unmatched_check]),
        SchemaSnapshot(tables=[table("flavor", [column("id", primary_key=True)])]),
    )
    assert (
        planning._snapshot_with_table_check_expressions(
            SchemaSnapshot(tables=[unmatched_check]),
            {},
        ).tables[0]
        is unmatched_check
    )
    assert (
        planning._snapshot_with_table_check_expressions(
            SchemaSnapshot(
                tables=[
                    unmatched_check,
                    table("other", [column("id", primary_key=True)]),
                ]
            ),
            {("public", "missing"): {"missing": "id > 0"}},
        ).tables[0]
        is unmatched_check
    )
    assert not planning._has_wrapping_parentheses("(value) + 1")

    namespace_before, namespace_after = planning._compile_namespace_diff(
        "mssql",
        SchemaSnapshot(namespaces=[NamespaceSnapshot("app", comment="old")]),
        SchemaSnapshot(
            namespaces=[
                NamespaceSnapshot("app", comment=None),
                NamespaceSnapshot("archive", comment="Archive"),
            ]
        ),
    )
    assert any("sp_dropextendedproperty" in item["sql"] for item in namespace_before)
    assert any("CREATE SCHEMA" in item["sql"] for item in namespace_before)
    assert namespace_after == []
    namespace_comment_before, _namespace_comment_after = (
        planning._compile_namespace_diff(
            "postgresql",
            SchemaSnapshot(namespaces=[NamespaceSnapshot("app")]),
            SchemaSnapshot(
                namespaces=[NamespaceSnapshot("app", comment="Application")]
            ),
        )
    )
    assert namespace_comment_before == [
        {"sql": "COMMENT ON SCHEMA \"app\" IS 'Application'"}
    ]
    enum_before, enum_after = planning._compile_enum_type_diff(
        "postgresql",
        SchemaSnapshot(enum_types=[EnumTypeSnapshot("kind", ["a"], comment="old")]),
        SchemaSnapshot(
            enum_types=[EnumTypeSnapshot("kind", ["a", "b"], comment="new")]
        ),
    )
    assert enum_before == []
    assert any("COMMENT ON TYPE" in item["sql"] for item in enum_after)
    sequence_before, sequence_after = planning._compile_sequence_diff(
        "postgresql",
        SchemaSnapshot(sequences=[SequenceSnapshot("seq", start=1, comment="old")]),
        SchemaSnapshot(sequences=[SequenceSnapshot("seq", start=2, comment="new")]),
    )
    assert sequence_before == []
    assert any("COMMENT ON SEQUENCE" in item["sql"] for item in sequence_after)
    sequence_comment_before, sequence_comment_after = planning._compile_sequence_diff(
        "postgresql",
        SchemaSnapshot(sequences=[SequenceSnapshot("seq", comment="old")]),
        SchemaSnapshot(sequences=[SequenceSnapshot("seq", comment="new")]),
    )
    assert sequence_comment_after == []
    assert sequence_comment_before == [{"sql": "COMMENT ON SEQUENCE \"seq\" IS 'new'"}]

    index_error_cases = [
        (
            "sqlite",
            IndexSnapshot("ix", ["name"], mssql_filegroup="fg"),
            "SQL Server index",
        ),
        (
            "sqlite",
            IndexSnapshot("ix", ["name"], oracle_tablespace="fast"),
            "Oracle index",
        ),
        (
            "sqlite",
            IndexSnapshot("ix", ["name"], postgres_ops={"name": "text_ops"}),
            "operator classes",
        ),
        (
            "sqlite",
            IndexSnapshot(
                "ix", ["name"], unique=True, postgres_nulls_not_distinct=True
            ),
            "NULLS NOT DISTINCT",
        ),
        (
            "mariadb",
            IndexSnapshot("ix", ["name"], mysql_visible=False),
            "index visibility",
        ),
        (
            "sqlite",
            IndexSnapshot("ix", ["name"], mysql_prefix="FULLTEXT"),
            "prefixes",
        ),
    ]
    for dialect, index, message in index_error_cases:
        with pytest.raises(ValueError, match=message):
            planning._compile_index_metadata_diff(
                dialect,
                SchemaSnapshot.empty(),
                SchemaSnapshot(
                    tables=[
                        table(
                            "flavor",
                            [column("id", primary_key=True), column("name")],
                            indexes=[index],
                        )
                    ]
                ),
            )
    with pytest.raises(ValueError, match="requires unique indexes"):
        planning._compile_index_metadata_diff(
            "postgresql",
            SchemaSnapshot.empty(),
            SchemaSnapshot(
                tables=[
                    table(
                        "flavor",
                        [column("id", primary_key=True), column("name")],
                        indexes=[
                            IndexSnapshot(
                                "ix",
                                ["name"],
                                unique=False,
                                postgres_nulls_not_distinct=True,
                            )
                        ],
                    )
                ]
            ),
        )
    with pytest.raises(ValueError, match="INCLUDE columns"):
        planning._compile_unique_constraint_metadata_diff(
            "sqlite",
            SchemaSnapshot.empty(),
            SchemaSnapshot(
                tables=[
                    table(
                        "flavor",
                        [column("id", primary_key=True), column("name")],
                        named_unique_constraints=[
                            UniqueConstraintSnapshot(
                                "uq_name",
                                ["name"],
                                postgres_include=["id"],
                            )
                        ],
                    )
                ]
            ),
        )
    assert planning._explicit_constraint_definition(None, "missing") is None
    assert (
        planning._explicit_constraint_definition(
            table("flavor", [column("id", primary_key=True)]),
            "missing",
        )
        is None
    )

    basic_table = table("flavor", [column("id", primary_key=True)])
    basic_index = IndexSnapshot("ix", ["id"])
    with pytest.raises(ValueError, match="enum type comments"):
        planning._set_enum_type_comment_sql(
            "sqlite", EnumTypeSnapshot("kind", ["a"], comment=None)
        )
    assert (
        planning._set_index_tablespace_sql(
            "postgresql",
            basic_table,
            basic_index,
            for_create=True,
        )
        is None
    )
    with pytest.raises(ValueError, match="standalone index comments"):
        planning._set_index_comment_sql("sqlite", basic_table, basic_index)
    with pytest.raises(ValueError, match="index tablespaces"):
        planning._set_index_tablespace_sql("sqlite", basic_table, basic_index)
    with pytest.raises(ValueError, match="table tablespaces"):
        planning._set_mysql_table_tablespace_sql("sqlite", basic_table)
    with pytest.raises(ValueError, match="constraint comments"):
        planning._set_constraint_comment_sql("sqlite", basic_table, "pk", "comment")
    assert "sp_dropextendedproperty" in planning._mssql_constraint_comment_sql(
        basic_table,
        "pk_flavor",
        None,
    )
    assert planning._quote_table_name("postgresql", basic_table) == '"flavor"'
    assert planning._quote_index_name("postgresql", basic_table, basic_index) == '"ix"'

    with pytest.raises(ValueError, match="namespace migrations"):
        planning._create_namespace_sql("sqlite", NamespaceSnapshot("app"))
    assert (
        planning._set_namespace_comment_sql(
            "postgresql",
            "app",
            None,
            for_create=True,
        )
        is None
    )
    with pytest.raises(ValueError, match="namespace comments"):
        planning._set_namespace_comment_sql("mysql", "app", "comment")
    assert "sp_dropextendedproperty" in planning._mssql_namespace_comment_sql(
        "app",
        None,
    )
    assert "sp_dropextendedproperty" in planning._mssql_index_comment_sql(
        basic_table,
        basic_index,
    )

    with pytest.raises(ValueError, match="sequence migrations"):
        planning._create_sequence_sql("sqlite", SequenceSnapshot("seq"))
    with pytest.raises(ValueError, match="no_min_value"):
        planning._create_sequence_sql(
            "postgresql", SequenceSnapshot("seq", no_min_value=True, min_value=1)
        )
    with pytest.raises(ValueError, match="no_max_value"):
        planning._create_sequence_sql(
            "postgresql", SequenceSnapshot("seq", no_max_value=True, max_value=10)
        )
    with pytest.raises(ValueError, match="sequence ordering"):
        planning._create_sequence_sql("mssql", SequenceSnapshot("seq", order=True))
    assert (
        planning._set_sequence_comment_sql(
            "postgresql",
            SequenceSnapshot("seq"),
            for_create=True,
        )
        is None
    )
    with pytest.raises(ValueError, match="sequence comments"):
        planning._set_sequence_comment_sql(
            "oracle", SequenceSnapshot("seq", comment="x")
        )
    assert "sp_dropextendedproperty" in planning._mssql_sequence_comment_sql(
        SequenceSnapshot("seq")
    )
    assert (
        planning._quote_sequence_name("postgresql", SequenceSnapshot("seq")) == '"seq"'
    )
    assert planning._sequence_qualified_name(SequenceSnapshot("seq")) == "seq"
    assert (
        planning._default_sequence_bound(
            "mssql",
            SequenceSnapshot("seq", data_type="tinyint"),
            bound="min",
        )
        == 0
    )
    assert planning._default_sequence_bound(
        "oracle",
        SequenceSnapshot("seq", increment=-1),
        bound="min",
    ) == -(10**27 - 1)
    assert (
        planning._default_sequence_bound(
            "mariadb",
            SequenceSnapshot("seq", increment=-1, data_type="tinyint unsigned"),
            bound="min",
        )
        == 0
    )
    assert (
        planning._default_sequence_bound(
            "mariadb",
            SequenceSnapshot("seq", data_type="smallint"),
            bound="max",
        )
        == 32767
    )
    assert planning._normalized_integer_type_name(None) is None

    with pytest.raises(ValueError, match="view migrations"):
        planning._create_view_sql("unknown", ViewSnapshot("v", "SELECT 1"))
    with pytest.raises(ValueError, match="materialized view migrations"):
        planning._create_view_sql(
            "sqlite", ViewSnapshot("v", "SELECT 1", materialized=True)
        )
    assert (
        planning._set_view_comment_sql(
            "postgresql",
            ViewSnapshot("v", "SELECT 1"),
            for_create=True,
        )
        is None
    )
    with pytest.raises(ValueError, match="view comments"):
        planning._set_view_comment_sql(
            "sqlite", ViewSnapshot("v", "SELECT 1", comment="x")
        )
    assert "sp_dropextendedproperty" in planning._mssql_view_comment_sql(
        "app.v",
        None,
    )
    assert (
        planning._compiled_view_comment_sql(
            "postgresql",
            ViewSnapshot("v", "SELECT 1"),
            for_create=True,
        )
        is None
    )
    view_comment_before, view_comment_after = planning._compile_view_diff(
        "postgresql",
        SchemaSnapshot(views=[ViewSnapshot("v", "SELECT 1", comment="old")]),
        SchemaSnapshot(views=[ViewSnapshot("v", "SELECT 1", comment="new")]),
    )
    assert view_comment_after == []
    assert [item["sql"] for item in view_comment_before] == [
        "COMMENT ON VIEW \"v\" IS 'new'"
    ]

    assert planning._mysql_simple_view_signature("not select") is None
    assert planning._mysql_simple_view_signature("SELECT id FROM bad table") is None
    assert planning._mysql_simple_view_column_signature("name AS bad alias") is None
    assert planning._mysql_view_identifier_name(None) is None
    assert planning._mysql_view_identifier_parts("") is None
    assert planning._mysql_view_identifier_part("9bad") is None

    assert planning._postgres_simple_view_signature("not select") is None
    assert planning._postgres_simple_view_signature("SELECT id FROM bad table") is None
    assert planning._postgres_simple_view_column_signature("name AS bad alias") is None
    assert planning._postgres_view_identifier_name(None) is None
    assert planning._postgres_view_identifier_parts("") is None
    assert planning._postgres_view_identifier_part('""') is None
    assert planning._postgres_simple_view_column_signature("id AS id") == (
        "id",
        None,
    )

    assert planning._mssql_simple_view_signature("not select") is None
    assert planning._mssql_simple_view_signature("SELECT id FROM bad table") is None
    assert planning._mssql_simple_view_column_signature("name AS bad alias") is None
    assert planning._mssql_view_identifier_name(None) is None
    assert planning._mssql_view_identifier_parts("") is None
    assert planning._mssql_view_identifier_part("9bad") is None
    assert planning._mssql_simple_view_column_signature("[id] AS [id]") == (
        "id",
        None,
    )
    assert planning._split_qualified_name("flavor") == (None, "flavor")
    assert planning._split_qualified_name("inventory.flavor") == (
        "inventory",
        "flavor",
    )
    assert (
        planning._check_expression(
            "name",
            ("length", ">=", "2"),
            dialect="mssql",
        )
        == "LEN(name) >= 2"
    )
    assert planning._check_expression("kind", ("enum", "in", "'a', 'b'")) == (
        "kind IN ('a', 'b')"
    )
    assert planning._check_expression("code", ("pattern", "matches", "'^[A-Z]+$'")) == (
        "ormdantic_regex_match(code, '^[A-Z]+$')"
    )
    assert planning._check_expression("quantity", ("multiple_of", "=", "5")) == (
        "ormdantic_multiple_of(quantity, 5)"
    )
    with pytest.raises(ValueError, match="unsupported check constraint"):
        planning._check_expression("quantity", ("custom", "=", "5"))


def test_compile_index_metadata_diff_recreates_mysql_structural_option_changes() -> (
    None
):
    before = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name"),
                    column("code"),
                ],
                indexes=[
                    IndexSnapshot(
                        "flavor_lookup_idx",
                        ["name"],
                        mysql_visible=True,
                    ),
                    IndexSnapshot(
                        "flavor_prefix_idx",
                        ["name"],
                        mysql_length={"name": 12},
                        mysql_using="BTREE",
                    ),
                ],
            )
        ]
    )
    after = SchemaSnapshot(
        tables=[
            table(
                "flavor",
                [
                    column("id", primary_key=True),
                    column("name"),
                    column("code"),
                ],
                indexes=[
                    IndexSnapshot(
                        "flavor_lookup_idx",
                        ["code"],
                        mysql_visible=False,
                    ),
                    IndexSnapshot(
                        "flavor_prefix_idx",
                        ["code"],
                        mysql_prefix="FULLTEXT",
                        mysql_length={"code": 8},
                    ),
                ],
            )
        ]
    )

    statements = planning._compile_index_metadata_diff("mysql", before, after)
    sql = [statement["sql"] for statement in statements]

    assert "DROP INDEX `flavor_lookup_idx` ON `flavor`" in sql
    assert "CREATE INDEX `flavor_lookup_idx` ON `flavor` (`code`) INVISIBLE" in sql
    assert "DROP INDEX `flavor_prefix_idx` ON `flavor`" in sql
    assert "CREATE FULLTEXT INDEX `flavor_prefix_idx` ON `flavor` (`code`(8))" in sql
