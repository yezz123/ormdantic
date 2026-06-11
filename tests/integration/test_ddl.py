from __future__ import annotations

from decimal import Decimal
from enum import Enum

import pytest
from ormdantic._ormdantic import execute_native
from pydantic import BaseModel, Field

import ormdantic.orm as orm_module
from ormdantic import (
    Ormdantic,
    TableCheck,
    TableColumn,
    TableExclusion,
    TableForeignKey,
    TableIndex,
    TableUnique,
    column,
)
from ormdantic._migrations import planning
from ormdantic.migrations import (
    EnumTypeSnapshot,
    ExclusionConstraintSnapshot,
    ForeignKeyConstraintSnapshot,
    NamespaceSnapshot,
    SchemaSnapshot,
    SequenceSnapshot,
    ViewSnapshot,
)
from ormdantic.schema import compile_create_table_sql


class DdlFlavor(Enum):
    MOCHA = "mocha"
    LATTE = "latte"


def test_compile_create_table_sql_includes_types_indexes_and_checks() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        pk="id",
        indexed=["name"],
        unique=["code"],
        unique_constraints=[
            ["name", "code"],
            TableUnique(
                name="flavor_name_price_unique",
                columns=["name", "price"],
            ),
        ],
    )
    class Flavor(BaseModel):
        id: str
        name: str = Field(min_length=2, max_length=63)
        code: bytes
        price: Decimal = Field(gt=0)
        flavor: DdlFlavor

    table = db._table_map.name_to_data["flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "sqlite")
    snapshot = db.migrations.snapshot()
    native_snapshot = SchemaSnapshot.from_database(db, native_enum_types=True)

    assert statements[0] == (
        'CREATE TABLE IF NOT EXISTS "flavor" ('
        '"id" TEXT PRIMARY KEY NOT NULL, '
        '"name" TEXT NOT NULL, '
        '"code" BLOB NOT NULL, '
        '"price" DECIMAL_TEXT NOT NULL, '
        '"flavor" TEXT NOT NULL, '
        'CONSTRAINT "flavor_name_price_unique" UNIQUE ("name", "price"), '
        'CONSTRAINT "flavor_unique_0" UNIQUE ("name", "code"), '
        'CONSTRAINT "flavor_unique_1" UNIQUE ("code"), '
        'CONSTRAINT "flavor_name_min_length_check" CHECK (LENGTH(name) >= 2), '
        'CONSTRAINT "flavor_name_max_length_check" CHECK (LENGTH(name) <= 63), '
        'CONSTRAINT "flavor_price_gt_check" '
        "CHECK (ormdantic_decimal_cmp(price, '0') > 0), "
        'CONSTRAINT "flavor_flavor_enum_values_check" '
        "CHECK (flavor IN ('mocha', 'latte')))"
    )
    assert (
        'CREATE INDEX IF NOT EXISTS "flavor_name_idx" ON "flavor" ("name")'
        in statements
    )
    assert (
        'CREATE UNIQUE INDEX IF NOT EXISTS "flavor_code_unique_idx" ON "flavor" ("code")'
        in statements
    )
    assert snapshot.tables[0].named_unique_constraints[0].to_dict() == {
        "name": "flavor_name_price_unique",
        "columns": ["name", "price"],
    }
    assert native_snapshot.enum_types == [
        EnumTypeSnapshot("ddl_flavor", ["mocha", "latte"])
    ]
    assert native_snapshot.tables[0].columns[4].kind == "enum:ddl_flavor"


def test_table_schema_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/test")

    @db.table(
        "schema_flavor",
        pk="id",
        schema="inventory",
        foreign_key_constraints=[
            TableForeignKey(
                name="schema_flavor_supplier_fk",
                columns=["id", "name"],
                foreign_table="inventory.schema_supplier",
                foreign_columns=["id", "name"],
            )
        ],
    )
    class SchemaFlavor(BaseModel):
        id: str
        name: str

    statements = compile_create_table_sql(db._table_map, "schema_flavor", "postgresql")
    snapshot = db.migrations.snapshot()

    assert SchemaFlavor.__name__ == "SchemaFlavor"
    assert statements[0].startswith(
        'CREATE TABLE IF NOT EXISTS "inventory"."schema_flavor"'
    )
    assert (
        'CONSTRAINT "schema_flavor_supplier_fk" '
        'FOREIGN KEY ("id", "name") '
        'REFERENCES "inventory"."schema_supplier" ("id", "name")'
    ) in statements[0]
    assert snapshot.tables[0].schema == "inventory"
    assert snapshot.tables[0].foreign_key_constraints == [
        ForeignKeyConstraintSnapshot(
            "schema_flavor_supplier_fk",
            ["id", "name"],
            "inventory.schema_supplier",
            ["id", "name"],
        )
    ]
    assert snapshot.tables[0].to_dict()["schema"] == "inventory"


def test_table_schema_rejects_empty_name() -> None:
    db = Ormdantic("postgresql://localhost/test")

    with pytest.raises(ValueError, match="schema for table 'bad_schema_flavor'"):

        @db.table("bad_schema_flavor", pk="id", schema=" ")
        class BadSchemaFlavor(BaseModel):
            id: str


def test_field_pattern_constraints_compile_by_dialect() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table("pattern_flavor", pk="id")
    class PatternFlavor(BaseModel):
        id: str
        code: str = Field(pattern=r"^[A-Z]{2}$")

    table = db._table_map.name_to_data["pattern_flavor"]
    sqlite_sql = compile_create_table_sql(db._table_map, table.tablename, "sqlite")
    postgres_sql = compile_create_table_sql(
        db._table_map, table.tablename, "postgresql"
    )
    mysql_sql = compile_create_table_sql(db._table_map, table.tablename, "mysql")
    oracle_sql = compile_create_table_sql(db._table_map, table.tablename, "oracle")
    snapshot = db.migrations.snapshot()

    assert PatternFlavor.__name__ == "PatternFlavor"
    assert (
        'CONSTRAINT "pattern_flavor_code_pattern_check" '
        "CHECK (ormdantic_regex_match(code, '^[A-Z]{2}$') = 1)"
    ) in sqlite_sql[0]
    assert (
        "CONSTRAINT \"pattern_flavor_code_pattern_check\" CHECK (code ~ '^[A-Z]{2}$')"
    ) in postgres_sql[0]
    assert (
        "CONSTRAINT `pattern_flavor_code_pattern_check` "
        "CHECK (code REGEXP '^[A-Z]{2}$')"
    ) in mysql_sql[0]
    assert (
        'CONSTRAINT "pattern_flavor_code_pattern_check" '
        "CHECK (REGEXP_LIKE(code, '^[A-Z]{2}$'))"
    ) in oracle_sql[0]
    assert snapshot.tables[0].columns[1].checks == [
        ("pattern", "matches", "'^[A-Z]{2}$'")
    ]
    with pytest.raises(ValueError, match="regular expression CHECK constraints"):
        compile_create_table_sql(db._table_map, table.tablename, "mssql")


def test_field_multiple_of_constraints_compile_by_dialect() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table("multiple_flavor", pk="id")
    class MultipleFlavor(BaseModel):
        id: str
        quantity: int = Field(multiple_of=5)
        increment: Decimal = Field(multiple_of=Decimal("0.05"))

    table = db._table_map.name_to_data["multiple_flavor"]
    sqlite_sql = compile_create_table_sql(db._table_map, table.tablename, "sqlite")
    postgres_sql = compile_create_table_sql(
        db._table_map, table.tablename, "postgresql"
    )
    mssql_sql = compile_create_table_sql(db._table_map, table.tablename, "mssql")
    snapshot = db.migrations.snapshot()

    assert MultipleFlavor.__name__ == "MultipleFlavor"
    assert (
        'CONSTRAINT "multiple_flavor_quantity_multiple_of_check" '
        "CHECK (ormdantic_decimal_multiple_of(quantity, 5) = 1)"
    ) in sqlite_sql[0]
    assert (
        'CONSTRAINT "multiple_flavor_increment_multiple_of_check" '
        "CHECK (ormdantic_decimal_multiple_of(increment, '0.05') = 1)"
    ) in sqlite_sql[0]
    assert (
        'CONSTRAINT "multiple_flavor_quantity_multiple_of_check" '
        "CHECK (MOD(quantity, 5) = 0)"
    ) in postgres_sql[0]
    assert (
        "CONSTRAINT [multiple_flavor_quantity_multiple_of_check] "
        "CHECK (quantity % 5 = 0)"
    ) in mssql_sql[0]
    checks = {column.name: column.checks for column in snapshot.tables[0].columns}
    assert checks["quantity"] == [("multiple_of", "=", "5")]
    assert checks["increment"] == [("multiple_of", "=", "'0.05'")]


def test_pydantic_decimal_shape_compiles_and_snapshots() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table("decimal_shape_flavor", pk="id")
    class DecimalShapeFlavor(BaseModel):
        id: str
        price: Decimal = Field(max_digits=12, decimal_places=2)
        override_price: Decimal = Field(max_digits=12, decimal_places=2)

    db._table_map.name_to_data["decimal_shape_flavor"].column_options[
        "override_price"
    ] = TableColumn(numeric_precision=16, numeric_scale=4)
    table = db._table_map.name_to_data["decimal_shape_flavor"]
    sqlite_sql = compile_create_table_sql(db._table_map, table.tablename, "sqlite")
    postgres_sql = compile_create_table_sql(
        db._table_map, table.tablename, "postgresql"
    )
    snapshot = db.migrations.snapshot()

    assert DecimalShapeFlavor.__name__ == "DecimalShapeFlavor"
    assert '"price" DECIMAL_TEXT(12, 2) NOT NULL' in sqlite_sql[0]
    assert '"override_price" DECIMAL_TEXT(16, 4) NOT NULL' in sqlite_sql[0]
    assert '"price" NUMERIC(12, 2) NOT NULL' in postgres_sql[0]
    columns = {column.name: column for column in snapshot.tables[0].columns}
    assert columns["price"].numeric_precision == 12
    assert columns["price"].numeric_scale == 2
    assert columns["override_price"].numeric_precision == 16
    assert columns["override_price"].numeric_scale == 4


def test_table_decorator_comment_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("commented_flavor", pk="id", comment="Chef's flavor table")
    class CommentedFlavor(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "commented_flavor", "postgresql"
    )
    sqlite_sql = compile_create_table_sql(db._table_map, "commented_flavor", "sqlite")
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "commented_flavor"
    )

    assert CommentedFlavor.__name__ == "CommentedFlavor"
    assert (
        "COMMENT ON TABLE \"commented_flavor\" IS 'Chef''s flavor table'"
        in postgres_sql
    )
    assert not any("COMMENT ON TABLE" in statement for statement in sqlite_sql)
    assert table.comment == "Chef's flavor table"


def test_column_comment_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "commented_column_flavor",
        pk="id",
        column_options={"name": TableColumn(comment="Chef's display name")},
    )
    class CommentedColumnFlavor(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "commented_column_flavor", "postgresql"
    )
    mysql_sql = compile_create_table_sql(
        db._table_map, "commented_column_flavor", "mysql"
    )
    sqlite_sql = compile_create_table_sql(
        db._table_map, "commented_column_flavor", "sqlite"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "commented_column_flavor"
    )

    assert CommentedColumnFlavor.__name__ == "CommentedColumnFlavor"
    assert (
        'COMMENT ON COLUMN "commented_column_flavor"."name" '
        "IS 'Chef''s display name'"
    ) in postgres_sql
    assert "`name` TEXT NOT NULL COMMENT 'Chef''s display name'" in mysql_sql[0]
    assert not any("COMMENT ON COLUMN" in statement for statement in sqlite_sql)
    assert table.columns[1].comment == "Chef's display name"


def test_table_decorator_tablespace_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("spaced_flavor", pk="id", tablespace="fastspace")
    class SpacedFlavor(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "spaced_flavor", "postgresql"
    )
    mysql_sql = compile_create_table_sql(db._table_map, "spaced_flavor", "mysql")
    mariadb_sql = compile_create_table_sql(db._table_map, "spaced_flavor", "mariadb")
    mssql_sql = compile_create_table_sql(db._table_map, "spaced_flavor", "mssql")
    sqlite_sql = compile_create_table_sql(db._table_map, "spaced_flavor", "sqlite")
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "spaced_flavor"
    )

    assert SpacedFlavor.__name__ == "SpacedFlavor"
    assert postgres_sql[0].endswith(' TABLESPACE "fastspace"')
    assert mysql_sql[0].endswith(" TABLESPACE `fastspace`")
    assert mariadb_sql == mysql_sql
    assert mssql_sql[0].endswith(" ON [fastspace]")
    assert not any("TABLESPACE" in statement for statement in sqlite_sql)
    assert table.tablespace == "fastspace"


def test_table_decorator_oracle_compression_compiles_and_snapshots() -> None:
    db = Ormdantic("oracle://localhost/db")

    @db.table("compressed_flavor", pk="id", oracle_compress=6)
    class CompressedFlavor(BaseModel):
        id: int
        name: str

    @db.table("basic_compressed_flavor", pk="id", oracle_compress=True)
    class BasicCompressedFlavor(BaseModel):
        id: int
        name: str

    oracle_sql = compile_create_table_sql(db._table_map, "compressed_flavor", "oracle")
    basic_oracle_sql = compile_create_table_sql(
        db._table_map, "basic_compressed_flavor", "oracle"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "compressed_flavor"
    )

    assert CompressedFlavor.__name__ == "CompressedFlavor"
    assert BasicCompressedFlavor.__name__ == "BasicCompressedFlavor"
    assert oracle_sql[0].endswith(" COMPRESS FOR 6")
    assert basic_oracle_sql[0].endswith(" COMPRESS")
    assert table.oracle_compress == 6
    with pytest.raises(ValueError, match="Oracle table compression"):
        compile_create_table_sql(db._table_map, "compressed_flavor", "sqlite")


def test_table_decorator_postgres_inheritance_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("inherited_flavor", pk="id", postgres_inherits=["base_flavor"])
    class InheritedFlavor(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "inherited_flavor", "postgresql"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "inherited_flavor"
    )

    assert InheritedFlavor.__name__ == "InheritedFlavor"
    assert postgres_sql == [
        'CREATE TABLE IF NOT EXISTS "inherited_flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL, "name" TEXT NOT NULL) '
        'INHERITS ("base_flavor")'
    ]
    assert table.postgres_inherits == ["base_flavor"]
    with pytest.raises(ValueError, match="PostgreSQL table inheritance"):
        compile_create_table_sql(db._table_map, "inherited_flavor", "sqlite")


def test_table_decorator_postgres_storage_parameters_compile_and_snapshot() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "storage_parameter_flavor",
        pk="id",
        postgres_with={
            "fillfactor": 70,
            "toast.autovacuum_enabled": False,
        },
    )
    class StorageParameterFlavor(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "storage_parameter_flavor", "postgresql"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "storage_parameter_flavor"
    )

    assert StorageParameterFlavor.__name__ == "StorageParameterFlavor"
    assert postgres_sql == [
        'CREATE TABLE IF NOT EXISTS "storage_parameter_flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL, "name" TEXT NOT NULL) '
        "WITH (fillfactor = 70, toast.autovacuum_enabled = false)"
    ]
    assert table.postgres_with == [
        ("fillfactor", "70"),
        ("toast.autovacuum_enabled", "false"),
    ]
    with pytest.raises(ValueError, match="PostgreSQL table storage parameters"):
        compile_create_table_sql(db._table_map, "storage_parameter_flavor", "sqlite")


def test_table_decorator_postgres_access_method_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("access_method_flavor", pk="id", postgres_using="heap")
    class AccessMethodFlavor(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "access_method_flavor", "postgresql"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "access_method_flavor"
    )

    assert AccessMethodFlavor.__name__ == "AccessMethodFlavor"
    assert postgres_sql == [
        'CREATE TABLE IF NOT EXISTS "access_method_flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL, "name" TEXT NOT NULL) USING "heap"'
    ]
    assert table.postgres_using == "heap"
    with pytest.raises(ValueError, match="PostgreSQL table access methods"):
        compile_create_table_sql(db._table_map, "access_method_flavor", "sqlite")


def test_table_decorator_postgres_unlogged_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("unlogged_flavor", pk="id", postgres_unlogged=True)
    class UnloggedFlavor(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "unlogged_flavor", "postgresql"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "unlogged_flavor"
    )

    assert UnloggedFlavor.__name__ == "UnloggedFlavor"
    assert postgres_sql == [
        'CREATE UNLOGGED TABLE IF NOT EXISTS "unlogged_flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL, "name" TEXT NOT NULL)'
    ]
    assert table.postgres_unlogged is True
    with pytest.raises(ValueError, match="PostgreSQL unlogged tables"):
        compile_create_table_sql(db._table_map, "unlogged_flavor", "sqlite")


def test_table_decorator_postgres_partition_key_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("partitioned_flavor", pk="id", postgres_partition_by="range (id)")
    class PartitionedFlavor(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "partitioned_flavor", "postgresql"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "partitioned_flavor"
    )

    assert PartitionedFlavor.__name__ == "PartitionedFlavor"
    assert postgres_sql == [
        'CREATE TABLE IF NOT EXISTS "partitioned_flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL, "name" TEXT NOT NULL) '
        "PARTITION BY RANGE (id)"
    ]
    assert table.postgres_partition_by == "RANGE (id)"
    with pytest.raises(ValueError, match="PostgreSQL table partitioning"):
        compile_create_table_sql(db._table_map, "partitioned_flavor", "sqlite")


def test_table_decorator_postgres_child_partition_compiles_and_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "partitioned_flavor_2026",
        pk="id",
        postgres_partition_of="partitioned_flavor",
        postgres_partition_for="from (2026) to (2027)",
    )
    class PartitionedFlavor2026(BaseModel):
        id: int
        name: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "partitioned_flavor_2026", "postgresql"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "partitioned_flavor_2026"
    )

    assert PartitionedFlavor2026.__name__ == "PartitionedFlavor2026"
    assert postgres_sql == [
        'CREATE TABLE IF NOT EXISTS "partitioned_flavor_2026" '
        'PARTITION OF "partitioned_flavor" FOR VALUES FROM (2026) TO (2027)'
    ]
    assert table.postgres_partition_of == "partitioned_flavor"
    assert table.postgres_partition_for == "FOR VALUES FROM (2026) TO (2027)"
    with pytest.raises(ValueError, match="PostgreSQL table partitions"):
        compile_create_table_sql(db._table_map, "partitioned_flavor_2026", "sqlite")


def test_table_decorator_mysql_options_compile_and_snapshot() -> None:
    db = Ormdantic("mysql://localhost/db")

    @db.table(
        "mysql_storage_flavor",
        pk="id",
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
        mysql_partition_by="hash (id)",
        mysql_partitions=4,
        mysql_subpartition_by="key (id)",
        mysql_subpartitions=2,
        mysql_auto_increment=101,
    )
    class MysqlStorageFlavor(BaseModel):
        id: int

    mysql_sql = compile_create_table_sql(db._table_map, "mysql_storage_flavor", "mysql")
    postgres_sql = compile_create_table_sql(
        db._table_map, "mysql_storage_flavor", "postgresql"
    )
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "mysql_storage_flavor"
    )

    assert MysqlStorageFlavor.__name__ == "MysqlStorageFlavor"
    assert mysql_sql == [
        "CREATE TABLE IF NOT EXISTS `mysql_storage_flavor` "
        "(`id` INTEGER NOT NULL PRIMARY KEY) ENGINE = InnoDB "
        "DEFAULT CHARACTER SET = utf8mb4 COLLATE = utf8mb4_unicode_ci "
        "ROW_FORMAT = DYNAMIC KEY_BLOCK_SIZE = 8 PACK_KEYS = 1 CHECKSUM = 1 "
        "DELAY_KEY_WRITE = 1 STATS_PERSISTENT = 1 STATS_AUTO_RECALC = 0 "
        "STATS_SAMPLE_PAGES = 32 AVG_ROW_LENGTH = 64 MAX_ROWS = 1000 "
        "MIN_ROWS = 10 INSERT_METHOD = LAST "
        "DATA DIRECTORY = '/var/lib/mysql/data' "
        "INDEX DIRECTORY = '/var/lib/mysql/index' "
        "CONNECTION = 'mysql://remote.example/db/flavor' "
        "UNION = (`flavor_hot`, `flavor_cold`) "
        "PARTITION BY HASH (id) PARTITIONS 4 "
        "SUBPARTITION BY KEY (id) SUBPARTITIONS 2 "
        "AUTO_INCREMENT = 101"
    ]
    assert postgres_sql == [
        'CREATE TABLE IF NOT EXISTS "mysql_storage_flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL)'
    ]
    assert table.mysql_engine == "InnoDB"
    assert table.mysql_charset == "utf8mb4"
    assert table.mysql_collation == "utf8mb4_unicode_ci"
    assert table.mysql_row_format == "DYNAMIC"
    assert table.mysql_key_block_size == 8
    assert table.mysql_pack_keys is True
    assert table.mysql_checksum is True
    assert table.mysql_delay_key_write is True
    assert table.mysql_stats_persistent is True
    assert table.mysql_stats_auto_recalc is False
    assert table.mysql_stats_sample_pages == 32
    assert table.mysql_avg_row_length == 64
    assert table.mysql_max_rows == 1000
    assert table.mysql_min_rows == 10
    assert table.mysql_insert_method == "LAST"
    assert table.mysql_data_directory == "/var/lib/mysql/data"
    assert table.mysql_index_directory == "/var/lib/mysql/index"
    assert table.mysql_connection == "mysql://remote.example/db/flavor"
    assert table.mysql_union == ["flavor_hot", "flavor_cold"]
    assert table.mysql_partition_by == "HASH (id)"
    assert table.mysql_partitions == 4
    assert table.mysql_subpartition_by == "KEY (id)"
    assert table.mysql_subpartitions == 2
    assert table.mysql_auto_increment == 101


def test_table_decorator_rejects_invalid_mysql_storage_token() -> None:
    db = Ormdantic("mysql://localhost/db")

    with pytest.raises(ValueError, match="MySQL engine .* must contain only"):

        @db.table("bad_mysql_storage", pk="id", mysql_engine="InnoDB; DROP TABLE x")
        class BadMysqlStorage(BaseModel):
            id: int

    with pytest.raises(ValueError, match="MySQL row format .* must contain only"):

        @db.table("bad_mysql_row_format", pk="id", mysql_row_format="DYNAMIC;")
        class BadMysqlRowFormat(BaseModel):
            id: int

    with pytest.raises(ValueError, match="KEY_BLOCK_SIZE .* must be positive"):

        @db.table("bad_mysql_key_block_size", pk="id", mysql_key_block_size=0)
        class BadMysqlKeyBlockSize(BaseModel):
            id: int

    with pytest.raises(ValueError, match="PACK_KEYS .* must be true or false"):

        @db.table("bad_mysql_pack_keys", pk="id", mysql_pack_keys=1)  # type: ignore[arg-type]
        class BadMysqlPackKeys(BaseModel):
            id: int

    with pytest.raises(ValueError, match="CHECKSUM .* must be true or false"):

        @db.table("bad_mysql_checksum", pk="id", mysql_checksum=1)  # type: ignore[arg-type]
        class BadMysqlChecksum(BaseModel):
            id: int

    with pytest.raises(ValueError, match="DELAY_KEY_WRITE .* must be true or false"):

        @db.table("bad_mysql_delay_key_write", pk="id", mysql_delay_key_write=1)  # type: ignore[arg-type]
        class BadMysqlDelayKeyWrite(BaseModel):
            id: int

    with pytest.raises(ValueError, match="STATS_PERSISTENT .* must be true or false"):

        @db.table("bad_mysql_stats_persistent", pk="id", mysql_stats_persistent=1)  # type: ignore[arg-type]
        class BadMysqlStatsPersistent(BaseModel):
            id: int

    with pytest.raises(ValueError, match="STATS_AUTO_RECALC .* must be true or false"):

        @db.table("bad_mysql_stats_auto_recalc", pk="id", mysql_stats_auto_recalc=1)  # type: ignore[arg-type]
        class BadMysqlStatsAutoRecalc(BaseModel):
            id: int

    with pytest.raises(ValueError, match="STATS_SAMPLE_PAGES .* must be positive"):

        @db.table("bad_mysql_stats_sample_pages", pk="id", mysql_stats_sample_pages=0)
        class BadMysqlStatsSamplePages(BaseModel):
            id: int

    with pytest.raises(ValueError, match="AVG_ROW_LENGTH .* must be positive"):

        @db.table("bad_mysql_avg_row_length", pk="id", mysql_avg_row_length=0)
        class BadMysqlAvgRowLength(BaseModel):
            id: int

    with pytest.raises(ValueError, match="MAX_ROWS .* must be positive"):

        @db.table("bad_mysql_max_rows", pk="id", mysql_max_rows=0)
        class BadMysqlMaxRows(BaseModel):
            id: int

    with pytest.raises(ValueError, match="MIN_ROWS .* must be positive"):

        @db.table("bad_mysql_min_rows", pk="id", mysql_min_rows=0)
        class BadMysqlMinRows(BaseModel):
            id: int

    with pytest.raises(ValueError, match="INSERT_METHOD .* must contain only"):

        @db.table("bad_mysql_insert_method", pk="id", mysql_insert_method="LAST;")
        class BadMysqlInsertMethod(BaseModel):
            id: int

    with pytest.raises(ValueError, match="DATA DIRECTORY .* cannot be empty"):

        @db.table("bad_mysql_data_directory", pk="id", mysql_data_directory=" ")
        class BadMysqlDataDirectory(BaseModel):
            id: int

    with pytest.raises(ValueError, match="INDEX DIRECTORY .* cannot be empty"):

        @db.table("bad_mysql_index_directory", pk="id", mysql_index_directory=" ")
        class BadMysqlIndexDirectory(BaseModel):
            id: int

    with pytest.raises(ValueError, match="CONNECTION .* cannot be empty"):

        @db.table("bad_mysql_connection", pk="id", mysql_connection=" ")
        class BadMysqlConnection(BaseModel):
            id: int

    with pytest.raises(ValueError, match="UNION .* must be a list of strings"):

        @db.table("bad_mysql_union", pk="id", mysql_union="flavor_hot")  # type: ignore[arg-type]
        class BadMysqlUnion(BaseModel):
            id: int

    with pytest.raises(ValueError, match="UNION .* item 0 cannot be empty"):

        @db.table("bad_mysql_union_item", pk="id", mysql_union=[" "])
        class BadMysqlUnionItem(BaseModel):
            id: int

    with pytest.raises(ValueError, match="PARTITION BY .* must use"):

        @db.table("bad_mysql_partition_by", pk="id", mysql_partition_by="id")
        class BadMysqlPartitionBy(BaseModel):
            id: int

    with pytest.raises(ValueError, match="PARTITION BY .* separators"):

        @db.table(
            "bad_mysql_partition_separator",
            pk="id",
            mysql_partition_by="HASH (id); DROP TABLE x",
        )
        class BadMysqlPartitionSeparator(BaseModel):
            id: int

    with pytest.raises(ValueError, match="PARTITIONS .* must be positive"):

        @db.table("bad_mysql_partitions", pk="id", mysql_partitions=0)
        class BadMysqlPartitions(BaseModel):
            id: int

    with pytest.raises(ValueError, match="SUBPARTITION BY .* must use"):

        @db.table("bad_mysql_subpartition_by", pk="id", mysql_subpartition_by="id")
        class BadMysqlSubpartitionBy(BaseModel):
            id: int

    with pytest.raises(ValueError, match="SUBPARTITIONS .* must be positive"):

        @db.table("bad_mysql_subpartitions", pk="id", mysql_subpartitions=0)
        class BadMysqlSubpartitions(BaseModel):
            id: int

    with pytest.raises(ValueError, match="AUTO_INCREMENT .* must be positive"):

        @db.table("bad_mysql_auto_increment", pk="id", mysql_auto_increment=0)
        class BadMysqlAutoIncrement(BaseModel):
            id: int


def test_table_decorator_rejects_empty_tablespace() -> None:
    db = Ormdantic("sqlite:///:memory:")

    with pytest.raises(ValueError, match="tablespace .* cannot be empty"):

        @db.table("empty_tablespace", pk="id", tablespace=" ")
        class EmptyTablespace(BaseModel):
            id: int


def test_table_decorator_rejects_invalid_oracle_table_compression() -> None:
    db = Ormdantic("sqlite:///:memory:")

    with pytest.raises(ValueError, match="Oracle table compression"):

        @db.table("bad_oracle_table_compress", pk="id", oracle_compress=0)
        class BadOracleTableCompress(BaseModel):
            id: int


def test_table_decorator_rejects_invalid_postgres_inherits_name() -> None:
    db = Ormdantic("postgresql://localhost/db")

    with pytest.raises(
        ValueError,
        match="PostgreSQL inherited table .* cannot contain",
    ):

        @db.table("bad_inherit", pk="id", postgres_inherits=["base.flavor"])
        class BadInherit(BaseModel):
            id: int


def test_table_decorator_rejects_invalid_postgres_storage_parameter() -> None:
    db = Ormdantic("postgresql://localhost/db")

    with pytest.raises(
        ValueError,
        match="PostgreSQL storage parameter .* dotted identifier",
    ):

        @db.table("bad_postgres_with", pk="id", postgres_with={"bad-name": 70})
        class BadPostgresWith(BaseModel):
            id: int


def test_table_decorator_rejects_invalid_postgres_index_storage_parameter() -> None:
    with pytest.raises(
        ValueError,
        match="PostgreSQL storage parameter for index 'bad_idx'",
    ):
        TableIndex(name="bad_idx", columns=["name"], postgres_with={"bad-name": 70})


def test_table_decorator_rejects_invalid_postgres_index_operator_class() -> None:
    with pytest.raises(
        ValueError,
        match="PostgreSQL index operator class .* must contain only",
    ):
        TableIndex(
            name="bad_idx",
            columns=["name"],
            postgres_ops={"name": "text pattern ops"},
        )
    with pytest.raises(
        ValueError,
        match="reference columns or expressions not present",
    ):
        TableIndex(
            name="unknown_idx",
            columns=["name"],
            postgres_ops={"code": "text_pattern_ops"},
        )


def test_table_decorator_rejects_invalid_index_method() -> None:
    with pytest.raises(
        ValueError,
        match="index 'bad_idx' method",
    ):
        TableIndex(name="bad_idx", columns=["name"], method="btree;drop")


def test_table_decorator_rejects_invalid_postgres_access_method() -> None:
    db = Ormdantic("postgresql://localhost/db")

    with pytest.raises(
        ValueError,
        match="PostgreSQL table access method .* must contain only",
    ):

        @db.table("bad_postgres_using", pk="id", postgres_using="heap;drop")
        class BadPostgresUsing(BaseModel):
            id: int


def test_table_decorator_rejects_invalid_postgres_partition_key() -> None:
    db = Ormdantic("postgresql://localhost/db")

    with pytest.raises(
        ValueError,
        match="PostgreSQL partition key .* must use RANGE",
    ):

        @db.table("bad_postgres_partition", pk="id", postgres_partition_by="id")
        class BadPostgresPartition(BaseModel):
            id: int

    with pytest.raises(
        ValueError,
        match="PostgreSQL partition key .* cannot contain SQL statement separators",
    ):

        @db.table(
            "bad_postgres_partition_separator",
            pk="id",
            postgres_partition_by="RANGE (id); DROP TABLE x",
        )
        class BadPostgresPartitionSeparator(BaseModel):
            id: int


def test_table_decorator_rejects_invalid_postgres_child_partition_options() -> None:
    db = Ormdantic("postgresql://localhost/db")

    with pytest.raises(
        ValueError,
        match="requires both postgres_partition_of and postgres_partition_for",
    ):

        @db.table(
            "missing_partition_bound",
            pk="id",
            postgres_partition_of="partitioned_flavor",
        )
        class MissingPartitionBound(BaseModel):
            id: int

    with pytest.raises(
        ValueError,
        match="PostgreSQL partition bound .* must use IN",
    ):

        @db.table(
            "bad_partition_bound",
            pk="id",
            postgres_partition_of="partitioned_flavor",
            postgres_partition_for="2026",
        )
        class BadPartitionBound(BaseModel):
            id: int

    with pytest.raises(
        ValueError,
        match="cannot also use postgres_inherits",
    ):

        @db.table(
            "partition_with_inherits",
            pk="id",
            postgres_partition_of="partitioned_flavor",
            postgres_partition_for="DEFAULT",
            postgres_inherits=["base_flavor"],
        )
        class PartitionWithInherits(BaseModel):
            id: int


def test_table_decorator_rejects_empty_comment() -> None:
    db = Ormdantic("sqlite:///:memory:")

    with pytest.raises(ValueError, match="comment .* cannot be empty"):

        @db.table("empty_comment", pk="id", comment=" ")
        class EmptyComment(BaseModel):
            id: int


def test_table_column_rejects_empty_comment() -> None:
    with pytest.raises(ValueError, match="column comment cannot be empty"):
        TableColumn(comment=" ")


def test_table_constraints_reject_empty_comments() -> None:
    with pytest.raises(
        ValueError, match="table check constraint comment cannot be empty"
    ):
        TableCheck(name="empty_check_comment", expression="rating >= 0", comment=" ")
    with pytest.raises(
        ValueError, match="table unique constraint comment cannot be empty"
    ):
        TableUnique(name="empty_unique_comment", columns=["code"], comment=" ")
    with pytest.raises(
        ValueError,
        match="table foreign key constraint comment cannot be empty",
    ):
        TableForeignKey(
            name="empty_fk_comment",
            columns=["supplier_id"],
            foreign_table="supplier",
            foreign_columns=["id"],
            comment=" ",
        )
    with pytest.raises(
        ValueError,
        match="table exclusion constraint comment cannot be empty",
    ):
        TableExclusion(
            name="empty_exclusion_comment", columns=[("room_id", "=")], comment=" "
        )


@pytest.mark.asyncio
async def test_constraint_comments_execute_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "commented_runtime_constraint_flavor",
        pk="id",
        check_constraints=[
            TableCheck(
                name="commented_runtime_constraint_flavor_rating_check",
                expression="rating >= 0",
                comment="Runtime rating guard",
            )
        ],
    )
    class CommentedRuntimeConstraintFlavor(BaseModel):
        id: str
        rating: int

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "postgresql://localhost/db"
            assert tables[0][7] == [
                (
                    "commented_runtime_constraint_flavor_rating_check",
                    "rating >= 0",
                    True,
                    False,
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert CommentedRuntimeConstraintFlavor.__name__ == (
        "CommentedRuntimeConstraintFlavor"
    )
    assert executed == [
        "runtime.create_all",
        'COMMENT ON CONSTRAINT "commented_runtime_constraint_flavor_rating_check" '
        'ON "commented_runtime_constraint_flavor" IS '
        "'Runtime rating guard'",
    ]


@pytest.mark.asyncio
async def test_postgres_unique_include_recreates_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "covered_runtime_unique_flavor",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="covered_runtime_unique_flavor_code_unique",
                columns=["code"],
                postgres_include=["rating"],
                comment="Runtime code covering",
            )
        ],
    )
    class CoveredRuntimeUniqueFlavor(BaseModel):
        id: str
        code: str
        rating: int

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "postgresql://localhost/db"
            assert tables[0][6] == [
                (
                    "covered_runtime_unique_flavor_code_unique",
                    ["code"],
                    None,
                    False,
                    False,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        'ALTER TABLE "covered_runtime_unique_flavor" '
        'DROP CONSTRAINT "covered_runtime_unique_flavor_code_unique"',
        'ALTER TABLE "covered_runtime_unique_flavor" '
        'ADD CONSTRAINT "covered_runtime_unique_flavor_code_unique" '
        'UNIQUE ("code") INCLUDE ("rating")',
        'COMMENT ON CONSTRAINT "covered_runtime_unique_flavor_code_unique" '
        'ON "covered_runtime_unique_flavor" IS '
        "'Runtime code covering'",
    ]


def test_registered_view_rejects_empty_comment() -> None:
    db = Ormdantic("postgresql://localhost/db")

    with pytest.raises(ValueError, match="view comment cannot be empty"):
        db.view("empty_comment_view", "SELECT 1", comment=" ")


def test_registered_sequence_rejects_empty_comment() -> None:
    db = Ormdantic("postgresql://localhost/db")

    with pytest.raises(ValueError, match="sequence comment cannot be empty"):
        db.sequence("empty_comment_seq", comment=" ")


def test_registered_sequence_rejects_invalid_data_type() -> None:
    db = Ormdantic("postgresql://localhost/db")

    with pytest.raises(ValueError, match="sequence data_type .* safe SQL type"):
        db.sequence("bad_sequence_type_seq", data_type="bigint; DROP TABLE flavor")


def test_postgres_migration_snapshot_infers_native_enum_types() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("native_enum_flavor", pk="id")
    class NativeEnumFlavor(BaseModel):
        id: str
        flavor: DdlFlavor

    snapshot = db.migrations.snapshot()

    assert snapshot.enum_types == [EnumTypeSnapshot("ddl_flavor", ["mocha", "latte"])]
    assert snapshot.tables[0].columns[1].kind == "enum:ddl_flavor"


def test_runtime_native_enum_specs_are_postgres_only() -> None:
    postgres_db = Ormdantic("postgresql://localhost/db")

    @postgres_db.table("postgres_runtime_enum_flavor", pk="id")
    class PostgresRuntimeEnumFlavor(BaseModel):
        id: str
        flavor: DdlFlavor

    sqlite_db = Ormdantic("sqlite:///:memory:")

    @sqlite_db.table("sqlite_runtime_enum_flavor", pk="id")
    class SqliteRuntimeEnumFlavor(BaseModel):
        id: str
        flavor: DdlFlavor

    assert postgres_db._runtime_enum_type_specs() == [
        ("ddl_flavor", ["mocha", "latte"], None, None)
    ]
    assert sqlite_db._runtime_enum_type_specs() == []


def test_native_enum_options_customize_type_name_and_schema() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "custom_native_enum_flavor",
        pk="id",
        column_options={
            "flavor": TableColumn(
                enum_type_name="coffee_flavor_kind",
                enum_schema="inventory",
                enum_type_comment="Coffee flavor enum",
            )
        },
    )
    class CustomNativeEnumFlavor(BaseModel):
        id: str
        flavor: DdlFlavor

    snapshot = db.migrations.snapshot()
    native_snapshot = SchemaSnapshot.from_database(db, native_enum_types=True)

    assert snapshot.enum_types == [
        EnumTypeSnapshot(
            "coffee_flavor_kind",
            ["mocha", "latte"],
            schema="inventory",
            comment="Coffee flavor enum",
        )
    ]
    assert snapshot.tables[0].columns[1].kind == "enum:inventory.coffee_flavor_kind"
    assert native_snapshot.enum_types == snapshot.enum_types
    assert native_snapshot.tables[0].columns[1].kind == (
        "enum:inventory.coffee_flavor_kind"
    )
    assert db._runtime_enum_type_specs() == [
        ("coffee_flavor_kind", ["mocha", "latte"], "inventory", "Coffee flavor enum")
    ]


def test_native_enum_comments_merge_across_duplicate_type_references() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "multi_enum_flavor",
        pk="id",
        column_options={"primary_flavor": TableColumn(enum_type_comment="Flavor enum")},
    )
    class MultiEnumFlavor(BaseModel):
        id: str
        primary_flavor: DdlFlavor
        fallback_flavor: DdlFlavor

    assert db._runtime_enum_type_specs() == [
        ("ddl_flavor", ["mocha", "latte"], None, "Flavor enum")
    ]


def test_native_enum_comments_reject_conflicting_duplicate_metadata() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "conflicting_enum_flavor",
        pk="id",
        column_options={
            "primary_flavor": TableColumn(enum_type_comment="Primary flavor enum"),
            "fallback_flavor": TableColumn(enum_type_comment="Fallback flavor enum"),
        },
    )
    class ConflictingEnumFlavor(BaseModel):
        id: str
        primary_flavor: DdlFlavor
        fallback_flavor: DdlFlavor

    with pytest.raises(ValueError, match="different comments"):
        db._runtime_enum_type_specs()


def test_native_enum_options_reject_invalid_identifiers() -> None:
    with pytest.raises(ValueError, match="enum_type_name cannot contain"):
        TableColumn(enum_type_name="inventory.flavor_kind")
    with pytest.raises(ValueError, match="enum_schema cannot be empty"):
        TableColumn(enum_schema=" ")
    with pytest.raises(ValueError, match="enum_type_comment cannot be empty"):
        TableColumn(enum_type_comment=" ")


@pytest.mark.asyncio
async def test_native_enum_comments_execute_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "commented_native_enum_flavor",
        pk="id",
        column_options={"flavor": TableColumn(enum_type_comment="Flavor enum")},
    )
    class CommentedNativeEnumFlavor(BaseModel):
        id: str
        flavor: DdlFlavor

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
            enum_types: list[tuple[str, list[str], str | None]],
        ) -> None:
            assert connection == "postgresql://localhost/db"
            assert tables
            assert enum_types == [("ddl_flavor", ["mocha", "latte"], None)]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        "COMMENT ON TYPE \"ddl_flavor\" IS 'Flavor enum'",
    ]


def test_native_enum_options_require_string_enum_fields() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "invalid_native_enum_options",
        pk="id",
        column_options={"name": TableColumn(enum_type_name="name_kind")},
    )
    class InvalidNativeEnumOptions(BaseModel):
        id: str
        name: str

    with pytest.raises(ValueError, match="require a string-valued Enum field"):
        db.migrations.snapshot()


def test_constraint_timing_options_compile_and_snapshot() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("timed_supplier", pk="id")
    class TimedSupplier(BaseModel):
        id: str

    @db.table(
        "timed_flavor",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="timed_flavor_code_unique",
                columns=["code"],
                postgres_include=["supplier"],
                deferrable=True,
                initially_deferred=True,
                nulls_not_distinct=True,
            )
        ],
        column_options={
            "supplier": TableColumn(
                foreign_key_name="timed_flavor_supplier_fk",
                deferrable=True,
                initially_deferred=True,
            )
        },
    )
    class TimedFlavor(BaseModel):
        id: str
        code: str
        supplier: TimedSupplier | str

    for table_data in db._table_map.name_to_data.values():
        table_data.relationships = db.get(table_data)

    table = db._table_map.name_to_data["timed_flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "postgresql")
    snapshot = db.migrations.snapshot()
    timed_snapshot = next(
        table for table in snapshot.tables if table.name == "timed_flavor"
    )

    assert (
        'CONSTRAINT "timed_flavor_code_unique" '
        'UNIQUE NULLS NOT DISTINCT ("code") '
        'INCLUDE ("supplier") '
        "DEFERRABLE INITIALLY DEFERRED"
    ) in statements[0]
    assert (
        'CONSTRAINT "timed_flavor_supplier_fk" '
        'FOREIGN KEY ("supplier") REFERENCES "timed_supplier" ("id") '
        "DEFERRABLE INITIALLY DEFERRED"
    ) in statements[0]
    assert timed_snapshot.named_unique_constraints[0].deferrable is True
    assert timed_snapshot.named_unique_constraints[0].initially_deferred is True
    assert timed_snapshot.named_unique_constraints[0].nulls_not_distinct is True
    assert timed_snapshot.named_unique_constraints[0].postgres_include == ["supplier"]
    assert timed_snapshot.columns[2].deferrable is True
    assert timed_snapshot.columns[2].initially_deferred is True
    with pytest.raises(ValueError, match="PostgreSQL unique NULLS NOT DISTINCT"):
        compile_create_table_sql(db._table_map, table.tablename, "sqlite")


def test_constraint_timing_options_are_dialect_aware() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table("timing_supplier", pk="id")
    class TimingSupplier(BaseModel):
        id: str

    @db.table(
        "sqlite_timing_flavor",
        pk="id",
        column_options={
            "supplier": TableColumn(
                foreign_key_name="sqlite_timing_flavor_supplier_fk",
                deferrable=True,
                initially_deferred=True,
            )
        },
    )
    class SQLiteTimingFlavor(BaseModel):
        id: str
        supplier: TimingSupplier | str

    @db.table(
        "bad_deferrable_unique",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="bad_deferrable_unique_code_unique",
                columns=["code"],
                deferrable=True,
            )
        ],
    )
    class BadDeferrableUnique(BaseModel):
        id: str
        code: str

    for table_data in db._table_map.name_to_data.values():
        table_data.relationships = db.get(table_data)

    sqlite_statements = compile_create_table_sql(
        db._table_map, "sqlite_timing_flavor", "sqlite"
    )
    oracle_statements = compile_create_table_sql(
        db._table_map, "bad_deferrable_unique", "oracle"
    )

    assert (
        'CONSTRAINT "sqlite_timing_flavor_supplier_fk" '
        'FOREIGN KEY ("supplier") REFERENCES "timing_supplier" ("id") '
        "DEFERRABLE INITIALLY DEFERRED"
    ) in sqlite_statements[0]
    assert (
        'CONSTRAINT "bad_deferrable_unique_code_unique" UNIQUE ("code") DEFERRABLE'
    ) in oracle_statements[0]
    with pytest.raises(ValueError, match="deferrable unique constraints"):
        compile_create_table_sql(db._table_map, "bad_deferrable_unique", "sqlite")
    with pytest.raises(ValueError, match="deferrable foreign keys"):
        compile_create_table_sql(db._table_map, "sqlite_timing_flavor", "mysql")


def test_postgres_unique_include_columns_are_dialect_aware() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "covered_unique_flavor",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="covered_unique_flavor_code_unique",
                columns=["code"],
                postgres_include=["rating"],
            )
        ],
    )
    class CoveredUniqueFlavor(BaseModel):
        id: str
        code: str
        rating: int

    postgres_statements = compile_create_table_sql(
        db._table_map, "covered_unique_flavor", "postgresql"
    )

    assert (
        'CONSTRAINT "covered_unique_flavor_code_unique" '
        'UNIQUE ("code") INCLUDE ("rating")'
    ) in postgres_statements[0]
    with pytest.raises(ValueError, match="unique constraint INCLUDE"):
        compile_create_table_sql(db._table_map, "covered_unique_flavor", "sqlite")


def test_mssql_unique_constraint_clustering_compiles_and_snapshots() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "clustered_unique_flavor",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="clustered_unique_flavor_code_unique",
                columns=["code"],
                mssql_filegroup="constraintspace",
                mssql_clustered=True,
            )
        ],
    )
    class ClusteredUniqueFlavor(BaseModel):
        id: str
        code: str

    @db.table(
        "nonclustered_unique_flavor",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="nonclustered_unique_flavor_code_unique",
                columns=["code"],
                mssql_filegroup="constraintspace",
                mssql_clustered=False,
            )
        ],
    )
    class NonclusteredUniqueFlavor(BaseModel):
        id: str
        code: str

    @db.table(
        "filegroup_unique_flavor",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="filegroup_unique_flavor_code_unique",
                columns=["code"],
                mssql_filegroup="constraintspace",
            )
        ],
    )
    class FilegroupUniqueFlavor(BaseModel):
        id: str
        code: str

    clustered = compile_create_table_sql(
        db._table_map,
        "clustered_unique_flavor",
        "mssql",
    )[0]
    nonclustered = compile_create_table_sql(
        db._table_map,
        "nonclustered_unique_flavor",
        "mssql",
    )[0]
    snapshot = db.migrations.snapshot()
    clustered_snapshot = next(
        table for table in snapshot.tables if table.name == "clustered_unique_flavor"
    )
    nonclustered_snapshot = next(
        table for table in snapshot.tables if table.name == "nonclustered_unique_flavor"
    )

    assert "PRIMARY KEY NONCLUSTERED" in clustered
    assert (
        "CONSTRAINT [clustered_unique_flavor_code_unique] UNIQUE CLUSTERED ([code])"
    ) in clustered
    assert "ON [constraintspace]" in clustered
    assert (
        "CONSTRAINT [nonclustered_unique_flavor_code_unique] "
        "UNIQUE NONCLUSTERED ([code]) ON [constraintspace]"
    ) in nonclustered
    assert clustered_snapshot.named_unique_constraints[0].mssql_filegroup == (
        "constraintspace"
    )
    assert clustered_snapshot.named_unique_constraints[0].mssql_clustered is True
    assert nonclustered_snapshot.named_unique_constraints[0].mssql_filegroup == (
        "constraintspace"
    )
    assert nonclustered_snapshot.named_unique_constraints[0].mssql_clustered is False
    with pytest.raises(ValueError, match="SQL Server unique constraint clustering"):
        compile_create_table_sql(db._table_map, "clustered_unique_flavor", "postgresql")
    with pytest.raises(ValueError, match="SQL Server unique constraint filegroups"):
        compile_create_table_sql(
            db._table_map,
            "filegroup_unique_flavor",
            "postgresql",
        )


def test_oracle_unique_constraint_tablespaces_compile_and_snapshot() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "oracle_spaced_unique_flavor",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="oracle_spaced_unique_flavor_code_unique",
                columns=["code"],
                oracle_tablespace="constraintspace",
                oracle_compress=2,
                deferrable=True,
                initially_deferred=True,
            )
        ],
    )
    class OracleSpacedUniqueFlavor(BaseModel):
        id: str
        code: str

    oracle = compile_create_table_sql(
        db._table_map,
        "oracle_spaced_unique_flavor",
        "oracle",
    )[0]
    snapshot = db.migrations.snapshot()
    table = next(
        table
        for table in snapshot.tables
        if table.name == "oracle_spaced_unique_flavor"
    )

    assert OracleSpacedUniqueFlavor.__name__ == "OracleSpacedUniqueFlavor"
    assert (
        'CONSTRAINT "oracle_spaced_unique_flavor_code_unique" '
        'UNIQUE ("code") USING INDEX COMPRESS 2 TABLESPACE "constraintspace" '
        "DEFERRABLE INITIALLY DEFERRED"
    ) in oracle
    assert table.named_unique_constraints[0].oracle_tablespace == "constraintspace"
    assert table.named_unique_constraints[0].oracle_compress == 2
    with pytest.raises(ValueError, match="Oracle unique constraint tablespaces"):
        compile_create_table_sql(
            db._table_map,
            "oracle_spaced_unique_flavor",
            "postgresql",
        )


def test_constraint_timing_options_reject_contradictions() -> None:
    with pytest.raises(ValueError, match="initially_deferred requires"):
        TableUnique(
            name="flavor_code_unique",
            columns=["code"],
            deferrable=False,
            initially_deferred=True,
        )
    with pytest.raises(ValueError, match="initially_deferred requires"):
        TableColumn(deferrable=False, initially_deferred=True)
    with pytest.raises(ValueError, match="PostgreSQL INCLUDE columns"):
        TableUnique(
            name="flavor_code_unique",
            columns=["code"],
            postgres_include=[" "],
        )


def test_table_constraint_comments_compile_and_snapshot() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "commented_constraint_supplier",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="commented_constraint_supplier_code_unique",
                columns=["code"],
                comment="Supplier code identity",
            )
        ],
    )
    class CommentedConstraintSupplier(BaseModel):
        id: int
        code: str

    @db.table(
        "commented_constraint_flavor",
        pk="id",
        check_constraints=[
            TableCheck(
                name="commented_constraint_flavor_rating_check",
                expression="rating >= 0",
                comment="Rating guard",
            )
        ],
        foreign_key_constraints=[
            TableForeignKey(
                name="commented_constraint_flavor_supplier_fk",
                columns=["supplier_id"],
                foreign_table="commented_constraint_supplier",
                foreign_columns=["id"],
                comment="Supplier lookup",
            )
        ],
    )
    class CommentedConstraintFlavor(BaseModel):
        id: int
        rating: int
        supplier_id: int

    supplier_sql = compile_create_table_sql(
        db._table_map, "commented_constraint_supplier", "postgresql"
    )
    flavor_sql = compile_create_table_sql(
        db._table_map, "commented_constraint_flavor", "postgresql"
    )
    mssql_supplier_sql = compile_create_table_sql(
        db._table_map, "commented_constraint_supplier", "mssql"
    )
    mssql_flavor_sql = compile_create_table_sql(
        db._table_map, "commented_constraint_flavor", "mssql"
    )
    snapshot = db.migrations.snapshot()
    supplier = next(
        table
        for table in snapshot.tables
        if table.name == "commented_constraint_supplier"
    )
    flavor = next(
        table
        for table in snapshot.tables
        if table.name == "commented_constraint_flavor"
    )

    assert CommentedConstraintSupplier.__name__ == "CommentedConstraintSupplier"
    assert CommentedConstraintFlavor.__name__ == "CommentedConstraintFlavor"
    assert (
        'COMMENT ON CONSTRAINT "commented_constraint_supplier_code_unique" '
        'ON "commented_constraint_supplier" IS '
        "'Supplier code identity'"
    ) in supplier_sql
    assert (
        'COMMENT ON CONSTRAINT "commented_constraint_flavor_rating_check" '
        'ON "commented_constraint_flavor" IS '
        "'Rating guard'"
    ) in flavor_sql
    assert (
        'COMMENT ON CONSTRAINT "commented_constraint_flavor_supplier_fk" '
        'ON "commented_constraint_flavor" IS '
        "'Supplier lookup'"
    ) in flavor_sql
    assert any(
        "sys.sp_addextendedproperty" in statement
        and "@level2type = N'CONSTRAINT'" in statement
        and "@level2name = N'commented_constraint_supplier_code_unique'" in statement
        and "@value = N'Supplier code identity'" in statement
        for statement in mssql_supplier_sql
    )
    assert any(
        "sys.sp_addextendedproperty" in statement
        and "@level2type = N'CONSTRAINT'" in statement
        and "@level2name = N'commented_constraint_flavor_rating_check'" in statement
        and "@value = N'Rating guard'" in statement
        for statement in mssql_flavor_sql
    )
    assert any(
        "sys.sp_addextendedproperty" in statement
        and "@level2type = N'CONSTRAINT'" in statement
        and "@level2name = N'commented_constraint_flavor_supplier_fk'" in statement
        and "@value = N'Supplier lookup'" in statement
        for statement in mssql_flavor_sql
    )
    assert supplier.named_unique_constraints[0].comment == "Supplier code identity"
    assert flavor.check_constraints[0].comment == "Rating guard"
    assert flavor.foreign_key_constraints[0].comment == "Supplier lookup"
    with pytest.raises(ValueError, match="constraint comments"):
        compile_create_table_sql(db._table_map, "commented_constraint_flavor", "mysql")


def test_table_decorator_foreign_key_constraints_compile_and_snapshot() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("composite_supplier", pk="id", unique_constraints=[["id", "code"]])
    class CompositeSupplier(BaseModel):
        id: int
        code: str

    @db.table(
        "composite_flavor",
        pk="id",
        foreign_key_constraints=[
            TableForeignKey(
                name="composite_flavor_primary_supplier_fk",
                columns=["primary_supplier_id"],
                foreign_table="composite_supplier",
                foreign_columns=["id"],
                deferrable=True,
                initially_deferred=True,
                match="full",
            ),
            TableForeignKey(
                name="composite_flavor_supplier_fk",
                columns=["supplier_id", "supplier_code"],
                foreign_table="composite_supplier",
                foreign_columns=["id", "code"],
                on_delete="cascade",
                deferrable=True,
                initially_deferred=True,
                match="full",
            ),
        ],
    )
    class CompositeFlavor(BaseModel):
        id: int
        primary_supplier_id: int
        supplier_id: int
        supplier_code: str

    sql = "\n".join(
        compile_create_table_sql(db._table_map, "composite_flavor", "postgresql")
    )
    sqlite_sql = "\n".join(
        compile_create_table_sql(db._table_map, "composite_flavor", "sqlite")
    )
    snapshot = db.migrations.snapshot()
    flavor = next(
        table for table in snapshot.tables if table.name == "composite_flavor"
    )

    assert CompositeSupplier.__name__ == "CompositeSupplier"
    assert CompositeFlavor.__name__ == "CompositeFlavor"
    assert (
        'CONSTRAINT "composite_flavor_primary_supplier_fk" '
        'FOREIGN KEY ("primary_supplier_id") '
        'REFERENCES "composite_supplier" ("id") '
        "MATCH FULL DEFERRABLE INITIALLY DEFERRED"
    ) in sql
    assert (
        'CONSTRAINT "composite_flavor_primary_supplier_fk" '
        'FOREIGN KEY ("primary_supplier_id") '
        'REFERENCES "composite_supplier" ("id") '
        "MATCH FULL DEFERRABLE INITIALLY DEFERRED"
    ) in sqlite_sql
    assert (
        'CONSTRAINT "composite_flavor_supplier_fk" '
        'FOREIGN KEY ("supplier_id", "supplier_code") '
        'REFERENCES "composite_supplier" ("id", "code") '
        "MATCH FULL ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED"
    ) in sql
    assert (
        'CONSTRAINT "composite_flavor_supplier_fk" '
        'FOREIGN KEY ("supplier_id", "supplier_code") '
        'REFERENCES "composite_supplier" ("id", "code") '
        "MATCH FULL ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED"
    ) in sqlite_sql
    assert flavor.foreign_key_constraints == [
        ForeignKeyConstraintSnapshot(
            "composite_flavor_primary_supplier_fk",
            ["primary_supplier_id"],
            "composite_supplier",
            ["id"],
            deferrable=True,
            initially_deferred=True,
            match="full",
        ),
        ForeignKeyConstraintSnapshot(
            "composite_flavor_supplier_fk",
            ["supplier_id", "supplier_code"],
            "composite_supplier",
            ["id", "code"],
            on_delete="cascade",
            deferrable=True,
            initially_deferred=True,
            match="full",
        ),
    ]


def test_postgres_not_valid_check_and_foreign_key_constraints() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("not_valid_supplier", pk="id", unique_constraints=[["id", "code"]])
    class NotValidSupplier(BaseModel):
        id: int
        code: str

    @db.table(
        "not_valid_flavor",
        pk="id",
        check_constraints=[
            TableCheck(
                name="not_valid_flavor_rating_check",
                expression="rating >= 0",
                validated=False,
                no_inherit=True,
            )
        ],
        foreign_key_constraints=[
            TableForeignKey(
                name="not_valid_flavor_supplier_fk",
                columns=["supplier_id", "supplier_code"],
                foreign_table="not_valid_supplier",
                foreign_columns=["id", "code"],
                validated=False,
            )
        ],
    )
    class NotValidFlavor(BaseModel):
        id: int
        rating: int
        supplier_id: int
        supplier_code: str

    postgres_sql = compile_create_table_sql(
        db._table_map, "not_valid_flavor", "postgresql"
    )
    snapshot = db.migrations.snapshot()
    table = next(table for table in snapshot.tables if table.name == "not_valid_flavor")

    assert NotValidSupplier.__name__ == "NotValidSupplier"
    assert NotValidFlavor.__name__ == "NotValidFlavor"
    assert not any("NOT VALID" in statement for statement in postgres_sql[:1])
    assert (
        'ALTER TABLE "not_valid_flavor" ADD CONSTRAINT '
        '"not_valid_flavor_rating_check" CHECK (rating >= 0) '
        "NO INHERIT NOT VALID"
    ) in postgres_sql
    assert (
        'ALTER TABLE "not_valid_flavor" ADD CONSTRAINT '
        '"not_valid_flavor_supplier_fk" FOREIGN KEY ("supplier_id", "supplier_code") '
        'REFERENCES "not_valid_supplier" ("id", "code") NOT VALID'
    ) in postgres_sql
    assert table.check_constraints[0].validated is False
    assert table.check_constraints[0].no_inherit is True
    assert table.foreign_key_constraints[0].validated is False
    with pytest.raises(ValueError, match="constraint validation toggles"):
        compile_create_table_sql(db._table_map, "not_valid_flavor", "sqlite")


def test_mysql_not_enforced_check_constraints() -> None:
    db = Ormdantic("mysql://localhost/db")

    @db.table(
        "mysql_not_enforced_flavor",
        pk="id",
        check_constraints=[
            TableCheck(
                name="mysql_not_enforced_flavor_rating_check",
                expression="rating >= 0",
                validated=False,
            )
        ],
    )
    class MySqlNotEnforcedFlavor(BaseModel):
        id: int
        rating: int

    @db.table("mysql_not_enforced_supplier", pk="id")
    class MySqlNotEnforcedSupplier(BaseModel):
        id: int

    @db.table(
        "mysql_not_enforced_fk_flavor",
        pk="id",
        foreign_key_constraints=[
            TableForeignKey(
                name="mysql_not_enforced_fk_flavor_supplier_fk",
                columns=["supplier_id"],
                foreign_table="mysql_not_enforced_supplier",
                foreign_columns=["id"],
                validated=False,
            )
        ],
    )
    class MySqlNotEnforcedFkFlavor(BaseModel):
        id: int
        supplier_id: int

    mysql_sql = compile_create_table_sql(
        db._table_map,
        "mysql_not_enforced_flavor",
        "mysql",
    )
    snapshot = db.migrations.snapshot()
    table = next(
        table for table in snapshot.tables if table.name == "mysql_not_enforced_flavor"
    )

    assert MySqlNotEnforcedFlavor.__name__ == "MySqlNotEnforcedFlavor"
    assert MySqlNotEnforcedSupplier.__name__ == "MySqlNotEnforcedSupplier"
    assert MySqlNotEnforcedFkFlavor.__name__ == "MySqlNotEnforcedFkFlavor"
    assert (
        "CONSTRAINT `mysql_not_enforced_flavor_rating_check` "
        "CHECK (rating >= 0) NOT ENFORCED"
    ) in mysql_sql[0]
    assert table.check_constraints[0].validated is False
    with pytest.raises(ValueError, match="constraint validation toggles"):
        compile_create_table_sql(
            db._table_map,
            "mysql_not_enforced_flavor",
            "mariadb",
        )
    with pytest.raises(ValueError, match="constraint validation toggles"):
        compile_create_table_sql(
            db._table_map,
            "mysql_not_enforced_fk_flavor",
            "mysql",
        )


def test_oracle_novalidate_check_and_foreign_key_constraints() -> None:
    db = Ormdantic("oracle://localhost/db")

    @db.table(
        "oracle_novalidate_supplier", pk="id", unique_constraints=[["id", "code"]]
    )
    class OracleNovalidateSupplier(BaseModel):
        id: int
        code: str

    @db.table(
        "oracle_novalidate_flavor",
        pk="id",
        check_constraints=[
            TableCheck(
                name="oracle_novalidate_flavor_rating_check",
                expression="rating >= 0",
                validated=False,
            )
        ],
        foreign_key_constraints=[
            TableForeignKey(
                name="oracle_novalidate_flavor_supplier_fk",
                columns=["supplier_id", "supplier_code"],
                foreign_table="oracle_novalidate_supplier",
                foreign_columns=["id", "code"],
                validated=False,
            )
        ],
    )
    class OracleNovalidateFlavor(BaseModel):
        id: int
        rating: int
        supplier_id: int
        supplier_code: str

    oracle_sql = compile_create_table_sql(
        db._table_map, "oracle_novalidate_flavor", "oracle"
    )
    snapshot = db.migrations.snapshot()
    table = next(
        table for table in snapshot.tables if table.name == "oracle_novalidate_flavor"
    )

    assert OracleNovalidateSupplier.__name__ == "OracleNovalidateSupplier"
    assert OracleNovalidateFlavor.__name__ == "OracleNovalidateFlavor"
    assert (
        'CONSTRAINT "oracle_novalidate_flavor_rating_check" '
        "CHECK (rating >= 0) ENABLE NOVALIDATE"
    ) in oracle_sql[0]
    assert (
        'CONSTRAINT "oracle_novalidate_flavor_supplier_fk" '
        'FOREIGN KEY ("supplier_id", "supplier_code") '
        'REFERENCES "oracle_novalidate_supplier" ("id", "code") '
        "ENABLE NOVALIDATE"
    ) in oracle_sql[0]
    assert table.check_constraints[0].validated is False
    assert table.foreign_key_constraints[0].validated is False


def test_table_foreign_key_constraints_validate_shape_and_columns() -> None:
    with pytest.raises(ValueError, match="at least one column"):
        TableForeignKey(
            name="empty_fk",
            columns=[],
            foreign_table="supplier",
            foreign_columns=[],
        )

    assert (
        TableForeignKey(
            name="single_column_fk",
            columns=["supplier_id"],
            foreign_table="supplier",
            foreign_columns=["id"],
            match="full",
        ).match
        == "full"
    )

    with pytest.raises(ValueError, match="same length"):
        TableForeignKey(
            name="mismatched_fk",
            columns=["supplier_id", "supplier_code"],
            foreign_table="supplier",
            foreign_columns=["id"],
        )
    with pytest.raises(ValueError, match="foreign key match type"):
        TableForeignKey(
            name="bad_match_fk",
            columns=["supplier_id", "supplier_code"],
            foreign_table="supplier",
            foreign_columns=["id", "code"],
            match="partial",
        )

    db = Ormdantic("sqlite:///:memory:")
    with pytest.raises(ValueError, match="reference unknown fields"):

        @db.table(
            "bad_composite_fk",
            pk="id",
            foreign_key_constraints=[
                TableForeignKey(
                    name="bad_composite_fk_supplier_fk",
                    columns=["supplier_id", "missing_code"],
                    foreign_table="supplier",
                    foreign_columns=["id", "code"],
                )
            ],
        )
        class BadCompositeFk(BaseModel):
            id: int
            supplier_id: int


def test_table_decorator_exclusion_constraints_compile_and_snapshot() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "booking",
        pk="id",
        exclusion_constraints=[
            TableExclusion(
                name="booking_room_overlap",
                columns=[("room_id", "="), ("during", "&&")],
                expressions=[("lower(status)", "<>")],
                ops={
                    "during": "gist_tstzrange_ops",
                    "lower(status)": "text_ops",
                },
                using="gist",
                where="cancelled = false",
                deferrable=True,
                initially_deferred=True,
            )
        ],
    )
    class Booking(BaseModel):
        id: int
        room_id: int
        during: str
        status: str
        cancelled: bool

    sql = "\n".join(compile_create_table_sql(db._table_map, "booking", "postgresql"))
    snapshot = db.migrations.snapshot()
    table = next(table for table in snapshot.tables if table.name == "booking")

    assert Booking.__name__ == "Booking"
    assert (
        'CONSTRAINT "booking_room_overlap" EXCLUDE USING gist '
        '("room_id" WITH =, "during" gist_tstzrange_ops WITH &&, '
        "lower(status) text_ops WITH <>) "
        "WHERE (cancelled = false) DEFERRABLE INITIALLY DEFERRED"
    ) in sql
    assert table.exclusion_constraints == [
        ExclusionConstraintSnapshot(
            "booking_room_overlap",
            columns=[("room_id", "="), ("during", "&&")],
            expressions=[("lower(status)", "<>")],
            ops={
                "during": "gist_tstzrange_ops",
                "lower(status)": "text_ops",
            },
            using="gist",
            where="cancelled = false",
            deferrable=True,
            initially_deferred=True,
        )
    ]


def test_table_exclusion_constraints_validate_shape_columns_and_dialect() -> None:
    with pytest.raises(ValueError, match="at least one column or SQL expression"):
        TableExclusion(name="empty_exclusion")

    with pytest.raises(ValueError, match="operator cannot be empty"):
        TableExclusion(name="bad_operator", columns=[("room_id", " ")])

    with pytest.raises(ValueError, match="operator class item.*cannot be empty"):
        TableExclusion(
            name="bad_exclusion_ops",
            columns=[("room_id", "=")],
            ops={" ": "gist_int4_ops"},
        )

    with pytest.raises(ValueError, match="not present in the constraint"):
        TableExclusion(
            name="unknown_exclusion_ops",
            columns=[("room_id", "=")],
            ops={"during": "gist_tstzrange_ops"},
        )

    db = Ormdantic("sqlite:///:memory:")
    with pytest.raises(ValueError, match="reference unknown fields"):

        @db.table(
            "bad_exclusion",
            pk="id",
            exclusion_constraints=[
                TableExclusion(
                    name="bad_exclusion_room_overlap",
                    columns=[("missing_room_id", "=")],
                )
            ],
        )
        class BadExclusion(BaseModel):
            id: int

    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "booking",
        pk="id",
        exclusion_constraints=[
            TableExclusion(
                name="booking_room_overlap",
                columns=[("room_id", "=")],
            )
        ],
    )
    class SqliteBooking(BaseModel):
        id: int
        room_id: int

    assert SqliteBooking.__name__ == "SqliteBooking"
    with pytest.raises(ValueError, match="exclusion constraints"):
        compile_create_table_sql(db._table_map, "booking", "sqlite")


def test_registered_sequences_roundtrip_through_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")
    sequence = db.sequence(
        "flavor_id_seq",
        schema="inventory",
        data_type="BIGINT",
        start=10,
        increment=5,
        min_value=1,
        max_value=1000,
        cycle=True,
        cache=20,
        comment="Flavor ids",
    )

    @db.table(
        "sequenced_flavor",
        pk="id",
        column_options={
            "id": TableColumn(server_default="nextval('inventory.flavor_id_seq')")
        },
    )
    class SequencedFlavor(BaseModel):
        id: int | None = None
        name: str

    snapshot = db.migrations.snapshot()

    assert sequence.name == "flavor_id_seq"
    assert sequence.data_type == "bigint"
    assert sequence.order is False
    assert sequence.comment == "Flavor ids"
    assert db._runtime_sequence_specs() == [
        (
            "flavor_id_seq",
            "inventory",
            10,
            5,
            1,
            1000,
            True,
            20,
            "Flavor ids",
            "bigint",
            False,
            False,
            False,
        )
    ]
    assert snapshot.sequences == [
        SequenceSnapshot(
            "flavor_id_seq",
            schema="inventory",
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


def test_registered_sequence_no_bound_options_compile() -> None:
    db = Ormdantic("postgresql://localhost/db")
    sequence = db.sequence(
        "flavor_id_seq",
        no_min_value=True,
        no_max_value=True,
    )

    snapshot = db.migrations.snapshot()
    plan = planning._build_plan("postgresql", SchemaSnapshot.empty(), snapshot)

    assert sequence.no_min_value is True
    assert sequence.no_max_value is True
    assert db._runtime_sequence_specs() == [
        (
            "flavor_id_seq",
            None,
            None,
            None,
            None,
            None,
            False,
            None,
            None,
            None,
            False,
            True,
            True,
        )
    ]
    assert snapshot.sequences == [
        SequenceSnapshot("flavor_id_seq", no_min_value=True, no_max_value=True)
    ]
    assert plan.dry_run() == [
        'CREATE SEQUENCE IF NOT EXISTS "flavor_id_seq" NO MINVALUE NO MAXVALUE'
    ]
    with pytest.raises(ValueError, match="no_min_value"):
        db.sequence("bad_min_seq", min_value=1, no_min_value=True)
    with pytest.raises(ValueError, match="no_max_value"):
        db.sequence("bad_max_seq", max_value=1000, no_max_value=True)


def test_registered_sequences_execute_runtime_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    db.sequence("flavor_id_seq", comment="Flavor ids")
    executed: list[str] = []

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    db._create_registered_sequences()
    db._drop_registered_sequences()

    assert executed == [
        'CREATE SEQUENCE IF NOT EXISTS "flavor_id_seq"',
        "COMMENT ON SEQUENCE \"flavor_id_seq\" IS 'Flavor ids'",
        'DROP SEQUENCE IF EXISTS "flavor_id_seq"',
    ]


def test_registered_mssql_sequences_execute_runtime_comment_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("mssql://localhost/db")
    db.sequence("flavor_id_seq", schema="inventory", comment="Flavor ids")
    executed: list[str] = []

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "mssql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    db._create_registered_sequences()
    db._drop_registered_sequences()

    assert executed[0] == "CREATE SEQUENCE [inventory].[flavor_id_seq]"
    assert "DECLARE @schema sysname = N'inventory'" in executed[1]
    assert "sys.sp_addextendedproperty" in executed[1]
    assert "@level1type = N'SEQUENCE'" in executed[1]
    assert "@level1name = N'flavor_id_seq'" in executed[1]
    assert "@value = N'Flavor ids'" in executed[1]
    assert executed[2] == "DROP SEQUENCE IF EXISTS [inventory].[flavor_id_seq]"


def test_registered_namespaces_roundtrip_through_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")
    namespace = db.namespace("inventory", comment="Warehouse schema")

    snapshot = db.migrations.snapshot()

    assert namespace.name == "inventory"
    assert namespace.comment == "Warehouse schema"
    assert db._runtime_namespace_specs() == [("inventory", "Warehouse schema")]
    assert snapshot.namespaces == [
        NamespaceSnapshot("inventory", comment="Warehouse schema")
    ]


def test_registered_namespaces_reject_duplicate_names() -> None:
    db = Ormdantic("postgresql://localhost/db")

    db.namespace("inventory")
    with pytest.raises(ValueError, match="duplicate namespace"):
        db.namespace("inventory")


def test_registered_namespaces_execute_runtime_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    db.namespace("inventory", comment="Warehouse schema")
    executed: list[str] = []

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    db._create_registered_namespaces()
    db._drop_registered_namespaces()

    assert executed == [
        'CREATE SCHEMA IF NOT EXISTS "inventory"',
        "COMMENT ON SCHEMA \"inventory\" IS 'Warehouse schema'",
        'DROP SCHEMA IF EXISTS "inventory"',
    ]


def test_registered_sequences_reject_duplicate_names() -> None:
    db = Ormdantic("postgresql://localhost/db")

    db.sequence("flavor_id_seq", schema="inventory")
    with pytest.raises(ValueError, match="duplicate sequence"):
        db.sequence("flavor_id_seq", schema="inventory")


def test_registered_views_roundtrip_through_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")
    view = db.view(
        "active_flavors",
        "SELECT id, name FROM flavor WHERE deleted_at IS NULL;",
        schema="inventory",
        comment="Active flavors",
    )

    snapshot = db.migrations.snapshot()

    assert view.name == "active_flavors"
    assert view.definition == "SELECT id, name FROM flavor WHERE deleted_at IS NULL"
    assert view.comment == "Active flavors"
    assert db._runtime_view_specs() == [
        (
            "active_flavors",
            "inventory",
            "SELECT id, name FROM flavor WHERE deleted_at IS NULL",
            False,
            "Active flavors",
        )
    ]
    assert snapshot.views == [
        ViewSnapshot(
            "active_flavors",
            "SELECT id, name FROM flavor WHERE deleted_at IS NULL",
            schema="inventory",
            comment="Active flavors",
        )
    ]


def test_registered_materialized_views_roundtrip_through_snapshots() -> None:
    db = Ormdantic("postgresql://localhost/db")
    view = db.view(
        "active_flavor_counts",
        "SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id;",
        schema="inventory",
        materialized=True,
    )

    snapshot = db.migrations.snapshot()

    assert view.name == "active_flavor_counts"
    assert view.definition == (
        "SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id"
    )
    assert db._runtime_view_specs() == [
        (
            "active_flavor_counts",
            "inventory",
            "SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id",
            True,
            None,
        )
    ]
    assert snapshot.views == [
        ViewSnapshot(
            "active_flavor_counts",
            "SELECT supplier_id, count(*) AS flavor_count FROM flavor GROUP BY supplier_id",
            schema="inventory",
            materialized=True,
        )
    ]


def test_registered_views_reject_duplicate_names() -> None:
    db = Ormdantic("postgresql://localhost/db")

    db.view("active_flavors", "SELECT id FROM flavor", schema="inventory")
    with pytest.raises(ValueError, match="duplicate view"):
        db.view("active_flavors", "SELECT id FROM flavor", schema="inventory")


def test_registered_views_execute_runtime_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    db.view("active_flavors", "SELECT id FROM flavor", comment="Active flavors")
    executed: list[str] = []

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    db._create_registered_views()
    db._drop_registered_views()

    assert executed == [
        'CREATE VIEW "active_flavors" AS SELECT id FROM flavor',
        "COMMENT ON VIEW \"active_flavors\" IS 'Active flavors'",
        'DROP VIEW IF EXISTS "active_flavors"',
    ]


def test_registered_materialized_views_execute_runtime_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")
    db.view("active_flavors", "SELECT id FROM flavor", materialized=True)
    executed: list[str] = []

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    db._create_registered_views()
    db._drop_registered_views()

    assert executed == [
        'CREATE MATERIALIZED VIEW "active_flavors" AS SELECT id FROM flavor',
        'DROP MATERIALIZED VIEW IF EXISTS "active_flavors"',
    ]


def test_registered_oracle_materialized_views_execute_comment_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("oracle://localhost/db")
    db.view(
        "active_flavors",
        "SELECT id FROM flavor",
        materialized=True,
        comment="Active flavors",
    )
    executed: list[str] = []

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "oracle://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    db._create_registered_views()
    db._drop_registered_views()

    assert executed == [
        'CREATE MATERIALIZED VIEW "active_flavors" AS SELECT id FROM flavor',
        "COMMENT ON MATERIALIZED VIEW \"active_flavors\" IS 'Active flavors'",
        'DROP MATERIALIZED VIEW "active_flavors"',
    ]


def test_registered_oracle_views_execute_comment_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("oracle://localhost/db")
    db.view("active_flavors", "SELECT id FROM flavor", comment="Active flavors")
    executed: list[str] = []

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "oracle://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    db._create_registered_views()
    db._drop_registered_views()

    assert executed == [
        'CREATE VIEW "active_flavors" AS SELECT id FROM flavor',
        "COMMENT ON TABLE \"active_flavors\" IS 'Active flavors'",
        'DROP VIEW "active_flavors"',
    ]


def test_table_decorator_rejects_duplicate_unique_constraint_names() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "duplicate_unique_flavor",
        pk="id",
        unique=["code"],
        unique_constraints=[
            TableUnique(name="duplicate_unique_flavor_code_unique", columns=["name"]),
            TableUnique(name="duplicate_unique_flavor_unique_0", columns=["code"]),
        ],
    )
    class DuplicateUniqueFlavor(BaseModel):
        id: str
        name: str
        code: str

    table = db._table_map.name_to_data["duplicate_unique_flavor"]

    with pytest.raises(ValueError, match="duplicate unique constraint name"):
        compile_create_table_sql(db._table_map, table.tablename, "sqlite")


@pytest.mark.asyncio
async def test_enum_value_checks_apply_at_runtime(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'enum_checks.sqlite3'}")

    @db.table("enum_checked_flavor", pk="id")
    class EnumCheckedFlavor(BaseModel):
        id: str
        flavor: DdlFlavor

    await db.init()

    await db[EnumCheckedFlavor].insert(
        EnumCheckedFlavor(id="1", flavor=DdlFlavor.MOCHA)
    )
    stored = await db[EnumCheckedFlavor].find_one("1")

    assert stored is not None
    assert stored.flavor is DdlFlavor.MOCHA


@pytest.mark.asyncio
async def test_named_unique_constraints_apply_at_runtime(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'named_uniques.sqlite3'}")

    @db.table(
        "runtime_named_unique_flavor",
        pk="id",
        unique_constraints=[
            TableUnique(
                name="runtime_named_unique_flavor_name_code_unique",
                columns=["name", "code"],
            )
        ],
    )
    class RuntimeNamedUniqueFlavor(BaseModel):
        id: str
        name: str
        code: str

    await db.init()

    await db[RuntimeNamedUniqueFlavor].insert(
        RuntimeNamedUniqueFlavor(id="1", name="Mocha", code="m")
    )
    with pytest.raises(Exception, match="UNIQUE constraint failed"):
        await db[RuntimeNamedUniqueFlavor].insert(
            RuntimeNamedUniqueFlavor(id="2", name="Mocha", code="m")
        )


@pytest.mark.asyncio
async def test_sqlite_conflict_clauses_compile_snapshot_and_apply(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'sqlite_conflicts.sqlite3'}")

    @db.table(
        "conflict_flavor",
        pk="id",
        unique=["name"],
        column_options={
            "id": TableColumn(sqlite_on_conflict_primary_key="replace"),
            "name": TableColumn(
                sqlite_on_conflict_not_null="fail",
                sqlite_on_conflict_unique="ignore",
            ),
        },
        unique_constraints=[
            TableUnique(
                name="conflict_flavor_code_unique",
                columns=["code"],
                sqlite_on_conflict="ignore",
            )
        ],
    )
    class SqliteConflictFlavor(BaseModel):
        id: int
        name: str
        code: str

    sqlite_sql = compile_create_table_sql(db._table_map, "conflict_flavor", "sqlite")
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "conflict_flavor"
    )
    name_column = next(column for column in table.columns if column.name == "name")
    id_column = next(column for column in table.columns if column.name == "id")

    assert sqlite_sql == [
        'CREATE TABLE IF NOT EXISTS "conflict_flavor" '
        '("id" INTEGER PRIMARY KEY ON CONFLICT REPLACE NOT NULL, '
        '"name" TEXT NOT NULL ON CONFLICT FAIL, "code" TEXT NOT NULL, '
        'CONSTRAINT "conflict_flavor_code_unique" UNIQUE ("code") '
        'ON CONFLICT IGNORE, CONSTRAINT "conflict_flavor_unique_0" '
        'UNIQUE ("name") ON CONFLICT IGNORE)',
        'CREATE UNIQUE INDEX IF NOT EXISTS "conflict_flavor_name_unique_idx" '
        'ON "conflict_flavor" ("name")',
    ]
    assert id_column.sqlite_on_conflict_primary_key == "REPLACE"
    assert name_column.sqlite_on_conflict_not_null == "FAIL"
    assert name_column.sqlite_on_conflict_unique == "IGNORE"
    assert table.named_unique_constraints[0].sqlite_on_conflict == "IGNORE"

    with pytest.raises(ValueError, match="SQLite .* conflict clauses"):
        compile_create_table_sql(db._table_map, "conflict_flavor", "postgresql")

    await db.init()
    await db[SqliteConflictFlavor].insert(
        SqliteConflictFlavor(id=1, name="Mocha", code="m")
    )
    await db[SqliteConflictFlavor].insert(
        SqliteConflictFlavor(id=2, name="Latte", code="m")
    )

    assert await db[SqliteConflictFlavor].count() == 1


@pytest.mark.asyncio
async def test_sqlite_table_options_compile_snapshot_and_apply(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'sqlite_table_options.sqlite3'}")

    @db.table(
        "strict_flavor",
        pk="id",
        sqlite_strict=True,
        sqlite_without_rowid=True,
    )
    class StrictFlavor(BaseModel):
        id: int
        name: str

    sqlite_sql = compile_create_table_sql(db._table_map, "strict_flavor", "sqlite")
    table = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "strict_flavor"
    )

    assert sqlite_sql == [
        'CREATE TABLE IF NOT EXISTS "strict_flavor" '
        '("id" INTEGER PRIMARY KEY NOT NULL, "name" TEXT NOT NULL) '
        "STRICT, WITHOUT ROWID"
    ]
    assert table.sqlite_strict is True
    assert table.sqlite_without_rowid is True
    with pytest.raises(ValueError, match="SQLite table options"):
        compile_create_table_sql(db._table_map, "strict_flavor", "postgresql")

    await db.init()
    await db[StrictFlavor].insert(StrictFlavor(id=1, name="Mocha"))
    stored = await db[StrictFlavor].find_one(1)

    assert stored is not None
    assert stored.name == "Mocha"


@pytest.mark.asyncio
async def test_sqlite_decimal_columns_store_high_precision_text(tmp_path) -> None:
    path = tmp_path / "decimal_text.sqlite3"
    url = f"sqlite:///{path}"
    value = Decimal("12345678901234567890.123456789")
    db = Ormdantic(url)

    @db.table(
        "decimal_text_price",
        pk="id",
        column_options={
            "amount": TableColumn(numeric_precision=30, numeric_scale=9),
        },
    )
    class DecimalTextPrice(BaseModel):
        id: str
        amount: Decimal

    await db.init()
    await db[DecimalTextPrice].insert(DecimalTextPrice(id="1", amount=value))

    stored = await db[DecimalTextPrice].find_one("1")
    native = execute_native(
        url,
        "SELECT amount, typeof(amount) FROM decimal_text_price WHERE id = ?1",
        ["1"],
    )

    assert stored is not None
    assert stored.amount == value
    assert native["rows"][0] == [value, "text"]


@pytest.mark.asyncio
async def test_sqlite_decimal_filters_ordering_and_checks_are_numeric(tmp_path) -> None:
    path = tmp_path / "decimal_numeric.sqlite3"
    url = f"sqlite:///{path}"
    db = Ormdantic(url)

    @db.table("decimal_numeric_price", pk="id")
    class DecimalNumericPrice(BaseModel):
        id: str
        amount: Decimal = Field(gt=Decimal("0"))

    sqlite_sql = compile_create_table_sql(
        db._table_map,
        "decimal_numeric_price",
        "sqlite",
    )

    assert (
        'CONSTRAINT "decimal_numeric_price_amount_gt_check" '
        "CHECK (ormdantic_decimal_cmp(amount, '0') > 0)"
    ) in sqlite_sql[0]

    await db.init()
    await db[DecimalNumericPrice].insert(
        DecimalNumericPrice(id="two", amount=Decimal("2"))
    )
    await db[DecimalNumericPrice].insert(
        DecimalNumericPrice(id="ten", amount=Decimal("10"))
    )
    await db[DecimalNumericPrice].insert(
        DecimalNumericPrice(id="one-two", amount=Decimal("1.2"))
    )
    await db[DecimalNumericPrice].insert(
        DecimalNumericPrice(id="one-two-one", amount=Decimal("1.201"))
    )

    with pytest.raises(Exception, match="CHECK constraint failed"):
        execute_native(
            url,
            "INSERT INTO decimal_numeric_price (id, amount) VALUES (?1, ?2)",
            ["negative", Decimal("-0.01")],
        )

    ordered = await db[DecimalNumericPrice].find_many(order_by=["amount"])
    greater_than_nine = await db[DecimalNumericPrice].find_many(
        {"amount__gt": Decimal("9")}
    )
    equal_with_different_scale = await db[DecimalNumericPrice].find_many(
        {"amount__in": [Decimal("1.20")]}
    )
    expression_ordered = await db[DecimalNumericPrice].find_many(
        order_by=[column("amount").asc()]
    )
    expression_greater_than_nine = await db[DecimalNumericPrice].find_many(
        where=column("amount") > Decimal("9")
    )
    expression_equal_with_different_scale = await db[DecimalNumericPrice].find_many(
        where=column("amount").in_([Decimal("1.20")])
    )

    assert [item.id for item in ordered.data] == [
        "one-two",
        "one-two-one",
        "two",
        "ten",
    ]
    assert [item.id for item in greater_than_nine.data] == ["ten"]
    assert [item.id for item in equal_with_different_scale.data] == ["one-two"]
    assert [item.id for item in expression_ordered.data] == [
        "one-two",
        "one-two-one",
        "two",
        "ten",
    ]
    assert [item.id for item in expression_greater_than_nine.data] == ["ten"]
    assert [item.id for item in expression_equal_with_different_scale.data] == [
        "one-two"
    ]


def test_table_decorator_indexes_compile_advanced_index_metadata() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "advanced_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="advanced_flavor_name_active_idx",
                columns=["name"],
                unique=True,
                where="deleted_at IS NULL",
                include_columns=["code"],
                method="btree",
                expressions=["LOWER(name)"],
                postgres_with={"fillfactor": 70},
                postgres_ops={
                    "name": "text_pattern_ops",
                    "LOWER(name)": "pg_catalog.text_pattern_ops",
                },
                postgres_nulls_not_distinct=True,
                comment="Active flavor lookup",
                postgres_tablespace="fastspace",
            )
        ],
    )
    class AdvancedFlavor(BaseModel):
        id: str
        name: str
        code: str
        deleted_at: str | None = None

    table = db._table_map.name_to_data["advanced_flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "postgresql")
    snapshot = db.migrations.snapshot()

    assert (
        'CREATE UNIQUE INDEX IF NOT EXISTS "advanced_flavor_name_active_idx" '
        'ON "advanced_flavor" USING btree ("name" text_pattern_ops, '
        "LOWER(name) pg_catalog.text_pattern_ops) "
        'INCLUDE ("code") NULLS NOT DISTINCT WITH (fillfactor = 70) '
        "WHERE deleted_at IS NULL"
    ) in statements
    assert (
        "COMMENT ON INDEX \"advanced_flavor_name_active_idx\" IS 'Active flavor lookup'"
    ) in statements
    assert (
        'ALTER INDEX "advanced_flavor_name_active_idx" SET TABLESPACE "fastspace"'
    ) in statements
    with pytest.raises(ValueError, match="PostgreSQL index storage parameters"):
        compile_create_table_sql(db._table_map, table.tablename, "sqlite")
    assert snapshot.tables[0].indexes[0].to_dict() == {
        "name": "advanced_flavor_name_active_idx",
        "columns": ["name"],
        "unique": True,
        "where": "deleted_at IS NULL",
        "include_columns": ["code"],
        "method": "btree",
        "expressions": ["LOWER(name)"],
        "postgres_with": [["fillfactor", "70"]],
        "postgres_ops": {
            "name": "text_pattern_ops",
            "LOWER(name)": "pg_catalog.text_pattern_ops",
        },
        "postgres_nulls_not_distinct": True,
        "comment": "Active flavor lookup",
        "postgres_tablespace": "fastspace",
    }


def test_table_decorator_indexes_are_dialect_aware() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "sqlite_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="sqlite_index_flavor_lower_name_idx",
                expressions=["LOWER(name)"],
                where="deleted_at IS NULL",
            )
        ],
    )
    class SQLiteIndexFlavor(BaseModel):
        id: str
        name: str
        deleted_at: str | None = None

    sqlite_statements = compile_create_table_sql(
        db._table_map, "sqlite_index_flavor", "sqlite"
    )

    assert (
        'CREATE INDEX IF NOT EXISTS "sqlite_index_flavor_lower_name_idx" '
        'ON "sqlite_index_flavor" (LOWER(name)) WHERE deleted_at IS NULL'
    ) in sqlite_statements

    @db.table(
        "mssql_filtered_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="mssql_filtered_index_flavor_name_idx",
                columns=["name"],
                include_columns=["rating"],
                where="[name] IS NOT NULL",
            )
        ],
    )
    class MSSQLFilteredIndexFlavor(BaseModel):
        id: str
        name: str
        rating: int

    mssql_filtered_statements = compile_create_table_sql(
        db._table_map, "mssql_filtered_index_flavor", "mssql"
    )

    assert (
        "CREATE INDEX [mssql_filtered_index_flavor_name_idx] "
        "ON [mssql_filtered_index_flavor] ([name]) "
        "INCLUDE ([rating]) WHERE [name] IS NOT NULL"
    ) in mssql_filtered_statements
    with pytest.raises(ValueError, match="index INCLUDE columns"):
        compile_create_table_sql(db._table_map, "mssql_filtered_index_flavor", "mysql")

    @db.table("plain_index_flavor", pk="id", indexed=["id"])
    class PlainIndexFlavor(BaseModel):
        id: str

    mysql_statements = compile_create_table_sql(
        db._table_map, "plain_index_flavor", "mysql"
    )
    mssql_statements = compile_create_table_sql(
        db._table_map, "plain_index_flavor", "mssql"
    )
    oracle_statements = compile_create_table_sql(
        db._table_map, "plain_index_flavor", "oracle"
    )

    assert (
        "CREATE INDEX `plain_index_flavor_id_idx` ON `plain_index_flavor` (`id`)"
    ) in mysql_statements
    assert (
        "CREATE INDEX [plain_index_flavor_id_idx] ON [plain_index_flavor] ([id])"
    ) in mssql_statements
    assert (
        'CREATE INDEX "plain_index_flavor_id_idx" ON "plain_index_flavor" ("id")'
    ) in oracle_statements

    @db.table(
        "commented_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="commented_index_flavor_name_idx",
                columns=["name"],
                comment="Flavor lookup",
            )
        ],
    )
    class CommentedIndexFlavor(BaseModel):
        id: str
        name: str

    mysql_index_statements = compile_create_table_sql(
        db._table_map, "commented_index_flavor", "mysql"
    )
    mariadb_index_statements = compile_create_table_sql(
        db._table_map, "commented_index_flavor", "mariadb"
    )
    mssql_index_statements = compile_create_table_sql(
        db._table_map, "commented_index_flavor", "mssql"
    )

    assert (
        "CREATE INDEX `commented_index_flavor_name_idx` "
        "ON `commented_index_flavor` (`name`) COMMENT 'Flavor lookup'"
    ) in mysql_index_statements
    assert (
        "CREATE INDEX `commented_index_flavor_name_idx` "
        "ON `commented_index_flavor` (`name`) COMMENT 'Flavor lookup'"
    ) in mariadb_index_statements
    assert (
        "CREATE INDEX [commented_index_flavor_name_idx] "
        "ON [commented_index_flavor] ([name])"
    ) in mssql_index_statements
    assert any(
        "sys.sp_addextendedproperty" in statement
        and "@level2type = N'INDEX'" in statement
        and "@level2name = N'commented_index_flavor_name_idx'" in statement
        and "@value = N'Flavor lookup'" in statement
        for statement in mssql_index_statements
    )
    with pytest.raises(ValueError, match="index comments"):
        compile_create_table_sql(db._table_map, "commented_index_flavor", "sqlite")

    @db.table(
        "prefix_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="prefix_index_flavor_name_code_idx",
                columns=["name", "code"],
                comment="Prefix lookup",
                mysql_length={"name": 12, "code": 6},
                mysql_using="HASH",
            )
        ],
    )
    class PrefixIndexFlavor(BaseModel):
        id: str
        name: str
        code: str

    mysql_prefix_statements = compile_create_table_sql(
        db._table_map, "prefix_index_flavor", "mysql"
    )
    mariadb_prefix_statements = compile_create_table_sql(
        db._table_map, "prefix_index_flavor", "mariadb"
    )

    assert (
        "CREATE INDEX `prefix_index_flavor_name_code_idx` "
        "USING HASH ON `prefix_index_flavor` (`name`(12), `code`(6)) "
        "COMMENT 'Prefix lookup'"
    ) in mysql_prefix_statements
    assert (
        "CREATE INDEX `prefix_index_flavor_name_code_idx` "
        "USING HASH ON `prefix_index_flavor` (`name`(12), `code`(6)) "
        "COMMENT 'Prefix lookup'"
    ) in mariadb_prefix_statements
    prefix_snapshot = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "prefix_index_flavor"
    )
    assert prefix_snapshot.indexes[0].mysql_length == {"name": 12, "code": 6}
    assert prefix_snapshot.indexes[0].mysql_using == "HASH"
    with pytest.raises(ValueError, match="MySQL/MariaDB index"):
        compile_create_table_sql(db._table_map, "prefix_index_flavor", "sqlite")

    @db.table(
        "fulltext_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="fulltext_index_flavor_name_idx",
                columns=["name"],
                comment="Search lookup",
                mysql_prefix="FULLTEXT",
            )
        ],
    )
    class FulltextIndexFlavor(BaseModel):
        id: str
        name: str

    mysql_fulltext_statements = compile_create_table_sql(
        db._table_map, "fulltext_index_flavor", "mysql"
    )
    mariadb_fulltext_statements = compile_create_table_sql(
        db._table_map, "fulltext_index_flavor", "mariadb"
    )
    assert (
        "CREATE FULLTEXT INDEX `fulltext_index_flavor_name_idx` "
        "ON `fulltext_index_flavor` (`name`) COMMENT 'Search lookup'"
    ) in mysql_fulltext_statements
    assert (
        "CREATE FULLTEXT INDEX `fulltext_index_flavor_name_idx` "
        "ON `fulltext_index_flavor` (`name`) COMMENT 'Search lookup'"
    ) in mariadb_fulltext_statements
    fulltext_snapshot = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "fulltext_index_flavor"
    )
    assert fulltext_snapshot.indexes[0].mysql_prefix == "FULLTEXT"
    with pytest.raises(ValueError, match="MySQL/MariaDB index"):
        compile_create_table_sql(db._table_map, "fulltext_index_flavor", "sqlite")

    @db.table(
        "hidden_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="hidden_index_flavor_name_idx",
                columns=["name"],
                comment="Hidden lookup",
                mysql_visible=False,
            ),
            TableIndex(
                name="hidden_index_flavor_code_idx",
                columns=["code"],
                mysql_visible=True,
            ),
        ],
    )
    class HiddenIndexFlavor(BaseModel):
        id: str
        name: str
        code: str

    mysql_hidden_statements = compile_create_table_sql(
        db._table_map, "hidden_index_flavor", "mysql"
    )
    assert (
        "CREATE INDEX `hidden_index_flavor_name_idx` "
        "ON `hidden_index_flavor` (`name`) INVISIBLE COMMENT 'Hidden lookup'"
    ) in mysql_hidden_statements
    assert (
        "CREATE INDEX `hidden_index_flavor_code_idx` "
        "ON `hidden_index_flavor` (`code`) VISIBLE"
    ) in mysql_hidden_statements
    hidden_snapshot = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "hidden_index_flavor"
    )
    assert hidden_snapshot.indexes[0].mysql_visible is False
    assert hidden_snapshot.indexes[1].mysql_visible is True
    with pytest.raises(ValueError, match="MySQL index visibility"):
        compile_create_table_sql(db._table_map, "hidden_index_flavor", "mariadb")
    with pytest.raises(ValueError, match="MySQL index visibility"):
        compile_create_table_sql(db._table_map, "hidden_index_flavor", "sqlite")

    @db.table(
        "mssql_spaced_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="mssql_spaced_index_flavor_name_idx",
                columns=["name"],
                mssql_filegroup="indexspace",
                mssql_clustered=True,
            )
        ],
    )
    class MSSQLSpacedIndexFlavor(BaseModel):
        id: str
        name: str

    mssql_filegroup_statements = compile_create_table_sql(
        db._table_map, "mssql_spaced_index_flavor", "mssql"
    )

    assert ("PRIMARY KEY NONCLUSTERED") in mssql_filegroup_statements[0]
    assert (
        "CREATE CLUSTERED INDEX [mssql_spaced_index_flavor_name_idx] "
        "ON [mssql_spaced_index_flavor] ([name]) ON [indexspace]"
    ) in mssql_filegroup_statements
    mssql_spaced_snapshot = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "mssql_spaced_index_flavor"
    )
    assert mssql_spaced_snapshot.indexes[0].mssql_filegroup == "indexspace"
    assert mssql_spaced_snapshot.indexes[0].mssql_clustered is True
    with pytest.raises(ValueError, match="SQL Server clustered indexes"):
        compile_create_table_sql(
            db._table_map, "mssql_spaced_index_flavor", "postgresql"
        )

    @db.table(
        "oracle_spaced_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="oracle_spaced_index_flavor_name_idx",
                columns=["name", "code"],
                oracle_tablespace="oraclespace",
                oracle_bitmap=True,
                oracle_compress=2,
            )
        ],
    )
    class OracleSpacedIndexFlavor(BaseModel):
        id: str
        name: str
        code: str

    oracle_tablespace_statements = compile_create_table_sql(
        db._table_map, "oracle_spaced_index_flavor", "oracle"
    )

    assert (
        'CREATE BITMAP INDEX "oracle_spaced_index_flavor_name_idx" '
        'ON "oracle_spaced_index_flavor" ("name", "code") '
        'COMPRESS 2 TABLESPACE "oraclespace"'
    ) in oracle_tablespace_statements
    oracle_spaced_snapshot = next(
        table
        for table in db.migrations.snapshot().tables
        if table.name == "oracle_spaced_index_flavor"
    )
    assert oracle_spaced_snapshot.indexes[0].oracle_tablespace == "oraclespace"
    assert oracle_spaced_snapshot.indexes[0].oracle_bitmap is True
    assert oracle_spaced_snapshot.indexes[0].oracle_compress == 2
    with pytest.raises(ValueError, match="Oracle index tablespaces"):
        compile_create_table_sql(db._table_map, "oracle_spaced_index_flavor", "mssql")

    @db.table(
        "spaced_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="spaced_index_flavor_name_idx",
                columns=["name"],
                postgres_tablespace="fastspace",
            )
        ],
    )
    class SpacedIndexFlavor(BaseModel):
        id: str
        name: str

    with pytest.raises(ValueError, match="index tablespaces"):
        compile_create_table_sql(db._table_map, "spaced_index_flavor", "mysql")

    @db.table(
        "include_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="include_index_flavor_name_idx",
                columns=["name"],
                include_columns=["code"],
            )
        ],
    )
    class IncludeIndexFlavor(BaseModel):
        id: str
        name: str
        code: str

    with pytest.raises(ValueError, match="index INCLUDE columns"):
        compile_create_table_sql(db._table_map, "include_index_flavor", "sqlite")


def test_table_decorator_rejects_duplicate_index_names() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "duplicate_index_flavor",
        pk="id",
        indexed=["name"],
        indexes=[TableIndex(name="duplicate_index_flavor_name_idx", columns=["code"])],
    )
    class DuplicateIndexFlavor(BaseModel):
        id: str
        name: str
        code: str

    table = db._table_map.name_to_data["duplicate_index_flavor"]

    with pytest.raises(ValueError, match="duplicate index name"):
        compile_create_table_sql(db._table_map, table.tablename, "sqlite")


def test_table_index_rejects_empty_comment() -> None:
    with pytest.raises(ValueError, match="index comment cannot be empty"):
        TableIndex(name="empty_comment_idx", columns=["name"], comment=" ")


def test_table_index_rejects_empty_postgres_tablespace() -> None:
    with pytest.raises(ValueError, match="PostgreSQL tablespace .* cannot be empty"):
        TableIndex(
            name="empty_tablespace_idx", columns=["name"], postgres_tablespace=" "
        )


def test_table_index_rejects_postgres_nulls_not_distinct_without_unique() -> None:
    with pytest.raises(ValueError, match="NULLS NOT DISTINCT requires a unique index"):
        TableIndex(
            name="invalid_postgres_nulls_idx",
            columns=["name"],
            postgres_nulls_not_distinct=True,
        )


def test_table_index_rejects_empty_mssql_filegroup() -> None:
    with pytest.raises(ValueError, match="SQL Server filegroup .* cannot be empty"):
        TableIndex(name="empty_filegroup_idx", columns=["name"], mssql_filegroup=" ")


def test_table_unique_rejects_empty_mssql_filegroup() -> None:
    with pytest.raises(ValueError, match="SQL Server filegroup .* cannot be empty"):
        TableUnique(
            name="empty_filegroup_unique",
            columns=["name"],
            mssql_filegroup=" ",
        )


def test_table_unique_rejects_empty_oracle_tablespace() -> None:
    with pytest.raises(ValueError, match="Oracle tablespace"):
        TableUnique(
            name="empty_oracle_unique_tablespace",
            columns=["code"],
            oracle_tablespace=" ",
        )


def test_table_unique_rejects_invalid_oracle_compression() -> None:
    with pytest.raises(ValueError, match="Oracle index compression"):
        TableUnique(
            name="invalid_oracle_unique_compression",
            columns=["code"],
            oracle_compress=0,
        )


def test_table_decorator_rejects_multiple_mssql_clustered_indexes() -> None:
    db = Ormdantic("sqlite:///:memory:")

    with pytest.raises(ValueError, match="multiple SQL Server clustered indexes"):

        @db.table(
            "duplicate_clustered_index_flavor",
            pk="id",
            indexes=[
                TableIndex(
                    name="duplicate_clustered_index_flavor_name_idx",
                    columns=["name"],
                    mssql_clustered=True,
                ),
                TableIndex(
                    name="duplicate_clustered_index_flavor_code_idx",
                    columns=["code"],
                    mssql_clustered=True,
                ),
            ],
        )
        class DuplicateClusteredIndexFlavor(BaseModel):
            id: str
            name: str
            code: str

    with pytest.raises(ValueError, match="multiple SQL Server clustered indexes"):

        @db.table(
            "duplicate_clustered_unique_flavor",
            pk="id",
            indexes=[
                TableIndex(
                    name="duplicate_clustered_unique_flavor_name_idx",
                    columns=["name"],
                    mssql_clustered=True,
                )
            ],
            unique_constraints=[
                TableUnique(
                    name="duplicate_clustered_unique_flavor_code_unique",
                    columns=["code"],
                    mssql_clustered=True,
                )
            ],
        )
        class DuplicateClusteredUniqueFlavor(BaseModel):
            id: str
            name: str
            code: str


def test_table_index_rejects_empty_oracle_tablespace() -> None:
    with pytest.raises(ValueError, match="Oracle tablespace .* cannot be empty"):
        TableIndex(
            name="empty_oracle_tablespace_idx",
            columns=["name"],
            oracle_tablespace=" ",
        )


def test_table_index_rejects_invalid_oracle_compression() -> None:
    with pytest.raises(ValueError, match="Oracle index compression"):
        TableIndex(
            name="invalid_oracle_compression_idx",
            columns=["name"],
            oracle_compress=0,
        )


def test_table_index_rejects_unique_oracle_bitmap() -> None:
    with pytest.raises(ValueError, match="Oracle bitmap indexes cannot be unique"):
        TableIndex(
            name="invalid_oracle_bitmap_idx",
            columns=["name"],
            unique=True,
            oracle_bitmap=True,
        )


def test_table_index_rejects_invalid_mysql_prefix_lengths() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        TableIndex(
            name="invalid_mysql_prefix_idx",
            columns=["name"],
            mysql_length={"name": 0},
        )
    with pytest.raises(ValueError, match="not present in the index"):
        TableIndex(
            name="unknown_mysql_prefix_idx",
            columns=["name"],
            mysql_length={"code": 8},
        )


def test_table_index_rejects_invalid_mysql_index_prefix() -> None:
    with pytest.raises(ValueError, match="MySQL/MariaDB index prefix"):
        TableIndex(
            name="invalid_mysql_prefix_idx",
            columns=["name"],
            mysql_prefix="bad prefix",
        )
    with pytest.raises(ValueError, match="must be FULLTEXT or SPATIAL"):
        TableIndex(
            name="unsupported_mysql_prefix_idx",
            columns=["name"],
            mysql_prefix="BTREE",
        )
    with pytest.raises(ValueError, match="cannot be combined with unique indexes"):
        TableIndex(
            name="unique_mysql_prefix_idx",
            columns=["name"],
            unique=True,
            mysql_prefix="FULLTEXT",
        )
    with pytest.raises(ValueError, match="cannot be combined with USING methods"):
        TableIndex(
            name="using_mysql_prefix_idx",
            columns=["name"],
            mysql_prefix="FULLTEXT",
            mysql_using="HASH",
        )


def test_table_index_rejects_invalid_mysql_using_method() -> None:
    with pytest.raises(ValueError, match="MySQL/MariaDB index USING method"):
        TableIndex(
            name="invalid_mysql_using_idx",
            columns=["name"],
            mysql_using="bad method",
        )


@pytest.mark.asyncio
async def test_index_comments_execute_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "commented_runtime_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="commented_runtime_index_flavor_name_idx",
                columns=["name"],
                comment="Runtime flavor lookup",
                postgres_tablespace="fastspace",
            )
        ],
    )
    class CommentedRuntimeIndexFlavor(BaseModel):
        id: str
        name: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "postgresql://localhost/db"
            indexes = tables[0][4]
            assert indexes == [
                (
                    "commented_runtime_index_flavor_name_idx",
                    ["name"],
                    False,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        'ALTER INDEX "commented_runtime_index_flavor_name_idx" '
        'SET TABLESPACE "fastspace"',
        'COMMENT ON INDEX "commented_runtime_index_flavor_name_idx" '
        "IS 'Runtime flavor lookup'",
    ]


@pytest.mark.asyncio
async def test_postgres_index_operator_classes_recreate_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table(
        "postgres_ops_runtime_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="postgres_ops_runtime_index_flavor_name_idx",
                columns=["name"],
                unique=True,
                comment="Runtime pattern lookup",
                postgres_tablespace="fastspace",
                postgres_ops={"name": "text_pattern_ops"},
                postgres_nulls_not_distinct=True,
            )
        ],
    )
    class PostgresOpsRuntimeIndexFlavor(BaseModel):
        id: str
        name: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "postgresql://localhost/db"
            indexes = tables[0][4]
            assert indexes == [
                (
                    "postgres_ops_runtime_index_flavor_name_idx",
                    ["name"],
                    True,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "postgresql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        'DROP INDEX IF EXISTS "postgres_ops_runtime_index_flavor_name_idx"',
        'CREATE UNIQUE INDEX IF NOT EXISTS "postgres_ops_runtime_index_flavor_name_idx" '
        'ON "postgres_ops_runtime_index_flavor" ("name" text_pattern_ops) '
        "NULLS NOT DISTINCT",
        'ALTER INDEX "postgres_ops_runtime_index_flavor_name_idx" '
        'SET TABLESPACE "fastspace"',
        'COMMENT ON INDEX "postgres_ops_runtime_index_flavor_name_idx" '
        "IS 'Runtime pattern lookup'",
    ]


@pytest.mark.asyncio
async def test_mssql_index_comments_execute_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("mssql://localhost/db")

    @db.table(
        "commented_runtime_mssql_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="commented_runtime_mssql_index_flavor_name_idx",
                columns=["name"],
                comment="Runtime flavor lookup",
            )
        ],
    )
    class CommentedRuntimeMSSQLIndexFlavor(BaseModel):
        id: str
        name: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "mssql://localhost/db"
            indexes = tables[0][4]
            assert indexes == [
                (
                    "commented_runtime_mssql_index_flavor_name_idx",
                    ["name"],
                    False,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "mssql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed[0] == "runtime.create_all"
    assert len(executed) == 2
    assert "sys.sp_addextendedproperty" in executed[1]
    assert "@level2type = N'INDEX'" in executed[1]
    assert (
        "@level2name = N'commented_runtime_mssql_index_flavor_name_idx'" in executed[1]
    )
    assert "@value = N'Runtime flavor lookup'" in executed[1]


@pytest.mark.asyncio
async def test_mssql_index_filegroups_recreate_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("mssql://localhost/db")

    @db.table(
        "filegroup_runtime_mssql_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="filegroup_runtime_mssql_index_flavor_name_idx",
                columns=["name"],
                comment="Runtime flavor lookup",
                mssql_filegroup="indexspace",
                mssql_clustered=True,
            )
        ],
    )
    class FilegroupRuntimeMSSQLIndexFlavor(BaseModel):
        id: str
        name: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "mssql://localhost/db"
            assert tables[0][10][16] is True
            indexes = tables[0][4]
            assert indexes == [
                (
                    "filegroup_runtime_mssql_index_flavor_name_idx",
                    ["name"],
                    False,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "mssql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed[0] == "runtime.create_all"
    assert (
        "DROP INDEX [filegroup_runtime_mssql_index_flavor_name_idx] "
        "ON [filegroup_runtime_mssql_index_flavor]"
    ) == executed[1]
    assert (
        "CREATE CLUSTERED INDEX [filegroup_runtime_mssql_index_flavor_name_idx] "
        "ON [filegroup_runtime_mssql_index_flavor] ([name]) ON [indexspace]"
    ) == executed[2]
    assert "sys.sp_addextendedproperty" in executed[3]
    assert "@level2type = N'INDEX'" in executed[3]
    assert (
        "@level2name = N'filegroup_runtime_mssql_index_flavor_name_idx'" in executed[3]
    )
    assert "@value = N'Runtime flavor lookup'" in executed[3]


@pytest.mark.asyncio
async def test_oracle_index_tablespaces_recreate_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("oracle://localhost/db")

    @db.table(
        "tablespace_runtime_oracle_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="tablespace_runtime_oracle_index_flavor_name_idx",
                columns=["name", "code"],
                oracle_tablespace="oraclespace",
                oracle_bitmap=True,
                oracle_compress=2,
            )
        ],
    )
    class TablespaceRuntimeOracleIndexFlavor(BaseModel):
        id: str
        name: str
        code: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "oracle://localhost/db"
            indexes = tables[0][4]
            assert indexes == [
                (
                    "tablespace_runtime_oracle_index_flavor_name_idx",
                    ["name", "code"],
                    False,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "oracle://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        'DROP INDEX "tablespace_runtime_oracle_index_flavor_name_idx"',
        'CREATE BITMAP INDEX "tablespace_runtime_oracle_index_flavor_name_idx" '
        'ON "tablespace_runtime_oracle_index_flavor" ("name", "code") '
        'COMPRESS 2 TABLESPACE "oraclespace"',
    ]


@pytest.mark.asyncio
async def test_mysql_index_comments_recreate_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("mysql://localhost/db")

    @db.table(
        "commented_runtime_mysql_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="commented_runtime_mysql_index_flavor_name_idx",
                columns=["name"],
                comment="Runtime flavor lookup",
            )
        ],
    )
    class CommentedRuntimeMySQLIndexFlavor(BaseModel):
        id: str
        name: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "mysql://localhost/db"
            indexes = tables[0][4]
            assert indexes == [
                (
                    "commented_runtime_mysql_index_flavor_name_idx",
                    ["name"],
                    False,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "mysql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        "DROP INDEX `commented_runtime_mysql_index_flavor_name_idx` "
        "ON `commented_runtime_mysql_index_flavor`",
        "CREATE INDEX `commented_runtime_mysql_index_flavor_name_idx` "
        "ON `commented_runtime_mysql_index_flavor` (`name`) "
        "COMMENT 'Runtime flavor lookup'",
    ]


@pytest.mark.asyncio
async def test_mysql_index_prefix_lengths_recreate_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("mysql://localhost/db")

    @db.table(
        "prefix_runtime_mysql_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="prefix_runtime_mysql_index_flavor_name_code_idx",
                columns=["name", "code"],
                comment="Runtime prefix lookup",
                mysql_length={"name": 12, "code": 6},
                mysql_using="HASH",
            )
        ],
    )
    class PrefixRuntimeMySQLIndexFlavor(BaseModel):
        id: str
        name: str
        code: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "mysql://localhost/db"
            indexes = tables[0][4]
            assert indexes == [
                (
                    "prefix_runtime_mysql_index_flavor_name_code_idx",
                    ["name", "code"],
                    False,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "mysql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        "DROP INDEX `prefix_runtime_mysql_index_flavor_name_code_idx` "
        "ON `prefix_runtime_mysql_index_flavor`",
        "CREATE INDEX `prefix_runtime_mysql_index_flavor_name_code_idx` "
        "USING HASH ON `prefix_runtime_mysql_index_flavor` "
        "(`name`(12), `code`(6)) "
        "COMMENT 'Runtime prefix lookup'",
    ]


@pytest.mark.asyncio
async def test_mysql_index_prefix_recreates_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("mysql://localhost/db")

    @db.table(
        "fulltext_runtime_mysql_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="fulltext_runtime_mysql_index_flavor_name_idx",
                columns=["name"],
                comment="Runtime search lookup",
                mysql_prefix="FULLTEXT",
            )
        ],
    )
    class FulltextRuntimeMySQLIndexFlavor(BaseModel):
        id: str
        name: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "mysql://localhost/db"
            indexes = tables[0][4]
            assert indexes == [
                (
                    "fulltext_runtime_mysql_index_flavor_name_idx",
                    ["name"],
                    False,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "mysql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        "DROP INDEX `fulltext_runtime_mysql_index_flavor_name_idx` "
        "ON `fulltext_runtime_mysql_index_flavor`",
        "CREATE FULLTEXT INDEX `fulltext_runtime_mysql_index_flavor_name_idx` "
        "ON `fulltext_runtime_mysql_index_flavor` (`name`) "
        "COMMENT 'Runtime search lookup'",
    ]


@pytest.mark.asyncio
async def test_mysql_index_visibility_recreates_after_runtime_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = Ormdantic("mysql://localhost/db")

    @db.table(
        "hidden_runtime_mysql_index_flavor",
        pk="id",
        indexes=[
            TableIndex(
                name="hidden_runtime_mysql_index_flavor_name_idx",
                columns=["name"],
                comment="Runtime hidden lookup",
                mysql_visible=False,
            )
        ],
    )
    class HiddenRuntimeMySQLIndexFlavor(BaseModel):
        id: str
        name: str

    executed: list[str] = []

    class FakeRuntime:
        def __init__(
            self,
            connection: str,
            tables: list[tuple[object, ...]],
        ) -> None:
            assert connection == "mysql://localhost/db"
            indexes = tables[0][4]
            assert indexes == [
                (
                    "hidden_runtime_mysql_index_flavor_name_idx",
                    ["name"],
                    False,
                    None,
                    [],
                    None,
                    [],
                    [],
                )
            ]

        def create_all(self) -> None:
            executed.append("runtime.create_all")

    def fake_execute_native(
        connection: str,
        sql: str,
        values: list[object],
    ) -> dict[str, object]:
        assert connection == "mysql://localhost/db"
        assert values == []
        executed.append(sql)
        return {}

    monkeypatch.setattr(orm_module._ormdantic, "PyDatabase", FakeRuntime)
    monkeypatch.setattr(orm_module._ormdantic, "execute_native", fake_execute_native)

    await db.create_all()

    assert executed == [
        "runtime.create_all",
        "DROP INDEX `hidden_runtime_mysql_index_flavor_name_idx` "
        "ON `hidden_runtime_mysql_index_flavor`",
        "CREATE INDEX `hidden_runtime_mysql_index_flavor_name_idx` "
        "ON `hidden_runtime_mysql_index_flavor` (`name`) "
        "INVISIBLE COMMENT 'Runtime hidden lookup'",
    ]


def test_table_decorator_column_options_compile_advanced_column_metadata() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "column_option_flavor",
        pk="id",
        column_options={
            "id": TableColumn(autoincrement=True),
            "name": TableColumn(
                server_default="'vanilla'",
                collation="NOCASE",
            ),
            "name_lower": TableColumn(
                computed="LOWER(name)",
                computed_persisted=True,
            ),
            "price": TableColumn(numeric_precision=12, numeric_scale=2),
        },
    )
    class ColumnOptionFlavor(BaseModel):
        id: int
        name: str
        name_lower: str
        price: Decimal

    table = db._table_map.name_to_data["column_option_flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "sqlite")
    snapshot = db.migrations.snapshot()

    assert statements[0] == (
        'CREATE TABLE IF NOT EXISTS "column_option_flavor" ('
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, '
        "\"name\" TEXT NOT NULL DEFAULT 'vanilla' COLLATE NOCASE, "
        '"name_lower" TEXT NOT NULL GENERATED ALWAYS AS (LOWER(name)) STORED, '
        '"price" DECIMAL_TEXT(12, 2) NOT NULL)'
    )
    assert snapshot.tables[0].columns[1].server_default == "'vanilla'"
    assert snapshot.tables[0].columns[2].computed == "LOWER(name)"
    assert snapshot.tables[0].columns[2].computed_persisted is True
    assert snapshot.tables[0].columns[3].numeric_precision == 12
    assert snapshot.tables[0].columns[3].numeric_scale == 2


def test_table_decorator_column_options_compile_identity_metadata() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "identity_flavor",
        pk="id",
        column_options={
            "id": TableColumn(
                identity=True,
                identity_always=True,
                identity_start=10,
                identity_increment=5,
                identity_min_value=1,
                identity_max_value=1000,
                identity_cycle=True,
                identity_cache=20,
            )
        },
    )
    class IdentityFlavor(BaseModel):
        id: int
        name: str

    table = db._table_map.name_to_data["identity_flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "postgresql")
    snapshot = db.migrations.snapshot()

    assert statements[0] == (
        'CREATE TABLE IF NOT EXISTS "identity_flavor" ('
        '"id" INTEGER GENERATED ALWAYS AS IDENTITY '
        "(START WITH 10 INCREMENT BY 5 MINVALUE 1 MAXVALUE 1000 CYCLE CACHE 20) "
        "PRIMARY KEY NOT NULL, "
        '"name" TEXT NOT NULL)'
    )
    assert snapshot.tables[0].columns[0].identity is True
    assert snapshot.tables[0].columns[0].identity_always is True
    assert snapshot.tables[0].columns[0].identity_start == 10
    assert snapshot.tables[0].columns[0].identity_increment == 5
    assert snapshot.tables[0].columns[0].identity_min_value == 1
    assert snapshot.tables[0].columns[0].identity_max_value == 1000
    assert snapshot.tables[0].columns[0].identity_cycle is True
    assert snapshot.tables[0].columns[0].identity_cache == 20


def test_table_decorator_column_options_compile_identity_no_bounds() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "identity_no_bound_flavor",
        pk="id",
        column_options={
            "id": TableColumn(
                identity=True,
                identity_no_min_value=True,
                identity_no_max_value=True,
            )
        },
    )
    class IdentityNoBoundFlavor(BaseModel):
        id: int
        name: str

    table = db._table_map.name_to_data["identity_no_bound_flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "postgresql")
    snapshot = db.migrations.snapshot()

    assert statements[0] == (
        'CREATE TABLE IF NOT EXISTS "identity_no_bound_flavor" ('
        '"id" INTEGER GENERATED BY DEFAULT AS IDENTITY '
        "(NO MINVALUE NO MAXVALUE) "
        "PRIMARY KEY NOT NULL, "
        '"name" TEXT NOT NULL)'
    )
    assert snapshot.tables[0].columns[0].identity is True
    assert snapshot.tables[0].columns[0].identity_no_min_value is True
    assert snapshot.tables[0].columns[0].identity_no_max_value is True

    with pytest.raises(ValueError, match="identity_no_min_value"):
        TableColumn(identity_min_value=1, identity_no_min_value=True)
    with pytest.raises(ValueError, match="identity_no_max_value"):
        TableColumn(identity_max_value=1000, identity_no_max_value=True)


def test_table_decorator_column_options_compile_oracle_identity_on_null() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "identity_on_null_flavor",
        pk="id",
        column_options={"id": TableColumn(identity=True, identity_on_null=True)},
    )
    class IdentityOnNullFlavor(BaseModel):
        id: int
        name: str

    table = db._table_map.name_to_data["identity_on_null_flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "oracle")
    snapshot = db.migrations.snapshot()

    assert statements[0] == (
        'CREATE TABLE "identity_on_null_flavor" ('
        '"id" INTEGER GENERATED BY DEFAULT ON NULL AS IDENTITY '
        "PRIMARY KEY NOT NULL, "
        '"name" TEXT NOT NULL)'
    )
    assert snapshot.tables[0].columns[0].identity is True
    assert snapshot.tables[0].columns[0].identity_on_null is True

    with pytest.raises(ValueError, match="identity_on_null requires BY DEFAULT"):
        TableColumn(identity_always=True, identity_on_null=True)


def test_table_decorator_check_constraints_compile_table_checks() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "checked_flavor",
        pk="id",
        check_constraints=[
            TableCheck(
                name="checked_flavor_rating_range_check",
                expression="rating BETWEEN 0 AND 100",
            )
        ],
    )
    class CheckedFlavor(BaseModel):
        id: str
        rating: int

    table = db._table_map.name_to_data["checked_flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "sqlite")
    snapshot = db.migrations.snapshot()

    assert statements[0] == (
        'CREATE TABLE IF NOT EXISTS "checked_flavor" ('
        '"id" TEXT PRIMARY KEY NOT NULL, '
        '"rating" INTEGER NOT NULL, '
        'CONSTRAINT "checked_flavor_rating_range_check" '
        "CHECK (rating BETWEEN 0 AND 100))"
    )
    assert snapshot.tables[0].check_constraints[0].to_dict() == {
        "name": "checked_flavor_rating_range_check",
        "expression": "rating BETWEEN 0 AND 100",
    }

    @db.table(
        "postgres_no_inherit_flavor",
        pk="id",
        check_constraints=[
            TableCheck(
                name="postgres_no_inherit_flavor_rating_check",
                expression="rating >= 0",
                no_inherit=True,
            )
        ],
    )
    class PostgresNoInheritFlavor(BaseModel):
        id: str
        rating: int

    postgres_table = db._table_map.name_to_data["postgres_no_inherit_flavor"]
    postgres_statements = compile_create_table_sql(
        db._table_map,
        postgres_table.tablename,
        "postgresql",
    )
    assert (
        'CONSTRAINT "postgres_no_inherit_flavor_rating_check" '
        "CHECK (rating >= 0) NO INHERIT"
    ) in postgres_statements[0]
    with pytest.raises(ValueError, match="check constraint NO INHERIT"):
        compile_create_table_sql(db._table_map, postgres_table.tablename, "sqlite")


def test_table_decorator_column_options_compile_foreign_key_actions() -> None:
    db = Ormdantic("postgresql://localhost/db")

    @db.table("supplier", pk="id", schema="inventory")
    class Supplier(BaseModel):
        id: str = Field(max_length=255)

    @db.table(
        "supplied_flavor",
        pk="id",
        column_options={
            "supplier": TableColumn(
                foreign_key_name="supplied_flavor_supplier_fk",
                on_delete="set_null",
                on_update="cascade",
            )
        },
    )
    class SuppliedFlavor(BaseModel):
        id: str
        supplier: Supplier | str | None = None

    for table in db._table_map.name_to_data.values():
        table.relationships = db.get(table)

    table = db._table_map.name_to_data["supplied_flavor"]
    statements = compile_create_table_sql(db._table_map, table.tablename, "postgresql")
    snapshot = db.migrations.snapshot()

    assert (
        'CONSTRAINT "supplied_flavor_supplier_fk" '
        'FOREIGN KEY ("supplier") REFERENCES "inventory"."supplier" ("id") '
        "ON DELETE SET NULL ON UPDATE CASCADE"
    ) in statements[0]
    assert snapshot.tables[1].columns[1].foreign_key_name == (
        "supplied_flavor_supplier_fk"
    )
    assert snapshot.tables[1].columns[1].foreign_table == "inventory.supplier"
    assert snapshot.tables[1].columns[1].kind == "str"
    assert snapshot.tables[1].columns[1].max_length == 255
    assert snapshot.tables[1].columns[1].on_delete == "set_null"
    assert snapshot.tables[1].columns[1].on_update == "cascade"


def test_table_decorator_rejects_foreign_key_options_without_relationship() -> None:
    db = Ormdantic("sqlite:///:memory:")

    @db.table(
        "bad_foreign_key_option_flavor",
        pk="id",
        column_options={"name": TableColumn(on_delete="cascade")},
    )
    class BadForeignKeyOptionFlavor(BaseModel):
        id: str
        name: str

    table = db._table_map.name_to_data["bad_foreign_key_option_flavor"]

    with pytest.raises(ValueError, match="foreign key options.*require"):
        compile_create_table_sql(db._table_map, table.tablename, "sqlite")


def test_table_column_rejects_unknown_foreign_key_action() -> None:
    with pytest.raises(ValueError, match="foreign key action must be one of"):
        TableColumn(on_delete="explode")


@pytest.mark.asyncio
async def test_table_check_constraints_apply_at_runtime(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'table_checks.sqlite3'}")

    @db.table(
        "runtime_checked_flavor",
        pk="id",
        check_constraints=[
            TableCheck(
                name="runtime_checked_flavor_rating_range_check",
                expression="rating BETWEEN 0 AND 100",
            )
        ],
    )
    class RuntimeCheckedFlavor(BaseModel):
        id: str
        rating: int

    await db.init()

    await db[RuntimeCheckedFlavor].insert(RuntimeCheckedFlavor(id="ok", rating=80))
    with pytest.raises(Exception, match="CHECK constraint failed"):
        await db[RuntimeCheckedFlavor].insert(
            RuntimeCheckedFlavor(id="bad", rating=101)
        )


@pytest.mark.asyncio
async def test_sqlite_pattern_constraints_apply_at_runtime(tmp_path) -> None:
    url = f"sqlite:///{tmp_path / 'pattern_checks.sqlite3'}"
    db = Ormdantic(url)

    @db.table("runtime_pattern_flavor", pk="id")
    class RuntimePatternFlavor(BaseModel):
        id: str
        code: str = Field(pattern=r"^[A-Z]{2}$")

    await db.init()

    await db[RuntimePatternFlavor].insert(RuntimePatternFlavor(id="ok", code="AB"))
    with pytest.raises(Exception, match="CHECK constraint failed"):
        execute_native(
            url,
            "INSERT INTO runtime_pattern_flavor (id, code) VALUES (?1, ?2)",
            ["bad", "ab"],
        )


@pytest.mark.asyncio
async def test_sqlite_multiple_of_constraints_apply_at_runtime(tmp_path) -> None:
    url = f"sqlite:///{tmp_path / 'multiple_checks.sqlite3'}"
    db = Ormdantic(url)

    @db.table("runtime_multiple_flavor", pk="id")
    class RuntimeMultipleFlavor(BaseModel):
        id: str
        quantity: int = Field(multiple_of=5)
        amount: Decimal = Field(multiple_of=Decimal("0.05"))

    await db.init()

    await db[RuntimeMultipleFlavor].insert(
        RuntimeMultipleFlavor(id="ok", quantity=10, amount=Decimal("1.20"))
    )
    with pytest.raises(Exception, match="CHECK constraint failed"):
        execute_native(
            url,
            "INSERT INTO runtime_multiple_flavor (id, quantity, amount) "
            "VALUES (?1, ?2, ?3)",
            ["bad-quantity", 7, Decimal("1.20")],
        )
    with pytest.raises(Exception, match="CHECK constraint failed"):
        execute_native(
            url,
            "INSERT INTO runtime_multiple_flavor (id, quantity, amount) "
            "VALUES (?1, ?2, ?3)",
            ["bad-amount", 10, Decimal("1.21")],
        )


def test_table_decorator_rejects_unknown_column_options() -> None:
    db = Ormdantic("sqlite:///:memory:")

    with pytest.raises(ValueError, match="unknown fields: missing"):

        @db.table(
            "bad_column_option_flavor",
            pk="id",
            column_options={"missing": TableColumn(server_default="'x'")},
        )
        class BadColumnOptionFlavor(BaseModel):
            id: str


@pytest.mark.asyncio
async def test_column_options_skip_generated_and_defaulted_insert_columns(
    tmp_path,
) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'column_options.sqlite3'}")

    @db.table(
        "runtime_column_option_flavor",
        pk="id",
        column_options={
            "status": TableColumn(server_default="'fresh'"),
            "name_lower": TableColumn(
                computed="LOWER(name)",
                computed_persisted=True,
            ),
        },
    )
    class RuntimeColumnOptionFlavor(BaseModel):
        id: str
        name: str
        status: str | None = None
        name_lower: str | None = None

    await db.init()

    await db[RuntimeColumnOptionFlavor].insert(
        RuntimeColumnOptionFlavor(id="1", name="Mocha")
    )
    stored = await db[RuntimeColumnOptionFlavor].find_one("1")

    assert stored is not None
    assert stored.status == "fresh"
    assert stored.name_lower == "mocha"


@pytest.mark.asyncio
async def test_autoincrement_insert_omits_none_primary_key(tmp_path) -> None:
    db = Ormdantic(f"sqlite:///{tmp_path / 'autoincrement.sqlite3'}")

    @db.table(
        "autoincrement_flavor",
        pk="id",
        column_options={"id": TableColumn(autoincrement=True)},
    )
    class AutoincrementFlavor(BaseModel):
        id: int | None = None
        name: str

    await db.init()

    snapshot = db.migrations.snapshot()
    id_column = next(
        column
        for table in snapshot.tables
        if table.name == "autoincrement_flavor"
        for column in table.columns
        if column.name == "id"
    )
    assert id_column.nullable is False

    await db[AutoincrementFlavor].insert(AutoincrementFlavor(name="Mocha"))
    stored = await db[AutoincrementFlavor].find_many()

    assert [(row.id, row.name) for row in stored.data] == [(1, "Mocha")]
