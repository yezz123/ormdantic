use ormdantic_core::TableId;
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ExclusionConstraintDef, ExclusionElementDef, FieldKind,
    ForeignKeyDef, IndexDef, MysqlTableOptions, OracleTableCompression, RelationshipCardinality,
    RelationshipDef, TableDef, UniqueConstraintDef,
};

#[test]
fn table_new_builds_unknown_columns() {
    let table = TableDef::new("flavor", "id", vec!["id".to_string(), "name".to_string()]);

    assert_eq!(table.model_key(), "flavor");
    assert_eq!(table.name(), "flavor");
    assert_eq!(table.primary_key(), "id");
    assert_eq!(table.column_names().collect::<Vec<_>>(), vec!["id", "name"]);
    assert!(table
        .columns()
        .iter()
        .all(|column| column.kind() == &FieldKind::Unknown));
}

#[test]
fn table_from_parts_exposes_metadata_and_qualified_name() {
    let relationship =
        RelationshipDef::new("supplier", "supplier", "id", RelationshipCardinality::One);
    let mut table = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::Integer).primary_key(true),
            ColumnDef::new("supplier_id", FieldKind::Integer),
        ],
        vec![IndexDef::new(
            "flavor_supplier_idx",
            vec!["supplier_id".to_string()],
        )],
        vec![UniqueConstraintDef::new(
            "flavor_supplier_unique",
            vec!["supplier_id".to_string()],
        )],
        vec![relationship],
    )
    .with_schema("inventory")
    .with_comment("flavor table")
    .with_tablespace("fastspace")
    .with_mysql_engine("InnoDB")
    .with_mysql_charset("utf8mb4")
    .with_mysql_collation("utf8mb4_unicode_ci")
    .with_mysql_row_format("DYNAMIC")
    .with_mysql_key_block_size(8)
    .with_mysql_pack_keys(true)
    .with_mysql_checksum(true)
    .with_mysql_delay_key_write(true)
    .with_mysql_stats_persistent(true)
    .with_mysql_stats_auto_recalc(false)
    .with_mysql_stats_sample_pages(32)
    .with_mysql_avg_row_length(64)
    .with_mysql_max_rows(1000)
    .with_mysql_min_rows(10)
    .with_mysql_insert_method("LAST")
    .with_mysql_data_directory("/var/lib/mysql/data")
    .with_mysql_index_directory("/var/lib/mysql/index")
    .with_mysql_connection("mysql://remote.example/db/flavor")
    .with_mysql_union(vec!["flavor_hot".to_string(), "flavor_cold".to_string()])
    .with_mysql_partition_by("HASH (id)")
    .with_mysql_partitions(4)
    .with_mysql_subpartition_by("KEY (supplier_id)")
    .with_mysql_subpartitions(2)
    .with_mysql_auto_increment(101)
    .with_postgres_inherits(vec!["base_flavor".to_string()])
    .with_postgres_with(vec![("fillfactor".to_string(), "70".to_string())])
    .with_postgres_using("heap")
    .postgres_unlogged()
    .sqlite_strict()
    .sqlite_without_rowid()
    .with_oracle_compress_level(6)
    .with_postgres_partition_by("RANGE (id)")
    .with_check_constraints(vec![
        CheckConstraintDef::new("supplier_id > 0").named("supplier_check")
    ])
    .with_foreign_keys(vec![ForeignKeyDef::new(
        vec!["supplier_id".to_string()],
        "supplier",
        vec!["id".to_string()],
    )])
    .with_exclusion_constraints(vec![ExclusionConstraintDef::new(
        "flavor_supplier_exclusion",
        vec![ExclusionElementDef::column("supplier_id", "=")],
    )]);
    table.set_id(TableId(7));

    assert_eq!(table.id(), Some(TableId(7)));
    assert_eq!(table.model_key(), "Flavor");
    assert_eq!(table.schema(), Some("inventory"));
    assert_eq!(table.comment(), Some("flavor table"));
    assert_eq!(table.tablespace(), Some("fastspace"));
    assert_eq!(table.mysql_engine(), Some("InnoDB"));
    assert_eq!(table.mysql_charset(), Some("utf8mb4"));
    assert_eq!(table.mysql_collation(), Some("utf8mb4_unicode_ci"));
    assert_eq!(table.mysql_row_format(), Some("DYNAMIC"));
    assert_eq!(table.mysql_key_block_size(), Some(8));
    assert_eq!(table.mysql_pack_keys(), Some(true));
    assert_eq!(table.mysql_checksum(), Some(true));
    assert_eq!(table.mysql_delay_key_write(), Some(true));
    assert_eq!(table.mysql_stats_persistent(), Some(true));
    assert_eq!(table.mysql_stats_auto_recalc(), Some(false));
    assert_eq!(table.mysql_stats_sample_pages(), Some(32));
    assert_eq!(table.mysql_avg_row_length(), Some(64));
    assert_eq!(table.mysql_max_rows(), Some(1000));
    assert_eq!(table.mysql_min_rows(), Some(10));
    assert_eq!(table.mysql_insert_method(), Some("LAST"));
    assert_eq!(table.mysql_data_directory(), Some("/var/lib/mysql/data"));
    assert_eq!(table.mysql_index_directory(), Some("/var/lib/mysql/index"));
    assert_eq!(
        table.mysql_connection(),
        Some("mysql://remote.example/db/flavor")
    );
    assert_eq!(
        table.mysql_union(),
        &["flavor_hot".to_string(), "flavor_cold".to_string()]
    );
    assert_eq!(table.mysql_partition_by(), Some("HASH (id)"));
    assert_eq!(table.mysql_partitions(), Some(4));
    assert_eq!(table.mysql_subpartition_by(), Some("KEY (supplier_id)"));
    assert_eq!(table.mysql_subpartitions(), Some(2));
    assert_eq!(table.mysql_auto_increment(), Some(101));
    assert_eq!(table.postgres_inherits(), &["base_flavor".to_string()]);
    assert_eq!(
        table.postgres_with(),
        &[("fillfactor".to_string(), "70".to_string())]
    );
    assert_eq!(table.postgres_using(), Some("heap"));
    assert!(table.is_postgres_unlogged());
    assert!(table.is_sqlite_strict());
    assert!(table.is_sqlite_without_rowid());
    assert_eq!(
        table.oracle_compress(),
        Some(&OracleTableCompression::Level(6))
    );
    assert_eq!(table.postgres_partition_by(), Some("RANGE (id)"));
    assert_eq!(table.qualified_name().to_string(), "inventory.flavor");
    assert_eq!(table.indexes().len(), 1);
    assert_eq!(table.unique_constraints().len(), 1);
    assert_eq!(table.relationships().len(), 1);
    assert_eq!(table.check_constraints().len(), 1);
    assert_eq!(table.foreign_keys().len(), 1);
    assert_eq!(table.exclusion_constraints().len(), 1);
}

#[test]
fn table_exposes_postgres_child_partition_metadata() {
    let table = TableDef::from_parts(
        "flavor_2026",
        "Flavor2026",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_partition_of("flavor")
    .with_postgres_partition_for("FOR VALUES FROM (2026) TO (2027)");

    assert_eq!(table.postgres_partition_of(), Some("flavor"));
    assert_eq!(
        table.postgres_partition_for(),
        Some("FOR VALUES FROM (2026) TO (2027)")
    );
}

#[test]
fn table_bulk_and_option_builders_expose_all_optional_metadata() {
    let options = MysqlTableOptions {
        engine: Some("InnoDB".to_string()),
        charset: Some("utf8mb4".to_string()),
        collation: Some("utf8mb4_bin".to_string()),
        row_format: Some("COMPRESSED".to_string()),
        key_block_size: Some(4),
        pack_keys: Some(false),
        checksum: Some(true),
        delay_key_write: Some(false),
        stats_persistent: Some(true),
        stats_auto_recalc: Some(true),
        stats_sample_pages: Some(8),
        avg_row_length: Some(128),
        max_rows: Some(10_000),
        min_rows: Some(100),
        insert_method: Some("FIRST".to_string()),
        data_directory: Some("/data".to_string()),
        index_directory: Some("/index".to_string()),
        connection: Some("mysql://remote/flavor".to_string()),
        union: vec!["archive".to_string()],
        partition_by: Some("HASH(id)".to_string()),
        partitions: Some(8),
        subpartition_by: Some("KEY(id)".to_string()),
        subpartitions: Some(4),
        auto_increment: Some(500),
    };

    let table = TableDef::new("flavor", "id", vec!["id".to_string()])
        .with_mysql_options(options)
        .with_postgres_using_option(Some("zheap".to_string()))
        .with_postgres_unlogged(false)
        .with_sqlite_strict(false)
        .with_sqlite_without_rowid(false)
        .with_oracle_compress()
        .with_oracle_compress_option(Some(OracleTableCompression::Enabled))
        .with_mssql_primary_key_nonclustered(true)
        .with_postgres_partition_by_option(Some("LIST(id)".to_string()))
        .with_postgres_partition_of_option(Some("parent".to_string()))
        .with_postgres_partition_for_option(Some("FOR VALUES IN (1)".to_string()));

    assert_eq!(table.mysql_engine(), Some("InnoDB"));
    assert_eq!(table.mysql_charset(), Some("utf8mb4"));
    assert_eq!(table.mysql_collation(), Some("utf8mb4_bin"));
    assert_eq!(table.mysql_row_format(), Some("COMPRESSED"));
    assert_eq!(table.mysql_key_block_size(), Some(4));
    assert_eq!(table.mysql_pack_keys(), Some(false));
    assert_eq!(table.mysql_checksum(), Some(true));
    assert_eq!(table.mysql_delay_key_write(), Some(false));
    assert_eq!(table.mysql_stats_persistent(), Some(true));
    assert_eq!(table.mysql_stats_auto_recalc(), Some(true));
    assert_eq!(table.mysql_stats_sample_pages(), Some(8));
    assert_eq!(table.mysql_avg_row_length(), Some(128));
    assert_eq!(table.mysql_max_rows(), Some(10_000));
    assert_eq!(table.mysql_min_rows(), Some(100));
    assert_eq!(table.mysql_insert_method(), Some("FIRST"));
    assert_eq!(table.mysql_data_directory(), Some("/data"));
    assert_eq!(table.mysql_index_directory(), Some("/index"));
    assert_eq!(table.mysql_connection(), Some("mysql://remote/flavor"));
    assert_eq!(table.mysql_union(), &["archive".to_string()]);
    assert_eq!(table.mysql_partition_by(), Some("HASH(id)"));
    assert_eq!(table.mysql_partitions(), Some(8));
    assert_eq!(table.mysql_subpartition_by(), Some("KEY(id)"));
    assert_eq!(table.mysql_subpartitions(), Some(4));
    assert_eq!(table.mysql_auto_increment(), Some(500));
    assert_eq!(table.postgres_using(), Some("zheap"));
    assert!(!table.is_postgres_unlogged());
    assert!(!table.is_sqlite_strict());
    assert!(!table.is_sqlite_without_rowid());
    assert_eq!(
        table.oracle_compress(),
        Some(&OracleTableCompression::Enabled)
    );
    assert!(table.is_mssql_primary_key_nonclustered());
    assert_eq!(table.postgres_partition_by(), Some("LIST(id)"));
    assert_eq!(table.postgres_partition_of(), Some("parent"));
    assert_eq!(table.postgres_partition_for(), Some("FOR VALUES IN (1)"));
}
