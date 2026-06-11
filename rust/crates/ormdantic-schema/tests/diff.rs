use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ConstraintDef, ExclusionConstraintDef, ExclusionElementDef,
    FieldKind, IndexDef, NamespaceDef, SchemaDef, SchemaDiffer, SchemaOperation, SchemaSnapshot,
    TableDef, UniqueConstraintDef,
};

#[test]
fn schema_differ_orders_namespace_changes_around_tables() {
    let before_table = TableDef::new("old_flavor", "id", vec!["id".to_string()]);
    let after_table = TableDef::new("flavor", "id", vec!["id".to_string()]);
    let before = SchemaDef::from_tables(vec![before_table])
        .with_namespaces(vec![NamespaceDef::new("legacy")]);
    let after = SchemaDef::from_tables(vec![after_table])
        .with_namespaces(vec![NamespaceDef::new("inventory")]);

    let diff = SchemaDiffer::diff(&SchemaSnapshot::new(before), &SchemaSnapshot::new(after))
        .expect("schema diff should compile");

    assert!(matches!(
        &diff.operations()[0],
        SchemaOperation::CreateNamespace(namespace) if namespace.name() == "inventory"
    ));
    assert!(diff
        .operations()
        .iter()
        .any(|operation| matches!(operation, SchemaOperation::CreateTable(table) if table.name() == "flavor")));
    assert!(matches!(
        diff.operations().last().unwrap(),
        SchemaOperation::DropNamespace { name } if name == "legacy"
    ));
}

#[test]
fn schema_differ_reports_namespace_comment_changes() {
    let before = SchemaDef::new().with_namespaces(vec![
        NamespaceDef::new("inventory").with_comment("old schema")
    ]);
    let after = SchemaDef::new().with_namespaces(vec![
        NamespaceDef::new("inventory").with_comment("warehouse schema")
    ]);

    let diff = SchemaDiffer::diff(&SchemaSnapshot::new(before), &SchemaSnapshot::new(after))
        .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 1);
    assert!(matches!(
        &diff.operations()[0],
        SchemaOperation::SetNamespaceComment { name, comment }
            if name == "inventory" && comment.as_deref() == Some("warehouse schema")
    ));
}

#[test]
fn schema_differ_reports_table_column_and_index_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String).nullable(true),
            ColumnDef::new("code", FieldKind::String),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::AddColumn { table, column }
                if table == "flavor" && column.name() == "code"
        )
    }));
    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::AlterColumn { table, column }
                if table == "flavor" && column.name() == "name"
        )
    }));
}

#[test]
fn schema_differ_reports_table_comment_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_comment("Flavor metadata");
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::SetTableComment { table, comment }
                if table == "flavor" && comment.as_deref() == Some("Flavor metadata")
        )
    }));
}

#[test]
fn schema_differ_reports_column_comment_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String).with_comment("Flavor name"),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 1);
    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::SetColumnComment {
                table,
                column,
            } if table == "flavor"
                && column.name() == "name"
                && column.comment() == Some("Flavor name")
        )
    }));
}

#[test]
fn schema_differ_reports_table_tablespace_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_tablespace("fastspace");
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::SetTableTablespace { table, tablespace }
                if table == "flavor" && tablespace.as_deref() == Some("fastspace")
        )
    }));
}

#[test]
fn schema_differ_reports_mysql_table_option_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_mysql_engine("InnoDB")
    .with_mysql_charset("utf8mb4")
    .with_mysql_collation("utf8mb4_unicode_ci")
    .with_mysql_row_format("DYNAMIC")
    .with_mysql_key_block_size(8)
    .with_mysql_pack_keys(true)
    .with_mysql_checksum(true)
    .with_mysql_delay_key_write(true)
    .with_mysql_stats_persistent(true)
    .with_mysql_stats_auto_recalc(true)
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
    .with_mysql_subpartition_by("KEY (id)")
    .with_mysql_subpartitions(2)
    .with_mysql_auto_increment(101);
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_mysql_engine("MyISAM")
    .with_mysql_charset("latin1")
    .with_mysql_collation("latin1_swedish_ci")
    .with_mysql_row_format("COMPACT")
    .with_mysql_key_block_size(4)
    .with_mysql_pack_keys(false)
    .with_mysql_checksum(false)
    .with_mysql_delay_key_write(false)
    .with_mysql_stats_persistent(false)
    .with_mysql_stats_auto_recalc(false)
    .with_mysql_stats_sample_pages(16)
    .with_mysql_avg_row_length(32)
    .with_mysql_max_rows(500)
    .with_mysql_min_rows(5)
    .with_mysql_insert_method("FIRST")
    .with_mysql_data_directory("/srv/mysql/data")
    .with_mysql_index_directory("/srv/mysql/index")
    .with_mysql_connection("mysql://remote.example/db/flavor_archive")
    .with_mysql_union(vec![
        "flavor_archive_hot".to_string(),
        "flavor_archive_cold".to_string(),
    ])
    .with_mysql_partition_by("LINEAR HASH (id)")
    .with_mysql_partitions(8)
    .with_mysql_subpartition_by("KEY (code)")
    .with_mysql_subpartitions(4)
    .with_mysql_auto_increment(202);
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::SetTableMysqlOptions {
                table,
                engine,
                charset,
                collation,
                row_format,
                key_block_size,
                pack_keys,
                checksum,
                delay_key_write,
                stats_persistent,
                stats_auto_recalc,
                stats_sample_pages,
                avg_row_length,
                max_rows,
                min_rows,
                insert_method,
                data_directory,
                index_directory,
                connection,
                union,
                partition_by,
                partitions,
                subpartition_by,
                subpartitions,
                auto_increment,
            } if table == "flavor"
                && engine.as_deref() == Some("MyISAM")
                && charset.as_deref() == Some("latin1")
                && collation.as_deref() == Some("latin1_swedish_ci")
                && row_format.as_deref() == Some("COMPACT")
                && *key_block_size == Some(4)
                && *pack_keys == Some(false)
                && *checksum == Some(false)
                && *delay_key_write == Some(false)
                && *stats_persistent == Some(false)
                && *stats_auto_recalc == Some(false)
                && *stats_sample_pages == Some(16)
                && *avg_row_length == Some(32)
                && *max_rows == Some(500)
                && *min_rows == Some(5)
                && insert_method.as_deref() == Some("FIRST")
                && data_directory.as_deref() == Some("/srv/mysql/data")
                && index_directory.as_deref() == Some("/srv/mysql/index")
                && connection.as_deref() == Some("mysql://remote.example/db/flavor_archive")
                && union == &vec![
                    "flavor_archive_hot".to_string(),
                    "flavor_archive_cold".to_string()
                ]
                && partition_by.as_deref() == Some("LINEAR HASH (id)")
                && *partitions == Some(8)
                && subpartition_by.as_deref() == Some("KEY (code)")
                && *subpartitions == Some(4)
                && *auto_increment == Some(202)
        )
    }));
}

#[test]
fn schema_differ_reports_postgres_inheritance_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_inherits(vec!["old_base".to_string(), "shared_base".to_string()]);
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_inherits(vec!["shared_base".to_string(), "base_flavor".to_string()]);
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::SetTablePostgresInherits { table, add, drop }
                if table == "flavor"
                    && add == &["base_flavor".to_string()]
                    && drop == &["old_base".to_string()]
        )
    }));
}

#[test]
fn schema_differ_reports_postgres_storage_parameter_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_with(vec![
        ("fillfactor".to_string(), "80".to_string()),
        ("autovacuum_enabled".to_string(), "true".to_string()),
    ]);
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_with(vec![
        ("fillfactor".to_string(), "70".to_string()),
        ("toast.autovacuum_enabled".to_string(), "false".to_string()),
    ]);
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::SetTablePostgresWith { table, set, reset }
                if table == "flavor"
                    && set == &[
                        ("fillfactor".to_string(), "70".to_string()),
                        ("toast.autovacuum_enabled".to_string(), "false".to_string()),
                    ]
                    && reset == &["autovacuum_enabled".to_string()]
        )
    }));
}

#[test]
fn schema_differ_reports_postgres_access_method_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_using("heap");
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_using("custom_heap");
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::SetTablePostgresUsing { table, using }
                if table == "flavor" && using.as_deref() == Some("custom_heap")
        )
    }));
}

#[test]
fn schema_differ_reports_postgres_unlogged_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .postgres_unlogged();
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::SetTablePostgresUnlogged { table, unlogged }
                if table == "flavor" && *unlogged
        )
    }));
}

#[test]
fn schema_differ_recreates_postgres_partition_key_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_partition_by("RANGE (id)");
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 1);
    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::RecreateTable(table)
                if table.name() == "flavor"
                    && table.postgres_partition_by() == Some("RANGE (id)")
        )
    }));
}

#[test]
fn schema_differ_recreates_changed_sqlite_table_options() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_sqlite_strict(true)
    .with_sqlite_without_rowid(true);
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 1);
    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::RecreateTable(table)
                if table.name() == "flavor"
                    && table.is_sqlite_strict()
                    && table.is_sqlite_without_rowid()
        )
    }));
}

#[test]
fn schema_differ_recreates_changed_oracle_table_compression() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_oracle_compress();
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 1);
    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::RecreateTable(table)
                if table.name() == "flavor" && table.oracle_compress().is_some()
        )
    }));
}

#[test]
fn schema_differ_attaches_postgres_partition_children() {
    let before = TableDef::from_parts(
        "flavor_2026",
        "Flavor2026",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
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
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 1);
    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::AttachPostgresPartition { table, parent, bound }
                if table == "flavor_2026"
                    && parent == "flavor"
                    && bound == "FOR VALUES FROM (2026) TO (2027)"
        )
    }));
}

#[test]
fn schema_differ_detaches_postgres_partition_children() {
    let before = TableDef::from_parts(
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
    let after = TableDef::from_parts(
        "flavor_2026",
        "Flavor2026",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 1);
    assert!(diff.operations().iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::DetachPostgresPartition { table, parent }
                if table == "flavor_2026" && parent == "flavor"
        )
    }));
}

#[test]
fn schema_differ_rebinds_postgres_partition_children() {
    let before = TableDef::from_parts(
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
    let after = TableDef::from_parts(
        "flavor_2026",
        "Flavor2026",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_partition_of("flavor")
    .with_postgres_partition_for("FOR VALUES FROM (2026) TO (2028)");
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 2);
    assert!(matches!(
        &diff.operations()[0],
        SchemaOperation::DetachPostgresPartition { table, parent }
            if table == "flavor_2026" && parent == "flavor"
    ));
    assert!(matches!(
        &diff.operations()[1],
        SchemaOperation::AttachPostgresPartition { table, parent, bound }
            if table == "flavor_2026"
                && parent == "flavor"
                && bound == "FOR VALUES FROM (2026) TO (2028)"
    ));
}

#[test]
fn schema_differ_rebinds_postgres_partition_children_to_new_parent() {
    let before = TableDef::from_parts(
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
    let after = TableDef::from_parts(
        "flavor_2026",
        "Flavor2026",
        "id",
        vec![ColumnDef::new("id", FieldKind::Integer).primary_key(true)],
        Vec::new(),
        Vec::new(),
        Vec::new(),
    )
    .with_postgres_partition_of("archived_flavor")
    .with_postgres_partition_for("FOR VALUES FROM (2026) TO (2027)");
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("schema diff should compile");

    assert_eq!(diff.operations().len(), 2);
    assert!(matches!(
        &diff.operations()[0],
        SchemaOperation::DetachPostgresPartition { table, parent }
            if table == "flavor_2026" && parent == "flavor"
    ));
    assert!(matches!(
        &diff.operations()[1],
        SchemaOperation::AttachPostgresPartition {
            table,
            parent,
            bound
        } if table == "flavor_2026"
            && parent == "archived_flavor"
            && bound == "FOR VALUES FROM (2026) TO (2027)"
    ));
}

#[test]
fn schema_differ_reports_named_constraint_changes() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String),
            ColumnDef::new("code", FieldKind::String),
        ],
        Vec::new(),
        vec![UniqueConstraintDef::new(
            "flavor_unique_0",
            vec!["name".to_string()],
        )],
        Vec::new(),
    )
    .with_check_constraints(vec![
        CheckConstraintDef::new("LENGTH(name) >= 2").named("flavor_name_check")
    ])
    .with_exclusion_constraints(vec![ExclusionConstraintDef::new(
        "flavor_code_exclusion",
        vec![ExclusionElementDef::column("name", "=")],
    )]);
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String),
            ColumnDef::new("code", FieldKind::String),
        ],
        Vec::new(),
        vec![UniqueConstraintDef::new(
            "flavor_unique_0",
            vec!["name".to_string(), "code".to_string()],
        )],
        Vec::new(),
    )
    .with_check_constraints(vec![
        CheckConstraintDef::new("LENGTH(code) >= 2").named("flavor_code_check")
    ])
    .with_exclusion_constraints(vec![ExclusionConstraintDef::new(
        "flavor_code_exclusion",
        vec![ExclusionElementDef::column("code", "=")],
    )]);
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("constraint diff should compile");
    let operations = diff.operations();

    assert!(operations.iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::DropConstraint { table, name }
                if table == "flavor" && name == "flavor_name_check"
        )
    }));
    assert!(operations.iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::AddConstraint {
                table,
                constraint: ConstraintDef::Check(constraint),
            } if table == "flavor" && constraint.name() == Some("flavor_code_check")
        )
    }));
    assert!(operations.iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::DropConstraint { table, name }
                if table == "flavor" && name == "flavor_unique_0"
        )
    }));
    assert!(operations.iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::AddConstraint {
                table,
                constraint: ConstraintDef::Unique(constraint),
            } if table == "flavor" && constraint.columns() == ["name", "code"]
        )
    }));
    assert!(operations.iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::AddConstraint {
                table,
                constraint: ConstraintDef::Exclusion(constraint),
            } if table == "flavor" && constraint.name() == "flavor_code_exclusion"
        )
    }));
}

#[test]
fn schema_differ_recreates_changed_indexes_with_same_name() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String),
            ColumnDef::new("rating", FieldKind::Integer),
        ],
        vec![IndexDef::new("flavor_name_idx", vec!["name".to_string()])],
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String),
            ColumnDef::new("rating", FieldKind::Integer),
        ],
        vec![IndexDef::new("flavor_name_idx", vec!["name".to_string()])
            .expressions(vec!["LOWER(name)".to_string()])
            .include_columns(vec!["rating".to_string()])
            .where_expr("name IS NOT NULL")],
        Vec::new(),
        Vec::new(),
    );
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("index diff should compile");
    let operations = diff.operations();

    assert!(operations.iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::DropIndex { table, name }
                if table == "flavor" && name == "flavor_name_idx"
        )
    }));
    assert!(operations.iter().any(|operation| {
        matches!(
            operation,
            SchemaOperation::CreateIndex { table, index }
                if table == "flavor"
                    && index.name() == "flavor_name_idx"
                    && index.expressions_ref() == ["LOWER(name)".to_string()]
                    && index.predicate() == Some("name IS NOT NULL")
        )
    }));
}

#[test]
fn schema_differ_treats_default_btree_index_method_as_omitted() {
    let before = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String),
        ],
        vec![IndexDef::new("flavor_name_idx", vec!["name".to_string()])],
        Vec::new(),
        Vec::new(),
    );
    let after = TableDef::from_parts(
        "flavor",
        "Flavor",
        "id",
        vec![
            ColumnDef::new("id", FieldKind::String).primary_key(true),
            ColumnDef::new("name", FieldKind::String),
        ],
        vec![IndexDef::new("flavor_name_idx", vec!["name".to_string()]).method("btree")],
        Vec::new(),
        Vec::new(),
    );
    let diff = SchemaDiffer::diff(
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![before])),
        &SchemaSnapshot::new(SchemaDef::from_tables(vec![after])),
    )
    .expect("index diff should compile");

    assert!(diff.operations().is_empty());
}
