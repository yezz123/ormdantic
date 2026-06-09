use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, ConstraintDef, FieldKind, SchemaDef, SchemaDiffer,
    SchemaOperation, SchemaSnapshot, TableDef, UniqueConstraintDef,
};

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
    ]);
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
    ]);
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
}
