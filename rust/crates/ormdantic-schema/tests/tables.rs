use ormdantic_core::TableId;
use ormdantic_schema::{
    CheckConstraintDef, ColumnDef, FieldKind, ForeignKeyDef, IndexDef, RelationshipCardinality,
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
    .with_check_constraints(vec![
        CheckConstraintDef::new("supplier_id > 0").named("supplier_check")
    ])
    .with_foreign_keys(vec![ForeignKeyDef::new(
        vec!["supplier_id".to_string()],
        "supplier",
        vec!["id".to_string()],
    )]);
    table.set_id(TableId(7));

    assert_eq!(table.id(), Some(TableId(7)));
    assert_eq!(table.model_key(), "Flavor");
    assert_eq!(table.schema(), Some("inventory"));
    assert_eq!(table.comment(), Some("flavor table"));
    assert_eq!(table.qualified_name().to_string(), "inventory.flavor");
    assert_eq!(table.indexes().len(), 1);
    assert_eq!(table.unique_constraints().len(), 1);
    assert_eq!(table.relationships().len(), 1);
    assert_eq!(table.check_constraints().len(), 1);
    assert_eq!(table.foreign_keys().len(), 1);
}
