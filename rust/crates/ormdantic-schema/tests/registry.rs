use ormdantic_schema::{
    ColumnDef, FieldKind, RelationshipCardinality, RelationshipDef, SchemaRegistry, TableDef,
};

#[test]
fn registry_validates_related_tables() {
    let mut registry = SchemaRegistry::new();
    registry
        .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
        .expect("flavors table should register");
    registry
        .register_table(TableDef::from_parts(
            "coffee",
            "Coffee",
            "id",
            vec![
                ColumnDef::new("id", FieldKind::Uuid),
                ColumnDef::new(
                    "flavor",
                    FieldKind::ForeignKey {
                        target_table: "flavors".to_string(),
                    },
                ),
            ],
            Vec::new(),
            Vec::new(),
            vec![RelationshipDef::new(
                "flavor",
                "flavors",
                "id",
                RelationshipCardinality::One,
            )],
        ))
        .expect("coffee table should register");

    registry
        .validate_relationships()
        .expect("relationships should validate");
}

#[test]
fn registry_rejects_duplicate_columns() {
    let mut registry = SchemaRegistry::new();
    let error = registry
        .register_table(TableDef::new(
            "flavors",
            "id",
            vec!["id".to_string(), "id".to_string()],
        ))
        .expect_err("duplicate columns should fail");

    assert_eq!(
        error.to_string(),
        "column 'flavors.id' is already registered"
    );
}
