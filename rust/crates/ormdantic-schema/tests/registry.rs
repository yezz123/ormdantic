use ormdantic_schema::{
    ColumnDef, FieldKind, RelationshipCardinality, RelationshipDef, SchemaRegistry, TableDef,
};

#[test]
fn registry_registers_tables() {
    let mut registry = SchemaRegistry::new();

    let table_id = registry
        .register_table(TableDef::new(
            "flavors",
            "id",
            vec!["id".to_string(), "name".to_string()],
        ))
        .expect("table should register");

    assert_eq!(table_id.0, 0);
    assert_eq!(registry.get_table("flavors").unwrap().name(), "flavors");
}

#[test]
fn registry_rejects_duplicate_tables() {
    let mut registry = SchemaRegistry::new();

    registry
        .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
        .expect("first table should register");
    let error = registry
        .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
        .expect_err("duplicate table should fail");

    assert_eq!(error.to_string(), "table 'flavors' is already registered");
}

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

#[test]
fn registry_rejects_missing_primary_key() {
    let mut registry = SchemaRegistry::new();

    let error = registry
        .register_table(TableDef::new("flavors", "id", vec!["name".to_string()]))
        .expect_err("missing pk should fail");

    assert_eq!(
        error.to_string(),
        "table 'flavors' does not define primary key column 'id'"
    );
}

#[test]
fn registry_rejects_unknown_relationship_target() {
    let mut registry = SchemaRegistry::new();
    registry
        .register_table(TableDef::from_parts(
            "coffee",
            "Coffee",
            "id",
            vec![ColumnDef::new("id", FieldKind::Uuid)],
            Vec::new(),
            Vec::new(),
            vec![RelationshipDef::new(
                "flavor",
                "flavors",
                "id",
                RelationshipCardinality::One,
            )],
        ))
        .expect("source should register");

    let error = registry
        .validate_relationships()
        .expect_err("relationship target should fail");

    assert_eq!(
        error.to_string(),
        "relationship 'coffee.flavor' targets unknown table 'flavors'"
    );
}
