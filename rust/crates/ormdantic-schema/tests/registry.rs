use ormdantic_schema::{
    ColumnDef, ExclusionConstraintDef, ExclusionElementDef, FieldKind, ForeignKeyDef, IndexDef,
    RelationshipCardinality, RelationshipDef, SchemaRegistry, TableDef, UniqueConstraintDef,
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
    assert_eq!(registry.tables().len(), 1);
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

#[test]
fn registry_rejects_relationships_to_unknown_target_columns() {
    let mut registry = SchemaRegistry::new();
    registry
        .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
        .expect("target should register");
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
                "missing_id",
                RelationshipCardinality::One,
            )],
        ))
        .expect("source should register");

    let error = registry
        .validate_relationships()
        .expect_err("relationship target column should fail");

    assert_eq!(
        error.to_string(),
        "relationship 'coffee.flavor' targets unknown table 'flavors'"
    );
}

#[test]
fn registry_rejects_invalid_index_shapes_and_column_references() {
    assert_eq!(
        register_error(TableDef::from_parts(
            "flavors",
            "Flavor",
            "id",
            base_columns(),
            vec![IndexDef::new("flavor_empty_idx", Vec::new())],
            Vec::new(),
            Vec::new(),
        )),
        "index 'flavor_empty_idx' on table 'flavors' must reference at least one column or expression"
    );
    assert_eq!(
        register_error(TableDef::from_parts(
            "flavors",
            "Flavor",
            "id",
            base_columns(),
            vec![IndexDef::new(
                "flavor_missing_idx",
                vec!["missing".to_string()]
            )],
            Vec::new(),
            Vec::new(),
        )),
        "index 'flavor_missing_idx' on table 'flavors' references unknown column 'missing'"
    );
    assert_eq!(
        register_error(TableDef::from_parts(
            "flavors",
            "Flavor",
            "id",
            base_columns(),
            vec![
                IndexDef::new("flavor_include_idx", vec!["name".to_string()])
                    .include_columns(vec!["missing".to_string()])
            ],
            Vec::new(),
            Vec::new(),
        )),
        "index 'flavor_include_idx' on table 'flavors' references unknown column 'missing'"
    );
}

#[test]
fn registry_rejects_constraint_column_references() {
    assert_eq!(
        register_error(TableDef::from_parts(
            "flavors",
            "Flavor",
            "id",
            base_columns(),
            Vec::new(),
            vec![UniqueConstraintDef::new(
                "flavor_missing_unique",
                vec!["missing".to_string()],
            )],
            Vec::new(),
        )),
        "unique constraint 'flavor_missing_unique' on table 'flavors' references unknown column 'missing'"
    );
    assert_eq!(
        register_error(
            TableDef::new("flavors", "id", vec!["id".to_string(), "name".to_string()])
                .with_foreign_keys(vec![ForeignKeyDef::new(
                    vec!["missing".to_string()],
                    "suppliers",
                    vec!["id".to_string()],
                )])
        ),
        "foreign key 'foreign_key' on table 'flavors' references unknown column 'missing'"
    );
    assert_eq!(
        register_error(
            TableDef::new("flavors", "id", vec!["id".to_string(), "name".to_string()])
                .with_exclusion_constraints(vec![ExclusionConstraintDef::new(
                    "flavor_overlap",
                    vec![ExclusionElementDef::column("missing", "=")],
                )])
        ),
        "exclusion constraint 'flavor_overlap' on table 'flavors' references unknown column 'missing'"
    );
}

#[test]
fn registry_accepts_expression_only_indexes_and_exclusions() {
    let mut registry = SchemaRegistry::new();
    registry
        .register_table(
            TableDef::from_parts(
                "flavors",
                "Flavor",
                "id",
                base_columns(),
                vec![IndexDef::new("flavor_lower_idx", Vec::new())
                    .expressions(vec!["LOWER(name)".to_string()])],
                Vec::new(),
                Vec::new(),
            )
            .with_exclusion_constraints(vec![ExclusionConstraintDef::new(
                "flavor_expr_exclude",
                vec![ExclusionElementDef::expression("lower(name)", "=")],
            )]),
        )
        .expect("expression-only index and exclusion should register");
}

fn base_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("id", FieldKind::Uuid),
        ColumnDef::new("name", FieldKind::String),
    ]
}

fn register_error(table: TableDef) -> String {
    let mut registry = SchemaRegistry::new();
    registry
        .register_table(table)
        .expect_err("table registration should fail")
        .to_string()
}
