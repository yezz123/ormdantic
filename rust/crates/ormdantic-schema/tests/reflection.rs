use ormdantic_schema::{ColumnDef, FieldKind, NamespaceDef, ReflectedSchema, ReflectedTable};

#[test]
fn reflected_schema_converts_to_schema_def() {
    let schema = ReflectedSchema::new()
        .with_namespaces(vec![NamespaceDef::new("inventory")])
        .with_tables(vec![ReflectedTable::new(
            "flavors",
            "id",
            vec![ColumnDef::new("id", FieldKind::String).primary_key(true)],
        )
        .with_schema("inventory")])
        .into_schema_def();

    assert_eq!(schema.namespaces()[0].name(), "inventory");
    let table = schema.table("flavors").expect("table should be present");
    assert_eq!(table.schema(), Some("inventory"));
    assert_eq!(table.primary_key(), "id");
}
