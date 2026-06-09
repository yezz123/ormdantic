use ormdantic_schema::{NamespaceDef, SchemaDef, TableDef};

#[test]
fn namespace_def_tracks_comment() {
    let namespace = NamespaceDef::new("inventory").with_comment("warehouse schema");

    assert_eq!(namespace.name(), "inventory");
    assert_eq!(namespace.comment(), Some("warehouse schema"));
}

#[test]
fn schema_def_tracks_namespaces_and_tables() {
    let table = TableDef::new("flavor", "id", vec!["id".to_string()]);
    let schema = SchemaDef::from_tables(vec![table.clone()])
        .with_namespaces(vec![NamespaceDef::new("inventory")]);

    assert_eq!(SchemaDef::new().tables().len(), 0);
    assert_eq!(schema.namespaces()[0].name(), "inventory");
    assert_eq!(schema.tables(), &[table]);
    assert_eq!(schema.table("flavor").unwrap().primary_key(), "id");
    assert!(schema.table("missing").is_none());
}
