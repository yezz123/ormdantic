use ormdantic_schema::IndexDef;

#[test]
fn index_builder_exposes_predicate_include_and_method() {
    let index = IndexDef::new("flavor_name_idx", vec!["name".to_string()])
        .unique(true)
        .where_expr("name IS NOT NULL")
        .include_columns(vec!["rating".to_string()])
        .method("btree");

    assert_eq!(index.name(), "flavor_name_idx");
    assert_eq!(index.columns(), &["name".to_string()]);
    assert!(index.is_unique());
    assert_eq!(index.predicate(), Some("name IS NOT NULL"));
    assert_eq!(index.include_columns_ref(), &["rating".to_string()]);
    assert_eq!(index.method_name(), Some("btree"));
}
