use ormdantic_schema::ColumnAlias;

#[test]
fn parses_column_aliases() {
    let alias = ColumnAlias::parse("flavors\\id").expect("alias should parse");

    assert_eq!(alias.column_for_table("flavors"), Some("id"));
    assert_eq!(alias.column_for_table("coffee"), None);
    assert_eq!(alias.table_path(), "flavors");
    assert_eq!(alias.column(), "id");
}

#[test]
fn rejects_aliases_without_separator() {
    assert!(ColumnAlias::parse("flavors.id").is_none());
}
