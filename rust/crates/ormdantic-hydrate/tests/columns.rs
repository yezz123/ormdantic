use ormdantic_hydrate::{ResultColumn, ResultShape};

#[test]
fn parses_result_columns() {
    let column = ResultColumn::parse("coffee/flavor\\name").expect("column should parse");

    assert_eq!(column.alias(), "coffee/flavor\\name");
    assert_eq!(column.table_path(), "coffee/flavor");
    assert_eq!(column.column(), "name");
}

#[test]
fn rejects_invalid_result_column_aliases() {
    assert!(ResultColumn::parse("coffee/flavor/name").is_none());
    assert!(ResultColumn::parse("name").is_none());
}

#[test]
fn result_shape_extracts_relationship_paths_and_columns() {
    let shape = ResultShape::new(
        "coffee",
        &[
            "coffee\\id".to_string(),
            "coffee/flavor\\id".to_string(),
            "coffee/flavor\\name".to_string(),
            "coffee/flavor/roast\\id".to_string(),
            "invalid".to_string(),
        ],
        vec!["coffee/flavor".to_string()],
    );

    assert_eq!(shape.root_table(), "coffee");
    assert_eq!(shape.columns().len(), 4);
    assert_eq!(
        shape.relationship_paths(),
        &[
            "coffee/flavor".to_string(),
            "coffee/flavor/roast".to_string(),
        ]
    );
    assert_eq!(shape.array_paths(), &["coffee/flavor".to_string()]);
}
