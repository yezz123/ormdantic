use ormdantic_hydrate::FlatHydrationPlan;
use ormdantic_schema::TableDef;

#[test]
fn builds_flat_hydration_plan() {
    let table = TableDef::new("flavors", "id", vec!["id".to_string(), "name".to_string()]);

    let plan = FlatHydrationPlan::new(
        table,
        &["flavors\\id".to_string(), "flavors\\name".to_string()],
    )
    .expect("plan should build");

    assert_eq!(plan.table().name(), "flavors");
    assert_eq!(plan.primary_key_index(), 0);
    assert_eq!(
        plan.parsed_columns(),
        &[Some("id".to_string()), Some("name".to_string())]
    );
}

#[test]
fn tracks_only_columns_for_the_plan_table() {
    let table = TableDef::new("flavors", "id", vec!["id".to_string(), "name".to_string()]);

    let plan = FlatHydrationPlan::new(
        table,
        &[
            "flavors\\id".to_string(),
            "suppliers\\name".to_string(),
            "flavors\\name".to_string(),
        ],
    )
    .expect("plan should build");

    assert_eq!(
        plan.parsed_columns(),
        &[Some("id".to_string()), None, Some("name".to_string())]
    );
}

#[test]
fn errors_when_primary_key_alias_is_missing() {
    let table = TableDef::new("flavors", "id", vec!["id".to_string()]);

    let error = FlatHydrationPlan::new(table, &["flavors\\name".to_string()])
        .expect_err("missing primary key should error");

    assert_eq!(
        error.to_string(),
        "primary key column 'flavors\\id' was not found"
    );
}
