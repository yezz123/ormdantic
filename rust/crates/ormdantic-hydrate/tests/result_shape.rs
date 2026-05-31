use ormdantic_hydrate::{FlatHydrationPlan, ResultShape};
use ormdantic_schema::TableDef;

#[test]
fn flat_plan_tracks_primary_key_position() {
    let table = TableDef::new("flavors", "id", vec!["id".to_string(), "name".to_string()]);
    let plan = FlatHydrationPlan::new(
        table,
        &["flavors\\name".to_string(), "flavors\\id".to_string()],
    )
    .expect("flat plan should build");

    assert_eq!(plan.primary_key_index(), 1);
}

#[test]
fn result_shape_extracts_nested_relationship_paths() {
    let shape = ResultShape::new(
        "coffee",
        &[
            "coffee\\id".to_string(),
            "coffee/flavor\\id".to_string(),
            "coffee/flavor/roast\\id".to_string(),
        ],
        vec!["coffee/flavor/roast".to_string()],
    );

    assert_eq!(
        shape.relationship_paths(),
        &[
            "coffee/flavor".to_string(),
            "coffee/flavor/roast".to_string()
        ]
    );
    assert_eq!(shape.array_paths(), &["coffee/flavor/roast".to_string()]);
}
