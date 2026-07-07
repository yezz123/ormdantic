use ormdantic_hydrate::{merge_selectin_results, HydratedRow, SelectInHydrationPlan};
use ormdantic_schema::{RelationshipCardinality, RelationshipDef};

fn row(values: &[(&str, &str)]) -> HydratedRow {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

fn plan(cardinality: RelationshipCardinality) -> SelectInHydrationPlan {
    SelectInHydrationPlan::new(
        vec!["id".to_string()],
        vec!["coffee_id".to_string()],
        RelationshipDef::new("flavors", "flavor", "id", cardinality),
    )
}

#[test]
fn parent_keys_are_deduplicated_and_skip_incomplete_rows() {
    let plan = plan(RelationshipCardinality::Many);
    assert_eq!(plan.parent_key_columns(), &["id".to_string()]);
    assert_eq!(plan.child_key_columns(), &["coffee_id".to_string()]);
    assert_eq!(plan.relationship().field(), "flavors");
    assert_eq!(plan.child_collection_key(), "flavors");
    let keys = plan.parent_keys(&[
        row(&[("id", "1")]),
        row(&[("id", "1")]),
        row(&[("id", "")]),
        row(&[("name", "missing")]),
        row(&[("id", "2")]),
    ]);

    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0].values(), &["1".to_string()]);
    assert_eq!(keys[1].values(), &["2".to_string()]);
}

#[test]
fn merges_selectin_collection_results_and_deduplicates_exact_child_rows() {
    let plan = plan(RelationshipCardinality::Many);
    let merged = merge_selectin_results(
        vec![row(&[("id", "1")]), row(&[("id", "2")])],
        vec![
            row(&[("coffee_id", "1"), ("id", "10"), ("name", "vanilla")]),
            row(&[("coffee_id", "1"), ("id", "10"), ("name", "vanilla")]),
            row(&[("coffee_id", "1"), ("id", "11"), ("name", "mocha")]),
            row(&[("coffee_id", "2"), ("id", "12"), ("name", "latte")]),
        ],
        &plan,
    );

    assert_eq!(
        merged[0].get("flavors").unwrap(),
        "coffee_id=1,id=10,name=vanilla;coffee_id=1,id=11,name=mocha"
    );
    assert_eq!(
        merged[1].get("flavors").unwrap(),
        "coffee_id=2,id=12,name=latte"
    );
}

#[test]
fn merges_selectin_scalar_results_into_collection_key() {
    let plan = SelectInHydrationPlan::new(
        vec!["id".to_string()],
        vec!["coffee_id".to_string()],
        RelationshipDef::new("profile", "profile", "id", RelationshipCardinality::One),
    );
    let merged = merge_selectin_results(
        vec![row(&[("id", "1")])],
        vec![row(&[("coffee_id", "1"), ("id", "10"), ("tier", "gold")])],
        &plan,
    );

    assert_eq!(
        merged[0].get("profile").unwrap(),
        "coffee_id=1,id=10,tier=gold"
    );
}

#[test]
fn merge_selectin_results_skips_children_and_parents_with_missing_keys() {
    let plan = plan(RelationshipCardinality::Many);
    let merged = merge_selectin_results(
        vec![row(&[("name", "missing")]), row(&[("id", "1")])],
        vec![
            row(&[("id", "10"), ("name", "orphan")]),
            row(&[("coffee_id", "1"), ("id", "11"), ("name", "mocha")]),
        ],
        &plan,
    );

    assert!(merged[0].get("flavors").is_none());
    assert_eq!(
        merged[1].get("flavors").unwrap(),
        "coffee_id=1,id=11,name=mocha"
    );
}
