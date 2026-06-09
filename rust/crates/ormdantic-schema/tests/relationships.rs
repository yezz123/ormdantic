use ormdantic_schema::{
    CascadeAction, LoaderStrategy, RelationshipCardinality, RelationshipDef, RelationshipDirection,
};

#[test]
fn relationship_defaults_follow_cardinality() {
    let many = RelationshipDef::new(
        "flavors",
        "flavor",
        "coffee_id",
        RelationshipCardinality::Many,
    );
    let one = RelationshipDef::new("owner", "user", "id", RelationshipCardinality::One);

    assert_eq!(many.direction(), &RelationshipDirection::OneToMany);
    assert!(many.is_uselist());
    assert_eq!(one.direction(), &RelationshipDirection::ManyToOne);
    assert!(!one.is_uselist());
}

#[test]
fn relationship_builder_exposes_mapping_options() {
    let relationship = RelationshipDef::new(
        "flavors",
        "flavor",
        "coffee_id",
        RelationshipCardinality::Many,
    )
    .with_back_reference("coffee")
    .with_columns(vec!["id".to_string()], vec!["coffee_id".to_string()])
    .with_direction(RelationshipDirection::OneToMany)
    .uselist(true)
    .nullable(false)
    .secondary_table("coffee_flavor")
    .cascade(vec![CascadeAction::Delete, CascadeAction::DeleteOrphan])
    .loader_strategy(LoaderStrategy::SelectIn);

    assert_eq!(relationship.field(), "flavors");
    assert_eq!(relationship.target_table(), "flavor");
    assert_eq!(relationship.target_field(), "coffee_id");
    assert_eq!(relationship.cardinality(), &RelationshipCardinality::Many);
    assert_eq!(relationship.back_reference(), Some("coffee"));
    assert_eq!(relationship.local_columns(), &["id".to_string()]);
    assert_eq!(relationship.remote_columns(), &["coffee_id".to_string()]);
    assert_eq!(relationship.direction(), &RelationshipDirection::OneToMany);
    assert!(relationship.is_uselist());
    assert!(!relationship.is_nullable());
    assert_eq!(relationship.secondary_table_name(), Some("coffee_flavor"));
    assert_eq!(
        relationship.cascade_actions(),
        &[CascadeAction::Delete, CascadeAction::DeleteOrphan]
    );
    assert_eq!(
        relationship.loader_strategy_ref(),
        &LoaderStrategy::SelectIn
    );
}
