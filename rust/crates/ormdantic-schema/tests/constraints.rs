use ormdantic_schema::{
    CheckConstraintDef, ConstraintDef, ConstraintTiming, ExclusionConstraintDef,
    ExclusionElementDef, ForeignKeyAction, ForeignKeyDef, ForeignKeyMatch, OracleIndexCompression,
    UniqueConstraintDef,
};

#[test]
fn unique_and_check_constraints_expose_names_and_expressions() {
    let unique = UniqueConstraintDef::new(
        "flavor_name_unique",
        vec!["tenant_id".to_string(), "name".to_string()],
    )
    .with_timing(ConstraintTiming::new(Some(true), true))
    .nulls_not_distinct()
    .with_mssql_filegroup("constraintspace")
    .with_mssql_clustered(false)
    .with_oracle_tablespace("constraintspace")
    .with_oracle_compress_prefix(2);
    let check = CheckConstraintDef::new("rating >= 0")
        .named("rating_check")
        .no_inherit();

    assert_eq!(unique.name(), "flavor_name_unique");
    assert_eq!(
        unique.columns(),
        &["tenant_id".to_string(), "name".to_string()]
    );
    assert_eq!(unique.timing().deferrable(), Some(true));
    assert!(unique.timing().initially_deferred());
    assert!(unique.is_nulls_not_distinct());
    assert_eq!(unique.mssql_filegroup(), Some("constraintspace"));
    assert_eq!(unique.mssql_clustered(), Some(false));
    assert_eq!(unique.oracle_tablespace(), Some("constraintspace"));
    assert_eq!(
        unique.oracle_compress(),
        Some(&OracleIndexCompression::Prefix(2))
    );
    assert_eq!(check.name(), Some("rating_check"));
    assert_eq!(check.expression(), "rating >= 0");
    assert!(check.is_validated());
    assert!(check.is_no_inherit());
    assert!(!CheckConstraintDef::new("rating >= 0")
        .named("rating_check")
        .not_validated()
        .is_validated());
    assert!(matches!(
        ConstraintDef::Unique(unique),
        ConstraintDef::Unique(_)
    ));
    assert!(matches!(
        ConstraintDef::Check(check),
        ConstraintDef::Check(_)
    ));
}

#[test]
fn exclusion_constraint_builder_exposes_elements_and_timing() {
    let exclusion = ExclusionConstraintDef::new(
        "booking_room_overlap",
        vec![
            ExclusionElementDef::column("room_id", "=").opclass("gist_int4_ops"),
            ExclusionElementDef::expression("tsrange(starts_at, ends_at)", "&&"),
        ],
    )
    .method("gist")
    .where_expr("cancelled = false")
    .with_timing(ConstraintTiming::new(Some(true), true));

    assert_eq!(exclusion.name(), "booking_room_overlap");
    assert_eq!(exclusion.method_name(), "gist");
    assert_eq!(exclusion.predicate(), Some("cancelled = false"));
    assert_eq!(exclusion.elements().len(), 2);
    assert!(exclusion.elements()[0].is_quoted());
    assert_eq!(
        exclusion.elements()[0].operator_class(),
        Some("gist_int4_ops")
    );
    assert!(!exclusion.elements()[1].is_quoted());
    assert_eq!(exclusion.timing().deferrable(), Some(true));
    assert!(exclusion.timing().initially_deferred());
    assert!(matches!(
        ConstraintDef::Exclusion(exclusion),
        ConstraintDef::Exclusion(_)
    ));
}

#[test]
fn foreign_key_builder_exposes_actions() {
    let foreign_key = ForeignKeyDef::new(
        vec!["supplier_id".to_string()],
        "supplier",
        vec!["id".to_string()],
    )
    .named("flavor_supplier_fk")
    .on_delete(ForeignKeyAction::SetNull)
    .on_update(ForeignKeyAction::Cascade)
    .with_timing(ConstraintTiming::new(Some(false), false))
    .with_match(ForeignKeyMatch::Full);

    assert_eq!(foreign_key.name(), Some("flavor_supplier_fk"));
    assert_eq!(foreign_key.local_columns(), &["supplier_id".to_string()]);
    assert_eq!(foreign_key.remote_table(), "supplier");
    assert_eq!(foreign_key.remote_columns(), &["id".to_string()]);
    assert_eq!(
        foreign_key.on_delete_action(),
        Some(&ForeignKeyAction::SetNull)
    );
    assert_eq!(
        foreign_key.on_update_action(),
        Some(&ForeignKeyAction::Cascade)
    );
    assert_eq!(foreign_key.timing().deferrable(), Some(false));
    assert!(!foreign_key.timing().initially_deferred());
    assert_eq!(foreign_key.match_type(), Some(&ForeignKeyMatch::Full));
    assert!(foreign_key.is_validated());
    assert!(!ForeignKeyDef::new(
        vec!["supplier_id".to_string()],
        "supplier",
        vec!["id".to_string()],
    )
    .named("flavor_supplier_fk")
    .not_validated()
    .is_validated());
    assert!(matches!(
        ConstraintDef::ForeignKey(foreign_key),
        ConstraintDef::ForeignKey(_)
    ));
}
