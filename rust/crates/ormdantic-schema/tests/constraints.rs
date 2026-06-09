use ormdantic_schema::{
    CheckConstraintDef, ConstraintDef, ForeignKeyAction, ForeignKeyDef, UniqueConstraintDef,
};

#[test]
fn unique_and_check_constraints_expose_names_and_expressions() {
    let unique = UniqueConstraintDef::new(
        "flavor_name_unique",
        vec!["tenant_id".to_string(), "name".to_string()],
    );
    let check = CheckConstraintDef::new("rating >= 0").named("rating_check");

    assert_eq!(unique.name(), "flavor_name_unique");
    assert_eq!(
        unique.columns(),
        &["tenant_id".to_string(), "name".to_string()]
    );
    assert_eq!(check.name(), Some("rating_check"));
    assert_eq!(check.expression(), "rating >= 0");
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
fn foreign_key_builder_exposes_actions() {
    let foreign_key = ForeignKeyDef::new(
        vec!["supplier_id".to_string()],
        "supplier",
        vec!["id".to_string()],
    )
    .named("flavor_supplier_fk")
    .on_delete(ForeignKeyAction::SetNull)
    .on_update(ForeignKeyAction::Cascade);

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
    assert!(matches!(
        ConstraintDef::ForeignKey(foreign_key),
        ConstraintDef::ForeignKey(_)
    ));
}
