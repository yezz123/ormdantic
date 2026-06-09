use ormdantic_schema::{ColumnDef, ColumnDefault, ComputedDef, FieldKind, IdentityDef};

#[test]
fn column_builder_exposes_all_options() {
    let identity = IdentityDef::new().start(10).increment(5).always(true);
    let computed = ComputedDef::new("price * quantity").persisted(true);
    let column = ColumnDef::new("total", FieldKind::Decimal)
        .nullable(true)
        .primary_key(true)
        .default(ColumnDefault::Expression("0".to_string()))
        .with_server_default("0")
        .with_identity(identity.clone())
        .with_computed(computed.clone())
        .autoincrement(true)
        .with_collation("NOCASE")
        .with_comment("computed total")
        .numeric(12, 2);

    assert_eq!(column.name(), "total");
    assert_eq!(column.kind(), &FieldKind::Decimal);
    assert!(column.is_nullable());
    assert!(column.is_primary_key());
    assert_eq!(
        column.default_value(),
        Some(&ColumnDefault::Expression("0".to_string()))
    );
    assert_eq!(column.server_default(), Some("0"));
    assert_eq!(column.identity(), Some(&identity));
    assert_eq!(column.computed(), Some(&computed));
    assert!(column.is_autoincrement());
    assert_eq!(column.collation(), Some("NOCASE"));
    assert_eq!(column.comment(), Some("computed total"));
    assert_eq!(column.precision(), Some(12));
    assert_eq!(column.scale(), Some(2));
}

#[test]
fn identity_and_computed_defaults_are_minimal() {
    let identity = IdentityDef::default();
    let computed = ComputedDef::new("lower(name)");

    assert_eq!(identity.start_value(), None);
    assert_eq!(identity.increment_value(), None);
    assert!(!identity.is_always());
    assert_eq!(computed.expression(), "lower(name)");
    assert!(!computed.is_persisted());
}
