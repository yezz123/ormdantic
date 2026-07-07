use ormdantic_schema::{ColumnDef, ColumnDefault, ComputedDef, FieldKind, IdentityDef};

#[test]
fn column_builder_exposes_all_options() {
    let identity = IdentityDef::new()
        .start(10)
        .increment(5)
        .min_value(1)
        .max_value(1000)
        .no_min_value(false)
        .no_max_value(false)
        .cycle(true)
        .cache(20)
        .order(true)
        .on_null(true)
        .always(true);
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
        .numeric(12, 2)
        .with_max_length(255)
        .with_sqlite_on_conflict_primary_key("REPLACE")
        .with_sqlite_on_conflict_not_null("FAIL")
        .with_sqlite_on_conflict_unique("IGNORE");

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
    assert_eq!(identity.minimum_value(), Some(1));
    assert_eq!(identity.maximum_value(), Some(1000));
    assert!(!identity.is_no_min_value());
    assert!(!identity.is_no_max_value());
    assert!(identity.is_cycle());
    assert_eq!(identity.cache_value(), Some(20));
    assert!(identity.is_ordered());
    assert!(identity.is_on_null());
    assert_eq!(column.computed(), Some(&computed));
    assert!(column.is_autoincrement());
    assert_eq!(column.collation(), Some("NOCASE"));
    assert_eq!(column.comment(), Some("computed total"));
    assert_eq!(column.precision(), Some(12));
    assert_eq!(column.scale(), Some(2));
    assert_eq!(column.max_length(), Some(255));
    assert_eq!(column.sqlite_on_conflict_primary_key(), Some("REPLACE"));
    assert_eq!(column.sqlite_on_conflict_not_null(), Some("FAIL"));
    assert_eq!(column.sqlite_on_conflict_unique(), Some("IGNORE"));
}

#[test]
fn identity_and_computed_defaults_are_minimal() {
    let identity = IdentityDef::default();
    let computed = ComputedDef::new("lower(name)");

    assert_eq!(identity.start_value(), None);
    assert_eq!(identity.increment_value(), None);
    assert_eq!(identity.minimum_value(), None);
    assert_eq!(identity.maximum_value(), None);
    assert!(!identity.is_no_min_value());
    assert!(!identity.is_no_max_value());
    assert_eq!(identity.cache_value(), None);
    assert!(!identity.is_always());
    assert!(!identity.is_cycle());
    assert!(!identity.is_ordered());
    assert!(!identity.is_on_null());
    assert_eq!(computed.expression(), "lower(name)");
    assert!(!computed.is_persisted());
}

#[test]
fn identity_builder_exposes_no_bound_flags() {
    let identity = IdentityDef::new().no_min_value(true).no_max_value(true);

    assert!(identity.is_no_min_value());
    assert!(identity.is_no_max_value());
    assert_eq!(identity.minimum_value(), None);
    assert_eq!(identity.maximum_value(), None);
}

#[test]
fn column_builder_accepts_sqlite_conflict_options() {
    let column = ColumnDef::new("name", FieldKind::String)
        .with_sqlite_on_conflict_primary_key_option(Some("ROLLBACK".to_string()))
        .with_sqlite_on_conflict_not_null_option(Some("ABORT".to_string()))
        .with_sqlite_on_conflict_unique_option(Some("FAIL".to_string()));

    assert_eq!(column.sqlite_on_conflict_primary_key(), Some("ROLLBACK"));
    assert_eq!(column.sqlite_on_conflict_not_null(), Some("ABORT"));
    assert_eq!(column.sqlite_on_conflict_unique(), Some("FAIL"));

    let cleared = column
        .with_sqlite_on_conflict_primary_key_option(None)
        .with_sqlite_on_conflict_not_null_option(None)
        .with_sqlite_on_conflict_unique_option(None);

    assert_eq!(cleared.sqlite_on_conflict_primary_key(), None);
    assert_eq!(cleared.sqlite_on_conflict_not_null(), None);
    assert_eq!(cleared.sqlite_on_conflict_unique(), None);
}
