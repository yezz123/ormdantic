use ormdantic_core::BackendFeature;
use ormdantic_dialects::{
    Dialect, MariaDbDialect, MsSqlDialect, MySqlDialect, PostgresDialect, SqliteDialect,
};
use ormdantic_schema::{ColumnDef, FieldKind};

#[test]
fn feature_sets_include_backend_capabilities() {
    assert!(PostgresDialect.supports_feature(BackendFeature::Returning));
    assert!(PostgresDialect.supports_feature(BackendFeature::NativeUuid));
    assert!(PostgresDialect.supports_feature(BackendFeature::NativeJson));

    assert!(MariaDbDialect.supports_feature(BackendFeature::Returning));
    assert!(!MySqlDialect.supports_feature(BackendFeature::Returning));
    assert!(!MsSqlDialect.supports_feature(BackendFeature::NativeJson));
    assert!(SqliteDialect.supports_feature(BackendFeature::Savepoints));
}

#[test]
fn column_types_follow_backend_capabilities() {
    assert_eq!(
        PostgresDialect.render_column_type(&ColumnDef::new("id", FieldKind::Uuid)),
        "UUID"
    );
    assert_eq!(
        SqliteDialect.render_column_type(&ColumnDef::new("id", FieldKind::Uuid)),
        "TEXT"
    );
    assert_eq!(
        MySqlDialect.render_column_type(&ColumnDef::new("payload", FieldKind::Json)),
        "JSON"
    );
    assert_eq!(
        MsSqlDialect.render_column_type(&ColumnDef::new("payload", FieldKind::Json)),
        "TEXT"
    );
    assert_eq!(
        PostgresDialect
            .render_column_type(&ColumnDef::new("amount", FieldKind::Decimal).numeric(9, 2)),
        "NUMERIC(9, 2)"
    );
}
