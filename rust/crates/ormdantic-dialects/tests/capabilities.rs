use ormdantic_core::BackendFeature;
use ormdantic_dialects::{
    Dialect, MariaDbDialect, MsSqlDialect, MySqlDialect, OracleDialect, PostgresDialect,
    SqliteDialect,
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
        PostgresDialect.render_column_type(&ColumnDef::new("name", FieldKind::String)),
        "TEXT"
    );
    assert_eq!(
        SqliteDialect.render_column_type(&ColumnDef::new("name", FieldKind::String)),
        "TEXT"
    );
    assert_eq!(
        MySqlDialect.render_column_type(&ColumnDef::new("name", FieldKind::String)),
        "VARCHAR(255)"
    );
    assert_eq!(
        MariaDbDialect.render_column_type(&ColumnDef::new("name", FieldKind::String)),
        "VARCHAR(255)"
    );
    assert_eq!(
        MsSqlDialect.render_column_type(&ColumnDef::new("name", FieldKind::String)),
        "NVARCHAR(255)"
    );
    assert_eq!(
        OracleDialect.render_column_type(&ColumnDef::new("name", FieldKind::String)),
        "VARCHAR2(255)"
    );
    assert_eq!(
        PostgresDialect
            .render_column_type(&ColumnDef::new("name", FieldKind::String).with_max_length(255)),
        "VARCHAR(255)"
    );
    assert_eq!(
        MySqlDialect
            .render_column_type(&ColumnDef::new("name", FieldKind::String).with_max_length(255)),
        "VARCHAR(255)"
    );
    assert_eq!(
        MsSqlDialect
            .render_column_type(&ColumnDef::new("name", FieldKind::String).with_max_length(255)),
        "NVARCHAR(255)"
    );
    assert_eq!(
        OracleDialect
            .render_column_type(&ColumnDef::new("name", FieldKind::String).with_max_length(255)),
        "VARCHAR2(255)"
    );
    assert_eq!(
        PostgresDialect
            .render_column_type(&ColumnDef::new("amount", FieldKind::Decimal).numeric(9, 2)),
        "NUMERIC(9, 2)"
    );
    assert_eq!(
        SqliteDialect
            .render_column_type(&ColumnDef::new("amount", FieldKind::Decimal).numeric(9, 2)),
        "DECIMAL_TEXT(9, 2)"
    );
    assert_eq!(
        PostgresDialect.render_column_type(&ColumnDef::new(
            "flavor",
            FieldKind::Enum {
                name: Some("ddl_flavor".to_string()),
                schema: None
            }
        )),
        r#""ddl_flavor""#
    );
    assert_eq!(
        PostgresDialect.render_column_type(&ColumnDef::new(
            "flavor",
            FieldKind::Enum {
                name: Some("ddl_flavor".to_string()),
                schema: Some("inventory".to_string())
            }
        )),
        r#""inventory"."ddl_flavor""#
    );
    assert_eq!(
        SqliteDialect.render_column_type(&ColumnDef::new(
            "flavor",
            FieldKind::Enum {
                name: Some("ddl_flavor".to_string()),
                schema: None
            }
        )),
        "TEXT"
    );
    assert_eq!(
        PostgresDialect.render_column_type(&ColumnDef::new(
            "legacy_flavor",
            FieldKind::Enum {
                name: None,
                schema: None
            }
        )),
        "TEXT"
    );
}
