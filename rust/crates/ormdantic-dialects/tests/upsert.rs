use ormdantic_dialects::{
    Dialect, MariaDbDialect, MsSqlDialect, MySqlDialect, PostgresDialect, SqliteDialect,
};

#[test]
fn renders_upsert_conflict_clauses_by_backend() {
    assert_eq!(
        SqliteDialect.upsert_conflict_clause("id", &["name".to_string()]),
        r#"ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name""#
    );
    assert_eq!(
        PostgresDialect.upsert_conflict_clause("id", &[]),
        r#"ON CONFLICT ("id") DO NOTHING"#
    );
    assert_eq!(
        MySqlDialect.upsert_conflict_clause("id", &["name".to_string()]),
        "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
    );
    assert_eq!(
        MariaDbDialect.upsert_conflict_clause("id", &["name".to_string()]),
        "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
    );
    assert_eq!(
        MySqlDialect.upsert_conflict_clause("id", &[]),
        "ON DUPLICATE KEY UPDATE 1 = 1"
    );
    assert_eq!(
        MsSqlDialect.upsert_conflict_clause("id", &["name".to_string()]),
        "ON CONFLICT ([id]) DO UPDATE SET [name] = excluded.[name]"
    );
}
