use ormdantic_dialects::{
    Dialect, MariaDbDialect, MsSqlDialect, MySqlDialect, OracleDialect, PostgresDialect,
    SqliteDialect,
};

#[test]
fn renders_upsert_conflict_clauses_by_backend() {
    assert_eq!(
        SqliteDialect
            .upsert_conflict_clause("id", &["name".to_string()])
            .unwrap(),
        r#"ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name""#
    );
    assert_eq!(
        PostgresDialect.upsert_conflict_clause("id", &[]).unwrap(),
        r#"ON CONFLICT ("id") DO NOTHING"#
    );
    assert_eq!(
        MySqlDialect
            .upsert_conflict_clause("id", &["name".to_string()])
            .unwrap(),
        "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
    );
    assert_eq!(
        MariaDbDialect
            .upsert_conflict_clause("id", &["name".to_string()])
            .unwrap(),
        "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
    );
    assert_eq!(
        MySqlDialect.upsert_conflict_clause("id", &[]).unwrap(),
        "ON DUPLICATE KEY UPDATE 1 = 1"
    );
}

#[test]
fn rejects_insert_conflict_clauses_for_merge_upsert_dialects() {
    for error in [
        MsSqlDialect
            .upsert_conflict_clause("id", &["name".to_string()])
            .expect_err("sql server upsert is rendered as MERGE"),
        OracleDialect
            .upsert_conflict_clause("id", &["name".to_string()])
            .expect_err("oracle upsert is rendered as MERGE"),
    ] {
        assert!(error.to_string().contains("INSERT conflict-clause upsert"));
    }
}
