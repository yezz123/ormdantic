use ormdantic_core::{DeferrableMode, IsolationLevel, SavepointName, TransactionOptions};
use ormdantic_dialects::{
    Dialect, MariaDbDialect, MsSqlDialect, MySqlDialect, OracleDialect, PostgresDialect,
    SqliteDialect,
};

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[test]
fn renders_begin_transaction_options() {
    let options = TransactionOptions::new()
        .with_isolation_level(IsolationLevel::RepeatableRead)
        .read_only()
        .with_deferrable_mode(DeferrableMode::Deferrable);
    let serializable_read_only = TransactionOptions::new()
        .with_isolation_level(IsolationLevel::Serializable)
        .read_only();

    assert_eq!(
        PostgresDialect.begin_transaction_sql(&options),
        strings(&[
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "BEGIN READ ONLY DEFERRABLE",
        ])
    );
    assert_eq!(
        PostgresDialect.begin_transaction_sql(&TransactionOptions::new()),
        strings(&["BEGIN"])
    );
    assert_eq!(
        MySqlDialect.begin_transaction_sql(&options),
        strings(&[
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "START TRANSACTION READ ONLY",
        ])
    );
    assert_eq!(
        MariaDbDialect.begin_transaction_sql(&options),
        strings(&[
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "START TRANSACTION READ ONLY",
        ])
    );
    assert_eq!(
        MsSqlDialect.begin_transaction_sql(&options),
        strings(&[
            "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
            "BEGIN TRANSACTION",
        ])
    );
    assert_eq!(
        OracleDialect.begin_transaction_sql(&serializable_read_only),
        strings(&["SET TRANSACTION READ ONLY"])
    );
    assert_eq!(
        OracleDialect.begin_transaction_sql(
            &TransactionOptions::new().with_isolation_level(IsolationLevel::Serializable)
        ),
        strings(&["SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"])
    );
    assert!(OracleDialect
        .begin_transaction_sql(&TransactionOptions::new())
        .is_empty());
    assert_eq!(
        SqliteDialect.begin_transaction_sql(&TransactionOptions::new()),
        strings(&["BEGIN"])
    );
}

#[test]
fn renders_isolation_levels() {
    assert_eq!(
        PostgresDialect.set_isolation_sql(IsolationLevel::ReadUncommitted),
        "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
    );
    assert_eq!(
        PostgresDialect.set_isolation_sql(IsolationLevel::ReadCommitted),
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    );
    assert_eq!(
        PostgresDialect.set_isolation_sql(IsolationLevel::Serializable),
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
    );
    assert_eq!(
        PostgresDialect.set_isolation_sql(IsolationLevel::Snapshot),
        "SET TRANSACTION ISOLATION LEVEL SNAPSHOT"
    );
}

#[test]
fn renders_savepoint_lifecycle_by_backend() {
    let name = SavepointName::new("sp_1").unwrap();

    assert_eq!(SqliteDialect.savepoint_sql(&name), r#"SAVEPOINT "sp_1""#);
    assert_eq!(
        PostgresDialect.rollback_to_savepoint_sql(&name),
        r#"ROLLBACK TO SAVEPOINT "sp_1""#
    );
    assert_eq!(
        MySqlDialect.release_savepoint_sql(&name),
        "RELEASE SAVEPOINT `sp_1`"
    );
    assert_eq!(
        MariaDbDialect.release_savepoint_sql(&name),
        "RELEASE SAVEPOINT `sp_1`"
    );
    assert_eq!(MsSqlDialect.savepoint_sql(&name), "SAVE TRANSACTION [sp_1]");
    assert_eq!(
        MsSqlDialect.rollback_to_savepoint_sql(&name),
        "ROLLBACK TRANSACTION [sp_1]"
    );
    assert_eq!(MsSqlDialect.release_savepoint_sql(&name), "");
    assert_eq!(
        OracleDialect.rollback_to_savepoint_sql(&name),
        r#"ROLLBACK TO SAVEPOINT "sp_1""#
    );
    assert_eq!(OracleDialect.release_savepoint_sql(&name), "");
}
