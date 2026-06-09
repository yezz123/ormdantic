mod support;

use ormdantic_core::{RevisionId, TransactionOptions};
use ormdantic_dialects::ReflectionScope;
use ormdantic_engine::{
    execute_url, returns_rows, runtime_capabilities, Connection, DbValue, MigrationStore,
    NativeConnection, QueryResult, Reflector, StatementResult,
};

#[test]
fn statement_result_preserves_query_shape() {
    let query = QueryResult::new(
        vec!["id".to_string(), "name".to_string()],
        vec![vec![
            DbValue::Integer(1),
            DbValue::Text("vanilla".to_string()),
        ]],
    );

    let statement = StatementResult::from_query_result(query);

    assert_eq!(statement.row_count(), 1);
    assert_eq!(statement.last_insert_id(), None);
    assert_eq!(statement.columns(), &["id".to_string(), "name".to_string()]);
    assert_eq!(
        statement.returned_rows(),
        &[vec![
            DbValue::Integer(1),
            DbValue::Text("vanilla".to_string())
        ]]
    );
}

#[test]
fn runtime_capabilities_expose_supported_dialects() {
    let capabilities = runtime_capabilities();

    assert_eq!(capabilities.len(), 6);
    assert!(capabilities.iter().any(|(name, _)| *name == "sqlite"));
    assert!(capabilities.iter().any(|(name, _)| *name == "postgresql"));
    assert!(capabilities.iter().any(|(name, _)| *name == "mysql"));
}

#[test]
fn returns_rows_detects_rowset_producing_dml() {
    assert!(returns_rows("SELECT 1"));
    assert!(returns_rows(
        "INSERT INTO flavors (id) VALUES ($1) RETURNING id"
    ));
    assert!(returns_rows(
        "UPDATE flavors SET name = @P1 OUTPUT inserted.id WHERE id = @P2"
    ));
    assert!(returns_rows("DELETE FROM flavors RETURNING id"));
    assert!(!returns_rows("INSERT INTO flavors (id) VALUES (?)"));
    assert!(!returns_rows("UPDATE flavors SET name = ? WHERE id = ?"));
}

#[test]
fn sqlite_execute_url_runs_basic_statements() {
    let url = support::sqlite_url(&support::unique_name("engine_execute_url"));

    execute_url(
        &url,
        "CREATE TABLE flavors (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        &[],
    )
    .expect("create table should work");
    execute_url(
        &url,
        "INSERT INTO flavors (id, name) VALUES (?1, ?2)",
        &[DbValue::Integer(1), DbValue::Text("vanilla".to_string())],
    )
    .expect("insert should work");
    let result = execute_url(
        &url,
        "SELECT name FROM flavors WHERE id = ?1",
        &[DbValue::Integer(1)],
    )
    .expect("select should work");

    support::assert_rows(&result, &[vec![DbValue::Text("vanilla".to_string())]]);
}

#[test]
fn native_connection_batches_and_transactions_work_for_sqlite() {
    let url = support::sqlite_url(&support::unique_name("engine_native_connection"));
    let mut connection = NativeConnection::open(&url).expect("sqlite connection should open");

    connection
        .execute(
            "CREATE TABLE flavors (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            &[],
        )
        .expect("create table should work");
    connection
        .execute_batch(&[
            "INSERT INTO flavors (id, name) VALUES (1, 'vanilla')".to_string(),
            "INSERT INTO flavors (id, name) VALUES (2, 'mocha')".to_string(),
        ])
        .expect("batch insert should work");
    connection
        .begin_with(TransactionOptions::new())
        .expect("begin should work");
    connection
        .execute("INSERT INTO flavors (id, name) VALUES (3, 'mint')", &[])
        .expect("insert in transaction should work");
    connection.rollback().expect("rollback should work");

    let result = connection
        .execute("SELECT COUNT(*) AS count FROM flavors", &[])
        .expect("count should work");
    support::assert_rows(&result, &[vec![DbValue::Integer(2)]]);
}

#[test]
fn native_connection_savepoints_work_for_sqlite() {
    let url = support::sqlite_url(&support::unique_name("engine_savepoint"));
    let mut connection = NativeConnection::open(&url).expect("sqlite connection should open");

    connection
        .execute("CREATE TABLE flavors (id INTEGER PRIMARY KEY)", &[])
        .expect("create table should work");
    connection.begin().expect("begin should work");
    connection
        .execute("INSERT INTO flavors (id) VALUES (1)", &[])
        .expect("first insert should work");
    connection
        .savepoint("sp_one")
        .expect("savepoint should work");
    connection
        .execute("INSERT INTO flavors (id) VALUES (2)", &[])
        .expect("second insert should work");
    connection
        .rollback_to_savepoint("sp_one")
        .expect("rollback to savepoint should work");
    connection
        .release_savepoint("sp_one")
        .expect("release savepoint should work");
    connection.commit().expect("commit should work");

    let result = connection
        .execute("SELECT COUNT(*) AS count FROM flavors", &[])
        .expect("count should work");
    support::assert_rows(&result, &[vec![DbValue::Integer(1)]]);
}

#[test]
fn migration_store_records_revisions_for_sqlite() {
    let url = support::sqlite_url(&support::unique_name("engine_migration_store"));
    let mut connection = NativeConnection::open(&url).expect("sqlite connection should open");
    let revision = RevisionId::new("001_initial").expect("revision id should be valid");

    let mut store = MigrationStore::new(&mut connection).with_table_name("migration_store_test");
    store
        .record_revision(&revision)
        .expect("record revision should work");

    assert_eq!(
        store.revisions().expect("revisions should load"),
        vec![revision]
    );
}

#[test]
fn reflector_exposes_dialect_queries_and_empty_schema() {
    let reflector = Reflector::for_url("sqlite:///:memory:").expect("reflector should parse url");
    let queries = reflector.reflection_queries(&ReflectionScope::default());

    assert!(!queries.is_empty());
    assert!(reflector.empty_schema().tables().is_empty());
}
