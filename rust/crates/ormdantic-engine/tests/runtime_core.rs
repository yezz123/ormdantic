mod support;

use ormdantic_core::{
    ExecutionErrorKind, IsolationLevel, OrmdanticError, RevisionId, SavepointName,
    TransactionOptions,
};
use ormdantic_dialects::ReflectionScope;
use ormdantic_engine::{
    execute_url, returns_rows, runtime_capabilities, Connection, DbValue, Inspector,
    MigrationStore, NativeConnection, QueryResult, Reflector, StatementResult, TransactionState,
};
use ormdantic_schema::FieldKind;
use rusqlite::types::{ToSqlOutput, Value as RusqliteValue, ValueRef};
use rusqlite::ToSql;

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
fn statement_result_new_preserves_explicit_statement_metadata() {
    let statement = StatementResult::new(
        3,
        Some(DbValue::UnsignedInteger(42)),
        vec![vec![DbValue::Text("mint".to_string())]],
        vec!["name".to_string()],
    );

    assert_eq!(statement.row_count(), 3);
    assert_eq!(
        statement.last_insert_id(),
        Some(&DbValue::UnsignedInteger(42))
    );
    assert_eq!(
        statement.returned_rows(),
        &[vec![DbValue::Text("mint".to_string())]]
    );
    assert_eq!(statement.columns(), &["name".to_string()]);
}

#[test]
fn sqlite_db_value_to_sql_covers_scalar_variants() {
    let cases = [
        (DbValue::Null, RusqliteValue::Null),
        (DbValue::Integer(-7), RusqliteValue::Integer(-7)),
        (DbValue::UnsignedInteger(42), RusqliteValue::Integer(42)),
        (
            DbValue::UnsignedInteger(u64::MAX),
            RusqliteValue::Blob(u64::MAX.to_string().into_bytes()),
        ),
        (
            DbValue::Decimal("123.45".to_string()),
            RusqliteValue::Text("123.45".to_string()),
        ),
        (DbValue::Real(3.25), RusqliteValue::Real(3.25)),
        (
            DbValue::Text("vanilla".to_string()),
            RusqliteValue::Text("vanilla".to_string()),
        ),
        (DbValue::Bool(true), RusqliteValue::Integer(1)),
        (DbValue::Bool(false), RusqliteValue::Integer(0)),
    ];

    for (value, expected) in cases {
        let actual = value.to_sql().expect("db value should convert to sqlite");
        match (&actual, &expected) {
            (ToSqlOutput::Owned(actual), expected) => assert_eq!(actual, expected),
            (ToSqlOutput::Borrowed(ValueRef::Text(actual)), RusqliteValue::Text(expected)) => {
                assert_eq!(*actual, expected.as_bytes());
            }
            _ => panic!("unexpected sqlite value for {value:?}: {actual:?}"),
        }
    }
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
fn sqlite_declared_numeric_columns_decode_as_decimal() {
    let url = support::sqlite_url(&support::unique_name("engine_sqlite_decimal"));

    execute_url(
        &url,
        "CREATE TABLE prices (id INTEGER PRIMARY KEY, amount DECIMAL_TEXT(30, 9), label TEXT)",
        &[],
    )
    .expect("create table should work");
    execute_url(
        &url,
        "INSERT INTO prices (id, amount, label) VALUES (?1, ?2, ?3)",
        &[
            DbValue::Integer(1),
            DbValue::Decimal("12345678901234567890.123456789".to_string()),
            DbValue::Text("12345678901234567890.123456789".to_string()),
        ],
    )
    .expect("insert should work");

    let result = execute_url(
        &url,
        "SELECT amount, label, typeof(amount) FROM prices WHERE id = ?1",
        &[DbValue::Integer(1)],
    )
    .expect("select should work");

    support::assert_rows(
        &result,
        &[vec![
            DbValue::Decimal("12345678901234567890.123456789".to_string()),
            DbValue::Text("12345678901234567890.123456789".to_string()),
            DbValue::Text("text".to_string()),
        ]],
    );
}

#[test]
fn sqlite_declared_integer_columns_decode_large_unsigned_blob_storage() {
    let url = support::sqlite_url(&support::unique_name("engine_sqlite_unsigned"));

    execute_url(
        &url,
        "CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)",
        &[],
    )
    .expect("create table should work");
    execute_url(
        &url,
        "INSERT INTO counters (id, value) VALUES (?1, ?2)",
        &[DbValue::Integer(1), DbValue::UnsignedInteger(u64::MAX)],
    )
    .expect("insert should work");

    let result = execute_url(
        &url,
        "SELECT value, typeof(value) FROM counters WHERE id = ?1",
        &[DbValue::Integer(1)],
    )
    .expect("select should work");

    support::assert_rows(
        &result,
        &[vec![
            DbValue::UnsignedInteger(u64::MAX),
            DbValue::Text("blob".to_string()),
        ]],
    );
}

#[test]
fn sqlite_execution_errors_are_classified() {
    let url = support::sqlite_url(&support::unique_name("engine_sqlite_errors"));
    let mut connection = NativeConnection::open(&url).expect("sqlite connection should open");

    connection
        .execute("PRAGMA foreign_keys = ON", &[])
        .expect("foreign keys should enable");
    connection
        .execute(
            "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
            &[],
        )
        .expect("create parent table should work");
    connection
        .execute(
            "CREATE TABLE children (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER NOT NULL,
                rating INTEGER CHECK (rating > 0),
                FOREIGN KEY(parent_id) REFERENCES parents(id)
            )",
            &[],
        )
        .expect("create child table should work");
    connection
        .execute("INSERT INTO parents (id, name) VALUES (1, 'vanilla')", &[])
        .expect("first parent insert should work");

    assert_execution_error_kind(
        connection.execute("INSERT INTO parents (id, name) VALUES (2, 'vanilla')", &[]),
        ExecutionErrorKind::UniqueViolation,
    );
    assert_execution_error_kind(
        connection.execute("INSERT INTO parents (id, name) VALUES (3, NULL)", &[]),
        ExecutionErrorKind::NotNullViolation,
    );
    assert_execution_error_kind(
        connection.execute(
            "INSERT INTO children (id, parent_id, rating) VALUES (1, 999, 1)",
            &[],
        ),
        ExecutionErrorKind::ForeignKeyViolation,
    );
    assert_execution_error_kind(
        connection.execute(
            "INSERT INTO children (id, parent_id, rating) VALUES (2, 1, -1)",
            &[],
        ),
        ExecutionErrorKind::CheckViolation,
    );
    assert_execution_error_kind(
        connection.execute("SELECT * FROM", &[]),
        ExecutionErrorKind::Syntax,
    );
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
fn native_connection_trait_forwards_sqlite_operations() {
    let url = support::sqlite_url(&support::unique_name("engine_connection_trait"));
    let mut connection = NativeConnection::open(&url).expect("sqlite connection should open");

    {
        let trait_connection: &mut dyn Connection = &mut connection;
        assert!(trait_connection
            .set_isolation(IsolationLevel::Serializable)
            .is_err());
        trait_connection
            .execute(
                "CREATE TABLE flavors (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                &[],
            )
            .expect("trait execute should create a table");
        trait_connection
            .execute_batch(&[
                "INSERT INTO flavors (id, name) VALUES (1, 'vanilla')".to_string(),
                "INSERT INTO flavors (id, name) VALUES (2, 'mocha')".to_string(),
            ])
            .expect("trait execute_batch should run statements");

        let queried = trait_connection
            .query("SELECT COUNT(*) AS count FROM flavors", &[])
            .expect("trait query should run select");
        support::assert_rows(&queried, &[vec![DbValue::Integer(2)]]);

        trait_connection
            .begin_with(TransactionOptions::new())
            .expect("trait begin_with should start transaction");
        trait_connection
            .begin_nested(SavepointName::new("trait_sp").expect("valid savepoint"))
            .expect("trait begin_nested should create savepoint");
        assert_eq!(
            trait_connection.transaction_state(),
            TransactionState::Unknown
        );
    }

    connection
        .rollback()
        .expect("rollback should close transaction");
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

#[test]
fn inspector_reflects_sqlite_tables_and_columns() {
    let url = support::sqlite_url(&support::unique_name("engine_reflection"));
    let mut connection = NativeConnection::open(&url).expect("sqlite connection should open");
    connection
        .execute(
            "CREATE TABLE flavors (id INTEGER PRIMARY KEY, name TEXT NOT NULL, rating REAL)",
            &[],
        )
        .expect("table should be created");

    let mut inspector = Inspector::new(&mut connection);
    let schema = inspector
        .inspect(&ReflectionScope::new().tables(vec!["flavors".to_string()]))
        .expect("schema should reflect")
        .into_schema_def();
    let table = schema.table("flavors").expect("table should be reflected");

    assert_eq!(table.primary_key(), "id");
    assert_eq!(table.columns().len(), 3);
    assert!(table.columns()[0].is_primary_key());
    assert!(!table.columns()[1].is_nullable());
    assert_eq!(table.columns()[1].name(), "name");
    assert_eq!(table.columns()[2].name(), "rating");
}

#[test]
fn inspector_reflects_sqlite_declared_type_variants() {
    let url = support::sqlite_url(&support::unique_name("engine_reflection_types"));
    let mut connection = NativeConnection::open(&url).expect("sqlite connection should open");
    connection
        .execute(
            "CREATE TABLE reflected_types (
                id UUID PRIMARY KEY,
                payload JSON,
                bytes BLOB,
                active BOOLEAN,
                amount NUMERIC,
                born DATE,
                created DATETIME,
                label VARCHAR(20),
                mystery CUSTOMTYPE
            )",
            &[],
        )
        .expect("table should be created");

    let mut inspector = Inspector::new(&mut connection);
    let schema = inspector
        .inspect(&ReflectionScope::new().tables(vec!["reflected_types".to_string()]))
        .expect("schema should reflect")
        .into_schema_def();
    let table = schema
        .table("reflected_types")
        .expect("table should be reflected");
    let kinds = table
        .columns()
        .iter()
        .map(|column| (column.name(), column.kind()))
        .collect::<Vec<_>>();

    assert!(kinds.contains(&("id", &FieldKind::Uuid)));
    assert!(kinds.contains(&("payload", &FieldKind::Json)));
    assert!(kinds.contains(&("bytes", &FieldKind::Binary)));
    assert!(kinds.contains(&("active", &FieldKind::Boolean)));
    assert!(kinds.contains(&("amount", &FieldKind::Decimal)));
    assert!(kinds.contains(&("born", &FieldKind::Date)));
    assert!(kinds.contains(&("created", &FieldKind::DateTime)));
    assert!(kinds.contains(&("label", &FieldKind::String)));
    assert!(kinds.contains(&("mystery", &FieldKind::Unknown)));
}

fn assert_execution_error_kind<T: std::fmt::Debug>(
    result: Result<T, OrmdanticError>,
    expected: ExecutionErrorKind,
) {
    match result.expect_err("operation should fail") {
        OrmdanticError::ExecutionError { kind, .. } => assert_eq!(kind, expected),
        other => panic!("expected execution error {expected:?}, got {other:?}"),
    }
}
