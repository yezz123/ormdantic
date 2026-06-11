use ormdantic_core::{ExecutionErrorKind, OrmdanticError};
use ormdantic_dialects::ReflectionScope;
use ormdantic_engine::{DbValue, Inspector, NativeConnection};
use url::Url;

use crate::support;

pub struct CrudSql {
    pub create_sql: String,
    pub drop_sql: String,
    pub insert_sql: String,
    pub select_sql: String,
    pub update_sql: String,
    pub delete_sql: String,
    pub count_sql: String,
    pub syntax_error_sql: &'static str,
}

pub struct ConstraintSql {
    pub create_sql: String,
    pub drop_sql: String,
    pub insert_sql: String,
    pub count_sql: String,
    pub commit_after_first_insert: bool,
    pub rollback_after_error: bool,
}

pub struct ValueRoundTripSql {
    pub sql: &'static str,
    pub expected_real: DbValue,
    pub expected_bool: DbValue,
}

pub struct ReturnedRowsSql {
    pub create_sql: String,
    pub drop_sql: String,
    pub insert_returning_sql: String,
}

pub struct NumericEdgeSql {
    pub sql: &'static str,
    pub expected: Vec<DbValue>,
}

pub struct DdlEdgeSql {
    pub create_table_sql: String,
    pub drop_table_sql: String,
    pub add_column_sql: String,
    pub create_index_sql: String,
    pub drop_index_sql: String,
    pub drop_column_sql: String,
    pub insert_sql: String,
    pub select_by_added_column_sql: String,
    pub select_dropped_column_sql: String,
}

pub fn alias_url(raw_url: &str, scheme: &str) -> String {
    let mut url = Url::parse(raw_url).expect("driver URL should parse");
    url.set_scheme(scheme)
        .expect("driver URL should accept alias scheme");
    url.to_string()
}

pub fn run_alias_url_flow(
    url: &str,
    alias_scheme: &str,
    expected_dialect: &str,
    validation_sql: &str,
) {
    let alias = alias_url(url, alias_scheme);
    let mut connection = NativeConnection::open(&alias)
        .unwrap_or_else(|error| panic!("{alias_scheme} alias URL should open: {error}"));
    assert_eq!(connection.dialect(), expected_dialect);

    let result = connection
        .execute(validation_sql, &[])
        .unwrap_or_else(|error| {
            panic!("{alias_scheme} alias validation query should work: {error}")
        });
    assert_eq!(result.rows(), &[vec![DbValue::Integer(1)]]);
}

pub fn run_value_round_trip_flow(url: &str, sql: ValueRoundTripSql) {
    let mut connection = NativeConnection::open(url).expect("native connection should open");
    let result = connection
        .execute(
            sql.sql,
            &[
                DbValue::Integer(42),
                DbValue::Real(3.25),
                DbValue::Text("vanilla".to_string()),
                DbValue::Bool(true),
                DbValue::Null,
            ],
        )
        .expect("value round-trip select should execute");

    assert_eq!(
        result.rows(),
        &[vec![
            DbValue::Integer(42),
            sql.expected_real,
            DbValue::Text("vanilla".to_string()),
            sql.expected_bool,
            DbValue::Null,
        ]]
    );
}

pub fn run_returned_rows_flow(url: &str, table: &str, sql: ReturnedRowsSql) {
    let _ = ormdantic_engine::execute_url(url, &sql.drop_sql, &[]);

    let mut connection = NativeConnection::open(url).expect("native connection should open");
    connection
        .execute(&sql.create_sql, &[])
        .unwrap_or_else(|error| panic!("create returning table for {table} should work: {error}"));

    let statement = connection
        .statement(
            &sql.insert_returning_sql,
            &[DbValue::Integer(1), DbValue::Text("vanilla".to_string())],
        )
        .unwrap_or_else(|error| panic!("insert returning for {table} should work: {error}"));
    assert_eq!(statement.row_count(), 1);
    assert_eq!(
        statement.returned_rows(),
        &[vec![
            DbValue::Integer(1),
            DbValue::Text("vanilla".to_string())
        ]]
    );

    let count = connection
        .execute(&format!("SELECT COUNT(*) AS count FROM {table}"), &[])
        .unwrap_or_else(|error| panic!("count after insert returning for {table}: {error}"));
    assert_eq!(count.rows(), &[vec![DbValue::Integer(1)]]);

    ormdantic_engine::execute_url(url, &sql.drop_sql, &[])
        .unwrap_or_else(|error| panic!("cleanup for {table} should work: {error}"));
}

pub fn run_numeric_edge_flow(url: &str, sql: NumericEdgeSql) {
    let mut connection = NativeConnection::open(url).expect("native connection should open");
    let result = connection
        .execute(sql.sql, &[])
        .expect("numeric edge query should execute");

    assert_eq!(result.rows(), &[sql.expected]);
}

pub fn run_ddl_edge_flow(url: &str, table: &str, sql: DdlEdgeSql) {
    let _ = ormdantic_engine::execute_url(url, &sql.drop_table_sql, &[]);

    let mut connection = NativeConnection::open(url).expect("native connection should open");
    connection
        .execute(&sql.create_table_sql, &[])
        .unwrap_or_else(|error| panic!("create DDL edge table for {table}: {error}"));
    connection
        .execute(&sql.add_column_sql, &[])
        .unwrap_or_else(|error| panic!("add DDL edge column for {table}: {error}"));
    connection
        .execute(&sql.create_index_sql, &[])
        .unwrap_or_else(|error| panic!("create DDL edge index for {table}: {error}"));

    connection
        .execute(
            &sql.insert_sql,
            &[DbValue::Integer(1), DbValue::Text("vanilla".to_string())],
        )
        .unwrap_or_else(|error| panic!("insert after DDL edge alteration for {table}: {error}"));
    let selected = connection
        .execute(
            &sql.select_by_added_column_sql,
            &[DbValue::Text("vanilla".to_string())],
        )
        .unwrap_or_else(|error| panic!("select through DDL edge column for {table}: {error}"));
    assert_eq!(selected.rows(), &[vec![DbValue::Integer(1)]]);

    connection
        .execute(&sql.drop_index_sql, &[])
        .unwrap_or_else(|error| panic!("drop DDL edge index for {table}: {error}"));
    connection
        .execute(&sql.drop_column_sql, &[])
        .unwrap_or_else(|error| panic!("drop DDL edge column for {table}: {error}"));
    assert!(
        connection
            .execute(&sql.select_dropped_column_sql, &[])
            .is_err(),
        "selecting a dropped column for {table} should return an engine error"
    );

    ormdantic_engine::execute_url(url, &sql.drop_table_sql, &[])
        .unwrap_or_else(|error| panic!("cleanup for {table} should work: {error}"));
}

pub fn invalid_credentials_url(raw_url: &str) -> String {
    let mut url = Url::parse(raw_url).expect("driver URL should parse");
    if url.username().is_empty() {
        url.set_username("ormdantic_bad_user")
            .expect("driver URL should accept a username");
    }
    url.set_password(Some("ormdantic_bad_password"))
        .expect("driver URL should accept a password");
    url.to_string()
}

pub fn run_connection_failure_flow(url: &str) {
    let bad_url = invalid_credentials_url(url);
    assert!(
        NativeConnection::open(&bad_url).is_err(),
        "invalid credentials should be mapped to an engine connection error"
    );
}

pub fn run_reflection_smoke_flow(url: &str, table: &str, create_sql: String, drop_sql: String) {
    let _ = ormdantic_engine::execute_url(url, &drop_sql, &[]);

    let mut connection = NativeConnection::open(url).expect("native connection should open");
    connection
        .execute(&create_sql, &[])
        .unwrap_or_else(|error| panic!("create reflection table for {table} should work: {error}"));
    let mut inspector = Inspector::new(&mut connection);
    let queries = inspector
        .reflection_queries(&ReflectionScope::new())
        .unwrap_or_else(|error| panic!("reflection queries for {table} should compile: {error}"));
    assert!(
        !queries.is_empty(),
        "driver reflection should expose catalog queries for {table}"
    );
    inspector
        .inspect(&ReflectionScope::new())
        .unwrap_or_else(|error| panic!("reflection smoke for {table} should execute: {error}"));

    ormdantic_engine::execute_url(url, &drop_sql, &[])
        .unwrap_or_else(|error| panic!("cleanup for {table} should work: {error}"));
}

pub fn run_crud_result_and_error_flow(url: &str, table: &str, sql: CrudSql) {
    let _ = ormdantic_engine::execute_url(url, &sql.drop_sql, &[]);

    let mut connection = NativeConnection::open(url).expect("native connection should open");
    connection
        .execute(&sql.create_sql, &[])
        .unwrap_or_else(|error| panic!("create table for {table} should work: {error}"));
    connection
        .execute(
            &sql.insert_sql,
            &[
                DbValue::Integer(1),
                DbValue::Text("vanilla".to_string()),
                DbValue::Integer(3),
            ],
        )
        .unwrap_or_else(|error| panic!("insert for {table} should work: {error}"));

    let selected = connection
        .execute(&sql.select_sql, &[DbValue::Integer(1)])
        .unwrap_or_else(|error| panic!("select for {table} should work: {error}"));
    assert_eq!(selected.columns().len(), 2);
    assert_eq!(
        selected.rows(),
        &[vec![
            DbValue::Text("vanilla".to_string()),
            DbValue::Integer(3),
        ]]
    );

    connection
        .execute(
            &sql.update_sql,
            &[
                DbValue::Text("mocha".to_string()),
                DbValue::Integer(7),
                DbValue::Integer(1),
            ],
        )
        .unwrap_or_else(|error| panic!("update for {table} should work: {error}"));
    let updated = connection
        .execute(&sql.select_sql, &[DbValue::Integer(1)])
        .unwrap_or_else(|error| panic!("select after update for {table} should work: {error}"));
    assert_eq!(
        updated.rows(),
        &[vec![
            DbValue::Text("mocha".to_string()),
            DbValue::Integer(7)
        ]]
    );

    connection
        .execute(&sql.delete_sql, &[DbValue::Integer(1)])
        .unwrap_or_else(|error| panic!("delete for {table} should work: {error}"));
    let count = connection
        .execute(&sql.count_sql, &[])
        .unwrap_or_else(|error| panic!("count after delete for {table} should work: {error}"));
    assert_eq!(count.rows(), &[vec![DbValue::Integer(0)]]);

    assert_execution_error_kind(
        connection.execute(sql.syntax_error_sql, &[]),
        ExecutionErrorKind::Syntax,
        &format!("syntax error for {table} should be mapped to an engine error"),
    );

    ormdantic_engine::execute_url(url, &sql.drop_sql, &[])
        .unwrap_or_else(|error| panic!("cleanup for {table} should work: {error}"));
}

pub fn run_constraint_error_flow(url: &str, table: &str, sql: ConstraintSql) {
    let _ = ormdantic_engine::execute_url(url, &sql.drop_sql, &[]);

    let mut connection = NativeConnection::open(url).expect("native connection should open");
    connection
        .execute(&sql.create_sql, &[])
        .unwrap_or_else(|error| panic!("create constraint table for {table} should work: {error}"));
    connection
        .execute(
            &sql.insert_sql,
            &[DbValue::Integer(1), DbValue::Text("vanilla".to_string())],
        )
        .unwrap_or_else(|error| {
            panic!("first constrained insert for {table} should work: {error}")
        });
    if sql.commit_after_first_insert {
        connection
            .commit()
            .unwrap_or_else(|error| panic!("commit first constrained insert for {table}: {error}"));
    }

    assert_execution_error_kind(
        connection.execute(
            &sql.insert_sql,
            &[DbValue::Integer(2), DbValue::Text("vanilla".to_string())],
        ),
        ExecutionErrorKind::UniqueViolation,
        &format!("duplicate unique value for {table} should be mapped to an engine error"),
    );
    if sql.rollback_after_error {
        connection
            .rollback()
            .unwrap_or_else(|error| panic!("rollback after constraint error for {table}: {error}"));
    }

    let count = ormdantic_engine::execute_url(url, &sql.count_sql, &[])
        .unwrap_or_else(|error| panic!("count after constraint error for {table}: {error}"));
    assert_eq!(count.rows(), &[vec![DbValue::Integer(1)]]);

    ormdantic_engine::execute_url(url, &sql.drop_sql, &[])
        .unwrap_or_else(|error| panic!("cleanup for {table} should work: {error}"));
}

pub fn run_transaction_savepoint_flow(
    url: &str,
    table: &str,
    create_sql: String,
    drop_sql: String,
    insert_sql: &str,
    count_sql: String,
) {
    let _ = ormdantic_engine::execute_url(url, &drop_sql, &[]);
    ormdantic_engine::execute_url(url, &create_sql, &[]).expect("create table should work");

    let mut connection = NativeConnection::open(url).expect("native connection should open");
    connection.begin().expect("begin should work");
    connection
        .execute(insert_sql, &[DbValue::Integer(1)])
        .expect("first insert should work");
    connection
        .savepoint("sp_one")
        .expect("savepoint should work");
    connection
        .execute(insert_sql, &[DbValue::Integer(2)])
        .expect("second insert should work");
    connection
        .rollback_to_savepoint("sp_one")
        .expect("rollback to savepoint should work");
    connection
        .release_savepoint("sp_one")
        .expect("release savepoint should work");
    connection.commit().expect("commit should work");

    let result = ormdantic_engine::execute_url(url, &count_sql, &[]).expect("count should work");
    assert_eq!(result.rows(), &[vec![DbValue::Integer(1)]]);

    ormdantic_engine::execute_url(url, &drop_sql, &[])
        .unwrap_or_else(|error| panic!("cleanup for {table} should work: {error}"));
}

pub fn unique_table(prefix: &str) -> String {
    support::unique_name(prefix)
}

fn assert_execution_error_kind<T: std::fmt::Debug>(
    result: Result<T, OrmdanticError>,
    expected: ExecutionErrorKind,
    context: &str,
) {
    match result.expect_err(context) {
        OrmdanticError::ExecutionError { kind, .. } => assert_eq!(kind, expected, "{context}"),
        other => panic!("{context}: expected execution error {expected:?}, got {other:?}"),
    }
}

#[cfg(feature = "oracle")]
pub fn unique_table_with_limit(prefix: &str, max_len: usize) -> String {
    let name = support::unique_name(prefix);
    if name.len() <= max_len {
        return name;
    }
    let suffix = name
        .rsplit_once('_')
        .map(|(_, suffix)| suffix)
        .unwrap_or(name.as_str());
    let suffix_len = suffix.len().min(max_len.saturating_sub(1));
    let prefix_len = max_len.saturating_sub(suffix_len + 1);
    format!(
        "{}_{}",
        &prefix[..prefix_len.min(prefix.len())],
        &suffix[..suffix_len]
    )
}
