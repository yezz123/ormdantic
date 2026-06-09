use ormdantic_engine::DbValue;

use super::common;
use crate::support;

fn url() -> Option<String> {
    support::env_url("ORMDANTIC_MARIADB_URL")
}

#[test]
fn mariadb_parameterized_selects_cover_core_values() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };

    common::run_value_round_trip_flow(
        &url,
        common::ValueRoundTripSql {
            sql: "SELECT ? AS int_value, ? AS real_value, ? AS text_value, ? AS bool_value, ? AS null_value",
            expected_bool: DbValue::Integer(1),
        },
    );
}

#[test]
fn mariadb_numeric_edges_round_trip() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };

    common::run_numeric_edge_flow(
        &url,
        common::NumericEdgeSql {
            sql: "SELECT CAST(9223372036854775807 AS UNSIGNED) AS unsigned_value, CAST(-2147483648 AS SIGNED) AS signed_value, CAST(3.5 AS DOUBLE) AS real_value, CAST(123.45 AS DECIMAL(10,2)) AS decimal_text",
            expected: vec![
                DbValue::Integer(9_223_372_036_854_775_807),
                DbValue::Integer(-2_147_483_648),
                DbValue::Real(3.5),
                DbValue::Text("123.45".to_string()),
            ],
        },
    );
}

#[test]
fn mariadb_alias_url_opens_with_expected_dialect() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };

    common::run_alias_url_flow(
        &url,
        "mariadb+mariadbconnector",
        "mariadb",
        "SELECT 1 AS value",
    );
}

#[test]
fn mariadb_crud_results_and_errors_work() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mariadb_crud");

    common::run_crud_result_and_error_flow(
        &url,
        &table,
        common::CrudSql {
            create_sql: format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(100), strength BIGINT)"
            ),
            drop_sql: format!("DROP TABLE IF EXISTS {table}"),
            insert_sql: format!("INSERT INTO {table} (id, name, strength) VALUES (?, ?, ?)"),
            select_sql: format!("SELECT name, strength FROM {table} WHERE id = ?"),
            update_sql: format!("UPDATE {table} SET name = ?, strength = ? WHERE id = ?"),
            delete_sql: format!("DELETE FROM {table} WHERE id = ?"),
            count_sql: format!("SELECT COUNT(*) AS count FROM {table}"),
            syntax_error_sql: "SELECT * FROM",
        },
    );
}

#[test]
fn mariadb_ddl_edges_work() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mariadb_ddl");
    let index = common::unique_table("orm_driver_mariadb_ddl_idx");

    common::run_ddl_edge_flow(
        &url,
        &table,
        common::DdlEdgeSql {
            create_table_sql: format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY)"),
            drop_table_sql: format!("DROP TABLE IF EXISTS {table}"),
            add_column_sql: format!("ALTER TABLE {table} ADD COLUMN flavor VARCHAR(100)"),
            create_index_sql: format!("CREATE INDEX {index} ON {table} (flavor)"),
            drop_index_sql: format!("DROP INDEX {index} ON {table}"),
            drop_column_sql: format!("ALTER TABLE {table} DROP COLUMN flavor"),
            insert_sql: format!("INSERT INTO {table} (id, flavor) VALUES (?, ?)"),
            select_by_added_column_sql: format!("SELECT id FROM {table} WHERE flavor = ?"),
            select_dropped_column_sql: format!("SELECT flavor FROM {table}"),
        },
    );
}

#[test]
fn mariadb_insert_returning_populates_statement_rows() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mariadb_returning");

    common::run_returned_rows_flow(
        &url,
        &table,
        common::ReturnedRowsSql {
            create_sql: format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(100))"),
            drop_sql: format!("DROP TABLE IF EXISTS {table}"),
            insert_returning_sql: format!(
                "INSERT INTO {table} (id, name) VALUES (?, ?) RETURNING id, name"
            ),
        },
    );
}

#[test]
fn mariadb_constraint_errors_are_mapped() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mariadb_constraint");

    common::run_constraint_error_flow(
        &url,
        &table,
        common::ConstraintSql {
            create_sql: format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(100) UNIQUE)"
            ),
            drop_sql: format!("DROP TABLE IF EXISTS {table}"),
            insert_sql: format!("INSERT INTO {table} (id, name) VALUES (?, ?)"),
            count_sql: format!("SELECT COUNT(*) AS count FROM {table}"),
            commit_after_first_insert: false,
            rollback_after_error: false,
        },
    );
}

#[test]
fn mariadb_connection_failures_are_mapped() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };

    common::run_connection_failure_flow(&url);
}

#[test]
fn mariadb_reflection_smoke_executes_catalog_queries() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mariadb_reflect");

    common::run_reflection_smoke_flow(
        &url,
        &table,
        format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(100))"),
        format!("DROP TABLE IF EXISTS {table}"),
    );
}

#[test]
fn mariadb_transactions_and_savepoints_work() {
    let Some(url) = url() else {
        eprintln!("skipping mariadb driver test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mariadb_tx");

    common::run_transaction_savepoint_flow(
        &url,
        &table,
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
        format!("DROP TABLE IF EXISTS {table}"),
        &format!("INSERT INTO {table} (id) VALUES (?)"),
        format!("SELECT COUNT(*) AS count FROM {table}"),
    );
}
