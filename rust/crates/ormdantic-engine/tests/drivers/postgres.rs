use ormdantic_engine::DbValue;

use super::common;
use crate::support;

fn url() -> Option<String> {
    support::env_url("ORMDANTIC_POSTGRES_URL")
}

#[test]
fn postgres_parameterized_selects_cover_core_values() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };

    common::run_value_round_trip_flow(
        &url,
        common::ValueRoundTripSql {
            sql: "SELECT $1::BIGINT AS int_value, $2::DOUBLE PRECISION AS real_value, $3::TEXT AS text_value, $4::BOOLEAN AS bool_value, $5::TEXT AS null_value",
            expected_real: DbValue::Real(3.25),
            expected_bool: DbValue::Bool(true),
        },
    );
}

#[test]
fn postgres_numeric_edges_round_trip() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };

    common::run_numeric_edge_flow(
        &url,
        common::NumericEdgeSql {
            sql: "SELECT CAST(-32768 AS SMALLINT) AS small_value, CAST(2147483647 AS INTEGER) AS int_value, CAST(9223372036854775807 AS BIGINT) AS big_value, CAST(3.5 AS DOUBLE PRECISION) AS real_value, CAST(123.45 AS NUMERIC(10,2)) AS decimal_value",
            expected: vec![
                DbValue::Integer(-32768),
                DbValue::Integer(2_147_483_647),
                DbValue::Integer(9_223_372_036_854_775_807),
                DbValue::Real(3.5),
                DbValue::Decimal("123.45".to_string()),
            ],
        },
    );
}

#[test]
fn postgres_alias_url_opens_with_expected_dialect() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };

    common::run_alias_url_flow(&url, "postgresql+asyncpg", "postgresql", "SELECT 1::BIGINT");
}

#[test]
fn postgres_crud_results_and_errors_work() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_postgres_crud");

    common::run_crud_result_and_error_flow(
        &url,
        &table,
        common::CrudSql {
            create_sql: format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT, strength BIGINT)"
            ),
            drop_sql: format!("DROP TABLE IF EXISTS {table}"),
            insert_sql: format!("INSERT INTO {table} (id, name, strength) VALUES ($1, $2, $3)"),
            select_sql: format!("SELECT name, strength FROM {table} WHERE id = $1"),
            update_sql: format!("UPDATE {table} SET name = $1, strength = $2 WHERE id = $3"),
            delete_sql: format!("DELETE FROM {table} WHERE id = $1"),
            count_sql: format!("SELECT COUNT(*) AS count FROM {table}"),
            syntax_error_sql: "SELECT * FROM",
        },
    );
}

#[test]
fn postgres_ddl_edges_work() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_postgres_ddl");
    let index = common::unique_table("orm_driver_postgres_ddl_idx");

    common::run_ddl_edge_flow(
        &url,
        &table,
        common::DdlEdgeSql {
            create_table_sql: format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY)"),
            drop_table_sql: format!("DROP TABLE IF EXISTS {table}"),
            add_column_sql: format!("ALTER TABLE {table} ADD COLUMN flavor TEXT"),
            create_index_sql: format!("CREATE INDEX {index} ON {table} (flavor)"),
            drop_index_sql: format!("DROP INDEX IF EXISTS {index}"),
            drop_column_sql: format!("ALTER TABLE {table} DROP COLUMN flavor"),
            insert_sql: format!("INSERT INTO {table} (id, flavor) VALUES ($1, $2)"),
            select_by_added_column_sql: format!("SELECT id FROM {table} WHERE flavor = $1"),
            select_dropped_column_sql: format!("SELECT flavor FROM {table}"),
        },
    );
}

#[test]
fn postgres_insert_returning_populates_statement_rows() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_postgres_returning");

    common::run_returned_rows_flow(
        &url,
        &table,
        common::ReturnedRowsSql {
            create_sql: format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"),
            drop_sql: format!("DROP TABLE IF EXISTS {table}"),
            insert_returning_sql: format!(
                "INSERT INTO {table} (id, name) VALUES ($1, $2) RETURNING id, name"
            ),
        },
    );
}

#[test]
fn postgres_constraint_errors_are_mapped() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_postgres_constraint");

    common::run_constraint_error_flow(
        &url,
        &table,
        common::ConstraintSql {
            create_sql: format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT UNIQUE)"),
            drop_sql: format!("DROP TABLE IF EXISTS {table}"),
            insert_sql: format!("INSERT INTO {table} (id, name) VALUES ($1, $2)"),
            count_sql: format!("SELECT COUNT(*) AS count FROM {table}"),
            commit_after_first_insert: false,
            rollback_after_error: true,
        },
    );
}

#[test]
fn postgres_connection_failures_are_mapped() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };

    common::run_connection_failure_flow(&url);
}

#[test]
fn postgres_reflection_smoke_executes_catalog_queries() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_postgres_reflect");

    common::run_reflection_smoke_flow(
        &url,
        &table,
        format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name TEXT)"),
        format!("DROP TABLE IF EXISTS {table}"),
    );
}

#[test]
fn postgres_transactions_and_savepoints_work() {
    let Some(url) = url() else {
        eprintln!("skipping postgres driver test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_postgres_tx");

    common::run_transaction_savepoint_flow(
        &url,
        &table,
        format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY)"),
        format!("DROP TABLE IF EXISTS {table}"),
        &format!("INSERT INTO {table} (id) VALUES ($1)"),
        format!("SELECT COUNT(*) AS count FROM {table}"),
    );
}
