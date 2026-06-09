use ormdantic_engine::DbValue;

use super::common;
use crate::support;

fn url() -> Option<String> {
    support::env_url("ORMDANTIC_MSSQL_URL")
}

#[test]
fn mssql_parameterized_selects_cover_core_values() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };

    common::run_value_round_trip_flow(
        &url,
        common::ValueRoundTripSql {
            sql: "SELECT CAST(@P1 AS BIGINT) AS int_value, CAST(@P2 AS FLOAT) AS real_value, CAST(@P3 AS NVARCHAR(100)) AS text_value, CAST(@P4 AS BIT) AS bool_value, CAST(@P5 AS NVARCHAR(100)) AS null_value",
            expected_bool: DbValue::Bool(true),
        },
    );
}

#[test]
fn mssql_numeric_edges_round_trip() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };

    common::run_numeric_edge_flow(
        &url,
        common::NumericEdgeSql {
            sql: "SELECT CAST(-32768 AS SMALLINT) AS small_value, CAST(2147483647 AS INT) AS int_value, CAST(9223372036854775807 AS BIGINT) AS big_value, CAST(3.5 AS FLOAT) AS real_value, CAST(123.45 AS DECIMAL(10,2)) AS decimal_value",
            expected: vec![
                DbValue::Integer(-32768),
                DbValue::Integer(2_147_483_647),
                DbValue::Integer(9_223_372_036_854_775_807),
                DbValue::Real(3.5),
                DbValue::Real(123.45),
            ],
        },
    );
}

#[test]
fn mssql_alias_url_opens_with_expected_dialect() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };

    common::run_alias_url_flow(&url, "mssql+pyodbc", "mssql", "SELECT CAST(1 AS BIGINT)");
}

#[test]
fn mssql_crud_results_and_errors_work() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mssql_crud");

    common::run_crud_result_and_error_flow(
        &url,
        &table,
        common::CrudSql {
            create_sql: format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name NVARCHAR(100), strength BIGINT)"
            ),
            drop_sql: format!("IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}"),
            insert_sql: format!("INSERT INTO {table} (id, name, strength) VALUES (@P1, @P2, @P3)"),
            select_sql: format!("SELECT name, strength FROM {table} WHERE id = @P1"),
            update_sql: format!("UPDATE {table} SET name = @P1, strength = @P2 WHERE id = @P3"),
            delete_sql: format!("DELETE FROM {table} WHERE id = @P1"),
            count_sql: format!("SELECT COUNT(*) AS count FROM {table}"),
            syntax_error_sql: "SELECT * FROM",
        },
    );
}

#[test]
fn mssql_ddl_edges_work() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mssql_ddl");
    let index = common::unique_table("orm_driver_mssql_ddl_idx");

    common::run_ddl_edge_flow(
        &url,
        &table,
        common::DdlEdgeSql {
            create_table_sql: format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY)"),
            drop_table_sql: format!(
                "IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}"
            ),
            add_column_sql: format!("ALTER TABLE {table} ADD flavor NVARCHAR(100)"),
            create_index_sql: format!("CREATE INDEX {index} ON {table} (flavor)"),
            drop_index_sql: format!("DROP INDEX {index} ON {table}"),
            drop_column_sql: format!("ALTER TABLE {table} DROP COLUMN flavor"),
            insert_sql: format!("INSERT INTO {table} (id, flavor) VALUES (@P1, @P2)"),
            select_by_added_column_sql: format!("SELECT id FROM {table} WHERE flavor = @P1"),
            select_dropped_column_sql: format!("SELECT flavor FROM {table}"),
        },
    );
}

#[test]
fn mssql_insert_output_populates_statement_rows() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mssql_output");

    common::run_returned_rows_flow(
        &url,
        &table,
        common::ReturnedRowsSql {
            create_sql: format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name NVARCHAR(100))"),
            drop_sql: format!("IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}"),
            insert_returning_sql: format!(
                "INSERT INTO {table} (id, name) OUTPUT inserted.id, inserted.name VALUES (@P1, @P2)"
            ),
        },
    );
}

#[test]
fn mssql_constraint_errors_are_mapped() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mssql_constraint");

    common::run_constraint_error_flow(
        &url,
        &table,
        common::ConstraintSql {
            create_sql: format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, name NVARCHAR(100) UNIQUE)"
            ),
            drop_sql: format!("IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}"),
            insert_sql: format!("INSERT INTO {table} (id, name) VALUES (@P1, @P2)"),
            count_sql: format!("SELECT COUNT(*) AS count FROM {table}"),
            commit_after_first_insert: false,
            rollback_after_error: false,
        },
    );
}

#[test]
fn mssql_connection_failures_are_mapped() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };

    common::run_connection_failure_flow(&url);
}

#[test]
fn mssql_reflection_smoke_executes_catalog_queries() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mssql_reflect");

    common::run_reflection_smoke_flow(
        &url,
        &table,
        format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY, name NVARCHAR(100))"),
        format!("IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}"),
    );
}

#[test]
fn mssql_transactions_and_savepoints_work() {
    let Some(url) = url() else {
        eprintln!("skipping mssql driver test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };
    let table = common::unique_table("orm_driver_mssql_tx");

    common::run_transaction_savepoint_flow(
        &url,
        &table,
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY)"),
        format!("IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}"),
        &format!("INSERT INTO {table} (id) VALUES (@P1)"),
        format!("SELECT COUNT(*) AS count FROM {table}"),
    );
}
