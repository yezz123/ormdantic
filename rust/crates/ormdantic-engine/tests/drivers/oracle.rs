use ormdantic_engine::DbValue;

use super::common;
use crate::support;

fn url() -> Option<String> {
    support::env_url("ORMDANTIC_ORACLE_URL")
}

#[test]
fn oracle_parameterized_selects_cover_core_values() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };

    common::run_value_round_trip_flow(
        &url,
        common::ValueRoundTripSql {
            sql: "SELECT :1 AS int_value, CAST(:2 AS NUMBER(10,2)) AS real_value, :3 AS text_value, CAST(:4 AS NUMBER(1)) AS bool_value, CAST(:5 AS VARCHAR2(100)) AS null_value FROM dual",
            expected_bool: DbValue::Integer(1),
        },
    );
}

#[test]
fn oracle_number_edges_round_trip() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };

    common::run_numeric_edge_flow(
        &url,
        common::NumericEdgeSql {
            sql: "SELECT CAST(-32768 AS NUMBER(5,0)) AS small_value, CAST(2147483647 AS NUMBER(10,0)) AS int_value, CAST(9223372036854775807 AS NUMBER(19,0)) AS big_value, CAST(3.5 AS NUMBER(10,1)) AS real_value, CAST(123.45 AS NUMBER(10,2)) AS decimal_value FROM dual",
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
fn oracle_alias_url_opens_with_expected_dialect() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };

    common::run_alias_url_flow(
        &url,
        "oracle+oracledb",
        "oracle",
        "SELECT 1 AS value FROM dual",
    );
}

#[test]
fn oracle_crud_results_and_errors_work() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };
    let table = common::unique_table_with_limit("od_ora_crud", 30);

    common::run_crud_result_and_error_flow(
        &url,
        &table,
        common::CrudSql {
            create_sql: format!(
                "CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(100), strength NUMBER)"
            ),
            drop_sql: format!("DROP TABLE {table}"),
            insert_sql: format!("INSERT INTO {table} (id, name, strength) VALUES (:1, :2, :3)"),
            select_sql: format!("SELECT name, strength FROM {table} WHERE id = :1"),
            update_sql: format!("UPDATE {table} SET name = :1, strength = :2 WHERE id = :3"),
            delete_sql: format!("DELETE FROM {table} WHERE id = :1"),
            count_sql: format!("SELECT COUNT(*) AS count FROM {table}"),
            syntax_error_sql: "SELECT * FROM",
        },
    );
}

#[test]
fn oracle_ddl_edges_work() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };
    let table = common::unique_table_with_limit("od_ora_ddl", 30);
    let index = common::unique_table_with_limit("od_ora_di", 30);

    common::run_ddl_edge_flow(
        &url,
        &table,
        common::DdlEdgeSql {
            create_table_sql: format!("CREATE TABLE {table} (id NUMBER PRIMARY KEY)"),
            drop_table_sql: format!("DROP TABLE {table}"),
            add_column_sql: format!("ALTER TABLE {table} ADD flavor VARCHAR2(100)"),
            create_index_sql: format!("CREATE INDEX {index} ON {table} (flavor)"),
            drop_index_sql: format!("DROP INDEX {index}"),
            drop_column_sql: format!("ALTER TABLE {table} DROP COLUMN flavor"),
            insert_sql: format!("INSERT INTO {table} (id, flavor) VALUES (:1, :2)"),
            select_by_added_column_sql: format!("SELECT id FROM {table} WHERE flavor = :1"),
            select_dropped_column_sql: format!("SELECT flavor FROM {table}"),
        },
    );
}

#[test]
fn oracle_constraint_errors_are_mapped() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };
    let table = common::unique_table_with_limit("od_ora_cons", 30);

    common::run_constraint_error_flow(
        &url,
        &table,
        common::ConstraintSql {
            create_sql: format!(
                "CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(100) UNIQUE)"
            ),
            drop_sql: format!("DROP TABLE {table}"),
            insert_sql: format!("INSERT INTO {table} (id, name) VALUES (:1, :2)"),
            count_sql: format!("SELECT COUNT(*) AS count FROM {table}"),
            commit_after_first_insert: true,
            rollback_after_error: false,
        },
    );
}

#[test]
fn oracle_connection_failures_are_mapped() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };

    common::run_connection_failure_flow(&url);
}

#[test]
fn oracle_reflection_smoke_executes_catalog_queries() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };
    let table = common::unique_table_with_limit("od_ora_refl", 30);

    common::run_reflection_smoke_flow(
        &url,
        &table,
        format!("CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(100))"),
        format!("DROP TABLE {table}"),
    );
}

#[test]
fn oracle_transactions_and_savepoints_work() {
    let Some(url) = url() else {
        eprintln!("skipping oracle driver test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };
    let table = common::unique_table_with_limit("od_ora_tx", 30);

    common::run_transaction_savepoint_flow(
        &url,
        &table,
        format!("CREATE TABLE {table} (id NUMBER PRIMARY KEY)"),
        format!("DROP TABLE {table}"),
        &format!("INSERT INTO {table} (id) VALUES (:1)"),
        format!("SELECT COUNT(*) AS count FROM {table}"),
    );
}
