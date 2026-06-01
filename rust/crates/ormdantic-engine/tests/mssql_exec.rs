use ormdantic_engine::{execute_url, DbValue};

fn mssql_url() -> Option<String> {
    std::env::var("ORMDANTIC_MSSQL_URL").ok()
}

#[test]
fn mssql_executes_parameterized_queries_when_url_is_available() {
    let Some(url) = mssql_url() else {
        eprintln!("skipping mssql test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };

    let result = execute_url(&url, "SELECT @P1 AS value", &[DbValue::Integer(42)])
        .expect("mssql should execute parameterized SELECT");

    assert_eq!(result.columns(), &["value".to_string()]);
    assert_eq!(result.rows(), &[vec![DbValue::Integer(42)]]);
}

#[test]
fn mssql_connection_supports_transactions_when_url_is_available() {
    let Some(url) = mssql_url() else {
        eprintln!("skipping mssql transaction test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };

    let table = "ormdantic_mssql_runtime_tx";
    execute_url(
        &url,
        &format!(
            "IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {table}; CREATE TABLE {table} (id INT PRIMARY KEY, name NVARCHAR(50))"
        ),
        &[],
    )
    .expect("mssql should create transaction test table");

    let mut connection = ormdantic_engine::NativeConnection::open(&url)
        .expect("mssql native connection should open");
    connection.begin().expect("mssql begin should work");
    connection
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (@P1, @P2)"),
            &[DbValue::Integer(1), DbValue::Text("vanilla".to_string())],
        )
        .expect("mssql insert in transaction should work");
    connection.rollback().expect("mssql rollback should work");

    let result = execute_url(&url, &format!("SELECT COUNT(*) AS count FROM {table}"), &[])
        .expect("mssql count should work");
    assert_eq!(result.rows(), &[vec![DbValue::Integer(0)]]);

    execute_url(&url, &format!("DROP TABLE {table}"), &[]).expect("mssql cleanup should work");
}
