use ormdantic_engine::{execute_url, DbValue};

fn oracle_url() -> Option<String> {
    std::env::var("ORMDANTIC_ORACLE_URL").ok()
}

#[test]
fn oracle_executes_parameterized_queries_when_url_is_available() {
    let Some(url) = oracle_url() else {
        eprintln!("skipping oracle test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };

    let result = execute_url(
        &url,
        "SELECT :1 AS value FROM dual",
        &[DbValue::Integer(42)],
    )
    .expect("oracle should execute parameterized SELECT");

    assert_eq!(result.rows(), &[vec![DbValue::Integer(42)]]);
}

#[test]
fn oracle_connection_supports_transactions_when_url_is_available() {
    let Some(url) = oracle_url() else {
        eprintln!("skipping oracle transaction test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };

    let table = format!("ORM_TX_{}", std::process::id());
    let _ = execute_url(&url, &format!("DROP TABLE {table}"), &[]);
    execute_url(
        &url,
        &format!("CREATE TABLE {table} (id NUMBER PRIMARY KEY, name VARCHAR2(50))"),
        &[],
    )
    .expect("oracle should create transaction test table");

    let mut connection = ormdantic_engine::NativeConnection::open(&url)
        .expect("oracle native connection should open");
    connection.begin().expect("oracle begin should work");
    connection
        .execute(
            &format!("INSERT INTO {table} (id, name) VALUES (:1, :2)"),
            &[DbValue::Integer(1), DbValue::Text("vanilla".to_string())],
        )
        .expect("oracle insert in transaction should work");
    connection.rollback().expect("oracle rollback should work");

    let result = execute_url(&url, &format!("SELECT COUNT(*) AS count FROM {table}"), &[])
        .expect("oracle count should work");
    assert_eq!(result.rows(), &[vec![DbValue::Integer(0)]]);

    execute_url(&url, &format!("DROP TABLE {table}"), &[]).expect("oracle cleanup should work");
}
