use ormdantic_engine::{execute_url, DbValue, NativeConnection};

fn mysql_url() -> Option<String> {
    std::env::var("ORMDANTIC_MYSQL_URL").ok()
}

#[test]
fn mysql_executes_parameterized_queries_when_url_is_available() {
    let Some(url) = mysql_url() else {
        eprintln!("skipping mysql test: ORMDANTIC_MYSQL_URL is not set");
        return;
    };

    execute_url(&url, "DROP TABLE IF EXISTS ormdantic_mysql_flavors", &[]).unwrap();
    execute_url(
        &url,
        "CREATE TABLE ormdantic_mysql_flavors (id VARCHAR(64) PRIMARY KEY, name TEXT, strength BIGINT)",
        &[],
    )
    .unwrap();
    execute_url(
        &url,
        "INSERT INTO ormdantic_mysql_flavors (id, name, strength) VALUES (%s, %s, %s)",
        &[
            DbValue::Text("1".to_string()),
            DbValue::Text("mocha".to_string()),
            DbValue::Integer(3),
        ],
    )
    .unwrap();
    let result = execute_url(
        &url,
        "SELECT name, strength FROM ormdantic_mysql_flavors WHERE id = %s",
        &[DbValue::Text("1".to_string())],
    )
    .unwrap();

    assert_eq!(
        result.columns(),
        &["name".to_string(), "strength".to_string()]
    );
    assert_eq!(
        result.rows(),
        &[vec![
            DbValue::Text("mocha".to_string()),
            DbValue::Integer(3)
        ]]
    );
}

#[test]
fn mysql_connection_supports_transactions_when_url_is_available() {
    let Some(url) = mysql_url() else {
        eprintln!("skipping mysql test: ORMDANTIC_MYSQL_URL is not set");
        return;
    };

    execute_url(&url, "DROP TABLE IF EXISTS ormdantic_mysql_tx", &[]).unwrap();
    execute_url(
        &url,
        "CREATE TABLE ormdantic_mysql_tx (id VARCHAR(64) PRIMARY KEY)",
        &[],
    )
    .unwrap();
    let mut connection = NativeConnection::open(&url).unwrap();
    connection.begin().unwrap();
    connection
        .execute(
            "INSERT INTO ormdantic_mysql_tx (id) VALUES (%s)",
            &[DbValue::Text("1".to_string())],
        )
        .unwrap();
    connection.rollback().unwrap();
    let result = execute_url(&url, "SELECT id FROM ormdantic_mysql_tx", &[]).unwrap();

    assert!(result.rows().is_empty());
}
