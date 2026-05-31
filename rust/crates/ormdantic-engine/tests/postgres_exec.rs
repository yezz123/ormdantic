use ormdantic_engine::{execute_url, DbValue, NativeConnection};

fn postgres_url() -> Option<String> {
    std::env::var("ORMDANTIC_POSTGRES_URL").ok()
}

#[test]
fn postgres_executes_parameterized_queries_when_url_is_available() {
    let Some(url) = postgres_url() else {
        eprintln!("skipping postgres test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };

    execute_url(&url, "DROP TABLE IF EXISTS ormdantic_pg_flavors", &[]).unwrap();
    execute_url(
        &url,
        "CREATE TABLE ormdantic_pg_flavors (id TEXT PRIMARY KEY, name TEXT, strength BIGINT)",
        &[],
    )
    .unwrap();
    execute_url(
        &url,
        "INSERT INTO ormdantic_pg_flavors (id, name, strength) VALUES ($1, $2, $3)",
        &[
            DbValue::Text("1".to_string()),
            DbValue::Text("mocha".to_string()),
            DbValue::Integer(3),
        ],
    )
    .unwrap();
    let result = execute_url(
        &url,
        "SELECT name, strength FROM ormdantic_pg_flavors WHERE id = $1",
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
fn postgres_connection_supports_transactions_when_url_is_available() {
    let Some(url) = postgres_url() else {
        eprintln!("skipping postgres test: ORMDANTIC_POSTGRES_URL is not set");
        return;
    };

    execute_url(&url, "DROP TABLE IF EXISTS ormdantic_pg_tx", &[]).unwrap();
    execute_url(
        &url,
        "CREATE TABLE ormdantic_pg_tx (id TEXT PRIMARY KEY)",
        &[],
    )
    .unwrap();
    let mut connection = NativeConnection::open(&url).unwrap();
    connection.begin().unwrap();
    connection
        .execute(
            "INSERT INTO ormdantic_pg_tx (id) VALUES ($1)",
            &[DbValue::Text("1".to_string())],
        )
        .unwrap();
    connection.rollback().unwrap();
    let result = execute_url(&url, "SELECT id FROM ormdantic_pg_tx", &[]).unwrap();

    assert!(result.rows().is_empty());
}
