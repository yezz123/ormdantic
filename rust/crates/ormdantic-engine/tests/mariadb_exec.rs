use ormdantic_engine::{execute_url, DbValue};

fn mariadb_url() -> Option<String> {
    std::env::var("ORMDANTIC_MARIADB_URL").ok()
}

#[test]
fn mariadb_executes_mysql_protocol_queries_when_url_is_available() {
    let Some(url) = mariadb_url() else {
        eprintln!("skipping mariadb test: ORMDANTIC_MARIADB_URL is not set");
        return;
    };

    execute_url(&url, "DROP TABLE IF EXISTS ormdantic_mariadb_flavors", &[]).unwrap();
    execute_url(
        &url,
        "CREATE TABLE ormdantic_mariadb_flavors (id VARCHAR(64) PRIMARY KEY, name TEXT)",
        &[],
    )
    .unwrap();
    execute_url(
        &url,
        "INSERT INTO ormdantic_mariadb_flavors (id, name) VALUES (?, ?)",
        &[
            DbValue::Text("1".to_string()),
            DbValue::Text("mocha".to_string()),
        ],
    )
    .unwrap();
    let result = execute_url(
        &url,
        "SELECT name FROM ormdantic_mariadb_flavors WHERE id = ?",
        &[DbValue::Text("1".to_string())],
    )
    .unwrap();

    assert_eq!(result.columns(), &["name".to_string()]);
    assert_eq!(result.rows(), &[vec![DbValue::Text("mocha".to_string())]]);
}
