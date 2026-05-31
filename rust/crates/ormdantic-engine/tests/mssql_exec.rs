use ormdantic_engine::{execute_url, DbValue};

fn mssql_url() -> Option<String> {
    std::env::var("ORMDANTIC_MSSQL_URL").ok()
}

#[test]
fn mssql_reports_optional_runtime_until_driver_is_enabled() {
    let Some(url) = mssql_url() else {
        eprintln!("skipping mssql test: ORMDANTIC_MSSQL_URL is not set");
        return;
    };

    let error = execute_url(&url, "SELECT 1", &[DbValue::Integer(1)])
        .expect_err("mssql runtime is currently feature-gated until the native driver is enabled");

    assert!(error.to_string().contains("mssql runtime requires"));
}
