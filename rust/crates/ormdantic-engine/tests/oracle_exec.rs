use ormdantic_engine::{execute_url, DbValue};

fn oracle_url() -> Option<String> {
    std::env::var("ORMDANTIC_ORACLE_URL").ok()
}

#[test]
fn oracle_reports_optional_runtime_until_client_is_enabled() {
    let Some(url) = oracle_url() else {
        eprintln!("skipping oracle test: ORMDANTIC_ORACLE_URL is not set");
        return;
    };

    let error = execute_url(&url, "SELECT 1 FROM dual", &[DbValue::Integer(1)])
        .expect_err("oracle runtime is currently feature-gated until client libraries are enabled");

    assert!(error.to_string().contains("oracle runtime requires"));
}
