#![allow(dead_code)]

use std::time::{SystemTime, UNIX_EPOCH};

use ormdantic_engine::{DbValue, QueryResult};

pub fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

pub fn sqlite_url(name: &str) -> String {
    let path = std::env::temp_dir().join(format!("{name}.sqlite3"));
    let _ = std::fs::remove_file(&path);
    format!("sqlite:///{}", path.display())
}

pub fn env_url(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

pub fn assert_rows(result: &QueryResult, expected: &[Vec<DbValue>]) {
    assert_eq!(result.rows(), expected);
}
