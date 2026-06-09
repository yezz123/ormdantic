use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_dialects::DialectKind;

use crate::{drivers, DbValue, QueryResult};

pub fn runtime_capabilities() -> [(&'static str, bool); 6] {
    [
        ("sqlite", cfg!(feature = "sqlite")),
        ("postgresql", cfg!(feature = "postgres")),
        ("mysql", cfg!(feature = "mysql")),
        (
            "mariadb",
            cfg!(feature = "mysql") || cfg!(feature = "mariadb"),
        ),
        ("mssql", cfg!(feature = "mssql")),
        ("oracle", cfg!(feature = "oracle")),
    ]
}

pub fn execute_url(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    match DialectKind::parse(url)? {
        DialectKind::Sqlite => drivers::sqlite::execute_url(url, sql, params),
        DialectKind::Postgres => drivers::postgres::execute_url(url, sql, params),
        DialectKind::MySql => drivers::mysql::execute_url(url, sql, params),
        DialectKind::MariaDb => drivers::mysql::execute_url(url, sql, params),
        DialectKind::MsSql => drivers::mssql::execute_url(url, sql, params),
        DialectKind::Oracle => drivers::oracle::execute_url(url, sql, params),
    }
}

pub fn returns_rows(sql: &str) -> bool {
    let normalized = sql.trim_start().to_ascii_lowercase();
    let first_token = normalized.split_whitespace().next().unwrap_or_default();
    if matches!(first_token, "select" | "with") {
        return true;
    }
    matches!(first_token, "insert" | "update" | "delete" | "merge")
        && (contains_sql_keyword(&normalized, "returning")
            || contains_sql_keyword(&normalized, "output"))
}

fn contains_sql_keyword(sql: &str, keyword: &str) -> bool {
    sql.split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .any(|token| token == keyword)
}

pub fn sql_error(error: impl std::fmt::Display) -> OrmdanticError {
    OrmdanticError::ExecutionError {
        message: error.to_string(),
    }
}
