use ormdantic_core::{ExecutionErrorKind, OrmdanticError, OrmdanticResult};
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
    let message = error.to_string();
    OrmdanticError::ExecutionError {
        kind: classify_sql_error_message(&message),
        message,
    }
}

fn classify_sql_error_message(message: &str) -> ExecutionErrorKind {
    let normalized = message.to_ascii_lowercase();
    if contains_any(
        &normalized,
        &[
            "timeout",
            "timed out",
            "lock wait timeout",
            "query timeout",
            "ora-01013",
        ],
    ) {
        return ExecutionErrorKind::Timeout;
    }
    if contains_any(
        &normalized,
        &[
            "could not serialize",
            "serialization failure",
            "deadlock detected",
            "deadlock",
            "40001",
            "40p01",
        ],
    ) {
        return ExecutionErrorKind::SerializationFailure;
    }
    if contains_any(
        &normalized,
        &[
            "permission denied",
            "not authorized",
            "insufficient privileges",
            "ora-01031",
        ],
    ) {
        return ExecutionErrorKind::PermissionDenied;
    }
    if contains_any(
        &normalized,
        &[
            "connection refused",
            "could not connect",
            "no route to host",
            "password authentication failed",
            "login failed",
            "access denied for user",
            "invalid authorization specification",
            "ora-01017",
        ],
    ) {
        return ExecutionErrorKind::Connection;
    }
    if contains_any(
        &normalized,
        &[
            "unique constraint failed",
            "duplicate key",
            "duplicate entry",
            "unique key constraint",
            "violation of unique key constraint",
            "ora-00001",
        ],
    ) {
        return ExecutionErrorKind::UniqueViolation;
    }
    if contains_any(
        &normalized,
        &[
            "foreign key constraint failed",
            "foreign key constraint fails",
            "violates foreign key constraint",
            "conflicted with the foreign key constraint",
            "the insert statement conflicted with the foreign key constraint",
            "ora-02291",
            "ora-02292",
        ],
    ) {
        return ExecutionErrorKind::ForeignKeyViolation;
    }
    if contains_any(
        &normalized,
        &[
            "not null constraint failed",
            "null value in column",
            "cannot insert the value null",
            "cannot insert null",
            "ora-01400",
        ],
    ) {
        return ExecutionErrorKind::NotNullViolation;
    }
    if contains_any(
        &normalized,
        &[
            "check constraint failed",
            "violates check constraint",
            "check constraint",
            "ora-02290",
        ],
    ) {
        return ExecutionErrorKind::CheckViolation;
    }
    if contains_any(
        &normalized,
        &[
            "syntax error",
            "sql syntax",
            "incomplete input",
            "incorrect syntax near",
            "unexpected end of command",
            "ora-00900",
            "ora-00905",
            "ora-00907",
            "ora-00923",
            "ora-00933",
        ],
    ) {
        return ExecutionErrorKind::Syntax;
    }
    ExecutionErrorKind::Unknown
}

fn contains_any(value: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| value.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::{classify_sql_error_message, ExecutionErrorKind};

    #[test]
    fn classifies_common_backend_execution_errors() {
        let cases = [
            (
                "ERROR: duplicate key value violates unique constraint \"flavors_name_key\"",
                ExecutionErrorKind::UniqueViolation,
            ),
            (
                "Duplicate entry 'vanilla' for key 'flavors.name'",
                ExecutionErrorKind::UniqueViolation,
            ),
            (
                "UNIQUE constraint failed: flavors.name",
                ExecutionErrorKind::UniqueViolation,
            ),
            (
                "insert or update on table violates foreign key constraint",
                ExecutionErrorKind::ForeignKeyViolation,
            ),
            (
                "The INSERT statement conflicted with the FOREIGN KEY constraint",
                ExecutionErrorKind::ForeignKeyViolation,
            ),
            (
                "NOT NULL constraint failed: flavors.name",
                ExecutionErrorKind::NotNullViolation,
            ),
            (
                "new row for relation violates check constraint",
                ExecutionErrorKind::CheckViolation,
            ),
            ("near \"FROM\": syntax error", ExecutionErrorKind::Syntax),
            (
                "ORA-00001: unique constraint violated",
                ExecutionErrorKind::UniqueViolation,
            ),
            (
                "ORA-02291: integrity constraint violated - parent key not found",
                ExecutionErrorKind::ForeignKeyViolation,
            ),
            (
                "ORA-01400: cannot insert NULL",
                ExecutionErrorKind::NotNullViolation,
            ),
            (
                "ORA-00923: FROM keyword not found where expected",
                ExecutionErrorKind::Syntax,
            ),
            (
                "password authentication failed for user",
                ExecutionErrorKind::Connection,
            ),
            (
                "deadlock detected",
                ExecutionErrorKind::SerializationFailure,
            ),
            ("query timeout expired", ExecutionErrorKind::Timeout),
            (
                "permission denied for table flavors",
                ExecutionErrorKind::PermissionDenied,
            ),
        ];

        for (message, expected) in cases {
            assert_eq!(classify_sql_error_message(message), expected);
        }
    }
}
