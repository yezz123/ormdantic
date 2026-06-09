use ormdantic_core::{OrmdanticError, OrmdanticResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectKind {
    Sqlite,
    Postgres,
    MySql,
    MariaDb,
    MsSql,
    Oracle,
}

impl DialectKind {
    pub fn parse(name: &str) -> OrmdanticResult<Self> {
        let normalized = normalize_dialect_name(name);
        match normalized.as_str() {
            "sqlite" | "sqlite3" | "aiosqlite" => Ok(Self::Sqlite),
            "postgres" | "postgresql" | "asyncpg" | "psycopg" | "psycopg2" | "pg8000" => {
                Ok(Self::Postgres)
            }
            "mysql" | "pymysql" | "mysqlconnector" | "aiomysql" | "asyncmy" => Ok(Self::MySql),
            "mariadb" | "mariadbconnector" => Ok(Self::MariaDb),
            "mssql" | "pyodbc" | "pymssql" | "aioodbc" => Ok(Self::MsSql),
            "oracle" | "oracledb" | "cxoracle" => Ok(Self::Oracle),
            other => Err(OrmdanticError::UnsupportedDialect {
                dialect: other.to_string(),
            }),
        }
    }
}

pub fn normalize_dialect_name(name_or_url: &str) -> String {
    let lower = name_or_url.trim().to_ascii_lowercase();
    let before_url = lower
        .split_once("://")
        .map_or(lower.as_str(), |(scheme, _)| scheme);
    before_url
        .split('+')
        .next()
        .unwrap_or(before_url)
        .replace(['-', '_'], "")
}
