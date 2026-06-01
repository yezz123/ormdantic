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
            "oracle" | "oracledb" | "cx_oracle" => Ok(Self::Oracle),
            other => Err(OrmdanticError::UnsupportedDialect {
                dialect: other.to_string(),
            }),
        }
    }
}

pub trait Dialect {
    fn kind(&self) -> DialectKind;
    fn name(&self) -> &'static str;
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, index: usize) -> String;
    fn supports_returning(&self) -> bool;
    fn supports_native_uuid(&self) -> bool;
    fn supports_json(&self) -> bool;

    fn upsert_conflict_clause(&self, conflict_column: &str, update_columns: &[String]) -> String {
        let target = self.quote_ident(conflict_column);
        if update_columns.is_empty() {
            return format!("ON CONFLICT ({target}) DO NOTHING");
        }

        let assignments = update_columns
            .iter()
            .map(|column| {
                format!(
                    "{} = excluded.{}",
                    self.quote_ident(column),
                    self.quote_ident(column)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("ON CONFLICT ({target}) DO UPDATE SET {assignments}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Sqlite
    }

    fn name(&self) -> &'static str {
        "sqlite"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_double(ident)
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        false
    }

    fn supports_json(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostgresDialect;

impl Dialect for PostgresDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Postgres
    }

    fn name(&self) -> &'static str {
        "postgresql"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_double(ident)
    }

    fn placeholder(&self, index: usize) -> String {
        format!("${index}")
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        true
    }

    fn supports_json(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MySqlDialect;

impl Dialect for MySqlDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::MySql
    }

    fn name(&self) -> &'static str {
        "mysql"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_backtick(ident)
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn supports_returning(&self) -> bool {
        false
    }

    fn supports_native_uuid(&self) -> bool {
        false
    }

    fn supports_json(&self) -> bool {
        true
    }

    fn upsert_conflict_clause(&self, _conflict_column: &str, update_columns: &[String]) -> String {
        if update_columns.is_empty() {
            return "ON DUPLICATE KEY UPDATE 1 = 1".to_string();
        }
        let assignments = update_columns
            .iter()
            .map(|column| {
                format!(
                    "{} = VALUES({})",
                    self.quote_ident(column),
                    self.quote_ident(column)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("ON DUPLICATE KEY UPDATE {assignments}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MariaDbDialect;

impl Dialect for MariaDbDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::MariaDb
    }

    fn name(&self) -> &'static str {
        "mariadb"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_backtick(ident)
    }

    fn placeholder(&self, _index: usize) -> String {
        "?".to_string()
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        false
    }

    fn supports_json(&self) -> bool {
        true
    }

    fn upsert_conflict_clause(&self, conflict_column: &str, update_columns: &[String]) -> String {
        MySqlDialect.upsert_conflict_clause(conflict_column, update_columns)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsSqlDialect;

impl Dialect for MsSqlDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::MsSql
    }

    fn name(&self) -> &'static str {
        "mssql"
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("[{}]", ident.replace(']', "]]"))
    }

    fn placeholder(&self, index: usize) -> String {
        format!("@P{index}")
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        true
    }

    fn supports_json(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleDialect;

impl Dialect for OracleDialect {
    fn kind(&self) -> DialectKind {
        DialectKind::Oracle
    }

    fn name(&self) -> &'static str {
        "oracle"
    }

    fn quote_ident(&self, ident: &str) -> String {
        quote_double(ident)
    }

    fn placeholder(&self, index: usize) -> String {
        format!(":{index}")
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn supports_native_uuid(&self) -> bool {
        false
    }

    fn supports_json(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone, Copy)]
pub enum AnyDialect {
    Sqlite(SqliteDialect),
    Postgres(PostgresDialect),
    MySql(MySqlDialect),
    MariaDb(MariaDbDialect),
    MsSql(MsSqlDialect),
    Oracle(OracleDialect),
}

impl AnyDialect {
    pub fn parse(name: &str) -> OrmdanticResult<Self> {
        Ok(match DialectKind::parse(name)? {
            DialectKind::Sqlite => Self::Sqlite(SqliteDialect),
            DialectKind::Postgres => Self::Postgres(PostgresDialect),
            DialectKind::MySql => Self::MySql(MySqlDialect),
            DialectKind::MariaDb => Self::MariaDb(MariaDbDialect),
            DialectKind::MsSql => Self::MsSql(MsSqlDialect),
            DialectKind::Oracle => Self::Oracle(OracleDialect),
        })
    }
}

impl Dialect for AnyDialect {
    fn kind(&self) -> DialectKind {
        match self {
            Self::Sqlite(dialect) => dialect.kind(),
            Self::Postgres(dialect) => dialect.kind(),
            Self::MySql(dialect) => dialect.kind(),
            Self::MariaDb(dialect) => dialect.kind(),
            Self::MsSql(dialect) => dialect.kind(),
            Self::Oracle(dialect) => dialect.kind(),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Sqlite(dialect) => dialect.name(),
            Self::Postgres(dialect) => dialect.name(),
            Self::MySql(dialect) => dialect.name(),
            Self::MariaDb(dialect) => dialect.name(),
            Self::MsSql(dialect) => dialect.name(),
            Self::Oracle(dialect) => dialect.name(),
        }
    }

    fn quote_ident(&self, ident: &str) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.quote_ident(ident),
            Self::Postgres(dialect) => dialect.quote_ident(ident),
            Self::MySql(dialect) => dialect.quote_ident(ident),
            Self::MariaDb(dialect) => dialect.quote_ident(ident),
            Self::MsSql(dialect) => dialect.quote_ident(ident),
            Self::Oracle(dialect) => dialect.quote_ident(ident),
        }
    }

    fn placeholder(&self, index: usize) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.placeholder(index),
            Self::Postgres(dialect) => dialect.placeholder(index),
            Self::MySql(dialect) => dialect.placeholder(index),
            Self::MariaDb(dialect) => dialect.placeholder(index),
            Self::MsSql(dialect) => dialect.placeholder(index),
            Self::Oracle(dialect) => dialect.placeholder(index),
        }
    }

    fn supports_returning(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_returning(),
            Self::Postgres(dialect) => dialect.supports_returning(),
            Self::MySql(dialect) => dialect.supports_returning(),
            Self::MariaDb(dialect) => dialect.supports_returning(),
            Self::MsSql(dialect) => dialect.supports_returning(),
            Self::Oracle(dialect) => dialect.supports_returning(),
        }
    }

    fn supports_native_uuid(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_native_uuid(),
            Self::Postgres(dialect) => dialect.supports_native_uuid(),
            Self::MySql(dialect) => dialect.supports_native_uuid(),
            Self::MariaDb(dialect) => dialect.supports_native_uuid(),
            Self::MsSql(dialect) => dialect.supports_native_uuid(),
            Self::Oracle(dialect) => dialect.supports_native_uuid(),
        }
    }

    fn supports_json(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_json(),
            Self::Postgres(dialect) => dialect.supports_json(),
            Self::MySql(dialect) => dialect.supports_json(),
            Self::MariaDb(dialect) => dialect.supports_json(),
            Self::MsSql(dialect) => dialect.supports_json(),
            Self::Oracle(dialect) => dialect.supports_json(),
        }
    }

    fn upsert_conflict_clause(&self, conflict_column: &str, update_columns: &[String]) -> String {
        match self {
            Self::Sqlite(dialect) => {
                dialect.upsert_conflict_clause(conflict_column, update_columns)
            }
            Self::Postgres(dialect) => {
                dialect.upsert_conflict_clause(conflict_column, update_columns)
            }
            Self::MySql(dialect) => dialect.upsert_conflict_clause(conflict_column, update_columns),
            Self::MariaDb(dialect) => {
                dialect.upsert_conflict_clause(conflict_column, update_columns)
            }
            Self::MsSql(dialect) => dialect.upsert_conflict_clause(conflict_column, update_columns),
            Self::Oracle(dialect) => {
                dialect.upsert_conflict_clause(conflict_column, update_columns)
            }
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

fn quote_double(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn quote_backtick(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_dialect_name, AnyDialect, Dialect, MariaDbDialect, MsSqlDialect, MySqlDialect,
        OracleDialect, PostgresDialect, SqliteDialect,
    };

    #[test]
    fn quotes_identifiers() {
        assert_eq!(SqliteDialect.quote_ident("user"), "\"user\"");
        assert_eq!(
            PostgresDialect.quote_ident("weird\"name"),
            "\"weird\"\"name\""
        );
    }

    #[test]
    fn renders_placeholders() {
        assert_eq!(SqliteDialect.placeholder(1), "?");
        assert_eq!(PostgresDialect.placeholder(2), "$2");
    }

    #[test]
    fn parses_supported_dialects() {
        assert_eq!(AnyDialect::parse("sqlite").unwrap().name(), "sqlite");
        assert_eq!(
            AnyDialect::parse("postgresql+asyncpg").unwrap().name(),
            "postgresql"
        );
        assert_eq!(
            AnyDialect::parse("postgresql+asyncpg://user:pass@host/db")
                .unwrap()
                .name(),
            "postgresql"
        );
        assert_eq!(
            AnyDialect::parse("mysql+pymysql://host/db").unwrap().name(),
            "mysql"
        );
        assert_eq!(
            AnyDialect::parse("mariadb+mariadbconnector://host/db")
                .unwrap()
                .name(),
            "mariadb"
        );
        assert_eq!(
            AnyDialect::parse("mssql+pyodbc://host/db").unwrap().name(),
            "mssql"
        );
        assert_eq!(
            AnyDialect::parse("oracle+oracledb://host/db")
                .unwrap()
                .name(),
            "oracle"
        );
    }

    #[test]
    fn rejects_unknown_dialects() {
        let error = AnyDialect::parse("db2").expect_err("dialect should fail");

        assert_eq!(error.to_string(), "dialect 'db2' is not supported");
    }

    #[test]
    fn renders_upsert_conflict_clauses() {
        assert_eq!(
            SqliteDialect.upsert_conflict_clause("id", &["name".to_string()]),
            "ON CONFLICT (\"id\") DO UPDATE SET \"name\" = excluded.\"name\""
        );
        assert_eq!(
            PostgresDialect.upsert_conflict_clause("id", &[]),
            "ON CONFLICT (\"id\") DO NOTHING"
        );
        assert_eq!(
            MySqlDialect.upsert_conflict_clause("id", &["name".to_string()]),
            "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
        assert_eq!(
            MariaDbDialect.upsert_conflict_clause("id", &["name".to_string()]),
            "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
        );
    }

    #[test]
    fn normalizes_sqlalchemy_connection_strings() {
        assert_eq!(
            normalize_dialect_name("postgresql+asyncpg://user:pass@localhost/db"),
            "postgresql"
        );
        assert_eq!(
            normalize_dialect_name("mysql-connector://localhost/db"),
            "mysqlconnector"
        );
    }

    #[test]
    fn renders_additional_dialect_placeholders() {
        assert_eq!(MySqlDialect.placeholder(1), "?");
        assert_eq!(MariaDbDialect.placeholder(1), "?");
        assert_eq!(MsSqlDialect.placeholder(1), "@P1");
        assert_eq!(OracleDialect.placeholder(3), ":3");
    }
}
