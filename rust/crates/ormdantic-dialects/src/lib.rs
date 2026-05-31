use ormdantic_core::{OrmdanticError, OrmdanticResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialectKind {
    Sqlite,
    Postgres,
}

impl DialectKind {
    pub fn parse(name: &str) -> OrmdanticResult<Self> {
        match name {
            "sqlite" | "sqlite3" => Ok(Self::Sqlite),
            "postgres" | "postgresql" | "postgresql+asyncpg" => Ok(Self::Postgres),
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

#[derive(Debug, Clone, Copy)]
pub enum AnyDialect {
    Sqlite(SqliteDialect),
    Postgres(PostgresDialect),
}

impl AnyDialect {
    pub fn parse(name: &str) -> OrmdanticResult<Self> {
        Ok(match DialectKind::parse(name)? {
            DialectKind::Sqlite => Self::Sqlite(SqliteDialect),
            DialectKind::Postgres => Self::Postgres(PostgresDialect),
        })
    }
}

impl Dialect for AnyDialect {
    fn kind(&self) -> DialectKind {
        match self {
            Self::Sqlite(dialect) => dialect.kind(),
            Self::Postgres(dialect) => dialect.kind(),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Sqlite(dialect) => dialect.name(),
            Self::Postgres(dialect) => dialect.name(),
        }
    }

    fn quote_ident(&self, ident: &str) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.quote_ident(ident),
            Self::Postgres(dialect) => dialect.quote_ident(ident),
        }
    }

    fn placeholder(&self, index: usize) -> String {
        match self {
            Self::Sqlite(dialect) => dialect.placeholder(index),
            Self::Postgres(dialect) => dialect.placeholder(index),
        }
    }

    fn supports_returning(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_returning(),
            Self::Postgres(dialect) => dialect.supports_returning(),
        }
    }

    fn supports_native_uuid(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_native_uuid(),
            Self::Postgres(dialect) => dialect.supports_native_uuid(),
        }
    }

    fn supports_json(&self) -> bool {
        match self {
            Self::Sqlite(dialect) => dialect.supports_json(),
            Self::Postgres(dialect) => dialect.supports_json(),
        }
    }
}

fn quote_double(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::{AnyDialect, Dialect, PostgresDialect, SqliteDialect};

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
    }

    #[test]
    fn rejects_unknown_dialects() {
        let error = AnyDialect::parse("oracle").expect_err("dialect should fail");

        assert_eq!(error.to_string(), "dialect 'oracle' is not supported");
    }
}
