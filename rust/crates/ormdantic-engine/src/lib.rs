mod drivers;
mod result;
mod url;
mod value;

use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_dialects::DialectKind;

pub use result::QueryResult;
pub use value::DbValue;

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

pub enum NativeConnection {
    Sqlite(Box<drivers::sqlite::SqliteConnection>),
    Postgres(Box<drivers::postgres::PostgresConnection>),
    MySql(Box<drivers::mysql::MySqlConnection>),
    MariaDb(Box<drivers::mysql::MySqlConnection>),
    MsSql(Box<drivers::mssql::MsSqlConnection>),
    Oracle(Box<drivers::oracle::OracleConnection>),
}

impl NativeConnection {
    pub fn open(url: &str) -> OrmdanticResult<Self> {
        match DialectKind::parse(url)? {
            DialectKind::Sqlite => Ok(Self::Sqlite(Box::new(
                drivers::sqlite::SqliteConnection::open(url)?,
            ))),
            DialectKind::Postgres => Ok(Self::Postgres(Box::new(
                drivers::postgres::PostgresConnection::open(url)?,
            ))),
            DialectKind::MySql => Ok(Self::MySql(Box::new(
                drivers::mysql::MySqlConnection::open(url)?,
            ))),
            DialectKind::MariaDb => Ok(Self::MariaDb(Box::new(
                drivers::mysql::MySqlConnection::open(url)?,
            ))),
            DialectKind::MsSql => Ok(Self::MsSql(Box::new(
                drivers::mssql::MsSqlConnection::open(url)?,
            ))),
            DialectKind::Oracle => Ok(Self::Oracle(Box::new(
                drivers::oracle::OracleConnection::open(url)?,
            ))),
        }
    }

    pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        match self {
            Self::Sqlite(connection) => execute_sqlite_connection(connection, sql, params),
            Self::Postgres(connection) => connection.execute(sql, params),
            Self::MySql(connection) | Self::MariaDb(connection) => connection.execute(sql, params),
            Self::MsSql(connection) => connection.execute(sql, params),
            Self::Oracle(connection) => connection.execute(sql, params),
        }
    }

    pub fn begin(&mut self) -> OrmdanticResult<()> {
        match self {
            Self::MySql(connection) | Self::MariaDb(connection) => connection.begin(),
            Self::MsSql(connection) => connection.begin(),
            Self::Oracle(connection) => connection.begin(),
            _ => self.execute("BEGIN", &[]).map(|_| ()),
        }
    }

    pub fn commit(&mut self) -> OrmdanticResult<()> {
        match self {
            Self::MySql(connection) | Self::MariaDb(connection) => connection.commit(),
            Self::Oracle(connection) => connection.commit(),
            _ => self.execute("COMMIT", &[]).map(|_| ()),
        }
    }

    pub fn rollback(&mut self) -> OrmdanticResult<()> {
        match self {
            Self::MySql(connection) | Self::MariaDb(connection) => connection.rollback(),
            Self::Oracle(connection) => connection.rollback(),
            _ => self.execute("ROLLBACK", &[]).map(|_| ()),
        }
    }

    pub fn savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        match self {
            Self::MySql(connection) | Self::MariaDb(connection) => connection.savepoint(name),
            Self::MsSql(connection) => connection.savepoint(name),
            Self::Oracle(connection) => connection.savepoint(name),
            _ => self.execute(&format!("SAVEPOINT {name}"), &[]).map(|_| ()),
        }
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        match self {
            Self::MsSql(_) => self
                .execute(&format!("ROLLBACK TRANSACTION {name}"), &[])
                .map(|_| ()),
            Self::Oracle(_) => self
                .execute(&format!("ROLLBACK TO SAVEPOINT {name}"), &[])
                .map(|_| ()),
            _ => self
                .execute(&format!("ROLLBACK TO SAVEPOINT {name}"), &[])
                .map(|_| ()),
        }
    }

    pub fn release_savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        match self {
            Self::MsSql(_) | Self::Oracle(_) => Ok(()),
            _ => self
                .execute(&format!("RELEASE SAVEPOINT {name}"), &[])
                .map(|_| ()),
        }
    }
    pub fn dialect(&self) -> &'static str {
        match self {
            Self::Sqlite(_) => "sqlite",
            Self::Postgres(_) => "postgresql",
            Self::MySql(_) => "mysql",
            Self::MariaDb(_) => "mariadb",
            Self::MsSql(_) => "mssql",
            Self::Oracle(_) => "oracle",
        }
    }
}

pub fn returns_rows(sql: &str) -> bool {
    sql.trim_start().to_ascii_lowercase().starts_with("select")
}

fn execute_sqlite_connection(
    connection: &mut drivers::sqlite::SqliteConnection,
    sql: &str,
    params: &[DbValue],
) -> OrmdanticResult<QueryResult> {
    connection.execute(sql, params)
}

pub fn sql_error(error: impl std::fmt::Display) -> OrmdanticError {
    OrmdanticError::SqlCompile {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::execute_url;

    #[test]
    fn executes_sqlite_statements() {
        let url = "sqlite:///:memory:";

        execute_url(
            url,
            "CREATE TABLE flavors (id TEXT PRIMARY KEY, name TEXT)",
            &[],
        )
        .expect("create table should work");
        // In-memory sqlite URLs create separate connections per call, so this
        // test verifies basic statement execution. File-backed behavior is
        // covered by Python integration tests.
    }
}
