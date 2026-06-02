mod drivers;
mod result;
mod url;
mod value;

use ormdantic_core::{
    IsolationLevel, OrmdanticError, OrmdanticResult, RevisionId, SavepointName, TransactionOptions,
};
use ormdantic_dialects::{AnyDialect, Dialect, DialectKind, ReflectionQuery, ReflectionScope};
use ormdantic_schema::{ReflectedSchema, SchemaDef};

pub use result::QueryResult;
pub use value::DbValue;

#[derive(Debug, Clone, PartialEq)]
pub struct StatementResult {
    row_count: u64,
    last_insert_id: Option<DbValue>,
    returned_rows: Vec<Vec<DbValue>>,
    columns: Vec<String>,
}

impl StatementResult {
    pub fn new(
        row_count: u64,
        last_insert_id: Option<DbValue>,
        returned_rows: Vec<Vec<DbValue>>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            row_count,
            last_insert_id,
            returned_rows,
            columns,
        }
    }

    pub fn from_query_result(result: QueryResult) -> Self {
        let row_count = result.rows().len() as u64;
        Self {
            row_count,
            last_insert_id: None,
            returned_rows: result.rows().to_vec(),
            columns: result.columns().to_vec(),
        }
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn last_insert_id(&self) -> Option<&DbValue> {
        self.last_insert_id.as_ref()
    }

    pub fn returned_rows(&self) -> &[Vec<DbValue>] {
        &self.returned_rows
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Idle,
    InTransaction,
    Failed,
    Unknown,
}

pub trait Connection {
    fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<StatementResult>;
    fn query(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult>;
    fn execute_batch(&mut self, statements: &[String]) -> OrmdanticResult<Vec<StatementResult>>;
    fn begin_with(&mut self, options: TransactionOptions) -> OrmdanticResult<()>;
    fn set_isolation(&mut self, isolation_level: IsolationLevel) -> OrmdanticResult<()>;
    fn begin_nested(&mut self, name: SavepointName) -> OrmdanticResult<()>;
    fn transaction_state(&self) -> TransactionState;
}

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

    pub fn statement(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<StatementResult> {
        self.execute(sql, params)
            .map(StatementResult::from_query_result)
    }

    pub fn begin(&mut self) -> OrmdanticResult<()> {
        match self {
            Self::MySql(connection) | Self::MariaDb(connection) => connection.begin(),
            Self::MsSql(connection) => connection.begin(),
            Self::Oracle(connection) => connection.begin(),
            _ => self.execute("BEGIN", &[]).map(|_| ()),
        }
    }

    pub fn begin_with(&mut self, options: TransactionOptions) -> OrmdanticResult<()> {
        let dialect = AnyDialect::parse(self.dialect())?;
        for statement in dialect.begin_transaction_sql(&options) {
            if !statement.is_empty() {
                self.execute(&statement, &[])?;
            }
        }
        Ok(())
    }

    pub fn set_isolation(&mut self, isolation_level: IsolationLevel) -> OrmdanticResult<()> {
        let dialect = AnyDialect::parse(self.dialect())?;
        self.execute(&dialect.set_isolation_sql(isolation_level), &[])
            .map(|_| ())
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
        let name = SavepointName::new(name.to_string())?;
        self.begin_nested(name)
    }

    pub fn begin_nested(&mut self, name: SavepointName) -> OrmdanticResult<()> {
        let dialect = AnyDialect::parse(self.dialect())?;
        let sql = dialect.savepoint_sql(&name);
        if sql.is_empty() {
            Ok(())
        } else {
            self.execute(&sql, &[]).map(|_| ())
        }
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        let name = SavepointName::new(name.to_string())?;
        let dialect = AnyDialect::parse(self.dialect())?;
        let sql = dialect.rollback_to_savepoint_sql(&name);
        if sql.is_empty() {
            Ok(())
        } else {
            self.execute(&sql, &[]).map(|_| ())
        }
    }

    pub fn release_savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        let name = SavepointName::new(name.to_string())?;
        let dialect = AnyDialect::parse(self.dialect())?;
        let sql = dialect.release_savepoint_sql(&name);
        if sql.is_empty() {
            Ok(())
        } else {
            self.execute(&sql, &[]).map(|_| ())
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

impl Connection for NativeConnection {
    fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<StatementResult> {
        self.statement(sql, params)
    }

    fn query(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        NativeConnection::execute(self, sql, params)
    }

    fn execute_batch(&mut self, statements: &[String]) -> OrmdanticResult<Vec<StatementResult>> {
        statements
            .iter()
            .map(|statement| self.statement(statement, &[]))
            .collect()
    }

    fn begin_with(&mut self, options: TransactionOptions) -> OrmdanticResult<()> {
        NativeConnection::begin_with(self, options)
    }

    fn set_isolation(&mut self, isolation_level: IsolationLevel) -> OrmdanticResult<()> {
        NativeConnection::set_isolation(self, isolation_level)
    }

    fn begin_nested(&mut self, name: SavepointName) -> OrmdanticResult<()> {
        NativeConnection::begin_nested(self, name)
    }

    fn transaction_state(&self) -> TransactionState {
        TransactionState::Unknown
    }
}

#[derive(Debug, Clone)]
pub struct Reflector {
    dialect: AnyDialect,
}

impl Reflector {
    pub fn new(dialect: AnyDialect) -> Self {
        Self { dialect }
    }

    pub fn for_url(url: &str) -> OrmdanticResult<Self> {
        Ok(Self {
            dialect: AnyDialect::parse(url)?,
        })
    }

    pub fn reflection_queries(&self, scope: &ReflectionScope) -> Vec<ReflectionQuery> {
        self.dialect.reflection_queries(scope)
    }

    pub fn empty_schema(&self) -> SchemaDef {
        ReflectedSchema::new().into_schema_def()
    }
}

pub struct Inspector<'a> {
    connection: &'a mut NativeConnection,
}

impl<'a> Inspector<'a> {
    pub fn new(connection: &'a mut NativeConnection) -> Self {
        Self { connection }
    }

    pub fn reflection_queries(
        &self,
        scope: &ReflectionScope,
    ) -> OrmdanticResult<Vec<ReflectionQuery>> {
        Ok(AnyDialect::parse(self.connection.dialect())?.reflection_queries(scope))
    }

    pub fn inspect(&mut self, scope: &ReflectionScope) -> OrmdanticResult<ReflectedSchema> {
        for query in self.reflection_queries(scope)? {
            let _ = self.connection.execute(query.sql(), &[])?;
        }
        Ok(ReflectedSchema::new())
    }
}

pub struct MigrationStore<'a> {
    connection: &'a mut NativeConnection,
    table_name: String,
}

impl<'a> MigrationStore<'a> {
    pub fn new(connection: &'a mut NativeConnection) -> Self {
        Self {
            connection,
            table_name: "ormdantic_migrations".to_string(),
        }
    }

    pub fn with_table_name(mut self, table_name: impl Into<String>) -> Self {
        self.table_name = table_name.into();
        self
    }

    pub fn ensure(&mut self) -> OrmdanticResult<()> {
        let dialect = AnyDialect::parse(self.connection.dialect())?;
        self.connection.execute(
            &format!(
                "CREATE TABLE IF NOT EXISTS {} (revision TEXT PRIMARY KEY, applied_at TEXT NOT NULL)",
                dialect.quote_ident(&self.table_name)
            ),
            &[],
        )?;
        Ok(())
    }

    pub fn record_revision(&mut self, revision: &RevisionId) -> OrmdanticResult<()> {
        self.ensure()?;
        let dialect = AnyDialect::parse(self.connection.dialect())?;
        self.connection.execute(
            &format!(
                "INSERT INTO {} (revision, applied_at) VALUES ({}, CURRENT_TIMESTAMP)",
                dialect.quote_ident(&self.table_name),
                dialect.placeholder(1)
            ),
            &[DbValue::Text(revision.as_str().to_string())],
        )?;
        Ok(())
    }

    pub fn revisions(&mut self) -> OrmdanticResult<Vec<RevisionId>> {
        self.ensure()?;
        let dialect = AnyDialect::parse(self.connection.dialect())?;
        let result = self.connection.execute(
            &format!(
                "SELECT revision FROM {} ORDER BY applied_at",
                dialect.quote_ident(&self.table_name)
            ),
            &[],
        )?;
        result
            .rows()
            .iter()
            .filter_map(|row| match row.first() {
                Some(DbValue::Text(value)) => Some(RevisionId::new(value.clone())),
                _ => None,
            })
            .collect()
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
    OrmdanticError::ExecutionError {
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
