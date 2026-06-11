use ormdantic_core::{IsolationLevel, OrmdanticResult, SavepointName, TransactionOptions};
use ormdantic_dialects::{AnyDialect, Dialect, DialectKind};

use crate::{drivers, DbValue, QueryResult, StatementResult};

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
            Self::Sqlite(connection) => connection.execute(sql, params),
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
        if let Self::Oracle(connection) = self {
            connection.begin()?;
            for statement in dialect.begin_transaction_sql(&options) {
                if !statement.is_empty() {
                    connection.execute(&statement, &[])?;
                }
            }
            return Ok(());
        }
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
            Self::MsSql(connection) => connection.commit(),
            Self::Oracle(connection) => connection.commit(),
            _ => self.execute("COMMIT", &[]).map(|_| ()),
        }
    }

    pub fn rollback(&mut self) -> OrmdanticResult<()> {
        match self {
            Self::MySql(connection) | Self::MariaDb(connection) => connection.rollback(),
            Self::MsSql(connection) => connection.rollback(),
            Self::Oracle(connection) => connection.rollback(),
            _ => self.execute("ROLLBACK", &[]).map(|_| ()),
        }
    }

    pub fn savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        let name = SavepointName::new(name.to_string())?;
        self.begin_nested(name)
    }

    pub fn begin_nested(&mut self, name: SavepointName) -> OrmdanticResult<()> {
        match self {
            Self::MySql(connection) | Self::MariaDb(connection) => {
                return connection.savepoint(name.as_str());
            }
            Self::MsSql(connection) => return connection.savepoint(name.as_str()),
            _ => {}
        }

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
        match self {
            Self::MySql(connection) | Self::MariaDb(connection) => {
                return connection.rollback_to_savepoint(name.as_str());
            }
            Self::MsSql(connection) => return connection.rollback_to_savepoint(name.as_str()),
            _ => {}
        }

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
        if let Self::MySql(connection) | Self::MariaDb(connection) = self {
            return connection.release_savepoint(name.as_str());
        }

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
