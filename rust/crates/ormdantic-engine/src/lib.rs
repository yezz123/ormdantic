use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_dialects::DialectKind;
use postgres::{Client, NoTls};
use rusqlite::types::{ToSqlOutput, Value as RusqliteValue, ValueRef};
use rusqlite::{params_from_iter, Connection, ToSql};

#[derive(Debug, Clone, PartialEq)]
pub enum DbValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Bool(bool),
}

impl ToSql for DbValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Null => ToSqlOutput::Owned(RusqliteValue::Null),
            Self::Integer(value) => ToSqlOutput::Owned(RusqliteValue::Integer(*value)),
            Self::Real(value) => ToSqlOutput::Owned(RusqliteValue::Real(*value)),
            Self::Text(value) => ToSqlOutput::Owned(RusqliteValue::Text(value.clone())),
            Self::Bool(value) => ToSqlOutput::Owned(RusqliteValue::Integer(i64::from(*value))),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<DbValue>>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn new(columns: Vec<String>, rows: Vec<Vec<DbValue>>) -> Self {
        Self { columns, rows }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn rows(&self) -> &[Vec<DbValue>] {
        &self.rows
    }
}

pub fn execute_url(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    match DialectKind::parse(url)? {
        DialectKind::Sqlite => execute_sqlite(url, sql, params),
        DialectKind::Postgres => execute_postgres(url, sql, params),
        other => Err(OrmdanticError::UnsupportedDialect {
            dialect: format!("{other:?}"),
        }),
    }
}

pub enum NativeConnection {
    Sqlite(Connection),
}

impl NativeConnection {
    pub fn open(url: &str) -> OrmdanticResult<Self> {
        match DialectKind::parse(url)? {
            DialectKind::Sqlite => Ok(Self::Sqlite(
                Connection::open(sqlite_path(url)).map_err(sql_error)?,
            )),
            other => Err(OrmdanticError::UnsupportedDialect {
                dialect: format!("{other:?}"),
            }),
        }
    }

    pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        match self {
            Self::Sqlite(connection) => execute_sqlite_connection(connection, sql, params),
        }
    }

    pub fn begin(&mut self) -> OrmdanticResult<()> {
        self.execute("BEGIN", &[]).map(|_| ())
    }

    pub fn commit(&mut self) -> OrmdanticResult<()> {
        self.execute("COMMIT", &[]).map(|_| ())
    }

    pub fn rollback(&mut self) -> OrmdanticResult<()> {
        self.execute("ROLLBACK", &[]).map(|_| ())
    }

    pub fn savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        self.execute(&format!("SAVEPOINT {name}"), &[]).map(|_| ())
    }
}

fn execute_sqlite(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    let path = sqlite_path(url);
    let mut connection = Connection::open(path).map_err(sql_error)?;
    execute_sqlite_connection(&mut connection, sql, params)
}

fn execute_sqlite_connection(
    connection: &mut Connection,
    sql: &str,
    params: &[DbValue],
) -> OrmdanticResult<QueryResult> {
    if returns_rows(sql) {
        let mut statement = connection.prepare(sql).map_err(sql_error)?;
        let columns = statement
            .column_names()
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let column_count = statement.column_count();
        let rows = statement
            .query_map(params_from_iter(params.iter()), |row| {
                let mut values = Vec::with_capacity(column_count);
                for idx in 0..column_count {
                    values.push(sqlite_value(row.get_ref(idx)?));
                }
                Ok(values)
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        Ok(QueryResult::new(columns, rows))
    } else {
        connection
            .execute(sql, params_from_iter(params.iter()))
            .map_err(sql_error)?;
        Ok(QueryResult::empty())
    }
}

fn execute_postgres(url: &str, sql: &str, _params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    let mut client = Client::connect(url, NoTls).map_err(sql_error)?;
    if returns_rows(sql) {
        // Parameter binding is added in the next execution-hardening slice.
        let rows = client.query(sql, &[]).map_err(sql_error)?;
        let columns = rows
            .first()
            .map(|row| {
                row.columns()
                    .iter()
                    .map(|column| column.name().to_string())
                    .collect()
            })
            .unwrap_or_default();
        Ok(QueryResult::new(columns, Vec::new()))
    } else {
        client.execute(sql, &[]).map_err(sql_error)?;
        Ok(QueryResult::empty())
    }
}

fn returns_rows(sql: &str) -> bool {
    sql.trim_start().to_ascii_lowercase().starts_with("select")
}

fn sqlite_path(url: &str) -> String {
    if url == "sqlite:///:memory:" || url == "sqlite+aiosqlite:///:memory:" {
        return ":memory:".to_string();
    }
    url.split_once(":///")
        .map(|(_, path)| path.to_string())
        .unwrap_or_else(|| url.to_string())
}

fn sqlite_value(value: ValueRef<'_>) -> DbValue {
    match value {
        ValueRef::Null => DbValue::Null,
        ValueRef::Integer(value) => DbValue::Integer(value),
        ValueRef::Real(value) => DbValue::Real(value),
        ValueRef::Text(value) => DbValue::Text(String::from_utf8_lossy(value).to_string()),
        ValueRef::Blob(value) => DbValue::Text(String::from_utf8_lossy(value).to_string()),
    }
}

fn sql_error(error: impl std::fmt::Display) -> OrmdanticError {
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
