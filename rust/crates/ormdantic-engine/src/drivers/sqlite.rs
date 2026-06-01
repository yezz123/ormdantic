use ormdantic_core::OrmdanticResult;
use rusqlite::types::ValueRef;
use rusqlite::{params_from_iter, Connection};

use crate::url::sqlite_path;
use crate::{sql_error, DbValue, QueryResult};

pub struct SqliteConnection {
    connection: Connection,
}

impl SqliteConnection {
    pub fn open(url: &str) -> OrmdanticResult<Self> {
        Ok(Self {
            connection: Connection::open(sqlite_path(url)).map_err(sql_error)?,
        })
    }

    pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        execute_connection(&mut self.connection, sql, params)
    }
}

pub fn execute_url(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    let mut connection = SqliteConnection::open(url)?;
    connection.execute(sql, params)
}

pub fn execute_connection(
    connection: &mut Connection,
    sql: &str,
    params: &[DbValue],
) -> OrmdanticResult<QueryResult> {
    if crate::returns_rows(sql) {
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

fn sqlite_value(value: ValueRef<'_>) -> DbValue {
    match value {
        ValueRef::Null => DbValue::Null,
        ValueRef::Integer(value) => DbValue::Integer(value),
        ValueRef::Real(value) => DbValue::Real(value),
        ValueRef::Text(value) => DbValue::Text(String::from_utf8_lossy(value).to_string()),
        ValueRef::Blob(value) => DbValue::Text(String::from_utf8_lossy(value).to_string()),
    }
}
