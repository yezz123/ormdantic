use mysql::prelude::Queryable;
use mysql::{Params, Pool, PooledConn, Row, Value};
use ormdantic_core::OrmdanticResult;

use crate::url::normalize_driver_url;
use crate::{sql_error, DbValue, QueryResult};

pub struct MySqlConnection {
    connection: PooledConn,
}

impl MySqlConnection {
    pub fn open(url: &str) -> OrmdanticResult<Self> {
        let pool = Pool::new(normalize_driver_url(url).as_str()).map_err(sql_error)?;
        Ok(Self {
            connection: pool.get_conn().map_err(sql_error)?,
        })
    }

    pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        execute_conn(&mut self.connection, sql, params)
    }
}

pub fn execute_url(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    let mut connection = MySqlConnection::open(url)?;
    connection.execute(sql, params)
}

fn execute_conn(
    conn: &mut PooledConn,
    sql: &str,
    params: &[DbValue],
) -> OrmdanticResult<QueryResult> {
    if crate::returns_rows(sql) {
        let result = conn
            .exec_iter(sql, Params::Positional(mysql_params(params)))
            .map_err(sql_error)?;
        let columns = result
            .columns()
            .as_ref()
            .iter()
            .map(|column| column.name_str().to_string())
            .collect::<Vec<_>>();
        let rows = result
            .map(|row| row.map(mysql_row).map_err(sql_error))
            .collect::<OrmdanticResult<Vec<_>>>()?;
        Ok(QueryResult::new(columns, rows))
    } else {
        conn.exec_drop(sql, Params::Positional(mysql_params(params)))
            .map_err(sql_error)?;
        Ok(QueryResult::empty())
    }
}

fn mysql_params(params: &[DbValue]) -> Vec<Value> {
    params
        .iter()
        .map(|value| match value {
            DbValue::Null => Value::NULL,
            DbValue::Integer(value) => Value::Int(*value),
            DbValue::Real(value) => Value::Double(*value),
            DbValue::Text(value) => Value::Bytes(value.as_bytes().to_vec()),
            DbValue::Bool(value) => Value::Int(i64::from(*value)),
        })
        .collect()
}

fn mysql_row(row: Row) -> Vec<DbValue> {
    row.unwrap()
        .into_iter()
        .map(|value| match value {
            Value::NULL => DbValue::Null,
            Value::Int(value) => DbValue::Integer(value),
            Value::UInt(value) => DbValue::Integer(value as i64),
            Value::Float(value) => DbValue::Real(value.into()),
            Value::Double(value) => DbValue::Real(value),
            Value::Bytes(value) => DbValue::Text(String::from_utf8_lossy(&value).to_string()),
            other => DbValue::Text(format!("{other:?}")),
        })
        .collect()
}
