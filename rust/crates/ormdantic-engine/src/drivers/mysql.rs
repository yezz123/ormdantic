use mysql::consts::ColumnType;
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

    pub fn begin(&mut self) -> OrmdanticResult<()> {
        self.connection
            .query_drop("START TRANSACTION")
            .map_err(sql_error)
    }

    pub fn commit(&mut self) -> OrmdanticResult<()> {
        self.connection.query_drop("COMMIT").map_err(sql_error)
    }

    pub fn rollback(&mut self) -> OrmdanticResult<()> {
        self.connection.query_drop("ROLLBACK").map_err(sql_error)
    }

    pub fn savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        self.connection
            .query_drop(format!("SAVEPOINT {name}"))
            .map_err(sql_error)
    }

    pub fn rollback_to_savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        self.connection
            .query_drop(format!("ROLLBACK TO SAVEPOINT {name}"))
            .map_err(sql_error)
    }

    pub fn release_savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
        self.connection
            .query_drop(format!("RELEASE SAVEPOINT {name}"))
            .map_err(sql_error)
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
        Ok(QueryResult::affected(conn.affected_rows()))
    }
}

fn mysql_params(params: &[DbValue]) -> Vec<Value> {
    params
        .iter()
        .map(|value| match value {
            DbValue::Null => Value::NULL,
            DbValue::Integer(value) => Value::Int(*value),
            DbValue::UnsignedInteger(value) => Value::UInt(*value),
            DbValue::Decimal(value) => Value::Bytes(value.as_bytes().to_vec()),
            DbValue::Real(value) => Value::Double(*value),
            DbValue::Text(value) => Value::Bytes(value.as_bytes().to_vec()),
            DbValue::Bool(value) => Value::Int(i64::from(*value)),
        })
        .collect()
}

fn mysql_row(row: Row) -> Vec<DbValue> {
    let column_types = row
        .columns_ref()
        .iter()
        .map(|column| column.column_type())
        .collect::<Vec<_>>();
    row.unwrap()
        .into_iter()
        .enumerate()
        .map(|(idx, value)| mysql_value(value, column_types.get(idx).copied()))
        .collect()
}

fn mysql_value(value: Value, column_type: Option<ColumnType>) -> DbValue {
    match value {
        Value::NULL => DbValue::Null,
        Value::Int(value) => DbValue::Integer(value),
        Value::UInt(value) => DbValue::UnsignedInteger(value),
        Value::Float(value) => DbValue::Real(value.into()),
        Value::Double(value) => DbValue::Real(value),
        Value::Bytes(value)
            if matches!(
                column_type,
                Some(ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL)
            ) =>
        {
            DbValue::Decimal(String::from_utf8_lossy(&value).to_string())
        }
        Value::Bytes(value) => DbValue::Text(String::from_utf8_lossy(&value).to_string()),
        other => DbValue::Text(format!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mysql_decimal_bytes_use_column_type_metadata() {
        assert_eq!(
            mysql_value(
                Value::Bytes(b"123.45".to_vec()),
                Some(ColumnType::MYSQL_TYPE_NEWDECIMAL)
            ),
            DbValue::Decimal("123.45".to_string())
        );
        assert_eq!(
            mysql_value(
                Value::Bytes(b"123.45".to_vec()),
                Some(ColumnType::MYSQL_TYPE_DECIMAL)
            ),
            DbValue::Decimal("123.45".to_string())
        );
        assert_eq!(
            mysql_value(
                Value::Bytes(b"123.45".to_vec()),
                Some(ColumnType::MYSQL_TYPE_VAR_STRING)
            ),
            DbValue::Text("123.45".to_string())
        );
    }

    #[test]
    fn mysql_params_cover_all_db_value_variants() {
        let params = mysql_params(&[
            DbValue::Null,
            DbValue::Integer(-1),
            DbValue::UnsignedInteger(42),
            DbValue::Decimal("12.34".to_string()),
            DbValue::Real(1.5),
            DbValue::Text("flavor".to_string()),
            DbValue::Bool(true),
        ]);

        assert_eq!(
            params,
            vec![
                Value::NULL,
                Value::Int(-1),
                Value::UInt(42),
                Value::Bytes(b"12.34".to_vec()),
                Value::Double(1.5),
                Value::Bytes(b"flavor".to_vec()),
                Value::Int(1),
            ]
        );
    }

    #[test]
    fn mysql_value_covers_scalar_and_fallback_values() {
        assert_eq!(mysql_value(Value::NULL, None), DbValue::Null);
        assert_eq!(mysql_value(Value::Int(-2), None), DbValue::Integer(-2));
        assert_eq!(
            mysql_value(Value::UInt(2), None),
            DbValue::UnsignedInteger(2)
        );
        assert_eq!(mysql_value(Value::Float(2.5), None), DbValue::Real(2.5));
        assert_eq!(mysql_value(Value::Double(3.5), None), DbValue::Real(3.5));

        match mysql_value(Value::Date(2026, 7, 7, 1, 2, 3, 0), None) {
            DbValue::Text(value) => assert!(value.contains("Date")),
            other => panic!("expected fallback text, got {other:?}"),
        }
    }
}
