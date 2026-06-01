use ormdantic_core::OrmdanticResult;
use postgres::types::{FromSql, ToSql, Type};
use postgres::{Client, NoTls, Row};

use crate::url::normalize_driver_url;
use crate::{sql_error, DbValue, QueryResult};

pub struct PostgresConnection {
    client: Client,
}

impl PostgresConnection {
    pub fn open(url: &str) -> OrmdanticResult<Self> {
        Ok(Self {
            client: Client::connect(&normalize_driver_url(url), NoTls).map_err(sql_error)?,
        })
    }

    pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        execute_client(&mut self.client, sql, params)
    }
}

pub fn execute_url(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    let mut connection = PostgresConnection::open(url)?;
    connection.execute(sql, params)
}

fn execute_client(
    client: &mut Client,
    sql: &str,
    params: &[DbValue],
) -> OrmdanticResult<QueryResult> {
    let boxed = pg_params(params);
    let refs = boxed
        .iter()
        .map(|value| &**value as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();
    if crate::returns_rows(sql) {
        let rows = client.query(sql, &refs).map_err(sql_error)?;
        Ok(rows_to_result(&rows))
    } else {
        client.execute(sql, &refs).map_err(sql_error)?;
        Ok(QueryResult::empty())
    }
}

fn pg_params(params: &[DbValue]) -> Vec<Box<dyn ToSql + Sync>> {
    params
        .iter()
        .map(|value| match value {
            DbValue::Null => Box::new(None::<String>) as Box<dyn ToSql + Sync>,
            DbValue::Integer(value) => Box::new(*value),
            DbValue::Real(value) => Box::new(*value),
            DbValue::Text(value) => Box::new(value.clone()),
            DbValue::Bool(value) => Box::new(*value),
        })
        .collect()
}

fn rows_to_result(rows: &[Row]) -> QueryResult {
    let columns = rows
        .first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let data = rows
        .iter()
        .map(|row| {
            row.columns()
                .iter()
                .enumerate()
                .map(|(idx, column)| pg_value(row, idx, column.type_()))
                .collect::<Vec<_>>()
        })
        .collect();
    QueryResult::new(columns, data)
}

fn pg_value(row: &Row, idx: usize, ty: &Type) -> DbValue {
    if *ty == Type::BOOL {
        nullable::<bool>(row, idx).map_or(DbValue::Null, DbValue::Bool)
    } else if *ty == Type::INT2 {
        nullable::<i16>(row, idx).map_or(DbValue::Null, |value| DbValue::Integer(value.into()))
    } else if *ty == Type::INT4 {
        nullable::<i32>(row, idx).map_or(DbValue::Null, |value| DbValue::Integer(value.into()))
    } else if *ty == Type::INT8 {
        nullable::<i64>(row, idx).map_or(DbValue::Null, DbValue::Integer)
    } else if *ty == Type::FLOAT4 || *ty == Type::FLOAT8 {
        nullable::<f64>(row, idx).map_or_else(
            || nullable::<f32>(row, idx).map_or(DbValue::Null, |value| DbValue::Real(value.into())),
            DbValue::Real,
        )
    } else {
        nullable::<String>(row, idx).map_or(DbValue::Null, DbValue::Text)
    }
}

fn nullable<T>(row: &Row, idx: usize) -> Option<T>
where
    T: for<'a> FromSql<'a>,
{
    row.try_get::<usize, Option<T>>(idx).ok().flatten()
}
