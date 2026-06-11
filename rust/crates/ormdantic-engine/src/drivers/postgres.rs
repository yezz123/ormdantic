use ormdantic_core::OrmdanticResult;
use postgres::types::{FromSql, ToSql, Type};
use postgres::{Client, NoTls, Row};
use std::error::Error;

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
            DbValue::UnsignedInteger(value) => match i64::try_from(*value) {
                Ok(value) => Box::new(value) as Box<dyn ToSql + Sync>,
                Err(_) => Box::new(value.to_string()) as Box<dyn ToSql + Sync>,
            },
            DbValue::Decimal(value) => Box::new(value.clone()),
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
    } else if *ty == Type::NUMERIC {
        nullable::<PgNumeric>(row, idx).map_or(DbValue::Null, |value| DbValue::Decimal(value.0))
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

struct PgNumeric(String);

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(parse_pg_numeric(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

fn parse_pg_numeric(raw: &[u8]) -> Result<String, Box<dyn Error + Sync + Send>> {
    if raw.len() < 8 {
        return Err("invalid PostgreSQL numeric payload".into());
    }
    let ndigits = i16::from_be_bytes([raw[0], raw[1]]) as usize;
    let weight = i16::from_be_bytes([raw[2], raw[3]]);
    let sign = u16::from_be_bytes([raw[4], raw[5]]);
    let dscale = i16::from_be_bytes([raw[6], raw[7]]) as usize;
    if raw.len() < 8 + ndigits * 2 {
        return Err("truncated PostgreSQL numeric payload".into());
    }
    match sign {
        0xC000 => return Ok("NaN".to_string()),
        0xD000 => return Ok("Infinity".to_string()),
        0xF000 => return Ok("-Infinity".to_string()),
        0x0000 | 0x4000 => {}
        _ => return Err("invalid PostgreSQL numeric sign".into()),
    }

    let digits = (0..ndigits)
        .map(|idx| {
            let offset = 8 + idx * 2;
            u16::from_be_bytes([raw[offset], raw[offset + 1]])
        })
        .collect::<Vec<_>>();

    let mut integer = String::new();
    if weight < 0 {
        integer.push('0');
    } else {
        for idx in 0..=weight as usize {
            let digit = digits.get(idx).copied().unwrap_or(0);
            if integer.is_empty() {
                integer.push_str(&digit.to_string());
            } else {
                integer.push_str(&format!("{digit:04}"));
            }
        }
    }
    while integer.len() > 1 && integer.starts_with('0') {
        integer.remove(0);
    }

    let mut fraction = String::new();
    if dscale > 0 {
        if weight < -1 {
            for _ in 0..((-weight - 1) as usize) {
                fraction.push_str("0000");
            }
        }
        let first_fraction_idx = if weight < 0 { 0 } else { weight as usize + 1 };
        for digit in digits.iter().skip(first_fraction_idx) {
            fraction.push_str(&format!("{digit:04}"));
        }
        while fraction.len() < dscale {
            fraction.push('0');
        }
        fraction.truncate(dscale);
    }

    let negative = sign == 0x4000 && digits.iter().any(|digit| *digit != 0);
    let mut rendered = if dscale == 0 {
        integer
    } else {
        format!("{integer}.{fraction}")
    };
    if negative {
        rendered.insert(0, '-');
    }
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::parse_pg_numeric;

    #[test]
    fn parses_postgres_numeric_decimal_payload() {
        let raw = [
            0x00, 0x02, // ndigits
            0x00, 0x00, // weight
            0x00, 0x00, // positive
            0x00, 0x02, // dscale
            0x00, 0x7b, // 123
            0x11, 0x94, // 4500
        ];

        assert_eq!(parse_pg_numeric(&raw).unwrap(), "123.45");
    }

    #[test]
    fn parses_postgres_numeric_negative_fraction_payload() {
        let raw = [
            0x00, 0x01, // ndigits
            0xff, 0xff, // weight -1
            0x40, 0x00, // negative
            0x00, 0x04, // dscale
            0x00, 0x0c, // 12
        ];

        assert_eq!(parse_pg_numeric(&raw).unwrap(), "-0.0012");
    }
}
