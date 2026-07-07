use ormdantic_core::{ExecutionErrorKind, OrmdanticError, OrmdanticResult};
use postgres::types::private::BytesMut;
use postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
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
            client: Client::connect(&normalize_driver_url(url), NoTls).map_err(postgres_error)?,
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
        let rows = client.query(sql, &refs).map_err(postgres_error)?;
        Ok(rows_to_result(&rows))
    } else {
        client.execute(sql, &refs).map_err(postgres_error)?;
        Ok(QueryResult::empty())
    }
}

fn postgres_error(error: postgres::Error) -> OrmdanticError {
    let message = error.to_string();
    if let Some(db_error) = error.as_db_error() {
        if let Some(kind) = classify_postgres_sqlstate(db_error.code().code()) {
            return OrmdanticError::ExecutionError { kind, message };
        }
    }
    sql_error(message)
}

fn classify_postgres_sqlstate(code: &str) -> Option<ExecutionErrorKind> {
    match code {
        "08000" | "08001" | "08003" | "08004" | "08006" | "08007" | "08P01" | "28000" | "28P01" => {
            Some(ExecutionErrorKind::Connection)
        }
        "23505" => Some(ExecutionErrorKind::UniqueViolation),
        "23503" => Some(ExecutionErrorKind::ForeignKeyViolation),
        "23502" => Some(ExecutionErrorKind::NotNullViolation),
        "23514" => Some(ExecutionErrorKind::CheckViolation),
        "40001" | "40P01" => Some(ExecutionErrorKind::SerializationFailure),
        "42501" => Some(ExecutionErrorKind::PermissionDenied),
        "42601" => Some(ExecutionErrorKind::Syntax),
        "57014" => Some(ExecutionErrorKind::Timeout),
        _ => None,
    }
}

fn pg_params(params: &[DbValue]) -> Vec<Box<dyn ToSql + Sync>> {
    params
        .iter()
        .map(|value| match value {
            DbValue::Null => Box::new(PgNull) as Box<dyn ToSql + Sync>,
            DbValue::Integer(value) => Box::new(PgInteger(*value)),
            DbValue::UnsignedInteger(value) => Box::new(PgUnsignedInteger(*value)),
            DbValue::Decimal(value) => Box::new(PgNumeric(value.clone())),
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

#[derive(Debug)]
struct PgNull;

impl ToSql for PgNull {
    fn to_sql(
        &self,
        _ty: &Type,
        _out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct PgInteger(i64);

impl ToSql for PgInteger {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty == Type::INT2 {
            return i16::try_from(self.0)?.to_sql(ty, out);
        }
        if *ty == Type::INT4 {
            return i32::try_from(self.0)?.to_sql(ty, out);
        }
        if *ty == Type::INT8 {
            return self.0.to_sql(ty, out);
        }
        if *ty == Type::NUMERIC {
            return PgNumeric(self.0.to_string()).to_sql(ty, out);
        }
        Err(format!("unsupported PostgreSQL integer target type: {ty:?}").into())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT2 || *ty == Type::INT4 || *ty == Type::INT8 || *ty == Type::NUMERIC
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct PgUnsignedInteger(u64);

impl ToSql for PgUnsignedInteger {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if *ty == Type::INT2 {
            return i16::try_from(self.0)?.to_sql(ty, out);
        }
        if *ty == Type::INT4 {
            return i32::try_from(self.0)?.to_sql(ty, out);
        }
        if *ty == Type::INT8 {
            return i64::try_from(self.0)?.to_sql(ty, out);
        }
        if *ty == Type::NUMERIC {
            return PgNumeric(self.0.to_string()).to_sql(ty, out);
        }
        Err(format!("unsupported PostgreSQL unsigned integer target type: {ty:?}").into())
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::INT2 || *ty == Type::INT4 || *ty == Type::INT8 || *ty == Type::NUMERIC
    }

    to_sql_checked!();
}

#[derive(Debug)]
struct PgNumeric(String);

impl<'a> FromSql<'a> for PgNumeric {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(parse_pg_numeric(raw)?))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }
}

impl ToSql for PgNumeric {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        encode_pg_numeric(&self.0, out)?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::NUMERIC
    }

    to_sql_checked!();
}

fn encode_pg_numeric(value: &str, out: &mut BytesMut) -> Result<(), Box<dyn Error + Sync + Send>> {
    let raw = value.trim();
    if raw.eq_ignore_ascii_case("nan") {
        out.extend_from_slice(&0_i16.to_be_bytes());
        out.extend_from_slice(&0_i16.to_be_bytes());
        out.extend_from_slice(&0xC000_u16.to_be_bytes());
        out.extend_from_slice(&0_i16.to_be_bytes());
        return Ok(());
    }

    let (negative, unsigned) = match raw.as_bytes().first() {
        Some(b'-') => (true, &raw[1..]),
        Some(b'+') => (false, &raw[1..]),
        _ => (false, raw),
    };
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if (integer.is_empty() && fraction.is_empty())
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return Err(format!("invalid PostgreSQL numeric literal: {value}").into());
    }

    let integer = integer.trim_start_matches('0');
    let integer_group_count = if integer.is_empty() {
        0
    } else {
        integer.len().div_ceil(4)
    };
    let fraction_group_count = fraction.len().div_ceil(4);
    let scale = i16::try_from(fraction.len())
        .map_err(|_| format!("PostgreSQL numeric scale is too large: {}", fraction.len()))?;
    let weight = if integer_group_count == 0 {
        if fraction_group_count == 0 {
            0
        } else {
            -1
        }
    } else {
        i16::try_from(integer_group_count - 1).map_err(|_| {
            format!("PostgreSQL numeric integer group count is too large: {integer_group_count}")
        })?
    };

    let mut digits = Vec::with_capacity(integer_group_count + fraction_group_count);
    if integer_group_count > 0 {
        let padded_len = integer_group_count * 4;
        let mut padded = String::with_capacity(padded_len);
        padded.push_str(&"0".repeat(padded_len - integer.len()));
        padded.push_str(integer);
        push_pg_numeric_digits(&padded, &mut digits)?;
    }
    if fraction_group_count > 0 {
        let padded_len = fraction_group_count * 4;
        let mut padded = String::with_capacity(padded_len);
        padded.push_str(fraction);
        padded.push_str(&"0".repeat(padded_len - fraction.len()));
        push_pg_numeric_digits(&padded, &mut digits)?;
    }
    let digit_count = i16::try_from(digits.len()).map_err(|_| {
        format!(
            "PostgreSQL numeric digit count is too large: {}",
            digits.len()
        )
    })?;

    out.extend_from_slice(&digit_count.to_be_bytes());
    out.extend_from_slice(&weight.to_be_bytes());
    out.extend_from_slice(&(if negative { 0x4000_u16 } else { 0x0000_u16 }).to_be_bytes());
    out.extend_from_slice(&scale.to_be_bytes());
    for digit in digits {
        out.extend_from_slice(&digit.to_be_bytes());
    }
    Ok(())
}

fn push_pg_numeric_digits(
    padded: &str,
    digits: &mut Vec<u16>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    for chunk in padded.as_bytes().chunks(4) {
        let rendered = std::str::from_utf8(chunk)?;
        digits.push(rendered.parse::<u16>()?);
    }
    Ok(())
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
    use super::{
        classify_postgres_sqlstate, encode_pg_numeric, parse_pg_numeric, pg_params, PgInteger,
        PgUnsignedInteger,
    };
    use crate::DbValue;
    use ormdantic_core::ExecutionErrorKind;
    use postgres::types::private::BytesMut;
    use postgres::types::{ToSql, Type};

    #[test]
    fn classifies_postgres_sqlstate_codes() {
        let cases = [
            ("23505", ExecutionErrorKind::UniqueViolation),
            ("23503", ExecutionErrorKind::ForeignKeyViolation),
            ("23502", ExecutionErrorKind::NotNullViolation),
            ("23514", ExecutionErrorKind::CheckViolation),
            ("40001", ExecutionErrorKind::SerializationFailure),
            ("40P01", ExecutionErrorKind::SerializationFailure),
            ("42501", ExecutionErrorKind::PermissionDenied),
            ("42601", ExecutionErrorKind::Syntax),
            ("57014", ExecutionErrorKind::Timeout),
            ("28P01", ExecutionErrorKind::Connection),
        ];

        for (code, expected) in cases {
            assert_eq!(classify_postgres_sqlstate(code), Some(expected));
        }
        assert_eq!(classify_postgres_sqlstate("99999"), None);
    }

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

    #[test]
    fn parses_postgres_numeric_integer_and_padded_fraction_payloads() {
        let integer = [
            0x00, 0x01, // ndigits
            0x00, 0x00, // weight
            0x00, 0x00, // positive
            0x00, 0x00, // dscale
            0x00, 0x7b, // 123
        ];
        let padded = [
            0x00, 0x01, // ndigits
            0xff, 0xff, // weight -1
            0x00, 0x00, // positive
            0x00, 0x06, // dscale
            0x00, 0x0c, // 12
        ];
        let shifted = [
            0x00, 0x01, // ndigits
            0xff, 0xfe, // weight -2
            0x00, 0x00, // positive
            0x00, 0x08, // dscale
            0x00, 0x0c, // 12
        ];

        assert_eq!(parse_pg_numeric(&integer).unwrap(), "123");
        assert_eq!(parse_pg_numeric(&padded).unwrap(), "0.001200");
        assert_eq!(parse_pg_numeric(&shifted).unwrap(), "0.00000012");
    }

    #[test]
    fn postgres_params_cover_all_db_value_variants() {
        let params = pg_params(&[
            DbValue::Null,
            DbValue::Integer(-1),
            DbValue::UnsignedInteger(42),
            DbValue::UnsignedInteger(u64::MAX),
            DbValue::Decimal("12.34".to_string()),
            DbValue::Real(1.5),
            DbValue::Text("flavor".to_string()),
            DbValue::Bool(true),
        ]);

        assert_eq!(params.len(), 8);
    }

    #[test]
    fn postgres_decimal_params_serialize_as_numeric() {
        let params = pg_params(&[DbValue::Decimal("123456789012345.123456789".to_string())]);
        let mut out = BytesMut::new();

        params[0]
            .to_sql_checked(&Type::NUMERIC, &mut out)
            .expect("decimal should serialize as PostgreSQL numeric");

        assert_eq!(parse_pg_numeric(&out).unwrap(), "123456789012345.123456789");
    }

    #[test]
    fn postgres_integer_params_serialize_to_int4_columns() {
        let params = pg_params(&[DbValue::Integer(1)]);
        let mut out = BytesMut::new();

        params[0]
            .to_sql_checked(&Type::INT4, &mut out)
            .expect("integer should serialize to PostgreSQL int4");
    }

    #[test]
    fn postgres_integer_params_cover_widths_numeric_and_rejections() {
        for ty in [Type::INT2, Type::INT4, Type::INT8] {
            let mut out = BytesMut::new();
            PgInteger(7).to_sql_checked(&ty, &mut out).unwrap();
        }

        let mut numeric = BytesMut::new();
        PgInteger(-7)
            .to_sql_checked(&Type::NUMERIC, &mut numeric)
            .unwrap();
        assert_eq!(parse_pg_numeric(&numeric).unwrap(), "-7");

        let mut unsupported = BytesMut::new();
        assert!(ToSql::to_sql(&PgInteger(1), &Type::TEXT, &mut unsupported).is_err());
    }

    #[test]
    fn postgres_unsigned_integer_params_cover_widths_numeric_and_rejections() {
        for ty in [Type::INT2, Type::INT4, Type::INT8] {
            let mut out = BytesMut::new();
            PgUnsignedInteger(7).to_sql_checked(&ty, &mut out).unwrap();
        }

        let mut numeric = BytesMut::new();
        PgUnsignedInteger(u64::MAX)
            .to_sql_checked(&Type::NUMERIC, &mut numeric)
            .unwrap();
        assert_eq!(parse_pg_numeric(&numeric).unwrap(), u64::MAX.to_string());

        let mut unsupported = BytesMut::new();
        assert!(ToSql::to_sql(&PgUnsignedInteger(1), &Type::TEXT, &mut unsupported).is_err());
    }

    #[test]
    fn postgres_null_params_accept_any_target_type() {
        let params = pg_params(&[DbValue::Null]);
        let mut out = BytesMut::new();

        let is_null = params[0]
            .to_sql_checked(&Type::INT4, &mut out)
            .expect("null should serialize to any PostgreSQL target type");

        assert!(matches!(is_null, postgres::types::IsNull::Yes));
    }

    #[test]
    fn postgres_numeric_encoder_covers_edge_literals() {
        let mut nan = BytesMut::new();
        encode_pg_numeric("NaN", &mut nan).unwrap();
        assert_eq!(parse_pg_numeric(&nan).unwrap(), "NaN");

        let mut zero = BytesMut::new();
        encode_pg_numeric("+0", &mut zero).unwrap();
        assert_eq!(parse_pg_numeric(&zero).unwrap(), "0");

        let mut fraction = BytesMut::new();
        encode_pg_numeric("-0.0012", &mut fraction).unwrap();
        assert_eq!(parse_pg_numeric(&fraction).unwrap(), "-0.0012");

        assert!(encode_pg_numeric("not-a-number", &mut BytesMut::new()).is_err());
    }

    #[test]
    fn parses_postgres_numeric_special_values_and_errors() {
        let nan = [0x00, 0x00, 0x00, 0x00, 0xc0, 0x00, 0x00, 0x00];
        let infinity = [0x00, 0x00, 0x00, 0x00, 0xd0, 0x00, 0x00, 0x00];
        let neg_infinity = [0x00, 0x00, 0x00, 0x00, 0xf0, 0x00, 0x00, 0x00];
        let invalid_sign = [0x00, 0x00, 0x00, 0x00, 0x12, 0x34, 0x00, 0x00];
        let truncated_digits = [0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

        assert_eq!(parse_pg_numeric(&nan).unwrap(), "NaN");
        assert_eq!(parse_pg_numeric(&infinity).unwrap(), "Infinity");
        assert_eq!(parse_pg_numeric(&neg_infinity).unwrap(), "-Infinity");
        assert!(parse_pg_numeric(&[]).is_err());
        assert!(parse_pg_numeric(&invalid_sign).is_err());
        assert!(parse_pg_numeric(&truncated_digits).is_err());
    }
}
