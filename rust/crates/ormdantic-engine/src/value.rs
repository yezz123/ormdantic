#[cfg(feature = "sqlite")]
use rusqlite::types::{ToSqlOutput, Value as RusqliteValue};
#[cfg(feature = "sqlite")]
use rusqlite::ToSql;

#[derive(Debug, Clone, PartialEq)]
pub enum DbValue {
    Null,
    Integer(i64),
    UnsignedInteger(u64),
    Decimal(String),
    Real(f64),
    Text(String),
    Bool(bool),
}

#[cfg(feature = "sqlite")]
impl ToSql for DbValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Null => ToSqlOutput::Owned(RusqliteValue::Null),
            Self::Integer(value) => ToSqlOutput::Owned(RusqliteValue::Integer(*value)),
            Self::UnsignedInteger(value) => match i64::try_from(*value) {
                Ok(value) => ToSqlOutput::Owned(RusqliteValue::Integer(value)),
                Err(_) => ToSqlOutput::Owned(RusqliteValue::Blob(value.to_string().into_bytes())),
            },
            Self::Decimal(value) => ToSqlOutput::Owned(RusqliteValue::Text(value.clone())),
            Self::Real(value) => ToSqlOutput::Owned(RusqliteValue::Real(*value)),
            Self::Text(value) => ToSqlOutput::Owned(RusqliteValue::Text(value.clone())),
            Self::Bool(value) => ToSqlOutput::Owned(RusqliteValue::Integer(i64::from(*value))),
        })
    }
}
