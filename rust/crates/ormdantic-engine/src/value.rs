#[cfg(feature = "sqlite")]
use rusqlite::types::{ToSqlOutput, Value as RusqliteValue, ValueRef};
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
            Self::Decimal(value) => ToSqlOutput::Borrowed(ValueRef::Text(value.as_bytes())),
            Self::Real(value) => ToSqlOutput::Owned(RusqliteValue::Real(*value)),
            Self::Text(value) => ToSqlOutput::Borrowed(ValueRef::Text(value.as_bytes())),
            Self::Bool(value) => ToSqlOutput::Owned(RusqliteValue::Integer(i64::from(*value))),
        })
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use rusqlite::types::ValueRef;

    #[test]
    fn sqlite_text_like_values_bind_as_borrowed_refs() {
        for value in [
            DbValue::Text("payload".to_string()),
            DbValue::Decimal("12.34".to_string()),
        ] {
            match value.to_sql().expect("value should bind") {
                ToSqlOutput::Borrowed(ValueRef::Text(bytes)) => {
                    assert!(!bytes.is_empty());
                }
                other => panic!("expected borrowed text binding, got {other:?}"),
            }
        }
    }
}
