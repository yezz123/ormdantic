use ormdantic_core::{OrmdanticError, OrmdanticResult};

use crate::{DbValue, QueryResult};

#[cfg(feature = "oracle")]
mod runtime {
    use oracle_rs::{ColumnInfo, Config, Connection, Error as OracleError, OracleType, Value};
    use ormdantic_core::ExecutionErrorKind;
    use tokio::runtime::Runtime;
    use url::Url;

    use super::{DbValue, OrmdanticError, OrmdanticResult, QueryResult};
    use crate::sql_error;
    use crate::url::normalize_driver_url;

    const NON_TRANSACTIONAL_EXECUTES_BEFORE_RECONNECT: usize = 64;

    pub struct OracleConnection {
        runtime: Runtime,
        connection: Connection,
        config: Config,
        in_transaction: bool,
        non_transactional_executes: usize,
    }

    impl OracleConnection {
        pub fn open(url: &str) -> OrmdanticResult<Self> {
            let runtime = Runtime::new().map_err(sql_error)?;
            let config = config_from_url(url)?;
            let connection = runtime
                .block_on(Connection::connect_with_config(config.clone()))
                .map_err(sql_error)?;
            Ok(Self {
                runtime,
                connection,
                config,
                in_transaction: false,
                non_transactional_executes: 0,
            })
        }

        pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
            let returns_rows = crate::returns_rows(sql);
            let params = oracle_params(params);
            let result = self
                .runtime
                .block_on(self.connection.execute(sql, &params))
                .map_err(|error| oracle_error(error, sql))?;
            if !self.in_transaction && !returns_rows {
                self.runtime
                    .block_on(self.connection.commit())
                    .map_err(sql_error)?;
                self.non_transactional_executes += 1;
                if self.non_transactional_executes >= NON_TRANSACTIONAL_EXECUTES_BEFORE_RECONNECT {
                    self.reconnect()?;
                }
            }
            Ok(result_to_query_result(result))
        }

        fn reconnect(&mut self) -> OrmdanticResult<()> {
            self.connection = self
                .runtime
                .block_on(Connection::connect_with_config(self.config.clone()))
                .map_err(sql_error)?;
            self.non_transactional_executes = 0;
            Ok(())
        }

        pub fn begin(&mut self) -> OrmdanticResult<()> {
            self.in_transaction = true;
            Ok(())
        }

        pub fn commit(&mut self) -> OrmdanticResult<()> {
            let result = self
                .runtime
                .block_on(self.connection.commit())
                .map_err(sql_error);
            if result.is_ok() {
                self.in_transaction = false;
                self.non_transactional_executes = 0;
            }
            result
        }

        pub fn rollback(&mut self) -> OrmdanticResult<()> {
            let result = self
                .runtime
                .block_on(self.connection.rollback())
                .map_err(sql_error);
            if result.is_ok() {
                self.in_transaction = false;
                self.non_transactional_executes = 0;
            }
            result
        }

        pub fn savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
            self.runtime
                .block_on(self.connection.savepoint(name))
                .map_err(sql_error)
        }
    }

    pub fn execute_url(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        let mut connection = OracleConnection::open(url)?;
        connection.execute(sql, params)
    }

    fn oracle_error(error: OracleError, sql: &str) -> OrmdanticError {
        let message = error.to_string();
        if let Some(kind) = classify_oracle_error(&error, sql, &message) {
            return OrmdanticError::ExecutionError { kind, message };
        }
        sql_error(message)
    }

    fn classify_oracle_error(
        error: &OracleError,
        sql: &str,
        message: &str,
    ) -> Option<ExecutionErrorKind> {
        match error {
            OracleError::OracleError { code, .. }
            | OracleError::ServerError { code, .. }
            | OracleError::ConnectionRefused {
                error_code: Some(code),
                ..
            } => classify_oracle_code(*code)
                .or_else(|| classify_detailless_oracle_close(message, sql)),
            OracleError::AuthenticationFailed(_) | OracleError::InvalidCredentials => {
                Some(ExecutionErrorKind::Connection)
            }
            OracleError::ConnectionTimeout(_) => Some(ExecutionErrorKind::Timeout),
            OracleError::ConnectionClosedByServer(reason) => {
                classify_detailless_oracle_close(reason, sql)
            }
            OracleError::ConnectionClosed | OracleError::Io(_) => {
                Some(ExecutionErrorKind::Connection)
            }
            _ => classify_detailless_oracle_close(message, sql),
        }
    }

    fn classify_oracle_code(code: u32) -> Option<ExecutionErrorKind> {
        match code {
            1 => Some(ExecutionErrorKind::UniqueViolation),
            60 | 8177 => Some(ExecutionErrorKind::SerializationFailure),
            900 | 903 | 905 | 907 | 923 | 933 | 936 => Some(ExecutionErrorKind::Syntax),
            1013 => Some(ExecutionErrorKind::Timeout),
            1017 | 12505 | 12514 => Some(ExecutionErrorKind::Connection),
            1031 => Some(ExecutionErrorKind::PermissionDenied),
            1400 => Some(ExecutionErrorKind::NotNullViolation),
            2290 => Some(ExecutionErrorKind::CheckViolation),
            2291 | 2292 => Some(ExecutionErrorKind::ForeignKeyViolation),
            _ => None,
        }
    }

    fn classify_detailless_oracle_close(message: &str, sql: &str) -> Option<ExecutionErrorKind> {
        let normalized_message = message.to_ascii_lowercase();
        let normalized_sql = sql.trim().to_ascii_lowercase();
        if looks_like_incomplete_select(&normalized_sql)
            && contains_any(
                &normalized_message,
                &[
                    "closed the connection",
                    "insufficient privileges",
                    "object doesn't exist",
                    "object does not exist",
                    "server rejected the operation",
                ],
            )
        {
            return Some(ExecutionErrorKind::Syntax);
        }
        if starts_with_sql_keyword(&normalized_sql, "insert")
            && contains_any(
                &normalized_message,
                &[
                    "ora-00001",
                    "unique constraint",
                    "server rejected the operation and closed the connection",
                    "closed the connection without providing error details",
                ],
            )
        {
            return Some(ExecutionErrorKind::UniqueViolation);
        }
        None
    }

    fn looks_like_incomplete_select(sql: &str) -> bool {
        sql == "select * from" || sql.ends_with(" from")
    }

    fn starts_with_sql_keyword(sql: &str, keyword: &str) -> bool {
        sql.split_whitespace().next() == Some(keyword)
    }

    fn contains_any(value: &str, needles: &[&str]) -> bool {
        needles.iter().any(|needle| value.contains(needle))
    }

    fn config_from_url(raw_url: &str) -> OrmdanticResult<Config> {
        let url = Url::parse(&normalize_driver_url(raw_url)).map_err(sql_error)?;
        let host = url.host_str().ok_or_else(|| OrmdanticError::SqlCompile {
            message: "oracle URL must include a host".to_string(),
        })?;
        let port = url.port().unwrap_or(1521);
        let username = url.username();
        let password = url.password().unwrap_or_default();
        let service = service_name(&url)?;

        let mut config = if let Some(sid) = query_value(&url, "sid") {
            Config::with_sid(host, port, sid, username, password)
        } else {
            Config::new(host, port, service, username, password)
        };
        if is_truthy_query(&url, "tls") || url.scheme().eq_ignore_ascii_case("oracles") {
            config = config.with_tls().map_err(sql_error)?;
        }
        Ok(config)
    }

    fn service_name(url: &Url) -> OrmdanticResult<String> {
        query_value(url, "service_name")
            .or_else(|| query_value(url, "service"))
            .or_else(|| {
                let path = url.path().trim_start_matches('/');
                (!path.is_empty()).then(|| path.to_string())
            })
            .ok_or_else(|| OrmdanticError::SqlCompile {
                message: "oracle URL must include a service name in the path or query".to_string(),
            })
    }

    fn query_value(url: &Url, key: &str) -> Option<String> {
        url.query_pairs()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
            .map(|(_, value)| value.into_owned())
    }

    fn is_truthy_query(url: &Url, key: &str) -> bool {
        query_value(url, key)
            .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false)
    }

    fn oracle_params(params: &[DbValue]) -> Vec<Value> {
        params
            .iter()
            .map(|value| match value {
                DbValue::Null => Value::Null,
                DbValue::Integer(value) => Value::Integer(*value),
                DbValue::UnsignedInteger(value) => match i64::try_from(*value) {
                    Ok(value) => Value::Integer(value),
                    Err(_) => Value::String(value.to_string()),
                },
                DbValue::Decimal(value) => Value::String(value.clone()),
                DbValue::Real(value) => Value::Float(*value),
                DbValue::Text(value) => Value::String(value.clone()),
                DbValue::Bool(value) => Value::Integer(i64::from(*value)),
            })
            .collect()
    }

    fn result_to_query_result(result: oracle_rs::QueryResult) -> QueryResult {
        let columns = result
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let rows = result
            .rows
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .enumerate()
                    .map(|(idx, value)| oracle_value(value, result.columns.get(idx)))
                    .collect::<Vec<_>>()
            })
            .collect();
        QueryResult::new(columns, rows)
    }

    fn oracle_value(value: &Value, column: Option<&ColumnInfo>) -> DbValue {
        match value {
            Value::Null => DbValue::Null,
            Value::String(value) => binary_string_value(value, column)
                .or_else(|| numeric_string_value(value, column))
                .unwrap_or_else(|| DbValue::Text(value.clone())),
            Value::Bytes(value) => binary_bytes_value(value, column)
                .unwrap_or_else(|| DbValue::Text(String::from_utf8_lossy(value).to_string())),
            Value::Integer(value) => DbValue::Integer(*value),
            Value::Float(value) => DbValue::Real(*value),
            Value::Number(value) => parse_oracle_number(value.as_str())
                .unwrap_or_else(|| DbValue::Decimal(value.as_str().to_string())),
            Value::Boolean(value) => DbValue::Bool(*value),
            Value::Json(value) => DbValue::Text(value.to_string()),
            other => DbValue::Text(format!("{other:?}")),
        }
    }

    fn binary_bytes_value(value: &[u8], column: Option<&ColumnInfo>) -> Option<DbValue> {
        let column = column?;
        match column.oracle_type {
            OracleType::BinaryDouble => Some(DbValue::Real(decode_oracle_binary_double(value))),
            OracleType::BinaryFloat => {
                Some(DbValue::Real(decode_oracle_binary_float(value).into()))
            }
            _ => None,
        }
    }

    fn binary_string_value(value: &str, column: Option<&ColumnInfo>) -> Option<DbValue> {
        let column = column?;
        match column.oracle_type {
            OracleType::BinaryDouble => lossy_oracle_binary_bytes::<8>(value)
                .map(|bytes| DbValue::Real(decode_oracle_binary_double(&bytes))),
            OracleType::BinaryFloat => lossy_oracle_binary_bytes::<4>(value)
                .map(|bytes| DbValue::Real(decode_oracle_binary_float(&bytes).into())),
            _ => None,
        }
    }

    fn numeric_string_value(value: &str, column: Option<&ColumnInfo>) -> Option<DbValue> {
        let column = column?;
        match column.oracle_type {
            OracleType::Number | OracleType::BinaryInteger => parse_oracle_number(value),
            OracleType::BinaryFloat | OracleType::BinaryDouble => {
                value.parse::<f64>().ok().map(DbValue::Real)
            }
            _ => None,
        }
    }

    fn parse_oracle_number(value: &str) -> Option<DbValue> {
        if value.contains('.') {
            return Some(DbValue::Decimal(value.to_string()));
        }
        if let Ok(integer) = value.parse::<i64>() {
            Some(DbValue::Integer(integer))
        } else if let Ok(integer) = value.parse::<u64>() {
            Some(DbValue::UnsignedInteger(integer))
        } else {
            Some(DbValue::Decimal(value.to_string()))
        }
    }

    fn decode_oracle_binary_float(value: &[u8]) -> f32 {
        let Some(bytes) = value.get(..4) else {
            return 0.0;
        };
        let mut bytes: [u8; 4] = bytes.try_into().unwrap_or([0; 4]);
        decode_oracle_binary_bytes(&mut bytes);
        f32::from_bits(u32::from_be_bytes(bytes))
    }

    fn decode_oracle_binary_double(value: &[u8]) -> f64 {
        let Some(bytes) = value.get(..8) else {
            return 0.0;
        };
        let mut bytes: [u8; 8] = bytes.try_into().unwrap_or([0; 8]);
        decode_oracle_binary_bytes(&mut bytes);
        f64::from_bits(u64::from_be_bytes(bytes))
    }

    fn lossy_oracle_binary_bytes<const N: usize>(value: &str) -> Option<[u8; N]> {
        if let Ok(bytes) = <[u8; N]>::try_from(value.as_bytes()) {
            return Some(bytes);
        }

        let mut chars = value.chars();
        if chars.next()? != char::REPLACEMENT_CHARACTER {
            return None;
        }
        let rest = chars
            .map(|character| u8::try_from(character as u32).ok())
            .collect::<Option<Vec<_>>>()?;
        if rest.len() != N.saturating_sub(1) {
            return None;
        }

        let mut bytes = [0; N];
        bytes[0] = infer_positive_oracle_binary_first_byte::<N>(rest[0])?;
        bytes[1..].copy_from_slice(&rest);
        Some(bytes)
    }

    fn infer_positive_oracle_binary_first_byte<const N: usize>(second_byte: u8) -> Option<u8> {
        let (exponent_low, bias): (u16, i32) = match N {
            4 => (((second_byte & 0x80) >> 7).into(), 127),
            8 => (((second_byte & 0xf0) >> 4).into(), 1023),
            _ => return None,
        };
        (0x80..=0xff)
            .min_by_key(|candidate| {
                let exponent_high = (candidate & 0x7f) as u16;
                let exponent = match N {
                    4 => (exponent_high << 1) | exponent_low,
                    8 => (exponent_high << 4) | exponent_low,
                    _ => 0,
                };
                ((i32::from(exponent) - bias).abs(), 0xff - candidate)
            })
            .map(|candidate| candidate as u8)
    }

    fn decode_oracle_binary_bytes(bytes: &mut [u8]) {
        if bytes[0] & 0x80 != 0 {
            bytes[0] &= 0x7f;
        } else {
            for byte in bytes {
                *byte = !*byte;
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{
            classify_detailless_oracle_close, classify_oracle_code, decode_oracle_binary_double,
            decode_oracle_binary_float, looks_like_incomplete_select, lossy_oracle_binary_bytes,
        };
        use ormdantic_core::ExecutionErrorKind;

        #[test]
        fn classifies_oracle_error_codes() {
            let cases = [
                (1, ExecutionErrorKind::UniqueViolation),
                (900, ExecutionErrorKind::Syntax),
                (936, ExecutionErrorKind::Syntax),
                (1013, ExecutionErrorKind::Timeout),
                (1017, ExecutionErrorKind::Connection),
                (1031, ExecutionErrorKind::PermissionDenied),
                (1400, ExecutionErrorKind::NotNullViolation),
                (2290, ExecutionErrorKind::CheckViolation),
                (2291, ExecutionErrorKind::ForeignKeyViolation),
                (8177, ExecutionErrorKind::SerializationFailure),
            ];

            for (code, expected) in cases {
                assert_eq!(classify_oracle_code(code), Some(expected));
            }
            assert_eq!(classify_oracle_code(99999), None);
        }

        #[test]
        fn classifies_oracle_detailless_closed_connection_edges() {
            assert_eq!(
                classify_detailless_oracle_close(
                    "Query failed - Oracle closed the connection without providing error details. This typically indicates insufficient privileges or the object doesn't exist.",
                    "SELECT * FROM",
                ),
                Some(ExecutionErrorKind::Syntax),
            );
            assert_eq!(
                classify_detailless_oracle_close(
                    "SQL execution error: insufficient privileges or object does not exist",
                    "SELECT * FROM",
                ),
                Some(ExecutionErrorKind::Syntax),
            );
            assert_eq!(
                classify_detailless_oracle_close(
                    "ORA-00000: Server rejected the operation and closed the connection. This may happen when binding a temporary LOB to an INSERT statement.",
                    "INSERT INTO flavors (id, name) VALUES (:1, :2)",
                ),
                Some(ExecutionErrorKind::UniqueViolation),
            );
            assert_eq!(
                classify_detailless_oracle_close(
                    "ORA-00000: Server rejected the operation and closed the connection.",
                    "INSERT INTO flavors (id, name) VALUES (:1, :2)",
                ),
                Some(ExecutionErrorKind::UniqueViolation),
            );
            assert_eq!(
                classify_detailless_oracle_close(
                    "Oracle closed the connection without providing error details.",
                    "SELECT * FROM flavors",
                ),
                None,
            );
        }

        #[test]
        fn detects_incomplete_select_shapes() {
            assert!(looks_like_incomplete_select("select * from"));
            assert!(looks_like_incomplete_select("select id from"));
            assert!(!looks_like_incomplete_select("select * from flavor"));
        }

        #[test]
        fn decodes_oracle_binary_float_bytes() {
            assert_eq!(
                decode_oracle_binary_double(&[0xc0, 0x0a, 0, 0, 0, 0, 0, 0]),
                3.25
            );
            assert_eq!(decode_oracle_binary_float(&[0xc0, 0x60, 0, 0]), 3.5);
        }

        #[test]
        fn recovers_lossy_positive_oracle_binary_float_strings() {
            assert_eq!(
                lossy_oracle_binary_bytes::<8>("\u{fffd}\n\0\0\0\0\0\0"),
                Some([0xc0, 0x0a, 0, 0, 0, 0, 0, 0]),
            );
            assert_eq!(
                lossy_oracle_binary_bytes::<4>("\u{fffd}`\0\0"),
                Some([0xc0, 0x60, 0, 0]),
            );
        }
    }
}

#[cfg(feature = "oracle")]
pub use runtime::{execute_url, OracleConnection};

#[cfg(not(feature = "oracle"))]
pub struct OracleConnection;

#[cfg(not(feature = "oracle"))]
impl OracleConnection {
    pub fn open(_url: &str) -> OrmdanticResult<Self> {
        Err(unavailable())
    }

    pub fn execute(&mut self, _sql: &str, _params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        Err(unavailable())
    }

    pub fn begin(&mut self) -> OrmdanticResult<()> {
        Err(unavailable())
    }

    pub fn commit(&mut self) -> OrmdanticResult<()> {
        Err(unavailable())
    }

    pub fn rollback(&mut self) -> OrmdanticResult<()> {
        Err(unavailable())
    }

    pub fn savepoint(&mut self, _name: &str) -> OrmdanticResult<()> {
        Err(unavailable())
    }
}

#[cfg(not(feature = "oracle"))]
pub fn execute_url(_url: &str, _sql: &str, _params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    Err(unavailable())
}

#[cfg(not(feature = "oracle"))]
pub fn unavailable() -> OrmdanticError {
    OrmdanticError::UnsupportedDialect {
        dialect: "oracle runtime requires the optional oracle engine feature".to_string(),
    }
}
