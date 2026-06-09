use ormdantic_core::{OrmdanticError, OrmdanticResult};

use crate::{DbValue, QueryResult};

#[cfg(feature = "oracle")]
mod runtime {
    use oracle_rs::{ColumnInfo, Config, Connection, OracleType, Value};
    use tokio::runtime::Runtime;
    use url::Url;

    use super::{DbValue, OrmdanticError, OrmdanticResult, QueryResult};
    use crate::sql_error;
    use crate::url::normalize_driver_url;

    pub struct OracleConnection {
        runtime: Runtime,
        connection: Connection,
    }

    impl OracleConnection {
        pub fn open(url: &str) -> OrmdanticResult<Self> {
            let runtime = Runtime::new().map_err(sql_error)?;
            let config = config_from_url(url)?;
            let connection = runtime
                .block_on(Connection::connect_with_config(config))
                .map_err(sql_error)?;
            Ok(Self {
                runtime,
                connection,
            })
        }

        pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
            let params = oracle_params(params);
            let result = self
                .runtime
                .block_on(self.connection.execute(sql, &params))
                .map_err(sql_error)?;
            Ok(result_to_query_result(result))
        }

        pub fn begin(&mut self) -> OrmdanticResult<()> {
            Ok(())
        }

        pub fn commit(&mut self) -> OrmdanticResult<()> {
            self.runtime
                .block_on(self.connection.commit())
                .map_err(sql_error)
        }

        pub fn rollback(&mut self) -> OrmdanticResult<()> {
            self.runtime
                .block_on(self.connection.rollback())
                .map_err(sql_error)
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
                DbValue::Real(value) => Value::Float(*value),
                DbValue::Text(value) => Value::String(value.clone()),
                DbValue::Bool(value) => Value::Boolean(*value),
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
            Value::String(value) => {
                numeric_string_value(value, column).unwrap_or_else(|| DbValue::Text(value.clone()))
            }
            Value::Bytes(value) => DbValue::Text(String::from_utf8_lossy(value).to_string()),
            Value::Integer(value) => DbValue::Integer(*value),
            Value::Float(value) => DbValue::Real(*value),
            Value::Number(value) => value
                .to_i64()
                .map(DbValue::Integer)
                .or_else(|_| value.to_f64().map(DbValue::Real))
                .unwrap_or_else(|_| DbValue::Text(format!("{value:?}"))),
            Value::Boolean(value) => DbValue::Bool(*value),
            Value::Json(value) => DbValue::Text(value.to_string()),
            other => DbValue::Text(format!("{other:?}")),
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
        if let Ok(integer) = value.parse::<i64>() {
            Some(DbValue::Integer(integer))
        } else {
            value.parse::<f64>().ok().map(DbValue::Real)
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
