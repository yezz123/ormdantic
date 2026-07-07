use ormdantic_core::{OrmdanticError, OrmdanticResult};

use crate::{DbValue, QueryResult};

#[cfg(feature = "mssql")]
mod runtime {
    use tiberius::{
        numeric::Numeric, AuthMethod, Client, ColumnData, Config, EncryptionLevel, Row, ToSql,
    };
    use tokio::net::TcpStream;
    use tokio::runtime::Runtime;
    use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
    use url::Url;

    use super::{DbValue, OrmdanticError, OrmdanticResult, QueryResult};
    use crate::sql_error;
    use crate::url::normalize_driver_url;

    pub struct MsSqlConnection {
        runtime: Runtime,
        client: Client<Compat<TcpStream>>,
    }

    impl MsSqlConnection {
        pub fn open(url: &str) -> OrmdanticResult<Self> {
            let runtime = Runtime::new().map_err(sql_error)?;
            let config = config_from_url(url)?;
            let client = runtime.block_on(async {
                let tcp = TcpStream::connect(config.get_addr()).await?;
                tcp.set_nodelay(true)?;
                Client::connect(config, tcp.compat_write()).await
            });
            Ok(Self {
                runtime,
                client: client.map_err(sql_error)?,
            })
        }

        pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
            let params = mssql_params(params);
            let refs = params
                .iter()
                .map(|value| &**value as &dyn ToSql)
                .collect::<Vec<_>>();
            if crate::returns_rows(sql) {
                let rows = self
                    .runtime
                    .block_on(self.client.query(sql, &refs))
                    .map_err(sql_error)?
                    .into_first_result();
                let rows = self.runtime.block_on(rows).map_err(sql_error)?;
                Ok(rows_to_result(&rows))
            } else if refs.is_empty() {
                self.simple_execute(sql)?;
                Ok(QueryResult::empty())
            } else {
                self.runtime
                    .block_on(self.client.execute(sql, &refs))
                    .map_err(sql_error)?;
                Ok(QueryResult::empty())
            }
        }

        pub fn begin(&mut self) -> OrmdanticResult<()> {
            self.simple_execute("BEGIN TRANSACTION")
        }

        pub fn commit(&mut self) -> OrmdanticResult<()> {
            self.simple_execute("COMMIT TRANSACTION")
        }

        pub fn rollback(&mut self) -> OrmdanticResult<()> {
            self.simple_execute("ROLLBACK TRANSACTION")
        }

        pub fn savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
            self.simple_execute(&format!("SAVE TRANSACTION {name}"))
        }

        pub fn rollback_to_savepoint(&mut self, name: &str) -> OrmdanticResult<()> {
            self.simple_execute(&format!("ROLLBACK TRANSACTION {name}"))
        }

        fn simple_execute(&mut self, sql: &str) -> OrmdanticResult<()> {
            let stream = self
                .runtime
                .block_on(self.client.simple_query(sql))
                .map_err(sql_error)?;
            self.runtime
                .block_on(stream.into_results())
                .map_err(sql_error)?;
            Ok(())
        }
    }

    pub fn execute_url(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        let mut connection = MsSqlConnection::open(url)?;
        connection.execute(sql, params)
    }

    fn config_from_url(raw_url: &str) -> OrmdanticResult<Config> {
        let url = Url::parse(&normalize_driver_url(raw_url)).map_err(sql_error)?;
        let mut config = Config::new();
        let host = url.host_str().ok_or_else(|| OrmdanticError::SqlCompile {
            message: "mssql URL must include a host".to_string(),
        })?;
        config.host(host);
        if let Some(port) = url.port() {
            config.port(port);
        }
        let database = url.path().trim_start_matches('/');
        if !database.is_empty() {
            config.database(database);
        }
        if !url.username().is_empty() {
            config.authentication(AuthMethod::sql_server(
                url.username(),
                url.password().unwrap_or_default(),
            ));
        }
        for (key, value) in url.query_pairs() {
            match key.to_ascii_lowercase().as_str() {
                "trust_cert" | "trustservercertificate" if is_truthy(&value) => {
                    config.trust_cert();
                }
                "encrypt" if value.eq_ignore_ascii_case("false") => {
                    config.encryption(EncryptionLevel::Off);
                }
                "encrypt" if value.eq_ignore_ascii_case("danger_plaintext") => {
                    config.encryption(EncryptionLevel::NotSupported);
                }
                _ => {}
            }
        }
        Ok(config)
    }

    fn is_truthy(value: &str) -> bool {
        matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
    }

    struct NullParam;

    impl ToSql for NullParam {
        fn to_sql(&self) -> ColumnData<'_> {
            ColumnData::String(None)
        }
    }

    fn mssql_params(params: &[DbValue]) -> Vec<Box<dyn ToSql>> {
        params
            .iter()
            .map(|value| match value {
                DbValue::Null => Box::new(NullParam) as Box<dyn ToSql>,
                DbValue::Integer(value) => Box::new(*value),
                DbValue::UnsignedInteger(value) => match i64::try_from(*value) {
                    Ok(value) => Box::new(value) as Box<dyn ToSql>,
                    Err(_) => Box::new(value.to_string()) as Box<dyn ToSql>,
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
        let data = rows.iter().map(row_values).collect();
        QueryResult::new(columns, data)
    }

    fn row_values(row: &Row) -> Vec<DbValue> {
        (0..row.len()).map(|idx| row_value(row, idx)).collect()
    }

    fn row_value(row: &Row, idx: usize) -> DbValue {
        if let Some(value) = row.try_get::<bool, _>(idx).ok().flatten() {
            DbValue::Bool(value)
        } else if let Some(value) = row.try_get::<i64, _>(idx).ok().flatten() {
            DbValue::Integer(value)
        } else if let Some(value) = row.try_get::<i32, _>(idx).ok().flatten() {
            DbValue::Integer(value.into())
        } else if let Some(value) = row.try_get::<i16, _>(idx).ok().flatten() {
            DbValue::Integer(value.into())
        } else if let Some(value) = row.try_get::<u8, _>(idx).ok().flatten() {
            DbValue::Integer(value.into())
        } else if let Some(value) = row.try_get::<f64, _>(idx).ok().flatten() {
            DbValue::Real(value)
        } else if let Some(value) = row.try_get::<f32, _>(idx).ok().flatten() {
            DbValue::Real(value.into())
        } else if let Some(value) = row.try_get::<Numeric, _>(idx).ok().flatten() {
            DbValue::Decimal(value.to_string())
        } else if let Some(value) = row.try_get::<&str, _>(idx).ok().flatten() {
            DbValue::Text(value.to_string())
        } else {
            DbValue::Null
        }
    }
}

#[cfg(feature = "mssql")]
pub use runtime::{execute_url, MsSqlConnection};

#[cfg(not(feature = "mssql"))]
pub struct MsSqlConnection;

#[cfg(not(feature = "mssql"))]
impl MsSqlConnection {
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

    pub fn rollback_to_savepoint(&mut self, _name: &str) -> OrmdanticResult<()> {
        Err(unavailable())
    }
}

#[cfg(not(feature = "mssql"))]
pub fn execute_url(_url: &str, _sql: &str, _params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    Err(unavailable())
}

#[cfg(not(feature = "mssql"))]
pub fn unavailable() -> OrmdanticError {
    OrmdanticError::UnsupportedDialect {
        dialect: "mssql runtime requires the optional mssql engine feature".to_string(),
    }
}

#[cfg(all(test, not(feature = "mssql")))]
mod fallback_tests {
    use super::*;

    #[test]
    fn mssql_fallback_methods_report_optional_feature_requirement() {
        let error = unavailable().to_string();
        assert!(error.contains("optional mssql engine feature"));
        assert!(MsSqlConnection::open("mssql://localhost").is_err());
        assert!(execute_url("mssql://localhost", "SELECT 1", &[]).is_err());

        let mut connection = MsSqlConnection;
        assert!(connection.execute("SELECT 1", &[]).is_err());
        assert!(connection.begin().is_err());
        assert!(connection.commit().is_err());
        assert!(connection.rollback().is_err());
        assert!(connection.savepoint("sp").is_err());
        assert!(connection.rollback_to_savepoint("sp").is_err());
    }
}
