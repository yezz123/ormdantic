use ormdantic_core::{OrmdanticError, OrmdanticResult};

use crate::{DbValue, QueryResult};

pub struct MsSqlConnection;

impl MsSqlConnection {
    pub fn open(_url: &str) -> OrmdanticResult<Self> {
        Err(unavailable())
    }

    pub fn execute(&mut self, _sql: &str, _params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        Err(unavailable())
    }
}

pub fn execute_url(_url: &str, _sql: &str, _params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    Err(unavailable())
}

pub fn unavailable() -> OrmdanticError {
    OrmdanticError::UnsupportedDialect {
        dialect: "mssql runtime requires the optional mssql engine feature".to_string(),
    }
}
