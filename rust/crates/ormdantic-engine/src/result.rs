use crate::DbValue;

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<DbValue>>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub fn new(columns: Vec<String>, rows: Vec<Vec<DbValue>>) -> Self {
        Self { columns, rows }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn rows(&self) -> &[Vec<DbValue>] {
        &self.rows
    }
}
