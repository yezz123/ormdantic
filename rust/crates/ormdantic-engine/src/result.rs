use crate::DbValue;

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<DbValue>>,
    row_count: Option<u64>,
}

impl QueryResult {
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: None,
        }
    }

    pub fn affected(row_count: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            row_count: Some(row_count),
        }
    }

    pub fn new(columns: Vec<String>, rows: Vec<Vec<DbValue>>) -> Self {
        Self {
            columns,
            rows,
            row_count: None,
        }
    }

    pub fn with_row_count(mut self, row_count: u64) -> Self {
        self.row_count = Some(row_count);
        self
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn rows(&self) -> &[Vec<DbValue>] {
        &self.rows
    }

    pub fn row_count(&self) -> Option<u64> {
        self.row_count
    }
}
