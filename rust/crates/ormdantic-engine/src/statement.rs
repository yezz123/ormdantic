use crate::{DbValue, QueryResult};

#[derive(Debug, Clone, PartialEq)]
pub struct StatementResult {
    row_count: u64,
    last_insert_id: Option<DbValue>,
    returned_rows: Vec<Vec<DbValue>>,
    columns: Vec<String>,
}

impl StatementResult {
    pub fn new(
        row_count: u64,
        last_insert_id: Option<DbValue>,
        returned_rows: Vec<Vec<DbValue>>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            row_count,
            last_insert_id,
            returned_rows,
            columns,
        }
    }

    pub fn from_query_result(result: QueryResult) -> Self {
        let row_count = result.rows().len() as u64;
        Self {
            row_count,
            last_insert_id: None,
            returned_rows: result.rows().to_vec(),
            columns: result.columns().to_vec(),
        }
    }

    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    pub fn last_insert_id(&self) -> Option<&DbValue> {
        self.last_insert_id.as_ref()
    }

    pub fn returned_rows(&self) -> &[Vec<DbValue>] {
        &self.returned_rows
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}
