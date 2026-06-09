#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnAlias {
    table_path: String,
    column: String,
}

impl ColumnAlias {
    pub fn parse(alias: &str) -> Option<Self> {
        let (table_path, column) = alias.split_once('\\')?;
        Some(Self {
            table_path: table_path.to_string(),
            column: column.to_string(),
        })
    }

    pub fn column_for_table(&self, tablename: &str) -> Option<&str> {
        if self.table_path == tablename {
            Some(&self.column)
        } else {
            None
        }
    }

    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    pub fn column(&self) -> &str {
        &self.column
    }
}
