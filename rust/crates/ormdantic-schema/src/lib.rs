#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    name: String,
    primary_key: String,
    columns: Vec<String>,
}

impl TableDef {
    pub fn new(
        name: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        Self {
            name: name.into(),
            primary_key: primary_key.into(),
            columns,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

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
}

#[cfg(test)]
mod tests {
    use super::ColumnAlias;

    #[test]
    fn parses_column_aliases() {
        let alias = ColumnAlias::parse("flavors\\id").expect("alias should parse");

        assert_eq!(alias.column_for_table("flavors"), Some("id"));
        assert_eq!(alias.column_for_table("coffee"), None);
    }
}
