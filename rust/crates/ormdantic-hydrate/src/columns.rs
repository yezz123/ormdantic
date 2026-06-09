use std::collections::BTreeSet;

use ormdantic_schema::ColumnAlias;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
    alias: String,
    table_path: String,
    column: String,
}

impl ResultColumn {
    pub fn parse(alias: impl Into<String>) -> Option<Self> {
        let alias = alias.into();
        let parsed = ColumnAlias::parse(&alias)?;
        Some(Self {
            alias,
            table_path: parsed.table_path().to_string(),
            column: parsed.column().to_string(),
        })
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    pub fn column(&self) -> &str {
        &self.column
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultShape {
    root_table: String,
    columns: Vec<ResultColumn>,
    relationship_paths: Vec<String>,
    array_paths: Vec<String>,
}

impl ResultShape {
    pub fn new(
        root_table: impl Into<String>,
        aliases: &[String],
        array_paths: Vec<String>,
    ) -> Self {
        let root_table = root_table.into();
        let columns: Vec<ResultColumn> = aliases
            .iter()
            .filter_map(|alias| ResultColumn::parse(alias.clone()))
            .collect();
        let mut relationship_paths = BTreeSet::new();
        for column in &columns {
            if column.table_path() == root_table {
                continue;
            }
            let mut current = String::new();
            for (idx, branch) in column.table_path().split('/').enumerate() {
                if idx == 0 {
                    current.push_str(branch);
                } else {
                    current.push('/');
                    current.push_str(branch);
                    relationship_paths.insert(current.clone());
                }
            }
        }

        Self {
            root_table,
            columns,
            relationship_paths: relationship_paths.into_iter().collect(),
            array_paths,
        }
    }

    pub fn root_table(&self) -> &str {
        &self.root_table
    }

    pub fn columns(&self) -> &[ResultColumn] {
        &self.columns
    }

    pub fn relationship_paths(&self) -> &[String] {
        &self.relationship_paths
    }

    pub fn array_paths(&self) -> &[String] {
        &self.array_paths
    }
}
