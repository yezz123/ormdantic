use std::collections::BTreeSet;

use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_schema::{ColumnAlias, TableDef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlatHydrationPlan {
    table: TableDef,
    parsed_columns: Vec<Option<String>>,
    primary_key_index: usize,
}

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

impl FlatHydrationPlan {
    pub fn new(table: TableDef, aliases: &[String]) -> OrmdanticResult<Self> {
        let parsed_columns: Vec<Option<String>> = aliases
            .iter()
            .map(|alias| {
                ColumnAlias::parse(alias).and_then(|column_alias| {
                    column_alias
                        .column_for_table(table.name())
                        .map(str::to_string)
                })
            })
            .collect();
        let primary_key_index = parsed_columns
            .iter()
            .position(|column| column.as_deref() == Some(table.primary_key()))
            .ok_or_else(|| OrmdanticError::MissingPrimaryKeyAlias {
                tablename: table.name().to_string(),
                primary_key: table.primary_key().to_string(),
            })?;

        Ok(Self {
            table,
            parsed_columns,
            primary_key_index,
        })
    }

    pub fn table(&self) -> &TableDef {
        &self.table
    }

    pub fn parsed_columns(&self) -> &[Option<String>] {
        &self.parsed_columns
    }

    pub fn primary_key_index(&self) -> usize {
        self.primary_key_index
    }
}

#[cfg(test)]
mod tests {
    use super::{FlatHydrationPlan, ResultColumn, ResultShape};
    use ormdantic_schema::TableDef;

    #[test]
    fn builds_flat_hydration_plan() {
        let table = TableDef::new("flavors", "id", vec!["id".to_string()]);

        let plan = FlatHydrationPlan::new(
            table,
            &["flavors\\id".to_string(), "flavors\\name".to_string()],
        )
        .expect("plan should build");

        assert_eq!(plan.primary_key_index(), 0);
        assert_eq!(
            plan.parsed_columns(),
            &[Some("id".to_string()), Some("name".to_string())]
        );
    }

    #[test]
    fn errors_when_primary_key_alias_is_missing() {
        let table = TableDef::new("flavors", "id", vec!["id".to_string()]);

        let error = FlatHydrationPlan::new(table, &["flavors\\name".to_string()])
            .expect_err("missing primary key should error");

        assert_eq!(
            error.to_string(),
            "primary key column 'flavors\\id' was not found"
        );
    }

    #[test]
    fn parses_result_columns() {
        let column = ResultColumn::parse("coffee/flavor\\name").expect("column should parse");

        assert_eq!(column.alias(), "coffee/flavor\\name");
        assert_eq!(column.table_path(), "coffee/flavor");
        assert_eq!(column.column(), "name");
    }

    #[test]
    fn builds_relationship_result_shape() {
        let shape = ResultShape::new(
            "coffee",
            &[
                "coffee\\id".to_string(),
                "coffee/flavor\\id".to_string(),
                "coffee/flavor\\name".to_string(),
                "coffee/flavor/roast\\id".to_string(),
            ],
            vec!["coffee/flavor/roast".to_string()],
        );

        assert_eq!(shape.root_table(), "coffee");
        assert_eq!(
            shape.relationship_paths(),
            &[
                "coffee/flavor".to_string(),
                "coffee/flavor/roast".to_string()
            ]
        );
        assert_eq!(shape.array_paths(), &["coffee/flavor/roast".to_string()]);
    }
}
