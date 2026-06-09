use ormdantic_core::{OrmdanticError, OrmdanticResult};
use ormdantic_schema::{ColumnAlias, TableDef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlatHydrationPlan {
    table: TableDef,
    parsed_columns: Vec<Option<String>>,
    primary_key_index: usize,
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
