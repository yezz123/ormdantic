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

#[cfg(test)]
mod tests {
    use super::FlatHydrationPlan;
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
}
