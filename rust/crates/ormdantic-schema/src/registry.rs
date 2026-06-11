use std::collections::{HashMap, HashSet};

use ormdantic_core::{OrmdanticError, OrmdanticResult, TableId};

use crate::TableDef;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaRegistry {
    tables: Vec<TableDef>,
    table_ids: HashMap<String, TableId>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_table(&mut self, mut table: TableDef) -> OrmdanticResult<TableId> {
        if self.table_ids.contains_key(table.name()) {
            return Err(OrmdanticError::DuplicateTable {
                tablename: table.name().to_string(),
            });
        }

        validate_columns(&table)?;
        validate_primary_key(&table)?;
        validate_indexes(&table)?;
        validate_unique_constraints(&table)?;
        validate_foreign_keys(&table)?;
        validate_exclusion_constraints(&table)?;

        let table_id = TableId(self.tables.len());
        table.set_id(table_id);
        self.table_ids.insert(table.name().to_string(), table_id);
        self.tables.push(table);
        Ok(table_id)
    }

    pub fn validate_relationships(&self) -> OrmdanticResult<()> {
        for table in &self.tables {
            for relationship in table.relationships() {
                let Some(target_table) = self.get_table(relationship.target_table()) else {
                    return Err(OrmdanticError::InvalidRelationship {
                        table: table.name().to_string(),
                        field: relationship.field().to_string(),
                        target_table: relationship.target_table().to_string(),
                    });
                };
                if !target_table
                    .column_names()
                    .any(|column| column == relationship.target_field())
                {
                    return Err(OrmdanticError::InvalidRelationship {
                        table: table.name().to_string(),
                        field: relationship.field().to_string(),
                        target_table: relationship.target_table().to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    pub fn get_table(&self, tablename: &str) -> Option<&TableDef> {
        self.table_ids
            .get(tablename)
            .and_then(|table_id| self.tables.get(table_id.0))
    }

    pub fn tables(&self) -> &[TableDef] {
        &self.tables
    }
}

fn validate_columns(table: &TableDef) -> OrmdanticResult<()> {
    let mut seen = HashSet::new();
    for column in table.columns() {
        if !seen.insert(column.name()) {
            return Err(OrmdanticError::DuplicateColumn {
                tablename: table.name().to_string(),
                column: column.name().to_string(),
            });
        }
    }
    Ok(())
}

fn validate_primary_key(table: &TableDef) -> OrmdanticResult<()> {
    if table
        .columns()
        .iter()
        .any(|column| column.name() == table.primary_key())
    {
        return Ok(());
    }

    Err(OrmdanticError::MissingPrimaryKey {
        tablename: table.name().to_string(),
        primary_key: table.primary_key().to_string(),
    })
}

fn validate_indexes(table: &TableDef) -> OrmdanticResult<()> {
    for index in table.indexes() {
        if index.columns().is_empty() && index.expressions_ref().is_empty() {
            return Err(OrmdanticError::SqlCompile {
                message: format!(
                    "index '{}' on table '{}' must reference at least one column or expression",
                    index.name(),
                    table.name()
                ),
            });
        }
        for column in index.columns() {
            validate_column_reference(table, column, "index", index.name())?;
        }
        for column in index.include_columns_ref() {
            validate_column_reference(table, column, "index", index.name())?;
        }
    }
    Ok(())
}

fn validate_unique_constraints(table: &TableDef) -> OrmdanticResult<()> {
    for constraint in table.unique_constraints() {
        for column in constraint.columns() {
            validate_column_reference(table, column, "unique constraint", constraint.name())?;
        }
    }
    Ok(())
}

fn validate_foreign_keys(table: &TableDef) -> OrmdanticResult<()> {
    for constraint in table.foreign_keys() {
        let owner_name = constraint.name().unwrap_or("foreign_key");
        for column in constraint.local_columns() {
            validate_column_reference(table, column, "foreign key", owner_name)?;
        }
    }
    Ok(())
}

fn validate_exclusion_constraints(table: &TableDef) -> OrmdanticResult<()> {
    for constraint in table.exclusion_constraints() {
        for element in constraint.elements() {
            if element.is_quoted() {
                validate_column_reference(
                    table,
                    element.value(),
                    "exclusion constraint",
                    constraint.name(),
                )?;
            }
        }
    }
    Ok(())
}

fn validate_column_reference(
    table: &TableDef,
    column: &str,
    owner_kind: &str,
    owner_name: &str,
) -> OrmdanticResult<()> {
    if table.column_names().any(|known| known == column) {
        return Ok(());
    }
    Err(OrmdanticError::SqlCompile {
        message: format!(
            "{owner_kind} '{owner_name}' on table '{}' references unknown column '{column}'",
            table.name()
        ),
    })
}
