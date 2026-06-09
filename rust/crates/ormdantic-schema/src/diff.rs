use std::collections::BTreeMap;

use ormdantic_core::{OrmdanticError, OrmdanticResult};

use crate::{ColumnDef, ConstraintDef, IndexDef, NamespaceDef, SchemaDef, TableDef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaSnapshot {
    schema: SchemaDef,
}

impl SchemaSnapshot {
    pub fn new(schema: SchemaDef) -> Self {
        Self { schema }
    }

    pub fn schema(&self) -> &SchemaDef {
        &self.schema
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaDiff {
    operations: Vec<SchemaOperation>,
}

impl SchemaDiff {
    pub fn new(operations: Vec<SchemaOperation>) -> Self {
        Self { operations }
    }

    pub fn operations(&self) -> &[SchemaOperation] {
        &self.operations
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaOperation {
    CreateNamespace(NamespaceDef),
    DropNamespace {
        name: String,
    },
    CreateTable(TableDef),
    DropTable {
        name: String,
    },
    AddColumn {
        table: String,
        column: ColumnDef,
    },
    DropColumn {
        table: String,
        column: String,
    },
    AlterColumn {
        table: String,
        column: ColumnDef,
    },
    CreateIndex {
        table: String,
        index: IndexDef,
    },
    DropIndex {
        table: String,
        name: String,
    },
    AddConstraint {
        table: String,
        constraint: ConstraintDef,
    },
    DropConstraint {
        table: String,
        name: String,
    },
}

pub struct SchemaDiffer;

impl SchemaDiffer {
    pub fn diff(from: &SchemaSnapshot, to: &SchemaSnapshot) -> OrmdanticResult<SchemaDiff> {
        let mut operations = Vec::new();
        let from_tables = table_map(from.schema().tables())?;
        let to_tables = table_map(to.schema().tables())?;

        for table in to.schema().tables() {
            if !from_tables.contains_key(table.name()) {
                operations.push(SchemaOperation::CreateTable(table.clone()));
            }
        }

        for table in from.schema().tables() {
            if !to_tables.contains_key(table.name()) {
                operations.push(SchemaOperation::DropTable {
                    name: table.name().to_string(),
                });
            }
        }

        for (name, from_table) in &from_tables {
            let Some(to_table) = to_tables.get(name) else {
                continue;
            };
            diff_columns(&mut operations, from_table, to_table);
            diff_indexes(&mut operations, from_table, to_table);
            diff_constraints(&mut operations, from_table, to_table);
        }

        Ok(SchemaDiff::new(operations))
    }
}

fn table_map(tables: &[TableDef]) -> OrmdanticResult<BTreeMap<String, &TableDef>> {
    let mut map = BTreeMap::new();
    for table in tables {
        if map.insert(table.name().to_string(), table).is_some() {
            return Err(OrmdanticError::SchemaDiffError {
                message: format!("duplicate table '{}' in schema snapshot", table.name()),
            });
        }
    }
    Ok(map)
}

fn diff_columns(operations: &mut Vec<SchemaOperation>, from: &TableDef, to: &TableDef) {
    let from_columns = from
        .columns()
        .iter()
        .map(|column| (column.name().to_string(), column))
        .collect::<BTreeMap<_, _>>();
    let to_columns = to
        .columns()
        .iter()
        .map(|column| (column.name().to_string(), column))
        .collect::<BTreeMap<_, _>>();

    for column in to.columns() {
        if !from_columns.contains_key(column.name()) {
            operations.push(SchemaOperation::AddColumn {
                table: to.name().to_string(),
                column: column.clone(),
            });
        }
    }
    for column in from.columns() {
        if !to_columns.contains_key(column.name()) {
            operations.push(SchemaOperation::DropColumn {
                table: from.name().to_string(),
                column: column.name().to_string(),
            });
        }
    }
    for (name, from_column) in from_columns {
        if let Some(to_column) = to_columns.get(&name) {
            if from_column != *to_column {
                operations.push(SchemaOperation::AlterColumn {
                    table: to.name().to_string(),
                    column: (*to_column).clone(),
                });
            }
        }
    }
}

fn diff_indexes(operations: &mut Vec<SchemaOperation>, from: &TableDef, to: &TableDef) {
    let from_indexes = from
        .indexes()
        .iter()
        .map(|index| (index.name().to_string(), index))
        .collect::<BTreeMap<_, _>>();
    let to_indexes = to
        .indexes()
        .iter()
        .map(|index| (index.name().to_string(), index))
        .collect::<BTreeMap<_, _>>();

    for index in to.indexes() {
        if !from_indexes.contains_key(index.name()) {
            operations.push(SchemaOperation::CreateIndex {
                table: to.name().to_string(),
                index: index.clone(),
            });
        }
    }
    for index in from.indexes() {
        if !to_indexes.contains_key(index.name()) {
            operations.push(SchemaOperation::DropIndex {
                table: from.name().to_string(),
                name: index.name().to_string(),
            });
        }
    }
}

fn diff_constraints(operations: &mut Vec<SchemaOperation>, from: &TableDef, to: &TableDef) {
    let from_constraints = named_constraints(from);
    let to_constraints = named_constraints(to);

    for (name, constraint) in &to_constraints {
        if !from_constraints.contains_key(name) {
            operations.push(SchemaOperation::AddConstraint {
                table: to.name().to_string(),
                constraint: constraint.clone(),
            });
        }
    }
    for name in from_constraints.keys() {
        if !to_constraints.contains_key(name) {
            operations.push(SchemaOperation::DropConstraint {
                table: from.name().to_string(),
                name: name.clone(),
            });
        }
    }
    for (name, from_constraint) in from_constraints {
        if let Some(to_constraint) = to_constraints.get(&name) {
            if &from_constraint != to_constraint {
                operations.push(SchemaOperation::DropConstraint {
                    table: from.name().to_string(),
                    name: name.clone(),
                });
                operations.push(SchemaOperation::AddConstraint {
                    table: to.name().to_string(),
                    constraint: to_constraint.clone(),
                });
            }
        }
    }
}

fn named_constraints(table: &TableDef) -> BTreeMap<String, ConstraintDef> {
    let mut constraints = BTreeMap::new();
    for constraint in table.unique_constraints() {
        constraints.insert(
            constraint.name().to_string(),
            ConstraintDef::Unique(constraint.clone()),
        );
    }
    for constraint in table.check_constraints() {
        if let Some(name) = constraint.name() {
            constraints.insert(name.to_string(), ConstraintDef::Check(constraint.clone()));
        }
    }
    for constraint in table.foreign_keys() {
        if let Some(name) = constraint.name() {
            constraints.insert(
                name.to_string(),
                ConstraintDef::ForeignKey(constraint.clone()),
            );
        }
    }
    constraints
}
