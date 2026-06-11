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
    SetNamespaceComment {
        name: String,
        comment: Option<String>,
    },
    CreateTable(TableDef),
    DropTable {
        name: String,
    },
    RecreateTable(TableDef),
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
    SetColumnComment {
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
    SetTableComment {
        table: String,
        comment: Option<String>,
    },
    SetTableTablespace {
        table: String,
        tablespace: Option<String>,
    },
    SetTableMysqlOptions {
        table: String,
        engine: Option<String>,
        charset: Option<String>,
        collation: Option<String>,
        row_format: Option<String>,
        key_block_size: Option<u32>,
        pack_keys: Option<bool>,
        checksum: Option<bool>,
        delay_key_write: Option<bool>,
        stats_persistent: Option<bool>,
        stats_auto_recalc: Option<bool>,
        stats_sample_pages: Option<u32>,
        avg_row_length: Option<u32>,
        max_rows: Option<u32>,
        min_rows: Option<u32>,
        insert_method: Option<String>,
        data_directory: Option<String>,
        index_directory: Option<String>,
        connection: Option<String>,
        union: Vec<String>,
        partition_by: Option<String>,
        partitions: Option<u32>,
        subpartition_by: Option<String>,
        subpartitions: Option<u32>,
        auto_increment: Option<u32>,
    },
    SetTablePostgresInherits {
        table: String,
        add: Vec<String>,
        drop: Vec<String>,
    },
    SetTablePostgresWith {
        table: String,
        set: Vec<(String, String)>,
        reset: Vec<String>,
    },
    SetTablePostgresUsing {
        table: String,
        using: Option<String>,
    },
    SetTablePostgresUnlogged {
        table: String,
        unlogged: bool,
    },
    AttachPostgresPartition {
        table: String,
        parent: String,
        bound: String,
    },
    DetachPostgresPartition {
        table: String,
        parent: String,
    },
}

pub struct SchemaDiffer;

impl SchemaDiffer {
    pub fn diff(from: &SchemaSnapshot, to: &SchemaSnapshot) -> OrmdanticResult<SchemaDiff> {
        let mut operations = Vec::new();
        let from_namespaces = namespace_map(from.schema().namespaces())?;
        let to_namespaces = namespace_map(to.schema().namespaces())?;
        let from_tables = table_map(from.schema().tables())?;
        let to_tables = table_map(to.schema().tables())?;
        let mut namespace_drops = Vec::new();

        for namespace in to.schema().namespaces() {
            if !from_namespaces.contains_key(namespace.name()) {
                operations.push(SchemaOperation::CreateNamespace(namespace.clone()));
            }
        }

        for namespace in from.schema().namespaces() {
            if !to_namespaces.contains_key(namespace.name()) {
                namespace_drops.push(SchemaOperation::DropNamespace {
                    name: namespace.name().to_string(),
                });
            }
        }

        for (name, from_namespace) in &from_namespaces {
            let Some(to_namespace) = to_namespaces.get(name) else {
                continue;
            };
            if from_namespace.comment() != to_namespace.comment() {
                operations.push(SchemaOperation::SetNamespaceComment {
                    name: (*name).to_string(),
                    comment: to_namespace.comment().map(str::to_string),
                });
            }
        }

        for table in to.schema().tables() {
            if !from_tables.contains_key(&table_key(table)) {
                operations.push(SchemaOperation::CreateTable(table.clone()));
            }
        }

        for table in from.schema().tables() {
            if !to_tables.contains_key(&table_key(table)) {
                operations.push(SchemaOperation::DropTable {
                    name: table_key(table),
                });
            }
        }

        for (name, from_table) in &from_tables {
            let Some(to_table) = to_tables.get(name) else {
                continue;
            };
            if requires_table_recreate(from_table, to_table) {
                operations.push(SchemaOperation::RecreateTable((*to_table).clone()));
                continue;
            }
            diff_columns(&mut operations, from_table, to_table);
            diff_indexes(&mut operations, from_table, to_table);
            diff_constraints(&mut operations, from_table, to_table);
            diff_table_metadata(&mut operations, from_table, to_table);
        }

        operations.extend(namespace_drops);
        Ok(SchemaDiff::new(operations))
    }
}

fn requires_table_recreate(from: &TableDef, to: &TableDef) -> bool {
    from.postgres_partition_by() != to.postgres_partition_by()
        || from.is_sqlite_strict() != to.is_sqlite_strict()
        || from.is_sqlite_without_rowid() != to.is_sqlite_without_rowid()
        || from.oracle_compress() != to.oracle_compress()
}

fn diff_table_metadata(operations: &mut Vec<SchemaOperation>, from: &TableDef, to: &TableDef) {
    if from.comment() != to.comment() {
        operations.push(SchemaOperation::SetTableComment {
            table: table_key(to),
            comment: to.comment().map(str::to_string),
        });
    }
    if from.tablespace() != to.tablespace() {
        operations.push(SchemaOperation::SetTableTablespace {
            table: table_key(to),
            tablespace: to.tablespace().map(str::to_string),
        });
    }
    if from.mysql_engine() != to.mysql_engine()
        || from.mysql_charset() != to.mysql_charset()
        || from.mysql_collation() != to.mysql_collation()
        || from.mysql_row_format() != to.mysql_row_format()
        || from.mysql_key_block_size() != to.mysql_key_block_size()
        || from.mysql_pack_keys() != to.mysql_pack_keys()
        || from.mysql_checksum() != to.mysql_checksum()
        || from.mysql_delay_key_write() != to.mysql_delay_key_write()
        || from.mysql_stats_persistent() != to.mysql_stats_persistent()
        || from.mysql_stats_auto_recalc() != to.mysql_stats_auto_recalc()
        || from.mysql_stats_sample_pages() != to.mysql_stats_sample_pages()
        || from.mysql_avg_row_length() != to.mysql_avg_row_length()
        || from.mysql_max_rows() != to.mysql_max_rows()
        || from.mysql_min_rows() != to.mysql_min_rows()
        || from.mysql_insert_method() != to.mysql_insert_method()
        || from.mysql_data_directory() != to.mysql_data_directory()
        || from.mysql_index_directory() != to.mysql_index_directory()
        || from.mysql_connection() != to.mysql_connection()
        || from.mysql_union() != to.mysql_union()
        || from.mysql_partition_by() != to.mysql_partition_by()
        || from.mysql_partitions() != to.mysql_partitions()
        || from.mysql_subpartition_by() != to.mysql_subpartition_by()
        || from.mysql_subpartitions() != to.mysql_subpartitions()
        || from.mysql_auto_increment() != to.mysql_auto_increment()
    {
        operations.push(SchemaOperation::SetTableMysqlOptions {
            table: table_key(to),
            engine: to.mysql_engine().map(str::to_string),
            charset: to.mysql_charset().map(str::to_string),
            collation: to.mysql_collation().map(str::to_string),
            row_format: to.mysql_row_format().map(str::to_string),
            key_block_size: to.mysql_key_block_size(),
            pack_keys: to.mysql_pack_keys(),
            checksum: to.mysql_checksum(),
            delay_key_write: to.mysql_delay_key_write(),
            stats_persistent: to.mysql_stats_persistent(),
            stats_auto_recalc: to.mysql_stats_auto_recalc(),
            stats_sample_pages: to.mysql_stats_sample_pages(),
            avg_row_length: to.mysql_avg_row_length(),
            max_rows: to.mysql_max_rows(),
            min_rows: to.mysql_min_rows(),
            insert_method: to.mysql_insert_method().map(str::to_string),
            data_directory: to.mysql_data_directory().map(str::to_string),
            index_directory: to.mysql_index_directory().map(str::to_string),
            connection: to.mysql_connection().map(str::to_string),
            union: to.mysql_union().to_vec(),
            partition_by: to.mysql_partition_by().map(str::to_string),
            partitions: to.mysql_partitions(),
            subpartition_by: to.mysql_subpartition_by().map(str::to_string),
            subpartitions: to.mysql_subpartitions(),
            auto_increment: to.mysql_auto_increment(),
        });
    }
    if from.postgres_inherits() != to.postgres_inherits() {
        let add = to
            .postgres_inherits()
            .iter()
            .filter(|parent| !from.postgres_inherits().contains(parent))
            .cloned()
            .collect::<Vec<_>>();
        let drop = from
            .postgres_inherits()
            .iter()
            .filter(|parent| !to.postgres_inherits().contains(parent))
            .cloned()
            .collect::<Vec<_>>();
        operations.push(SchemaOperation::SetTablePostgresInherits {
            table: table_key(to),
            add,
            drop,
        });
    }
    if from.postgres_with() != to.postgres_with() {
        let from_options = from
            .postgres_with()
            .iter()
            .cloned()
            .collect::<BTreeMap<_, _>>();
        let to_options = to
            .postgres_with()
            .iter()
            .cloned()
            .collect::<BTreeMap<_, _>>();
        let set = to_options
            .iter()
            .filter(|(name, value)| from_options.get(*name) != Some(*value))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect::<Vec<_>>();
        let reset = from_options
            .keys()
            .filter(|name| !to_options.contains_key(*name))
            .cloned()
            .collect::<Vec<_>>();
        operations.push(SchemaOperation::SetTablePostgresWith {
            table: table_key(to),
            set,
            reset,
        });
    }
    if from.postgres_using() != to.postgres_using() {
        operations.push(SchemaOperation::SetTablePostgresUsing {
            table: table_key(to),
            using: to.postgres_using().map(str::to_string),
        });
    }
    if from.is_postgres_unlogged() != to.is_postgres_unlogged() {
        operations.push(SchemaOperation::SetTablePostgresUnlogged {
            table: table_key(to),
            unlogged: to.is_postgres_unlogged(),
        });
    }
    if from.postgres_partition_of() != to.postgres_partition_of()
        || from.postgres_partition_for() != to.postgres_partition_for()
    {
        if let Some(parent) = from.postgres_partition_of() {
            operations.push(SchemaOperation::DetachPostgresPartition {
                table: table_key(from),
                parent: parent.to_string(),
            });
        }
        if let (Some(parent), Some(bound)) =
            (to.postgres_partition_of(), to.postgres_partition_for())
        {
            operations.push(SchemaOperation::AttachPostgresPartition {
                table: table_key(to),
                parent: parent.to_string(),
                bound: bound.to_string(),
            });
        }
    }
}

fn namespace_map(namespaces: &[NamespaceDef]) -> OrmdanticResult<BTreeMap<String, &NamespaceDef>> {
    let mut map = BTreeMap::new();
    for namespace in namespaces {
        if map
            .insert(namespace.name().to_string(), namespace)
            .is_some()
        {
            return Err(OrmdanticError::SchemaDiffError {
                message: format!(
                    "duplicate namespace '{}' in schema snapshot",
                    namespace.name()
                ),
            });
        }
    }
    Ok(map)
}

fn table_map(tables: &[TableDef]) -> OrmdanticResult<BTreeMap<String, &TableDef>> {
    let mut map = BTreeMap::new();
    for table in tables {
        let key = table_key(table);
        if map.insert(key.clone(), table).is_some() {
            return Err(OrmdanticError::SchemaDiffError {
                message: format!("duplicate table '{key}' in schema snapshot"),
            });
        }
    }
    Ok(map)
}

fn table_key(table: &TableDef) -> String {
    table.qualified_name().to_string()
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
                table: table_key(to),
                column: column.clone(),
            });
        }
    }
    for column in from.columns() {
        if !to_columns.contains_key(column.name()) {
            operations.push(SchemaOperation::DropColumn {
                table: table_key(from),
                column: column.name().to_string(),
            });
        }
    }
    for (name, from_column) in from_columns {
        if let Some(to_column) = to_columns.get(&name) {
            let to_column = *to_column;
            if from_column == to_column {
                continue;
            }
            if !from_column.definition_eq_ignoring_comment(to_column) {
                operations.push(SchemaOperation::AlterColumn {
                    table: table_key(to),
                    column: to_column.clone(),
                });
            }
            if from_column.comment() != to_column.comment() {
                operations.push(SchemaOperation::SetColumnComment {
                    table: table_key(to),
                    column: to_column.clone(),
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
                table: table_key(to),
                index: index.clone(),
            });
        }
    }
    for index in from.indexes() {
        if !to_indexes.contains_key(index.name()) {
            operations.push(SchemaOperation::DropIndex {
                table: table_key(from),
                name: index.name().to_string(),
            });
        }
    }
    for (name, from_index) in from_indexes {
        if let Some(to_index) = to_indexes.get(&name) {
            if !indexes_equivalent(from_index, to_index) {
                operations.push(SchemaOperation::DropIndex {
                    table: table_key(from),
                    name: name.clone(),
                });
                operations.push(SchemaOperation::CreateIndex {
                    table: table_key(to),
                    index: (*to_index).clone(),
                });
            }
        }
    }
}

fn indexes_equivalent(from: &IndexDef, to: &IndexDef) -> bool {
    from.name() == to.name()
        && from.columns() == to.columns()
        && from.expressions_ref() == to.expressions_ref()
        && from.is_unique() == to.is_unique()
        && from.predicate() == to.predicate()
        && from.include_columns_ref() == to.include_columns_ref()
        && normalized_default_index_method(from.method_name())
            == normalized_default_index_method(to.method_name())
        && from.postgres_with_ref() == to.postgres_with_ref()
}

fn normalized_default_index_method(method: Option<&str>) -> Option<String> {
    let normalized = method?.to_ascii_lowercase();
    if normalized == "btree" {
        None
    } else {
        Some(normalized)
    }
}

fn diff_constraints(operations: &mut Vec<SchemaOperation>, from: &TableDef, to: &TableDef) {
    let from_constraints = named_constraints(from);
    let to_constraints = named_constraints(to);

    for (name, constraint) in &to_constraints {
        if !from_constraints.contains_key(name) {
            operations.push(SchemaOperation::AddConstraint {
                table: table_key(to),
                constraint: constraint.clone(),
            });
        }
    }
    for name in from_constraints.keys() {
        if !to_constraints.contains_key(name) {
            operations.push(SchemaOperation::DropConstraint {
                table: table_key(from),
                name: name.clone(),
            });
        }
    }
    for (name, from_constraint) in from_constraints {
        if let Some(to_constraint) = to_constraints.get(&name) {
            if &from_constraint != to_constraint {
                operations.push(SchemaOperation::DropConstraint {
                    table: table_key(from),
                    name: name.clone(),
                });
                operations.push(SchemaOperation::AddConstraint {
                    table: table_key(to),
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
    for constraint in table.exclusion_constraints() {
        constraints.insert(
            constraint.name().to_string(),
            ConstraintDef::Exclusion(constraint.clone()),
        );
    }
    constraints
}
