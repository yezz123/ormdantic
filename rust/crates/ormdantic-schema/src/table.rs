use ormdantic_core::{QualifiedName, TableId};

use crate::{
    CheckConstraintDef, ColumnDef, ExclusionConstraintDef, FieldKind, ForeignKeyDef, IndexDef,
    RelationshipDef, UniqueConstraintDef,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleTableCompression {
    Enabled,
    Level(u32),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MysqlTableOptions {
    pub engine: Option<String>,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub row_format: Option<String>,
    pub key_block_size: Option<u32>,
    pub pack_keys: Option<bool>,
    pub checksum: Option<bool>,
    pub delay_key_write: Option<bool>,
    pub stats_persistent: Option<bool>,
    pub stats_auto_recalc: Option<bool>,
    pub stats_sample_pages: Option<u32>,
    pub avg_row_length: Option<u32>,
    pub max_rows: Option<u32>,
    pub min_rows: Option<u32>,
    pub insert_method: Option<String>,
    pub data_directory: Option<String>,
    pub index_directory: Option<String>,
    pub connection: Option<String>,
    pub union: Vec<String>,
    pub partition_by: Option<String>,
    pub partitions: Option<u32>,
    pub subpartition_by: Option<String>,
    pub subpartitions: Option<u32>,
    pub auto_increment: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    id: Option<TableId>,
    model_key: String,
    name: String,
    primary_key: String,
    columns: Vec<ColumnDef>,
    indexes: Vec<IndexDef>,
    unique_constraints: Vec<UniqueConstraintDef>,
    relationships: Vec<RelationshipDef>,
    check_constraints: Vec<CheckConstraintDef>,
    foreign_keys: Vec<ForeignKeyDef>,
    exclusion_constraints: Vec<ExclusionConstraintDef>,
    schema: Option<String>,
    comment: Option<String>,
    tablespace: Option<String>,
    mysql_engine: Option<String>,
    mysql_charset: Option<String>,
    mysql_collation: Option<String>,
    mysql_row_format: Option<String>,
    mysql_key_block_size: Option<u32>,
    mysql_pack_keys: Option<bool>,
    mysql_checksum: Option<bool>,
    mysql_delay_key_write: Option<bool>,
    mysql_stats_persistent: Option<bool>,
    mysql_stats_auto_recalc: Option<bool>,
    mysql_stats_sample_pages: Option<u32>,
    mysql_avg_row_length: Option<u32>,
    mysql_max_rows: Option<u32>,
    mysql_min_rows: Option<u32>,
    mysql_insert_method: Option<String>,
    mysql_data_directory: Option<String>,
    mysql_index_directory: Option<String>,
    mysql_connection: Option<String>,
    mysql_union: Vec<String>,
    mysql_partition_by: Option<String>,
    mysql_partitions: Option<u32>,
    mysql_subpartition_by: Option<String>,
    mysql_subpartitions: Option<u32>,
    mysql_auto_increment: Option<u32>,
    postgres_inherits: Vec<String>,
    postgres_with: Vec<(String, String)>,
    postgres_using: Option<String>,
    postgres_unlogged: bool,
    postgres_partition_by: Option<String>,
    postgres_partition_of: Option<String>,
    postgres_partition_for: Option<String>,
    sqlite_strict: bool,
    sqlite_without_rowid: bool,
    oracle_compress: Option<OracleTableCompression>,
    mssql_primary_key_nonclustered: bool,
}

impl TableDef {
    pub fn new(
        name: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<String>,
    ) -> Self {
        let name = name.into();
        Self {
            id: None,
            model_key: name.clone(),
            name,
            primary_key: primary_key.into(),
            columns: columns
                .into_iter()
                .map(|column| ColumnDef::new(column, FieldKind::Unknown))
                .collect(),
            indexes: Vec::new(),
            unique_constraints: Vec::new(),
            relationships: Vec::new(),
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            exclusion_constraints: Vec::new(),
            schema: None,
            comment: None,
            tablespace: None,
            mysql_engine: None,
            mysql_charset: None,
            mysql_collation: None,
            mysql_row_format: None,
            mysql_key_block_size: None,
            mysql_pack_keys: None,
            mysql_checksum: None,
            mysql_delay_key_write: None,
            mysql_stats_persistent: None,
            mysql_stats_auto_recalc: None,
            mysql_stats_sample_pages: None,
            mysql_avg_row_length: None,
            mysql_max_rows: None,
            mysql_min_rows: None,
            mysql_insert_method: None,
            mysql_data_directory: None,
            mysql_index_directory: None,
            mysql_connection: None,
            mysql_union: Vec::new(),
            mysql_partition_by: None,
            mysql_partitions: None,
            mysql_subpartition_by: None,
            mysql_subpartitions: None,
            mysql_auto_increment: None,
            postgres_inherits: Vec::new(),
            postgres_with: Vec::new(),
            postgres_using: None,
            postgres_unlogged: false,
            postgres_partition_by: None,
            postgres_partition_of: None,
            postgres_partition_for: None,
            sqlite_strict: false,
            sqlite_without_rowid: false,
            oracle_compress: None,
            mssql_primary_key_nonclustered: false,
        }
    }

    pub fn from_parts(
        name: impl Into<String>,
        model_key: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<ColumnDef>,
        indexes: Vec<IndexDef>,
        unique_constraints: Vec<UniqueConstraintDef>,
        relationships: Vec<RelationshipDef>,
    ) -> Self {
        Self {
            id: None,
            model_key: model_key.into(),
            name: name.into(),
            primary_key: primary_key.into(),
            columns,
            indexes,
            unique_constraints,
            relationships,
            check_constraints: Vec::new(),
            foreign_keys: Vec::new(),
            exclusion_constraints: Vec::new(),
            schema: None,
            comment: None,
            tablespace: None,
            mysql_engine: None,
            mysql_charset: None,
            mysql_collation: None,
            mysql_row_format: None,
            mysql_key_block_size: None,
            mysql_pack_keys: None,
            mysql_checksum: None,
            mysql_delay_key_write: None,
            mysql_stats_persistent: None,
            mysql_stats_auto_recalc: None,
            mysql_stats_sample_pages: None,
            mysql_avg_row_length: None,
            mysql_max_rows: None,
            mysql_min_rows: None,
            mysql_insert_method: None,
            mysql_data_directory: None,
            mysql_index_directory: None,
            mysql_connection: None,
            mysql_union: Vec::new(),
            mysql_partition_by: None,
            mysql_partitions: None,
            mysql_subpartition_by: None,
            mysql_subpartitions: None,
            mysql_auto_increment: None,
            postgres_inherits: Vec::new(),
            postgres_with: Vec::new(),
            postgres_using: None,
            postgres_unlogged: false,
            postgres_partition_by: None,
            postgres_partition_of: None,
            postgres_partition_for: None,
            sqlite_strict: false,
            sqlite_without_rowid: false,
            oracle_compress: None,
            mssql_primary_key_nonclustered: false,
        }
    }

    pub fn with_relationships(mut self, relationships: Vec<RelationshipDef>) -> Self {
        self.relationships = relationships;
        self
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn with_tablespace(mut self, tablespace: impl Into<String>) -> Self {
        self.tablespace = Some(tablespace.into());
        self
    }

    pub fn with_mysql_engine(mut self, engine: impl Into<String>) -> Self {
        self.mysql_engine = Some(engine.into());
        self
    }

    pub fn with_mysql_charset(mut self, charset: impl Into<String>) -> Self {
        self.mysql_charset = Some(charset.into());
        self
    }

    pub fn with_mysql_collation(mut self, collation: impl Into<String>) -> Self {
        self.mysql_collation = Some(collation.into());
        self
    }

    pub fn with_mysql_row_format(mut self, row_format: impl Into<String>) -> Self {
        self.mysql_row_format = Some(row_format.into());
        self
    }

    pub fn with_mysql_key_block_size(mut self, key_block_size: u32) -> Self {
        self.mysql_key_block_size = Some(key_block_size);
        self
    }

    pub fn with_mysql_pack_keys(mut self, pack_keys: bool) -> Self {
        self.mysql_pack_keys = Some(pack_keys);
        self
    }

    pub fn with_mysql_checksum(mut self, checksum: bool) -> Self {
        self.mysql_checksum = Some(checksum);
        self
    }

    pub fn with_mysql_delay_key_write(mut self, delay_key_write: bool) -> Self {
        self.mysql_delay_key_write = Some(delay_key_write);
        self
    }

    pub fn with_mysql_stats_persistent(mut self, stats_persistent: bool) -> Self {
        self.mysql_stats_persistent = Some(stats_persistent);
        self
    }

    pub fn with_mysql_stats_auto_recalc(mut self, stats_auto_recalc: bool) -> Self {
        self.mysql_stats_auto_recalc = Some(stats_auto_recalc);
        self
    }

    pub fn with_mysql_stats_sample_pages(mut self, stats_sample_pages: u32) -> Self {
        self.mysql_stats_sample_pages = Some(stats_sample_pages);
        self
    }

    pub fn with_mysql_avg_row_length(mut self, avg_row_length: u32) -> Self {
        self.mysql_avg_row_length = Some(avg_row_length);
        self
    }

    pub fn with_mysql_max_rows(mut self, max_rows: u32) -> Self {
        self.mysql_max_rows = Some(max_rows);
        self
    }

    pub fn with_mysql_min_rows(mut self, min_rows: u32) -> Self {
        self.mysql_min_rows = Some(min_rows);
        self
    }

    pub fn with_mysql_insert_method(mut self, insert_method: impl Into<String>) -> Self {
        self.mysql_insert_method = Some(insert_method.into());
        self
    }

    pub fn with_mysql_data_directory(mut self, data_directory: impl Into<String>) -> Self {
        self.mysql_data_directory = Some(data_directory.into());
        self
    }

    pub fn with_mysql_index_directory(mut self, index_directory: impl Into<String>) -> Self {
        self.mysql_index_directory = Some(index_directory.into());
        self
    }

    pub fn with_mysql_connection(mut self, connection: impl Into<String>) -> Self {
        self.mysql_connection = Some(connection.into());
        self
    }

    pub fn with_mysql_union(mut self, tables: Vec<String>) -> Self {
        self.mysql_union = tables;
        self
    }

    pub fn with_mysql_partition_by(mut self, partition_by: impl Into<String>) -> Self {
        self.mysql_partition_by = Some(partition_by.into());
        self
    }

    pub fn with_mysql_partitions(mut self, partitions: u32) -> Self {
        self.mysql_partitions = Some(partitions);
        self
    }

    pub fn with_mysql_subpartition_by(mut self, subpartition_by: impl Into<String>) -> Self {
        self.mysql_subpartition_by = Some(subpartition_by.into());
        self
    }

    pub fn with_mysql_subpartitions(mut self, subpartitions: u32) -> Self {
        self.mysql_subpartitions = Some(subpartitions);
        self
    }

    pub fn with_mysql_auto_increment(mut self, auto_increment: u32) -> Self {
        self.mysql_auto_increment = Some(auto_increment);
        self
    }

    pub fn with_mysql_options(mut self, options: MysqlTableOptions) -> Self {
        self.mysql_engine = options.engine;
        self.mysql_charset = options.charset;
        self.mysql_collation = options.collation;
        self.mysql_row_format = options.row_format;
        self.mysql_key_block_size = options.key_block_size;
        self.mysql_pack_keys = options.pack_keys;
        self.mysql_checksum = options.checksum;
        self.mysql_delay_key_write = options.delay_key_write;
        self.mysql_stats_persistent = options.stats_persistent;
        self.mysql_stats_auto_recalc = options.stats_auto_recalc;
        self.mysql_stats_sample_pages = options.stats_sample_pages;
        self.mysql_avg_row_length = options.avg_row_length;
        self.mysql_max_rows = options.max_rows;
        self.mysql_min_rows = options.min_rows;
        self.mysql_insert_method = options.insert_method;
        self.mysql_data_directory = options.data_directory;
        self.mysql_index_directory = options.index_directory;
        self.mysql_connection = options.connection;
        self.mysql_union = options.union;
        self.mysql_partition_by = options.partition_by;
        self.mysql_partitions = options.partitions;
        self.mysql_subpartition_by = options.subpartition_by;
        self.mysql_subpartitions = options.subpartitions;
        self.mysql_auto_increment = options.auto_increment;
        self
    }

    pub fn with_postgres_inherits(mut self, parents: Vec<String>) -> Self {
        self.postgres_inherits = parents;
        self
    }

    pub fn with_postgres_with(mut self, parameters: Vec<(String, String)>) -> Self {
        self.postgres_with = parameters;
        self
    }

    pub fn with_postgres_using(mut self, access_method: impl Into<String>) -> Self {
        self.postgres_using = Some(access_method.into());
        self
    }

    pub fn with_postgres_using_option(mut self, access_method: Option<String>) -> Self {
        self.postgres_using = access_method;
        self
    }

    pub fn postgres_unlogged(mut self) -> Self {
        self.postgres_unlogged = true;
        self
    }

    pub fn with_postgres_unlogged(mut self, unlogged: bool) -> Self {
        self.postgres_unlogged = unlogged;
        self
    }

    pub fn sqlite_strict(mut self) -> Self {
        self.sqlite_strict = true;
        self
    }

    pub fn with_sqlite_strict(mut self, strict: bool) -> Self {
        self.sqlite_strict = strict;
        self
    }

    pub fn sqlite_without_rowid(mut self) -> Self {
        self.sqlite_without_rowid = true;
        self
    }

    pub fn with_sqlite_without_rowid(mut self, without_rowid: bool) -> Self {
        self.sqlite_without_rowid = without_rowid;
        self
    }

    pub fn with_oracle_compress(mut self) -> Self {
        self.oracle_compress = Some(OracleTableCompression::Enabled);
        self
    }

    pub fn with_oracle_compress_level(mut self, level: u32) -> Self {
        self.oracle_compress = Some(OracleTableCompression::Level(level));
        self
    }

    pub fn with_oracle_compress_option(mut self, compress: Option<OracleTableCompression>) -> Self {
        self.oracle_compress = compress;
        self
    }

    pub fn with_mssql_primary_key_nonclustered(mut self, nonclustered: bool) -> Self {
        self.mssql_primary_key_nonclustered = nonclustered;
        self
    }

    pub fn with_postgres_partition_by(mut self, partition_by: impl Into<String>) -> Self {
        self.postgres_partition_by = Some(partition_by.into());
        self
    }

    pub fn with_postgres_partition_by_option(mut self, partition_by: Option<String>) -> Self {
        self.postgres_partition_by = partition_by;
        self
    }

    pub fn with_postgres_partition_of(mut self, parent: impl Into<String>) -> Self {
        self.postgres_partition_of = Some(parent.into());
        self
    }

    pub fn with_postgres_partition_of_option(mut self, parent: Option<String>) -> Self {
        self.postgres_partition_of = parent;
        self
    }

    pub fn with_postgres_partition_for(mut self, bound: impl Into<String>) -> Self {
        self.postgres_partition_for = Some(bound.into());
        self
    }

    pub fn with_postgres_partition_for_option(mut self, bound: Option<String>) -> Self {
        self.postgres_partition_for = bound;
        self
    }

    pub fn with_check_constraints(mut self, check_constraints: Vec<CheckConstraintDef>) -> Self {
        self.check_constraints = check_constraints;
        self
    }

    pub fn with_foreign_keys(mut self, foreign_keys: Vec<ForeignKeyDef>) -> Self {
        self.foreign_keys = foreign_keys;
        self
    }

    pub fn with_exclusion_constraints(
        mut self,
        exclusion_constraints: Vec<ExclusionConstraintDef>,
    ) -> Self {
        self.exclusion_constraints = exclusion_constraints;
        self
    }

    pub fn set_id(&mut self, id: TableId) {
        self.id = Some(id);
    }

    pub fn id(&self) -> Option<TableId> {
        self.id
    }

    pub fn model_key(&self) -> &str {
        &self.model_key
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }

    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|column| column.name())
    }

    pub fn relationships(&self) -> &[RelationshipDef] {
        &self.relationships
    }

    pub fn indexes(&self) -> &[IndexDef] {
        &self.indexes
    }

    pub fn unique_constraints(&self) -> &[UniqueConstraintDef] {
        &self.unique_constraints
    }

    pub fn check_constraints(&self) -> &[CheckConstraintDef] {
        &self.check_constraints
    }

    pub fn foreign_keys(&self) -> &[ForeignKeyDef] {
        &self.foreign_keys
    }

    pub fn exclusion_constraints(&self) -> &[ExclusionConstraintDef] {
        &self.exclusion_constraints
    }

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn tablespace(&self) -> Option<&str> {
        self.tablespace.as_deref()
    }

    pub fn mysql_engine(&self) -> Option<&str> {
        self.mysql_engine.as_deref()
    }

    pub fn mysql_charset(&self) -> Option<&str> {
        self.mysql_charset.as_deref()
    }

    pub fn mysql_collation(&self) -> Option<&str> {
        self.mysql_collation.as_deref()
    }

    pub fn mysql_row_format(&self) -> Option<&str> {
        self.mysql_row_format.as_deref()
    }

    pub fn mysql_key_block_size(&self) -> Option<u32> {
        self.mysql_key_block_size
    }

    pub fn mysql_pack_keys(&self) -> Option<bool> {
        self.mysql_pack_keys
    }

    pub fn mysql_checksum(&self) -> Option<bool> {
        self.mysql_checksum
    }

    pub fn mysql_delay_key_write(&self) -> Option<bool> {
        self.mysql_delay_key_write
    }

    pub fn mysql_stats_persistent(&self) -> Option<bool> {
        self.mysql_stats_persistent
    }

    pub fn mysql_stats_auto_recalc(&self) -> Option<bool> {
        self.mysql_stats_auto_recalc
    }

    pub fn mysql_stats_sample_pages(&self) -> Option<u32> {
        self.mysql_stats_sample_pages
    }

    pub fn mysql_avg_row_length(&self) -> Option<u32> {
        self.mysql_avg_row_length
    }

    pub fn mysql_max_rows(&self) -> Option<u32> {
        self.mysql_max_rows
    }

    pub fn mysql_min_rows(&self) -> Option<u32> {
        self.mysql_min_rows
    }

    pub fn mysql_insert_method(&self) -> Option<&str> {
        self.mysql_insert_method.as_deref()
    }

    pub fn mysql_data_directory(&self) -> Option<&str> {
        self.mysql_data_directory.as_deref()
    }

    pub fn mysql_index_directory(&self) -> Option<&str> {
        self.mysql_index_directory.as_deref()
    }

    pub fn mysql_connection(&self) -> Option<&str> {
        self.mysql_connection.as_deref()
    }

    pub fn mysql_union(&self) -> &[String] {
        &self.mysql_union
    }

    pub fn mysql_partition_by(&self) -> Option<&str> {
        self.mysql_partition_by.as_deref()
    }

    pub fn mysql_partitions(&self) -> Option<u32> {
        self.mysql_partitions
    }

    pub fn mysql_subpartition_by(&self) -> Option<&str> {
        self.mysql_subpartition_by.as_deref()
    }

    pub fn mysql_subpartitions(&self) -> Option<u32> {
        self.mysql_subpartitions
    }

    pub fn mysql_auto_increment(&self) -> Option<u32> {
        self.mysql_auto_increment
    }

    pub fn postgres_inherits(&self) -> &[String] {
        &self.postgres_inherits
    }

    pub fn postgres_with(&self) -> &[(String, String)] {
        &self.postgres_with
    }

    pub fn postgres_using(&self) -> Option<&str> {
        self.postgres_using.as_deref()
    }

    pub fn is_postgres_unlogged(&self) -> bool {
        self.postgres_unlogged
    }

    pub fn postgres_partition_by(&self) -> Option<&str> {
        self.postgres_partition_by.as_deref()
    }

    pub fn postgres_partition_of(&self) -> Option<&str> {
        self.postgres_partition_of.as_deref()
    }

    pub fn postgres_partition_for(&self) -> Option<&str> {
        self.postgres_partition_for.as_deref()
    }

    pub fn is_sqlite_strict(&self) -> bool {
        self.sqlite_strict
    }

    pub fn is_sqlite_without_rowid(&self) -> bool {
        self.sqlite_without_rowid
    }

    pub fn oracle_compress(&self) -> Option<&OracleTableCompression> {
        self.oracle_compress.as_ref()
    }

    pub fn is_mssql_primary_key_nonclustered(&self) -> bool {
        self.mssql_primary_key_nonclustered
    }

    pub fn qualified_name(&self) -> QualifiedName {
        QualifiedName::unchecked(self.schema.clone(), self.name.clone())
    }
}
