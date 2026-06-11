#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleIndexCompression {
    Enabled,
    Prefix(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueConstraintDef {
    name: String,
    columns: Vec<String>,
    timing: ConstraintTiming,
    nulls_not_distinct: bool,
    sqlite_on_conflict: Option<String>,
    mssql_filegroup: Option<String>,
    mssql_clustered: Option<bool>,
    oracle_tablespace: Option<String>,
    oracle_compress: Option<OracleIndexCompression>,
}

impl UniqueConstraintDef {
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
            timing: ConstraintTiming::default(),
            nulls_not_distinct: false,
            sqlite_on_conflict: None,
            mssql_filegroup: None,
            mssql_clustered: None,
            oracle_tablespace: None,
            oracle_compress: None,
        }
    }

    pub fn with_timing(mut self, timing: ConstraintTiming) -> Self {
        self.timing = timing;
        self
    }

    pub fn nulls_not_distinct(mut self) -> Self {
        self.nulls_not_distinct = true;
        self
    }

    pub fn with_nulls_not_distinct(mut self, nulls_not_distinct: bool) -> Self {
        self.nulls_not_distinct = nulls_not_distinct;
        self
    }

    pub fn with_sqlite_on_conflict(mut self, policy: impl Into<String>) -> Self {
        self.sqlite_on_conflict = Some(policy.into());
        self
    }

    pub fn with_sqlite_on_conflict_option(mut self, policy: Option<String>) -> Self {
        self.sqlite_on_conflict = policy;
        self
    }

    pub fn with_mssql_filegroup(mut self, filegroup: impl Into<String>) -> Self {
        self.mssql_filegroup = Some(filegroup.into());
        self
    }

    pub fn with_mssql_filegroup_option(mut self, filegroup: Option<String>) -> Self {
        self.mssql_filegroup = filegroup;
        self
    }

    pub fn with_mssql_clustered(mut self, clustered: bool) -> Self {
        self.mssql_clustered = Some(clustered);
        self
    }

    pub fn with_mssql_clustered_option(mut self, clustered: Option<bool>) -> Self {
        self.mssql_clustered = clustered;
        self
    }

    pub fn with_oracle_tablespace(mut self, tablespace: impl Into<String>) -> Self {
        self.oracle_tablespace = Some(tablespace.into());
        self
    }

    pub fn with_oracle_tablespace_option(mut self, tablespace: Option<String>) -> Self {
        self.oracle_tablespace = tablespace;
        self
    }

    pub fn with_oracle_compress(mut self) -> Self {
        self.oracle_compress = Some(OracleIndexCompression::Enabled);
        self
    }

    pub fn with_oracle_compress_prefix(mut self, prefix_length: u32) -> Self {
        self.oracle_compress = Some(OracleIndexCompression::Prefix(prefix_length));
        self
    }

    pub fn with_oracle_compress_option(mut self, compress: Option<OracleIndexCompression>) -> Self {
        self.oracle_compress = compress;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn timing(&self) -> &ConstraintTiming {
        &self.timing
    }

    pub fn is_nulls_not_distinct(&self) -> bool {
        self.nulls_not_distinct
    }

    pub fn sqlite_on_conflict(&self) -> Option<&str> {
        self.sqlite_on_conflict.as_deref()
    }

    pub fn mssql_filegroup(&self) -> Option<&str> {
        self.mssql_filegroup.as_deref()
    }

    pub fn mssql_clustered(&self) -> Option<bool> {
        self.mssql_clustered
    }

    pub fn oracle_tablespace(&self) -> Option<&str> {
        self.oracle_tablespace.as_deref()
    }

    pub fn oracle_compress(&self) -> Option<&OracleIndexCompression> {
        self.oracle_compress.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintDef {
    Unique(UniqueConstraintDef),
    Check(CheckConstraintDef),
    ForeignKey(ForeignKeyDef),
    Exclusion(ExclusionConstraintDef),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConstraintTiming {
    deferrable: Option<bool>,
    initially_deferred: bool,
}

impl ConstraintTiming {
    pub fn new(deferrable: Option<bool>, initially_deferred: bool) -> Self {
        Self {
            deferrable: if initially_deferred && deferrable.is_none() {
                Some(true)
            } else {
                deferrable
            },
            initially_deferred,
        }
    }

    pub fn deferrable(&self) -> Option<bool> {
        self.deferrable
    }

    pub fn initially_deferred(&self) -> bool {
        self.initially_deferred
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraintDef {
    name: Option<String>,
    expression: String,
    validated: bool,
    no_inherit: bool,
}

impl CheckConstraintDef {
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            name: None,
            expression: expression.into(),
            validated: true,
            no_inherit: false,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn validated(mut self, validated: bool) -> Self {
        self.validated = validated;
        self
    }

    pub fn not_validated(self) -> Self {
        self.validated(false)
    }

    pub fn no_inherit(mut self) -> Self {
        self.no_inherit = true;
        self
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    pub fn is_validated(&self) -> bool {
        self.validated
    }

    pub fn is_no_inherit(&self) -> bool {
        self.no_inherit
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExclusionElementDef {
    expression: String,
    operator: String,
    quoted: bool,
    opclass: Option<String>,
}

impl ExclusionElementDef {
    pub fn column(column: impl Into<String>, operator: impl Into<String>) -> Self {
        Self {
            expression: column.into(),
            operator: operator.into(),
            quoted: true,
            opclass: None,
        }
    }

    pub fn expression(expression: impl Into<String>, operator: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
            operator: operator.into(),
            quoted: false,
            opclass: None,
        }
    }

    pub fn opclass(mut self, opclass: impl Into<String>) -> Self {
        self.opclass = Some(opclass.into());
        self
    }

    pub fn value(&self) -> &str {
        &self.expression
    }

    pub fn operator(&self) -> &str {
        &self.operator
    }

    pub fn operator_class(&self) -> Option<&str> {
        self.opclass.as_deref()
    }

    pub fn is_quoted(&self) -> bool {
        self.quoted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExclusionConstraintDef {
    name: String,
    elements: Vec<ExclusionElementDef>,
    method: String,
    predicate: Option<String>,
    timing: ConstraintTiming,
}

impl ExclusionConstraintDef {
    pub fn new(name: impl Into<String>, elements: Vec<ExclusionElementDef>) -> Self {
        Self {
            name: name.into(),
            elements,
            method: "gist".to_string(),
            predicate: None,
            timing: ConstraintTiming::default(),
        }
    }

    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.method = method.into();
        self
    }

    pub fn where_expr(mut self, predicate: impl Into<String>) -> Self {
        self.predicate = Some(predicate.into());
        self
    }

    pub fn with_timing(mut self, timing: ConstraintTiming) -> Self {
        self.timing = timing;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn elements(&self) -> &[ExclusionElementDef] {
        &self.elements
    }

    pub fn method_name(&self) -> &str {
        &self.method
    }

    pub fn predicate(&self) -> Option<&str> {
        self.predicate.as_deref()
    }

    pub fn timing(&self) -> &ConstraintTiming {
        &self.timing
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForeignKeyAction {
    Cascade,
    Restrict,
    SetNull,
    SetDefault,
    NoAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForeignKeyMatch {
    Simple,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyDef {
    name: Option<String>,
    local_columns: Vec<String>,
    remote_table: String,
    remote_columns: Vec<String>,
    on_delete: Option<ForeignKeyAction>,
    on_update: Option<ForeignKeyAction>,
    timing: ConstraintTiming,
    validated: bool,
    match_type: Option<ForeignKeyMatch>,
}

impl ForeignKeyDef {
    pub fn new(
        local_columns: Vec<String>,
        remote_table: impl Into<String>,
        remote_columns: Vec<String>,
    ) -> Self {
        Self {
            name: None,
            local_columns,
            remote_table: remote_table.into(),
            remote_columns,
            on_delete: None,
            on_update: None,
            timing: ConstraintTiming::default(),
            validated: true,
            match_type: None,
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn on_delete(mut self, action: ForeignKeyAction) -> Self {
        self.on_delete = Some(action);
        self
    }

    pub fn on_update(mut self, action: ForeignKeyAction) -> Self {
        self.on_update = Some(action);
        self
    }

    pub fn with_timing(mut self, timing: ConstraintTiming) -> Self {
        self.timing = timing;
        self
    }

    pub fn with_match(mut self, match_type: ForeignKeyMatch) -> Self {
        self.match_type = Some(match_type);
        self
    }

    pub fn validated(mut self, validated: bool) -> Self {
        self.validated = validated;
        self
    }

    pub fn not_validated(self) -> Self {
        self.validated(false)
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn local_columns(&self) -> &[String] {
        &self.local_columns
    }

    pub fn remote_table(&self) -> &str {
        &self.remote_table
    }

    pub fn remote_columns(&self) -> &[String] {
        &self.remote_columns
    }

    pub fn on_delete_action(&self) -> Option<&ForeignKeyAction> {
        self.on_delete.as_ref()
    }

    pub fn on_update_action(&self) -> Option<&ForeignKeyAction> {
        self.on_update.as_ref()
    }

    pub fn timing(&self) -> &ConstraintTiming {
        &self.timing
    }

    pub fn match_type(&self) -> Option<&ForeignKeyMatch> {
        self.match_type.as_ref()
    }

    pub fn is_validated(&self) -> bool {
        self.validated
    }
}
