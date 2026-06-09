#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniqueConstraintDef {
    name: String,
    columns: Vec<String>,
}

impl UniqueConstraintDef {
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConstraintDef {
    Unique(UniqueConstraintDef),
    Check(CheckConstraintDef),
    ForeignKey(ForeignKeyDef),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckConstraintDef {
    name: Option<String>,
    expression: String,
}

impl CheckConstraintDef {
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            name: None,
            expression: expression.into(),
        }
    }

    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn expression(&self) -> &str {
        &self.expression
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
pub struct ForeignKeyDef {
    name: Option<String>,
    local_columns: Vec<String>,
    remote_table: String,
    remote_columns: Vec<String>,
    on_delete: Option<ForeignKeyAction>,
    on_update: Option<ForeignKeyAction>,
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
}
