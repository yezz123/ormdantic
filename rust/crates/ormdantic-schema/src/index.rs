#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    name: String,
    columns: Vec<String>,
    unique: bool,
    where_expr: Option<String>,
    include_columns: Vec<String>,
    method: Option<String>,
}

impl IndexDef {
    pub fn new(name: impl Into<String>, columns: Vec<String>) -> Self {
        Self {
            name: name.into(),
            columns,
            unique: false,
            where_expr: None,
            include_columns: Vec::new(),
            method: None,
        }
    }

    pub fn unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    pub fn where_expr(mut self, where_expr: impl Into<String>) -> Self {
        self.where_expr = Some(where_expr.into());
        self
    }

    pub fn include_columns(mut self, include_columns: Vec<String>) -> Self {
        self.include_columns = include_columns;
        self
    }

    pub fn method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn is_unique(&self) -> bool {
        self.unique
    }

    pub fn predicate(&self) -> Option<&str> {
        self.where_expr.as_deref()
    }

    pub fn include_columns_ref(&self) -> &[String] {
        &self.include_columns
    }

    pub fn method_name(&self) -> Option<&str> {
        self.method.as_deref()
    }
}
