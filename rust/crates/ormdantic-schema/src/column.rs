#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    name: String,
    kind: FieldKind,
    nullable: bool,
    primary_key: bool,
    default: Option<ColumnDefault>,
    server_default: Option<String>,
    identity: Option<IdentityDef>,
    computed: Option<ComputedDef>,
    autoincrement: bool,
    collation: Option<String>,
    comment: Option<String>,
    precision: Option<u32>,
    scale: Option<u32>,
}

impl ColumnDef {
    pub fn new(name: impl Into<String>, kind: FieldKind) -> Self {
        Self {
            name: name.into(),
            kind,
            nullable: false,
            primary_key: false,
            default: None,
            server_default: None,
            identity: None,
            computed: None,
            autoincrement: false,
            collation: None,
            comment: None,
            precision: None,
            scale: None,
        }
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn primary_key(mut self, primary_key: bool) -> Self {
        self.primary_key = primary_key;
        self
    }

    pub fn default(mut self, default: ColumnDefault) -> Self {
        self.default = Some(default);
        self
    }

    pub fn with_server_default(mut self, server_default: impl Into<String>) -> Self {
        self.server_default = Some(server_default.into());
        self
    }

    pub fn with_identity(mut self, identity: IdentityDef) -> Self {
        self.identity = Some(identity);
        self
    }

    pub fn with_computed(mut self, computed: ComputedDef) -> Self {
        self.computed = Some(computed);
        self
    }

    pub fn autoincrement(mut self, autoincrement: bool) -> Self {
        self.autoincrement = autoincrement;
        self
    }

    pub fn with_collation(mut self, collation: impl Into<String>) -> Self {
        self.collation = Some(collation.into());
        self
    }

    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn numeric(mut self, precision: u32, scale: u32) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> &FieldKind {
        &self.kind
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }

    pub fn default_value(&self) -> Option<&ColumnDefault> {
        self.default.as_ref()
    }

    pub fn server_default(&self) -> Option<&str> {
        self.server_default.as_deref()
    }

    pub fn identity(&self) -> Option<&IdentityDef> {
        self.identity.as_ref()
    }

    pub fn computed(&self) -> Option<&ComputedDef> {
        self.computed.as_ref()
    }

    pub fn is_autoincrement(&self) -> bool {
        self.autoincrement
    }

    pub fn collation(&self) -> Option<&str> {
        self.collation.as_deref()
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn precision(&self) -> Option<u32> {
        self.precision
    }

    pub fn scale(&self) -> Option<u32> {
        self.scale
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnDefault {
    Literal(String),
    Expression(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityDef {
    start: Option<i64>,
    increment: Option<i64>,
    always: bool,
}

impl IdentityDef {
    pub fn new() -> Self {
        Self {
            start: None,
            increment: None,
            always: false,
        }
    }

    pub fn start(mut self, start: i64) -> Self {
        self.start = Some(start);
        self
    }

    pub fn increment(mut self, increment: i64) -> Self {
        self.increment = Some(increment);
        self
    }

    pub fn always(mut self, always: bool) -> Self {
        self.always = always;
        self
    }

    pub fn start_value(&self) -> Option<i64> {
        self.start
    }

    pub fn increment_value(&self) -> Option<i64> {
        self.increment
    }

    pub fn is_always(&self) -> bool {
        self.always
    }
}

impl Default for IdentityDef {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputedDef {
    expression: String,
    persisted: bool,
}

impl ComputedDef {
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
            persisted: false,
        }
    }

    pub fn persisted(mut self, persisted: bool) -> Self {
        self.persisted = persisted;
        self
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    pub fn is_persisted(&self) -> bool {
        self.persisted
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKind {
    String,
    Integer,
    Float,
    Boolean,
    Uuid,
    Date,
    DateTime,
    Json,
    ModelJson,
    Enum,
    Decimal,
    Binary,
    ForeignKey { target_table: String },
    Unknown,
}
