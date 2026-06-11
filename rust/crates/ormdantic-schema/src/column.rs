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
    max_length: Option<u32>,
    precision: Option<u32>,
    scale: Option<u32>,
    sqlite_on_conflict_primary_key: Option<String>,
    sqlite_on_conflict_not_null: Option<String>,
    sqlite_on_conflict_unique: Option<String>,
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
            max_length: None,
            precision: None,
            scale: None,
            sqlite_on_conflict_primary_key: None,
            sqlite_on_conflict_not_null: None,
            sqlite_on_conflict_unique: None,
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

    pub fn with_max_length(mut self, max_length: u32) -> Self {
        self.max_length = Some(max_length);
        self
    }

    pub fn numeric(mut self, precision: u32, scale: u32) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    pub fn with_sqlite_on_conflict_primary_key(mut self, policy: impl Into<String>) -> Self {
        self.sqlite_on_conflict_primary_key = Some(policy.into());
        self
    }

    pub fn with_sqlite_on_conflict_primary_key_option(mut self, policy: Option<String>) -> Self {
        self.sqlite_on_conflict_primary_key = policy;
        self
    }

    pub fn with_sqlite_on_conflict_not_null(mut self, policy: impl Into<String>) -> Self {
        self.sqlite_on_conflict_not_null = Some(policy.into());
        self
    }

    pub fn with_sqlite_on_conflict_not_null_option(mut self, policy: Option<String>) -> Self {
        self.sqlite_on_conflict_not_null = policy;
        self
    }

    pub fn with_sqlite_on_conflict_unique(mut self, policy: impl Into<String>) -> Self {
        self.sqlite_on_conflict_unique = Some(policy.into());
        self
    }

    pub fn with_sqlite_on_conflict_unique_option(mut self, policy: Option<String>) -> Self {
        self.sqlite_on_conflict_unique = policy;
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

    pub fn definition_eq_ignoring_comment(&self, other: &Self) -> bool {
        self.name == other.name
            && self.kind == other.kind
            && self.nullable == other.nullable
            && self.primary_key == other.primary_key
            && self.default == other.default
            && self.server_default == other.server_default
            && self.identity == other.identity
            && self.computed == other.computed
            && self.autoincrement == other.autoincrement
            && self.collation == other.collation
            && self.max_length == other.max_length
            && self.precision == other.precision
            && self.scale == other.scale
            && self.sqlite_on_conflict_primary_key == other.sqlite_on_conflict_primary_key
            && self.sqlite_on_conflict_not_null == other.sqlite_on_conflict_not_null
            && self.sqlite_on_conflict_unique == other.sqlite_on_conflict_unique
    }

    pub fn precision(&self) -> Option<u32> {
        self.precision
    }

    pub fn max_length(&self) -> Option<u32> {
        self.max_length
    }

    pub fn scale(&self) -> Option<u32> {
        self.scale
    }

    pub fn sqlite_on_conflict_primary_key(&self) -> Option<&str> {
        self.sqlite_on_conflict_primary_key.as_deref()
    }

    pub fn sqlite_on_conflict_not_null(&self) -> Option<&str> {
        self.sqlite_on_conflict_not_null.as_deref()
    }

    pub fn sqlite_on_conflict_unique(&self) -> Option<&str> {
        self.sqlite_on_conflict_unique.as_deref()
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
    min_value: Option<i64>,
    max_value: Option<i64>,
    no_min_value: bool,
    no_max_value: bool,
    cache: Option<i64>,
    always: bool,
    cycle: bool,
    order: bool,
    on_null: bool,
}

impl IdentityDef {
    pub fn new() -> Self {
        Self {
            start: None,
            increment: None,
            min_value: None,
            max_value: None,
            no_min_value: false,
            no_max_value: false,
            cache: None,
            always: false,
            cycle: false,
            order: false,
            on_null: false,
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

    pub fn min_value(mut self, min_value: i64) -> Self {
        self.min_value = Some(min_value);
        self
    }

    pub fn max_value(mut self, max_value: i64) -> Self {
        self.max_value = Some(max_value);
        self
    }

    pub fn no_min_value(mut self, no_min_value: bool) -> Self {
        self.no_min_value = no_min_value;
        self
    }

    pub fn no_max_value(mut self, no_max_value: bool) -> Self {
        self.no_max_value = no_max_value;
        self
    }

    pub fn cache(mut self, cache: i64) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn always(mut self, always: bool) -> Self {
        self.always = always;
        self
    }

    pub fn cycle(mut self, cycle: bool) -> Self {
        self.cycle = cycle;
        self
    }

    pub fn order(mut self, order: bool) -> Self {
        self.order = order;
        self
    }

    pub fn on_null(mut self, on_null: bool) -> Self {
        self.on_null = on_null;
        self
    }

    pub fn start_value(&self) -> Option<i64> {
        self.start
    }

    pub fn increment_value(&self) -> Option<i64> {
        self.increment
    }

    pub fn minimum_value(&self) -> Option<i64> {
        self.min_value
    }

    pub fn maximum_value(&self) -> Option<i64> {
        self.max_value
    }

    pub fn is_no_min_value(&self) -> bool {
        self.no_min_value
    }

    pub fn is_no_max_value(&self) -> bool {
        self.no_max_value
    }

    pub fn cache_value(&self) -> Option<i64> {
        self.cache
    }

    pub fn is_always(&self) -> bool {
        self.always
    }

    pub fn is_cycle(&self) -> bool {
        self.cycle
    }

    pub fn is_ordered(&self) -> bool {
        self.order
    }

    pub fn is_on_null(&self) -> bool {
        self.on_null
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
    Enum {
        name: Option<String>,
        schema: Option<String>,
    },
    Decimal,
    Binary,
    ForeignKey {
        target_table: String,
    },
    Unknown,
}
