use std::collections::{BTreeMap, HashMap, HashSet};

use ormdantic_core::{OrmdanticError, OrmdanticResult, QualifiedName, TableId};

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
    schema: Option<String>,
    comment: Option<String>,
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
            schema: None,
            comment: None,
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
            schema: None,
            comment: None,
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

    pub fn with_check_constraints(mut self, check_constraints: Vec<CheckConstraintDef>) -> Self {
        self.check_constraints = check_constraints;
        self
    }

    pub fn with_foreign_keys(mut self, foreign_keys: Vec<ForeignKeyDef>) -> Self {
        self.foreign_keys = foreign_keys;
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

    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn qualified_name(&self) -> QualifiedName {
        QualifiedName::unchecked(self.schema.clone(), self.name.clone())
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipDef {
    field: String,
    target_table: String,
    target_field: String,
    cardinality: RelationshipCardinality,
    back_reference: Option<String>,
    local_columns: Vec<String>,
    remote_columns: Vec<String>,
    direction: RelationshipDirection,
    uselist: bool,
    nullable: bool,
    secondary_table: Option<String>,
    cascade: Vec<CascadeAction>,
    loader_strategy: LoaderStrategy,
}

impl RelationshipDef {
    pub fn new(
        field: impl Into<String>,
        target_table: impl Into<String>,
        target_field: impl Into<String>,
        cardinality: RelationshipCardinality,
    ) -> Self {
        Self {
            field: field.into(),
            target_table: target_table.into(),
            target_field: target_field.into(),
            cardinality,
            back_reference: None,
            local_columns: Vec::new(),
            remote_columns: Vec::new(),
            direction: match cardinality {
                RelationshipCardinality::One => RelationshipDirection::ManyToOne,
                RelationshipCardinality::Many => RelationshipDirection::OneToMany,
            },
            uselist: cardinality == RelationshipCardinality::Many,
            nullable: true,
            secondary_table: None,
            cascade: Vec::new(),
            loader_strategy: LoaderStrategy::Lazy,
        }
    }

    pub fn with_back_reference(mut self, back_reference: impl Into<String>) -> Self {
        self.back_reference = Some(back_reference.into());
        self
    }

    pub fn with_columns(mut self, local_columns: Vec<String>, remote_columns: Vec<String>) -> Self {
        self.local_columns = local_columns;
        self.remote_columns = remote_columns;
        self
    }

    pub fn with_direction(mut self, direction: RelationshipDirection) -> Self {
        self.direction = direction;
        self
    }

    pub fn uselist(mut self, uselist: bool) -> Self {
        self.uselist = uselist;
        self
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn secondary_table(mut self, secondary_table: impl Into<String>) -> Self {
        self.secondary_table = Some(secondary_table.into());
        self
    }

    pub fn cascade(mut self, cascade: Vec<CascadeAction>) -> Self {
        self.cascade = cascade;
        self
    }

    pub fn loader_strategy(mut self, loader_strategy: LoaderStrategy) -> Self {
        self.loader_strategy = loader_strategy;
        self
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn target_table(&self) -> &str {
        &self.target_table
    }

    pub fn target_field(&self) -> &str {
        &self.target_field
    }

    pub fn cardinality(&self) -> &RelationshipCardinality {
        &self.cardinality
    }

    pub fn back_reference(&self) -> Option<&str> {
        self.back_reference.as_deref()
    }

    pub fn local_columns(&self) -> &[String] {
        &self.local_columns
    }

    pub fn remote_columns(&self) -> &[String] {
        &self.remote_columns
    }

    pub fn direction(&self) -> &RelationshipDirection {
        &self.direction
    }

    pub fn is_uselist(&self) -> bool {
        self.uselist
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn secondary_table_name(&self) -> Option<&str> {
        self.secondary_table.as_deref()
    }

    pub fn cascade_actions(&self) -> &[CascadeAction] {
        &self.cascade
    }

    pub fn loader_strategy_ref(&self) -> &LoaderStrategy {
        &self.loader_strategy
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipCardinality {
    One,
    Many,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationshipDirection {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CascadeAction {
    SaveUpdate,
    Merge,
    Delete,
    DeleteOrphan,
    RefreshExpire,
    Expunge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoaderStrategy {
    Lazy,
    Joined,
    SelectIn,
    Raise,
    NoLoad,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaDef {
    namespaces: Vec<NamespaceDef>,
    tables: Vec<TableDef>,
}

impl SchemaDef {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_tables(tables: Vec<TableDef>) -> Self {
        Self {
            namespaces: Vec::new(),
            tables,
        }
    }

    pub fn with_namespaces(mut self, namespaces: Vec<NamespaceDef>) -> Self {
        self.namespaces = namespaces;
        self
    }

    pub fn namespaces(&self) -> &[NamespaceDef] {
        &self.namespaces
    }

    pub fn tables(&self) -> &[TableDef] {
        &self.tables
    }

    pub fn table(&self, name: &str) -> Option<&TableDef> {
        self.tables.iter().find(|table| table.name() == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceDef {
    name: String,
    comment: Option<String>,
}

impl NamespaceDef {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            comment: None,
        }
    }

    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReflectedSchema {
    namespaces: Vec<NamespaceDef>,
    tables: Vec<ReflectedTable>,
}

impl ReflectedSchema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_namespaces(mut self, namespaces: Vec<NamespaceDef>) -> Self {
        self.namespaces = namespaces;
        self
    }

    pub fn with_tables(mut self, tables: Vec<ReflectedTable>) -> Self {
        self.tables = tables;
        self
    }

    pub fn into_schema_def(self) -> SchemaDef {
        SchemaDef {
            namespaces: self.namespaces,
            tables: self
                .tables
                .into_iter()
                .map(ReflectedTable::into_table_def)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReflectedTable {
    name: String,
    primary_key: String,
    columns: Vec<ColumnDef>,
    schema: Option<String>,
}

impl ReflectedTable {
    pub fn new(
        name: impl Into<String>,
        primary_key: impl Into<String>,
        columns: Vec<ColumnDef>,
    ) -> Self {
        Self {
            name: name.into(),
            primary_key: primary_key.into(),
            columns,
            schema: None,
        }
    }

    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    fn into_table_def(self) -> TableDef {
        let mut table = TableDef::from_parts(
            self.name.clone(),
            self.name,
            self.primary_key,
            self.columns,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        table.schema = self.schema;
        table
    }
}

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
        for column in index.columns() {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnAlias {
    table_path: String,
    column: String,
}

impl ColumnAlias {
    pub fn parse(alias: &str) -> Option<Self> {
        let (table_path, column) = alias.split_once('\\')?;
        Some(Self {
            table_path: table_path.to_string(),
            column: column.to_string(),
        })
    }

    pub fn column_for_table(&self, tablename: &str) -> Option<&str> {
        if self.table_path == tablename {
            Some(&self.column)
        } else {
            None
        }
    }

    pub fn table_path(&self) -> &str {
        &self.table_path
    }

    pub fn column(&self) -> &str {
        &self.column
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ColumnAlias, ColumnDef, FieldKind, RelationshipCardinality, RelationshipDef,
        SchemaRegistry, TableDef,
    };

    #[test]
    fn parses_column_aliases() {
        let alias = ColumnAlias::parse("flavors\\id").expect("alias should parse");

        assert_eq!(alias.column_for_table("flavors"), Some("id"));
        assert_eq!(alias.column_for_table("coffee"), None);
    }

    #[test]
    fn registers_tables() {
        let mut registry = SchemaRegistry::new();

        let table_id = registry
            .register_table(TableDef::new(
                "flavors",
                "id",
                vec!["id".to_string(), "name".to_string()],
            ))
            .expect("table should register");

        assert_eq!(table_id.0, 0);
        assert_eq!(registry.get_table("flavors").unwrap().name(), "flavors");
    }

    #[test]
    fn rejects_duplicate_tables() {
        let mut registry = SchemaRegistry::new();

        registry
            .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
            .expect("first table should register");
        let error = registry
            .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
            .expect_err("duplicate table should fail");

        assert_eq!(error.to_string(), "table 'flavors' is already registered");
    }

    #[test]
    fn rejects_duplicate_columns() {
        let mut registry = SchemaRegistry::new();

        let error = registry
            .register_table(TableDef::new(
                "flavors",
                "id",
                vec!["id".to_string(), "id".to_string()],
            ))
            .expect_err("duplicate column should fail");

        assert_eq!(
            error.to_string(),
            "column 'flavors.id' is already registered"
        );
    }

    #[test]
    fn rejects_missing_primary_key() {
        let mut registry = SchemaRegistry::new();

        let error = registry
            .register_table(TableDef::new("flavors", "id", vec!["name".to_string()]))
            .expect_err("missing pk should fail");

        assert_eq!(
            error.to_string(),
            "table 'flavors' does not define primary key column 'id'"
        );
    }

    #[test]
    fn validates_relationship_targets() {
        let mut registry = SchemaRegistry::new();
        registry
            .register_table(TableDef::new("flavors", "id", vec!["id".to_string()]))
            .expect("target should register");
        registry
            .register_table(TableDef::from_parts(
                "coffee",
                "Coffee",
                "id",
                vec![
                    ColumnDef::new("id", FieldKind::Uuid),
                    ColumnDef::new(
                        "flavor",
                        FieldKind::ForeignKey {
                            target_table: "flavors".to_string(),
                        },
                    ),
                ],
                Vec::new(),
                Vec::new(),
                vec![RelationshipDef::new(
                    "flavor",
                    "flavors",
                    "id",
                    RelationshipCardinality::One,
                )],
            ))
            .expect("source should register");

        registry
            .validate_relationships()
            .expect("relationships should be valid");
    }

    #[test]
    fn rejects_unknown_relationship_target() {
        let mut registry = SchemaRegistry::new();
        registry
            .register_table(TableDef::from_parts(
                "coffee",
                "Coffee",
                "id",
                vec![ColumnDef::new("id", FieldKind::Uuid)],
                Vec::new(),
                Vec::new(),
                vec![RelationshipDef::new(
                    "flavor",
                    "flavors",
                    "id",
                    RelationshipCardinality::One,
                )],
            ))
            .expect("source should register");

        let error = registry
            .validate_relationships()
            .expect_err("relationship target should fail");

        assert_eq!(
            error.to_string(),
            "relationship 'coffee.flavor' targets unknown table 'flavors'"
        );
    }
}
