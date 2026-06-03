//! Shared primitives used by Ormdantic's Rust crates.
//!
//! ```
//! use ormdantic_core::{BackendFeature, FeatureSet, Identifier, QualifiedName};
//!
//! let table = Identifier::new("coffee")?;
//! assert_eq!(table.as_str(), "coffee");
//! assert!(Identifier::new("not-a-valid-identifier").is_err());
//!
//! let qualified = QualifiedName::with_schema("public", "coffee")?;
//! assert_eq!(qualified.to_string(), "public.coffee");
//!
//! let features = FeatureSet::new([BackendFeature::Returning, BackendFeature::Returning]);
//! assert_eq!(features.features(), &[BackendFeature::Returning]);
//!
//! # Ok::<(), ormdantic_core::OrmdanticError>(())
//! ```

use std::fmt::{Display, Formatter};

pub type OrmdanticResult<T> = Result<T, OrmdanticError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelationshipId(pub usize);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identifier(String);

impl Identifier {
    pub fn new(value: impl Into<String>) -> OrmdanticResult<Self> {
        let value = value.into();
        if is_valid_identifier(&value) {
            Ok(Self(value))
        } else {
            Err(OrmdanticError::InvalidIdentifier { identifier: value })
        }
    }

    pub fn unchecked(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl Display for Identifier {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<Identifier> for String {
    fn from(value: Identifier) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QualifiedName {
    schema: Option<Identifier>,
    name: Identifier,
}

impl QualifiedName {
    pub fn new(name: impl Into<String>) -> OrmdanticResult<Self> {
        Ok(Self {
            schema: None,
            name: Identifier::new(name)?,
        })
    }

    pub fn with_schema(
        schema: impl Into<String>,
        name: impl Into<String>,
    ) -> OrmdanticResult<Self> {
        Ok(Self {
            schema: Some(Identifier::new(schema)?),
            name: Identifier::new(name)?,
        })
    }

    pub fn unchecked(schema: Option<impl Into<String>>, name: impl Into<String>) -> Self {
        Self {
            schema: schema.map(Identifier::unchecked),
            name: Identifier::unchecked(name),
        }
    }

    pub fn schema(&self) -> Option<&Identifier> {
        self.schema.as_ref()
    }

    pub fn name(&self) -> &Identifier {
        &self.name
    }
}

impl Display for QualifiedName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = &self.schema {
            write!(formatter, "{schema}.{}", self.name)
        } else {
            write!(formatter, "{}", self.name)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BackendFeature {
    Ctes,
    Windows,
    Savepoints,
    TransactionalDdl,
    PartialIndexes,
    ExpressionIndexes,
    NativeEnum,
    NativeJson,
    NativeUuid,
    AlterColumn,
    Returning,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FeatureSet {
    features: Vec<BackendFeature>,
}

impl FeatureSet {
    pub fn new(features: impl IntoIterator<Item = BackendFeature>) -> Self {
        let mut features = features.into_iter().collect::<Vec<_>>();
        features.sort_by_key(|feature| *feature as u8);
        features.dedup();
        Self { features }
    }

    pub fn contains(&self, feature: BackendFeature) -> bool {
        self.features.contains(&feature)
    }

    pub fn insert(&mut self, feature: BackendFeature) {
        if !self.contains(feature) {
            self.features.push(feature);
            self.features.sort_by_key(|feature| *feature as u8);
        }
    }

    pub fn features(&self) -> &[BackendFeature] {
        &self.features
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionAccessMode {
    ReadWrite,
    ReadOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeferrableMode {
    Deferrable,
    NotDeferrable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionOptions {
    isolation_level: Option<IsolationLevel>,
    access_mode: TransactionAccessMode,
    deferrable_mode: Option<DeferrableMode>,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            isolation_level: None,
            access_mode: TransactionAccessMode::ReadWrite,
            deferrable_mode: None,
        }
    }
}

impl TransactionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = Some(isolation_level);
        self
    }

    pub fn read_only(mut self) -> Self {
        self.access_mode = TransactionAccessMode::ReadOnly;
        self
    }

    pub fn with_access_mode(mut self, access_mode: TransactionAccessMode) -> Self {
        self.access_mode = access_mode;
        self
    }

    pub fn with_deferrable_mode(mut self, deferrable_mode: DeferrableMode) -> Self {
        self.deferrable_mode = Some(deferrable_mode);
        self
    }

    pub fn isolation_level(&self) -> Option<IsolationLevel> {
        self.isolation_level
    }

    pub fn access_mode(&self) -> TransactionAccessMode {
        self.access_mode
    }

    pub fn deferrable_mode(&self) -> Option<DeferrableMode> {
        self.deferrable_mode
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SavepointName(Identifier);

impl SavepointName {
    pub fn new(name: impl Into<String>) -> OrmdanticResult<Self> {
        Ok(Self(Identifier::new(name)?))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for SavepointName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    BeforeExecute,
    AfterExecute,
    BeforeCommit,
    AfterCommit,
    AfterRollback,
    BeforeFlush,
    AfterFlush,
    BeforeMigration,
    AfterMigration,
    BeforeReflection,
    AfterReflection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventPayload {
    kind: EventKind,
    target: Option<String>,
    message: Option<String>,
}

impl EventPayload {
    pub fn new(kind: EventKind) -> Self {
        Self {
            kind,
            target: None,
            message: None,
        }
    }

    pub fn with_target(mut self, target: impl Into<String>) -> Self {
        self.target = Some(target.into());
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn kind(&self) -> EventKind {
        self.kind
    }

    pub fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }

    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdentityKey {
    model_key: String,
    primary_key: Vec<String>,
}

impl IdentityKey {
    pub fn new(model_key: impl Into<String>, primary_key: Vec<String>) -> Self {
        Self {
            model_key: model_key.into(),
            primary_key,
        }
    }

    pub fn model_key(&self) -> &str {
        &self.model_key
    }

    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RevisionId(String);

impl RevisionId {
    pub fn new(value: impl Into<String>) -> OrmdanticResult<Self> {
        let value = value.into();
        if value.trim().is_empty() {
            Err(OrmdanticError::MigrationError {
                message: "revision id cannot be empty".to_string(),
            })
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for RevisionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrmdanticError {
    MissingPrimaryKeyAlias {
        tablename: String,
        primary_key: String,
    },
    DuplicateTable {
        tablename: String,
    },
    DuplicateColumn {
        tablename: String,
        column: String,
    },
    MissingPrimaryKey {
        tablename: String,
        primary_key: String,
    },
    UnknownTable {
        tablename: String,
    },
    InvalidRelationship {
        table: String,
        field: String,
        target_table: String,
    },
    UnsupportedDialect {
        dialect: String,
    },
    InvalidIdentifier {
        identifier: String,
    },
    UnsupportedFeature {
        feature: String,
        dialect: String,
    },
    TransactionError {
        message: String,
    },
    ReflectionError {
        message: String,
    },
    MigrationError {
        message: String,
    },
    SchemaDiffError {
        message: String,
    },
    UnitOfWorkError {
        message: String,
    },
    EventError {
        message: String,
    },
    ExecutionError {
        message: String,
    },
    SqlCompile {
        message: String,
    },
}

impl Display for OrmdanticError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingPrimaryKeyAlias {
                tablename,
                primary_key,
            } => write!(
                formatter,
                "primary key column '{tablename}\\{primary_key}' was not found"
            ),
            Self::DuplicateTable { tablename } => {
                write!(formatter, "table '{tablename}' is already registered")
            }
            Self::DuplicateColumn { tablename, column } => {
                write!(
                    formatter,
                    "column '{tablename}.{column}' is already registered"
                )
            }
            Self::MissingPrimaryKey {
                tablename,
                primary_key,
            } => write!(
                formatter,
                "table '{tablename}' does not define primary key column '{primary_key}'"
            ),
            Self::UnknownTable { tablename } => {
                write!(formatter, "table '{tablename}' is not registered")
            }
            Self::InvalidRelationship {
                table,
                field,
                target_table,
            } => write!(
                formatter,
                "relationship '{table}.{field}' targets unknown table '{target_table}'"
            ),
            Self::UnsupportedDialect { dialect } => {
                write!(formatter, "dialect '{dialect}' is not supported")
            }
            Self::InvalidIdentifier { identifier } => {
                write!(formatter, "identifier '{identifier}' is not valid")
            }
            Self::UnsupportedFeature { feature, dialect } => {
                write!(
                    formatter,
                    "feature '{feature}' is not supported by dialect '{dialect}'"
                )
            }
            Self::TransactionError { message } => write!(formatter, "transaction error: {message}"),
            Self::ReflectionError { message } => write!(formatter, "reflection error: {message}"),
            Self::MigrationError { message } => write!(formatter, "migration error: {message}"),
            Self::SchemaDiffError { message } => write!(formatter, "schema diff error: {message}"),
            Self::UnitOfWorkError { message } => write!(formatter, "unit of work error: {message}"),
            Self::EventError { message } => write!(formatter, "event error: {message}"),
            Self::ExecutionError { message } => write!(formatter, "execution error: {message}"),
            Self::SqlCompile { message } => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for OrmdanticError {}

fn is_valid_identifier(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::{
        BackendFeature, FeatureSet, Identifier, IsolationLevel, SavepointName, TransactionOptions,
    };

    #[test]
    fn validates_identifiers() {
        assert_eq!(
            Identifier::new("valid_name").unwrap().as_str(),
            "valid_name"
        );
        assert!(Identifier::new("1_invalid").is_err());
    }

    #[test]
    fn stores_backend_features() {
        let mut features = FeatureSet::new([BackendFeature::Ctes]);
        features.insert(BackendFeature::Savepoints);

        assert!(features.contains(BackendFeature::Ctes));
        assert!(features.contains(BackendFeature::Savepoints));
    }

    #[test]
    fn builds_transaction_options_and_savepoints() {
        let options = TransactionOptions::new().with_isolation_level(IsolationLevel::Serializable);
        let savepoint = SavepointName::new("sp_1").unwrap();

        assert_eq!(
            options.isolation_level(),
            Some(IsolationLevel::Serializable)
        );
        assert_eq!(savepoint.as_str(), "sp_1");
    }
}
