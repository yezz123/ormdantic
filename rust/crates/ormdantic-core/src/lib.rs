use std::fmt::{Display, Formatter};

pub type OrmdanticResult<T> = Result<T, OrmdanticError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RelationshipId(pub usize);

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
            Self::SqlCompile { message } => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for OrmdanticError {}
