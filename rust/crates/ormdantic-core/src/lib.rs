use std::fmt::{Display, Formatter};

pub type OrmdanticResult<T> = Result<T, OrmdanticError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrmdanticError {
    MissingPrimaryKeyAlias {
        tablename: String,
        primary_key: String,
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
        }
    }
}

impl std::error::Error for OrmdanticError {}
