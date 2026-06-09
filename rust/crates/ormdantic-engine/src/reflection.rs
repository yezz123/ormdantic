use ormdantic_core::OrmdanticResult;
use ormdantic_dialects::{AnyDialect, Dialect, ReflectionQuery, ReflectionScope};
use ormdantic_schema::{ReflectedSchema, SchemaDef};

use crate::NativeConnection;

#[derive(Debug, Clone)]
pub struct Reflector {
    dialect: AnyDialect,
}

impl Reflector {
    pub fn new(dialect: AnyDialect) -> Self {
        Self { dialect }
    }

    pub fn for_url(url: &str) -> OrmdanticResult<Self> {
        Ok(Self {
            dialect: AnyDialect::parse(url)?,
        })
    }

    pub fn reflection_queries(&self, scope: &ReflectionScope) -> Vec<ReflectionQuery> {
        self.dialect.reflection_queries(scope)
    }

    pub fn empty_schema(&self) -> SchemaDef {
        ReflectedSchema::new().into_schema_def()
    }
}

pub struct Inspector<'a> {
    connection: &'a mut NativeConnection,
}

impl<'a> Inspector<'a> {
    pub fn new(connection: &'a mut NativeConnection) -> Self {
        Self { connection }
    }

    pub fn reflection_queries(
        &self,
        scope: &ReflectionScope,
    ) -> OrmdanticResult<Vec<ReflectionQuery>> {
        Ok(AnyDialect::parse(self.connection.dialect())?.reflection_queries(scope))
    }

    pub fn inspect(&mut self, scope: &ReflectionScope) -> OrmdanticResult<ReflectedSchema> {
        for query in self.reflection_queries(scope)? {
            let _ = self.connection.execute(query.sql(), &[])?;
        }
        Ok(ReflectedSchema::new())
    }
}
