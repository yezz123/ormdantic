use ormdantic_core::IsolationLevel;

pub(crate) fn render_isolation_level(isolation_level: IsolationLevel) -> &'static str {
    match isolation_level {
        IsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
        IsolationLevel::ReadCommitted => "READ COMMITTED",
        IsolationLevel::RepeatableRead => "REPEATABLE READ",
        IsolationLevel::Serializable => "SERIALIZABLE",
        IsolationLevel::Snapshot => "SNAPSHOT",
    }
}
