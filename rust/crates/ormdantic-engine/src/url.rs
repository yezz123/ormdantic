pub fn sqlite_path(url: &str) -> String {
    if url == "sqlite:///:memory:" || url == "sqlite+aiosqlite:///:memory:" {
        return ":memory:".to_string();
    }
    url.split_once(":///")
        .map(|(_, path)| path.to_string())
        .unwrap_or_else(|| url.to_string())
}

pub fn normalize_driver_url(url: &str) -> String {
    url.replace("postgresql+asyncpg://", "postgresql://")
        .replace("postgres+asyncpg://", "postgres://")
        .replace("mysql+pymysql://", "mysql://")
        .replace("mysql+aiomysql://", "mysql://")
        .replace("mysql+asyncmy://", "mysql://")
        .replace("mariadb+mariadbconnector://", "mysql://")
        .replace("mssql+pyodbc://", "mssql://")
        .replace("oracle+oracledb://", "oracle://")
}
