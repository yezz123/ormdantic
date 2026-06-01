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
        .replace("mariadb://", "mysql://")
        .replace("mariadb+mariadbconnector://", "mysql://")
        .replace("mssql+pyodbc://", "mssql://")
        .replace("oracle+oracledb://", "oracle://")
}

#[cfg(test)]
mod tests {
    use super::normalize_driver_url;

    #[test]
    fn normalizes_mariadb_urls_for_mysql_driver() {
        assert_eq!(
            normalize_driver_url("mariadb://root:mariadb@localhost:3307/mariadb"),
            "mysql://root:mariadb@localhost:3307/mariadb"
        );
        assert_eq!(
            normalize_driver_url("mariadb+mariadbconnector://root:mariadb@localhost:3307/mariadb",),
            "mysql://root:mariadb@localhost:3307/mariadb"
        );
    }
}
