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
    use super::{normalize_driver_url, sqlite_path};

    #[test]
    fn sqlite_path_accepts_sqlalchemy_style_urls() {
        assert_eq!(sqlite_path("sqlite:///:memory:"), ":memory:");
        assert_eq!(sqlite_path("sqlite+aiosqlite:///:memory:"), ":memory:");
        assert_eq!(
            sqlite_path("sqlite:////tmp/ormdantic.sqlite3"),
            "/tmp/ormdantic.sqlite3"
        );
    }

    #[test]
    fn normalizes_python_driver_aliases_for_native_drivers() {
        let cases = [
            (
                "postgresql+asyncpg://user:pass@localhost/db",
                "postgresql://user:pass@localhost/db",
            ),
            (
                "postgres+asyncpg://user:pass@localhost/db",
                "postgres://user:pass@localhost/db",
            ),
            (
                "mysql+pymysql://user:pass@localhost/db",
                "mysql://user:pass@localhost/db",
            ),
            (
                "mysql+aiomysql://user:pass@localhost/db",
                "mysql://user:pass@localhost/db",
            ),
            (
                "mysql+asyncmy://user:pass@localhost/db",
                "mysql://user:pass@localhost/db",
            ),
            (
                "mssql+pyodbc://user:pass@localhost/db?trust_cert=true",
                "mssql://user:pass@localhost/db?trust_cert=true",
            ),
            (
                "oracle+oracledb://user:pass@localhost/FREEPDB1",
                "oracle://user:pass@localhost/FREEPDB1",
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(normalize_driver_url(input), expected);
        }
    }

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
