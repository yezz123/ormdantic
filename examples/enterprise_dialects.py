"""Enterprise dialect URL examples."""

from ormdantic import runtime_capabilities

DATABASES = {
    "postgresql": "postgresql://postgres:postgres@localhost:5432/postgres",
    "mysql": "mysql://root:mysql@localhost:3306/mysql",
    "mariadb": "mariadb://root:mariadb@localhost:3307/mariadb",
    "mssql": "mssql://sa:Password123@localhost:1433/master?trust_cert=true",
    "oracle": "oracle://system:oracle@localhost:1521/FREEPDB1",
}


def main() -> None:
    capabilities = runtime_capabilities()
    print(capabilities)
    for dialect, url in DATABASES.items():
        if capabilities.get(dialect):
            print(f"{dialect}: {url}")


if __name__ == "__main__":
    main()
