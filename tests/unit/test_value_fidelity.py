from __future__ import annotations

from decimal import Decimal

from ormdantic._ormdantic import execute_native, sql_value


def test_sql_value_preserves_decimal_objects() -> None:
    value = sql_value(Decimal("12345678901234567890.123456789"))

    assert value == Decimal("12345678901234567890.123456789")
    assert isinstance(value, Decimal)


def test_sql_value_preserves_large_unsigned_python_ints() -> None:
    value = sql_value(2**64 - 1)

    assert value == 2**64 - 1
    assert isinstance(value, int)


def test_sqlite_declared_numeric_columns_return_decimal(tmp_path) -> None:
    url = f"sqlite:///{tmp_path / 'decimal.sqlite3'}"
    high_precision = Decimal("12345678901234567890.123456789")

    execute_native(
        url,
        "CREATE TABLE prices (id INTEGER PRIMARY KEY, amount DECIMAL_TEXT(30, 9), label TEXT)",
        [],
    )
    execute_native(
        url,
        "INSERT INTO prices (id, amount, label) VALUES (?1, ?2, ?3)",
        [1, high_precision, str(high_precision)],
    )
    result = execute_native(
        url,
        "SELECT amount, label, typeof(amount) FROM prices WHERE id = ?1",
        [1],
    )

    amount, label, storage_type = result["rows"][0]
    assert amount == high_precision
    assert isinstance(amount, Decimal)
    assert label == str(high_precision)
    assert storage_type == "text"


def test_sqlite_declared_integer_columns_return_large_unsigned_int(tmp_path) -> None:
    url = f"sqlite:///{tmp_path / 'unsigned.sqlite3'}"
    unsigned = 2**64 - 1

    execute_native(
        url,
        "CREATE TABLE counters (id INTEGER PRIMARY KEY, value INTEGER)",
        [],
    )
    execute_native(
        url,
        "INSERT INTO counters (id, value) VALUES (?1, ?2)",
        [1, unsigned],
    )
    result = execute_native(
        url,
        "SELECT value, typeof(value) FROM counters WHERE id = ?1",
        [1],
    )

    value, storage_type = result["rows"][0]
    assert value == unsigned
    assert isinstance(value, int)
    assert storage_type == "blob"
