use ormdantic_core::OrmdanticResult;
use regex::Regex;
use rusqlite::functions::FunctionFlags;
use rusqlite::types::ValueRef;
use rusqlite::{params_from_iter, Connection};
use std::cmp::Ordering;
use std::io;

use crate::url::sqlite_path;
use crate::{sql_error, DbValue, QueryResult};

pub struct SqliteConnection {
    connection: Connection,
}

impl SqliteConnection {
    pub fn open(url: &str) -> OrmdanticResult<Self> {
        let connection = Connection::open(sqlite_path(url)).map_err(sql_error)?;
        register_sqlite_functions(&connection)?;
        Ok(Self { connection })
    }

    pub fn execute(&mut self, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
        execute_connection(&mut self.connection, sql, params)
    }
}

pub fn execute_url(url: &str, sql: &str, params: &[DbValue]) -> OrmdanticResult<QueryResult> {
    let mut connection = SqliteConnection::open(url)?;
    connection.execute(sql, params)
}

pub fn execute_connection(
    connection: &mut Connection,
    sql: &str,
    params: &[DbValue],
) -> OrmdanticResult<QueryResult> {
    if crate::returns_rows(sql) {
        let mut statement = connection.prepare(sql).map_err(sql_error)?;
        let columns = statement
            .column_names()
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let column_decl_types = statement
            .columns()
            .into_iter()
            .map(|column| column.decl_type().map(str::to_string))
            .collect::<Vec<_>>();
        let column_count = statement.column_count();
        let rows = statement
            .query_map(params_from_iter(params.iter()), |row| {
                let mut values = Vec::with_capacity(column_count);
                for idx in 0..column_count {
                    values.push(sqlite_value(
                        row.get_ref(idx)?,
                        column_decl_types
                            .get(idx)
                            .and_then(|value| value.as_deref()),
                    ));
                }
                Ok(values)
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        Ok(QueryResult::new(columns, rows))
    } else {
        let row_count = connection
            .execute(sql, params_from_iter(params.iter()))
            .map_err(sql_error)?;
        Ok(QueryResult::affected(row_count as u64))
    }
}

fn sqlite_value(value: ValueRef<'_>, decl_type: Option<&str>) -> DbValue {
    if sqlite_decl_type_is_decimal(decl_type) {
        return sqlite_decimal_value(value);
    }
    if sqlite_decl_type_is_integer(decl_type) {
        return sqlite_integer_value(value);
    }
    match value {
        ValueRef::Null => DbValue::Null,
        ValueRef::Integer(value) => DbValue::Integer(value),
        ValueRef::Real(value) => DbValue::Real(value),
        ValueRef::Text(value) => DbValue::Text(sqlite_text_value(value)),
        ValueRef::Blob(value) => DbValue::Text(sqlite_text_value(value)),
    }
}

fn sqlite_integer_value(value: ValueRef<'_>) -> DbValue {
    match value {
        ValueRef::Null => DbValue::Null,
        ValueRef::Integer(value) => DbValue::Integer(value),
        ValueRef::Real(value) => DbValue::Real(value),
        ValueRef::Text(value) | ValueRef::Blob(value) => sqlite_integer_text_value(value),
    }
}

fn sqlite_integer_text_value(value: &[u8]) -> DbValue {
    let text = sqlite_text_value(value);
    if let Ok(value) = text.parse::<i64>() {
        return DbValue::Integer(value);
    }
    if let Ok(value) = text.parse::<u64>() {
        return DbValue::UnsignedInteger(value);
    }
    DbValue::Text(text)
}

fn sqlite_decimal_value(value: ValueRef<'_>) -> DbValue {
    match value {
        ValueRef::Null => DbValue::Null,
        ValueRef::Integer(value) => DbValue::Decimal(value.to_string()),
        ValueRef::Real(value) => DbValue::Decimal(value.to_string()),
        ValueRef::Text(value) | ValueRef::Blob(value) => DbValue::Decimal(sqlite_text_value(value)),
    }
}

fn sqlite_decl_type_is_decimal(decl_type: Option<&str>) -> bool {
    let Some(decl_type) = decl_type else {
        return false;
    };
    let normalized = decl_type.trim().to_ascii_uppercase();
    normalized.contains("DECIMAL")
        || normalized.contains("NUMERIC")
        || normalized.contains("NUMBER")
}

fn sqlite_decl_type_is_integer(decl_type: Option<&str>) -> bool {
    let Some(decl_type) = decl_type else {
        return false;
    };
    decl_type.trim().to_ascii_uppercase().contains("INT")
}

fn sqlite_text_value(value: &[u8]) -> String {
    String::from_utf8_lossy(value).to_string()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedDecimal {
    negative: bool,
    point: i64,
    digits: String,
}

fn register_sqlite_functions(connection: &Connection) -> OrmdanticResult<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    connection
        .create_scalar_function("ormdantic_decimal_cmp", 2, flags, |ctx| {
            let left = sqlite_function_text(ctx.get_raw(0));
            let right = sqlite_function_text(ctx.get_raw(1));
            match (left, right) {
                (Some(left), Some(right)) => decimal_cmp(&left, &right)
                    .map(ordering_value)
                    .map(Some)
                    .map_err(sqlite_function_error),
                _ => Ok(None),
            }
        })
        .map_err(sql_error)?;
    connection
        .create_scalar_function("ormdantic_decimal_sort_key", 1, flags, |ctx| {
            let value = sqlite_function_text(ctx.get_raw(0));
            value
                .map(|value| decimal_sort_key(&value).map_err(sqlite_function_error))
                .transpose()
        })
        .map_err(sql_error)?;
    connection
        .create_scalar_function("ormdantic_regex_match", 2, flags, |ctx| {
            let value = sqlite_function_text(ctx.get_raw(0));
            let pattern = sqlite_function_text(ctx.get_raw(1));
            match (value, pattern) {
                (Some(value), Some(pattern)) => regex_match(&value, &pattern)
                    .map(Some)
                    .map_err(sqlite_function_error),
                _ => Ok(None),
            }
        })
        .map_err(sql_error)?;
    connection
        .create_scalar_function("ormdantic_decimal_multiple_of", 2, flags, |ctx| {
            let value = sqlite_function_text(ctx.get_raw(0));
            let multiple = sqlite_function_text(ctx.get_raw(1));
            match (value, multiple) {
                (Some(value), Some(multiple)) => decimal_multiple_of(&value, &multiple)
                    .map(Some)
                    .map_err(sqlite_function_error),
                _ => Ok(None),
            }
        })
        .map_err(sql_error)?;
    Ok(())
}

fn sqlite_function_text(value: ValueRef<'_>) -> Option<String> {
    match value {
        ValueRef::Null => None,
        ValueRef::Integer(value) => Some(value.to_string()),
        ValueRef::Real(value) => Some(value.to_string()),
        ValueRef::Text(value) | ValueRef::Blob(value) => {
            Some(String::from_utf8_lossy(value).to_string())
        }
    }
}

fn regex_match(value: &str, pattern: &str) -> Result<bool, String> {
    Regex::new(pattern)
        .map_err(|error| error.to_string())
        .map(|regex| regex.is_match(value))
}

fn decimal_cmp(left: &str, right: &str) -> Result<Ordering, String> {
    let left = normalize_decimal(left)?;
    let right = normalize_decimal(right)?;
    Ok(compare_normalized_decimals(&left, &right))
}

fn decimal_sort_key(value: &str) -> Result<String, String> {
    let value = normalize_decimal(value)?;
    if value.digits == "0" {
        return Ok("1|".to_string());
    }
    let key = decimal_point_key(value.point);
    if value.negative {
        let inverted_key = u64::MAX - key;
        let inverted_digits = value
            .digits
            .bytes()
            .map(|digit| char::from(b'9' - (digit - b'0')))
            .collect::<String>();
        return Ok(format!("0|{inverted_key:020}|{inverted_digits}:"));
    }
    Ok(format!("2|{key:020}|{}/", value.digits))
}

fn decimal_multiple_of(value: &str, multiple: &str) -> Result<bool, String> {
    let value = normalize_decimal(value)?;
    let multiple = normalize_decimal(multiple)?;
    if multiple.digits == "0" {
        return Err("multiple_of value cannot be zero".to_string());
    }
    if value.digits == "0" {
        return Ok(true);
    }
    let value_scale = decimal_scale(&value)?;
    let multiple_scale = decimal_scale(&multiple)?;
    let scale_delta = multiple_scale
        .checked_sub(value_scale)
        .ok_or_else(|| "decimal scale overflow".to_string())?;
    let (mut numerator, mut denominator) = (value.digits.clone(), multiple.digits.clone());
    if scale_delta >= 0 {
        append_decimal_zeros(&mut numerator, scale_delta)?;
    } else {
        append_decimal_zeros(&mut denominator, -scale_delta)?;
    }
    decimal_digits_divisible(&numerator, &denominator)
}

fn decimal_scale(value: &NormalizedDecimal) -> Result<i64, String> {
    i64::try_from(value.digits.len())
        .map_err(|_| "decimal scale overflow".to_string())?
        .checked_sub(value.point)
        .ok_or_else(|| "decimal scale overflow".to_string())
}

fn append_decimal_zeros(value: &mut String, count: i64) -> Result<(), String> {
    let count = usize::try_from(count).map_err(|_| "decimal scale overflow".to_string())?;
    value.reserve(count);
    for _ in 0..count {
        value.push('0');
    }
    Ok(())
}

fn decimal_digits_divisible(numerator: &str, denominator: &str) -> Result<bool, String> {
    if denominator == "0" {
        return Err("division by zero".to_string());
    }
    let denominator = trim_decimal_digit_zeros(denominator);
    let mut remainder = String::from("0");
    for digit in numerator.bytes() {
        if !digit.is_ascii_digit() {
            return Err("decimal digits must contain only digits".to_string());
        }
        if remainder == "0" {
            remainder.clear();
        }
        remainder.push(char::from(digit));
        remainder = trim_decimal_digit_zeros(&remainder).to_string();
        while compare_decimal_digits(&remainder, denominator) != Ordering::Less {
            remainder = subtract_decimal_digits(&remainder, denominator)?;
        }
    }
    Ok(remainder == "0")
}

fn trim_decimal_digit_zeros(value: &str) -> &str {
    let trimmed = value.trim_start_matches('0');
    if trimmed.is_empty() {
        "0"
    } else {
        trimmed
    }
}

fn compare_decimal_digits(left: &str, right: &str) -> Ordering {
    let left = trim_decimal_digit_zeros(left);
    let right = trim_decimal_digit_zeros(right);
    match left.len().cmp(&right.len()) {
        Ordering::Equal => left.cmp(right),
        ordering => ordering,
    }
}

fn subtract_decimal_digits(left: &str, right: &str) -> Result<String, String> {
    if compare_decimal_digits(left, right) == Ordering::Less {
        return Err("left decimal digits must be greater than right".to_string());
    }
    let left = trim_decimal_digit_zeros(left).as_bytes();
    let right = trim_decimal_digit_zeros(right).as_bytes();
    let mut result = Vec::with_capacity(left.len());
    let mut borrow = 0i16;
    for index in 0..left.len() {
        let left_digit = i16::from(left[left.len() - 1 - index] - b'0') - borrow;
        let right_digit = if index < right.len() {
            i16::from(right[right.len() - 1 - index] - b'0')
        } else {
            0
        };
        let mut digit = left_digit - right_digit;
        if digit < 0 {
            digit += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        result.push(char::from(b'0' + u8::try_from(digit).unwrap_or(0)));
    }
    if borrow != 0 {
        return Err("decimal subtraction borrow overflow".to_string());
    }
    result.reverse();
    let value = result.into_iter().collect::<String>();
    Ok(trim_decimal_digit_zeros(&value).to_string())
}

fn normalize_decimal(raw: &str) -> Result<NormalizedDecimal, String> {
    let text = raw.trim();
    if text.is_empty() {
        return Err("decimal value cannot be empty".to_string());
    }
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut negative = false;
    if matches!(bytes.get(index), Some(b'+') | Some(b'-')) {
        negative = bytes[index] == b'-';
        index += 1;
    }

    let mut digits = String::new();
    let mut integer_digits: i64 = 0;
    let mut saw_digit = false;
    let mut saw_dot = false;
    while let Some(byte) = bytes.get(index) {
        match byte {
            b'0'..=b'9' => {
                saw_digit = true;
                digits.push(char::from(*byte));
                if !saw_dot {
                    integer_digits = integer_digits
                        .checked_add(1)
                        .ok_or_else(|| "decimal point overflow".to_string())?;
                }
                index += 1;
            }
            b'.' if !saw_dot => {
                saw_dot = true;
                index += 1;
            }
            _ => break,
        }
    }
    if !saw_digit {
        return Err(format!("invalid decimal value '{raw}'"));
    }

    let exponent = if matches!(bytes.get(index), Some(b'e') | Some(b'E')) {
        index += 1;
        parse_decimal_exponent(bytes, &mut index)?
    } else {
        0
    };
    if index != bytes.len() {
        return Err(format!("invalid decimal value '{raw}'"));
    }

    let mut point = integer_digits
        .checked_add(exponent)
        .ok_or_else(|| "decimal point overflow".to_string())?;
    let first_non_zero = digits
        .bytes()
        .position(|digit| digit != b'0')
        .unwrap_or(digits.len());
    if first_non_zero == digits.len() {
        return Ok(NormalizedDecimal {
            negative: false,
            point: 0,
            digits: "0".to_string(),
        });
    }
    point = point
        .checked_sub(
            i64::try_from(first_non_zero).map_err(|_| "decimal point overflow".to_string())?,
        )
        .ok_or_else(|| "decimal point overflow".to_string())?;
    let mut normalized_digits = digits[first_non_zero..].to_string();
    while normalized_digits.ends_with('0') {
        normalized_digits.pop();
    }
    Ok(NormalizedDecimal {
        negative,
        point,
        digits: normalized_digits,
    })
}

fn parse_decimal_exponent(bytes: &[u8], index: &mut usize) -> Result<i64, String> {
    let mut negative = false;
    if matches!(bytes.get(*index), Some(b'+') | Some(b'-')) {
        negative = bytes[*index] == b'-';
        *index += 1;
    }
    let mut value: i64 = 0;
    let mut saw_digit = false;
    while let Some(byte @ b'0'..=b'9') = bytes.get(*index) {
        saw_digit = true;
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(i64::from(*byte - b'0')))
            .ok_or_else(|| "decimal exponent overflow".to_string())?;
        *index += 1;
    }
    if !saw_digit {
        return Err("decimal exponent requires digits".to_string());
    }
    if negative {
        value = value
            .checked_neg()
            .ok_or_else(|| "decimal exponent overflow".to_string())?;
    }
    Ok(value)
}

fn compare_normalized_decimals(left: &NormalizedDecimal, right: &NormalizedDecimal) -> Ordering {
    if left.negative != right.negative {
        return if left.negative {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    if left.digits == "0" && right.digits == "0" {
        return Ordering::Equal;
    }
    let ordering = compare_decimal_magnitude(left, right);
    if left.negative {
        ordering.reverse()
    } else {
        ordering
    }
}

fn compare_decimal_magnitude(left: &NormalizedDecimal, right: &NormalizedDecimal) -> Ordering {
    match left.point.cmp(&right.point) {
        Ordering::Equal => {}
        ordering => return ordering,
    }
    let left_digits = left.digits.as_bytes();
    let right_digits = right.digits.as_bytes();
    let max_len = left_digits.len().max(right_digits.len());
    for index in 0..max_len {
        let left_digit = *left_digits.get(index).unwrap_or(&b'0');
        let right_digit = *right_digits.get(index).unwrap_or(&b'0');
        match left_digit.cmp(&right_digit) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    Ordering::Equal
}

fn decimal_point_key(point: i64) -> u64 {
    let shifted = i128::from(point) - i128::from(i64::MIN);
    u64::try_from(shifted).expect("shifted i64 decimal point fits in u64")
}

fn ordering_value(ordering: Ordering) -> i64 {
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

fn sqlite_function_error(message: String) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(io::Error::new(
        io::ErrorKind::InvalidInput,
        message,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_declared_numeric_values_decode_as_decimal() {
        assert_eq!(
            sqlite_value(ValueRef::Null, Some("NUMERIC(12, 2)")),
            DbValue::Null
        );
        assert_eq!(
            sqlite_value(ValueRef::Text(b"123.45"), Some("NUMERIC(12, 2)")),
            DbValue::Decimal("123.45".to_string())
        );
        assert_eq!(
            sqlite_value(ValueRef::Real(123.45), Some("decimal")),
            DbValue::Decimal("123.45".to_string())
        );
        assert_eq!(
            sqlite_value(ValueRef::Integer(123), Some("NUMBER")),
            DbValue::Decimal("123".to_string())
        );
        assert_eq!(
            sqlite_value(ValueRef::Text(b"123.45"), Some("TEXT")),
            DbValue::Text("123.45".to_string())
        );
        assert_eq!(sqlite_value(ValueRef::Real(3.5), None), DbValue::Real(3.5));
        assert_eq!(
            sqlite_value(ValueRef::Blob(b"blob-text"), None),
            DbValue::Text("blob-text".to_string())
        );
    }

    #[test]
    fn sqlite_declared_integer_text_values_decode_as_exact_integers() {
        assert_eq!(sqlite_value(ValueRef::Null, Some("INTEGER")), DbValue::Null);
        assert_eq!(
            sqlite_value(ValueRef::Integer(7), Some("INTEGER")),
            DbValue::Integer(7)
        );
        assert_eq!(
            sqlite_value(ValueRef::Real(7.5), Some("INTEGER")),
            DbValue::Real(7.5)
        );
        assert_eq!(
            sqlite_value(ValueRef::Text(b"18446744073709551615"), Some("INTEGER")),
            DbValue::UnsignedInteger(u64::MAX)
        );
        assert_eq!(
            sqlite_value(ValueRef::Blob(b"18446744073709551615"), Some("INTEGER")),
            DbValue::UnsignedInteger(u64::MAX)
        );
        assert_eq!(
            sqlite_value(ValueRef::Text(b"-9223372036854775808"), Some("BIGINT")),
            DbValue::Integer(i64::MIN)
        );
        assert_eq!(
            sqlite_value(ValueRef::Text(b"not-an-int"), Some("INTEGER")),
            DbValue::Text("not-an-int".to_string())
        );
        assert_eq!(
            sqlite_value(ValueRef::Text(b"18446744073709551615"), Some("TEXT")),
            DbValue::Text("18446744073709551615".to_string())
        );
    }

    #[test]
    fn sqlite_decimal_comparison_handles_precision_and_exponents() {
        assert_eq!(decimal_cmp("-1", "1").unwrap(), Ordering::Less);
        assert_eq!(decimal_cmp("0", "-0.0").unwrap(), Ordering::Equal);
        assert_eq!(decimal_cmp("10", "2").unwrap(), Ordering::Greater);
        assert_eq!(
            decimal_cmp(
                "12345678901234567890.123456789",
                "12345678901234567890.123456788"
            )
            .unwrap(),
            Ordering::Greater
        );
        assert_eq!(decimal_cmp("1.20", "1.2").unwrap(), Ordering::Equal);
        assert_eq!(decimal_cmp("1E+3", "999.99").unwrap(), Ordering::Greater);
        assert_eq!(decimal_cmp("-0.01", "-0.001").unwrap(), Ordering::Less);
        assert!(normalize_decimal("").is_err());
        assert!(normalize_decimal("abc").is_err());
        assert!(normalize_decimal("1e").is_err());
        assert!(normalize_decimal("1x").is_err());
    }

    #[test]
    fn sqlite_decimal_sort_keys_preserve_numeric_order() {
        let mut values = vec![
            "0",
            "10",
            "-0.001",
            "2",
            "-10",
            "1.201",
            "1.2",
            "12345678901234567890.123456789",
        ];
        values.sort_by_key(|value| decimal_sort_key(value).unwrap());
        assert_eq!(
            values,
            vec![
                "-10",
                "-0.001",
                "0",
                "1.2",
                "1.201",
                "2",
                "10",
                "12345678901234567890.123456789",
            ]
        );
    }

    #[test]
    fn sqlite_decimal_multiple_of_preserves_precision() {
        assert!(decimal_multiple_of("10", "2").unwrap());
        assert!(decimal_multiple_of("1.20", "0.05").unwrap());
        assert!(decimal_multiple_of("12345678901234567890.12345", "0.00001").unwrap());
        assert!(!decimal_multiple_of("1.21", "0.05").unwrap());
        assert!(!decimal_multiple_of("10", "3").unwrap());
        assert!(decimal_multiple_of("0", "0.05").unwrap());
        assert!(decimal_multiple_of("1", "0").is_err());
        assert!(!decimal_multiple_of("0.001", "0.01").unwrap());
        assert!(decimal_digits_divisible("12a", "3").is_err());
        assert!(decimal_digits_divisible("1", "0").is_err());
        assert!(subtract_decimal_digits("1", "2").is_err());
    }

    #[test]
    fn sqlite_regex_match_uses_rust_regex() {
        assert!(regex_match("AB", r"^[A-Z]{2}$").unwrap());
        assert!(!regex_match("ab", r"^[A-Z]{2}$").unwrap());
        assert!(regex_match("AB", "[").is_err());
    }

    #[test]
    fn sqlite_registered_scalar_functions_cover_nulls_scalars_and_errors() {
        let connection = Connection::open_in_memory().expect("sqlite memory should open");
        register_sqlite_functions(&connection).expect("sqlite functions should register");

        let cmp: Option<i64> = connection
            .query_row(
                "SELECT ormdantic_decimal_cmp(?, ?)",
                ["1.20", "1.2"],
                |row| row.get(0),
            )
            .expect("decimal cmp should run");
        assert_eq!(cmp, Some(0));
        let null_cmp: Option<i64> = connection
            .query_row("SELECT ormdantic_decimal_cmp(NULL, ?)", ["1.2"], |row| {
                row.get(0)
            })
            .expect("decimal cmp should return null for null operands");
        assert_eq!(null_cmp, None);
        let sort_key: Option<String> = connection
            .query_row("SELECT ormdantic_decimal_sort_key(?)", ["-2"], |row| {
                row.get(0)
            })
            .expect("decimal sort key should run");
        assert!(sort_key
            .expect("sort key should be present")
            .starts_with("0|"));
        let null_sort: Option<String> = connection
            .query_row("SELECT ormdantic_decimal_sort_key(NULL)", [], |row| {
                row.get(0)
            })
            .expect("decimal sort key should return null for null values");
        assert_eq!(null_sort, None);
        let regex: Option<bool> = connection
            .query_row(
                "SELECT ormdantic_regex_match(?, ?)",
                ["AB", "^[A-Z]+$"],
                |row| row.get(0),
            )
            .expect("regex match should run");
        assert_eq!(regex, Some(true));
        let null_regex: Option<bool> = connection
            .query_row(
                "SELECT ormdantic_regex_match(NULL, ?)",
                ["^[A-Z]+$"],
                |row| row.get(0),
            )
            .expect("regex match should return null for null operands");
        assert_eq!(null_regex, None);
        let multiple: Option<bool> = connection
            .query_row(
                "SELECT ormdantic_decimal_multiple_of(?, ?)",
                ["1.20", "0.05"],
                |row| row.get(0),
            )
            .expect("decimal multiple_of should run");
        assert_eq!(multiple, Some(true));
        let null_multiple: Option<bool> = connection
            .query_row(
                "SELECT ormdantic_decimal_multiple_of(NULL, ?)",
                ["0.05"],
                |row| row.get(0),
            )
            .expect("multiple_of should return null for null operands");
        assert_eq!(null_multiple, None);

        assert!(connection
            .query_row("SELECT ormdantic_regex_match(?, ?)", ["AB", "["], |row| {
                row.get::<_, Option<bool>>(0)
            })
            .is_err());
        assert!(connection
            .query_row("SELECT ormdantic_decimal_sort_key(?)", ["1e"], |row| {
                row.get::<_, Option<String>>(0)
            })
            .is_err());
    }
}
