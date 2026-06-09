pub(crate) fn quote_double(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

pub(crate) fn quote_backtick(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}
