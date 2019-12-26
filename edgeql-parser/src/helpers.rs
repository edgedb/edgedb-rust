use std::borrow::Cow;

use crate::tokenizer::is_keyword;


/// Converts the string into edgeql-compatible name (of a column or a property)
///
/// # Examples
/// ```
/// use edgeql_parser::helpers::quote_name;
/// assert_eq!(quote_name("col1"), "col1");
/// assert_eq!(quote_name("another name"), "`another name`");
/// assert_eq!(quote_name("with `quotes`"), "`with ``quotes```");
/// ```
pub fn quote_name(s: &str) -> Cow<str> {
    if s.chars().all(|c| c.is_alphanumeric() || c == '_') {
        let lower = s.to_ascii_lowercase();
        if !is_keyword(&lower) {
            return s.into();
        }
    }
    let escaped = s.replace('`', "``");
    let mut s = String::with_capacity(escaped.len()+2);
    s.push('`');
    s.push_str(&escaped);
    s.push('`');
    return s.into();
}
