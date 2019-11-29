use edgeql_parser::preparser::full_statement;

#[test]
fn test_simple() {
    assert_eq!(
        full_statement(b"select 1+1; some trailer"),
        Some(10));
}

#[test]
fn test_quotes() {
    assert_eq!(
        full_statement(b"select \"x\"; some trailer"),
        Some(10));
}

#[test]
fn test_quoted_semicolon() {
    assert_eq!(
        full_statement(b"select \"a;\"; some trailer"),
        Some(11));
}

#[test]
fn test_single_quoted_semicolon() {
    assert_eq!(
        full_statement(b"select \'a;\'; some trailer"),
        Some(11));
}

#[test]
fn test_commented_semicolon() {
    assert_eq!(
        full_statement(b"select # test;\n1+1;"),
        Some(18));
}
