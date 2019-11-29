use edgeql_parser::preparser::full_statement;

#[test]
fn test_simple() {
    assert_eq!(
        full_statement(b"select 1+1; some trailer"),
        Ok(10));
}

#[test]
fn test_quotes() {
    assert_eq!(
        full_statement(b"select \"x\"; some trailer"),
        Ok(10));
}

#[test]
fn test_quoted_semicolon() {
    assert_eq!(
        full_statement(b"select \"a;\"; some trailer"),
        Ok(11));
}

#[test]
fn test_single_quoted_semicolon() {
    assert_eq!(
        full_statement(b"select 'a;'; some trailer"),
        Ok(11));
}

#[test]
fn test_backtick_quoted_semicolon() {
    assert_eq!(
        full_statement(b"select `a;`; some trailer"),
        Ok(11));
}

#[test]
fn test_commented_semicolon() {
    assert_eq!(
        full_statement(b"select # test;\n1+1;"),
        Ok(18));
}

#[test]
fn test_continuation() {
    assert_eq!(
        full_statement(b"select 'a;'"),
        Err(11));
}

#[test]
fn test_quoted_continuation() {
    assert_eq!(
        full_statement(b"select \"a;"),
        Err(7));
}

#[test]
fn test_single_quoted_continuation() {
    assert_eq!(
        full_statement(b"select 'a;"),
        Err(7));
}

#[test]
fn test_backtick_quoted_continuation() {
    assert_eq!(
        full_statement(b"select `a;test"),
        Err(7));
}

#[test]
fn test_dollar_semicolon() {
    assert_eq!(
        full_statement(b"select $$ ; $$ test;"),
        Ok(19));
    assert_eq!(
        full_statement(b"select $$$$;"),
        Ok(b"select $$$$".len()));
    assert_eq!(
        full_statement(b"select $$$ ; $$;"),
        Ok(b"select $$$ ; $$".len()));
    assert_eq!(
        full_statement(b"select $some-long/name$ ; $some-long/name$;"),
        Ok(b"select $some-long/name$ ; $some-long/name$".len()));
}

#[test]
fn test_nested_dollar() {
    assert_eq!(
        full_statement(b"select $a$ ; $b$ ; $b$ ; $a$; x"),
        Ok(b"select $a$ ; $b$ ; $b$ ; $a$".len()));
    assert_eq!(
        full_statement(b"select $a$ ; $b$ ; $a$; x"),
        Ok(b"select $a$ ; $b$ ; $a$".len()));
}

#[test]
fn test_dollar_continuation() {
    assert_eq!(
        full_statement(b"select $a$ ; $$ test;"),
        Err(7));
    assert_eq!(
        full_statement(b"select $a$ ; test;"),
        Err(7));
    assert_eq!(
        full_statement(b"select $a$a$ ; $$ test;"),
        Err(7));
    assert_eq!(
        full_statement(b"select $a$ ; $b$ ; $c$ ; $b$ test;"),
        Err(7));
}
