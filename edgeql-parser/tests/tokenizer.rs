use edgeql_parser::tokenizer::{Kind, TokenStream};
use edgeql_parser::tokenizer::Kind::*;
use combine::easy::Error;

use combine::{StreamOnce, Positioned};

fn tok_str(s: &str) -> Vec<&str> {
    let mut r = Vec::new();
    let mut s = TokenStream::new(s);
    loop {
        match s.uncons() {
            Ok(x) => r.push(x.value),
            Err(ref e) if e == &Error::end_of_input() => break,
            Err(e) => panic!("Parse error at {}: {}", s.position(), e),
        }
    }
    return r;
}

fn tok_typ(s: &str) -> Vec<Kind> {
    let mut r = Vec::new();
    let mut s = TokenStream::new(s);
    loop {
        match s.uncons() {
            Ok(x) => r.push(x.kind),
            Err(ref e) if e == &Error::end_of_input() => break,
            Err(e) => panic!("Parse error at {}: {}", s.position(), e),
        }
    }
    return r;
}

fn tok_err(s: &str) -> String {
    let mut s = TokenStream::new(s);
    loop {
        match s.uncons() {
            Ok(_) => {}
            Err(ref e) if e == &Error::end_of_input() => break,
            Err(e) => return format!("{}", e),
        }
    }
    panic!("No error, where error expected");
}

#[test]
fn whitespace_and_comments() {
    assert_eq!(tok_str("# hello { world }"), &[] as &[&str]);
    assert_eq!(tok_str("# x\n  "), &[] as &[&str]);
    assert_eq!(tok_str("  # x"), &[] as &[&str]);
}

#[test]
fn idents() {
    assert_eq!(tok_str("a bc d127"), ["a", "bc", "d127"]);
    assert_eq!(tok_typ("a bc d127"), [Ident, Ident, Ident]);
    assert_eq!(tok_str("тест тест_abc abc_тест"),
                       ["тест", "тест_abc", "abc_тест"]);
    assert_eq!(tok_typ("тест тест_abc abc_тест"), [Ident, Ident, Ident]);
}

#[test]
fn keywords() {
    assert_eq!(tok_str("SELECT a"), ["SELECT", "a"]);
    assert_eq!(tok_typ("SELECT a"), [Keyword, Ident]);
    assert_eq!(tok_str("with Select"), ["with", "Select"]);
    assert_eq!(tok_typ("with Select"), [Keyword, Keyword]);
}

#[test]
fn colon_tokens() {
    assert_eq!(tok_str("a :=b"), ["a", ":=", "b"]);
    assert_eq!(tok_typ("a :=b"), [Ident, Assign, Ident]);
    assert_eq!(tok_str("a : = b"), ["a", ":", "=", "b"]);
    assert_eq!(tok_typ("a : = b"), [Ident, Colon, Eq, Ident]);
    assert_eq!(tok_str("a ::= b"), ["a", "::", "=", "b"]);
    assert_eq!(tok_typ("a ::= b"), [Ident, Namespace, Eq, Ident]);
}

#[test]
fn dash_tokens() {
    assert_eq!(tok_str("a-b -> c"), ["a", "-", "b", "->", "c"]);
    assert_eq!(tok_typ("a-b -> c"), [Ident, Sub, Ident, Arrow, Ident]);
    assert_eq!(tok_str("a - > b"), ["a", "-", ">", "b"]);
    assert_eq!(tok_typ("a - > b"), [Ident, Sub, Greater, Ident]);
    assert_eq!(tok_str("a --> b"), ["a", "-", "->", "b"]);
    assert_eq!(tok_typ("a --> b"), [Ident, Sub, Arrow, Ident]);
}

#[test]
fn greater_tokens() {
    assert_eq!(tok_str("a >= c"), ["a", ">=", "c"]);
    assert_eq!(tok_typ("a >= c"), [Ident, GreaterEq, Ident]);
    assert_eq!(tok_str("a > = b"), ["a", ">", "=", "b"]);
    assert_eq!(tok_typ("a > = b"), [Ident, Greater, Eq, Ident]);
    assert_eq!(tok_str("a>b"), ["a", ">", "b"]);
    assert_eq!(tok_typ("a>b"), [Ident, Greater, Ident]);
}

#[test]
fn less_tokens() {
    assert_eq!(tok_str("a <= c"), ["a", "<=", "c"]);
    assert_eq!(tok_typ("a <= c"), [Ident, LessEq, Ident]);
    assert_eq!(tok_str("a < = b"), ["a", "<", "=", "b"]);
    assert_eq!(tok_typ("a < = b"), [Ident, Less, Eq, Ident]);
    assert_eq!(tok_str("a<b"), ["a", "<", "b"]);
    assert_eq!(tok_typ("a<b"), [Ident, Less, Ident]);
}

#[test]
fn plus_tokens() {
    assert_eq!(tok_str("a+b += c"), ["a", "+", "b", "+=", "c"]);
    assert_eq!(tok_typ("a+b += c"), [Ident, Add, Ident, AddAssign, Ident]);
    assert_eq!(tok_str("a + = b"), ["a", "+", "=", "b"]);
    assert_eq!(tok_typ("a + = b"), [Ident, Add, Eq, Ident]);
    assert_eq!(tok_str("a ++= b"), ["a", "++", "=", "b"]);
    assert_eq!(tok_typ("a ++= b"), [Ident, Concat, Eq, Ident]);
}

#[test]
fn not_equals_tokens() {
    assert_eq!(tok_str("a != c"), ["a", "!=", "c"]);
    assert_eq!(tok_typ("a != c"), [Ident, NotEq, Ident]);
    assert_eq!(tok_str("a!=b"), ["a", "!=", "b"]);
    assert_eq!(tok_typ("a!=b"), [Ident, NotEq, Ident]);
    assert_eq!(tok_err("a ! = b"),
        "Unexpected `1:3: Bare `!` is not an operator, \
         did you mean `!=`?`");
}

#[test]
fn question_tokens() {
    assert_eq!(tok_str("a??b ?= c"), ["a", "??", "b", "?=", "c"]);
    assert_eq!(tok_typ("a??b ?= c"),
               [Ident, Coalesce, Ident, NotDistinctFrom, Ident]);
    assert_eq!(tok_str("a ?!= b"), ["a", "?!=", "b"]);
    assert_eq!(tok_typ("a ?!= b"), [Ident, DistinctFrom, Ident]);
    assert_eq!(tok_err("a ? b"),
        "Unexpected `1:3: Bare `?` is not an operator, \
         did you mean `?=` or `??` ?`");

    assert_eq!(tok_err("something ?!"),
        "Unexpected `1:11: `?!` is not an operator, \
         did you mean `?!=` ?`");
}

#[test]
fn dot_tokens() {
    assert_eq!(tok_str("a.b .> c"), ["a", ".", "b", ".>", "c"]);
    assert_eq!(tok_typ("a.b .> c"), [Ident, Dot, Ident, ForwardLink, Ident]);
    assert_eq!(tok_str("a . > b"), ["a", ".", ">", "b"]);
    assert_eq!(tok_typ("a . > b"), [Ident, Dot, Greater, Ident]);
    assert_eq!(tok_str("a .>> b"), ["a", ".>", ">", "b"]);
    assert_eq!(tok_typ("a .>> b"), [Ident, ForwardLink, Greater, Ident]);
    assert_eq!(tok_str("a ..> b"), ["a", ".", ".>", "b"]);
    assert_eq!(tok_typ("a ..> b"), [Ident, Dot, ForwardLink, Ident]);

    assert_eq!(tok_str("a.b .< c"), ["a", ".", "b", ".<", "c"]);
    assert_eq!(tok_typ("a.b .< c"), [Ident, Dot, Ident, BackwardLink, Ident]);
    assert_eq!(tok_str("a . < b"), ["a", ".", "<", "b"]);
    assert_eq!(tok_typ("a . < b"), [Ident, Dot, Less, Ident]);
    assert_eq!(tok_str("a .<< b"), ["a", ".<", "<", "b"]);
    assert_eq!(tok_typ("a .<< b"), [Ident, BackwardLink, Less, Ident]);
    assert_eq!(tok_str("a ..< b"), ["a", ".", ".<", "b"]);
    assert_eq!(tok_typ("a ..< b"), [Ident, Dot, BackwardLink, Ident]);
}

#[test]
fn div_tokens() {
    assert_eq!(tok_str("a // c"), ["a", "//", "c"]);
    assert_eq!(tok_typ("a // c"), [Ident, FloorDiv, Ident]);
    assert_eq!(tok_str("a / / b"), ["a", "/", "/", "b"]);
    assert_eq!(tok_typ("a / / b"), [Ident, Div, Div, Ident]);
    assert_eq!(tok_str("a/b"), ["a", "/", "b"]);
    assert_eq!(tok_typ("a/b"), [Ident, Div, Ident]);
}

#[test]
fn single_char_tokens() {
    assert_eq!(tok_str(".;:+-*"), [".", ";", ":", "+", "-", "*"]);
    assert_eq!(tok_typ(".;:+-*"), [Dot, Semicolon, Colon, Add, Sub, Mul]);
    assert_eq!(tok_str("/%^<>"), ["/", "%", "^", "<", ">"]);
    assert_eq!(tok_typ("/%^<>"), [Div, Modulo, Pow, Less, Greater]);
    assert_eq!(tok_str("=&|"), ["=", "&", "|"]);
    assert_eq!(tok_typ("=&|"), [Eq, Ampersand, Pipe]);

    assert_eq!(tok_str(". ; : + - *"), [".", ";", ":", "+", "-", "*"]);
    assert_eq!(tok_typ(". ; : + - *"), [Dot, Semicolon, Colon, Add, Sub, Mul]);
    assert_eq!(tok_str("/ % ^ < >"), ["/", "%", "^", "<", ">"]);
    assert_eq!(tok_typ("/ % ^ < >"), [Div, Modulo, Pow, Less, Greater]);
    assert_eq!(tok_str("= & |"), ["=", "&", "|"]);
    assert_eq!(tok_typ("= & |"), [Eq, Ampersand, Pipe]);
}
