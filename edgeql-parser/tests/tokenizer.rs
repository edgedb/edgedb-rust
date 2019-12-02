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

#[test]
fn integer() {
    assert_eq!(tok_str("0"), ["0"]);
    assert_eq!(tok_typ("0"), [IntConst]);
    assert_eq!(tok_str("*0"), ["*", "0"]);
    assert_eq!(tok_typ("*0"), [Mul, IntConst]);
    assert_eq!(tok_str("+0"), ["+0"]);
    assert_eq!(tok_typ("+0"), [IntConst]);
    assert_eq!(tok_str("-0"), ["-0"]);
    assert_eq!(tok_typ("-0"), [IntConst]);
    assert_eq!(tok_str("123"), ["123"]);
    assert_eq!(tok_typ("123"), [IntConst]);
    assert_eq!(tok_str("-123"), ["-123"]);
    assert_eq!(tok_typ("-123"), [IntConst]);
    assert_eq!(tok_str("+123"), ["+123"]);
    assert_eq!(tok_typ("+123"), [IntConst]);

    assert_eq!(tok_str("0 "), ["0"]);
    assert_eq!(tok_typ("0 "), [IntConst]);
    assert_eq!(tok_str("+0 "), ["+0"]);
    assert_eq!(tok_typ("+0 "), [IntConst]);
    assert_eq!(tok_str("-0 "), ["-0"]);
    assert_eq!(tok_typ("-0 "), [IntConst]);
    assert_eq!(tok_str("123 "), ["123"]);
    assert_eq!(tok_typ("123 "), [IntConst]);
    assert_eq!(tok_str("-123 "), ["-123"]);
    assert_eq!(tok_typ("-123 "), [IntConst]);
    assert_eq!(tok_str("+123 "), ["+123"]);
    assert_eq!(tok_typ("+123 "), [IntConst]);
}

#[test]
fn bigint() {
    assert_eq!(tok_str("0n"), ["0n"]);
    assert_eq!(tok_typ("0n"), [BigIntConst]);
    assert_eq!(tok_str("*0n"), ["*", "0n"]);
    assert_eq!(tok_typ("*0n"), [Mul, BigIntConst]);
    assert_eq!(tok_str("+0n"), ["+0n"]);
    assert_eq!(tok_typ("+0n"), [BigIntConst]);
    assert_eq!(tok_str("-0n"), ["-0n"]);
    assert_eq!(tok_typ("-0n"), [BigIntConst]);
    assert_eq!(tok_str("123n"), ["123n"]);
    assert_eq!(tok_typ("123n"), [BigIntConst]);
    assert_eq!(tok_str("-123n"), ["-123n"]);
    assert_eq!(tok_typ("-123n"), [BigIntConst]);
    assert_eq!(tok_str("+123n"), ["+123n"]);
    assert_eq!(tok_typ("+123n"), [BigIntConst]);

    assert_eq!(tok_str("0n "), ["0n"]);
    assert_eq!(tok_typ("0n "), [BigIntConst]);
    assert_eq!(tok_str("+0n "), ["+0n"]);
    assert_eq!(tok_typ("+0n "), [BigIntConst]);
    assert_eq!(tok_str("-0n "), ["-0n"]);
    assert_eq!(tok_typ("-0n "), [BigIntConst]);
    assert_eq!(tok_str("123n "), ["123n"]);
    assert_eq!(tok_typ("123n "), [BigIntConst]);
    assert_eq!(tok_str("-123n "), ["-123n"]);
    assert_eq!(tok_typ("-123n "), [BigIntConst]);
    assert_eq!(tok_str("+123n "), ["+123n"]);
    assert_eq!(tok_typ("+123n "), [BigIntConst]);
}

#[test]
fn float() {
    assert_eq!(tok_str("0."), ["0."]);
    assert_eq!(tok_typ("0."), [FloatConst]);
    assert_eq!(tok_str("     0.0"), ["0.0"]);
    assert_eq!(tok_typ("     0.0"), [FloatConst]);
    assert_eq!(tok_str("+0.123"), ["+0.123"]);
    assert_eq!(tok_typ("+0.123"), [FloatConst]);
    assert_eq!(tok_str("-0.234"), ["-0.234"]);
    assert_eq!(tok_typ("-0.234"), [FloatConst]);
    assert_eq!(tok_str("123.999"), ["123.999"]);
    assert_eq!(tok_typ("123.999"), [FloatConst]);
    assert_eq!(tok_str("-123.000"), ["-123.000"]);
    assert_eq!(tok_typ("-123.000"), [FloatConst]);
    assert_eq!(tok_str("+123.30"), ["+123.30"]);
    assert_eq!(tok_typ("+123.30"), [FloatConst]);
    assert_eq!(tok_str("123.999e3"), ["123.999e3"]);
    assert_eq!(tok_typ("123.999e3"), [FloatConst]);
    assert_eq!(tok_str("123.999e+99"), ["123.999e+99"]);
    assert_eq!(tok_typ("123.999e+99"), [FloatConst]);
    assert_eq!(tok_str("2345.567e-7"), ["2345.567e-7"]);
    assert_eq!(tok_typ("2345.567e-7"), [FloatConst]);
    assert_eq!(tok_str("123e3"), ["123e3"]);
    assert_eq!(tok_typ("123e3"), [FloatConst]);
    assert_eq!(tok_str("123e+99"), ["123e+99"]);
    assert_eq!(tok_typ("123e+99"), [FloatConst]);
    assert_eq!(tok_str("2345e-7"), ["2345e-7"]);
    assert_eq!(tok_typ("2345e-7"), [FloatConst]);

    assert_eq!(tok_str("0. "), ["0."]);
    assert_eq!(tok_typ("0. "), [FloatConst]);
    assert_eq!(tok_str("     0.0 "), ["0.0"]);
    assert_eq!(tok_typ("     0.0 "), [FloatConst]);
    assert_eq!(tok_str("+0.123 "), ["+0.123"]);
    assert_eq!(tok_typ("+0.123 "), [FloatConst]);
    assert_eq!(tok_str("-0.234 "), ["-0.234"]);
    assert_eq!(tok_typ("-0.234 "), [FloatConst]);
    assert_eq!(tok_str("123.999 "), ["123.999"]);
    assert_eq!(tok_typ("123.999 "), [FloatConst]);
    assert_eq!(tok_str("-123.000 "), ["-123.000"]);
    assert_eq!(tok_typ("-123.000 "), [FloatConst]);
    assert_eq!(tok_str("+123.30 "), ["+123.30"]);
    assert_eq!(tok_typ("+123.30 "), [FloatConst]);
    assert_eq!(tok_str("123.999e3 "), ["123.999e3"]);
    assert_eq!(tok_typ("123.999e3 "), [FloatConst]);
    assert_eq!(tok_str("123.999e+99 "), ["123.999e+99"]);
    assert_eq!(tok_typ("123.999e+99 "), [FloatConst]);
    assert_eq!(tok_str("2345.567e-7 "), ["2345.567e-7"]);
    assert_eq!(tok_typ("2345.567e-7 "), [FloatConst]);
    assert_eq!(tok_str("123e3 "), ["123e3"]);
    assert_eq!(tok_typ("123e3 "), [FloatConst]);
    assert_eq!(tok_str("123e+99 "), ["123e+99"]);
    assert_eq!(tok_typ("123e+99 "), [FloatConst]);
    assert_eq!(tok_str("2345e-7 "), ["2345e-7"]);
    assert_eq!(tok_typ("2345e-7 "), [FloatConst]);
}

#[test]
fn decimal() {
    assert_eq!(tok_str("0.n"), ["0.n"]);
    assert_eq!(tok_typ("0.n"), [DecimalConst]);
    assert_eq!(tok_str("     0.0n"), ["0.0n"]);
    assert_eq!(tok_typ("     0.0n"), [DecimalConst]);
    assert_eq!(tok_str("+0.123n"), ["+0.123n"]);
    assert_eq!(tok_typ("+0.123n"), [DecimalConst]);
    assert_eq!(tok_str("-0.234n"), ["-0.234n"]);
    assert_eq!(tok_typ("-0.234n"), [DecimalConst]);
    assert_eq!(tok_str("123.999n"), ["123.999n"]);
    assert_eq!(tok_typ("123.999n"), [DecimalConst]);
    assert_eq!(tok_str("-123.000n"), ["-123.000n"]);
    assert_eq!(tok_typ("-123.000n"), [DecimalConst]);
    assert_eq!(tok_str("+123.30n"), ["+123.30n"]);
    assert_eq!(tok_typ("+123.30n"), [DecimalConst]);
    assert_eq!(tok_str("123.999e3n"), ["123.999e3n"]);
    assert_eq!(tok_typ("123.999e3n"), [DecimalConst]);
    assert_eq!(tok_str("123.999e+99n"), ["123.999e+99n"]);
    assert_eq!(tok_typ("123.999e+99n"), [DecimalConst]);
    assert_eq!(tok_str("2345.567e-7n"), ["2345.567e-7n"]);
    assert_eq!(tok_typ("2345.567e-7n"), [DecimalConst]);
    assert_eq!(tok_str("123e3n"), ["123e3n"]);
    assert_eq!(tok_typ("123e3n"), [DecimalConst]);
    assert_eq!(tok_str("123e+99n"), ["123e+99n"]);
    assert_eq!(tok_typ("123e+99n"), [DecimalConst]);
    assert_eq!(tok_str("2345e-7n"), ["2345e-7n"]);
    assert_eq!(tok_typ("2345e-7n"), [DecimalConst]);

    assert_eq!(tok_str("0.n "), ["0.n"]);
    assert_eq!(tok_typ("0.n "), [DecimalConst]);
    assert_eq!(tok_str("     0.0n "), ["0.0n"]);
    assert_eq!(tok_typ("     0.0n "), [DecimalConst]);
    assert_eq!(tok_str("+0.123n "), ["+0.123n"]);
    assert_eq!(tok_typ("+0.123n "), [DecimalConst]);
    assert_eq!(tok_str("-0.234n "), ["-0.234n"]);
    assert_eq!(tok_typ("-0.234n "), [DecimalConst]);
    assert_eq!(tok_str("123.999n "), ["123.999n"]);
    assert_eq!(tok_typ("123.999n "), [DecimalConst]);
    assert_eq!(tok_str("-123.000n "), ["-123.000n"]);
    assert_eq!(tok_typ("-123.000n "), [DecimalConst]);
    assert_eq!(tok_str("+123.30n "), ["+123.30n"]);
    assert_eq!(tok_typ("+123.30n "), [DecimalConst]);
    assert_eq!(tok_str("123.999e3n "), ["123.999e3n"]);
    assert_eq!(tok_typ("123.999e3n "), [DecimalConst]);
    assert_eq!(tok_str("123.999e+99n "), ["123.999e+99n"]);
    assert_eq!(tok_typ("123.999e+99n "), [DecimalConst]);
    assert_eq!(tok_str("2345.567e-7n "), ["2345.567e-7n"]);
    assert_eq!(tok_typ("2345.567e-7n "), [DecimalConst]);
    assert_eq!(tok_str("123e3n "), ["123e3n"]);
    assert_eq!(tok_typ("123e3n "), [DecimalConst]);
    assert_eq!(tok_str("123e+99n "), ["123e+99n"]);
    assert_eq!(tok_typ("123e+99n "), [DecimalConst]);
    assert_eq!(tok_str("2345e-7n "), ["2345e-7n"]);
    assert_eq!(tok_typ("2345e-7n "), [DecimalConst]);
}

#[test]
fn numbers_from_py() {
    assert_eq!(tok_str("SELECT 3.5432;"), ["SELECT", "3.5432", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432;"), [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT +3.5432;"), ["SELECT", "+3.5432", ";"]);
    assert_eq!(tok_typ("SELECT +3.5432;"), [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT -3.5432;"), ["SELECT", "-3.5432", ";"]);
    assert_eq!(tok_typ("SELECT -3.5432;"), [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 354.32;"), ["SELECT", "354.32", ";"]);
    assert_eq!(tok_typ("SELECT 354.32;"), [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 35400000000000.32;"),
        ["SELECT", "35400000000000.32", ";"]);
    assert_eq!(tok_typ("SELECT 35400000000000.32;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 35400000000000000000.32;"),
        ["SELECT", "35400000000000000000.32", ";"]);
    assert_eq!(tok_typ("SELECT 35400000000000000000.32;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e20;"),
        ["SELECT", "3.5432e20", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e20;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e+20;"),
        ["SELECT", "3.5432e+20", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e+20;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e-20;"),
        ["SELECT", "3.5432e-20", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e-20;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT 354.32e-20;"),
        ["SELECT", "354.32e-20", ";"]);
    assert_eq!(tok_typ("SELECT 354.32e-20;"),
        [Keyword, FloatConst, Semicolon]);
    assert_eq!(tok_str("SELECT -0n;"),
        ["SELECT", "-0n", ";"]);
    assert_eq!(tok_typ("SELECT -0n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT 0n;"),
        ["SELECT", "0n", ";"]);
    assert_eq!(tok_typ("SELECT 0n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT 1n;"),
        ["SELECT", "1n", ";"]);
    assert_eq!(tok_typ("SELECT 1n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT -1n;"),
        ["SELECT", "-1n", ";"]);
    assert_eq!(tok_typ("SELECT -1n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT 100000n;"),
        ["SELECT", "100000n", ";"]);
    assert_eq!(tok_typ("SELECT 100000n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT -100000n;"),
        ["SELECT", "-100000n", ";"]);
    assert_eq!(tok_typ("SELECT -100000n;"),
        [Keyword, BigIntConst, Semicolon]);
    assert_eq!(tok_str("SELECT -354.32n;"),
        ["SELECT", "-354.32n", ";"]);
    assert_eq!(tok_typ("SELECT -354.32n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT 35400000000000.32n;"),
        ["SELECT", "35400000000000.32n", ";"]);
    assert_eq!(tok_typ("SELECT 35400000000000.32n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT -35400000000000000000.32n;"),
        ["SELECT", "-35400000000000000000.32n", ";"]);
    assert_eq!(tok_typ("SELECT -35400000000000000000.32n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e20n;"),
        ["SELECT", "3.5432e20n", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e20n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT -3.5432e+20n;"),
        ["SELECT", "-3.5432e+20n", ";"]);
    assert_eq!(tok_typ("SELECT -3.5432e+20n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT 3.5432e-20n;"),
        ["SELECT", "3.5432e-20n", ";"]);
    assert_eq!(tok_typ("SELECT 3.5432e-20n;"),
        [Keyword, DecimalConst, Semicolon]);
    assert_eq!(tok_str("SELECT 354.32e-20n;"),
        ["SELECT", "354.32e-20n", ";"]);
    assert_eq!(tok_typ("SELECT 354.32e-20n;"),
        [Keyword, DecimalConst, Semicolon]);
}

#[test]
fn num_errors() {
    assert_eq!(tok_err("1.0.x"),
        "Unexpected `1:1: extra decimal dot in number`");
    assert_eq!(tok_err("1.0e1."),
        "Unexpected `1:1: extra decimal dot in number`");
    assert_eq!(tok_err("1.0e."),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e"),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0ex"),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0en"),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e "),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e+"),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e+ "),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e+x"),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1.0e+n"),
        "Unexpected `1:1: optional `+` or `-` \
        followed by digits must follow `e` in float const`");
    assert_eq!(tok_err("1234numeric"),
        "Unexpected `1:1: suffix \"numeric\" \
        is invalid for numbers, perhaps you wanted `1234n` (bigint)?`");
    assert_eq!(tok_err("1234some_l0ng_trash"),
        "Unexpected `1:1: suffix \"some_l0n...\" \
        is invalid for numbers, perhaps you wanted `1234n` (bigint)?`");
    assert_eq!(tok_err("100O00"),
        "Unexpected `1:1: suffix \"O00\" is invalid for numbers, \
        perhaps mixed up letter `O` with zero `0`?`");
}
