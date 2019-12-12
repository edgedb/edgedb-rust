use std::char;
use std::collections::HashMap;
use std::iter::Peekable;
use std::slice::Iter;
use std::str::FromStr;

use cpython::{PyString, PyBytes, PyResult, Python, PyClone, PythonObject};
use cpython::{PyTuple, PyList, PyInt, PyObject, ToPyObject, ObjectProtocol};

use edgeql_parser::tokenizer::{TokenStream, Kind, is_keyword, SpannedToken};
use edgeql_parser::tokenizer::{MAX_KEYWORD_LENGTH, Token as RsToken};
use edgeql_parser::position::Pos;

static mut TOKENS: Option<Tokens> = None;

const UNRESERVED_KEYWORDS: [&str; 75] = [
    "abstract",
    "after",
    "alias",
    "allow",
    "all",
    "annotation",
    "as",
    "asc",
    "assignment",
    "before",
    "by",
    "cardinality",
    "cast",
    "config",
    "constraint",
    "database",
    "ddl",
    "default",
    "deferrable",
    "deferred",
    "delegated",
    "desc",
    "emit",
    "explicit",
    "expression",
    "final",
    "first",
    "from",
    "implicit",
    "index",
    "infix",
    "inheritable",
    "into",
    "isolation",
    "last",
    "link",
    "migration",
    "multi",
    "named",
    "object",
    "of",
    "oids",
    "on",
    "only",
    "operator",
    "overloaded",
    "postfix",
    "prefix",
    "property",
    "read",
    "rename",
    "required",
    "repeatable",
    "restrict",
    "role",
    "savepoint",
    "scalar",
    "schema",
    "sdl",
    "serializable",
    "session",
    "single",
    "source",
    "system",
    "target",
    "ternary",
    "text",
    "then",
    "to",
    "transaction",
    "type",
    "using",
    "verbose",
    "view",
    "write",
];


const FUTURE_RESERVED_KEYWORDS: [&str; 32] = [
    "analyze",
    "anyarray",
    "begin",
    "case",
    "check",
    "deallocate",
    "discard",
    "do",
    "end",
    "execute",
    "explain",
    "fetch",
    "get",
    "global",
    "grant",
    "import",
    "listen",
    "load",
    "lock",
    "match",
    "move",
    "notify",
    "prepare",
    "partition",
    "policy",
    "raise",
    "refresh",
    "reindex",
    "revoke",
    "over",
    "when",
    "window",
];

const CURRENT_RESERVED_KEYWORDS: [&str; 51] = [
    "__source__",
    "__subject__",
    "__type__",
    "alter",
    "and",
    "anytuple",
    "anytype",
    "commit",
    "configure",
    "create",
    "declare",
    "delete",
    "describe",
    "detached",
    "distinct",
    "drop",
    "else",
    "empty",
    "exists",
    "extending",
    "false",
    "filter",
    "for",
    "function",
    "group",
    "if",
    "ilike",
    "in",
    "insert",
    "introspect",
    "is",
    "like",
    "limit",
    "module",
    "not",
    "offset",
    "optional",
    "or",
    "order",
    "release",
    "reset",
    "rollback",
    "select",
    "set",
    "start",
    "true",
    "typeof",
    "update",
    "union",
    "variadic",
    "with",
];

py_class!(pub class Token |py| {
    data _kind: PyString;
    data _text: PyString;
    data _value: PyObject;
    data _start: Pos;
    data _end: Pos;
    def kind(&self) -> PyResult<PyString> {
        Ok(self._kind(py).clone_ref(py))
    }
    def text(&self) -> PyResult<PyString> {
        Ok(self._text(py).clone_ref(py))
    }
    def value(&self) -> PyResult<PyObject> {
        Ok(self._value(py).clone_ref(py))
    }
    def start(&self) -> PyResult<PyTuple> {
        let pos = self._start(py);
        Ok((pos.line, pos.column, pos.offset).to_py_object(py))
    }
    def end(&self) -> PyResult<PyTuple> {
        let pos = self._end(py);
        Ok((pos.line, pos.column, pos.offset).to_py_object(py))
    }
    def __repr__(&self) -> PyResult<PyString> {
        let val = self._value(py);
        let s = if *val == py.None() {
            format!("<Token {}>", self._kind(py).to_string(py)?)
        } else {
            format!("<Token {} {}>",
                self._kind(py).to_string(py)?,
                val.repr(py)?.to_string(py)?)
        };
        Ok(PyString::new(py, &s))
    }
});

py_exception!(_edgeql_rust, TokenizerError);

pub struct Tokens {
    pub ident: PyString,
    pub eof: PyString,
    pub empty: PyString,

    pub named_only: PyString,
    pub named_only_val: PyString,
    pub set_annotation: PyString,
    pub set_annotation_val: PyString,
    pub set_type: PyString,
    pub set_type_val: PyString,

    pub tokens: HashMap<Kind, TokenInfo>,
    pub keywords: HashMap<String, TokenInfo>,
}

struct ImportCache {
    decimal: Option<PyObject>,
}

pub struct TokenInfo {
    pub kind: Kind,
    pub name: PyString,
    pub value: Option<PyString>,
}

pub fn init_module(py: Python) {
    unsafe {
        TOKENS = Some(Tokens::new(py))
    }
}

fn peek_keyword(iter: &mut Peekable<Iter<SpannedToken>>, kw: &str) -> bool {
    iter.peek()
       .map(|t| t.token.kind == Kind::Ident &&
                t.token.value.eq_ignore_ascii_case(kw))
       .unwrap_or(false)
}

pub fn tokenize(py: Python, s: &PyString) -> PyResult<PyList> {
    let tokens = unsafe { TOKENS.as_ref().expect("module initialized") };
    let mut import_cache = ImportCache { decimal: None };
    let data = s.to_string(py)?;

    let mut token_stream = TokenStream::new(&data[..]);
    let rust_tokens: Vec<_> = py.allow_threads(|| -> Result<_, _> {
        token_stream.collect()
    }).map_err(|e| TokenizerError::new(py, e.to_string()))?;

    let mut buf = Vec::with_capacity(rust_tokens.len());
    let mut keyword_buf = String::with_capacity(MAX_KEYWORD_LENGTH);
    let mut tok_iter = rust_tokens.iter().peekable();
    while let Some(spanned_tok) = tok_iter.next() {
        let tok = spanned_tok.token;
        let (name, text) = match tok.kind {
            Kind::Keyword | Kind::Ident => {
                if tok.value.len() > MAX_KEYWORD_LENGTH {
                    (tokens.ident.clone_ref(py), PyString::new(py, tok.value))
                } else {
                    keyword_buf.clear();
                    keyword_buf.push_str(tok.value);
                    keyword_buf.make_ascii_lowercase();
                    match &keyword_buf[..] {
                        "named" if peek_keyword(&mut tok_iter, "only") => {
                            tok_iter.next();
                            (tokens.named_only.clone_ref(py),
                             tokens.named_only_val.clone_ref(py))
                        }
                        "set" if peek_keyword(&mut tok_iter, "annotation") => {
                            tok_iter.next();
                            (tokens.set_annotation.clone_ref(py),
                             tokens.set_annotation_val.clone_ref(py))
                        }
                        "set" if peek_keyword(&mut tok_iter, "type") => {
                            tok_iter.next();
                            (tokens.set_type.clone_ref(py),
                             tokens.set_type_val.clone_ref(py))
                        }
                        _ => match tokens.keywords.get(&keyword_buf) {
                            Some(tok_info) => {
                                debug_assert_eq!(tok_info.kind, tok.kind);
                                (tok_info.name.clone_ref(py),
                                 tok_info.value.as_ref().unwrap()
                                 .clone_ref(py))
                            }
                            None => {
                                (tokens.ident.clone_ref(py),
                                 PyString::new(py, tok.value))
                            }
                        },
                    }
                }
            }
            _ => {
                if let Some(tok_info) = tokens.tokens.get(&tok.kind) {
                    if let Some(ref value) = tok_info.value {
                        (tok_info.name.clone_ref(py), value.clone_ref(py))
                    } else {
                        (tok_info.name.clone_ref(py),
                         PyString::new(py, tok.value))
                    }
                } else {
                    unimplemented!("token {:?}", tok.kind);
                }
            }
        };
        let py_tok = Token::create_instance(py, name, text,
            py_value(py, &tokens, &mut import_cache, spanned_tok.token)?,
            spanned_tok.start, spanned_tok.end)?;

        buf.push(py_tok.into_object());
    }
    buf.push(Token::create_instance(py,
        tokens.eof.clone_ref(py),
        tokens.empty.clone_ref(py),
        py.None(),
        token_stream.current_pos(), token_stream.current_pos())?
        .into_object());
    Ok(PyList::new(py, &buf[..]))
}


impl Tokens {
    pub fn new(py: Python) -> Tokens {
        use Kind::*;
        let mut res = Tokens {
            ident: PyString::new(py, "IDENT"),
            eof: PyString::new(py, "EOF"),
            empty: PyString::new(py, ""),
            named_only: PyString::new(py, "NAMEDONLY"),
            named_only_val: PyString::new(py, "NAMED ONLY"),
            set_annotation: PyString::new(py, "SETANNOTATION"),
            set_annotation_val: PyString::new(py, "SET ANNOTATION"),
            set_type: PyString::new(py, "SETTYPE"),
            set_type_val: PyString::new(py, "SET TYPE"),
            tokens: HashMap::new(),
            keywords: HashMap::new(),
        };
        res.add_kind(py, BacktickName, "IDENT");
        res.add_static(py, Dot, ".", ".");
        res.add_static(py, ForwardLink, ".>", ".>");
        res.add_static(py, BackwardLink, ".>", ".>");
        res.add_static(py, OpenBracket, "[", "[");
        res.add_static(py, CloseBracket, "]", "]");
        res.add_static(py, OpenParen, "(", "(");
        res.add_static(py, CloseParen, ")", ")");
        res.add_static(py, OpenBrace, "{", "{");
        res.add_static(py, CloseBrace, "}", "}");
        res.add_static(py, Namespace, "::", "::");
        res.add_static(py, Coalesce, "::", "::");
        res.add_static(py, Colon, ":", ":");
        res.add_static(py, Semicolon, ";", ";");
        res.add_static(py, Comma, ",", ",");
        res.add_static(py, Add, "+", "+");
        res.add_static(py, Concat, "++", "++");
        res.add_static(py, Sub, "-", "-");
        res.add_static(py, Mul, "*", "*");
        res.add_static(py, Div, "/", "/");
        res.add_static(py, FloorDiv, "//", "//");
        res.add_static(py, Modulo, "%", "%");
        res.add_static(py, Pow, "^", "^");
        // res.add_static(py, At, "@", "@");  // what is it?
        res.add_kind(py, Argument, "ARGUMENT");
        res.add_static(py, Assign, "ASSIGN", ":=");
        res.add_static(py, AddAssign, "ADDASSIGN", "+=");
        res.add_static(py, SubAssign, "REMASSIGN", "-=");
        res.add_static(py, Arrow, "ARROW", "->");
        res.add_static(py, Less, "<", "<");
        res.add_static(py, Greater, ">", ">");
        res.add_static(py, Eq, "=", "=");
        res.add_static(py, Ampersand, "&", "&");
        res.add_static(py, Pipe, "|", "|");
        // 'NAMEDONLY', 'SETANNOTATION', 'SETTYPE',
        res.add_kind(py, IntConst, "ICONST");
        res.add_kind(py, BigIntConst, "NICONST");
        res.add_kind(py, FloatConst, "FCONST");
        res.add_kind(py, DecimalConst, "NFCONST");
        res.add_kind(py, BinStr, "BCONST");
        res.add_kind(py, Str, "SCONST");
        // 'RSCONST'
        res.add_static(py, GreaterEq, "OP", ">=");
        res.add_static(py, LessEq, "OP", "<=");
        res.add_static(py, NotEq, "OP", "!=");
        res.add_static(py, DistinctFrom, "OP", "?!=");
        res.add_static(py, NotDistinctFrom, "OP", "?=");
        // 'EOF'
        for kw in UNRESERVED_KEYWORDS.iter() {
            res.add_kw(py, kw);
        }
        for kw in CURRENT_RESERVED_KEYWORDS.iter() {
            res.add_kw(py, kw);
        }
        for kw in FUTURE_RESERVED_KEYWORDS.iter() {
            res.add_kw(py, kw);
        }
        return res;
    }
    fn add_static(&mut self, py: Python, kind: Kind, name: &str, value: &str) {
        let py_name = PyString::new(py, name);
        let value = if name == value {
            py_name.clone_ref(py)
        } else {
            PyString::new(py, value)
        };
        self.tokens.insert(kind, TokenInfo {
            kind, name: py_name, value: Some(value),
        });
    }
    fn add_kind(&mut self, py: Python, kind: Kind, name: &str) {
        self.tokens.insert(kind, TokenInfo {
            kind,
            name: PyString::new(py, name),
            value: None,
        });
    }
    fn add_kw(&mut self, py: Python, name: &str) {
        let py_name = PyString::new(py, &name.to_ascii_uppercase());
        let tok_name = if name.starts_with("__") && name.ends_with("__") {
            format!("DUNDER{}", name[2..name.len()-2].to_ascii_uppercase())
            .to_py_object(py)
        } else {
            py_name.clone_ref(py)
        };
        self.keywords.insert(name.into(), TokenInfo {
            kind: if is_keyword(name) { Kind::Keyword } else { Kind::Ident },
            name: tok_name,
            // Or maybe provide original case of value?
            value: Some(py_name),
        });
    }
}

impl ImportCache {
    fn decimal(&mut self, py: Python) -> PyResult<&mut PyObject> {
        if let Some(ref mut d) = self.decimal {
            return Ok(d);
        }
        let module = py.import("decimal")?;
        let typ = module.get(py, "Decimal")?;
        self.decimal = Some(typ);
        Ok(self.decimal.as_mut().unwrap())
    }
}


fn py_value(py: Python, _tokens: &Tokens, import_cache: &mut ImportCache,
            token: RsToken)
    -> PyResult<PyObject>
{
    use Kind::*;
    match token.kind {
        | Assign
        | SubAssign
        | AddAssign
        | Arrow
        | Coalesce
        | Namespace
        | ForwardLink
        | BackwardLink
        | FloorDiv
        | Concat
        | GreaterEq
        | LessEq
        | NotEq
        | NotDistinctFrom
        | DistinctFrom
        | Comma
        | OpenParen
        | CloseParen
        | OpenBracket
        | CloseBracket
        | OpenBrace
        | CloseBrace
        | Dot
        | Semicolon
        | Colon
        | Add
        | Sub
        | Mul
        | Div
        | Modulo
        | Pow
        | Less
        | Greater
        | Eq
        | Ampersand
        | Pipe
            => Ok(py.None()),
        | Argument => {
            if token.value[1..].starts_with('`') {
                Ok(PyString::new(py, &token.value[2..token.value.len()-1]
                                     .replace("``", "`"))
                   .into_object())
            } else {
                Ok(PyString::new(py, &token.value[1..])
                    .into_object())
            }
        }
        | DecimalConst => {
            Ok(import_cache.decimal(py)?.call(py, (token.value,), None)?)
        }
        FloatConst => {
            Ok(f64::from_str(token.value)
                .map_err(|e| TokenizerError::new(py,
                    format!("error reading float: {}", e)))?
               .to_py_object(py)
               .into_object())
        }
        IntConst => {
            Ok(i64::from_str(token.value)
                .map_err(|e| TokenizerError::new(py,
                    format!("error reading int: {}", e)))?
               .to_py_object(py)
               .into_object())
        }
        BigIntConst => {
            py.get_type::<PyInt>().call(py, (token.value,), None)
        }
        BinStr => {
            Ok(PyBytes::new(py,
                &unquote_bytes(&token.value[2..token.value.len()-1])
                .map_err(|s| TokenizerError::new(py, s))?)
               .into_object())
        }           // b"xx", b'xx'
        Str => {
            if token.value.starts_with('r') {
                Ok(PyString::new(py, &token.value[2..token.value.len()-1])
                   .into_object())
            } else if token.value.starts_with('$') {
                let msize = token.value[1..].find('$').unwrap() + 1;
                Ok(PyString::new(py,
                    &token.value[msize..token.value.len()-msize])
                   .into_object())
            } else {
                Ok(PyString::new(py,
                    &unquote_string(&token.value[1..token.value.len()-1])
                    .map_err(|s| TokenizerError::new(py, s))?)
                   .into_object())
            }
        },
        BacktickName => {
            Ok(PyString::new(py,
                &token.value[1..token.value.len()-1].replace("``", "`"))
               .into_object())
        }
        Keyword => Ok(py.None()),
        // TODO(tailhook) this is also a value. Elimitate duplicate PyString
        Ident => Ok(token.value.to_py_object(py).into_object()),
    }
}

fn unquote_string<'a>(s: &'a str) -> Result<String, String> {
    let mut res = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                match chars.next().expect("slash cant be at the end") {
                    c@'"' | c@'\\' | c@'/' | c@'\'' => res.push(c),
                    'b' => res.push('\u{0010}'),
                    'f' => res.push('\u{000C}'),
                    'n' => res.push('\n'),
                    'r' => res.push('\r'),
                    't' => res.push('\t'),
                    'x' => {
                        let hex = chars.as_str().get(0..2);
                        let code = hex.and_then(|s| {
                            u8::from_str_radix(s, 16).ok()
                        }).ok_or_else(|| {
                            format!("invalid \\x escape {:?}",
                                hex.unwrap_or_else(|| chars.as_str()))
                        })?;
                        if code > 0x7f {
                            return Err(format!("invalid \\x escape {:x} \
                                                (only ascii allowed)", code));
                        }
                        res.push(code as char);
                        chars.nth(1);
                    }
                    'u' => {
                        let hex = chars.as_str().get(0..4);
                        let ch = hex.and_then(|s| {
                                u32::from_str_radix(s, 16).ok()
                            })
                            .and_then(|code| char::from_u32(code))
                            .ok_or_else(|| {
                                format!("invalid \\u escape {:?}",
                                    hex.unwrap_or_else(|| chars.as_str()))
                            })?;
                        res.push(ch);
                        chars.nth(3);
                    }
                    'U' => {
                        let hex = chars.as_str().get(0..8);
                        let ch = hex.and_then(|s| {
                                u32::from_str_radix(s, 16).ok()
                            })
                            .and_then(|code| char::from_u32(code))
                            .ok_or_else(|| {
                                format!("invalid \\U escape {:?}",
                                    hex.unwrap_or_else(|| chars.as_str()))
                            })?;
                        res.push(ch);
                        chars.nth(7);
                    },
                    c => {
                        return Err(format!("bad escaped char {:?}", c));
                    }
                }
            }
            c => res.push(c),
        }
    }

    Ok(res)
}

fn unquote_bytes<'a>(s: &'a str) -> Result<Vec<u8>, String> {
    let mut res = Vec::with_capacity(s.len());
    let mut bytes = s.as_bytes().iter();
    while let Some(&c) = bytes.next() {
        match c {
            b'\\' => {
                match *bytes.next().expect("slash cant be at the end") {
                    c@b'"' | c@b'\\' | c@b'/' | c@b'\'' => res.push(c),
                    b'b' => res.push(b'\x10'),
                    b'f' => res.push(b'\x0C'),
                    b'n' => res.push(b'\n'),
                    b'r' => res.push(b'\r'),
                    b't' => res.push(b'\t'),
                    b'x' => {
                        let tail = &s[s.len() - bytes.as_slice().len()..];
                        let hex = tail.get(0..2);
                        let code = hex.and_then(|s| {
                            u8::from_str_radix(s, 16).ok()
                        }).ok_or_else(|| {
                            format!("invalid \\x escape {:?}",
                                hex.unwrap_or_else(|| tail))
                        })?;
                        res.push(code);
                        bytes.nth(1);
                    }
                    c => {
                        let ch = if c < 0x7f {
                            c as char
                        } else {
                            // recover the unicode byte
                            s[s.len()-bytes.as_slice().len()-1..]
                            .chars().next().unwrap()
                        };
                        return Err(format!("bad escaped char {:?}", ch));
                    }
                }
            }
            c => res.push(c),
        }
    }

    Ok(res)
}

#[test]
fn unquote_unicode_string() {
    // basic tests
    assert_eq!(unquote_string(r#"\x09"#).unwrap(), "\u{09}");
    assert_eq!(unquote_string(r#"\u000A"#).unwrap(), "\u{000A}");
    assert_eq!(unquote_string(r#"\u000D"#).unwrap(), "\u{000D}");
    assert_eq!(unquote_string(r#"\u0020"#).unwrap(), "\u{0020}");
    assert_eq!(unquote_string(r#"\uFFFF"#).unwrap(), "\u{FFFF}");

    // a more complex string
    assert_eq!(unquote_string(r#"\u0009 hello \u000A there"#).unwrap(),
        "\u{0009} hello \u{000A} there");

    assert_eq!(unquote_string(r#"\x62:\u2665:\U000025C6"#).unwrap(),
        "\u{62}:\u{2665}:\u{25C6}");
}
