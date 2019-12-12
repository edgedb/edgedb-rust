use std::collections::HashMap;

use cpython::{PyString, PyResult, Python, PyClone, PythonObject};
use cpython::{PyTuple, PyList, ToPyObject};

use edgeql_parser::tokenizer::{TokenStream, Kind, is_keyword};
use edgeql_parser::tokenizer::{MAX_KEYWORD_LENGTH};
use edgeql_parser::position::Pos;

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
    data _value: PyString;
    data _start: Pos;
    data _end: Pos;
    def kind(&self) -> PyResult<PyString> {
        Ok(self._kind(py).clone_ref(py))
    }
    def value(&self) -> PyResult<PyString> {
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
        Ok(PyString::new(py, &format!("<Token {} {:?}>",
            self._kind(py).to_string(py)?,
            self._value(py).to_string(py)?)))
    }
});

py_exception!(_edgeql_rust, TokenizerError);

py_capsule!(from edb.edgeql._edgeql_rust import _tokens as tokens for Tokens);

pub struct Tokens {
    pub ident: PyString,
    pub tokens: HashMap<Kind, TokenInfo>,
    pub keywords: HashMap<String, TokenInfo>,
}

pub struct TokenInfo {
    pub kind: Kind,
    pub name: PyString,
    pub value: Option<PyString>,
}

pub fn tokenize(py: Python, s: &PyString) -> PyResult<PyList> {
    let tokens = unsafe { tokens::retrieve(py)? };
    let data = s.to_string(py)?;

    let rust_tokens: Vec<_> = py.allow_threads(|| -> Result<_, _> {
        TokenStream::new(&data[..]).collect()
    }).map_err(|e| TokenizerError::new(py, e.to_string()))?;

    let mut buf = Vec::with_capacity(rust_tokens.len());
    let mut keyword_buf = String::with_capacity(MAX_KEYWORD_LENGTH);
    for spanned_tok in rust_tokens {
        let tok = spanned_tok.token;
        let (name, value) = match tok.kind {
            Kind::Keyword | Kind::Ident => {
                if tok.value.len() > MAX_KEYWORD_LENGTH {
                    (tokens.ident.clone_ref(py), PyString::new(py, tok.value))
                } else {
                    keyword_buf.clear();
                    keyword_buf.push_str(tok.value);
                    keyword_buf.make_ascii_lowercase();
                    match tokens.keywords.get(&keyword_buf) {
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
        let py_tok = Token::create_instance(py, name, value,
            spanned_tok.start, spanned_tok.end)?;

        buf.push(py_tok.into_object());
    }
    Ok(PyList::new(py, &buf[..]))
}


impl Tokens {
    pub fn new(py: Python) -> Tokens {
        use Kind::*;
        let mut res = Tokens {
            ident: PyString::new(py, "IDENT"),
            tokens: HashMap::new(),
            keywords: HashMap::new(),
        };
        res.add_static(py, Dot, ".", ".");
        res.add_static(py, ForwardLink, ".>", ".>");
        res.add_static(py, BackwardLink, ".>", ".>");
        res.add_static(py, OpenBracket, "[", "[");
        res.add_static(py, CloseBracket, "]", "]");
        res.add_static(py, OpenParen, "(", "(");
        res.add_static(py, CloseParen, ")", ")");
        res.add_static(py, OpenBrace, "}", "}");
        res.add_static(py, CloseBrace, "}", "}");
        res.add_static(py, Namespace, "::", "::");
        res.add_static(py, Coalesce, "::", "::");
        res.add_static(py, Colon, ":", ":");
        res.add_static(py, Semicolon, ";", ";");
        res.add_static(py, Comma, ",", ",");
        res.add_static(py, Add, "+", "+");
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
        self.keywords.insert(name.into(), TokenInfo {
            kind: if is_keyword(name) { Kind::Keyword } else { Kind::Ident },
            name: py_name.clone_ref(py),
            // Or maybe provide original case of value?
            value: Some(py_name),
        });
    }
}
