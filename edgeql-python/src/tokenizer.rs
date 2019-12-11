use std::collections::HashMap;

use cpython::{PyString, Python};

use edgeql_parser::tokenizer::Kind;


pub struct Tokens {
    pub tokens: HashMap<Kind, TokenInfo>,
}

pub struct TokenInfo {
    pub kind: Kind,
    pub name: PyString,
    pub value: Option<PyString>,
}

impl Tokens {
    pub fn new(py: Python) -> Tokens {
        let mut tokens = HashMap::new();
        return Tokens { tokens };
    }
}
