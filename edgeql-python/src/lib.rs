#[macro_use] extern crate cpython;

use cpython::PyString;

mod tokenizer;

use tokenizer::{Token, TokenizerError, tokenize};

py_module_initializer!(
    _edgeql_rust, init_edgeql_rust, PyInit__edgeql_rust,
    |py, m| {
        tokenizer::init_module(py);
        m.add(py, "__doc__", "Rust enhancements for edgeql language parser")?;

        m.add(py, "tokenize", py_fn!(py, tokenize(data: &PyString)))?;
        m.add(py, "Token", py.get_type::<Token>())?;
        m.add(py, "TokenizerError", py.get_type::<TokenizerError>())?;
        Ok(())
    });
