#[macro_use] extern crate cpython;

use cpython::{PyCapsule, PyString, PyErr};
use cpython::exc::RuntimeError;

mod tokenizer;

use tokenizer::{Tokens, TokenizerError, tokenize};

py_module_initializer!(
    _edgeql_rust, init_edgeql_rust, PyInit__edgeql_rust,
    |py, m| {
        m.add(py, "__doc__", "Rust enhancements for edgeql language parser")?;

        m.add(py, "tokenize", py_fn!(py, tokenize(data: &PyString)))?;
        m.add(py, "TokenizerError", py.get_type::<TokenizerError>())?;
        m.add(py, "_tokens", PyCapsule::new_data(py,
            Box::leak(Box::new(Tokens::new(py))),
            "edb.edgeql._edgeql_rust._tokens")
            .map_err(|_| PyErr::new::<RuntimeError, _>(py,
                "Can't initialize _edgeql_rust module"), )?)?;
        Ok(())
    });
