#[macro_use] extern crate cpython;

py_module_initializer!(
    _edgeql_rust, init_edgeql_rust, PyInit__edgeql_rust,
    |py, m| {
        m.add(py, "__doc__", "Rust enhancements for edgeql language parser")?;
        Ok(())
    });
