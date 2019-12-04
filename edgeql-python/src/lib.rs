#[macro_use] extern crate cpython;

py_module_initializer!(
    edgeql_python, initedgeql_python, PyInit_edgeql_python,
    |py, m| {
        m.add(py, "__doc__", "Rust enhancements for edgeql language parser")?;
        Ok(())
    });
