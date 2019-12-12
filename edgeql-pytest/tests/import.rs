use cpython::{PyResult};

mod py;


#[test]
fn import() -> PyResult<()> {
    let gil = py::init_and_acquire();
    let py = gil.python();
    py.import("edb.edgeql._edgeql_rust")?;
    Ok(())
}

#[test]
fn import_types() -> PyResult<()> {
    py::run("\
        from edb.edgeql._edgeql_rust import tokenize as _tokenize, Token
    ")
}
