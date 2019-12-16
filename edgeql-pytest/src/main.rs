use cpython::{Python, PyResult};


fn main() -> PyResult<()> {
    let gil = Python::acquire_gil();
    let py = gil.python();
    py.run("\
            import sys\n\
            sys.path.append('./edgeql-pytest')\n\
            sys.path.append('.')
        ", None, None)?;
    py.run("\
        import edb._edgeql_rust\n\
        help(edb._edgeql_rust)
    ", None, None)?;
    Ok(())
}
