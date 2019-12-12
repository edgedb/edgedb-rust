use std::sync::Once;
use cpython::{Python, PyDict, PyObject, PyString, GILGuard};
pub use cpython::PyResult as Result;

static PY_INIT: Once = Once::new();

pub type RunResult = Result<()>;

pub fn init_and_acquire() -> GILGuard {
    PY_INIT.call_once(|| {
        std::env::set_var("PYTHONPATH", "./edgeql-pytest:.");
    });
    return Python::acquire_gil();
}

pub fn run(s: &str) -> RunResult {
    let gil = init_and_acquire();
    let py = gil.python();
    match py.run(s, None, None) {
        Ok(()) => Ok(()),
        Err(mut e) => {
            let tb = py.import("traceback")
                .expect("can import traceback");
            let locals = PyDict::new(py);
            locals.set_item(py, "etype", e.get_type(py));
            locals.set_item(py, "evalue", e.instance(py));
            locals.set_item(py, "tb", &e.ptraceback);
            locals.set_item(py, "traceback", tb);
            println!("{}",
                py.eval("''.join(\
                    traceback.format_exception(etype, evalue, tb)\
                )", None, Some(&locals))
               .expect("can format exception")
               .to_string());
            return Err(e);
        }
    }
}
