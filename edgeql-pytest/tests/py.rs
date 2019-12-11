use std::sync::Once;
use cpython::{Python, GILGuard};

static PY_INIT: Once = Once::new();


pub fn init_and_acquire() -> GILGuard {
    PY_INIT.call_once(|| {
        std::env::set_var("PYTHONPATH", "./edgeql-pytest:.");
    });
    return Python::acquire_gil();
}
