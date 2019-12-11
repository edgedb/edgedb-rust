#[macro_use] extern crate cpython;

use std::ffi::c_void;

use cpython::{PyCapsule, PyResult, PyModule, Python, PyErr};
use cpython::exc::RuntimeError;

mod tokenizer;

use tokenizer::Tokens;

py_capsule!(from edb.edgeql._edgeql_rust import _tokens as tokens for Tokens);

unsafe extern "C" fn module_free(_: *mut c_void) {
    println!("free edgeql_rust");
}

fn module_init(py: Python, m: &PyModule) -> PyResult<()> {
    m.add(py, "__doc__", "Rust enhancements for edgeql language parser")?;
    m.add(py, "_tokens", PyCapsule::new_data(py,
        Box::leak(Box::new(Tokens::new(py))),
        "edb.edgeql._edgeql_rust._tokens")
        .map_err(|_| PyErr::new::<RuntimeError, _>(py,
            "Can't initialize _edgeql_rust module"), )?);
    Ok(())
}

// We don't use py_module_initializer macro because we want m_free
#[no_mangle]
#[allow(non_snake_case)]
pub unsafe extern "C" fn PyInit__edgeql_rust()
    -> *mut cpython::_detail::ffi::PyObject
{
    static mut MODULE_DEF: cpython::_detail::ffi::PyModuleDef
        = cpython::_detail::ffi::PyModuleDef_INIT;
    // We can't convert &'static str to *const c_char within a static
    // initializer, so we'll do it here in the module initialization:
    MODULE_DEF.m_name = "_edgeql_rust\0".as_ptr() as *const _;
    MODULE_DEF.m_free = Some(module_free);
    cpython::py_module_initializer_impl(&mut MODULE_DEF, module_init)
}
