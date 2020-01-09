use std::io;
use std::cmp::min;
use std::marker::PhantomData;
use std::convert::Infallible;

use atty;
use term_size;

use edgedb_protocol::value::Value;

mod format;
mod buffer;
mod stream;
mod formatter;
#[cfg(test)] mod tests;

use format::FormatExt;


pub(in crate::print) struct Printer<T, E> {

    // config
    colors: bool,
    indent: usize,
    max_width: usize,
    min_width: usize,
    max_column_width: usize,

    // state
    buffer: String,
    stream: T,

    error: PhantomData<*const E>,
}

pub fn print_to_stdout(v: &Value) -> Result<(), io::Error> {
    let w = term_size::dimensions_stdout().map(|(w, _h)| w).unwrap_or(80);
    let stdout = io::stdout();
    let mut prn = Printer {
        colors: atty::is(atty::Stream::Stdout),
        indent: 2,
        max_width: w,
        min_width: min(w/2, 40),
        max_column_width: min(w, 80),

        buffer: String::with_capacity(8192),
        stream: stdout.lock(),

        error: PhantomData::<*const io::Error>,
    };
    v.format(&mut prn)?;
    Ok(())
}

pub fn print_to_string(v: &Value) -> Result<String, Infallible> {
    let mut out = String::new();
    let mut prn = Printer {
        colors: false,
        indent: 2,
        max_width: 60,
        min_width: 40,
        max_column_width: 60,

        buffer: String::with_capacity(8192),
        stream: &mut out,

        error: PhantomData::<*const Infallible>,
    };
    v.format(&mut prn)?;
    Ok(out)
}
