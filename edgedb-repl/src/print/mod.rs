use std::cmp::min;
use std::error::Error;
use std::fmt;
use std::io;
use std::marker::PhantomData;

use async_std::stream::{Stream, StreamExt};
use atty;
use term_size;
use snafu::{Snafu, ResultExt, AsErrorSource};

mod format;
mod buffer;
mod stream;
mod formatter;
#[cfg(test)] mod tests;

use format::FormatExt;

#[derive(Snafu, Debug)]
pub enum PrintError<S: AsErrorSource + Error, P: AsErrorSource + Error> {
    #[snafu(display("error fetching element"))]
    StreamErr { source: S },
    #[snafu(display("error printing element"))]
    PrintErr { source: P },
}


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
    comma: bool,

    error: PhantomData<*const E>,
}

struct Stdout {}

unsafe impl<T: Send, E> Send for Printer<T, E> {}

pub async fn print_to_stdout<S, I, E>(mut rows: S)
    -> Result<(), PrintError<E, io::Error>>
    where S: Stream<Item=Result<I, E>> + Send + Unpin,
          I: FormatExt,
          E: fmt::Debug + Error + 'static,
{
    let w = term_size::dimensions_stdout().map(|(w, _h)| w).unwrap_or(80);
    let mut prn = Printer {
        colors: atty::is(atty::Stream::Stdout),
        indent: 2,
        max_width: w,
        min_width: min(w/2, 40),
        max_column_width: min(w, 80),

        buffer: String::with_capacity(8192),
        stream: Stdout {},
        comma: false,

        error: PhantomData::<*const io::Error>,
    };
    prn.open_brace().context(PrintErr)?;
    while let Some(v) = rows.next().await.transpose().context(StreamErr)? {
        v.format(&mut prn).context(PrintErr)?;
        prn.comma().context(PrintErr)?;
    }
    prn.close_brace().context(PrintErr)?;
    Ok(())
}

#[cfg(test)]
pub fn print_to_string<I: FormatExt>(items: &[I])
    -> Result<String, std::convert::Infallible>
{
    let mut out = String::new();
    let mut prn = Printer {
        colors: false,
        indent: 2,
        max_width: 60,
        min_width: 40,
        max_column_width: 60,

        buffer: String::with_capacity(8192),
        stream: &mut out,
        comma: false,

        error: PhantomData::<*const std::convert::Infallible>,
    };
    prn.open_brace()?;
    for v in items {
        v.format(&mut prn)?;
        prn.comma()?;
    }
    prn.close_brace()?;
    Ok(out)
}
