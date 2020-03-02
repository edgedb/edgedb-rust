use std::fmt::Write;

use colorful::Colorful;
use colorful::core::color_string::CString;
use colorful::core::StrMarker;
use unicode_segmentation::UnicodeSegmentation;
use snafu::{IntoError, Error, ErrorCompat};

use crate::print::Printer;
use crate::print::stream::Output;
use crate::print::formatter::ColorfulExt;

use Delim::*;


#[derive(Debug)]  // no Error trait, this struct should not escape to user
pub enum Exception<E> {
    DisableFlow,
    Error(E),
}

#[derive(Debug, PartialEq)]
pub(in crate::print) enum Delim {
    None,
    Comma,
    Field,
}

pub(in crate::print) type Result<E> = std::result::Result<(), Exception<E>>;

pub trait WrapErr<T, E>: Sized {
    fn wrap_err<C, E2>(self, context: C)
        -> std::result::Result<T, Exception<E2>>
        where C: IntoError<E2, Source = E>,
              E2: Error + ErrorCompat;
}

pub trait UnwrapExc<T, E>: Sized {
    fn unwrap_exc(self) -> std::result::Result<T, E>;
}

impl<T, E> WrapErr<T, E> for std::result::Result<T, Exception<E>> {
    fn wrap_err<C, E2>(self, context: C)
        -> std::result::Result<T, Exception<E2>>
        where C: IntoError<E2, Source = E>,
              E2: Error + ErrorCompat,
    {
        use Exception::*;
        match self {
            Ok(x) => Ok(x),
            Err(DisableFlow) => Err(DisableFlow),
            Err(Error(e)) => Err(Error(context.into_error(e))),
        }
    }
}

impl<T, E> UnwrapExc<T, E> for std::result::Result<T, Exception<E>> {
    fn unwrap_exc(self) -> std::result::Result<T, E> {
        match self {
            Ok(v) => Ok(v),
            Err(Exception::DisableFlow) => panic!("no DisableFlow allowed"),
            Err(Exception::Error(e)) => Err(e),
        }
    }
}

impl<T, E> WrapErr<T, E> for std::result::Result<T, E> {
    fn wrap_err<C, E2>(self, context: C)
        -> std::result::Result<T, Exception<E2>>
        where C: IntoError<E2, Source = E>,
              E2: Error + ErrorCompat,
    {
        use Exception::*;
        match self {
            Ok(x) => Ok(x),
            Err(e) => Err(Error(context.into_error(e))),
        }
    }
}

impl<'a, T: Output> Printer<'a, T> {
    pub(in crate::print) fn flush_buf(&mut self) -> Result<T::Error> {
        debug_assert_eq!(self.committed, self.buffer.len());
        self.stream.write(&self.buffer).map_err(Exception::Error)?;
        self.buffer.clear();
        self.committed = 0;
        Ok(())
    }
    pub(in crate::print) fn write(&mut self, s: CString) -> Result<T::Error> {
        for g in s.to_str().graphemes(true) {
            if g == "\n" {
                if self.flow {
                    return Err(Exception::DisableFlow);
                }
                self.column = 0;
            } else {
                self.column += 1;
            }
        }
        if self.flow && self.column > self.max_width {
            return Err(Exception::DisableFlow);
        }
        if self.colors {
            write!(&mut self.buffer, "{}", s)
                .expect("formatting CString always succeeds");
        } else {
            self.buffer.push_str(&s.to_str());
        }
        Ok(())
    }
    pub(in crate::print) fn commit_line(&mut self) -> Result<T::Error> {
        debug_assert!(!self.flow);
        self.column = 0;
        self.buffer.push('\n');
        self.committed = self.buffer.len();
        self.committed_indent = self.cur_indent;
        self.committed_column = 0;
        // TODO(tailhook) add watermark
        self.flush_buf()
    }
    pub(in crate::print) fn commit(&mut self) -> Result<T::Error> {
        self.committed = self.buffer.len();
        self.committed_indent = self.cur_indent;
        self.committed_column = self.column;
        // TODO(tailhook) add watermark
        self.flush_buf()
    }
    pub(in crate::print) fn write_indent(&mut self) -> Result<T::Error> {
        //debug_assert_eq!(self.column, 0);
        //debug_assert!(!self.flow);
        const INDENT32: &'static str = "                                ";
        for _ in 0..(self.cur_indent / INDENT32.len()) {
            self.buffer.push_str(INDENT32);
        }
        self.buffer.push_str(&INDENT32[..(self.cur_indent % INDENT32.len())]);
        self.column += self.cur_indent;
        Ok(())
    }
    pub(in crate::print) fn rollback(&mut self) {
        self.buffer.truncate(self.committed);
        self.cur_indent = self.committed_indent;
        self.column = self.committed_column;
    }
    pub(in crate::print) fn end(&mut self) -> Result<T::Error> {
        self.commit()?;
        self.flush_buf()
    }
    pub(in crate::print) fn open_block(&mut self, val: CString)
        -> std::result::Result<bool, Exception<T::Error>>
    {
        self.delim = None;
        self.write(val)?;
        if self.flow {
            Ok(false)
        } else {
            self.commit()?;
            self.flow = true;
            Ok(true)
        }
    }
    pub(in crate::print) fn reopen_block(&mut self) -> Result<T::Error> {
        self.delim = None;
        self.flow = false;
        self.rollback();
        self.commit_line()?;
        self.cur_indent += self.indent;
        Ok(())
    }
    pub(in crate::print) fn comma(&mut self) -> Result<T::Error> {
        if self.flow {
            self.delim = Comma;
        } else {
            self.write(",".clear())?;
            self.commit_line()?;
        }
        Ok(())
    }

    pub(in crate::print) fn ellipsis(&mut self) -> Result<T::Error> {
        self.delimit()?;
        if self.flow {
            self.write("...".clear())?;
        } else {
            self.write("...".clear())?;
            self.write(format!(" (further results hidden \\limit {limit})\n",
                limit=self.max_items.unwrap_or(0))
                .dark_gray())?;
        }
        Ok(())
    }
    pub(in crate::print) fn field(&mut self) -> Result<T::Error> {
        self.delim = Field;
        self.write(": ".clear())
    }
    pub(in crate::print) fn close_block(&mut self, val: CString, flag: bool)
        -> Result<T::Error>
    {
        self.delim = None;
        if !self.flow {
            self.cur_indent -= self.indent;
            self.write_indent()?;
        }
        self.write(val)?;
        if flag {
            self.flow = false;
        } else {
            debug_assert!(self.flow);
        }
        Ok(())
    }
    pub(in crate::print) fn block<F>(&mut self,
        open: CString, mut f: F, close: CString)
        -> Result<T::Error>
        where F: FnMut(&mut Self) -> Result<T::Error>
    {
        let flag = self.open_block(open)?;
        match f(self) {
            Ok(()) => {}
            Err(Exception::DisableFlow) if flag => {
                self.reopen_block()?;
                f(self)?;
            }
            Err(e) => return Err(e)?,
        }
        self.close_block(close, flag)?;
        Ok(())
    }
    pub(in crate::print) fn delimit(&mut self) -> Result<T::Error> {
        if self.delim == Comma { // assumes flow
            self.write(", ".clear())?;
        }
        if !self.flow && self.delim != Field {
            self.write_indent()?;
        }
        self.delim = None;
        Ok(())
    }
}
