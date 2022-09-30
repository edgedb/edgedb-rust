use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::str;

use crate::kinds::{tag_check, error_name};
use crate::kinds::{UserError};
use crate::traits::ErrorKind;


const FIELD_HINT: u16 = 0x_00_01;
const FIELD_DETAILS: u16 = 0x_00_02;
const FIELD_SERVER_TRACEBACK: u16 = 0x_01_01;

// TODO(tailhook) these might be deprecated?
const FIELD_POSITION_START: u16 = 0x_FF_F1;
const FIELD_POSITION_END: u16 = 0x_FF_F2;
const FIELD_LINE: u16 = 0x_FF_F3;
const FIELD_COLUMN: u16 = 0x_FF_F4;

/// Error type returned from any EdgeDB call.
// This includes boxed error, because propagating through call chain is
// faster when error is just one pointer
#[derive(Debug)]
pub struct Error(pub(crate) Box<Inner>);

pub struct Chain<'a>(Option<&'a (dyn StdError + 'static)>);

/// Tag that is used to group similar errors.
#[derive(Clone, Copy)]
pub struct Tag { pub(crate)  bit: u32 }

pub(crate) enum Source {
    Box(Box<dyn StdError + Send + Sync + 'static>),
    Ref(Box<
        dyn AsRef<dyn StdError + Send + Sync + 'static>
        + Send + Sync + 'static
    >),
}

#[derive(Debug)]
pub(crate) struct Inner {
    pub code: u32,
    pub messages: Vec<Cow<'static, str>>,
    pub error: Option<Source>,
    pub headers: HashMap<u16, bytes::Bytes>,
    pub source_code: Option<String>,
}

trait Assert: Send + Sync + 'static {}
impl Assert for Error {}

impl Error {
    pub fn is<T: ErrorKind>(&self) -> bool {
        T::is_superclass_of(self.0.code)
    }
    pub fn has_tag(&self, tag: Tag) -> bool {
        tag_check(self.0.code, tag.bit)
    }
    pub fn chain(&self) -> Chain {
        Chain(Some(self))
    }
    pub fn context<S: Into<Cow<'static, str>>>(mut self, msg: S) -> Error {
        self.0.messages.push(msg.into());
        self
    }
    pub fn headers(&self) -> &HashMap<u16, bytes::Bytes> {
        &self.0.headers
    }
    pub fn with_headers(mut self, headers: HashMap<u16, bytes::Bytes>)
        -> Error
    {
        self.0.headers = headers;
        self
    }
    pub fn kind_name(&self) -> &str {
        error_name(self.0.code)
    }
    pub fn kind_debug(&self) -> impl fmt::Display {
        format!("{} [0x{:08X}]", error_name(self.0.code), self.0.code)
    }
    pub fn initial_message(&self) -> Option<&str> {
        self.0.messages.first().map(|m| &m[..])
    }
    pub fn contexts(&self) -> impl DoubleEndedIterator<Item=&str> {
        self.0.messages.iter().skip(1).map(|m| &m[..])
    }
    fn header(&self, field: u16) -> Option<&str> {
        if let Some(value) = self.headers().get(&field) {
            if let Ok(value) = str::from_utf8(value) {
                return Some(value);
            }
        }
        None
    }
    fn usize_header(&self, field: u16) -> Option<usize> {
        self.header(field)
            .and_then(|x| x.parse::<u32>().ok())
            .map(|x| x as usize)
    }
    pub fn hint(&self) -> Option<&str> {
        self.header( FIELD_HINT)
    }
    pub fn details(&self) -> Option<&str> {
        self.header(FIELD_DETAILS)
    }
    pub fn server_traceback(&self) -> Option<&str> {
        self.header(FIELD_SERVER_TRACEBACK)
    }
    pub fn position_start(&self) -> Option<usize> {
        self.usize_header(FIELD_POSITION_START)
    }
    pub fn position_end(&self) -> Option<usize> {
        self.usize_header(FIELD_POSITION_END)
    }
    pub fn line(&self) -> Option<usize> {
        self.usize_header(FIELD_LINE)
    }
    pub fn column(&self) -> Option<usize> {
        self.usize_header(FIELD_COLUMN)
    }
    pub(crate) fn unknown_headers(&self)
        -> impl Iterator<Item=(&u16, &bytes::Bytes)>
    {
        self.headers().iter().filter(|(key, _)| {
            **key != FIELD_HINT &&
                **key != FIELD_DETAILS &&
                **key != FIELD_POSITION_START &&
                **key != FIELD_POSITION_END &&
                **key != FIELD_LINE &&
                **key != FIELD_COLUMN
        })
    }
    pub fn from_code(code: u32) -> Error {
        Error(Box::new(Inner {
            code,
            messages: Vec::new(),
            error: None,
            headers: HashMap::new(),
            source_code: None,
        }))
    }
    pub fn code(&self) -> u32 {
        self.0.code
    }
    pub fn refine_kind<T: ErrorKind>(mut self) -> Error {
        self.0.code = T::CODE;
        self
    }
    pub fn add_source_code(mut self, text: impl Into<String>) -> Error {
        self.0.source_code = Some(text.into());
        self
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let kind = self.kind_name();
        if f.alternate() {
            write!(f, "{}", kind)?;
            for msg in self.0.messages.iter().rev() {
                write!(f, ": {}", msg)?;
            }
            if let Some(mut src) = self.source() {
                write!(f, ": {}", src)?;
                while let Some(next) = src.source() {
                    write!(f, ": {}", next)?;
                    src = next;
                }
            }

        } else {
            if let Some(last) = self.0.messages.last() {
                write!(f, "{}: {}", kind, last)?;
            } else {
                write!(f, "{}", kind)?;
            }
        }
        if let Some(hint) = self.hint() {
            write!(f, "\n  Hint: {}", hint)?;
        }
        if let Some(detail) = self.details() {
            write!(f, "\n  Detail: {}", detail)?;
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.error.as_ref().map(|s| match s {
            Source::Box(b) => b.as_ref() as &dyn std::error::Error,
            Source::Ref(b) => (**b).as_ref() as &dyn std::error::Error,
        })
    }
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Source::Box(b) => fmt::Debug::fmt(b.as_ref(), f),
            Source::Ref(b) => fmt::Debug::fmt((**b).as_ref(), f),
        }
    }
}

impl<T> From<T> for Error
    where T: AsRef<dyn StdError + Send + Sync + 'static>
             + Send + Sync + 'static,
{
    fn from(err: T) -> Error {
        UserError::with_source_ref(err)
    }
}

impl<'a> Iterator for Chain<'a> {
    type Item = &'a (dyn StdError + 'static);
    fn next(&mut self) -> Option<Self::Item> {
        let result = self.0.take();
        self.0 = result.and_then(|e| e.source());
        result
    }
}
