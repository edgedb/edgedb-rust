use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::slice::Iter;
use std::str;

use crate::display;
use crate::kinds::{tag_check, error_name};
use crate::traits::ErrorKind;


/// Error type returned from any EdgeDB call.
// This includes boxed error, because propagating through call chain is
// faster when error is just one pointer
#[derive(Debug)]
pub struct Error(pub(crate) Box<Inner>);

/// Tag that is used to group similar errors.
pub struct Tag { pub(crate)  bit: u32 }

#[derive(Debug)]
pub(crate) struct Inner {
    pub code: u32,
    pub messages: Vec<Cow<'static, str>>,
    pub error: Option<Box<dyn StdError + Send + Sync + 'static>>,
    pub headers: HashMap<u16, bytes::Bytes>,
}


impl Error {
    pub fn is<T: ErrorKind>(&self) -> bool {
        T::is_superclass_of(self.0.code)
    }
    pub fn has_tag(&self, tag: Tag) -> bool {
        tag_check(self.0.code, tag.bit)
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
    pub fn contexts(&self) -> impl Iterator<Item=&str> {
        self.0.messages[1..].iter()
    }
    pub fn from_code(code: u32) -> Error {
        Error(Box::new(Inner {
            code,
            messages: Vec::new(),
            error: None,
            headers: HashMap::new(),
        }))
    }
    pub fn refine_kind<T: ErrorKind>(mut self) -> Error {
        self.0.code = T::CODE;
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
        if let Some(hint) = self.headers().get(&display::FIELD_HINT) {
            if let Ok(hint) = str::from_utf8(hint) {
                write!(f, "\n  Hint: {}", hint)?;
            }
        }
        if let Some(detail) = self.headers().get(&display::FIELD_DETAILS) {
            if let Ok(detail) = str::from_utf8(detail) {
                write!(f, "\n  Detail: {}", detail)?;
            }
        }
        Ok(())
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.0.error.as_ref().map(|b| b.as_ref() as &dyn std::error::Error)
    }
}
