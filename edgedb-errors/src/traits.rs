use std::borrow::Cow;
use std::collections::HashMap;

use crate::error::{Error, Inner};


/// Trait that marks EdgeDB errors
///
/// Currently sealed, because edgedb errors will be changed in future
pub trait ErrorKind: Sealed {
    fn with_message<S: Into<Cow<'static, str>>>(s: S) -> Error {
        Self::build().context(s)
    }
    fn with_source<E: std::error::Error+Send+Sync+'static>(src: E) -> Error {
        Error(Box::new(Inner {
            code: Self::CODE,
            messages: Vec::new(),
            error: Some(src.into()),
            headers: HashMap::new(),
        }))
    }
    fn with_source_box(src: Box<dyn std::error::Error + Send+Sync>) -> Error {
        Error(Box::new(Inner {
            code: Self::CODE,
            messages: Vec::new(),
            error: Some(src),
            headers: HashMap::new(),
        }))
    }
    fn build() -> Error {
        Error(Box::new(Inner {
            code: Self::CODE,
            messages: Vec::new(),
            error: None,
            headers: HashMap::new(),
        }))
    }
}

pub trait ResultExt<T> {
    fn context<C>(self, context: C) -> Result<T, Error>
        where C: Into<Cow<'static, str>>;
    fn with_context<C, F>(self, f: F) -> Result<T, Error>
        where C: Into<Cow<'static, str>>,
              F: FnOnce() -> C;
}

impl<T> ResultExt<T> for Result<T, Error> {
    fn context<C>(self, context: C) -> Result<T, Error>
        where C: Into<Cow<'static, str>>
    {
        self.map_err(|e| e.context(context))
    }
    fn with_context<C, F>(self, f: F) -> Result<T, Error>
        where C: Into<Cow<'static, str>>,
              F: FnOnce() -> C,
    {
        self.map_err(|e| e.context(f()))
    }
}

pub trait Sealed {
    const CODE: u32;
    const NAME: &'static str;
    const TAGS: u32;
    // TODO(tailhook) use uuids of errors instead
    fn is_superclass_of(code: u32) -> bool {
        let mask = 0xFFFFFFFF_u32
                   << (Self::CODE.trailing_zeros() / 8)*8;
        code & mask == Self::CODE
    }
    fn has_tag(bit: u32) -> bool {
        Self::TAGS & (1 << bit) != 0
    }
}
