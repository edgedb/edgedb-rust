use std::borrow::Cow;
use std::error::Error as StdError;
use std::collections::BTreeMap;

use crate::traits::ErrorKind;


/// Error object returned from any EdgeDB call
// This includes boxed error, because propagating through call chain is
// faster when error is just one pointer
pub struct Error(Box<Inner>);

struct Inner {
    code: u64,
    message: Cow<'static, str>,
    error: Option<Box<dyn StdError + Send + Sync + 'static>>,
    headers: BTreeMap<String, Vec<u8>>,
}


impl Error {
    pub fn is<T: ErrorKind>(&self) -> bool {
        T::is_superclass_of(self.0.code)
    }
}
