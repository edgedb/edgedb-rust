use std::borrow::Cow;
use std::error::Error as StdError;
use std::collections::BTreeMap;

use crate::traits::ErrorKind;
use crate::kinds::tag_check;


/// Error object returned from any EdgeDB call
// This includes boxed error, because propagating through call chain is
// faster when error is just one pointer
pub struct Error(Box<Inner>);

/// Tag that is used to group simiar errors
pub struct Tag { pub(crate)  bit: u32 }

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
    pub fn has_tag(&self, tag: Tag) -> bool {
        tag_check(self.0.code, tag.bit)
    }
}
