use std::io::Cursor;
use std::str;

use bytes::{Bytes, Buf};

use crate::errors::{self, DecodeError};
use snafu::{ResultExt};


pub trait RawCodec: Sized {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError>;
}

impl RawCodec for String {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        let val = str::from_utf8(&buf.bytes())
            .context(errors::InvalidUtf8)?
            .to_owned();
        buf.advance(buf.bytes().len());
        Ok(val)
    }
}
