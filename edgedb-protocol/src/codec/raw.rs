use std::io::Cursor;
use std::str;

use bytes::{Bytes, Buf};
use uuid::Uuid;

use crate::errors::{self, DecodeError};
use snafu::{ResultExt, ensure};


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

impl RawCodec for Uuid {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 16, errors::Underflow);
        let uuid = Uuid::from_slice(buf.bytes())
            .context(errors::InvalidUuid)?;
        buf.advance(16);
        Ok(uuid)
    }
}
