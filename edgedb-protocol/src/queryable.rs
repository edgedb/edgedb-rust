use std::io::Cursor;

use bytes::{Bytes, Buf};
use snafu::ensure;

use crate::errors::{self, DecodeError};
use crate::codec::raw::RawCodec;


pub trait Queryable: Sized {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        let result = Queryable::decode_raw(buf)?;
        ensure!(buf.bytes().len() == 0, errors::ExtraData);
        Ok(result)
    }
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError>;
}

impl Queryable for String {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        RawCodec::decode_raw(buf)
    }
}
