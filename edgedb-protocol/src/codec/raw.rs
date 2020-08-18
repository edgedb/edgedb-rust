use std::io::Cursor;
use std::str;

use bytes::{Bytes, Buf};

use crate::errors::{self, DecodeError};
use crate::model::{Json, Uuid};
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

impl RawCodec for Json {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let format = buf.get_u8();
        ensure!(format == 1, errors::InvalidJsonFormat);
        let val = str::from_utf8(&buf.bytes())
            .context(errors::InvalidUtf8)?
            .to_owned();
        buf.advance(val.len());
        Ok(Json::new_unchecked(val))
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

impl RawCodec for bool {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let res = match buf.get_u8() {
            0x00 => false,
            0x01 => true,
            _ => errors::InvalidBool.fail()?,
        };
        Ok(res)
    }
}

impl RawCodec for i64 {
    fn decode_raw(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        return Ok(buf.get_i64());
    }
}
