use std::collections::HashMap;
use std::u32;
use std::convert::TryFrom;
use std::io::Cursor;

use uuid::Uuid;
use bytes::{Bytes, BytesMut, BufMut, Buf};
use snafu::{ResultExt, OptionExt, ensure};

use crate::errors::{self, EncodeError, DecodeError};


pub type Headers = HashMap<u16, Bytes>;

pub(crate) trait Encode {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>;
}

pub(crate) trait Decode: Sized {
    fn decode(buf: &mut Cursor<Bytes>)
        -> Result<Self, DecodeError>;
}

pub(crate) fn encode<T: Encode>(buf: &mut BytesMut, code: u8, msg: &T)
    -> Result<(), EncodeError>
{
    buf.reserve(5);
    buf.put_u8(code);
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    msg.encode(buf)?;

    let size = u32::try_from(buf.len() - base).ok()
        .context(errors::MessageTooLong)?;
    buf[base..base+4].copy_from_slice(&size.to_be_bytes()[..]);
    Ok(())
}

impl Encode for String {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(2 + self.len());
        buf.put_u32(u32::try_from(self.len()).ok()
            .context(errors::StringTooLong)?);
        buf.extend(self.as_bytes());
        Ok(())
    }
}

impl Encode for Bytes {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(2 + self.len());
        buf.put_u32(u32::try_from(self.len()).ok()
            .context(errors::StringTooLong)?);
        buf.extend(&self[..]);
        Ok(())
    }
}

impl Decode for String {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let len = buf.get_u32() as usize;
        // TODO(tailhook) ensure size < i32::MAX
        ensure!(buf.remaining() >= len, errors::Underflow);
        let mut data = vec![0u8; len];
        buf.copy_to_slice(&mut data[..]);
        let result = String::from_utf8(data)
            .map_err(|e| e.utf8_error())
            .context(errors::InvalidUtf8);
        return result;
    }
}

impl Decode for Bytes {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let len = buf.get_u32() as usize;
        // TODO(tailhook) ensure size < i32::MAX
        ensure!(buf.remaining() >= len, errors::Underflow);
        let buf_pos = buf.position() as usize;
        let result = buf.get_ref().slice(buf_pos..buf_pos + len);
        buf.advance(len);
        Ok(result)
    }
}

impl Decode for Uuid {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 16, errors::Underflow);
        let mut bytes = [0u8; 16];
        buf.copy_to_slice(&mut bytes[..]);
        let result = Uuid::from_slice(&bytes)
            .context(errors::InvalidUuid)?;
        Ok(result)
    }
}

impl Encode for Uuid {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.extend(self.as_bytes());
        Ok(())
    }
}
