use std::collections::HashMap;
use std::convert::TryFrom;
use std::ops::{RangeBounds, Deref, DerefMut};
use std::u32;

use uuid::Uuid;
use bytes::{Bytes, BytesMut, BufMut, Buf};
use snafu::{ResultExt, OptionExt, ensure};

use crate::features::ProtocolVersion;
use crate::errors::{self, EncodeError, DecodeError};


pub type KeyValues = HashMap<u16, Bytes>;
pub type Annotations = HashMap<String, String>;

pub struct Input {
    #[allow(dead_code)]
    proto: ProtocolVersion,
    bytes: Bytes,
}

pub struct Output<'a> {
    #[allow(dead_code)]
    proto: &'a ProtocolVersion,
    bytes: &'a mut BytesMut,
}

pub(crate) trait Encode {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>;
}

pub(crate) trait Decode: Sized {
    fn decode(buf: &mut Input)
        -> Result<Self, DecodeError>;
}

impl Input {
    pub fn new(proto: ProtocolVersion, bytes: Bytes) -> Input {
        Input { proto, bytes }
    }
    pub fn proto(&self) -> &ProtocolVersion {
        &self.proto
    }
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Input {
        Input {
            proto: self.proto.clone(),
            bytes: self.bytes.slice(range),
        }
    }
}

impl Buf for Input {
    fn remaining(&self) -> usize {
        self.bytes.remaining()
    }

    fn chunk(&self) -> &[u8] {
        self.bytes.chunk()
    }

    fn advance(&mut self, cnt: usize) {
        self.bytes.advance(cnt)
    }

    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        self.bytes.copy_to_bytes(len)
    }
}

impl Deref for Input {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes[..]
    }
}

impl Deref for Output<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes[..]
    }
}

impl DerefMut for Output<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[..]
    }
}

impl Output<'_> {
    pub fn new<'x>(proto: &'x ProtocolVersion, bytes: &'x mut BytesMut)
        -> Output<'x>
    {
        Output {
            proto,
            bytes,
        }
    }
    pub fn proto(&self) -> &ProtocolVersion {
        &self.proto
    }
    pub fn reserve(&mut self, size: usize) {
        self.bytes.reserve(size)
    }
    pub fn extend(&mut self, slice: &[u8]) {
        self.bytes.extend(slice)
    }
}

unsafe impl BufMut for Output<'_> {
    fn remaining_mut(&self) -> usize {
        self.bytes.remaining_mut()
    }
    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.bytes.advance_mut(cnt)
    }
    fn chunk_mut(&mut self) -> &mut bytes::buf::UninitSlice {
        self.bytes.chunk_mut()
    }
}


pub(crate) fn encode<T: Encode>(buf: &mut Output, code: u8, msg: &T)
    -> Result<(), EncodeError>
{
    buf.reserve(5);
    buf.put_u8(code);
    let base = buf.len();
    buf.put_slice(&[0; 4]);

    msg.encode(buf)?;

    let size = u32::try_from(buf.len() - base).ok()
        .context(errors::MessageTooLong)?;
    buf[base..base+4].copy_from_slice(&size.to_be_bytes()[..]);
    Ok(())
}

impl Encode for String {
    fn encode(&self, buf: &mut Output)
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
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let len = buf.get_u32() as usize;
        // TODO(tailhook) ensure size < i32::MAX
        ensure!(buf.remaining() >= len, errors::Underflow);
        Ok(buf.copy_to_bytes(len))
    }
}

impl Decode for Uuid {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 16, errors::Underflow);
        let mut bytes = [0u8; 16];
        buf.copy_to_slice(&mut bytes[..]);
        let result = Uuid::from_slice(&bytes)
            .context(errors::InvalidUuid)?;
        Ok(result)
    }
}

impl Encode for Uuid {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        buf.extend(self.as_bytes());
        Ok(())
    }
}
