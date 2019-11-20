use std::collections::HashMap;
use std::u32;
use std::u16;

use bytes::{Bytes, BytesMut, BufMut};
use snafu::{Snafu, Backtrace, OptionExt};


#[derive(Snafu, Debug)]
pub enum EncodeError {
    #[snafu(display("message doesn't fit 4GiB"))]
    MessageTooLong { backtrace: Backtrace },
    #[snafu(display("string is larger than 64KiB"))]
    StringTooLong { backtrace: Backtrace },
    #[snafu(display("more than 64Ki extensions"))]
    TooManyExtensions { backtrace: Backtrace },
    #[snafu(display("more than 64Ki headers"))]
    TooManyHeaders { backtrace: Backtrace },
    #[snafu(display("more than 64Ki params"))]
    TooManyParams { backtrace: Backtrace },
    #[snafu(display("unknown message types can't be encoded"))]
    UnknownMessageCantBeEncoded { backtrace: Backtrace },
    #[doc(hidden)]
    __NonExhaustive,
}


// TODO(tailhook) non-exhaustive
pub enum Message {
    ClientHandshake(ClientHandshake),
    ServerHandshake(ServerHandshake),
    UnknownMessage(u8, Bytes),
    #[doc(hidden)]
    __NonExhaustive,
}

pub struct ClientHandshake {
    pub major_ver: u16,
    pub minor_ver: u16,
    pub params: HashMap<String, String>,
    pub extensions: HashMap<String, Headers>,
}

pub type Headers = HashMap<u16, Bytes>;

pub struct ServerHandshake {
    pub major_ver: u16,
    pub minor_ver: u16,
    pub extensions: HashMap<String, Headers>,
}

trait Encode {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>;
}

fn to_u32(val: usize) -> Option<u32> {
    if val > u32::MAX as usize {
        return None;
    }
    return Some(val as u32);
}

fn to_u16(val: usize) -> Option<u16> {
    if val > u16::MAX as usize {
        return None;
    }
    return Some(val as u16);
}

fn encode<T: Encode>(buf: &mut BytesMut, code: u8, msg: &T)
    -> Result<(), EncodeError>
{
    buf.reserve(5);
    buf.put_u8(code);
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);

    msg.encode(buf)?;

    let size = to_u32(buf.len() - base).context(MessageTooLong)?;
    buf[base..base+4].copy_from_slice(&size.to_be_bytes()[..]);
    Ok(())
}

impl Message {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        use Message::*;
        match self {
            ClientHandshake(h) => encode(buf, 0x56, h),
            ServerHandshake(h) => encode(buf, 0x76, h),

            UnknownMessage(_, _) => UnknownMessageCantBeEncoded.fail()?,

            // TODO(tailhook) maybe return error ?
            __NonExhaustive => panic!("Invalid Message"),
        }
    }
}

impl Encode for ClientHandshake {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(8);
        buf.put_u16_be(self.major_ver);
        buf.put_u16_be(self.minor_ver);
        buf.put_u16_be(to_u16(self.params.len()).context(TooManyParams)?);
        for (k, v) in &self.params {
            k.encode(buf)?;
            v.encode(buf)?;
        }
        buf.reserve(2);
        buf.put_u16_be(to_u16(self.extensions.len())
            .context(TooManyExtensions)?);
        for (name, headers) in &self.extensions {
            name.encode(buf)?;
            buf.reserve(2);
            buf.put_u16_be(to_u16(headers.len()).context(TooManyHeaders)?);
            for (&name, value) in headers {
                buf.reserve(2);
                buf.put_u16_be(name);
                value.encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Encode for ServerHandshake {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(6);
        buf.put_u16_be(self.major_ver);
        buf.put_u16_be(self.minor_ver);
        buf.put_u16_be(to_u16(self.extensions.len())
            .context(TooManyExtensions)?);
        for (name, headers) in &self.extensions {
            name.encode(buf)?;
            buf.reserve(2);
            buf.put_u16_be(to_u16(headers.len()).context(TooManyHeaders)?);
            for (&name, value) in headers {
                buf.reserve(2);
                buf.put_u16_be(name);
                value.encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Encode for String {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(2 + self.len());
        buf.put_u16_be(to_u16(self.len()).context(StringTooLong)?);
        buf.extend(self.as_bytes());
        Ok(())
    }
}

impl Encode for Bytes {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(2 + self.len());
        buf.put_u16_be(to_u16(self.len()).context(StringTooLong)?);
        buf.extend(&self[..]);
        Ok(())
    }
}
