use std::collections::HashMap;
use std::u16;
use std::convert::TryFrom;
use std::io::Cursor;

use bytes::{Bytes, BytesMut, BufMut, Buf};
use snafu::{OptionExt, ensure};

use crate::encoding::{Encode, Decode, Headers, encode};
use crate::errors::{self, EncodeError, DecodeError};


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    ClientHandshake(ClientHandshake),
    UnknownMessage(u8, Bytes),
    #[doc(hidden)]
    __NonExhaustive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHandshake {
    pub major_ver: u16,
    pub minor_ver: u16,
    pub params: HashMap<String, String>,
    pub extensions: HashMap<String, Headers>,
}

impl ClientMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        use ClientMessage::*;
        match self {
            ClientHandshake(h) => encode(buf, 0x56, h),

            UnknownMessage(_, _) => {
                errors::UnknownMessageCantBeEncoded.fail()?
            }

            // TODO(tailhook) maybe return error ?
            __NonExhaustive => panic!("Invalid Message"),
        }
    }
    /// Decode exactly one frame from the buffer
    ///
    /// This expect full frame already be in the buffer. It can return
    /// arbitrary error or be silent if message is only partially present
    /// in the buffer or if extra data present.
    pub fn decode(buf: &Bytes) -> Result<ClientMessage, DecodeError> {
        use self::ClientMessage as M;
        let mut data = Cursor::new(buf.slice_from(5));
        match buf[0] {
            0x56 => ClientHandshake::decode(&mut data).map(M::ClientHandshake),
            code => Ok(M::UnknownMessage(code, data.into_inner())),
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
        buf.put_u16_be(u16::try_from(self.params.len()).ok()
            .context(errors::TooManyParams)?);
        for (k, v) in &self.params {
            k.encode(buf)?;
            v.encode(buf)?;
        }
        buf.reserve(2);
        buf.put_u16_be(u16::try_from(self.extensions.len()).ok()
            .context(errors::TooManyExtensions)?);
        for (name, headers) in &self.extensions {
            name.encode(buf)?;
            buf.reserve(2);
            buf.put_u16_be(u16::try_from(headers.len()).ok()
                .context(errors::TooManyHeaders)?);
            for (&name, value) in headers {
                buf.reserve(2);
                buf.put_u16_be(name);
                value.encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Decode for ClientHandshake {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let major_ver = buf.get_u16_be();
        let minor_ver = buf.get_u16_be();
        let num_params = buf.get_u16_be();
        let mut params = HashMap::new();
        for _ in 0..num_params {
            params.insert(String::decode(buf)?, String::decode(buf)?);
        }

        ensure!(buf.remaining() >= 2, errors::Underflow);
        let num_ext = buf.get_u16_be();
        let mut extensions = HashMap::new();
        for _ in 0..num_ext {
            let name = String::decode(buf)?;
            ensure!(buf.remaining() >= 2, errors::Underflow);
            let num_headers = buf.get_u16_be();
            let mut headers = HashMap::new();
            for _ in 0..num_headers {
                ensure!(buf.remaining() >= 4, errors::Underflow);
                headers.insert(buf.get_u16_be(), Bytes::decode(buf)?);
            }
            extensions.insert(name, headers);
        }
        Ok(ClientHandshake {
            major_ver, minor_ver, params, extensions,
        })
    }
}
