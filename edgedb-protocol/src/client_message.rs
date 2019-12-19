use std::collections::HashMap;
use std::u16;
use std::convert::TryFrom;
use std::io::Cursor;

use bytes::{Bytes, BytesMut, BufMut, Buf};
use snafu::{OptionExt, ensure};

use crate::encoding::{Encode, Decode, Headers, encode};
use crate::errors::{self, EncodeError, DecodeError};
pub use crate::common::Cardinality;


#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientMessage {
    ClientHandshake(ClientHandshake),
    ExecuteScript(ExecuteScript),
    Prepare(Prepare),
    DescribeStatement(DescribeStatement),
    Execute(Execute),
    UnknownMessage(u8, Bytes),
    Sync,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHandshake {
    pub major_ver: u16,
    pub minor_ver: u16,
    pub params: HashMap<String, String>,
    pub extensions: HashMap<String, Headers>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteScript {
    pub headers: Headers,
    pub script_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepare {
    pub headers: Headers,
    pub io_format: IoFormat,
    pub expected_cardinality: Cardinality,
    pub statement_name: Bytes,
    pub command_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStatement {
    pub headers: Headers,
    pub aspect: DescribeAspect,
    pub statement_name: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execute {
    pub headers: Headers,
    pub statement_name: Bytes,
    pub arguments: Bytes,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DescribeAspect {
    DataDescription = 0x54,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IoFormat {
    Binary = 0x62,
    Json = 0x6a,
}


struct Empty;
impl ClientMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        use ClientMessage::*;
        match self {
            ClientHandshake(h) => encode(buf, 0x56, h),
            ExecuteScript(h) => encode(buf, 0x51, h),
            Prepare(h) => encode(buf, 0x50, h),
            DescribeStatement(h) => encode(buf, 0x44, h),
            Execute(h) => encode(buf, 0x45, h),
            Sync => encode(buf, 0x53, &Empty),

            UnknownMessage(_, _) => {
                errors::UnknownMessageCantBeEncoded.fail()?
            }
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
            0x51 => ExecuteScript::decode(&mut data).map(M::ExecuteScript),
            0x50 => Prepare::decode(&mut data).map(M::Prepare),
            0x45 => Execute::decode(&mut data).map(M::Execute),
            0x53 => Ok(M::Sync),
            0x44 => {
                DescribeStatement::decode(&mut data).map(M::DescribeStatement)
            }
            code => Ok(M::UnknownMessage(code, data.into_inner())),
        }
    }
}

impl Encode for Empty {
    fn encode(&self, _buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        Ok(())
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

impl Encode for ExecuteScript {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(6);
        buf.put_u16_be(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16_be(name);
            value.encode(buf)?;
        }
        self.script_text.encode(buf)?;
        Ok(())
    }
}

impl Decode for ExecuteScript {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 6, errors::Underflow);
        let num_headers = buf.get_u16_be();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16_be(), Bytes::decode(buf)?);
        }
        let script_text = String::decode(buf)?;
        Ok(ExecuteScript { script_text, headers })
    }
}

impl Encode for Prepare {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(12);
        buf.put_u16_be(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16_be(name);
            value.encode(buf)?;
        }
        buf.reserve(10);
        buf.put_u8(self.io_format as u8);
        buf.put_u8(self.expected_cardinality as u8);
        self.statement_name.encode(buf)?;
        self.command_text.encode(buf)?;
        Ok(())
    }
}

impl Decode for Prepare {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let num_headers = buf.get_u16_be();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16_be(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let io_format = match buf.get_u8() {
            0x62 => IoFormat::Binary,
            0x6a => IoFormat::Json,
            c => errors::InvalidIoFormat { io_format: c }.fail()?,
        };
        let expected_cardinality = match buf.get_u8() {
            0x6f => Cardinality::One,
            0x6d => Cardinality::Many,
            c => errors::InvalidCardinality { cardinality: c }.fail()?,
        };
        let statement_name = Bytes::decode(buf)?;
        let command_text = String::decode(buf)?;
        Ok(Prepare {
            headers,
            io_format,
            expected_cardinality,
            statement_name,
            command_text,
        })
    }
}

impl Encode for DescribeStatement {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(7);
        buf.put_u16_be(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        buf.reserve(5);
        buf.put_u8(self.aspect as u8);
        self.statement_name.encode(buf)?;
        Ok(())
    }
}

impl Decode for DescribeStatement {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let num_headers = buf.get_u16_be();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16_be(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let aspect = match buf.get_u8() {
            0x54 => DescribeAspect::DataDescription,
            c => errors::InvalidAspect { aspect: c }.fail()?,
        };
        let statement_name = Bytes::decode(buf)?;
        Ok(DescribeStatement {
            headers,
            aspect,
            statement_name,
        })
    }
}

impl Encode for Execute {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(10);
        buf.put_u16_be(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        self.statement_name.encode(buf)?;
        self.arguments.encode(buf)?;
        Ok(())
    }
}

impl Decode for Execute {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let num_headers = buf.get_u16_be();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16_be(), Bytes::decode(buf)?);
        }
        let statement_name = Bytes::decode(buf)?;
        let arguments = Bytes::decode(buf)?;
        Ok(Execute {
            headers,
            statement_name,
            arguments,
        })
    }
}
