use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Cursor;
use std::u16;

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
    AuthenticationSaslInitialResponse(SaslInitialResponse),
    AuthenticationSaslResponse(SaslResponse),
    Dump(Dump),
    Restore(Restore),
    RestoreBlock(RestoreBlock),
    RestoreEof,
    Sync,
    Flush,
    Terminate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslInitialResponse {
    pub method: String,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslResponse {
    pub data: Bytes
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dump {
    pub headers: Headers,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restore {
    pub headers: Headers,
    pub jobs: u16,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreBlock {
    pub data: Bytes,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DescribeAspect {
    DataDescription = 0x54,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IoFormat {
    Binary = 0x62,
    Json = 0x6a,
    JsonElements = 0x4a,
}


struct Empty;
impl ClientMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        use ClientMessage::*;
        match self {
            ClientHandshake(h) => encode(buf, 0x56, h),
            AuthenticationSaslInitialResponse(h) => encode(buf, 0x70, h),
            AuthenticationSaslResponse(h) => encode(buf, 0x72, h),
            ExecuteScript(h) => encode(buf, 0x51, h),
            Prepare(h) => encode(buf, 0x50, h),
            DescribeStatement(h) => encode(buf, 0x44, h),
            Execute(h) => encode(buf, 0x45, h),
            Dump(h) => encode(buf, 0x3e, h),
            Restore(h) => encode(buf, 0x3c, h),
            RestoreBlock(h) => encode(buf, 0x3d, h),
            RestoreEof => encode(buf, 0x2e, &Empty),
            Sync => encode(buf, 0x53, &Empty),
            Flush => encode(buf, 0x48, &Empty),
            Terminate => encode(buf, 0x58, &Empty),

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
        let mut data = Cursor::new(buf.slice(5..));
        match buf[0] {
            0x56 => ClientHandshake::decode(&mut data).map(M::ClientHandshake),
            0x70 => SaslInitialResponse::decode(&mut data)
                .map(M::AuthenticationSaslInitialResponse),
            0x72 => SaslResponse::decode(&mut data)
                .map(M::AuthenticationSaslResponse),
            0x51 => ExecuteScript::decode(&mut data).map(M::ExecuteScript),
            0x50 => Prepare::decode(&mut data).map(M::Prepare),
            0x45 => Execute::decode(&mut data).map(M::Execute),
            0x3e => Dump::decode(&mut data).map(M::Dump),
            0x3c => Restore::decode(&mut data).map(M::Restore),
            0x3d => RestoreBlock::decode(&mut data).map(M::RestoreBlock),
            0x2e => Ok(M::RestoreEof),
            0x53 => Ok(M::Sync),
            0x48 => Ok(M::Flush),
            0x58 => Ok(M::Terminate),
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
        buf.put_u16(self.major_ver);
        buf.put_u16(self.minor_ver);
        buf.put_u16(u16::try_from(self.params.len()).ok()
            .context(errors::TooManyParams)?);
        for (k, v) in &self.params {
            k.encode(buf)?;
            v.encode(buf)?;
        }
        buf.reserve(2);
        buf.put_u16(u16::try_from(self.extensions.len()).ok()
            .context(errors::TooManyExtensions)?);
        for (name, headers) in &self.extensions {
            name.encode(buf)?;
            buf.reserve(2);
            buf.put_u16(u16::try_from(headers.len()).ok()
                .context(errors::TooManyHeaders)?);
            for (&name, value) in headers {
                buf.reserve(2);
                buf.put_u16(name);
                value.encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Decode for ClientHandshake {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let major_ver = buf.get_u16();
        let minor_ver = buf.get_u16();
        let num_params = buf.get_u16();
        let mut params = HashMap::new();
        for _ in 0..num_params {
            params.insert(String::decode(buf)?, String::decode(buf)?);
        }

        ensure!(buf.remaining() >= 2, errors::Underflow);
        let num_ext = buf.get_u16();
        let mut extensions = HashMap::new();
        for _ in 0..num_ext {
            let name = String::decode(buf)?;
            ensure!(buf.remaining() >= 2, errors::Underflow);
            let num_headers = buf.get_u16();
            let mut headers = HashMap::new();
            for _ in 0..num_headers {
                ensure!(buf.remaining() >= 4, errors::Underflow);
                headers.insert(buf.get_u16(), Bytes::decode(buf)?);
            }
            extensions.insert(name, headers);
        }
        Ok(ClientHandshake {
            major_ver, minor_ver, params, extensions,
        })
    }
}

impl Encode for SaslInitialResponse {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.method.encode(buf)?;
        self.data.encode(buf)?;
        Ok(())
    }
}

impl Decode for SaslInitialResponse {
    fn decode(buf: &mut Cursor<Bytes>)
        -> Result<SaslInitialResponse, DecodeError>
    {
        let method = String::decode(buf)?;
        let data = Bytes::decode(buf)?;
        Ok(SaslInitialResponse { method, data })
    }
}

impl Encode for SaslResponse {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.data.encode(buf)?;
        Ok(())
    }
}

impl Decode for SaslResponse {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<SaslResponse, DecodeError> {
        let data = Bytes::decode(buf)?;
        Ok(SaslResponse { data })
    }
}

impl Encode for ExecuteScript {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(6);
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        self.script_text.encode(buf)?;
        Ok(())
    }
}

impl Decode for ExecuteScript {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 6, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
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
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
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
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 8, errors::Underflow);
        let io_format = match buf.get_u8() {
            0x62 => IoFormat::Binary,
            0x6a => IoFormat::Json,
            0x4a => IoFormat::JsonElements,
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
        buf.put_u16(u16::try_from(self.headers.len()).ok()
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
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
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
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        self.statement_name.encode(buf)?;
        self.arguments.encode(buf)?;
        Ok(())
    }
}

impl Decode for Execute {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
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

impl Encode for Dump {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(10);
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        Ok(())
    }
}

impl Decode for Dump {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        Ok(Dump { headers })
    }
}

impl Encode for Restore {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(4 + self.data.len());
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        buf.extend(&self.data);
        Ok(())
    }
}

impl Decode for Restore {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let jobs = buf.get_u16();

        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }

        let buf_pos = buf.position() as usize;
        let data = buf.get_ref().slice(buf_pos..);
        buf.advance(data.len());
        return Ok(Restore { jobs, headers, data })
    }
}

impl Encode for RestoreBlock {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.extend(&self.data);
        Ok(())
    }
}

impl Decode for RestoreBlock {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        let buf_pos = buf.position() as usize;
        let data = buf.get_ref().slice(buf_pos..);
        buf.advance(data.len());
        return Ok(RestoreBlock { data })
    }
}
