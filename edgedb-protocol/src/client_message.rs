/*!
([Website reference](https://www.edgedb.com/docs/reference/protocol/messages)) The [ClientMessage](crate::client_message::ClientMessage) enum and related types. 

```rust
pub enum ClientMessage {
    ClientHandshake(ClientHandshake),
    ExecuteScript(ExecuteScript),
    Prepare(Prepare),
    Parse(Parse),
    DescribeStatement(DescribeStatement),
    Execute0(Execute0),
    Execute1(Execute1),
    OptimisticExecute(OptimisticExecute),
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
```
*/

use std::collections::HashMap;
use std::convert::TryFrom;
use std::u16;

use bytes::{Bytes, BufMut, Buf};
use uuid::Uuid;
use snafu::{OptionExt, ensure};

use crate::encoding::{Encode, Decode, encode, Input, Output};
use crate::encoding::{Annotations, KeyValues};
use crate::errors::{self, EncodeError, DecodeError};
pub use crate::common::{Cardinality, CompilationFlags, Capabilities};
pub use crate::common::CompilationOptions;
pub use crate::common::{State, RawTypedesc};


#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ClientMessage {
    AuthenticationSaslInitialResponse(SaslInitialResponse),
    AuthenticationSaslResponse(SaslResponse),
    ClientHandshake(ClientHandshake),
    Dump(Dump),
    Parse(Parse), // protocol > 1.0
    ExecuteScript(ExecuteScript),
    Execute0(Execute0),
    Execute1(Execute1),
    Restore(Restore),
    RestoreBlock(RestoreBlock),
    RestoreEof,
    Sync,
    Terminate,
    Prepare(Prepare),  // protocol < 1.0
    DescribeStatement(DescribeStatement),
    OptimisticExecute(OptimisticExecute),
    UnknownMessage(u8, Bytes),
    Flush,
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
    pub extensions: HashMap<String, KeyValues>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteScript {
    pub headers: KeyValues,
    pub script_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Prepare {
    pub headers: KeyValues,
    pub io_format: IoFormat,
    pub expected_cardinality: Cardinality,
    pub statement_name: Bytes,
    pub command_text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Parse {
    pub annotations: Annotations,
    pub allowed_capabilities: Capabilities,
    pub compilation_flags: CompilationFlags,
    pub implicit_limit: Option<u64>,
    pub output_format: IoFormat,
    pub expected_cardinality: Cardinality,
    pub command_text: String,
    pub state: State,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeStatement {
    pub headers: KeyValues,
    pub aspect: DescribeAspect,
    pub statement_name: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execute0 {
    pub headers: KeyValues,
    pub statement_name: Bytes,
    pub arguments: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execute1 {
    pub annotations: Annotations,
    pub allowed_capabilities: Capabilities,
    pub compilation_flags: CompilationFlags,
    pub implicit_limit: Option<u64>,
    pub output_format: IoFormat,
    pub expected_cardinality: Cardinality,
    pub command_text: String,
    pub state: State,
    pub input_typedesc_id: Uuid,
    pub output_typedesc_id: Uuid,
    pub arguments: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimisticExecute {
    pub headers: KeyValues,
    pub io_format: IoFormat,
    pub expected_cardinality: Cardinality,
    pub command_text: String,
    pub input_typedesc_id: Uuid,
    pub output_typedesc_id: Uuid,
    pub arguments: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dump {
    pub headers: KeyValues,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restore {
    pub headers: KeyValues,
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
    None = 0x6e,
}

struct Empty;
impl ClientMessage {
    pub fn encode(&self, buf: &mut Output) -> Result<(), EncodeError> {
        use ClientMessage::*;
        match self {
            ClientHandshake(h) => encode(buf, 0x56, h),
            AuthenticationSaslInitialResponse(h) => encode(buf, 0x70, h),
            AuthenticationSaslResponse(h) => encode(buf, 0x72, h),
            ExecuteScript(h) => encode(buf, 0x51, h),
            Prepare(h) => encode(buf, 0x50, h),
            Parse(h) => encode(buf, 0x50, h),
            DescribeStatement(h) => encode(buf, 0x44, h),
            Execute0(h) => encode(buf, 0x45, h),
            OptimisticExecute(h) => encode(buf, 0x4f, h),
            Execute1(h) => encode(buf, 0x4f, h),
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
    /// Decode exactly one frame from the buffer.
    ///
    /// This expects a full frame to already be in the buffer. It can return
    /// an arbitrary error or be silent if a message is only partially present
    /// in the buffer or if extra data is present.
    pub fn decode(buf: &mut Input) -> Result<ClientMessage, DecodeError> {
        use self::ClientMessage as M;
        let mut data = buf.slice(5..);
        let result = match buf[0] {
            0x56 => ClientHandshake::decode(&mut data)
                .map(M::ClientHandshake)?,
            0x70 => SaslInitialResponse::decode(&mut data)
                .map(M::AuthenticationSaslInitialResponse)?,
            0x72 => SaslResponse::decode(&mut data)
                .map(M::AuthenticationSaslResponse)?,
            0x51 => ExecuteScript::decode(&mut data).map(M::ExecuteScript)?,
            0x50 => if buf.proto().is_1() {
                Parse::decode(&mut data).map(M::Parse)?
            } else {
                Prepare::decode(&mut data).map(M::Prepare)?
            },
            0x45 => Execute0::decode(&mut data).map(M::Execute0)?,
            0x4f => if buf.proto().is_1() {
                Execute1::decode(&mut data).map(M::Execute1)?
            } else {
                OptimisticExecute::decode(&mut data).map(M::OptimisticExecute)?
            },
            0x3e => Dump::decode(&mut data).map(M::Dump)?,
            0x3c => Restore::decode(&mut data).map(M::Restore)?,
            0x3d => RestoreBlock::decode(&mut data).map(M::RestoreBlock)?,
            0x2e => M::RestoreEof,
            0x53 => M::Sync,
            0x48 => M::Flush,
            0x58 => M::Terminate,
            0x44 => {
                DescribeStatement::decode(&mut data).map(M::DescribeStatement)?
            }
            code => M::UnknownMessage(
                code,
                data.copy_to_bytes(data.remaining()),
            ),
        };
        ensure!(data.remaining() == 0, errors::ExtraData);
        Ok(result)
    }
}

impl Encode for Empty {
    fn encode(&self, _buf: &mut Output)
        -> Result<(), EncodeError>
    {
        Ok(())
    }
}

impl Encode for ClientHandshake {
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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
    fn encode(&self, buf: &mut Output) -> Result<(), EncodeError> {
        self.method.encode(buf)?;
        self.data.encode(buf)?;
        Ok(())
    }
}

impl Decode for SaslInitialResponse {
    fn decode(buf: &mut Input)
        -> Result<SaslInitialResponse, DecodeError>
    {
        let method = String::decode(buf)?;
        let data = Bytes::decode(buf)?;
        Ok(SaslInitialResponse { method, data })
    }
}

impl Encode for SaslResponse {
    fn encode(&self, buf: &mut Output) -> Result<(), EncodeError> {
        self.data.encode(buf)?;
        Ok(())
    }
}

impl Decode for SaslResponse {
    fn decode(buf: &mut Input) -> Result<SaslResponse, DecodeError> {
        let data = Bytes::decode(buf)?;
        Ok(SaslResponse { data })
    }
}

impl Encode for ExecuteScript {
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        debug_assert!(!buf.proto().is_1());
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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
        let expected_cardinality = TryFrom::try_from(buf.get_u8())?;
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
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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

impl Encode for Execute0 {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        debug_assert!(!buf.proto().is_1());
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

impl Decode for Execute0 {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        let statement_name = Bytes::decode(buf)?;
        let arguments = Bytes::decode(buf)?;
        Ok(Execute0 {
            headers,
            statement_name,
            arguments,
        })
    }
}

impl OptimisticExecute {
    pub fn new(
        flags: &CompilationOptions,
        query: &str, arguments: impl Into<Bytes>,
        input_typedesc_id: Uuid, output_typedesc_id: Uuid,
    ) -> OptimisticExecute {
        let mut headers = KeyValues::new();
        if let Some(limit) = flags.implicit_limit {
            headers.insert(0xFF01, Bytes::from(limit.to_string()));
        }
        if flags.implicit_typenames {
            headers.insert(0xFF02, "true".into());
        }
        if flags.implicit_typeids {
            headers.insert(0xFF03, "true".into());
        }
        let caps = flags.allow_capabilities.bits().to_be_bytes();
        headers.insert(0xFF04, caps[..].to_vec().into());
        if flags.explicit_objectids {
            headers.insert(0xFF03, "true".into());
        }
        OptimisticExecute {
            headers,
            io_format: flags.io_format,
            expected_cardinality: flags.expected_cardinality,
            command_text: query.into(),
            input_typedesc_id,
            output_typedesc_id,
            arguments: arguments.into(),
        }
    }
}

impl Encode for OptimisticExecute {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        buf.reserve(2+1+1+4+16+16+4);
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        buf.reserve(1+1+4+16+16+4);
        buf.put_u8(self.io_format as u8);
        buf.put_u8(self.expected_cardinality as u8);
        self.command_text.encode(buf)?;
        self.input_typedesc_id.encode(buf)?;
        self.output_typedesc_id.encode(buf)?;
        self.arguments.encode(buf)?;
        Ok(())
    }
}

impl Decode for OptimisticExecute {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 12, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        let io_format = match buf.get_u8() {
            0x62 => IoFormat::Binary,
            0x6a => IoFormat::Json,
            0x4a => IoFormat::JsonElements,
            c => errors::InvalidIoFormat { io_format: c }.fail()?,
        };
        let expected_cardinality = TryFrom::try_from(buf.get_u8())?;
        let command_text = String::decode(buf)?;
        let input_typedesc_id = Uuid::decode(buf)?;
        let output_typedesc_id = Uuid::decode(buf)?;
        let arguments = Bytes::decode(buf)?;
        Ok(OptimisticExecute {
            headers,
            io_format,
            expected_cardinality,
            command_text,
            input_typedesc_id,
            output_typedesc_id,
            arguments,
        })
    }
}

impl Encode for Execute1 {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        buf.reserve(2+3*8+1+1+4+16+4+16+16+4);
        buf.put_u16(u16::try_from(self.annotations.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (name, value) in &self.annotations {
            buf.reserve(4);
            name.encode(buf)?;
            value.encode(buf)?;
        }
        buf.reserve(3*8+1+1+4+16+4+16+16+4);
        buf.put_u64(self.allowed_capabilities.bits());
        buf.put_u64(self.compilation_flags.bits());
        buf.put_u64(self.implicit_limit.unwrap_or(0));
        buf.put_u8(self.output_format as u8);
        buf.put_u8(self.expected_cardinality as u8);
        self.command_text.encode(buf)?;
        self.state.typedesc_id.encode(buf)?;
        self.state.data.encode(buf)?;
        self.input_typedesc_id.encode(buf)?;
        self.output_typedesc_id.encode(buf)?;
        self.arguments.encode(buf)?;
        Ok(())
    }
}

impl Decode for Execute1 {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 2+3*8+2+4+16+4+16+16+4, errors::Underflow);
        let num_annotations = buf.get_u16();
        let mut annotations = HashMap::new();
        for _ in 0..num_annotations {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            annotations.insert(String::decode(buf)?, String::decode(buf)?);
        }
        ensure!(buf.remaining() >= 3*8+2+4+16+4+16+16+4, errors::Underflow);
        let allowed_capabilities = decode_capabilities(buf.get_u64())?;
        let compilation_flags = decode_compilation_flags(buf.get_u64())?;
        let implicit_limit = match buf.get_u64() {
            0 => None,
            val => Some(val),
        };
        let output_format = match buf.get_u8() {
            0x62 => IoFormat::Binary,
            0x6a => IoFormat::Json,
            0x4a => IoFormat::JsonElements,
            c => errors::InvalidIoFormat { io_format: c }.fail()?,
        };
        let expected_cardinality = TryFrom::try_from(buf.get_u8())?;
        let command_text = String::decode(buf)?;
        let state = State {
            typedesc_id: Uuid::decode(buf)?,
            data: Bytes::decode(buf)?,
        };
        let input_typedesc_id = Uuid::decode(buf)?;
        let output_typedesc_id = Uuid::decode(buf)?;
        let arguments = Bytes::decode(buf)?;
        Ok(Execute1 {
            annotations,
            allowed_capabilities,
            compilation_flags,
            implicit_limit,
            output_format,
            expected_cardinality,
            command_text,
            state,
            input_typedesc_id,
            output_typedesc_id,
            arguments,
        })
    }
}

impl Encode for Dump {
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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
    fn encode(&self, buf: &mut Output)
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
        buf.put_u16(self.jobs);
        buf.extend(&self.data);
        Ok(())
    }
}

impl Decode for Restore {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);

        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }

        let jobs = buf.get_u16();

        let data = buf.copy_to_bytes(buf.remaining());
        return Ok(Restore { jobs, headers, data })
    }
}

impl Encode for RestoreBlock {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        buf.extend(&self.data);
        Ok(())
    }
}

impl Decode for RestoreBlock {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        let data = buf.copy_to_bytes(buf.remaining());
        return Ok(RestoreBlock { data })
    }
}

impl Parse {
    pub fn new(opts: &CompilationOptions, query: &str, state: State) -> Parse {
        Parse {
            annotations: HashMap::new(),
            allowed_capabilities: opts.allow_capabilities,
            compilation_flags: opts.flags(),
            implicit_limit: opts.implicit_limit,
            output_format: opts.io_format,
            expected_cardinality: opts.expected_cardinality,
            command_text: query.into(),
            state,
        }
    }
}

impl Prepare {
    pub fn new(flags: &CompilationOptions, query: &str) -> Prepare {
        let mut headers = KeyValues::new();
        if let Some(limit) = flags.implicit_limit {
            headers.insert(0xFF01, Bytes::from(limit.to_string()));
        }
        if flags.implicit_typenames {
            headers.insert(0xFF02, "true".into());
        }
        if flags.implicit_typeids {
            headers.insert(0xFF03, "true".into());
        }
        let caps = flags.allow_capabilities.bits().to_be_bytes();
        headers.insert(0xFF04, caps[..].to_vec().into());
        if flags.explicit_objectids {
            headers.insert(0xFF03, "true".into());
        }
        Prepare {
            headers,
            io_format: flags.io_format,
            expected_cardinality: flags.expected_cardinality,
            statement_name: Bytes::from(""),
            command_text: query.into(),
        }
    }
}

fn decode_capabilities(val: u64) -> Result<Capabilities, DecodeError> {
    Capabilities::from_bits(val)
    .ok_or_else(|| errors::InvalidCapabilities { capabilities: val }.build())
}

fn decode_compilation_flags(val: u64) -> Result<CompilationFlags, DecodeError>
{
    CompilationFlags::from_bits(val).ok_or_else(|| {
        errors::InvalidCompilationFlags { compilation_flags: val }.build()
    })
}

impl Decode for Parse {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 52, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut annotations = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 8, errors::Underflow);
            annotations.insert(String::decode(buf)?, String::decode(buf)?);
        }
        ensure!(buf.remaining() >= 50, errors::Underflow);
        let allowed_capabilities = decode_capabilities(buf.get_u64())?;
        let compilation_flags = decode_compilation_flags(buf.get_u64())?;
        let implicit_limit = match buf.get_u64() {
            0 => None,
            val => Some(val),
        };
        let output_format = match buf.get_u8() {
            0x62 => IoFormat::Binary,
            0x6a => IoFormat::Json,
            0x4a => IoFormat::JsonElements,
            c => errors::InvalidIoFormat { io_format: c }.fail()?,
        };
        let expected_cardinality = TryFrom::try_from(buf.get_u8())?;
        let command_text = String::decode(buf)?;
        let state = State {
            typedesc_id: Uuid::decode(buf)?,
            data: Bytes::decode(buf)?,
        };
        Ok(Parse {
            annotations,
            allowed_capabilities,
            compilation_flags,
            implicit_limit,
            output_format,
            expected_cardinality,
            command_text,
            state,
        })
    }
}

impl Encode for Parse {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        debug_assert!(buf.proto().is_1());
        buf.reserve(52);
        buf.put_u16(u16::try_from(self.annotations.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (name, value) in &self.annotations {
            buf.reserve(8);
            name.encode(buf)?;
            value.encode(buf)?;
        }
        buf.reserve(50);
        buf.put_u64(self.allowed_capabilities.bits());
        buf.put_u64(self.compilation_flags.bits());
        buf.put_u64(self.implicit_limit.unwrap_or(0));
        buf.put_u8(self.output_format as u8);
        buf.put_u8(self.expected_cardinality as u8);
        self.command_text.encode(buf)?;
        self.state.typedesc_id.encode(buf)?;
        self.state.data.encode(buf)?;
        Ok(())
    }
}
