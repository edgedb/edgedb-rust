use std::collections::HashMap;
use std::u32;
use std::u16;
use std::convert::{TryFrom, TryInto};
use std::io::Cursor;

use bytes::{Bytes, BytesMut, BufMut, Buf};
use uuid::Uuid;
use snafu::{OptionExt, ensure};

use crate::errors::{self, EncodeError, DecodeError};
use crate::encoding::{Headers, Decode, Encode};
use crate::descriptors::{OutputTypedesc, InputTypedesc, Descriptor, TypePos};
pub use crate::common::Cardinality;


#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ServerMessage {
    ServerHandshake(ServerHandshake),
    UnknownMessage(u8, Bytes),
    LogMessage(LogMessage),
    ErrorResponse(ErrorResponse),
    Authentication(Authentication),
    ReadyForCommand(ReadyForCommand),
    ServerKeyData(ServerKeyData),
    ParameterStatus(ParameterStatus),
    CommandComplete(CommandComplete),
    PrepareComplete(PrepareComplete),
    CommandDataDescription(CommandDataDescription),
    Data(Data),
    RestoreReady(RestoreReady),
    // Don't decode Dump packets here as we only need to process them as
    // whole
    DumpHeader(RawPacket),
    DumpBlock(RawPacket),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyForCommand {
    pub headers: Headers,
    pub transaction_state: TransactionState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Authentication {
    Ok,
    Sasl { methods: Vec<String> },
    SaslContinue { data: Bytes },
    SaslFinal { data: Bytes },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    Error,
    Fatal,
    Panic,
    Unknown(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageSeverity {
    Debug,
    Info,
    Notice,
    Warning,
    Unknown(u8),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    // Not in a transaction block.
    NotInTransaction = 0x49,

    // In a transaction block.
    InTransaction = 0x54,

    // In a failed transaction block
    // (commands will be rejected until the block is ended).
    InFailedTransaction = 0x45
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    pub severity: ErrorSeverity,
    pub code: u32,
    pub message: String,
    pub attributes: Headers,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogMessage {
    pub severity: MessageSeverity,
    pub code: u32,
    pub text: String,
    pub attributes: Headers,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerHandshake {
    pub major_ver: u16,
    pub minor_ver: u16,
    pub extensions: HashMap<String, Headers>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerKeyData {
    pub data: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterStatus {
    pub name: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandComplete {
    pub headers: Headers,
    pub status_data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareComplete {
    pub headers: Headers,
    pub cardinality: Cardinality,
    pub input_typedesc_id: Uuid,
    pub output_typedesc_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandDataDescription {
    pub headers: Headers,
    pub result_cardinality: Cardinality,
    pub input_typedesc_id: Uuid,
    pub input_typedesc: Bytes,
    pub output_typedesc_id: Uuid,
    pub output_typedesc: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Data {
    pub data: Vec<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReady {
    pub headers: Headers,
    pub jobs: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPacket {
    pub data: Bytes,
}

fn encode<T: Encode>(buf: &mut BytesMut, code: u8, msg: &T)
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

impl CommandDataDescription {
    pub fn output(&self) -> Result<OutputTypedesc, DecodeError> {
        let mut cur = Cursor::new(self.output_typedesc.clone());
        let mut descriptors = Vec::new();
        while cur.remaining() > 0 {
            match Descriptor::decode(&mut cur)? {
                Descriptor::TypeAnnotation(_) => {}
                item => descriptors.push(item),
            }
        }
        let root_id = self.output_typedesc_id.clone();
        let root_pos = if root_id == Uuid::from_u128(0) {
            None
        } else {
            let idx = descriptors.iter().position(|x| *x.id() == root_id)
                .context(errors::UuidNotFound { uuid: root_id })?;
            let pos = idx.try_into().ok()
                .context(errors::TooManyDescriptors { index: idx })?;
            Some(TypePos(pos))
        };
        Ok(OutputTypedesc { array: descriptors, root_id, root_pos })
    }
    pub fn input(&self) -> Result<InputTypedesc, DecodeError> {
        let mut cur = Cursor::new(self.input_typedesc.clone());
        let mut descriptors = Vec::new();
        while cur.remaining() > 0 {
            match Descriptor::decode(&mut cur)? {
                Descriptor::TypeAnnotation(_) => {}
                item => descriptors.push(item),
            }
        }
        let root_id = self.input_typedesc_id.clone();
        let idx = descriptors.iter().position(|x| *x.id() == root_id)
            .context(errors::UuidNotFound { uuid: root_id })?;
        let pos = idx.try_into().ok()
            .context(errors::TooManyDescriptors { index: idx })?;
        let root_pos = TypePos(pos);
        Ok(InputTypedesc { array: descriptors, root_id, root_pos })
    }
}

impl ServerMessage {
    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        use ServerMessage::*;
        match self {
            ServerHandshake(h) => encode(buf, 0x76, h),
            ErrorResponse(h) => encode(buf, 0x45, h),
            LogMessage(h) => encode(buf, 0x4c, h),
            Authentication(h) => encode(buf, 0x52, h),
            ReadyForCommand(h) => encode(buf, 0x5a, h),
            ServerKeyData(h) => encode(buf, 0x4b, h),
            ParameterStatus(h) => encode(buf, 0x53, h),
            CommandComplete(h) => encode(buf, 0x43, h),
            PrepareComplete(h) => encode(buf, 0x31, h),
            CommandDataDescription(h) => encode(buf, 0x54, h),
            Data(h) => encode(buf, 0x44, h),
            RestoreReady(h) => encode(buf, 0x2b, h),
            DumpHeader(h) => encode(buf, 0x40, h),
            DumpBlock(h) => encode(buf, 0x3d, h),

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
    pub fn decode(buf: &Bytes) -> Result<ServerMessage, DecodeError> {
        use self::ServerMessage as M;
        let mut data = Cursor::new(buf.slice(5..));
        match buf[0] {
            0x76 => ServerHandshake::decode(&mut data).map(M::ServerHandshake),
            0x45 => ErrorResponse::decode(&mut data).map(M::ErrorResponse),
            0x4c => LogMessage::decode(&mut data).map(M::LogMessage),
            0x52 => Authentication::decode(&mut data).map(M::Authentication),
            0x5a => ReadyForCommand::decode(&mut data).map(M::ReadyForCommand),
            0x4b => ServerKeyData::decode(&mut data).map(M::ServerKeyData),
            0x53 => ParameterStatus::decode(&mut data).map(M::ParameterStatus),
            0x43 => CommandComplete::decode(&mut data).map(M::CommandComplete),
            0x31 => PrepareComplete::decode(&mut data).map(M::PrepareComplete),
            0x44 => Data::decode(&mut data).map(M::Data),
            0x2b => RestoreReady::decode(&mut data).map(M::RestoreReady),
            0x40 => RawPacket::decode(&mut data).map(M::DumpHeader),
            0x3d => RawPacket::decode(&mut data).map(M::DumpBlock),
            0x54 => {
                CommandDataDescription::decode(&mut data)
                .map(M::CommandDataDescription)
            }
            code => Ok(M::UnknownMessage(code, data.into_inner())),
        }
    }
}

impl Encode for ServerHandshake {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(6);
        buf.put_u16(self.major_ver);
        buf.put_u16(self.minor_ver);
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

impl Decode for ServerHandshake {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 6, errors::Underflow);
        let major_ver = buf.get_u16();
        let minor_ver = buf.get_u16();
        let num_ext = buf.get_u16();
        let mut extensions = HashMap::new();
        for _ in 0..num_ext {
            let name = String::decode(buf)?;
            ensure!(buf.remaining() >= 2, errors::Underflow);
            let num_headers = buf.get_u16();
            let mut headers = HashMap::new();
            for _ in 0..num_headers {
                headers.insert(buf.get_u16(), Bytes::decode(buf)?);
            }
            extensions.insert(name, headers);
        }
        Ok(ServerHandshake {
            major_ver, minor_ver, extensions,
        })
    }
}

impl Encode for ErrorResponse {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(11);
        buf.put_u8(self.severity.to_u8());
        buf.put_u32(self.code);
        self.message.encode(buf)?;
        buf.reserve(2);
        buf.put_u16(u16::try_from(self.attributes.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.attributes {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        Ok(())
    }
}

impl Decode for ErrorResponse {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<ErrorResponse, DecodeError> {
        ensure!(buf.remaining() >= 11, errors::Underflow);
        let severity = ErrorSeverity::from_u8(buf.get_u8());
        let code = buf.get_u32();
        let message = String::decode(buf)?;
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let num_attributes = buf.get_u16();
        let mut attributes = HashMap::new();
        for _ in 0..num_attributes {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            attributes.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        return Ok(ErrorResponse {
            severity, code, message, attributes,
        })
    }
}

impl Encode for LogMessage {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(11);
        buf.put_u8(self.severity.to_u8());
        buf.put_u32(self.code);
        self.text.encode(buf)?;
        buf.reserve(2);
        buf.put_u16(u16::try_from(self.attributes.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.attributes {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        Ok(())
    }
}

impl Decode for LogMessage {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<LogMessage, DecodeError> {
        ensure!(buf.remaining() >= 11, errors::Underflow);
        let severity = MessageSeverity::from_u8(buf.get_u8());
        let code = buf.get_u32();
        let text = String::decode(buf)?;
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let num_attributes = buf.get_u16();
        let mut attributes = HashMap::new();
        for _ in 0..num_attributes {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            attributes.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        return Ok(LogMessage {
            severity, code, text, attributes,
        })
    }
}

impl Encode for Authentication {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        use Authentication as A;
        buf.reserve(1);
        match self {
            A::Ok => buf.put_u32(0),
            A::Sasl { methods } => {
                buf.put_u32(0x0A);
                buf.reserve(4);
                buf.put_u32(methods.len().try_into()
                    .ok().context(errors::TooManyMethods)?);
                for meth in methods {
                    meth.encode(buf)?;
                }
            }
            A::SaslContinue { data } => {
                buf.put_u32(0x0B);
                data.encode(buf)?;
            }
            A::SaslFinal { data } => {
                buf.put_u32(0x0C);
                data.encode(buf)?;
            }
        }
        Ok(())
    }
}

impl Decode for Authentication {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Authentication, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        match buf.get_u32() {
            0x00 => Ok(Authentication::Ok),
            0x0A => {
                ensure!(buf.remaining() >= 4, errors::Underflow);
                let num_methods = buf.get_u32() as usize;
                let mut methods = Vec::with_capacity(num_methods);
                for _ in 0..num_methods {
                    methods.push(String::decode(buf)?);
                }
                Ok(Authentication::Sasl { methods })
            }
            0x0B => {
                let data = Bytes::decode(buf)?;
                Ok(Authentication::SaslContinue { data })
            }
            0x0C => {
                let data = Bytes::decode(buf)?;
                Ok(Authentication::SaslFinal { data })
            }
            c => errors::AuthStatusInvalid { auth_status: c }.fail()?,
        }
    }
}

impl Encode for ReadyForCommand {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        buf.reserve(3);
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        buf.reserve(1);
        buf.put_u8(self.transaction_state as u8);
        Ok(())
    }
}
impl Decode for ReadyForCommand {
    fn decode(buf: &mut Cursor<Bytes>)
        -> Result<ReadyForCommand, DecodeError>
    {
        use TransactionState::*;
        ensure!(buf.remaining() >= 3, errors::Underflow);
        let mut headers = HashMap::new();
        let num_headers = buf.get_u16();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 1, errors::Underflow);
        let transaction_state = match buf.get_u8() {
            0x49 => NotInTransaction,
            0x54 => InTransaction,
            0x45 => InFailedTransaction,
            s => {
                errors::InvalidTransactionState {
                    transaction_state: s
                }.fail()?
            }
        };
        Ok(ReadyForCommand { headers, transaction_state })
    }
}

impl ErrorSeverity {
    pub fn from_u8(code: u8) -> ErrorSeverity {
        use ErrorSeverity::*;
        match code {
            120 => Error,
            200 => Fatal,
            255 => Panic,
            _ => Unknown(code),
        }
    }
    pub fn to_u8(&self) -> u8 {
        use ErrorSeverity::*;
        match *self {
            Error => 120,
            Fatal => 200,
            Panic => 255,
            Unknown(code) => code,
        }
    }
}

impl MessageSeverity {
    fn from_u8(code: u8) -> MessageSeverity {
        use MessageSeverity::*;
        match code {
            20 => Debug,
            40 => Info,
            60 => Notice,
            80 => Warning,
            _ => Unknown(code),
        }
    }
    fn to_u8(&self) -> u8 {
        use MessageSeverity::*;
        match *self {
            Debug => 20,
            Info => 40,
            Notice => 60,
            Warning => 80,
            Unknown(code) => code,
        }
    }
}

impl Encode for ServerKeyData {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        buf.extend(&self.data[..]);
        Ok(())
    }
}
impl Decode for ServerKeyData {
    fn decode(buf: &mut Cursor<Bytes>)
        -> Result<ServerKeyData, DecodeError>
    {
        ensure!(buf.remaining() >= 32, errors::Underflow);
        let mut data = [0u8; 32];
        buf.copy_to_slice(&mut data[..]);
        Ok(ServerKeyData { data })
    }
}

impl Encode for ParameterStatus {
    fn encode(&self, buf: &mut BytesMut) -> Result<(), EncodeError> {
        self.name.encode(buf)?;
        self.value.encode(buf)?;
        Ok(())
    }
}
impl Decode for ParameterStatus {
    fn decode(buf: &mut Cursor<Bytes>)
        -> Result<ParameterStatus, DecodeError>
    {
        let name = Bytes::decode(buf)?;
        let value = Bytes::decode(buf)?;
        Ok(ParameterStatus { name, value })
    }
}

impl Encode for CommandComplete {
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
        self.status_data.encode(buf)?;
        Ok(())
    }
}

impl Decode for CommandComplete {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 6, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        let status_data = Bytes::decode(buf)?;
        Ok(CommandComplete { status_data, headers })
    }
}

impl Encode for PrepareComplete {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(35);
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        buf.reserve(33);
        buf.put_u8(self.cardinality as u8);
        self.input_typedesc_id.encode(buf)?;
        self.output_typedesc_id.encode(buf)?;
        Ok(())
    }
}

impl Decode for PrepareComplete {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 35, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 33, errors::Underflow);
        let cardinality = match buf.get_u8() {
            0x6e => Cardinality::NoResult,
            0x6f => Cardinality::One,
            0x6d => Cardinality::Many,
            c => errors::InvalidCardinality { cardinality: c }.fail()?,
        };
        let input_typedesc_id = Uuid::decode(buf)?;
        let output_typedesc_id = Uuid::decode(buf)?;
        Ok(PrepareComplete {
            headers,
            cardinality,
            input_typedesc_id,
            output_typedesc_id,
        })
    }
}

impl Encode for CommandDataDescription {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(43);
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        buf.reserve(41);
        buf.put_u8(self.result_cardinality as u8);
        self.input_typedesc_id.encode(buf)?;
        self.input_typedesc.encode(buf)?;
        self.output_typedesc_id.encode(buf)?;
        self.output_typedesc.encode(buf)?;
        Ok(())
    }
}

impl Decode for CommandDataDescription {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 43, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 41, errors::Underflow);
        let result_cardinality = match buf.get_u8() {
            0x6e => Cardinality::NoResult,
            0x6f => Cardinality::One,
            0x6d => Cardinality::Many,
            c => errors::InvalidCardinality { cardinality: c }.fail()?,
        };

        let input_typedesc_id = Uuid::decode(buf)?;
        let input_typedesc = Bytes::decode(buf)?;
        let output_typedesc_id = Uuid::decode(buf)?;
        let output_typedesc = Bytes::decode(buf)?;

        Ok(CommandDataDescription {
            headers,
            result_cardinality,
            input_typedesc_id,
            input_typedesc,
            output_typedesc_id,
            output_typedesc,
        })
    }
}

impl Encode for Data {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(2);
        buf.put_u16(u16::try_from(self.data.len()).ok()
            .context(errors::TooManyHeaders)?);
        for chunk in &self.data {
            chunk.encode(buf)?;
        }
        Ok(())
    }
}

impl Decode for Data {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let num_chunks = buf.get_u16() as usize;
        let mut data = Vec::with_capacity(num_chunks);
        for _ in 0..num_chunks {
            data.push(Bytes::decode(buf)?);
        }
        return Ok(Data { data })
    }
}

impl Encode for RestoreReady {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.reserve(4);
        buf.put_u16(u16::try_from(self.headers.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (&name, value) in &self.headers {
            buf.reserve(2);
            buf.put_u16(name);
            value.encode(buf)?;
        }
        buf.reserve(2);
        buf.put_u16(self.jobs);
        Ok(())
    }
}

impl Decode for RestoreReady {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 4, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 2, errors::Underflow);
        let jobs = buf.get_u16();
        return Ok(RestoreReady { jobs, headers })
    }
}

impl Encode for RawPacket {
    fn encode(&self, buf: &mut BytesMut)
        -> Result<(), EncodeError>
    {
        buf.extend(&self.data);
        Ok(())
    }
}

impl Decode for RawPacket {
    fn decode(buf: &mut Cursor<Bytes>) -> Result<Self, DecodeError> {
        let buf_pos = buf.position() as usize;
        let data = buf.get_ref().slice(buf_pos..);
        buf.advance(data.len());
        return Ok(RawPacket { data })
    }
}
