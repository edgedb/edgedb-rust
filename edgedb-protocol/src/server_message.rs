use std::collections::HashMap;
use std::u32;
use std::u16;
use std::convert::{TryFrom, TryInto};

use bytes::{Bytes, BufMut, Buf};
use uuid::Uuid;
use snafu::{OptionExt, ensure};

use crate::features::ProtocolVersion;
use crate::errors::{self, EncodeError, DecodeError};
use crate::encoding::{Input, Output, KeyValues, Annotations, Decode, Encode};
use crate::descriptors::Typedesc;
pub use crate::common::{Cardinality, State, RawTypedesc};
use crate::common::Capabilities;


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
    CommandComplete0(CommandComplete0),
    CommandComplete1(CommandComplete1),
    PrepareComplete(PrepareComplete),
    CommandDataDescription0(CommandDataDescription0), // protocol < 1.0
    CommandDataDescription1(CommandDataDescription1), // protocol >= 1.0
    StateDataDescription(StateDataDescription),
    Data(Data),
    RestoreReady(RestoreReady),
    // Don't decode Dump packets here as we only need to process them as
    // whole
    DumpHeader(RawPacket),
    DumpBlock(RawPacket),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyForCommand {
    pub headers: KeyValues,
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
    pub attributes: KeyValues,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogMessage {
    pub severity: MessageSeverity,
    pub code: u32,
    pub text: String,
    pub attributes: KeyValues,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerHandshake {
    pub major_ver: u16,
    pub minor_ver: u16,
    pub extensions: HashMap<String, KeyValues>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerKeyData {
    pub data: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParameterStatus {
    pub proto: ProtocolVersion,
    pub name: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandComplete0 {
    pub headers: KeyValues,
    pub status_data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandComplete1 {
    pub annotations: Annotations,
    pub capabilities: Capabilities,
    pub status_data: Bytes,
    pub state: Option<State>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrepareComplete {
    pub headers: KeyValues,
    pub cardinality: Cardinality,
    pub input_typedesc_id: Uuid,
    pub output_typedesc_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseComplete {
    pub headers: KeyValues,
    pub cardinality: Cardinality,
    pub input_typedesc_id: Uuid,
    pub output_typedesc_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandDataDescription0 {
    pub headers: KeyValues,
    pub result_cardinality: Cardinality,
    pub input: RawTypedesc,
    pub output: RawTypedesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandDataDescription1 {
    pub annotations: Annotations,
    pub capabilities: Capabilities,
    pub result_cardinality: Cardinality,
    pub input: RawTypedesc,
    pub output: RawTypedesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateDataDescription {
    pub typedesc: RawTypedesc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Data {
    pub data: Vec<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReady {
    pub headers: KeyValues,
    pub jobs: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawPacket {
    pub data: Bytes,
}

fn encode<T: Encode>(buf: &mut Output, code: u8, msg: &T)
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

impl CommandDataDescription0 {
    pub fn output(&self) -> Result<Typedesc, DecodeError> {
        self.output.decode()
    }
    pub fn input(&self) -> Result<Typedesc, DecodeError> {
        self.input.decode()
    }
}

impl CommandDataDescription1 {
    pub fn output(&self) -> Result<Typedesc, DecodeError> {
        self.output.decode()
    }
    pub fn input(&self) -> Result<Typedesc, DecodeError> {
        self.input.decode()
    }
}

impl StateDataDescription {
    pub fn parse(self) -> Result<Typedesc, DecodeError> {
        self.typedesc.decode()
    }
}

impl ParameterStatus {
    pub fn parse_system_config(self) -> Result<(Typedesc, Bytes), DecodeError> {
        let ref mut cur = Input::new(
            self.proto.clone(),
            self.value,
        );
        let typedesc_data = Bytes::decode(cur)?;
        let data = Bytes::decode(cur)?;

        let ref mut typedesc_buf = Input::new(
            self.proto,
            typedesc_data,
        );
        let typedesc_id = Uuid::decode(typedesc_buf)?.into();
        let typedesc = Typedesc::decode_with_id(typedesc_id, typedesc_buf)?;
        Ok((typedesc, data))
    }
}

impl ServerMessage {
    pub fn encode(&self, buf: &mut Output) -> Result<(), EncodeError> {
        use ServerMessage::*;
        match self {
            ServerHandshake(h) => encode(buf, 0x76, h),
            ErrorResponse(h) => encode(buf, 0x45, h),
            LogMessage(h) => encode(buf, 0x4c, h),
            Authentication(h) => encode(buf, 0x52, h),
            ReadyForCommand(h) => encode(buf, 0x5a, h),
            ServerKeyData(h) => encode(buf, 0x4b, h),
            ParameterStatus(h) => encode(buf, 0x53, h),
            CommandComplete0(h) => encode(buf, 0x43, h),
            CommandComplete1(h) => encode(buf, 0x43, h),
            PrepareComplete(h) => encode(buf, 0x31, h),
            CommandDataDescription0(h) => encode(buf, 0x54, h),
            CommandDataDescription1(h) => encode(buf, 0x54, h),
            StateDataDescription(h) => encode(buf, 0x73, h),
            Data(h) => encode(buf, 0x44, h),
            RestoreReady(h) => encode(buf, 0x2b, h),
            DumpHeader(h) => encode(buf, 0x40, h),
            DumpBlock(h) => encode(buf, 0x3d, h),

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
    pub fn decode(buf: &mut Input) -> Result<ServerMessage, DecodeError> {
        use self::ServerMessage as M;
        let ref mut data = buf.slice(5..);
        let result = match buf[0] {
            0x76 => ServerHandshake::decode(data).map(M::ServerHandshake)?,
            0x45 => ErrorResponse::decode(data).map(M::ErrorResponse)?,
            0x4c => LogMessage::decode(data).map(M::LogMessage)?,
            0x52 => Authentication::decode(data).map(M::Authentication)?,
            0x5a => ReadyForCommand::decode(data).map(M::ReadyForCommand)?,
            0x4b => ServerKeyData::decode(data).map(M::ServerKeyData)?,
            0x53 => ParameterStatus::decode(data).map(M::ParameterStatus)?,
            0x43 => if buf.proto().is_1() {
                CommandComplete1::decode(data).map(M::CommandComplete1)?
            } else {
                CommandComplete0::decode(data).map(M::CommandComplete0)?
            },
            0x31 => PrepareComplete::decode(data).map(M::PrepareComplete)?,
            0x44 => Data::decode(data).map(M::Data)?,
            0x2b => RestoreReady::decode(data).map(M::RestoreReady)?,
            0x40 => RawPacket::decode(data).map(M::DumpHeader)?,
            0x3d => RawPacket::decode(data).map(M::DumpBlock)?,
            0x54 => if buf.proto().is_1() {
                CommandDataDescription1::decode(data)
                .map(M::CommandDataDescription1)?
            } else {
                CommandDataDescription0::decode(data)
                .map(M::CommandDataDescription0)?
            }
            0x73 => {
                StateDataDescription::decode(data)
                .map(M::StateDataDescription)?
            }
            code => {
                M::UnknownMessage(
                    code,
                    data.copy_to_bytes(data.remaining())
                )
            }
        };
        ensure!(data.remaining() == 0, errors::ExtraData);
        Ok(result)
    }
}

impl Encode for ServerHandshake {
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<ErrorResponse, DecodeError> {
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
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<LogMessage, DecodeError> {
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
    fn encode(&self, buf: &mut Output) -> Result<(), EncodeError> {
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
    fn decode(buf: &mut Input) -> Result<Authentication, DecodeError> {
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
    fn encode(&self, buf: &mut Output) -> Result<(), EncodeError> {
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
    fn decode(buf: &mut Input)
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
    fn encode(&self, buf: &mut Output) -> Result<(), EncodeError> {
        buf.extend(&self.data[..]);
        Ok(())
    }
}
impl Decode for ServerKeyData {
    fn decode(buf: &mut Input)
        -> Result<ServerKeyData, DecodeError>
    {
        ensure!(buf.remaining() >= 32, errors::Underflow);
        let mut data = [0u8; 32];
        buf.copy_to_slice(&mut data[..]);
        Ok(ServerKeyData { data })
    }
}

impl Encode for ParameterStatus {
    fn encode(&self, buf: &mut Output) -> Result<(), EncodeError> {
        self.name.encode(buf)?;
        self.value.encode(buf)?;
        Ok(())
    }
}
impl Decode for ParameterStatus {
    fn decode(buf: &mut Input)
        -> Result<ParameterStatus, DecodeError>
    {
        let name = Bytes::decode(buf)?;
        let value = Bytes::decode(buf)?;
        Ok(ParameterStatus { proto: buf.proto().clone(), name, value })
    }
}

impl Encode for CommandComplete0 {
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
        self.status_data.encode(buf)?;
        Ok(())
    }
}

impl Decode for CommandComplete0 {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 6, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        let status_data = Bytes::decode(buf)?;
        Ok(CommandComplete0 { status_data, headers })
    }
}

impl Encode for CommandComplete1 {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        buf.reserve(26);
        buf.put_u16(u16::try_from(self.annotations.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (name, value) in &self.annotations {
            name.encode(buf)?;
            value.encode(buf)?;
        }
        buf.put_u64(self.capabilities.bits());
        self.status_data.encode(buf)?;
        if let Some(state) = &self.state {
            state.typedesc_id.encode(buf)?;
            state.data.encode(buf)?;
        } else {
            Uuid::from_u128(0).encode(buf)?;
            Bytes::new().encode(buf)?;
        }
        Ok(())
    }
}

impl Decode for CommandComplete1 {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 26, errors::Underflow);
        let num_annotations = buf.get_u16();
        let mut annotations = HashMap::new();
        for _ in 0..num_annotations {
            annotations.insert(String::decode(buf)?, String::decode(buf)?);
        }
        let capabilities = Capabilities::from_bits_retain(buf.get_u64());
        let status_data = Bytes::decode(buf)?;
        let typedesc_id = Uuid::decode(buf)?;
        let state_data = Bytes::decode(buf)?;
        let state = if typedesc_id == Uuid::from_u128(0) {
            None
        } else {
            Some(State {
                typedesc_id,
                data: state_data,
            })
        };
        Ok(CommandComplete1 {
            annotations,
            capabilities,
            status_data,
            state,
        })
    }
}

impl Encode for PrepareComplete {
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 35, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 33, errors::Underflow);
        let cardinality = TryFrom::try_from(buf.get_u8())?;
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

impl Encode for CommandDataDescription0 {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        debug_assert!(!buf.proto().is_1());
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
        self.input.id.encode(buf)?;
        self.input.data.encode(buf)?;
        self.output.id.encode(buf)?;
        self.output.data.encode(buf)?;
        Ok(())
    }
}

impl Decode for CommandDataDescription0 {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 43, errors::Underflow);
        let num_headers = buf.get_u16();
        let mut headers = HashMap::new();
        for _ in 0..num_headers {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            headers.insert(buf.get_u16(), Bytes::decode(buf)?);
        }
        ensure!(buf.remaining() >= 41, errors::Underflow);
        let result_cardinality = TryFrom::try_from(buf.get_u8())?;
        let input = RawTypedesc {
            proto: buf.proto().clone(),
            id: Uuid::decode(buf)?,
            data: Bytes::decode(buf)?,
        };
        let output = RawTypedesc {
            proto: buf.proto().clone(),
            id: Uuid::decode(buf)?,
            data: Bytes::decode(buf)?,
        };

        Ok(CommandDataDescription0 {
            headers,
            result_cardinality,
            input,
            output,
        })
    }
}

impl Encode for CommandDataDescription1 {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        debug_assert!(buf.proto().is_1());
        buf.reserve(51);
        buf.put_u16(u16::try_from(self.annotations.len()).ok()
            .context(errors::TooManyHeaders)?);
        for (name, value) in &self.annotations {
            buf.reserve(4);
            name.encode(buf)?;
            value.encode(buf)?;
        }
        buf.reserve(49);
        buf.put_u64(self.capabilities.bits());
        buf.put_u8(self.result_cardinality as u8);
        self.input.id.encode(buf)?;
        self.input.data.encode(buf)?;
        self.output.id.encode(buf)?;
        self.output.data.encode(buf)?;
        Ok(())
    }
}

impl Decode for CommandDataDescription1 {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        ensure!(buf.remaining() >= 51, errors::Underflow);
        let num_annotations = buf.get_u16();
        let mut annotations = HashMap::new();
        for _ in 0..num_annotations {
            ensure!(buf.remaining() >= 4, errors::Underflow);
            annotations.insert(String::decode(buf)?, String::decode(buf)?);
        }
        ensure!(buf.remaining() >= 49, errors::Underflow);
        let capabilities = Capabilities::from_bits_retain(buf.get_u64());
        let result_cardinality = TryFrom::try_from(buf.get_u8())?;
        let input = RawTypedesc {
            proto: buf.proto().clone(),
            id: Uuid::decode(buf)?,
            data: Bytes::decode(buf)?,
        };
        let output = RawTypedesc {
            proto: buf.proto().clone(),
            id: Uuid::decode(buf)?,
            data: Bytes::decode(buf)?,
        };

        Ok(CommandDataDescription1 {
            annotations,
            capabilities,
            result_cardinality,
            input,
            output,
        })
    }
}

impl Encode for StateDataDescription {
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        debug_assert!(buf.proto().is_1());
        self.typedesc.id.encode(buf)?;
        self.typedesc.data.encode(buf)?;
        Ok(())
    }
}

impl Decode for StateDataDescription {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        let typedesc = RawTypedesc {
            proto: buf.proto().clone(),
            id: Uuid::decode(buf)?,
            data: Bytes::decode(buf)?,
        };

        Ok(StateDataDescription {
            typedesc,
        })
    }
}

impl Encode for Data {
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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
    fn encode(&self, buf: &mut Output)
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
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
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
    fn encode(&self, buf: &mut Output)
        -> Result<(), EncodeError>
    {
        buf.extend(&self.data);
        Ok(())
    }
}

impl Decode for RawPacket {
    fn decode(buf: &mut Input) -> Result<Self, DecodeError> {
        return Ok(RawPacket { data: buf.copy_to_bytes(buf.remaining()) })
    }
}

impl PrepareComplete {
    pub fn get_capabilities(&self) -> Option<Capabilities> {
        self.headers.get(&0x1001).and_then(|bytes| {
            if bytes.len() == 8 {
                let mut array = [0u8; 8];
                array.copy_from_slice(bytes);
                Some(Capabilities::from_bits_retain(u64::from_be_bytes(array)))
            } else {
                None
            }
        })
    }
}
