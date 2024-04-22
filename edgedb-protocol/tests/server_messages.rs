use std::collections::HashMap;
use std::error::Error;
use std::fs;

use bytes::{Bytes, BytesMut};
use uuid::Uuid;

use edgedb_protocol::common::{Capabilities, RawTypedesc};
use edgedb_protocol::encoding::{Input, Output};
use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::server_message::Authentication;
use edgedb_protocol::server_message::CommandDataDescription1;
use edgedb_protocol::server_message::RestoreReady;
use edgedb_protocol::server_message::ServerHandshake;
use edgedb_protocol::server_message::ServerMessage;
use edgedb_protocol::server_message::StateDataDescription;
use edgedb_protocol::server_message::{Cardinality, PrepareComplete};
use edgedb_protocol::server_message::{CommandComplete0, CommandComplete1};
use edgedb_protocol::server_message::{CommandDataDescription0, Data};
use edgedb_protocol::server_message::{ErrorResponse, ErrorSeverity};
use edgedb_protocol::server_message::{LogMessage, MessageSeverity};
use edgedb_protocol::server_message::{ParameterStatus, ServerKeyData};
use edgedb_protocol::server_message::{ReadyForCommand, TransactionState};

mod base;

macro_rules! encoding_eq_ver {
    ($major: expr, $minor: expr, $message: expr, $bytes: expr) => {
        let proto = ProtocolVersion::new($major, $minor);
        let data: &[u8] = $bytes;
        let mut bytes = BytesMut::new();
        $message.encode(&mut Output::new(&proto, &mut bytes))?;
        println!("Serialized bytes {:?}", bytes);
        let bytes = bytes.freeze();
        assert_eq!(&bytes[..], data);
        assert_eq!(
            ServerMessage::decode(&mut Input::new(proto, Bytes::copy_from_slice(data)))?,
            $message,
        );
    };
}

macro_rules! encoding_eq {
    ($message: expr, $bytes: expr) => {
        let (major, minor) = ProtocolVersion::current().version_tuple();
        encoding_eq_ver!(major, minor, $message, $bytes);
    };
}

macro_rules! map {
    ($($key:expr => $value:expr),*) => {
        {
            #[allow(unused_mut)]
            let mut h = HashMap::new();
            $(
                h.insert($key, $value);
            )*
            h
        }
    }
}

#[test]
fn server_handshake() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::ServerHandshake(ServerHandshake {
            major_ver: 1,
            minor_ver: 0,
            extensions: HashMap::new(),
        }),
        b"v\0\0\0\n\0\x01\0\0\0\0"
    );
    Ok(())
}

#[test]
fn ready_for_command() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::ReadyForCommand(ReadyForCommand {
            transaction_state: TransactionState::NotInTransaction,
            headers: HashMap::new(),
        }),
        b"Z\0\0\0\x07\0\0I"
    );
    Ok(())
}

#[test]
fn error_response() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::ErrorResponse(ErrorResponse {
            severity: ErrorSeverity::Error,
            code: 50397184,
            message: String::from(
                "missing required connection parameter \
                               in ClientHandshake message: \"user\""
            ),
            attributes: map! {
                257 => Bytes::from_static("Traceback (most recent call last):\n  File \"edb/server/mng_port/edgecon.pyx\", line 1077, in edb.server.mng_port.edgecon.EdgeConnection.main\n    await self.auth()\n  File \"edb/server/mng_port/edgecon.pyx\", line 178, in auth\n    raise errors.BinaryProtocolError(\nedb.errors.BinaryProtocolError: missing required connection parameter in ClientHandshake message: \"user\"\n".as_bytes())
            },
        }),
        &fs::read("tests/error_response.bin")?[..]
    );
    Ok(())
}

#[test]
fn server_key_data() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::ServerKeyData(ServerKeyData { data: [0u8; 32] }),
        &fs::read("tests/server_key_data.bin")?[..]
    );
    Ok(())
}

#[test]
fn parameter_status() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::ParameterStatus(ParameterStatus {
            proto: ProtocolVersion::current(),
            name: Bytes::from_static(b"pgaddr"),
            value: Bytes::from_static(b"/work/tmp/db/.s.PGSQL.60128"),
        }),
        &fs::read("tests/parameter_status.bin")?[..]
    );
    Ok(())
}

#[test]
fn command_complete0() -> Result<(), Box<dyn Error>> {
    encoding_eq_ver!(
        0,
        13,
        ServerMessage::CommandComplete0(CommandComplete0 {
            headers: HashMap::new(),
            status_data: Bytes::from_static(b"okay"),
        }),
        b"C\0\0\0\x0e\0\0\0\0\0\x04okay"
    );
    Ok(())
}

#[test]
fn command_complete1() -> Result<(), Box<dyn Error>> {
    encoding_eq_ver!(
        1,
        0,
        ServerMessage::CommandComplete1(CommandComplete1 {
            annotations: HashMap::new(),
            capabilities: Capabilities::MODIFICATIONS,
            status_data: Bytes::from_static(b"okay"),
            state: None,
        }),
        b"C\0\0\0*\0\0\0\0\0\0\0\0\0\x01\0\0\0\x04okay\
          \0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\
          \0\0\0\0"
    );
    Ok(())
}

#[test]
fn prepare_complete() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::PrepareComplete(PrepareComplete {
            headers: HashMap::new(),
            cardinality: Cardinality::AtMostOne,
            input_typedesc_id: Uuid::from_u128(0xFF),
            output_typedesc_id: Uuid::from_u128(0x105),
        }),
        b"1\0\0\0'\0\0o\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"
    );
    encoding_eq!(
        ServerMessage::PrepareComplete(PrepareComplete {
            headers: HashMap::new(),
            cardinality: Cardinality::NoResult,
            input_typedesc_id: Uuid::from_u128(0xFF),
            output_typedesc_id: Uuid::from_u128(0x0),
        }),
        b"1\0\0\0'\0\0n\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"
    );
    Ok(())
}

#[test]
fn command_data_description0() -> Result<(), Box<dyn Error>> {
    encoding_eq_ver!(
        0,
        13,
        ServerMessage::CommandDataDescription0(CommandDataDescription0 {
            headers: HashMap::new(),
            result_cardinality: Cardinality::AtMostOne,
            input: RawTypedesc {
                proto: ProtocolVersion::new(0, 13),
                id: Uuid::from_u128(0xFF),
                data: Bytes::from_static(b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0"),
            },
            output: RawTypedesc {
                proto: ProtocolVersion::new(0, 13),
                id: Uuid::from_u128(0x105),
                data: Bytes::from_static(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"),
            },
        }),
        bconcat!(b"T\0\0\0S\0\0o"
                     b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff"
                     b"\0\0\0\x13"
                     b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0"
                     b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"
                     b"\0\0\0\x11"
                     b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05")
    );
    encoding_eq_ver!(
        0,
        13,
        ServerMessage::CommandDataDescription0(CommandDataDescription0 {
            headers: HashMap::new(),
            result_cardinality: Cardinality::NoResult,
            input: RawTypedesc {
                proto: ProtocolVersion::new(0, 13),
                id: Uuid::from_u128(0xFF),
                data: Bytes::from_static(b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0"),
            },
            output: RawTypedesc {
                proto: ProtocolVersion::new(0, 13),
                id: Uuid::from_u128(0),
                data: Bytes::from_static(b""),
            },
        }),
        bconcat!(b"T\0\0\0B\0\0n"
                     b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff"
                     b"\0\0\0\x13"
                     b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0"
                     b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0")
    );
    Ok(())
}

#[test]
fn command_data_description1() -> Result<(), Box<dyn Error>> {
    encoding_eq_ver!(
        1,
        0,
        ServerMessage::CommandDataDescription1(CommandDataDescription1 {
            annotations: HashMap::new(),
            capabilities: Capabilities::MODIFICATIONS,
            result_cardinality: Cardinality::AtMostOne,
            input: RawTypedesc {
                proto: ProtocolVersion::current(),
                id: Uuid::from_u128(0xFF),
                data: Bytes::from_static(b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0"),
            },
            output: RawTypedesc {
                proto: ProtocolVersion::current(),
                id: Uuid::from_u128(0x105),
                data: Bytes::from_static(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"),
            },
        }),
        bconcat!(b"T\0\0\0[\0\0\0\0\0\0\0\0\0\x01o"
                     b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff"
                     b"\0\0\0\x13"
                     b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0"
                     b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"
                     b"\0\0\0\x11"
                     b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05")
    );
    encoding_eq_ver!(
        1,
        0,
        ServerMessage::CommandDataDescription1(CommandDataDescription1 {
            annotations: HashMap::new(),
            capabilities: Capabilities::MODIFICATIONS,
            result_cardinality: Cardinality::NoResult,
            input: RawTypedesc {
                proto: ProtocolVersion::current(),
                id: Uuid::from_u128(0xFF),
                data: Bytes::from_static(b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0\0"),
            },
            output: RawTypedesc {
                proto: ProtocolVersion::current(),
                id: Uuid::from_u128(0),
                data: Bytes::from_static(b""),
            },
        }),
        bconcat!(b"T\0\0\0J\0\0\0\0\0\0\0\0\0\x01n"
                     b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff"
                     b"\0\0\0\x13"
                     b"\x04\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\xff\0"
                     b"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0")
    );
    Ok(())
}

#[test]
fn data() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::Data(Data {
            data: vec![Bytes::from_static(b"\0\0\0\0\0\0\0\x01")],
        }),
        b"D\0\0\0\x12\0\x01\0\0\0\x08\0\0\0\0\0\0\0\x01"
    );
    Ok(())
}

#[test]
fn restore_ready() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::RestoreReady(RestoreReady {
            jobs: 1,
            headers: HashMap::new(),
        }),
        b"+\0\0\0\x08\0\0\0\x01"
    );
    Ok(())
}

#[test]
fn authentication() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::Authentication(Authentication::Ok),
        b"\x52\0\0\0\x08\x00\x00\x00\x00"
    );
    encoding_eq!(
        ServerMessage::Authentication(Authentication::Sasl {
            methods: vec![String::from("SCRAM-SHA-256")],
        }),
        b"R\0\0\0\x1d\0\0\0\n\0\0\0\x01\0\0\0\rSCRAM-SHA-256"
    );
    encoding_eq!(
        ServerMessage::Authentication(Authentication::SaslContinue {
            data: Bytes::from_static(b"sasl_interim_data"),
        }),
        b"R\0\0\0\x1d\x00\x00\x00\x0b\0\0\0\x11sasl_interim_data"
    );
    encoding_eq!(
        ServerMessage::Authentication(Authentication::SaslFinal {
            data: Bytes::from_static(b"sasl_final_data"),
        }),
        b"R\0\0\0\x1b\x00\x00\x00\x0c\0\0\0\x0fsasl_final_data"
    );
    Ok(())
}

#[test]
fn log_message() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::LogMessage(LogMessage {
            severity: MessageSeverity::Notice,
            code: 0xF0_00_00_00,
            text: "changing system config".into(),
            attributes: map! {},
        }),
        b"L\0\0\0%<\xf0\0\0\0\0\0\0\x16changing system config\0\0"
    );
    Ok(())
}

#[test]
fn state_data_description() -> Result<(), Box<dyn Error>> {
    encoding_eq!(
        ServerMessage::StateDataDescription(StateDataDescription {
            typedesc: RawTypedesc {
                proto: ProtocolVersion::current(),
                id: Uuid::from_u128(0x105),
                data: Bytes::from_static(b"\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"),
            },
        }),
        b"s\0\0\0)\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05\0\0\0\
        \x11\x02\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x01\x05"
    );
    Ok(())
}
