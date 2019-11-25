use std::collections::HashMap;
use std::error::Error;

use bytes::{Bytes, BytesMut};

use edgedb_protocol::client_message::{ClientMessage, ClientHandshake};
use edgedb_protocol::client_message::{ExecuteScript, Execute};
use edgedb_protocol::client_message::{Prepare, IoFormat, Cardinality};
use edgedb_protocol::client_message::{DescribeStatement, DescribeAspect};

macro_rules! encoding_eq {
    ($message: expr, $bytes: expr) => {
        let data: &[u8] = $bytes;
        let mut bytes = BytesMut::new();
        $message.encode(&mut bytes)?;
        println!("Serialized bytes {:?}", bytes);
        let bytes = bytes.freeze();
        assert_eq!(&bytes[..], data);
        assert_eq!(ClientMessage::decode(&data.into())?, $message);
    }
}

#[test]
fn client_handshake() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ClientMessage::ClientHandshake(ClientHandshake {
        major_ver: 1,
        minor_ver: 2,
        params: HashMap::new(),
        extensions: HashMap::new(),
    }), b"\x56\x00\x00\x00\x0C\x00\x01\x00\x02\x00\x00\x00\x00");
    Ok(())
}

#[test]
fn execute_script() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ClientMessage::ExecuteScript(ExecuteScript {
        headers: HashMap::new(),
        script_text: String::from("START TRANSACTION"),
    }), b"Q\0\0\0\x1b\0\0\0\0\0\x11START TRANSACTION");
    Ok(())
}

#[test]
fn prepare() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ClientMessage::Prepare(Prepare {
        headers: HashMap::new(),
        io_format: IoFormat::Binary,
        expected_cardinality: Cardinality::One,
        statement_name: Bytes::from_static(b"example"),
        command_text: String::from("SELECT 1;"),
    }), b"P\0\0\0 \0\0bo\0\0\0\x07example\0\0\0\tSELECT 1;");
    Ok(())
}

#[test]
fn describe_statement() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ClientMessage::DescribeStatement(DescribeStatement {
        headers: HashMap::new(),
        aspect: DescribeAspect::DataDescription,
        statement_name: Bytes::from_static(b"example"),
    }), b"D\0\0\0\x12\0\0T\0\0\0\x07example");
    Ok(())
}

#[test]
fn execute() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ClientMessage::Execute(Execute {
        headers: HashMap::new(),
        statement_name: Bytes::from_static(b"example"),
        arguments: Bytes::new(),
    }), b"E\0\0\0\x15\0\0\0\0\0\x07example\0\0\0\0");
    Ok(())
}

#[test]
fn sync() -> Result<(), Box<dyn Error>> {
    encoding_eq!(ClientMessage::Sync, b"S\0\0\0\x04");
    Ok(())
}
