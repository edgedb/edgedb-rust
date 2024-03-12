/*!
([Website reference](https://www.edgedb.com/docs/reference/protocol/messages#parse)) Capabilities, CompilationFlags etc. from the message protocol.
*/

use crate::errors;
use crate::model::Uuid;
use bytes::Bytes;

use crate::descriptors::Typedesc;
use crate::encoding::Input;
use crate::errors::DecodeError;
use crate::features::ProtocolVersion;

pub use crate::client_message::IoFormat;


#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Cardinality {
    NoResult = 0x6e,
    AtMostOne = 0x6f,
    One = 0x41,
    Many = 0x6d,
    AtLeastOne = 0x4d,
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Capabilities: u64 {
        const MODIFICATIONS =       0b00000001;
        const SESSION_CONFIG =      0b00000010;
        const TRANSACTION =         0b00000100;
        const DDL =                 0b00001000;
        const PERSISTENT_CONFIG =   0b00010000;
        const ALL =                 0b00011111;
    }
}

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct CompilationFlags: u64 {
        const INJECT_OUTPUT_TYPE_IDS =       0b00000001;
        const INJECT_OUTPUT_TYPE_NAMES =     0b00000010;
        const INJECT_OUTPUT_OBJECT_IDS =     0b00000100;
    }
}

#[derive(Debug, Clone)]
pub struct CompilationOptions {
    pub implicit_limit: Option<u64>,
    pub implicit_typenames: bool,
    pub implicit_typeids: bool,
    pub allow_capabilities: Capabilities,
    pub explicit_objectids: bool,
    pub io_format: IoFormat,
    pub expected_cardinality: Cardinality,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State {
    pub typedesc_id: Uuid,
    pub data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawTypedesc {
    pub proto: ProtocolVersion,
    pub id: Uuid,
    pub data: Bytes,
}

impl RawTypedesc {
    pub fn uninitialized() -> RawTypedesc {
        RawTypedesc {
            proto: ProtocolVersion::current(),
            id: Uuid::from_u128(0),
            data: Bytes::new(),
        }
    }
    pub fn decode(&self) -> Result<Typedesc, DecodeError> {
        let ref mut cur = Input::new(
            self.proto.clone(),
            self.data.clone(),
        );
        Typedesc::decode_with_id(self.id, cur)
    }
}

impl std::convert::TryFrom<u8> for Cardinality {
    type Error = errors::DecodeError;
    fn try_from(cardinality: u8) -> Result<Cardinality, errors::DecodeError> {
        match cardinality {
            0x6e => Ok(Cardinality::NoResult),
            0x6f => Ok(Cardinality::AtMostOne),
            0x41 => Ok(Cardinality::One),
            0x6d => Ok(Cardinality::Many),
            0x4d => Ok(Cardinality::AtLeastOne),
            _ => Err(errors::InvalidCardinality { cardinality }.build()),
        }
    }
}

impl Cardinality {
    pub fn is_optional(&self) -> bool {
        use Cardinality::*;
        match self {
            NoResult => true,
            AtMostOne => true,
            One => false,
            Many => true,
            AtLeastOne => false,
        }
    }
}

impl State {
    pub fn empty() -> State {
        State {
            typedesc_id: Uuid::from_u128(0),
            data: Bytes::new(),
        }
    }
    pub fn descriptor_id(&self) -> Uuid {
        self.typedesc_id
    }
}

impl CompilationOptions {
    pub fn flags(&self) -> CompilationFlags {
        let mut cflags = CompilationFlags::empty();
        if self.implicit_typenames {
            cflags |= CompilationFlags::INJECT_OUTPUT_TYPE_NAMES;
        }
        if self.implicit_typeids {
            cflags |= CompilationFlags::INJECT_OUTPUT_TYPE_IDS;
        }
        // TODO(tailhook) object ids
        return cflags;
    }
}
