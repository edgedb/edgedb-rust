use crate::errors;

pub use crate::client_message::IoFormat;


#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Cardinality {
    NoResult = 0x6e,
    AtMostOne = 0x6f,
    One = 0x41,
    Many = 0x6d,
    AtLeastOne = 0x4d,
}

bitflags::bitflags! {
    pub struct Capabilities: u64 {
        const MODIFICATIONS =       0b00000001;
        const SESSION_CONFIG =      0b00000010;
        const TRANSACTION =         0b00000100;
        const DDL =                 0b00001000;
        const PERSISTENT_CONFIG =   0b00010000;
    }
}

pub struct CompilationFlags {
    pub implicit_limit: Option<u64>,
    pub implicit_typenames: bool,
    pub implicit_typeids: bool,
    pub allow_capabilities: Capabilities,
    pub explicit_objectids: bool,
    pub io_format: IoFormat,
    pub expected_cardinality: Cardinality,
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
