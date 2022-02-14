use crate::errors;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Cardinality {
    NoResult = 0x6e,
    AtMostOne = 0x6f,
    One = 0x41,
    Many = 0x6d,
    AtLeastOne = 0x4d,
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
