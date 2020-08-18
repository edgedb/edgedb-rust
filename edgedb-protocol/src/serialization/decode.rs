mod raw_scalar;
mod raw_composite;

use snafu::ensure;
use crate::errors::{self, DecodeError};

pub use self::raw_composite::DecodeTupleLike;
pub(crate) use self::raw_scalar::RawCodec;
pub(crate) use self::raw_composite::{DecodeArrayLike, DecodeInputTuple};

pub(crate) fn required_element(buf:Option<&[u8]>) -> Result<&[u8], DecodeError> {
    ensure!(buf.is_some(), errors::MissingRequiredElement);
    Ok(buf.unwrap())
}