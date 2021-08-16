mod error;
mod traits;

pub mod kinds;

pub use traits::{ErrorKind, ResultExt};
pub use error::{Error, Tag};
pub use kinds::*;
