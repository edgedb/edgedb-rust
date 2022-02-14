mod error;
mod traits;

pub mod display;
pub mod kinds;

pub use error::{Error, Tag};
pub use kinds::*;
pub use traits::{ErrorKind, ResultExt};
