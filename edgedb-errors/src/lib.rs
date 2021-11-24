mod display;
mod error;
mod traits;

pub mod kinds;

pub use display::{display_error, display_error_verbose};
pub use traits::{ErrorKind, ResultExt};
pub use error::{Error, Tag};
pub use kinds::*;
