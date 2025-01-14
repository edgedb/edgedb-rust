/*!
# Error Handling for EdgeDB

All errors that EdgeDB Rust bindings produce are encapsulated into the
[`Error`] structure. The structure is a bit like `Box<dyn Error>` or
[`anyhow::Error`], except it can only contain EdgeDB error types. Or
[`UserError`] can be used to encapsulate custom errors (commonly used
to return an error from a transaction).

A full list of EdgeDB error types on a single page can be found on the [website documentation](https://www.edgedb.com/docs/reference/protocol/errors#error-codes).

Each error kind is represented as a separate type that implements the
[`ErrorKind`] trait. But error kinds are used like marker structs; you can
use [`Error::is`] for error kinds and use them to create instances of the
error:

```rust
# use std::io;
# use gel_errors::{UserError, ErrorKind};
let err = UserError::with_source(io::Error::from(io::ErrorKind::NotFound));
assert!(err.is::<UserError>());
```

Since errors are hirarchical, [`Error::is`] works with any ancestor:

```rust
# use gel_errors::*;
# let err = MissingArgumentError::with_message("test error");
assert!(err.is::<MissingArgumentError>());
assert!(err.is::<QueryArgumentError>());  // implied by the assertion above
assert!(err.is::<InterfaceError>());  // and this one
assert!(err.is::<ClientError>());  // and this one
```

Error hierarchy doesn't have multiple inheritance (i.e. every error has only
single parent). When we match across different parents we use error tags:

```rust
# use gel_errors::*;
# let err1 = ClientConnectionTimeoutError::with_message("test error");
# let err2 = TransactionConflictError::with_message("test error");

assert!(err1.is::<ClientConnectionTimeoutError>());
assert!(err2.is::<TransactionConflictError>());
// Both of these are retried
assert!(err1.has_tag(SHOULD_RETRY));
assert!(err2.has_tag(SHOULD_RETRY));

// But they aren't a part of common hierarchy
assert!(err1.is::<ClientError>());
assert!(!err1.is::<ExecutionError>());
assert!(err2.is::<ExecutionError>());
assert!(!err2.is::<ClientError>());
```

[`anyhow::Error`]: https://docs.rs/anyhow/latest/anyhow/struct.Error.html

# Errors in Transactions

Special care for errors must be taken in transactions. Generally:

1. Errors from queries should not be ignored, and should be propagagated
   up to the transaction function.
2. User errors can be encapsulated into [`UserError`] via one of the
   methods:
    * [`ErrorKind::with_source`] (for any [`std::error::Error`])
    * [`ErrorKind::with_source_box`] already boxed error
    * [`ErrorKind::with_source_ref`] for smart wrappers such as
      [`anyhow::Error`]
3. Original query error must be propagated via error chain. It can be in the
   `.source()` chain but must not be swallowed, otherwise retrying
   transaction may work incorrectly.

# Nice Error Reporting

Refer to documentation in the [gel-tokio](https://docs.rs/gel-tokio) crate.
*/
mod error;
mod traits;

pub mod display;
pub mod fields;
pub mod kinds;

#[cfg(feature = "miette")]
pub mod miette;

pub use error::{Error, Tag};
pub use kinds::*;
pub use traits::{ErrorKind, Field, ResultExt};
