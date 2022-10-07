//! # Error Handling for EdgeDB
//!
//! All errors that EdgeDB Rust bindings produce are encapsulated into the
//! [`Error`] structure. The structure is a bit like `Box<dyn Error>` or
//! [`anyhow::Error`], except it can only contain EdgeDB error types. Or
//! [`UserError`] can be used to encapsulate custom errors (commonly used
//! to return error from the transaction).
//!
//! Each error kind is represented as a separate type that implements
//! [`ErrorKind`] trait. But error kinds are used like marker structs you can
//! use [`Error::is`] for error kinds and use them to create instances of the
//! error:
//!
//! ```rust
//! # use std::io;
//! # use edgedb_errors::{UserError, ErrorKind};
//! let err = UserError::with_source(io::Error::from(io::ErrorKind::NotFound));
//! assert!(err.is::<UserError>());
//! ```
//!
//! Since errors are hirarhical [`Error::is`] works with any ancestor:
//!
//! ```rust
//! # use edgedb_errors::*;
//! # let err = MissingArgumentError::with_message("test error");
//! assert!(err.is::<MissingArgumentError>());
//! assert!(err.is::<QueryArgumentError>());  // implied by the assertion above
//! assert!(err.is::<InterfaceError>());  // and this one
//! assert!(err.is::<ClientError>());  // and this one
//! ```
//!
//! Error hierarchy doesn't have multiple inheritance (i.e. every error has only
//! single parent). When we match across different parents we use error tags:
//!
//! ```rust
//! # use edgedb_errors::*;
//! # let err1 = ClientConnectionTimeoutError::with_message("test error");
//! # let err2 = TransactionConflictError::with_message("test error");
//!
//! assert!(err1.is::<ClientConnectionTimeoutError>());
//! assert!(err2.is::<TransactionConflictError>());
//! // Both of these are retried
//! assert!(err1.has_tag(SHOULD_RETRY));
//! assert!(err2.has_tag(SHOULD_RETRY));
//!
//! // But they aren't a part of common hierarchy
//! assert!(err1.is::<ClientError>());
//! assert!(!err1.is::<ExecutionError>());
//! assert!(err2.is::<ExecutionError>());
//! assert!(!err2.is::<ClientError>());
//! ```
//!
//! [`anyhow::Error`]: https://docs.rs/anyhow/latest/anyhow/struct.Error.html
//!
//! # Errors in Transactions
//!
//! Special care for errors must be taken in transactions. Generally:
//!
//! 1. Errors from queries should not be ignored, and should be propagagated
//!    up to the transaction function.
//! 2. User errors can be encapsulated into [`UserError`] via one of the
//!    methods:
//!     * [`ErrorKind::with_source`] (for any [`std::error::Error`])
//!     * [`ErrorKind::with_source_box`] already boxed error
//!     * [`ErrorKind::with_source_ref`] for smart wrappers such as
//!       [`anyhow::Error`]
//! 3. Original query error must be propagated via error chain. It can be in the
//!    `.source()` chain but must not be swallowed, otherwise retrying
//!    transaction may work incorrectly.
//!
//! # Nice Error Reporting
//!
//! We use [miette] crate for including snippets in your error reporting code.
//!
//! To make it work, first you need enable `fancy` feature in your top-level
//! crate's `Cargo.toml`:
//! ```toml
//! [dependencies]
//! miette = { version="5.3.0", features=["fancy"] }
//! edgedb-tokio = { version="*", features=["miette-errors"] }
//! ```
//!
//! Then if you use `miette` all the way through your application, it just
//! works:
//! ```rust,no_run
//! #[tokio::main]
//! async fn main() -> miette::Result<()> {
//!     let conn = edgedb_tokio::create_client().await?;
//!     conn.query::<String, _>("SELECT 1+2)", &()).await?;
//!     Ok(())
//! }
//! ```
//!
//! However, if you use some boxed error container (e.g. [anyhow]), you
//! might need to downcast error for printing:
//! ```rust,no_run
//! async fn do_something() -> anyhow::Result<()> {
//!     let conn = edgedb_tokio::create_client().await?;
//!     conn.query::<String, _>("SELECT 1+2)", &()).await?;
//!     Ok(())
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     match do_something().await {
//!         Ok(res) => res,
//!         Err(e) => {
//!             e.downcast::<edgedb_tokio::Error>()
//!                 .map(|e| eprintln!("{:?}", miette::Report::new(e)))
//!                 .unwrap_or_else(|e| eprintln!("{:#}", e));
//!             std::process::exit(1);
//!         }
//!     }
//! }
//! ```
//!
//! In some cases, where parts of your code use `miette::Result` or
//! `miette::Report` before converting to the boxed (anyhow) container, you
//! might want a little bit more complex downcasting:
//!
//! ```rust,no_run
//! # async fn do_something() -> anyhow::Result<()> { unimplemented!() }
//! #[tokio::main]
//! async fn main() {
//!     match do_something().await {
//!         Ok(res) => res,
//!         Err(e) => {
//!             e.downcast::<edgedb_tokio::Error>()
//!                 .map(|e| eprintln!("{:?}", miette::Report::new(e)))
//!                 .or_else(|e| e.downcast::<miette::Report>()
//!                     .map(|e| eprintln!("{:?}", e)))
//!                 .unwrap_or_else(|e| eprintln!("{:#}", e));
//!             std::process::exit(1);
//!         }
//!     }
//! }
//! ```
//!
//! Note that last two examples do hide error contexts from anyhow and do not
//! pretty print if `source()` of the error is `edgedb_errors::Error` but not
//! the top-level one. We leave those more complex cases as an excersize to the
//! reader.
//!
//! [miette]: https://crates.io/crates/miette
//! [anyhow]: https://crates.io/crates/anyhow
//!
mod error;
mod traits;

pub mod display;
pub mod kinds;

#[cfg(feature="miette")]
pub mod miette;

pub use traits::{ErrorKind, ResultExt};
pub use error::{Error, Tag};
pub use kinds::*;
