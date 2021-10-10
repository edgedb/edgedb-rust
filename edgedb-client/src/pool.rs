use std::sync::Arc;

use async_std::channel::{Sender};
use async_std::task::JoinHandle;
use async_std::sync::Mutex;

mod command;
mod connection;
mod implementation;
mod main;

use command::Command;
use connection::PoolConn;
use main::PoolState;


#[derive(Debug, Clone)]
struct Options {
}

#[derive(Debug)]
/// This structure is shared between Pool instances when options are changed.
pub(crate) struct PoolInner {
    chan: Sender<Command>,
    task: Mutex<Option<JoinHandle<()>>>,
    state: Arc<PoolState>,
}

/// A database connection client.
///
/// This is the struct used to interact with the database. Typically, you will
/// use the [`connect()`](crate::connect) function to create this struct, or
/// with a [`Builder`](crate::Builder) that you pass to
/// [`Client::new()`](crate::Client::new).
///
// User-visible instance of connection pool. Shallowly clonable contains
// options (clone pool to modify options). All the functionality is actually
// in the `PoolInner`
#[derive(Debug, Clone)]
pub struct Client {
    options: Arc<Options>,
    pub(crate) inner: Arc<PoolInner>,
}
