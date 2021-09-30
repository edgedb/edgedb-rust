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
/// This structure is shared between Pool instances when options are changed
pub(crate) struct PoolInner {
    chan: Sender<Command>,
    task: Mutex<Option<JoinHandle<()>>>,
    state: Arc<PoolState>,
}

/// A database connection pool
///
/// This is main way how to interact with the database. Usually it's created
/// using [connect](crate::connect) function.
///
// User-visible instance of connection pool. Shallowly clonable contains
// options (clone pool to modify options). All the functionality is actually
// in the `PoolInner`
#[derive(Debug, Clone)]
pub struct Client {
    options: Arc<Options>,
    pub(crate) inner: Arc<PoolInner>,
}
