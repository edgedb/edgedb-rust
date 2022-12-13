#![cfg_attr(not(feature="unstable"), allow(dead_code))]

mod connection;
mod options;
mod queries;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex as BlockingMutex};

use bytes::{Bytes, BytesMut};
use tls_api::{TlsStream};
use tokio::sync::{self, Semaphore};

use edgedb_protocol::features::ProtocolVersion;
use edgedb_protocol::common::{RawTypedesc, Capabilities};
use edgedb_protocol::server_message::Data;

use crate::errors::{Error, ErrorKind, ClientError};
use crate::builder::Config;

pub use options::Options;

#[derive(Clone, Debug)]
pub struct Pool(Arc<PoolInner>);

pub enum QueryCapabilities {
    Unparsed,
    Parsed(Capabilities),
}

#[derive(Debug)]
struct PoolInner {
    pub config: Config,
    pub semaphore: Arc<Semaphore>,
    pub queue: BlockingMutex<VecDeque<Connection>>,
}

#[derive(Debug)]
pub struct PoolConnection {
    inner: Option<Connection>,
    #[allow(dead_code)]  // needed only for Drop side effect
    permit: sync::OwnedSemaphorePermit,
    pool: Arc<PoolInner>,
}

#[derive(Debug)]
pub struct Connection {
    proto: ProtocolVersion,
    #[allow(dead_code)] // TODO
    params: typemap::TypeMap<dyn typemap::DebugAny + Send + Sync>,
    mode: connection::Mode,
    state_desc: RawTypedesc,
    in_buf: BytesMut,
    out_buf: BytesMut,
    stream: TlsStream,
}

#[derive(Debug)]
pub struct Response {
    pub status_data: Bytes,
    new_state: Option<edgedb_protocol::common::State>,
    data: Vec<Data>,
}

trait AssertConn: Send + 'static {}
impl AssertConn for PoolConnection {}

trait AssertPool: Send + Sync + 'static {}
impl AssertPool for Pool {}

impl edgedb_errors::Field for QueryCapabilities {
    const NAME: &'static str = "capabilities";
    type Value = QueryCapabilities;
}

impl Pool {
    pub fn new(config: &Config) -> Pool {
        Pool(Arc::new(PoolInner {
            semaphore: Arc::new(Semaphore::new(config.0.max_connections)),
            queue: BlockingMutex::new(
                VecDeque::with_capacity(config.0.max_connections)),
            config: config.clone(),
        }))
    }
    pub async fn acquire(&self) -> Result<PoolConnection, Error> {
        self.0.acquire().await
    }
}

impl PoolInner {
    fn _next_conn(&self, _permit: &sync::OwnedSemaphorePermit)
        -> Option<Connection>
    {
        self.queue.lock()
            .expect("pool shared state mutex is not poisoned")
            .pop_front()
    }
    async fn acquire(self: &Arc<Self>) -> Result<PoolConnection, Error> {
        let permit = self.semaphore.clone().acquire_owned().await
            .map_err(|e| ClientError::with_source(e)
                     .context("cannot acquire connection"))?;
        while let Some(mut conn) = self._next_conn(&permit) {
            assert!(conn.is_consistent());
            if conn.is_connection_reset().await {
                continue;
            }
            return Ok(PoolConnection {
                inner: Some(conn),
                permit,
                pool: self.clone(),
            });
        }
        let conn = Connection::connect(&self.config).await?;
        // Make sure that connection is wrapped before we commit,
        // so that connection is returned into a pool if we fail
        // to commit because of async stuff
        return Ok(PoolConnection {
            inner: Some(conn),
            permit,
            pool: self.clone(),
        });
    }
}

impl PoolConnection {
    pub fn is_consistent(&self) -> bool {
        self.inner.as_ref().map(|c| c.is_consistent()).unwrap_or(false)
    }
}

impl Drop for PoolConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.take() {
            if conn.is_consistent() {
                self.pool.queue.lock()
                    .expect("pool shared state mutex is not poisoned")
                    .push_back(conn);
            }
        }
    }
}
