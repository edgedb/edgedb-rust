#![cfg_attr(not(feature="unstable"), allow(dead_code))]

mod connection;
mod options;
mod queries;

use std::sync::{Arc, Mutex as BlockingMutex};
use std::collections::VecDeque;

use bytes::BytesMut;
use tls_api::{TlsStream};
use tokio::sync::{self, Semaphore};

use edgedb_protocol::features::ProtocolVersion;

use crate::errors::{Error, ErrorKind, ClientError};
use crate::builder::Config;

pub use options::Options;

#[derive(Clone, Debug)]
pub struct Pool(Arc<PoolInner>);

#[derive(Debug)]
struct PoolInner {
    pub config: Config,
    pub semaphore: Arc<Semaphore>,
    pub queue: BlockingMutex<VecDeque<ConnInner>>,
}

#[derive(Debug)]
pub struct Connection {
    inner: Option<ConnInner>,
    #[allow(dead_code)]  // needed only for Drop side effect
    permit: sync::OwnedSemaphorePermit,
    pool: Arc<PoolInner>,
}

#[derive(Debug)]
pub struct ConnInner {
    proto: ProtocolVersion,
    #[allow(dead_code)] // TODO
    params: typemap::TypeMap<dyn typemap::DebugAny + Send + Sync>,
    mode: connection::Mode,
    in_buf: BytesMut,
    out_buf: BytesMut,
    stream: TlsStream,
}

trait AssertConn: Send + 'static {}
impl AssertConn for Connection {}

trait AssertPool: Send + Sync + 'static {}
impl AssertPool for Pool {}

impl Pool {
    pub fn new(config: &Config) -> Pool {
        Pool(Arc::new(PoolInner {
            semaphore: Arc::new(Semaphore::new(config.0.max_connections)),
            queue: BlockingMutex::new(
                VecDeque::with_capacity(config.0.max_connections)),
            config: config.clone(),
        }))
    }
    pub async fn acquire(&self) -> Result<Connection, Error> {
        self.0.acquire().await
    }
}

impl PoolInner {
    fn _next_conn(&self, _permit: &sync::OwnedSemaphorePermit)
        -> Option<ConnInner>
    {
        self.queue.lock()
            .expect("pool shared state mutex is not poisoned")
            .pop_front()
    }
    async fn acquire(self: &Arc<Self>) -> Result<Connection, Error> {
        let permit = self.semaphore.clone().acquire_owned().await
            .map_err(|e| ClientError::with_source(e)
                     .context("cannot acquire connection"))?;
        if let Some(conn) = self._next_conn(&permit) {
            assert!(conn.is_consistent());
            return Ok(Connection {
                inner: Some(conn),
                permit,
                pool: self.clone(),
            });
        }
        let conn = ConnInner::connect(&self.config).await?;
        // Make sure that connection is wrapped before we commit,
        // so that connection is returned into a pool if we fail
        // to commit because of async stuff
        return Ok(Connection {
            inner: Some(conn),
            permit,
            pool: self.clone(),
        });
    }
}

impl Connection {
    pub fn is_consistent(&self) -> bool {
        self.inner.as_ref().map(|c| c.is_consistent()).unwrap_or(false)
    }
}

impl Drop for Connection {
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
