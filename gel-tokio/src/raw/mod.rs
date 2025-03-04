#![cfg_attr(not(feature = "unstable"), allow(dead_code))]

mod connection;
#[cfg(feature = "unstable")]
mod dumps;
mod options;
mod queries;
mod response;
pub mod state;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex as BlockingMutex};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::{self, Semaphore};

use gel_dsn::gel::{Config, DEFAULT_POOL_SIZE};
use gel_protocol::common::{Capabilities, RawTypedesc};
use gel_protocol::features::ProtocolVersion;
use gel_protocol::server_message::CommandDataDescription1;
use gel_protocol::server_message::TransactionState;

use crate::errors::{ClientError, Error, ErrorKind};
use crate::server_params::ServerParams;

pub use options::Options;
pub use response::ResponseStream;
pub use state::{PoolState, State};

#[cfg(feature = "unstable")]
pub use dumps::DumpStream;

#[derive(Clone, Debug)]
pub struct Pool(Arc<PoolInner>);

pub enum QueryCapabilities {
    Unparsed,
    Parsed(Capabilities),
}

pub struct Description;

#[derive(Debug)]
struct PoolInner {
    pub config: Config,
    pub semaphore: Arc<Semaphore>,
    pub queue: BlockingMutex<VecDeque<Connection>>,
}

#[derive(Debug)]
pub struct PoolConnection {
    inner: Option<Connection>,
    #[allow(dead_code)] // needed only for Drop side effect
    permit: sync::OwnedSemaphorePermit,
    pool: Arc<PoolInner>,
}

#[derive(Debug)]
pub struct Connection {
    proto: ProtocolVersion,
    server_params: ServerParams,
    mode: connection::Mode,
    transaction_state: TransactionState,
    state_desc: RawTypedesc,
    in_buf: BytesMut,
    out_buf: BytesMut,
    stream: gel_stream::RawStream,
    ping_interval: PingInterval,
}

#[derive(Debug)]
pub struct Response<T> {
    pub status_data: Bytes,
    pub new_state: Option<gel_protocol::common::State>,
    pub data: T,
    pub warnings: Vec<gel_protocol::annotations::Warning>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum PingInterval {
    Unknown,
    Disabled,
    Interval(Duration),
}

impl gel_errors::Field for QueryCapabilities {
    const NAME: &'static str = "capabilities";
    type Value = QueryCapabilities;
}

impl gel_errors::Field for Description {
    const NAME: &'static str = "descriptor";
    type Value = CommandDataDescription1;
}

impl Pool {
    pub fn new(config: &Config) -> Pool {
        let concurrency = config
            .max_concurrency
            // TODO(tailhook) use 1 and get concurrency from the connection
            .unwrap_or(DEFAULT_POOL_SIZE);
        Pool(Arc::new(PoolInner {
            semaphore: Arc::new(Semaphore::new(concurrency)),
            queue: BlockingMutex::new(VecDeque::with_capacity(concurrency)),
            config: config.clone(),
        }))
    }
    pub async fn acquire(&self) -> Result<PoolConnection, Error> {
        self.0.acquire().await
    }
}

impl PoolInner {
    fn _next_conn(&self, _permit: &sync::OwnedSemaphorePermit) -> Option<Connection> {
        self.queue
            .lock()
            .expect("pool shared state mutex is not poisoned")
            .pop_front()
    }
    async fn acquire(self: &Arc<Self>) -> Result<PoolConnection, Error> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| ClientError::with_source(e).context("cannot acquire connection"))?;
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
        Ok(PoolConnection {
            inner: Some(conn),
            permit,
            pool: self.clone(),
        })
    }
}

impl PoolConnection {
    pub fn is_consistent(&self) -> bool {
        self.inner
            .as_ref()
            .map(|c| c.is_consistent())
            .unwrap_or(false)
    }
}

impl Drop for PoolConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.take() {
            if conn.is_consistent() {
                self.pool
                    .queue
                    .lock()
                    .expect("pool shared state mutex is not poisoned")
                    .push_back(conn);
            }
        }
    }
}

impl<T> Response<T> {
    fn map<U, R>(self, f: impl FnOnce(T) -> Result<U, R>) -> Result<Response<U>, R> {
        Ok(Response {
            status_data: self.status_data,
            new_state: self.new_state,
            data: f(self.data)?,
            warnings: self.warnings,
        })
    }

    fn log_warnings(&self) {
        for w in &self.warnings {
            log::warn!(target: "gel_tokio::warning", "{w}");
        }
    }
}
