use async_std::channel::unbounded;
use async_std::task;
use async_std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;

use edgedb_protocol::QueryResult;
use edgedb_protocol::client_message::{IoFormat, Cardinality};
use edgedb_protocol::query_arg::QueryArgs;
use edgedb_protocol::value::Value;

use crate::ExecuteResult;
use crate::model::Json;
use crate::builder::Builder;
use crate::client::{Connection, StatementParams};
use crate::errors::{Error, ErrorKind, NoDataError, NoResultExpected};
use crate::pool::command::Command;
use crate::pool::main;
use crate::pool::{Client, PoolInner, PoolState, PoolConn, Options};

pub enum InProgressState {
    Connecting,
    Comitting,
    Done,
}

struct InProgress {
    state: InProgressState,
    pool: Arc<PoolInner>,
}

impl InProgress {
    fn new(mut guard: MutexGuard<'_, main::Inner>, pool: &Arc<PoolInner>)
        -> InProgress
    {
        guard.in_progress += 1;
        InProgress { pool: pool.clone(), state: InProgressState::Connecting }
    }
    async fn commit(mut self) {
        self.state = InProgressState::Comitting;
        let mut inner = self.pool.state.inner.lock().await;
        inner.in_progress -= 1;
        inner.acquired_conns += 1;
        self.state = InProgressState::Done;
    }
}

impl Drop for InProgress {
    fn drop(&mut self) {
        use InProgressState::*;

        match self.state {
            Connecting => {
                self.pool.chan.try_send(Command::ConnectionCanceled).ok();
            }
            Comitting => {
                self.pool.chan.try_send(Command::ConnectionEstablished).ok();
            }
            Done => {}
        }
    }
}

impl PoolInner {
    async fn query<R, A>(self: &Arc<Self>, request: &str, arguments: &A,
        bld: &StatementParams)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        // TODO(tailhook) retry loop
        let mut conn = self.acquire().await?;
        conn.query(request, arguments, bld).await
    }
}

impl Client {
    /// Create a new connection pool.
    ///
    /// Note this does not create a connection immediately.
    /// Use [`ensure_connected()`][Client::ensure_connected] to establish a
    /// connection and verify that the connection is usable.
    pub fn new(builder: Builder) -> Client {
        let (chan, rcv) = unbounded();
        let state = Arc::new(PoolState::new(builder));
        let state2 = state.clone();
        let task = Mutex::new(Some(task::spawn(main::main(state2, rcv))));
        Client {
            options: Arc::new(Options {}),
            inner: Arc::new(PoolInner {
                chan,
                task,
                state,
            }),
        }
    }

    /// Start shutting down the connection pool.
    ///
    /// Note that this waits for all connections to be released when called
    /// for the first time. But if it is called multiple times concurrently,
    /// only the first call will wait and subsequent call will exit
    /// immediately.
    pub async fn close(&self) {
        self.inner.chan.send(Command::Close).await.ok();
        if let Some(task) = self.inner.task.lock().await.take() {
            task.await;
        }
    }

    /// Ensure that there is at least one working connection to the pool.
    ///
    /// This can be used at application startup to ensure that you have a
    /// working connection.
    pub async fn ensure_connected(&self) -> Result<(), Error> {
        self.inner.acquire().await?;
        Ok(())
    }

    /// Execute a query and return a collection of results.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query::<String, _>("SELECT 'hello'", &());
    /// // or
    /// let greeting: Vec<String> = pool.query("SELECT 'hello'", &());
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query<R, A>(&self, request: &str, arguments: &A)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        self.inner.query(request, arguments, &StatementParams::new()).await
    }

    /// Execute a query and return a single result.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query_single::<String, _>("SELECT 'hello'", &());
    /// // or
    /// let greeting: String = pool.query_single("SELECT 'hello'", &());
    /// ```
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_single<R, A>(&self, request: &str, arguments: &A)
        -> Result<R, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        let result = self.inner.query(request, arguments,
            StatementParams::new()
            .cardinality(Cardinality::AtMostOne)
        ).await?;
        result.into_iter().next()
            .ok_or_else(|| {
                NoDataError::with_message(
                    "query_single() returned zero results")
            })
    }

    /// Execute a query and return the result as JSON.
    pub async fn query_json<A>(&self, request: &str, arguments: &A)
        -> Result<Json, Error>
        where A: QueryArgs,
    {
        let result = self.inner.query(request, arguments,
            StatementParams::new()
            .io_format(IoFormat::Json),
        ).await?;
        result.into_iter().next()
            // we trust database to produce valid json
            .map(|v| unsafe { Json::new_unchecked(v) })
            .ok_or_else(|| {
                NoDataError::with_message("query row returned zero results")
            })
    }

    /// Execute a query and return a single result as JSON.
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    pub async fn query_single_json<A>(&self, request: &str, arguments: &A)
        -> Result<Json, Error>
        where A: QueryArgs,
    {
        let result = self.inner.query(request, arguments,
            StatementParams::new()
            .io_format(IoFormat::Json)
            .cardinality(Cardinality::AtMostOne)
        ).await?;
        result.into_iter().next()
            // we trust database to produce valid json
            .map(|v| unsafe { Json::new_unchecked(v) })
            .ok_or_else(|| {
                NoDataError::with_message("query row returned zero results")
            })
    }
    /// Execute one or more EdgeQL commands.
    ///
    /// Note that if you want the results of query, use
    /// [`query()`][Client::query] or [`query_single()`][Client::query_single]
    /// instead.
    pub async fn execute<A>(&self, request: &str, arguments: &A)
        -> Result<ExecuteResult, Error>
        where A: QueryArgs,
    {
        let result = self.inner.query::<Value, _>(request, arguments,
                StatementParams::new()
                .cardinality(Cardinality::Many) // TODO: NoResult
            ).await;
        match result {
            // TODO(tailhook) propagate better rather than returning nothing
            Ok(_) => Ok(ExecuteResult { marker: Bytes::from_static(b"") }),
            Err(e) if e.is::<NoResultExpected>() => {
                // TODO(tailhook) propagate better rather than parsing a
                // message
                match e.initial_message() {
                    Some(m) => {
                        Ok(ExecuteResult {
                            marker: Bytes::from(m.as_bytes().to_vec()),
                        })
                    }
                    None => {
                        Ok(ExecuteResult { marker: Bytes::from_static(b"") })
                    }
                }
            }
            Err(e) => return Err(e),
        }
    }
}

impl PoolInner {
    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<PoolConn, Error> {
        let mut inner = self.state.inner.lock().await;
        loop {
            if let Some(conn) = inner.conns.pop_front() {
                assert!(conn.is_consistent());
                inner.acquired_conns += 1;
                return Ok(PoolConn { conn: Some(conn), pool: self.clone() });
            }
            let in_pool = inner.in_progress + inner.acquired_conns;
            if in_pool < self.state.config.max_connections {
                let guard = InProgress::new(inner, self);
                let conn = self.state.config.private_connect().await?;
                // Make sure that connection is wrapped before we commit,
                // so that connection is returned into a pool if we fail
                // to commit because of async stuff
                let conn = PoolConn { conn: Some(conn), pool: self.clone() };
                guard.commit().await;
                return Ok(conn);
            }
            inner = self.state.connection_released.wait(inner).await;
        }
    }
    pub(crate) fn release(&self, conn: Connection) {
        self.chan.try_send(Command::Release(conn)).ok();
    }
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        // If task is locked (i.e. try_lock returns an error) it means
        // somebody is currently waiting for pool to be closed, which is fine.
        self.task.try_lock()
            .and_then(|mut task| task.take().map(|t| t.cancel()));
    }
}
