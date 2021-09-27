use std::sync::Arc;

use async_std::channel::{Sender};
use async_std::task::JoinHandle;
use async_std::sync::Mutex;

use edgedb_protocol::QueryResult;
use edgedb_protocol::client_message::{IoFormat, Cardinality};
use edgedb_protocol::query_arg::QueryArgs;
use edgedb_protocol::value::Value;

use crate::client::StatementParams;
use crate::errors::{Error, ErrorKind, NoDataError, NoResultExpected};

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

// User-visible instance of connection pool. Shallowly clonable contains
// options (clone pool to modify options). All the functionality is actually
// in the `PoolInner`
#[derive(Debug, Clone)]
pub struct Pool {
    options: Arc<Options>,
    pub(crate) inner: Arc<PoolInner>,
}

#[derive(Debug, Clone)]
pub struct ExecuteResult {
    marker: String,
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

impl Pool {

    pub async fn ensure_connected(&self) -> Result<(), Error> {
        self.inner.acquire().await?;
        Ok(())
    }

    pub async fn query<R, A>(&self, request: &str, arguments: &A)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        self.inner.query(request, arguments, &StatementParams::new()).await
    }

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
                NoDataError::with_message("query row returned zero results")
            })
    }

    pub async fn query_json<A>(&self, request: &str, arguments: &A)
        -> Result<String, Error>
        where A: QueryArgs,
    {
        let result = self.inner.query(request, arguments,
            StatementParams::new()
            .io_format(IoFormat::Json),
        ).await?;
        result.into_iter().next()
            .ok_or_else(|| {
                NoDataError::with_message("query row returned zero results")
            })
    }

    pub async fn query_single_json<A>(&self, request: &str, arguments: &A)
        -> Result<String, Error>
        where A: QueryArgs,
    {
        let result = self.inner.query(request, arguments,
            StatementParams::new()
            .io_format(IoFormat::Json)
            .cardinality(Cardinality::AtMostOne)
        ).await?;
        result.into_iter().next()
            .ok_or_else(|| {
                NoDataError::with_message("query row returned zero results")
            })
    }

    pub async fn execute<A>(&self, request: &str, arguments: &A)
        -> Result<Option<ExecuteResult>, Error>
        where A: QueryArgs,
    {
        let result = self.inner.query::<Value, _>(request, arguments,
                StatementParams::new()
                .cardinality(Cardinality::NoResult)
            ).await;
        match result {
            Ok(_) => Ok(None),
            Err(e) if e.is::<NoResultExpected>() => {
                match e.initial_message() {
                    Some(m) => Ok(Some(ExecuteResult { marker: m.into() })),
                    None => Ok(None),
                }
            }
            Err(e) => return Err(e),
        }
    }
}
