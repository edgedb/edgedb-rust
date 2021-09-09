use std::sync::Arc;

use async_std::channel::{Sender};
use async_std::task::JoinHandle;
use async_std::sync::Mutex;

use edgedb_protocol::QueryResult;
use edgedb_protocol::client_message::{IoFormat, Cardinality};
use edgedb_protocol::query_arg::QueryArgs;
use edgedb_protocol::value::Value;

use crate::client::StatementBuilder;
use crate::errors::{Error, ErrorKind, NoDataError, NoResultExpected};

mod command;
mod config;
mod connection;
mod implementation;
mod main;

pub use config::PoolConfig;

use command::Command;
use connection::PoolConn;
use main::PoolState;


#[derive(Debug, Clone)]
struct Options {
}

#[derive(Debug)]
pub(crate) struct PoolInner {
    chan: Sender<Command>,
    task: Mutex<Option<JoinHandle<()>>>,
    state: Arc<PoolState>,
}

#[derive(Debug, Clone)]
pub struct Pool {
    options: Arc<Options>,
    inner: Arc<PoolInner>,
}

#[derive(Debug, Clone)]
pub struct ExecuteResult {
    marker: String,
}

impl PoolInner {
    async fn query<R, A>(&self, request: &str, arguments: &A,
        bld: &StatementBuilder)
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
    pub async fn query<R, A>(&self, request: &str, arguments: &A)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        self.inner.query(request, arguments, &StatementBuilder::new()).await
    }

    pub async fn query_single<R, A>(&self, request: &str, arguments: &A)
        -> Result<R, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        let result = self.inner.query(request, arguments,
            StatementBuilder::new()
            .cardinality(Cardinality::NoResult)
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
            StatementBuilder::new()
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
            StatementBuilder::new()
            .io_format(IoFormat::Json)
            .cardinality(Cardinality::One)
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
                StatementBuilder::new()
                .io_format(IoFormat::Json)
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
