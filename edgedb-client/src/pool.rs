use std::sync::Arc;

use bytes::Bytes;

use edgedb_protocol::QueryResult;
use edgedb_protocol::query_arg::QueryArgs;

use crate::errors::{Error, ErrorKind, NoDataError};


#[derive(Debug, Clone, Copy)]
enum IoFormat {
    Binary,
    Json,
}

#[derive(Debug, Clone)]
struct Config {
}

#[derive(Debug, Clone)]
struct PoolInner {
}

#[derive(Debug, Clone)]
pub struct Pool {
    config: Arc<Config>,
    inner: PoolInner,
}

#[derive(Debug, Clone)]
pub struct ExecuteResult {
    marker: Bytes,
}

#[derive(Debug, Clone)]
struct StatementBuilder {
    io_format: IoFormat,
    expect_single: bool,
}

impl StatementBuilder {
    fn new() -> StatementBuilder {
        StatementBuilder {
            io_format: IoFormat::Binary,
            expect_single: false,
        }
    }
    fn io_format(&mut self, fmt: IoFormat) -> &mut Self {
        self.io_format = fmt;
        self
    }
    fn expect_single(&mut self) -> &mut Self {
        self.expect_single = true;
        self
    }
}

impl PoolInner {
    async fn query<R, A>(&mut self, request: &str, arguments: &A,
        bld: &StatementBuilder)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        todo!();
    }
}

impl Pool {
    pub async fn query<R, A>(&mut self, request: &str, arguments: &A)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        self.inner.query(request, arguments, &StatementBuilder::new()).await
    }

    pub async fn query_single<R, A>(&mut self, request: &str, arguments: &A)
        -> Result<R, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        let result = self.inner.query(request, arguments,
            StatementBuilder::new()
            .expect_single()
        ).await?;
        result.into_iter().next()
            .ok_or_else(|| {
                NoDataError::with_message("query row returned zero results")
            })
    }

    pub async fn query_json<A>(&mut self, request: &str, arguments: &A)
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

    pub async fn query_single_json<A>(&mut self, request: &str, arguments: &A)
        -> Result<String, Error>
        where A: QueryArgs,
    {
        let result = self.inner.query(request, arguments,
            StatementBuilder::new()
            .io_format(IoFormat::Json)
            .expect_single()
        ).await?;
        result.into_iter().next()
            .ok_or_else(|| {
                NoDataError::with_message("query row returned zero results")
            })
    }

    pub async fn execute<A>(&mut self, request: &str, arguments: &A)
        -> Result<ExecuteResult, Error>
        where A: QueryArgs,
    {
        todo!();
    }
}
