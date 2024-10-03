use std::future::Future;
use std::sync::Arc;

use bytes::BytesMut;
use edgedb_protocol::common::CompilationOptions;
use edgedb_protocol::common::{Capabilities, Cardinality, IoFormat};
use edgedb_protocol::model::Json;
use edgedb_protocol::query_arg::{Encoder, QueryArgs};
use edgedb_protocol::QueryResult;
use tokio::sync::oneshot;
use tokio::time::sleep;

use crate::errors::ClientError;
use crate::errors::{Error, ErrorKind, SHOULD_RETRY};
use crate::errors::{NoDataError, NoResultExpected, ProtocolEncodingError};
use crate::raw::{Options, Pool, PoolConnection, PoolState};

/// Transaction object passed to the closure via
/// [`Client::transaction()`](crate::Client::transaction) method
///
/// The Transaction object must be dropped by the end of the closure execution.
///
/// All database queries in transaction should be executed using methods on
/// this object instead of using original [`Client`](crate::Client) instance.
#[derive(Debug)]
pub struct Transaction {
    iteration: u32,
    state: Arc<PoolState>,
    inner: Option<Inner>,
}

#[derive(Debug)]
pub struct TransactionResult {
    conn: PoolConnection,
    started: bool,
}

#[derive(Debug)]
pub struct Inner {
    started: bool,
    conn: PoolConnection,
    return_conn: oneshot::Sender<TransactionResult>,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.take().map(
            |Inner {
                 started,
                 conn,
                 return_conn,
             }| { return_conn.send(TransactionResult { started, conn }).ok() },
        );
    }
}

pub(crate) async fn transaction<T, B, F>(
    pool: &Pool,
    options: &Options,
    mut body: B,
) -> Result<T, Error>
where
    B: FnMut(Transaction) -> F,
    F: Future<Output = Result<T, Error>>,
{
    let mut iteration = 0;
    'transaction: loop {
        let conn = pool.acquire().await?;
        let (tx, mut rx) = oneshot::channel();
        let tran = Transaction {
            iteration,
            state: options.state.clone(),
            inner: Some(Inner {
                started: false,
                conn,
                return_conn: tx,
            }),
        };
        let result = body(tran).await;
        let TransactionResult { mut conn, started } = rx.try_recv().expect(
            "Transaction object must \
            be dropped by the time transaction body finishes.",
        );
        match result {
            Ok(val) => {
                log::debug!("Comitting transaction");
                if started {
                    conn.statement("COMMIT", &options.state).await?;
                }
                return Ok(val);
            }
            Err(outer) => {
                log::debug!("Rolling back transaction on error");
                if started {
                    conn.statement("ROLLBACK", &options.state).await?;
                }

                let some_retry = outer.chain().find_map(|e| {
                    e.downcast_ref::<Error>().and_then(|e| {
                        if e.has_tag(SHOULD_RETRY) {
                            Some(e)
                        } else {
                            None
                        }
                    })
                });

                if some_retry.is_none() {
                    return Err(outer);
                } else {
                    let e = some_retry.unwrap();
                    let rule = options.retry.get_rule(e);
                    if iteration >= rule.attempts {
                        return Err(outer);
                    } else {
                        log::info!("Retrying transaction on {:#}", e);
                        iteration += 1;
                        sleep((rule.backoff)(iteration)).await;
                        continue 'transaction;
                    }
                }
            }
        }
    }
}

impl Transaction {
    /// Zero-based iteration (attempt) number for the current transaction
    ///
    /// First attempt gets `iteration = 0`, second attempt `iteration = 1`,
    /// etc.
    pub fn iteration(&self) -> u32 {
        self.iteration
    }
    async fn ensure_started(&mut self) -> anyhow::Result<(), Error> {
        if let Some(inner) = &mut self.inner {
            if !inner.started {
                inner
                    .conn
                    .statement("START TRANSACTION", &self.state)
                    .await?;
                inner.started = true;
            }
            return Ok(());
        }
        Err(ClientError::with_message("using transaction after drop"))
    }
    fn inner(&mut self) -> &mut Inner {
        self.inner
            .as_mut()
            .expect("transaction object is not dropped")
    }

    /// Query with retry.
    async fn _query<R, A>(
        &mut self,
        query: impl AsRef<str> + Send,
        arguments: &A,
        expected_cardinality: Cardinality,
    ) -> Result<Vec<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        self.ensure_started().await?;
        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Binary,
            expected_cardinality,
        };
        let state = self.state.clone(); // TODO: optimize, by careful borrow
        let conn = &mut self.inner().conn;
        let desc = conn.parse(&flags, query.as_ref(), &state).await?;
        let inp_desc = desc.input().map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn
            .execute(&flags, query.as_ref(), &state, &desc, &arg_buf.freeze())
            .await?;

        let out_desc = desc.output().map_err(ProtocolEncodingError::with_source)?;
        match out_desc.root_pos() {
            Some(root_pos) => {
                let ctx = out_desc.as_queryable_context();
                let mut state = R::prepare(&ctx, root_pos)?;
                let rows = data
                    .into_iter()
                    .flat_map(|chunk| chunk.data)
                    .map(|chunk| R::decode(&mut state, &chunk))
                    .collect::<Result<_, _>>()?;
                Ok(rows)
            }
            None => Err(NoResultExpected::build()),
        }
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
    pub async fn query<R, A>(
        &mut self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> Result<Vec<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        Transaction::_query(self, query, arguments, Cardinality::Many).await
    }

    /// Execute a query and return a single result
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query_required_single::<String, _>(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// // or
    /// let greeting: String = pool.query_required_single(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_single<R, A>(
        &mut self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> Result<Option<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Transaction::_query(self, query, arguments, Cardinality::AtMostOne)
            .await
            .map(|x| x.into_iter().next())
    }

    /// Execute a query and return a single result
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query_required_single::<String, _>(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// // or
    /// let greeting: String = pool.query_required_single(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_required_single<R, A>(
        &mut self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> Result<R, Error>
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Transaction::_query(self, query, arguments, Cardinality::AtMostOne)
            .await
            .and_then(|x| {
                x.into_iter()
                    .next()
                    .ok_or_else(|| NoDataError::with_message("query row returned zero results"))
            })
    }

    /// Execute a query and return the result as JSON.
    pub async fn query_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> Result<Json, Error> {
        self.ensure_started().await?;
        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Json,
            expected_cardinality: Cardinality::Many,
        };
        let state = self.state.clone(); // TODO optimize using careful borrow
        let conn = &mut self.inner().conn;
        let desc = conn.parse(&flags, query, &state).await?;
        let inp_desc = desc.input().map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn
            .execute(&flags, query, &state, &desc, &arg_buf.freeze())
            .await?;

        let out_desc = desc.output().map_err(ProtocolEncodingError::with_source)?;
        match out_desc.root_pos() {
            Some(root_pos) => {
                let ctx = out_desc.as_queryable_context();
                // JSON objects are returned as strings :(
                let mut state = String::prepare(&ctx, root_pos)?;
                let bytes = data
                    .into_iter()
                    .next()
                    .and_then(|chunk| chunk.data.into_iter().next());
                if let Some(bytes) = bytes {
                    // we trust database to produce valid json
                    let s = String::decode(&mut state, &bytes)?;
                    Ok(Json::new_unchecked(s))
                } else {
                    Err(NoDataError::with_message("query row returned zero results"))
                }
            }
            None => Err(NoResultExpected::build()),
        }
    }

    /// Execute a query and return a single result as JSON.
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised.
    pub async fn query_single_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> Result<Option<Json>, Error> {
        self.ensure_started().await?;
        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Json,
            expected_cardinality: Cardinality::AtMostOne,
        };
        let state = self.state.clone(); // TODO optimize using careful borrow
        let conn = &mut self.inner().conn;
        let desc = conn.parse(&flags, query, &state).await?;
        let inp_desc = desc.input().map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn
            .execute(&flags, query, &state, &desc, &arg_buf.freeze())
            .await?;

        let out_desc = desc.output().map_err(ProtocolEncodingError::with_source)?;
        match out_desc.root_pos() {
            Some(root_pos) => {
                let ctx = out_desc.as_queryable_context();
                // JSON objects are returned as strings :(
                let mut state = String::prepare(&ctx, root_pos)?;
                let bytes = data
                    .into_iter()
                    .next()
                    .and_then(|chunk| chunk.data.into_iter().next());
                if let Some(bytes) = bytes {
                    // we trust database to produce valid json
                    let s = String::decode(&mut state, &bytes)?;
                    Ok(Some(Json::new_unchecked(s)))
                } else {
                    Ok(None)
                }
            }
            None => Err(NoResultExpected::build()),
        }
    }

    /// Execute a query and return a single result as JSON.
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    pub async fn query_required_single_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> Result<Json, Error> {
        self.query_single_json(query, arguments)
            .await?
            .ok_or_else(|| NoDataError::with_message("query row returned zero results"))
    }

    /// Execute a query and don't expect result
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn execute<A>(&mut self, query: &str, arguments: &A) -> Result<(), Error>
    where
        A: QueryArgs,
    {
        self.ensure_started().await?;
        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
        };
        let state = self.state.clone(); // TODO: optimize, by careful borrow
        let conn = &mut self.inner().conn;
        let desc = conn.parse(&flags, query, &state).await?;
        let inp_desc = desc.input().map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        conn.execute(&flags, query, &state, &desc, &arg_buf.freeze())
            .await?;
        Ok(())
    }
}
