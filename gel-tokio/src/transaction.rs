use std::future::Future;
use std::sync::Arc;

use bytes::BytesMut;
use gel_protocol::common::CompilationOptions;
use gel_protocol::common::{Capabilities, Cardinality, InputLanguage, IoFormat};
use gel_protocol::model::Json;
use gel_protocol::query_arg::{Encoder, QueryArgs};
use gel_protocol::QueryResult;
use tokio::sync::oneshot;
use tokio::time::sleep;

use crate::errors::{Error, ErrorKind, SHOULD_RETRY};
use crate::errors::{NoDataError, ProtocolEncodingError};
use crate::raw::{Options, Pool, PoolConnection, Response};
use crate::ResultVerbose;

/// A representation of a transaction.
///
/// It can be obtained in two flavors:
/// - [`RetryingTransaction`] from [`Client::within_transaction()`](crate::Client::within_transaction),
/// - [`RawTransaction`] from [`Client::transaction_raw()`](crate::Client::transaction_raw).
///
/// Implements all query & execute functions as [Client](crate::Client) as well as
/// [QueryExecutor](crate::QueryExecutor).
#[derive(Debug)]
pub struct Transaction {
    options: Arc<Options>,
    conn: PoolConnection,

    started: bool,
}

/// Transaction object returned by [`Client::transaction_raw()`](crate::Client::transaction_raw) method.
///
/// When this object is dropped, the transaction will implicitly roll back.
/// Use [commit](RawTransaction::commit) method to commit the changes made in the transaction.
///
/// All database queries in transaction should be executed using methods on
/// this object instead of using original [`Client`](crate::Client) instance.
#[derive(Debug)]
pub struct RawTransaction {
    inner: Option<Transaction>,
}

impl RawTransaction {
    /// Commit the transaction.
    ///
    /// If this method is not called, the transaction rolls back
    /// when [RawTransaction] is dropped.
    pub async fn commit(mut self) -> Result<(), Error> {
        if let Some(tran) = self.inner.take() {
            tran.commit().await
        } else {
            log::trace!("raw transaction done, noop commit");
            Ok(())
        }
    }

    /// Rollback the transaction.
    pub async fn rollback(mut self) -> Result<(), Error> {
        if let Some(tran) = self.inner.take() {
            tran.rollback().await
        } else {
            log::trace!("raw transaction done, noop rollback");
            Ok(())
        }
    }
}

impl std::ops::Deref for RawTransaction {
    type Target = Transaction;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for RawTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}

impl Drop for RawTransaction {
    fn drop(&mut self) {
        if let Some(tran) = self.inner.take() {
            tokio::task::spawn(tran.rollback());
        }
    }
}

pub(crate) async fn start(pool: &Pool, options: Arc<Options>) -> Result<RawTransaction, Error> {
    let conn = pool.acquire().await?;

    Ok(RawTransaction {
        inner: Some(Transaction::new(options, conn)),
    })
}

/// Transaction object passed to the closure via
/// [`Client::within_transaction()`](crate::Client::within_transaction) method.
///
/// This object must be dropped by the end of the closure execution.
///
/// All database queries in transaction should be executed using methods on
/// this object instead of using original [`Client`](crate::Client) instance.
#[derive(Debug)]
pub struct RetryingTransaction {
    inner: Option<Transaction>,
    iteration: u32,
    result_tx: Option<oneshot::Sender<Transaction>>,
}

impl RetryingTransaction {
    /// Zero-based iteration (attempt) number for the current transaction
    ///
    /// First attempt gets `iteration = 0`, second attempt `iteration = 1`,
    /// etc.
    pub fn iteration(&self) -> u32 {
        self.iteration
    }
}

impl std::ops::Deref for RetryingTransaction {
    type Target = Transaction;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().unwrap()
    }
}

impl std::ops::DerefMut for RetryingTransaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap()
    }
}

impl Drop for RetryingTransaction {
    fn drop(&mut self) {
        let tran = self.inner.take().unwrap();
        self.result_tx.take().unwrap().send(tran).ok();
    }
}

pub(crate) async fn run_and_retry<T, B, F>(
    pool: &Pool,
    options: Arc<Options>,
    mut body: B,
) -> Result<T, Error>
where
    B: FnMut(RetryingTransaction) -> F,
    F: Future<Output = Result<T, Error>>,
{
    let mut iteration = 0;
    'transaction: loop {
        let conn = pool.acquire().await?;
        let tran = Transaction::new(options.clone(), conn);

        let (tx, mut rx) = oneshot::channel();

        let tran = RetryingTransaction {
            inner: Some(tran),
            iteration,
            result_tx: Some(tx),
        };
        let result = body(tran).await;
        let tran = rx.try_recv().expect(
            "Transaction object must \
            be dropped by the time transaction body finishes.",
        );
        match result {
            Ok(val) => {
                log::debug!("Comitting transaction");
                tran.commit().await?;
                return Ok(val);
            }
            Err(outer) => {
                log::debug!("Rolling back transaction on error");
                tran.rollback().await?;

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
    fn new(options: Arc<Options>, conn: PoolConnection) -> Self {
        Transaction {
            options,
            conn,
            started: false,
        }
    }

    async fn ensure_started(&mut self) -> anyhow::Result<(), Error> {
        if !self.started {
            let options = &self.options;
            self.conn
                .statement("START TRANSACTION", &options.state, &options.annotations)
                .await?;
            self.started = true;
        }
        Ok(())
    }

    async fn commit(mut self) -> anyhow::Result<(), Error> {
        if !self.started {
            log::trace!("transaction was never started, noop commit");
            return Ok(());
        }

        log::trace!("commit");
        let options = &self.options;
        self.conn
            .statement("COMMIT", &options.state, &options.annotations)
            .await?;
        Ok(())
    }

    async fn rollback(mut self) -> anyhow::Result<(), Error> {
        if !self.started {
            log::trace!("transaction was never started, noop commit");
            return Ok(());
        }

        log::trace!("rollback");
        let options = &self.options;
        self.conn
            .statement("ROLLBACK", &options.state, &options.annotations)
            .await?;
        Ok(())
    }

    async fn query_helper<R, A>(
        &mut self,
        query: impl AsRef<str> + Send,
        arguments: &A,
        io_format: IoFormat,
        cardinality: Cardinality,
    ) -> Result<Response<Vec<R>>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        self.ensure_started().await?;

        self.conn
            .inner()
            .query(
                query.as_ref(),
                arguments,
                &self.options.state,
                &self.options.annotations,
                Capabilities::MODIFICATIONS,
                io_format,
                cardinality,
            )
            .await
    }

    /// Execute a query and return a collection of results.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = tran.query::<String, _>("SELECT 'hello'", &());
    /// // or
    /// let greeting: Vec<String> = tran.query("SELECT 'hello'", &());
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
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
        self.query_helper(query, arguments, IoFormat::Binary, Cardinality::Many)
            .await
            .map(|x| x.data)
    }

    /// Execute a query and return a collection of results and warnings produced by the server.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting: (Vec<String>, _) = tran.query_with_warnings("select 'hello'", &()).await?;
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_verbose<R, A>(
        &mut self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> Result<ResultVerbose<Vec<R>>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        self.query_helper(query, arguments, IoFormat::Binary, Cardinality::Many)
            .await
            .map(|Response { data, warnings, .. }| ResultVerbose { data, warnings })
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
    /// let greeting = tran.query_required_single::<String, _>(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// // or
    /// let greeting: String = tran.query_required_single(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
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
        self.query_helper(query, arguments, IoFormat::Binary, Cardinality::AtMostOne)
            .await
            .map(|x| x.data.into_iter().next())
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
    /// let greeting = tran.query_required_single::<String, _>(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// // or
    /// let greeting: String = tran.query_required_single(
    ///     "SELECT 'hello'",
    ///     &(),
    /// );
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
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
        self.query_helper(query, arguments, IoFormat::Binary, Cardinality::AtMostOne)
            .await
            .and_then(|x| {
                x.data
                    .into_iter()
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
        let res = self
            .query_helper::<String, _>(query, arguments, IoFormat::Json, Cardinality::Many)
            .await?;

        let json = res
            .data
            .into_iter()
            .next()
            .ok_or_else(|| NoDataError::with_message("query row returned zero results"))?;

        // we trust database to produce valid json
        Ok(Json::new_unchecked(json))
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
        let res = self
            .query_helper::<String, _>(query, arguments, IoFormat::Json, Cardinality::AtMostOne)
            .await?;

        // we trust database to produce valid json
        Ok(res.data.into_iter().next().map(Json::new_unchecked))
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
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
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
            input_language: InputLanguage::EdgeQL,
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
        };
        let state = &self.options.state;
        let desc = self
            .conn
            .parse(&flags, query, state, &self.options.annotations)
            .await?;
        let inp_desc = desc.input().map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        self.conn
            .execute(
                &flags,
                query,
                state,
                &self.options.annotations,
                &desc,
                &arg_buf.freeze(),
            )
            .await?;
        Ok(())
    }
}
