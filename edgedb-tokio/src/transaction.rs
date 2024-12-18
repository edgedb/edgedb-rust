use std::future::Future;
use std::sync::Arc;

use bytes::BytesMut;
use edgedb_protocol::common::CompilationOptions;
use edgedb_protocol::common::{Capabilities, Cardinality, InputLanguage, IoFormat};
use edgedb_protocol::encoding::Annotations;
use edgedb_protocol::model::Json;
use edgedb_protocol::query_arg::{Encoder, QueryArgs};
use edgedb_protocol::QueryResult;
use tokio::time::sleep;

use crate::errors::ClientError;
use crate::errors::{Error, ErrorKind, SHOULD_RETRY};
use crate::errors::{NoDataError, ProtocolEncodingError};
use crate::raw::{Options, Pool, PoolConnection, PoolState, Response};
use crate::ResultVerbose;

/// Transaction struct created by calling the [`Client::transaction()`](crate::Client::transaction) method
///
/// The Transaction object must be explicitly commited. They are automatically rollback on drop if not commited.
///
/// All database queries in transaction should be executed using methods on
/// this object instead of using original [`Client`](crate::Client) instance.
#[derive(Debug)]
pub struct Transaction {
    state: Arc<PoolState>,
    annotations: Arc<Annotations>,
    /// this has to be an option because during Drop we need to take the value out and spawn a task to rollback the
    /// transaction. Revisit this when async drop is stable.
    conn: Option<PoolConnection>,
    /// used to know if there is anything to be commited or rolled back
    started: bool,
    /// used to know if a rollback is required on drop
    explicitly_commited_or_rolled_back: bool,
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.started {
            log::trace!("transaction was never started, noop drop");
            return;
        }
        if self.explicitly_commited_or_rolled_back {
            log::trace!("transaction explicitly_commited_or_rolled_back, noop drop");
            return;
        }
        log::debug!("transaction dropped, so rolling back");
        let mut con = self
            .conn
            .take()
            .expect("con to only be taken on transaction drop");
        let state = self.state.clone();
        let annotations = self.annotations.clone();
        tokio::task::spawn(async move {
            match con.statement("ROLLBACK", &state, &annotations).await {
                Ok(_) => {
                    log::debug!("rollback successful");
                }
                Err(e) => {
                    // let user know something happened
                    log::error!("rollback error: {}", e);
                }
            }
        });
    }
}

pub(crate) async fn retryable_transaction<T, B, F>(
    pool: &Pool,
    options: &Options,
    annotations: &Arc<Annotations>,
    mut body: B,
) -> Result<T, Error>
where
    B: FnMut(Transaction) -> F,
    F: Future<Output = Result<T, Error>>,
{
    let mut iteration = 0;
    'transaction: loop {
        let conn = pool.acquire().await?;
        let tx = Transaction::new(conn, options.state.clone(), annotations.clone());
        let result = body(tx).await;
        match result {
            Ok(val) => {
                log::debug!("transaction successful");
                return Ok(val);
            }
            Err(outer) => {
                log::debug!("transaction error");
                let some_retry = outer.chain().find_map(|e| {
                    e.downcast_ref::<Error>().and_then(|e| {
                        if e.has_tag(SHOULD_RETRY) {
                            Some(e)
                        } else {
                            None
                        }
                    })
                });

                match some_retry {
                    None => {
                        log::trace!("transaction NOT set as SHOULD_RETRY");
                        return Err(outer);
                    }
                    Some(e) => {
                        log::trace!("transaction set as SHOULD_RETRY");
                        let rule = options.retry.get_rule(e);
                        if iteration >= rule.attempts {
                            log::trace!("max retry count reached");
                            return Err(outer);
                        } else {
                            log::info!("Retrying transaction on {:#}", e);
                            log::trace!("iteration: {}", iteration);
                            iteration += 1;
                            sleep((rule.backoff)(iteration)).await;
                            continue 'transaction;
                        }
                    }
                }
            }
        }
    }
}

impl Transaction {
    pub(crate) fn new(
        conn: PoolConnection,
        state: Arc<PoolState>,
        annotations: Arc<Annotations>,
    ) -> Self {
        Self {
            state,
            annotations,
            conn: Some(conn),
            started: false,
            explicitly_commited_or_rolled_back: false,
        }
    }

    /// Commits the transaction.
    pub async fn commit(mut self) -> Result<(), Error> {
        log::debug!("Commiting transaction");
        self.explicitly_commited_or_rolled_back = true;
        let mut con = self.conn.take().expect("con always exist before drop");
        if self.started {
            log::trace!("transaction was started, commiting");
            con.statement("COMMIT", &self.state, &self.annotations)
                .await?;
        } else {
            log::trace!("transaction was not started, nothing to commit");
        }
        Ok(())
    }

    /// Explicitly rolls back a transaction.
    ///
    /// This should be preferable to dropping the transaction struct since this allows the user to check for errors.
    pub async fn rollback(mut self) -> Result<(), Error> {
        log::debug!("Rolling back transaction");
        self.explicitly_commited_or_rolled_back = true;
        let mut con = self.conn.take().expect("con always exist before drop");
        if self.started {
            log::trace!("transaction was started, rolling back");
            con.statement("ROLLBACK", &self.state, &self.annotations)
                .await?;
        } else {
            log::trace!("transaction was not started, nothing to rollback");
        }
        Ok(())
    }

    async fn ensure_started(&mut self) -> anyhow::Result<(), Error> {
        if let Some(conn) = &mut self.conn {
            if !self.started {
                conn.statement("START TRANSACTION", &self.state, &self.annotations)
                    .await?;
                self.started = true;
            }
            return Ok(());
        }
        // Is this needed? This should be unreachable since drops never calls it.
        Err(ClientError::with_message("using transaction after drop"))
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

        let conn = self
            .conn
            .as_mut()
            .take()
            .expect("con always exist before drop");

        conn.inner()
            .query(
                query.as_ref(),
                arguments,
                &self.state,
                &self.annotations,
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
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
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
            input_language: InputLanguage::EdgeQL,
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
        };
        let state = self.state.clone(); // TODO: optimize, by careful borrow

        let conn = self
            .conn
            .as_mut()
            .take()
            .expect("con always exist before drop");
        let desc = conn.parse(&flags, query, &state, &self.annotations).await?;
        let inp_desc = desc.input().map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        conn.execute(
            &flags,
            query,
            &state,
            &self.annotations,
            &desc,
            &arg_buf.freeze(),
        )
        .await?;
        Ok(())
    }
}
