use std::future::Future;
use std::sync::Arc;

use gel_dsn::gel::Config;
use gel_protocol::common::{Capabilities, Cardinality, IoFormat};
use gel_protocol::model::Json;
use gel_protocol::query_arg::QueryArgs;
use gel_protocol::QueryResult;
use tokio::time::sleep;

use crate::errors::InvalidArgumentError;
use crate::errors::NoDataError;
use crate::errors::{Error, ErrorKind, SHOULD_RETRY};
use crate::options::{RetryOptions, TransactionOptions};
use crate::raw::{Options, PoolState, Response};
use crate::raw::{Pool, QueryCapabilities};
use crate::state::{AliasesDelta, ConfigDelta, GlobalsDelta};
use crate::state::{AliasesModifier, ConfigModifier, Fn, GlobalsModifier};
use crate::transaction;
use crate::ResultVerbose;

/// Gel database client.
///
/// Internally it contains a connection pool.
///
/// To create a client, use [`create_client`](crate::create_client) function (it
/// gets database connection configuration from environment). You can also use
/// [`Builder`](crate::Builder) to [`build`](`crate::Builder::new`) custom
/// [`Config`] and [create a client](Client::new) using that config.
///
/// The `with_` methods ([`with_retry_options`](crate::Client::with_retry_options), [`with_transaction_options`](crate::Client::with_transaction_options), etc.)
/// let you create a shallow copy of the client with adjusted options.
#[derive(Debug, Clone)]
pub struct Client {
    options: Arc<Options>,
    pool: Pool,
}

impl Client {
    /// Create a new connection pool.
    ///
    /// Note this does not create a connection immediately.
    /// Use [`ensure_connected()`][Client::ensure_connected] to establish a
    /// connection and verify that the connection is usable.
    pub fn new(config: &Config) -> Client {
        Client {
            options: Default::default(),
            pool: Pool::new(config),
        }
    }

    /// Ensure that there is at least one working connection to the pool.
    ///
    /// This can be used at application startup to ensure that you have a
    /// working connection.
    pub async fn ensure_connected(&self) -> Result<(), Error> {
        self.pool.acquire().await?;
        Ok(())
    }

    /// Query with retry.
    async fn query_helper<R, A>(
        &self,
        query: impl AsRef<str>,
        arguments: &A,
        io_format: IoFormat,
        cardinality: Cardinality,
    ) -> Result<Response<Vec<R>>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        let mut iteration = 0;
        loop {
            let mut conn = self.pool.acquire().await?;

            let conn = conn.inner();
            let state = &self.options.state;
            let caps = Capabilities::MODIFICATIONS | Capabilities::DDL;
            match conn
                .query(
                    query.as_ref(),
                    arguments,
                    state,
                    &self.options.annotations,
                    caps,
                    io_format,
                    cardinality,
                )
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    let allow_retry = match e.get::<QueryCapabilities>() {
                        // Error from a weird source, or just a bug
                        // Let's keep on the safe side
                        None => false,
                        Some(QueryCapabilities::Unparsed) => true,
                        Some(QueryCapabilities::Parsed(c)) => c.is_empty(),
                    };
                    if allow_retry && e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Execute a query and return a collection of results and warnings produced by the server.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting: (Vec<String>, _) = conn.query_with_warnings("select 'hello'", &()).await?;
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_verbose<R, A>(
        &self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> Result<ResultVerbose<Vec<R>>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        Client::query_helper(self, query, arguments, IoFormat::Binary, Cardinality::Many)
            .await
            .map(|Response { data, warnings, .. }| ResultVerbose { data, warnings })
    }

    /// Execute a query and return a collection of results.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query::<String, _>("SELECT 'hello'", &());
    /// // or
    /// let greeting: Vec<String> = pool.query("SELECT 'hello'", &());
    ///
    /// let two_numbers: Vec<i32> = conn.query("select {<int32>$0, <int32>$1}", &(10, 20)).await?;
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query<R, A>(
        &self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> Result<Vec<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        Client::query_helper(self, query, arguments, IoFormat::Binary, Cardinality::Many)
            .await
            .map(|r| r.data)
    }

    /// Execute a query and return a single result
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query_single::<String, _>("SELECT 'hello'", &());
    /// // or
    /// let greeting: Option<String> = pool.query_single(
    ///     "SELECT 'hello'",
    ///     &()
    /// );
    /// ```
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_single<R, A>(
        &self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> Result<Option<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Client::query_helper(
            self,
            query,
            arguments,
            IoFormat::Binary,
            Cardinality::AtMostOne,
        )
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
    /// scalars, and with dynamic arguments [`gel_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_required_single<R, A>(
        &self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> Result<R, Error>
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Client::query_helper(
            self,
            query,
            arguments,
            IoFormat::Binary,
            Cardinality::AtMostOne,
        )
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
        &self,
        query: impl AsRef<str>,
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
    ///
    /// ```rust,ignore
    /// let query = "select <json>(
    ///     insert Account {
    ///     username := <str>$0
    ///     }) {
    ///     username,
    ///     id
    ///     };";
    /// let json_res: Option<Json> = client
    ///  .query_single_json(query, &("SomeUserName",))
    ///     .await?;
    /// ```
    pub async fn query_single_json(
        &self,
        query: impl AsRef<str>,
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
        &self,
        query: impl AsRef<str>,
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
    pub async fn execute<A>(&self, query: impl AsRef<str>, arguments: &A) -> Result<(), Error>
    where
        A: QueryArgs,
    {
        let mut iteration = 0;
        loop {
            let mut conn = self.pool.acquire().await?;

            let conn = conn.inner();
            let state = &self.options.state;
            let caps = Capabilities::MODIFICATIONS | Capabilities::DDL;
            match conn
                .execute(
                    query.as_ref(),
                    arguments,
                    state,
                    &self.options.annotations,
                    caps,
                )
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let allow_retry = match e.get::<QueryCapabilities>() {
                        // Error from a weird source, or just a bug
                        // Let's keep on the safe side
                        None => false,
                        Some(QueryCapabilities::Unparsed) => true,
                        Some(QueryCapabilities::Parsed(c)) => c.is_empty(),
                    };
                    if allow_retry && e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...", e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Execute a transaction and retry.
    ///
    /// Transaction body must be encompassed in the closure. The closure **may
    /// be executed multiple times**. This includes not only database queries
    /// but also executing the whole function, so the transaction code must be
    /// prepared to be idempotent.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn main_() -> Result<(), gel_tokio::Error> {
    /// let conn = gel_tokio::create_client().await?;
    /// let val = conn.transaction(|mut tx| async move {
    ///     tx.query_required_single::<i64, _>("
    ///         WITH C := UPDATE Counter SET { value := .value + 1}
    ///         SELECT C.value LIMIT 1
    ///         ", &()
    ///     ).await
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Commit and rollback
    ///
    /// If the closure returns [Result::Ok], the transaction is committed.
    /// If the closure returns [Result::Err], the transaction is either retried or aborted,
    /// depending on weather the error has `SHOULD_RETRY`` tag set.
    ///
    /// To manually abort a transaction, [gel_errors::UserError] can be returned:
    ///
    /// ```rust,no_run
    /// use gel_errors::ErrorKind;
    /// # async fn main_() -> Result<(), gel_tokio::Error> {
    /// # let conn = gel_tokio::create_client().await?;
    /// let val = conn.transaction(|mut tx| async move {
    ///     tx.execute("UPDATE Foo SET { x := 1 };", &()).await;
    ///     Err(gel_errors::UserError::build()) // abort transaction
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Returning custom errors
    ///
    /// See [this example](https://github.com/edgedb/edgedb-rust/blob/master/gel-tokio/examples/transaction_errors.rs)
    /// and [the documentation of the `gel-errors` crate](https://docs.rs/gel-errors/latest/gel_errors/)
    /// for how to return custom error types.
    ///
    /// # Panics
    ///
    /// Function panics when transaction object passed to the closure is not
    /// dropped after closure exists. General rule: do not store transaction
    /// anywhere and do not send to another coroutine. Pass to all further
    /// function calls by reference.
    pub async fn transaction<T, B, F>(&self, body: B) -> Result<T, Error>
    where
        B: FnMut(transaction::RetryingTransaction) -> F,
        F: Future<Output = Result<T, Error>>,
    {
        transaction::run_and_retry(&self.pool, self.options.clone(), body).await
    }

    /// Start a transaction without the retry mechanism.
    ///
    /// Returns [RawTransaction] which implements [crate::QueryExecutor] and can
    /// be used to execute queries within the transaction.
    ///
    /// The transaction will never retry failed queries, even if the database signals that the
    /// query should be retried. For this reason, it is recommended to use [Client::within_transaction]
    /// when possible.
    ///
    /// <div class="warning">
    /// Transactions can fail for benign reasons and should always handle that case gracefully.
    /// `RawTransaction` does not provide any retry mechanisms, so this responsibility falls
    /// onto the user. For example, even only two select queries in a transaction can fail due to
    /// concurrent modification of the database.
    /// </div>
    ///
    /// # Commit and rollback
    ///
    /// To commit the changes made during the transaction,
    /// [commit](crate::RawTransaction::commit) method must be called, otherwise the
    /// transaction will roll back when [RawTransaction] is dropped.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn main_() -> Result<(), gel_tokio::Error> {
    /// let conn = gel_tokio::create_client().await?;
    /// let mut tx = conn.transaction_raw().await?;
    /// tx.query_required_single::<i64, _>("
    ///     WITH C := UPDATE Counter SET { value := .value + 1}
    ///     SELECT C.value LIMIT 1
    ///     ", &()
    /// ).await;
    /// tx.commit().await;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "unstable")]
    pub async fn transaction_raw(&self) -> Result<transaction::RawTransaction, Error> {
        crate::transaction::start(&self.pool, self.options.clone()).await
    }

    /// Returns client with adjusted options for future transactions.
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified transaction options.
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    ///
    /// Transaction options are used by the ``transaction`` method.
    pub fn with_transaction_options(&self, options: TransactionOptions) -> Self {
        Client {
            options: Arc::new(Options {
                transaction: options,
                retry: self.options.retry.clone(),
                state: self.options.state.clone(),
                annotations: self.options.annotations.clone(),
            }),
            pool: self.pool.clone(),
        }
    }
    /// Returns client with adjusted options for future retrying
    /// transactions.
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified transaction options.
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    pub fn with_retry_options(&self, options: RetryOptions) -> Self {
        Client {
            options: Arc::new(Options {
                transaction: self.options.transaction.clone(),
                retry: options,
                state: self.options.state.clone(),
                annotations: self.options.annotations.clone(),
            }),
            pool: self.pool.clone(),
        }
    }

    fn with_state(&self, f: impl FnOnce(&PoolState) -> PoolState) -> Self {
        Client {
            options: Arc::new(Options {
                transaction: self.options.transaction.clone(),
                retry: self.options.retry.clone(),
                state: Arc::new(f(&self.options.state)),
                annotations: self.options.annotations.clone(),
            }),
            pool: self.pool.clone(),
        }
    }

    /// Returns the client with the specified global variables set
    ///
    /// Most commonly used with `#[derive(GlobalsDelta)]`.
    ///
    /// Note: this method is incremental, i.e. it adds (or removes) globals
    /// instead of setting a definite set of variables. Use
    /// `.with_globals(Unset(["name1", "name2"]))` to unset some variables.
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified global variables
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    pub fn with_globals(&self, globals: impl GlobalsDelta) -> Self {
        self.with_state(|s| s.with_globals(globals))
    }

    /// Returns the client with the specified global variables set
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified global variables
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    ///
    /// This is equivalent to `.with_globals(Fn(f))` but more ergonomic as it
    /// allows type inference for lambda.
    pub fn with_globals_fn(&self, f: impl FnOnce(&mut GlobalsModifier)) -> Self {
        self.with_state(|s| s.with_globals(Fn(f)))
    }

    /// Returns the client with the specified aliases set
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified aliases.
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    pub fn with_aliases(&self, aliases: impl AliasesDelta) -> Self {
        self.with_state(|s| s.with_aliases(aliases))
    }

    /// Returns the client with the specified aliases set
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified aliases.
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    ///
    /// This is equivalent to `.with_aliases(Fn(f))` but more ergonomic as it
    /// allows type inference for lambda.
    pub fn with_aliases_fn(&self, f: impl FnOnce(&mut AliasesModifier)) -> Self {
        self.with_state(|s| s.with_aliases(Fn(f)))
    }

    /// Returns the client with the default module set or unset
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified default module.
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    pub fn with_default_module(&self, module: Option<impl Into<String>>) -> Self {
        self.with_state(|s| s.with_default_module(module.map(|m| m.into())))
    }

    /// Returns the client with the specified config
    ///
    /// Note: this method is incremental, i.e. it adds (or removes) individual
    /// settings instead of setting a definite configuration. Use
    /// `.with_config(Unset(["name1", "name2"]))` to unset some settings.
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified global variables
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    pub fn with_config(&self, cfg: impl ConfigDelta) -> Self {
        self.with_state(|s| s.with_config(cfg))
    }

    /// Returns the client with the specified config
    ///
    /// Most commonly used with `#[derive(ConfigDelta)]`.
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified global variables
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    ///
    /// This is equivalent to `.with_config(Fn(f))` but more ergonomic as it
    /// allows type inference for lambda.
    pub fn with_config_fn(&self, f: impl FnOnce(&mut ConfigModifier)) -> Self {
        self.with_state(|s| s.with_config(Fn(f)))
    }

    /// Returns the client with the specified query tag.
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified query tag.
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them query tag applied will be different.
    pub fn with_tag(&self, tag: Option<&str>) -> Result<Self, Error> {
        const KEY: &str = "tag";

        let annotations = if self.options.annotations.get(KEY).map(|s| s.as_str()) != tag {
            let mut annotations = (*self.options.annotations).clone();
            if let Some(tag) = tag {
                if tag.starts_with("edgedb/") {
                    return Err(InvalidArgumentError::with_message("reserved tag: edgedb/*"));
                }
                if tag.starts_with("gel/") {
                    return Err(InvalidArgumentError::with_message("reserved tag: gel/*"));
                }
                if tag.len() > 128 {
                    return Err(InvalidArgumentError::with_message(
                        "tag too long (> 128 bytes)",
                    ));
                }
                annotations.insert(KEY.to_string(), tag.to_string());
            } else {
                annotations.remove(KEY);
            }
            Arc::new(annotations)
        } else {
            self.options.annotations.clone()
        };

        Ok(Client {
            options: Arc::new(Options {
                transaction: self.options.transaction.clone(),
                retry: self.options.retry.clone(),
                state: self.options.state.clone(),
                annotations,
            }),
            pool: self.pool.clone(),
        })
    }
}
