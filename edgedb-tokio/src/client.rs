use std::sync::Arc;
use std::future::Future;

use bytes::BytesMut;
use edgedb_protocol::model::Json;
use edgedb_protocol::common::CompilationOptions;
use edgedb_protocol::common::{IoFormat, Capabilities, Cardinality};
use edgedb_protocol::query_arg::{QueryArgs, Encoder};
use edgedb_protocol::QueryResult;
use tokio::time::sleep;

use crate::raw::{Pool, QueryCapabilities};
use crate::builder::Config;
use crate::errors::{Error, ErrorKind, SHOULD_RETRY};
use crate::errors::{ProtocolEncodingError, NoResultExpected, NoDataError};
use crate::transaction::{Transaction, transaction};
use crate::options::{TransactionOptions, RetryOptions};
use crate::raw::{Options, PoolState};
use crate::state::{AliasesDelta, GlobalsDelta, ConfigDelta};
use crate::state::{AliasesModifier, GlobalsModifier, ConfigModifier, Fn};


/// The EdgeDB Client.
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
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query<R, A>(&self, query: &str, arguments: &A)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        let mut iteration = 0;
        loop {
            let mut conn = self.pool.acquire().await?;

            let conn = conn.inner();
            let state = &self.options.state;
            let caps = Capabilities::MODIFICATIONS | Capabilities::DDL;
            match conn.query(query, arguments, state, caps).await {
                Ok(resp) => return Ok(resp.data),
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
                            log::info!("Error: {:#}. Retrying in {:?}...",
                                       e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
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
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_single<R, A>(&self, query: &str, arguments: &A)
        -> Result<Option<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        let mut iteration = 0;
        loop {
            let mut conn = self.pool.acquire().await?;
            let conn = conn.inner();
            let state = &self.options.state;
            let caps = Capabilities::MODIFICATIONS | Capabilities::DDL;
            match conn.query_single(query, arguments, state, caps).await {
                Ok(resp) => return Ok(resp.data),
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
                            log::info!("Error: {:#}. Retrying in {:?}...",
                                       e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
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
    pub async fn query_required_single<R, A>(&self, query: &str, arguments: &A)
        -> Result<R, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        self.query_single(query, arguments).await?
            .ok_or_else(|| NoDataError::with_message(
                        "query row returned zero results"))
    }

    /// Execute a query and return the result as JSON.
    pub async fn query_json(&self, query: &str, arguments: &impl QueryArgs)
        -> Result<Json, Error>
    {
        let mut iteration = 0;
        loop {
            let mut conn = self.pool.acquire().await?;

            let flags = CompilationOptions {
                implicit_limit: None,
                implicit_typenames: false,
                implicit_typeids: false,
                explicit_objectids: true,
                allow_capabilities: Capabilities::MODIFICATIONS | Capabilities::DDL,
                io_format: IoFormat::Json,
                expected_cardinality: Cardinality::Many,
            };
            let desc = match conn.parse(&flags, query, &self.options.state).await {
                Ok(parsed) => parsed,
                Err(e) => {
                    if e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...",
                                       e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            };
            let inp_desc = desc.input()
                .map_err(ProtocolEncodingError::with_source)?;

            let mut arg_buf = BytesMut::with_capacity(8);
            arguments.encode(&mut Encoder::new(
                &inp_desc.as_query_arg_context(),
                &mut arg_buf,
            ))?;

            let res = conn.execute(
                    &flags, query, &self.options.state, &desc, &arg_buf.freeze(),
                ).await;
            let data = match res {
                Ok(data) => data,
                Err(e) => {
                    dbg!(&e, e.has_tag(SHOULD_RETRY));
                    if desc.capabilities == Capabilities::empty() &&
                        e.has_tag(SHOULD_RETRY)
                    {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...",
                                       e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            };

            let out_desc = desc.output()
                .map_err(ProtocolEncodingError::with_source)?;
            match out_desc.root_pos() {
                Some(root_pos) => {
                    let ctx = out_desc.as_queryable_context();
                    // JSON objects are returned as strings :(
                    let mut state = String::prepare(&ctx, root_pos)?;
                    let bytes = data.into_iter().next()
                        .and_then(|chunk| chunk.data.into_iter().next());
                    if let Some(bytes) = bytes {
                        // we trust database to produce valid json
                        let s = String::decode(&mut state, &bytes)?;
                        return Ok(Json::new_unchecked(s))
                    } else {
                        return Err(NoDataError::with_message(
                            "query row returned zero results"))
                    }
                }
                None => return Err(NoResultExpected::build()),
            }
        }
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
    pub async fn query_single_json(&self,
                                   query: &str, arguments: &impl QueryArgs)
        -> Result<Option<Json>, Error>
    {
        let mut iteration = 0;
        loop {
            let mut conn = self.pool.acquire().await?;

            let flags = CompilationOptions {
                implicit_limit: None,
                implicit_typenames: false,
                implicit_typeids: false,
                explicit_objectids: true,
                allow_capabilities: Capabilities::MODIFICATIONS | Capabilities::DDL,
                io_format: IoFormat::Json,
                expected_cardinality: Cardinality::AtMostOne,
            };
            let desc = match conn.parse(&flags, query, &self.options.state).await {
                Ok(parsed) => parsed,
                Err(e) => {
                    if e.has_tag(SHOULD_RETRY) {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...",
                                       e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            };
            let inp_desc = desc.input()
                .map_err(ProtocolEncodingError::with_source)?;

            let mut arg_buf = BytesMut::with_capacity(8);
            arguments.encode(&mut Encoder::new(
                &inp_desc.as_query_arg_context(),
                &mut arg_buf,
            ))?;

            let res = conn.execute(
                    &flags, query, &self.options.state, &desc, &arg_buf.freeze(),
                ).await;
            let data = match res {
                Ok(data) => data,
                Err(e) => {
                    if desc.capabilities == Capabilities::empty() &&
                        e.has_tag(SHOULD_RETRY)
                    {
                        let rule = self.options.retry.get_rule(&e);
                        iteration += 1;
                        if iteration < rule.attempts {
                            let duration = (rule.backoff)(iteration);
                            log::info!("Error: {:#}. Retrying in {:?}...",
                                       e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            };

            let out_desc = desc.output()
                .map_err(ProtocolEncodingError::with_source)?;
            match out_desc.root_pos() {
                Some(root_pos) => {
                    let ctx = out_desc.as_queryable_context();
                    // JSON objects are returned as strings :(
                    let mut state = String::prepare(&ctx, root_pos)?;
                    let bytes = data.into_iter().next()
                        .and_then(|chunk| chunk.data.into_iter().next());
                    if let Some(bytes) = bytes {
                        // we trust database to produce valid json
                        let s = String::decode(&mut state, &bytes)?;
                        return Ok(Some(Json::new_unchecked(s)))
                    } else {
                        return Ok(None)
                    }
                }
                None => return Err(NoResultExpected::build()),
            }
        }
    }

    /// Execute a query and return a single result as JSON.
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    pub async fn query_required_single_json(&self,
                                   query: &str, arguments: &impl QueryArgs)
        -> Result<Json, Error>
    {
        self.query_single_json(query, arguments).await?
            .ok_or_else(|| NoDataError::with_message(
                        "query row returned zero results"))
    }

    /// Execute a query and don't expect result
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn execute<A>(&self, query: &str, arguments: &A)
        -> Result<(), Error>
        where A: QueryArgs,
    {
        let mut iteration = 0;
        loop {
            let mut conn = self.pool.acquire().await?;

            let conn = conn.inner();
            let state = &self.options.state;
            let caps = Capabilities::MODIFICATIONS | Capabilities::DDL;
            match conn.execute(query, arguments, state, caps).await {
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
                            log::info!("Error: {:#}. Retrying in {:?}...",
                                       e, duration);
                            sleep(duration).await;
                            continue;
                        }
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Execute a transaction
    ///
    /// Transaction body must be encompassed in the closure. The closure **may
    /// be executed multiple times**. This includes not only database queries
    /// but also executing the whole function, so the transaction code must be
    /// prepared to be idempotent.
    ///
    /// # Returning custom errors
    ///
    /// See [this example](https://github.com/edgedb/edgedb-rust/blob/master/edgedb-tokio/examples/transaction_errors.rs)
    /// and [the documentation of the `edgedb_errors` crate](https://docs.rs/edgedb-errors/latest/edgedb_errors/)
    /// for how to return custom error types.
    ///
    /// # Panics
    ///
    /// Function panics when transaction object passed to the closure is not
    /// dropped after closure exists. General rule: do not store transaction
    /// anywhere and do not send to another coroutine. Pass to all further
    /// function calls by reference.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn transaction() -> Result<(), edgedb_tokio::Error> {
    /// let conn = edgedb_tokio::create_client().await?;
    /// let val = conn.transaction(|mut tx| async move {
    ///     tx.query_required_single::<i64, _>("
    ///         WITH C := UPDATE Counter SET { value := .value + 1}
    ///         SELECT C.value LIMIT 1
    ///     ", &()
    ///     ).await
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn transaction<T, B, F>(&self, body: B) -> Result<T, Error>
        where B: FnMut(Transaction) -> F,
              F: Future<Output=Result<T, Error>>,
    {
        transaction(&self.pool, &self.options, body).await
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
    pub fn with_transaction_options(&self, options: TransactionOptions)
        -> Self
    {
        Client {
            options: Arc::new(Options {
                transaction: options,
                retry: self.options.retry.clone(),
                state: self.options.state.clone(),
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
    pub fn with_retry_options(&self, options: RetryOptions)
        -> Self
    {
        Client {
            options: Arc::new(Options {
                transaction: self.options.transaction.clone(),
                retry: options,
                state: self.options.state.clone(),
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
    pub fn with_globals_fn(&self, f: impl FnOnce(&mut GlobalsModifier))
        -> Self
    {
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
    pub fn with_aliases_fn(&self, f: impl FnOnce(&mut AliasesModifier))
        -> Self
    {
        self.with_state(|s| s.with_aliases(Fn(f)))
    }

    /// Returns the client with the default module set or unset
    ///
    /// This method returns a "shallow copy" of the current client
    /// with modified default module.
    ///
    /// Both ``self`` and returned client can be used after, but when using
    /// them transaction options applied will be different.
    pub fn with_default_module(&self, module: Option<impl Into<String>>)
        -> Self
    {
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
    pub fn with_config_fn(&self, f: impl FnOnce(&mut ConfigModifier))
        -> Self
    {
        self.with_state(|s| s.with_config(Fn(f)))
    }
}
