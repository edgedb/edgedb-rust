use std::sync::Arc;
use std::future::Future;

use bytes::BytesMut;
use edgedb_protocol::model::Json;
use edgedb_protocol::common::CompilationOptions;
use edgedb_protocol::common::{IoFormat, Capabilities, Cardinality};
use edgedb_protocol::query_arg::{QueryArgs, Encoder};
use edgedb_protocol::QueryResult;

use crate::raw::Pool;
use crate::builder::Config;
use crate::errors::{Error, ErrorKind};
use crate::errors::{ProtocolEncodingError, NoResultExpected, NoDataError};
use crate::transaction::{Transaction, transaction};
use crate::options::{TransactionOptions, RetryOptions};
use crate::raw::Options;

/// EdgeDB Client
///
/// Internally it contains a connection pool.
///
/// To create client, use [`create_client`](crate::create_client) function (it
/// gets database connection configuration from environment). You can also use
/// [`Builder`](crate::Builder) to [`build`](`crate::Builder::build`) custom
/// [`Config`] and [create a client](Client::new) using that config.
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
        let mut conn = self.pool.acquire().await?;

        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
        };
        let desc = conn.parse(&flags, query).await?;
        let inp_desc = desc.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn.execute(&flags, query, &desc, &arg_buf.freeze()).await?;

        let out_desc = desc.output()
            .map_err(ProtocolEncodingError::with_source)?;
        match out_desc.root_pos() {
            Some(root_pos) => {
                let ctx = out_desc.as_queryable_context();
                let mut state = R::prepare(&ctx, root_pos)?;
                let rows = data.into_iter()
                    .flat_map(|chunk| chunk.data)
                    .map(|chunk| R::decode(&mut state, &chunk))
                    .collect::<Result<_, _>>()?;
                Ok(rows)
            }
            None => Err(NoResultExpected::build()),
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
        let mut conn = self.pool.acquire().await?;

        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::AtMostOne,
        };
        let desc = conn.parse(&flags, query).await?;
        let inp_desc = desc.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn.execute(&flags, query, &desc, &arg_buf.freeze()).await?;

        let out_desc = desc.output()
            .map_err(ProtocolEncodingError::with_source)?;
        match out_desc.root_pos() {
            Some(root_pos) => {
                let ctx = out_desc.as_queryable_context();
                let mut state = R::prepare(&ctx, root_pos)?;
                let bytes = data.into_iter().next()
                    .and_then(|chunk| chunk.data.into_iter().next());
                if let Some(bytes) = bytes {
                    Ok(Some(R::decode(&mut state, &bytes)?))
                } else {
                    Ok(None)
                }
            }
            None => Err(NoResultExpected::build()),
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
        let mut conn = self.pool.acquire().await?;

        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Json,
            expected_cardinality: Cardinality::Many,
        };
        let desc = conn.parse(&flags, query).await?;
        let inp_desc = desc.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn.execute(&flags, query, &desc, &arg_buf.freeze()).await?;

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
                    Ok(unsafe { Json::new_unchecked(s) })
                } else {
                    Err(NoDataError::with_message(
                        "query row returned zero results"))
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
    pub async fn query_single_json(&self,
                                   query: &str, arguments: &impl QueryArgs)
        -> Result<Option<Json>, Error>
    {
        let mut conn = self.pool.acquire().await?;

        let flags = CompilationOptions {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Json,
            expected_cardinality: Cardinality::AtMostOne,
        };
        let desc = conn.parse(&flags, query).await?;
        let inp_desc = desc.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn.execute(&flags, query, &desc, &arg_buf.freeze()).await?;

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
                    Ok(Some(unsafe { Json::new_unchecked(s) }))
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
    pub async fn query_required_single_json(&self,
                                   query: &str, arguments: &impl QueryArgs)
        -> Result<Json, Error>
    {
        self.query_single_json(query, arguments).await?
            .ok_or_else(|| NoDataError::with_message(
                        "query row returned zero results"))
    }

    /// Execute a transaction
    ///
    /// Transaction body must be encompassed in the closure. The closure **may
    /// be executed multiple times**. This includes not only database queries
    /// but also executing the whole function, so the transaction code must be
    /// prepared to be idempotent.
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
    pub async fn transaction<T, B, F>(self, body: B) -> Result<T, Error>
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
            }),
            pool: self.pool.clone(),
        }
    }
}
