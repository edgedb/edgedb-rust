use std::future::Future;

use bytes::BytesMut;
use edgedb_protocol::QueryResult;
use edgedb_protocol::common::CompilationFlags;
use edgedb_protocol::common::{IoFormat, Capabilities, Cardinality};
use edgedb_protocol::model::Json;
use edgedb_protocol::query_arg::{QueryArgs, Encoder};
use tokio::sync::oneshot;

use crate::errors::{ClientError};
use crate::errors::{Error, ErrorKind};
use crate::errors::{ProtocolEncodingError, NoResultExpected, NoDataError};
use crate::raw::{Pool, Connection};

pub struct Transaction(Option<Inner>);

pub struct TransactionResult {
    conn: Connection,
    started: bool,
}

pub struct Inner {
    started: bool,
    conn: Connection,
    return_conn: oneshot::Sender<TransactionResult>,
}

trait Assert: Send {}
impl Assert for Transaction {}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.0.take().map(|Inner { started, conn, return_conn }| {
            return_conn.send(TransactionResult {
                started,
                conn,
            }).ok()
        });
    }
}

pub(crate) async fn transaction<T, B, F>(pool: &Pool, mut body: B)
    -> Result<T, Error>
        where B: FnMut(Transaction) -> F,
              F: Future<Output=Result<T, Error>>,
{
    loop {
        let conn = pool.acquire().await?;
        let (tx, mut rx) = oneshot::channel();
        let tran = Transaction(Some(Inner {
            started: false,
            conn,
            return_conn: tx,
        }));
        let result = body(tran).await;
        let TransactionResult { mut conn, started } =
            rx.try_recv().expect("Transaction object must \
            be dropped by the time transaction body finishes.");
        match result {
            Ok(val) => {
                if started {
                    log::debug!("Comitting transaction");
                    conn.statement("COMMIT").await?;
                }
                return Ok(val)
            }
            Err(e) => {
                if started {
                    log::debug!("Rolling back transaction on error");
                    conn.statement("ROLLBACK").await?;
                }
                return Err(e);
            }
        }
    }
}

impl Transaction {

    async fn ensure_started(&mut self) -> anyhow::Result<(), Error> {
        if let Some(inner) = &mut self.0 {
            if !inner.started {
                inner.conn.statement("START TRANSACTION").await?;
                inner.started = true;
            }
            return Ok(());
        }
        Err(ClientError::with_message("using transaction after drop"))
    }
    fn inner(&mut self) -> &mut Inner {
        self.0.as_mut().expect("transaction object is not dropped")
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
    pub async fn query<R, A>(&mut self, query: &str, arguments: &A)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        self.ensure_started().await?;
        let flags = CompilationFlags {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::Many,
        };
        let ref mut conn = self.inner().conn;
        let _prepare = conn.prepare(&flags, query).await?;
        let desc = conn.describe_data().await?;
        let inp_desc = desc.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn.execute(&arg_buf.freeze()).await?;

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
    pub async fn query_single<R, A>(&mut self, query: &str, arguments: &A)
        -> Result<Option<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        self.ensure_started().await?;
        let flags = CompilationFlags {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Binary,
            expected_cardinality: Cardinality::AtMostOne,
        };
        let ref mut conn = self.inner().conn;
        let _prepare = conn.prepare(&flags, query).await?;
        let desc = conn.describe_data().await?;
        let inp_desc = desc.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn.execute(&arg_buf.freeze()).await?;

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
    pub async fn query_required_single<R, A>(&mut self, query: &str, arguments: &A)
        -> Result<R, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        self.query_single(query, arguments).await?
            .ok_or_else(|| NoDataError::with_message(
                        "query row returned zero results"))
    }

    /// Execute a query and return the result as JSON.
    pub async fn query_json(&mut self, query: &str, arguments: &impl QueryArgs)
        -> Result<Json, Error>
    {
        self.ensure_started().await?;
        let flags = CompilationFlags {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Json,
            expected_cardinality: Cardinality::Many,
        };
        let ref mut conn = self.inner().conn;
        let _prepare = conn.prepare(&flags, query).await?;
        let desc = conn.describe_data().await?;
        let inp_desc = desc.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn.execute(&arg_buf.freeze()).await?;

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
    pub async fn query_single_json(&mut self,
                                   query: &str, arguments: &impl QueryArgs)
        -> Result<Option<Json>, Error>
    {
        self.ensure_started().await?;
        let flags = CompilationFlags {
            implicit_limit: None,
            implicit_typenames: false,
            implicit_typeids: false,
            explicit_objectids: true,
            allow_capabilities: Capabilities::MODIFICATIONS,
            io_format: IoFormat::Json,
            expected_cardinality: Cardinality::AtMostOne,
        };
        let ref mut conn = self.inner().conn;
        let _prepare = conn.prepare(&flags, query).await?;
        let desc = conn.describe_data().await?;
        let inp_desc = desc.input()
            .map_err(ProtocolEncodingError::with_source)?;

        let mut arg_buf = BytesMut::with_capacity(8);
        arguments.encode(&mut Encoder::new(
            &inp_desc.as_query_arg_context(),
            &mut arg_buf,
        ))?;

        let data = conn.execute(&arg_buf.freeze()).await?;

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
    pub async fn query_required_single_json(&mut self,
                                   query: &str, arguments: &impl QueryArgs)
        -> Result<Json, Error>
    {
        self.query_single_json(query, arguments).await?
            .ok_or_else(|| NoDataError::with_message(
                        "query row returned zero results"))
    }
}
