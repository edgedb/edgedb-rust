use bytes::Bytes;

use edgedb_protocol::client_message::{Cardinality, IoFormat};
use edgedb_protocol::descriptors::OutputTypedesc;
use edgedb_protocol::query_arg::QueryArgs;
use edgedb_protocol::QueryResult;

use crate::client::StatementParams;
use crate::errors::{Error, ErrorKind, NoDataError, NoResultExpected};
use crate::model::Json;
use crate::Client;

/// Result returned from an [`execute()`][Executor#method.execute] call.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    pub(crate) marker: Bytes,
}

struct Statement<'a, A> {
    params: StatementParams,
    query: &'a str,
    arguments: &'a A,
}

pub struct GenericResult {
    pub(crate) descriptor: OutputTypedesc,
    pub(crate) data: Vec<Bytes>,
    pub(crate) completion: Bytes,
}

pub trait GenericQuery: Send + Sync {
    fn query(&self) -> &str;
    fn arguments(&self) -> &dyn QueryArgs;
    fn params(&self) -> &StatementParams;
}

pub trait Encoder: Send + Sync {}
pub trait Decoder: Send + Sync {}
pub trait Decodable {}
#[async_trait::async_trait]
pub trait Sealed {
    async fn query_dynamic(&mut self, query: &dyn GenericQuery) -> Result<GenericResult, Error>;
}

/// The main trait that allows query execution.
///
/// Note that compared to [Client] this trait has `&mut self` for query methods.
/// This is because we need to support [Executor] for a transaction.
/// To overcome this issue for [Client] you can either use inherent methods on
/// the pool rather than this trait or just clone it (cloning [Client] is cheap):
///
/// ```rust,ignore
/// do_query(&mut global_pool_reference.clone())?;
/// ```
/// Due to limitations of the Rust type system, the query methods are part of
/// the inherent implementation for `dyn Executor`, not in the trait
/// itself. This should not be a problem in most cases.
///
/// This trait is sealed (no imlementation can be done outside of this crate),
/// since we don't want to expose too many implementation details for now.
pub trait Executor: Sealed {}

#[async_trait::async_trait]
impl Sealed for Client {
    async fn query_dynamic(&mut self, query: &dyn GenericQuery) -> Result<GenericResult, Error> {
        // TODO(tailhook) retry loop
        let mut conn = self.inner.acquire().await?;
        conn.query_dynamic(query).await
    }
}

impl Executor for Client {}

impl dyn Executor + '_ {
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
    pub async fn query<R, A>(&mut self, query: &str, arguments: &A) -> Result<Vec<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        let result = self
            .query_dynamic(&Statement {
                params: StatementParams::new(),
                query,
                arguments,
            })
            .await?;
        match result.descriptor.root_pos() {
            Some(root_pos) => {
                let ctx = result.descriptor.as_queryable_context();
                let mut state = R::prepare(&ctx, root_pos)?;
                let mut res = Vec::with_capacity(result.data.len());
                for datum in result.data.into_iter() {
                    res.push(R::decode(&mut state, &datum)?);
                }
                Ok(res)
            }
            None => Err(NoResultExpected::with_message(
                String::from_utf8_lossy(&result.completion[..]).to_string(),
            ))?,
        }
    }

    /// Execute a query and return a single result.
    ///
    /// You will usually have to specify the return type for the query:
    ///
    /// ```rust,ignore
    /// let greeting = pool.query_single::<String, _>("SELECT 'hello'", &());
    /// // or
    /// let greeting: String = pool.query_single("SELECT 'hello'", &());
    /// ```
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    ///
    /// This method can be used with both static arguments, like a tuple of
    /// scalars, and with dynamic arguments [`edgedb_protocol::value::Value`].
    /// Similarly, dynamically typed results are also supported.
    pub async fn query_single<R, A>(&mut self, query: &str, arguments: &A) -> Result<R, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        let result = self
            .query_dynamic(&Statement {
                params: StatementParams::new()
                    .cardinality(Cardinality::AtMostOne)
                    .clone(),
                query,
                arguments,
            })
            .await?;
        match result.descriptor.root_pos() {
            Some(root_pos) => {
                let ctx = result.descriptor.as_queryable_context();
                let mut state = R::prepare(&ctx, root_pos)?;
                if result.data.len() == 0 {
                    return Err(NoDataError::with_message(
                        "query_single() returned zero results",
                    ));
                }
                return Ok(R::decode(&mut state, &result.data[0])?);
            }
            None => Err(NoResultExpected::with_message(
                String::from_utf8_lossy(&result.completion[..]).to_string(),
            ))?,
        }
    }

    /// Execute a query and return the result as JSON.
    pub async fn query_json<A>(&mut self, query: &str, arguments: &A) -> Result<Json, Error>
    where
        A: QueryArgs,
    {
        let result = self
            .query_dynamic(&Statement {
                params: StatementParams::new().io_format(IoFormat::Json).clone(),
                query,
                arguments,
            })
            .await?;
        match result.descriptor.root_pos() {
            Some(root_pos) => {
                let ctx = result.descriptor.as_queryable_context();
                let mut state = <String as QueryResult>::prepare(&ctx, root_pos)?;
                if result.data.len() == 0 {
                    return Err(NoDataError::with_message(
                        "query_json() returned zero results",
                    ));
                }
                let data = <String as QueryResult>::decode(&mut state, &result.data[0])?;
                // trust database to produce valid JSON
                let json = unsafe { Json::new_unchecked(data) };
                return Ok(json);
            }
            None => Err(NoResultExpected::with_message(
                String::from_utf8_lossy(&result.completion[..]).to_string(),
            ))?,
        }
    }

    /// Execute a query and return a single result as JSON.
    ///
    /// The query must return exactly one element. If the query returns more
    /// than one element, a
    /// [`ResultCardinalityMismatchError`][crate::errors::ResultCardinalityMismatchError]
    /// is raised. If the query returns an empty set, a
    /// [`NoDataError`][crate::errors::NoDataError] is raised.
    pub async fn query_single_json<A>(&mut self, query: &str, arguments: &A) -> Result<Json, Error>
    where
        A: QueryArgs,
    {
        let result = self
            .query_dynamic(&Statement {
                params: StatementParams::new()
                    .io_format(IoFormat::Json)
                    .cardinality(Cardinality::AtMostOne)
                    .clone(),
                query,
                arguments,
            })
            .await?;
        match result.descriptor.root_pos() {
            Some(root_pos) => {
                let ctx = result.descriptor.as_queryable_context();
                let mut state = <String as QueryResult>::prepare(&ctx, root_pos)?;
                if result.data.len() == 0 {
                    return Err(NoDataError::with_message(
                        "query_single_json() returned zero results",
                    ));
                }
                let data = <String as QueryResult>::decode(&mut state, &result.data[0])?;
                // trust database to produce valid JSON
                let json = unsafe { Json::new_unchecked(data) };
                return Ok(json);
            }
            None => Err(NoResultExpected::with_message(
                String::from_utf8_lossy(&result.completion[..]).to_string(),
            ))?,
        }
    }
    /// Execute one or more EdgeQL commands.
    ///
    /// Note that if you want the results of query, use
    /// [`query()`][Client::query] or [`query_single()`][Client::query_single]
    /// instead.
    pub async fn execute<A>(&mut self, query: &str, arguments: &A) -> Result<ExecuteResult, Error>
    where
        A: QueryArgs,
    {
        let result = self
            .query_dynamic(&Statement {
                params: StatementParams::new(),
                query,
                arguments,
            })
            .await?;
        // Dropping the actual results
        return Ok(ExecuteResult {
            marker: result.completion,
        });
    }
}

impl<A: QueryArgs + Send + Sync + Sized> GenericQuery for Statement<'_, A> {
    fn query(&self) -> &str {
        &self.query
    }
    fn arguments(&self) -> &dyn QueryArgs {
        self.arguments
    }
    fn params(&self) -> &StatementParams {
        &self.params
    }
}
