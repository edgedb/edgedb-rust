use bytes::Bytes;

use edgedb_protocol::QueryResult;
use edgedb_protocol::query_arg::QueryArgs;
use edgedb_protocol::descriptors::OutputTypedesc;

use crate::errors::{Error, NoResultExpected, ErrorKind};
use crate::Pool;
use crate::client::StatementParams;

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

pub trait Encoder: Send + Sync {
}
pub trait Decoder: Send + Sync {
}
pub trait Decodable {
}
#[async_trait::async_trait]
pub trait Sealed {
    async fn query_dynamic(&mut self, query: &dyn GenericQuery)
        -> Result<GenericResult, Error>;
}
pub trait Executor: Sealed {
}

#[async_trait::async_trait]
impl Sealed for Pool {
    async fn query_dynamic(&mut self, query: &dyn GenericQuery)
        -> Result<GenericResult, Error>
    {
        // TODO(tailhook) retry loop
        let mut conn = self.inner.acquire().await?;
        conn.query_dynamic(query).await
    }
}

impl Executor for Pool {}

impl dyn Executor + '_ {
    pub async fn query<R, A>(&mut self, query: &str, arguments: &A)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        let result = self.query_dynamic(&Statement {
            params: StatementParams::new(),
            query,
            arguments,
        }).await?;
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
            None => {
                Err(NoResultExpected::with_message(
                    String::from_utf8_lossy(&result.completion[..])
                    .to_string()))?
            }
        }
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
