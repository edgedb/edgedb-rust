use std::sync::Arc;

use async_std::stream::StreamExt;

use edgedb_protocol::query_arg::QueryArgs;
use edgedb_protocol::QueryResult;

use crate::client::{Connection, StatementParams};
use crate::errors::{Error, ErrorKind, NoResultExpected};
use crate::pool::PoolInner;
use crate::traits::{GenericQuery, GenericResult};

pub(crate) struct PoolConn {
    pub conn: Option<Connection>,
    pub pool: Arc<PoolInner>,
}

impl PoolConn {
    pub async fn query<R, A>(
        &mut self,
        request: &str,
        arguments: &A,
        bld: &StatementParams,
    ) -> Result<Vec<R>, Error>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        let mut seq = self.conn.as_mut().unwrap().start_sequence().await?;
        let desc = seq._query(request, arguments, bld).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                let ctx = desc.as_queryable_context();
                let state = R::prepare(&ctx, root_pos)?;

                let mut items = seq.response(state);
                let mut res = Vec::new();
                while let Some(item) = items.next().await.transpose()? {
                    res.push(item);
                }
                Ok(res)
            }
            None => {
                let completion_message = seq._process_exec().await?;
                Err(NoResultExpected::with_message(
                    String::from_utf8_lossy(&completion_message[..]).to_string(),
                ))?
            }
        }
    }

    pub async fn query_dynamic(
        &mut self,
        query: &dyn GenericQuery,
    ) -> Result<GenericResult, Error> {
        let mut seq = self.conn.as_mut().unwrap().start_sequence().await?;
        let desc = seq
            ._query(query.query(), query.arguments(), query.params())
            .await?;
        if desc.root_pos().is_some() {
            let (data, completion) = seq.response_blobs().await?;
            Ok(GenericResult {
                descriptor: desc,
                data,
                completion,
            })
        } else {
            let completion_message = seq._process_exec().await?;
            Ok(GenericResult {
                descriptor: desc,
                data: Vec::new(),
                completion: completion_message,
            })
        }
    }
}

impl Drop for PoolConn {
    fn drop(&mut self) {
        self.pool.release(self.conn.take().unwrap());
    }
}
