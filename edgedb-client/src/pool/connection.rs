use std::sync::Arc;

use async_std::stream::StreamExt;

use edgedb_protocol::QueryResult;
use edgedb_protocol::query_arg::QueryArgs;

use crate::errors::{Error, ErrorKind, NoResultExpected};
use crate::client::{Connection, StatementBuilder};
use crate::pool::PoolInner;


pub(crate) struct PoolConn {
    conn: Option<Connection>,
    pool: Arc<PoolInner>,
}

impl PoolConn {
    pub async fn query<R, A>(&mut self, request: &str, arguments: &A,
        bld: &StatementBuilder)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        let mut seq = self.conn.as_mut().unwrap().start_sequence().await?;
        let desc = seq._query(request, arguments, bld).await?;
        match desc.root_pos() {
            Some(root_pos) => {
                let mut ctx = desc.as_queryable_context();
                ctx.has_implicit_tid = seq.proto.has_implicit_tid();
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
                    String::from_utf8_lossy(&completion_message[..])
                    .to_string()))?
            }
        }
    }
}

impl Drop for PoolConn {
    fn drop(&mut self) {
        self.pool.release(self.conn.take().unwrap());
    }
}
