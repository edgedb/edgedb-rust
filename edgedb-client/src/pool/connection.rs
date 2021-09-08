use std::sync::Arc;

use bytes::Bytes;

use async_std::channel::{Sender};
use async_std::task::JoinHandle;
use async_std::sync::Mutex;
use async_std::stream::StreamExt;

use edgedb_protocol::QueryResult;
use edgedb_protocol::query_arg::QueryArgs;

use crate::errors::{Error, ErrorKind, NoResultExpected};
use crate::client::{Connection, StatementBuilder};


pub(crate) struct PoolConn {
    conn: Connection,
}

impl PoolConn {
    pub async fn query<R, A>(&mut self, request: &str, arguments: &A,
        bld: &StatementBuilder)
        -> Result<Vec<R>, Error>
        where A: QueryArgs,
              R: QueryResult,
    {
        let mut seq = self.conn.start_sequence().await?;
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
