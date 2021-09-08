use std::sync::Arc;

use async_std::channel::{Receiver, RecvError};

use crate::pool::PoolState;
use crate::pool::command::Command;


pub(crate) async fn main(state: Arc<PoolState>, rcv: Receiver<Command>) {
    loop {
        match rcv.recv().await {
            Ok(Command::Release(conn)) => {
                todo!();
            }
            Ok(Command::Close) | Err(RecvError) => {
                // TODO(tailhook) graceful closure:
                // 1. Wait for existing queries to finish
                // 2. Send termination to all connections
                break;
            }
        }
    }
}
