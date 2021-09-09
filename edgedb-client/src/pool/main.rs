use std::cmp::min;
use std::collections::VecDeque;

use async_std::sync::{Arc, Mutex};
use async_std::channel::{Receiver, RecvError};

use crate::client::Connection;
use crate::pool::command::Command;
use crate::pool::config::PoolConfig;


#[derive(Debug)]
pub(crate) struct PoolState {
    cfg: PoolConfig,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
pub(crate) struct Inner {
    acquired_conns: usize,
    conns: VecDeque<Connection>,
}

impl PoolState {
    pub(crate) fn new(cfg: PoolConfig) -> PoolState {
        PoolState {
            inner: Mutex::new(Inner {
                acquired_conns: 0,
                conns: VecDeque::with_capacity(min(cfg.max_connections, 16)),
            }),
            cfg,
        }
    }
}


pub(crate) async fn main(state: Arc<PoolState>, rcv: Receiver<Command>) {
    loop {
        match rcv.recv().await {
            Ok(Command::Release(conn)) => {
                let mut inner = state.inner.lock().await;
                if conn.is_consistent() {
                    inner.conns.push_back(conn);
                } else {
                    inner.acquired_conns -= 1;
                }
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
