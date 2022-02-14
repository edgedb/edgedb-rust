use std::cmp::min;
use std::collections::VecDeque;

use async_std::channel::{Receiver, RecvError};
use async_std::sync::{Arc, Condvar, Mutex};

use crate::builder::Builder;
use crate::client::Connection;
use crate::pool::command::Command;

/// This is common state of the pool shared between background task
/// (which runs `pool::main::main`) and `Pool` instance
#[derive(Debug)]
pub(crate) struct PoolState {
    pub config: Builder,
    pub inner: Mutex<Inner>,
    pub connection_released: Condvar,
}

/// This is mutable part of the `PoolState` (protected via mutex)
#[derive(Debug)]
pub(crate) struct Inner {
    pub in_progress: usize,
    pub acquired_conns: usize,
    pub conns: VecDeque<Connection>,
}

impl PoolState {
    pub(crate) fn new(config: Builder) -> PoolState {
        PoolState {
            inner: Mutex::new(Inner {
                in_progress: 0,
                acquired_conns: 0,
                conns: VecDeque::with_capacity(min(config.max_connections, 16)),
            }),
            connection_released: Condvar::new(),
            config,
        }
    }
}

pub(crate) async fn main(state: Arc<PoolState>, rcv: Receiver<Command>) {
    loop {
        // TODO(tailhook) poll current connections
        match rcv.recv().await {
            Ok(Command::Release(conn)) => {
                let mut inner = state.inner.lock().await;
                if conn.is_consistent() {
                    inner.conns.push_back(conn);
                } else {
                    inner.acquired_conns -= 1;
                }
                state.connection_released.notify_one();
                drop(inner);
            }
            Ok(Command::ConnectionCanceled) => {
                let mut inner = state.inner.lock().await;
                inner.in_progress -= 1;
                state.connection_released.notify_one();
                drop(inner);
            }
            Ok(Command::ConnectionEstablished) => {
                let mut inner = state.inner.lock().await;
                inner.in_progress -= 1;
                inner.acquired_conns += 1;
                // We don't notify here because we don't have an
                // in_progress connection limit for now.
                drop(inner);
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
