use async_std::channel::unbounded;
use async_std::task;
use async_std::sync::{Arc, Mutex};

use crate::client::Connection;
use crate::errors::Error;
use crate::pool::command::Command;
use crate::pool::config::PoolConfig;
use crate::pool::main;
use crate::pool::{Pool, PoolInner, PoolState, PoolConn, Options};


impl Pool {
    pub async fn new(config: PoolConfig) -> Result<Pool, Error> {
        let (chan, rcv) = unbounded();
        let state = Arc::new(PoolState::new(config));
        let state2 = state.clone();
        let task = Mutex::new(Some(task::spawn(main::main(state2, rcv))));
        Ok(Pool {
            options: Arc::new(Options {}),
            inner: Arc::new(PoolInner { chan, task, state }),
        })
    }
}

impl PoolInner {
    pub(crate) async fn acquire(&self) -> Result<PoolConn, Error> {
        todo!();
    }
    pub(crate) fn release(&self, conn: Connection) {
        self.chan.try_send(Command::Release(conn)).ok();
    }
}

impl Drop for PoolInner {
    fn drop(&mut self) {
        // If task is locked (i.e. try_lock returns an error) it means
        // somebody is currently waiting for pool to be closed, which is fine.
        self.task.try_lock()
            .and_then(|mut task| task.take().map(|t| t.cancel()));
    }
}

impl Pool {
    // TODO(tailhook) this currently awaits for close only on the first
    // close call. Subsequent parallel calls will exit early.
    pub async fn close(&self) {
        self.inner.chan.send(Command::Close).await.ok();
        if let Some(task) = self.inner.task.lock().await.take() {
            task.await;
        }
    }
}
