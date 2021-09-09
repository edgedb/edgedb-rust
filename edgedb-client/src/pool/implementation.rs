use async_std::channel::unbounded;
use async_std::task;
use async_std::sync::{Arc, Mutex, MutexGuard};

use crate::builder::Builder;
use crate::client::Connection;
use crate::errors::Error;
use crate::pool::command::Command;
use crate::pool::main;
use crate::pool::{Pool, PoolInner, PoolState, PoolConn, Options};

pub enum InProgressState {
    Connecting,
    Comitting,
    Done,
}

struct InProgress {
    state: InProgressState,
    pool: Arc<PoolInner>,
}

impl InProgress {
    fn new(mut guard: MutexGuard<'_, main::Inner>, pool: &Arc<PoolInner>)
        -> InProgress
    {
        guard.in_progress += 1;
        InProgress { pool: pool.clone(), state: InProgressState::Connecting }
    }
    async fn commit(mut self) {
        self.state = InProgressState::Comitting;
        let mut inner = self.pool.state.inner.lock().await;
        inner.in_progress -= 1;
        inner.acquired_conns += 1;
        self.state = InProgressState::Done;
    }
}

impl Drop for InProgress {
    fn drop(&mut self) {
        use InProgressState::*;

        match self.state {
            Connecting => {
                self.pool.chan.try_send(Command::ConnectionCanceled).ok();
            }
            Comitting => {
                self.pool.chan.try_send(Command::ConnectionEstablished).ok();
            }
            Done => {}
        }
    }
}

impl Pool {
    pub fn new(builder: Builder) -> Pool {
        let (chan, rcv) = unbounded();
        let state = Arc::new(PoolState::new(builder));
        let state2 = state.clone();
        let task = Mutex::new(Some(task::spawn(main::main(state2, rcv))));
        Pool {
            options: Arc::new(Options {}),
            inner: Arc::new(PoolInner {
                chan,
                task,
                state,
            }),
        }
    }
}

impl PoolInner {
    pub(crate) async fn acquire(self: &Arc<Self>) -> Result<PoolConn, Error> {
        let mut inner = self.state.inner.lock().await;
        loop {
            if let Some(conn) = inner.conns.pop_front() {
                assert!(conn.is_consistent());
                inner.acquired_conns += 1;
                return Ok(PoolConn { conn: Some(conn), pool: self.clone() });
            }
            let in_pool = inner.in_progress + inner.acquired_conns;
            if in_pool < self.state.config.max_connections {
                let guard = InProgress::new(inner, self);
                let conn = self.state.config.connect().await?;
                // Make sure that connection is wrapped before we commit,
                // so that connection is returned into a pool if we fail
                // to commit because of async stuff
                let conn = PoolConn { conn: Some(conn), pool: self.clone() };
                guard.commit().await;
                return Ok(conn);
            }
            inner = self.state.connection_released.wait(inner).await;
        }
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
