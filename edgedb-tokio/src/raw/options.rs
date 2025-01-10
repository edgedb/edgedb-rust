use std::sync::Arc;

use edgedb_protocol::encoding::Annotations;

use crate::options::{RetryOptions, TransactionOptions};
use crate::raw::state::PoolState;

#[derive(Debug, Clone, Default)]
pub struct Options {
    pub(crate) transaction: TransactionOptions,
    pub(crate) retry: RetryOptions,
    pub(crate) state: Arc<PoolState>,
    pub(crate) annotations: Arc<Annotations>,
}
