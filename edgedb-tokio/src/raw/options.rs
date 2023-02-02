use std::sync::Arc;

use crate::options::{TransactionOptions, RetryOptions};
use crate::raw::state::PoolState;


#[derive(Debug, Clone, Default)]
pub struct Options {
    pub(crate) transaction: TransactionOptions,
    pub(crate) retry: RetryOptions,
    pub(crate) state: Arc<PoolState>,
}
