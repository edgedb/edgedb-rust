use std::sync::Arc;

use crate::options::{TransactionOptions, RetryOptions};
use crate::state::State;


#[derive(Debug, Clone, Default)]
pub struct Options {
    pub(crate) transaction: TransactionOptions,
    pub(crate) retry: RetryOptions,
    pub(crate) state: Arc<State>,
}
