use crate::options::{TransactionOptions, RetryOptions};


#[derive(Debug, Clone, Default)]
pub struct Options {
    pub(crate) transaction: TransactionOptions,
    pub(crate) retry: RetryOptions,
}
