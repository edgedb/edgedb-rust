use std::collections::HashMap;
use std::default::Default;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use rand::{thread_rng, Rng};
use once_cell::sync::Lazy;

use crate::errors::{Error, IdleSessionTimeoutError};

trait Assert: Send + Sync + 'static {}
impl Assert for RetryOptions {}
impl Assert for TransactionOptions {}


/// Single immediate retry on idle is fine
///
/// This doesn't have to be configured.
static IDLE_TIMEOUT_RULE: Lazy<RetryRule> = Lazy::new(|| RetryRule {
    attempts: 2,
    backoff: Arc::new(|_| { Duration::new(0, 0) }),
});


/// Transaction isolation level
///
/// Only single isolation level is supported for now
#[derive(Debug, Clone)]
pub enum IsolationLevel {
    /// Serializable isolation level
    Serializable,
}

/// Specific condition for retrying queries
///
/// This is used for fine-grained control for retrying queries and transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RetryCondition {
    /// Optimistic transaction error
    TransactionConflict,
    /// Network failure between client and server
    NetworkError,
}

/// Options for [`transaction()`](crate::Client::transaction)
///
/// Must be set on a [`Client`](crate::Client) via
/// [`with_transaction_options`](crate::Client::with_transaction_options).
#[derive(Debug, Clone)]
pub struct TransactionOptions {
    isolation: IsolationLevel,
    read_only: bool,
    deferrable: bool,
}

/// This structure contains options for retrying transactions and queries
///
/// Must be set on a [`Client`](crate::Client) via
/// [`with_retry_options`](crate::Client::with_retry_options).
#[derive(Debug, Clone)]
pub struct RetryOptions(Arc<RetryOptionsInner>);

#[derive(Debug, Clone)]
struct RetryOptionsInner {
    default: RetryRule,
    overrides: HashMap<RetryCondition, RetryRule>,
}

#[derive(Clone)]
pub(crate) struct RetryRule {
    pub(crate) attempts: u32,
    pub(crate) backoff: Arc<dyn Fn(u32) -> Duration + Send + Sync>,
}

impl Default for TransactionOptions {
    fn default() -> TransactionOptions {
        TransactionOptions {
            isolation: IsolationLevel::Serializable,
            read_only: false,
            deferrable: false,
        }
    }
}

impl TransactionOptions {
    /// Set isolation level for the transaction
    pub fn isolation(mut self, isolation: IsolationLevel) -> Self {
        self.isolation = isolation;
        self
    }
    /// Set whether transaction is read-only
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
    /// Set whether transaction is deferrable
    pub fn deferrable(mut self, deferrable: bool) -> Self {
        self.deferrable = deferrable;
        self
    }
}

impl Default for RetryRule {
    fn default() -> RetryRule {
        RetryRule {
            attempts: 3,
            backoff: Arc::new(|n| {
                Duration::from_millis(
                    2u64.pow(n)*100 + thread_rng().gen_range(0..100)
                )
            }),
        }
    }
}

impl Default for RetryOptions {
    fn default() -> RetryOptions {
        RetryOptions(Arc::new(RetryOptionsInner {
            default: RetryRule::default(),
            overrides: HashMap::new(),
        }))
    }
}

impl RetryOptions {
    /// Create a new [`RetryOptions`] object with the default rule
    pub fn new(self, attempts: u32,
               backoff: impl Fn(u32) -> Duration + Send + Sync + 'static)
        -> Self
    {
        RetryOptions(Arc::new(RetryOptionsInner {
            default: RetryRule {
                attempts,
                backoff: Arc::new(backoff),
            },
            overrides: HashMap::new(),
        }))
    }
    /// Add a retrying rule for a specific condition
    pub fn with_rule<F>(mut self,
        condition: RetryCondition,
        attempts: u32,
        backoff: impl Fn(u32) -> Duration + Send + Sync + 'static)
        -> Self
    {
        let inner =  Arc::make_mut(&mut self.0);
        inner.overrides.insert(condition, RetryRule {
            attempts,
            backoff: Arc::new(backoff),
        });
        self
    }
    pub(crate) fn get_rule(&self, err: &Error) -> &RetryRule {
        use edgedb_errors::{TransactionConflictError, ClientError};
        use RetryCondition::*;

        if err.is::<IdleSessionTimeoutError>() {
            return &*IDLE_TIMEOUT_RULE;
        } else if err.is::<TransactionConflictError>() {
            self.0.overrides.get(&TransactionConflict)
                .unwrap_or(&self.0.default)
        } else if err.is::<ClientError>() {
            self.0.overrides.get(&NetworkError).unwrap_or(&self.0.default)
        } else {
            &self.0.default
       }
    }
}

struct DebugBackoff<F>(F, u32);

impl<F> fmt::Debug for DebugBackoff<F>
    where F: Fn(u32) -> Duration,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.1 > 3 {
            for i in 0..3 {
                write!(f, "{:?}, ", (self.0)(i))?;
            }
            write!(f, "...")?;
        } else {
            write!(f, "{:?}", (self.0)(0))?;
            for i in 1..self.1 {
                write!(f, ", {:?}", (self.0)(i))?;
            }
        }
        Ok(())
    }
}

#[test]
fn debug_backoff() {
    assert_eq!(
        format!("{:?}",
            DebugBackoff(|i| Duration::from_secs(10+(i as u64)*10), 3)),
        "10s, 20s, 30s");
    assert_eq!(
        format!("{:?}",
            DebugBackoff(|i| Duration::from_secs(10+(i as u64)*10), 10)),
        "10s, 20s, 30s, ...");
    assert_eq!(
        format!("{:?}",
            DebugBackoff(|i| Duration::from_secs(10+(i as u64)*10), 2)),
        "10s, 20s");
}

impl fmt::Debug for RetryRule {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RetryRule")
            .field("attempts", &self.attempts)
            .field("backoff", &DebugBackoff(&*self.backoff, self.attempts))
            .finish()
    }
}

