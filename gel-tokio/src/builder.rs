use std::pin::Pin;
use std::sync::Arc;
use std::future::Future;

pub trait CertChecker: Send + Sync + 'static {
    fn call(&self, cert: &[u8]) -> Pin<Box<dyn Future<Output = Result<(), gel_errors::Error>> + Send + Sync + 'static>>;
}

impl<T> CertChecker for T where T: for <'a> Fn(&'a [u8]) -> Pin<Box<dyn Future<Output = Result<(), gel_errors::Error>> + Send + Sync + 'static>> + Send + Sync + 'static {
    fn call(&self, cert: &[u8]) -> Pin<Box<dyn Future<Output = Result<(), gel_errors::Error>> + Send + Sync + 'static>> {
        (self)(cert)
    }
}

#[derive(Clone)]
pub struct CertCheck {
    function: Arc<dyn CertChecker>
}

#[allow(dead_code)]
impl CertCheck {
    pub fn new(function: impl Into<Arc<dyn CertChecker>>) -> Self {
        Self { function: function.into() }
    }

    pub fn new_fn<F: Future<Output = Result<(), gel_errors::Error>> + Send + Sync + 'static>(function: impl for <'a> Fn(&'a [u8]) -> F + Send + Sync + 'static) -> Self {
        let function = Arc::new(move |cert: &'_[u8]| { 
            let fut = function(cert);
            Box::pin(fut) as _
        });

        Self { function }
    }

    pub(crate) fn call(&self, cert: &[u8]) -> impl Future<Output = Result<(), gel_errors::Error>> + Send + Sync + Unpin + 'static {
        self.function.call(cert)
    }
}

impl std::fmt::Debug for CertCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(fn)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cert_check() {
        CertCheck::new_fn(|cert| { 
            let cert = cert.to_vec();
            async move {
                assert_eq!(cert, b"cert");
                Ok(())
            }
        });
    }
}
