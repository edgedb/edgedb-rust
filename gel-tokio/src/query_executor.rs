use gel_protocol::query_arg::QueryArgs;
use gel_protocol::QueryResult;
use gel_protocol::{annotations::Warning, model::Json};
use std::future::Future;

use crate::{Client, Error, Transaction};

/// Query result with additional metadata.
#[non_exhaustive]
#[derive(Debug)]
pub struct ResultVerbose<R> {
    /// Query results
    pub data: R,

    /// Query warnings
    pub warnings: Vec<Warning>,
}

/// Abstracts over different query executors
/// In particular &Client and &mut Transaction
pub trait QueryExecutor: Sized {
    /// see [Client::query]
    fn query<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<Vec<R>, Error>> + Send
    where
        A: QueryArgs,
        R: QueryResult + Send;

    /// see [Client::query_verbose]
    fn query_verbose<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<ResultVerbose<Vec<R>>, Error>> + Send
    where
        A: QueryArgs,
        R: QueryResult + Send;

    /// see [Client::query_single]
    fn query_single<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<Option<R>, Error>> + Send
    where
        A: QueryArgs,
        R: QueryResult + Send;

    /// see [Client::query_required_single]
    fn query_required_single<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl std::future::Future<Output = Result<R, Error>> + Send
    where
        A: QueryArgs,
        R: QueryResult + Send;

    /// see [Client::query_json]
    fn query_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>> + Send;

    /// see [Client::query_single_json]
    fn query_single_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Option<Json>, Error>> + Send;

    /// see [Client::query_required_single_json]
    fn query_required_single_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>>;

    /// see [Client::execute]
    fn execute<A>(
        self,
        query: &str,
        arguments: &A,
    ) -> impl Future<Output = Result<(), Error>> + Send
    where
        A: QueryArgs;
}

impl QueryExecutor for &Client {
    fn query<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<Vec<R>, Error>>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        Client::query(self, query, arguments)
    }

    fn query_verbose<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<ResultVerbose<Vec<R>>, Error>> + Send
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Client::query_verbose(self, query, arguments)
    }

    fn query_single<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<Option<R>, Error>>
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Client::query_single(self, query, arguments)
    }

    fn query_required_single<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<R, Error>>
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Client::query_required_single(self, query, arguments)
    }

    fn query_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>> {
        Client::query_json(self, query, arguments)
    }

    fn query_single_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Option<Json>, Error>> {
        Client::query_single_json(self, query, arguments)
    }

    fn query_required_single_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>> {
        Client::query_required_single_json(self, query, arguments)
    }

    fn execute<A>(self, query: &str, arguments: &A) -> impl Future<Output = Result<(), Error>>
    where
        A: QueryArgs,
    {
        Client::execute(self, query, arguments)
    }
}

impl<T: std::ops::DerefMut<Target = Transaction>> QueryExecutor for &mut T {
    fn query<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<Vec<R>, Error>>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        Transaction::query(self, query, arguments)
    }

    fn query_verbose<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<ResultVerbose<Vec<R>>, Error>> + Send
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Transaction::query_verbose(self, query, arguments)
    }

    fn query_single<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<Option<R>, Error>>
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Transaction::query_single(self, query, arguments)
    }

    fn query_required_single<R, A>(
        self,
        query: impl AsRef<str> + Send,
        arguments: &A,
    ) -> impl Future<Output = Result<R, Error>>
    where
        A: QueryArgs,
        R: QueryResult + Send,
    {
        Transaction::query_required_single(self, query, arguments)
    }

    fn query_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>> {
        Transaction::query_json(self, query, arguments)
    }

    fn query_single_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Option<Json>, Error>> {
        Transaction::query_single_json(self, query, arguments)
    }

    fn query_required_single_json(
        self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>> {
        Transaction::query_required_single_json(self, query, arguments)
    }

    fn execute<A>(self, query: &str, arguments: &A) -> impl Future<Output = Result<(), Error>>
    where
        A: QueryArgs,
    {
        Transaction::execute(self, query, arguments)
    }
}
