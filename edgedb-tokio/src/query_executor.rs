use edgedb_protocol::model::Json;
use edgedb_protocol::query_arg::QueryArgs;
use edgedb_protocol::QueryResult;
use std::future::Future;

use crate::{Client, Error, Transaction};

/// Abstracts over different query executors
/// In particular &Client and &mut Transaction
pub trait QueryExecutor {
    /// see [Client::query]
    fn query<R, A>(self, query: &str, arguments: &A) -> impl Future<Output = Result<Vec<R>, Error>> + Send
    where
        A: QueryArgs,
        R: QueryResult + Send;

    /// see [Client::query_single]
    fn query_single<R, A>(
        self,
        query: &str,
        arguments: &A,
    ) -> impl Future<Output = Result<Option<R>, Error>> + Send
    where
        A: QueryArgs,
        R: QueryResult + Send;

    /// see [Client::query_required_single]
    fn query_required_single<R, A>(
        self,
        query: &str,
        arguments: &A,
    ) -> impl Future<Output = Result<R, Error>> + Send
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
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Option<Json>, Error>> + Send;

    /// see [Client::query_required_single_json]
    fn query_required_single_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>>;

    /// see [Client::execute]
    fn execute<A>(&mut self, query: &str, arguments: &A) -> impl Future<Output = Result<(), Error>> + Send
    where
        A: QueryArgs;
}

impl QueryExecutor for &Client {
    fn query<R, A>(self, query: &str, arguments: &A) -> impl Future<Output = Result<Vec<R>, Error>>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        Client::query(self, query, arguments)
    }

    fn query_single<R, A>(
        self,
        query: &str,
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
        query: &str,
        arguments: &A,
    ) -> impl Future<Output = Result<R, Error>> + Send
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
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Option<Json>, Error>> {
        Client::query_single_json(self, query, arguments)
    }

    fn query_required_single_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>> {
        Client::query_required_single_json(self, query, arguments)
    }

    fn execute<A>(&mut self, query: &str, arguments: &A) -> impl Future<Output = Result<(), Error>>
    where
        A: QueryArgs,
    {
        Client::execute(self, query, arguments)
    }
}

impl QueryExecutor for &mut Transaction {
    fn query<R, A>(self, query: &str, arguments: &A) -> impl Future<Output = Result<Vec<R>, Error>>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        Transaction::query(self, query, arguments)
    }

    fn query_single<R, A>(
        self,
        query: &str,
        arguments: &A,
    ) -> impl Future<Output = Result<Option<R>, Error>>
    where
        A: QueryArgs,
        R: QueryResult,
    {
        Transaction::query_single(self, query, arguments)
    }

    fn query_required_single<R, A>(
        self,
        query: &str,
        arguments: &A,
    ) -> impl Future<Output = Result<R, Error>>
    where
        A: QueryArgs,
        R: QueryResult,
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
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Option<Json>, Error>> {
        Transaction::query_single_json(self, query, arguments)
    }

    fn query_required_single_json(
        &mut self,
        query: &str,
        arguments: &impl QueryArgs,
    ) -> impl Future<Output = Result<Json, Error>> {
        Transaction::query_required_single_json(self, query, arguments)
    }

    fn execute<A>(&mut self, query: &str, arguments: &A) -> impl Future<Output = Result<(), Error>>
    where
        A: QueryArgs,
    {
        Transaction::execute(self, query, arguments)
    }
}
