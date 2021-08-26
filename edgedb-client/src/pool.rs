struct Pool {
}

impl Poll {
    pub async fn query<R, A>(&mut self, request: &str, arguments: &Value)
        -> Result<QueryResponse<'_, QueryableDecoder<R>>, Error>
        where R: Queryable,
    {

    pub async fn execute(&mut self, request: &str, arguments: &Value)
        -> Result<Bytes, Error>
    {
        let mut seq = self.start_sequence().await?;
        seq._query(request, arguments, IoFormat::Binary).await?;
        return seq._process_exec().await;
    }
}
