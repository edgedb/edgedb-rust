use std::collections::HashMap;

use async_std::prelude::StreamExt;
use uuid::Uuid;

use edgedb_derive::Queryable;
use crate::client::Client;


#[derive(Queryable)]
struct Row {
    id: Uuid,
    name: String,
}


pub async fn get_type_names<'x>(cli: &mut Client<'x>)
    -> Result<HashMap<Uuid, String>, anyhow::Error>
{
    let mut items = cli.query::<Row>(r###"
        WITH MODULE schema
        SELECT Type { id, name }
        FILTER Type IS (ObjectType | ScalarType);
    "###).await?;
    let mut types = HashMap::new();
    while let Some(row) = items.next().await.transpose()? {
        types.insert(row.id, row.name);
    }
    Ok(types)
}
