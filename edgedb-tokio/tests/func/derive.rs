use edgedb_derive::Queryable;
use edgedb_protocol::model::Uuid;
use edgedb_tokio::Client;

use crate::server::SERVER;

#[derive(Queryable, Debug, PartialEq)]
struct FreeOb {
    one: i64,
    two: i64,
}

#[derive(Queryable, Debug, PartialEq)]
struct SchemaType {
    name: String,
}

#[derive(Queryable, Debug, PartialEq)]
struct SchemaTypeId {
    id: Uuid,
    name: String,
}

#[tokio::test]
async fn free_object() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let value = client
        .query_required_single::<FreeOb, _>("SELECT { one := 1, two := 2 }", &())
        .await?;
    assert_eq!(value, FreeOb { one: 1, two: 2 });

    Ok(())
}

#[tokio::test]
async fn schema_type() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client.ensure_connected().await?;

    let value = client
        .query_required_single::<SchemaType, _>(
            "
        SELECT schema::ObjectType { name }
        FILTER .name = 'schema::Object'
        LIMIT 1
        ",
            &(),
        )
        .await?;
    assert_eq!(
        value,
        SchemaType {
            name: "schema::Object".into(),
        }
    );

    let value = client
        .query_required_single::<SchemaTypeId, _>(
            "
        SELECT schema::ObjectType { id, name }
        FILTER .name = 'schema::Object'
        LIMIT 1
        ",
            &(),
        )
        .await?;
    // id is unstable
    assert_eq!(value.name, "schema::Object");

    Ok(())
}
