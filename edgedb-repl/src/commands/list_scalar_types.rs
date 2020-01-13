
use crate::commands::Options;
use crate::client::Client;

use edgedb_derive::Queryable;

#[derive(Queryable)]
struct Row {
    name: String,
    extending: String,
    kind: String,
}

pub async fn list_scalar_types<'x>(cli: &mut Client<'x>, options: &Options,
    pattern: &Option<String>, system: bool, insensitive: bool)
    -> Result<(), anyhow::Error>
{
    let list = cli.query::<Row>(r#"
        WITH MODULE schema
        SELECT ScalarType {
            name,
            `extending` := to_str(array_agg(.bases.name), ', '),
            kind := (
                'enum' IF 'std::anyenum' IN .ancestors.name ELSE
                'sequence' IF 'std::sequence' IN .ancestors.name ELSE
                'normal'
            ),
        }
    "#).await?;
    for item in list {
        println!("{}\t{}\t{}", item.name, item.extending, item.kind);
    }
    Ok(())
}
