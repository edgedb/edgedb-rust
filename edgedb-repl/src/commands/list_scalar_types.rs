use async_std::prelude::StreamExt;

use edgedb_protocol::value::Value;
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
    pattern: &Option<String>, system: bool, case_sensitive: bool)
    -> Result<(), anyhow::Error>
{
    let pattern = pattern.as_ref().map(|pattern| {
        if case_sensitive {
            pattern.clone()
        } else {
            String::from("(?i)") + pattern
        }
    });
    let (filter, var) = match (pattern, system) {
        (None, true) => {
            ("FILTER NOT .is_from_alias",
             Value::empty_tuple())
        }
        (None, false) => {
            (r#"FILTER NOT
                re_test("^(?:std|schema|math|sys|cfg|cal|stdgraphql)::",
                .name)"#,
             Value::empty_tuple())
        }
        (Some(pattern), true) => {
            ("FILTER NOT .is_from_alias AND re_test(<str>$0, .name)",
             Value::Tuple(vec![Value::Str(pattern)]))
        }
        (Some(pattern), false) => {
            (r#"FILTER NOT .is_from_alias
                AND re_test(<str>$0, .name) AND
                NOT re_test("^(?:std|schema|math|sys|cfg|cal|stdgraphql)::",
                .name)"#,
             Value::Tuple(vec![Value::Str(pattern)]))
        }
    };

    let query = &format!(r###"
        WITH MODULE schema
        SELECT ScalarType {{
            name,
            `extending` := to_str(array_agg(.bases.name), ', '),
            kind := (
                'enum' IF 'std::anyenum' IN .ancestors.name ELSE
                'sequence' IF 'std::sequence' IN .ancestors.name ELSE
                'normal'
            ),
        }}
        {filter}
        ORDER BY .name;
    "###, filter=filter);

    let mut items = cli.query::<Row>(&query, &var).await?;

    while let Some(item) = items.next().await.transpose()? {
        println!("{}\t{}\t{}", item.name, item.extending, item.kind);
    }
    Ok(())
}
