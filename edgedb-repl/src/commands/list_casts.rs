use async_std::prelude::StreamExt;

use prettytable::{Table, Row, Cell};

use edgedb_derive::Queryable;
use crate::commands::Options;
use crate::commands::filter;
use crate::commands::table;
use crate::client::Client;


#[derive(Queryable)]
struct Cast {
    from_type_name: String,
    to_type_name: String,
    kind: String,
    volatility_str: String,
}


pub async fn list_casts<'x>(cli: &mut Client<'x>, options: &Options,
    pattern: &Option<String>, case_sensitive: bool)
    -> Result<(), anyhow::Error>
{
    let pat = filter::pattern_to_value(pattern, case_sensitive);
    let filter = if pattern.is_some() {
        r#"FILTER
            re_test(<str>$0, .from_type_name)
            OR re_test(<str>$0, .to_type_name)"#
    } else {
        ""
    };
    let query = &format!(r###"
        WITH MODULE schema
        SELECT Cast {{
            from_type_name := .from_type.name,
            to_type_name := .to_type.name,
            kind := (
                'implicit' IF .allow_implicit ELSE
                'assignment' IF .allow_assignment ELSE
                'regular'
            ),
            volatility_str := <str>.volatility,
        }}
        {filter}
        ORDER BY .kind THEN .from_type.name THEN .to_type.name;
    "###, filter=filter);
    let mut items = cli.query::<Cast>(&query, &pat).await?;
    if !options.command_line || atty::is(atty::Stream::Stdout) {
        let mut table = Table::new();
        table.set_format(*table::FORMAT);
        table.set_titles(Row::new(
            ["From Type", "To Type", "Kind", "Volatility"]
            .iter().map(|x| table::header_cell(x)).collect()));
        while let Some(item) = items.next().await.transpose()? {
            table.add_row(Row::new(vec![
                Cell::new(&item.from_type_name),
                Cell::new(&item.to_type_name),
                Cell::new(&item.kind),
                Cell::new(&item.volatility_str),
            ]));
        }
        if table.is_empty() {
            if let Some(pattern) = pattern {
                eprintln!("No casts found matching {:?}", pattern);
            }
        } else {
            table.printstd();
        }
    } else {
        while let Some(item) = items.next().await.transpose()? {
            println!("{}\t{}\t{}\t{}",
                item.from_type_name, item.to_type_name,
                item.kind, item.volatility_str);
        }
    }
    Ok(())
}
