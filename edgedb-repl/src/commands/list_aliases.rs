use async_std::prelude::StreamExt;

use prettytable::{Table, Row, Cell};

use edgedb_derive::Queryable;
use crate::commands::Options;
use crate::commands::filter;
use crate::commands::table;
use crate::client::Client;



#[derive(Queryable)]
struct Alias {
    name: String,
    expr: String,
    klass: String,
}

pub async fn list_aliases<'x>(cli: &mut Client<'x>, options: &Options,
    pattern: &Option<String>, system: bool, case_sensitive: bool,
    verbose: bool)
    -> Result<(), anyhow::Error>
{
    let pat = filter::pattern_to_value(pattern, case_sensitive);
    let filter = match (pattern, system) {
        (None, true) => "FILTER .is_from_alias",
        (None, false) => {
            r#"FILTER .is_from_alias AND
                NOT re_test("^(?:std|schema|math|sys|cfg|cal|stdgraphql)::",
                    .name)"#
        }
        (Some(_), true) => {
            "FILTER .is_from_alias AND re_test(<str>$0, .name)"
        }
        (Some(_), false) => {
            r#"FILTER .is_from_alias
                AND re_test(<str>$0, .name) AND
                NOT re_test("^(?:std|schema|math|sys|cfg|cal|stdgraphql)::",
                .name)"#
        }
    };
    let query = &format!(r###"
        WITH MODULE schema
        SELECT Type {{
            name,
            expr,
            klass := (
                'object' IF Type IS ObjectType ELSE
                'scalar' IF Type IS ScalarType ELSE
                'tuple' IF Type IS Tuple ELSE
                'array' IF Type IS Array ELSE
                'unknown'
            ),
        }}
        {filter}
        ORDER BY .name;
    "###, filter=filter);
    let mut items = cli.query::<Alias>(&query, &pat).await?;
    if !options.command_line || atty::is(atty::Stream::Stdout) {
        let mut table = Table::new();
        table.set_format(*table::FORMAT);
        if verbose {
            table.set_titles(Row::new(
                ["Name", "Klass", "Expression"]
                .iter().map(|x| table::header_cell(x)).collect()));
            while let Some(item) = items.next().await.transpose()? {
                table.add_row(Row::new(vec![
                    Cell::new(&item.name),
                    Cell::new(&item.klass),
                    Cell::new(&item.expr),
                ]));
            }
        } else {
            table.set_titles(Row::new(
                ["Name", "Klass"]
                .iter().map(|x| table::header_cell(x)).collect()));
            while let Some(item) = items.next().await.transpose()? {
                table.add_row(Row::new(vec![
                    Cell::new(&item.name),
                    Cell::new(&item.klass),
                ]));
            }
        }
        if table.is_empty() {
            if let Some(pattern) = pattern {
                eprintln!("No aliases found matching {:?}", pattern);
            } else if !system {
                eprintln!("No user-defined expression aliases found.");
            } else {
                eprintln!("No aliases found.");
            }
        } else {
            table.printstd();
        }
    } else {
        if verbose {
            while let Some(item) = items.next().await.transpose()? {
                println!("{}\t{}\t{}", item.name, item.klass, item.expr);
            }
        } else {
            while let Some(item) = items.next().await.transpose()? {
                println!("{}\t{}", item.name, item.klass);
            }
        }
    }
    Ok(())
}
