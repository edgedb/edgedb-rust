use async_std::prelude::StreamExt;

use prettytable::{Table, Row, Cell};

use edgedb_derive::Queryable;
use crate::commands::Options;
use crate::commands::filter;
use crate::commands::table;
use crate::client::Client;



#[derive(Queryable)]
struct TypeRow {
    name: String,
    extending: String,
}

pub async fn list_object_types<'x>(cli: &mut Client<'x>, options: &Options,
    pattern: &Option<String>, system: bool, case_sensitive: bool)
    -> Result<(), anyhow::Error>
{
    let pat = filter::pattern_to_value(pattern, case_sensitive);
    let filter = match (pattern, system) {
        (None, true) => "FILTER NOT .is_from_alias",
        (None, false) => {
            r#"FILTER NOT
                re_test("^(?:std|schema|math|sys|cfg|cal|stdgraphql)::",
                .name)"#
        }
        (Some(_), true) => {
            "FILTER NOT .is_from_alias AND re_test(<str>$0, .name)"
        }
        (Some(_), false) => {
            r#"FILTER NOT .is_from_alias
                AND re_test(<str>$0, .name) AND
                NOT re_test("^(?:std|schema|math|sys|cfg|cal|stdgraphql)::",
                .name)"#
        }
    };

    let query = &format!(r###"
        WITH MODULE schema
        SELECT ObjectType {{
            name,
            `extending` := to_str(array_agg(.ancestors.name), ', '),
        }}
        {filter}
        ORDER BY .name;
    "###, filter=filter);

    let mut items = cli.query::<TypeRow>(&query, &pat).await?;
    if !options.command_line || atty::is(atty::Stream::Stdout) {
        let mut table = Table::new();
        table.set_format(*table::FORMAT);
        table.set_titles(Row::new(
            ["Name", "Extending"]
            .iter().map(|x| table::header_cell(x)).collect()));
        while let Some(item) = items.next().await.transpose()? {
            table.add_row(Row::new(vec![
                Cell::new(&item.name),
                Cell::new(&item.extending),
            ]));
        }
        if table.is_empty() {
            if let Some(pattern) = pattern {
                eprintln!("No object types found matching {:?}", pattern);
            } else if !system {
                eprintln!("No user-defined object types found. {}",
                    if options.command_line { "Try --system" }
                    else { r"Try \ltS" });
            }
        } else {
            table.printstd();
        }
    } else {
        while let Some(item) = items.next().await.transpose()? {
            println!("{}\t{}", item.name, item.extending);
        }
    }
    Ok(())
}
