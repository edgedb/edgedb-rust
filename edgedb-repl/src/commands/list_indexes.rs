use async_std::prelude::StreamExt;

use prettytable::{Table, Row, Cell};

use edgedb_derive::Queryable;
use crate::commands::Options;
use crate::commands::filter;
use crate::commands::table;
use crate::client::Client;



#[derive(Queryable)]
struct Index {
    expr: String,
    is_implicit: bool,
    subject_name: String,
}

pub async fn list_indexes<'x>(cli: &mut Client<'x>, options: &Options,
    pattern: &Option<String>, system: bool, case_sensitive: bool,
    verbose: bool)
    -> Result<(), anyhow::Error>
{
    let pat = filter::pattern_to_value(pattern, case_sensitive);
    let mut filters = Vec::with_capacity(3);
    if !system {
        filters.push(
            r#"NOT re_test("^(?:std|schema|math|sys|cfg|cal|stdgraphql)::",
               .subject_name)"#);
    }
    if !verbose {
        filters.push("NOT .is_implicit");
    }
    if pattern.is_some() {
        filters.push("re_test(<str>$0, .subject_name)");
    }
    let filter = if filters.is_empty() {
        String::from("")
    } else {
        format!("FILTER {}", filters.join(" AND "))
    };
    let query = &format!(r###"
        WITH
            MODULE schema,
            I := {{
                Index,
                (
                    SELECT Constraint
                    FILTER .name = 'std::exclusive' AND NOT .is_abstract
                )
            }}
        SELECT I {{
            expr,
            subject_name := I[IS Index].<indexes[IS Source].name,
            cons_on := '.' ++ I[IS Constraint].subject.name,
            cons_of := I[Is Constraint].subject[IS Pointer]
                .<pointers[IS Source].name,
            cons_of_of := I[Is Constraint].subject[IS Pointer]
                .<properties[IS Source].<links[IS Source].name,
        }} {{
            expr := .cons_on ?? .expr,
            is_implicit := EXISTS .cons_on,
            subject_name :=
                (.cons_of_of ++ '.' ++ .cons_of) ??
                (.cons_of) ??
                (.subject_name)
        }}
        {filter}
        ORDER BY .subject_name;
    "###, filter=filter);
    let mut items = cli.query::<Index>(&query, &pat).await?;
    if !options.command_line || atty::is(atty::Stream::Stdout) {
        let mut table = Table::new();
        table.set_format(*table::FORMAT);
        if verbose {
            table.set_titles(Row::new(
                ["Index On", "Implicit", "Subject"]
                .iter().map(|x| table::header_cell(x)).collect()));
            while let Some(item) = items.next().await.transpose()? {
                table.add_row(Row::new(vec![
                    Cell::new(&item.expr),
                    Cell::new(&item.is_implicit.to_string()),
                    Cell::new(&item.subject_name),
                ]));
            }
        } else {
            table.set_titles(Row::new(
                ["Index On", "Subject"]
                .iter().map(|x| table::header_cell(x)).collect()));
            while let Some(item) = items.next().await.transpose()? {
                table.add_row(Row::new(vec![
                    Cell::new(&item.expr),
                    Cell::new(&item.subject_name),
                ]));
            }
        }
        if table.is_empty() {
            if let Some(pattern) = pattern {
                eprintln!("No indexes found matching {:?}", pattern);
            } else if !verbose {
                if options.command_line {
                    eprintln!("No explicit indexes found. Try --verbose");
                } else {
                    eprintln!("No explicit indexes found. Try \\li+");
                }
            } else {
                eprintln!("No indexes found.");
            }
        } else {
            table.printstd();
        }
    } else {
        if verbose {
            while let Some(item) = items.next().await.transpose()? {
                println!("{}\t{}\t{}",
                    item.expr, item.is_implicit, item.subject_name);
            }
        } else {
            while let Some(item) = items.next().await.transpose()? {
                println!("{}\t{}", item.expr, item.subject_name);
            }
        }
    }
    Ok(())
}
