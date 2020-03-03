use std::default::Default;
use std::str;

use codespan::{Files, Span};
use codespan_reporting::diagnostic::{Diagnostic, Label};
use codespan_reporting::term::{emit};
use termcolor::{StandardStream, ColorChoice};

use edgedb_protocol::error_response::ErrorResponse;
use edgedb_protocol::error_response::FIELD_POSITION_START;
use edgedb_protocol::error_response::FIELD_POSITION_END;
use edgedb_protocol::error_response::{FIELD_HINT, FIELD_DETAILS};
use edgedb_protocol::error_response::FIELD_SERVER_TRACEBACK;


pub fn print_query_error(err: &ErrorResponse, query: &str, verbose: bool)
    -> Result<(), anyhow::Error>
{
    let pstart = err.attributes.get(&FIELD_POSITION_START)
       .and_then(|x| str::from_utf8(x).ok())
       .and_then(|x| x.parse::<u32>().ok());
    let pend = err.attributes.get(&FIELD_POSITION_END)
       .and_then(|x| str::from_utf8(x).ok())
       .and_then(|x| x.parse::<u32>().ok());
    let (pstart, pend) = match (pstart, pend) {
        (Some(s), Some(e)) => (s, e),
        _ => {
            eprintln!("{}", err.display(verbose));
            return Ok(());
        }
    };
    let hint = err.attributes.get(&FIELD_HINT)
        .and_then(|x| str::from_utf8(x).ok())
        .unwrap_or("error");
    let detail = err.attributes.get(&FIELD_DETAILS)
        .and_then(|x| String::from_utf8(x.to_vec()).ok());
    let mut files = Files::new();
    let file_id = files.add("query", query);
    let diag = Diagnostic::new_error(&err.message, Label {
        file_id,
        span: Span::new(pstart, pend),
        message: hint.into(),
    }).with_notes(detail.into_iter().collect());

    emit(&mut StandardStream::stderr(ColorChoice::Auto),
        &Default::default(), &files, &diag)?;

    if err.code == 0x_01_00_00_00 || verbose {
        let tb = err.attributes.get(&FIELD_SERVER_TRACEBACK);
        if let Some(traceback) = tb {
            if let Ok(traceback) = str::from_utf8(traceback) {
                eprintln!("  Server traceback:");
                for line in traceback.lines() {
                    eprintln!("      {}", line);
                }
            }
        }
    }
    Ok(())
}
