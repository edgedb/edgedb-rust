use rand::{rng, Rng};
use std::error::Error;

use gel_errors::{ErrorKind, UserError};

#[derive(thiserror::Error, Debug)]
#[error("should not apply this counter update")]
struct CounterError;

fn check_val0(val: i64) -> anyhow::Result<()> {
    if val % 3 == 0 && rng().random_bool(0.9) {
        Err(CounterError)?;
    }
    Ok(())
}

fn check_val1(val: i64) -> Result<(), CounterError> {
    if val % 3 == 1 && rng().random_bool(0.1) {
        Err(CounterError)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let conn = gel_tokio::create_client().await?;
    let res = conn
        .transaction(|mut transaction| async move {
            let val = transaction
                .query_required_single::<i64, _>(
                    "
                WITH counter := (UPDATE Counter SET { value := .value + 1}),
                SELECT counter.value LIMIT 1
            ",
                    &(),
                )
                .await?;
            check_val0(val)?;
            check_val1(val).map_err(UserError::with_source)?;
            Ok(val)
        })
        .await;
    match res {
        Ok(val) => println!("New counter value: {val}"),
        Err(e) if e.source().map_or(false, |e| e.is::<CounterError>()) => {
            println!("Skipping: {e:#}");
        }
        Err(e) => return Err(e)?,
    }
    Ok(())
}
