use std::error::Error;
use rand::{thread_rng, Rng};

use edgedb_errors::{ErrorKind, UserError, TransactionError};

#[derive(thiserror::Error, Debug)]
#[error("should not apply this counter update")]
struct CounterError;


fn check_val1(val: i64) -> Result<(), CounterError> {
    if val % 3 == 1 {
        if thread_rng().gen_bool(0.1) {
            return Err(CounterError)?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let conn = edgedb_tokio::create_client().await?;
    let res = conn.transaction(|mut transaction| async move {
        let val = transaction.query_required_single::<i64, _>("
                WITH counter := (UPDATE Counter SET { value := .value + 1}),
                SELECT counter.value LIMIT 1
            ", &(),
        ).await?;
        check_val1(val).map_err(TransactionError::User)?;
        Ok::<_, TransactionError<CounterError>>(val)
    }).await;
    match res {
        Ok(val) => println!("New counter value: {val}"),
        Err(TransactionError::User(e)) => {
            println!("Skipping: {e:#}");
        }
        Err(TransactionError::Edgedb(e)) => return Err(e)?,
    }
    Ok(())
}
