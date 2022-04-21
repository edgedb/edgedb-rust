use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::{Barrier, Mutex};

use edgedb_tokio::{Client, Transaction};

use crate::server::SERVER;

async fn transaction1(client: Client, name: &str, iterations: Arc<AtomicUsize>,
                      barrier: Arc<Barrier>, lock: Arc<Mutex<()>>)
    -> anyhow::Result<i32>
{
    let val = client.transaction(|mut tx| {
        let lock = lock.clone();
        let iterations = iterations.clone();
        let barrier = barrier.clone();
        async move {
            iterations.fetch_add(1, Ordering::SeqCst);
            // This magic query makes starts real transaction,
            // that is otherwise started lazily
            tx.query::<i64, _>("SELECT 1", &()).await?;
            barrier.wait().await;
            let val = {
                let _lock = lock.lock().await;
                tx.query_required_single("
                        SELECT (
                            INSERT test::Counter {
                                name := <str>$0,
                                value := 1,
                            } UNLESS CONFLICT ON .name
                            ELSE (
                                UPDATE test::Counter
                                SET { value := .value + 1 }
                            )
                        ).value
                    ", &(name,)).await?
            };
            Ok(val)
        }
    }).await?;
    Ok(val)
}

#[tokio::test]
async fn transaction_conflict() -> anyhow::Result<()> {
    let cli1 = Client::new(&SERVER.config);
    let cli2 = Client::new(&SERVER.config);
    tokio::try_join!(cli1.ensure_connected(), cli2.ensure_connected())?;
    let barrier = Arc::new(Barrier::new(2));
    let lock = Arc::new(Mutex::new(()));
    let iters = Arc::new(AtomicUsize::new(0));

    // TODO(tailhook) set retry options
    let res = tokio::try_join!(
        transaction1(cli1, "x", iters.clone(), barrier.clone(), lock.clone()),
        transaction1(cli2, "x", iters.clone(), barrier.clone(), lock.clone()),
    );
    println!("Result {:#?}", res);
    let tup = res?;

    assert!(tup == (1, 2) || tup == (2, 1), "Wrong result: {:?}", tup);
    assert_eq!(iters.load(Ordering::SeqCst), 3);
    Ok(())
}

async fn get_counter_value(tx: &mut Transaction, name: &str)
    -> anyhow::Result<i32>
{
    let value = tx.query_required_single("
            SELECT (
                INSERT test::Counter {
                    name := <str>$0,
                    value := 1,
                } UNLESS CONFLICT ON .name
                ELSE (
                    UPDATE test::Counter
                    SET { value := .value + 1 }
                )
            ).value
        ", &(name,)).await?;
    Ok(value)
}

async fn transaction1e(
    client: Client, name: &str, iterations: Arc<AtomicUsize>,
    barrier: Arc<Barrier>, lock: Arc<Mutex<()>>)
    -> anyhow::Result<i32>
{
    let val = client.transaction(|mut tx| {
        let lock = lock.clone();
        let iterations = iterations.clone();
        let barrier = barrier.clone();
        async move {
            iterations.fetch_add(1, Ordering::SeqCst);
            // This magic query makes starts real transaction,
            // that is otherwise started lazily
            tx.query::<i64, _>("SELECT 1", &()).await?;
            barrier.wait().await;
            let _lock = lock.lock().await;
            let val = get_counter_value(&mut tx, name).await?;
            Ok(val)
        }
    }).await?;
    Ok(val)
}

#[tokio::test]
async fn transaction_conflict_with_complex_err() -> anyhow::Result<()> {
    let cli1 = Client::new(&SERVER.config);
    let cli2 = Client::new(&SERVER.config);
    tokio::try_join!(cli1.ensure_connected(), cli2.ensure_connected())?;
    let barrier = Arc::new(Barrier::new(2));
    let lock = Arc::new(Mutex::new(()));
    let iters = Arc::new(AtomicUsize::new(0));

    // TODO(tailhook) set retry options
    let res = tokio::try_join!(
        transaction1e(cli1, "x", iters.clone(), barrier.clone(), lock.clone()),
        transaction1e(cli2, "x", iters.clone(), barrier.clone(), lock.clone()),
    );
    println!("Result {:#?}", res);
    let tup = res?;

    assert!(tup == (1, 2) || tup == (2, 1), "Wrong result: {:?}", tup);
    assert_eq!(iters.load(Ordering::SeqCst), 3);
    Ok(())
}
