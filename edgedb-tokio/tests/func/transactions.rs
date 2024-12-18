use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

use edgedb_errors::NoDataError;
use edgedb_tokio::{Client, Transaction};

use crate::server::SERVER;

struct OnceBarrier(AtomicBool, tokio::sync::Barrier);

impl OnceBarrier {
    fn new(n: usize) -> OnceBarrier {
        OnceBarrier(AtomicBool::new(false), tokio::sync::Barrier::new(n))
    }
    async fn wait(&self) {
        if self.0.load(Ordering::SeqCst) {
            return;
        }
        self.1.wait().await;
        self.0.store(true, Ordering::SeqCst)
    }
}

async fn transaction1(
    client: Client,
    name: &str,
    iterations: Arc<AtomicUsize>,
    barrier: Arc<OnceBarrier>,
    lock: Arc<Mutex<()>>,
) -> anyhow::Result<i32> {
    let val = client
        .retryable_transaction(|mut tx| {
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
                    tx.query_required_single(
                        "
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
                    ",
                        &(name,),
                    )
                    .await?
                };
                Ok(val)
            }
        })
        .await?;
    Ok(val)
}

#[test_log::test(tokio::test)]
async fn transaction_conflict() -> anyhow::Result<()> {
    let cli1 = Client::new(&SERVER.config);
    let cli2 = Client::new(&SERVER.config);
    tokio::try_join!(cli1.ensure_connected(), cli2.ensure_connected())?;
    let barrier = Arc::new(OnceBarrier::new(2));
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

async fn get_counter_value(tx: &mut Transaction, name: &str) -> anyhow::Result<i32> {
    let value = tx
        .query_required_single(
            "
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
        ",
            &(name,),
        )
        .await?;
    Ok(value)
}

async fn transaction1e(
    client: Client,
    name: &str,
    iterations: Arc<AtomicUsize>,
    barrier: Arc<OnceBarrier>,
    lock: Arc<Mutex<()>>,
) -> anyhow::Result<i32> {
    let val = client
        .retryable_transaction(|mut tx| {
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
        })
        .await?;
    Ok(val)
}

#[tokio::test]
async fn transaction_conflict_with_complex_err() -> anyhow::Result<()> {
    let cli1 = Client::new(&SERVER.config);
    let cli2 = Client::new(&SERVER.config);
    tokio::try_join!(cli1.ensure_connected(), cli2.ensure_connected())?;
    let barrier = Arc::new(OnceBarrier::new(2));
    let lock = Arc::new(Mutex::new(()));
    let iters = Arc::new(AtomicUsize::new(0));

    // TODO(tailhook) set retry options
    let res = tokio::try_join!(
        transaction1e(cli1, "y", iters.clone(), barrier.clone(), lock.clone()),
        transaction1e(cli2, "y", iters.clone(), barrier.clone(), lock.clone()),
    );
    println!("Result {:#?}", res);
    let tup = res?;

    assert!(tup == (1, 2) || tup == (2, 1), "Wrong result: {:?}", tup);
    assert_eq!(iters.load(Ordering::SeqCst), 3);
    Ok(())
}

#[tokio::test]
async fn queries() -> anyhow::Result<()> {
    let client = Client::new(&SERVER.config);
    client
        .retryable_transaction(|mut tx| async move {
            let value = tx.query::<i64, _>("SELECT 7*93", &()).await?;
            assert_eq!(value, vec![651]);

            let value = tx.query_single::<i64, _>("SELECT 5*11", &()).await?;
            assert_eq!(value, Some(55));

            let value = tx.query_single::<i64, _>("SELECT <int64>{}", &()).await?;
            assert_eq!(value, None);

            let value = tx
                .query_required_single::<i64, _>("SELECT 5*11", &())
                .await?;
            assert_eq!(value, 55);

            let err = tx
                .query_required_single::<i64, _>("SELECT <int64>{}", &())
                .await
                .unwrap_err();
            assert!(err.is::<NoDataError>());

            let value = tx.query_json("SELECT 'x' ++ 'y'", &()).await?;
            assert_eq!(value.as_ref(), r#"["xy"]"#);

            let value = tx.query_single_json("SELECT 'x' ++ 'y'", &()).await?;
            assert_eq!(value.as_deref(), Some(r#""xy""#));

            let value = tx.query_single_json("SELECT <str>{}", &()).await?;
            assert_eq!(value.as_deref(), None);

            let err = tx
                .query_required_single_json("SELECT <int64>{}", &())
                .await
                .unwrap_err();
            assert!(err.is::<NoDataError>());

            tx.execute("SELECT 1+1", &()).await?;

            Ok(())
        })
        .await?;
    Ok(())
}
