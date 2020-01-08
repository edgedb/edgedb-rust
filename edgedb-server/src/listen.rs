use std::time::Duration;

use async_std::net::{TcpListener, TcpStream};
use async_std::task;
use async_std::future::Future;
use async_std::stream::StreamExt;

use anyhow;

use crate::options::Options;


pub async fn accept_loop(options: Options) -> Result<(), anyhow::Error> {
    use std::io::ErrorKind::*;
    let addr = (&options.bind_address[..], options.port);
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = match stream {
            Ok(stream) => stream,
            Err(e) => match e.kind() {
                // These errors that are per-connection.
                // Which basically means that if we get this error from
                // `accept()` system call it means next connection might be
                // ready to be accepted.
                | ConnectionRefused
                | ConnectionAborted
                | ConnectionReset
                => continue,
                // All other errors will incur a timeout before next
                // `accept()` is performed.  The timeout is useful to handle
                // resource exhaustion errors like ENFILE and EMFILE.
                // Otherwise, could enter into tight loop.
                _ => {
                    eprintln!("Sleeping");
                    task::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
        };
        spawn_and_log_error(connection_loop(stream));
    }
    Ok(())
}

// TODO(tailhook) move to real log
fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<(), anyhow::Error>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })
}

async fn connection_loop(_stream: TcpStream) -> Result<(), anyhow::Error> {
    task::sleep(Duration::from_secs(10)).await;
    Ok(())
}
