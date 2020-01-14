use std::io;
use std::time::Duration;
use std::fs;

use async_std::net::TcpListener;
use async_std::task;
use async_std::future::Future;
use async_std::stream::StreamExt;
use anyhow;
use async_listen::{ListenExt, ByteStream, backpressure, error_hint};

use crate::options::Options;
use crate::connection::connection_loop;

fn spawn<F>(f: F)
    where F: Future<Output=Result<(), ()>> + Send + 'static,
{
    task::spawn(f);
}

#[cfg(not(unix))]
pub async fn accept_loop(options: Options) -> Result<(), anyhow::Error> {
    _accept_loop(options)
}

#[allow(dead_code)]  // only for windows, but always compile to check validity
pub async fn _accept_loop(options: Options) -> Result<(), anyhow::Error> {
    let addr = (&options.bind_address[..], options.port);
    let (_, bp) = backpressure::new(options.max_backend_connections);
    let tcp_listener = TcpListener::bind(addr).await?;
    log::info!("Serving on {:?}", addr);
    let mut incoming = tcp_listener.incoming()
        .log_warnings(log_accept_error)
        .handle_errors(Duration::from_millis(500))
        .backpressure_wrapper(bp);
    while let Some(stream) = incoming.next().await {
        spawn(connection_loop(stream));
    }
    Ok(())
}

#[cfg(unix)]
pub async fn accept_loop(options: Options) -> Result<(), anyhow::Error> {
    use async_std::os::unix::net::UnixListener;

    let unix_path = options.runstate_dir
        .join(format!(".s.EDGEDB.{}", options.port));
    let unix_path_admin = options.runstate_dir
        .join(format!(".s.EDGEDB.admin.{}", options.port));

    let addr = (&options.bind_address[..], options.port);
    let (ctl, throttle) = backpressure::new(options.max_backend_connections);

    // First listen TCP socket, this ensures that process is unique
    let tcp_listener = TcpListener::bind(addr).await?;
    log::info!("Serving on {:?}", addr);
    // If TCP socket is bind, we don't conflict on Unix socket
    fs::remove_file(&unix_path).ok();
    fs::remove_file(&unix_path_admin).ok();
    let unix_listener = UnixListener::bind(&unix_path).await?;
    log::info!("Serving on {:?}", unix_path);
    let unix_admin_listener = UnixListener::bind(&unix_path_admin).await?;
    log::info!("Serving on {:?}", unix_path_admin);

    let ctl1 = ctl.clone();
    task::spawn(async move {
        let mut incoming = unix_admin_listener.incoming()
            .log_warnings(log_accept_error)
            .handle_errors(Duration::from_millis(500))
            // We occupy connection slot but don't block new connection on
            // admin socket. So we can connect in case in emergency
            .map(|conn| ByteStream::new_unix(ctl1.token(), conn));
        while let Some(stream) = incoming.next().await {
            spawn(connection_loop(stream));
        }
    });


    let ctl2 = ctl.clone();
    let ctl3 = ctl.clone();
    let incoming_tcp = tcp_listener.incoming()
        .log_warnings(log_accept_error)
        .handle_errors(Duration::from_millis(500))
        .map(|conn| ByteStream::new_tcp(ctl2.token(), conn));
    let incoming_unix = unix_listener.incoming()
        .log_warnings(log_accept_error)
        .handle_errors(Duration::from_millis(500))
        .map(|conn| ByteStream::new_unix(ctl3.token(), conn));
    let mut incoming = incoming_tcp.merge(incoming_unix)
        .apply_backpressure(throttle);

    while let Some(stream) = incoming.next().await {
        spawn(connection_loop(stream));
    }
    Ok(())
}

fn log_accept_error(e: &io::Error) {
    log::error!("Accept error: {}. Paused for 0.5s. {}", e, error_hint(&e));
}
