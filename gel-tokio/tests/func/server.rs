use std::{path::PathBuf, str::FromStr};

use gel_tokio::{Builder, Config};
use once_cell::sync::Lazy;
use test_utils::server::ServerInstance;

pub struct ServerGuard {
    instance: ServerInstance,
    pub config: Config,
}

pub static SERVER: Lazy<ServerGuard> = Lazy::new(start_server);

/// Starts edgedb-server. Stops it after the test process exits.
/// Writes its log into a tmp file.
///
/// To debug, run any test with --nocapture Rust flag.
fn start_server() -> ServerGuard {
    shutdown_hooks::add_shutdown_hook(stop_server);

    let instance = ServerInstance::start();

    instance.apply_schema(&PathBuf::from_str("./tests/func/dbschema").unwrap());

    let cert_data = std::fs::read_to_string(&instance.info.tls_cert_file)
        .expect("cert file should be readable");
    let config = Builder::new()
        .port(instance.info.port)
        .unwrap()
        .pem_certificates(&cert_data)
        .unwrap()
        .constrained_build() // if this method is not found, you need --features=unstable
        .unwrap();
    ServerGuard { instance, config }
}

extern "C" fn stop_server() {
    SERVER.instance.stop()
}
