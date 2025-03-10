#![allow(dead_code)]
use dtor::dtor;
use gel_tokio::{Builder, Config};
use once_cell::sync::Lazy;
use std::{path::PathBuf, str::FromStr};
use test_utils::server::ServerInstance;

pub struct ServerGuard {
    instance: ServerInstance,
    pub config: Config,
}

pub static SERVER: Lazy<ServerGuard> = Lazy::new(start_server);

#[dtor]
unsafe fn stop_server() {
    SERVER.instance.stop()
}

/// Starts gel-server. Stops it after the test process exits.
/// Writes its log into a tmp file.
///
/// To debug, run any test with --nocapture Rust flag.
fn start_server() -> ServerGuard {
    let instance = ServerInstance::start();

    let schema_dir = PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))
        .unwrap()
        .join("functional")
        .join("testdata")
        .join("dbschema");
    eprintln!("Applying schema in {schema_dir:?}");
    instance.apply_schema(&schema_dir);

    let cert_data = std::fs::read_to_string(&instance.info.tls_cert_file)
        .expect("cert file should be readable");
    let config = Builder::new()
        .port(instance.info.port)
        .tls_ca_string(&cert_data)
        .without_system()
        .build()
        .unwrap();
    ServerGuard { instance, config }
}
