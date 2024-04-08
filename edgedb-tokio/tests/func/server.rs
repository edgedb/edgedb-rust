use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::os::unix::io::FromRawFd;
use std::process;
use std::sync::Mutex;

use command_fds::{CommandFdExt, FdMapping};
use once_cell::sync::Lazy;

use edgedb_tokio::{Builder, Config};

pub static SHUTDOWN_INFO: Lazy<Mutex<Vec<ShutdownInfo>>> = Lazy::new(|| Mutex::new(Vec::new()));
pub static SERVER: Lazy<ServerGuard> = Lazy::new(ServerGuard::start);

pub struct ShutdownInfo {
    process: process::Child,
}

pub struct ServerGuard {
    pub config: Config,
}

#[derive(Debug, serde::Deserialize)]
pub struct ServerInfo {
    port: u16,
    tls_cert_file: String,
}

impl ServerGuard {
    fn start() -> ServerGuard {
        use std::process::Command;

        let bin_name = if let Ok(ver) = env::var("EDGEDB_MAJOR_VERSION") {
            format!("edgedb-server-{}", ver)
        } else {
            "edgedb-server".to_string()
        };
        let (pipe_read, pipe_write) = nix::unistd::pipe().unwrap();
        let mut cmd = Command::new(&bin_name);
        cmd.env("EDGEDB_SERVER_SECURITY", "insecure_dev_mode");
        cmd.arg("--temp-dir");
        cmd.arg("--testmode");
        cmd.arg("--emit-server-status=fd://3");
        cmd.arg("--port=auto");
        cmd.arg("--tls-cert-mode=generate_self_signed");
        cmd.fd_mappings(vec![FdMapping {
            parent_fd: pipe_write,
            child_fd: 3,
        }])
        .unwrap();

        if nix::unistd::Uid::effective().as_raw() == 0 {
            use std::os::unix::process::CommandExt;
            // This is moslty true in vagga containers, so run edgedb/postgres
            // by any non-root user
            cmd.uid(1);
        }

        let process = cmd.spawn().unwrap_or_else(|_| panic!("Can run {}", bin_name));
        let pipe = BufReader::new(unsafe { File::from_raw_fd(pipe_read) });
        let mut result = Err(anyhow::anyhow!("no server info emitted"));
        for line in pipe.lines() {
            match line {
                Ok(line) => {
                    if let Some(data) = line.strip_prefix("READY=") {
                        let data: ServerInfo =
                            serde_json::from_str(data).expect("valid server data");
                        println!("Server data {:?}", data);
                        result = Ok(data);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from server: {}", e);
                    result = Err(e.into());
                    break;
                }
            }
        }

        let mut sinfo = SHUTDOWN_INFO.lock().expect("shutdown mutex works");
        if sinfo.is_empty() {
            shutdown_hooks::add_shutdown_hook(stop_processes);
        }
        sinfo.push(ShutdownInfo { process });
        let info = result.unwrap();

        fs::remove_file("tests/func/dbschema/migrations/00001.edgeql").ok();
        assert!(Command::new("edgedb")
            .current_dir("./tests/func")
            .arg("--tls-security")
            .arg("insecure")
            .arg("--port")
            .arg(info.port.to_string())
            .arg("migration")
            .arg("create")
            .arg("--non-interactive")
            .status()
            .expect("cannot run edgedb-cli to create a migration")
            .success());
        dbg!("2");
        assert!(Command::new("edgedb")
            .current_dir("./tests/func")
            .arg("--tls-security")
            .arg("insecure")
            .arg("--port")
            .arg(info.port.to_string())
            .arg("migration")
            .arg("apply")
            .status()
            .expect("cannot run edgedb-cli to apply a migration")
            .success());

        let cert_data =
            std::fs::read_to_string(&info.tls_cert_file).expect("cert file should be readable");
        let config = Builder::new()
            .port(info.port)
            .unwrap()
            .pem_certificates(&cert_data)
            .unwrap()
            .constrained_build()
            .unwrap();
        ServerGuard { config }
    }
}

extern "C" fn stop_processes() {
    let mut items = SHUTDOWN_INFO.lock().expect("shutdown mutex works");
    for item in items.iter_mut() {
        term_process(&mut item.process);
    }
    for item in items.iter_mut() {
        item.process.wait().ok();
    }
}

fn term_process(proc: &mut process::Child) {
    use nix::sys::signal::{self, Signal};
    use nix::unistd::Pid;

    if let Err(e) = signal::kill(Pid::from_raw(proc.id() as i32), Signal::SIGTERM) {
        eprintln!("could not send SIGTERM to edgedb-server: {:?}", e);
    };
}
