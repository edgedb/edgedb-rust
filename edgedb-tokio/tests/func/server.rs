use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::os::unix::io::FromRawFd;
use std::process::{self, Stdio};
use std::str::FromStr;
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

        let mut cmd = Command::new(&bin_name);
        cmd.env("EDGEDB_SERVER_SECURITY", "insecure_dev_mode");
        cmd.arg("--temp-dir");
        cmd.arg("--testmode");
        cmd.arg("--emit-server-status=fd://3");
        cmd.arg("--port=auto");
        cmd.arg("--tls-cert-mode=generate_self_signed");

        // pipe server status on fd 3 into a reader bellow
        let (status_read, status_write) = nix::unistd::pipe().unwrap();
        cmd.fd_mappings(vec![FdMapping {
            parent_fd: status_write,
            child_fd: 3,
        }])
        .unwrap();

        // pipe stderr into a buffer that's printed only when there is an error
        cmd.stderr(Stdio::piped());

        if nix::unistd::Uid::effective().as_raw() == 0 {
            use std::os::unix::process::CommandExt;
            // This is moslty true in vagga containers, so run edgedb/postgres
            // by any non-root user
            cmd.uid(1);
        }

        eprintln!("Starting {}", bin_name);

        let mut process = cmd
            .spawn()
            .unwrap_or_else(|_| panic!("Can run {}", bin_name));

        // write log file
        let stdout = process.stderr.take().unwrap();
        std::thread::spawn(move || write_log_into_file(stdout));

        // wait for server to start
        let result = wait_for_server_status(status_read);

        let mut sinfo = SHUTDOWN_INFO.lock().expect("shutdown mutex works");
        if sinfo.is_empty() {
            shutdown_hooks::add_shutdown_hook(stop_processes);
        }
        sinfo.push(ShutdownInfo { process });
        let info = result.unwrap();

        // delete all migration files generated in previous runs
        if let Ok(read_dir) = fs::read_dir("tests/func/dbschema/migrations/") {
            for entry in read_dir {
                let dir_entry = entry.unwrap();
                fs::remove_file(dir_entry.path()).ok();
            }
        }

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

/// Reads the stream at file descriptor `status_read` until edgedb-server notifies that it is ready
fn wait_for_server_status(status_read: i32) -> Result<ServerInfo, anyhow::Error> {
    let pipe = BufReader::new(unsafe { File::from_raw_fd(status_read) });
    let mut result = Err(anyhow::anyhow!("no server info emitted"));
    for line in pipe.lines() {
        match line {
            Ok(line) => {
                if let Some(data) = line.strip_prefix("READY=") {
                    let data: ServerInfo = serde_json::from_str(data).expect("valid server data");
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
    result
}

/// Writes a stream to a log file
fn write_log_into_file(stream: impl std::io::Read) {
    let target_tmp_dir = std::env::var("CARGO_TARGET_TMPDIR").unwrap();
    let mut log_dir = std::path::PathBuf::from_str(&target_tmp_dir).unwrap();
    log_dir.push("server-logs");

    let time_the_epoch = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis();

    let mut log_file = log_dir.clone();
    let file_name = time_the_epoch.to_string() + ".log";
    log_file.push(file_name);

    eprintln!("Writing server logs into {:?}", &log_file);

    std::fs::create_dir_all(&log_dir).unwrap();
    let mut log_file = File::create(log_file).unwrap();

    let mut reader = BufReader::new(stream);
    std::io::copy(&mut reader, &mut log_file).unwrap();
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
