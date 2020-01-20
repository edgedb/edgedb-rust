use std::path::PathBuf;

use log::LevelFilter;
use structopt::StructOpt;
use structopt::clap::AppSettings;

use crate::postgres;


#[derive(StructOpt, Debug)]
#[structopt(setting=AppSettings::UnifiedHelpMessage)]
pub struct TmpOptions {
    /// Database cluster directory
    #[structopt(long, short="D")]
    pub data_dir: Option<PathBuf>,

    /// DSN of a remote Postgres cluster, if using one")]
    #[structopt(long, value_name="dsn")]
    pub postgres_dsn: Option<postgres::Dsn>,

    /// Logging level. Possible values:
    /// (d)ebug, (i)nfo, (w)arn, (e)rror, (s)ilent
    #[structopt(short="l", long)]
    pub log_level: Option<String>, // TODO(tailhook) make enum

    /// Send logs to <dest>, where <dest> can be a file
    /// name, "syslog", or "stderr"
    #[structopt(long, value_name="dest")]
    pub log_to: Option<PathBuf>,

    /// Bootstrap the database cluster and exit
    #[structopt(long)]
    pub bootstrap: bool,

    /// The name of the default database to create
    #[structopt(long, value_name="text")]
    pub default_database: Option<String>,

    /// The name of the default database owner
    #[structopt(long, value_name="text")]
    pub default_database_user: Option<String>,

    /// enable the development mode
    #[structopt(long)]
    pub devmode: bool,

    #[structopt(long, overrides_with="devmode", hidden=true)]
    pub _no_devmode: bool,

    /// enable the test mode
    #[structopt(long)]
    pub testmode: bool,

    #[structopt(long, overrides_with="devmode", hidden=true)]
    pub _no_testmode: bool,

    /// IP address to listen on
    #[structopt(short="I", long, value_name="host-or-ip",
                default_value="127.0.0.1")]
    pub bind_address: String,

    /// Port to listen on
    #[structopt(short="P", long, value_name="integer", default_value="5656")]
    pub port: u16,

    /// Daemonize
    #[structopt(short="b", long)]
    background: bool,

    /// Path to PID file directory
    #[structopt(long, value_name="dir", default_value="/run/edgedb")]
    pidfile_dir: PathBuf,

    #[structopt(long)]
    daemon_user: Option<u32>,
    #[structopt(long)]
    daemon_group: Option<u32>,

    /// Directory where UNIX sockets will be created
    /// ("/run" on Linux by default)
    #[structopt(long, value_name="dir")]
    runstate_dir: Option<PathBuf>,

    #[structopt(long, value_name="int", default_value="100")]
    max_backend_connections: usize,
}

#[derive(Debug)]
pub enum Mode {
    DataDir(PathBuf),
    External(postgres::Dsn),
}

#[derive(Debug)]
pub struct Options {
    pub log_level: LevelFilter,
    pub log_to: Option<PathBuf>,
    pub bootstrap: bool,
    pub default_database: Option<String>,
    pub default_database_user: Option<String>,
    pub devmode: bool,
    pub testmode: bool,
    pub bind_address: String,
    pub port: u16,
    pub mode: Mode,
    pub background: bool,
    pub pidfile_dir: PathBuf,
    pub daemon_user: Option<u32>,
    pub daemon_group: Option<u32>,
    pub runstate_dir: PathBuf,
    pub max_backend_connections: usize,
}

impl Options {
    pub fn from_args() -> Options {
        let t = TmpOptions::from_args();
        let data_dir = t.data_dir.clone();
        return Options {
            log_level: match t.log_level.as_ref().map(|x| &x[..]) {
                None => LevelFilter::Info,
                Some("d") | Some("debug") => LevelFilter::Debug,
                Some("i") | Some("info") => LevelFilter::Info,
                Some("w") | Some("warn") => LevelFilter::Warn,
                Some("e") | Some("error") => LevelFilter::Error,
                Some("s") | Some("silent") => LevelFilter::Off,
                // TODO(tailhook) exit app with error
                Some(_) => LevelFilter::Error,
            },
            log_to: t.log_to,
            bootstrap: t.bootstrap,
            default_database: t.default_database,
            default_database_user: t.default_database_user,
            devmode: t.devmode,
            testmode: t.testmode,
            bind_address: t.bind_address,
            port: t.port,
            background: t.background,
            pidfile_dir: t.pidfile_dir,
            daemon_user: t.daemon_user,
            daemon_group: t.daemon_group,
            runstate_dir: t.runstate_dir.unwrap_or_else(|| {
                data_dir.unwrap_or("/run/edgedb".into())
            }),
            max_backend_connections: t.max_backend_connections,
            mode: if let Some(dsn) = t.postgres_dsn {
                Mode::External(dsn)
            } else {
                Mode::DataDir(t.data_dir.expect("data_dir or dsn"))
            },
        }
    }
}
