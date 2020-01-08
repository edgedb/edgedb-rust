use std::path::PathBuf;

use structopt::StructOpt;
use structopt::clap::AppSettings;


#[derive(StructOpt)]
#[structopt(setting=AppSettings::UnifiedHelpMessage)]
pub struct Options {
    /// Database cluster directory
    #[structopt(long)]
    pub data_dir: Option<PathBuf>,

    /// DSN of a remote Postgres cluster, if using one")]
    #[structopt(long, value_name="dsn")]
    pub postgres_dsn: Option<String>,

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
    pub background: bool,

    /// Path to PID file directory
    #[structopt(long, value_name="dir")]
    pub pidfile: Option<PathBuf>,

    #[structopt(long)]
    pub daemon_user: Option<u32>,
    #[structopt(long)]
    pub daemon_group: Option<u32>,

    /// Directory where UNIX sockets will be created
    /// ("/run" on Linux by default)
    #[structopt(long, value_name="dir")]
    pub runstate_dir: Option<String>,

    #[structopt(long, value_name="int")]
    pub max_backend_connections: Option<u32>,
}

