use structopt::StructOpt;
use structopt::clap::AppSettings;
use std::env;

use whoami;
use atty;


#[derive(StructOpt, Debug)]
struct TmpOptions {
    #[structopt(short="H", long)]
    pub host: Option<String>,
    #[structopt(short="P", long)]
    pub port: Option<u16>,
    #[structopt(short="u", long)]
    pub user: Option<String>,
    #[structopt(short="d", long)]
    pub database: Option<String>,
    #[structopt(long)]
    pub admin: bool,
    #[structopt(long)]
    pub password: bool,
    #[structopt(long)]
    pub no_password: bool,
    #[structopt(long)]
    pub password_from_stdin: bool,

    #[structopt(long)]
    pub debug_print_data_frames: bool,
    #[structopt(long)]
    pub debug_print_descriptors: bool,
    #[structopt(long)]
    pub debug_print_codecs: bool,

    #[structopt(subcommand)]
    pub subcommand: Option<Command>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Password {
    NoPassword,
    FromTerminal,
    Password(String),
}

#[derive(StructOpt, Clone, Debug)]
pub enum Command {
    AlterRole(RoleParams),
    CreateDatabase(CreateDatabase),
    CreateSuperuserRole(RoleParams),
    DropRole(RoleName),
    ListDatabases,
    Pgaddr,
    Psql,
    ListAliases(ListAliases),
    ListCasts(ListCasts),
    ListIndexes(ListIndexes),
    ListScalarTypes(ListTypes),
    ListObjectTypes(ListTypes),
    ListRoles(ListRoles),
    ListModules(ListModules),
    /// Modify database configuration
    Configure(Configure),
    Describe(Describe),
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct CreateDatabase {
    pub database_name: String,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ListAliases {
    pub pattern: Option<String>,
    #[structopt(long, short="I")]
    pub case_sensitive: bool,
    #[structopt(long, short="S")]
    pub system: bool,
    #[structopt(long, short="v")]
    pub verbose: bool,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ListCasts {
    pub pattern: Option<String>,
    #[structopt(long, short="I")]
    pub case_sensitive: bool,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ListIndexes {
    pub pattern: Option<String>,
    #[structopt(long, short="I")]
    pub case_sensitive: bool,
    #[structopt(long, short="S")]
    pub system: bool,
    #[structopt(long, short="v")]
    pub verbose: bool,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ListTypes {
    pub pattern: Option<String>,
    #[structopt(long, short="I")]
    pub case_sensitive: bool,
    #[structopt(long, short="S")]
    pub system: bool,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ListRoles {
    pub pattern: Option<String>,
    #[structopt(long, short="I")]
    pub case_sensitive: bool,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ListModules {
    pub pattern: Option<String>,
    #[structopt(long, short="I")]
    pub case_sensitive: bool,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct Describe {
    pub name: String,
    #[structopt(long, short="v")]
    pub verbose: bool,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct Configure {
    #[structopt(subcommand)]
    pub command: ConfigureCommand,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub enum ConfigureCommand {
    /// Insert another configuration entry to the list setting
    Insert(ConfigureInsert),
    /// Reset configuration entry (empty the list for list settings)
    Reset(ConfigureReset),
    /// Set scalar configuration value
    Set(ConfigureSet),
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ConfigureInsert {
    #[structopt(subcommand)]
    pub parameter: ListParameter,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ConfigureReset {
    #[structopt(subcommand)]
    pub parameter: ConfigParameter,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ConfigureSet {
    #[structopt(subcommand)]
    pub parameter: ValueParameter,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub enum ListParameter {

    /// Insert a client authentication rule
    #[structopt(name="Auth")]
    Auth(AuthParameter),

    /// Insert an application port with the specicified protocol
    #[structopt(name="Port")]
    Port(PortParameter),
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
#[structopt(rename_all="snake_case")]
pub enum ValueParameter {
    /// Specifies the TCP/IP address(es) on which the server is to listen for
    /// connections from client applications.
    ///
    /// If the list is empty, the server does not listen on any IP interface
    /// at all, in which case only Unix-domain sockets can be used to connect
    /// to it.
    ListenAddresses(ListenAddresses),

    /// The TCP port the server listens on; 5656 by default. Note that the
    /// same port number is used for all IP addresses the server listens on.
    ListenPort(ListenPort),

    /// The amount of memory the database uses for shared memory buffers.
    ///
    /// Corresponds to the PostgreSQL configuration parameter of the same
    /// name. Changing this value requires server restart.
    SharedBuffers(ConfigStr),

    /// The amount of memory used by internal query operations such as sorting.
    ///
    /// Corresponds to the PostgreSQL work_mem configuration parameter.
    QueryWorkMem(ConfigStr),

    /// Sets the planner’s assumption about the effective size of the disk
    /// cache that is available to a single query.
    ///
    /// Corresponds to the PostgreSQL configuration parameter of the same name
    EffectiveCacheSize(ConfigStr),

    /// Sets the default data statistics target for the planner.
    ///
    /// Corresponds to the PostgreSQL configuration parameter of the same name
    DefaultStatisticsTarget(ConfigStr),
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
#[structopt(rename_all="snake_case")]
pub enum ConfigParameter {
    /// Reset listen addresses to 127.0.0.1
    ListenAddresses,
    /// Reset port to 5656
    ListenPort,
    /// Remove all the application ports
    #[structopt(name="Port")]
    Port,
    /// Clear authentication table (only admin socket can be used to connect)
    #[structopt(name="Auth")]
    Auth,
    /// Reset shared_buffers postgres configuration parameter to default value
    SharedBuffers,
    /// Reset work_mem postgres configuration parameter to default value
    QueryWorkMem,
    /// Reset postgres configuration parameter of the same name
    EffectiveCacheSize,
    /// Reset postgres configuration parameter of the same name
    DefaultStatisticsTarget,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ListenAddresses {
    pub address: Vec<String>,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ListenPort {
    pub port: u16,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct ConfigStr {
    pub value: String,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct RoleParams {
    /// Role name
    pub role: String,
    /// Set the password for role (read separately from the terminal)
    #[structopt(long="password")]
    pub password: bool,
    /// Set the password for role, read from the stdin
    #[structopt(long)]
    pub password_from_stdin: bool,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct RoleName {
    pub role: String,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct AuthParameter {
    /// The priority of the authentication rule. The lower this number, the
    /// higher the priority.
    #[structopt(long)]
    pub priority: i64,

    /// The name(s) of the database role(s) this rule applies to. If set to
    /// '*', then it applies to all roles.
    #[structopt(long="user")]
    pub users: Vec<String>,

    /// The name of the authentication method type. Valid values are: Trust
    /// for no authentication and SCRAM for SCRAM-SHA-256 password
    /// authentication.
    #[structopt(long)]
    pub method: String,

    /// An optional comment for the authentication rule.
    #[structopt(long)]
    pub comment: Option<String>,
}

#[derive(StructOpt, Clone, Debug)]
#[structopt(setting=AppSettings::DisableVersion)]
pub struct PortParameter {

    /// The TCP/IP address(es) for the application port.
    #[structopt(long="address")]
    pub addresses: Vec<String>,

    /// The TCP port for the application port.
    #[structopt(long)]
    pub port: u16,

    /// The protocol for the application port. Valid values are:
    /// 'graphql+http' and 'edgeql+http'.
    #[structopt(long)]
    pub protocol: String,

    /// The name of the database the application port is attached to.
    #[structopt(long)]
    pub database: String,

    /// The name of the database role the application port is attached to.
    #[structopt(long)]
    pub user: String,

    /// The maximum number of backend connections available for this
    /// application port.
    #[structopt(long)]
    pub concurrency: i64,
}


#[derive(Debug, Clone)]
pub struct Options {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    pub admin: bool,
    pub password: Password,
    pub subcommand: Option<Command>,
    pub interactive: bool,
    pub debug_print_data_frames: bool,
    pub debug_print_descriptors: bool,
    pub debug_print_codecs: bool,
}

impl Options {
    pub fn from_args_and_env() -> Options {
        let tmp = TmpOptions::from_args();
        let admin = tmp.admin;
        let user = tmp.user
            .or_else(|| env::var("EDGEDB_USER").ok())
            .unwrap_or_else(|| if admin  {
                String::from("edgedb")
            } else {
                whoami::username()
            });
        let host = tmp.host
            .or_else(|| env::var("EDGEDB_HOST").ok())
            .unwrap_or_else(|| String::from("localhost"));
        let port = tmp.port
            .or_else(|| env::var("EDGEDB_PORT").ok()
                        .and_then(|x| x.parse().ok()))
            .unwrap_or_else(|| 5656);
        let database = tmp.database
            .or_else(|| env::var("EDGEDB_DATABASE").ok())
            .unwrap_or_else(|| if admin  {
                String::from("edgedb")
            } else {
                user.clone()
            });

        // TODO(pc) add option to force interactive mode not on a tty (tests)
        let interactive = atty::is(atty::Stream::Stdin);
        let password = if tmp.password_from_stdin {
            let password = rpassword::read_password()
                .expect("password can be read");
            Password::Password(password)
        } else if tmp.no_password {
            Password::NoPassword
        } else {
            Password::FromTerminal
        };

        return Options {
            host, port, user, database, interactive,
            admin: tmp.admin,
            subcommand: tmp.subcommand,
            password,
            debug_print_data_frames: tmp.debug_print_data_frames,
            debug_print_descriptors: tmp.debug_print_descriptors,
            debug_print_codecs: tmp.debug_print_codecs,
        }
    }
}
