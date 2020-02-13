use structopt::StructOpt;
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
    CreateDatabase(CreateDatabase),
    ListDatabases,
    Pgaddr,
    Psql,
    ListScalarTypes(ListTypes),
    ListRoles(ListRoles),
    Describe(Describe),
}

#[derive(StructOpt, Clone, Debug)]
pub struct CreateDatabase {
    pub database_name: String,
}


#[derive(StructOpt, Clone, Debug)]
pub struct ListTypes {
    pub pattern: Option<String>,
    #[structopt(long, short="I")]
    pub case_sensitive: bool,
    #[structopt(long, short="S")]
    pub system: bool,
}

#[derive(StructOpt, Clone, Debug)]
pub struct ListRoles {
    pub pattern: Option<String>,
    #[structopt(long, short="I")]
    pub case_sensitive: bool,
}

#[derive(StructOpt, Clone, Debug)]
pub struct Describe {
    pub name: String,
    #[structopt(long, short="v")]
    pub verbose: bool,
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
