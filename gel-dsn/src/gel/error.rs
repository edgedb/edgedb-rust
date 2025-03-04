use crate::host::HostParseError;
use std::{convert::Infallible, num::ParseIntError};

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, PartialOrd, Ord)]
pub enum CompoundSource {
    Dsn,
    Instance,
    CredentialsFile,
    HostPort,
    UnixSocket,
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, PartialOrd, Ord)]

pub enum TlsSecurityError {
    IncompatibleSecurityOptions,
    InvalidValue,
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, PartialOrd, Ord)]
pub enum InstanceNameError {
    InvalidInstanceName,
    InvalidCloudOrgName,
    InvalidCloudInstanceName,
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, PartialOrd, Ord)]
pub enum InvalidCredentialsFileError {
    FileNotFound,
    #[display("{}={}, {}={}", _0.0, _0.1, _1.0, _1.1)]
    ConflictingSettings((String, String), (String, String)),
    SerializationError(String),
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, PartialOrd, Ord)]
pub enum InvalidSecretKeyError {
    InvalidJwt,
    MissingIssuer,
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, PartialOrd, Ord)]
pub enum InvalidDsnError {
    InvalidScheme,
    ParseError,
    DuplicateOptions(String),
    BranchAndDatabase,
}

#[derive(Debug, derive_more::Error, derive_more::Display, PartialEq, Eq, PartialOrd, Ord)]
pub enum ParseError {
    CredentialsFileNotFound,
    EnvNotFound,
    ExclusiveOptions,
    FileNotFound,
    InvalidCredentialsFile(#[error(not(source))] InvalidCredentialsFileError),
    InvalidDatabase,
    InvalidDsn(#[error(not(source))] InvalidDsnError),
    InvalidDsnOrInstanceName,
    InvalidHost,
    InvalidInstanceName(#[error(not(source))] InstanceNameError),
    InvalidPort,
    InvalidSecretKey(#[error(not(source))] InvalidSecretKeyError),
    InvalidTlsSecurity(#[error(not(source))] TlsSecurityError),
    InvalidUser,
    InvalidCertificate,
    InvalidDuration,
    #[display("{:?}", _0)]
    MultipleCompoundEnv(#[error(not(source))] Vec<CompoundSource>),
    #[display("{:?}", _0)]
    MultipleCompoundOpts(#[error(not(source))] Vec<CompoundSource>),
    NoOptionsOrToml,
    ProjectNotInitialised,
    SecretKeyNotFound,
    UnixSocketUnsupported,
}

impl ParseError {
    pub fn error_type(&self) -> &str {
        match self {
            Self::EnvNotFound => "env_not_found",
            Self::CredentialsFileNotFound => "credentials_file_not_found",
            Self::ExclusiveOptions => "exclusive_options",
            Self::FileNotFound => "file_not_found",
            Self::InvalidCredentialsFile(_) => "invalid_credentials_file",
            Self::InvalidDatabase => "invalid_database",
            Self::InvalidDsn(_) => "invalid_dsn",
            Self::InvalidDsnOrInstanceName => "invalid_dsn_or_instance_name",
            Self::InvalidHost => "invalid_host",
            Self::InvalidInstanceName(_) => "invalid_instance_name",
            Self::InvalidPort => "invalid_port",
            Self::InvalidSecretKey(_) => "invalid_secret_key",
            Self::InvalidTlsSecurity(_) => "invalid_tls_security",
            Self::InvalidUser => "invalid_user",
            Self::InvalidCertificate => "invalid_certificate",
            Self::InvalidDuration => "invalid_duration",
            Self::MultipleCompoundEnv(_) => "multiple_compound_env",
            Self::MultipleCompoundOpts(_) => "multiple_compound_opts",
            Self::NoOptionsOrToml => "no_options_or_toml",
            Self::ProjectNotInitialised => "project_not_initialised",
            Self::SecretKeyNotFound => "secret_key_not_found",
            Self::UnixSocketUnsupported => "unix_socket_unsupported",
        }
    }

    pub fn gel_error(self) -> gel_errors::Error {
        use gel_errors::ErrorKind;

        match self {
            Self::EnvNotFound
            | Self::CredentialsFileNotFound
            | Self::ExclusiveOptions
            | Self::FileNotFound
            | Self::InvalidCredentialsFile(_)
            | Self::InvalidDatabase
            | Self::InvalidDsn(_)
            | Self::InvalidDsnOrInstanceName
            | Self::InvalidHost
            | Self::InvalidInstanceName(_)
            | Self::InvalidPort
            | Self::InvalidSecretKey(_)
            | Self::InvalidTlsSecurity(_)
            | Self::InvalidUser
            | Self::InvalidCertificate
            | Self::InvalidDuration
            | Self::MultipleCompoundEnv(_)
            | Self::MultipleCompoundOpts(_)
            | Self::NoOptionsOrToml
            | Self::ProjectNotInitialised
            | Self::UnixSocketUnsupported => {
                gel_errors::ClientNoCredentialsError::with_source(self)
            }

            Self::SecretKeyNotFound => gel_errors::NoCloudConfigFound::with_source(self),
        }
    }
}

impl From<ParseError> for gel_errors::Error {
    fn from(val: ParseError) -> Self {
        val.gel_error()
    }
}

impl From<ParseIntError> for ParseError {
    fn from(_: ParseIntError) -> Self {
        ParseError::InvalidPort
    }
}

impl From<HostParseError> for ParseError {
    fn from(_: HostParseError) -> Self {
        ParseError::InvalidHost
    }
}

impl From<std::env::VarError> for ParseError {
    fn from(error: std::env::VarError) -> Self {
        match error {
            std::env::VarError::NotPresent => ParseError::EnvNotFound,
            std::env::VarError::NotUnicode(_) => ParseError::EnvNotFound,
        }
    }
}

impl From<Infallible> for ParseError {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, derive_more::Display, PartialOrd, Ord)]
pub enum Warning {
    #[display("Deprecated credential property: {_0}")]
    DeprecatedCredentialProperty(String),
    #[display("Deprecated environment variable: {_0}")]
    DeprecatedEnvironmentVariable(String, String),
    #[display("Multiple environment variables set: {}", _0.join(", "))]
    MultipleEnvironmentVariables(Vec<String>),
    #[display("{_0} is ignored when using Docker TCP port")]
    DockerPortIgnored(String),
}

#[derive(Debug, Default)]
pub struct Warnings {
    warnings: Vec<Warning>,
}

impl Warnings {
    pub fn warn(&mut self, warning: Warning) {
        self.warnings.push(warning);
    }

    pub fn into_vec(self) -> Vec<Warning> {
        self.warnings
    }

    pub fn iter(&self) -> impl Iterator<Item = &Warning> {
        self.warnings.iter()
    }
}

impl<'a> IntoIterator for &'a Warnings {
    type Item = &'a Warning;

    type IntoIter = std::slice::Iter<'a, Warning>;

    fn into_iter(self) -> Self::IntoIter {
        self.warnings.iter()
    }
}
