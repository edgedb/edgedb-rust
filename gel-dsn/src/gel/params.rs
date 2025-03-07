use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    num::NonZeroU16,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use url::Url;

use super::{
    env::Env,
    error::*,
    project::{find_project_file, ProjectDir},
    BuildContext, BuildContextImpl, ClientSecurity, CloudCerts, Config, DatabaseBranch,
    FromParamStr, InstanceName, Logging, Param, TcpKeepalive, TlsSecurity, DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_PORT, DEFAULT_WAIT,
};
use crate::{
    env::SystemEnvVars,
    file::SystemFileAccess,
    gel::{context_trace, Authentication},
    host::{Host, HostTarget, HostType},
    user::SystemUserProfile,
    EnvVar, FileAccess, UserProfile,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BuildPhase {
    Options,
    Environment,
    Project,
}

/// A builder for [`Config`].
#[derive(Debug, Clone, Default)]
pub struct Builder {
    params: Params,
}

macro_rules! define_params {
    ($(
        $(#[doc = $doc:expr])* $name:ident: $type:ty
    ),* $(,)?) => {
        /// The parameters used to build the [`Config`].
        #[derive(Clone, Default)]
        #[non_exhaustive]
        pub struct Params {
            $(
                $(#[doc = $doc])*
                pub $name: Param<$type>,
            )*

            pub server_settings: HashMap<String, String>,
        }

        impl Debug for Params {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let mut s = f.debug_struct("Params");
                $(
                    if self.$name.is_some() {
                        s.field(stringify!($name), &self.$name);
                    }
                )*
                s.finish()
            }
        }

        impl Params {
            pub fn merge(&mut self, mut other: Self) {
                // Special case: database/branch cannot be set if the other is
                // already set.
                if self.database.is_none() && self.branch.is_some() {
                    other.database = Param::None;
                }
                if self.branch.is_none() && self.database.is_some() {
                    other.branch = Param::None;
                }

                $(
                    if self.$name.is_none() {
                        self.$name = other.$name;
                    }
                )*


                for (key, value) in other.server_settings {
                    self.server_settings.entry(key).or_insert(value);
                }
            }
        }

        impl Builder {
            $(
                paste::paste!(
                    $(#[doc = $doc])*
                    pub fn $name(mut self, value: impl Into<$type>) -> Self {
                        self.params.$name = Param::Parsed(value.into());
                        self
                    }

                    $(#[doc = $doc])*
                    #[doc = "\n\nThis value will be loaded from `path` in the filesystem and parsed as [`"]
                    #[doc = stringify!($type)]
                    #[doc = "`]."]
                    pub fn [<$name _file>](mut self, path: impl AsRef<Path>) -> Self {
                        self.params.$name = Param::File(path.as_ref().to_path_buf());
                        self
                    }

                    $(#[doc = $doc])*
                    #[doc = "\n\nWill be loaded from the provided environment variableand parsed as [`"]
                    #[doc = stringify!($type)]
                    #[doc = "`]."]
                    pub fn [<$name _env>](mut self, value: impl AsRef<str>) -> Self {
                        self.params.$name = Param::Env(value.as_ref().to_string());
                        self
                    }
                );
            )*
        }
    }
}

define_params!(
    /// The datasource name URL. Valid URLs must start with `edgedb://` or `gel://`.
    dsn: String,
    /// The instance.
    instance: InstanceName,
    /// The credentials. Note that [`CredentialsFile`] is considered an opaque
    /// type that can only be created from a string (see [`std::str::FromStr`])
    /// or deserialized from a file (see [`Self::credentials_file`]).
    credentials: CredentialsFile,
    /// The host. Note that [`HostType`] is considered an opaque type that can
    /// only be created from a string (see [`std::str::FromStr`]) or from
    /// [`std::net::Ipv4Addr`], [`std::net::Ipv6Addr`], or [`std::net::IpAddr`].
    ///
    /// ```rust
    /// # use gel_dsn::{HostType, gel::Builder};
    /// # use std::net::*;
    /// # use std::str::FromStr;
    /// # let builder = Builder::new();
    /// builder.host(HostType::from_str("localhost").unwrap());
    /// # let builder = Builder::new();
    /// builder.host(Ipv4Addr::new(127, 0, 0, 1));
    /// ```
    host: HostType,
    /// The port.
    port: u16,
    /// The unix path.
    unix_path: PathBuf,
    /// The database
    database: String,
    /// The branch.
    branch: String,
    /// The user.
    user: String,
    /// The password.
    password: String,
    /// The client security.
    client_security: ClientSecurity,
    /// The TLS CA.
    tls_ca: String,
    /// The TLS security.
    tls_security: TlsSecurity,
    /// The TLS server name.
    tls_server_name: String,
    /// The secret key.
    secret_key: String,
    /// The cloud profile.
    cloud_profile: String,
    /// How long to wait for the server to be available.
    wait_until_available: Duration,
    /// The cloud certificates to use.
    cloud_certs: CloudCerts,
    /// The TCP keepalive.
    tcp_keepalive: TcpKeepalive,
    /// The maximum number of concurrent connections.
    max_concurrency: usize,
    /// The connection timeout.
    connect_timeout: Duration,
);

impl Builder {
    /// Create a new builder with default parameters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new builder from something that can be converted into a [`Params`] object.
    pub fn new_from(
        params: impl TryInto<Params, Error = ParseError>,
    ) -> Result<Self, gel_errors::Error> {
        let params = params.try_into().map_err(|e| e.gel_error())?;
        Ok(Self { params })
    }

    /// Merge the given parameters into this builder.
    pub fn params(mut self, params: impl Into<Params>) -> Self {
        self.params.merge(params.into());
        self
    }

    /// Merge the given parameters into this builder.
    pub fn try_params<E>(mut self, params: impl TryInto<Params, Error = E>) -> Result<Self, E> {
        self.params.merge(params.try_into()?);
        Ok(self)
    }

    /// Set the allowed certificate as a PEM file.
    #[deprecated(note = "Use `tls_ca` instead")]
    pub fn pem_certificates(&mut self, cert_data: &str) -> &mut Self {
        self.params.tls_ca = Param::Unparsed(cert_data.to_string());
        self
    }

    /// Build the [`Config`] from the parameters and the local system
    /// environment, including environment variables and credentials assumed
    /// from the current working directory.
    ///
    /// NOTE: This method is not actually `async` but maintains the `async`
    /// signature for compatibility.
    #[deprecated(note = "Use `build` instead")]
    pub async fn build_env(self) -> Result<Config, gel_errors::Error> {
        self.with_system().build()
    }

    /// Prepare the builder for building the config without any system access
    /// configured. `with_env` and `with_fs` can be used to add system access.
    #[allow(deprecated, private_interfaces)]
    pub fn without_system(
        self,
    ) -> BuilderPrepare<WithoutEnv, WithoutFs, WithoutUser, WithoutProject> {
        BuilderPrepare {
            params: self.params,
            project_dir: None,
            env: Default::default(),
            fs: Default::default(),
            user: Default::default(),
            logging: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }

    #[doc(hidden)]
    #[allow(deprecated, private_interfaces)]
    pub fn with_system_impl<S: EnvVar + FileAccess + UserProfile + Clone>(
        self,
        system: S,
    ) -> BuilderPrepare<WithEnv<S>, WithFs<S>, WithUser<S>, WithoutProject> {
        self.without_system()
            .set_env(system.clone())
            .set_fs(system.clone())
            .set_user(system)
    }

    /// Prepare the builder for building the config with system access
    /// configured. This is equivalent to calling
    /// `without_system().with_env().with_fs().with_auto_project_cwd()`.
    #[allow(deprecated, private_interfaces)]
    pub fn with_system(
        self,
    ) -> BuilderPrepare<
        WithEnv<SystemEnvVars>,
        WithFs<SystemFileAccess>,
        WithUser<SystemUserProfile>,
        WithProject,
    > {
        self.without_system()
            .with_env()
            .with_fs()
            .with_auto_project_cwd()
    }

    #[allow(deprecated, private_interfaces)]
    pub fn with_env(
        self,
    ) -> BuilderPrepare<WithEnv<SystemEnvVars>, WithoutFs, WithoutUser, WithoutProject> {
        self.without_system().with_env()
    }

    #[allow(deprecated, private_interfaces)]
    pub fn with_fs(
        self,
    ) -> BuilderPrepare<
        WithoutEnv,
        WithFs<SystemFileAccess>,
        WithUser<SystemUserProfile>,
        WithoutProject,
    > {
        self.without_system().with_fs()
    }

    /// Build the [`Config`] from the parameters and the local system
    /// environment, including environment variables and credentials assumed
    /// from the current working directory.
    pub fn build(self) -> Result<Config, gel_errors::Error> {
        self.with_system().build()
    }
}

#[doc(hidden)]
pub struct WithEnv<E: EnvVar>(std::marker::PhantomData<E>);
#[doc(hidden)]
pub struct WithoutEnv;
#[doc(hidden)]
pub struct WithFs<F: FileAccess>(std::marker::PhantomData<F>);
#[doc(hidden)]
pub struct WithoutFs;
#[doc(hidden)]
pub struct WithUser<U: UserProfile>(std::marker::PhantomData<U>);
#[doc(hidden)]
pub struct WithoutUser;
#[doc(hidden)]
pub struct WithProject;
#[doc(hidden)]
pub struct WithoutProject;

trait BuilderEnv: Sized {
    type Env: EnvVar;
}
trait BuilderFs: Sized {
    type File: FileAccess;
}
trait BuilderUser: Sized {
    type UserProfile: UserProfile;
}
trait BuilderProject: Sized {}

impl<E: EnvVar> BuilderEnv for WithEnv<E> {
    type Env = E;
}
impl BuilderEnv for WithoutEnv {
    type Env = ();
}
impl<F: FileAccess> BuilderFs for WithFs<F> {
    type File = F;
}
impl BuilderFs for WithoutFs {
    type File = ();
}
impl<U: UserProfile> BuilderUser for WithUser<U> {
    type UserProfile = U;
}
impl BuilderUser for WithoutUser {
    type UserProfile = ();
}
impl BuilderProject for WithProject {}
impl BuilderProject for WithoutProject {}

/// An internal type used to prepare a [`Builder`] for building a [`Config`].
///
/// This type is considered an implementation detail and should not be named
/// directly.
#[allow(deprecated, private_bounds)]
pub struct BuilderPrepare<E: BuilderEnv, F: BuilderFs, U: BuilderUser, P: BuilderProject> {
    params: Params,
    project_dir: Option<ProjectDir>,
    env: E::Env,
    fs: F::File,
    user: U::UserProfile,
    logging: Logging,
    _phantom: std::marker::PhantomData<(E, F, P)>,
}

#[allow(deprecated, private_bounds)]
impl<E: BuilderEnv, F: BuilderFs, U: BuilderUser, P: BuilderProject> BuilderPrepare<E, F, U, P> {
    #[doc(hidden)]
    #[allow(deprecated, private_interfaces)]
    fn set_env<NEW: EnvVar>(self, env: NEW) -> BuilderPrepare<WithEnv<NEW>, F, U, P> {
        BuilderPrepare {
            params: self.params,
            project_dir: self.project_dir,
            env,
            fs: self.fs,
            user: self.user,
            logging: self.logging,
            _phantom: std::marker::PhantomData,
        }
    }

    #[doc(hidden)]
    #[allow(deprecated, private_interfaces)]
    fn set_fs<NEW: FileAccess>(self, fs: NEW) -> BuilderPrepare<E, WithFs<NEW>, U, P> {
        BuilderPrepare {
            params: self.params,
            project_dir: self.project_dir,
            env: self.env,
            fs,
            user: self.user,
            logging: self.logging,
            _phantom: std::marker::PhantomData,
        }
    }

    #[doc(hidden)]
    #[allow(deprecated, private_interfaces)]
    fn set_user<NEW: UserProfile>(self, user: NEW) -> BuilderPrepare<E, F, WithUser<NEW>, P> {
        BuilderPrepare {
            params: self.params,
            project_dir: self.project_dir,
            env: self.env,
            fs: self.fs,
            user,
            logging: self.logging,
            _phantom: std::marker::PhantomData,
        }
    }

    #[doc(hidden)]
    #[allow(deprecated, private_interfaces)]
    fn set_project_dir(
        self,
        project_dir: Option<ProjectDir>,
    ) -> BuilderPrepare<E, F, U, WithProject> {
        BuilderPrepare {
            params: self.params,
            project_dir,
            env: self.env,
            fs: self.fs,
            user: self.user,
            logging: self.logging,
            _phantom: std::marker::PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn with_env_impl<NEW: EnvVar>(self, env: NEW) -> BuilderPrepare<WithEnv<NEW>, F, U, P> {
        self.set_env(env)
    }

    #[doc(hidden)]
    pub fn with_fs_impl<NEW: FileAccess>(self, fs: NEW) -> BuilderPrepare<E, WithFs<NEW>, U, P> {
        self.set_fs(fs)
    }

    #[doc(hidden)]
    pub fn with_user_impl<NEW: UserProfile>(
        self,
        user: NEW,
    ) -> BuilderPrepare<E, F, WithUser<NEW>, P> {
        self.set_user(user)
    }

    /// Enable tracing of the build process.
    pub fn with_tracing(mut self, f: impl Fn(&str) + 'static) -> BuilderPrepare<E, F, U, P> {
        self.logging.tracing = Some(Box::new(f));
        self
    }

    pub fn with_warning(mut self, f: impl Fn(Warning) + 'static) -> BuilderPrepare<E, F, U, P> {
        self.logging.warning = Some(Box::new(f));
        self
    }

    /// Enable logging for build warnings and traces.
    #[cfg(feature = "log")]
    pub fn with_logging(mut self) -> BuilderPrepare<E, F, U, P> {
        self.logging.log_warning = true;
        self.logging.log_trace = true;
        self
    }
}

#[allow(deprecated, private_bounds)]
impl<E: BuilderEnv, F: FileAccess, U: UserProfile, P: BuilderProject>
    BuilderPrepare<E, WithFs<F>, WithUser<U>, P>
{
    /// Configure the project directory based on the current working directory.
    /// If sufficient information is found is found in the initial
    /// configuration, the project will not be loaded.
    #[allow(deprecated)]
    pub fn with_auto_project_cwd(self) -> BuilderPrepare<E, WithFs<F>, WithUser<U>, WithProject> {
        self.set_project_dir(Some(ProjectDir::SearchCwd))
    }

    /// Configure the project directory to be the given directory. If no project
    /// is found, it is silently ignored. If sufficient information is found is found
    /// in the initial configuration, the project will not be loaded.
    #[allow(deprecated)]
    pub fn with_auto_project(
        self,
        project_dir: impl AsRef<Path>,
    ) -> BuilderPrepare<E, WithFs<F>, WithUser<U>, WithProject> {
        self.set_project_dir(Some(ProjectDir::Search(project_dir.as_ref().to_path_buf())))
    }

    /// Configure the project directory to be the given directory. Does not
    /// perform a search for project files. If sufficient information is found
    /// is found in the initial configuration, the project will not be loaded.
    #[allow(deprecated)]
    pub fn with_explicit_project(
        self,
        project_dir: impl AsRef<Path>,
    ) -> BuilderPrepare<E, WithFs<F>, WithUser<U>, WithProject> {
        self.set_project_dir(Some(ProjectDir::NoSearch(
            project_dir.as_ref().to_path_buf(),
        )))
    }
}

#[allow(deprecated, private_bounds)]
impl<F: BuilderFs, U: BuilderUser, P: BuilderProject> BuilderPrepare<WithoutEnv, F, U, P> {
    /// Configure the builder to use the environment for configuration.
    #[allow(deprecated)]
    pub fn with_env(self) -> BuilderPrepare<WithEnv<SystemEnvVars>, F, U, P> {
        self.set_env(SystemEnvVars)
    }
}

#[allow(deprecated, private_bounds)]
impl<E: BuilderEnv, P: BuilderProject> BuilderPrepare<E, WithoutFs, WithoutUser, P> {
    /// Configure the builder to use the file system for configuration.
    #[allow(deprecated)]
    pub fn with_fs(
        self,
    ) -> BuilderPrepare<E, WithFs<SystemFileAccess>, WithUser<SystemUserProfile>, P> {
        self.set_fs(SystemFileAccess).set_user(SystemUserProfile)
    }
}

#[allow(deprecated, private_bounds)]
impl<E: BuilderEnv, F: BuilderFs, U: BuilderUser, P: BuilderProject> BuilderPrepare<E, F, U, P> {
    /// Build the [`Config`] from the parameters, with optional environment,
    /// file system access, and project directory potentially configured.
    pub fn build(self) -> Result<Config, gel_errors::Error> {
        self.build_parse_error().map_err(|e| e.gel_error())
    }

    #[doc(hidden)]
    pub fn build_parse_error(self) -> Result<Config, ParseError> {
        let params = self.params;

        let mut context = BuildContextImpl::new_with_user_profile(self.env, self.fs, self.user);
        context.logging = self.logging;
        parse(params, &mut context, self.project_dir)
    }
}

impl Params {
    fn check_overlap(&self) -> Vec<CompoundSource> {
        let mut sources = Vec::new();
        if self.dsn.is_some() {
            sources.push(CompoundSource::Dsn);
        }
        if self.instance.is_some() {
            sources.push(CompoundSource::Instance);
        }
        if self.unix_path.is_some() {
            sources.push(CompoundSource::UnixSocket);
            if self.host.is_some() {
                sources.push(CompoundSource::HostPort);
            }
        } else if self.host.is_some() || self.port.is_some() {
            sources.push(CompoundSource::HostPort);
        }
        if self.credentials.is_some() {
            sources.push(CompoundSource::CredentialsFile);
        }
        sources
    }

    /// Try to build the config. Returns `None` if the config is incomplete.
    pub(crate) fn try_build(
        &self,
        context: &mut impl BuildContext,
        phase: BuildPhase,
    ) -> Result<Option<Config>, ParseError> {
        // Step 0: Check for compound option overlap. If there is, return an error.
        let compound_sources = self.check_overlap();
        if compound_sources.len() > 1 {
            if phase == BuildPhase::Options {
                return Err(ParseError::MultipleCompoundOpts(compound_sources));
            } else {
                return Err(ParseError::MultipleCompoundEnv(compound_sources));
            }
        }

        // Step 1: Resolve DSN, credentials file, instance if available
        let mut explicit = self.clone();

        context_trace!(context, "Start: {:?}", explicit);

        if let Some(dsn) = self.dsn.get(context)? {
            let res = parse_dsn(&dsn, context);
            if let Err(e) = &res {
                context_trace!(context, "DSN error: {:?}", e);
            }
            let dsn = res?;
            context_trace!(context, "DSN: {:?}", dsn);
            explicit.merge(dsn);
        }
        if let Some(file) = self.credentials.get(context).map_err(|e| {
            // Special case: map FileNotFound to InvalidCredentialsFile
            if e == ParseError::FileNotFound {
                ParseError::InvalidCredentialsFile(InvalidCredentialsFileError::FileNotFound)
            } else {
                e
            }
        })? {
            let file = parse_credentials(file, context)?;
            context_trace!(context, "Credentials: {:?}", file);
            explicit.merge(file);
        }
        if let Some(instance) = self.instance.get(context)? {
            match instance {
                InstanceName::Local(local) => {
                    let instance = parse_instance(&local, context)?;
                    context_trace!(context, "Instance: {:?}", instance);
                    explicit.merge(instance);
                }
                InstanceName::Cloud { .. } => {
                    let profile = explicit
                        .cloud_profile
                        .get(context)?
                        .unwrap_or("default".to_string());
                    let cloud = parse_cloud(&profile, context)?;
                    context_trace!(context, "Cloud: {:?}", cloud);
                    explicit.merge(cloud);

                    if let Some(secret_key) = explicit.secret_key.get(context)? {
                        match instance.cloud_address(&secret_key) {
                            Ok(Some(address)) => {
                                explicit.host = Param::Unparsed(address);
                            }
                            Ok(None) => {
                                unreachable!();
                            }
                            Err(e) => {
                                // Special case: we ignore the secret key error until the final phase
                                if phase == BuildPhase::Project {
                                    return Err(e);
                                }
                            }
                        }
                    } else {
                        return Err(ParseError::SecretKeyNotFound);
                    }
                }
            }
        }

        context_trace!(context, "Merged: {:?}", explicit);

        // Step 2: Resolve host. If we have no host yet, exit.
        let port = explicit.port.get(context)?;
        if port == Some(0) {
            return Err(ParseError::InvalidPort);
        }

        let host = if let Some(unix_path) = explicit.unix_path.get(context)? {
            match port {
                Some(port) => Host::new(HostType::from_unix_path(unix_path), port, HostTarget::Gel),
                None => Host::new(
                    HostType::from_unix_path(unix_path),
                    DEFAULT_PORT,
                    HostTarget::Raw,
                ),
            }
        } else {
            let host = match (explicit.host.get(context)?, port) {
                (Some(host), Some(port)) => Host::new(host, port, HostTarget::Gel),
                (Some(host), None) => Host::new(host, DEFAULT_PORT, HostTarget::Gel),
                (None, Some(port)) => Host::new(
                    HostType::try_from_str("localhost").unwrap(),
                    port,
                    HostTarget::Gel,
                ),
                (None, None) => {
                    return Ok(None);
                }
            };

            // Only allow the unix socket if it's explicitly set through unix_path
            if host.is_unix() {
                return Err(ParseError::UnixSocketUnsupported);
            }

            host
        };

        let authentication = if let Some(password) = explicit.password.get(context)? {
            Authentication::Password(password)
        } else if let Some(secret_key) = explicit.secret_key.get(context)? {
            Authentication::SecretKey(secret_key)
        } else {
            Authentication::None
        };

        let user = explicit.user.get(context)?;
        let database = explicit.database.get(context)?;
        let branch = explicit.branch.get(context)?;

        for (param, error) in [
            (&user, ParseError::InvalidUser),
            (&database, ParseError::InvalidDatabase),
            (&branch, ParseError::InvalidDatabase),
        ] {
            if let Some(param) = param {
                if param.trim() != param || param.is_empty() {
                    return Err(error);
                }
            }
        }

        let db = match (database, branch) {
            (Some(db), Some(branch)) if db != branch => {
                return Err(ParseError::InvalidDatabase);
            }
            (Some(_), Some(branch)) => DatabaseBranch::Ambiguous(branch),
            (Some(db), None) => DatabaseBranch::Database(db),
            (None, Some(branch)) => DatabaseBranch::Branch(branch),
            (None, None) => DatabaseBranch::Default,
        };

        let tls_ca = if let Some(tls_ca) = explicit.tls_ca.get(context)? {
            let mut cursor = std::io::Cursor::new(tls_ca);
            let mut certs = Vec::new();
            for cert in rustls_pemfile::read_all(&mut cursor) {
                match cert.map_err(|_| ParseError::InvalidCertificate)? {
                    rustls_pemfile::Item::X509Certificate(data) => {
                        certs.push(data);
                    }
                    _ => return Err(ParseError::InvalidCertificate),
                }
            }
            Some(certs)
        } else {
            None
        };

        let client_security = explicit.client_security.get(context)?.unwrap_or_default();
        let tls_security = explicit.tls_security.get(context)?.unwrap_or_default();
        let tls_server_name = explicit.tls_server_name.get(context)?;
        let wait_until_available = explicit.wait_until_available.get(context)?;
        let cloud_certs = explicit.cloud_certs.get(context)?;
        let tcp_keepalive = explicit.tcp_keepalive.get(context)?;
        let max_concurrency = explicit.max_concurrency.get(context)?;
        let connect_timeout = explicit.connect_timeout.get(context)?;

        let server_settings = explicit.server_settings;

        // If we have a client security option, we need to check if it's compatible with the TLS security option.
        let tls_security = match (client_security, tls_security) {
            (ClientSecurity::Strict, TlsSecurity::Insecure | TlsSecurity::NoHostVerification) => {
                return Err(ParseError::InvalidTlsSecurity(
                    TlsSecurityError::IncompatibleSecurityOptions,
                ));
            }
            (ClientSecurity::Strict, _) => TlsSecurity::Strict,
            (ClientSecurity::InsecureDevMode, TlsSecurity::Default) => TlsSecurity::Insecure,
            (ClientSecurity::Default, TlsSecurity::Insecure) => TlsSecurity::Insecure,
            (_, TlsSecurity::Default) if tls_ca.is_none() => TlsSecurity::Strict,
            (_, TlsSecurity::Default) => TlsSecurity::NoHostVerification,
            (_, TlsSecurity::NoHostVerification) => TlsSecurity::NoHostVerification,
            (_, TlsSecurity::Strict) => TlsSecurity::Strict,
            (_, TlsSecurity::Insecure) => TlsSecurity::Insecure,
        };

        let user = user.unwrap_or_else(|| "edgedb".to_string());

        {
            let value = Some(Config {
                host,
                db,
                user,
                authentication,
                client_security,
                tls_security,
                tls_ca,
                tls_server_name,
                wait_until_available: wait_until_available.unwrap_or(DEFAULT_WAIT),
                server_settings,
                connect_timeout: connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT),
                max_concurrency,
                tcp_keepalive: tcp_keepalive.unwrap_or(TcpKeepalive::Default),
                cloud_certs,
            });
            Ok(value)
        }
    }
}

fn parse_dsn(dsn: &str, context: &mut impl BuildContext) -> Result<Params, ParseError> {
    let mut explicit = Params::default();

    context_trace!(context, "Parsing DSN: {:?}", dsn);

    let dsn = <Url as FromParamStr>::from_param_str(dsn, context)
        .map_err(|_| ParseError::InvalidDsn(InvalidDsnError::ParseError))?;

    if !(dsn.scheme() == "edgedb" || dsn.scheme() == "gel") {
        return Err(ParseError::InvalidDsn(InvalidDsnError::InvalidScheme));
    }

    let mut set = HashSet::new();
    if let Some(host) = dsn.host() {
        set.insert("host".to_string());
        match host {
            url::Host::Domain(domain) => {
                explicit.host = Param::Unparsed(domain.to_string());
            }
            url::Host::Ipv4(address) => {
                explicit.host = Param::Parsed(address.into());
            }
            url::Host::Ipv6(address) => {
                explicit.host = Param::Parsed(address.into());
            }
        }
    } else {
        explicit.host = Param::Unparsed("localhost".to_string());
    }
    if let Some(port) = dsn.port() {
        if let Some(port) = NonZeroU16::new(port) {
            set.insert("port".to_string());
            explicit.port = Param::Parsed(port.into());
        } else {
            return Err(ParseError::InvalidPort);
        }
    } else {
        explicit.port = Param::Parsed(DEFAULT_PORT);
    }

    let path = dsn.path().strip_prefix('/').unwrap_or(dsn.path());
    if !path.is_empty() {
        set.insert("branch".to_string());
        explicit.branch = Param::Unparsed(path.to_string());
        set.insert("database".to_string());
        explicit.database = Param::Unparsed(path.to_string());
    }

    if !dsn.username().is_empty() {
        set.insert("user".to_string());
        explicit.user = Param::Unparsed(dsn.username().to_string());
    }

    if let Some(password) = dsn.password() {
        if !password.is_empty() {
            set.insert("password".to_string());
            explicit.password = Param::Unparsed(password.to_string());
        }
    }

    explicit.server_settings = HashMap::new();

    for (key, value) in dsn.query_pairs() {
        // Weird case: database and branch are stripped of the leading '/'
        let value = if key == "database" || key == "branch" {
            value.strip_prefix('/').unwrap_or(value.as_ref()).into()
        } else {
            value
        };

        let (key, param) = if let Some(key) = key.strip_suffix("_file_env") {
            (key, Param::EnvFile(value.to_string()))
        } else if let Some(key) = key.strip_suffix("_env") {
            (key, Param::<String>::Env(value.to_string()))
        } else if let Some(key) = key.strip_suffix("_file") {
            (key, Param::File(PathBuf::from(value.to_string())))
        } else {
            (key.as_ref(), Param::Unparsed(value.to_string()))
        };
        if !set.insert(key.to_string()) {
            return Err(ParseError::InvalidDsn(InvalidDsnError::DuplicateOptions(
                key.to_string(),
            )));
        }
        match key {
            "host" => explicit.host = param.cast().unwrap(),
            "user" => explicit.user = param,
            "password" => explicit.password = param,
            "secret_key" => explicit.secret_key = param,
            "tls_ca" => explicit.tls_ca = param,
            "tls_server_name" => explicit.tls_server_name = param,
            "database" => explicit.database = param,
            "branch" => explicit.branch = param,
            "port" => explicit.port = param.cast().unwrap(),
            "tls_security" => explicit.tls_security = param.cast().unwrap(),
            "cloud_profile" => explicit.cloud_profile = param,
            "wait_until_available" => explicit.wait_until_available = param.cast().unwrap(),
            key => {
                if explicit
                    .server_settings
                    .insert(key.to_string(), value.to_string())
                    .is_some()
                {
                    return Err(ParseError::InvalidDsn(InvalidDsnError::DuplicateOptions(
                        key.to_string(),
                    )));
                }
            }
        }
    }

    if explicit.database.is_some() && explicit.branch.is_some() && path.is_empty() {
        return Err(ParseError::InvalidDsn(InvalidDsnError::BranchAndDatabase));
    }

    Ok(explicit)
}

fn parse_credentials(
    credentials: CredentialsFile,
    context: &mut impl BuildContext,
) -> Result<Params, ParseError> {
    let explicit = Params {
        host: Param::from_unparsed(credentials.host.clone()),
        port: Param::from_parsed(credentials.port.map(|p| p.into())),
        user: Param::Unparsed(credentials.user.clone()),
        password: Param::from_unparsed(credentials.password.clone()),
        database: Param::from_unparsed(credentials.database.clone()),
        branch: Param::from_unparsed(credentials.branch.clone()),
        tls_ca: Param::from_unparsed(credentials.tls_ca.clone()),
        tls_security: Param::Unparsed(credentials.tls_security.to_string()),
        tls_server_name: Param::from_unparsed(credentials.tls_server_name.clone()),
        ..Default::default()
    };

    for warning in credentials.warnings() {
        context.warn(warning.clone());
    }

    Ok(explicit)
}

fn parse_env(context: &mut impl BuildContext) -> Result<Params, ParseError> {
    let mut explicit = Params {
        dsn: Param::from_parsed(context.read_env(Env::dsn)?),
        instance: Param::from_parsed(context.read_env(Env::instance)?),
        credentials: Param::from_file(context.read_env(Env::credentials_file)?),
        host: Param::from_parsed(context.read_env(Env::host)?),
        port: Param::from_parsed(context.read_env(Env::port)?.map(|p| p.into())),
        database: Param::from_parsed(context.read_env(Env::database)?),
        branch: Param::from_parsed(context.read_env(Env::branch)?),
        user: Param::from_parsed(context.read_env(Env::user)?),
        password: Param::from_parsed(context.read_env(Env::password)?),
        tls_security: Param::from_parsed(context.read_env(Env::client_tls_security)?),
        tls_ca: Param::from_parsed(context.read_env(Env::tls_ca)?),
        tls_server_name: Param::from_parsed(context.read_env(Env::tls_server_name)?),
        client_security: Param::from_parsed(context.read_env(Env::client_security)?),
        secret_key: Param::from_parsed(context.read_env(Env::secret_key)?),
        cloud_profile: Param::from_parsed(context.read_env(Env::cloud_profile)?),
        wait_until_available: Param::from_parsed(context.read_env(Env::wait_until_available)?),
        cloud_certs: Param::from_parsed(context.read_env(Env::_cloud_certs)?),
        ..Default::default()
    };

    if explicit.branch.is_some() && explicit.database.is_some() {
        return Err(ParseError::ExclusiveOptions);
    }

    let ca_file = Param::from_file(context.read_env(Env::tls_ca_file)?);
    if explicit.tls_ca.is_none() {
        explicit.tls_ca = ca_file;
    } else if ca_file.is_some() {
        return Err(ParseError::ExclusiveOptions);
    }

    Ok(explicit)
}

/// Parse the early environment variables, ensuring that we always read the
/// client security and cloud certs.
fn parse_env_early(context: &mut impl BuildContext) -> Result<Params, ParseError> {
    let explicit = Params {
        client_security: Param::from_parsed(context.read_env(Env::client_security)?),
        cloud_certs: Param::from_parsed(context.read_env(Env::_cloud_certs)?),
        ..Default::default()
    };

    Ok(explicit)
}

fn parse_instance(local: &str, context: &mut impl BuildContext) -> Result<Params, ParseError> {
    let Some(credentials) = (match context.read_config_file(format!("credentials/{local}.json"))? {
        Some(credentials) => Some(credentials),
        None => {
            return Err(ParseError::CredentialsFileNotFound);
        }
    }) else {
        return {
            let value = Params::default();
            Ok(value)
        };
    };
    parse_credentials(credentials, context)
}

fn parse_cloud(profile: &str, context: &mut impl BuildContext) -> Result<Params, ParseError> {
    let mut explicit = Params::default();

    let Some(cloud_credentials): Option<CloudCredentialsFile> =
        context.read_config_file(format!("cloud-credentials/{profile}.json"))?
    else {
        return {
            let value = Params::default();
            Ok(value)
        };
    };
    explicit.secret_key = Param::Unparsed(cloud_credentials.secret_key);

    Ok(explicit)
}

/// An opaque type representing a credentials file.
///
/// Use [`std::str::FromStr`] to parse a credentials file from a string.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialsFile {
    pub(crate) user: String,

    pub(crate) host: Option<String>,
    pub(crate) port: Option<NonZeroU16>,
    pub(crate) password: Option<String>,
    pub(crate) database: Option<String>,
    pub(crate) branch: Option<String>,
    pub(crate) tls_ca: Option<String>,
    #[serde(default)]
    pub(crate) tls_security: TlsSecurity,
    pub(crate) tls_server_name: Option<String>,

    #[serde(skip)]
    warnings: Vec<Warning>,
}

impl CredentialsFile {
    pub fn warnings(&self) -> &[Warning] {
        &self.warnings
    }
}

impl FromStr for CredentialsFile {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(res) = serde_json::from_str::<CredentialsFile>(s) {
            // Special case: don't allow database and branch to be set at the same time
            if let (Some(database), Some(branch)) = (&res.database, &res.branch) {
                if database != branch {
                    return Err(ParseError::InvalidCredentialsFile(
                        InvalidCredentialsFileError::ConflictingSettings(
                            ("database".to_string(), database.clone()),
                            ("branch".to_string(), branch.clone()),
                        ),
                    ));
                }
            }

            return Ok(res);
        }

        let res = serde_json::from_str::<CredentialsFileCompat>(s).map_err(|e| {
            ParseError::InvalidCredentialsFile(InvalidCredentialsFileError::SerializationError(
                e.to_string(),
            ))
        })?;

        res.try_into()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CredentialsFileCompat {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    port: Option<NonZeroU16>,
    user: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tls_cert_data: Option<String>, // deprecated
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tls_ca: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tls_server_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tls_verify_hostname: Option<bool>, // deprecated
    tls_security: Option<TlsSecurity>,
}

impl CredentialsFileCompat {
    fn validate(&self) -> Vec<Warning> {
        let mut warnings = Vec::new();
        if self.tls_verify_hostname.is_some() {
            warnings.push(Warning::DeprecatedCredentialProperty(
                "tls_verify_hostname".to_string(),
            ));
        }
        if self.tls_cert_data.is_some() {
            warnings.push(Warning::DeprecatedCredentialProperty(
                "tls_cert_data".to_string(),
            ));
        }
        warnings
    }
}

impl TryInto<CredentialsFile> for CredentialsFileCompat {
    type Error = ParseError;

    fn try_into(self) -> Result<CredentialsFile, Self::Error> {
        let expected_verify = match self.tls_security {
            Some(TlsSecurity::Strict) => Some(true),
            Some(TlsSecurity::NoHostVerification) => Some(false),
            Some(TlsSecurity::Insecure) => Some(false),
            _ => None,
        };
        if self.tls_verify_hostname.is_some()
            && self.tls_security.is_some()
            && expected_verify
                .zip(self.tls_verify_hostname)
                .map(|(actual, expected)| actual != expected)
                .unwrap_or(false)
        {
            Err(ParseError::InvalidCredentialsFile(
                InvalidCredentialsFileError::ConflictingSettings(
                    (
                        "tls_security".to_string(),
                        self.tls_security.unwrap().to_string(),
                    ),
                    (
                        "tls_verify_hostname".to_string(),
                        self.tls_verify_hostname.unwrap().to_string(),
                    ),
                ),
            ))
        } else if self.tls_ca.is_some()
            && self.tls_cert_data.is_some()
            && self.tls_ca != self.tls_cert_data
        {
            return Err(ParseError::InvalidCredentialsFile(
                InvalidCredentialsFileError::ConflictingSettings(
                    ("tls_ca".to_string(), self.tls_ca.unwrap().to_string()),
                    (
                        "tls_cert_data".to_string(),
                        self.tls_cert_data.unwrap().to_string(),
                    ),
                ),
            ));
        } else {
            // Special case: don't allow database and branch to be set at the same time
            if self.database.is_some() && self.branch.is_some() && self.database != self.branch {
                return Err(ParseError::InvalidCredentialsFile(
                    InvalidCredentialsFileError::ConflictingSettings(
                        ("database".to_string(), self.database.unwrap().to_string()),
                        ("branch".to_string(), self.branch.unwrap().to_string()),
                    ),
                ));
            }

            let warnings = self.validate();

            Ok(CredentialsFile {
                host: self.host,
                port: self.port,
                user: self.user,
                password: self.password,
                database: self.database,
                branch: self.branch,
                tls_ca: self.tls_ca.or(self.tls_cert_data.clone()),
                tls_server_name: self.tls_server_name,
                tls_security: self.tls_security.unwrap_or(match self.tls_verify_hostname {
                    None => TlsSecurity::Default,
                    Some(true) => TlsSecurity::Strict,
                    Some(false) => TlsSecurity::NoHostVerification,
                }),
                warnings,
            })
        }
    }
}

/// An opaque type representing a cloud credentials file.
///
/// Use [`std::str::FromStr`] to parse a cloud credentials file from a string.
#[derive(Debug, Clone, Deserialize)]
pub struct CloudCredentialsFile {
    pub(crate) secret_key: String,
}

impl FromStr for CloudCredentialsFile {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|e| {
            ParseError::InvalidCredentialsFile(InvalidCredentialsFileError::SerializationError(
                e.to_string(),
            ))
        })
    }
}

/// Parse the connection from the given sources given the following precedence:
///
/// 1. Explicit options
/// 2. Environment variables (GEL_DSN / GEL_INSTANCE / GEL_CREDENTIALS_FILE / GEL_HOST+GEL_PORT)
///
/// If no explicit options or environment variables were provided, the project-linked credentials will be used.
///
pub(crate) fn parse(
    mut explicit: Params,
    context: &mut impl BuildContext,
    project: Option<ProjectDir>,
) -> Result<Config, ParseError> {
    // We always want to read the early environment variables.
    let env_params = parse_env_early(context)?;
    explicit.merge(env_params);

    if let Some(config) = explicit.try_build(context, BuildPhase::Options)? {
        return Ok(config);
    }

    let env_params = parse_env(context)?;
    explicit.merge(env_params);

    if let Some(config) = explicit.try_build(context, BuildPhase::Environment)? {
        return Ok(config);
    }

    if let Some(project) = project {
        if let Ok(Some(project)) = find_project_file(context, project) {
            if let Some(project) = project.project {
                explicit.merge(Params {
                    cloud_profile: Param::from_unparsed(project.cloud_profile),
                    instance: Param::from_parsed(Some(project.instance_name)),
                    database: Param::from_unparsed(project.database),
                    branch: Param::from_unparsed(project.branch),
                    ..Default::default()
                });
            } else {
                return Err(ParseError::ProjectNotInitialised);
            }
        }
    }

    if let Some(config) = explicit.try_build(context, BuildPhase::Project)? {
        return Ok(config);
    }

    Err(ParseError::NoOptionsOrToml)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let _params = Builder::default().without_system();
        let _params = Builder::default()
            .with_fs()
            .with_explicit_project(Path::new("."));
        // This intentionally won't work
        // let params = Builder::default().with_env().project_dir(Path::new("."));
    }
}
