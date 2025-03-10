//! Parses DSNs for Gel database connections.

mod config;
mod duration;
mod env;
pub mod error;
mod instance_name;
mod param;
mod params;
mod project;

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    env::SystemEnvVars, file::SystemFileAccess, user::SystemUserProfile, EnvVar, FileAccess,
    UserProfile,
};
pub use config::*;
use error::Warning;
pub use instance_name::*;
pub use param::*;
pub use params::*;

#[cfg(feature = "unstable")]
pub use env::define_env;

#[cfg(feature = "unstable")]
pub use project::{Project, ProjectDir, ProjectSearchResult};

/// Internal helper to parse a duration string into a `std::time::Duration`.
#[doc(hidden)]
pub fn parse_duration(s: &str) -> Result<std::time::Duration, Box<dyn std::error::Error>> {
    use std::str::FromStr;
    Ok(std::time::Duration::from_micros(
        duration::Duration::from_str(s)?.micros as u64,
    ))
}

/// Internal helper to format a `std::time::Duration` into a duration string.
#[doc(hidden)]
pub fn format_duration(d: &std::time::Duration) -> String {
    duration::Duration::from_micros(d.as_micros() as i64).to_string()
}

fn config_dirs<U: UserProfile>(user: &U) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if cfg!(unix) {
        if let Some(dir) = user.config_dir() {
            dirs.push(dir.join("gel"));
            dirs.push(dir.join("edgedb"));
        }
    }
    if cfg!(windows) {
        if let Some(dir) = user.data_local_dir() {
            dirs.push(dir.join("Gel").join("config"));
            dirs.push(dir.join("EdgeDB").join("config"));
        }
    }
    dirs
}

type LoggingFn = Box<dyn Fn(&str) + 'static>;
type WarningFn = Box<dyn Fn(Warning) + 'static>;

#[derive(Default)]
struct Logging {
    tracing: Option<LoggingFn>,
    warning: Option<WarningFn>,
    #[cfg(feature = "log")]
    log_trace: bool,
    #[cfg(feature = "log")]
    log_warning: bool,
}

impl Logging {
    fn trace(&self, message: impl Fn(&dyn Fn(&str))) {
        let mut needs_trace = false;

        #[cfg(feature = "log")]
        {
            if self.log_trace || cfg!(feature = "auto-log-trace") {
                needs_trace = true;
            }
        }

        if self.tracing.is_some() {
            needs_trace = true;
        }

        if needs_trace {
            message(&|message| {
                #[cfg(feature = "log")]
                {
                    if self.log_trace || cfg!(feature = "auto-log-trace") {
                        log::trace!("{}", message);
                    }
                }
                {
                    if let Some(tracing) = &self.tracing {
                        tracing(message);
                    }
                }
            });
        }
    }

    fn warn(&self, warning: Warning) {
        #[cfg(feature = "log")]
        {
            if self.log_warning || cfg!(feature = "auto-log-warning") {
                log::warn!("{}", warning);
            }
        }
        if let Some(warning_fn) = &self.warning {
            warning_fn(warning);
        }
    }
}

#[derive(Default, Clone)]
pub struct Warnings {
    warnings: Arc<Mutex<Vec<Warning>>>,
}

impl Warnings {
    pub fn into_vec(self) -> Vec<Warning> {
        match Arc::try_unwrap(self.warnings) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(arc) => arc.lock().unwrap().clone(),
        }
    }

    pub fn warn(&self, warning: Warning) {
        let mut warnings = self.warnings.lock().unwrap();
        warnings.push(warning);
    }

    pub fn warn_fn(self) -> WarningFn {
        Box::new(move |warning| self.warn(warning))
    }
}

#[derive(Default, Clone)]
pub struct Traces {
    traces: Arc<Mutex<Vec<String>>>,
}

impl Traces {
    pub fn into_vec(self) -> Vec<String> {
        match Arc::try_unwrap(self.traces) {
            Ok(mutex) => mutex.into_inner().unwrap(),
            Err(arc) => arc.lock().unwrap().clone(),
        }
    }

    pub fn trace(&self, message: &str) {
        let mut traces = self.traces.lock().unwrap();
        traces.push(message.to_string());
    }

    pub fn trace_fn(self) -> LoggingFn {
        Box::new(move |message| self.trace(message))
    }
}
struct BuildContextImpl<E: EnvVar = SystemEnvVars, F: FileAccess = SystemFileAccess> {
    env: E,
    files: F,
    pub config_dir: Option<Vec<PathBuf>>,
    pub(crate) logging: Logging,
}

impl Default for BuildContextImpl<SystemEnvVars, SystemFileAccess> {
    fn default() -> Self {
        Self::new()
    }
}

impl BuildContextImpl<SystemEnvVars, SystemFileAccess> {
    /// Create a new build context with default values.
    pub fn new() -> Self {
        Self {
            env: SystemEnvVars,
            files: SystemFileAccess,
            config_dir: Some(config_dirs(&SystemUserProfile)),
            logging: Logging::default(),
        }
    }
}

impl<E: EnvVar, F: FileAccess> BuildContextImpl<E, F> {
    /// Create a new build context with default values.
    pub fn new_with_user_profile<U: UserProfile>(env: E, files: F, user: U) -> Self {
        let config_dir = config_dirs(&user);
        Self {
            env,
            files,
            config_dir: Some(config_dir),
            logging: Logging::default(),
        }
    }

    #[cfg(test)]
    /// Create a new build context with default values.
    pub fn new_with(env: E, files: F) -> Self {
        Self {
            env,
            files,
            config_dir: None,
            logging: Logging::default(),
        }
    }
}

macro_rules! context_trace {
    ($context:expr, $message:expr $(, $arg:expr)*) => {
        $context.trace(|f: &dyn Fn(&str)| f(&format!($message, $($arg),*)));
    };
}

pub(crate) use context_trace;

pub(crate) trait BuildContext {
    type EnvVar: EnvVar;
    fn env(&self) -> &impl EnvVar;
    fn cwd(&self) -> Option<PathBuf>;
    fn files(&self) -> &impl FileAccess;
    fn warn(&mut self, warning: error::Warning);
    fn read_config_file<T: FromParamStr>(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<Option<T>, T::Err>;
    fn read_env<'a, 'b, 'c, T: FromParamStr>(
        &'c mut self,
        env: impl Fn(&'b mut Self) -> Result<Option<T>, error::ParseError>,
    ) -> Result<Option<T>, error::ParseError>
    where
        Self::EnvVar: 'a,
        'c: 'a,
        'c: 'b;
    fn trace(&self, message: impl Fn(&dyn Fn(&str)));
}

impl<E: EnvVar, F: FileAccess> BuildContext for BuildContextImpl<E, F> {
    type EnvVar = E;
    fn env(&self) -> &impl EnvVar {
        &self.env
    }

    fn cwd(&self) -> Option<PathBuf> {
        self.files.cwd()
    }

    fn files(&self) -> &impl FileAccess {
        &self.files
    }

    fn warn(&mut self, warning: error::Warning) {
        self.logging.warn(warning);
    }

    fn read_config_file<T: FromParamStr>(
        &mut self,
        path: impl AsRef<Path>,
    ) -> Result<Option<T>, T::Err> {
        for config_dir in self.config_dir.iter().flatten() {
            let path = config_dir.join(path.as_ref());
            context_trace!(self, "Reading config file: {}", path.display());
            if let Ok(file) = self.files.read(&path) {
                // TODO?
                let res = T::from_param_str(&file, self);
                context_trace!(
                    self,
                    "File content: {:?}",
                    res.as_ref().map(|_| ()).map_err(|_| ())
                );
                return match res {
                    Ok(value) => Ok(Some(value)),
                    Err(e) => Err(e),
                };
            }
        }

        Ok(None)
    }

    fn read_env<'a, 'b, 'c, T: FromParamStr>(
        &'c mut self,
        env: impl Fn(&'b mut Self) -> Result<Option<T>, error::ParseError>,
    ) -> Result<Option<T>, error::ParseError>
    where
        Self::EnvVar: 'a,
        'c: 'a,
        'c: 'b,
    {
        let res = env(self);
        match res {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn trace(&self, message: impl Fn(&dyn Fn(&str))) {
        self.logging.trace(message);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{host::Host, HostTarget, HostType};
    use std::{collections::HashMap, time::Duration};

    #[test]
    fn test_parse() {
        let cfg = Builder::default()
            .dsn("edgedb://hostname:1234")
            .without_system()
            .build();

        assert_eq!(
            cfg.unwrap(),
            Config {
                host: Host::new(
                    HostType::try_from_str("hostname").unwrap(),
                    1234,
                    HostTarget::Gel
                ),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_credentials_file() {
        let credentials = json!({
            "port": 10702,
            "user": "test3n",
            "password": "lZTBy1RVCfOpBAOwSCwIyBIR",
            "database": "test3n"
        });

        let credentials_file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(credentials_file.path(), credentials.to_string()).unwrap();

        let credentials = Builder::new()
            .credentials_file(credentials_file.path())
            .with_fs()
            .build()
            .expect("Failed to build credentials");

        assert_eq!(
            credentials.host,
            Host::new(DEFAULT_HOST.clone(), 10702, HostTarget::Gel)
        );
        assert_eq!(&credentials.user, "test3n");
        assert_eq!(
            credentials.db,
            DatabaseBranch::Database("test3n".to_string())
        );
        assert_eq!(
            credentials.authentication,
            Authentication::Password("lZTBy1RVCfOpBAOwSCwIyBIR".into())
        );
    }

    #[test]
    fn test_schemes() {
        let dsn_schemes = ["edgedb", "gel"];
        for dsn_scheme in dsn_schemes {
            let cfg = Builder::new()
                .dsn(format!("{dsn_scheme}://localhost:1756"))
                .build()
                .unwrap();

            let host = cfg.host.target_name().unwrap();
            assert_eq!(host.host(), Some("localhost".into()));
            assert_eq!(host.port(), Some(1756));
        }
    }

    #[test]
    fn test_unix_path() {
        // Test unix path without a port
        let cfg = Builder::new()
            .unix_path("/test/.s.EDGEDB.8888")
            .build()
            .unwrap();

        let host = cfg.host.target_name().unwrap();
        assert_eq!(host.path(), Some(Path::new("/test/.s.EDGEDB.8888")));

        // Test unix path with a port
        let cfg = Builder::new()
            .port(8888_u16)
            .unix_path("/test")
            .build()
            .unwrap();
        let host = cfg.host.target_name().unwrap();
        assert_eq!(host.path(), Some(Path::new("/test/.s.EDGEDB.8888")));
    }

    /// Test that the hidden CloudCerts env var is parsed correctly.
    #[test]
    fn test_cloud_certs() {
        let cloud_cert =
            HashMap::from_iter([("_GEL_CLOUD_CERTS".to_string(), "local".to_string())]);
        let cfg = Builder::new()
            .port(5656_u16)
            .without_system()
            .with_env_impl(cloud_cert)
            .build()
            .unwrap();
        assert_eq!(cfg.cloud_certs, Some(CloudCerts::Local));
    }

    #[test]
    fn test_tcp_keepalive() {
        let cfg = Builder::new()
            .port(5656_u16)
            .tcp_keepalive(TcpKeepalive::Explicit(Duration::from_secs(10)))
            .without_system()
            .build()
            .unwrap();
        assert_eq!(
            cfg.tcp_keepalive,
            TcpKeepalive::Explicit(Duration::from_secs(10))
        );
    }
}
