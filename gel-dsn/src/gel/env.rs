use super::{
    error::*, BuildContext, ClientSecurity, CloudCerts, FromParamStr, InstanceName, TlsSecurity,
};
use crate::host::HostType;
use crate::EnvVar;
use std::{borrow::Cow, fmt::Debug, num::NonZeroU16, path::PathBuf, time::Duration};

define_env!(
    type Error = ParseError;

    /// The host to connect to.
    #[env(GEL_HOST, EDGEDB_HOST)]
    host: HostType,

    /// The port to connect to.
    #[env(GEL_PORT, EDGEDB_PORT)]
    #[preprocess=ignore_docker_tcp_port]
    port: NonZeroU16,

    /// The database name to connect to.
    #[env(GEL_DATABASE, EDGEDB_DATABASE)]
    database: String,

    /// The branch name to connect to.
    #[env(GEL_BRANCH, EDGEDB_BRANCH)]
    branch: String,

    /// The username to connect as.
    #[env(GEL_USER, EDGEDB_USER)]
    user: String,

    /// The password to use for authentication.
    #[env(GEL_PASSWORD, EDGEDB_PASSWORD)]
    password: String,

    /// TLS server name to verify.
    #[env(GEL_TLS_SERVER_NAME, EDGEDB_TLS_SERVER_NAME)]
    tls_server_name: String,

    /// Path to credentials file.
    #[env(GEL_CREDENTIALS_FILE, EDGEDB_CREDENTIALS_FILE)]
    credentials_file: PathBuf,

    /// Instance name to connect to.
    #[env(GEL_INSTANCE, EDGEDB_INSTANCE)]
    instance: InstanceName,

    /// Connection DSN string.
    #[env(GEL_DSN, EDGEDB_DSN)]
    dsn: String,

    /// Secret key for authentication.
    #[env(GEL_SECRET_KEY, EDGEDB_SECRET_KEY)]
    secret_key: String,

    /// Client security mode.
    #[env(GEL_CLIENT_SECURITY, EDGEDB_CLIENT_SECURITY)]
    client_security: ClientSecurity,

    /// TLS security mode.
    #[env(GEL_CLIENT_TLS_SECURITY, EDGEDB_CLIENT_TLS_SECURITY)]
    client_tls_security: TlsSecurity,

    /// Path to TLS CA certificate file.
    #[env(GEL_TLS_CA, EDGEDB_TLS_CA)]
    tls_ca: String,

    /// Path to TLS CA certificate file.
    #[env(GEL_TLS_CA_FILE, EDGEDB_TLS_CA_FILE)]
    tls_ca_file: PathBuf,

    /// Cloud profile name.
    #[env(GEL_CLOUD_PROFILE, EDGEDB_CLOUD_PROFILE)]
    cloud_profile: String,

    /// Cloud certificates mode.
    #[env(_GEL_CLOUD_CERTS, _EDGEDB_CLOUD_CERTS)]
    _cloud_certs: CloudCerts,

    /// How long to wait for server to become available.
    #[env(GEL_WAIT_UNTIL_AVAILABLE, EDGEDB_WAIT_UNTIL_AVAILABLE)]
    wait_until_available: Duration,
);

fn ignore_docker_tcp_port(
    s: &str,
    context: &mut impl BuildContext,
) -> Result<Option<String>, ParseError> {
    if s.starts_with("tcp://") {
        context.warn(Warning::DockerPortIgnored(
            "GEL_PORT/EDGEDB_PORT".to_string(),
        ));
        Ok(None)
    } else {
        Ok(Some(s.to_string()))
    }
}

/// Helper to define environment variables.
pub use crate::__UNEXPORTED_define_env as define_env;

#[doc(hidden)]
#[macro_export]
macro_rules! __UNEXPORTED_define_env {
    (
        type Error = $error:ty;
        $(
            #[doc=$doc:expr]
            #[env($($env_name:expr),+)]
            $(#[preprocess=$preprocess:expr])?
            $(#[parse=$parse:expr])?
            $(#[validate=$validate:expr])?
            $name:ident: $type:ty
        ),* $(,)?
    ) => {
        #[derive(Debug, Clone)]
        pub struct Env {
        }

        #[allow(clippy::diverging_sub_expression)]
        impl Env {
            $(
                #[doc = $doc]
                pub fn $name(context: &mut impl $crate::gel::BuildContext) -> ::std::result::Result<::std::option::Option<$type>, $error> {
                    const ENV_NAMES: &[&str] = &[$(stringify!($env_name)),+];
                    let Some((_name, s)) = $crate::gel::env::get_envs(ENV_NAMES, context)? else {
                        return Ok(None);
                    };
                    $(let Some(s) = $preprocess(&s, context)? else {
                        return Ok(None);
                    };)?

                    // This construct lets us choose between $parse and std::str::FromStr
                    // without requiring all types to implement FromStr.
                    #[allow(unused_labels)]
                    let value: $type = 'block: {
                        $(
                            break 'block $parse(&name, &s)?;

                            // Disable the fallback parser
                            #[cfg(all(debug_assertions, not(debug_assertions)))]
                        )?
                        $crate::gel::env::parse::<_, $error>(s, context)?
                    };

                    $($validate(name, &value)?;)?
                    Ok(Some(value))
                }
            )*
        }
    };
}

#[inline(never)]
#[doc(hidden)]
pub fn parse<T: FromParamStr, E>(
    s: impl AsRef<str>,
    context: &mut impl BuildContext,
) -> Result<T, E>
where
    <T as FromParamStr>::Err: Into<E>,
{
    match T::from_param_str(s.as_ref(), context) {
        Ok(value) => Ok(value),
        Err(e) => Err(e.into()),
    }
}

#[inline(never)]
#[doc(hidden)]
pub fn get_envs(
    names: &'static [&'static str],
    context: &mut impl BuildContext,
) -> Result<Option<(&'static str, Cow<'static, str>)>, ParseError> {
    let mut value = None;
    let mut found_vars = Vec::new();

    for name in names {
        match context.env().read(name) {
            Ok(val) => {
                found_vars.push(format!("{}={}", name, val));
                if value.is_none() {
                    value = Some((*name, Cow::Owned(val.to_string())));
                }
            }
            Err(std::env::VarError::NotPresent) => continue,
            Err(err @ std::env::VarError::NotUnicode(_)) => {
                return Err(ParseError::EnvNotFound(
                    EnvironmentSource::Explicit,
                    err.to_string(),
                ));
            }
        }
    }

    if found_vars.len() > 1 {
        context.warn(Warning::MultipleEnvironmentVariables(found_vars));
    }

    Ok(value)
}

#[cfg(test)]
mod tests {
    use crate::gel::{error::Warning, Warnings};
    use std::collections::HashMap;

    use super::*;
    use crate::gel::BuildContextImpl;

    define_env! {
        type Error = ParseError;

        #[doc="The host to connect to."]
        #[env(GEL_HOST, EDGEDB_HOST)]
        host: String,
    }

    #[test]
    fn test_define_env() {
        let map = HashMap::from([("GEL_HOST", "localhost"), ("EDGEDB_HOST", "localhost")]);
        let mut context = BuildContextImpl::new_with(&map, ());
        let warnings = Warnings::default();
        context.logging.warning = Some(warnings.clone().warn_fn());
        assert_eq!(
            Env::host(&mut context).unwrap(),
            Some("localhost".to_string())
        );
        assert_eq!(
            warnings.into_vec(),
            vec![Warning::MultipleEnvironmentVariables(vec![
                "GEL_HOST=localhost".to_string(),
                "EDGEDB_HOST=localhost".to_string(),
            ])]
        );
    }
}
