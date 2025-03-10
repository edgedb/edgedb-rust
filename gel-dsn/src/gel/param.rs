use rustls_pki_types::CertificateDer;
use std::{
    num::NonZeroU16,
    path::{Path, PathBuf},
    str::FromStr,
};
use url::Url;

use super::{
    duration, error::*, BuildContext, ClientSecurity, CloudCerts, CloudCredentialsFile,
    CredentialsFile, InstanceName, TcpKeepalive, TlsSecurity,
};
use crate::{gel::context_trace, host::HostType, EnvVar, FileAccess};

pub(crate) trait FromParamStr: Sized {
    type Err;
    fn from_param_str(s: &str, context: &mut impl BuildContext) -> Result<Self, Self::Err>;
}

macro_rules! impl_from_param_str {
    ($($t:ty),*) => {
        $(
            impl FromParamStr for $t {
                type Err = <$t as FromStr>::Err;
                fn from_param_str(s: &str, _context: &mut impl BuildContext) -> Result<Self, Self::Err> {
                    FromStr::from_str(s)
                }
            }
        )*
    };
}

impl_from_param_str!(
    InstanceName,
    HostType,
    u16,
    NonZeroU16,
    usize,
    PathBuf,
    String,
    CredentialsFile,
    TlsSecurity,
    ClientSecurity,
    CloudCredentialsFile,
    CloudCerts,
    TcpKeepalive
);

impl FromParamStr for std::time::Duration {
    type Err = ParseError;
    fn from_param_str(s: &str, _context: &mut impl BuildContext) -> Result<Self, Self::Err> {
        duration::Duration::from_str(s)
            .map_err(|_| ParseError::InvalidDuration)
            .map(|d| std::time::Duration::from_micros(d.to_micros() as u64))
    }
}

impl FromParamStr for Url {
    type Err = ParseError;
    fn from_param_str(s: &str, context: &mut impl BuildContext) -> Result<Self, Self::Err> {
        // Ensure the URL contains `://`
        if !s.starts_with("edgedb://") && !s.starts_with("gel://") {
            return Err(ParseError::InvalidDsn(InvalidDsnError::InvalidScheme));
        }

        let res = Url::parse(s);
        match res {
            Ok(url) => Ok(url),
            Err(e) => {
                // Because the url crate refuses to add scope identifiers, we need to
                // strip them for now.
                if e == url::ParseError::InvalidIpv6Address && s.contains("%25") {
                    // Try to re-parse "s" without the scope identifier. It's possible that
                    // the URL has a username/password and we're trying to parse out
                    // scheme://username:password@[<ipv6>%25<scope>] and replace it with
                    // scheme://username:password@[<ipv6>].

                    let original_url = s;

                    // First, trim off the scheme.
                    let Some(scheme_end) = s.find("://") else {
                        return Err(ParseError::InvalidDsn(InvalidDsnError::InvalidScheme));
                    };
                    let s = &s[scheme_end + 3..];

                    // Next, find the end of the authority.
                    let authority_end = if let Some(authority_end) = s.find('/') {
                        authority_end
                    } else {
                        s.len()
                    };

                    let s = &s[..authority_end];

                    let Some(scope_start) = s.rfind("%25") else {
                        return Err(ParseError::InvalidDsn(InvalidDsnError::ParseError));
                    };
                    let Some(addr_end) = s.rfind(']') else {
                        return Err(ParseError::InvalidDsn(InvalidDsnError::ParseError));
                    };

                    // Now we can do the math to remove the scope chunk of original_url. We
                    // start from the %25 and go until the ].
                    let scope_len = addr_end - scope_start;
                    let scope_start = scheme_end + 3 + scope_start;

                    let new_url = original_url[..scope_start].to_string()
                        + &original_url[scope_start + scope_len..];

                    context_trace!(context,
                        "Ignored scope identifier in IPv6 URL: {}, use an explicit host parameter instead",
                        &original_url[scope_start..scope_start + scope_len]
                    );

                    // YOLO parse the new URL.
                    Url::parse(&new_url)
                        .map_err(|_| ParseError::InvalidDsn(InvalidDsnError::ParseError))
                } else {
                    Err(ParseError::InvalidDsn(InvalidDsnError::ParseError))
                }
            }
        }
    }
}

impl FromParamStr for Vec<CertificateDer<'static>> {
    type Err = ParseError;
    fn from_param_str(s: &str, _context: &mut impl BuildContext) -> Result<Self, Self::Err> {
        let mut cursor = std::io::Cursor::new(s);
        let mut certs = Vec::new();
        for cert in rustls_pemfile::read_all(&mut cursor) {
            match cert.map_err(|_| ParseError::InvalidCertificate)? {
                rustls_pemfile::Item::X509Certificate(data) => {
                    certs.push(data);
                }
                _ => return Err(ParseError::InvalidCertificate),
            }
        }
        Ok(certs)
    }
}

/// A parameter with a lazily-loaded source.
///
/// This type may be sourced from a file, an environment variable, or provided
/// explicitly.
#[derive(Default, Debug, Clone)]
pub enum Param<T: Clone> {
    /// No value.
    #[default]
    None,
    /// Unparsed value.
    Unparsed(String),
    /// Value from given environment variable.
    Env(String),
    /// Value from given file.
    File(PathBuf),
    /// Value from environment variable pointing to a file.
    EnvFile(String),
    /// Parsed value.
    Parsed(T),
}

#[allow(private_bounds)]
impl<T: Clone> Param<T>
where
    T: FromParamStr,
    <T as FromParamStr>::Err: Into<ParseError>,
{
    pub fn from_unparsed(value: Option<String>) -> Self {
        if let Some(value) = value {
            Self::Unparsed(value)
        } else {
            Self::None
        }
    }

    pub fn from_file(value: Option<impl AsRef<Path>>) -> Self {
        if let Some(value) = value {
            Self::File(value.as_ref().to_path_buf())
        } else {
            Self::None
        }
    }

    pub fn from_parsed(value: Option<T>) -> Self {
        if let Some(value) = value {
            Self::Parsed(value)
        } else {
            Self::None
        }
    }

    pub fn take(&mut self) -> Param<T> {
        std::mem::take(self)
    }

    pub fn cast<U: Clone>(self) -> Result<Param<U>, Self> {
        match self {
            Self::None => Ok(Param::None),
            Self::Unparsed(value) => Ok(Param::Unparsed(value)),
            Self::Env(value) => Ok(Param::Env(value)),
            Self::File(value) => Ok(Param::File(value)),
            Self::EnvFile(value) => Ok(Param::EnvFile(value)),
            Self::Parsed(value) => Err(Self::Parsed(value)),
        }
    }

    pub fn get(&self, context: &mut impl BuildContext) -> Result<Option<T>, ParseError> {
        let value = match self {
            Self::None => {
                return Ok(None);
            }
            Self::Unparsed(value) => value.clone(),
            Self::Env(key) => {
                context_trace!(context, "Reading env: {key}");
                context
                    .env()
                    .read(key)
                    .map(|s| s.to_string())
                    .map_err(|_| {
                        ParseError::EnvNotFound(EnvironmentSource::Param, key.to_string())
                    })?
            }
            Self::File(path) => {
                context_trace!(context, "Reading file: {path:?}");
                let res = context
                    .files()
                    .read(path)
                    .map(|s| s.to_string())
                    .map_err(|_| ParseError::FileNotFound);
                context_trace!(context, "File content: {res:?}");
                res?
            }
            Self::EnvFile(key) => {
                context_trace!(context, "Reading env for file: {key}");
                let env = context
                    .env()
                    .read(key)
                    .map_err(|_| {
                        ParseError::EnvNotFound(EnvironmentSource::Param, key.to_string())
                    })?
                    .to_string();
                context_trace!(context, "Reading file: {env}");
                let res = context
                    .files()
                    .read(&PathBuf::from(env))
                    .map_err(|_| ParseError::FileNotFound);
                context_trace!(context, "File content: {res:?}");
                res?
            }
            Self::Parsed(value) => return Ok(Some(value.clone())),
        };

        let value = T::from_param_str(&value, context).map_err(|e| e.into())?;
        Ok(Some(value))
    }
}

impl<T: Clone> Param<T> {
    pub fn is_some(&self) -> bool {
        !matches!(self, Self::None)
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

#[cfg(test)]
mod tests {
    use crate::gel::BuildContextImpl;

    use super::*;

    #[test]
    fn test_dsn_with_scope() {
        for dsn in [
            "edgedb://[::1%25lo0]:5656",
            "edgedb://[::1%25lo0]:5656/",
            "edgedb://username%25@password%25:[::1%25lo0]:5656/db",
            "edgedb://username%25@password%25:[::1%25lo0]:5656/db/",
            "edgedb://user3@[fe80::1ff:fe23:4567:890a%25lo0]:3000/ab",
        ] {
            let result = <Url as FromParamStr>::from_param_str(dsn, &mut BuildContextImpl::new());
            let dsn2 = dsn.replace("%25lo0", "");
            let result2 =
                <Url as FromParamStr>::from_param_str(&dsn2, &mut BuildContextImpl::new());
            eprintln!("{dsn} = {result:?}, {dsn2} = {result2:?}");
            assert_eq!(
                result, result2,
                "Expected {} to parse the same as {}",
                dsn, dsn2
            );
        }
    }
}
