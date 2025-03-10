use gel_stream::TargetName;
use std::{
    borrow::Cow,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    path::PathBuf,
};

#[cfg(feature = "serde")]
use serde::Serialize;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub enum HostTarget {
    PostgreSQL,
    Gel,
    GelAdmin,
    Raw,
}

impl HostTarget {
    fn target_name(&self, port: u16) -> Option<String> {
        match self {
            HostTarget::PostgreSQL => Some(format!(".s.PGSQL.{}", port)),
            HostTarget::Gel => Some(format!(".s.EDGEDB.{}", port)),
            HostTarget::GelAdmin => Some(format!(".s.EDGEDB.admin.{}", port)),
            HostTarget::Raw => None,
        }
    }
}

/// A pointer to a host and port which may be a hostname, IP address or unix
/// socket.
/// ```
#[derive(Clone, derive_more::Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize))]
#[debug("{}", self)]
pub struct Host(pub(crate) HostType, pub(crate) u16, pub(crate) HostTarget);

impl Host {
    pub fn new(host: HostType, port: u16, host_target: HostTarget) -> Self {
        Self(host, port, host_target)
    }

    pub fn target_name(&self) -> Result<TargetName, std::io::Error> {
        match &self.0 .0 {
            HostTypeInner::Hostname(hostname) => Ok(TargetName::new_tcp((hostname, self.1))),
            HostTypeInner::IP(ip, Some(interface)) => Ok(TargetName::new_tcp((
                format!("{}%{}", ip, interface),
                self.1,
            ))),
            HostTypeInner::IP(ip, None) => Ok(TargetName::new_tcp((format!("{}", ip), self.1))),
            HostTypeInner::Path(path) => {
                if let Some(suffix) = self.2.target_name(self.1) {
                    TargetName::new_unix_path(path.join(suffix))
                } else {
                    TargetName::new_unix_path(path)
                }
            }
            #[allow(unused)]
            HostTypeInner::Abstract(name) => {
                #[cfg(any(target_os = "linux", target_os = "android"))]
                {
                    if let Some(suffix) = self.2.target_name(self.1) {
                        TargetName::new_unix_domain(format!("{}/{}", name, suffix))
                    } else {
                        TargetName::new_unix_domain(name)
                    }
                }
                #[cfg(not(any(target_os = "linux", target_os = "android")))]
                {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "Abstract sockets unsupported on this platform",
                    ))
                }
            }
        }
    }

    pub fn is_unix(&self) -> bool {
        matches!(
            self.0 .0,
            HostTypeInner::Path(_) | HostTypeInner::Abstract(_)
        )
    }
}

impl std::fmt::Display for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let port = self.1;
        match &self.0 .0 {
            HostTypeInner::Hostname(hostname) => write!(f, "{}:{}", hostname, port),
            HostTypeInner::IP(ip, Some(interface)) => write!(f, "[{}%{}]:{}", ip, interface, port),
            HostTypeInner::IP(ip, None) => write!(f, "[{}]:{}", ip, port),
            HostTypeInner::Path(path) => {
                if let Some(target_name) = self.2.target_name(port) {
                    write!(f, "{}/{}", path.display(), target_name)
                } else {
                    write!(f, "{}", path.display())
                }
            }
            HostTypeInner::Abstract(name) => {
                if let Some(target_name) = self.2.target_name(port) {
                    write!(f, "@{}/{}", name, target_name)
                } else {
                    write!(f, "@{}", name)
                }
            }
        }
    }
}

/// A pointer to a host which may be a hostname, IP address or unix socket.
///
/// ```
/// # use gel_dsn::HostType;
/// # use std::str::FromStr;
/// let host = HostType::from_str("localhost").unwrap();
/// assert_eq!(host.to_string(), "localhost");
/// let host = HostType::from_str("192.168.1.1").unwrap();
/// assert_eq!(host.to_string(), "192.168.1.1");
/// # #[cfg(unix)] {
/// let host = HostType::from_str("/tmp/my.sock").unwrap();
/// assert_eq!(host.to_string(), "/tmp/my.sock");
/// # }
/// ```

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct HostType(HostTypeInner);

pub const LOCALHOST: &HostType = &HostType(HostTypeInner::Hostname(Cow::Borrowed("localhost")));

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize))]
enum HostTypeInner {
    Hostname(Cow<'static, str>),
    IP(IpAddr, Option<String>),
    Path(PathBuf),
    Abstract(String),
}

impl std::fmt::Display for HostType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            HostTypeInner::Hostname(hostname) => write!(f, "{}", hostname),
            HostTypeInner::IP(ip, Some(interface)) => write!(f, "{}%{}", ip, interface),
            HostTypeInner::IP(ip, None) => {
                write!(f, "{}", ip)
            }
            HostTypeInner::Path(path) => write!(f, "{}", path.display()),
            HostTypeInner::Abstract(name) => write!(f, "@{}", name),
        }
    }
}

impl HostType {
    pub fn from_unix_path(path: PathBuf) -> Self {
        HostType(HostTypeInner::Path(path))
    }

    pub fn try_from_str(s: &str) -> Result<Self, &str> {
        if s.is_empty() {
            return Err(s);
        }
        if s.contains('[') || s.contains(']') {
            return Err(s);
        }
        if s.starts_with('/') {
            return Ok(HostType(HostTypeInner::Path(PathBuf::from(s))));
        }
        if let Some(s) = s.strip_prefix('@') {
            return Ok(HostType(HostTypeInner::Abstract(s.to_string())));
        }
        if s.contains('%') {
            let (ip_str, interface) = s.split_once('%').unwrap();
            if interface.is_empty() {
                return Err(s);
            }
            let ip = ip_str.parse::<Ipv6Addr>().map_err(|_| s)?;
            return Ok(HostType(HostTypeInner::IP(
                IpAddr::V6(ip),
                Some(interface.to_string()),
            )));
        }
        if let Ok(ip) = s.parse::<IpAddr>() {
            Ok(HostType(HostTypeInner::IP(ip, None)))
        } else {
            if s.contains(':') || s.contains(',') {
                return Err(s);
            }
            Ok(HostType(HostTypeInner::Hostname(Cow::Owned(s.to_string()))))
        }
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Error, derive_more::Display,
)]
pub struct HostParseError;

impl std::str::FromStr for HostType {
    type Err = HostParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        HostType::try_from_str(s).map_err(|_| HostParseError)
    }
}

impl<S: AsRef<str>> From<&url::Host<S>> for HostType {
    fn from(host: &url::Host<S>) -> Self {
        match host {
            url::Host::Domain(domain) => HostType(HostTypeInner::Hostname(Cow::Owned(
                domain.as_ref().to_string(),
            ))),
            url::Host::Ipv4(ip) => HostType(HostTypeInner::IP(IpAddr::V4(*ip), None)),
            url::Host::Ipv6(ip) => HostType(HostTypeInner::IP(IpAddr::V6(*ip), None)),
        }
    }
}

impl From<IpAddr> for HostType {
    fn from(ip: IpAddr) -> Self {
        HostType(HostTypeInner::IP(ip, None))
    }
}

impl From<Ipv6Addr> for HostType {
    fn from(ip: Ipv6Addr) -> Self {
        HostType(HostTypeInner::IP(IpAddr::V6(ip), None))
    }
}

impl From<Ipv4Addr> for HostType {
    fn from(ip: Ipv4Addr) -> Self {
        HostType(HostTypeInner::IP(IpAddr::V4(ip), None))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_host_display() {
        let host = Host::new(
            HostType::from_str("localhost").unwrap(),
            5656,
            HostTarget::Gel,
        );
        assert_eq!(host.to_string(), "localhost:5656");
    }
}
