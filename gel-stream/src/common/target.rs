use std::{
    borrow::Cow,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    sync::Arc,
};

use derive_more::Debug;
use rustls_pki_types::ServerName;

use crate::TlsParameters;

/// A target name describes the TCP or Unix socket that a client will connect to.
pub struct TargetName {
    inner: MaybeResolvedTarget,
}

impl std::fmt::Debug for TargetName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl TargetName {
    /// Create a new target for a Unix socket.
    #[cfg(unix)]
    pub fn new_unix_path(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let path = ResolvedTarget::from(std::os::unix::net::SocketAddr::from_pathname(path)?);
        Ok(Self {
            inner: MaybeResolvedTarget::Resolved(path),
        })
    }

    /// Create a new target for a Unix socket.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn new_unix_domain(domain: impl AsRef<[u8]>) -> Result<Self, std::io::Error> {
        use std::os::linux::net::SocketAddrExt;
        let domain =
            ResolvedTarget::from(std::os::unix::net::SocketAddr::from_abstract_name(domain)?);
        Ok(Self {
            inner: MaybeResolvedTarget::Resolved(domain),
        })
    }

    /// Create a new target for a TCP socket.
    #[allow(private_bounds)]
    pub fn new_tcp(host: impl TcpResolve) -> Self {
        Self { inner: host.into() }
    }

    /// Resolves the target addresses for a given host.
    pub fn to_addrs_sync(&self) -> Result<Vec<ResolvedTarget>, std::io::Error> {
        use std::net::ToSocketAddrs;
        let mut result = Vec::new();
        match &self.inner {
            MaybeResolvedTarget::Resolved(addr) => {
                return Ok(vec![addr.clone()]);
            }
            MaybeResolvedTarget::Unresolved(host, port, _interface) => {
                let addrs = format!("{}:{}", host, port).to_socket_addrs()?;
                result.extend(addrs.map(ResolvedTarget::SocketAddr));
            }
        }
        Ok(result)
    }
}

#[derive(Clone, Debug)]
pub struct Target {
    inner: TargetInner,
}

#[allow(private_bounds)]
impl Target {
    pub fn new(name: TargetName) -> Self {
        Self {
            inner: TargetInner::NoTls(name.inner),
        }
    }

    pub fn new_tls(name: TargetName, params: TlsParameters) -> Self {
        Self {
            inner: TargetInner::Tls(name.inner, params.into()),
        }
    }

    pub fn new_starttls(name: TargetName, params: TlsParameters) -> Self {
        Self {
            inner: TargetInner::StartTls(name.inner, params.into()),
        }
    }

    pub fn new_resolved(target: ResolvedTarget) -> Self {
        Self {
            inner: TargetInner::NoTls(target.into()),
        }
    }

    pub fn new_resolved_tls(target: ResolvedTarget, params: TlsParameters) -> Self {
        Self {
            inner: TargetInner::Tls(target.into(), params.into()),
        }
    }

    pub fn new_resolved_starttls(target: ResolvedTarget, params: TlsParameters) -> Self {
        Self {
            inner: TargetInner::StartTls(target.into(), params.into()),
        }
    }

    /// Create a new target for a Unix socket.
    #[cfg(unix)]
    pub fn new_unix_path(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let path = ResolvedTarget::from(std::os::unix::net::SocketAddr::from_pathname(path)?);
        Ok(Self {
            inner: TargetInner::NoTls(path.into()),
        })
    }

    /// Create a new target for a Unix socket.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn new_unix_domain(domain: impl AsRef<[u8]>) -> Result<Self, std::io::Error> {
        use std::os::linux::net::SocketAddrExt;
        let domain =
            ResolvedTarget::from(std::os::unix::net::SocketAddr::from_abstract_name(domain)?);
        Ok(Self {
            inner: TargetInner::NoTls(domain.into()),
        })
    }

    /// Create a new target for a TCP socket.
    pub fn new_tcp(host: impl TcpResolve) -> Self {
        Self {
            inner: TargetInner::NoTls(host.into()),
        }
    }

    /// Create a new target for a TCP socket with TLS.
    pub fn new_tcp_tls(host: impl TcpResolve, params: TlsParameters) -> Self {
        Self {
            inner: TargetInner::Tls(host.into(), params.into()),
        }
    }

    /// Create a new target for a TCP socket with STARTTLS.
    pub fn new_tcp_starttls(host: impl TcpResolve, params: TlsParameters) -> Self {
        Self {
            inner: TargetInner::StartTls(host.into(), params.into()),
        }
    }

    pub fn try_set_tls(&mut self, params: TlsParameters) -> Option<Option<Arc<TlsParameters>>> {
        // Don't set TLS parameters on Unix sockets.
        if self.maybe_resolved().path().is_some() {
            return None;
        }

        let params = params.into();

        // Temporary
        let no_target = TargetInner::NoTls(MaybeResolvedTarget::Resolved(
            ResolvedTarget::SocketAddr(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)),
        ));

        match std::mem::replace(&mut self.inner, no_target) {
            TargetInner::NoTls(target) => {
                self.inner = TargetInner::Tls(target, params);
                Some(None)
            }
            TargetInner::Tls(target, old_params) => {
                self.inner = TargetInner::Tls(target, params);
                Some(Some(old_params))
            }
            TargetInner::StartTls(target, old_params) => {
                self.inner = TargetInner::StartTls(target, params);
                Some(Some(old_params))
            }
        }
    }

    pub fn try_remove_tls(&mut self) -> Option<Arc<TlsParameters>> {
        // Temporary
        let no_target = TargetInner::NoTls(MaybeResolvedTarget::Resolved(
            ResolvedTarget::SocketAddr(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)),
        ));

        match std::mem::replace(&mut self.inner, no_target) {
            TargetInner::NoTls(target) => {
                self.inner = TargetInner::NoTls(target);
                None
            }
            TargetInner::Tls(target, old_params) => {
                self.inner = TargetInner::NoTls(target);
                Some(old_params)
            }
            TargetInner::StartTls(target, old_params) => {
                self.inner = TargetInner::NoTls(target);
                Some(old_params)
            }
        }
    }

    /// Get the port of the target. If the target type does not include a port,
    /// this will return None.
    pub fn port(&self) -> Option<u16> {
        self.maybe_resolved().port()
    }

    /// Set the port of the target. If the target type does not include a port,
    /// this will return None. Otherwise, it will return the old port.
    pub fn try_set_port(&mut self, port: u16) -> Option<u16> {
        self.maybe_resolved_mut().set_port(port)
    }

    /// Get the path of the target. If the target type does not include a path,
    /// this will return None.
    pub fn path(&self) -> Option<&Path> {
        self.maybe_resolved().path()
    }

    /// Get the host of the target. For resolved IP addresses, this is the
    /// string representation of the IP address. For unresolved hostnames, this
    /// is the hostname. If the target type does not include a host, this will
    /// return None.
    pub fn host(&self) -> Option<Cow<str>> {
        self.maybe_resolved().host()
    }

    /// Get the name of the target. For resolved IP addresses, this is the
    /// string representation of the IP address. For unresolved hostnames, this
    /// is the hostname.
    pub fn name(&self) -> Option<ServerName> {
        self.maybe_resolved().name()
    }

    /// Get the host and port of the target. If the target type does not include
    /// a host or port, this will return None.
    pub fn tcp(&self) -> Option<(Cow<str>, u16)> {
        self.maybe_resolved().tcp()
    }

    pub(crate) fn maybe_resolved(&self) -> &MaybeResolvedTarget {
        match &self.inner {
            TargetInner::NoTls(target) => target,
            TargetInner::Tls(target, _) => target,
            TargetInner::StartTls(target, _) => target,
        }
    }

    pub(crate) fn maybe_resolved_mut(&mut self) -> &mut MaybeResolvedTarget {
        match &mut self.inner {
            TargetInner::NoTls(target) => target,
            TargetInner::Tls(target, _) => target,
            TargetInner::StartTls(target, _) => target,
        }
    }

    pub(crate) fn is_starttls(&self) -> bool {
        matches!(self.inner, TargetInner::StartTls(_, _))
    }

    pub(crate) fn maybe_ssl(&self) -> Option<&TlsParameters> {
        match &self.inner {
            TargetInner::NoTls(_) => None,
            TargetInner::Tls(_, params) => Some(params),
            TargetInner::StartTls(_, params) => Some(params),
        }
    }
}

#[derive(Clone, derive_more::From)]
pub(crate) enum MaybeResolvedTarget {
    Resolved(ResolvedTarget),
    Unresolved(Cow<'static, str>, u16, Option<Cow<'static, str>>),
}

impl std::fmt::Debug for MaybeResolvedTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(addr)) => {
                if let SocketAddr::V6(addr) = addr {
                    if addr.scope_id() != 0 {
                        write!(f, "[{}%{}]:{}", addr.ip(), addr.scope_id(), addr.port())
                    } else {
                        write!(f, "[{}]:{}", addr.ip(), addr.port())
                    }
                } else {
                    write!(f, "{}:{}", addr.ip(), addr.port())
                }
            }
            MaybeResolvedTarget::Resolved(ResolvedTarget::UnixSocketAddr(addr)) => {
                if let Some(path) = addr.as_pathname() {
                    return write!(f, "{}", path.to_string_lossy());
                } else {
                    #[cfg(any(target_os = "linux", target_os = "android"))]
                    {
                        use std::os::linux::net::SocketAddrExt;
                        if let Some(name) = addr.as_abstract_name() {
                            return write!(f, "@{}", String::from_utf8_lossy(name));
                        }
                    }
                }
                Ok(())
            }
            MaybeResolvedTarget::Unresolved(host, port, interface) => {
                write!(f, "{}:{}", host, port)?;
                if let Some(interface) = interface {
                    write!(f, "%{}", interface)?;
                }
                Ok(())
            }
        }
    }
}

impl MaybeResolvedTarget {
    fn name(&self) -> Option<ServerName> {
        match self {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(addr)) => {
                Some(ServerName::IpAddress(addr.ip().into()))
            }
            MaybeResolvedTarget::Unresolved(host, _, _) => {
                Some(ServerName::DnsName(host.to_string().try_into().ok()?))
            }
            _ => None,
        }
    }

    fn tcp(&self) -> Option<(Cow<str>, u16)> {
        match self {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(addr)) => {
                Some((Cow::Owned(addr.ip().to_string()), addr.port()))
            }
            MaybeResolvedTarget::Unresolved(host, port, _) => Some((Cow::Borrowed(host), *port)),
            _ => None,
        }
    }

    fn path(&self) -> Option<&Path> {
        match self {
            MaybeResolvedTarget::Resolved(ResolvedTarget::UnixSocketAddr(addr)) => {
                addr.as_pathname()
            }
            _ => None,
        }
    }

    fn host(&self) -> Option<Cow<str>> {
        match self {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(addr)) => {
                Some(Cow::Owned(addr.ip().to_string()))
            }
            MaybeResolvedTarget::Unresolved(host, _, _) => Some(Cow::Borrowed(host)),
            _ => None,
        }
    }

    fn port(&self) -> Option<u16> {
        match self {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(addr)) => Some(addr.port()),
            MaybeResolvedTarget::Unresolved(_, port, _) => Some(*port),
            _ => None,
        }
    }

    fn set_port(&mut self, new_port: u16) -> Option<u16> {
        match self {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(addr)) => {
                let old_port = addr.port();
                addr.set_port(new_port);
                Some(old_port)
            }
            MaybeResolvedTarget::Unresolved(_, port, _) => {
                let old_port = *port;
                *port = new_port;
                Some(old_port)
            }
            _ => None,
        }
    }
}

/// The type of connection.
#[derive(Clone, Debug)]
enum TargetInner {
    NoTls(MaybeResolvedTarget),
    Tls(MaybeResolvedTarget, Arc<TlsParameters>),
    StartTls(MaybeResolvedTarget, Arc<TlsParameters>),
}

#[derive(Clone, Debug, derive_more::From)]
/// The resolved target of a connection attempt.
pub enum ResolvedTarget {
    SocketAddr(std::net::SocketAddr),
    #[cfg(unix)]
    UnixSocketAddr(std::os::unix::net::SocketAddr),
}

impl ResolvedTarget {
    pub fn tcp(&self) -> Option<SocketAddr> {
        match self {
            ResolvedTarget::SocketAddr(addr) => Some(*addr),
            _ => None,
        }
    }
}

pub trait LocalAddress {
    fn local_address(&self) -> std::io::Result<ResolvedTarget>;
}

trait TcpResolve {
    fn into(self) -> MaybeResolvedTarget;
}

impl<S: AsRef<str>> TcpResolve for (S, u16) {
    fn into(self) -> MaybeResolvedTarget {
        if let Ok(addr) = self.0.as_ref().parse::<IpAddr>() {
            MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(SocketAddr::new(addr, self.1)))
        } else {
            MaybeResolvedTarget::Unresolved(Cow::Owned(self.0.as_ref().to_owned()), self.1, None)
        }
    }
}

impl TcpResolve for SocketAddr {
    fn into(self) -> MaybeResolvedTarget {
        MaybeResolvedTarget::Resolved(ResolvedTarget::SocketAddr(self))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddrV6;

    use super::*;

    #[test]
    fn test_target() {
        let target = Target::new_tcp(("localhost", 5432));
        assert_eq!(
            target.name(),
            Some(ServerName::DnsName("localhost".try_into().unwrap()))
        );
    }

    #[test]
    fn test_target_name() {
        let target = TargetName::new_tcp(("localhost", 5432));
        assert_eq!(format!("{target:?}"), "localhost:5432");

        let target = TargetName::new_tcp(("127.0.0.1", 5432));
        assert_eq!(format!("{target:?}"), "127.0.0.1:5432");

        let target = TargetName::new_tcp(("::1", 5432));
        assert_eq!(format!("{target:?}"), "[::1]:5432");

        let target = TargetName::new_tcp(SocketAddr::V6(SocketAddrV6::new(
            "fe80::1ff:fe23:4567:890a".parse().unwrap(),
            5432,
            0,
            2,
        )));
        assert_eq!(format!("{target:?}"), "[fe80::1ff:fe23:4567:890a%2]:5432");

        let target = TargetName::new_unix_path("/tmp/test.sock").unwrap();
        assert_eq!(format!("{target:?}"), "/tmp/test.sock");

        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            let target = TargetName::new_unix_domain("test").unwrap();
            assert_eq!(format!("{target:?}"), "@test");
        }
    }
}
