use std::sync::Arc;

use rustls::{Certificate, RootCertStore, TLSError, ServerCertVerified};
use rustls::{ServerCertVerifier, OwnedTrustAnchor};
use tls_api::{TlsConnector as _, TlsConnectorBuilder as _};
use tls_api::{TlsConnectorBox};
use tls_api_rustls::{TlsConnector};
use webpki::{DNSNameRef, SignatureAlgorithm, EndEntityCert};


static SIG_ALGS: &[&SignatureAlgorithm] = &[
    &webpki::ECDSA_P256_SHA256,
    &webpki::ECDSA_P256_SHA384,
    &webpki::ECDSA_P384_SHA256,
    &webpki::ECDSA_P384_SHA384,
    &webpki::ED25519,
    &webpki::RSA_PKCS1_2048_8192_SHA256,
    &webpki::RSA_PKCS1_2048_8192_SHA384,
    &webpki::RSA_PKCS1_2048_8192_SHA512,
    &webpki::RSA_PKCS1_3072_8192_SHA384,
];

pub struct NullVerifier;

pub struct CertVerifier {
    verify_hostname: bool,
}

impl CertVerifier {
    pub fn new(verify_hostname: bool) -> Self {
        Self {
            verify_hostname
        }
    }
}

type CertChainAndRoots<'a, 'b> = (
    webpki::EndEntityCert<'a>,
    Vec<&'a [u8]>,
    Vec<webpki::TrustAnchor<'b>>,
);


fn webpki_now() -> Result<webpki::Time, TLSError> {
    webpki::Time::try_from(std::time::SystemTime::now())
        .map_err(|_| TLSError::FailedToGetCurrentTime)
}

fn prepare<'a, 'b>(
    roots: &'b RootCertStore,
    presented_certs: &'a [Certificate],
) -> Result<CertChainAndRoots<'a, 'b>, TLSError> {
    if presented_certs.is_empty() {
        return Err(TLSError::NoCertificatesPresented);
    }

    // EE cert must appear first.
    let cert = webpki::EndEntityCert::from(&presented_certs[0].0).map_err(TLSError::WebPKIError)?;

    let chain: Vec<&'a [u8]> = presented_certs
        .iter()
        .skip(1)
        .map(|cert| cert.0.as_ref())
        .collect();

    let trustroots: Vec<webpki::TrustAnchor> = roots
        .roots
        .iter()
        .map(OwnedTrustAnchor::to_trust_anchor)
        .collect();
    Ok((cert, chain, trustroots))
}

impl ServerCertVerifier for CertVerifier {
    fn verify_server_cert(&self,
        roots: &RootCertStore,
        presented_certs: &[Certificate],
        dns_name: DNSNameRef,
        _ocsp_response: &[u8],
    ) -> Result<ServerCertVerified, TLSError> {
        let cert = verify_server_cert(roots, presented_certs)?;
        if self.verify_hostname {
            cert.verify_is_valid_for_dns_name(dns_name)
                .map_err(TLSError::WebPKIError)?;
        };
        Ok(ServerCertVerified::assertion())
    }
}

impl ServerCertVerifier for NullVerifier {
    fn verify_server_cert(&self,
        _roots: &RootCertStore,
        _presented_certs: &[Certificate],
        _dns_name: DNSNameRef,
        _ocsp_response: &[u8],
    ) -> Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }
}

pub fn verify_server_cert<'a>(
    roots: &RootCertStore,
    presented_certs: &'a [Certificate],
) -> Result<EndEntityCert<'a>, TLSError> {
    let (cert, chain, trust_roots) = prepare(roots, presented_certs)?;
    cert.verify_is_valid_tls_server_cert(
        &SIG_ALGS,
        &webpki::TLSServerTrustAnchors(&trust_roots),
        &chain,
        webpki_now()?,
    )
    .map_err(TLSError::WebPKIError)
    .map(|_| cert)
}

pub fn connector(
    cert: &rustls::RootCertStore,
    cert_verifier: Arc<dyn ServerCertVerifier>,
) -> Result<TlsConnectorBox, tls_api::Error>
{
    let mut builder = TlsConnector::builder()?;
    if cert.is_empty() {
        log::debug!("Loading native root certificates");
        match rustls_native_certs::load_native_certs() {
            Ok(loaded) => {
                builder.underlying_mut()
                        .root_store.roots.extend(loaded.roots);
            }
            Err((Some(loaded), e)) => {
                log::warn!("Error while loading native TLS certificates: {}. \
                    Using {} loaded ones.",
                    e, loaded.roots.len());
                builder.underlying_mut()
                        .root_store.roots.extend(loaded.roots);
            }
            Err((None, e)) => {
                log::warn!("Error while loading native TLS certificates: {}. \
                    Will use built-in ones.", e);
                // builtin certs are used by default in TLS API
            }
        }
    } else {
        log::debug!("Using custom root chain, {} certificates",
            cert.roots.len());
        builder.underlying_mut()
                .root_store.roots.extend(cert.roots.iter().cloned());
    };
    builder.config.dangerous().set_certificate_verifier(cert_verifier);
    builder.set_alpn_protocols(&[b"edgedb-binary"])?;
    let connector = builder.build()?.into_dyn();
    Ok(connector)
}
