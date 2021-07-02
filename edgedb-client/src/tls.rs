use std::sync::Arc;

use rustls::{Certificate, RootCertStore, TLSError, ServerCertVerified};
use rustls::{ServerCertVerifier, OwnedTrustAnchor};
use tls_api::{TlsConnector as _, TlsConnectorBuilder as _};
use tls_api::{TlsConnectorBox};
use tls_api_rustls::{TlsConnector};
use webpki::{DNSNameRef, SignatureAlgorithm};


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


pub struct CertVerifier {
    verify_hostname: bool,
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
        _dns_name: DNSNameRef,
        _ocsp_response: &[u8],
    ) -> Result<ServerCertVerified, TLSError> {
        let (cert, chain, trust_roots) = prepare(roots, presented_certs)?;
        cert.verify_is_valid_tls_server_cert(
            &SIG_ALGS,
            &webpki::TLSServerTrustAnchors(&trust_roots),
            &chain,
            webpki_now()?,
        ).map_err(TLSError::WebPKIError)?;
        Ok(ServerCertVerified::assertion())
    }
}

pub fn connector(cert: &rustls::RootCertStore, verify_hostname: Option<bool>)
    -> anyhow::Result<TlsConnectorBox>
{
    let mut builder = TlsConnector::builder()?;
    let verify;
    if cert.is_empty() {
        verify = verify_hostname.unwrap_or(true);
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
                anyhow::bail!("Error reading root certificates: {:#}. \
                    Cannot initialize TLS connection.", e);
            }
        }
    } else {
        verify = verify_hostname.unwrap_or(false);
        builder.underlying_mut()
                .root_store.roots.extend(cert.roots.iter().cloned());
    };
    builder.config.dangerous()
        .set_certificate_verifier(Arc::new(CertVerifier {
            verify_hostname: verify,
        }));
    builder.set_alpn_protocols(&[b"edgedb-binary"])?;
    Ok(builder.build()?.into_dyn())
}
