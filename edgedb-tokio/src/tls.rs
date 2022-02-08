use std::sync::Arc;
use std::time::SystemTime;

use rustls::client::{ServerCertVerifier, ServerCertVerified};
use rustls::{Certificate, RootCertStore, TLSError, OwnedTrustAnchor};
use rustls::{ServerName};
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

pub struct NoHostnameVerifier;

impl NoHostnameVerifier {
    pub fn new() -> Self {
        Self {
        }
    }
}

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(&self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: SystemTime
    ) -> Result<ServerCertVerified, TLSError> {
        /*
        let end_entity: webpki::EndEntityCert = end_entity.0[..].try_into()
            .map_err(|e| {
                log::warn!("Could not parse TLS certificate {:#}", e);
                TLSError::WebPKIError(e)
            });
        let trust_roots = self.
        end_entity.verify_is_valid_tls_server_cert(
            &SIG_ALGS,
            &webpki::TlsServerTrustAnchors(&trust_roots),
            &intermediaries,
            now,
        ).map_err(TLSError::WebPKIError);
        Ok(ServerCertVerified::assertion())
        */
        todo!();
    }
}

impl ServerCertVerifier for NullVerifier {
    fn verify_server_cert(&self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        server_name: &ServerName,
        scts: &mut dyn Iterator<Item = &[u8]>,
        ocsp_response: &[u8],
        now: SystemTime
    ) -> Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }
}

pub fn connector(
    cert_verifier: Arc<dyn ServerCertVerifier>,
) -> anyhow::Result<TlsConnectorBox>
{
    let mut builder = TlsConnector::builder()?;
    builder.config.dangerous().set_certificate_verifier(cert_verifier);
    builder.set_alpn_protocols(&[b"edgedb-binary"])?;
    let connector = builder.build()?.into_dyn();
    Ok(connector)
}
