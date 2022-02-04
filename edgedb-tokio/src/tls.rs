use std::io;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use rustls::client::{ServerCertVerifier, ServerCertVerified};
use rustls::{Certificate};
use rustls::{ServerName};
use tls_api::{TlsConnector as _, TlsConnectorBuilder as _};
use tls_api::{TlsConnectorBox};
use tls_api_rustls::{TlsConnector};
use webpki::{SignatureAlgorithm};


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

pub struct NoHostnameVerifier {
    trust_anchors: Vec<OwnedTrustAnchor>,
}

pub struct OwnedTrustAnchor {
    subject: Vec<u8>,
    spki: Vec<u8>,
    name_constraints: Option<Vec<u8>>,
}

impl NoHostnameVerifier {
    pub fn new(trust_anchors: Vec<OwnedTrustAnchor>) -> Self {
        NoHostnameVerifier {
            trust_anchors
        }
    }
}

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(&self,
        end_entity: &Certificate,
        intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        now: SystemTime
    ) -> Result<ServerCertVerified, rustls::Error> {
        let webpki_now = webpki::Time::try_from(now)
            .map_err(|_| rustls::Error::FailedToGetCurrentTime)?;
        let end_entity: webpki::EndEntityCert = end_entity.0[..].try_into()
            .map_err(|e| {
                log::warn!("Could not parse TLS certificate {:#}", e);
                pki_error(e)
            })?;
        let trust_roots = self.trust_anchors.iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        let chain = intermediates.iter()
            .map(|c| c.as_ref())
            .collect::<Vec<_>>();
        end_entity.verify_is_valid_tls_server_cert(
            &SIG_ALGS,
            &webpki::TlsServerTrustAnchors(&trust_roots),
            &chain,
            webpki_now,
        ).map_err(pki_error)?;
        Ok(ServerCertVerified::assertion())
    }
}

impl ServerCertVerifier for NullVerifier {
    fn verify_server_cert(&self,
        _end_entity: &Certificate,
        _intermediates: &[Certificate],
        _server_name: &ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: SystemTime
    ) -> Result<ServerCertVerified, rustls::Error> {
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

impl From<webpki::TrustAnchor<'_>> for OwnedTrustAnchor {
    fn from(src: webpki::TrustAnchor) -> OwnedTrustAnchor {
        OwnedTrustAnchor {
            subject: src.subject.into(),
            spki: src.spki.into(),
            name_constraints: src.name_constraints.map(|b| b.into()),
        }
    }
}

impl<'a> Into<webpki::TrustAnchor<'a>> for &'a OwnedTrustAnchor {
    fn into(self) -> webpki::TrustAnchor<'a> {
        webpki::TrustAnchor {
            subject: &self.subject,
            spki: &self.spki,
            name_constraints: self.name_constraints.as_deref(),
        }
    }
}

impl Into<rustls::OwnedTrustAnchor> for OwnedTrustAnchor {
    fn into(self) -> rustls::OwnedTrustAnchor {
        rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
            self.subject,
            self.spki,
            self.name_constraints,
        )
    }
}

impl OwnedTrustAnchor {
    pub fn read_all(data: &str) -> anyhow::Result<Vec<OwnedTrustAnchor>> {
        let mut result = Vec::new();
        let open_data = rustls_pemfile::read_all(&mut io::Cursor::new(data))
            .context("error reading PEM data")?;
        for item in open_data {
            match item {
                rustls_pemfile::Item::X509Certificate(data) => {
                    result.push(
                        webpki::TrustAnchor::try_from_cert_der(&data)
                        .context("certificate data found, \
                                 but trust anchor is invalid")?
                        .into()
                    );
                }
                | rustls_pemfile::Item::RSAKey(_)
                | rustls_pemfile::Item::PKCS8Key(_)
                | rustls_pemfile::Item::ECKey(_)
                => {
                    log::debug!("Skipping private key in cert data");
                }
                _ => {
                    log::debug!("Skipping unknown item cert data");
                }
            }
        }
        Ok(result)
    }
}

fn pki_error(error: webpki::Error) -> rustls::Error {
    use webpki::Error::*;
    match error {
        BadDer | BadDerTime => rustls::Error::InvalidCertificateEncoding,
        InvalidSignatureForPublicKey
            => rustls::Error::InvalidCertificateSignature,
        UnsupportedSignatureAlgorithm
        | UnsupportedSignatureAlgorithmForPublicKey
        => rustls::Error::InvalidCertificateSignatureType,
        e => {
            rustls::Error::InvalidCertificateData(
                format!("invalid peer certificate: {}", e))
        }
    }
}
