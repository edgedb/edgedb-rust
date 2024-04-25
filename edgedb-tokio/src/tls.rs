use std::io;
use std::sync::Arc;

use anyhow::Context;
use rustls::client::danger::HandshakeSignatureValid;
use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring;
use rustls::crypto::WebPkiSupportedAlgorithms;
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tls_api::TlsConnectorBox;
use tls_api::{TlsConnector as _, TlsConnectorBuilder as _};
use tls_api_rustls::TlsConnector;

#[derive(Debug)]
pub struct NullVerifier;

#[derive(Debug)]
pub struct NoHostnameVerifier {
    roots: Arc<rustls::RootCertStore>,
    supported: WebPkiSupportedAlgorithms,
}

impl NoHostnameVerifier {
    pub fn new(roots: Arc<rustls::RootCertStore>) -> Self {
        NoHostnameVerifier {
            roots,
            supported: ring::default_provider().signature_verification_algorithms,
        }
    }
}

impl ServerCertVerifier for NoHostnameVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert = webpki::EndEntityCert::try_from(end_entity).map_err(pki_error)?;

        let result = cert.verify_for_usage(
            self.supported.all,
            &self.roots.roots,
            intermediates,
            now,
            webpki::KeyUsage::server_auth(),
            None,
            None,
        );

        match result {
            Ok(_) => Ok(ServerCertVerified::assertion()),
            Err(e) => Err(pki_error(e)),
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.supported)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.supported)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl ServerCertVerifier for NullVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub fn connector(cert_verifier: Arc<dyn ServerCertVerifier>) -> anyhow::Result<TlsConnectorBox> {
    let mut builder = TlsConnector::builder()?;
    builder
        .config
        .dangerous()
        .set_certificate_verifier(cert_verifier);
    builder.set_alpn_protocols(&[b"edgedb-binary"])?;
    let connector = builder.build()?.into_dyn();
    Ok(connector)
}

pub fn read_root_cert_pem(data: &str) -> anyhow::Result<rustls::RootCertStore> {
    let mut cursor = io::Cursor::new(data);
    let open_data = rustls_pemfile::read_all(&mut cursor);
    let mut cert_store = rustls::RootCertStore::empty();
    for item in open_data {
        match item {
            Ok(rustls_pemfile::Item::X509Certificate(data)) => {
                cert_store
                    .add(data)
                    .context("certificate data found, but is not a valid root certificate")?;
            }
            Ok(rustls_pemfile::Item::Pkcs1Key(_))
            | Ok(rustls_pemfile::Item::Pkcs8Key(_))
            | Ok(rustls_pemfile::Item::Sec1Key(_)) => {
                log::debug!("Skipping private key in cert data");
            }
            Ok(rustls_pemfile::Item::Crl(_)) => {
                log::debug!("Skipping CRL in cert data");
            }
            Ok(_) => {
                log::debug!("Skipping unknown item cert data");
            }
            Err(e) => {
                log::error!("could not parse item in PEM file: {:?}", e);
            }
        }
    }
    Ok(cert_store)
}

fn pki_error(error: webpki::Error) -> rustls::Error {
    use webpki::Error::*;
    match error {
        BadDer | BadDerTime => {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadEncoding)
        }
        InvalidSignatureForPublicKey => {
            rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature)
        }
        e => rustls::Error::InvalidCertificate(rustls::CertificateError::Other(
            rustls::OtherError(Arc::new(e)),
        )),
    }
}
